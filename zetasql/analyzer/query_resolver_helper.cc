//
// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "zetasql/analyzer/query_resolver_helper.h"

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zetasql/analyzer/analytic_function_resolver.h"
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/parser/parse_tree_visitor.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

// Converts a list of ResolvedComputedColumn to a list ResolvedColumnRef.
std::vector<std::unique_ptr<const ResolvedColumnRef>> MakeResolvedColumnRefs(
    absl::Span<const ResolvedComputedColumn* const> column_list) {
  absl::flat_hash_set<ResolvedColumn> distinct_column_set;
  std::vector<std::unique_ptr<const ResolvedColumnRef>> column_ref_list;
  for (const ResolvedComputedColumn* computed_column : column_list) {
    std::unique_ptr<ResolvedColumnRef> column_ref = MakeResolvedColumnRef(
        computed_column->column().type(), computed_column->column(),
        /*is_correlated=*/false);
    // Don't duplicate columns in a the grouping set.
    if (zetasql_base::InsertIfNotPresent(&distinct_column_set, column_ref->column())) {
      column_ref_list.push_back(std::move(column_ref));
    }
  }
  return column_ref_list;
}

// Releases rollup list to rollup_column_list and grouping_set_list, this is the
// legacy way to represent rollup list in ResolvedAggregateScan. The method will
// called only when FEATURE_V_1_4_GROUPING_SETS isn't enabled, and it will be
// deprecated soon using the new representation of rollup, see more in
// (broken link).
absl::Status ReleaseLegacyRollupColumnList(
    std::vector<GroupingSetInfo>& grouping_set_info_list,
    std::vector<std::unique_ptr<const ResolvedGroupingSetBase>>*
        grouping_set_list,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>* rollup_column_list) {
  // Extract the rollup column list from the grouping_set_info_list.
  std::vector<const ResolvedComputedColumn*> rollup_columns;
  for (const GroupingSetInfo& grouping_set : grouping_set_info_list) {
    ZETASQL_RET_CHECK(grouping_set.kind == GroupingSetKind::kRollup);
    for (const ResolvedComputedColumnList& column_list :
         grouping_set.grouping_set_item_list) {
      ZETASQL_RET_CHECK_EQ(column_list.size(), 1)
          << "There should be exactly one column in the column_list";
      rollup_columns.push_back(column_list.front());
    }
  }

  if (rollup_columns.empty()) {
    return absl::InvalidArgumentError("rollup column list is empty");
  }
  // group_by_info_.rollup_column_list stores the rollup columns. Grouping
  // sets are computed from the prefixes of
  // group_by_info_.rollup_column_list, and references to the same column
  // are deduplicated within a particular grouping set.
  //
  // For example, suppose that group_by_info_.rollup_column_list contains
  // columns b, a, c, a. This function will return:
  //
  // rollup_list: b, a, c, a.
  // grouping_set_list: {b, a, c}, {b, a, c}, {b, a}, {b}, {}.

  // Unowned pointers.
  std::vector<const ResolvedColumnRef*> current_grouping_set;
  std::set<ResolvedColumn> distinct_rollup_columns;

  // Add the empty grouping set.
  grouping_set_list->push_back(MakeResolvedGroupingSet());

  for (const ResolvedComputedColumn* rollup_column : rollup_columns) {
    auto rollup_column_ref = MakeResolvedColumnRef(
        rollup_column->column().type(), rollup_column->column(),
        /*is_correlated=*/false);
    // Don't duplicate columns in the grouping sets.
    if (zetasql_base::InsertIfNotPresent(&distinct_rollup_columns,
                                rollup_column_ref->column())) {
      current_grouping_set.push_back(rollup_column_ref.get());
    }
    rollup_column_list->push_back(std::move(rollup_column_ref));
    std::vector<std::unique_ptr<const ResolvedColumnRef>> grouping_set_columns;
    grouping_set_columns.reserve(current_grouping_set.size());
    for (const ResolvedColumnRef* grouping_column : current_grouping_set) {
      grouping_set_columns.push_back(MakeResolvedColumnRef(
          grouping_column->column().type(), grouping_column->column(),
          /*is_correlated=*/false));
    }
    grouping_set_list->push_back(
        MakeResolvedGroupingSet(std::move(grouping_set_columns)));
  }
  // Order of the rows resulting from ROLLUP are not guaranteed, but engines
  // will generally want to compute aggregates from more to less granular
  // levels of subtotals, e.g. (a, b, c), (a, b), (a), and then ().
  std::reverse(grouping_set_list->begin(), grouping_set_list->end());

  grouping_set_info_list.clear();
  return absl::OkStatus();
}

// Traverse the parser AST for an `ASTSelectColumn` and look for evidence that
// the column should have it's first-pass resolution deferred until after the
// GROUP BY clause is resolved. A column should have it's first-pass resolution
// deferred if it uses `GROUP ROWS` or a `GROUP BY` aggregate function modifier
// outside of an expression subquery. See comments in
// `ResolveSelectListExprsFirstPass` for more details.
//
// For instance, the following expression should have it's resolution deferred:
//    `1 + SUM(... GROUP BY...) + SUM(X) WITH GROUP ROWS (...)`
// But the following expression should not:
//    `(SELECT 1 + SUM(... GROUP BY...) + SUM(X) WITH GROUP ROWS (...) FROM...)`
class DeferredResolutionFinder : public NonRecursiveParseTreeVisitor {
 public:
  DeferredResolutionFinder() = default;
  DeferredResolutionFinder(const DeferredResolutionFinder&) = delete;
  DeferredResolutionFinder(DeferredResolutionFinder&&) = delete;
  DeferredResolutionFinder& operator=(const DeferredResolutionFinder&) = delete;
  DeferredResolutionFinder& operator=(DeferredResolutionFinder&&) = delete;

  absl::StatusOr<VisitResult> defaultVisit(const ASTNode* node) override {
    return VisitResult::VisitChildren(node);
  }

  absl::StatusOr<VisitResult> visitASTExpressionSubquery(
      const ASTExpressionSubquery* node) override {
    return VisitResult::Empty();
  }

  absl::StatusOr<VisitResult> visitASTFunctionCall(
      const ASTFunctionCall* node) override {
    if (node->group_by() != nullptr || node->with_group_rows() != nullptr) {
      info_.has_outer_group_rows_or_group_by_modifiers = true;
    }
    return VisitResult::VisitChildren(node);
  };

  absl::StatusOr<VisitResult> visitASTAnalyticFunctionCall(
      const ASTAnalyticFunctionCall* node) override {
    info_.has_outer_analytic_function = true;
    return VisitResult::VisitChildren(node);
  };

  DeferredResolutionSelectColumnInfo GetDeferredResolutionSelectColumnInfo()
      const {
    return info_;
  }

 private:
  DeferredResolutionSelectColumnInfo info_;
};

}  // namespace

void QueryGroupByAndAggregateInfo::Reset() {
  has_group_by = false;
  has_aggregation = false;
  is_group_by_all = false;
  aggregate_expr_map.clear();
  group_by_column_state_list.clear();
  group_by_expr_map.clear();
  grouping_call_list.clear();
  grouping_output_columns.clear();
  grouping_set_list.clear();
  aggregate_columns_to_compute.clear();
  group_by_valid_field_info_map.Clear();
  is_post_distinct = false;
}

const Type* SelectColumnState::GetType() const {
  if (resolved_select_column.IsInitialized()) {
    return resolved_select_column.type();
  }
  if (resolved_expr != nullptr) {
    return resolved_expr->type();
  }
  return nullptr;
}

std::string SelectColumnState::DebugString(absl::string_view indent) const {
  std::string debug_string;
  absl::StrAppend(&debug_string, indent, "expr:\n   ", ast_expr->DebugString(),
                  "\n");
  absl::StrAppend(&debug_string, indent, "alias: ", alias.ToStringView(), "\n");
  absl::StrAppend(&debug_string, indent, "is_explicit: ", is_explicit, "\n");
  absl::StrAppend(&debug_string, indent,
                  "select_list_position: ", select_list_position, "\n");
  absl::StrAppend(
      &debug_string, indent, "resolved_expr:\n  ",
      (resolved_expr != nullptr ? resolved_expr->DebugString() : "<null>"),
      "\n");
  absl::StrAppend(&debug_string, indent, "resolved_computed_column:\n  ",
                  (resolved_computed_column != nullptr
                       ? resolved_computed_column->DebugString()
                       : "<null>"),
                  "\n");
  absl::StrAppend(&debug_string, indent, "has_aggregation: ", has_aggregation,
                  "\n");
  absl::StrAppend(&debug_string, indent, "has_analytic: ", has_analytic, "\n");
  absl::StrAppend(&debug_string, indent,
                  "is_group_by_column: ", is_group_by_column, "\n");
  absl::StrAppend(&debug_string, indent, "resolved_select_column: ",
                  (resolved_select_column.IsInitialized()
                       ? resolved_select_column.DebugString()
                       : "<uninitialized>"),
                  "\n");
  absl::StrAppend(&debug_string, indent,
                  "resolved_pre_group_by_select_column: ",
                  (resolved_pre_group_by_select_column.IsInitialized()
                       ? resolved_pre_group_by_select_column.DebugString()
                       : "<uninitialized>"));
  return debug_string;
}

void SelectColumnStateList::AddSelectColumn(
    const ASTSelectColumn* ast_select_column, IdString alias, bool is_explicit,
    bool has_aggregation, bool has_analytic, bool has_volatile,
    std::unique_ptr<const ResolvedExpr> resolved_expr) {
  AddSelectColumn(std::make_unique<SelectColumnState>(
      ast_select_column, alias, is_explicit, has_aggregation, has_analytic,
      has_volatile, std::move(resolved_expr)));
}

absl::Status SelectColumnStateList::ReplaceSelectColumn(
    int index, std::unique_ptr<SelectColumnState> new_select_column_state) {
  ZETASQL_RET_CHECK_LT(index, select_column_state_list_.size());
  select_column_state_list_[index] = std::move(new_select_column_state);
  return absl::OkStatus();
}

void SelectColumnStateList::AddSelectColumn(
    std::unique_ptr<SelectColumnState> select_column_state) {
  ABSL_DCHECK_EQ(select_column_state->select_list_position, -1);
  select_column_state->select_list_position =
      static_cast<int>(select_column_state_list_.size());
  // Save a mapping from the alias to this SelectColumnState. The mapping is
  // later used for validations performed by
  // FindAndValidateSelectColumnStateByAlias().
  const IdString alias = select_column_state->alias;
  if (!IsInternalAlias(alias)) {
    if (!zetasql_base::InsertIfNotPresent(&column_alias_to_state_list_position_, alias,
                                 select_column_state->select_list_position)) {
      // Now ambiguous.
      column_alias_to_state_list_position_[alias] = -1;
    }
  }
  select_column_state_list_.emplace_back(std::move(select_column_state));
}

absl::Status SelectColumnStateList::FindAndValidateSelectColumnStateByAlias(
    const char* clause_name, const ASTNode* ast_location, IdString alias,
    const ExprResolutionInfo* expr_resolution_info,
    const SelectColumnState** select_column_state) const {
  *select_column_state = nullptr;
  // TODO Should probably do this more generally with name scoping.
  const int* state_list_position =
      zetasql_base::FindOrNull(column_alias_to_state_list_position_, alias);
  if (state_list_position != nullptr) {
    if (*state_list_position == -1) {
      return MakeSqlErrorAt(ast_location)
             << "Name " << alias << " in " << clause_name
             << " is ambiguous; it may refer to multiple columns in the"
                " SELECT-list";
    } else {
      const SelectColumnState* found_select_column_state =
          select_column_state_list_[*state_list_position].get();
      ZETASQL_RETURN_IF_ERROR(ValidateAggregateAndAnalyticSupport(
          alias.ToStringView(), ast_location, found_select_column_state,
          expr_resolution_info));
      *select_column_state = found_select_column_state;
    }
  }
  return absl::OkStatus();
}

absl::Status SelectColumnStateList::FindAndValidateSelectColumnStateByOrdinal(
    absl::string_view expr_description, const ASTNode* ast_location,
    const int64_t ordinal, const ExprResolutionInfo* expr_resolution_info,
    const SelectColumnState** select_column_state) const {
  *select_column_state = nullptr;
  if (ordinal < 1 || ordinal > select_column_state_list_.size()) {
    return MakeSqlErrorAt(ast_location)
           << expr_description
           << " is out of SELECT column number range: " << ordinal;
  }
  const SelectColumnState* found_select_column_state =
      select_column_state_list_[ordinal - 1].get();  // Convert to 0-based.
  ZETASQL_RETURN_IF_ERROR(ValidateAggregateAndAnalyticSupport(
      absl::StrCat(ordinal), ast_location, found_select_column_state,
      expr_resolution_info));
  *select_column_state = found_select_column_state;
  return absl::OkStatus();
}

absl::Status SelectColumnStateList::ValidateAggregateAndAnalyticSupport(
    const absl::string_view column_description, const ASTNode* ast_location,
    const SelectColumnState* select_column_state,
    const ExprResolutionInfo* expr_resolution_info) {
  // If `contains_outer_group_rows_or_group_by_modifiers` is true, then
  // `has_aggregation` should also be true. This condition is just to help
  // provide a better error message if a user writes something like
  // 'SCALAR_FUNCTION(...) WITH GROUP ROWS (...)'.
  if (select_column_state->contains_outer_group_rows_or_group_by_modifiers &&
      !expr_resolution_info->allows_aggregation) {
    ZETASQL_RET_CHECK(select_column_state->has_aggregation);
    return MakeSqlErrorAt(ast_location)
           << "Column " << column_description
           << " contains a GROUP ROWS subquery or a GROUP BY modifier, which "
              "is not allowed in "
           << expr_resolution_info->clause_name
           << (expr_resolution_info->is_post_distinct()
                   ? " after SELECT DISTINCT"
                   : "");
  }
  if (select_column_state->has_aggregation &&
      !expr_resolution_info->allows_aggregation) {
    return MakeSqlErrorAt(ast_location)
           << "Column " << column_description
           << " contains an aggregation function, which is not allowed in "
           << expr_resolution_info->clause_name
           << (expr_resolution_info->is_post_distinct()
                   ? " after SELECT DISTINCT"
                   : "");
  }
  if (select_column_state->has_analytic &&
      !expr_resolution_info->allows_analytic) {
    return MakeSqlErrorAt(ast_location)
           << "Column " << column_description
           << " contains an analytic function, which is not allowed in "
           << expr_resolution_info->clause_name
           << (expr_resolution_info->is_post_distinct()
                   ? " after SELECT DISTINCT"
                   : "");
  }
  return absl::OkStatus();
}

SelectColumnState* SelectColumnStateList::GetSelectColumnState(
    int select_list_position) {
  ABSL_CHECK_GE(select_list_position, 0);
  ABSL_CHECK_LT(select_list_position, select_column_state_list_.size());
  return select_column_state_list_[select_list_position].get();
}

const SelectColumnState* SelectColumnStateList::GetSelectColumnState(
    int select_list_position) const {
  ABSL_CHECK_GE(select_list_position, 0);
  ABSL_CHECK_LT(select_list_position, select_column_state_list_.size());
  return select_column_state_list_[select_list_position].get();
}

const std::vector<std::unique_ptr<SelectColumnState>>&
SelectColumnStateList::select_column_state_list() const {
  return select_column_state_list_;
}

size_t SelectColumnStateList::Size() const {
  return select_column_state_list_.size();
}

std::string SelectColumnStateList::DebugString() const {
  std::string debug_string("SelectColumnStateList, size = ");
  absl::StrAppend(&debug_string, Size(), "\n");
  for (int idx = 0; idx < Size(); ++idx) {
    absl::StrAppend(&debug_string, "    [", idx, "]:\n",
                    GetSelectColumnState(idx)->DebugString("       "), "\n");
  }
  absl::StrAppend(&debug_string, "  alias map:\n");
  for (const auto& alias_to_position : column_alias_to_state_list_position_) {
    absl::StrAppend(&debug_string, "    ",
                    alias_to_position.first.ToStringView(), " : ",
                    alias_to_position.second, "\n");
  }
  return debug_string;
}

QueryResolutionInfo::QueryResolutionInfo(Resolver* resolver) {
  select_column_state_list_ = std::make_unique<SelectColumnStateList>();
  analytic_resolver_ = std::make_unique<AnalyticFunctionResolver>(resolver);
}

// Keep destructor impl in .cc to resolve circular deps.
QueryResolutionInfo::~QueryResolutionInfo() {}

const ResolvedComputedColumn*
QueryResolutionInfo::AddGroupByComputedColumnIfNeeded(
    const ResolvedColumn& column, std::unique_ptr<const ResolvedExpr> expr,
    const ResolvedExpr* pre_group_by_expr) {
  group_by_info_.has_group_by = true;
  const ResolvedComputedColumn*& stored_column =
      group_by_info_.group_by_expr_map[expr.get()];
  if (stored_column != nullptr) {
    return stored_column;
  }
  auto new_column = MakeResolvedComputedColumn(column, std::move(expr));
  stored_column = new_column.get();
  group_by_info_.group_by_column_state_list.emplace_back(std::move(new_column),
                                                         pre_group_by_expr);
  return stored_column;
}

const ResolvedComputedColumn*
QueryResolutionInfo::GetEquivalentGroupByComputedColumnOrNull(
    const ResolvedExpr* expr) const {
  return zetasql_base::FindPtrOrNull(group_by_info_.group_by_expr_map, expr);
}

void QueryResolutionInfo::AddGroupingSet(const GroupingSetInfo& grouping_set) {
  group_by_info_.grouping_set_list.push_back(grouping_set);
}

void QueryResolutionInfo::AddGroupingColumn(
    std::unique_ptr<const ResolvedGroupingCall> column) {
  group_by_info_.grouping_call_list.push_back(std::move(column));
}

// Add the grouping column to the expr map, but since at this point it's an
// AggregateFunctionCall, don't add it to the grouping_call_list yet. That will
// happen during the second pass of the selectList, where the
// correct group_by column reference is resolved.
absl::Status QueryResolutionInfo::AddGroupingColumnToExprMap(
    const ASTFunctionCall* ast_function_call,
    std::unique_ptr<const ResolvedComputedColumn> grouping_output_col) {
  group_by_info_.has_aggregation = true;
  ZETASQL_RET_CHECK(ast_function_call != nullptr);
  zetasql_base::InsertIfNotPresent(&group_by_info_.aggregate_expr_map, ast_function_call,
                          grouping_output_col.get());
  group_by_info_.grouping_output_columns.push_back(
      std::move(grouping_output_col));
  return absl::OkStatus();
}

absl::Status QueryResolutionInfo::ReleaseGroupingSetsAndRollupList(
    std::vector<std::unique_ptr<const ResolvedGroupingSetBase>>*
        grouping_set_list,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>* rollup_column_list,
    const LanguageOptions& language_options) {
  if (group_by_info_.grouping_set_list.empty()) {
    return absl::OkStatus();
  }

  // Release the rollup column list to the legacy resolved ast representation
  // when grouping sets feature isn't enabled.
  if (!language_options.LanguageFeatureEnabled(FEATURE_V_1_4_GROUPING_SETS)) {
    return ReleaseLegacyRollupColumnList(group_by_info_.grouping_set_list,
                                         grouping_set_list, rollup_column_list);
  }

  for (const GroupingSetInfo& grouping_set : group_by_info_.grouping_set_list) {
    if (grouping_set.kind == GroupingSetKind::kGroupingSet) {
      std::vector<const ResolvedComputedColumn*> grouping_set_columns;
      for (const ResolvedComputedColumnList& column_list :
           grouping_set.grouping_set_item_list) {
        ZETASQL_RET_CHECK_LE(column_list.size(), 1)
            << "There should be at most one column in the column_list for a "
               "grouping set";
        // An empty list means an empty grouping set.
        if (!column_list.empty()) {
          grouping_set_columns.push_back(column_list.front());
        }
      }
      grouping_set_list->push_back(MakeResolvedGroupingSet(
          MakeResolvedColumnRefs(absl::MakeSpan(grouping_set_columns))));
    } else {
      std::vector<std::unique_ptr<const ResolvedGroupingSetMultiColumn>>
          multi_columns;
      for (const ResolvedComputedColumnList& column_list :
           grouping_set.grouping_set_item_list) {
        std::vector<std::unique_ptr<const ResolvedColumnRef>> column_ref_list =
            MakeResolvedColumnRefs(absl::MakeSpan(column_list));
        ZETASQL_RET_CHECK_GT(column_ref_list.size(), 0)
            << "At least one column in the rollup or cube's column list";
        multi_columns.push_back(
            MakeResolvedGroupingSetMultiColumn(std::move(column_ref_list)));
      }
      ZETASQL_RET_CHECK_GT(multi_columns.size(), 0)
          << "rollup or cube column list can not be empty";
      if (grouping_set.kind == GroupingSetKind::kRollup) {
        grouping_set_list->push_back(
            MakeResolvedRollup(std::move(multi_columns)));
      } else {
        grouping_set_list->push_back(
            MakeResolvedCube(std::move(multi_columns)));
      }
    }
  }
  group_by_info_.grouping_set_list.clear();
  return absl::OkStatus();
}

void QueryResolutionInfo::AddAggregateComputedColumn(
    const ASTFunctionCall* ast_function_call,
    std::unique_ptr<const ResolvedComputedColumnBase> column) {
  group_by_info_.has_aggregation = true;
  if (ast_function_call != nullptr) {
    zetasql_base::InsertIfNotPresent(&group_by_info_.aggregate_expr_map,
                            ast_function_call, column.get());
  }
  group_by_info_.aggregate_columns_to_compute.push_back(std::move(column));
}

absl::Status QueryResolutionInfo::SelectListColumnHasAnalytic(
    const ResolvedColumn& column, bool* has_analytic) const {
  for (const std::unique_ptr<SelectColumnState>& select_column_state :
       select_column_state_list_->select_column_state_list()) {
    if (select_column_state->resolved_select_column == column) {
      *has_analytic = select_column_state->has_analytic;
      return absl::OkStatus();
    }
  }
  ZETASQL_RET_CHECK_FAIL() << "SelectListColumnHasAnalytic <column> is not a SELECT "
                      "list resolved column";
}

absl::Status QueryResolutionInfo::GetAndRemoveSelectListColumnsWithoutAnalytic(
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
        select_columns_without_analytic_out) {
  // Split <select_list_columns_to_compute_> into those that contain analytic
  // functions and those that do not. We do not add columns to
  // <select_columns_without_analytic_out> until the end since otherwise
  // if an error occurs, an ResolvedComputedColumn may appear in both
  // <select_columns_without_analytic_out> and
  // <select_columns_without_analytic_out>, and be deleted twice.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      select_columns_without_analytic;
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      select_columns_with_analytic;
  for (std::unique_ptr<const ResolvedComputedColumn>& computed_column :
       select_list_columns_to_compute_) {
    bool has_analytic = false;
    ZETASQL_RETURN_IF_ERROR(
        SelectListColumnHasAnalytic(computed_column->column(), &has_analytic));
    if (has_analytic) {
      select_columns_with_analytic.push_back(std::move(computed_column));
    } else {
      select_columns_without_analytic.push_back(std::move(computed_column));
    }
  }

  *select_columns_without_analytic_out =
      std::move(select_columns_without_analytic);
  select_list_columns_to_compute_ = std::move(select_columns_with_analytic);
  return absl::OkStatus();
}

ResolvedColumnList QueryResolutionInfo::GetResolvedColumnList() const {
  ResolvedColumnList resolved_column_list;
  for (const std::unique_ptr<SelectColumnState>& select_column_state :
       select_column_state_list_->select_column_state_list()) {
    resolved_column_list.push_back(select_column_state->resolved_select_column);
  }
  return resolved_column_list;
}

bool QueryResolutionInfo::HasAnalytic() const {
  return analytic_resolver_->HasAnalytic();
}

bool QueryResolutionInfo::HasAggregation() const {
  return group_by_info_.has_aggregation;
}

void QueryResolutionInfo::SetHasAggregation(bool value) {
  group_by_info_.has_aggregation = value;
}

bool QueryResolutionInfo::SelectFormAllowsSelectStar() const {
  switch (select_form_) {
    case SelectForm::kClassic:
      return true;
    default:
      return false;
  }
}

bool QueryResolutionInfo::SelectFormAllowsAggregation() const {
  switch (select_form_) {
    case SelectForm::kClassic:
      return true;
    default:
      return false;
  }
}

bool QueryResolutionInfo::SelectFormAllowsAnalytic() const {
  switch (select_form_) {
    case SelectForm::kClassic:
      return true;
    default:
      return false;
  }
}

const char* QueryResolutionInfo::SelectFormClauseName() const {
  switch (select_form_) {
    case SelectForm::kClassic:
      return "SELECT";
    case SelectForm::kNoFrom:
      return "SELECT without FROM clause";
  }
}

void QueryResolutionInfo::ResetAnalyticResolver(Resolver* resolver) {
  analytic_resolver_ = std::make_unique<AnalyticFunctionResolver>(
      resolver, analytic_resolver_->ReleaseNamedWindowInfoMap());
}

absl::Status QueryResolutionInfo::CheckComputedColumnListsAreEmpty() const {
  ZETASQL_RET_CHECK(select_list_columns_to_compute_before_aggregation_.empty());
  ZETASQL_RET_CHECK(select_list_columns_to_compute_.empty());
  ZETASQL_RET_CHECK(group_by_info_.group_by_column_state_list.empty());
  ZETASQL_RET_CHECK(group_by_info_.aggregate_columns_to_compute.empty());
  ZETASQL_RET_CHECK(group_by_info_.grouping_set_list.empty());
  ZETASQL_RET_CHECK(order_by_columns_to_compute_.empty());
  ZETASQL_RET_CHECK(!analytic_resolver_->HasWindowColumnsToCompute());
  return absl::OkStatus();
}

void QueryResolutionInfo::ClearGroupByInfo() {
  group_by_info_.Reset();
}

std::string QueryResolutionInfo::DebugString() const {
  std::string debug_string;
  absl::StrAppend(&debug_string, "\nselect_column_state_list: ",
                  (select_column_state_list_ == nullptr
                       ? "NULL"
                       : select_column_state_list_->DebugString()),
                  "\n");
  absl::StrAppend(&debug_string, "has_order_by: ", has_order_by_, "\n");
  absl::StrAppend(&debug_string, "has_group_by: ", group_by_info_.has_group_by,
                  "\n");
  absl::StrAppend(&debug_string,
                  "has_aggregation: ", group_by_info_.has_aggregation, "\n");
  absl::StrAppend(&debug_string,
                  "is_group_by_all: ", group_by_info_.is_group_by_all, "\n");

  const absl::string_view select_with_mode_str = [&] {
    switch (select_with_mode_) {
      case SelectWithMode::NONE:
        return "NONE";
      case SelectWithMode::ANONYMIZATION:
        return "ANONYMIZATION";
      case SelectWithMode::DIFFERENTIAL_PRIVACY:
        return "DIFFERENTIAL_PRIVACY";
      case SelectWithMode::AGGREGATION_THRESHOLD:
        return "AGGREGATION_THRESHOLD";
    }
  }();
  absl::StrAppend(&debug_string, "select_with_mode: ", select_with_mode_str,
                  "\n");
  absl::StrAppend(&debug_string, "group_by_column_state_list(size ",
                  group_by_info_.group_by_column_state_list.size(), "):\n");
  for (const GroupByColumnState& group_by_column_state :
       group_by_info_.group_by_column_state_list) {
    absl::StrAppend(&debug_string, group_by_column_state.DebugString("  "),
                    "\n");
  }
  absl::StrAppend(&debug_string, "aggregate_columns(size ",
                  group_by_info_.aggregate_columns_to_compute.size(), "):\n");
  for (const auto& column : group_by_info_.aggregate_columns_to_compute) {
    absl::StrAppend(&debug_string, "  ", column->DebugString(), "\n");
  }
  absl::StrAppend(&debug_string, "grouping_call_list(size ",
                  group_by_info_.grouping_call_list.size(), "):\n");
  for (const auto& grouping_call : group_by_info_.grouping_call_list) {
    absl::StrAppend(&debug_string, "  ", grouping_call->DebugString(), "\n");
  }
  absl::StrAppend(&debug_string, "grouping_output_columns(size ",
                  group_by_info_.grouping_output_columns.size(), "):\n");
  for (const auto& column : group_by_info_.grouping_output_columns) {
    absl::StrAppend(&debug_string, "  ", column->DebugString(), "\n");
  }
  absl::StrAppend(&debug_string, "aggregate_expr_map size: ",
                  group_by_info_.aggregate_expr_map.size(), "\n");
  absl::StrAppend(
      &debug_string, "group_by_valid_field_info:\n",
      group_by_info_.group_by_valid_field_info_map.DebugString("  "));
  absl::StrAppend(&debug_string, "select_list_valid_field_info:\n",
                  select_list_valid_field_info_map_.DebugString("  "));
  absl::StrAppend(
      &debug_string, "select_list_columns_to_compute_before_aggregation [",
      absl::StrJoin(select_list_columns_to_compute_before_aggregation_, ", ",
                    [](std::string* out, const auto& column) {
                      absl::StrAppend(out, column->column().DebugString());
                    }),
      "]\n");
  absl::StrAppend(
      &debug_string,
      "dot_star_columns_with_aggregation_for_second_pass_resolution:[",
      absl::StrJoin(
          dot_star_columns_with_aggregation_for_second_pass_resolution_, ", ",
          [](std::string* out, const auto& column) {
            absl::StrAppend(out, column.first.DebugString());
          }),
      "]\n");
  absl::StrAppend(
      &debug_string,
      "dot_star_columns_with_analytic_for_second_pass_resolution:[",
      absl::StrJoin(dot_star_columns_with_analytic_for_second_pass_resolution_,
                    ", ",
                    [](std::string* out, const auto& column) {
                      absl::StrAppend(out, column.first.DebugString());
                    }),
      "]\n");
  return debug_string;
}

const ResolvedExpr* UntypedLiteralMap::Find(const ResolvedColumn& column) {
  if (column_id_to_untyped_literal_map_ == nullptr) {
    if (scan_ == nullptr || scan_->node_kind() != RESOLVED_PROJECT_SCAN) {
      return nullptr;
    }

    // Populate column_id_to_untyped_literal_map_.
    column_id_to_untyped_literal_map_ =
        std::make_unique<absl::flat_hash_map<int, const ResolvedExpr*>>();
    for (const auto& computed_column :
        scan_->GetAs<ResolvedProjectScan>()->expr_list()) {
      const ResolvedExpr* expr = computed_column->expr();
      if (expr != nullptr && expr->node_kind() == RESOLVED_LITERAL) {
        const ResolvedLiteral* literal = expr->GetAs<ResolvedLiteral>();
        if (!literal->has_explicit_type() &&
            (literal->value().is_null() ||
             literal->value().is_empty_array())) {
          column_id_to_untyped_literal_map_->emplace(
              computed_column->column().column_id(), expr);
        }
      }
    }
  }

  return zetasql_base::FindWithDefault(*column_id_to_untyped_literal_map_,
                              column.column_id());
}

absl::StatusOr<DeferredResolutionSelectColumnInfo>
GetDeferredResolutionSelectColumnInfo(const ASTSelectColumn* column) {
  DeferredResolutionFinder deferred_resolution_finder;
  ZETASQL_RETURN_IF_ERROR(column->TraverseNonRecursive(&deferred_resolution_finder));
  return deferred_resolution_finder.GetDeferredResolutionSelectColumnInfo();
}

}  // namespace zetasql
