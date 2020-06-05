//
// Copyright 2019 ZetaSQL Authors
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

#include <algorithm>
#include <set>
#include <tuple>
#include <utility>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/analyzer/analytic_function_resolver.h"
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

size_t FieldPathHash(const ResolvedExpr* expr) {
  DCHECK(expr != nullptr);
  switch (expr->node_kind()) {
    case RESOLVED_GET_PROTO_FIELD: {
      // Note that this only hashes the top-level type (e.g. ARRAY).
      const ResolvedGetProtoField* proto_field =
          expr->GetAs<ResolvedGetProtoField>();
      return absl::Hash<std::tuple<int, int, int, size_t>>()(
          std::make_tuple(expr->node_kind(), expr->type()->kind(),
                          proto_field->field_descriptor()->number(),
                          FieldPathHash(proto_field->expr())));
    }
    case RESOLVED_GET_STRUCT_FIELD: {
      const ResolvedGetStructField* struct_field =
          expr->GetAs<ResolvedGetStructField>();
      // Note that this only hashes the top-level type (e.g. ARRAY).
      return absl::Hash<std::tuple<int, int, int, size_t>>()(std::make_tuple(
          expr->node_kind(), expr->type()->kind(), struct_field->field_idx(),
          FieldPathHash(struct_field->expr())));
    }
    case RESOLVED_COLUMN_REF:
      return absl::Hash<std::pair<int, int>>()(std::make_pair(
          expr->node_kind(),
          expr->GetAs<ResolvedColumnRef>()->column().column_id()));
    default:
      return absl::Hash<int>()(expr->node_kind());
  }
}

bool IsSameFieldPath(const ResolvedExpr* field_path1,
                     const ResolvedExpr* field_path2,
                     FieldPathMatchingOption match_option) {
  // Checking types is a useful optimization to allow returning early. However,
  // we can't rely on Type::Equals() because it considers two otherwise
  // identical proto descriptors from different descriptor pools to be
  // different. So we compare Type::Kinds instead.
  if (field_path1->node_kind() != field_path2->node_kind() ||
      field_path1->type()->kind() != field_path2->type()->kind()) {
    return false;
  }

  switch (field_path1->node_kind()) {
    case RESOLVED_GET_PROTO_FIELD: {
      const ResolvedGetProtoField* proto_field1 =
          field_path1->GetAs<ResolvedGetProtoField>();
      const ResolvedGetProtoField* proto_field2 =
          field_path2->GetAs<ResolvedGetProtoField>();

      const bool field_paths_match =
          (proto_field1->expr()->type()->kind() ==
           proto_field2->expr()->type()->kind()) &&
          proto_field1->field_descriptor()->number() ==
              proto_field2->field_descriptor()->number() &&
          proto_field1->default_value() == proto_field2->default_value() &&
          proto_field1->get_has_bit() == proto_field2->get_has_bit() &&
          proto_field1->format() == proto_field2->format() &&
          IsSameFieldPath(proto_field1->expr(), proto_field2->expr(),
                          match_option);
      if (match_option == FieldPathMatchingOption::kFieldPath) {
        return field_paths_match;
      }
      return field_paths_match &&
             proto_field1->type()->Equals(proto_field2->type()) &&
             proto_field1->expr()->type()->Equals(
                 proto_field2->expr()->type()) &&
             proto_field1->return_default_value_when_unset() ==
                 proto_field2->return_default_value_when_unset();
    }
    case RESOLVED_GET_STRUCT_FIELD: {
      const ResolvedGetStructField* struct_field1 =
          field_path1->GetAs<ResolvedGetStructField>();
      const ResolvedGetStructField* struct_field2 =
          field_path2->GetAs<ResolvedGetStructField>();

      const bool field_paths_match =
          (struct_field1->expr()->type()->kind() ==
           struct_field2->expr()->type()->kind()) &&
          (struct_field1->field_idx() == struct_field2->field_idx()) &&
          IsSameFieldPath(struct_field1->expr(), struct_field2->expr(),
                          match_option);
      if (match_option == FieldPathMatchingOption::kFieldPath) {
        return field_paths_match;
      }
      return field_paths_match &&
             struct_field1->type()->Equals(struct_field2->type());
    }
    case RESOLVED_COLUMN_REF: {
      // Ignore ResolvedColumnRef::is_correlated because it is IGNORABLE and
      // therefore semantically meaningless. If the ResolvedColumnRefs indicate
      // the same ResolvedColumn, then the expressions must be equivalent.
      return field_path1->GetAs<ResolvedColumnRef>()->column() ==
             field_path2->GetAs<ResolvedColumnRef>()->column();
    }
    default:
      return false;
  }
}

void QueryGroupByAndAggregateInfo::Reset() {
  has_group_by = false;
  has_aggregation = false;
  aggregate_expr_map.clear();
  group_by_columns_to_compute.clear();
  group_by_expr_map.clear();
  rollup_column_list.clear();
  aggregate_columns_to_compute.clear();
  group_by_valid_field_info_map.Clear();
  is_post_distinct = false;
}

QueryResolutionInfo::QueryResolutionInfo(Resolver* resolver) {
  select_column_state_list_ = absl::make_unique<SelectColumnStateList>();
  analytic_resolver_ = absl::make_unique<AnalyticFunctionResolver>(resolver);
}

// Keep destructor impl in .cc to resolve circular deps.
QueryResolutionInfo::~QueryResolutionInfo() {}

const ResolvedComputedColumn*
QueryResolutionInfo::AddGroupByComputedColumnIfNeeded(
    const ResolvedColumn& column, std::unique_ptr<const ResolvedExpr> expr) {
  group_by_info_.has_group_by = true;
  const ResolvedComputedColumn*& stored_column =
      group_by_info_.group_by_expr_map[expr.get()];
  if (stored_column != nullptr) {
    return stored_column;
  }
  auto new_column = MakeResolvedComputedColumn(column, std::move(expr));
  stored_column = new_column.get();
  group_by_info_.group_by_columns_to_compute.push_back(std::move(new_column));
  return stored_column;
}

const ResolvedComputedColumn*
QueryResolutionInfo::GetEquivalentGroupByComputedColumnOrNull(
    const ResolvedExpr* expr) const {
  return zetasql_base::FindPtrOrNull(group_by_info_.group_by_expr_map, expr);
}

void QueryResolutionInfo::AddRollupColumn(
    const ResolvedComputedColumn* column) {
  group_by_info_.rollup_column_list.push_back(column);
}

void QueryResolutionInfo::ReleaseGroupingSetsAndRollupList(
    std::vector<std::unique_ptr<const ResolvedGroupingSet>>* grouping_set_list,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>* rollup_column_list) {
  if (group_by_info_.rollup_column_list.empty()) {
    return;
  }

  // group_by_info_.rollup_column_list stores the rollup columns. Grouping sets
  // are computed from the prefixes of group_by_info_.rollup_column_list, and
  // references to the same column are deduplicated within a particular grouping
  // set.
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

  for (const ResolvedComputedColumn* rollup_column :
       group_by_info_.rollup_column_list) {
    auto rollup_column_ref =
        MakeResolvedColumnRef(rollup_column->column().type(),
                              rollup_column->column(), /*is_correlated=*/false);
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
  group_by_info_.rollup_column_list.clear();
  // Order of the rows resulting from ROLLUP are not guaranteed, but engines
  // will generally want to compute aggregates from more to less granular
  // levels of subtotals, e.g. (a, b, c), (a, b), (a), and then ().
  std::reverse(grouping_set_list->begin(), grouping_set_list->end());
}

void QueryResolutionInfo::AddAggregateComputedColumn(
    const ASTFunctionCall* ast_function_call,
    std::unique_ptr<const ResolvedComputedColumn> column) {
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

bool QueryResolutionInfo::HasAnalytic() const {
  return analytic_resolver_->HasAnalytic();
}

void QueryResolutionInfo::ResetAnalyticResolver(Resolver* resolver) {
  analytic_resolver_ = absl::make_unique<AnalyticFunctionResolver>(
      resolver, analytic_resolver_->ReleaseNamedWindowInfoMap());
}

absl::Status QueryResolutionInfo::CheckComputedColumnListsAreEmpty() const {
  ZETASQL_RET_CHECK(select_list_columns_to_compute_before_aggregation_.empty());
  ZETASQL_RET_CHECK(select_list_columns_to_compute_.empty());
  ZETASQL_RET_CHECK(group_by_info_.group_by_columns_to_compute.empty());
  ZETASQL_RET_CHECK(group_by_info_.aggregate_columns_to_compute.empty());
  ZETASQL_RET_CHECK(group_by_info_.rollup_column_list.empty());
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
  absl::StrAppend(&debug_string, "has_group_by: ", group_by_info_.has_group_by,
                  "\n");
  absl::StrAppend(&debug_string,
                  "has_aggregation: ", group_by_info_.has_aggregation, "\n");
  absl::StrAppend(&debug_string, "group_by_columns(size ",
                  group_by_info_.group_by_columns_to_compute.size(), "):\n");
  for (const auto& column : group_by_info_.group_by_columns_to_compute) {
    absl::StrAppend(&debug_string, "  ", column->DebugString(), "\n");
  }
  absl::StrAppend(&debug_string, "aggregate_columns(size ",
                  group_by_info_.aggregate_columns_to_compute.size(), "):\n");
  for (const auto& column : group_by_info_.aggregate_columns_to_compute) {
    absl::StrAppend(&debug_string, "  ", column->DebugString(), "\n");
  }
  absl::StrAppend(&debug_string, "aggregate_expr_map size: ",
                  group_by_info_.aggregate_expr_map.size(), "\n");
  absl::StrAppend(
      &debug_string, "group_by_valid_field_info:\n",
      group_by_info_.group_by_valid_field_info_map.DebugString("  "));
  absl::StrAppend(&debug_string, "select_list_valid_field_info:\n",
                  select_list_valid_field_info_map_.DebugString("  "));

  return debug_string;
}

const ResolvedExpr* UntypedLiteralMap::Find(const ResolvedColumn& column) {
  if (column_id_to_untyped_literal_map_ == nullptr) {
    if (scan_ == nullptr || scan_->node_kind() != RESOLVED_PROJECT_SCAN) {
      return nullptr;
    }

    // Populate column_id_to_untyped_literal_map_.
    column_id_to_untyped_literal_map_ =
        absl::make_unique<absl::flat_hash_map<int, const ResolvedExpr*>>();
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
                              column.column_id(), nullptr);
}

}  // namespace zetasql
