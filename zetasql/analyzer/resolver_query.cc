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

// This file contains the implementation of query-related (i.e. SELECT)
// resolver methods from resolver.h.
#include <ctype.h>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <stack>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/analyzer/analytic_function_resolver.h"
#include "zetasql/analyzer/expr_matching_helpers.h"
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/function_resolver.h"
#include "zetasql/analyzer/function_signature_matcher.h"
#include "zetasql/analyzer/input_argument_type_resolver_helper.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/analyzer/recursive_queries.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/analyzer/resolver_common_inl.h"
// This includes common macro definitions to define in the resolver cc files.
#include "zetasql/common/string_util.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/annotation/collation.h"
#include "zetasql/public/cast.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/cycle_detector.h"
#include "zetasql/public/function.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/enum_type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/make_node_vector.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_ast_helper.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_collation.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/base/case.h"
#include "absl/base/casts.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// These are constant identifiers used mostly for generated column or table
// names.  We use a single IdString for each so we never have to allocate
// or copy these strings again.
const IdString& Resolver::kArrayId =
    *new IdString(IdString::MakeGlobal("$array"));
const IdString& Resolver::kOffsetAlias =
    *new IdString(IdString::MakeGlobal("offset"));
const IdString& Resolver::kWeightAlias =
    *new IdString(IdString::MakeGlobal("weight"));
const IdString& Resolver::kArrayOffsetId =
    *new IdString(IdString::MakeGlobal("$array_offset"));
const IdString& Resolver::kLambdaArgId =
    *new IdString(IdString::MakeGlobal("$lambda_arg"));
const IdString& Resolver::kWithActionId =
    *new IdString(IdString::MakeGlobal("$with_action"));

STATIC_IDSTRING(kDistinctId, "$distinct");
STATIC_IDSTRING(kFullJoinId, "$full_join");
STATIC_IDSTRING(kGroupById, "$groupby");
STATIC_IDSTRING(kMakeProtoId, "$make_proto");
STATIC_IDSTRING(kMakeStructId, "$make_struct");
STATIC_IDSTRING(kOrderById, "$orderby");
STATIC_IDSTRING(kPreGroupById, "$pre_groupby");
STATIC_IDSTRING(kPreProjectId, "$preproject");
STATIC_IDSTRING(kProtoId, "$proto");
STATIC_IDSTRING(kStructId, "$struct");
STATIC_IDSTRING(kValueColumnId, "$value_column");
STATIC_IDSTRING(kCastedColumnId, "$casted_column");
STATIC_IDSTRING(kWeightId, "$sample_weight");
STATIC_IDSTRING(kDummyTableId, "$dummy_table");
STATIC_IDSTRING(kPivotId, "$pivot");
STATIC_IDSTRING(kUnpivotColumnId, "$unpivot");

absl::Status Resolver::ResolveQueryAfterWith(
    const ASTQuery* query, const NameScope* scope, IdString query_alias,
    std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  // Note: If <query> is the input to a PIVOT clause, force each projection
  // to create a new column. This is necessary because it is possible for a
  // pivot clause to reference some, but not all, projections of the column, and
  // the rules for PIVOT require only unreferenced columns to be used for
  // grouping; collapsing projections to their underlying column would lose this
  // distinction and cause the list of referenced columns to be calculated
  // incorrectly. As an example, consider the following query:
  //   SELECT * FROM (SELECT x AS y, x AS z FROM t)
  //                 PIVOT (SUM(y) FOR y IN (0,1))
  //
  // If y, and z were collapsed, z would be excluded in the groupby list
  // because we would see y and z as the same column, so the reference to y
  // would incorrectly count as a reference to z.
  bool force_new_columns_for_projected_outputs = query->is_pivot_input();

  if (query->query_expr()->node_kind() == AST_SELECT) {
    // If we just have a single SELECT, then we treat that specially so
    // we can resolve the ORDER BY and LIMIT directly inside that SELECT.
    return ResolveSelect(query->query_expr()->GetAsOrDie<ASTSelect>(),
                         query->order_by(), query->limit_offset(), scope,
                         query_alias, force_new_columns_for_projected_outputs,
                         output, output_name_list);
  }

  ZETASQL_RETURN_IF_ERROR(ResolveQueryExpression(
      query->query_expr(), scope, query_alias,
      force_new_columns_for_projected_outputs, output, output_name_list));

  if (query->order_by() != nullptr) {
    const std::unique_ptr<const NameScope> query_expression_name_scope(
        new NameScope(scope, *output_name_list));
    ZETASQL_RETURN_IF_ERROR(ResolveOrderByAfterSetOperations(
        query->order_by(), query_expression_name_scope.get(),
        std::move(*output), output));
  }

  if (query->limit_offset() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveLimitOffsetScan(query->limit_offset(),
                                           std::move(*output), output));
  }

  return absl::OkStatus();
}

void Resolver::AddNamedSubquery(const std::vector<IdString>& alias,
                                std::unique_ptr<NamedSubquery> named_subquery) {
  auto it = named_subquery_map_.find(alias);
  if (it == named_subquery_map_.end()) {
    named_subquery_map_[std::vector<IdString>{alias}] =
        std::vector<std::unique_ptr<NamedSubquery>>{};
    it = named_subquery_map_.find(alias);
  }
  it->second.push_back(std::move(named_subquery));
}

absl::StatusOr<std::vector<std::unique_ptr<const ResolvedWithEntry>>>
Resolver::ResolveWithClauseIfPresent(const ASTQuery* query,
                                     bool is_outer_query) {
  std::vector<std::unique_ptr<const ResolvedWithEntry>> with_entries;
  if (query->with_clause() != nullptr) {
    if (!is_outer_query &&
        !language().LanguageFeatureEnabled(FEATURE_V_1_1_WITH_ON_SUBQUERY)) {
      return MakeSqlErrorAt(query->with_clause())
             << "WITH is not supported on subqueries in this language version";
    }

    // Check for duplicate WITH aliases
    IdStringHashSetCase alias_names;
    for (const ASTWithClauseEntry* with_entry : query->with_clause()->with()) {
      if (!zetasql_base::InsertIfNotPresent(&alias_names,
                                   with_entry->alias()->GetAsIdString())) {
        return MakeSqlErrorAt(with_entry->alias())
               << "Duplicate alias " << with_entry->alias()->GetAsString()
               << " for WITH subquery";
      }
    }

    if (query->with_clause()->recursive()) {
      if (!language().LanguageFeatureEnabled(FEATURE_V_1_3_WITH_RECURSIVE)) {
        return MakeSqlErrorAt(query->with_clause())
               << "RECURSIVE is not supported in the WITH clause";
      }

      ZETASQL_ASSIGN_OR_RETURN(WithEntrySortResult sort_result,
                       SortWithEntries(query->with_clause()));

      for (const ASTWithClauseEntry* with_entry : sort_result.sorted_entries) {
        ZETASQL_ASSIGN_OR_RETURN(
            std::unique_ptr<const ResolvedWithEntry> resolved_with_entry,
            ResolveWithEntry(
                with_entry,
                sort_result.self_recursive_entries.contains(with_entry)));
        with_entries.push_back(std::move(resolved_with_entry));
      }
    } else {
      // Non-recursive WITH
      for (const ASTWithClauseEntry* with_entry :
           query->with_clause()->with()) {
        ZETASQL_ASSIGN_OR_RETURN(
            std::unique_ptr<const ResolvedWithEntry> resolved_with_entry,
            ResolveWithEntry(with_entry, /*recursive=*/false));
        with_entries.push_back(std::move(resolved_with_entry));
      }
    }
  }
  return with_entries;
}

absl::Status Resolver::FinishResolveWithClauseIfPresent(
    const ASTQuery* query,
    std::vector<std::unique_ptr<const ResolvedWithEntry>> with_entries,
    std::unique_ptr<const ResolvedScan>* output) {
  if (query->with_clause() == nullptr) {
    return absl::OkStatus();
  }
  // Now remove any WITH entry mappings we added, restoring what was visible
  // outside this WITH clause.
  for (const ASTWithClauseEntry* with_entry : query->with_clause()->with()) {
    const IdString with_alias = with_entry->alias()->GetAsIdString();
    auto it = named_subquery_map_.find({with_alias});
    ZETASQL_RET_CHECK(it != named_subquery_map_.end());
    it->second.pop_back();
    if (it->second.empty()) {
      named_subquery_map_.erase(it);
    }
  }

  // Wrap a ResolvedWithScan around the output query.
  const auto& tmp_column_list = (*output)->column_list();
  *output = MakeResolvedWithScan(tmp_column_list, std::move(with_entries),
                                 std::move(*output),
                                 query->with_clause()->recursive());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveQuery(
    const ASTQuery* query, const NameScope* scope, IdString query_alias,
    bool is_outer_query, std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {

  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::unique_ptr<const ResolvedWithEntry>> with_entries,
      ResolveWithClauseIfPresent(query, is_outer_query));
  ZETASQL_RETURN_IF_ERROR(ResolveQueryAfterWith(query, scope, query_alias, output,
                                        output_name_list));

  // Add coercions to the final column output types if needed.
  if (is_outer_query && !analyzer_options().get_target_column_types().empty()) {
    ZETASQL_RETURN_IF_ERROR(CoerceQueryStatementResultToTypes(
        query, analyzer_options().get_target_column_types(), output,
        output_name_list));
  }

  ZETASQL_RETURN_IF_ERROR(
      FinishResolveWithClauseIfPresent(query, std::move(with_entries), output));

  // Add parse location to the outermost ResolvedScan only. This is intended
  // because the outermost ResolvedScan represents a query_expr (query or
  // subquery) in the syntax
  // in (broken link)#sql-syntax.
  // This is safe for existing engines because parse locations in resolved AST
  // are mainly for debugging and indexing purposes. Existing engines don't
  // need parse locations during query execution.
  MaybeRecordParseLocation(query, const_cast<ResolvedScan*>(output->get()));

  return absl::OkStatus();
}

static absl::Status VerifyNoLimitOrOrderByInRecursiveQuery(
    const ASTQuery* query) {
  if (query->order_by() != nullptr) {
    return MakeSqlErrorAt(query->order_by())
           << "A recursive query may not use ORDER BY";
  }
  if (query->limit_offset() != nullptr) {
    return MakeSqlErrorAt(query->limit_offset())
           << "A recursive query may not use LIMIT";
  }
  return absl::OkStatus();
}

absl::StatusOr<const ASTSetOperation*> Resolver::GetRecursiveUnion(
    const ASTQuery* query) {
  // Skip redundant parentheses around the UNION
  while (query->query_expr()->node_kind() == AST_QUERY) {
    ZETASQL_RETURN_IF_ERROR(VerifyNoLimitOrOrderByInRecursiveQuery(query));
    query = query->query_expr()->GetAsOrDie<ASTQuery>();
  }
  ZETASQL_RETURN_IF_ERROR(VerifyNoLimitOrOrderByInRecursiveQuery(query));

  const ASTSetOperation* query_set_op =
      query->query_expr()->GetAsOrNull<ASTSetOperation>();
  if (query_set_op == nullptr ||
      query_set_op->op_type() != ASTSetOperation::UNION) {
    return MakeSqlErrorAt(query)
           << "Recursive query does not have the form <non-recursive-term> "
           << "UNION [ALL|DISTINCT] <recursive-term>";
  }
  return query_set_op;
}

absl::StatusOr<std::unique_ptr<const ResolvedWithEntry>>
Resolver::ResolveWithEntry(const ASTWithClauseEntry* with_entry,
                           bool recursive) {
  const IdString with_alias = with_entry->alias()->GetAsIdString();

  // Generate a unique alias for this WITH subquery, if necessary.
  IdString unique_alias = with_alias;
  while (!zetasql_base::InsertIfNotPresent(&unique_with_alias_names_, unique_alias)) {
    unique_alias = MakeIdString(absl::StrCat(unique_alias.ToStringView(), "_",
                                             unique_with_alias_names_.size()));
  }

  std::unique_ptr<const ResolvedScan> resolved_subquery;
  std::shared_ptr<const NameList> subquery_name_list;
  if (recursive) {
    // WITH entry is actually recursive (not just defined using the RECURSIVE
    // keyword).
    ZETASQL_ASSIGN_OR_RETURN(std::vector<std::unique_ptr<const ResolvedWithEntry>>
                         inner_with_entries,
                     ResolveWithClauseIfPresent(with_entry->query(),
                                                /*is_outer_query=*/false));
    ZETASQL_ASSIGN_OR_RETURN(const ASTSetOperation* recursive_union,
                     GetRecursiveUnion(with_entry->query()));
    SetOperationResolver setop_resolver(recursive_union, this);
    ZETASQL_RETURN_IF_ERROR(setop_resolver.ResolveRecursive(
        empty_name_scope_.get(), {with_alias}, unique_alias, &resolved_subquery,
        &subquery_name_list));
    ZETASQL_RETURN_IF_ERROR(FinishResolveWithClauseIfPresent(
        with_entry->query(), std::move(inner_with_entries),
        &resolved_subquery));
  } else {
    // We always pass empty_name_scope_ when resolving the subquery inside
    // WITH.  Those queries must stand alone and cannot reference any
    // correlated columns or other names defined outside.
    ZETASQL_RETURN_IF_ERROR(ResolveQuery(with_entry->query(), empty_name_scope_.get(),
                                 with_alias, false /* is_outer_query */,
                                 &resolved_subquery, &subquery_name_list));
    AddNamedSubquery({with_alias},
                     absl::make_unique<NamedSubquery>(
                         unique_alias, /*recursive_in=*/false,
                         resolved_subquery->column_list(), subquery_name_list));
  }
  std::unique_ptr<const ResolvedWithEntry> resolved_with_entry =
      MakeResolvedWithEntry(unique_alias.ToString(),
                            std::move(resolved_subquery));
  return resolved_with_entry;
}

void Resolver::MaybeAddProjectForComputedColumns(
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> computed_columns,
    std::unique_ptr<const ResolvedScan>* current_scan) {
  if (!computed_columns.empty()) {
    ResolvedColumnList wrapper_column_list = (*current_scan)->column_list();
    for (const auto& computed_column : computed_columns) {
      wrapper_column_list.push_back(computed_column->column());
    }
    *current_scan = MakeResolvedProjectScan(wrapper_column_list,
                                            std::move(computed_columns),
                                            std::move(*current_scan));
  }
}

absl::Status Resolver::AddAggregateScan(
    const ASTSelect* select, bool is_for_select_distinct,
    QueryResolutionInfo* query_resolution_info,
    std::unique_ptr<const ResolvedScan>* current_scan) {
  ResolvedColumnList column_list;
  for (const std::unique_ptr<const ResolvedComputedColumn>& group_by_column :
       query_resolution_info->group_by_columns_to_compute()) {
    column_list.push_back(group_by_column->column());
  }
  for (const std::unique_ptr<const ResolvedComputedColumn>& aggregate_column :
       query_resolution_info->aggregate_columns_to_compute()) {
    column_list.push_back(aggregate_column->column());
  }

  std::vector<std::unique_ptr<const ResolvedColumnRef>> rollup_column_list;
  std::vector<std::unique_ptr<const ResolvedGroupingSet>> grouping_set_list;

  // Retrieve the grouping sets and rollup list for the aggregate scan, if any.
  query_resolution_info->ReleaseGroupingSetsAndRollupList(&grouping_set_list,
                                                          &rollup_column_list);

  ZETASQL_RET_CHECK(!column_list.empty());
  std::unique_ptr<ResolvedAggregateScan> aggregate_scan =
      MakeResolvedAggregateScan(
          column_list, std::move(*current_scan),
          query_resolution_info->release_group_by_columns_to_compute(),
          query_resolution_info->release_aggregate_columns_to_compute(),
          std::move(grouping_set_list), std::move(rollup_column_list));
  // If the feature is not enabled, any collation annotation that might exist on
  // the grouping expressions is ignored.
  if (language().LanguageFeatureEnabled(FEATURE_V_1_3_COLLATION_SUPPORT)) {
    std::vector<ResolvedCollation> collation_list;
    bool empty = true;
    for (const auto& group_by_expr : aggregate_scan->group_by_list()) {
      ResolvedCollation resolved_collation;
      if (group_by_expr->expr()->type_annotation_map() != nullptr) {
        ZETASQL_ASSIGN_OR_RETURN(resolved_collation,
                         ResolvedCollation::MakeResolvedCollation(
                             *group_by_expr->expr()->type_annotation_map()));
        empty &= resolved_collation.Empty();
      }
      collation_list.push_back(std::move(resolved_collation));
    }
    if (!empty) {
      aggregate_scan->set_collation_list(collation_list);
    }
  }
  // We might have aggregation without GROUP BY.
  if (!is_for_select_distinct && select->group_by() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ResolveHintsForNode(select->group_by()->hint(), aggregate_scan.get()));
  }

  *current_scan = std::move(aggregate_scan);
  return absl::OkStatus();
}

absl::Status Resolver::AddAnonymizedAggregateScan(
    const ASTSelect* select, QueryResolutionInfo* query_resolution_info,
    std::unique_ptr<const ResolvedScan>* current_scan) {
  ResolvedColumnList column_list;
  for (const std::unique_ptr<const ResolvedComputedColumn>& group_by_column :
       query_resolution_info->group_by_columns_to_compute()) {
    column_list.push_back(group_by_column->column());
  }
  for (const std::unique_ptr<const ResolvedComputedColumn>& aggregate_column :
       query_resolution_info->aggregate_columns_to_compute()) {
    column_list.push_back(aggregate_column->column());
  }

  ZETASQL_RET_CHECK(!column_list.empty());
  std::vector<std::unique_ptr<const ResolvedOption>>
      resolved_anonymization_options;
  if (select->anonymization_options() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveAnonymizationOptionsList(
        select->anonymization_options(), &resolved_anonymization_options));
  }

  auto anonymized_scan = MakeResolvedAnonymizedAggregateScan(
      column_list, std::move(*current_scan),
      query_resolution_info->release_group_by_columns_to_compute(),
      query_resolution_info->release_aggregate_columns_to_compute(),
      /*k_threshold_expr=*/nullptr, std::move(resolved_anonymization_options));

  analyzer_output_properties_.MarkRelevant(REWRITE_ANONYMIZATION);

  *current_scan = std::move(anonymized_scan);
  return absl::OkStatus();
}

absl::Status Resolver::AddAnalyticScan(
    const NameScope* having_and_order_by_name_scope,
    QueryResolutionInfo* query_resolution_info,
    std::unique_ptr<const ResolvedScan>* current_scan) {
  // An analytic function in the ORDER BY can reference select list aliases,
  // so if there are any such SELECT list columns that need precomputing,
  // project them now.  We have to be careful though.  We can only project
  // SELECT list precomputed columns if they do not themselves include
  // analytic functions.  If they do, they must be computed after the
  // AnalyticScan.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      select_columns_without_analytic;
  ZETASQL_RETURN_IF_ERROR(
      query_resolution_info->GetAndRemoveSelectListColumnsWithoutAnalytic(
          &select_columns_without_analytic));

  // TODO: Consider using MaybeAddProject...() here, if we don't
  // care about sorting the column list.
  if (!select_columns_without_analytic.empty()) {
    const std::vector<ResolvedColumn>& column_list =
        (*current_scan)->column_list();
    ResolvedColumnList concat_columns =
        ConcatColumnListWithComputedColumnsAndSort(
            column_list, select_columns_without_analytic);

    *current_scan = MakeResolvedProjectScan(
        concat_columns, std::move(select_columns_without_analytic),
        std::move(*current_scan));
    // Avoid deletion after transfer.
    select_columns_without_analytic.clear();
  }
  return query_resolution_info->analytic_resolver()->CreateAnalyticScan(
      query_resolution_info, current_scan);
}

// Given an input pre-GROUP BY NameScope and a ValidFieldInfo vector,
// returns a post-GROUP BY NameScope.  The ValidFieldInfo vector
// represents the mapping between pre-GROUP BY to post-GROUP BY columns
// and fields.  The returned NameScope includes the same previous_scope_
// as the pre-GROUP BY NameScope, while the returned NameScope's local
// NameList is created by merging the pre-GROUP BY local NameList with
// the ValidFieldInfo vector.  The resulting NameScope's local NameList
// includes all the same names as the old one, but columns/fields that are
// not available to access after GROUP BY are marked as invalid to access.
// The names/fields that are valid to access map to post-GROUP BY versions
// of those columns.
//
// WARNING: When we call this function we *MUST* only use a NameScope whose
// previous_scope_ is for an outer/correlation NameScope.  Outer/correlation
// NameScope names should and will remain accessible after GROUP BY or
// DISTINCT, but local names that are not grouped by become invalid.  If
// the 'pre_group_by_scope' used to create the post-GROUP BY Namescope
// has a previous_scope_ that is not an outer/correlation NameScope (for
// example a layered NameScope that adds new names that resolve over the
// FROM clause names) then any names in that previous_scope_ will
// incorrectly remain valid to access after creating the new post-GROUP BY
// NameScope.  This is because CreateNameScopeGivenValidNamePaths() creates
// a new NameScope from 'pre_group_by_scope' by updating its local names()
// and value_table_columns(), but names from previous scopes remain
// unchanged and valid to access in the returned NameScope.
static absl::Status CreatePostGroupByNameScope(
    const NameScope* pre_group_by_scope,
    QueryResolutionInfo* query_resolution_info,
    std::unique_ptr<const NameScope>* post_group_by_scope_out) {
  std::unique_ptr<NameScope> post_group_by_scope;
  ZETASQL_RETURN_IF_ERROR(pre_group_by_scope->CreateNameScopeGivenValidNamePaths(
      query_resolution_info->group_by_valid_field_info_map(),
      &post_group_by_scope));

  *post_group_by_scope_out = std::move(post_group_by_scope);
  return absl::OkStatus();
}

absl::Status Resolver::AddRemainingScansForSelect(
    const ASTSelect* select, const ASTOrderBy* order_by,
    const ASTLimitOffset* limit_offset,
    const NameScope* having_and_order_by_scope,
    std::unique_ptr<const ResolvedExpr>* resolved_having_expr,
    std::unique_ptr<const ResolvedExpr>* resolved_qualify_expr,
    QueryResolutionInfo* query_resolution_info,
    std::shared_ptr<const NameList>* output_name_list,
    std::unique_ptr<const ResolvedScan>* current_scan) {
  SelectColumnStateList* select_column_state_list =
      query_resolution_info->select_column_state_list();

  // Precompute any other columns necessary before aggregation.
  MaybeAddProjectForComputedColumns(
      query_resolution_info
          ->release_select_list_columns_to_compute_before_aggregation(),
      current_scan);

  if (query_resolution_info->HasGroupByOrAggregation()) {
    if (select->anonymization_options() != nullptr ||
        query_resolution_info->has_anonymized_aggregation()) {
      if (query_resolution_info->HasGroupByRollup()) {
        ZETASQL_RET_CHECK_EQ(select->group_by()->grouping_items().size(), 1);
        return MakeSqlErrorAt(select->group_by()->grouping_items(0)->rollup())
               << "GROUP BY ROLLUP is not supported in anonymization queries";
      }
      ZETASQL_RETURN_IF_ERROR(AddAnonymizedAggregateScan(select, query_resolution_info,
                                                 current_scan));
    } else {
      // We know all the GROUP BY and aggregate columns, so can now create an
      // AggregateScan.
      ZETASQL_RETURN_IF_ERROR(AddAggregateScan(select,
                                       false /* is_for_select_distinct */,
                                       query_resolution_info, current_scan));
    }
  }

  // Precompute any other columns necessary after aggregation.
  MaybeAddProjectForComputedColumns(
      query_resolution_info->release_columns_to_compute_after_aggregation(),
      current_scan);

  if (*resolved_having_expr != nullptr) {
    // The HAVING might reference select list aliases, so if there are
    // any such SELECT list columns that need precomputing, precompute
    // those without an analytic function.
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        select_columns_without_analytic;
    ZETASQL_RETURN_IF_ERROR(
        query_resolution_info->GetAndRemoveSelectListColumnsWithoutAnalytic(
            &select_columns_without_analytic));

    if (!select_columns_without_analytic.empty()) {
      const std::vector<ResolvedColumn>& column_list =
          (*current_scan)->column_list();
      ResolvedColumnList concat_columns =
          ConcatColumnListWithComputedColumnsAndSort(
              column_list, select_columns_without_analytic);

      *current_scan = MakeResolvedProjectScan(
          concat_columns, std::move(select_columns_without_analytic),
          std::move(*current_scan));
    }

    const auto& tmp_column_list = (*current_scan)->column_list();
    *current_scan =
        MakeResolvedFilterScan(tmp_column_list, std::move(*current_scan),
                               std::move(*resolved_having_expr));
  }

  // TODO: There might be some test cases here that are broken,
  // we should not need to use a NameScope at this point.  It is currently
  // being used for resolving the AnalyticFunctionGroup, which ideally should
  // already have had its expressions resolved above.  Clean this up and
  // add more tests if necessary.
  if (query_resolution_info->HasAnalytic()) {
    ZETASQL_RETURN_IF_ERROR(AddAnalyticScan(having_and_order_by_scope,
                                    query_resolution_info, current_scan));
  }

  // Precompute any other columns necessary after analytic functions.
  MaybeAddProjectForComputedColumns(
      query_resolution_info->release_columns_to_compute_after_analytic(),
      current_scan);

  // Make additional filter scan if QUALIFY is present.
  if (*resolved_qualify_expr != nullptr) {
    // The QUALIFY clause might reference select list aliases, so if there are
    // any such SELECT list columns that need precomputing, precompute them.
    MaybeAddProjectForComputedColumns(
      query_resolution_info->release_select_list_columns_to_compute(),
      current_scan);

    const auto& tmp_column_list = (*current_scan)->column_list();
    *current_scan =
        MakeResolvedFilterScan(tmp_column_list, std::move(*current_scan),
                               std::move(*resolved_qualify_expr));
  }

  if (select->distinct()) {
    // If there are (aliased or non-aliased) select list columns to compute
    // then add a project first.
    MaybeAddProjectForComputedColumns(
        query_resolution_info->release_select_list_columns_to_compute(),
        current_scan);

    // Note: The DISTINCT processing is very similar to the GROUP BY
    // processing.  The output of GROUP BY is used for resolving subsequent
    // clauses and expressions (e.g., the SELECT list), and the output of
    // DISTINCT is used for resolving the subsequent ORDER BY expressions.
    //
    // These steps include:
    // 1) Creating a new DISTINCT NameScope that includes names from the
    //    SELECT list that are available after DISTINCT.  Other names
    //    become invalid targets in the new DISTINCT NameScope.
    // 2) Then below, the ORDER BY expressions will be resolved against
    //    the new DISTINCT NameScope, returning an error if an invalid
    //    column/name is accessed.
    //
    // For handling DISTINCT, we use all the same machinery as GROUP BY,
    // but mark the QueryResolutionInfo so we know subsequent resolution
    // is for post-DISTINCT processing.
    ZETASQL_RETURN_IF_ERROR(ResolveSelectDistinct(
        select, select_column_state_list, output_name_list->get(), current_scan,
        query_resolution_info, output_name_list));
  }

  if (order_by != nullptr) {
    if (select->distinct()) {
      // Check expected state.  If DISTINCT is present, then we already
      // computed any necessary SELECT list columns before processing the
      // DISTINCT.
      ZETASQL_RET_CHECK(
          query_resolution_info->select_list_columns_to_compute()->empty());

      // If DISTINCT is present, then the ORDER BY expressions have *not*
      // been resolved yet.  Resolve the ORDER BY expressions to reference
      // the post-DISTINCT versions of columns.  Note that the DISTINCT
      // processing already updated <query_resolution_info> with the
      // mapping from pre-DISTINCT to post-DISTINCT versions of columns
      // and expressions, so we simply need to resolve the ORDER BY
      // expressions with the updated <query_resolution_info> and
      // post-distinct NameScope.  Resolution of ORDER BY expressions
      // against the output of DISTINCT has the same characteristics
      // as post-GROUP BY expression resolution.  ORDER BY expressions
      // resolve successfully to columns and path expressions that were
      // output from DISTINCT.  As such, any column reference that is
      // not in the SELECT list is an error.

      // Create a new NameScope for what comes out of the DISTINCT
      // AggregateScan.  It is derived from the <having_and_order_by_scope>,
      // and allows column references to resolve to the post-DISTINCT versions
      // of the columns.
      std::unique_ptr<const NameScope> distinct_scope;
      ZETASQL_RETURN_IF_ERROR(CreatePostGroupByNameScope(
          having_and_order_by_scope, query_resolution_info, &distinct_scope));

      // The second 'distinct_scope' NameScope argument is only
      // used for resolving the arguments to aggregate functions, but when
      // DISTINCT is present then aggregate functions are not allowed in
      // ORDER BY so we will always get an error regardless of whether or
      // not the name is visible post-DISTINCT.
      ZETASQL_RETURN_IF_ERROR(ResolveOrderByExprs(
          order_by, distinct_scope.get(), distinct_scope.get(),
          true /* is_post_distinct */, query_resolution_info));
    } else {
      // DISTINCT is *not* present so we have already resolved the ORDER BY
      // expressions in ResolveSelect() and do not resolve them here.
      // Also, the ORDER BY might have computed columns to compute so
      // add a wrapper project to compute them if necessary.
      // TODO: In many cases we can combine the two projects into
      // one, so fix this where possible.  Two projects are required when
      // an ORDER BY expression references a SELECT list alias, such as:
      //   SELECT a+1 as foo FROM T ORDER BY foo + 1;
      // In this case, we need a ProjectScan to compute foo, then another
      // ProjectScan to compute foo + 1.  Simply combining the SELECT
      // list columns to compute with the
      // query_resolution_info->order_by_columns_to_compute() does not work.
      MaybeAddProjectForComputedColumns(
          query_resolution_info->release_select_list_columns_to_compute(),
          current_scan);
    }

    // <query_resolution_info>->order_by_columns_to_compute() already
    // contains the ORDER BY expressions that need computing, but we
    // also need to compute SELECT list computed columns before ordering,
    // so add them to the list.
    for (std::unique_ptr<const ResolvedComputedColumn>& select_computed_column :
         query_resolution_info->release_select_list_columns_to_compute()) {
      query_resolution_info->order_by_columns_to_compute()->push_back(
          std::move(select_computed_column));
    }

    MaybeAddProjectForComputedColumns(
        query_resolution_info->release_order_by_columns_to_compute(),
        current_scan);

    ZETASQL_RETURN_IF_ERROR(MakeResolvedOrderByScan(
        order_by, current_scan,
        select_column_state_list->resolved_column_list(),
        query_resolution_info->order_by_item_info(), current_scan));
  }

  if (order_by == nullptr && !select->distinct()) {
    // TODO: For now, we always add the project if no ORDER BY and
    // no DISTINCT, including both aliased and non-aliased select list
    // columns to precompute.  This is primarily to minimize diffs
    // with the original plans.  We can probably do better about avoiding
    // unnecessary PROJECT nodes and delay projecting the non-aliased
    // expressions once this refactoring is submitted.
    *current_scan = MakeResolvedProjectScan(
        select_column_state_list->resolved_column_list(),
        query_resolution_info->release_select_list_columns_to_compute(),
        std::move(*current_scan));
  }

  if (limit_offset != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveLimitOffsetScan(
        limit_offset, std::move(*current_scan), current_scan));
  }

  // Check here, because if there is SELECT AS STRUCT or SELECT AS PROTO
  // then the column counts will no longer match.
  ZETASQL_RET_CHECK_EQ(select_column_state_list->Size(),
               (*output_name_list)->num_columns());

  if (select->select_as() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ResolveSelectAs(select->select_as(), *select_column_state_list,
                        std::move(*current_scan), output_name_list->get(),
                        current_scan, output_name_list));
  }

  // Resolve the hint last since we want to attach it as the outermost Scan.
  if (select->hint() != nullptr) {
    // TODO Currently we always add a new ProjectScan to store the
    // hint.  We construct it mutable so we can build the hint_list.

    // To avoid undefined behavior, don't release 'current_scan' and use it in
    // the same function call.
    const std::vector<ResolvedColumn>& column_list =
        (*current_scan)->column_list();
    auto hinted_scan = MakeResolvedProjectScan(column_list, {} /* expr_list */,
                                               std::move(*current_scan));

    ZETASQL_RETURN_IF_ERROR(ResolveHintsForNode(select->hint(), hinted_scan.get()));

    *current_scan = std::move(hinted_scan);
  }
  return absl::OkStatus();
}

void Resolver::AddColumnsForOrderByExprs(
    IdString query_alias, std::vector<OrderByItemInfo>* order_by_info,
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
        computed_columns) {
  for (int order_by_item_idx = 0; order_by_item_idx < order_by_info->size();
       ++order_by_item_idx) {
    OrderByItemInfo& item_info = (*order_by_info)[order_by_item_idx];
    if (!item_info.is_select_list_index()) {
      if (item_info.order_expression->node_kind() == RESOLVED_COLUMN_REF &&
          !item_info.order_expression->GetAs<ResolvedColumnRef>()
               ->is_correlated()) {
        item_info.order_column =
            item_info.order_expression->GetAs<ResolvedColumnRef>()->column();
      } else {
        bool already_computed = false;
        for (const std::unique_ptr<const ResolvedComputedColumn>&
                 computed_column : *computed_columns) {
          if (IsSameFieldPath(item_info.order_expression.get(),
                              computed_column->expr(),
                              FieldPathMatchingOption::kExpression)) {
            item_info.order_column = computed_column->column();
            item_info.order_expression.reset();  // not needed any more
            already_computed = true;
            break;
          }
        }
        if (already_computed) {
          continue;
        }
        const IdString order_column_alias =
            MakeIdString(absl::StrCat("$orderbycol", order_by_item_idx + 1));
        ResolvedColumn resolved_column(
            AllocateColumnId(), query_alias, order_column_alias,
            item_info.order_expression->annotated_type());
        item_info.order_column = resolved_column;
        computed_columns->emplace_back(MakeResolvedComputedColumn(
            item_info.order_column, std::move(item_info.order_expression)));
      }
    }
  }
}

// Note this is currently only used after set operations, and resolves against
// the set operation NameScope (which derives from the SELECT list aliases
// of its first subquery).  Resolution of ORDER BY for SELECT is done
// elsewhere since it gets resolved in two phases.
absl::Status Resolver::ResolveOrderByAfterSetOperations(
    const ASTOrderBy* order_by, const NameScope* scope,
    std::unique_ptr<const ResolvedScan> input_scan,
    std::unique_ptr<const ResolvedScan>* output_scan) {
  // We use a new QueryResolutionInfo because resolving the ORDER BY
  // outside of set operations is independent from its input subquery
  // resolution.
  std::unique_ptr<QueryResolutionInfo> query_resolution_info(
      new QueryResolutionInfo(this));
  static const char clause_name[] = "ORDER BY clause after set operation";

  query_resolution_info->analytic_resolver()->DisableNamedWindowRefs(
      clause_name);

  ExprResolutionInfo expr_resolution_info(
      scope, scope, false /* allows_aggregation */, true /* allows_analytic */,
      false /* use_post_grouping_columns (not relevant in this path) */,
      clause_name, query_resolution_info.get());
  ZETASQL_RETURN_IF_ERROR(ResolveOrderingExprs(
      order_by->ordering_expressions(), &expr_resolution_info,
      expr_resolution_info.query_resolution_info
          ->mutable_order_by_item_info()));

  // If the ORDER BY clause after set operations includes analytic functions,
  // then we need to create an analytic scan for them before we do ordering.
  // For example:
  //
  // SELECT a, b, c FROM t1
  // UNION ALL
  // SELECT a, b, c FROM t2
  // ORDER BY sum(a) OVER (PARTITION BY b ORDER BY c);
  //
  // The ORDER BY binds outside the UNION ALL, so the UNION ALL feeds
  // an AnalyticScan, which in turn feeds the OrderByScan.
  if (query_resolution_info->HasAnalytic()) {
    ZETASQL_RETURN_IF_ERROR(
        query_resolution_info->analytic_resolver()->CreateAnalyticScan(
            query_resolution_info.get(), &input_scan));
  }

  std::vector<std::unique_ptr<const ResolvedComputedColumn>> computed_columns;
  AddColumnsForOrderByExprs(kOrderById /* query_alias */,
                            query_resolution_info->mutable_order_by_item_info(),
                            &computed_columns);

  // The output columns of the ORDER BY are the same as the output of the
  // original input.
  const ResolvedColumnList output_columns = input_scan->column_list();

  // If the ORDER BY requires computed columns, add a wrapper project to
  // compute them.
  MaybeAddProjectForComputedColumns(std::move(computed_columns), &input_scan);

  return MakeResolvedOrderByScan(order_by, &input_scan, output_columns,
                                 query_resolution_info->order_by_item_info(),
                                 output_scan);
}

absl::Status Resolver::ResolveOrderByItems(
    const ASTOrderBy* order_by,
    const std::vector<ResolvedColumn>& output_column_list,
    const std::vector<OrderByItemInfo>& order_by_info,
    std::vector<std::unique_ptr<const ResolvedOrderByItem>>*
        resolved_order_by_items) {
  resolved_order_by_items->clear();
  ZETASQL_RET_CHECK_EQ(order_by_info.size(), order_by->ordering_expressions().size());

  for (int i = 0; i < order_by_info.size(); ++i) {
    const OrderByItemInfo& item_info = order_by_info[i];

    std::unique_ptr<const ResolvedColumnRef> resolved_column_ref;
    if (item_info.is_select_list_index()) {
      if (item_info.select_list_index < 0 ||
          item_info.select_list_index >= output_column_list.size()) {
        return MakeSqlErrorAt(item_info.ast_location)
               << "ORDER BY is out of SELECT column number range: "
               << item_info.select_list_index + 1;
      }
      // NOTE: Accessing scan column list works now as we don't deduplicate
      // anything from the column list.  Thus it matches 1:1 with the select
      // list.  If that changes, we should use name list instead.
      // Convert the select list ordinal reference to a column reference.
      resolved_column_ref =
          MakeColumnRef(output_column_list[item_info.select_list_index]);
    } else {
      resolved_column_ref = MakeColumnRef(item_info.order_column);
    }

    std::unique_ptr<const ResolvedExpr> resolved_collation_name;
    const ASTCollate* ast_collate =
        order_by->ordering_expressions().at(i)->collate();
    if (ast_collate != nullptr) {
      ZETASQL_RETURN_IF_ERROR(ValidateAndResolveOrderByCollate(
          ast_collate, order_by->ordering_expressions().at(i),
          resolved_column_ref->column().type(), &resolved_collation_name));
    }

    auto resolved_order_by_item = MakeResolvedOrderByItem(
        std::move(resolved_column_ref), std::move(resolved_collation_name),
        item_info.is_descending, item_info.null_order);
    if (language().LanguageFeatureEnabled(FEATURE_V_1_3_COLLATION_SUPPORT)) {
      ZETASQL_RETURN_IF_ERROR(
          CollationAnnotation::ResolveCollationForResolvedOrderByItem(
              resolved_order_by_item.get()));
    }
    resolved_order_by_items->push_back(std::move(resolved_order_by_item));

    if (!resolved_order_by_items->back()
             ->column_ref()
             ->type()
             ->SupportsOrdering(language(), /*type_description=*/nullptr)) {
      return MakeSqlErrorAt(item_info.ast_location)
             << "ORDER BY does not support expressions of type "
             << resolved_order_by_items->back()
                    ->column_ref()
                    ->type()
                    ->ShortTypeName(product_mode());
    }
  }

  return absl::OkStatus();
}

absl::Status Resolver::MakeResolvedOrderByScan(
    const ASTOrderBy* order_by, std::unique_ptr<const ResolvedScan>* input_scan,
    const std::vector<ResolvedColumn>& output_column_list,
    const std::vector<OrderByItemInfo>& order_by_info,
    std::unique_ptr<const ResolvedScan>* output_scan) {
  std::vector<std::unique_ptr<const ResolvedOrderByItem>>
      resolved_order_by_items;

  ZETASQL_RETURN_IF_ERROR(ResolveOrderByItems(order_by, output_column_list,
                                      order_by_info, &resolved_order_by_items));

  std::unique_ptr<ResolvedOrderByScan> order_by_scan =
      zetasql::MakeResolvedOrderByScan(output_column_list,
                                         std::move(*input_scan),
                                         std::move(resolved_order_by_items));
  order_by_scan->set_is_ordered(true);

  ZETASQL_RETURN_IF_ERROR(ResolveHintsForNode(order_by->hint(), order_by_scan.get()));
  *output_scan = std::move(order_by_scan);

  return absl::OkStatus();
}

absl::Status Resolver::ResolveQueryExpression(
    const ASTQueryExpression* query_expr, const NameScope* scope,
    IdString query_alias, bool force_new_columns_for_projected_outputs,
    std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  switch (query_expr->node_kind()) {
    case AST_SELECT:
      return ResolveSelect(
          query_expr->GetAsOrDie<ASTSelect>(), nullptr /* order_by */,
          nullptr /* limit_offset */, scope, query_alias,
          force_new_columns_for_projected_outputs, output, output_name_list);

    case AST_SET_OPERATION:
      return ResolveSetOperation(query_expr->GetAsOrDie<ASTSetOperation>(),
                                 scope, output, output_name_list);

    case AST_QUERY:
      return ResolveQuery(query_expr->GetAsOrDie<ASTQuery>(), scope,
                          query_alias, false /* is_outer_query */, output,
                          output_name_list);

    default:
      break;
  }

  return MakeSqlErrorAt(query_expr) << "Unhandled query_expr:\n"
                                    << query_expr->DebugString();
}

absl::Status Resolver::ResolveAdditionalExprsSecondPass(
    const NameScope* from_clause_or_group_by_scope,
    QueryResolutionInfo* query_resolution_info) {
  for (const auto& entry :
       *query_resolution_info
            ->dot_star_columns_with_aggregation_for_second_pass_resolution()) {
    // Re-resolve the source expression for the dot-star expressions that
    // contain aggregation.
    ExprResolutionInfo expr_resolution_info(
        from_clause_or_group_by_scope, from_clause_or_group_by_scope,
        true /* allows_aggregation */, true /* allows_analytic */,
        query_resolution_info->HasGroupByOrAggregation(), "SELECT list",
        query_resolution_info);
    std::unique_ptr<const ResolvedExpr> resolved_expr;
    ZETASQL_RETURN_IF_ERROR(
        ResolveExpr(entry.second, &expr_resolution_info, &resolved_expr));
    query_resolution_info->columns_to_compute_after_aggregation()->emplace_back(
        MakeResolvedComputedColumn(entry.first, std::move(resolved_expr)));
  }
  for (const auto& entry :
       *query_resolution_info
            ->dot_star_columns_with_analytic_for_second_pass_resolution()) {
    // Re-resolve the source expression for the dot-star expressions.
    ExprResolutionInfo expr_resolution_info(
        from_clause_or_group_by_scope, from_clause_or_group_by_scope,
        true /* allows_aggregation */, true /* allows_analytic */,
        query_resolution_info->HasGroupByOrAggregation(), "SELECT list",
        query_resolution_info);
    std::unique_ptr<const ResolvedExpr> resolved_expr;
    ZETASQL_RETURN_IF_ERROR(
        ResolveExpr(entry.second, &expr_resolution_info, &resolved_expr));
    query_resolution_info->columns_to_compute_after_analytic()->emplace_back(
        MakeResolvedComputedColumn(entry.first, std::move(resolved_expr)));
  }
  return absl::OkStatus();
}

static absl::Status ValidateAnonymizationSetup(const LanguageOptions& language,
                                               const ASTSelect* select) {
  if (!language.LanguageFeatureEnabled(FEATURE_ANONYMIZATION)) {
    if (select->anonymization_options() != nullptr) {
      return MakeSqlErrorAt(select)
             << "Anonymization queries are not supported";
    }
  } else {
    if (select->anonymization_options() != nullptr &&
        select->from_clause() == nullptr) {
      return MakeSqlErrorAt(select) << "SELECT without FROM clause cannot "
                                       "specify WITH ANONYMIZATION";
    } else if (select->anonymization_options() != nullptr &&
               select->distinct()) {
      return MakeSqlErrorAt(select)
             << "SELECT WITH ANONYMIZATION does not support DISTINCT";
    }
  }
  return absl::OkStatus();
}

// Resolves a SELECT query/subquery, resolving all the expressions and
// generating scans necessary to produce its output (as defined by the
// SELECT list).  The logic is generally as follows:
//
// 1) Resolve the FROM clause, generate a scan for it, and build
//    a NameScope for what comes out of it.
//
// 2) If present, resolve the WHERE clause against the FROM NameScope,
//    and generate a scan for it on top of the FROM scan.
//
// 3) Do resolution of the remaining clauses (the SELECT list,
//    GROUP BY, HAVING, QUALIFY and ORDER BY).  This step resolves all the
//    expressions (without creating scans), and collects information about the
//    expressions in QueryResolutionInfo.  The collected information includes
//    SELECT list columns and aliases, GROUP BY columns, aggregate
//    columns, analytic function columns, and ORDER BY columns.
//    At a high level, this is done by:
//
//    1. Resolve the SELECT list first pass.  This resolves expressions
//       against the FROM clause NameScope, including star and dot-star
//       expansion.  This first pass is necessary to allow the GROUP BY to
//       resolve against SELECT list aliases.
//    2. Resolve the GROUP BY expressions against SELECT list aliases and
//       the FROM clause NameScope.
//    3. Resolve the SELECT list second pass.  If GROUP BY is present, this
//       re-resolves expressions against a new NameScope that includes
//       grouped versions of the columns (expressions computed after
//       grouping/aggregation must reference the grouped versions of the
//       columns since the original columns are not visible after
//       aggregation).  In this new NameScope, non-grouped columns can still
//       be looked up by name but they provide errors if accessed.  There
//       are optimizations in place to avoid re-resolution of expressions
//       whenever possible.
//    4. Resolve the HAVING clause against another new post-grouped column
//       NameScope that includes SELECT list aliases.
//    5. Resolve the QUALIFY clause against same NameScope as HAVING.
//    6. Resolve ORDER BY expressions against this post-grouped column
//       NameScope (or if DISTINCT is present, then a post-DISTINCT
//       NameScope) that includes SELECT list aliases.
//
// 4) Generate all necessary remaining scans based on the information in
//    QueryResolutionInfo, in the following order:
//    1. PROJECT scan for dot-star columns
//    2. AGGREGATE scan
//    3. PROJECT scan for columns needed by HAVING
//    4. FILTER scan for HAVING
//    5. PROJECT scan for columns needed by analytic functions
//    6. ANALYTIC scan
//    7. PROJECT scan if needed for DISTINCT
//    8. AGGREGATE scan for DISTINCT
//    9. PROJECT scan for columns needed by ORDER BY
//   10. ORDER BY scan
//   11. LIMIT OFFSET scan
//   12. PROJECT scan for AS STRUCT/PROTO
//   13. PROJECT scan for handling HINTs
//
// For a more detailed discussion, see (broken link).
absl::Status Resolver::ResolveSelect(
    const ASTSelect* select, const ASTOrderBy* order_by,
    const ASTLimitOffset* limit_offset, const NameScope* external_scope,
    IdString query_alias, bool force_new_columns_for_projected_outputs,
    std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  ZETASQL_RETURN_IF_ERROR(ValidateAnonymizationSetup(language(), select));

  std::unique_ptr<const ResolvedScan> scan;
  std::shared_ptr<const NameList> from_clause_name_list;

  ZETASQL_RETURN_IF_ERROR(ResolveFromClauseAndCreateScan(
      select, order_by, external_scope, &scan, &from_clause_name_list));

  std::unique_ptr<const NameScope> from_scan_scope(
      new NameScope(external_scope, from_clause_name_list));

  // The WHERE clause depends only on the FROM clause, so we resolve it before
  // looking at the SELECT-list or GROUP BY.
  if (select->where_clause() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveWhereClauseAndCreateScan(
        select->where_clause(), from_scan_scope.get(), &scan));
  }

  ZETASQL_RET_CHECK(select->select_list() != nullptr);

  std::unique_ptr<QueryResolutionInfo> query_resolution_info(
      new QueryResolutionInfo(this));
  SelectColumnStateList* select_column_state_list =
      query_resolution_info->select_column_state_list();

  if (select->window_clause() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(query_resolution_info->analytic_resolver()->SetWindowClause(
        *select->window_clause()));
  }

  // Remember the columns of current scan (after the FROM clause and WHERE, but
  // before SELECT).
  query_resolution_info->set_from_clause_name_list(from_clause_name_list);

  ZETASQL_RETURN_IF_ERROR(ResolveSelectListExprsFirstPass(
      select->select_list(), from_scan_scope.get(),
      select->from_clause() != nullptr, from_clause_name_list,
      query_resolution_info.get()));

  query_resolution_info->set_has_group_by(select->group_by() != nullptr);
  query_resolution_info->set_has_having(select->having() != nullptr);
  query_resolution_info->set_has_order_by(order_by != nullptr);

  // Return an appropriate error for anonymization queries that don't perform
  // aggregation.
  // This check is required because we have to wait until after resolving the
  // SELECT list (first pass) to know if there are aggregate functions present.
  if (!query_resolution_info->HasGroupByOrAggregation() &&
      select->anonymization_options() != nullptr) {
    ZETASQL_RET_CHECK_GT(select->select_list()->columns().size(), 0);
    return MakeSqlErrorAt(select->select_list()->columns(0))
           << "SELECT WITH ANONYMIZATION queries require GROUP BY or "
              "aggregation, but neither was present";
  }

  if (query_resolution_info->HasGroupByOrAggregation() &&
      query_resolution_info->HasHavingOrOrderBy()) {
    // We have GROUP BY or aggregation in the SELECT list (we performed
    // first pass SELECT list expression resolution above), and we have
    // either HAVING or ORDER BY.  This implies that the expressions in
    // HAVING or ORDER BY could reference SELECT list aliases, which might
    // need to resolve against either the pre- or post- grouping version
    // of the column.  Consider:
    //
    //   SELECT key as foo
    //   FROM table
    //   GROUP BY key
    //   HAVING foo > 5
    //
    //   SELECT key as foo
    //   FROM table
    //   GROUP BY key
    //   HAVING sum(foo) > 5
    //
    // In the first query, HAVING 'foo' must resolve to the post-grouped version
    // of table.key since the HAVING gets applied after aggregation.  In the
    // second query, HAVING 'foo' must resolve to the pre-grouped version of
    // table.key since the aggregation function is applied on top of it.  To
    // address this, we need to assign and remember pre-grouped versions of
    // all the SELECT list columns that have non-internal aliases (since those
    // could get referenced in HAVING or ORDER BY).
    ZETASQL_RETURN_IF_ERROR(AnalyzeSelectColumnsToPrecomputeBeforeAggregation(
        query_resolution_info.get()));
  }

  if (select->group_by() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveGroupByExprs(select->group_by(),
                                        from_scan_scope.get(),
                                        query_resolution_info.get()));
  }

  // Note that we do not allow HAVING or ORDER BY if there is no FROM
  // clause, so even though aggregation or analytic can appear there
  // we do not have to wait until we analyze those clauses to check for
  // this error condition.
  if (query_resolution_info->HasGroupByOrAggregation() &&
      select->from_clause() == nullptr) {
    return MakeSqlErrorAt(select)
           << "SELECT without FROM clause cannot use aggregation";
  }

  if (query_resolution_info->HasAnalytic() &&
      select->from_clause() == nullptr) {
    return MakeSqlErrorAt(select)
           << "SELECT without FROM clause cannot use analytic functions";
  }

  if (!query_resolution_info->HasGroupByOrAggregation() &&
      !query_resolution_info->HasAnalytic()) {
    // There is no GROUP BY, and no aggregation or analytic functions in the
    // SELECT list, so the initial resolution pass on the SELECT list is
    // final.  This will create ResolvedColumns for the SELECT columns, and
    // identify any columns necessary to precompute.  Once these SELECT list
    // ResolvedColumns are assigned, we avoid calling ResolveExpr() again on
    // the SELECT list expressions in ResolveAdditionalExprsSecondPass().
    // TODO: We should be able to avoid the second pass on SELECT
    // list expressions even if there is analytic present, but we currently
    // reset the analytic resolver state before re-resolving the SELECT and
    // resolving the ORDER BY (which can contain analytic functions).
    // Fix this.
    FinalizeSelectColumnStateList(select->select_list(), query_alias,
                                  force_new_columns_for_projected_outputs,
                                  query_resolution_info.get(),
                                  select_column_state_list);
  }

  // Resolve the SELECT list against what comes out of the GROUP BY.
  // GROUP BY columns that are select list ordinals or aliases already
  // have resolved and updated column references.  Aggregate subexpressions
  // have also already been resolved.  Other expressions will resolve against
  // a GROUP BY name scope, with aggregate subexpressions mapped to their
  // already-resolved columns.

  // Create a new NameScope for what comes out of the AggregateScan.
  // It is derived from the FROM clause scope, allows column references to
  // resolve to the grouped versions of the columns, and provides errors
  // for column references that are not grouped by or aggregated.
  std::unique_ptr<const NameScope> group_by_scope;
  const NameScope* from_clause_or_group_by_scope = from_scan_scope.get();
  if (query_resolution_info->HasGroupByOrAggregation()) {
    // Create a new NameScope that reflect what names are and are not
    // available post-GROUP BY.
    ZETASQL_RETURN_IF_ERROR(CreatePostGroupByNameScope(
        from_scan_scope.get(), query_resolution_info.get(), &group_by_scope));
    from_clause_or_group_by_scope = group_by_scope.get();
  }

  // The analytic function resolver contains information collected during
  // the initial analysis of the SELECT list columns.  Reset and
  // re-initialize the analytic function resolver for second-pass SELECT
  // list resolution and ORDER BY resolution.
  //
  // Note that when we reset the analytic resolver here, we lose all of
  // the information about the currently resolved analytic expressions.
  // This implies that we *must* re-resolve all analytic expressions in
  // order to be able to generate appropriate AnalyticScans.
  if (query_resolution_info->HasAnalytic()) {
    query_resolution_info->ResetAnalyticResolver(this);
  }

  std::shared_ptr<NameList> final_project_name_list(new NameList);

  ZETASQL_RETURN_IF_ERROR(ResolveSelectListExprsSecondPass(
      query_alias, from_clause_or_group_by_scope, &final_project_name_list,
      query_resolution_info.get()));

  ZETASQL_RETURN_IF_ERROR(ResolveAdditionalExprsSecondPass(
      from_clause_or_group_by_scope, query_resolution_info.get()));

  *output_name_list = final_project_name_list;

  // Create new NameLists for SELECT list columns/aliases, since they can be
  // referenced elsewhere in the query (in the GROUP BY, HAVING, and ORDER BY).
  // This NameList reflects post-grouped versions of SELECT list columns
  // (if grouping is present).
  std::shared_ptr<NameList> post_group_by_alias_name_list(new NameList);
  // This NameList reflects pre-grouped versions of SELECT list columns
  // (if grouping is present).
  std::shared_ptr<NameList> pre_group_by_alias_name_list(new NameList);
  // The 'error_name_targets' identify SELECT list aliases whose related
  // expressions contain aggregation or analytic functions.  These
  // NameTargets will be used in a NameScope for resolving HAVING clause
  // aggregate function arguments, where references to these SELECT
  // list aliases are invalid.
  IdStringHashMapCase<NameTarget> error_name_targets;
  std::set<IdString, IdStringCaseLess> select_column_aliases;
  for (const std::unique_ptr<SelectColumnState>& select_column_state :
       select_column_state_list->select_column_state_list()) {
    ZETASQL_RETURN_IF_ERROR(CreateSelectNamelists(
        select_column_state.get(), post_group_by_alias_name_list.get(),
        pre_group_by_alias_name_list.get(), &error_name_targets,
        &select_column_aliases));
  }

  // The NameScope to use when resolving the HAVING and ORDER BY clauses.
  // Includes the GROUP BY scope, along with additional SELECT list aliases
  // for post-grouping versions of columns.
  // SELECT list aliases override any names in group_by_scope.
  std::unique_ptr<NameScope> having_and_order_by_scope;
  ZETASQL_RETURN_IF_ERROR(
      from_clause_or_group_by_scope->CopyNameScopeWithOverridingNames(
          post_group_by_alias_name_list, &having_and_order_by_scope));

  // The NameScope to use when resolving aggregate functions in the HAVING
  // and ORDER BY clauses.  It is the <from_scan_scope>, extended with
  // SELECT list aliases for pre-grouping columns.  The SELECT list aliases
  // override the names in <from_scan_scope>.
  std::unique_ptr<const NameScope> original_select_list_and_from_scan_scope(
      new NameScope(from_scan_scope.get(), pre_group_by_alias_name_list));

  // SELECT list aliases related to aggregate expressions are not valid
  // to access as aggregate function arguments, so update the
  // <original_select_list_and_from_scan_scope> to mark those aliases as
  // invalid to access.
  std::unique_ptr<NameScope> select_list_and_from_scan_scope;

  ZETASQL_RETURN_IF_ERROR(
      original_select_list_and_from_scan_scope
          ->CopyNameScopeWithOverridingNameTargets(
              error_name_targets, &select_list_and_from_scan_scope));

  // Analyze the HAVING.
  // TODO: Should probably move <resolved_having_expr> to
  // <query_resolution_info>.
  std::unique_ptr<const ResolvedExpr> resolved_having_expr;
  if (select->having() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ResolveHavingExpr(select->having(), having_and_order_by_scope.get(),
                          select_list_and_from_scan_scope.get(),
                          query_resolution_info.get(), &resolved_having_expr));
  }

  std::unique_ptr<const ResolvedExpr> resolved_qualify_expr;
  if (select->qualify() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveQualifyExpr(
        select->qualify(), having_and_order_by_scope.get(),
        select_list_and_from_scan_scope.get(), query_resolution_info.get(),
        &resolved_qualify_expr));
  }

  // Analyze the ORDER BY.  If we have SELECT DISTINCT, we will resolve
  // the ORDER BY expressions after resolving the DISTINCT since we must
  // resolve the ORDER BY against post-DISTINCT versions of columns.
  if (order_by != nullptr && !select->distinct()) {
    ZETASQL_RETURN_IF_ERROR(ResolveOrderByExprs(
        order_by, having_and_order_by_scope.get(),
        select_list_and_from_scan_scope.get(), false /* is_post_distinct */,
        query_resolution_info.get()));
  }

  // We are done with analysis and can now build the remaining scans.
  // The current <scan> covers the FROM and WHERE clauses.  The remaining
  // scans are built on top of the current <scan>.
  ZETASQL_RETURN_IF_ERROR(AddRemainingScansForSelect(
      select, order_by, limit_offset, having_and_order_by_scope.get(),
      &resolved_having_expr, &resolved_qualify_expr,
      query_resolution_info.get(), output_name_list, &scan));

  // Any columns produced in a SELECT list (for the final query or any subquery)
  // count as referenced and cannot be pruned.
  RecordColumnAccess(scan->column_list());

  // Some sanity checks.
  // Note that we cannot check that the number of columns in the
  // output_name_list is the same as the number in select_column_state_list,
  // because this is not true for SELECT AS STRUCT or SELECT AS PROTO.

  // All columns to compute have been consumed.
  ZETASQL_RETURN_IF_ERROR(query_resolution_info->CheckComputedColumnListsAreEmpty());

  *output = std::move(scan);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveModelTransformSelectList(
    const NameScope* input_scope, const ASTSelectList* select_list,
    const std::shared_ptr<const NameList>& input_cols_name_list,
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>* transform_list,
    std::vector<std::unique_ptr<const ResolvedOutputColumn>>*
        transform_output_column_list,
    std::vector<std::unique_ptr<const ResolvedAnalyticFunctionGroup>>*
        transform_analytic_function_group_list) {
  QueryResolutionInfo query_info(this);
  for (int i = 0; i < select_list->columns().size(); ++i) {
    ZETASQL_RETURN_IF_ERROR(ResolveSelectColumnFirstPass(
        select_list->columns(i), input_scope, input_cols_name_list, i,
        /*has_from_clause=*/true, &query_info));
  }
  FinalizeSelectColumnStateList(
      select_list, kDummyTableId,
      /*force_new_columns_for_projected_outputs=*/false, &query_info,
      query_info.select_column_state_list());

  // Creates ResolvedComputedColumn for each select column.
  absl::flat_hash_set<IdString, IdStringCaseHash, IdStringCaseEqualFunc>
      col_names;
  for (int i = 0;
       i <
       query_info.select_column_state_list()->select_column_state_list().size();
       ++i) {
    const SelectColumnState* select_column_state =
        query_info.select_column_state_list()->GetSelectColumnState(i);
    if (IsInternalAlias(select_column_state->alias)) {
      return MakeSqlErrorAt(select_column_state->ast_expr)
             << "Anonymous columns are disallowed in TRANSFORM clause. Please "
                "provide a column name";
    }
    if (select_column_state->has_aggregation) {
      return MakeSqlErrorAt(select_column_state->ast_expr)
             << "Aggregation functions are not supported in TRANSFORM clause";
    }
    if (!zetasql_base::InsertIfNotPresent(&col_names, select_column_state->alias)) {
      return MakeSqlErrorAt(select_column_state->ast_expr)
             << "Duplicate column aliases are disallowed in TRANSFORM clause";
    }
    if (select_column_state->has_analytic) {
      const std::vector<
          std::unique_ptr<AnalyticFunctionResolver::AnalyticFunctionGroupInfo>>&
          analytic_function_groups =
              query_info.analytic_resolver()->analytic_function_groups();
      for (const auto& analytic_function_group : analytic_function_groups) {
        if (analytic_function_group->ast_partition_by != nullptr ||
            analytic_function_group->ast_order_by != nullptr) {
          return MakeSqlErrorAt(select_column_state->ast_expr)
                 << "Analytic functions with a non-empty OVER() clause are "
                    "disallowed in the TRANSFORM clause";
        }
        // resolved_computed_columns could be empty after merging. We only add
        // non-empty ones for clarity.
        if (!analytic_function_group->resolved_computed_columns.empty()) {
          transform_analytic_function_group_list->push_back(
              MakeResolvedAnalyticFunctionGroup(
                  /*partition_by=*/nullptr, /*order_by=*/nullptr,
                  std::move(
                      analytic_function_group->resolved_computed_columns)));
        }
      }
    }

    if (select_column_state->resolved_expr != nullptr) {
      // This is a column reference without any computation.
      ZETASQL_RET_CHECK(
          select_column_state->resolved_expr->GetAs<ResolvedColumnRef>() !=
          nullptr)
          << "resolved_expr should be of type ResolvedColumnRef in "
             "ResolveModelTransformSelectList";
      const ResolvedColumnRef* resolved_col_ref =
          select_column_state->resolved_expr->GetAs<ResolvedColumnRef>();
      const ResolvedColumn resolved_col_cp(
          AllocateColumnId(),
          analyzer_options_.id_string_pool()->Make(
              resolved_col_ref->column().table_name()),
          analyzer_options_.id_string_pool()->Make(
              select_column_state->alias.ToString()),
          resolved_col_ref->column().type());
      transform_list->push_back(MakeResolvedComputedColumn(
          resolved_col_cp,
          MakeResolvedColumnRef(resolved_col_ref->column().type(),
                                resolved_col_ref->column(),
                                /*is_correlated=*/false)));
    } else {
      // This is a computed column.
      ZETASQL_RET_CHECK(select_column_state->resolved_computed_column != nullptr)
          << "resolved_computed_column cannot be nullptr in "
             "ResolveModelTransformSelectList when resolved_expr is nullptr";
      zetasql::ResolvedASTDeepCopyVisitor deep_copy_visitor;
      ZETASQL_RETURN_IF_ERROR(
          select_column_state->resolved_computed_column->expr()->Accept(
              &deep_copy_visitor));
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> resolved_expr_copy,
                       deep_copy_visitor.ConsumeRootNode<ResolvedExpr>());
      transform_list->push_back(MakeResolvedComputedColumn(
          select_column_state->resolved_computed_column->column(),
          std::move(resolved_expr_copy)));
    }
    transform_output_column_list->push_back(MakeResolvedOutputColumn(
        select_column_state->alias.ToString(),
        transform_list->at(transform_list->size() - 1)->column()));
  }
  return absl::OkStatus();
}

absl::Status Resolver::CreateSelectNamelists(
    const SelectColumnState* select_column_state,
    NameList* post_group_by_alias_name_list,
    NameList* pre_group_by_alias_name_list,
    IdStringHashMapCase<NameTarget>* error_name_targets,
    std::set<IdString, IdStringCaseLess>* select_column_aliases) {
  // Test expected invariant.
  ZETASQL_RET_CHECK(select_column_state->resolved_select_column.IsInitialized());

  // The alias names should be marked explicit, since they were either
  // explicitly in the query (SELECT col AS alias), were derived
  // from a path expression that was explicitly in the query
  // (SELECT table.col), or were internal (SELECT a+1) where it does
  // not matter.
  //
  // TODO: Consider only doing this for non-internal aliases
  // (i.e., if !IsInternalAlias(select_column_state->alias)), since only
  // those should be able to be referenced elsewhere in the query.  There
  // might be reasons why this will not work (for instance positional
  // references), so investigate this in a post-refactoring changelist.
  ZETASQL_RETURN_IF_ERROR(post_group_by_alias_name_list->AddColumn(
      select_column_state->alias, select_column_state->resolved_select_column,
      true /* is_explicit */));

  const ResolvedColumn target_column =
      select_column_state->HasPreGroupByResolvedColumn()
          ? select_column_state->resolved_pre_group_by_select_column
          : select_column_state->resolved_select_column;
  ZETASQL_RETURN_IF_ERROR(pre_group_by_alias_name_list->AddColumn(
      select_column_state->alias, target_column, true /* is_explicit */));
  if (select_column_state->has_aggregation ||
      select_column_state->has_analytic) {
    if (zetasql_base::ContainsKey(*select_column_aliases, select_column_state->alias)) {
      // There's already a SELECT list alias for this, so make the
      // NameTarget ambiguous as well.  Note that the NameTarget may or
      // may not exist yet in <error_name_targets>.
      zetasql_base::InsertOrUpdate(error_name_targets, select_column_state->alias,
                          NameTarget());
      return absl::OkStatus();
    }
    NameTarget name_target(target_column, true /* is_explicit */);
    name_target.SetAccessError(
        NameTarget::EXPLICIT_COLUMN,
        select_column_state->has_aggregation
            ? "Aggregations of aggregations are not allowed"
            : "Analytic functions cannot be arguments to aggregate functions");
    // This insert should succeed, since if there was already an entry for
    // this alias we would have handled the alias as ambiguous above.
    ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(error_name_targets,
                                      select_column_state->alias, name_target));
  } else {
    if (zetasql_base::ContainsKey(*error_name_targets, select_column_state->alias)) {
      // We saw a non-aggregate SELECT column with the same alias as
      // an aggregate/analytic SELECT column.  Ensure that the related
      // NameTarget is ambiguous.
      zetasql_base::InsertOrUpdate(error_name_targets, select_column_state->alias,
                          NameTarget());
      return absl::OkStatus();
    }
  }
  select_column_aliases->insert(select_column_state->alias);
  return absl::OkStatus();
}

// Analyzes an expression, and if it is logically a path expression (of
// one or more names) then returns true, along with the 'source_column'
// where the path expression starts and a 'valid_name_path' that identifies
// the path name list along with the 'target_column' that the entire path
// expression resolves to.
// If the expression is not a path expression then sets 'source_column'
// to be uninitialized and returns false.
bool Resolver::GetSourceColumnAndNamePath(
    const ResolvedExpr* resolved_expr, ResolvedColumn target_column,
    ResolvedColumn* source_column, ValidNamePath* valid_name_path) const {
  *source_column = ResolvedColumn();
  while (resolved_expr->node_kind() == RESOLVED_GET_PROTO_FIELD) {
    const ResolvedGetProtoField* get_proto_field =
        resolved_expr->GetAs<ResolvedGetProtoField>();
    // NOTE - The ResolvedGetProtoField has a get_has_bit() function
    // that identifies whether this expression fetches the field value, or
    // a boolean that indicates if the value was present.  If get_has_bit()
    // is true, by convention the name of that pseudocolumn is the field
    // name with the prefix 'has_'.
    if (get_proto_field->get_has_bit()) {
      valid_name_path->mutable_name_path()->push_back(MakeIdString(
          absl::StrCat("has_", get_proto_field->field_descriptor()->name())));
    } else {
      valid_name_path->mutable_name_path()->push_back(
          MakeIdString(get_proto_field->field_descriptor()->name()));
    }
    resolved_expr = get_proto_field->expr();
  }
  while (resolved_expr->node_kind() == RESOLVED_GET_STRUCT_FIELD) {
    const ResolvedGetStructField* get_struct_field =
        resolved_expr->GetAs<ResolvedGetStructField>();
    const StructType* struct_type =
        get_struct_field->expr()->type()->AsStruct();
    valid_name_path->mutable_name_path()->push_back(
        MakeIdString(struct_type->field(get_struct_field->field_idx()).name));
    resolved_expr = get_struct_field->expr();
  }
  std::reverse(valid_name_path->mutable_name_path()->begin(),
               valid_name_path->mutable_name_path()->end());
  if (resolved_expr->node_kind() == RESOLVED_COLUMN_REF) {
    *source_column = resolved_expr->GetAs<ResolvedColumnRef>()->column();
    valid_name_path->set_target_column(target_column);
    return true;
  }
  return false;
}

absl::Status Resolver::AnalyzeSelectColumnsToPrecomputeBeforeAggregation(
    QueryResolutionInfo* query_resolution_info) {
  SelectColumnStateList* select_column_state_list =
      query_resolution_info->select_column_state_list();
  for (int idx = 0; idx < select_column_state_list->Size(); ++idx) {
    SelectColumnState* select_column_state =
        select_column_state_list->GetSelectColumnState(idx);
    // If the column has analytic or aggregation, then we do not compute
    // this before the AggregateScan.
    if (select_column_state->has_aggregation ||
        select_column_state->has_analytic) {
      continue;
    }
    if (!IsInternalAlias(select_column_state->alias)) {
      ZETASQL_RET_CHECK(select_column_state->resolved_expr != nullptr);
      ResolvedColumn pre_group_by_column;
      if (select_column_state->resolved_expr->node_kind() ==
          RESOLVED_COLUMN_REF) {
        // The expression already resolved to a column (either correlated
        // or uncorrelated is ok), so just use it.
        pre_group_by_column =
            select_column_state->resolved_expr->GetAs<ResolvedColumnRef>()
                ->column();
      } else {
        // The expression is not a simple column reference, it is a more
        // complicated expression that must be computed before aggregation
        // so that we can GROUP BY that computed column.
        pre_group_by_column = ResolvedColumn(
            AllocateColumnId(), kPreGroupById, select_column_state->alias,
            select_column_state->resolved_expr->annotated_type());
        // If the expression is a path expression then collect that
        // information in the QueryResolutionInfo so that we know that
        // accessing that path is valid post-GROUP BY, even if accessing
        // the source of the path is not.
        ResolvedColumn source_column;
        ValidNamePath valid_name_path;
        if (GetSourceColumnAndNamePath(select_column_state->resolved_expr.get(),
                                       pre_group_by_column, &source_column,
                                       &valid_name_path)) {
          // We found a field access path, register it in QueryResolutionInfo.
          query_resolution_info->mutable_select_list_valid_field_info_map()
              ->InsertNamePath(source_column, valid_name_path);
        }
        query_resolution_info
            ->select_list_columns_to_compute_before_aggregation()
            ->push_back(MakeResolvedComputedColumn(
                pre_group_by_column,
                std::move(select_column_state->resolved_expr)));
        // This column reference will be used when resolving the GROUP BY
        // expressions.
        select_column_state->resolved_expr = MakeColumnRef(pre_group_by_column);
      }
      select_column_state->resolved_pre_group_by_select_column =
          pre_group_by_column;
    }
  }
  return absl::OkStatus();
}

class FindReferencedColumnsVisitor : public ResolvedASTVisitor {
 public:
  explicit FindReferencedColumnsVisitor(
      absl::flat_hash_set<ResolvedColumn>* columns)
      : columns_(columns) {}
  absl::Status DefaultVisit(const ResolvedNode* node) override {
    return node->ChildrenAccept(this);
  }

  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* node) override {
    columns_->insert(node->column());
    return absl::OkStatus();
  }

 private:
  absl::flat_hash_set<ResolvedColumn>* columns_;
};

absl::Status Resolver::ResolveQualifyExpr(
    const ASTQualify* qualify, const NameScope* having_and_order_by_scope,
    const NameScope* select_list_and_from_scan_scope,
    QueryResolutionInfo* query_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_qualify_expr) {

  if (!language().LanguageFeatureEnabled(FEATURE_V_1_3_QUALIFY)) {
    return MakeSqlErrorAt(qualify) << "QUALIFY is not supported";
  }

  ExprResolutionInfo expr_resolution_info(
      having_and_order_by_scope, select_list_and_from_scan_scope,
      /*allows_aggregation_in=*/true, /*allows_analytic_in=*/true,
      query_resolution_info->HasAnalytic(), "QUALIFY clause",
      query_resolution_info);
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(qualify->expression(), &expr_resolution_info,
                              resolved_qualify_expr));

  ZETASQL_RET_CHECK(*resolved_qualify_expr != nullptr);
  ZETASQL_RETURN_IF_ERROR(CoerceExprToBool(qualify->expression(), "QUALIFY clause",
                                   resolved_qualify_expr));

  if (!query_resolution_info->HasAnalytic()) {
    return MakeSqlErrorAt(qualify->expression())
           << "The QUALIFY clause requires analytic function to be present";
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveHavingExpr(
    const ASTHaving* having, const NameScope* having_and_order_by_scope,
    const NameScope* select_list_and_from_scan_scope,
    QueryResolutionInfo* query_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_having_expr) {
  // Aggregation is only allowed if we already saw GROUP BY or aggregation.
  // In theory, the following is valid, though it's not very interesting:
  //   SELECT 1 from T HAVING sum(T.a) > 5;
  // Return an error for that case.  The only way I can think of how to
  // allow this is to do an initial resolution pass on HAVING to detect
  // aggregations, so that when we resolve the SELECT list above it
  // can detect errors when referencing non-grouped and
  // non-aggregated columns, such as:
  //   SELECT value FROM KeyValue HAVING sum(key) > 5;
  const bool already_saw_group_by_or_aggregation =
      query_resolution_info->HasGroupByOrAggregation();
  ExprResolutionInfo expr_resolution_info(
      having_and_order_by_scope, select_list_and_from_scan_scope,
      true /* allows_aggregation */, false /* allows_analytic */,
      query_resolution_info->HasGroupByOrAggregation(), "HAVING clause",
      query_resolution_info);
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(having->expression(), &expr_resolution_info,
                              resolved_having_expr));

  ZETASQL_RET_CHECK(*resolved_having_expr != nullptr);
  ZETASQL_RETURN_IF_ERROR(CoerceExprToBool(having->expression(), "HAVING clause",
                                   resolved_having_expr));

  if (!already_saw_group_by_or_aggregation &&
      query_resolution_info->HasGroupByOrAggregation()) {
    return MakeSqlErrorAt(having->expression())
           << "The HAVING clause only allows aggregation if GROUP BY or "
              "SELECT list aggregation is present";
  }
  // TODO: Should we move this up above, and simply bail
  // if HAVING is present without GROUP BY or aggregation in the
  // SELECT list?  We end up bailing anyway, but it is sort of nice
  // to detect the corner case and give a more specific error message
  // above.
  if (!query_resolution_info->HasGroupByOrAggregation()) {
    return MakeSqlErrorAt(having->expression())
           << "The HAVING clause requires GROUP BY or aggregation to "
              "be present";
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveOrderByExprs(
    const ASTOrderBy* order_by, const NameScope* having_and_order_by_scope,
    const NameScope* select_list_and_from_scan_scope, bool is_post_distinct,
    QueryResolutionInfo* query_resolution_info) {
  // Aggregation is only allowed if we already saw GROUP BY or aggregation.
  // In theory, we could support:
  // SELECT 1 from T ORDER BY sum(T.a) > 5;
  const bool already_saw_group_by_or_aggregation =
      query_resolution_info->HasGroupByOrAggregation();

  // TODO: Clean this up.  This is setting global state, in the
  // context of analyzing the ORDER BY clause.  This is ok because once we
  // get to resolving the ORDER BY expressions, we won't re-resolve an
  // analytic function where named windows are allowed.  Make sure we
  // have test coverage for this:
  // select agg() over (named_window)
  // from table
  // order by agg2() over (partition by a)
  // window named_window (partition by b)
  static const char clause_name[] = "ORDER BY clause";
  query_resolution_info->analytic_resolver()->DisableNamedWindowRefs(
      clause_name);

  // Aggregation is not allowed in ORDER BY if the query is SELECT DISTINCT.
  // Analytic functions are also currently disallowed after SELECT DISTINCT,
  // but could be allowed (it would require a bunch of additional analysis
  // logic though).  TODO: It is low priority but make this work.
  // Maybe wait until we get a feature request.
  ExprResolutionInfo expr_resolution_info(
      having_and_order_by_scope, select_list_and_from_scan_scope,
      !is_post_distinct /* allows_aggregation */,
      !is_post_distinct /* allows_analytic */,
      query_resolution_info->HasGroupByOrAggregation(), clause_name,
      query_resolution_info);

  ZETASQL_RETURN_IF_ERROR(ResolveOrderingExprs(
      order_by->ordering_expressions(), &expr_resolution_info,
      expr_resolution_info.query_resolution_info
          ->mutable_order_by_item_info()));

  AddColumnsForOrderByExprs(
      kOrderById /* query_alias */,
      query_resolution_info->mutable_order_by_item_info(),
      query_resolution_info->order_by_columns_to_compute());

  if (!already_saw_group_by_or_aggregation &&
      query_resolution_info->HasGroupByOrAggregation()) {
    // Return an error for now.  The only way I can think of how to
    // allow this is to do an earlier pass over the ORDER BY to detect
    // aggregations, so that the select list can resolve against the
    // post grouped columns only so it can detect an error for a query
    // that looks like:
    //  SELECT value FROM KeyValue ORDER BY sum(key);
    return MakeSqlErrorAt(order_by)
           << "The ORDER BY clause only allows aggregation if GROUP BY or "
              "SELECT list aggregation is present";
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveWhereClauseAndCreateScan(
    const ASTWhereClause* where_clause, const NameScope* from_scan_scope,
    std::unique_ptr<const ResolvedScan>* current_scan) {
  std::unique_ptr<const ResolvedExpr> resolved_where;
  static constexpr char kWhereClause[] = "WHERE clause";
  ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(where_clause->expression(), from_scan_scope,
                                    kWhereClause, &resolved_where));
  ZETASQL_RETURN_IF_ERROR(CoerceExprToBool(where_clause->expression(), kWhereClause,
                                   &resolved_where));

  const auto& tmp_column_list = (*current_scan)->column_list();
  *current_scan = MakeResolvedFilterScan(
      tmp_column_list, std::move(*current_scan), std::move(resolved_where));
  return absl::OkStatus();
}

void Resolver::FinalizeSelectColumnStateList(
    const ASTSelectList* ast_select_list, IdString query_alias,
    bool force_new_columns_for_projected_outputs,
    QueryResolutionInfo* query_resolution_info,
    SelectColumnStateList* select_column_state_list) {
  // TODO: Consider renaming SelectColumnStateList to
  // SelectListColumnStateInfo or similar.  It's just weird that we have
  // SelectColumnStateList that has a method select_column_state_list()
  // that returns a vector<SelectColumnsState*>.
  for (const std::unique_ptr<SelectColumnState>& select_column_state :
       select_column_state_list->select_column_state_list()) {
    if (!force_new_columns_for_projected_outputs &&
        select_column_state->resolved_expr->node_kind() ==
            RESOLVED_COLUMN_REF &&
        !select_column_state->resolved_expr->GetAs<ResolvedColumnRef>()
             ->is_correlated() &&
        !analyzer_options_.create_new_column_for_each_projected_output()) {
      // The expression was already resolved to a column.  If it was not
      // correlated, just use the column.
      const ResolvedColumn& select_column =
          select_column_state->resolved_expr->GetAs<ResolvedColumnRef>()
              ->column();
      select_column_state->resolved_select_column = select_column;
    } else {
      ResolvedColumn select_column(
          AllocateColumnId(), query_alias, select_column_state->alias,
          select_column_state->resolved_expr->annotated_type());
      std::unique_ptr<ResolvedComputedColumn> resolved_computed_column =
          MakeResolvedComputedColumn(
              select_column, std::move(select_column_state->resolved_expr));
      select_column_state->resolved_computed_column =
          resolved_computed_column.get();
      // TODO: Also do not include internal aliases, i.e.,
      // !IsInternalAlias(select_column_state->alias).  Do this in a
      // subsequent changelist, as it will impact where/when such columns
      // get PROJECTed.
      query_resolution_info->select_list_columns_to_compute()->push_back(
          std::move(resolved_computed_column));
      select_column_state->resolved_select_column = select_column;
    }
  }
}

IdString Resolver::ComputeSelectColumnAlias(
    const ASTSelectColumn* ast_select_column, int column_idx) const {
  IdString alias;
  if (ast_select_column->alias() != nullptr) {
    alias = ast_select_column->alias()->GetAsIdString();
  } else {
    alias = GetAliasForExpression(ast_select_column->expression());
    if (alias.empty()) {
      // Arbitrary locally unique name.
      alias = MakeIdString(absl::StrCat("$col", column_idx + 1));
    }
  }
  return alias;
}

// This struct represents sets of column names to be excluded or replaced.
// This is used for SELECT * EXCEPT (...) REPLACE (...) syntax.
// See (broken link).
struct ColumnReplacements {
  std::string DebugString() const {
    std::string debug_string;
    absl::StrAppend(&debug_string, "\nexcluded_columns: (",
                    absl::StrJoin(excluded_columns, ",", IdStringFormatter),
                    ")");
    absl::StrAppend(&debug_string, "\nreplaced_columns:");
    for (const auto& replaced_column : replaced_columns) {
      absl::StrAppend(&debug_string, "\n  (",
                      replaced_column.first.ToStringView(), ",",
                      (replaced_column.second == nullptr
                           ? "null"
                           : replaced_column.second->DebugString()),
                      ")");
    }
    return debug_string;
  }

  // Column names that should be skipped.
  IdStringHashSetCase excluded_columns;

  // Column names that should be replaced.  Each should get used only once.
  // This map stores a single SelectColumnState that will be used as a
  // replacement.  When the replacement happens, that object is moved out and
  // the map will store NULL.  Callers should assert that they don't find a
  // NULL in this map.
  IdStringHashMapCase<std::unique_ptr<SelectColumnState>> replaced_columns;
};

// Check if <column_name> should be excluded or replaced according to
// <column_replacements> (which may be NULL).
// Return true if the caller should skip adding this column.
// For replaces, the replacement column will have been added
// to <select_column_state_list>.
static bool ExcludeOrReplaceColumn(
    const ASTExpression* ast_expression, IdString column_name,
    ColumnReplacements* column_replacements,
    SelectColumnStateList* select_column_state_list) {
  if (column_replacements == nullptr) {
    return false;
  }
  if (zetasql_base::ContainsKey(column_replacements->excluded_columns, column_name)) {
    return true;
  }
  if (zetasql_base::ContainsKey(column_replacements->replaced_columns, column_name)) {
    select_column_state_list->AddSelectColumn(std::move(
        zetasql_base::FindOrDie(column_replacements->replaced_columns, column_name)));
    // I'd use ZETASQL_RET_CHECK here, except then I'd have to return StatusOr<bool>.
    ZETASQL_DCHECK(select_column_state_list->select_column_state_list().back() !=
           nullptr);
    return true;
  }
  return false;
}

absl::Status Resolver::AddNameListToSelectList(
    const ASTExpression* ast_expression,
    const std::shared_ptr<const NameList>& name_list,
    const CorrelatedColumnsSetList& correlated_columns_set_list,
    bool ignore_excluded_value_table_fields,
    SelectColumnStateList* select_column_state_list,
    ColumnReplacements* column_replacements) {
  const int orig_num_columns = select_column_state_list->Size();
  for (const NamedColumn& named_column : name_list->columns()) {
    // Process exclusions first because MakeColumnRef will add columns
    // to referenced_columns_ and then they cannot be pruned.
    if (!named_column.is_value_table_column &&
        ExcludeOrReplaceColumn(ast_expression, named_column.name,
                               column_replacements, select_column_state_list)) {
      continue;
    }

    std::unique_ptr<const ResolvedColumnRef> column_ref =
        MakeColumnRefWithCorrelation(named_column.column,
                                     correlated_columns_set_list);
    if (named_column.is_value_table_column) {
      // For value tables with fields, SELECT * expands to those fields
      // rather than showing the container value.
      // For scalar-valued value tables, or values with zero fields,
      // we'll just get the value rather than its fields.
      ZETASQL_RET_CHECK(!named_column.name.empty());

      ZETASQL_RETURN_IF_ERROR(AddColumnFieldsToSelectList(
          ast_expression, column_ref.get(),
          false /* src_column_has_aggregation */,
          false /* src_column_has_analytic */,
          named_column.name /* column_alias_if_no_fields */,
          (ignore_excluded_value_table_fields
               ? &named_column.excluded_field_names
               : nullptr),
          select_column_state_list, column_replacements));
    } else {
      select_column_state_list->AddSelectColumn(
          ast_expression, named_column.name, named_column.is_explicit,
          /*has_aggregation=*/false, /*has_analytic=*/false,
          std::move(column_ref));
    }
  }
  // Detect if the * ended up expanding to zero columns after applying EXCEPT,
  // and treat that as an error.
  if (orig_num_columns == select_column_state_list->Size()) {
    ZETASQL_RET_CHECK(column_replacements != nullptr &&
              !column_replacements->excluded_columns.empty());
    return MakeSqlErrorAt(ast_expression)
           << "SELECT * expands to zero columns after applying EXCEPT";
  }
  return absl::OkStatus();
}

// static.
std::string Resolver::ColumnAliasOrPosition(IdString alias, int column_pos) {
  return IsInternalAlias(alias) ? absl::StrCat(1 + column_pos)
                                : alias.ToString();
}

// If 'resolved_expr' is a resolved path expression (zero or more
// RESOLVED_GET_*_FIELD expressions over a ResolvedColumnRef) then inserts
// a new entry into 'query_resolution_info->group_by_valid_field_info_map'
// with a source ResolvedColumn that is the 'resolved_expr' source
// ResolvedColumnRef column, the name path derived from the 'resolved_expr'
// get_*_field expressions, along with the 'target_column'.
// If 'resolved_expr' is not a resolved path expression then has no
// effect.
absl::Status Resolver::CollectResolvedPathExpressionInfoIfRelevant(
    QueryResolutionInfo* query_resolution_info,
    const ResolvedExpr* resolved_expr, ResolvedColumn target_column) const {
  ResolvedColumn source_column;
  ValidNamePath valid_name_path;
  if (!GetSourceColumnAndNamePath(resolved_expr, target_column, &source_column,
                                  &valid_name_path)) {
    return absl::OkStatus();
  }
  // The 'source_column' might itself come from resolving a path expression.
  // If so, then we need to merge the ValidFieldInfo that produced the
  // 'source_column' with 'valid_name_path'.  This gives us a resulting
  // ValidFieldInfo that relates the original ValidFieldInfo source column
  // through a full (concatenated) path name list to the 'target_column'.
  ResolvedColumn new_source_column = source_column;
  for (const auto& entry :
       query_resolution_info->select_list_valid_field_info_map().map()) {
    const ValidNamePathList* select_list_valid_name_path_list =
        entry.second.get();
    ZETASQL_RET_CHECK(select_list_valid_name_path_list != nullptr);
    bool found = false;
    for (const ValidNamePath& select_list_valid_name_path :
         *select_list_valid_name_path_list) {
      if (source_column == select_list_valid_name_path.target_column()) {
        const int total_name_path_size =
            valid_name_path.name_path().size() +
            select_list_valid_name_path.name_path().size();
        std::vector<IdString> new_name_path;
        new_name_path.reserve(total_name_path_size);
        valid_name_path.mutable_name_path()->reserve(total_name_path_size);
        new_name_path.insert(new_name_path.end(),
                             select_list_valid_name_path.name_path().begin(),
                             select_list_valid_name_path.name_path().end());
        new_name_path.insert(new_name_path.end(),
                             valid_name_path.name_path().begin(),
                             valid_name_path.name_path().end());
        valid_name_path.set_name_path(new_name_path);
        new_source_column = entry.first;
        found = true;
        break;
      }
    }
    if (found) {
      // There should only be one match, so if we find it then we are
      // done.
      break;
    }
  }
  query_resolution_info->mutable_group_by_valid_field_info_map()
      ->InsertNamePath(new_source_column, valid_name_path);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveSelectDistinct(
    const ASTSelect* select, SelectColumnStateList* select_column_state_list,
    const NameList* input_name_list,
    std::unique_ptr<const ResolvedScan>* current_scan,
    QueryResolutionInfo* query_resolution_info,
    std::shared_ptr<const NameList>* output_name_list) {

  // For DISTINCT processing, we will build and maintain a mapping from
  // SELECT list expressions to post-DISTINCT versions of columns.  This
  // mapping will be used when resolving ORDER BY expression after the
  // DISTINCT.

  // DISTINCT processing will re-use the GROUP BY information, so clear out
  // any old GROUP BY information that may exist.  But first, we must
  // preserve the mapping from SELECT list expressions to post-GROUP BY
  // columns so that we can update it during DISTINCT processing to capture the
  // mapping from SELECT list expressions to post-DISTINCT columns instead.
  query_resolution_info->ClearGroupByInfo();

  ZETASQL_RET_CHECK_EQ(select_column_state_list->Size(),
               input_name_list->num_columns());

  std::shared_ptr<NameList> name_list(new NameList);

  for (int column_pos = 0; column_pos < input_name_list->num_columns();
       ++column_pos) {
    const NamedColumn& named_column = input_name_list->column(column_pos);
    const ResolvedColumn& column = named_column.column;
    SelectColumnState* select_column_state =
        select_column_state_list->GetSelectColumnState(column_pos);
    const ASTNode* ast_column_location = select_column_state->ast_expr;

    std::string no_grouping_type;
    if (!TypeSupportsGrouping(column.type(), &no_grouping_type)) {
      return MakeSqlErrorAt(ast_column_location)
             << "Column "
             << ColumnAliasOrPosition(named_column.name, column_pos)
             << " of type " << no_grouping_type
             << " cannot be used in SELECT DISTINCT";
    }

    const ResolvedExpr* resolved_expr =
        select_column_state->resolved_expr.get();
    if (resolved_expr == nullptr) {
      ZETASQL_RET_CHECK(select_column_state->resolved_computed_column != nullptr);
      resolved_expr = select_column_state->resolved_computed_column->expr();
    }
    ZETASQL_RET_CHECK(resolved_expr != nullptr);

    ResolvedColumn distinct_column;
    const ResolvedComputedColumn* existing_computed_column =
        query_resolution_info->GetEquivalentGroupByComputedColumnOrNull(
            resolved_expr);
    if (existing_computed_column == nullptr) {
      // We could not find a computed column that matches the original
      // select column state expression.  Now look for a computed column
      // that matches a reference to <column>, since if a previous SELECT
      // list computed column was created in this method then it was
      // added in the AddGroupByComputedColumnIfNeeded() call below.  We
      // detect that duplicate here so that we can re-use it, and do not
      // need to compute another column for it.
      std::unique_ptr<ResolvedColumnRef> column_ref = MakeColumnRef(column);
      existing_computed_column =
          query_resolution_info->GetEquivalentGroupByComputedColumnOrNull(
              column_ref.get());
    }
    if (existing_computed_column != nullptr) {
      // Reference the existing column rather than recompute the expression.
      distinct_column = existing_computed_column->column();
    } else {
      // Create a new DISTINCT column.
      distinct_column =
          ResolvedColumn(AllocateColumnId(), kDistinctId, column.name_id(),
                         column.annotated_type());
      // Add a computed column for the new post-DISTINCT column.
      query_resolution_info->AddGroupByComputedColumnIfNeeded(
          distinct_column, MakeColumnRef(column));
    }

    ZETASQL_RETURN_IF_ERROR(name_list->AddColumn(named_column.name, distinct_column,
                                         named_column.is_explicit));
    // Update the SelectListColumnState with the new post-DISTINCT
    // ResolvedColumn information.
    select_column_state->resolved_select_column = distinct_column;

    // Store the mapping of pre-DISTINCT to post-DISTINCT column.
    query_resolution_info->mutable_group_by_valid_field_info_map()
        ->InsertNamePath(column, {{} /* name_path */, distinct_column});

    // If the 'resolved_expr' is a path expression, we must collect
    // information in the 'query_resolution_info' about that path
    // expression and its relationship to the 'distinct_column'.
    // This information will get used later when constructing a
    // NameScope for what names are valid post-DISTINCT, where the
    // new NameScope is used when resolving a subsequent ORDER BY.
    // If the 'resolved_expr' is not a path expression then this
    // is a no-op.
    ZETASQL_RETURN_IF_ERROR(CollectResolvedPathExpressionInfoIfRelevant(
        query_resolution_info, resolved_expr, distinct_column));
  }

  *output_name_list = name_list;

  // Set the query resolution context so subsequent ORDER BY expression
  // resolution will know it is in the context of post-DISTINCT processing.
  query_resolution_info->set_is_post_distinct(true);

  return AddAggregateScan(select, true /* is_for_select_distinct */,
                          query_resolution_info, current_scan);
}

absl::Status Resolver::ResolveSelectStarModifiers(
    const ASTNode* ast_location, const ASTStarModifiers* modifiers,
    const NameList* name_list_for_star, const Type* type_for_star,
    const NameScope* scope, QueryResolutionInfo* query_resolution_info,
    ColumnReplacements* column_replacements) {
  ZETASQL_RET_CHECK(name_list_for_star != nullptr || type_for_star != nullptr);
  ZETASQL_RET_CHECK(name_list_for_star == nullptr || type_for_star == nullptr);
  ZETASQL_RET_CHECK(modifiers != nullptr);

  const ASTStarExceptList* except_list = modifiers->except_list();
  const absl::Span<const ASTStarReplaceItem* const>& replace_items =
      modifiers->replace_items();

  if (!language().LanguageFeatureEnabled(
          FEATURE_V_1_1_SELECT_STAR_EXCEPT_REPLACE)) {
    if (except_list != nullptr) {
      return MakeSqlErrorAt(ast_location) << "SELECT * EXCEPT is not supported";
    } else {
      return MakeSqlErrorAt(ast_location)
             << "SELECT * REPLACE is not supported";
    }
  }

  if (except_list != nullptr) {
    for (const ASTIdentifier* ast_identifier : except_list->identifiers()) {
      const IdString identifier = ast_identifier->GetAsIdString();
      if (IsInternalAlias(identifier)) {
        return MakeSqlErrorAt(ast_identifier)
               << "Cannot use EXCEPT with internal alias "
               << ToIdentifierLiteral(identifier);
      }
      const Type::HasFieldResult has_field =
          name_list_for_star != nullptr
              ? name_list_for_star->SelectStarHasColumn(identifier)
              : type_for_star->HasField(identifier.ToString(),
                                        /*field_id=*/nullptr,
                                        /*include_pseudo_fields=*/false);
      switch (has_field) {
        case Type::HAS_NO_FIELD:
          return MakeSqlErrorAt(ast_identifier)
                 << "Column " << ToIdentifierLiteral(identifier)
                 << " in SELECT * EXCEPT list does not exist";
        case Type::HAS_FIELD:
        case Type::HAS_AMBIGUOUS_FIELD:  // Duplicate columns ok for EXCEPT.
          break;
        case Type::HAS_PSEUDO_FIELD:
          // SelectStarHasColumn can never return HAS_PSEUDO_FIELD.
          // HasField with include_pseudo_fields=false can never return
          // HAS_PSEUDO_FIELD either.
          ZETASQL_RET_CHECK_FAIL() << "Unexpected Type::HAS_PSEUDO_FIELD value";
          break;
      }
      if (!zetasql_base::InsertIfNotPresent(&column_replacements->excluded_columns,
                                   identifier)) {
        return MakeSqlErrorAt(ast_identifier)
               << "Duplicate column " << ToIdentifierLiteral(identifier)
               << " in SELECT * EXCEPT list";
      }
    }
  }
  for (const ASTStarReplaceItem* ast_replace_item : replace_items) {
    ExprResolutionInfo expr_resolution_info(
        scope, query_resolution_info, ast_replace_item->expression(),
        ast_replace_item->alias()->GetAsIdString());
    std::unique_ptr<const ResolvedExpr> resolved_expr;
    ZETASQL_RETURN_IF_ERROR(ResolveExpr(ast_replace_item->expression(),
                                &expr_resolution_info, &resolved_expr));
    if (expr_resolution_info.has_analytic) {
      // TODO This is disallowed only because it doesn't work in
      // the current implementation, because of a problem that occurs later.
      return MakeSqlErrorAt(ast_replace_item->expression())
             << "Cannot use analytic functions inside SELECT * REPLACE";
    }

    const IdString identifier = ast_replace_item->alias()->GetAsIdString();
    if (IsInternalAlias(identifier)) {
      return MakeSqlErrorAt(ast_replace_item)
             << "Cannot use REPLACE with internal alias "
             << ToIdentifierLiteral(identifier);
    }
    if (zetasql_base::ContainsKey(column_replacements->excluded_columns, identifier)) {
      return MakeSqlErrorAt(ast_replace_item->alias())
             << "Column " << ToIdentifierLiteral(identifier)
             << " cannot occur in both SELECT * EXCEPT and REPLACE";
    }
    const Type::HasFieldResult has_field =
        name_list_for_star != nullptr
            ? name_list_for_star->SelectStarHasColumn(identifier)
            : type_for_star->HasField(identifier.ToString(),
                                      /*field_id=*/nullptr,
                                      /*include_pseudo_fields=*/false);
    switch (has_field) {
      case Type::HAS_NO_FIELD:
        return MakeSqlErrorAt(ast_replace_item->alias())
               << "Column " << ToIdentifierLiteral(identifier)
               << " in SELECT * REPLACE list does not exist";
      case Type::HAS_FIELD:
        break;
      case Type::HAS_AMBIGUOUS_FIELD:
        return MakeSqlErrorAt(ast_replace_item->alias())
               << "Column " << ToIdentifierLiteral(identifier)
               << " in SELECT * REPLACE list is ambiguous";
      case Type::HAS_PSEUDO_FIELD:
        // SelectStarHasColumn can never return HAS_PSEUDO_FIELD.
        // HasField with include_pseudo_fields=false can never return
        // HAS_PSEUDO_FIELD either.
        ZETASQL_RET_CHECK_FAIL() << "Unexpected Type::HAS_PSEUDO_FIELD value";
    }

    auto select_column_state = absl::make_unique<SelectColumnState>(
        ast_replace_item->expression(), identifier,
        /*is_explicit=*/true, expr_resolution_info.has_aggregation,
        expr_resolution_info.has_analytic, std::move(resolved_expr));

    if (!column_replacements->replaced_columns
             .emplace(identifier, std::move(select_column_state))
             .second) {
      return MakeSqlErrorAt(ast_replace_item->alias())
             << "Duplicate column " << ToIdentifierLiteral(identifier)
             << " in SELECT * REPLACE list";
    }
  }

  return absl::OkStatus();
}

// NOTE: The behavior of star expansion here must match
// NameList::SelectStarHasColumn.
absl::Status Resolver::ResolveSelectStar(
    const ASTExpression* ast_select_expr,
    const std::shared_ptr<const NameList>& from_clause_name_list,
    const NameScope* from_scan_scope, bool has_from_clause,
    QueryResolutionInfo* query_resolution_info) {
  if (in_strict_mode()) {
    return MakeSqlErrorAt(ast_select_expr)
           << "SELECT * is not allowed in strict name resolution mode";
  }
  if (!has_from_clause) {
    return MakeSqlErrorAt(ast_select_expr)
           << "SELECT * must have a FROM clause";
  }
  if (from_clause_name_list->num_columns() == 0) {
    return MakeSqlErrorAt(ast_select_expr)
           << "SELECT * would expand to zero columns";
  }

  // Process SELECT * EXCEPT(...) REPLACE(...) if present.
  ColumnReplacements column_replacements;
  if (ast_select_expr->node_kind() == AST_STAR_WITH_MODIFIERS) {
    const ASTStarWithModifiers* ast_node =
        ast_select_expr->GetAsOrDie<ASTStarWithModifiers>();
    ZETASQL_RETURN_IF_ERROR(ResolveSelectStarModifiers(
        ast_node, ast_node->modifiers(), from_clause_name_list.get(),
        nullptr /* type_for_star */, from_scan_scope, query_resolution_info,
        &column_replacements));
  }

  const CorrelatedColumnsSetList correlated_columns_set_list;
  ZETASQL_RETURN_IF_ERROR(AddNameListToSelectList(
      ast_select_expr, from_clause_name_list, correlated_columns_set_list,
      true /* ignore_excluded_value_table_fields */,
      query_resolution_info->select_column_state_list(), &column_replacements));

  return absl::OkStatus();
}

static absl::Status MakeErrorIfTypeDotStarHasNoFields(
    const ASTNode* ast_location, const Type* type, ProductMode product_mode) {
  if (!type->HasAnyFields()) {
    if (type->IsStruct()) {
      return MakeSqlErrorAt(ast_location)
             << "Star expansion is not allowed on a struct with zero fields";
    } else if (type->IsProto()) {
      return MakeSqlErrorAt(ast_location)
             << "Star expansion is not allowed on proto "
             << type->AsProto()->descriptor()->full_name()
             << " which has zero fields";
    }
    return MakeSqlErrorAt(ast_location) << "Dot-star is not supported for type "
                                        << type->ShortTypeName(product_mode);
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveSelectDotStar(
    const ASTExpression* ast_dotstar, const NameScope* from_scan_scope,
    QueryResolutionInfo* query_resolution_info) {
  const ASTExpression* ast_expr;
  const ASTStarModifiers* ast_modifiers = nullptr;
  if (ast_dotstar->node_kind() == AST_DOT_STAR) {
    ast_expr = ast_dotstar->GetAsOrDie<ASTDotStar>()->expr();
  } else {
    ZETASQL_RET_CHECK_EQ(ast_dotstar->node_kind(), AST_DOT_STAR_WITH_MODIFIERS);
    const ASTDotStarWithModifiers* ast_with_modifiers =
        ast_dotstar->GetAsOrDie<ASTDotStarWithModifiers>();
    ast_expr = ast_with_modifiers->expr();
    ast_modifiers = ast_with_modifiers->modifiers();
  }

  if (in_strict_mode()) {
    return MakeSqlErrorAt(ast_dotstar)
           << "Dot-star is not allowed in strict name resolution mode";
  }

  // If DotStar expression has exactly one identifier and resolves to a range
  // variable, add the scan columns directly to the select_column_state_list.
  // For anything else, we expect to resolve the lhs as a value that should
  // have type struct or proto.
  // Value table range variables are excluded here because we want to resolve
  // it to a value first and then expand its fields, if possible.
  if (ast_expr->node_kind() == AST_PATH_EXPRESSION) {
    const ASTPathExpression* path_expr =
        ast_expr->GetAsOrDie<ASTPathExpression>();

    if (path_expr->num_names() == 1) {
      NameTarget target;
      CorrelatedColumnsSetList correlated_columns_set_list;
      if (from_scan_scope->LookupName(path_expr->first_name()->GetAsIdString(),
                                      &target, &correlated_columns_set_list) &&
          target.kind() == NameTarget::RANGE_VARIABLE &&
          !target.scan_columns()->is_value_table()) {
        if (target.scan_columns()->num_columns() == 0) {
          return MakeSqlErrorAt(path_expr)
                 << "Dot-star would expand to zero columns";
        }

        // Process .* EXCEPT(...) REPLACE(...) if present.
        ColumnReplacements column_replacements;
        if (ast_modifiers != nullptr) {
          ZETASQL_RETURN_IF_ERROR(ResolveSelectStarModifiers(
              ast_dotstar, ast_modifiers, target.scan_columns().get(),
              nullptr /* type_for_star */, from_scan_scope,
              query_resolution_info, &column_replacements));
        }

        ZETASQL_RETURN_IF_ERROR(AddNameListToSelectList(
            ast_dotstar, target.scan_columns(), correlated_columns_set_list,
            false /* ignore_excluded_value_table_fields */,
            query_resolution_info->select_column_state_list(),
            &column_replacements));

        return absl::OkStatus();
      }
    }
  }

  std::unique_ptr<const ResolvedExpr> resolved_dotstar_expr;
  ExprResolutionInfo expr_resolution_info(from_scan_scope,
                                          query_resolution_info);
  ZETASQL_RETURN_IF_ERROR(
      ResolveExpr(ast_expr, &expr_resolution_info, &resolved_dotstar_expr));
  const Type* source_type = resolved_dotstar_expr->type();

  std::unique_ptr<const ResolvedColumnRef> src_column_ref;
  if (resolved_dotstar_expr->node_kind() == RESOLVED_COLUMN_REF &&
      !resolved_dotstar_expr->GetAs<ResolvedColumnRef>()->is_correlated()) {
    src_column_ref.reset(
        resolved_dotstar_expr.release()->GetAs<ResolvedColumnRef>());
    if (expr_resolution_info.has_analytic) {
      query_resolution_info
          ->dot_star_columns_with_analytic_for_second_pass_resolution()
          ->emplace_back(src_column_ref->column(), ast_expr);
    }
  } else {
    // We resolved the DotStar to be derived from an expression.
    const ResolvedColumn src_column(
        AllocateColumnId(), kPreProjectId,
        resolved_dotstar_expr->type()->IsStruct() ? kStructId : kProtoId,
        resolved_dotstar_expr->annotated_type());

    if (expr_resolution_info.has_analytic) {
      // The DotStar source expression contains analytic functions (and maybe
      // aggregate functions too), so we need to compute this expression after
      // the analytic scan, but before the final project of the SELECT (since
      // the final project of the SELECT will extract all the fields/columns
      // from this source expression).
      query_resolution_info
          ->dot_star_columns_with_analytic_for_second_pass_resolution()
          ->emplace_back(src_column, ast_expr);
    } else if (expr_resolution_info.has_aggregation) {
      // The DotStar source expression contains aggregation (but not analytic)
      // functions, so we need to compute this expression after the aggregation,
      // but before the final project of the SELECT (since the final project of
      // the SELECT will extract all the fields/columns from this source
      // expression).
      query_resolution_info
          ->dot_star_columns_with_aggregation_for_second_pass_resolution()
          ->emplace_back(src_column, ast_expr);
    } else {
      // The dot-star source expression contains neither analytic functions
      // nor aggregate functions, so it must be computed before any aggregation
      // that might be present (the dot-star columns are effectively
      // pre-GROUP BY columns).

      // However, if this dot-star source expression is in DML returning clause,
      // we do not support pre-GROUP BY columns with Project Scan. (b/207519939)
      if (query_resolution_info->is_resolving_returning_clause()) {
        // PROJECT scan for dot-star columns in DML THEN RETURN are not
        // supported.
        return MakeSqlErrorAt(ast_dotstar)
               << "Dot-star is only allowed on range variables and columns in "
                  "THEN RETURN. It cannot be applied on other expressions "
                  "including field access.";
      }

      query_resolution_info->select_list_columns_to_compute_before_aggregation()
          ->emplace_back(MakeResolvedComputedColumn(
              src_column, std::move(resolved_dotstar_expr)));
    }
    src_column_ref = MakeColumnRef(src_column);
  }
  ZETASQL_RET_CHECK(src_column_ref != nullptr);

  ZETASQL_RETURN_IF_ERROR(MakeErrorIfTypeDotStarHasNoFields(ast_dotstar, source_type,
                                                    product_mode()));

  // Process .* EXCEPT(...) REPLACE(...) if present.
  ColumnReplacements column_replacements;
  if (ast_modifiers != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveSelectStarModifiers(
        ast_dotstar, ast_modifiers, nullptr /* name_list_for_star */,
        source_type, from_scan_scope, query_resolution_info,
        &column_replacements));
  }

  const int orig_num_columns =
      query_resolution_info->select_column_state_list()->Size();
  ZETASQL_RETURN_IF_ERROR(AddColumnFieldsToSelectList(
      ast_dotstar, src_column_ref.get(), expr_resolution_info.has_aggregation,
      expr_resolution_info.has_analytic,
      IdString() /* column_alias_if_no_fields */,
      nullptr /* excluded_field_names */,
      query_resolution_info->select_column_state_list(), &column_replacements));

  // Detect if the * ended up expanding to zero columns after applying EXCEPT,
  // and treat that as an error.
  if (orig_num_columns ==
      query_resolution_info->select_column_state_list()->Size()) {
    ZETASQL_RET_CHECK(!column_replacements.excluded_columns.empty());
    return MakeSqlErrorAt(ast_dotstar)
           << "SELECT * expands to zero columns after applying EXCEPT";
  }

  return absl::OkStatus();
}

// NOTE: The behavior of star expansion here must match
// NameList::SelectStarHasColumn.
absl::Status Resolver::AddColumnFieldsToSelectList(
    const ASTExpression* ast_expression,
    const ResolvedColumnRef* src_column_ref, bool src_column_has_aggregation,
    bool src_column_has_analytic, IdString column_alias_if_no_fields,
    const IdStringSetCase* excluded_field_names,
    SelectColumnStateList* select_column_state_list,
    ColumnReplacements* column_replacements) {
  const bool allow_no_fields = !column_alias_if_no_fields.empty();
  const Type* type = src_column_ref->type();

  // Check if the value has no fields because it either has scalar type
  // or is a compound type with zero fields.
  // Value table columns with no fields will expand in SELECT * to the
  // value itself rather than to an empty list of fields.
  if (!type->HasAnyFields()) {
    if (!allow_no_fields) {
      ZETASQL_RETURN_IF_ERROR(MakeErrorIfTypeDotStarHasNoFields(ast_expression, type,
                                                        product_mode()));
    }

    if (ExcludeOrReplaceColumn(ast_expression, column_alias_if_no_fields,
                               column_replacements, select_column_state_list)) {
      return absl::OkStatus();
    }

    // The value doesn't have any fields, but that is allowed here.
    // Just add a ColumnRef directly.
    // is_explicit=false because the column is coming from SELECT *, even if
    // we had an explicit alias for the table.
    // This is not a strict requirement and we could change it.
    select_column_state_list->AddSelectColumn(
        ast_expression, column_alias_if_no_fields, /*is_explicit=*/false,
        src_column_has_aggregation, src_column_has_analytic,
        CopyColumnRef(src_column_ref));
    return absl::OkStatus();
  }

  if (type->IsStruct()) {
    const StructType* struct_type = type->AsStruct();

    for (int field_idx = 0; field_idx < struct_type->num_fields();
         ++field_idx) {
      const auto& field = struct_type->field(field_idx);
      const IdString field_name = MakeIdString(
          (field.name.empty()) ? absl::StrCat("$field", 1 + field_idx)
                               : field.name);

      if ((excluded_field_names != nullptr &&
           zetasql_base::ContainsKey(*excluded_field_names, field_name)) ||
          ExcludeOrReplaceColumn(ast_expression, field_name,
                                 column_replacements,
                                 select_column_state_list)) {
        continue;
      }

      auto get_struct_field = MakeResolvedGetStructField(
          field.type, CopyColumnRef(src_column_ref), field_idx);
      ZETASQL_RETURN_IF_ERROR(CheckAndPropagateAnnotations(
          /*error_node=*/nullptr, get_struct_field.get()));
      // is_explicit=false because we're extracting all fields of a struct.
      select_column_state_list->AddSelectColumn(
          ast_expression, field_name,
          /*is_explicit=*/false, src_column_has_aggregation,
          src_column_has_analytic, std::move(get_struct_field));
    }
  } else {
    const google::protobuf::Descriptor* proto_descriptor = type->AsProto()->descriptor();

    std::map<int32_t, const google::protobuf::FieldDescriptor*>
        tag_number_ordered_field_map;
    for (int proto_idx = 0; proto_idx < proto_descriptor->field_count();
         ++proto_idx) {
      const google::protobuf::FieldDescriptor* field = proto_descriptor->field(proto_idx);
      const IdString field_name = MakeIdString(field->name());
      if (excluded_field_names != nullptr &&
          zetasql_base::ContainsKey(*excluded_field_names, field_name)) {
        continue;
      }
      ZETASQL_RET_CHECK(
          zetasql_base::InsertIfNotPresent(&tag_number_ordered_field_map,
                                  std::make_pair(field->number(), field)));
    }

    for (const auto& entry : tag_number_ordered_field_map) {
      const google::protobuf::FieldDescriptor* field = entry.second;

      const IdString field_name = MakeIdString(field->name());
      if (ExcludeOrReplaceColumn(ast_expression, field_name,
                                 column_replacements,
                                 select_column_state_list)) {
        continue;
      }

      const Type* field_type;
      Value default_value;
      RETURN_SQL_ERROR_AT_IF_ERROR(
          ast_expression,
          GetProtoFieldTypeAndDefault(
              ProtoFieldDefaultOptions::FromFieldAndLanguage(field, language()),
              field, type_factory_, &field_type, &default_value));
      // TODO: This really should be check for
      // !field_type->IsSupportedType(language())
      // but that breaks existing tests :(
      if (field_type->UsingFeatureV12CivilTimeType() &&
          !language().LanguageFeatureEnabled(FEATURE_V_1_2_CIVIL_TIME)) {
        return MakeSqlErrorAt(ast_expression)
               << "Dot-star expansion includes field " << field_name
               << " with unsupported type "
               << field_type->TypeName(language().product_mode());
      }
      std::unique_ptr<const ResolvedExpr> resolved_expr =
          MakeResolvedGetProtoField(
              field_type, CopyColumnRef(src_column_ref), field, default_value,
              /*get_has_bit=*/false, ProtoType::GetFormatAnnotation(field),
              /*return_default_value_when_unset=*/false);
      // is_explicit=false because we're extracting all fields of a proto.
      select_column_state_list->AddSelectColumn(
          ast_expression, field_name, /*is_explicit=*/false,
          src_column_has_aggregation, src_column_has_analytic,
          std::move(resolved_expr));
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveSelectColumnFirstPass(
    const ASTSelectColumn* ast_select_column, const NameScope* from_scan_scope,
    const std::shared_ptr<const NameList>& from_clause_name_list,
    int ast_select_column_idx, bool has_from_clause,
    QueryResolutionInfo* query_resolution_info) {

  const ASTExpression* ast_select_expr = ast_select_column->expression();
  switch (ast_select_expr->node_kind()) {
    case AST_STAR:
    case AST_STAR_WITH_MODIFIERS:
      return ResolveSelectStar(ast_select_expr, from_clause_name_list,
                               from_scan_scope, has_from_clause,
                               query_resolution_info);
    case AST_DOT_STAR:
    case AST_DOT_STAR_WITH_MODIFIERS:
      return ResolveSelectDotStar(ast_select_expr, from_scan_scope,
                                  query_resolution_info);
    default:
      break;
  }

  IdString select_column_alias =
      ComputeSelectColumnAlias(ast_select_column, ast_select_column_idx);
  // Save stack space for nested SELECT list subqueries.
  std::unique_ptr<ExprResolutionInfo> expr_resolution_info(
      new ExprResolutionInfo(from_scan_scope, query_resolution_info,
                             ast_select_expr, select_column_alias));
  std::unique_ptr<const ResolvedExpr> resolved_expr;
  ZETASQL_RETURN_IF_ERROR(
      ResolveExpr(ast_select_expr, expr_resolution_info.get(), &resolved_expr));
  // We can set is_explicit=true unconditionally because this either came
  // from an AS alias or from a path in the query, or it's an internal name
  // for an anonymous column (that can't be looked up).
  query_resolution_info->select_column_state_list()->AddSelectColumn(
      ast_select_expr, select_column_alias, /*is_explicit=*/true,
      expr_resolution_info->has_aggregation, expr_resolution_info->has_analytic,
      std::move(resolved_expr));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveSelectListExprsFirstPass(
    const ASTSelectList* select_list, const NameScope* from_scan_scope,
    bool has_from_clause,
    const std::shared_ptr<const NameList>& from_clause_name_list,
    QueryResolutionInfo* query_resolution_info) {
  for (int i = 0; i < select_list->columns().size(); ++i) {
    ZETASQL_RETURN_IF_ERROR(ResolveSelectColumnFirstPass(
        select_list->columns(i), from_scan_scope, from_clause_name_list, i,
        has_from_clause, query_resolution_info));
  }
  return absl::OkStatus();
}

absl::Status Resolver::ValidateAndResolveOrderByCollate(
    const ASTCollate* ast_collate, const ASTNode* ast_order_by_item_location,
    const Type* order_by_item_column,
    std::unique_ptr<const ResolvedExpr>* resolved_collate) {
  if (!language().LanguageFeatureEnabled(FEATURE_V_1_1_ORDER_BY_COLLATE)) {
    return MakeSqlErrorAt(ast_collate) << "COLLATE is not supported";
  }
  if (!order_by_item_column->IsString()) {
    return MakeSqlErrorAt(ast_order_by_item_location)
           << "COLLATE can only be applied to expressions of type "
              "STRING, but was applied to "
           << order_by_item_column->ShortTypeName(product_mode());
  }
  return ResolveCollate(ast_collate, resolved_collate);
}

absl::Status Resolver::ResolveOrderingExprs(
    const absl::Span<const ASTOrderingExpression* const> ordering_expressions,
    ExprResolutionInfo* expr_resolution_info,
    std::vector<OrderByItemInfo>* order_by_info) {
  for (const ASTOrderingExpression* order_by_expression :
       ordering_expressions) {
    ResolvedOrderByItemEnums::NullOrderMode null_order =
        ResolvedOrderByItemEnums::ORDER_UNSPECIFIED;
    if (order_by_expression->null_order() != nullptr) {
      if (!language().LanguageFeatureEnabled(
              FEATURE_V_1_3_NULLS_FIRST_LAST_IN_ORDER_BY)) {
        return MakeSqlErrorAt(order_by_expression->null_order())
               << "NULLS FIRST and NULLS LAST are not supported";
      } else {
        null_order = order_by_expression->null_order()->nulls_first()
                         ? ResolvedOrderByItemEnums::NULLS_FIRST
                         : ResolvedOrderByItemEnums::NULLS_LAST;
      }
    }
    std::unique_ptr<const ResolvedExpr> resolved_order_expression;
    ZETASQL_RETURN_IF_ERROR(ResolveExpr(order_by_expression->expression(),
                                expr_resolution_info,
                                &resolved_order_expression));

    // If the expression was an integer literal, remember that mapping.
    if (resolved_order_expression->node_kind() == RESOLVED_LITERAL &&
        !resolved_order_expression->GetAs<ResolvedLiteral>()
             ->has_explicit_type()) {
      const Value& value =
          resolved_order_expression->GetAs<ResolvedLiteral>()->value();
      if (value.type_kind() == TYPE_INT64 && !value.is_null()) {
        if (value.int64_value() < 1) {
          return MakeSqlErrorAt(order_by_expression)
                 << "ORDER BY column number item is out of range. "
                 << "Column numbers must be greater than or equal to one. "
                 << "Found : " << value.int64_value();
        }
        const int64_t int_value = value.int64_value() - 1;  // Make it 0-based.
        order_by_info->emplace_back(order_by_expression, int_value,
                                    order_by_expression->descending(),
                                    null_order);
      } else {
        return MakeSqlErrorAt(order_by_expression)
               << "Cannot ORDER BY literal values";
      }
      resolved_order_expression.reset();  // No longer needed.
    } else {
      order_by_info->emplace_back(
          order_by_expression, std::move(resolved_order_expression),
          order_by_expression->descending(), null_order);
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::HandleGroupBySelectColumn(
    const SelectColumnState* group_by_column_state,
    QueryResolutionInfo* query_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr,
    ResolvedColumn* group_by_column) {
  // If this SELECT list column is already being grouped by then we should
  // not be calling this.
  ZETASQL_RET_CHECK(!group_by_column_state->is_group_by_column);

  // We are grouping by either a SELECT list ordinal or alias so we must
  // update the SelectColumnState to reflect it is being grouped by.
  // We need a mutable version of the SelectColumnState since we will
  // be updating its expression and other information.
  SelectColumnState* select_column_state =
      const_cast<SelectColumnState*>(group_by_column_state);

  // Move the expression from the SelectColumnState to the
  // group_by_columns list, and update the SelectColumnState to reference
  // the associated group by column.
  ZETASQL_RET_CHECK(select_column_state->resolved_expr != nullptr)
      << select_column_state->DebugString();

  const ResolvedComputedColumn* existing_computed_column =
      query_resolution_info->GetEquivalentGroupByComputedColumnOrNull(
          select_column_state->resolved_expr.get());
  if (existing_computed_column != nullptr) {
    // Make a reference to the existing column rather than recomputing the
    // expression.
    *group_by_column = existing_computed_column->column();
  } else {
    *group_by_column = ResolvedColumn(
        AllocateColumnId(), kGroupById, select_column_state->alias,
        select_column_state->resolved_expr->annotated_type());
  }

  *resolved_expr = std::move(select_column_state->resolved_expr);
  select_column_state->resolved_expr = MakeColumnRef(*group_by_column);
  select_column_state->is_group_by_column = true;
  // Update the SelectColumnState to reflect the grouped by version of
  // the column.
  select_column_state->resolved_select_column = *group_by_column;

  // If the 'resolved_expr' is a path expression, we must collect
  // information in the 'query_resolution_info' about that path
  // expression and its relationship to the 'group_by_column'.
  // This information will get used later when constructing a
  // NameScope for what names are valid post-GROUP BY, where the
  // new NameScope is used when resolving subsequent expressions.
  // If the 'resolved_expr' is not a path expression then this
  // is a no-op.
  ZETASQL_RETURN_IF_ERROR(CollectResolvedPathExpressionInfoIfRelevant(
      query_resolution_info, resolved_expr->get(), *group_by_column));

  return absl::OkStatus();
}

absl::Status Resolver::HandleGroupByExpression(
    const ASTExpression* ast_group_by_expr,
    QueryResolutionInfo* query_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr,
    ResolvedColumn* group_by_column) {
  // We're grouping by an expression that was not a SELECT list alias
  // or ordinal.
  ZETASQL_RET_CHECK(resolved_expr != nullptr && (*resolved_expr) != nullptr);

  if ((*resolved_expr)->node_kind() == RESOLVED_LITERAL &&
      !(*resolved_expr)->GetAs<ResolvedLiteral>()->has_explicit_type()) {
    return MakeSqlErrorAt(ast_group_by_expr)
           << "Cannot GROUP BY literal values";
  }

  // This expression might match one that is already going to be
  // precomputed before the GROUP BY.  If so, then set <group_by_column>
  // to it and update the expression to be a simple column reference.
  // For instance, consider:
  //   SELECT k.col1
  //   FROM valuetable k
  //   GROUP BY k.col1
  // This code is needed to detect that the GROUP BY k.col1 expression
  // is the same thing as the k.col1 that we are precomputing before the
  // GROUP BY, so we can re-use the same column and avoid an additional
  // expression evaluation.
  bool found_precomputed_expression = false;
  for (const std::unique_ptr<const ResolvedComputedColumn>& computed_column :
       *query_resolution_info
            ->select_list_columns_to_compute_before_aggregation()) {
    if (IsSameFieldPath(resolved_expr->get(), computed_column->expr(),
                        FieldPathMatchingOption::kExpression)) {
      *group_by_column = computed_column->column();
      found_precomputed_expression = true;
      break;
    }
  }
  if (found_precomputed_expression) {
    *resolved_expr = MakeColumnRef(*group_by_column);
  }
  const ResolvedComputedColumn* existing_computed_column =
      query_resolution_info->GetEquivalentGroupByComputedColumnOrNull(
          (*resolved_expr).get());
  if (existing_computed_column != nullptr) {
    // Make a reference to the existing column rather than recomputing the
    // expression.
    *group_by_column = existing_computed_column->column();
  } else {
    IdString alias = GetAliasForExpression(ast_group_by_expr);
    if (alias.empty()) {
      alias = MakeIdString(absl::StrCat(
          "$groupbycol",
          query_resolution_info->group_by_columns_to_compute().size() + 1));
    }
    *group_by_column = ResolvedColumn(AllocateColumnId(), kGroupById, alias,
                                      (*resolved_expr)->annotated_type());
  }

  // If the 'resolved_expr' is a path expression, we must collect
  // information in the 'query_resolution_info' about that path
  // expression and its relationship to the 'group_by_column'.
  // This information will get used later when constructing a
  // NameScope for what names are valid post-GROUP BY, where the
  // new NameScope is used when resolving subsequent expressions.
  // If the 'resolved_expr' is not a path expression then this
  // is a no-op.
  ZETASQL_RETURN_IF_ERROR(CollectResolvedPathExpressionInfoIfRelevant(
      query_resolution_info, resolved_expr->get(), *group_by_column));

  return absl::OkStatus();
}

// Analyze the GROUP BY expressions.  Map SELECT list ordinal and
// alias references to the appropriate SelectColumnState in
// <query_resolution_info>, and resolve other expressions against
// the <from_clause_scope>.  For GROUP BY expressions that
// are SELECT list items, update the related SelectColumnState
// information with the grouped version of the column.  Updates
// query_resolution_info with the GROUP BY ResolvedColumns and
// ResolvedComputedColumns.
//
// Note that the current logic will not allow the use of SELECT list
// aliases within GROUP BY expressions, because we only match aliases
// exactly.  For example, the following is not supported:
//
//   select a+b as foo
//   ...
//   group by foo + 1;
//
// However, in most cases these types of queries would be invalid even
// without this restriction anyway since the SELECT list expression would
// not be something that was grouped by.  But it is possible to have a
// valid example (though highly contrived), such as:
//
//   select a+b as foo
//   ...
//   group by foo, foo + 1
//
// The general logic implemented by this function for each GROUP BY column is:
// 1) Determine if the GROUP BY expression exactly matches a SELECT list alias
//    or is an integer literal (representing a SELECT list column ordinal)
// 2) If either, update the corresponding SelectColumnState
// 3) If neither, resolve the expression against the <from_clause_scope>
// 4) Provide an error if grouping by literals, constant expressions, structs,
//    protos, or arrays.
// 5) If grouping by a column, update the mapping from pre-GROUP BY column to
//    post-GROUP BY column.
// 6) If grouping by an expression, update the mapping from the pre-GROUP BY
//    expression to the post-GROUP BY column.
// 7) Add a ResolvedComputedColumn for the GROUP BY expression.
absl::Status Resolver::ResolveGroupByExprs(
    const ASTGroupBy* group_by, const NameScope* from_clause_scope,
    QueryResolutionInfo* query_resolution_info) {
  // Check whether this is a GROUP BY ROLLUP. Only a single ROLLUP with no
  // other items in the list is supported.
  std::vector<const ASTExpression*> grouping_expressions;
  bool is_rollup = false;
  if (group_by->grouping_items().size() == 1 &&
      group_by->grouping_items()[0]->rollup() != nullptr) {
    const ASTRollup* rollup = group_by->grouping_items()[0]->rollup();
    if (!language().LanguageFeatureEnabled(FEATURE_GROUP_BY_ROLLUP)) {
      return MakeSqlErrorAt(rollup) << "GROUP BY ROLLUP is unsupported";
    }
    const absl::Span<const ASTExpression* const>& expressions =
        rollup->expressions();
    grouping_expressions.assign(expressions.begin(), expressions.end());
    is_rollup = true;
  } else {
    // Ensure that there is no ROLLUP in the list, and build the list of
    // expressions.
    grouping_expressions.reserve(group_by->grouping_items().size());
    for (const ASTGroupingItem* ast_grouping_item :
         group_by->grouping_items()) {
      if (ast_grouping_item->rollup() != nullptr) {
        if (!language().LanguageFeatureEnabled(FEATURE_GROUP_BY_ROLLUP)) {
          return MakeSqlErrorAt(ast_grouping_item->rollup())
                 << "GROUP BY ROLLUP is unsupported";
        } else {
          return MakeSqlErrorAt(ast_grouping_item->rollup())
                 << "The GROUP BY clause only supports ROLLUP when there are "
                    "no other grouping elements";
        }
      }
      ZETASQL_RET_CHECK(ast_grouping_item->expression() != nullptr);
      grouping_expressions.push_back(ast_grouping_item->expression());
    }
  }

  // Populate the group by list in <query_resolution_info>.
  for (const ASTExpression* ast_group_by_expr : grouping_expressions) {
    ZETASQL_RET_CHECK(ast_group_by_expr != nullptr);

    ExprResolutionInfo no_aggregation(from_clause_scope, "GROUP BY");

    ZETASQL_DCHECK_NE(ast_group_by_expr->node_kind(), AST_IDENTIFIER)
        << "We expect to get PathExpressions, not Identifiers here";

    const SelectColumnState* group_by_column_state = nullptr;
    // Determine if the GROUP BY expression exactly matches a SELECT list alias.
    if (ast_group_by_expr->node_kind() == AST_PATH_EXPRESSION) {
      const IdString alias = ast_group_by_expr->GetAsOrDie<ASTPathExpression>()
                                 ->first_name()
                                 ->GetAsIdString();
      ZETASQL_RETURN_IF_ERROR(query_resolution_info->select_column_state_list()
                          ->FindAndValidateSelectColumnStateByAlias(
                              "GROUP BY clause" /* clause_name */,
                              ast_group_by_expr, alias, &no_aggregation,
                              &group_by_column_state));
      if (group_by_column_state != nullptr &&
          ast_group_by_expr->GetAsOrDie<ASTPathExpression>()->num_names() !=
              1) {
        // We resolved the first identifier in a path expression to a SELECT
        // list alias.  There is currently no way that accessing the column's
        // fields in the GROUP BY can possibly be valid.  Consider:
        //   SELECT foo as foo2
        //   FROM (select as struct 1 as a, 2 as b) foo
        //   GROUP BY foo2.a
        // This is invalid since 'foo' is in the SELECT list but it is not
        // in the GROUP BY.
        //
        // If we add foo2 to the GROUP BY, then the query is invalid since
        // we do not allow grouping by STRUCT.
        //
        // If we were to allow grouping by PROTO or STRUCT then the following
        // query could be valid, but at this time it is not valid:
        //   SELECT foo as foo2
        //   FROM (select as struct 1 as a, 2 as b) foo
        //   GROUP BY foo, foo.a, foo.b;
        return MakeSqlErrorAt(ast_group_by_expr)
               << "Cannot GROUP BY field references from SELECT list alias "
               << alias;
      }
    }

    std::unique_ptr<const ResolvedExpr> resolved_expr;
    if (group_by_column_state == nullptr) {
      ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(ast_group_by_expr, from_clause_scope,
                                        "GROUP BY", &resolved_expr));

      // Determine if the GROUP BY expression is an integer literal
      // representing a SELECT list column ordinal.  Look for GROUP BY 1,2,3.
      if (resolved_expr->node_kind() == RESOLVED_LITERAL &&
          !resolved_expr->GetAs<ResolvedLiteral>()->has_explicit_type()) {
        const Value& value = resolved_expr->GetAs<ResolvedLiteral>()->value();
        if (value.type_kind() == TYPE_INT64 && !value.is_null()) {
          ZETASQL_RETURN_IF_ERROR(query_resolution_info->select_column_state_list()
                              ->FindAndValidateSelectColumnStateByOrdinal(
                                  "GROUP BY" /* expr_description */,
                                  ast_group_by_expr, value.int64_value(),
                                  &no_aggregation, &group_by_column_state));
        }
      }
    }

    ResolvedColumn group_by_column;
    if (group_by_column_state != nullptr) {
      if (group_by_column_state->is_group_by_column) {
        // We are already grouping by this SELECT list column, so we do not need
        // to do more unless the query uses GROUP BY ROLLUP, in which case we
        // need to add another entry in the rollup list for it.
        if (!is_rollup) {
          continue;
        }

        const ResolvedComputedColumn* existing_computed_column = nullptr;
        for (const std::unique_ptr<const ResolvedComputedColumn>&
                 group_by_column :
             query_resolution_info->group_by_columns_to_compute()) {
          if (group_by_column_state->resolved_select_column ==
              group_by_column->column()) {
            existing_computed_column = group_by_column.get();
            break;
          }
        }

        ZETASQL_RET_CHECK_NE(existing_computed_column, nullptr)
            << "Expected to find existing group by column matching "
            << group_by_column_state->resolved_select_column.DebugString();
        // Field paths may repeat inside the rollup list. We have already
        // resolved this field path, so just add another entry for it.
        query_resolution_info->AddRollupColumn(existing_computed_column);
        continue;
      }
      ZETASQL_RETURN_IF_ERROR(HandleGroupBySelectColumn(
          group_by_column_state, query_resolution_info, &resolved_expr,
          &group_by_column));
    } else {
      ZETASQL_RETURN_IF_ERROR(
          HandleGroupByExpression(ast_group_by_expr, query_resolution_info,
                                  &resolved_expr, &group_by_column));
    }
    ZETASQL_RET_CHECK(resolved_expr != nullptr);

    // Cannot GROUP BY proto, array, or struct.
    std::string no_grouping_type;
    if (!TypeSupportsGrouping(resolved_expr->type(), &no_grouping_type)) {
      return MakeSqlErrorAt(ast_group_by_expr)
             << "Grouping by expressions of type " << no_grouping_type
             << " is not allowed";
    }

    const ResolvedComputedColumn* computed_column =
        query_resolution_info->AddGroupByComputedColumnIfNeeded(
            group_by_column, std::move(resolved_expr));
    if (is_rollup) {
      query_resolution_info->AddRollupColumn(computed_column);
    }
  }

  return absl::OkStatus();
}

// Performs a second pass of analysis over a SELECT list column,
// (re)evaluating the expression if necessary.
//
// SELECT list expressions that already have a ResolvedColumn assigned
// are not re-resolved.  Those that do not are re-resolved against the
// post-GROUP BY NameScope <group_by_scope> (in this case the ResolvedExpr
// from the first pass is deleted).
//
// Aggregate expressions that were resolved in the first pass are not
// re-resolved, but use the ResolvedExpr from the first pass.
//
// All necessary computed columns are created and assigned to the relevant
// computed column list (dot-star computed columns, and computed columns that
// can be referenced by GROUP BY/etc.).
//
// After this pass, all SELECT list columns have initialized output
// ResolvedColumns.
absl::Status Resolver::ResolveSelectColumnSecondPass(
    IdString query_alias, const NameScope* group_by_scope,
    SelectColumnState* select_column_state,
    std::shared_ptr<NameList>* final_project_name_list,
    QueryResolutionInfo* query_resolution_info) {
  if (!select_column_state->resolved_select_column.IsInitialized()) {
    // If we have not already fully resolved this SELECT list expression
    // to a ResolvedColumn, then resolve the SELECT list expression.

    // First, look for SELECT list columns that resulted from star
    // expansion.  Star expansion was already performed in
    // ResolveSelectColumnFirstPass().   It is not done here.  The
    // star-expanded columns are often simple column references, but
    // could be GET_PROTO_FIELD or GET_STRUCT_FIELD if a FROM clause
    // subquery is SELECT AS PROTO or SELECT AS STRUCT.
    if (select_column_state->ast_expr->node_kind() == AST_DOT_STAR ||
        select_column_state->ast_expr->node_kind() ==
            AST_DOT_STAR_WITH_MODIFIERS ||
        select_column_state->ast_expr->node_kind() == AST_STAR ||
        select_column_state->ast_expr->node_kind() == AST_STAR_WITH_MODIFIERS) {
      if (select_column_state->resolved_expr->node_kind() ==
              RESOLVED_COLUMN_REF &&
          !select_column_state->resolved_expr->GetAs<ResolvedColumnRef>()
               ->is_correlated()) {
        // We already have column references after star expansion.  Do not
        // re-resolve the expression.  Just update the SelectColumnState
        // to reflect the associated ResolvedColumn.
        if (query_resolution_info->HasGroupByOrAggregation()) {
          // If this column is part of the grouping key, mark the post-grouping
          // column ref as such.
          const ResolvedExpr* const original_resolved_expr =
              select_column_state->resolved_expr.get();
          ZETASQL_RETURN_IF_ERROR(ResolveColumnRefExprToPostGroupingColumn(
              select_column_state->ast_expr, "Star expansion" /* clause_name */,
              query_resolution_info, &select_column_state->resolved_expr));

          if (original_resolved_expr !=
              select_column_state->resolved_expr.get()) {
            // <select_column_state->resolved_expr> has been replaced with the
            // post-grouping column.  This indicates that this column is part of
            // the grouping key.
            ZETASQL_RET_CHECK_EQ(RESOLVED_COLUMN_REF,
                         select_column_state->resolved_expr->node_kind());
            select_column_state->is_group_by_column = true;
          }
        }
        select_column_state->resolved_select_column =
            select_column_state->resolved_expr->GetAs<ResolvedColumnRef>()
                ->column();
      } else {
        ResolvedColumn select_column(
            AllocateColumnId(), query_alias, select_column_state->alias,
            select_column_state->resolved_expr->annotated_type());
        query_resolution_info->select_list_columns_to_compute()->push_back(
            MakeResolvedComputedColumn(
                select_column, std::move(select_column_state->resolved_expr)));
        select_column_state->resolved_computed_column =
            query_resolution_info->select_list_columns_to_compute()
                ->back()
                .get();
        // Update the SelectColumnState to reflect the associated
        // ResolvedColumn.
        select_column_state->resolved_select_column = select_column;
      }
      // Check if the source of the '*' is a correlated column reference,
      // i.e., 'outercol.*'.
      bool is_correlated_column_ref = false;
      if (select_column_state->resolved_computed_column != nullptr &&
          select_column_state->resolved_computed_column->expr()->node_kind() ==
              RESOLVED_COLUMN_REF &&
          select_column_state->resolved_computed_column->expr()
              ->GetAs<ResolvedColumnRef>()
              ->is_correlated()) {
        is_correlated_column_ref = true;
      }
      // If the query has grouping or aggregation, then check the
      // *-expansion columns to ensure that they are either outer correlation
      // references or they are being grouped by.  If not, then produce
      // an error.
      //
      // We do not do this check for columns with aggregation or analytic
      // functions because those column expressions will be re-resolved
      // against the post-GROUP BY NameScope and errors will be detected then.
      if (query_resolution_info->HasGroupByOrAggregation() &&
          !select_column_state->is_group_by_column &&
          !is_correlated_column_ref && !select_column_state->has_aggregation &&
          !select_column_state->has_analytic) {
        return MakeSqlErrorAt(select_column_state->ast_expr)
               << "Star expansion expression references column "
               << select_column_state->alias
               << " which is neither grouped nor aggregated";
      }
    } else {
      ExprResolutionInfo expr_resolution_info(
          group_by_scope, group_by_scope, true /* allows_aggregation */,
          true /* allows_analytic */,
          query_resolution_info->HasGroupByOrAggregation(), "SELECT list",
          query_resolution_info, select_column_state->ast_expr,
          select_column_state->alias);
      std::unique_ptr<const ResolvedExpr> resolved_expr;
      const absl::Status resolve_expr_status = ResolveExpr(
          select_column_state->ast_expr, &expr_resolution_info, &resolved_expr);
      if (!resolve_expr_status.ok() &&
          select_column_state->resolved_expr != nullptr) {
        // Look at the QueryResolutionInfo to see if there is a GROUP BY
        // expression that exactly matches the ResolvedExpr from the
        // first pass resolution.
        bool found_group_by_expression = false;
        for (const std::unique_ptr<const ResolvedComputedColumn>&
                 resolved_computed_column :
             query_resolution_info->group_by_columns_to_compute()) {
          ZETASQL_ASSIGN_OR_RETURN(bool is_same_expr,
                           IsSameExpressionForGroupBy(
                               select_column_state->resolved_expr.get(),
                               resolved_computed_column->expr()));
          if (is_same_expr) {
            // We matched this SELECT list expression to a GROUP BY
            // expression.
            // Update the select_column_state to point at the GROUP BY
            // computed column.
            select_column_state->resolved_select_column =
                resolved_computed_column->column();
            found_group_by_expression = true;
            break;
          }
        }
        if (!found_group_by_expression) {
          // TODO: Improve error message to say that expressions didn't
          // match.
          ZETASQL_RETURN_IF_ERROR(resolve_expr_status);
        }
      } else if (resolved_expr->node_kind() == RESOLVED_COLUMN_REF &&
                 !resolved_expr->GetAs<ResolvedColumnRef>()->is_correlated()) {
        // The expression was already resolved to a column.  If it was not
        // correlated, just use the column.
        const ResolvedColumn& select_column =
            resolved_expr->GetAs<ResolvedColumnRef>()->column();
        select_column_state->resolved_select_column = select_column;
      } else {
        ResolvedColumn select_column(AllocateColumnId(), query_alias,
                                     select_column_state->alias,
                                     resolved_expr->annotated_type());
        std::unique_ptr<ResolvedComputedColumn> computed_column =
            MakeResolvedComputedColumn(select_column, std::move(resolved_expr));
        query_resolution_info->select_list_columns_to_compute()->push_back(
            std::move(computed_column));
        select_column_state->resolved_select_column = select_column;
      }
    }
  }

  return (*final_project_name_list)
      ->AddColumn(select_column_state->alias,
                  select_column_state->resolved_select_column,
                  select_column_state->is_explicit);
}

absl::Status Resolver::ResolveSelectListExprsSecondPass(
    IdString query_alias, const NameScope* group_by_scope,
    std::shared_ptr<NameList>* final_project_name_list,
    QueryResolutionInfo* query_resolution_info) {
  SelectColumnStateList* select_column_state_list =
      query_resolution_info->select_column_state_list();

  for (const std::unique_ptr<SelectColumnState>& select_column_state :
       select_column_state_list->select_column_state_list()) {
    ZETASQL_RETURN_IF_ERROR(ResolveSelectColumnSecondPass(
        query_alias, group_by_scope, select_column_state.get(),
        final_project_name_list, query_resolution_info));

    // Some sanity checks.
    ZETASQL_RET_CHECK(select_column_state->GetType() != nullptr);
    ZETASQL_RET_CHECK(select_column_state->resolved_select_column.IsInitialized());
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveSelectAs(
    const ASTSelectAs* select_as,
    const SelectColumnStateList& select_column_state_list,
    std::unique_ptr<const ResolvedScan> input_scan,
    const NameList* input_name_list,
    std::unique_ptr<const ResolvedScan>* output_scan,
    std::shared_ptr<const NameList>* output_name_list) {
  if (select_as->is_select_as_struct()) {
    // Convert to an anonymous struct type.
    return ConvertScanToStruct(select_as, nullptr /* named_struct_type */,
                               std::move(input_scan), input_name_list,
                               output_scan, output_name_list);
  } else if (select_as->is_select_as_value()) {
    // For SELECT AS VALUE, we just check that the input has exactly one
    // column, and then build a new NameList with is_value_table true.
    if (input_name_list->num_columns() != 1) {
      return MakeSqlErrorAt(select_as)
             << "SELECT AS VALUE query must have exactly one column";
    }
    std::unique_ptr<NameList> name_list(new NameList);
    ZETASQL_RETURN_IF_ERROR(name_list->AddColumn(kValueColumnId,
                                         input_name_list->column(0).column,
                                         false /* is_explicit */));
    name_list->set_is_value_table(true);
    *output_name_list = std::move(name_list);
    *output_scan = std::move(input_scan);
    return absl::OkStatus();
  } else {
    ZETASQL_DCHECK(select_as->type_name() != nullptr);

    const Type* type;
    ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsType(
        select_as->type_name(), false /* is_single_identifier */, &type));

    if (type->IsStruct()) {
      // Convert to a named struct type.
      return ConvertScanToStruct(select_as->type_name(), type->AsStruct(),
                                 std::move(input_scan), input_name_list,
                                 output_scan, output_name_list);
    } else if (type->IsProto()) {
      return ConvertScanToProto(select_as->type_name(),
                                select_column_state_list, type->AsProto(),
                                std::move(input_scan), input_name_list,
                                output_scan, output_name_list);
    } else if (product_mode() == PRODUCT_EXTERNAL) {
      return MakeSqlErrorAt(select_as->type_name())
             << "SELECT AS TypeName can only be used for type STRUCT";
    } else {
      return MakeSqlErrorAt(select_as->type_name())
             << "SELECT AS TypeName can only be used for STRUCT or PROTO "
                "types, but "
             << select_as->type_name()->ToIdentifierPathString() << " has type "
             << type->ShortTypeName(product_mode());
    }
  }
}

absl::Status Resolver::ConvertScanToStruct(
    const ASTNode* ast_location,
    const StructType* named_struct_type,  // May be NULL.
    std::unique_ptr<const ResolvedScan> input_scan,
    const NameList* input_name_list,
    std::unique_ptr<const ResolvedScan>* output_scan,
    std::shared_ptr<const NameList>* output_name_list) {
  if (named_struct_type != nullptr) {
    // TODO Implement named struct construction - match fields
    // by name and verify types, with coercion.
    return MakeSqlErrorAt(ast_location)
           << "Constructing named STRUCT types in subqueries not implemented "
              "yet";
  }

  std::unique_ptr<ResolvedComputedColumn> computed_column;
  const CorrelatedColumnsSetList correlated_columns_set_list;
  ZETASQL_RETURN_IF_ERROR(CreateStructFromNameList(
      input_name_list, correlated_columns_set_list, &computed_column));

  const ResolvedColumn& struct_column = computed_column->column();
  NameList* mutable_name_list;
  output_name_list->reset((mutable_name_list = new NameList));
  // is_explicit=false because the created column is always anonymous.
  ZETASQL_RET_CHECK(IsInternalAlias(struct_column.name()));
  ZETASQL_RETURN_IF_ERROR(mutable_name_list->AddColumn(
      struct_column.name_id(), struct_column, false /* is_explicit */));
  // Make the output table a value table.
  mutable_name_list->set_is_value_table(true);

  *output_scan = MakeResolvedProjectScan(
      std::vector<ResolvedColumn>{struct_column},
      MakeNodeVector(std::move(computed_column)), std::move(input_scan));
  return absl::OkStatus();
}

absl::Status Resolver::CreateStructFromNameList(
    const NameList* name_list,
    const CorrelatedColumnsSetList& correlated_column_sets,
    std::unique_ptr<ResolvedComputedColumn>* computed_column) {
  ZETASQL_RET_CHECK(computed_column != nullptr);
  ZETASQL_RET_CHECK(*computed_column == nullptr);

  std::vector<std::unique_ptr<const ResolvedExpr>> field_exprs;
  std::vector<StructType::StructField> fields;

  for (const auto& named_column : name_list->columns()) {
    // Internal aliases mean the column has no visible name, so we make a
    // struct with an anonymous field.
    fields.emplace_back(
        IsInternalAlias(named_column.name) ? "" : named_column.name.ToString(),
        named_column.column.type());
    field_exprs.emplace_back(MakeColumnRefWithCorrelation(
        named_column.column, correlated_column_sets));
  }
  const StructType* struct_type;
  ZETASQL_RETURN_IF_ERROR(type_factory_->MakeStructType(fields, &struct_type));

  auto make_struct =
      MakeResolvedMakeStruct(struct_type, std::move(field_exprs));

  ZETASQL_RETURN_IF_ERROR(CheckAndPropagateAnnotations(
      /*error_node=*/nullptr, make_struct.get()));
  const ResolvedColumn struct_column(AllocateColumnId(), kMakeStructId,
                                     kStructId, make_struct->annotated_type());
  *computed_column =
      MakeResolvedComputedColumn(struct_column, std::move(make_struct));
  return absl::OkStatus();
}

absl::Status Resolver::ConvertScanToProto(
    const ASTNode* ast_type_location,
    const SelectColumnStateList& select_column_state_list,
    const ProtoType* proto_type, std::unique_ptr<const ResolvedScan> input_scan,
    const NameList* input_name_list,
    std::unique_ptr<const ResolvedScan>* output_scan,
    std::shared_ptr<const NameList>* output_name_list) {
  ZETASQL_RET_CHECK_EQ(select_column_state_list.Size(), input_name_list->num_columns());

  std::vector<ResolvedBuildProtoArg> arguments;
  for (int i = 0; i < input_name_list->num_columns(); ++i) {
    const ASTNode* ast_column_location =
        select_column_state_list.GetSelectColumnState(i)->ast_expr;
    const NamedColumn& named_column = input_name_list->column(i);
    if (IsInternalAlias(named_column.name)) {
      return MakeSqlErrorAt(ast_column_location)
             << "Cannot construct PROTO from query result because column "
             << (i + 1) << " has no name";
    }

    std::unique_ptr<ResolvedColumnRef> expr =
        MakeColumnRef(named_column.column);
    MaybeRecordParseLocation(ast_column_location, expr.get());
    arguments.emplace_back(
        ast_column_location, std::move(expr),
        absl::make_unique<AliasOrASTPathExpression>(named_column.name));
  }

  std::unique_ptr<const ResolvedExpr> resolved_build_proto_expr;
  ZETASQL_RETURN_IF_ERROR(ResolveBuildProto(ast_type_location, proto_type,
                                    input_scan.get(), "Column", "Query",
                                    &arguments, &resolved_build_proto_expr));

  // Wrap resolved_query with a projection that creates the proto.
  const ResolvedColumn proto_column(AllocateColumnId(), kMakeProtoId, kProtoId,
                                    proto_type);

  *output_scan = MakeResolvedProjectScan(
      std::vector<ResolvedColumn>{proto_column},
      MakeNodeVector(MakeResolvedComputedColumn(
          proto_column, std::move(resolved_build_proto_expr))),
      std::move(input_scan));

  NameList* mutable_name_list;
  output_name_list->reset((mutable_name_list = new NameList));
  // is_explicit=false because the created column is always anonymous.
  ZETASQL_RET_CHECK(IsInternalAlias(proto_column.name()));
  ZETASQL_RETURN_IF_ERROR(
      mutable_name_list->AddColumn(MakeIdString(proto_column.name()),
                                   proto_column, false /* is_explicit */));
  // Make the output table a value table.
  mutable_name_list->set_is_value_table(true);

  return absl::OkStatus();
}

static absl::Status GetSetScanEnumType(
    const ASTSetOperation* set_operation,
    ResolvedSetOperationScan::SetOperationType* op_type) {
  switch (set_operation->op_type()) {
    case ASTSetOperation::UNION:
      *op_type = set_operation->distinct()
                     ? ResolvedSetOperationScan::UNION_DISTINCT
                     : ResolvedSetOperationScan::UNION_ALL;
      break;

    case ASTSetOperation::EXCEPT:
      *op_type = set_operation->distinct()
                     ? ResolvedSetOperationScan::EXCEPT_DISTINCT
                     : ResolvedSetOperationScan::EXCEPT_ALL;
      break;

    case ASTSetOperation::INTERSECT:
      *op_type = set_operation->distinct()
                     ? ResolvedSetOperationScan::INTERSECT_DISTINCT
                     : ResolvedSetOperationScan::INTERSECT_ALL;
      break;

    case ASTSetOperation::NOT_SET:
      return MakeSqlErrorAtLocalNode(set_operation)
             << "Invalid set operation type";
  }

  return absl::OkStatus();
}

static absl::Status GetRecursiveScanEnumType(
    const ASTSetOperation* set_operation,
    ResolvedRecursiveScan::RecursiveSetOperationType* op_type) {
  ZETASQL_RET_CHECK_EQ(set_operation->op_type(), ASTSetOperation::UNION);
  if (set_operation->distinct()) {
    *op_type = ResolvedRecursiveScan::UNION_DISTINCT;
  } else {
    *op_type = ResolvedRecursiveScan::UNION_ALL;
  }
  return absl::OkStatus();
}

static std::string FormatColumnCount(const NameList& name_list) {
  return name_list.is_value_table()
             ? std::string(" is value table with 1 column")
             : absl::StrCat(" has ", name_list.num_columns(), " column",
                            (name_list.num_columns() == 1 ? "" : "s"));
}

Resolver::SetOperationResolver::SetOperationResolver(
    const ASTSetOperation* set_operation, Resolver* resolver)
    : set_operation_(set_operation),
      resolver_(resolver),
      op_type_str_(resolver_->MakeIdString(
          absl::StrCat("$", absl::AsciiStrToLower(ReplaceFirst(
                                set_operation_->GetSQLForOperation(),
                                /*oldsub=*/" ", /*newsub=*/"_"))))) {}

absl::Status Resolver::SetOperationResolver::Resolve(
    const NameScope* scope, std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  ZETASQL_RET_CHECK_GE(set_operation_->inputs().size(), 2);
  ResolvedSetOperationScan::SetOperationType op_type;
  ZETASQL_RETURN_IF_ERROR(GetSetScanEnumType(set_operation_, &op_type));

  std::vector<ResolvedInputResult> resolved_inputs;
  resolved_inputs.reserve(set_operation_->inputs().size());
  for (int idx = 0; idx < set_operation_->inputs().size(); ++idx) {
    ZETASQL_ASSIGN_OR_RETURN(resolved_inputs.emplace_back(),
                     ResolveInputQuery(scope, idx));
  }

  std::vector<std::vector<InputArgumentType>> column_type_lists;
  ZETASQL_ASSIGN_OR_RETURN(column_type_lists,
                   BuildColumnTypeLists(absl::MakeSpan(resolved_inputs)));
  ZETASQL_ASSIGN_OR_RETURN(
      ResolvedColumnList final_column_list,
      BuildColumnLists(column_type_lists, *resolved_inputs.front().name_list));

  std::vector<std::unique_ptr<ResolvedSetOperationItem>>
      resolved_input_set_op_items;
  resolved_input_set_op_items.reserve(resolved_inputs.size());
  for (ResolvedInputResult& result : resolved_inputs) {
    resolved_input_set_op_items.push_back(std::move(result.node));
  }

  ZETASQL_RETURN_IF_ERROR(CreateWrapperScansWithCasts(
      final_column_list, absl::MakeSpan(resolved_input_set_op_items)));
  auto set_op_scan = MakeResolvedSetOperationScan(
      final_column_list, op_type, std::move(resolved_input_set_op_items));
  ZETASQL_RETURN_IF_ERROR(resolver_->CheckAndPropagateAnnotations(set_operation_,
                                                          set_op_scan.get()));

  // Resolve the ResolvedOption (Query Hint), if present.
  if (set_operation_->hint() != nullptr) {
    std::vector<std::unique_ptr<const ResolvedOption>> hint_list;
    ZETASQL_RETURN_IF_ERROR(
        resolver_->ResolveHintAndAppend(set_operation_->hint(), &hint_list));
    set_op_scan->set_hint_list(std::move(hint_list));
  }

  *output = std::move(set_op_scan);
  ZETASQL_ASSIGN_OR_RETURN(*output_name_list,
                   BuildFinalNameList(*resolved_inputs.front().name_list,
                                      final_column_list));
  return absl::OkStatus();
}

Resolver::ValidateRecursiveTermVisitor::ValidateRecursiveTermVisitor(
    const Resolver* resolver, IdString recursive_query_name)
    : resolver_(resolver), recursive_query_name_(recursive_query_name) {}

absl::Status Resolver::ValidateRecursiveTermVisitor::DefaultVisit(
    const ResolvedNode* node) {
  ZETASQL_RETURN_IF_ERROR(node->ChildrenAccept(this));
  return absl::OkStatus();
}

absl::Status Resolver::ValidateRecursiveTermVisitor::VisitResolvedAggregateScan(
    const ResolvedAggregateScan* node) {
  ++aggregate_scan_count_;
  ZETASQL_RETURN_IF_ERROR(node->ChildrenAccept(this));
  --aggregate_scan_count_;
  return absl::OkStatus();
}

absl::Status
Resolver::ValidateRecursiveTermVisitor::VisitResolvedFunctionArgument(
    const ResolvedFunctionArgument* node) {
  ++tvf_argument_count_;
  ZETASQL_RETURN_IF_ERROR(node->ChildrenAccept(this));
  --tvf_argument_count_;
  return absl::OkStatus();
}

absl::Status
Resolver::ValidateRecursiveTermVisitor::VisitResolvedLimitOffsetScan(
    const ResolvedLimitOffsetScan* node) {
  ++limit_offset_scan_count_;
  ZETASQL_RETURN_IF_ERROR(node->ChildrenAccept(this));
  --limit_offset_scan_count_;
  return absl::OkStatus();
}

absl::Status Resolver::ValidateRecursiveTermVisitor::VisitResolvedWithEntry(
    const ResolvedWithEntry* node) {
  ++nested_with_entry_count_;
  ZETASQL_RETURN_IF_ERROR(node->ChildrenAccept(this));
  --nested_with_entry_count_;
  return absl::OkStatus();
}

absl::Status
Resolver::ValidateRecursiveTermVisitor::VisitResolvedSetOperationScan(
    const ResolvedSetOperationScan* node) {
  switch (node->op_type()) {
    case ResolvedSetOperationScan::EXCEPT_ALL:
      if (node->input_item_list_size() == 0) {
        return absl::OkStatus();
      }
      ZETASQL_RETURN_IF_ERROR(node->input_item_list(0)->Accept(this));
      ++except_clause_count_;
      for (int i = 1; i < node->input_item_list_size(); ++i) {
        ZETASQL_RETURN_IF_ERROR(node->input_item_list(i)->Accept(this));
      }
      --except_clause_count_;
      return absl::OkStatus();
    case ResolvedSetOperationScan::EXCEPT_DISTINCT:
    case ResolvedSetOperationScan::INTERSECT_DISTINCT:
    case ResolvedSetOperationScan::UNION_DISTINCT:
      ++setop_distinct_count_;
      ZETASQL_RETURN_IF_ERROR(node->ChildrenAccept(this));
      --setop_distinct_count_;
      return absl::OkStatus();
    default:
      return node->ChildrenAccept(this);
  }
}

absl::Status Resolver::ValidateRecursiveTermVisitor::VisitResolvedAnalyticScan(
    const ResolvedAnalyticScan* node) {
  ++analytic_scan_count_;
  ZETASQL_RETURN_IF_ERROR(node->ChildrenAccept(this));
  --analytic_scan_count_;
  return absl::OkStatus();
}

absl::Status Resolver::ValidateRecursiveTermVisitor::VisitResolvedSampleScan(
    const ResolvedSampleScan* node) {
  ++sample_scan_count_;
  ZETASQL_RETURN_IF_ERROR(node->ChildrenAccept(this));
  --sample_scan_count_;
  return absl::OkStatus();
}

absl::Status Resolver::ValidateRecursiveTermVisitor::VisitResolvedOrderByScan(
    const ResolvedOrderByScan* node) {
  ++order_by_scan_count_;
  ZETASQL_RETURN_IF_ERROR(node->ChildrenAccept(this));
  --order_by_scan_count_;
  return absl::OkStatus();
}

absl::Status Resolver::ValidateRecursiveTermVisitor::VisitResolvedSubqueryExpr(
    const ResolvedSubqueryExpr* node) {
  ++subquery_expr_count_;
  ZETASQL_RETURN_IF_ERROR(node->ChildrenAccept(this));
  --subquery_expr_count_;
  return absl::OkStatus();
}

absl::Status Resolver::ValidateRecursiveTermVisitor::VisitResolvedRecursiveScan(
    const ResolvedRecursiveScan* node) {
  // If we have an inner recursive query, process only the query's non-recursive
  // term. As recursive queries are validated in a bottom-up fashon, the inner
  // query's recursive term has already been validated, and part of the
  // inner query's validation prevents its recursive term from referencing the
  // current (outer) query.
  ZETASQL_RETURN_IF_ERROR(node->non_recursive_term()->Accept(this));
  return absl::OkStatus();
}

int* Resolver::ValidateRecursiveTermVisitor::GetJoinCountField(
    const ResolvedJoinScan::JoinType join_type, bool left_operand) {
  switch (join_type) {
    case ResolvedJoinScan::LEFT:
      return left_operand ? nullptr : &right_operand_of_left_join_count_;
    case ResolvedJoinScan::RIGHT:
      return left_operand ? &left_operand_of_right_join_count_ : nullptr;
    case ResolvedJoinScan::FULL:
      return &full_join_operand_count_;
    case ResolvedJoinScan::INNER:
      return nullptr;
  }
}

void Resolver::ValidateRecursiveTermVisitor::MaybeAdjustJoinCount(
    const ResolvedJoinScan::JoinType join_type, bool left_operand, int offset) {
  int* field = GetJoinCountField(join_type, left_operand);
  if (field != nullptr) {
    (*field) += offset;
  }
}

absl::Status Resolver::ValidateRecursiveTermVisitor::VisitResolvedJoinScan(
    const ResolvedJoinScan* node) {
  // Process left operand
  MaybeAdjustJoinCount(node->join_type(), /*left_operand=*/true, 1);
  ZETASQL_RETURN_IF_ERROR(node->left_scan()->Accept(this));
  MaybeAdjustJoinCount(node->join_type(), /*left_operand=*/true, -1);

  // Process right operand
  MaybeAdjustJoinCount(node->join_type(), /*left_operand=*/false, 1);
  ZETASQL_RETURN_IF_ERROR(node->right_scan()->Accept(this));
  MaybeAdjustJoinCount(node->join_type(), /*left_operand=*/false, -1);

  // Process ON expression
  if (node->join_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(node->join_expr()->Accept(this));
  }

  return absl::OkStatus();
}

absl::Status
Resolver::ValidateRecursiveTermVisitor::VisitResolvedRecursiveRefScan(
    const ResolvedRecursiveRefScan* node) {
  auto it = resolver_->recursive_ref_info_.find(node);
  ZETASQL_RET_CHECK(it != resolver_->recursive_ref_info_.end());
  const RecursiveRefScanInfo& info = it->second;
  std::string query_type =
      (recursive_query_name_.ToStringView() == "$view") ? "view" : "table";

  if (seen_recursive_reference_) {
    return MakeSqlErrorAt(info.path)
           << "Multiple recursive references to " << query_type << " '"
           << info.path->ToIdentifierPathString() << "' are not allowed";
  }
  seen_recursive_reference_ = true;

  if (!info.recursive_query_unique_name.Equals(recursive_query_name_)) {
    return MakeSqlErrorAt(info.path)
           << query_type << " '" << info.path->ToIdentifierPathString()
           << "' may not be recursively referenced from inside an "
              "inner WITH entry";
  }

  if (nested_with_entry_count_ > 0) {
    return MakeSqlErrorAt(info.path)
           << query_type << " '" << info.path->ToIdentifierPathString()
           << "' may not be recursively referenced from inside an "
              "inner WITH entry";
  }

  if (subquery_expr_count_ > 0) {
    return MakeSqlErrorAt(info.path)
           << "A recursive reference from inside an expression subquery is not "
              "allowed";
  }

  if (aggregate_scan_count_ > 0) {
    return MakeSqlErrorAt(info.path)
           << "A subquery containing a recursive reference may not use "
              "DISTINCT, GROUP BY, or any aggregate function";
  }

  if (setop_distinct_count_ > 0) {
    return MakeSqlErrorAt(info.path)
           << "A subquery containing a recursive reference may not use "
              "INTERSECT, UNION, or EXCEPT with the DISTINCT modifier";
  }

  if (analytic_scan_count_ > 0) {
    return MakeSqlErrorAt(info.path)
           << "A subquery containing a recursive reference may not use an "
              "analytic function";
  }

  if (sample_scan_count_ > 0) {
    return MakeSqlErrorAt(info.path)
           << "A subquery containing a recursive reference may not use the "
              "TABLESAMPLE operator";
  }

  if (order_by_scan_count_ > 0) {
    return MakeSqlErrorAt(info.path)
           << "A subquery containing a recursive reference may not contain an "
              "ORDER BY clause";
  }

  if (limit_offset_scan_count_ > 0) {
    return MakeSqlErrorAt(info.path) << "A query containing a recursive "
                                        "reference may not use a LIMIT clause";
  }

  if (right_operand_of_left_join_count_ > 0) {
    return MakeSqlErrorAt(info.path)
           << "A query containing a recursive reference may not be used as the "
              "right operand of a LEFT JOIN";
  }

  if (left_operand_of_right_join_count_ > 0) {
    return MakeSqlErrorAt(info.path)
           << "A query containing a recursive reference may not be used as the "
              "left operand of a RIGHT JOIN";
  }

  if (full_join_operand_count_ > 0) {
    return MakeSqlErrorAt(info.path)
           << "A query containing a recursive reference may not be used as an "
              "operand of a FULL OUTER JOIN";
  }

  if (tvf_argument_count_ > 0) {
    return MakeSqlErrorAt(info.path)
           << "A query containing a recursive reference may not be used as an "
              "argument to a table-valued function";
  }

  if (except_clause_count_ > 0) {
    return MakeSqlErrorAt(info.path)
           << "A subquery containing a recursive reference may not be used as "
              "the right operand of EXCEPT";
  }

  return absl::OkStatus();
}

absl::Status Resolver::SetOperationResolver::ResolveRecursive(
    const NameScope* scope, const std::vector<IdString>& recursive_alias,
    const IdString& recursive_query_unique_name,
    std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  ZETASQL_RET_CHECK_GE(set_operation_->inputs().size(), 2);

  ResolvedRecursiveScan::RecursiveSetOperationType recursive_op_type;
  ZETASQL_RETURN_IF_ERROR(GetRecursiveScanEnumType(set_operation_, &recursive_op_type));

  // Register a NULL entry for the named subquery so that any references to it
  // from within the non-recursive term result in an error. We'll change this
  // to an actual NamedSubquery object when the recursive term resolves.
  resolver_->named_subquery_map_[recursive_alias].push_back(nullptr);

  // The standard requires that any recursion be confined to the rhs of the
  // UNION. However, the ZetaSQL parser collapses chains of UNION's like:
  //   SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 ...
  // into a single ASTSetOperation node, which is left-associative. When this
  // happens, we will treat the UNION of all terms except the last as the
  // overall non-recursive term, and the last term of the UNION as the overall
  // recursive term. To override and include an inner UNION within the recursive
  // term, the user will need to provide explicit parentheses.
  std::vector<ResolvedInputResult> nonrecursive_resolved_inputs;
  int num_nonrecursive_inputs =
      static_cast<int>(set_operation_->inputs().size() - 1);
  for (int idx = 0; idx < num_nonrecursive_inputs; ++idx) {
    ZETASQL_ASSIGN_OR_RETURN(nonrecursive_resolved_inputs.emplace_back(),
                     ResolveInputQuery(scope, idx));
  }

  ZETASQL_RET_CHECK_EQ(set_operation_->inputs().size() - 1,
               nonrecursive_resolved_inputs.size());

  // Determine the UNION's column list and name list using only the
  // non-recursive terms.
  std::vector<std::vector<InputArgumentType>> column_type_lists;
  ZETASQL_ASSIGN_OR_RETURN(
      column_type_lists,
      BuildColumnTypeLists(absl::MakeSpan(nonrecursive_resolved_inputs)));
  ZETASQL_ASSIGN_OR_RETURN(
      ResolvedColumnList column_list,
      BuildColumnLists(column_type_lists,
                       *nonrecursive_resolved_inputs.front().name_list));

  std::vector<std::unique_ptr<ResolvedSetOperationItem>>
      resolved_nonrecursive_input_set_op_items;
  resolved_nonrecursive_input_set_op_items.reserve(
      nonrecursive_resolved_inputs.size());
  for (ResolvedInputResult& result : nonrecursive_resolved_inputs) {
    resolved_nonrecursive_input_set_op_items.push_back(std::move(result.node));
  }

  ZETASQL_RETURN_IF_ERROR(CreateWrapperScansWithCasts(
      column_list, absl::MakeSpan(resolved_nonrecursive_input_set_op_items)));

  std::shared_ptr<const NameList> final_name_list;
  ZETASQL_ASSIGN_OR_RETURN(
      final_name_list,
      BuildFinalNameList(*nonrecursive_resolved_inputs.front().name_list,
                         column_list));

  // Register the WITH entry so that self-references in the recursive term can
  // resolve correctly.
  resolver_->named_subquery_map_.at(recursive_alias).back() =
      absl::make_unique<NamedSubquery>(recursive_query_unique_name,
                                       /*recursive_in=*/true, column_list,
                                       final_name_list);

  // Resolve the recursive term.
  ZETASQL_ASSIGN_OR_RETURN(
      ResolvedInputResult resolved_recursive_input,
      ResolveInputQuery(scope,
                        static_cast<int>(set_operation_->inputs().size() - 1)));

  // The recursive query is now resolved; clear the 'recursive' flag in
  // named_subquery_map_ so that future references simply resolve like an
  // ordinary WITH entry.
  resolver_->named_subquery_map_[recursive_alias].back()->is_recursive = false;

  if (resolved_recursive_input.name_list->num_columns() !=
      column_type_lists.size()) {
    return MakeSqlErrorAt(set_operation_->inputs().back())
           << "Queries in " << set_operation_->GetSQLForOperation()
           << " have mismatched column count; query 1"
           << FormatColumnCount(*nonrecursive_resolved_inputs.front().name_list)
           << ", query " << (nonrecursive_resolved_inputs.size() + 1)
           << FormatColumnCount(*resolved_recursive_input.name_list);
  }

  ValidateRecursiveTermVisitor validation_visitor(resolver_,
                                                  recursive_query_unique_name);
  ZETASQL_RETURN_IF_ERROR(resolved_recursive_input.node->Accept(&validation_visitor));

  // Obtain the type of each column in the recursive term and verify that it
  // is coercible to the corresponding column type based on the non-recursive
  // terms.
  std::vector<std::vector<InputArgumentType>> recursive_column_type_lists;
  ZETASQL_ASSIGN_OR_RETURN(
      recursive_column_type_lists,
      BuildColumnTypeLists(absl::MakeSpan(&resolved_recursive_input, 1)));

  for (int i = 0; i < column_list.size(); ++i) {
    SignatureMatchResult result;
    if (!resolver_->coercer_.CoercesTo(recursive_column_type_lists.at(i).at(0),
                                       column_list.at(i).type(),
                                       /*is_explicit=*/false, &result)) {
      return MakeSqlErrorAt(set_operation_->inputs().back())
             << "Cannot coerce column " << (i + 1) << " of recursive term "
             << "("
             << recursive_column_type_lists.at(i).at(0).type()->TypeName(
                    resolver_->analyzer_options_.language().product_mode())
             << ") to column type in non-recursive term ( "
             << column_list.at(i).type()->TypeName(
                    resolver_->analyzer_options_.language().product_mode())
             << ")";
    }
  }

  // Add casts as needed to ensure that every column produced by the recursive
  // term is the correct type (we already verified that the casts are legal
  // through implicit coercion).
  ZETASQL_RETURN_IF_ERROR(CreateWrapperScansWithCasts(
      column_list, absl::MakeSpan(&resolved_recursive_input.node, 1)));

  // Package everything together in a ResolvedRecursiveScan node.
  std::unique_ptr<ResolvedRecursiveScan> recursive_scan;
  if (nonrecursive_resolved_inputs.size() == 1) {
    recursive_scan = MakeResolvedRecursiveScan(
        column_list, recursive_op_type,
        std::move(resolved_nonrecursive_input_set_op_items.at(0)),
        std::move(resolved_recursive_input.node));
  } else {
    // If we have multiple non-recursive operands, wrap them in an inner UNION
    // so that the ResolvedRecursive scan can have just one non-recursive
    // operand.
    ResolvedSetOperationScan::SetOperationType op_type;
    ZETASQL_RETURN_IF_ERROR(GetSetScanEnumType(set_operation_, &op_type));

    // Clone the column list so that the columns in the inner set operation used
    // for the non-recursive term have unique ids.
    ResolvedColumnList inner_set_op_column_list;
    for (const auto& column : column_list) {
      inner_set_op_column_list.push_back(
          ResolvedColumn(resolver_->AllocateColumnId(), op_type_str_,
                         column.name_id(), column.type()));
      resolver_->RecordColumnAccess(inner_set_op_column_list.back());
    }

    auto set_op_scan = MakeResolvedSetOperationScan(
        inner_set_op_column_list, op_type,
        std::move(resolved_nonrecursive_input_set_op_items));
    auto non_recursive_operand = MakeResolvedSetOperationItem(
        std::move(set_op_scan), inner_set_op_column_list);
    recursive_scan = MakeResolvedRecursiveScan(
        column_list, recursive_op_type, std::move(non_recursive_operand),
        std::move(resolved_recursive_input.node));
  }
  // Resolve the ResolvedOption (Query Hint), if present.
  if (set_operation_->hint() != nullptr) {
    std::vector<std::unique_ptr<const ResolvedOption>> hint_list;
    ZETASQL_RETURN_IF_ERROR(
        resolver_->ResolveHintAndAppend(set_operation_->hint(), &hint_list));
    recursive_scan->set_hint_list(std::move(hint_list));
  }
  *output = std::move(recursive_scan);
  *output_name_list = final_name_list;
  return absl::OkStatus();
}

absl::StatusOr<Resolver::SetOperationResolver::ResolvedInputResult>
Resolver::SetOperationResolver::ResolveInputQuery(const NameScope* scope,
                                                  int query_index) const {
  ZETASQL_RET_CHECK_GE(query_index, 0);
  ZETASQL_RET_CHECK_LT(query_index, set_operation_->inputs().size());
  const IdString query_alias = resolver_->MakeIdString(
      absl::StrCat(op_type_str_.ToStringView(), query_index + 1));

  ResolvedInputResult result;
  std::unique_ptr<const ResolvedScan> resolved_scan;
  ZETASQL_RETURN_IF_ERROR(resolver_->ResolveQueryExpression(
      set_operation_->inputs()[query_index], scope, query_alias,
      /*force_new_columns_for_projected_outputs=*/false, &resolved_scan,
      &result.name_list));

  result.node = MakeResolvedSetOperationItem(
      std::move(resolved_scan), result.name_list->GetResolvedColumns());
  return result;
}

absl::StatusOr<std::vector<std::vector<InputArgumentType>>>
Resolver::SetOperationResolver::BuildColumnTypeLists(
    absl::Span<ResolvedInputResult> resolved_inputs) const {
  std::vector<std::vector<InputArgumentType>> column_type_lists;
  column_type_lists.resize(resolved_inputs.front().name_list->num_columns());
  // Resolve all the input scans, and collect <column_type_lists>.
  for (int idx = 0; idx < resolved_inputs.size(); ++idx) {
    const ResolvedScan* resolved_scan = resolved_inputs[idx].node->scan();
    const NameList& curr_name_list = *resolved_inputs.at(idx).name_list;

    if (curr_name_list.num_columns() != column_type_lists.size()) {
      return MakeSqlErrorAt(set_operation_->inputs()[idx])
             << "Queries in " << set_operation_->GetSQLForOperation()
             << " have mismatched column count; query 1"
             << FormatColumnCount(*resolved_inputs.front().name_list)
             << ", query " << (idx + 1) << FormatColumnCount(curr_name_list);
    }

    // Construct an InputArgumentType for each column in the name_list,
    // including literal values when present.
    for (int i = 0; i < curr_name_list.num_columns(); ++i) {
      const ResolvedColumn& column = curr_name_list.column(i).column;

      // If this column was computed, find the expr that computed it.
      // If the computed expr was a literal, include the literal value.
      const ResolvedExpr* expr = nullptr;
      if (resolved_scan->node_kind() == RESOLVED_PROJECT_SCAN) {
        expr = FindProjectExpr(resolved_scan->GetAs<ResolvedProjectScan>(),
                               column);
      }
      if (expr != nullptr) {
        column_type_lists[i].emplace_back(GetInputArgumentTypeForExpr(expr));
      } else {
        column_type_lists[i].emplace_back(InputArgumentType(column.type()));
      }
    }
  }
  return column_type_lists;
}

absl::StatusOr<ResolvedColumnList>
Resolver::SetOperationResolver::BuildColumnLists(
    const std::vector<std::vector<InputArgumentType>>& column_type_lists,
    const NameList& first_item_name_list) const {
  ResolvedColumnList column_list;

  // Compute common supertypes and final column_list names for the set
  // operation.
  for (int i = 0; i < column_type_lists.size(); ++i) {
    const ASTNode* ast_input_location = set_operation_->inputs()[1];

    InputArgumentTypeSet type_set;
    for (const InputArgumentType& type : column_type_lists[i]) {
      type_set.Insert(type);
    }
    const Type* supertype = nullptr;
    ZETASQL_RETURN_IF_ERROR(
        resolver_->coercer_.GetCommonSuperType(type_set, &supertype));
    if (supertype == nullptr) {
      // We location in set_operation points at the start of the first query,
      // because of how the grammar is expressed, I think.
      // Point at the start of the second query so the error is close to the
      // set operation keyword, at least.
      return MakeSqlErrorAt(ast_input_location)
             << "Column " << (i + 1) << " in "
             << set_operation_->GetSQLForOperation()
             << " has incompatible types: "
             << InputArgumentType::ArgumentsToString(column_type_lists[i]);
    }

    std::string no_grouping_type;
    bool column_types_must_support_grouping =
        set_operation_->op_type() != ASTSetOperation::UNION ||
        set_operation_->distinct();
    if (column_types_must_support_grouping &&
        !resolver_->TypeSupportsGrouping(supertype, &no_grouping_type)) {
      return MakeSqlErrorAt(ast_input_location)
             << "Column " << (i + 1) << " in "
             << set_operation_->GetSQLForOperation()
             << " has type that does not support set operation comparisons: "
             << no_grouping_type;
    }

    const IdString name = first_item_name_list.column(i).name;
    column_list.push_back(ResolvedColumn(resolver_->AllocateColumnId(),
                                         op_type_str_, name, supertype));
    resolver_->RecordColumnAccess(column_list.back());
  }

  return column_list;
}

// Modifies <resolved_inputs>, adding a cast if necessary to convert each
// column to the respective overall column type of the set operation.
absl::Status Resolver::SetOperationResolver::CreateWrapperScansWithCasts(
    const ResolvedColumnList& column_list,
    absl::Span<std::unique_ptr<ResolvedSetOperationItem>> resolved_inputs)
    const {
  for (int idx = 0; idx < resolved_inputs.size(); ++idx) {
    ResolvedSetOperationItem* input = resolved_inputs.at(idx).get();

    std::unique_ptr<const ResolvedScan> resolved_scan = input->release_scan();

    ZETASQL_RETURN_IF_ERROR(resolver_->CreateWrapperScanWithCasts(
        set_operation_->inputs()[idx], column_list,
        resolver_->MakeIdString(
            absl::StrCat(op_type_str_.ToStringView(), idx + 1, "_cast")),
        &resolved_scan, input->mutable_output_column_list()));

    input->set_scan(std::move(resolved_scan));
  }
  return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<const NameList>>
Resolver::SetOperationResolver::BuildFinalNameList(
    const NameList& first_item_name_list,
    const ResolvedColumnList& final_column_list) const {
  std::shared_ptr<NameList> name_list(new NameList);
  // The first subquery determines the name and explicit attribute of each
  // column, as well as whether the result is a value table.
  for (int i = 0; i < final_column_list.size(); ++i) {
    const IdString name = first_item_name_list.column(i).name;
    ZETASQL_RETURN_IF_ERROR(
        name_list->AddColumn(name, final_column_list.at(i),
                             first_item_name_list.column(i).is_explicit));
  }

  if (first_item_name_list.is_value_table()) {
    ZETASQL_RET_CHECK_EQ(name_list->num_columns(), 1);
    name_list->set_is_value_table(true);
  }
  return name_list;
}

// Note that we allow set operations between value tables and regular
// tables with exactly one column.  The output will be a value table if
// the first subquery was a value table.
absl::Status Resolver::ResolveSetOperation(
    const ASTSetOperation* set_operation, const NameScope* scope,
    std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  SetOperationResolver resolver(set_operation, this);
  return resolver.Resolve(scope, output, output_name_list);
}

absl::Status Resolver::ValidateIntegerParameterOrLiteral(
    const char* clause_name, const ASTNode* ast_location,
    const ResolvedExpr& expr) const {
  if ((expr.node_kind() != RESOLVED_PARAMETER &&
       expr.node_kind() != RESOLVED_LITERAL) ||
      !expr.type()->IsInteger()) {
    return MakeSqlErrorAt(ast_location)
           << clause_name << " expects an integer literal or parameter";
  }
  return absl::OkStatus();
}

absl::Status Resolver::ValidateParameterOrLiteralAndCoerceToInt64IfNeeded(
    const char* clause_name, const ASTNode* ast_location,
    std::unique_ptr<const ResolvedExpr>* expr) const {
  if ((*expr)->type()->IsInt64() && (*expr)->node_kind() == RESOLVED_CAST) {
    // We allow CAST(<expr> AS INT64), as long as <expr> is a parameter
    // or literal.
    ZETASQL_RETURN_IF_ERROR(ValidateIntegerParameterOrLiteral(
        clause_name, ast_location, *(*expr)->GetAs<ResolvedCast>()->expr()));
  } else {
    ZETASQL_RETURN_IF_ERROR(
        ValidateIntegerParameterOrLiteral(clause_name, ast_location, **expr));
  }
  // kExplicitCoercion is safe to use here because the above lines validate the
  // type. If those restrictions are relaxed we might need to do something
  // differently.
  ZETASQL_RETURN_IF_ERROR(CoerceExprToType(ast_location, type_factory_->get_int64(),
                                   kExplicitCoercion, expr));

  if ((*expr)->node_kind() == RESOLVED_LITERAL) {
    // If a literal, we can also validate its value.
    const Value value = (*expr)->GetAs<ResolvedLiteral>()->value();
    if (!value.is_null() && value.int64_value() < 0) {
      return MakeSqlErrorAt(ast_location)
             << clause_name
             << " expects a non-negative integer literal or parameter";
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveLimitOrOffsetExpr(
    const ASTExpression* ast_expr, const char* clause_name,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedExpr>* resolved_expr) {
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(ast_expr, expr_resolution_info, resolved_expr));
  ZETASQL_DCHECK(resolved_expr != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateParameterOrLiteralAndCoerceToInt64IfNeeded(
      clause_name, ast_expr, resolved_expr));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveHavingModifier(
    const ASTHavingModifier* ast_having_modifier,
    ExprResolutionInfo* expr_resolution_info,
    std::unique_ptr<const ResolvedAggregateHavingModifier>* resolved_having) {
  std::unique_ptr<const ResolvedExpr> resolved_expr;
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(ast_having_modifier->expr(), expr_resolution_info,
                              &resolved_expr));
  // The HAVING MIN/MAX expression type must support ordering, and it cannot
  // be an array since MIN/MAX is currently undefined for arrays (even if
  // the array element supports MIN/MAX).
  if (!resolved_expr->type()->SupportsOrdering(language(),
                                               /*type_description=*/nullptr) ||
      resolved_expr->type()->IsArray()) {
    return MakeSqlErrorAt(ast_having_modifier)
           << "HAVING modifier does not support expressions of type "
           << resolved_expr->type()->ShortTypeName(product_mode());
  }

  if (language().LanguageFeatureEnabled(FEATURE_DISALLOW_GROUP_BY_FLOAT) &&
      resolved_expr->type()->IsFloatingPoint()) {
    return MakeSqlErrorAt(ast_having_modifier)
           << "HAVING modifier does not support expressions of type "
           << resolved_expr->type()->ShortTypeName(product_mode());
  }

  ZETASQL_DCHECK(resolved_having != nullptr);
  ResolvedAggregateHavingModifier::HavingModifierKind kind;
  if (ast_having_modifier->modifier_kind() ==
      ASTHavingModifier::ModifierKind::MAX) {
    kind = ResolvedAggregateHavingModifier::MAX;
  } else {
    kind = ResolvedAggregateHavingModifier::MIN;
  }
  *resolved_having =
      MakeResolvedAggregateHavingModifier(kind, std::move(resolved_expr));
  return absl::OkStatus();
}

// Resolves a LimitOffsetScan.
// If an OFFSET is not supplied, then the default value, 0, is used.
absl::Status Resolver::ResolveLimitOffsetScan(
    const ASTLimitOffset* limit_offset,
    std::unique_ptr<const ResolvedScan> input_scan,
    std::unique_ptr<const ResolvedScan>* output) {
  ExprResolutionInfo expr_resolution_info(empty_name_scope_.get(),
                                          "LIMIT OFFSET");

  // Resolve and validate the LIMIT.
  ZETASQL_RET_CHECK(limit_offset->limit() != nullptr);
  std::unique_ptr<const ResolvedExpr> limit_expr;
  ZETASQL_RETURN_IF_ERROR(ResolveLimitOrOffsetExpr(limit_offset->limit(),
                                           "LIMIT" /* clause_name */,
                                           &expr_resolution_info, &limit_expr));

  // Resolve and validate the OFFSET.
  std::unique_ptr<const ResolvedExpr> offset_expr;
  if (limit_offset->offset() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveLimitOrOffsetExpr(
        limit_offset->offset(), "OFFSET" /* clause_name */,
        &expr_resolution_info, &offset_expr));
  }

  const std::vector<ResolvedColumn>& column_list = input_scan->column_list();
  *output = MakeResolvedLimitOffsetScan(column_list, std::move(input_scan),
                                        std::move(limit_expr),
                                        std::move(offset_expr));
  return absl::OkStatus();
}

// Tries to return the AST node corresponding to <column_index> in the
// input <ast_location>.  <num_columns> is the expected total number of
// columns.
static const ASTNode* GetASTNodeForColumn(const ASTNode* ast_location,
                                          int column_index, int num_columns) {
  if (ast_location->node_kind() == AST_QUERY) {
    ast_location = ast_location->GetAsOrDie<ASTQuery>()->query_expr();
  }
  if (ast_location->node_kind() == AST_SELECT) {
    // We can only find a specific column if the column list matches 1:1 with
    // output columns.  If there is a SELECT *, the column indexes won't match.
    // Star can never expand to zero columns so if parse node has N children
    // and we have N columns, we know they must match 1:1.
    const ASTSelectList* select_list =
        ast_location->GetAsOrDie<ASTSelect>()->select_list();
    if (select_list->columns().size() == num_columns) {
      ast_location = select_list->columns(column_index);
    }
  }
  return ast_location;
}

// TODO: If all of the columns that need coercing are
// literals then we do not need to add a wrapper scan and the literals should
// be converted in place instead.
absl::Status Resolver::CreateWrapperScanWithCasts(
    const ASTQueryExpression* ast_query,
    const ResolvedColumnList& target_column_list, IdString scan_alias,
    std::unique_ptr<const ResolvedScan>* scan,
    ResolvedColumnList* scan_column_list) {
  ZETASQL_RET_CHECK(scan != nullptr && *scan != nullptr);
  ZETASQL_RET_CHECK_EQ(target_column_list.size(), scan_column_list->size());

  bool needs_casts = false;
  for (int i = 0; i < target_column_list.size(); ++i) {
    if (!target_column_list[i].type()->Equals((*scan_column_list)[i].type())) {
      needs_casts = true;
      break;
    }
  }
  if (needs_casts) {
    ResolvedColumnList casted_column_list;
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> casted_exprs;

    for (int i = 0; i < target_column_list.size(); ++i) {
      const Type* target_type = target_column_list[i].type();
      const ResolvedColumn& scan_column = (*scan_column_list)[i];

      if (target_type->Equals(scan_column.type())) {
        casted_column_list.emplace_back(scan_column);
      } else {
        // Determine the AST expression corresponding to the column. If the
        // query is not a SELECT clause (e.g., a set operation),
        // AddCastOrConvertLiteral below will not convert literals but add
        // casts. Hence, <ast_location> will be used for error reporting only
        // and not for recording parse location of literals.
        const ASTNode* ast_location =
            GetASTNodeForColumn(ast_query, i, target_column_list.size());
        std::unique_ptr<const ResolvedExpr> casted_expr =
            MakeColumnRef(scan_column);
        ZETASQL_RETURN_IF_ERROR(function_resolver_->AddCastOrConvertLiteral(
            ast_location, target_type, /*format=*/nullptr,
            /*time_zone=*/nullptr, TypeParameters(), scan->get(),
            /*set_has_explicit_type=*/false, /*return_null_on_error=*/false,
            &casted_expr));
        const ResolvedColumn casted_column(AllocateColumnId(), scan_alias,
                                           scan_column.name_id(),
                                           target_column_list[i].type());

        // These casted columns should not get pruned.  We wouldn't create them
        // if they weren't required for the query.
        RecordColumnAccess(casted_column);

        casted_column_list.emplace_back(casted_column);
        casted_exprs.push_back(
            MakeResolvedComputedColumn(casted_column, std::move(casted_expr)));
      }
    }

    *scan = MakeResolvedProjectScan(casted_column_list, std::move(casted_exprs),
                                    std::move(*scan));
    casted_exprs.clear();  // Avoid deletion after ownership transfer.

    ZETASQL_RET_CHECK_EQ(scan_column_list->size(), casted_column_list.size());
    *scan_column_list = casted_column_list;
  }

  return absl::OkStatus();
}

absl::Status Resolver::ResolveFromClauseAndCreateScan(
    const ASTSelect* select, const ASTOrderBy* order_by,
    const NameScope* external_scope,
    std::unique_ptr<const ResolvedScan>* output_scan,
    std::shared_ptr<const NameList>* output_name_list) {
  if (select->from_clause() != nullptr) {
    ZETASQL_RET_CHECK(select->from_clause()->table_expression() != nullptr);
    return ResolveTableExpression(select->from_clause()->table_expression(),
                                  external_scope, external_scope, output_scan,
                                  output_name_list);
  } else {
    // No-from-clause query has special rules about what else can exist.
    if (select->where_clause() != nullptr) {
      return MakeSqlErrorAt(select->where_clause())
             << "Query without FROM clause cannot have a WHERE clause";
    }
    if (select->distinct()) {
      return MakeSqlErrorAt(select)
             << "Query without FROM clause cannot use SELECT DISTINCT";
    }
    if (select->group_by() != nullptr) {
      return MakeSqlErrorAt(select->group_by())
             << "Query without FROM clause cannot have a GROUP BY clause";
    }
    if (select->having() != nullptr) {
      return MakeSqlErrorAt(select->having())
             << "Query without FROM clause cannot have a HAVING clause";
    }
    if (select->window_clause() != nullptr) {
      return MakeSqlErrorAt(select->window_clause())
             << "Query without FROM clause cannot have a WINDOW clause";
    }
    if (order_by != nullptr) {
      return MakeSqlErrorAt(order_by)
             << "Query without FROM clause cannot have an ORDER BY clause";
    }

    // All children of the select node that are allowed on no-FROM-clause
    // queries should be listed here.  Ones that are not allowed should have
    // errors above.  This checks we didn't miss anything.
    for (int i = 0; i < select->num_children(); ++i) {
      const ASTNode* child = select->child(i);
      if (child != select->select_list() && child != select->select_as() &&
          child != select->hint()) {
        ZETASQL_RET_CHECK_FAIL() << "Select without FROM clause has child of type "
                         << child->GetNodeKindString()
                         << " that wasn't caught with an error";
      }
    }

    // Set up a SingleRowScan for this from clause, which produces one
    // row with zero columns. All output columns will come from
    // expressions in the select-list.
    *output_scan = MakeResolvedSingleRowScan();
    *output_name_list = empty_name_list_;
  }
  return absl::OkStatus();
}

// This is a self-contained table expression.  It can be an UNNEST, but
// only as a leaf - not one that has to wrap another scan and flatten it.
absl::Status Resolver::ResolveTableExpression(
    const ASTTableExpression* table_expr, const NameScope* external_scope,
    const NameScope* local_scope, std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  switch (table_expr->node_kind()) {
    case AST_TABLE_PATH_EXPRESSION:
      return ResolveTablePathExpression(
          table_expr->GetAsOrDie<ASTTablePathExpression>(), local_scope, output,
          output_name_list);

    case AST_TABLE_SUBQUERY:
      return ResolveTableSubquery(table_expr->GetAsOrDie<ASTTableSubquery>(),
                                  external_scope, output, output_name_list);

    case AST_JOIN:
      return ResolveJoin(table_expr->GetAsOrDie<ASTJoin>(), external_scope,
                         local_scope, output, output_name_list);

    case AST_PARENTHESIZED_JOIN:
      return ResolveParenthesizedJoin(
          table_expr->GetAsOrDie<ASTParenthesizedJoin>(), external_scope,
          local_scope, output, output_name_list);

    case AST_TVF:
      return ResolveTVF(table_expr->GetAsOrDie<ASTTVF>(), external_scope,
                        local_scope, output, output_name_list);

    default:
      return MakeSqlErrorAt(table_expr)
             << "Unhandled node type in from clause: "
             << table_expr->GetNodeKindString();
  }
}

IdString Resolver::GetAliasForExpression(const ASTNode* node) {
  if (node->node_kind() == AST_IDENTIFIER) {
    return node->GetAsOrDie<ASTIdentifier>()->GetAsIdString();
  } else if (node->node_kind() == AST_PATH_EXPRESSION) {
    return node->GetAsOrDie<ASTPathExpression>()->last_name()->GetAsIdString();
  } else if (node->node_kind() == AST_DOT_IDENTIFIER) {
    return node->GetAsOrDie<ASTDotIdentifier>()->name()->GetAsIdString();
  } else {
    return IdString();
  }
}

bool Resolver::IsPathExpressionStartingFromScope(const ASTPathExpression* expr,
                                                 const NameScope* scope) {
  return scope->HasName(expr->first_name()->GetAsIdString());
}

bool Resolver::ShouldResolveAsArrayScan(const ASTTablePathExpression* table_ref,
                                        const NameScope* scope) {
  // Return true if it has UNNEST, or it is a path with at least two
  // identifiers where the first comes from <scope>.
  // Single-word identifiers are always resolved as table names.
  return table_ref->unnest_expr() != nullptr ||
         (table_ref->path_expr()->num_names() > 1 &&
          IsPathExpressionStartingFromScope(table_ref->path_expr(), scope));
}

// Convert a NameList representing a value table query result into a
// NameList for a FROM clause scanning that value table with <alias>.
// input_name_list and output_name_list may be the same NameList.
static absl::Status ConvertValueTableNameListToNameListWithValueTable(
    const ASTNode* ast_location, IdString alias,
    const std::shared_ptr<const NameList>& input_name_list,
    std::shared_ptr<const NameList>* output_name_list) {
  ZETASQL_RET_CHECK(input_name_list->is_value_table());
  ZETASQL_RET_CHECK_EQ(input_name_list->num_columns(), 1);

  std::shared_ptr<NameList> new_name_list(new NameList);
  ZETASQL_RETURN_IF_ERROR(new_name_list->AddValueTableColumn(
      alias, input_name_list->column(0).column, ast_location));
  *output_name_list = new_name_list;
  return absl::OkStatus();
}

absl::Status Resolver::CheckValidValueTable(const ASTPathExpression* path_expr,
                                            const Table* table) const {
  if (table->NumColumns() == 0 || table->GetColumn(0)->IsPseudoColumn()) {
    return MakeSqlErrorAt(path_expr)
           << "Table " << path_expr->ToIdentifierPathString()
           << " is a value table but does not have a value column";
  }
  for (int i = 1; i < table->NumColumns(); ++i) {
    if (!table->GetColumn(i)->IsPseudoColumn()) {
      return MakeSqlErrorAt(path_expr)
             << "Table " << path_expr->ToIdentifierPathString()
             << " is a value table but has multiple columns";
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::CheckValidValueTableFromTVF(
    const ASTTVF* path_expr, const std::string& full_tvf_name,
    const TVFRelation& schema) const {
  int64_t num_pseudo_columns = std::count_if(
      schema.columns().begin(), schema.columns().end(),
      [](const TVFSchemaColumn& column) { return column.is_pseudo_column; });
  if (schema.num_columns() - num_pseudo_columns != 1) {
    return MakeSqlErrorAt(path_expr)
           << "Table-valued functions returning value tables should have "
           << "exactly one column, but value table TVF " << full_tvf_name
           << " returned has " << schema.num_columns() - num_pseudo_columns
           << " columns";
  }
  if (schema.column(0).is_pseudo_column) {
    return MakeSqlErrorAt(path_expr)
           << "Table-valued functions returning value tables should have "
           << "a value column at index 0, but value table TVF " << full_tvf_name
           << " returned has a pseudo column at index 0";
  }
  return absl::OkStatus();
}

absl::Status Resolver::AppendPivotColumnNameViaStringCast(
    const Value& pivot_value, std::string* column_name) {
  ZETASQL_ASSIGN_OR_RETURN(
      Value string_value,
      CastValue(pivot_value, analyzer_options_.default_time_zone(),
                analyzer_options_.language(), type_factory_->get_string()));

  const std::string& raw_column_name = string_value.string_value();

  // Cast of any numeric type to string should never produce an empty string.
  ZETASQL_RET_CHECK(!raw_column_name.empty());

  // Transform the result so that it is a valid SQL identifier, which can be
  // accessed without backticks:
  // - If the entire column name (including prefixes from the pivot expression)
  //   would start with a digit, prepend an "_".
  // - For negative values, replace "-" as "minus_" for the minus sign.
  // - For decimal values, replace "." with "_point_" for the decimal point.
  if (column_name->empty() && isdigit(raw_column_name[0])) {
    absl::StrAppend(column_name, "_");
  }

  absl::StrAppend(column_name,
                  absl::StrReplaceAll(raw_column_name,
                                      {{"-", "minus_"}, {".", "_point_"}}));
  return absl::OkStatus();
}

absl::Status Resolver::AppendPivotColumnName(const Value& pivot_value,
                                             const ASTNode* ast_location,
                                             std::string* column_name) {

  const Type* type = pivot_value.type();
  if (pivot_value.is_null()) {
    absl::StrAppend(column_name, "NULL");
    return absl::OkStatus();
  }
  if (type->IsInt32() || type->IsInt64() || type->IsUint32() ||
      type->IsUint64() || type->IsNumericType() || type->IsBigNumericType() ||
      type->IsBool()) {
    ZETASQL_RETURN_IF_ERROR(
        AppendPivotColumnNameViaStringCast(pivot_value, column_name));
    return absl::OkStatus();
  }
  if (type->IsString()) {
    if (pivot_value.string_value().empty()) {
      if (!column_name->empty()) {
        absl::StrAppend(column_name, "_");
      }
      absl::StrAppend(column_name, "empty_string_value");
    } else {
      absl::StrAppend(column_name, pivot_value.string_value());
    }
    return absl::OkStatus();
  }
  if (type->IsDate()) {
    std::string formatted_date;
    ZETASQL_RETURN_IF_ERROR(functions::FormatDateToString(
        "%Y_%m_%d", pivot_value.date_value(),
        {.expand_Q = false, .expand_J = false}, &formatted_date));

    // If the column name is empty so far, prefix the date with an underscore
    // so that it's a valid SQL identifier.
    if (column_name->empty()) {
      absl::StrAppend(column_name, "_", formatted_date);
    } else {
      absl::StrAppend(column_name, formatted_date);
    }
    return absl::OkStatus();
  }
  if (type->IsEnum()) {
    const std::string* enum_value_name;
    if (type->AsEnum()->FindName(pivot_value.enum_value(), &enum_value_name)) {
      absl::StrAppend(column_name, *enum_value_name);
    } else {
      // As a fallback, treat the underlying value as an INT32.
      ZETASQL_RETURN_IF_ERROR(AppendPivotColumnNameViaStringCast(
          Value::Int32(pivot_value.enum_value()), column_name));
    }
    return absl::OkStatus();
  }
  if (type->IsStruct()) {
    for (int i = 0; i < type->AsStruct()->num_fields(); ++i) {
      if (i != 0) {
        absl::StrAppend(column_name, "_");
      }
      const StructField& field = type->AsStruct()->field(i);
      if (!field.name.empty()) {
        absl::StrAppend(column_name, field.name, "_");
      }
      ZETASQL_RETURN_IF_ERROR(AppendPivotColumnName(pivot_value.field(i), ast_location,
                                            column_name));
    }
    return absl::OkStatus();
  }
  return MakeSqlErrorAt(ast_location)
         << "PIVOT values of type " << type->TypeName(product_mode())
         << " must specify an alias";
}

// Consumes a ResolvedExpr used for an IN-clause value inside of a PIVOT clause.
// Returns the underlying value, if known a resolution time, or absl::nullopt
// otherwise.
//
// Currently, this function supports only literal values and struct constructors
// where all arguments are either literals or other struct constructors.
static absl::optional<Value> GetPivotValue(const ResolvedExpr* node);

static absl::optional<Value> GetStructPivotValue(
    const ResolvedMakeStruct* node) {
  const StructType* struct_type = node->type()->AsStruct();
  std::vector<Value> fields;
  fields.reserve(struct_type->num_fields());
  for (const auto& field : node->field_list()) {
    absl::optional<Value> field_value = GetPivotValue(field.get());
    if (!field_value.has_value()) {
      // The value of one of the struct's fields is unknown, so the value of the
      // struct itself is also unknown.
      return absl::nullopt;
    }
    fields.push_back(std::move(field_value.value()));
  }

  return Value::UnsafeStruct(struct_type, std::move(fields));
}

static absl::optional<Value> GetPivotValue(const ResolvedExpr* node) {
  switch (node->node_kind()) {
    case RESOLVED_LITERAL: {
      // Mark this literal for preservation in the literal remover. It cannot
      // be replaced by a query parameter without causing the query to fail to
      // resolve (because we do not support implicit pivot-value aliases on
      // query parameters and, even if we ever did, the name wouldn't match the
      // one generated by the actual value).
      const ResolvedLiteral* literal = node->GetAs<ResolvedLiteral>();
      const_cast<ResolvedLiteral*>(literal)->set_preserve_in_literal_remover(
          true);
      return literal->value();
    }
    case RESOLVED_MAKE_STRUCT:
      return GetStructPivotValue(node->GetAs<ResolvedMakeStruct>());
    default:
      return absl::nullopt;
  }
}

absl::StatusOr<ResolvedColumn> Resolver::CreatePivotColumn(
    const ASTPivotExpression* ast_pivot_expr,
    const ResolvedExpr* resolved_pivot_expr, bool is_only_pivot_expr,
    const ASTPivotValue* ast_pivot_value,
    const ResolvedExpr* resolved_pivot_value) {
  std::string column_name;

  // Generate the name of the pivot column according to rules described in
  // (broken link).
  if (ast_pivot_expr->alias() != nullptr) {
    absl::StrAppend(&column_name, ast_pivot_expr->alias()->GetAsString(), "_");
  } else if (!is_only_pivot_expr) {
    return MakeSqlErrorAt(ast_pivot_expr)
           << "PIVOT expression must specify an alias unless it is the only "
              "pivot expression in the PIVOT clause";
  }

  if (ast_pivot_value->alias() != nullptr) {
    absl::StrAppend(&column_name, ast_pivot_value->alias()->GetAsString());
  } else {
    absl::optional<Value> pivot_value = GetPivotValue(resolved_pivot_value);
    if (!pivot_value.has_value()) {
      return MakeSqlErrorAt(ast_pivot_value)
             << "Generating an implicit alias for this PIVOT value is not "
                "supported; please provide an explicit alias";
    }
    ZETASQL_RETURN_IF_ERROR(AppendPivotColumnName(pivot_value.value(), ast_pivot_value,
                                          &column_name));
  }

  return ResolvedColumn(AllocateColumnId(),
                        kPivotId,
                        analyzer_options_.id_string_pool()->Make(column_name),
                        resolved_pivot_expr->annotated_type());
}

absl::Status Resolver::ResolvePivotExpressions(
    const ASTPivotExpressionList* ast_pivot_expr_list, const NameScope* scope,
    std::vector<std::unique_ptr<const ResolvedExpr>>* pivot_expr_columns,
    QueryResolutionInfo& query_resolution_info) {
  ExprResolutionInfo info(scope, scope,
                          /*allows_aggregation_in=*/true,
                          /*allows_analytic_in=*/false,
                          /*use_post_grouping_columns_in=*/false, "PIVOT",
                          &query_resolution_info);
  for (const ASTPivotExpression* pivot_expr :
       ast_pivot_expr_list->expressions()) {
    std::unique_ptr<const ResolvedExpr> resolved_pivot_expr;
    ZETASQL_RETURN_IF_ERROR(
        ResolveExpr(pivot_expr->expression(), &info, &resolved_pivot_expr));

    // Unlike scalar functions, aggregate function calls don't appear directly
    // in the ResolvedExpr. Instead, <resolved_pivot_expr> contains a
    // post-aggregation expression, which refers to the aggregation result by
    // accessing a special column, whose definition is stored off to the side
    // in the QueryResolutionInfo.
    //
    // Verify that
    //   1) We indeed have an aggregate function call saved in the
    //        QueryResolutionInfo --and--
    //   2) The ResolvedExpr is just a ColumnRef referring to the aggregate
    //        column from 1 (since post-aggregation logic in pivot expressions
    //        is not supported).
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        pivot_expr_column_vector =
            query_resolution_info.release_aggregate_columns_to_compute();
    if (pivot_expr_column_vector.size() == 1) {
      std::unique_ptr<const ResolvedComputedColumn> pivot_expr_column =
          std::move(pivot_expr_column_vector[0]);
      if (resolved_pivot_expr->node_kind() == RESOLVED_COLUMN_REF &&
          (resolved_pivot_expr->GetAs<ResolvedColumnRef>()
               ->column()
               .column_id() == pivot_expr_column->column().column_id())) {
        // The rewriter requires the root nodes of PIVOT expressions to have a
        // parse location, which gets used in error messages if the function
        // call is not supported. So, always include the parse location for this
        // node, whether explicitly asked for or not.
        ResolvedComputedColumn* mutable_pivot_expr_column =
            const_cast<ResolvedComputedColumn*>(pivot_expr_column.get());
        std::unique_ptr<const ResolvedExpr> standalone_pivot_expr =
            mutable_pivot_expr_column->release_expr();
        const_cast<ResolvedExpr*>(standalone_pivot_expr.get())
            ->SetParseLocationRange(pivot_expr->GetParseLocationRange());

        pivot_expr_columns->push_back(std::move(standalone_pivot_expr));
        continue;
      }
    }
    return MakeSqlErrorAt(pivot_expr->expression())
           << "PIVOT expression must be an aggregate function call";
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveForExprInPivotClause(
    const ASTExpression* for_expr, const NameScope* scope,
    std::unique_ptr<const ResolvedExpr>* resolved_for_expr) {
  QueryResolutionInfo query_resolution_info(this);
  ExprResolutionInfo info(scope, scope,
                          /*allows_aggregation_in=*/false,
                          /*allows_analytic_in=*/false,
                          /*use_post_grouping_columns_in=*/false, "PIVOT",
                          &query_resolution_info);
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(for_expr, &info, resolved_for_expr));
  // TODO: figure out a way to remove the const_cast here.
  const_cast<ResolvedExpr*>(resolved_for_expr->get())
      ->SetParseLocationRange(for_expr->GetParseLocationRange());

  std::string no_grouping_type;
  if (!this->TypeSupportsGrouping(resolved_for_expr->get()->type(),
                                  &no_grouping_type)) {
    return MakeSqlErrorAt(for_expr)
           << "Type " << no_grouping_type
           << " cannot be used as a FOR expression because it is not groupable";
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveInClauseInPivotClause(
    const ASTPivotValueList* pivot_values, const NameScope* scope,
    const Type* for_expr_type,
    std::vector<std::unique_ptr<const ResolvedExpr>>* resolved_in_exprs) {
  for (const ASTPivotValue* ast_pivot_value : pivot_values->values()) {
    // Even though pivot values must be constant, we still resolve them in a
    // scope which includes columns from the input table; this way, when such
    // columns are referenced, you get an error message about the pivot value
    // not being constant vs. the column not existing.
    QueryResolutionInfo query_resolution_info(this);
    ExprResolutionInfo info(scope, scope,
                            /*allows_aggregation_in=*/false,
                            /*allows_analytic_in=*/false,
                            /*use_post_grouping_columns_in=*/false, "IN clause",
                            &query_resolution_info);

    std::unique_ptr<const ResolvedExpr> resolved_in_expr;
    ZETASQL_RETURN_IF_ERROR(
        ResolveExpr(ast_pivot_value->value(), &info, &resolved_in_expr));
    ZETASQL_ASSIGN_OR_RETURN(bool resolved_in_expr_is_constant_expr,
                     IsConstantExpression(resolved_in_expr.get()));
    if (!resolved_in_expr_is_constant_expr) {
      return MakeSqlErrorAt(ast_pivot_value->value())
             << "IN expression in PIVOT clause must be constant";
    }

    ZETASQL_RETURN_IF_ERROR(CoerceExprToType(ast_pivot_value->value(), for_expr_type,
                                     kImplicitCoercion,
                                     "PIVOT IN list item must be type $0 to "
                                     "match the PIVOT FOR expression; found $1",
                                     &resolved_in_expr));

    resolved_in_exprs->push_back(std::move(resolved_in_expr));
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolvePivotClause(
    std::unique_ptr<const ResolvedScan> input_scan,
    std::shared_ptr<const NameList> input_name_list,
    const NameScope* previous_scope, bool input_is_subquery,
    const ASTPivotClause* ast_pivot_clause,
    std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  if (!language().LanguageFeatureEnabled(FEATURE_V_1_3_PIVOT)) {
    return MakeSqlErrorAt(ast_pivot_clause) << "PIVOT is not supported";
  }

  if (in_strict_mode() && !input_is_subquery) {
    // PIVOT treats all unreferenced columns in the input table as GROUP BY
    // columns, allowing it to effectively function like "SELECT *". So, we
    // allow PIVOT in strict mode only when the input is a subquery. Since
    // strict mode require subqueries to explicitly state their columns in the
    // SELECT list, all of the grouping columns are explicitly stated in the
    // query and resiliant to new columns being added to underlying tables.
    return MakeSqlErrorAt(ast_pivot_clause)
           << "Input to PIVOT must be a subquery in strict name resolution "
              "mode";
  }

  if (input_name_list->HasValueTableColumns()) {
    return MakeSqlErrorAt(ast_pivot_clause)
           << "PIVOT is not allowed on value tables";
  }

  // Resolve pivot expressions
  QueryResolutionInfo query_resolution_info(this);
  auto pivot_scope =
      absl::make_unique<NameScope>(previous_scope, input_name_list);
  std::vector<std::unique_ptr<const ResolvedExpr>> pivot_expr_columns;
  ZETASQL_RETURN_IF_ERROR(ResolvePivotExpressions(
      ast_pivot_clause->pivot_expressions(), pivot_scope.get(),
      &pivot_expr_columns, query_resolution_info));

  // If one or more of the pivot expressions contains an ORDER BY clause,
  // wrap the input scan with a ProjectScan that computes the order-by columns.
  MaybeAddProjectForComputedColumns(
      query_resolution_info
          .release_select_list_columns_to_compute_before_aggregation(),
      &input_scan);

  // Resolve FOR expression
  std::unique_ptr<const ResolvedExpr> resolved_for_expr;
  ZETASQL_RETURN_IF_ERROR(
      ResolveForExprInPivotClause(ast_pivot_clause->for_expression(),
                                  pivot_scope.get(), &resolved_for_expr));

  // Resolve IN expressions.
  std::vector<std::unique_ptr<const ResolvedExpr>> resolved_in_exprs;
  ZETASQL_RETURN_IF_ERROR(ResolveInClauseInPivotClause(
      ast_pivot_clause->pivot_values(), pivot_scope.get(),
      resolved_for_expr->type(), &resolved_in_exprs));

  // Determine final column list, starting with the grouping columns.
  auto final_name_list = std::make_shared<NameList>();
  absl::flat_hash_set<ResolvedColumn> referenced_columns;
  FindReferencedColumnsVisitor visitor(&referenced_columns);
  for (const auto& pivot_expr : pivot_expr_columns) {
    ZETASQL_RETURN_IF_ERROR(pivot_expr->Accept(&visitor));
  }
  ZETASQL_RETURN_IF_ERROR(resolved_for_expr->Accept(&visitor));

  std::vector<std::unique_ptr<const ResolvedComputedColumn>> group_by_list;
  std::vector<ResolvedColumn> output_column_list;
  std::vector<std::unique_ptr<const ResolvedPivotColumn>>
      output_column_detail_list;
  output_column_list.reserve(input_scan->column_list().size());
  for (int i = 0; i < input_name_list->num_columns(); ++i) {
    const NamedColumn& named_col = input_name_list->column(i);
    const ResolvedColumn& col = named_col.column;
    if (!referenced_columns.contains(col)) {
      std::string no_grouping_type;
      if (!TypeSupportsGrouping(col.type(), &no_grouping_type)) {
        return MakeSqlErrorAt(ast_pivot_clause)
               << "Column "
               << ColumnAliasOrPosition(named_col.name, i)
               << " of type " << no_grouping_type
               << " cannot be used as an implicit grouping column of a PIVOT "
                  "clause";
      }

      // Columns from the input table not referenced in either a pivot
      // expression or FOR expression are considered implicit group-by columns.
      ResolvedColumn output_col(AllocateColumnId(), kGroupById, named_col.name,
                                col.annotated_type());
      output_column_list.push_back(output_col);

      group_by_list.push_back(
          MakeResolvedComputedColumn(output_col, MakeColumnRef(col)));

      ZETASQL_RETURN_IF_ERROR(final_name_list->AddColumn(named_col.name, output_col,
                                                 /*is_explicit=*/true));
    }
  }

  // Add a pivot column for each pivot-expr/pivot-value combination.
  output_column_list.reserve(resolved_in_exprs.size() *
                             pivot_expr_columns.size());
  for (int i = 0; i < resolved_in_exprs.size(); ++i) {
    for (int j = 0; j < pivot_expr_columns.size(); ++j) {
      ZETASQL_ASSIGN_OR_RETURN(
          ResolvedColumn pivot_column,
          CreatePivotColumn(
              ast_pivot_clause->pivot_expressions()->expressions()[j],
              pivot_expr_columns[j].get(),
              /*is_only_pivot_expr=*/pivot_expr_columns.size() == 1,
              ast_pivot_clause->pivot_values()->values()[i],
              resolved_in_exprs[i].get()));
      output_column_detail_list.push_back(
          MakeResolvedPivotColumn(pivot_column,
                                  /*pivot_expr_index=*/j,
                                  /*pivot_value_index=*/i));
      output_column_list.push_back(pivot_column);
      ZETASQL_RETURN_IF_ERROR(final_name_list->AddColumn(
          pivot_column.name_id(), pivot_column, /*is_explicit=*/true));
    }
  }

  *output_name_list = final_name_list;
  if (ast_pivot_clause->output_alias() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(*output_name_list,
                     NameList::AddRangeVariableInWrappingNameList(
                         ast_pivot_clause->output_alias()->GetAsIdString(),
                         ast_pivot_clause->output_alias(), *output_name_list));
  }

  *output = MakeResolvedPivotScan(
      output_column_list, std::move(input_scan), std::move(group_by_list),
      std::move(pivot_expr_columns), std::move(resolved_for_expr),
      std::move(resolved_in_exprs), std::move(output_column_detail_list));
  analyzer_output_properties_.MarkRelevant(REWRITE_PIVOT);

  return absl::OkStatus();
}

absl::Status Resolver::ResolveUnpivotOutputValueColumns(
    const ASTPathExpressionList* ast_unpivot_expr_list,
    std::vector<ResolvedColumn>* unpivot_value_columns,
    const std::vector<const Type*>& value_column_types,
    const NameScope* scope) {
  QueryResolutionInfo query_resolution_info(this);
  ExprResolutionInfo info(scope, scope,
                          /*allows_aggregation_in=*/false,
                          /*allows_analytic_in=*/false,
                          /*use_post_grouping_columns_in=*/false,
                          "UNPIVOT clause", &query_resolution_info);

  int column_index = 0;
  if (value_column_types.size() !=
      ast_unpivot_expr_list->path_expression_list().size()) {
    return MakeSqlErrorAt(ast_unpivot_expr_list)
           << "The number of new columns introduced as value columns must be "
              "the same as the number of columns in the column groups of "
              "UNPIVOT IN clause";
  }
  for (const ASTPathExpression* unpivot_column :
       ast_unpivot_expr_list->path_expression_list()) {
    if (unpivot_column->num_names() > 1) {
      return MakeSqlErrorAt(unpivot_column)
             << "Only names of the new columns are accepted as value columns "
                "in UNPIVOT. Qualified names are not allowed";
    }
    IdString column_name = unpivot_column->first_name()->GetAsIdString();
    ResolvedColumn unpivot_resolved_column(
        AllocateColumnId(), /*table_name=*/kUnpivotColumnId, column_name,
        AnnotatedType(value_column_types.at(column_index),
                      /*annotation_map=*/nullptr));
    column_index++;
    unpivot_value_columns->push_back(unpivot_resolved_column);
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveUnpivotInClause(
    const ASTUnpivotInItemList* ast_unpivot_expr_list,
    std::vector<std::unique_ptr<const ResolvedUnpivotArg>>* resolved_in_items,
    const std::vector<ResolvedColumn>* input_scan_columns,
    absl::flat_hash_set<ResolvedColumn>* in_clause_input_columns,
    std::vector<const Type*>* value_column_types, const Type** label_type,
    std::vector<std::unique_ptr<const ResolvedLiteral>>* resolved_label_list,
    const ASTUnpivotClause* ast_unpivot_clause, const NameScope* scope) {
  QueryResolutionInfo query_resolution_info(this);
  ExprResolutionInfo info(scope, scope,
                          /*allows_aggregation_in=*/false,
                          /*allows_analytic_in=*/false,
                          /*use_post_grouping_columns_in=*/false, "IN clause",
                          &query_resolution_info);

  absl::flat_hash_set<ResolvedColumn> input_columns;
  input_columns.insert(input_scan_columns->begin(), input_scan_columns->end());
  absl::flat_hash_set<ResolvedColumn> input_columns_seen;
  TypeKind label_type_kind = TYPE_UNKNOWN;
  // The first column group is used to determine the size and order of datatypes
  // of the column groups in IN clause.
  bool is_first_column_group = true;
  for (const ASTUnpivotInItem* in_column_list :
       ast_unpivot_expr_list->in_items()) {
    // IN clause contains nested columns, i.e. groups of columns inside a list,
    // Example .. FOR .. IN( (a , b) , ( c , d ) ).
    // in_column_group will contain the columns lists resolved from inner groups
    // and the resulting list will be pushed onto the resolved_in_items vector,
    // in the same order as they appear in the IN clause.
    std::vector<std::unique_ptr<const ResolvedColumnRef>> in_column_group;
    in_column_group.reserve(
        in_column_list->unpivot_columns()->path_expression_list().size());

    if (!is_first_column_group &&
        value_column_types->size() !=
            in_column_list->unpivot_columns()->path_expression_list().size()) {
      return MakeSqlErrorAt(in_column_list->unpivot_columns())
             << "All column groups in UNPIVOT IN clause must have the same "
                "number of columns";
    }
    int column_index = 0;
    for (const ASTPathExpression* in_column :
         in_column_list->unpivot_columns()->path_expression_list()) {
      // Get resolved column for column in the IN clause.
      std::unique_ptr<const ResolvedExpr> resolved_expr_out;
      const ASTExpression* in_column_expr = in_column->GetAs<ASTExpression>();
      ZETASQL_RETURN_IF_ERROR(ResolveExpr(in_column_expr, &info, &resolved_expr_out));
      std::unique_ptr<const ResolvedColumnRef> in_column_ref;
      if (resolved_expr_out->node_kind() != RESOLVED_COLUMN_REF) {
        return MakeSqlErrorAt(in_column)
               << "UNPIVOT IN clause cannot have expressions, only column "
                  "names are allowed";
      }
      in_column_ref.reset(
          resolved_expr_out.release()->GetAs<ResolvedColumnRef>());
      const Type* datatype = in_column_ref->type();
      // The first column group is used to determine the datatype order and
      // number of columns for all other column groups.
      if (is_first_column_group) {
        value_column_types->push_back(std::move(datatype));
      } else if (!datatype->Equals(value_column_types->at(column_index))) {
        ZETASQL_RET_CHECK_LT(column_index, value_column_types->size());
        return MakeSqlErrorAt(in_column)
               << "The datatype of column does not match with other datatypes "
                  "in the IN clause. Expected "
               << Type::TypeKindToString(
                      value_column_types->at(column_index)->kind(),
                      product_mode())
               << ", Found "
               << Type::TypeKindToString(datatype->kind(), product_mode());
      }
      column_index++;
      if (!input_columns.contains(in_column_ref->column())) {
        return MakeSqlErrorAt(in_column)
               << "Correlated column references in UNPIVOT IN clause is not "
                  "allowed";
      }
      if (input_columns_seen.contains(in_column_ref->column())) {
        // TODO: For queries where same input table column is
        // mapped to different columns in the unpivot input subquery, we might
        // want to allow those columns in the UNPIVOT IN clause.
        // Example, SELECT * FROM (SELECT x as a, x as b
        //            FROM t) UNPIVOT (y for z IN ( a , b ));
        return MakeSqlErrorAt(in_column)
               << "Column names in UNPIVOT IN clause cannot be repeated";
      }
      input_columns_seen.insert(in_column_ref->column());
      in_clause_input_columns->insert(in_column_ref->column());
      in_column_group.push_back(std::move(in_column_ref));
    }
    std::unique_ptr<const ResolvedUnpivotArg> col_set =
        MakeResolvedUnpivotArg(std::move(in_column_group));
    resolved_in_items->push_back(std::move(col_set));

    // Each column group can have an explicitly provided alias (after AS).
    // If the alias is not provided, a string label is auto-generated by
    // concatenating column names from the column group. The datatypes of all
    // labels must be the same and hence they must all be provided in the case
    // of non-string(integer) labels.
    ZETASQL_ASSIGN_OR_RETURN(Value label,
                     GetLabelForUnpivotInColumnList(in_column_list));
    if (label_type_kind == TYPE_UNKNOWN) {
      label_type_kind = label.type_kind();
    } else if (label_type_kind != label.type_kind()) {
      return MakeSqlErrorAt(in_column_list)
             << "All UNPIVOT labels must be the same type; when non-string "
                "labels are present then all labels must be explicitly "
                "provided";
    }
    resolved_label_list->push_back(MakeResolvedLiteralWithoutLocation(label));
    is_first_column_group = false;
  }
  *label_type = types::TypeFromSimpleTypeKind(label_type_kind);
  return absl::OkStatus();
}

absl::StatusOr<Value> Resolver::GetLabelForUnpivotInColumnList(
    const ASTUnpivotInItem* in_column_list) {
  if (in_column_list->alias() == nullptr ||
      in_column_list->alias()->label() == nullptr) {
    std::string column_group_label = "";
    for (const ASTPathExpression* in_col :
         in_column_list->unpivot_columns()->path_expression_list()) {
      if (column_group_label.empty()) {
        column_group_label = in_col->last_name()->GetAsString();
      } else {
        absl::StrAppend(&column_group_label, "_",
                        in_col->last_name()->GetAsString());
      }
    }
    return Value::String(column_group_label);
  }

  if (in_column_list->alias()->label()->node_kind() == AST_STRING_LITERAL) {
    return Value::String(in_column_list->alias()
                             ->label()
                             ->GetAsOrNull<ASTStringLiteral>()
                             ->string_value());
  } else if (in_column_list->alias()->label()->node_kind() == AST_INT_LITERAL) {
    Value label;
    if (!Value::ParseInteger(in_column_list->alias()
                                 ->label()
                                 ->GetAsOrNull<ASTIntLiteral>()
                                 ->image(),
                             &label)) {
      return MakeSqlErrorAt(in_column_list->alias()->label())
             << "Invalid integer label for UNPIVOT clause";
    }
    return label;
  }
  // The parser only accepts integer and string literal so we would
  // never get here, but added for sanity check.
  return MakeSqlErrorAt(in_column_list)
         << "UNPIVOT column labels must be string or integer";
}

absl::Status Resolver::ResolveUnpivotClause(
    std::unique_ptr<const ResolvedScan> input_scan,
    std::shared_ptr<const NameList> input_name_list,
    const NameScope* previous_scope, const ASTUnpivotClause* ast_unpivot_clause,
    std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  if (!language().LanguageFeatureEnabled(FEATURE_V_1_3_UNPIVOT)) {
    return MakeSqlErrorAt(ast_unpivot_clause) << "UNPIVOT is not supported";
  }

  if (input_name_list->HasValueTableColumns()) {
    return MakeSqlErrorAt(ast_unpivot_clause)
           << "UNPIVOT is not allowed on value tables";
  }

  auto unpivot_scope =
      absl::make_unique<NameScope>(previous_scope, input_name_list);

  // Resolve IN columns.
  std::vector<std::unique_ptr<const ResolvedUnpivotArg>> resolved_in_items;
  absl::flat_hash_set<ResolvedColumn> in_clause_input_columns;
  std::vector<const Type*> value_column_types;
  std::vector<std::unique_ptr<const ResolvedLiteral>> resolved_label_list;
  const Type* label_type = nullptr;
  ZETASQL_RETURN_IF_ERROR(ResolveUnpivotInClause(
      ast_unpivot_clause->unpivot_in_items(), &resolved_in_items,
      &input_scan->column_list(), &in_clause_input_columns, &value_column_types,
      &label_type, &resolved_label_list, ast_unpivot_clause,
      unpivot_scope.get()));

  // Resolve unpivot output value columns.
  std::vector<ResolvedColumn> unpivot_value_columns;
  ZETASQL_RETURN_IF_ERROR(ResolveUnpivotOutputValueColumns(
      ast_unpivot_clause->unpivot_output_value_columns(),
      &unpivot_value_columns, value_column_types, unpivot_scope.get()));

  // Resolve unpivot output name column.
  if (ast_unpivot_clause->unpivot_output_name_column()->num_names() > 1) {
    return MakeSqlErrorAt(ast_unpivot_clause->unpivot_output_name_column())
           << "Only name of the new column is accepted as label column in "
              "UNPIVOT. Qualified names are not allowed";
  }
  IdString column_name = ast_unpivot_clause->unpivot_output_name_column()
                             ->first_name()
                             ->GetAsIdString();
  ResolvedColumn unpivot_label_column(
      AllocateColumnId(), /*table_name=*/kUnpivotColumnId, column_name,
      AnnotatedType(label_type, /*annotation_map=*/nullptr));

  // Create a list of output columns. It would contain:
  // Input columns that are not present in the IN clause, in the order as they
  // appear in the <input_scan> + unpivot_value_columns + unpivot_label_column
  std::vector<ResolvedColumn> output_column_list;
  auto final_name_list = std::make_shared<NameList>();

  // Columns from the input table that are not unpivoted in IN clause are
  // projected in the output as a new computed column.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      projected_input_column_list;
  for (int i = 0; i < input_name_list->num_columns(); ++i) {
    const NamedColumn& named_col = input_name_list->column(i);
    const ResolvedColumn& col = named_col.column;
    if (!in_clause_input_columns.contains(col)) {
      ResolvedColumn output_col(AllocateColumnId(),
                                /*table_name=*/kUnpivotColumnId, named_col.name,
                                col.type());
      output_column_list.push_back(output_col);
      projected_input_column_list.push_back(
          MakeResolvedComputedColumn(output_col, MakeColumnRef(col)));
      ZETASQL_RETURN_IF_ERROR(final_name_list->AddColumn(named_col.name, output_col,
                                                 /*is_explicit=*/true));
    }
  }
  for (int i = 0; i < unpivot_value_columns.size(); i++) {
    output_column_list.push_back(unpivot_value_columns[i]);
    ZETASQL_RETURN_IF_ERROR(final_name_list->AddColumn(
        unpivot_value_columns[i].name_id(), unpivot_value_columns[i], true));
  }
  output_column_list.push_back(unpivot_label_column);
  ZETASQL_RETURN_IF_ERROR(final_name_list->AddColumn(unpivot_label_column.name_id(),
                                             unpivot_label_column, true));

  *output_name_list = final_name_list;

  if (ast_unpivot_clause->output_alias() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        *output_name_list,
        NameList::AddRangeVariableInWrappingNameList(
            ast_unpivot_clause->output_alias()->GetAsIdString(),
            ast_unpivot_clause->output_alias(), *output_name_list));
  }

  bool include_nulls = false;
  if (ast_unpivot_clause->null_filter() == ASTUnpivotClause::kInclude) {
    include_nulls = true;
  }

  *output = MakeResolvedUnpivotScan(
      output_column_list, std::move(input_scan),
      std::move(unpivot_value_columns), std::move(unpivot_label_column),
      std::move(resolved_label_list), std::move(resolved_in_items),
      std::move(projected_input_column_list), include_nulls);
  analyzer_output_properties_.MarkRelevant(REWRITE_UNPIVOT);
  return absl::OkStatus();
}

// This is a self-contained table expression.  It can be an UNNEST, but
// only as a leaf - not one that has to wrap another scan and flatten it.
absl::Status Resolver::ResolveTablePathExpression(
    const ASTTablePathExpression* table_ref, const NameScope* scope,
    std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  const ASTForSystemTime* for_system_time = table_ref->for_system_time();
  if (for_system_time != nullptr &&
      !language().LanguageFeatureEnabled(FEATURE_V_1_1_FOR_SYSTEM_TIME_AS_OF)) {
    return MakeSqlErrorAt(for_system_time)
           << "FOR SYSTEM_TIME AS OF is not supported";
  }

  if (ShouldResolveAsArrayScan(table_ref, scope)) {
    if (for_system_time != nullptr) {
      return MakeSqlErrorAt(for_system_time)
             << "FOR SYSTEM_TIME AS OF is not allowed with array scans";
    }
    if (table_ref->pivot_clause() != nullptr) {
      return MakeSqlErrorAt(table_ref->pivot_clause())
             << "PIVOT is not allowed with array scans";
    }

    if (table_ref->unpivot_clause() != nullptr) {
      return MakeSqlErrorAt(table_ref->unpivot_clause())
             << "UNPIVOT is not allowed with array scans";
    }
    std::unique_ptr<const ResolvedScan> no_lhs_scan;
    return ResolveArrayScan(
        table_ref, nullptr /* on_clause */, nullptr /* using_clause */,
        nullptr /* ast_join */, false /* is_outer_scan */,
        &no_lhs_scan /* resolved_lhs_scan */, nullptr /* name_list_lhs */,
        scope, output, output_name_list);
  }

  if (table_ref->with_offset() != nullptr) {
    return MakeSqlErrorAt(table_ref)
           << "WITH OFFSET can only be used with array scans";
  }

  const ASTPathExpression* path_expr = table_ref->path_expr();
  ZETASQL_RET_CHECK(path_expr != nullptr);

  IdString alias;
  bool has_explicit_alias;
  const ASTNode* alias_location;
  if (table_ref->alias() != nullptr) {
    alias = table_ref->alias()->GetAsIdString();
    alias_location = table_ref->alias();
    has_explicit_alias = true;
  } else {
    alias = GetAliasForExpression(path_expr);
    alias_location = table_ref;
    has_explicit_alias = false;
  }
  ZETASQL_RET_CHECK(!alias.empty());

  std::shared_ptr<const NameList> name_list;
  std::unique_ptr<const ResolvedScan> this_scan;
  if (named_subquery_map_.contains(path_expr->ToIdStringVector())) {
    if (for_system_time != nullptr) {
      return MakeSqlErrorAt(for_system_time) << "FOR SYSTEM_TIME AS OF cannot "
                                                "be used with tables defined "
                                                "in WITH clause";
    }
    ZETASQL_RETURN_IF_ERROR(ResolveNamedSubqueryRef(path_expr, table_ref->hint(),
                                            &this_scan, &name_list));

    if (name_list->is_value_table()) {
      ZETASQL_RETURN_IF_ERROR(ConvertValueTableNameListToNameListWithValueTable(
          table_ref, alias, name_list, &name_list));
    } else {
      // Add a range variable for the with_ref scan.
      ZETASQL_ASSIGN_OR_RETURN(name_list, NameList::AddRangeVariableInWrappingNameList(
                                      alias, alias_location, name_list));
    }
  } else if (path_expr->num_names() == 1 &&
             function_argument_info_ != nullptr &&
             function_argument_info_->FindTableArg(
                 path_expr->first_name()->GetAsIdString()) != nullptr) {
    if (for_system_time != nullptr) {
      return MakeSqlErrorAt(for_system_time)
             << "FOR SYSTEM_TIME AS OF cannot be used with TABLE parameter "
             << path_expr->first_name()->GetAsIdString() << " to FUNCTION";
    }
    ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsFunctionTableArgument(
        path_expr, table_ref->hint(), alias, alias_location, &this_scan,
        &name_list));
  } else {
    // The (possibly multi-part) table name did not match the WITH clause or a
    // table-valued argument (which only support single-part names), so try to
    // resolve this name as a Table from the Catalog.
    std::unique_ptr<const ResolvedTableScan> table_scan;
    ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsTableScan(
        path_expr, alias, has_explicit_alias, alias_location, table_ref->hint(),
        for_system_time, scope, &table_scan, &name_list));
    this_scan = std::move(table_scan);
  }
  ZETASQL_RET_CHECK(this_scan != nullptr);
  ZETASQL_RET_CHECK(name_list != nullptr);

  if (table_ref->pivot_clause() != nullptr) {
    ZETASQL_RET_CHECK_EQ(for_system_time, nullptr)
        << "Parser should not allow PIVOT and FOR SYSTEM TIME AS OF to coexist";
    ZETASQL_RETURN_IF_ERROR(ResolvePivotClause(std::move(this_scan), name_list, scope,
                                       /*input_is_subquery=*/false,
                                       table_ref->pivot_clause(), &this_scan,
                                       &name_list));
  }
  if (table_ref->unpivot_clause() != nullptr) {
    ZETASQL_RET_CHECK_EQ(for_system_time, nullptr)
        << "Parser should not allow UNPIVOT and FOR SYSTEM TIME AS OF to "
           "coexist";
    ZETASQL_RETURN_IF_ERROR(ResolveUnpivotClause(std::move(this_scan), name_list, scope,
                                         table_ref->unpivot_clause(),
                                         &this_scan, &name_list));
  }
  if (table_ref->sample_clause() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveTablesampleClause(table_ref->sample_clause(),
                                             &name_list, &this_scan));
  }

  *output_name_list = name_list;
  *output = std::move(this_scan);
  return absl::OkStatus();
}

absl::Status Resolver::ResolvePathExpressionAsFunctionTableArgument(
    const ASTPathExpression* path_expr, const ASTHint* hint, IdString alias,
    const ASTNode* ast_location, std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  std::shared_ptr<const NameList> name_list;
  std::unique_ptr<const ResolvedScan> this_scan;
  ZETASQL_RET_CHECK_NE(function_argument_info_, nullptr);
  // The path refers to a relation argument in a CREATE TABLE FUNCTION
  // statement. Create and return a resolved relation argument reference.
  const FunctionArgumentInfo::ArgumentDetails* details =
      function_argument_info_->FindTableArg(
          path_expr->first_name()->GetAsIdString());
  ZETASQL_RET_CHECK_NE(details, nullptr);
  // We do not expect to analyze a template function body until the function is
  // invoked and we have concrete argument types. If a template type is found
  // that could mean engine code is using an API incorrectly.
  ZETASQL_RET_CHECK(!details->arg_type.IsTemplated())
      << "Function bodies cannot be resolved with templated argument types";
  const TVFRelation& tvf_relation =
      details->arg_type.options().relation_input_schema();
  std::unique_ptr<NameList> new_name_list(new NameList);
  std::vector<ResolvedColumn> resolved_columns;
  if (tvf_relation.is_value_table()) {
    ZETASQL_RET_CHECK_EQ(1, tvf_relation.num_columns());
    resolved_columns.push_back(ResolvedColumn(
        AllocateColumnId(), path_expr->first_name()->GetAsIdString(),
        kValueColumnId, tvf_relation.column(0).type));
    ZETASQL_RETURN_IF_ERROR(new_name_list->AddValueTableColumn(
        alias, resolved_columns[0], path_expr));
    new_name_list->set_is_value_table(true);
    name_list = std::move(new_name_list);
  } else {
    resolved_columns.reserve(tvf_relation.num_columns());
    for (const TVFRelation::Column& column : tvf_relation.columns()) {
      resolved_columns.push_back(ResolvedColumn(
          AllocateColumnId(),
          id_string_pool_->Make(path_expr->first_name()->GetAsString()),
          id_string_pool_->Make(column.name), column.type));
      ZETASQL_RETURN_IF_ERROR(new_name_list->AddColumn(
          resolved_columns.back().name_id(), resolved_columns.back(),
          true /* is_explicit */));
    }
    name_list = std::move(new_name_list);
    // Add a range variable for the TVF relation argument scan.
    ZETASQL_ASSIGN_OR_RETURN(name_list, NameList::AddRangeVariableInWrappingNameList(
                                    alias, ast_location, name_list));
  }
  auto relation_argument_scan = MakeResolvedRelationArgumentScan(
      resolved_columns, path_expr->first_name()->GetAsString(),
      tvf_relation.is_value_table());
  if (hint != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveHintsForNode(hint, relation_argument_scan.get()));
  }
  MaybeRecordParseLocation(path_expr, relation_argument_scan.get());
  this_scan = std::move(relation_argument_scan);

  *output_name_list = name_list;
  *output = std::move(this_scan);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveTableSubquery(
    const ASTTableSubquery* table_ref, const NameScope* scope,
    std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  // Table subqueries cannot have correlated references to tables earlier in
  // the same FROM clause, but should still be able to see correlated names
  // from outer queries.  This is handled by the caller, who passes us the
  // external name scope excluding any local aliases.

  IdString alias;
  if (table_ref->alias() != nullptr) {
    alias = table_ref->alias()->GetAsIdString();
  } else {
    alias = AllocateSubqueryName();
  }
  ZETASQL_RET_CHECK(!alias.empty());

  std::unique_ptr<const ResolvedScan> resolved_subquery;
  std::shared_ptr<const NameList> subquery_name_list;
  ZETASQL_RETURN_IF_ERROR(ResolveQuery(table_ref->subquery(), scope, alias,
                               false /* is_outer_query */, &resolved_subquery,
                               &subquery_name_list));
  ZETASQL_RET_CHECK(nullptr != subquery_name_list);

  // A table subquery never preserves order, so we clear is_ordered on the
  // final scan of the subquery, even if it was a ResolvedOrderByScan.
  const_cast<ResolvedScan*>(resolved_subquery.get())->set_is_ordered(false);

  if (subquery_name_list->is_value_table()) {
    ZETASQL_RET_CHECK_EQ(subquery_name_list->num_columns(), 1);
    ZETASQL_RETURN_IF_ERROR(ConvertValueTableNameListToNameListWithValueTable(
        table_ref, alias, subquery_name_list, output_name_list));
  } else {
    *output_name_list = subquery_name_list;
    // Generated names should not be added as visible aliases.
    if (table_ref->alias() != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(*output_name_list,
                       NameList::AddRangeVariableInWrappingNameList(
                           alias, table_ref->alias(), *output_name_list));
    }
  }

  if (table_ref->pivot_clause() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolvePivotClause(
        std::move(resolved_subquery), *output_name_list, scope,
        /*input_is_subquery=*/true, table_ref->pivot_clause(),
        &resolved_subquery, output_name_list));
  }

  if (table_ref->unpivot_clause() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveUnpivotClause(
        std::move(resolved_subquery), *output_name_list, scope,
        table_ref->unpivot_clause(), &resolved_subquery, output_name_list));
  }
  if (table_ref->sample_clause() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveTablesampleClause(
        table_ref->sample_clause(), output_name_list, &resolved_subquery));
  }

  *output = std::move(resolved_subquery);
  return absl::OkStatus();
}

// Validates the parameter to PERCENT. It can either be a literal or a
// parameter. In either case, its type must be a double or an int64_t.
static absl::Status CheckPercentIsValid(
    const ASTNode* ast_location,
    const std::unique_ptr<const ResolvedExpr>& expr) {
  if ((expr->node_kind() != RESOLVED_PARAMETER &&
       expr->node_kind() != RESOLVED_LITERAL) ||
      (!expr->type()->IsInt64() && !expr->type()->IsDouble())) {
    return MakeSqlErrorAt(ast_location)
           << "PERCENT expects either a double or an integer literal or "
              "parameter";
  }

  if (expr->node_kind() == RESOLVED_LITERAL) {
    // If a literal, we can also validate its value.
    const Value value = expr->GetAs<ResolvedLiteral>()->value();
    bool is_valid = false;
    if (value.type()->IsInt64()) {
      is_valid = (!value.is_null() && value.int64_value() >= 0 &&
                  value.int64_value() <= 100);
    } else {
      ZETASQL_RET_CHECK(value.type()->IsDouble());
      is_valid = (!value.is_null() && value.double_value() >= 0.0 &&
                  value.double_value() <= 100.0);
    }
    if (!is_valid) {
      return MakeSqlErrorAt(ast_location)
             << "PERCENT value must be in the range [0, 100]";
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveTablesampleClause(
    const ASTSampleClause* sample_clause,
    std::shared_ptr<const NameList>* current_name_list,
    std::unique_ptr<const ResolvedScan>* current_scan) {
  if (!language().LanguageFeatureEnabled(FEATURE_TABLESAMPLE)) {
    return MakeSqlErrorAt(sample_clause) << "TABLESAMPLE not supported";
  }

  ZETASQL_RET_CHECK(sample_clause->sample_method() != nullptr);
  const ASTIdentifier* method = sample_clause->sample_method();

  ZETASQL_RET_CHECK(sample_clause->sample_size() != nullptr);
  std::unique_ptr<const ResolvedExpr> resolved_size;
  const ASTExpression* size = sample_clause->sample_size()->size();
  static constexpr char kTablesampleClause[] = "TABLESAMPLE clause";
  ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(size, empty_name_scope_.get(),
                                    kTablesampleClause, &resolved_size));

  const int unit = sample_clause->sample_size()->unit();
  ZETASQL_RET_CHECK(unit == ASTSampleSize::ROWS || unit == ASTSampleSize::PERCENT)
      << unit;
  const ResolvedSampleScan::SampleUnit resolved_unit =
      (unit == ASTSampleSize::ROWS) ? ResolvedSampleScan::ROWS
                                    : ResolvedSampleScan::PERCENT;
  if (resolved_unit == ResolvedSampleScan::ROWS) {
    ZETASQL_RETURN_IF_ERROR(ValidateParameterOrLiteralAndCoerceToInt64IfNeeded(
        "ROWS" /* clause_name */, size, &resolved_size));
  } else {
    ZETASQL_RETURN_IF_ERROR(CheckPercentIsValid(size, resolved_size));
  }

  std::vector<std::unique_ptr<const ResolvedExpr>> partition_by_list;
  const NameScope name_scope(**current_name_list);
  QueryResolutionInfo query_info(this);
  if (sample_clause->sample_size()->partition_by() != nullptr) {
    if (!language().LanguageFeatureEnabled(
            FEATURE_STRATIFIED_RESERVOIR_TABLESAMPLE)) {
      return MakeSqlErrorAt(sample_clause->sample_size()->partition_by())
             << "TABLESAMPLE does not support the PARTITION BY clause";
    }
    const std::string method = sample_clause->sample_method()->GetAsString();
    if (!zetasql_base::CaseEqual(method, "reservoir")) {
      return MakeSqlErrorAt(sample_clause->sample_size()->partition_by())
             << "The TABLESAMPLE " << method
             << " method does not support PARTITION BY. "
             << "Remove PARTITION BY, or use the TABLESAMPLE RESERVOIR method";
    }
    if (resolved_unit != ResolvedSampleScan::ROWS) {
      return MakeSqlErrorAt(sample_clause->sample_size()->partition_by())
             << "TABLESAMPLE with PERCENT does not support PARTITION BY. "
             << "Remove PARTITION BY, or use TABLESAMPLE RESERVOIR with ROWS";
    }
    ZETASQL_RETURN_IF_ERROR(ResolveCreateTablePartitionByList(
        sample_clause->sample_size()
            ->partition_by()
            ->partitioning_expressions(),
        PartitioningKind::PARTITION_BY, name_scope, &query_info,
        &partition_by_list));
  }

  std::unique_ptr<const ResolvedExpr> resolved_repeatable_argument;
  if (sample_clause->sample_suffix() != nullptr &&
      sample_clause->sample_suffix()->repeat() != nullptr) {
    ZETASQL_RET_CHECK(sample_clause->sample_suffix()->repeat()->argument() != nullptr);
    const ASTExpression* repeatable_argument =
        sample_clause->sample_suffix()->repeat()->argument();
    ZETASQL_RETURN_IF_ERROR(
        ResolveScalarExpr(repeatable_argument, empty_name_scope_.get(),
                          kTablesampleClause, &resolved_repeatable_argument));
    ZETASQL_RETURN_IF_ERROR(ValidateParameterOrLiteralAndCoerceToInt64IfNeeded(
        "REPEATABLE" /* clause_name */, repeatable_argument,
        &resolved_repeatable_argument));
  }

  // Resolve WITH WEIGHT if present.
  std::unique_ptr<ResolvedColumnHolder> weight_column;
  ResolvedColumnList output_column_list = (*current_scan)->column_list();
  if (sample_clause->sample_suffix() != nullptr &&
      sample_clause->sample_suffix()->weight() != nullptr) {
    // If the alias is NULL, we get "weight" as an implicit alias.
    const ASTAlias* with_weight_alias =
        sample_clause->sample_suffix()->weight()->alias();
    const IdString weight_alias =
        (with_weight_alias == nullptr ? kWeightAlias
                                      : with_weight_alias->GetAsIdString());

    const ResolvedColumn column(AllocateColumnId(),
                                /*table_name=*/kWeightId, /*name=*/weight_alias,
                                type_factory_->get_double());
    weight_column = MakeResolvedColumnHolder(column);
    output_column_list.push_back(weight_column->column());
    std::shared_ptr<NameList> name_list(new NameList);
    ZETASQL_RETURN_IF_ERROR(name_list->MergeFrom(**current_name_list, sample_clause));
    ZETASQL_RETURN_IF_ERROR(name_list->AddValueTableColumn(
        weight_alias, weight_column->column(),
        with_weight_alias != nullptr
            ? absl::implicit_cast<const ASTNode*>(with_weight_alias)
            : sample_clause->sample_suffix()->weight()));
    *current_name_list = name_list;
  }

  *current_scan = MakeResolvedSampleScan(
      output_column_list, std::move(*current_scan),
      absl::AsciiStrToLower(method->GetAsString()), std::move(resolved_size),
      resolved_unit, std::move(resolved_repeatable_argument),
      std::move(weight_column), std::move(partition_by_list));
  return absl::OkStatus();
}

// There's not much resolving left to do here.  We already know we have
// an identifier that matches a WITH subquery alias, so we build the
// ResolvedWithRefScan.
absl::Status Resolver::ResolveNamedSubqueryRef(
    const ASTPathExpression* table_path, const ASTHint* hint,
    std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  ZETASQL_RET_CHECK(table_path != nullptr);
  auto it = named_subquery_map_.find(table_path->ToIdStringVector());
  ZETASQL_RET_CHECK(it != named_subquery_map_.end() && !it->second.empty());
  const NamedSubquery* named_subquery = it->second.back().get();
  if (named_subquery == nullptr) {
    // This is possible only if we have a recursive reference from within the
    // non-recursive term of a recursive UNION. This is an error.
    return MakeSqlErrorAt(table_path)
           << "Recursive reference is not allowed in non-recursive UNION term";
  }

  // For each new column produced in the WithRefScan, we want to name it
  // using the WITH alias, not the original column name.  e.g. In
  //   WITH Q AS (SELECT Key K FROM KeyValue)
  //   SELECT * FROM Q;
  // we want to call the new column Q.K, not Q.Key.  Since the column_list
  // may not map 1:1 with select-list column names, we need to build a map.
  std::map<ResolvedColumn, IdString> with_column_to_alias;
  for (const NamedColumn& named_column : named_subquery->name_list->columns()) {
    zetasql_base::InsertIfNotPresent(&with_column_to_alias, named_column.column,
                            named_column.name);
  }

  // Make a new ResolvedColumn for each column from the WITH scan.
  // This is necessary so that if the WITH subquery is referenced twice,
  // we get distinct column names for each scan.
  ResolvedColumnList column_list;
  std::map<ResolvedColumn, ResolvedColumn> old_column_to_new_column;
  for (int i = 0; i < named_subquery->column_list.size(); ++i) {
    const ResolvedColumn& column = named_subquery->column_list[i];

    // Get the alias for the column produced by the WITH reference,
    // using the first alias for that column in the WITH subquery.
    // Every column in the column_list should correspond to at least column
    // in the WITH subquery's NameList.
    IdString new_column_alias;
    const IdString* found = zetasql_base::FindOrNull(with_column_to_alias, column);
    ZETASQL_RET_CHECK(found != nullptr) << column.DebugString();
    new_column_alias = *found;

    column_list.emplace_back(
        ResolvedColumn(AllocateColumnId(), named_subquery->unique_alias,
                       new_column_alias, column.annotated_type()));
    // Build mapping from WITH subquery column to the newly created column
    // for the WITH reference.
    old_column_to_new_column[column] = column_list.back();
    // We can't prune any columns from the ResolvedWithRefScan because they
    // need to match 1:1 with the column_list on the with subquery.
    RecordColumnAccess(column_list.back());
  }

  // Make a new NameList pointing at the new ResolvedColumns.
  std::shared_ptr<NameList> name_list(new NameList);
  for (const NamedColumn& named_column : named_subquery->name_list->columns()) {
    const ResolvedColumn& old_column = named_column.column;
    auto found_column = old_column_to_new_column.find(old_column);
    ZETASQL_RET_CHECK(found_column != old_column_to_new_column.end());
    const ResolvedColumn& new_column = found_column->second;

    ZETASQL_RETURN_IF_ERROR(name_list->AddColumn(named_column.name, new_column,
                                         named_column.is_explicit));
  }
  if (named_subquery->name_list->is_value_table()) {
    ZETASQL_RET_CHECK_EQ(name_list->num_columns(), 1);
    name_list->set_is_value_table(true);
  }

  std::unique_ptr<ResolvedScan> scan;
  if (named_subquery->is_recursive) {
    std::unique_ptr<ResolvedRecursiveRefScan> recursive_scan =
        MakeResolvedRecursiveRefScan(column_list);

    // Remember information about the recursive scan, which will be needed for
    // downstream validation checks.
    recursive_ref_info_[recursive_scan.get()] = RecursiveRefScanInfo{
        table_path,
        named_subquery->unique_alias,
    };
    scan = std::move(recursive_scan);
  } else {
    scan = MakeResolvedWithRefScan(column_list,
                                   named_subquery->unique_alias.ToString());
  }
  ZETASQL_RETURN_IF_ERROR(ResolveHintsForNode(hint, scan.get()));

  *output = std::move(scan);
  *output_name_list = name_list;
  return absl::OkStatus();
}

absl::Status Resolver::ResolveColumnInUsing(
    const ASTIdentifier* ast_identifier, const NameList& name_list,
    const std::string& side_name, IdString key_name,
    ResolvedColumn* found_column,
    std::unique_ptr<const ResolvedExpr>* compute_expr_for_found_column) {
  compute_expr_for_found_column->reset();
  // <ast_identifier> and <found_column> are redundant but we pass the
  // string in to avoid doing extra string copy.
  ZETASQL_DCHECK_EQ(ast_identifier->GetAsIdString(), key_name);

  NameTarget found_name;
  if (!name_list.LookupName(key_name, &found_name)) {
    return MakeSqlErrorAt(ast_identifier)
           << "Column " << key_name << " in USING clause not found on "
           << side_name << " side of join";
  }
  if (in_strict_mode() && found_name.IsImplicit()) {
    return MakeSqlErrorAt(ast_identifier)
           << "Column name " << ToIdentifierLiteral(key_name)
           << " cannot be used without a qualifier in strict name resolution"
           << " mode. Use JOIN ON with a qualified name instead";
  }
  switch (found_name.kind()) {
    case NameTarget::ACCESS_ERROR:
      // An ACCESS_ERROR should not be possible in this context, since
      // ACCESS_ERROR NameTargets only exist during post-GROUP BY processing,
      // and the USING clause is evaluated pre-GROUP BY.
      ZETASQL_RET_CHECK_FAIL() << "Accessing column " << key_name
                       << " in USING clause is invalid";
    case NameTarget::AMBIGUOUS:
      return MakeSqlErrorAt(ast_identifier)
             << "Column " << key_name << " in USING clause is ambiguous on "
             << side_name << " side of join";
    case NameTarget::RANGE_VARIABLE:
      if (found_name.scan_columns()->is_value_table()) {
        ZETASQL_RET_CHECK_EQ(found_name.scan_columns()->num_columns(), 1);
        *found_column = found_name.scan_columns()->column(0).column;
        break;
      } else {
        return MakeSqlErrorAt(ast_identifier)
               << "Name " << key_name
               << " in USING clause is a table alias, not a column name, on "
               << side_name << " side of join";
      }
    case NameTarget::FIELD_OF: {
      // We have an implicit field access.  Make the ResolvedColumnRef for
      // the column and then resolve the field access on top.
      std::unique_ptr<const ResolvedExpr> resolved_get_field;
      // We don't auto-flatten for USING. It could make for matches that look
      // the same but come from different structure which would be weird.
      ZETASQL_RETURN_IF_ERROR(ResolveFieldAccess(
          MakeColumnRef(found_name.column_containing_field()), ast_identifier,
          ast_identifier, /*flatten_state=*/nullptr, &resolved_get_field));

      // Then create a new ResolvedColumn to store this result.
      *found_column = ResolvedColumn(
          AllocateColumnId(), MakeIdString(absl::StrCat("$join_", side_name)),
          key_name, resolved_get_field->annotated_type());

      *compute_expr_for_found_column = std::move(resolved_get_field);
      break;
    }
    case NameTarget::IMPLICIT_COLUMN:
    case NameTarget::EXPLICIT_COLUMN:
      *found_column = found_name.column();
      break;
  }
  return absl::OkStatus();
}

// static
absl::Status Resolver::MaybeAddJoinHintKeyword(const ASTJoin* ast_join,
                                               ResolvedScan* resolved_scan) {
  if (ast_join->join_hint() != ASTJoin::NO_JOIN_HINT) {
    // Convert HASH and LOOKUP to HASH_JOIN and LOOKUP_JOIN, respectively.
    absl::string_view hint;
    switch (ast_join->join_hint()) {
      case ASTJoin::HASH:
        hint = "HASH_JOIN";
        break;
      case ASTJoin::LOOKUP:
        hint = "LOOKUP_JOIN";
        break;
      case ASTJoin::NO_JOIN_HINT:
        ZETASQL_RET_CHECK_FAIL() << "Can't get here";
    }
    // Join hint keywords don't directly correspond to query text so we don't
    // record the parse location.
    resolved_scan->add_hint_list(MakeResolvedOption(
        "" /* qualifier */, "join_type",
        MakeResolvedLiteralWithoutLocation(Value::String(hint))));
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveUsing(
    const ASTUsingClause* using_clause, const NameList& name_list_lhs,
    const NameList& name_list_rhs, const ResolvedJoinScan::JoinType join_type,
    bool is_array_scan,
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
        lhs_computed_columns,
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
        rhs_computed_columns,
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
        computed_columns,
    NameList* output_name_list,
    std::unique_ptr<const ResolvedExpr>* join_condition) {
  ZETASQL_RET_CHECK(using_clause != nullptr);
  ZETASQL_RET_CHECK(computed_columns != nullptr);
  ZETASQL_RET_CHECK(output_name_list != nullptr);
  ZETASQL_RET_CHECK(join_condition != nullptr);

  IdStringSetCase column_names_emitted_by_using;

  std::vector<std::unique_ptr<const ResolvedExpr>> join_key_exprs;

  for (const ASTIdentifier* using_key : using_clause->keys()) {
    const IdString key_name = using_key->GetAsIdString();

    ResolvedColumn lhs_column, rhs_column;
    std::unique_ptr<const ResolvedExpr> lhs_compute_expr, rhs_compute_expr;
    ZETASQL_RETURN_IF_ERROR(ResolveColumnInUsing(using_key, name_list_lhs, "left",
                                         key_name, &lhs_column,
                                         &lhs_compute_expr));
    // NOTE: For ArrayScan, there is no rhs_scan we can push a Project into, so
    // we'll never materialize rhs_column. Instead, we'll use rhs_compute_expr
    // directly.
    ZETASQL_RETURN_IF_ERROR(ResolveColumnInUsing(using_key, name_list_rhs, "right",
                                         key_name, &rhs_column,
                                         &rhs_compute_expr));

    std::unique_ptr<const ResolvedExpr> lhs_expr = MakeColumnRef(lhs_column);
    std::unique_ptr<const ResolvedExpr> rhs_expr =
        is_array_scan && rhs_compute_expr != nullptr
            ? std::move(rhs_compute_expr)
            : MakeColumnRef(rhs_column);

    std::unique_ptr<const ResolvedExpr> join_key_expr;
    absl::Status status = MakeEqualityComparison(
        using_key, std::move(lhs_expr), std::move(rhs_expr), &join_key_expr);
    if (absl::IsInvalidArgument(status)) {
      // We assume INVALID_ARGUMENT is never returned by MakeEqualityComparison
      // for reasons other than incompatible types.  In particular, looking up
      // catalog for equality operator should never return INVALID_ARGUMENT.
      return MakeSqlErrorAt(using_key)
             << "Column " << ToIdentifierLiteral(key_name)
             << " in USING has incompatible types on either side of the join: "
             << lhs_column.type()->ShortTypeName(product_mode()) << " and "
             << rhs_column.type()->ShortTypeName(product_mode());
    }
    // Propagate all other errors.
    ZETASQL_RETURN_IF_ERROR(status);
    join_key_exprs.push_back(std::move(join_key_expr));

    // The column name from inside USING should be visible as a column
    // exactly once, from the non-NULL side of the join.
    // As per specification, the output for the using column is:
    // 1) The lhs for INNER or LEFT JOIN
    // 2) The rhs for RIGHT JOIN
    // 3) A COALESCE(lhs, rhs) expression for FULL JOIN (whose result
    //    type is the supertype of lhs/rhs)
    if (zetasql_base::InsertIfNotPresent(&column_names_emitted_by_using, key_name)) {
      const IdString key_name = using_key->GetAsIdString();
      switch (join_type) {
        case ResolvedJoinScan::LEFT:
        case ResolvedJoinScan::INNER:
          // is_explicit=true because we always have a provided alias in
          // JOIN USING.
          ZETASQL_RETURN_IF_ERROR(output_name_list->AddColumn(key_name, lhs_column,
                                                      true /* is_explicit */));
          break;
        case ResolvedJoinScan::RIGHT:
          ZETASQL_RETURN_IF_ERROR(output_name_list->AddColumn(key_name, rhs_column,
                                                      true /* is_explicit */));
          break;
        case ResolvedJoinScan::FULL: {
          std::unique_ptr<const ResolvedExpr> coalesce_expr;
          ZETASQL_RETURN_IF_ERROR(MakeCoalesceExpr(using_key, {lhs_column, rhs_column},
                                           &coalesce_expr));
          const ResolvedColumn coalesce_column(AllocateColumnId(), kFullJoinId,
                                               key_name,
                                               coalesce_expr->annotated_type());
          computed_columns->push_back(MakeResolvedComputedColumn(
              coalesce_column, std::move(coalesce_expr)));
          ZETASQL_RETURN_IF_ERROR(output_name_list->AddColumn(key_name, coalesce_column,
                                                      true /* is_explicit */));
          // Mark the <coalesce_column> as referenced so that it does not get
          // pruned if column pruning is enabled in the AnalyzerOptions.
          // Pruning this column causes problems for the SQLBuilder and other
          // visitors that do not visit expressions computed in a ResolvedNode
          // unless that node actually projects the expressions.
          // TODO: Consider enhancing column pruning, so that when
          // pruning a computed column we determine if we can also prune the
          // related expression.
          RecordColumnAccess(coalesce_column);
          break;
        }
      }
    }

    if (lhs_compute_expr != nullptr) {
      lhs_computed_columns->push_back(
          MakeResolvedComputedColumn(lhs_column, std::move(lhs_compute_expr)));
    }
    if (rhs_compute_expr != nullptr) {
      rhs_computed_columns->push_back(
          MakeResolvedComputedColumn(rhs_column, std::move(rhs_compute_expr)));
    }
  }

  ZETASQL_RETURN_IF_ERROR(output_name_list->MergeFromExceptColumns(
      name_list_lhs, &column_names_emitted_by_using, using_clause));
  ZETASQL_RETURN_IF_ERROR(output_name_list->MergeFromExceptColumns(
      name_list_rhs, &column_names_emitted_by_using, using_clause));

  return MakeAndExpr(using_clause, std::move(join_key_exprs), join_condition);
}

// Inside this method, <external_scope> is the scope including names that
// are visible coming from outside the join.
// We build additional NameScopes inside the method where we need to resolve
// expressions using names that may come from the lhs or rhs of this join.
// TODO: Break ResolveJoin() into pieces, it is a monster.
absl::Status Resolver::ResolveJoin(
    const ASTJoin* join, const NameScope* external_scope,
    const NameScope* local_scope, std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {

  std::unique_ptr<const ResolvedScan> resolved_lhs;
  std::shared_ptr<const NameList> name_list_lhs;
  ZETASQL_RETURN_IF_ERROR(ResolveTableExpression(
      join->lhs(), external_scope, local_scope, &resolved_lhs, &name_list_lhs));

  // Join trees are normally left deep, and normally evaluated left to right,
  // but can be written with parentheses to change order.  In such cases, we
  // need different scoping rules inside the parentheses.
  //
  // From Date & Darwen, p143:
  //   ( ( T1 JOIN T2 ON cond1 )
  //     JOIN
  //     ( T3 JOIN T4 ON cond2 )
  //     ON cond3 )
  // * cond1 can see T1, T2
  // * cond2 can see T3, T4
  // * cond3 can see T1, T2, T3, T4
  // * The select-list can see T1, T2, T3, T4
  //
  // There is a further refinement we don't currently support because we
  // don't allow aliases on parenthesized join.
  //   ( ( T1 JOIN T2 ON cond1 ) AS TA
  //     JOIN
  //     ( T3 JOIN T4 ON cond2 ) AS TB
  //     ON cond3 )
  // * Now, cond3 can see only TA, TB, and not T1, T2, T3, T4.
  // * The select-list can see only TA, TB, and not T1, T2, T3, T4.
  std::unique_ptr<const NameScope> scope_for_rhs_storage;
  const NameScope* scope_for_rhs;
  if (join->rhs()->node_kind() == AST_JOIN) {
    // This is a parenthesized join.  We don't need to handle this specially in
    // the parser because we don't need to detect parentheses on the lhs.
    // If we supported the extra level of aliases in the second example above,
    // we'd need to mark parenthesized joins in the parse tree.
    //
    // Note that even though joins written inside parentheses are supposed to
    // observe a clean scope that only includes external names (and none of
    // the names introduced locally in the same FROM clause), we don't need
    // to pass around that clean scope separately.  When we traverse to a lhs
    // child, we always pass a clean scope.  When we traverse to a rhs child,
    // we pass a clean scope for all join nodes, and only pass a scope
    // including names from the lhs for non-join nodes (e.g. table names,
    // path expressions, table subqueries).  Therefore, we always have a clean
    // scope object available for parenthesized joins.
    scope_for_rhs = external_scope;
  } else {
    scope_for_rhs_storage =
        absl::make_unique<NameScope>(external_scope, name_list_lhs);
    scope_for_rhs = scope_for_rhs_storage.get();
  }

  // Peek at rhs_node to see if we should try to resolve it as an array scan.
  // If the first identifier can be resolved inside <scope_for_rhs>, then try
  // to resolve this join as an array scan.
  if (join->rhs()->node_kind() == AST_TABLE_PATH_EXPRESSION) {
    const ASTTablePathExpression* table_ref =
        join->rhs()->GetAsOrDie<ASTTablePathExpression>();
    const ASTPathExpression* rhs_path_expr = table_ref->path_expr();
    // We may have an unnest_expr instead of a path_expr here.
    // Single-word identifiers are always resolved as table names.
    if (rhs_path_expr == nullptr ||
        (rhs_path_expr->num_names() > 1 &&
         IsPathExpressionStartingFromScope(rhs_path_expr, scope_for_rhs))) {
      std::string error_label;
      if (rhs_path_expr != nullptr) {
        error_label = rhs_path_expr->ToIdentifierPathString();
      } else {
        error_label = "UNNEST expression";
      }

      // Make sure this join is valid for an array scan.
      bool is_left_outer = false;
      switch (join->join_type()) {
        case ASTJoin::DEFAULT_JOIN_TYPE:
        case ASTJoin::CROSS:
        case ASTJoin::INNER:
        case ASTJoin::COMMA:
          break;  // These are all inner joins.
        case ASTJoin::LEFT:
          is_left_outer = true;
          break;
        case ASTJoin::RIGHT:
          return MakeSqlErrorAt(join->rhs())
                 << "Array scan is not allowed with RIGHT JOIN: "
                 << error_label;
        case ASTJoin::FULL:
          return MakeSqlErrorAt(join->rhs())
                 << "Array scan is not allowed with FULL JOIN: " << error_label;
      }
      if (join->natural()) {
        return MakeSqlErrorAt(join->rhs())
               << "Array scan is not allowed with NATURAL JOIN: "
               << error_label;
      }
      if (table_ref->for_system_time() != nullptr) {
        return MakeSqlErrorAt(table_ref->for_system_time())
               << "FOR SYSTEM TIME is not allowed with array scan";
      }
      return ResolveArrayScan(table_ref, join->on_clause(),
                              join->using_clause(), join, is_left_outer,
                              &resolved_lhs, name_list_lhs, scope_for_rhs,
                              output, output_name_list);
    }
  }

  // Now we're in the normal table-scan case.
  std::unique_ptr<const ResolvedScan> resolved_rhs;
  std::shared_ptr<const NameList> name_list_rhs;
  ZETASQL_RETURN_IF_ERROR(ResolveTableExpression(join->rhs(), external_scope,
                                         scope_for_rhs, &resolved_rhs,
                                         &name_list_rhs));

  // True iff this join type accepts (and requires) an ON or USING clause.
  bool expect_join_condition;

  const char* join_type_name = "";  // For error messages.
  ResolvedJoinScan::JoinType resolved_join_type;
  switch (join->join_type()) {
    case ASTJoin::COMMA:
      // This is the only case without a "JOIN" keyword.
      // This join_type_name never gets used currently because none of the
      // error cases below apply for comma joins.
      join_type_name = "comma join";
      expect_join_condition = false;
      resolved_join_type = ResolvedJoinScan::INNER;
      break;
    case ASTJoin::CROSS:
      join_type_name = "CROSS JOIN";
      expect_join_condition = false;
      resolved_join_type = ResolvedJoinScan::INNER;
      break;
    case ASTJoin::DEFAULT_JOIN_TYPE:  // No join_type keyword - same as INNER.
    case ASTJoin::INNER:
      join_type_name = "INNER JOIN";
      expect_join_condition = true;
      resolved_join_type = ResolvedJoinScan::INNER;
      break;
    case ASTJoin::LEFT:
      join_type_name = "LEFT JOIN";
      expect_join_condition = true;
      resolved_join_type = ResolvedJoinScan::LEFT;
      break;
    case ASTJoin::RIGHT:
      join_type_name = "RIGHT JOIN";
      expect_join_condition = true;
      resolved_join_type = ResolvedJoinScan::RIGHT;
      break;
    case ASTJoin::FULL:
      join_type_name = "FULL JOIN";
      expect_join_condition = true;
      resolved_join_type = ResolvedJoinScan::FULL;
      break;
  }

  const char* natural_str = "";  // For error messages.
  if (join->natural()) {
    if (!expect_join_condition) {
      return MakeSqlErrorAtLocalNode(join)
             << "NATURAL cannot be used with " << join_type_name;
    }
    expect_join_condition = false;
    natural_str = "NATURAL ";

    return MakeSqlErrorAtLocalNode(join) << "Natural join not supported";
  }

  // This stores the extra casted (for LEFT, RIGHT, INNER JOIN) and
  // coalesced columns (for FULL JOIN) that may be required for USING.
  // These columns are computed after the join.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>> computed_columns;

  std::shared_ptr<NameList> name_list(new NameList);
  std::unique_ptr<const ResolvedExpr> join_condition;

  if (join->using_clause() != nullptr) {
    ZETASQL_RET_CHECK(join->on_clause() == nullptr);  // Can't have both.
    if (!expect_join_condition) {
      return MakeSqlErrorAt(join->using_clause())
             << "USING clause cannot be used with " << natural_str
             << join_type_name;
    }

    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        lhs_computed_columns;
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        rhs_computed_columns;

    ZETASQL_RETURN_IF_ERROR(
        ResolveUsing(join->using_clause(), *name_list_lhs, *name_list_rhs,
                     resolved_join_type, false /* is_array_scan */,
                     &lhs_computed_columns, &rhs_computed_columns,
                     &computed_columns, name_list.get(), &join_condition));

    // Add a Project for any columns we need to computed before the join.
    MaybeAddProjectForComputedColumns(std::move(lhs_computed_columns),
                                      &resolved_lhs);
    MaybeAddProjectForComputedColumns(std::move(rhs_computed_columns),
                                      &resolved_rhs);
  } else {
    ZETASQL_RETURN_IF_ERROR(
        name_list->MergeFrom(*name_list_lhs, join->lhs()->alias_location()));
    ZETASQL_RETURN_IF_ERROR(
        name_list->MergeFrom(*name_list_rhs, join->rhs()->alias_location()));

    static constexpr char kJoinOnClause[] = "JOIN ON clause";
    if (join->on_clause() != nullptr) {
      if (!expect_join_condition) {
        return MakeSqlErrorAt(join->on_clause())
               << "ON clause cannot be used with " << natural_str
               << join_type_name;
      }
      const std::unique_ptr<const NameScope> on_scope(
          new NameScope(external_scope, name_list));

      ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(join->on_clause()->expression(),
                                        on_scope.get(), kJoinOnClause,
                                        &join_condition));
      ZETASQL_RETURN_IF_ERROR(CoerceExprToBool(join->on_clause()->expression(),
                                       kJoinOnClause, &join_condition));
    } else {
      // No ON or USING clause.
      if (expect_join_condition) {
        return MakeSqlErrorAtLocalNode(join)
               << natural_str << join_type_name
               << " must have an immediately following ON or USING clause";
      }
    }
  }

  *output_name_list = name_list;

  return AddScansForJoin(join, std::move(resolved_lhs), std::move(resolved_rhs),
                         resolved_join_type, std::move(join_condition),
                         std::move(computed_columns), output);
}

absl::Status Resolver::AddScansForJoin(
    const ASTJoin* join, std::unique_ptr<const ResolvedScan> resolved_lhs,
    std::unique_ptr<const ResolvedScan> resolved_rhs,
    ResolvedJoinScan::JoinType resolved_join_type,
    std::unique_ptr<const ResolvedExpr> join_condition,
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> computed_columns,
    std::unique_ptr<const ResolvedScan>* output_scan) {
  ResolvedColumnList concat_columns = ConcatColumnLists(
      resolved_lhs->column_list(), resolved_rhs->column_list());
  std::unique_ptr<ResolvedJoinScan> resolved_join = MakeResolvedJoinScan(
      concat_columns, resolved_join_type, std::move(resolved_lhs),
      std::move(resolved_rhs), std::move(join_condition));

  // If we have a join_type keyword hint (e.g. HASH JOIN or LOOKUP JOIN),
  // add it on the front of hint_list, before any long-form hints.
  ZETASQL_RETURN_IF_ERROR(MaybeAddJoinHintKeyword(join, resolved_join.get()));

  ZETASQL_RETURN_IF_ERROR(ResolveHintsForNode(join->hint(), resolved_join.get()));

  *output_scan = std::move(resolved_join);

  // If we created a CAST expression for RIGHT/INNER/LEFT JOIN or a
  // COALESCE for FULL JOIN with USING, we create a wrapper ProjectScan node to
  // produce additional columns for those expressions.
  MaybeAddProjectForComputedColumns(std::move(computed_columns), output_scan);

  return absl::OkStatus();
}

absl::Status Resolver::ResolveParenthesizedJoin(
    const ASTParenthesizedJoin* parenthesized_join,
    const NameScope* external_scope, const NameScope* local_scope,
    std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  std::unique_ptr<const ResolvedScan> resolved_join;
  ZETASQL_RETURN_IF_ERROR(ResolveJoin(parenthesized_join->join(), external_scope,
                              local_scope, &resolved_join, output_name_list));

  if (parenthesized_join->sample_clause()) {
    ZETASQL_RETURN_IF_ERROR(ResolveTablesampleClause(
        parenthesized_join->sample_clause(), output_name_list, &resolved_join));
  }

  *output = std::move(resolved_join);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveGroupRowsTVF(
    const ASTTVF* ast_tvf, std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* group_rows_name_list) {
  if (name_lists_for_group_rows_.empty()) {
    return MakeSqlErrorAt(ast_tvf)
           << "GROUP_ROWS() can only be used inside WITH GROUP_ROWS clause";
  }

  std::shared_ptr<const NameList> from_clause_name_list =
      name_lists_for_group_rows_.top().name_list;
  name_lists_for_group_rows_.top().group_rows_tvf_used = true;

  // Clone the FROM clause's name list. Also remember mapping and new columns in
  // column_list and in out_cols.
  ResolvedColumnList column_list;
  // For each cloned column, create a new computed column with a
  // column reference pointing to the column in the original table.
  // This is necessary so that if the GROUP_ROWS() is referenced twice,
  // we get distinct column ids for each scan.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>> out_cols;
  absl::flat_hash_map<ResolvedColumn, ResolvedColumn> cloned_columns;
  auto clone_column = [this, &column_list, &out_cols, &cloned_columns](
                          const ResolvedColumn& from_clause_column) {
    ResolvedColumn group_rows_column(
        AllocateColumnId(), from_clause_column.table_name_id(),
        from_clause_column.name_id(), from_clause_column.type());
    cloned_columns[from_clause_column] = group_rows_column;
    column_list.emplace_back(group_rows_column);
    out_cols.push_back(MakeResolvedComputedColumn(
        group_rows_column, MakeColumnRef(from_clause_column)));
    return group_rows_column;
  };

  absl::string_view value_table_error =
      "Value tables are not allowed to pass through GROUP_ROWS() TVF";
  // Table value currently does not propagate. When it is encountered,
  // value_table_error is raised.
  ZETASQL_ASSIGN_OR_RETURN(auto cloned_name_list,
                   from_clause_name_list->CloneWithNewColumns(
                       ast_tvf, value_table_error, ast_tvf->alias(),
                       clone_column, id_string_pool_));

  ZETASQL_RET_CHECK_EQ(cloned_name_list->num_columns(),
               from_clause_name_list->num_columns());

  std::string alias;
  *group_rows_name_list = std::move(cloned_name_list);
  if (ast_tvf->alias() != nullptr) {
    alias = ast_tvf->alias()->GetAsString();
  }

  // Resolve the query hint, if present.
  std::vector<std::unique_ptr<const ResolvedOption>> hints;
  if (ast_tvf->hint() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveHintAndAppend(ast_tvf->hint(), &hints));
  }

  std::unique_ptr<ResolvedGroupRowsScan> group_rows_scan =
      MakeResolvedGroupRowsScan(column_list, std::move(out_cols), alias);
  group_rows_scan->set_hint_list(std::move(hints));

  *output = std::move(group_rows_scan);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveTVF(
    const ASTTVF* ast_tvf, const NameScope* external_scope,
    const NameScope* local_scope, std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {

  if (language().LanguageFeatureEnabled(FEATURE_V_1_3_WITH_GROUP_ROWS)) {
    std::vector<std::string> fn_name = ast_tvf->name()->ToIdentifierVector();
    if (ast_tvf->name()->num_names() == 1 &&
        zetasql_base::CaseEqual(
            ast_tvf->name()->first_name()->GetAsIdString().ToStringView(),
            "GROUP_ROWS") &&
        ast_tvf->argument_entries().empty()) {
      return ResolveGroupRowsTVF(ast_tvf, output, output_name_list);
    }
  }

  // Check the language options to make sure TVFs are supported on this server.
  if (!language().LanguageFeatureEnabled(FEATURE_TABLE_VALUED_FUNCTIONS)) {
    return MakeSqlErrorAt(ast_tvf)
           << "Table-valued functions are not supported";
  }

  // Lookup into the catalog to get the TVF definition.
  const std::string tvf_name_string = ast_tvf->name()->ToIdentifierPathString();
  const IdString tvf_name_idstring = MakeIdString(tvf_name_string);
  const TableValuedFunction* tvf_catalog_entry = nullptr;
  const absl::Status find_status = catalog_->FindTableValuedFunction(
      ast_tvf->name()->ToIdentifierVector(), &tvf_catalog_entry,
      analyzer_options_.find_options());
  if (find_status.code() == absl::StatusCode::kNotFound) {
    std::string error_message;
    absl::StrAppend(&error_message,
                    "Table-valued function not found: ", tvf_name_string);

    const std::string tvf_suggestion = catalog_->SuggestTableValuedFunction(
        ast_tvf->name()->ToIdentifierVector());
    if (!tvf_suggestion.empty()) {
      absl::StrAppend(&error_message, "; Did you mean ", tvf_suggestion, "?");
    }

    return MakeSqlErrorAt(ast_tvf) << error_message;
  } else if (!find_status.ok()) {
    // The FindTableValuedFunction() call can return an invalid argument error,
    // for example, when looking up LazyResolutionTableFunctions (which are
    // resolved upon lookup).
    //
    // Rather than directly return the <find_status>, we update the location
    // of the error to indicate the function call in this statement.  We also
    // preserve the ErrorSource payload from <find_status>, since that
    // indicates source errors for this error.
    return WrapNestedErrorStatus(
        ast_tvf,
        absl::StrCat("Invalid table-valued function ", tvf_name_string),
        find_status, analyzer_options_.error_message_mode());
  }
  ZETASQL_RETURN_IF_ERROR(find_status);
  // Get the TVF signature. Each TVF has exactly one signature; overloading is
  // not currently allowed.
  ZETASQL_RET_CHECK_EQ(1, tvf_catalog_entry->NumSignatures());
  std::unique_ptr<FunctionSignature> result_signature;
  // <arg_locations> and <resolved_tvf_args> reflect the concrete function call
  // arguments in <result_signature> and match 1:1 to them.
  std::vector<const ASTNode*> arg_locations;
  std::vector<ResolvedTVFArg> resolved_tvf_args;
  SignatureMatchResult signature_match_result;

  FunctionResolver function_resolver(catalog_, type_factory_, this);
  ZETASQL_ASSIGN_OR_RETURN(
      const int matching_signature_idx,
      MatchTVFSignature(ast_tvf, tvf_catalog_entry, external_scope, local_scope,
                        function_resolver, &result_signature, &arg_locations,
                        &resolved_tvf_args, &signature_match_result));
  ZETASQL_RET_CHECK_EQ(arg_locations.size(), resolved_tvf_args.size());
  const FunctionSignature* function_signature =
      tvf_catalog_entry->GetSignature(matching_signature_idx);
  ZETASQL_RETURN_IF_ERROR(AddAdditionalDeprecationWarningsForCalledFunction(
      ast_tvf, *function_signature, tvf_catalog_entry->FullName(),
      /*is_tvf=*/true));

  // Add casts or coerce literals for TVF arguments.
  ZETASQL_RET_CHECK(result_signature->IsConcrete()) << ast_tvf->DebugString();
  for (int arg_idx = 0; arg_idx < resolved_tvf_args.size(); ++arg_idx) {
    if (resolved_tvf_args[arg_idx].IsExpr()) {
      ZETASQL_RET_CHECK_LT(arg_idx, result_signature->NumConcreteArguments());
      const Type* target_type = result_signature->ConcreteArgumentType(arg_idx);
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> expr,
                       resolved_tvf_args[arg_idx].MoveExpr());
      const ASTNode* ast = arg_locations[arg_idx];
      if (ast->node_kind() == AST_NAMED_ARGUMENT) {
        ast = ast->GetAs<ASTNamedArgument>()->expr();
        ZETASQL_RET_CHECK(ast != nullptr);
      }
      ZETASQL_RETURN_IF_ERROR(
          CoerceExprToType(ast, target_type, kExplicitCoercion, &expr));
      resolved_tvf_args[arg_idx].SetExpr(std::move(expr));
    } else {
      bool must_add_projection = false;
      ZETASQL_RETURN_IF_ERROR(CheckIfMustCoerceOrRearrangeTVFRelationArgColumns(
          function_signature->argument(arg_idx), arg_idx,
          signature_match_result, resolved_tvf_args[arg_idx],
          &must_add_projection));
      if (must_add_projection) {
        ZETASQL_RETURN_IF_ERROR(CoerceOrRearrangeTVFRelationArgColumns(
            function_signature->argument(arg_idx), arg_idx,
            signature_match_result, ast_tvf, &resolved_tvf_args[arg_idx]));
      }
    }
  }

  // Prepare the list of TVF input arguments for calling the
  // TableValuedFunction::Resolve method.
  std::vector<TVFInputArgumentType> tvf_input_arguments;
  tvf_input_arguments.reserve(resolved_tvf_args.size());
  for (int i = 0; i < resolved_tvf_args.size(); ++i) {
    if (resolved_tvf_args[i].IsExpr()) {
      ZETASQL_ASSIGN_OR_RETURN(const ResolvedExpr* const expr,
                       resolved_tvf_args[i].GetExpr());
      if (expr->node_kind() == RESOLVED_LITERAL) {
        const Value& value = expr->GetAs<ResolvedLiteral>()->value();
        tvf_input_arguments.push_back(
            TVFInputArgumentType(InputArgumentType(value)));
      } else {
        tvf_input_arguments.push_back(
            TVFInputArgumentType(InputArgumentType(expr->type())));
      }
      tvf_input_arguments.back().set_scalar_expr(expr);
    } else if (resolved_tvf_args[i].IsDescriptor()) {
      ZETASQL_ASSIGN_OR_RETURN(const ResolvedDescriptor* const descriptor,
                       resolved_tvf_args[i].GetDescriptor());
      tvf_input_arguments.push_back(TVFInputArgumentType(
          TVFDescriptorArgument(descriptor->descriptor_column_name_list())));
    } else if (resolved_tvf_args[i].IsConnection()) {
      ZETASQL_ASSIGN_OR_RETURN(const ResolvedConnection* const connection,
                       resolved_tvf_args[i].GetConnection());
      tvf_input_arguments.push_back(TVFInputArgumentType(
          TVFConnectionArgument(connection->connection())));
    } else if (resolved_tvf_args[i].IsModel()) {
      ZETASQL_ASSIGN_OR_RETURN(const ResolvedModel* const model,
                       resolved_tvf_args[i].GetModel());
      tvf_input_arguments.push_back(
          TVFInputArgumentType(TVFModelArgument(model->model())));
    } else {
      ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<const NameList> name_list,
                       resolved_tvf_args[i].GetNameList());
      const std::vector<ResolvedColumn> column_list =
          name_list->GetResolvedColumns();
      if (name_list->is_value_table()) {
        ZETASQL_RET_CHECK_EQ(1, name_list->num_columns()) << ast_tvf->DebugString();
        ZETASQL_RET_CHECK_EQ(1, column_list.size()) << ast_tvf->DebugString();
        tvf_input_arguments.push_back(TVFInputArgumentType(
            TVFRelation::ValueTable(column_list[0].type())));
      } else {
        TVFRelation::ColumnList tvf_relation_columns;
        tvf_relation_columns.reserve(column_list.size());
        ZETASQL_RET_CHECK_GE(column_list.size(), name_list->num_columns())
            << ast_tvf->DebugString();
        for (int j = 0; j < name_list->num_columns(); ++j) {
          tvf_relation_columns.emplace_back(
              name_list->column(j).name.ToString(), column_list[j].type());
        }
        tvf_input_arguments.push_back(
            TVFInputArgumentType(TVFRelation(tvf_relation_columns)));
      }
    }
  }

  // Call the TableValuedFunction::Resolve method to get the output schema.
  // Use a new empty cycle detector, or the cycle detector from an enclosing
  // Resolver if we are analyzing one or more templated function calls.
  std::shared_ptr<TVFSignature> tvf_signature;
  CycleDetector owned_cycle_detector;
  AnalyzerOptions analyzer_options = analyzer_options_;
  if (analyzer_options.find_options().cycle_detector() == nullptr) {
    analyzer_options.mutable_find_options()->set_cycle_detector(
        &owned_cycle_detector);
  }
  const absl::Status resolve_status = tvf_catalog_entry->Resolve(
      &analyzer_options, tvf_input_arguments, *result_signature, catalog_,
      type_factory_, &tvf_signature);

  if (!resolve_status.ok()) {
    // The Resolve method returned an error status that is already updated
    // based on the <analyzer_options> ErrorMessageMode.  Make a new
    // ErrorSource based on the <resolve_status>, and return a new error
    // status that indicates that the TVF call is invalid, while indicating
    // the TVF call location for the error.
    return WrapNestedErrorStatus(
        ast_tvf,
        absl::StrCat("Invalid table-valued function ", tvf_name_string),
        resolve_status, analyzer_options_.error_message_mode());
  }
  RETURN_SQL_ERROR_AT_IF_ERROR(ast_tvf, resolve_status);

  bool is_value_table = tvf_signature->result_schema().is_value_table();
  if (is_value_table) {
    ZETASQL_RETURN_IF_ERROR(
        CheckValidValueTableFromTVF(ast_tvf, tvf_catalog_entry->FullName(),
                                    tvf_signature->result_schema()));
  } else if (tvf_signature->result_schema().num_columns() == 0) {
    return MakeSqlErrorAt(ast_tvf)
           << "Table-valued functions must return at least one column, but "
           << "TVF " << tvf_catalog_entry->FullName() << " returned no columns";
  }

  // Fill the column and name list based on the output schema.
  // These columns match up 1:1 by position because we don't have a guarantee
  // of unique names. For value table, the output schema must have the value
  // column at index 0 and the rest columns if present must be pseudo columns.
  ResolvedColumnList column_list;
  std::shared_ptr<NameList> name_list(new NameList);
  column_list.reserve(tvf_signature->result_schema().num_columns());
  for (int i = 0; i < tvf_signature->result_schema().num_columns(); ++i) {
    const TVFRelation::Column& column =
        tvf_signature->result_schema().column(i);
    const IdString column_name = MakeIdString(
        !column.name.empty() ? column.name : absl::StrCat("$col", i));
    column_list.push_back(ResolvedColumn(AllocateColumnId(), tvf_name_idstring,
                                         column_name, column.type));
    if (column.is_pseudo_column) {
      ZETASQL_RETURN_IF_ERROR(
          name_list->AddPseudoColumn(column_name, column_list.back(), ast_tvf));
    } else if (is_value_table) {
      ZETASQL_RET_CHECK_EQ(i, 0);  // Verified by CheckValidValueTableFromTVF
      // Defer AddValueTableColumn until after adding the pseudo-columns.
    } else {
      ZETASQL_RETURN_IF_ERROR(name_list->AddColumn(column_name, column_list.back(),
                                           /*is_explicit=*/true));
    }
  }
  if (is_value_table) {
    IdString alias = ast_tvf->alias() != nullptr
                         ? ast_tvf->alias()->GetAsIdString()
                         : MakeIdString("$col0");
    // So far, we've accumulated the pseudo-columns only.  Now add the
    // value table column, and pass in the list of pseudo-columns so they
    // can be attached to the range variable for the value table.
    ZETASQL_RETURN_IF_ERROR(name_list->AddValueTableColumn(
        alias, column_list[0], ast_tvf, /*excluded_field_names =*/{},
        name_list));
    name_list->set_is_value_table(true);
  }
  *output_name_list = name_list;

  // If the TVF call has an alias, add a range variable to the name list so that
  // the enclosing query can refer to that alias.
  if (ast_tvf->alias() != nullptr && !is_value_table) {
    ZETASQL_ASSIGN_OR_RETURN(*output_name_list,
                     NameList::AddRangeVariableInWrappingNameList(
                         ast_tvf->alias()->GetAsIdString(), ast_tvf->alias(),
                         *output_name_list));
  }

  // Resolve the query hint, if present.
  std::vector<std::unique_ptr<const ResolvedOption>> hints;
  if (ast_tvf->hint() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveHintAndAppend(ast_tvf->hint(), &hints));
  }

  // Create the resolved TVF scan.
  std::vector<const ResolvedTVFArgument*> final_resolved_tvf_args;
  for (ResolvedTVFArg& arg : resolved_tvf_args) {
    if (arg.IsExpr()) {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> expr,
                       arg.MoveExpr());
      final_resolved_tvf_args.push_back(
          MakeResolvedTVFArgument(std::move(expr), /*scan=*/nullptr,
                                  /*model=*/nullptr, /*connection=*/nullptr,
                                  /*descriptor_arg=*/nullptr,
                                  /*argument_column_list=*/{})
              .release());
    } else if (arg.IsScan()) {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedScan> scan,
                       arg.MoveScan());
      ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<const NameList> name_list,
                       arg.GetNameList());
      final_resolved_tvf_args.push_back(
          MakeResolvedTVFArgument(/*expr=*/nullptr, std::move(scan),
                                  /*model=*/nullptr, /*connection=*/nullptr,
                                  /*descriptor_arg=*/nullptr,
                                  name_list->GetResolvedColumns())
              .release());
    } else if (arg.IsConnection()) {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedConnection> connection,
                       arg.MoveConnection());
      final_resolved_tvf_args.push_back(
          MakeResolvedTVFArgument(/*expr=*/nullptr, /*scan=*/nullptr,
                                  /*model=*/nullptr, std::move(connection),
                                  /*descriptor_arg=*/nullptr,
                                  /*argument_column_list=*/{})
              .release());
    } else if (arg.IsDescriptor()) {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedDescriptor> descriptor,
                       arg.MoveDescriptor());
      final_resolved_tvf_args.push_back(
          MakeResolvedTVFArgument(/*expr=*/nullptr, /*scan=*/nullptr,
                                  /*model=*/nullptr, /*connection=*/nullptr,
                                  /*descriptor_arg=*/std::move(descriptor),
                                  /*argument_column_list=*/{})
              .release());
    } else {
      ZETASQL_RET_CHECK(arg.IsModel());
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedModel> model,
                       arg.MoveModel());
      final_resolved_tvf_args.push_back(
          MakeResolvedTVFArgument(/*expr=*/nullptr, /*scan=*/nullptr,
                                  std::move(model), /*connection=*/nullptr,
                                  /*descriptor_arg=*/nullptr,
                                  /*argument_column_list=*/{})
              .release());
    }
  }
  std::string alias;
  if (ast_tvf->alias() != nullptr) {
    alias = ast_tvf->alias()->GetAsString();
  }

  // When this is true, we are setting <result_signature> into the
  // ResolvedTVFScan. This will happen when there are omitted arguments
  // preceding any provided arguments in the call and the engine might need the
  // result signature to figure out which arguments are provided.
  bool provide_result_signature = false;
  bool saw_omitted_arguments = false;
  for (const FunctionArgumentType& arg : result_signature->arguments()) {
    if (arg.optional() && arg.num_occurrences() == 0) {
      saw_omitted_arguments = true;
    } else if (saw_omitted_arguments) {
      provide_result_signature = true;
      break;
    }
  }

  std::vector<int> column_index_list(
      tvf_signature->result_schema().columns().size());
  // Fill column_index_list with 0, 1, 2, ..., column_list.size()-1.
  std::iota(column_index_list.begin(), column_index_list.end(), 0);

  auto tvf_scan = MakeResolvedTVFScan(
      column_list, tvf_catalog_entry, tvf_signature,
      std::move(final_resolved_tvf_args), column_index_list, alias,
      provide_result_signature ? std::move(result_signature) : nullptr);
  // WARNING: <result_signature> is destroyed at this point.

  tvf_scan->set_hint_list(std::move(hints));

  MaybeRecordTVFCallParseLocation(ast_tvf, tvf_scan.get());
  *output = std::move(tvf_scan);

  // Resolve the PIVOT clause, if present.
  if (ast_tvf->pivot_clause() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolvePivotClause(
        std::move(*output), *output_name_list, external_scope,
        /*input_is_subquery=*/false, ast_tvf->pivot_clause(), output,
        output_name_list));
  }

  if (ast_tvf->unpivot_clause() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveUnpivotClause(
        std::move(*output), *output_name_list, external_scope,
        ast_tvf->unpivot_clause(), output, output_name_list));
  }
  // Resolve the TABLESAMPLE clause, if present.
  if (ast_tvf->sample() != nullptr) {
    if (!language().LanguageFeatureEnabled(
            FEATURE_TABLESAMPLE_FROM_TABLE_VALUED_FUNCTIONS)) {
      return MakeSqlErrorAt(ast_tvf->sample())
             << "TABLESAMPLE from table-valued function calls is not supported";
    }
    ZETASQL_RETURN_IF_ERROR(
        ResolveTablesampleClause(ast_tvf->sample(), output_name_list, output));
  }
  return absl::OkStatus();
}

absl::StatusOr<int> Resolver::MatchTVFSignature(
    const ASTTVF* ast_tvf, const TableValuedFunction* tvf_catalog_entry,
    const NameScope* external_scope, const NameScope* local_scope,
    const FunctionResolver& function_resolver,
    std::unique_ptr<FunctionSignature>* result_signature,
    std::vector<const ASTNode*>* arg_locations,
    std::vector<ResolvedTVFArg>* resolved_tvf_args,
    SignatureMatchResult* signature_match_result) {
  // Get the TVF signature. Each TVF has exactly one signature; overloading is
  // not currently allowed.
  ZETASQL_RET_CHECK_EQ(1, tvf_catalog_entry->NumSignatures())
      << tvf_catalog_entry->DebugString();
  const int64_t signature_idx = 0;
  const FunctionSignature& function_signature =
      *tvf_catalog_entry->GetSignature(signature_idx);
  int64_t num_tvf_args = ast_tvf->argument_entries().size();

  // Check whether descriptors present in this TVF call.
  bool descriptor_arg_present = false;
  for (int i = 0; i < num_tvf_args; ++i) {
    const ASTTVFArgument* ast_tvf_arg = ast_tvf->argument_entries()[i];
    if (ast_tvf_arg->descriptor() != nullptr) {
      descriptor_arg_present = true;
      break;
    }
  }

  // Resolve the TVF arguments. Each one becomes either an expression, a scan,
  // an ML model, a connection or a descriptor object. We allow correlation
  // references to the enclosing query if this TVF call is inside a scalar
  // subquery expression, but we do not allow references to columns in previous
  // tables in the same FROM clause as the TVF. For this reason, we use
  // 'external_scope' for the ExprResolutionInfo object here when resolving
  // expressions in the TVF call.
  std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments;
  std::unordered_map<int, std::unique_ptr<const NameScope>> tvf_table_scope_map;
  arg_locations->reserve(num_tvf_args);
  resolved_tvf_args->reserve(num_tvf_args);
  for (int i = 0; i < num_tvf_args; ++i) {
    int arg_index = i;
    const ASTExpression* ast_expr = ast_tvf->argument_entries()[i]->expr();
    if (ast_expr == nullptr) {
      arg_locations->push_back(ast_tvf->argument_entries()[i]);
    } else {
      arg_locations->push_back(ast_expr);
      if (ast_expr->node_kind() == AST_NAMED_ARGUMENT) {
        // Make sure the language feature is enabled.
        if (!language().LanguageFeatureEnabled(FEATURE_NAMED_ARGUMENTS)) {
          return MakeSqlErrorAt(ast_expr)
                 << "Named arguments are not supported";
        }
        // Add the named argument to the map.
        const ASTNamedArgument* named_arg = ast_expr->GetAs<ASTNamedArgument>();
        named_arguments.emplace_back(named_arg, i);
        const absl::string_view arg_name =
            named_arg->name()->GetAsIdString().ToStringView();

        arg_index = -1;
        for (int j = 0; j < function_signature.arguments().size(); ++j) {
          const FunctionArgumentType& arg_type = function_signature.argument(j);
          if (arg_type.has_argument_name() &&
              zetasql_base::CaseEqual(arg_type.argument_name(), arg_name)) {
            arg_index = j;
            break;
          }
        }
        if (arg_index == -1) {
          return MakeSqlErrorAt(ast_expr)
                 << "Named argument " << arg_name
                 << " not found in signature for call to function "
                 << tvf_catalog_entry->FullName();
        }
      }
    }
    auto tvf_arg_or_status = ResolveTVFArg(
        ast_tvf->argument_entries()[i], external_scope, local_scope,
        arg_index < function_signature.arguments().size()
            ? &function_signature.argument(arg_index)
            : nullptr,
        tvf_catalog_entry, i,
        descriptor_arg_present ? &tvf_table_scope_map : nullptr);
    ZETASQL_RETURN_IF_ERROR(tvf_arg_or_status.status());
    resolved_tvf_args->push_back(std::move(tvf_arg_or_status).value());
  }

  // We perform a second resolution pass for descriptors whose columns must be
  // resolved against a related table argument. The second pass is necessary
  // because column resolution is done with respect to the table's NameScope,
  // which requires that the related table argument is already resolved
  // (descriptor columns can reference table arguments that appear after them in
  // the function call).
  if (descriptor_arg_present) {
    for (int i = 0; i < num_tvf_args; i++) {
      if ((*resolved_tvf_args)[i].IsDescriptor()) {
        const ASTTVFArgument* ast_tvf_arg = ast_tvf->argument_entries()[i];
        const FunctionArgumentType* function_argument =
            i < function_signature.arguments().size()
                ? &function_signature.argument(i)
                : nullptr;
        if (function_argument != nullptr) {
          std::optional<int> table_offset =
              function_argument->GetDescriptorResolutionTableOffset();
          if (table_offset.has_value()) {
            ZETASQL_RET_CHECK_GE(table_offset.value(), 0);
            if (!zetasql_base::ContainsKey(tvf_table_scope_map, table_offset.value())) {
              return MakeSqlErrorAt(ast_tvf_arg)
                     << "DESCRIPTOR specifies resolving names from non-table "
                        "argument "
                     << table_offset.value();
            }

            std::unique_ptr<const ResolvedDescriptor> resolved_descriptor =
                (*resolved_tvf_args)[i].MoveDescriptor().value();
            ZETASQL_RETURN_IF_ERROR(FinishResolvingDescriptor(
                ast_tvf_arg, tvf_table_scope_map[table_offset.value()],
                table_offset.value(), &resolved_descriptor));
            (*resolved_tvf_args)[i].SetDescriptor(
                std::move(resolved_descriptor));
          }
        }
      }
    }
  }

  // Check if the function call contains any named arguments.
  const std::string tvf_name_string = ast_tvf->name()->ToIdentifierPathString();
  int repetitions = 0;
  int optionals = 0;
  ZETASQL_RET_CHECK_LE(arg_locations->size(), std::numeric_limits<int32_t>::max());

  std::vector<InputArgumentType> input_arg_types;
  input_arg_types.reserve(num_tvf_args);
  for (int i = 0; i < num_tvf_args; ++i) {
    auto input_arg_type_or_status = GetTVFArgType(resolved_tvf_args->at(i));
    ZETASQL_RETURN_IF_ERROR(input_arg_type_or_status.status());
    input_arg_types.push_back(std::move(input_arg_type_or_status).value());
  }

  if (!SignatureArgumentCountMatches(function_signature,
                                     static_cast<int>(arg_locations->size()),
                                     &repetitions, &optionals)) {
    return GenerateTVFNotMatchError(ast_tvf, *signature_match_result,
                                    *tvf_catalog_entry, tvf_name_string,
                                    input_arg_types, signature_idx);
  }

  std::vector<FunctionResolver::ArgIndexPair> index_mapping;
  // TVFs can only have one signature for now, so either this call
  // returns an error, or the arguments match the signature.
  ZETASQL_RETURN_IF_ERROR(function_resolver.GetFunctionArgumentIndexMappingPerSignature(
      tvf_name_string, function_signature, ast_tvf, *arg_locations,
      named_arguments, repetitions,
      /*always_include_omitted_named_arguments_in_index_mapping=*/false,
      &index_mapping));
  ZETASQL_RETURN_IF_ERROR(
      FunctionResolver::
          ReorderInputArgumentTypesPerIndexMappingAndInjectDefaultValues(
              function_signature, index_mapping, &input_arg_types));

  // Check if the TVF arguments match its signature. If not, return an error.
  ZETASQL_ASSIGN_OR_RETURN(const bool matches,
                   function_resolver.SignatureMatches(
                       *arg_locations, input_arg_types, function_signature,
                       /*allow_argument_coercion=*/true, /*name_scope=*/nullptr,
                       result_signature, signature_match_result,
                       /*arg_overrides=*/nullptr));

  if (!matches) {
    return GenerateTVFNotMatchError(ast_tvf, *signature_match_result,
                                    *tvf_catalog_entry, tvf_name_string,
                                    input_arg_types, signature_idx);
  }

  ZETASQL_RETURN_IF_ERROR(FunctionResolver::ReorderArgumentExpressionsPerIndexMapping(
      tvf_name_string, function_signature, index_mapping, ast_tvf,
      input_arg_types, arg_locations, /*resolved_args=*/nullptr,
      resolved_tvf_args));

  return signature_idx;
}

absl::Status Resolver::GenerateTVFNotMatchError(
    const ASTTVF* ast_tvf, const SignatureMatchResult& signature_match_result,
    const TableValuedFunction& tvf_catalog_entry, const std::string& tvf_name,
    const std::vector<InputArgumentType>& input_arg_types, int signature_idx) {
  const ASTNode* ast_location = ast_tvf;
  if (signature_match_result.tvf_bad_argument_index() != -1) {
    ast_location = ast_tvf->argument_entries()[signature_match_result
                                                   .tvf_bad_argument_index()];
  }
  return MakeSqlErrorAt(ast_location)
         << tvf_catalog_entry.GetTVFSignatureErrorMessage(
                tvf_name, input_arg_types, signature_idx,
                signature_match_result, language());
}

absl::StatusOr<ResolvedTVFArg> Resolver::ResolveTVFArg(
    const ASTTVFArgument* ast_tvf_arg, const NameScope* external_scope,
    const NameScope* local_scope, const FunctionArgumentType* function_argument,
    const TableValuedFunction* tvf_catalog_entry, int arg_num,
    std::unordered_map<int, std::unique_ptr<const NameScope>>*
        tvf_table_scope_map) {
  const ASTExpression* ast_expr = ast_tvf_arg->expr();
  const ASTTableClause* ast_table_clause = ast_tvf_arg->table_clause();
  const ASTModelClause* ast_model_clause = ast_tvf_arg->model_clause();
  const ASTConnectionClause* ast_connection_clause =
      ast_tvf_arg->connection_clause();
  const ASTDescriptor* ast_descriptor = ast_tvf_arg->descriptor();
  ResolvedTVFArg resolved_tvf_arg;
  if (ast_table_clause != nullptr) {
    // Resolve the TVF argument as a relation including all original columns
    // from the named table.
    const Table* table = nullptr;
    if (ast_table_clause->tvf() != nullptr) {
      // The TABLE clause represents a TVF call with arguments. Resolve the
      // TVF inside. Then add an identity projection to match the plan shape
      // expected by engines.
      // When X is a value table, TABLE X produces a scan of it as a value
      // table. (The same is true when we have TABLE tvf(...), and the tvf
      // returns a value table.)
      std::unique_ptr<const ResolvedScan> scan;
      std::shared_ptr<const NameList> name_list;
      ZETASQL_RETURN_IF_ERROR(ResolveTVF(ast_table_clause->tvf(), external_scope,
                                 local_scope, &scan, &name_list));
      const auto column_list = scan->column_list();
      resolved_tvf_arg.SetScan(
          MakeResolvedProjectScan(column_list, {}, std::move(scan)),
          std::move(name_list));
    } else {
      // If the TVF argument is a TABLE clause, then the table name can be a
      // WITH clause entry, one of the table arguments to the TVF, or a table
      // from the <catalog_>.
      const ASTPathExpression* table_path = ast_table_clause->table_path();
      if (named_subquery_map_.contains(table_path->ToIdStringVector())) {
        std::unique_ptr<const ResolvedScan> scan;
        std::shared_ptr<const NameList> name_list;
        ZETASQL_RETURN_IF_ERROR(ResolveNamedSubqueryRef(table_path, /*hint=*/nullptr,
                                                &scan, &name_list));
        const auto column_list = scan->column_list();
        resolved_tvf_arg.SetScan(
            MakeResolvedProjectScan(column_list, {}, std::move(scan)),
            std::move(name_list));
      } else if (table_path->num_names() == 1 &&
                 function_argument_info_ != nullptr &&
                 function_argument_info_->FindTableArg(
                     table_path->first_name()->GetAsIdString()) != nullptr &&
                 (language().LanguageFeatureEnabled(
                      FEATURE_FUNCTION_ARGUMENT_NAMES_HIDE_LOCAL_NAMES) ||
                  catalog_->FindTable(table_path->ToIdentifierVector(), &table,
                                      analyzer_options_.find_options())
                          .code() == absl::StatusCode::kNotFound)) {
        std::unique_ptr<const ResolvedScan> scan;
        std::shared_ptr<const NameList> name_list;
        ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsFunctionTableArgument(
            table_path, /*hint=*/nullptr, GetAliasForExpression(table_path),
            /*ast_location=*/ast_table_clause, &scan, &name_list));
        resolved_tvf_arg.SetScan(std::move(scan), std::move(name_list));
      } else {
        ZETASQL_RET_CHECK(ast_expr == nullptr);
        std::unique_ptr<const ResolvedTableScan> table_scan;
        std::shared_ptr<const NameList> name_list;
        ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsTableScan(
            table_path, GetAliasForExpression(table_path),
            false /* has_explicit_alias */, ast_table_clause,
            nullptr /* hints */, nullptr /* for_system_time */, external_scope,
            &table_scan, &name_list));
        resolved_tvf_arg.SetScan(std::move(table_scan), std::move(name_list));
      }
    }
    ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<const NameList> name_list,
                     resolved_tvf_arg.GetNameList());
    RecordColumnAccess(name_list->GetResolvedColumns());
  } else if (ast_expr != nullptr) {
    if (ast_expr->node_kind() == AST_NAMED_ARGUMENT) {
      // Set 'ast_expr' to the named argument value for further resolving.
      ast_expr = ast_expr->GetAs<ASTNamedArgument>()->expr();
    }
    if (function_argument &&
        (function_argument->IsRelation() || function_argument->IsModel() ||
         function_argument->IsConnection())) {
      if (function_argument->IsRelation()) {
        // Resolve the TVF argument as a relation. The argument should be
        // written in the TVF call as a table subquery. We parsed all
        // arguments as expressions, so the parse node should initially have
        // scalar subquery type. We check that the expression subquery
        // modifier type is NONE to exclude ARRAY and EXISTS subqueries.
        if (ast_expr->node_kind() != AST_EXPRESSION_SUBQUERY ||
            ast_expr->GetAsOrDie<ASTExpressionSubquery>()->modifier() !=
                ASTExpressionSubquery::NONE) {
          std::string error =
              absl::StrCat("Table-valued function ",
                           tvf_catalog_entry->FullName(), " argument ", arg_num,
                           " must be a relation (i.e. table subquery)");
          if (ast_expr->node_kind() == AST_PATH_EXPRESSION) {
            const std::string table_name =
                ast_expr->GetAsOrDie<ASTPathExpression>()
                    ->ToIdentifierPathString();
            // Return a specific error message helping the user figure out
            // what to change.
            absl::StrAppend(&error, "; if you meant to refer to table ",
                            table_name, " then add the TABLE keyword ",
                            "before the table name (i.e. TABLE ", table_name,
                            ")");
          }
          return MakeSqlErrorAt(ast_expr) << error;
        }
        std::unique_ptr<const ResolvedScan> scan;
        std::shared_ptr<const NameList> name_list;
        ZETASQL_RETURN_IF_ERROR(
            ResolveQuery(ast_expr->GetAsOrDie<ASTExpressionSubquery>()->query(),
                         external_scope, AllocateSubqueryName(),
                         false /* is_outer_query */, &scan, &name_list));

        // The <tvf_table_scope_map> is not nullptr means descriptors appear in
        // TVF thus there is a need to build NameScopes for the table arguments.
        if (tvf_table_scope_map != nullptr) {
          tvf_table_scope_map->emplace(
              arg_num, absl::make_unique<NameScope>(external_scope, name_list));
        }
        resolved_tvf_arg.SetScan(std::move(scan), std::move(name_list));
      } else if (function_argument->IsConnection()) {
        // This argument has to be a connection. Return an error.
        return MakeSqlErrorAt(ast_expr)
               << "Table-valued function " << tvf_catalog_entry->FullName()
               << " argument " << arg_num
               << " must be a connection specified with the CONNECTION keyword";
      } else {
        // This argument has to be a model. Return an error.
        return MakeSqlErrorAt(ast_expr)
               << "Table-valued function " << tvf_catalog_entry->FullName()
               << " argument " << arg_num
               << " must be a model specified with the MODEL keyword";
      }
    } else {
      // Resolve the TVF argument as a scalar expression.
      std::unique_ptr<const ResolvedExpr> expr;
      ZETASQL_RETURN_IF_ERROR(
          ResolveScalarExpr(ast_expr, external_scope, "FROM clause", &expr));
      resolved_tvf_arg.SetExpr(std::move(expr));
    }
  } else if (ast_connection_clause != nullptr) {
    std::unique_ptr<const ResolvedConnection> resolved_connection;
    ZETASQL_RETURN_IF_ERROR(ResolveConnection(ast_connection_clause->connection_path(),
                                      &resolved_connection));
    resolved_tvf_arg.SetConnection(std::move(resolved_connection));
  } else if (ast_model_clause != nullptr) {
    std::unique_ptr<const ResolvedModel> resolved_model;
    ZETASQL_RETURN_IF_ERROR(
        ResolveModel(ast_model_clause->model_path(), &resolved_model));
    resolved_tvf_arg.SetModel(std::move(resolved_model));
  } else {
    ZETASQL_RET_CHECK(ast_descriptor != nullptr);
    std::unique_ptr<const ResolvedDescriptor> resolved_descriptor;
    const ASTDescriptorColumnList* column_list = ast_descriptor->columns();
    ZETASQL_RETURN_IF_ERROR(
        ResolveDescriptorFirstPass(column_list, &resolved_descriptor));
    resolved_tvf_arg.SetDescriptor(std::move(resolved_descriptor));
  }

  return resolved_tvf_arg;
}

absl::StatusOr<InputArgumentType> Resolver::GetTVFArgType(
    const ResolvedTVFArg& resolved_tvf_arg) {
  InputArgumentType input_arg_type;
  if (resolved_tvf_arg.IsExpr()) {
    ZETASQL_ASSIGN_OR_RETURN(const ResolvedExpr* const expr,
                     resolved_tvf_arg.GetExpr());
    input_arg_type = GetInputArgumentTypeForExpr(expr);
  } else if (resolved_tvf_arg.IsScan()) {
    ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<const NameList> name_list,
                     resolved_tvf_arg.GetNameList());
    if (name_list->is_value_table()) {
      input_arg_type = InputArgumentType::RelationInputArgumentType(
          TVFRelation::ValueTable(name_list->column(0).column.type()));
    } else {
      TVFRelation::ColumnList provided_input_relation_columns;
      provided_input_relation_columns.reserve(name_list->num_columns());
      // Loop over each explicit column returned from the relation argument.
      // Use the number of names in the relation argument's name list instead
      // of the scan's column_list().size() here since the latter includes
      // pseudo-columns, which we do not want to consider here.
      for (int j = 0; j < name_list->num_columns(); ++j) {
        const NamedColumn& named_column = name_list->column(j);
        provided_input_relation_columns.emplace_back(
            named_column.name.ToString(), named_column.column.type());
      }
      input_arg_type = InputArgumentType::RelationInputArgumentType(
          TVFRelation(provided_input_relation_columns));
    }
  } else if (resolved_tvf_arg.IsConnection()) {
    ZETASQL_ASSIGN_OR_RETURN(const ResolvedConnection* const connection,
                     resolved_tvf_arg.GetConnection());
    input_arg_type = InputArgumentType::ConnectionInputArgumentType(
        TVFConnectionArgument(connection->connection()));
  } else if (resolved_tvf_arg.IsModel()) {
    // We are processing a model argument.
    ZETASQL_ASSIGN_OR_RETURN(const ResolvedModel* const model,
                     resolved_tvf_arg.GetModel());
    input_arg_type = InputArgumentType::ModelInputArgumentType(
        TVFModelArgument(model->model()));
  } else {
    ZETASQL_RET_CHECK(resolved_tvf_arg.IsDescriptor());
    // We are processing a descriptor argument.
    input_arg_type = InputArgumentType::DescriptorInputArgumentType();
  }
  return input_arg_type;
}

absl::Status Resolver::CheckIfMustCoerceOrRearrangeTVFRelationArgColumns(
    const FunctionArgumentType& tvf_signature_arg, int arg_idx,
    const SignatureMatchResult& signature_match_result,
    const ResolvedTVFArg& resolved_tvf_arg, bool* add_projection) {
  // If the function signature did not include a required schema for this
  // particular relation argument, there is no need to add a projection.
  if (!tvf_signature_arg.options().has_relation_input_schema()) {
    *add_projection = false;
    return absl::OkStatus();
  }

  // Add a projection to add or drop columns as needed if the number of provided
  // columns was not equal to the number of required columns.
  const TVFRelation& required_schema =
      tvf_signature_arg.options().relation_input_schema();
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<const NameList> name_list,
                   resolved_tvf_arg.GetNameList());
  const int num_provided_columns = name_list->num_columns();
  if (required_schema.num_columns() != num_provided_columns) {
    *add_projection = true;
    return absl::OkStatus();
  }

  // Add a projection if the function signature-matching process indicates a
  // need to coerce the type of one of the function arguments.
  for (const std::pair<const std::pair<int /* arg_idx */, int /* col_idx */>,
                       const Type*>& kv :
       signature_match_result.tvf_arg_col_nums_to_coerce_type()) {
    if (arg_idx == kv.first.first) {
      *add_projection = true;
      return absl::OkStatus();
    }
  }

  // If the required schema was a value table and no type coercion was
  // necessary, then there is no need to add a projection.
  if (required_schema.is_value_table()) {
    *add_projection = false;
    return absl::OkStatus();
  }

  // If the order of provided columns was not equal to the order of required
  // columns, add a projection to rearrange provided columns as needed.
  ZETASQL_RET_CHECK_EQ(required_schema.num_columns(), name_list->columns().size());
  for (int i = 0; i < num_provided_columns; ++i) {
    if (!zetasql_base::CaseEqual(required_schema.column(i).name,
                                name_list->column(i).name.ToString())) {
      *add_projection = true;
      return absl::OkStatus();
    }
  }

  *add_projection = false;
  return absl::OkStatus();
}

absl::Status Resolver::CoerceOrRearrangeTVFRelationArgColumns(
    const FunctionArgumentType& tvf_signature_arg, int arg_idx,
    const SignatureMatchResult& signature_match_result,
    const ASTNode* ast_location, ResolvedTVFArg* resolved_tvf_arg) {
  // Generate a name for a new projection to perform the TVF relation argument
  // coercion and create vectors to hold new columns for the projection.
  const IdString new_project_alias = AllocateSubqueryName();
  std::vector<ResolvedColumn> new_column_list;
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      new_project_columns;
  std::unique_ptr<NameList> new_project_name_list(new NameList);

  // Reserve vectors.
  // Use the number of names in the relation argument's name list instead of the
  // scan's column_list().size() here since the latter includes pseudo-columns,
  // which we do not want to consider here.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<const NameList> name_list,
                   resolved_tvf_arg->GetNameList());
  const int num_provided_columns = name_list->num_columns();
  new_column_list.reserve(num_provided_columns);
  new_project_columns.reserve(num_provided_columns);
  new_project_name_list->ReserveColumns(num_provided_columns);

  // Build a map from provided column name to the index of that column in the
  // list of provided columns.
  std::map<std::string, int, zetasql_base::CaseLess> col_name_to_idx;
  for (int col_idx = 0; col_idx < num_provided_columns; ++col_idx) {
    col_name_to_idx.emplace(name_list->column(col_idx).name.ToString(),
                            col_idx);
  }

  // Iterate through each required column of the relation from the function
  // signature and find the matching provided column with the same name. Also
  // create a cast expression if any provided column type is not equivalent to
  // the corresponding required input column type.
  const int num_required_columns =
      tvf_signature_arg.options().relation_input_schema().columns().size();
  for (int required_col_idx = 0; required_col_idx < num_required_columns;
       ++required_col_idx) {
    const std::string required_col_name = tvf_signature_arg.options()
                                              .relation_input_schema()
                                              .column(required_col_idx)
                                              .name;
    int provided_col_idx;
    if (tvf_signature_arg.options().relation_input_schema().is_value_table()) {
      provided_col_idx = 0;
    } else {
      const int* lookup = zetasql_base::FindOrNull(col_name_to_idx, required_col_name);
      ZETASQL_RET_CHECK(lookup != nullptr) << required_col_name;
      provided_col_idx = *lookup;
    }
    const Type* result_type = zetasql_base::FindPtrOrNull(
        signature_match_result.tvf_arg_col_nums_to_coerce_type(),
        std::make_pair(arg_idx, provided_col_idx));
    const ResolvedColumn& provided_input_column =
        name_list->column(provided_col_idx).column;
    if (result_type == nullptr) {
      new_column_list.push_back(provided_input_column);
    } else {
      new_column_list.emplace_back(AllocateColumnId(), new_project_alias,
                                   name_list->column(provided_col_idx).name,
                                   result_type);
      std::unique_ptr<const ResolvedExpr> resolved_cast(
          MakeColumnRef(provided_input_column, false /* is_correlated */));
      ZETASQL_RETURN_IF_ERROR(ResolveCastWithResolvedArgument(
          ast_location, result_type, false /* return_null_on_error */,
          &resolved_cast));
      new_project_columns.push_back(MakeResolvedComputedColumn(
          new_column_list.back(), std::move(resolved_cast)));
    }
    // Add the new referenced column to the set of referenced columns so that it
    // is not pruned away later.
    RecordColumnAccess(new_column_list.back());
    ZETASQL_RETURN_IF_ERROR(new_project_name_list->AddColumn(
        name_list->column(provided_col_idx).name, new_column_list.back(),
        true /* is_explicit */));
  }
  if (tvf_signature_arg.options().relation_input_schema().is_value_table()) {
    new_project_name_list->set_is_value_table(true);
  }

  // Reset the scan and name list to the new projection.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedScan> scan,
                   resolved_tvf_arg->MoveScan());
  resolved_tvf_arg->SetScan(
      MakeResolvedProjectScan(new_column_list, std::move(new_project_columns),
                              std::move(scan)),
      std::move(new_project_name_list));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveArrayScan(
    const ASTTablePathExpression* table_ref, const ASTOnClause* on_clause,
    const ASTUsingClause* using_clause, const ASTJoin* ast_join,
    bool is_outer_scan,
    std::unique_ptr<const ResolvedScan>* resolved_input_scan,
    const std::shared_ptr<const NameList>& name_list_input,
    const NameScope* scope, std::unique_ptr<const ResolvedScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  // There might not be a lhs_scan, but the unique_ptr should be non-NULL.
  ZETASQL_RET_CHECK(resolved_input_scan != nullptr);
  ZETASQL_RET_CHECK_EQ(*resolved_input_scan == nullptr, name_list_input == nullptr);

  if (table_ref->sample_clause() != nullptr) {
    return MakeSqlErrorAt(table_ref)
           << "TABLESAMPLE cannot be used with arrays";
  }

  // We have either an array reference or UNNEST.
  // These variables get set in either branch below.
  std::unique_ptr<const ResolvedExpr> resolved_value_expr;
  const Type* value_type = nullptr;

  if (table_ref->path_expr() != nullptr) {
    const ASTPathExpression* rhs_path_expr = table_ref->path_expr();

    // Single-word identifiers are always resolved as table names
    // and shouldn't have made it into ResolveArrayScan.
    ZETASQL_RET_CHECK_GE(rhs_path_expr->num_names(), 2);

    NameTarget target;
    ZETASQL_RET_CHECK(scope->LookupName(rhs_path_expr->first_name()->GetAsIdString(),
                                &target));

    switch (target.kind()) {
      case NameTarget::EXPLICIT_COLUMN:
      case NameTarget::RANGE_VARIABLE:
        // These are the allowed cases.
        break;

      case NameTarget::IMPLICIT_COLUMN:
        // We disallowed this because the results were very confusing.
        //   FROM TableName, ColumnName.array_value
        // is not allowed.
        // Explicit column names are allowed in order to support chained
        // references like
        //   FROM Table t, t.Column.arr1 a1, a1.arr2 a2
        //
        // If we decide to make this allowed in normal mode, we would still
        // need to give an error if in_strict_mode().
        return MakeSqlErrorAt(rhs_path_expr)
               << "Aliases referenced in the from clause must refer to "
                  "preceding scans, and cannot refer to columns on those "
                  "scans. "
               << rhs_path_expr->first_name()->GetAsIdString()
               << " refers to a column and must be qualified with a table "
                  "name.";

      case NameTarget::FIELD_OF:
        return MakeSqlErrorAt(rhs_path_expr)
               << "Aliases referenced in the from clause must refer to "
                  "preceding scans, and cannot refer to columns or fields on "
                  "those scans. "
               << rhs_path_expr->first_name()->GetAsIdString()
               << " refers to a field and must be qualified with a table name.";

      case NameTarget::ACCESS_ERROR:
        // This error message is very specific, for the only known case where
        // this error occurs (a correlated array scan that is not
        // visible or valid to access in the outer query).  For example:
        //
        // select tt.key as key,
        //        IF(EXISTS(select *
        //                  from tt.KitchenSink.repeated_int32_val),
        //           count(distinct(tt.key)),
        //           0)
        // from TestTable tt
        // group by tt.key
        //
        // In this query, the reference to tt.KitchenSink.repeated_int32_val
        // in the EXISTS subquery is invalid because the outer query contains
        // GROUP BY and the array is not valid to access post-GROUP BY.
        // TODO: It would be nice to say either 'GROUP BY' or
        // 'DISTINCT' in this message, not both.  But we currently do not
        // have context from the outer query to know which one is correct,
        // so for now we say 'GROUP BY or DISTINCT'.  Fix this.
        return MakeSqlErrorAt(rhs_path_expr)
               << "Correlated aliases referenced in the from clause must refer "
                  "to arrays that are valid to access from the outer query, "
                  "but "
               << rhs_path_expr->first_name()->GetAsIdString()
               << " refers to an array that is not valid to access after GROUP"
               << " BY or DISTINCT in the outer query";

      case NameTarget::AMBIGUOUS:
        // This can happen if the array name is ambiguous (resolves to a name
        // in more than one table previously in the FROM clause).
        return MakeSqlErrorAt(rhs_path_expr)
               << rhs_path_expr->first_name()->GetAsIdString()
               << " ambiguously references multiple columns in previous FROM"
               << " clause tables";
    }

    ExprResolutionInfo no_aggregation(scope, "FROM clause");
    FlattenState::Restorer restorer;
    if (language().LanguageFeatureEnabled(
            FEATURE_V_1_3_UNNEST_AND_FLATTEN_ARRAYS)) {
      no_aggregation.flatten_state.set_can_flatten(true, &restorer);
    }

    // Now we know we have an identifier path starting with a scan.
    // Resolve that with ResolvePathExpr to expand proto field accesses.
    ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsExpression(
        rhs_path_expr, &no_aggregation, ResolvedStatement::READ,
        &resolved_value_expr));

    value_type = resolved_value_expr->type();
    if (!value_type->IsArray()) {
      return MakeSqlErrorAt(rhs_path_expr)
             << "Values referenced in FROM clause must be arrays. "
             << rhs_path_expr->ToIdentifierPathString() << " has type "
             << value_type->ShortTypeName(product_mode());
    }
  } else {
    ZETASQL_RET_CHECK(table_ref->unnest_expr() != nullptr);
    const ASTUnnestExpression* unnest = table_ref->unnest_expr();

    ExprResolutionInfo info(scope, "UNNEST");
    FlattenState::Restorer restorer;
    if (language().LanguageFeatureEnabled(
            FEATURE_V_1_3_UNNEST_AND_FLATTEN_ARRAYS)) {
      info.flatten_state.set_can_flatten(true, &restorer);
    }
    const absl::Status resolve_expr_status =
        ResolveExpr(unnest->expression(), &info, &resolved_value_expr);

    // If resolving the expression failed, and it looked like a valid table
    // name, then give a more helpful error message.
    if (!resolve_expr_status.ok() &&
        absl::StartsWith(resolve_expr_status.message(),
                         "Unrecognized name: ") &&
        unnest->expression()->node_kind() == AST_PATH_EXPRESSION) {
      const ASTPathExpression* path_expr =
          unnest->expression()->GetAsOrDie<ASTPathExpression>();
      const Table* table;
      const absl::Status find_status =
          catalog_->FindTable(path_expr->ToIdentifierVector(), &table,
                              analyzer_options_.find_options());
      if (find_status.ok()) {
        return MakeSqlErrorAt(path_expr)
               << "UNNEST cannot be applied on a table: "
               << path_expr->ToIdentifierPathString();
      }
      if (find_status.code() != absl::StatusCode::kNotFound) {
        ZETASQL_RETURN_IF_ERROR(find_status);
      }
    }
    ZETASQL_RETURN_IF_ERROR(resolve_expr_status);  // Return original error.

    value_type = resolved_value_expr->type();
    if (!value_type->IsArray()) {
      return MakeSqlErrorAt(unnest->expression())
             << "Values referenced in UNNEST must be arrays. "
             << "UNNEST contains expression of type "
             << value_type->ShortTypeName(product_mode());
    }
  }
  ZETASQL_RET_CHECK(resolved_value_expr != nullptr);
  ZETASQL_RET_CHECK(value_type != nullptr);
  ZETASQL_RET_CHECK(value_type->IsArray());

  IdString alias;
  const ASTNode* alias_location;
  if (table_ref->alias() != nullptr) {
    alias = table_ref->alias()->GetAsIdString();
    alias_location = table_ref->alias();
  } else {
    if (table_ref->path_expr() != nullptr) {
      alias = GetAliasForExpression(table_ref->path_expr());
    } else {
      alias = AllocateUnnestName();
    }
    alias_location = table_ref;
  }
  ZETASQL_RET_CHECK(!alias.empty());

  const AnnotationMap* element_annotation = nullptr;
  if (resolved_value_expr->type_annotation_map() != nullptr) {
    element_annotation =
        resolved_value_expr->type_annotation_map()->AsArrayMap()->element();
  }
  const ResolvedColumn array_element_column(
      AllocateColumnId(), /*table_name=*/kArrayId, /*name=*/alias,
      AnnotatedType(value_type->AsArray()->element_type(), element_annotation));

  ResolvedColumnList output_column_list;
  if (*resolved_input_scan != nullptr) {
    output_column_list = (*resolved_input_scan)->column_list();
  }
  output_column_list.emplace_back(array_element_column);
  std::shared_ptr<NameList> name_list_lhs(new NameList);
  if (name_list_input != nullptr) {
    ZETASQL_RETURN_IF_ERROR(name_list_lhs->MergeFrom(*name_list_input, table_ref));
  }
  // Array aliases are always treated as explicit range variables,
  // even if computed.
  // This allows
  //   SELECT t.key, array1, array2
  //   FROM Table t, t.Column.array1, array1.array2;
  // `array1` and `array2` are also available implicitly as columns on
  // the preceding scan, but the array scan makes them implicit.
  std::shared_ptr<NameList> name_list_rhs(new NameList);
  ZETASQL_RETURN_IF_ERROR(name_list_rhs->AddValueTableColumn(
      alias, array_element_column, alias_location));

  // Resolve WITH OFFSET if present.
  std::unique_ptr<ResolvedColumnHolder> array_position_column;
  if (table_ref->with_offset() != nullptr) {
    // If the alias is NULL, we get "offset" as an implicit alias.
    const ASTAlias* with_offset_alias = table_ref->with_offset()->alias();
    const IdString offset_alias =
        (with_offset_alias == nullptr ? kOffsetAlias
                                      : with_offset_alias->GetAsIdString());

    const ResolvedColumn column(AllocateColumnId(),
                                /*table_name=*/kArrayOffsetId,
                                /*name=*/offset_alias,
                                AnnotatedType(type_factory_->get_int64(),
                                              /*annotation_map=*/nullptr));
    array_position_column = MakeResolvedColumnHolder(column);
    output_column_list.push_back(array_position_column->column());

    // We add the offset column as a value table column so its name acts
    // like a range variable and we get an error if it conflicts with
    // other range variables in the same FROM clause.
    ZETASQL_RETURN_IF_ERROR(name_list_rhs->AddValueTableColumn(
        offset_alias, array_position_column->column(),
        with_offset_alias != nullptr
            ? absl::implicit_cast<const ASTNode*>(with_offset_alias)
            : table_ref->with_offset()));
  }

  std::shared_ptr<NameList> name_list(new NameList);
  std::unique_ptr<const ResolvedExpr> resolved_condition;
  std::vector<std::unique_ptr<const ResolvedComputedColumn>> computed_columns;

  if (using_clause != nullptr) {
    ZETASQL_RET_CHECK(on_clause == nullptr);  // Can't have both.

    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        lhs_computed_columns;
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        rhs_computed_columns;

    ZETASQL_RETURN_IF_ERROR(ResolveUsing(
        using_clause, *name_list_lhs, *name_list_rhs,
        is_outer_scan ? ResolvedJoinScan::LEFT : ResolvedJoinScan::INNER,
        true /* is_array_scan */, &lhs_computed_columns, &rhs_computed_columns,
        &computed_columns, name_list.get(), &resolved_condition));

    // Add the USING columns to the <output_column_list>.  These will show
    // up in SELECT *, and can be referenced unqualified (without a table
    // alias qualifier).
    for (const std::unique_ptr<const ResolvedComputedColumn>& column :
         lhs_computed_columns) {
      output_column_list.push_back(column->column());
    }

    // Add a Project for any columns we need to computed before the join.
    MaybeAddProjectForComputedColumns(std::move(lhs_computed_columns),
                                      resolved_input_scan);
    // The <rhs_computed_columns> and <computed_columns> should be empty as
    // ArrayScan has no notion of right or full outer join.
    ZETASQL_RET_CHECK(rhs_computed_columns.empty());
    ZETASQL_RET_CHECK(computed_columns.empty());
  } else {
    ZETASQL_RETURN_IF_ERROR(name_list->MergeFrom(*name_list_lhs, table_ref));
    // We explicitly add the array element and offset columns to the name_list
    // instead of merging name_list_rhs to get the exact error location.
    ZETASQL_RETURN_IF_ERROR(name_list->AddValueTableColumn(alias, array_element_column,
                                                   alias_location));
    if (array_position_column != nullptr) {
      const ASTAlias* with_offset_alias = table_ref->with_offset()->alias();
      ZETASQL_RETURN_IF_ERROR(name_list->AddValueTableColumn(
          with_offset_alias == nullptr ? kOffsetAlias
                                       : with_offset_alias->GetAsIdString(),
          array_position_column->column(),
          with_offset_alias != nullptr
              ? absl::implicit_cast<const ASTNode*>(with_offset_alias)
              : table_ref->with_offset()));
    }

    if (on_clause != nullptr) {
      const std::unique_ptr<const NameScope> on_clause_scope(
          new NameScope(scope, name_list));
      ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(on_clause->expression(),
                                        on_clause_scope.get(), "ON clause",
                                        &resolved_condition));
      ZETASQL_RETURN_IF_ERROR(CoerceExprToBool(on_clause->expression(),
                                       "JOIN ON clause", &resolved_condition));
    }
  }

  // TODO We need to make fields of protos or structs visible
  // as implicit columns here, to allow this without qualification:
  //   SELECT [some_array.]some_array_field
  //   FROM Table t, t.col.some_array;
  std::unique_ptr<ResolvedArrayScan> resolved_array_scan =
      MakeResolvedArrayScan(output_column_list, std::move(*resolved_input_scan),
                            std::move(resolved_value_expr),
                            array_element_column,
                            std::move(array_position_column),
                            std::move(resolved_condition), is_outer_scan);

  // We can have hints attached to either or both the JOIN keyword for this
  // array scan and the table_path_expr for the array itself.
  // Add hints from both places onto the ResolvedArrayScan node.
  if (ast_join != nullptr) {
    // If we have HASH JOIN or LOOKUP JOIN on an array join, we still add those
    // hints on the ArrayScan node even though they may have no meaning.
    ZETASQL_RETURN_IF_ERROR(
        MaybeAddJoinHintKeyword(ast_join, resolved_array_scan.get()));

    ZETASQL_RETURN_IF_ERROR(
        ResolveHintsForNode(ast_join->hint(), resolved_array_scan.get()));
  }
  if (table_ref->hint() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ResolveHintsForNode(table_ref->hint(), resolved_array_scan.get()));
  }

  *output = std::move(resolved_array_scan);
  // If we created a CAST expression for JOIN with USING, we create a wrapper
  // ProjectScan node to produce additional columns for those expressions.
  MaybeAddProjectForComputedColumns(std::move(computed_columns), output);
  *output_name_list = name_list;
  return absl::OkStatus();
}

void Resolver::MaybeRecordParseLocation(const ASTNode* ast_location,
                                        ResolvedNode* resolved_node) const {
  if (analyzer_options_.parse_location_record_type() !=
          PARSE_LOCATION_RECORD_NONE &&
      ast_location != nullptr) {
    resolved_node->SetParseLocationRange(ast_location->GetParseLocationRange());
  }
}

void Resolver::MaybeRecordExpressionSubqueryParseLocation(
    const ASTExpressionSubquery* ast_expr_subquery,
    ResolvedNode* resolved_node) const {
  switch (analyzer_options_.parse_location_record_type()) {
    case PARSE_LOCATION_RECORD_FULL_NODE_SCOPE:
      MaybeRecordParseLocation(ast_expr_subquery, resolved_node);
      break;
    case PARSE_LOCATION_RECORD_CODE_SEARCH:
      MaybeRecordParseLocation(ast_expr_subquery->query(), resolved_node);
      break;
    case PARSE_LOCATION_RECORD_NONE:  // NO-OP
      break;
  }
}

void Resolver::MaybeRecordTVFCallParseLocation(
    const ASTTVF* ast_location, ResolvedNode* resolved_node) const {
  switch (analyzer_options_.parse_location_record_type()) {
    case PARSE_LOCATION_RECORD_FULL_NODE_SCOPE:
      MaybeRecordParseLocation(ast_location, resolved_node);
      break;
    case PARSE_LOCATION_RECORD_CODE_SEARCH:
      MaybeRecordParseLocation(ast_location->name(), resolved_node);
      break;
    case PARSE_LOCATION_RECORD_NONE:  // NO-OP
      break;
  }
}

void Resolver::MaybeRecordFunctionCallParseLocation(
    const ASTFunctionCall* ast_location, ResolvedNode* resolved_node) const {
  if (ast_location == nullptr) {
    return;
  }
  switch (analyzer_options_.parse_location_record_type()) {
    case PARSE_LOCATION_RECORD_FULL_NODE_SCOPE:
      MaybeRecordParseLocation(ast_location, resolved_node);
      break;
    case PARSE_LOCATION_RECORD_CODE_SEARCH:
      MaybeRecordParseLocation(ast_location->function(), resolved_node);
      break;
    case PARSE_LOCATION_RECORD_NONE:  // NO-OP
      break;
  }
}

void Resolver::MaybeRecordFieldAccessParseLocation(
    const ASTNode* ast_path, const ASTIdentifier* ast_field,
    ResolvedNode* resolved_node) const {
  switch (analyzer_options_.parse_location_record_type()) {
    case PARSE_LOCATION_RECORD_CODE_SEARCH:
      MaybeRecordParseLocation(ast_field, resolved_node);
      break;
    case PARSE_LOCATION_RECORD_FULL_NODE_SCOPE: {
      if (ast_field == nullptr) {
        MaybeRecordParseLocation(ast_path, resolved_node);
      } else if (ast_path == nullptr) {
        MaybeRecordParseLocation(ast_field, resolved_node);
      } else {
        ParseLocationRange range;
        range.set_start(ast_path->GetParseLocationRange().start());
        range.set_end(ast_field->GetParseLocationRange().end());
        resolved_node->SetParseLocationRange(range);
      }
      break;
    }
    case PARSE_LOCATION_RECORD_NONE:  // NO-OP
      break;
  }
}

void Resolver::RecordArgumentParseLocationsIfPresent(
    const ASTFunctionParameter& function_argument,
    FunctionArgumentTypeOptions* options) const {
  if (analyzer_options_.parse_location_record_type() ==
      PARSE_LOCATION_RECORD_NONE) {
    return;
  }

  if (function_argument.name() != nullptr) {
    options->set_argument_name_parse_location(
        function_argument.name()->GetParseLocationRange());
  }
  if (function_argument.type() != nullptr) {
    options->set_argument_type_parse_location(
        function_argument.type()->GetParseLocationRange());
  } else if (function_argument.templated_parameter_type() != nullptr) {
    options->set_argument_type_parse_location(
        function_argument.templated_parameter_type()->GetParseLocationRange());
  } else if (function_argument.tvf_schema() != nullptr) {
    options->set_argument_type_parse_location(
        function_argument.tvf_schema()->GetParseLocationRange());
  }
}

void Resolver::RecordTVFRelationColumnParseLocationsIfPresent(
    const ASTTVFSchemaColumn& tvf_schema_column, TVFRelation::Column* column) {
  if (analyzer_options_.parse_location_record_type() ==
      PARSE_LOCATION_RECORD_NONE) {
    return;
  }
  // Column name is an optional field.
  if (tvf_schema_column.name() != nullptr) {
    column->name_parse_location_range =
        tvf_schema_column.name()->GetParseLocationRange();
  }
  column->type_parse_location_range =
      tvf_schema_column.type()->GetParseLocationRange();
}

absl::Status Resolver::ResolveModel(
    const ASTPathExpression* path_expr,
    std::unique_ptr<const ResolvedModel>* resolved_model) {
  const Model* model = nullptr;
  const absl::Status find_status =
      catalog_->FindModel(path_expr->ToIdentifierVector(), &model,
                          analyzer_options_.find_options());

  if (find_status.code() == absl::StatusCode::kNotFound) {
    return MakeSqlErrorAt(path_expr)
           << "Model not found: " << path_expr->ToIdentifierPathString();
  }
  ZETASQL_RETURN_IF_ERROR(find_status);

  *resolved_model = MakeResolvedModel(model);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDescriptorFirstPass(
    const ASTDescriptorColumnList* column_list,
    std::unique_ptr<const ResolvedDescriptor>* resolved_descriptor) {
  std::vector<std::string> descriptor_column_name_list;
  for (const ASTDescriptorColumn* const descriptor_column :
       column_list->descriptor_column_list()) {
    descriptor_column_name_list.push_back(
        descriptor_column->name()->GetAsString());
  }
  *resolved_descriptor = MakeResolvedDescriptor(std::vector<ResolvedColumn>(),
                                                descriptor_column_name_list);
  return ::absl::OkStatus();
}

absl::Status Resolver::FinishResolvingDescriptor(
    const ASTTVFArgument* ast_tvf_argument,
    const std::unique_ptr<const NameScope>& name_scope,
    int table_argument_offset,
    std::unique_ptr<const ResolvedDescriptor>* resolved_descriptor) {
  std::vector<ResolvedColumn> descriptor_column_list;
  std::vector<std::string> descriptor_column_name_list(
      resolved_descriptor->get()->descriptor_column_name_list());

  // resolve descriptor names from input table.
  for (int i = 0;
       i < resolved_descriptor->get()->descriptor_column_name_list().size();
       i++) {
    const std::string& name =
        resolved_descriptor->get()->descriptor_column_name_list()[i];
    NameTarget target;

    if (!name_scope->LookupName(id_string_pool_->Make(name), &target)) {
      return MakeSqlErrorAt(ast_tvf_argument->descriptor()
                                ->columns()
                                ->descriptor_column_list()
                                .at(i))
             << "DESCRIPTOR specifies " << name
             << ", which does not exist in the table passed as argument "
             << table_argument_offset + 1;
    } else if (target.IsAmbiguous()) {
      return MakeSqlErrorAt(ast_tvf_argument->descriptor()
                                ->columns()
                                ->descriptor_column_list()
                                .at(i))
             << "DESCRIPTOR specifies " << name
             << ", which is ambiguous in the table passed as argument "
             << table_argument_offset + 1;
    } else {
      descriptor_column_list.push_back(target.column());
    }
  }

  *resolved_descriptor = MakeResolvedDescriptor(descriptor_column_list,
                                                descriptor_column_name_list);
  return ::absl::OkStatus();
}

absl::Status Resolver::ResolveConnection(
    const ASTPathExpression* path_expr,
    std::unique_ptr<const ResolvedConnection>* resolved_connection) {
  const Connection* connection = nullptr;
  const absl::Status find_status =
      catalog_->FindConnection(path_expr->ToIdentifierVector(), &connection,
                               analyzer_options_.find_options());

  if (find_status.code() == absl::StatusCode::kNotFound) {
    return MakeSqlErrorAt(path_expr)
           << "Connection not found: " << path_expr->ToIdentifierPathString();
  }
  ZETASQL_RETURN_IF_ERROR(find_status);

  *resolved_connection = MakeResolvedConnection(connection);
  return absl::OkStatus();
}

bool Resolver::IsPathExpressionStartingFromNamedSubquery(
    const ASTPathExpression* path_expr) {
  std::vector<IdString> path;
  path.reserve(path_expr->num_names() - 1);
  for (int i = 0; i < path_expr->num_names() - 1; ++i) {
    path.push_back(path_expr->names().at(i)->GetAsIdString());
    if (named_subquery_map_.contains(path)) {
      return true;
    }
  }
  return false;
}

static bool IsColumnOfTableArgument(const ASTPathExpression* path_expr,
                                    const FunctionArgumentInfo* arguments) {
  if (arguments == nullptr) {
    return false;  // No arguments are in scope.
  }
  if (path_expr->num_names() < 2) {
    return false;
  }
  const FunctionArgumentInfo::ArgumentDetails* details =
      arguments->FindTableArg(path_expr->first_name()->GetAsIdString());
  if (details == nullptr || details->arg_type.IsTemplated()) {
    return false;
  }
  const TVFRelation& table =
      details->arg_type.options().relation_input_schema();
  for (int i = 0; i < table.num_columns(); ++i) {
    const TVFSchemaColumn& column = table.column(i);
    if (zetasql_base::CaseEqual(
            path_expr->name(1)->GetAsIdString().ToStringView(), column.name)) {
      return true;
    }
  }
  return false;
}

absl::Status Resolver::ResolvePathExpressionAsTableScan(
    const ASTPathExpression* path_expr, IdString alias, bool has_explicit_alias,
    const ASTNode* alias_location, const ASTHint* hints,
    const ASTForSystemTime* for_system_time, const NameScope* scope,
    std::unique_ptr<const ResolvedTableScan>* output,
    std::shared_ptr<const NameList>* output_name_list) {
  ZETASQL_RET_CHECK(output != nullptr);
  ZETASQL_RET_CHECK(output_name_list != nullptr);
  ZETASQL_RET_CHECK(path_expr != nullptr);
  ZETASQL_RET_CHECK(!alias.empty());
  ZETASQL_RET_CHECK(alias_location != nullptr);

  if (analyzing_partition_by_clause_name_ != nullptr) {
    return MakeSqlErrorAt(path_expr)
           << analyzing_partition_by_clause_name_
           << " expression cannot contain a table scan";
  }

  const Table* table = nullptr;
  const absl::Status find_status =
      catalog_->FindTable(path_expr->ToIdentifierVector(), &table,
                          analyzer_options_.find_options());
  if (find_status.code() == absl::StatusCode::kNotFound) {
    std::string error_message;
    absl::StrAppend(&error_message,
                    "Table not found: ", path_expr->ToIdentifierPathString());

    // We didn't find the name when trying to resolve it as a table.
    // If it looks like it might have been intended as a name from the scope,
    // give a more helpful error.
    if (IsPathExpressionStartingFromScope(path_expr, scope)) {
      absl::StrAppend(
          &error_message,
          " (Unqualified identifiers in a FROM clause are always resolved "
          "as tables. Identifier ",
          ToIdentifierLiteral(path_expr->first_name()->GetAsIdString()),
          " is in scope but unqualified names cannot be resolved here.)");
    } else if (IsPathExpressionStartingFromNamedSubquery(path_expr)) {
      absl::StrAppend(
          &error_message, "; Table name ", path_expr->ToIdentifierPathString(),
          " starts with a WITH clause alias and references a column from that",
          " table, which is invalid in the FROM clause");
    } else if (IsColumnOfTableArgument(path_expr, function_argument_info_)) {
      absl::StrAppend(
          &error_message, "; Table name ", path_expr->ToIdentifierPathString(),
          " starts with a TVF table-valued argument name and references a ",
          "column from that table, which is invalid in the FROM clause");
    } else {
      const std::string table_suggestion =
          catalog_->SuggestTable(path_expr->ToIdentifierVector());
      if (!table_suggestion.empty()) {
        absl::StrAppend(&error_message, "; Did you mean ", table_suggestion,
                        "?");
      }
    }
    return MakeSqlErrorAt(path_expr) << error_message;
  }
  ZETASQL_RETURN_IF_ERROR(find_status);

  ZETASQL_RET_CHECK(table != nullptr);
  const IdString table_name = MakeIdString(table->Name());

  const bool is_value_table = table->IsValueTable();
  if (is_value_table) {
    ZETASQL_RETURN_IF_ERROR(CheckValidValueTable(path_expr, table));
  }

  ResolvedColumnList column_list;
  std::shared_ptr<NameList> name_list(new NameList);
  for (int i = 0; i < table->NumColumns(); ++i) {
    const Column* column = table->GetColumn(i);
    IdString column_name = MakeIdString(column->Name());
    if (column_name.empty()) {
      column_name = MakeIdString(absl::StrCat("$col", i + 1));
    }

    column_list.emplace_back(ResolvedColumn(
        AllocateColumnId(), table_name, column_name,
        AnnotatedType(column->GetType(), column->GetTypeAnnotationMap())));
    // Save the Catalog column for this ResolvedColumn so it can later be used
    // for checking column properties like Column::IsWritableColumn().
    resolved_columns_from_table_scans_[column_list.back()] = column;
    if (column->IsPseudoColumn()) {
      ZETASQL_RETURN_IF_ERROR(name_list->AddPseudoColumn(
          column_name, column_list.back(), path_expr));
    } else if (is_value_table) {
      ZETASQL_RET_CHECK_EQ(i, 0);  // Verified in CheckValidValueTable.
      // Defer AddValueTableColumn until after adding the pseudo-columns.
    } else {
      // is_explicit=false because we're adding all columns of a table.
      ZETASQL_RETURN_IF_ERROR(name_list->AddColumn(column_name, column_list.back(),
                                           false /* is_explicit */));
    }
  }
  if (is_value_table) {
    // So far, we've accumulated the pseudo-columns only.  Now add the
    // value table column, and pass in the list of pseudo-columns so they
    // can be attached to the range variable for the value table.
    ZETASQL_RET_CHECK_EQ(name_list->num_columns(), 0);
    ZETASQL_RETURN_IF_ERROR(name_list->AddValueTableColumn(
        alias, column_list[0], path_expr, {} /* excluded_field_names */,
        name_list));
    name_list->set_is_value_table(true);
  }

  std::unique_ptr<const ResolvedExpr> for_system_time_expr;
  if (for_system_time != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ResolveForSystemTimeExpr(for_system_time, &for_system_time_expr));
  }

  std::vector<int> column_index_list(column_list.size());
  // Fill column_index_list with 0, 1, 2, ..., column_list.size()-1.
  std::iota(column_index_list.begin(), column_index_list.end(), 0);

  std::unique_ptr<ResolvedTableScan> table_scan =
      MakeResolvedTableScan(column_list, table, std::move(for_system_time_expr),
                            has_explicit_alias ? alias.ToString() : "");
  table_scan->set_column_index_list(column_index_list);
  ZETASQL_RETURN_IF_ERROR(ResolveHintsForNode(hints, table_scan.get()));

  // The number of columns should equal the number of regular columns plus
  // the number of pseudo-columns in name_list, but we don't maintain the
  // count of pseudo-columns so we can't check that exactly.
  ZETASQL_RET_CHECK_GE(table_scan->column_list_size(), name_list->num_columns());

  *output_name_list = name_list;
  // Add a range variable for the whole scan unless this is a value table. For
  // value tables, the column's name already serves that purpose.
  if (!is_value_table) {
    ZETASQL_ASSIGN_OR_RETURN(*output_name_list,
                     NameList::AddRangeVariableInWrappingNameList(
                         alias, alias_location, *output_name_list));
  }
  MaybeRecordParseLocation(path_expr, table_scan.get());
  *output = std::move(table_scan);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveForSystemTimeExpr(
    const ASTForSystemTime* for_system_time,
    std::unique_ptr<const ResolvedExpr>* resolved) {
  // Resolve against an empty NameScope, because column references don't
  // make sense in this clause (it needs to be constant).
  ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(for_system_time->expression(),
                                    empty_name_scope_.get(),
                                    "FOR SYSTEM_TIME AS OF", resolved));

  // Try to coerce STRING literals to TIMESTAMP, but ignore error if it
  // didn't work - proper error will be raised below.
  if (((*resolved)->node_kind() == RESOLVED_LITERAL &&
       (*resolved)->type()->IsString())) {
    CoerceExprToType(for_system_time, type_factory_->get_timestamp(),
                     kExplicitCoercion, resolved)
        .IgnoreError();  // TODO
  }

  if (!(*resolved)->type()->IsTimestamp()) {
    return MakeSqlErrorAt(for_system_time->expression())
           << "FOR SYSTEM_TIME AS OF must be of type TIMESTAMP but was of "
              "type "
           << (*resolved)->type()->ShortTypeName(product_mode());
  }
  return absl::OkStatus();
}

namespace {

// Extracts an expression from the given scan at the provided column,
// returning nullptr if the column doesn't exist in the scan or does not
// contain an expression.
absl::StatusOr<const ResolvedExpr*> GetColumnExpr(
    const ResolvedProjectScan* scan, const ResolvedColumn& column) {
  for (const std::unique_ptr<const ResolvedComputedColumn>& computed_column :
       scan->expr_list()) {
    const ResolvedExpr* expr = computed_column->expr();
    ZETASQL_RET_CHECK_NE(expr, nullptr);
    if (computed_column->column().column_id() == column.column_id()) {
      return expr;
    }
  }
  return nullptr;
}

}  // namespace

absl::Status Resolver::CoerceQueryStatementResultToTypes(
    const ASTNode* ast_node, absl::Span<const Type* const> types,
    std::unique_ptr<const ResolvedScan>* scan,
    std::shared_ptr<const NameList>* output_name_list) {
  const std::vector<NamedColumn>& column_list = (*output_name_list)->columns();
  if (types.size() != column_list.size()) {
    return MakeSqlErrorAt(ast_node)
           << "Query has unexpected number of output columns, "
           << "expected " << types.size() << ", but had " << column_list.size();
  }
  ZETASQL_RET_CHECK((*scan)->node_kind() == RESOLVED_PROJECT_SCAN);
  ResolvedColumnList casted_column_list;
  std::vector<std::unique_ptr<const ResolvedComputedColumn>> casted_exprs;
  auto name_list = std::make_shared<NameList>();
  for (int i = 0; i < types.size(); ++i) {
    const Type* result_type = column_list[i].column.type();
    const Type* target_type = types[i];
    if (result_type->Equals(target_type)) {
      casted_column_list.emplace_back(column_list[i].column);
      ZETASQL_RETURN_IF_ERROR(name_list->AddColumn(column_list[i].name,
                                           column_list[i].column,
                                           column_list[i].is_explicit));
    } else {
      // Extract and coerce an expression out of each column.
      //
      // When the result type of the query's output column does
      // not match the target type, then we try to coerce the output
      // column to the target type.  We use assignment coercion
      // rules for determining if coercion is allowed.  We also use
      // the projected expression when present (as opposed to the
      // projected column), since that allows us to do extra coercion
      // for literals such as:
      //
      // target_type: {DATE, TIMESTAMP}
      // query: SELECT '2011-01-01' as d, '2011-01-01 12:34:56' as t;
      //
      // target_type: {ENUM}
      // query: SELECT CAST(0 AS INT32) as e;

      ZETASQL_ASSIGN_OR_RETURN(const ResolvedExpr* column_expr,
                       GetColumnExpr((*scan)->GetAs<ResolvedProjectScan>(),
                                     column_list[i].column));
      std::unique_ptr<const ResolvedExpr> column_ref;
      if (column_expr == nullptr) {
        column_ref = MakeColumnRef(column_list[i].column);
        column_expr = column_ref.get();
      }
      // Disallow untyped parameters for now, we can't mutably change them and
      // adding in a duplicate parameter into the tree with a different type
      // triggers an error.
      if (column_expr->node_kind() == RESOLVED_PARAMETER &&
          column_expr->GetAs<ResolvedParameter>()->is_untyped()) {
        return MakeSqlErrorAt(ast_node)
               << "Untyped parameter cannot be coerced to an output target "
               << "type for a query";
      }
      SignatureMatchResult unused;
      if (!coercer_.AssignableTo(GetInputArgumentTypeForExpr(column_expr),
                                 target_type,
                                 /* is_explicit = */ false, &unused)) {
        return MakeSqlErrorAt(ast_node)
               << "Query column " << (i + 1) << " has type "
               << result_type->ShortTypeName(product_mode())
               << " which cannot be coerced to target type "
               << target_type->ShortTypeName(product_mode());
      }
      std::unique_ptr<const ResolvedExpr> casted_expr =
          MakeColumnRef(column_list[i].column);
      const ASTNode* ast_location =
          GetASTNodeForColumn(ast_node, i, static_cast<int>(types.size()));
      ZETASQL_RETURN_IF_ERROR(function_resolver_->AddCastOrConvertLiteral(
          ast_location, target_type, /*format=*/nullptr,
          /*time_zone=*/nullptr, TypeParameters(), &**scan,
          /* set_has_explicit_type =*/false,
          /* return_null_on_error =*/false, &casted_expr));
      const ResolvedColumn casted_column(AllocateColumnId(), kCastedColumnId,
                                         column_list[i].name, target_type);
      RecordColumnAccess(casted_column);
      casted_column_list.emplace_back(casted_column);
      casted_exprs.push_back(
          MakeResolvedComputedColumn(casted_column, std::move(casted_expr)));
      ZETASQL_RETURN_IF_ERROR(
          name_list->AddColumn(column_list[i].name, casted_column,
                               (*output_name_list)->column(i).is_explicit));
    }
  }
  if (!casted_exprs.empty()) {
    *scan = MakeResolvedProjectScan(casted_column_list, std::move(casted_exprs),
                                    std::move(*scan));
    ZETASQL_RET_CHECK_EQ((*output_name_list)->num_columns(), name_list->num_columns());
    name_list->set_is_value_table((*output_name_list)->is_value_table());
    *output_name_list = name_list;
  }

  return absl::OkStatus();
}

}  // namespace zetasql
