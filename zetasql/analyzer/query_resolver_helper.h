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

#ifndef ZETASQL_ANALYZER_QUERY_RESOLVER_HELPER_H_
#define ZETASQL_ANALYZER_QUERY_RESOLVER_HELPER_H_

#include <stddef.h>

#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zetasql/analyzer/expr_matching_helpers.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/select_with_mode.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace zetasql {

class AnalyticFunctionResolver;
class Resolver;
class SelectColumnStateList;
struct ExprResolutionInfo;

using ResolvedComputedColumnList = std::vector<const ResolvedComputedColumn*>;

// An enum kind for grouping set.
enum class GroupingSetKind {
  // It indicates the current grouping set is user-written grouping set, e.g.
  // GROUPING SETS(x, y)
  kGroupingSet,
  // It indicates the grouping set is a rollup, e.g. ROLLUP(x, y), or a rollup
  // inside GROUPING SETS, e.g. GROUPING SETS(ROLLUP(x, y))
  kRollup,
  // It incidates the grouping set is a cube, e.g. CUBE(x, y), or a cube inside
  // GROUPING SETS, e.g. GROUPING SETS(CUBE(x, y))
  kCube,
};

// An enum that describes special query forms within ASTSelect which modify
// certain other behaviors during SELECT resolving.
enum class SelectForm {
  // A normal SELECT query.
  kClassic,
  // A no-FROM-clause SELECT query.
  kNoFrom,
  // An ASTSelect representing a pipe SELECT operator.
  kPipeSelect,
  // An ASTSelect representing a pipe EXTEND operator.
  kPipeExtend,
  // An ASTSelect representing a pipe AGGREGATE operator.
  kPipeAggregate,
  // An ASTSelect representing a pipe WINDOW operator.
  kPipeWindow,
  // An ASTSelect representing the graph query RETURN operator.
  kGqlReturn,
  // An ASTSelect representing the graph query WITH operator.
  kGqlWith,
};

struct GroupingSetInfo {
  // It contains a list of grouping set item in the current grouping set.
  // Each grouping set item is stored as a list of computed columns to
  // represent multi-columns in a grouping sets.
  // When the current grouping set kind is kGroupingSet, we guarantee there is
  // exactly one expression in the ResolvedComputedColumnList, as there aren't
  // multi-columns in grouping set. E.g. In GROUPING SETS(x, (y, z)), grouping
  // set x and y are stored the below accordingly:
  // x: GroupingSetInfo {
  //   grouping_set_item_list: {{x}}
  //   kind: kGroupingSet
  //}
  // y: GroupingSetInfo {
  //   grouping_set_item_list: {{y}, {z}}
  //   kind: kGroupingSet
  // }
  // When the current grouping set is a rollup or cube, multi-columns will be
  // stored as a list of columns in ResolvedComputedColumnList. In the query,
  // GROUPING SETS(ROLLUP((x, y), z), CUBE((x, y), (y, z))), the grouping set
  // rollup and cube are stored as the below accordindly.
  // ROLLUP((x, y), z):
  //   GroupingSetInfo {
  //     grouping_set_item_list: {{x,y}, {z}}
  //     kind: kRollup
  //   }
  // CUBE((x, y), (y, z)):
  //   GroupingSetInfo {
  //     grouping_set_item_list: {{x,y}, {y,z}}
  //     kind: kCube
  //   }
  std::vector<ResolvedComputedColumnList> grouping_set_item_list;
  // The kind of current grouping set, it can be a kGroupingSet, kRollup, or
  // kCube.
  GroupingSetKind kind;
};

// A struct to preserve the column ids in the grouping set.
struct GroupingSetIds {
  // The list of column ids of the grouping set, with the same preserved struct
  // of the current grouping set.
  // The outer vector represents the list of grouping set item, and the inner
  // vector represents the list of columns in the multi-column of the current
  // grouping set.
  std::vector<std::vector<int>> ids;
  // The kind of grouping set, it can be a kGroupingSet, kRollup, or kCube.
  GroupingSetKind kind;
};

// This stores a column to order by in the final ResolvedOrderByScan.
// It is used for
// * items from ORDER BY (in QueryResolutionInfo::order_by_item_info).
// * items from pipe AGGREGATE ... GROUP BY to order by
//   (in QueryResolutionInfo::aggregate_order_by_item_info and
//    QueryResolutionInfo::group_by_order_by_item_info).
// Since ordering items come from multiple sources, any needed attributes are
// passed via this object rather than the original ASTOrderBy.
struct OrderByItemInfo {
  // Constructor for ordering by an ordinal (column number).
  OrderByItemInfo(const ASTNode* ast_location_in,
                  const ASTCollate* ast_collate_in, int64_t index,
                  bool descending,
                  ResolvedOrderByItemEnums::NullOrderMode null_order)
      : ast_location(ast_location_in),
        ast_collate(ast_collate_in),
        select_list_index(index),
        is_descending(descending),
        null_order(null_order) {}
  // Constructor for ordering by an expression.
  OrderByItemInfo(const ASTNode* ast_location_in,
                  const ASTCollate* ast_collate_in,
                  std::unique_ptr<const ResolvedExpr> expr, bool descending,
                  ResolvedOrderByItemEnums::NullOrderMode null_order)
      : ast_location(ast_location_in),
        ast_collate(ast_collate_in),
        order_expression(std::move(expr)),
        is_descending(descending),
        null_order(null_order) {}
  // Constructor for ordering by a ResolvedColumn (that will exist when
  // we get to MakeResolvedOrderByScan).
  OrderByItemInfo(const ASTNode* ast_location_in,
                  const ASTCollate* ast_collate_in,
                  const ResolvedColumn& order_column_in, bool descending,
                  ResolvedOrderByItemEnums::NullOrderMode null_order)
      : ast_location(ast_location_in),
        ast_collate(ast_collate_in),
        order_column(order_column_in),
        is_descending(descending),
        null_order(null_order) {}

  // This value is not valid as a 0-based select list index.
  static constexpr int64_t kInvalidSelectListIndex =
      std::numeric_limits<int64_t>::max();

  const ASTNode* ast_location;    // Expression being ordered by.
  const ASTCollate* ast_collate;  // Collate clause, if present.

  bool is_select_list_index() const {
    return select_list_index != kInvalidSelectListIndex;
  }

  // 0-based index into the SELECT list.  A kInvalidSelectListIndex value
  // indicates this ORDER BY expression is not a select list column
  // reference, in which case <order_expression> and <order_column> will
  // be populated.
  int64_t select_list_index = kInvalidSelectListIndex;

  // Expression or ResolvedColumn to order by.
  // Not populated if selecting by ordinal, i.e. <select_list_index> != -1
  // The <order_expression> is originally filled in by ResolveOrderingExprs.
  // AddColumnsForOrderByExprs resolves those expressions to a specific
  // <order_column>, which is then referenced in MakeResolvedOrderByScan.
  //
  // For OrderByItemInfos added from the GROUP BY, the <order_column>
  // is filled in directly, without an <order_expression>,
  // in ResolveGroupingItemExpression.
  std::unique_ptr<const ResolvedExpr> order_expression;
  ResolvedColumn order_column;

  bool is_descending;  // Indicates DESC or ASC.
  // Indicates NULLS LAST or NULLS FIRST.
  ResolvedOrderByItemEnums::NullOrderMode null_order;
};

// A struct representing the state of group by columns.
struct GroupByColumnState {
  // The computed column that will be used as group by keys.
  std::unique_ptr<const ResolvedComputedColumn> computed_column;

  // The pre-group-by expression from the select list without being converted to
  // a reference of the pre-group-by computed column.
  // For example
  //
  // SELECT key+1 AS key1
  // FROM KeyValue
  // GROUP BY key
  // ORDER BY 1
  //
  // The computed column above will contain a column reference of the computed
  // column named `key1`, whose the original form of pre-group-by expression in
  // the select list is `key+1`.
  const ResolvedExpr* pre_group_by_expr = nullptr;

  GroupByColumnState(
      std::unique_ptr<const ResolvedComputedColumn> computed_column,
      const ResolvedExpr* pre_group_by_expr)
      : computed_column(std::move(computed_column)),
        pre_group_by_expr(pre_group_by_expr) {}

  GroupByColumnState(const GroupByColumnState&) = delete;
  GroupByColumnState& operator=(const GroupByColumnState&) = delete;
  GroupByColumnState(GroupByColumnState&&) = default;
  GroupByColumnState& operator=(GroupByColumnState&&) = default;

  std::string DebugString(absl::string_view indent) const {
    std::string debug_string;
    if (computed_column != nullptr) {
      absl::StrAppend(&debug_string, indent, computed_column->DebugString(),
                      "\n");
    }
    if (pre_group_by_expr != nullptr) {
      absl::StrAppend(&debug_string, indent, "pre_group_by_expr:\n",
                      pre_group_by_expr->DebugString(), "\n");
    }
    return debug_string;
  }

  // Returns the pre-group-by expression for a given group-by column state.
  const ResolvedExpr* GetPreGroupByResolvedExpr() const {
    if (pre_group_by_expr != nullptr) {
      return pre_group_by_expr;
    }
    return computed_column->expr();
  }
};

// QueryGroupByAndAggregateInfo is used (and mutated) to store info related
// to grouping/distinct and aggregation analysis for a single SELECT query
// block.
struct QueryGroupByAndAggregateInfo {
  QueryGroupByAndAggregateInfo() {}

  // Identifies whether or not group by or aggregation is present in this
  // (sub)query.
  bool has_group_by = false;
  bool has_aggregation = false;

  // Map from an aggregate function ASTNode to the related
  // ResolvedComputedColumn.  Populated during the first pass resolution of
  // expressions.  Second pass resolution of expressions will use these
  // computed columns for the given aggregate expression.
  // Not owned.
  // The ResolvedComputedColumns are owned by <aggregate_columns_>.
  std::map<const ASTFunctionCall*, const ResolvedComputedColumnBase*>
      aggregate_expr_map;

  // A list of GroupByColumnState containing expressions that must be computed.
  std::vector<GroupByColumnState> group_by_column_state_list;

  // Map of group by expressions to entries within group_by_columns_to_compute.
  // TODO: FieldPathExpressionEqualsOperator is not 100% compatible
  // with the behavior expected by hash maps: some expressions cannot be found
  // in the map right after the insertion. We need either to use a different
  // comparison function, or make sure that the map is used only for the
  // supported expression types.
  std::unordered_map<const ResolvedExpr*, const ResolvedComputedColumn*,
                     FieldPathHashOperator, FieldPathExpressionEqualsOperator>
      group_by_expr_map;

  // This is a list of grouping sets, or an empty vector if the query doesn't
  // have ROLLUP, CUBE, or GROPING SETS.
  std::vector<GroupingSetInfo> grouping_set_list;

  // Columns referenced by GROUPING function calls. A GROUPING function call
  // has a single ResolvedComputedColumn argument per call as well as an
  // output column to be referenced in column lists.
  std::vector<std::unique_ptr<const ResolvedGroupingCall>> grouping_call_list;

  // Aggregations to compute that are scoped by a MATCH_RECOGNIZE pattern
  // variable. This is like `aggregate_columns_to_compute` but maintains the
  // pattern variable representing the rows in the match over which these
  // aggregations are computed.  Only applicable for aggregate functions in the
  // MEASURE clause of a MATCH_RECOGNIZE clause.
  IdStringHashMapCase<
      std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>>
      match_recognize_aggregate_columns_to_compute;

  // Aggregate function calls that must be computed.
  // This is built up as expressions are resolved.  During expression
  // resolution, aggregate functions are moved into <aggregate_columns_> and
  // replaced by a ResolvedColumnRef pointing at the ResolvedColumn created
  // here.
  std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
      aggregate_columns_to_compute;

  // A list of unique pointers that need to stick around until being cleaned
  // up when the aggregate scan is built.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      grouping_output_columns;

  // Stores information about STRUCT or PROTO fields that appear in the
  // GROUP BY.
  //
  // Each entry indicates:
  //   - a (pre-GROUP BY) source ResolvedColumn key where field access begins
  //   - a ValidNamePathList value, where each ValidNamePath indicates:
  //     - a name path (i.e., 'a.b.c')
  //     - a target (post-GROUP BY) ResolvedColumn that the name path resolves
  //       to from the source column.
  //
  // For instance, "GROUP BY a.b.c, a.d" will produce a single map entry for
  // the source ResolvedColumn for 'a', along with a related list with two
  // valid name paths:
  //   - 'b.c' with its corresponding post-GROUP BY ResolvedColumn
  //   - 'd' with its corresponding post-GROUP BY ResolvedColumn
  ValidFieldInfoMap group_by_valid_field_info_map;

  // Whether or not resolution context is post-DISTINCT, implying that
  // the group by/aggregation related information is for DISTINCT (which
  // impacts error messaging).
  bool is_post_distinct = false;

  // Identifies whether the original SQL syntax is GROUP BY ALL.
  bool is_group_by_all = false;

  // Resets all fields to their initial values, and empties all maps and
  // lists.
  void Reset();
};

// SelectColumnState contains state related to an expression in the
// select-list of a query, while it is being resolved.  This is used and
// mutated in multiple passes while resolving the SELECT-list and GROUP BY.
// TODO: Convert this to an enacapsulated class.
struct SelectColumnState {
  explicit SelectColumnState(
      const ASTSelectColumn* ast_select_column, IdString alias_in,
      bool is_explicit_in, bool has_aggregation_in, bool has_analytic_in,
      bool has_volatile_in,
      std::unique_ptr<const ResolvedExpr> resolved_expr_in)
      : ast_select_column(ast_select_column),
        ast_expr(ast_select_column->expression()),
        ast_grouping_item_order(ast_select_column->grouping_item_order()),
        alias(alias_in),
        is_explicit(is_explicit_in),
        select_list_position(-1),
        resolved_expr(std::move(resolved_expr_in)),
        has_aggregation(has_aggregation_in),
        has_analytic(has_analytic_in),
        has_volatile(has_volatile_in) {}

  SelectColumnState(const SelectColumnState&) = delete;
  SelectColumnState& operator=(const SelectColumnState&) = delete;

  // Gets the Type of this SELECT list column.  Can return NULL if the
  // related <ast_expr> has not been resolved yet.
  const Type* GetType() const;

  // Returns whether or not this SELECT list column has a pre-GROUP BY
  // column assigned to it.
  bool HasPreGroupByResolvedColumn() const {
    return resolved_pre_group_by_select_column.IsInitialized();
  }

  // Returns a multi-line debug string, where each line is prefixed by <indent>.
  std::string DebugString(absl::string_view indent = "") const;

  // The `ASTSelectColumn` for with this `SelectColumnState`.
  const ASTSelectColumn* ast_select_column;

  // The expression for this selected column.
  // Points at the * if this came from SELECT *.
  const ASTExpression* ast_expr;

  // Stores the ordering suffix applied for this select item.
  const ASTGroupingItemOrder* ast_grouping_item_order;

  // The alias provided by the user or computed for this column.
  const IdString alias;

  // True if the alias for this column is an explicit name. Generally, explicit
  // names come directly from the query text, and implicit names are those that
  // are generated automatically from something outside the query text, like
  // column names that come from a table schema. Explicitness does not change
  // any scoping behavior except for the final check in strict mode that may
  // raise an error. For more information, please see the beginning of
  // (broken link).
  const bool is_explicit;

  // 0-based position in the SELECT-list after star expansion.
  // Stores -1 when position is not known yet. This never happens for a
  // SelectColumnState stored inside a SelectColumnStateList.
  int select_list_position;

  // Owned ResolvedExpr for this SELECT list column.  If we need a
  // ResolvedComputedColumn for this SELECT column, then ownership of
  // this <resolved_expr> will be transferred to that ResolvedComputedColumn
  // and <resolved_expr> will be set to NULL.
  std::unique_ptr<const ResolvedExpr> resolved_expr;

  // Unowned ResolvedExpr for this SELECT list item's original expression before
  // it's updated to be a reference to a computed expression.
  // It's only used if the `resolved_expr` is updated to a computed column.
  // TODO: b/325532418 - propagate this as `resolved_expr` unconditionally
  const ResolvedExpr* original_resolved_expr = nullptr;

  // References the related ResolvedComputedColumn for this SELECT list column,
  // if one is needed.  Otherwise it is NULL.  The referenced
  // ResolvedComputedColumn is owned by a column list in QueryResolutionInfo.
  // The reference here is required to allow us to maintain the relationship
  // between this SELECT list column and its related expression for
  // subsequent HAVING and ORDER BY expression analysis.
  // Not owned.
  const ResolvedComputedColumn* resolved_computed_column = nullptr;

  // True if this expression includes aggregation.  Select-list expressions
  // that use aggregation cannot be referenced in GROUP BY.
  bool has_aggregation = false;

  // True if this expression includes analytic functions.
  bool has_analytic = false;

  // True if this expression includes any volatile function.
  bool has_volatile = false;

  // If true, this expression is used as a GROUP BY key.
  bool is_group_by_column = false;

  // If true, this expression uses a function with GROUP ROWS or GROUP BY
  // modifiers and that function is not in an expression subquery. This
  // expression is resolved in `ResolveDeferredFirstPassSelectListExprs` after
  // the GROUP BY clause is resolved.
  bool contains_outer_group_rows_or_group_by_modifiers = false;

  // The output column of this select list item.  It is projected by a scan
  // that computes the related expression.  After the SELECT list has
  // been fully resolved, <resolved_select_column> will be initialized.
  // After it is set, it is used in subsequent expression resolution (SELECT
  // list ordinal references and SELECT list alias references).
  ResolvedColumn resolved_select_column;

  // If set, indicates the pre-GROUP BY version of the column.  Will only
  // be set if the column must be computed before the AggregateScan (so
  // it will not necessarily always be set if is_group_by_column is true).
  ResolvedColumn resolved_pre_group_by_select_column;
};

// This class contains a SelectColumnState for each column in the SELECT list
// and resolves the alias or ordinal references to the SELECT-list column.
class SelectColumnStateList {
 public:
  SelectColumnStateList() {}
  SelectColumnStateList(const SelectColumnStateList&) = delete;
  SelectColumnStateList& operator=(const SelectColumnStateList&) = delete;

  // Creates and returns a SelectColumnState for a new SELECT-list column.
  // 'is_explicit' should be true if 'alias' is an explicit name. Generally,
  // explicit names come directly from the query text, and implicit names are
  // those that are generated automatically from something outside the query
  // text, like column names that come from a table schema. Explicitness does
  // not change any scoping behavior except for the final check in strict mode
  // that may raise an error. For more information, please see the beginning of
  // (broken link).
  void AddSelectColumn(const ASTSelectColumn* ast_select_column, IdString alias,
                       bool is_explicit, bool has_aggregation,
                       bool has_analytic, bool has_volatile,
                       std::unique_ptr<const ResolvedExpr> resolved_expr);

  // Add an already created SelectColumnState. If save_mapping is true, saves a
  // mapping from the alias to this SelectColumnState. The mapping is later used
  // for validations performed by FindAndValidateSelectColumnStateByAlias().
  void AddSelectColumn(std::unique_ptr<SelectColumnState> select_column_state);

  // Replace the existing `SelectColumnState` at `index` in
  // `select_column_state_list_` with `new_select_column_state`.
  absl::Status ReplaceSelectColumn(
      int index, std::unique_ptr<SelectColumnState> new_select_column_state);

  // Finds a SELECT-list column by alias. Returns an error if the
  // name is ambiguous or the referenced column contains an aggregate or
  // analytic function that is disallowed as per <expr_resolution_info>.
  // If the name is not found, sets <*select_column_state> to NULL and
  // returns OK.
  absl::Status FindAndValidateSelectColumnStateByAlias(
      const char* clause_name, const ASTNode* ast_location, IdString alias,
      const ExprResolutionInfo* expr_resolution_info,
      const SelectColumnState** select_column_state) const;

  // Finds a SELECT-list column by ordinal. Returns an error if
  // the ordinal number is out of the valid range or the referenced column
  // contains an aggregate or analytic function that is disallowed as per
  // <expr_resolution_info>.
  absl::Status FindAndValidateSelectColumnStateByOrdinal(
      absl::string_view expr_description, const ASTNode* ast_location,
      int64_t ordinal, const ExprResolutionInfo* expr_resolution_info,
      const SelectColumnState** select_column_state) const;

  static absl::Status ValidateAggregateAndAnalyticSupport(
      absl::string_view column_description, const ASTNode* ast_location,
      const SelectColumnState* select_column_state,
      const ExprResolutionInfo* expr_resolution_info);

  // <select_list_position> is 0-based position after star expansion.
  SelectColumnState* GetSelectColumnState(int select_list_position);
  const SelectColumnState* GetSelectColumnState(int select_list_position) const;

  const std::vector<std::unique_ptr<SelectColumnState>>&
  select_column_state_list() const;

  // Returns the number of SelectColumnStates.
  size_t Size() const;

  std::string DebugString() const;

 private:
  std::vector<std::unique_ptr<SelectColumnState>> select_column_state_list_;

  // Map from SELECT-list column aliases (lowercase) to column
  // position in select_column_state_list_. These names can be referenced in
  // GROUP BY, overriding other names in scope. Ambiguous names will be
  // stored as -1.
  std::map<IdString, int, IdStringCaseLess>
      column_alias_to_state_list_position_;
};

// This represents extra items to prepend on the output column list and
// NameList, used when resolving pipe operators.  For pipe AGGREGATE, it's the
// GROUP BY columns.  For pipe WINDOW, it's the passed-through input columns.
struct PipeExtraSelectItem {
  PipeExtraSelectItem(IdString alias_in, ResolvedColumn column_in)
      : alias(alias_in), column(column_in) {}

  const IdString alias;
  const ResolvedColumn column;
};

// Tracks the detected scope of the input range variable while resolving an
// AggregateFunctionCall. If a MATCH_RECOGNIZE pattern variable is detected,
// e.g. when resolving A.x in MAX(A.x) where A is one of the defined variables,
// the `target_pattern_variable_ref` will be set to indicate that the current
// aggregation is ranging only over rows assigned to A in each match.
// `row_range_determined` is set to true during query resolution when the
// aggregate function resolves its *first* non-correlated column. This column
// determines the range of rows the aggregate function operates over. If the
// column is referenced from a pattern variable, then
// `target_pattern_variable_ref` is updated with the pattern variable name and
// the range of rows is determined by the given pattern variable. Else,
// `target_pattern_variable_ref` is empty and the range of rows is all the input
// rows to the aggregate function.

// Once `row_range_determined` is set, the resolver requires that all remaining
// non-correlated column references for the aggregate function match
// `target_pattern_variable_ref`.
//
// If no uncorrelated, pre-grouping columns are referenced yet, and still no
// pattern variable is detected, the state is still unknown and can change to
// either upon detection.
struct MatchRecognizeAggregationState {
  // The MATCH_RECOGNIZE pattern variable ref indicating the rows this
  // expression is ranging against, nullopt if it's not scoped to a pattern
  // variable. For example, when resolving the measures in:
  //   MEASURES avg(a.x - a.y) AS m1, avg(x - y) AS m2
  //   .. DEFINE a AS ..
  // * For m1, this field will indicate "a"
  // * For m2, this field will be nullopt.
  std::optional<IdString> target_pattern_variable_ref = std::nullopt;

  // True if this expression references any uncorrelated, pre-grouping columns.
  // When true, this means the expression already determined the range of rows
  // it will operate over (all input rows if `target_pattern_variable_ref` is
  // not set, or rows assigned to that variable if it is set).
  //
  // When false, it means the expression is only referencing correlated columns
  // or grouping constants, and the choice for the range is still an open
  // decision.
  bool row_range_determined = false;
};

// QueryResolutionInfo is used (and mutated) to store info related to
// the analysis of a single SELECT query block.  It stores information
// related to SELECT list entries, grouping and aggregation, and analytic
// functions.  Detailed descriptions for each field are included below.
// See comments on Resolver::ResolveSelect() for discussion of
// the various phases of analysis and how QueryResolutionInfo is updated
// and referenced during that process.
class QueryResolutionInfo {
 public:
  // Constructor. Does not take ownership of <resolver>.
  explicit QueryResolutionInfo(Resolver* resolver)
      : QueryResolutionInfo(resolver, /*parent=*/nullptr) {}
  // Constructor used when resolving nested aggregates within a multi-level
  // aggregate function.
  explicit QueryResolutionInfo(Resolver* resolver,
                               const QueryResolutionInfo* parent);

  QueryResolutionInfo(const QueryResolutionInfo&) = delete;
  QueryResolutionInfo& operator=(const QueryResolutionInfo&) = delete;
  ~QueryResolutionInfo();

  // Adds group by column `column`, which is computed from `expr`, and the
  // pre-group-by format of `expr` from the select list. If a field path
  // expression that is equivalent to `expr` is already present in the list of
  // group by computed columns AND `override_existing_column` is set to false,
  // the column is not added to the list. Returns an unowned pointer to the
  // ResolvedComputedColumn for this expression.
  const ResolvedComputedColumn* AddGroupByComputedColumnIfNeeded(
      const ResolvedColumn& column, std::unique_ptr<const ResolvedExpr> expr,
      const ResolvedExpr* pre_group_by_expr, bool override_existing_column);

  // Returns a pointer to an existing equivalent ResolvedComputedColumn in
  // group_by_columns_to_compute(), as determined by IsSameFieldPath(), or
  // nullptr if there is no equivalent expression.
  const ResolvedComputedColumn* GetEquivalentGroupByComputedColumnOrNull(
      const ResolvedExpr* expr) const;

  // Adds a grouping set to the grouping_set_list.
  void AddGroupingSet(const GroupingSetInfo& grouping_set);

  // Adds a GROUPING <column> to the grouping_call_list.
  void AddGroupingColumn(std::unique_ptr<const ResolvedGroupingCall> column);

  // Adds a ResolvedAggregateFunctionCall to the aggregate_expression_map.
  // It's argument will be evaluated for a group_by_column match later on and
  // added to the grouping_call_list. This function is used in the
  // ResolveSelectColumnFirstPass, while AddGroupingColumn is used in
  // ResolveSelectColumnSecondPass.
  absl::Status AddGroupingColumnToExprMap(
      const ASTFunctionCall* ast_function_call,
      std::unique_ptr<const ResolvedComputedColumn> grouping_output_col);

  // Returns the grouping sets and list of rollup columns for queries that use
  // GROUP BY ROLLUP.  This clears columns previously added via AddGroupingSet.
  absl::Status ReleaseGroupingSetsAndRollupList(
      std::vector<std::unique_ptr<const ResolvedGroupingSetBase>>*
          grouping_set_list,
      std::vector<std::unique_ptr<const ResolvedColumnRef>>* rollup_column_list,
      const LanguageOptions& language_options);

  // Resets all state related to group by and aggregation context.  This
  // is invoked before processing DISTINCT, since DISTINCT processing
  // re-uses the same 'group_by_info_' member as the GROUP BY.
  void ClearGroupByInfo();

  // Adds <column> to <aggregate_columns_>. If <ast_function_call> is non-NULL
  // adds a map entry in <aggregate_expr_map_> from <ast_function_call> to
  // <column>.
  // If the range is determined (to be either all rows or to a pattern variable)
  // the computed column is added to the appropriate list.
  void AddAggregateComputedColumn(
      const ASTFunctionCall* ast_function_call,
      std::unique_ptr<const ResolvedComputedColumnBase> column);

  // Creates a new AnalyticFunctionResolver, but preserves the
  // NamedWindowInfoMap from the existing one.
  void ResetAnalyticResolver(Resolver* resolver);

  // Returns true if <column>'s ResolvedColumn contains an analytic function.
  // Returns an error if <column> is not a <select_column_state_list_>
  // resolved select column.
  absl::Status SelectListColumnHasAnalytic(const ResolvedColumn& column,
                                           bool* has_analytic) const;

  // Returns the computed columns that do not contain analytic functions,
  // and removes them from <select_list_columns_to_compute_>. The caller
  // must take ownership of the returned pointers.
  absl::Status GetAndRemoveSelectListColumnsWithoutAnalytic(
      std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
          select_columns_without_analytic_out);

  // Returns a list of output ResolvedColumns to use as the column_list
  // on final scans.  Currently only used when creating an
  // OrderByScan and subsequent ProjectScan, ensuring that all SELECT list
  // columns are produced by those scans.  For those callers, all
  // ResolvedColumns in the list are initialized.
  ResolvedColumnList GetResolvedColumnList() const;

  // Returns whether or not the query includes a GROUP BY clause or
  // aggregation functions.
  bool HasGroupByOrAggregation() const {
    return group_by_info_.has_group_by || group_by_info_.has_aggregation;
  }

  // Returns whether or not the query includes a GROUP BY ROLLUP, GROUP BY CUBE,
  // or GROUP BY GROUPING SETS.
  bool HasGroupByGroupingSets() const {
    return !group_by_info_.grouping_set_list.empty();
  }

  // Returns whether the query contains GROUPING function calls. grouping_list
  // and grouping_output_columns are populated at different stages, so we check
  // them both.
  bool HasGroupingCall() const {
    return !group_by_info_.grouping_call_list.empty() ||
           !group_by_info_.grouping_output_columns.empty();
  }

  // Returns whether or not the query includes analytic functions.
  bool HasAnalytic() const;

  // Returns whether or not the query includes aggregate functions.
  bool HasAggregation() const;

  // Sets the boolean indicating the query contains an aggregation function.
  void SetHasAggregation(bool value);

  absl::Status CheckComputedColumnListsAreEmpty() const;

  void set_is_post_distinct(bool is_post_distinct) {
    group_by_info_.is_post_distinct = is_post_distinct;
  }

  bool is_post_distinct() const { return group_by_info_.is_post_distinct; }

  void set_is_group_by_all(bool is_group_by_all) {
    group_by_info_.is_group_by_all = is_group_by_all;
  }

  bool is_group_by_all() const { return group_by_info_.is_group_by_all; }

  const std::vector<GroupByColumnState>& group_by_column_state_list() const {
    return group_by_info_.group_by_column_state_list;
  }

  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
  release_group_by_columns_to_compute() {
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> result;
    result.reserve(group_by_info_.group_by_column_state_list.size());
    for (int i = 0; i < group_by_info_.group_by_column_state_list.size(); ++i) {
      result.push_back(std::move(
          group_by_info_.group_by_column_state_list[i].computed_column));
    }
    group_by_info_.group_by_column_state_list.clear();
    return result;
  }

  std::vector<std::unique_ptr<const ResolvedGroupingCall>>
  release_grouping_call_list() {
    std::vector<std::unique_ptr<const ResolvedGroupingCall>> tmp;
    group_by_info_.grouping_call_list.swap(tmp);
    return tmp;
  }

  const std::vector<std::unique_ptr<const ResolvedGroupingCall>>&
  grouping_columns_list() const {
    return group_by_info_.grouping_call_list;
  }

  // Counts all aggregate columns to compute, including scoped aggregate
  // columns.
  size_t num_aggregate_columns_to_compute_across_all_scopes() const {
    size_t count = group_by_info_.aggregate_columns_to_compute.size();
    for (const auto& [name, aggs] :
         group_by_info_.match_recognize_aggregate_columns_to_compute) {
      count += aggs.size();
    }
    return count;
  }

  const std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>&
  unscoped_aggregate_columns_to_compute() const {
    return group_by_info_.aggregate_columns_to_compute;
  }

  // Transfer ownership of aggregate_columns_to_compute, clearing the
  // internal storage.
  std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
  release_unscoped_aggregate_columns_to_compute() {
    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>> tmp;
    group_by_info_.aggregate_columns_to_compute.swap(tmp);
    return tmp;
  }

  const std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>&
  aggregate_columns_to_compute() {
    // This function is only called from legacy paths, from before
    // MATCH_RECOGNIZE and didn't know about scoping. There shouldn't be any
    // aggregations pinned to a different range.
    ABSL_DCHECK(unscoped_aggregate_columns_to_compute().empty());
    if (scoped_aggregate_columns_to_compute().empty()) {
      // If there were no aggregations at all, make an empty list.
      // Calling "reserve(0)" to avoid a reassignment.
      group_by_info_.match_recognize_aggregate_columns_to_compute[IdString()]
          .reserve(0);
    }
    ABSL_DCHECK_EQ(scoped_aggregate_columns_to_compute().size(), 1);
    ABSL_DCHECK(scoped_aggregate_columns_to_compute().begin()->first.empty());
    return scoped_aggregate_columns_to_compute().at(IdString());
  }

  // Transfer ownership of aggregate_columns_to_compute, clearing the
  // internal storage.
  std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
  release_aggregate_columns_to_compute() {
    // This function is only called from legacy paths, from before
    // MATCH_RECOGNIZE and didn't know about scoping. There shouldn't be any
    // aggregations pinned to a different range.
    ABSL_DCHECK(unscoped_aggregate_columns_to_compute().empty());
    if (scoped_aggregate_columns_to_compute().empty()) {
      // If there were no aggregations at all, return an empty list.
      return {};
    }
    ABSL_DCHECK_EQ(scoped_aggregate_columns_to_compute().size(), 1);
    ABSL_DCHECK(scoped_aggregate_columns_to_compute().begin()->first.empty());
    return std::move(
        release_scoped_aggregate_columns_to_compute().at(IdString()));
  }

  // Pins this QueryResolutionInfo to the row range as specified by
  // `pattern_variable`:
  // - If `pattern_variable` is nullopt, pins to all rows.
  // - If `pattern_variable` is non-empty, pins to the row range of the
  //   pattern variable.
  //
  // Ideally, this should require `row_range_determined` on the scoping state
  // to be false, but ResolveSelectDistinct() reuses the QueryResolutionInfo
  // and there are paths where PinToRowRange() was already called before and
  // cases where it wasn't.
  absl::Status PinToRowRange(std::optional<IdString> pattern_variable);

  const IdStringHashMapCase<
      std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>>&
  scoped_aggregate_columns_to_compute() const {
    return group_by_info_.match_recognize_aggregate_columns_to_compute;
  }

  IdStringHashMapCase<
      std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>>
  release_scoped_aggregate_columns_to_compute() {
    IdStringHashMapCase<
        std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>>
        tmp;
    group_by_info_.match_recognize_aggregate_columns_to_compute.swap(tmp);
    return tmp;
  }

  const ValidFieldInfoMap& group_by_valid_field_info_map() const {
    return group_by_info_.group_by_valid_field_info_map;
  }

  ValidFieldInfoMap* mutable_group_by_valid_field_info_map() {
    return &group_by_info_.group_by_valid_field_info_map;
  }

  const ValidFieldInfoMap& select_list_valid_field_info_map() const {
    return select_list_valid_field_info_map_;
  }

  ValidFieldInfoMap* mutable_select_list_valid_field_info_map() {
    return &select_list_valid_field_info_map_;
  }

  const std::map<const ASTFunctionCall*, const ResolvedComputedColumnBase*>&
  aggregate_expr_map() {
    return group_by_info_.aggregate_expr_map;
  }

  const std::vector<OrderByItemInfo>& order_by_item_info() {
    return order_by_item_info_;
  }
  std::vector<OrderByItemInfo>* mutable_order_by_item_info() {
    return &order_by_item_info_;
  }

  bool group_by_has_and_order_by() const { return group_by_has_and_order_by_; }
  void set_group_by_has_and_order_by() { group_by_has_and_order_by_ = true; }

  const std::vector<OrderByItemInfo>& aggregate_order_by_item_info() const {
    return aggregate_order_by_item_info_;
  }
  std::vector<OrderByItemInfo>* mutable_aggregate_order_by_item_info() {
    return &aggregate_order_by_item_info_;
  }

  const std::vector<OrderByItemInfo>& group_by_order_by_item_info() const {
    return group_by_order_by_item_info_;
  }
  std::vector<OrderByItemInfo>* mutable_group_by_order_by_item_info() {
    return &group_by_order_by_item_info_;
  }

  AnalyticFunctionResolver* analytic_resolver() {
    return analytic_resolver_.get();
  }

  SelectColumnStateList* select_column_state_list() {
    return select_column_state_list_.get();
  }

  // This is the list of extra items to prepend on the output column list and
  // NameList, used when resolving pipe operators.  For pipe AGGREGATE, it's the
  // GROUP BY columns.  For pipe WINDOW, it's the passed-through input columns.
  std::vector<PipeExtraSelectItem>* pipe_extra_select_items() {
    return &pipe_extra_select_items_;
  }

  std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
  select_list_columns_to_compute() {
    return &select_list_columns_to_compute_;
  }

  // Transfer ownership of select_list_columns_to_compute, clearing the
  // internal storage.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
  release_select_list_columns_to_compute() {
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> tmp;
    select_list_columns_to_compute_.swap(tmp);
    return tmp;
  }

  std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
  select_list_columns_to_compute_before_aggregation() {
    return &select_list_columns_to_compute_before_aggregation_;
  }

  // Transfer ownership of select_list_columns_to_compute_before_aggregation,
  // clearing the internal storage.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
  release_select_list_columns_to_compute_before_aggregation() {
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> tmp;
    select_list_columns_to_compute_before_aggregation_.swap(tmp);
    return tmp;
  }

  std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
  columns_to_compute_after_aggregation() {
    return &columns_to_compute_after_aggregation_;
  }

  // Transfer ownership of columns_to_compute_after_aggregation, clearing the
  // internal storage.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
  release_columns_to_compute_after_aggregation() {
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> tmp;
    columns_to_compute_after_aggregation_.swap(tmp);
    return tmp;
  }

  std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
  columns_to_compute_after_analytic() {
    return &columns_to_compute_after_analytic_;
  }

  // Transfer ownership of columns_to_compute_after_analytic, clearing the
  // internal storage.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
  release_columns_to_compute_after_analytic() {
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> tmp;
    columns_to_compute_after_analytic_.swap(tmp);
    return tmp;
  }

  std::vector<std::pair<const ResolvedColumn, const ASTExpression*>>*
  dot_star_columns_with_aggregation_for_second_pass_resolution() {
    return &dot_star_columns_with_aggregation_for_second_pass_resolution_;
  }

  std::vector<std::pair<const ResolvedColumn, const ASTExpression*>>*
  dot_star_columns_with_analytic_for_second_pass_resolution() {
    return &dot_star_columns_with_analytic_for_second_pass_resolution_;
  }

  std::vector<std::unique_ptr<const ResolvedComputedColumn>>*
  order_by_columns_to_compute() {
    return &order_by_columns_to_compute_;
  }

  // Transfer ownership of order_by_columns_to_compute, clearing the
  // internal storage.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
  release_order_by_columns_to_compute() {
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> tmp;
    order_by_columns_to_compute_.swap(tmp);
    return tmp;
  }

  MatchRecognizeAggregationState* scoped_aggregation_state() {
    return scoped_aggregation_state_.get();
  }

  bool is_nested_aggregation() const { return is_nested_aggregation_; }

  std::string DebugString() const;

  void set_has_group_by(bool has_group_by) {
    group_by_info_.has_group_by = has_group_by;
  }

  void set_select_form(SelectForm form) { select_form_ = form; }
  SelectForm select_form() const { return select_form_; }

  const char* SelectFormClauseName() const;

  bool IsPipeSelect() const { return select_form() == SelectForm::kPipeSelect; }
  bool IsPipeExtend() const { return select_form() == SelectForm::kPipeExtend; }
  bool IsPipeWindow() const { return select_form() == SelectForm::kPipeWindow; }
  bool IsPipeExtendOrWindow() const { return IsPipeExtend() || IsPipeWindow(); }
  bool IsPipeAggregate() const {
    return select_form() == SelectForm::kPipeAggregate;
  }
  bool IsPipeOp() const;  // True for any pipe operator.

  bool IsGqlReturn() const { return select_form() == SelectForm::kGqlReturn; }
  bool IsGqlWith() const { return select_form() == SelectForm::kGqlWith; }
  // Tests for conditions that depend on SelectForm.
  bool SelectFormAllowsSelectStar() const;
  bool SelectFormAllowsAggregation() const;
  bool SelectFormAllowsAnalytic() const;

  void set_select_with_mode(SelectWithMode select_with_mode) {
    select_with_mode_ = select_with_mode;
  }
  SelectWithMode select_with_mode() const { return select_with_mode_; }

  void set_has_having(bool has_having) { has_having_ = has_having; }

  void set_has_qualify(bool has_qualify) { has_qualify_ = has_qualify; }

  void set_has_order_by(bool has_order_by) { has_order_by_ = has_order_by; }

  bool HasHavingOrQualifyOrOrderBy() const {
    return has_having_ || has_qualify_ || has_order_by_;
  }

  void set_is_resolving_returning_clause() {
    is_resolving_returning_clause_ = true;
  }
  bool is_resolving_returning_clause() const {
    return is_resolving_returning_clause_;
  }

  std::shared_ptr<const NameList> from_clause_name_list() const {
    return from_clause_name_list_;
  }
  void set_from_clause_name_list(std::shared_ptr<const NameList> name_list) {
    from_clause_name_list_ = name_list;
  }

 private:
  // SELECT list information.

  // SELECT list column information.  This is used for resolving alias and
  // ordinal references to a SELECT-column, and providing the final scan
  // for this (sub)query.
  // Always non-NULL.
  std::unique_ptr<SelectColumnStateList> select_column_state_list_;

  // SELECT list computed columns, except for dot-star columns.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      select_list_columns_to_compute_;

  // Stores information about STRUCT or PROTO fields that appear in the
  // SELECT list.  Only populated if grouping or aggregation is present.
  // Used to help map pre-GROUP BY path expressions to post-GROUP BY
  // ResolvedColumns during aggregate processing.
  //
  // Each entry indicates:
  //   - a (pre-GROUP BY) source ResolvedColumn key where field access begins
  //   - a ValidNamePathList value, where each ValidNamePath indicates:
  //     - a name path (i.e., 'a.b.c')
  //     - a target (pre-GROUP BY) ResolvedColumn that the name path resolves
  //       to from the source column.
  ValidFieldInfoMap select_list_valid_field_info_map_;

  // Columns that must be computed before the AggregateScan. Includes columns
  // resulting from dot-star expansion (if the dot-star source expression does
  // not contain aggregation).  It is also populated with other SELECT list
  // columns if the query has GROUP BY or aggregation, and either HAVING or
  // ORDER BY is present in the query.  This list only contains SELECT columns
  // that do not themselves include aggregation.
  // Note that, the computed columns might or might not be used by other parts
  // of the query like GROUP BY, HAVING, ORDER BY, etc.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      select_list_columns_to_compute_before_aggregation_;

  // Columns that must be computed after the AggregateScan, but before the final
  // project for this SELECT.  Currently used for computing the dot-star source
  // expression after aggregation, but before the dot-star expansion.
  //
  // Example SQL query and expression where this applies:
  //
  //   SELECT ARRAY_AGG(t)[OFFSET(0)].*
  //   FROM Table t;
  //
  // In this query, after the ARRAY_AGG is computed then the array element
  // access must be computed before the dot-star can be expanded.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      columns_to_compute_after_aggregation_;

  // Columns that must be computed after the AnalyticScan, but before the final
  // project for this SELECT.  Currently used for computing the dot-star source
  // expression after analytic functions, but before the dot-star expansion.
  //
  // Example SQL query and expression where this applies:
  //
  //   SELECT ANY_VALUE([t]) OVER () [OFFSET(0)].*
  //   FROM Table t;
  //
  // In this query, after the ANY_VALUE analytic function is computed then the
  // array element access must be computed before the dot-star can be expanded.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      columns_to_compute_after_analytic_;

  // Unowned pointers for columns that must have their expressions
  // (re)resolved in a second pass after grouping/aggregation analysis
  // is performed.  Once (re)resolved, the ResolvedExpr is added to
  // <columns_to_compute_after_aggregation_>.
  std::vector<std::pair<const ResolvedColumn, const ASTExpression*>>
      dot_star_columns_with_aggregation_for_second_pass_resolution_;

  // Unowned pointers for columns that must have their expressions
  // (re)resolved in a second pass after grouping/aggregation analysis
  // is performed.  Once (re)resolved, the ResolvedExpr is added to
  // <columns_to_compute_after_analytic_>.
  std::vector<std::pair<const ResolvedColumn, const ASTExpression*>>
      dot_star_columns_with_analytic_for_second_pass_resolution_;

  // GROUP BY and aggregation information.  Also (re)used for
  // SELECT DISTINCT.

  QueryGroupByAndAggregateInfo group_by_info_;

  std::vector<PipeExtraSelectItem> pipe_extra_select_items_;

  // Indicates if we're resolving a special form of ASTSelect, like
  // a no-FROM-clause query.
  SelectForm select_form_ = SelectForm::kClassic;

  // Select mode defined by SELECT WITH <identifier> clause.
  SelectWithMode select_with_mode_ = SelectWithMode::NONE;

  // HAVING information.
  bool has_having_ = false;

  // QUALIFY information.
  bool has_qualify_ = false;

  // ORDER BY information.
  bool has_order_by_ = false;

  // List of items from ORDER BY.
  // This has items from the standard ORDER BY clause or pipe ORDER BY.
  // Orderings from other pipe clauses are in the vectors below.
  // This vector and the vectors below cannot both be non-empty.
  std::vector<OrderByItemInfo> order_by_item_info_;

  // Indicates whether the AND ORDER BY modifier is present on GROUP BY.
  bool group_by_has_and_order_by_ = false;

  // Ordering items added from the AGGREGATE list (in pipe AGGREGATE).
  std::vector<OrderByItemInfo> aggregate_order_by_item_info_;

  // Ordering items added from the GROUP BY (in pipe AGGREGATE).
  std::vector<OrderByItemInfo> group_by_order_by_item_info_;

  // DML THEN RETURN information, where it also uses the select list.
  bool is_resolving_returning_clause_ = false;

  // Columns that need to be computed for ORDER BY (before OrderByScan).
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      order_by_columns_to_compute_;

  // Analytic function information.

  // The analytic function resolver to use for this query. It stores info
  // for analytic functions while resolving expressions.
  // Always non-NULL.
  std::unique_ptr<AnalyticFunctionResolver> analytic_resolver_;

  // The output NameList of the FROM clause of this query.  Currently used for
  // WITH GROUP ROWS aggregate processing, as the GROUP_ROWS() TVF within the
  // GROUP ROWS subquery produces this NameList as its result.
  std::shared_ptr<const NameList> from_clause_name_list_ = nullptr;

  // Tracks the current scope of the input range variables while resolving an
  // aggregate expression. A single expression may create multiple child
  // `ExprResolutionInfo` which are unable to share this global
  // information, so we place it here instead.
  // This is a shared_ptr because in multi-level aggregates, the child QRIs
  // share the same ScopedAggregationState with the parent because they still
  // must have the same input range, across all modifiers and nested aggs.
  std::shared_ptr<MatchRecognizeAggregationState> scoped_aggregation_state_ =
      std::make_shared<MatchRecognizeAggregationState>();

  // Indicates that this QRI is used to resolve nested aggregations and
  // modifiers in a multi-level aggregation.
  bool is_nested_aggregation_ = false;
};

// A class for lazily identifying untyped literal expressions produced by
// <scan>.
class UntypedLiteralMap {
 public:
  // Does not take ownership of <scan>, which must outlive this class.
  // <scan> can be null.
  explicit UntypedLiteralMap(const ResolvedScan* scan) : scan_(scan) {}
  // Returns the untyped literal expression in <scan> that produces <column>,
  // where all the following are true:
  // 1) <scan> is not null;
  // 2) <scan> is a ResolvedProjectScan;
  // 3) <column> is produced by <scan>;
  // 4) <column> is produced by an untyped literal.
  // Returns null otherwise.
  //
  // The first call to Find() scans the expressions in <scan> and caches the
  // the untyped literals, so future calls to Find() do not need to go through
  // the expressions in <scan> again.
  const ResolvedExpr* Find(const ResolvedColumn& column);

 private:
  const ResolvedScan* scan_;
  std::unique_ptr<absl::flat_hash_map<int, const ResolvedExpr*>>
      column_id_to_untyped_literal_map_;
};

// Contains information about an unresolved `ASTSelectColumn`. This information
// is used to determine whether the `ASTSelectColumn` should have it's
// first pass resolution deferred until after the GROUP BY clause is resolved.
// See `ResolveDeferredFirstPassSelectListExprs` for details.
struct DeferredResolutionSelectColumnInfo {
  // If true, the `ASTSelectColumn` contains a function with a GROUP ROWS
  // expression or GROUP BY modifiers, and the function does not lie within a
  // subquery.
  bool has_outer_group_rows_or_group_by_modifiers = false;
  // If true, the `ASTSelectColumn` contains an analytic function (i.e. a
  // function with an OVER clause), and the function does not lie within a
  // subquery.
  bool has_outer_analytic_function = false;
};

absl::StatusOr<DeferredResolutionSelectColumnInfo>
GetDeferredResolutionSelectColumnInfo(const ASTSelectColumn* select_column);

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_QUERY_RESOLVER_HELPER_H_
