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

#ifndef ZETASQL_ANALYZER_QUERY_RESOLVER_HELPER_H_
#define ZETASQL_ANALYZER_QUERY_RESOLVER_HELPER_H_

#include <stddef.h>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zetasql/analyzer/name_scope.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include <cstdint>
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "zetasql/base/status.h"

namespace zetasql {

class AnalyticFunctionResolver;
class Resolver;
class SelectColumnStateList;

// Hashing function for field paths, which enables faster set lookups and
// insertions.
size_t FieldPathHash(const ResolvedExpr* expr);

enum class FieldPathMatchingOption { kExpression, kFieldPath };

// This function determines whether <field_path1> and <field_path2> are
// generalized path expressions that point to the same field.
//
// If the FieldPathMatchingOption::kExpression option is specified, this
// function returns true if <field_path1> and <field_path2> are interchangeable
// generalized path expressions. This considers specialized field accesses
// (currently, the only such case is PROTO_DEFAULT_IF_NULL) as well as ensures
// the descriptors for any proto types involved come from the same descriptor
// pool. This option guarantees <field_path1> and <field_path2> evaluate to the
// same result.
//
// If the FieldPathMatchingOption::kFieldPath option is specified, this function
// returns true if <field_path1> and <field_path2> read the same field. This
// option does consider whether the has_bit of the field is being accessed by
// both <field_path1> and <field_path2>. However, it does not consider any
// specialized field accesses. Therefore, this option does not guarantee
// <field_path1> and <field_path2> evaluate to the same result.
bool IsSameFieldPath(const ResolvedExpr* field_path1,
                     const ResolvedExpr* field_path2,
                     FieldPathMatchingOption match_option);

// Field path hashing operator for containers.
struct FieldPathHashOperator {
  size_t operator()(const ResolvedExpr* expr) const {
    return FieldPathHash(expr);
  }
};

// Field path equality operator for containers.
struct FieldPathEqualsOperator {
  bool operator()(const ResolvedExpr* expr1, const ResolvedExpr* expr2) const {
    return IsSameFieldPath(expr1, expr2, FieldPathMatchingOption::kFieldPath);
  }
};

// Field path expression equality operator for containers.
struct FieldPathExpressionEqualsOperator {
  bool operator()(const ResolvedExpr* expr1, const ResolvedExpr* expr2) const {
    return IsSameFieldPath(expr1, expr2, FieldPathMatchingOption::kExpression);
  }
};

struct OrderByItemInfo {
  OrderByItemInfo(const ASTNode* ast_location_in, int64_t index, bool descending,
                  ResolvedOrderByItemEnums::NullOrderMode null_order)
      : ast_location(ast_location_in),
        select_list_index(index),
        is_descending(descending),
        null_order(null_order) {}
  OrderByItemInfo(const ASTNode* ast_location_in,
                  std::unique_ptr<const ResolvedExpr> expr, bool descending,
                  ResolvedOrderByItemEnums::NullOrderMode null_order)
      : ast_location(ast_location_in),
        order_expression(std::move(expr)),
        is_descending(descending),
        null_order(null_order) {}

  // This value is not valid as a 0-based select list index.
  static constexpr int64_t kInvalidSelectListIndex =
      std::numeric_limits<int64_t>::max();

  const ASTNode* ast_location;

  bool is_select_list_index() const {
    return select_list_index != kInvalidSelectListIndex;
  }

  // 0-based index into the SELECT list.  A kInvalidSelectListIndex value
  // indicates this ORDER BY expression is not a select list column
  // reference, in which case <order_expression> and <order_column> will
  // be populated.
  int64_t select_list_index = kInvalidSelectListIndex;

  // Only populated if <select_list_index> == -1;
  std::unique_ptr<const ResolvedExpr> order_expression;
  ResolvedColumn order_column;

  bool is_descending;  // Indicates DESC or ASC.
  // Indicates NULLS LAST or NULLS FIRST.
  ResolvedOrderByItemEnums::NullOrderMode null_order;
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
  std::map<const ASTFunctionCall*, const ResolvedComputedColumn*>
      aggregate_expr_map;

  // Group by expressions that must be computed.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      group_by_columns_to_compute;

  // Map of group by expressions to entries within group_by_columns_to_compute.
  std::unordered_map<const ResolvedExpr*, const ResolvedComputedColumn*,
                     FieldPathHashOperator, FieldPathExpressionEqualsOperator>
      group_by_expr_map;

  // Columns in the ROLLUP list, or an empty vector if the query does
  // not use ROLLUP. Stores unowned pointers from <group_by_columns_to_compute>.
  std::vector<const ResolvedComputedColumn*> rollup_column_list;

  // Aggregate function calls that must be computed.
  // This is built up as expressions are resolved.  During expression
  // resolution, aggregate functions are moved into <aggregate_columns_> and
  // replaced by a ResolvedColumnRef pointing at the ResolvedColumn created
  // here.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      aggregate_columns_to_compute;

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

  // Resets all fields to their initial values, and empties all maps and
  // lists.
  void Reset();
};

// QueryResolutionInfo is used (and mutated) to store info related to
// the analysis of a single SELECT query block.  It stores information
// related to SELECT list entries, grouping and aggregation, and analytic
// functions.  Detailed descriptions for each field are included below.
// See comments on ResolveSelect() in resolver.cc for discussion of
// the various phases of analysis and how QueryResolutionInfo is updated
// and referenced during that process.
class QueryResolutionInfo {
 public:
  // Constructor. Does not take ownership of <resolver>.
  explicit QueryResolutionInfo(Resolver* resolver);
  QueryResolutionInfo(const QueryResolutionInfo&) = delete;
  QueryResolutionInfo& operator=(const QueryResolutionInfo&) = delete;
  ~QueryResolutionInfo();

  // Adds group by column <column>, which is computed from <expr>. If a field
  // path expression that is equivalent to <expr> is already present in the
  // list of group by computed columns, the column is not added to the list.
  // Returns an unowned pointer to the ResolvedComputedColumn for this
  // expression.
  const ResolvedComputedColumn* AddGroupByComputedColumnIfNeeded(
      const ResolvedColumn& column, std::unique_ptr<const ResolvedExpr> expr);

  // Returns a pointer to an existing equivalent ResolvedComputedColumn in
  // group_by_columns_to_compute(), as determined by IsSameFieldPath(), or
  // nullptr if there is no equivalent expression.
  const ResolvedComputedColumn* GetEquivalentGroupByComputedColumnOrNull(
      const ResolvedExpr* expr) const;

  // Adds a rollup column <column> that is present in the group by list.
  // Does not transfer ownership.
  void AddRollupColumn(const ResolvedComputedColumn* column);

  // Returns the grouping sets and list of rollup columns for queries that use
  // GROUP BY ROLLUP.  This clears columns previously added via AddRollupColumn.
  void ReleaseGroupingSetsAndRollupList(
      std::vector<std::unique_ptr<const ResolvedGroupingSet>>*
          grouping_set_list,
      std::vector<std::unique_ptr<const ResolvedColumnRef>>*
          rollup_column_list);

  // Resets all state related to group by and aggregation context.  This
  // is invoked before processing DISTINCT, since DISTINCT processing
  // re-uses the same 'group_by_info_' member as the GROUP BY.
  void ClearGroupByInfo();

  // Adds <column> to <aggregate_columns_>. If <ast_function_call> is non-NULL
  // adds a map entry in <aggregate_expr_map_> from <ast_function_call> to
  // <column>.
  void AddAggregateComputedColumn(
      const ASTFunctionCall* ast_function_call,
      std::unique_ptr<const ResolvedComputedColumn> column);

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

  // Returns whether or not the query includes a GROUP BY clause or
  // aggregation functions.
  bool HasGroupByOrAggregation() const {
    return group_by_info_.has_group_by ||
           group_by_info_.has_aggregation;
  }

  // Returns whether or not the query includes analytic functions.
  bool HasAnalytic() const;

  absl::Status CheckComputedColumnListsAreEmpty() const;

  void set_is_post_distinct(bool is_post_distinct) {
    group_by_info_.is_post_distinct = is_post_distinct;
  }

  bool is_post_distinct() const {
    return group_by_info_.is_post_distinct;
  }

  const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
  group_by_columns_to_compute() const {
    return group_by_info_.group_by_columns_to_compute;
  }

  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
  release_group_by_columns_to_compute() {
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> tmp;
    group_by_info_.group_by_columns_to_compute.swap(tmp);
    return tmp;
  }

  const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
  aggregate_columns_to_compute() const {
    return group_by_info_.aggregate_columns_to_compute;
  }

  // Transfer ownership of aggregate_columns_to_compute, clearing the
  // internal storage.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
  release_aggregate_columns_to_compute() {
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> tmp;
    group_by_info_.aggregate_columns_to_compute.swap(tmp);
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

  const std::map<const ASTFunctionCall*,
      const ResolvedComputedColumn*>& aggregate_expr_map() {
    return group_by_info_.aggregate_expr_map;
  }

  const std::vector<OrderByItemInfo>& order_by_item_info() {
    return order_by_item_info_;
  }

  std::vector<OrderByItemInfo>* mutable_order_by_item_info() {
    return &order_by_item_info_;
  }

  AnalyticFunctionResolver* analytic_resolver() {
    return analytic_resolver_.get();
  }

  SelectColumnStateList* select_column_state_list() {
    return select_column_state_list_.get();
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

  std::string DebugString() const;

  void set_has_group_by(bool has_group_by) {
    group_by_info_.has_group_by = has_group_by;
  }
  bool has_group_by() const { return group_by_info_.has_group_by; }

  void set_has_having(bool has_having) { has_having_ = has_having; }
  bool has_having() const { return has_having_; }

  void set_has_order_by(bool has_order_by) { has_order_by_ = has_order_by; }
  bool has_order_by() const { return has_order_by_; }

  bool HasHavingOrOrderBy() const { return has_having_ || has_order_by_; }

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

  // HAVING information.

  bool has_having_ = false;

  // ORDER BY information.

  bool has_order_by_ = false;

  // List of ORDER BY information.
  std::vector<OrderByItemInfo> order_by_item_info_;

  // Columns that need to be computed for ORDER BY (before OrderByScan).
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      order_by_columns_to_compute_;

  // Analytic function information.

  // The analytic function resolver to use for this query. It stores info
  // for analytic functions while resolving expressions.
  // Always non-NULL.
  std::unique_ptr<AnalyticFunctionResolver> analytic_resolver_;
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

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_QUERY_RESOLVER_HELPER_H_
