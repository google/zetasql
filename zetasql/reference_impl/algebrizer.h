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

// ZetaSQL Algebrizer
// Defines the transformation from a resolved abstract syntax tree (AST) into a
// relational algebra tree.
//
// See (broken link) for more information.

#ifndef ZETASQL_REFERENCE_IMPL_ALGEBRIZER_H_
#define ZETASQL_REFERENCE_IMPL_ALGEBRIZER_H_

#include <functional>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/public/language_options.h"
#include "zetasql/public/type.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/parameters.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/reference_impl/variable_generator.h"
#include "zetasql/reference_impl/variable_id.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "gtest/gtest_prod.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

struct AlgebrizerOptions {
  // If true, tables are represented as arrays, and the values are obtained
  // during evaluation through EvaluationContext. If false, tables are
  // represented using EvaluatorTableIterators.
  bool use_arrays_for_tables = false;

  // If true, the algebrizer arranges so that, e.g, proto_col.a and proto_col.b
  // reads both 'a' and 'b' from 'proto_col' in one shot, instead of
  // potentially deserializing it twice.
  bool consolidate_proto_field_accesses = false;

  // If true, the algebrizer attempts to use a hash join instead of a nested
  // loop join when the join condition is amenable, or when there is a
  // compatible filter immediately above the join.
  bool allow_hash_join = false;

  // If true, the algebrizer attempts to use a single operator for ORDER BY
  // LIMIT instead of LimitOp(SortOp), which saves memory.
  bool allow_order_by_limit_operator = false;

  // If true, the algebrizer attempts to push down filters into the highest
  // ancestor node that is either a join or an EvaluatorTableScanOp node. In the
  // latter case, the filter remains in its original location because
  // EvaluatorTableIterator does not have to honor the filter.
  bool push_down_filters = false;
};

class Algebrizer {
 public:
  Algebrizer(const Algebrizer&) = delete;
  Algebrizer& operator=(const Algebrizer&) = delete;

  // Returns the statement kinds that are supported by AlgebrizeStatement().
  static std::set<ResolvedNodeKind> GetSupportedStatementKinds() {
    return {RESOLVED_QUERY_STMT, RESOLVED_DELETE_STMT, RESOLVED_UPDATE_STMT,
            RESOLVED_INSERT_STMT};
  }

  // Algebrize the resolved AST for a SQL statement. 'parameters' returns either
  // a map of lower-cased parameter names appearing in the statement to variable
  // IDs, if the statement uses named parameters, or else a list of variable IDs
  // for positional parameters. 'column_map' returns a map of lower-cased column
  // parameter names to variable IDs.
  //
  // For query statements, 'output' is only valid for as long as 'type_factory'
  // is valid.
  //
  // For DML statements, 'output' is only valid for as long as 'type_factory'
  // and 'ast_root' are valid. Also, 'output' is always a DMLValueExpr.
  //
  // On output, <system_variables_map> is populated with a
  // name path=>variable id map for each system variable used in the
  // statement/expression.
  static zetasql_base::Status AlgebrizeStatement(
      const LanguageOptions& language_options,
      const AlgebrizerOptions& algebrizer_options, TypeFactory* type_factory,
      const ResolvedStatement* ast_root, std::unique_ptr<ValueExpr>* output,
      Parameters* parameters, ParameterMap* column_map,
      SystemVariablesAlgebrizerMap* system_variables_map);

  // Same as above, but only supports query statements and returns a
  // RelationalOp, which can be used to construct an iterator over the result
  // set (to avoid storing the entire result in memory). Populates
  // 'output_column_list' with the output columns of the query,
  // 'output_column_names' with the user-visible names, and
  // 'output_column_variables' with the corresponding VariableIds for the
  // TupleIterator returned by 'output'.
  static zetasql_base::Status AlgebrizeQueryStatementAsRelation(
      const LanguageOptions& language_options,
      const AlgebrizerOptions& algebrizer_options, TypeFactory* type_factory,
      const ResolvedQueryStmt* ast_root, ResolvedColumnList* output_column_list,
      std::unique_ptr<RelationalOp>* output,
      std::vector<std::string>* output_column_names,
      std::vector<VariableId>* output_column_variables, Parameters* parameters,
      ParameterMap* column_map,
      SystemVariablesAlgebrizerMap* system_variables_map);

  // Similar to AlgebrizeStatement(), but accepts any ResolvedExpr. 'output' is
  // only valid for as long as 'type_factory' is valid.
  static zetasql_base::Status AlgebrizeExpression(
      const LanguageOptions& language_options,
      const AlgebrizerOptions& algebrizer_options, TypeFactory* type_factory,
      const ResolvedExpr* ast_root, std::unique_ptr<ValueExpr>* output,
      Parameters* parameters, ParameterMap* column_map,
      SystemVariablesAlgebrizerMap* system_variables_map);

 private:
  friend class AlgebrizerTestBase;
  friend class AlgebrizerTestSelectColumn;
  friend class AlgebrizerTestFunctions;
  friend class AlgebrizerTestFilters;
  friend class AlgebrizerTestGroupingAggregation;
  FRIEND_TEST(ExpressionAlgebrizerTest, Parameters);
  FRIEND_TEST(ExpressionAlgebrizerTest, PositionalParametersInExpressions);
  FRIEND_TEST(StatementAlgebrizerTest, SingleRowScan);
  FRIEND_TEST(StatementAlgebrizerTest, SingleRowSelect);
  FRIEND_TEST(StatementAlgebrizerTest, TableScanAsArrayType);
  FRIEND_TEST(AlgebrizerTestBase, TableScanAsIterator);
  FRIEND_TEST(StatementAlgebrizerTest, TableSelectAll);
  FRIEND_TEST(StatementAlgebrizerTestSelectColumn, SelectColumn);
  FRIEND_TEST(AlgebrizerTestFunctions, Functions);
  FRIEND_TEST(AlgebrizerTestFunctions, SelectFunctions);
  FRIEND_TEST(AlgebrizerTestFilters, Filters);
  FRIEND_TEST(StatementAlgebrizerTest, CrossApply);
  FRIEND_TEST(AlgebrizerTestJoins, InnerJoin);
  FRIEND_TEST(AlgebrizerTestJoins, CorrelatedInnerJoin);
  FRIEND_TEST(AlgebrizerTestGroupingAggregation, GroupByAny);
  FRIEND_TEST(AlgebrizerTestGroupingAggregation, GroupByAvg);
  FRIEND_TEST(AlgebrizerTestGroupingAggregation, GroupByCountStar);
  FRIEND_TEST(AlgebrizerTestGroupingAggregation, GroupByCountColumn);
  FRIEND_TEST(AlgebrizerTestGroupingAggregation, GroupByMax);
  FRIEND_TEST(AlgebrizerTestGroupingAggregation, GroupByMin);
  FRIEND_TEST(AlgebrizerTestGroupingAggregation, GroupBySum);

  Algebrizer(const LanguageOptions& options,
             const AlgebrizerOptions& algebrizer_options,
             TypeFactory* type_factory, Parameters* parameters,
             ParameterMap* column_map,
             SystemVariablesAlgebrizerMap* system_variables_map);

  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeCast(
      const ResolvedCast* cast);

  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeFunctionCall(
      const ResolvedFunctionCall* function_call);

  zetasql_base::StatusOr<std::unique_ptr<NewStructExpr>> MakeStruct(
      const ResolvedMakeStruct* make_struct);

  zetasql_base::StatusOr<std::unique_ptr<FieldValueExpr>> AlgebrizeGetStructField(
      const ResolvedGetStructField* get_struct_field);

  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeGetProtoField(
      const ResolvedGetProtoField* get_proto_field);

  // Helper for AlgebrizeGetProtoField() for the case where we are getting a
  // proto field of an expression of the form
  // <column_or_param_expr>.<path>. <column_or_param> must be a
  // ResolvedColumnRef, a ResolvedParameter, or a ResolvedExpressionColumn.
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeGetProtoFieldOfPath(
      const ResolvedExpr* column_or_param_expr,
      const std::vector<absl::variant<const ResolvedGetProtoField*,
                                      const ResolvedGetStructField*>>& path);

  // Algebrize specific expressions.
  zetasql_base::StatusOr<std::unique_ptr<AggregateArg>> AlgebrizeAggregateFn(
      const VariableId& variable, const ResolvedExpr* expr);
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeSubqueryExpr(
      const ResolvedSubqueryExpr* subquery_expr);
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeInArray(
      std::unique_ptr<ValueExpr> in_value,
      std::unique_ptr<ValueExpr> array_value);
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeInRelation(
      std::unique_ptr<ValueExpr> in_value, const VariableId& haystack_var,
      std::unique_ptr<RelationalOp> haystack_rel);

  // Wrapper around AlgebrizeExpression() for use on standalone expressions.
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeStandaloneExpression(
      const ResolvedExpr* expr);

  // Algebrize a resolved expression. For aggregate function expressions, call
  // AlgebrizeAggregateFn.
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeExpression(
      const ResolvedExpr* expr);

  // Wraps 'value_expr' in a RootExpr to manage ownership of some objects
  // required by the algebrized tree.
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> WrapWithRootExpr(
      std::unique_ptr<ValueExpr> value_expr);

  // Moves state from this algebrizer into a RootData and returns it.
  std::unique_ptr<RootData> GetRootData();

  // Wraps a conjunct from a filter with some associated information.
  struct FilterConjunctInfo {
    // Describes 'conjunct'.
    enum Kind {
      kLE,  // Includes both < and <=
      kGE,  // Include both > and >=
      kEquals,
      kBetween,
      kIn,
      kInArray,
      kOther
    };

    static zetasql_base::StatusOr<std::unique_ptr<FilterConjunctInfo>> Create(
        const ResolvedExpr* conjunct);

    Kind kind = kOther;

    // A filter conjunct.
    const ResolvedExpr* conjunct = nullptr;

    // True if 'conjunct' is known to be non-volatile (per
    // FunctionEnums::VOLATILE).
    bool is_non_volatile = false;

    // All the columns referenced by 'conjunct'.
    absl::flat_hash_set<ResolvedColumn> referenced_columns;

    // If 'kind' is a ResolvedFunctionCall, these are the arguments.
    std::vector<const ResolvedExpr*> arguments;

    // The columns referenced by each argument. Corresponds positionally to
    // 'arguments'.
    std::vector<absl::flat_hash_set<ResolvedColumn>> argument_columns;

    // True if 'conjunct' is guaranteed to be satisfied by some node in the
    // algebrized tree. For example, consider the query:
    //   select * from KeyValue, KeyValue2 where KeyValue.Key = KeyValue2.Key
    // The naive algebrization looks like:
    //   FilterOp(KeyValue.Key = KeyValue2.Key,
    //   + JoinOp(INNER,
    //     + left_input: EvaluatorTableScan(KeyValue),
    //     + right_input: EvaluatorTableScan(KeyValue2)))
    // A better tree (for evaluation) is:
    //   JoinOp(INNER,
    //   + hash_join_equality_left_exprs: KeyValue.Key
    //   + hash_join_equality_right_exprs: KeyValue2.Key
    //   + left_input: EvaluatorTableScan(KeyValue)
    //   + right_input: EvaluatorTableScan(KeyValue2))
    // (The second plan is better because it avoids materializing all
    // combinations of tuples KeyValue and KeyValue2.) To implement this
    // approach, the algebrizer looks at the conjunct KeyValue.Key =
    // KeyValue2.Key while processing the join, notices that it can be used for
    // the hash join, and then marks it is as redundant so that the
    // algebrization of the ResolvedFilterScan corresponding to the WHERE clause
    // will just be the input scan (which is the JoinOp).
    bool redundant = false;
  };

  // Adds all the conjuncts in 'expr' to 'conjunct_infos'.
  static zetasql_base::Status AddFilterConjunctsTo(
      const ResolvedExpr* expr,
      std::vector<std::unique_ptr<FilterConjunctInfo>>* conjunct_infos);

  // Algebrize specific relational inputs. We only support filter pushdown for
  // certain scan types. For those, there is an 'active_conjuncts' argument
  // (which represents a stack) that contains the applicable FilterConjunctInfos
  // that the operator can choose to incorporate or push down. None of those
  // FilterConjunctInfos may be marked as redundant when these methods are
  // called. For example, consider this query:
  //   SELECT key FROM (SELECT key, (key + 10) AS k2 FROM KeyValue)
  //              WHERE key > 10 AND k2 > 11
  // The resolved AST looks like:
  //   ResolvedFilterScan(no active FilterConjunctInfos,
  //   + ResolvedProjectScan(active FilterConjunctInfos = {key > 10, k2 > 11},
  //     + ResolvedTableScan(active FilterConjunctInfos = {key > 10})))
  // The algebrized tree will ultimately push down the filters as far as they
  // can go.
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeSingleRowScan();
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeJoinScan(
      const ResolvedJoinScan* join_scan,
      std::vector<FilterConjunctInfo*>* active_conjuncts);
  // Returns an algebrized right scan with the given active conjuncts.
  using RightScanAlgebrizerCb =
      std::function<zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>(
          std::vector<FilterConjunctInfo*>*)>;
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeJoinScanInternal(
      JoinOp::JoinKind join_kind,
      const ResolvedExpr* join_expr,  // May be NULL
      const ResolvedScan* left_scan,
      const std::vector<ResolvedColumn>& right_output_column_list,
      const RightScanAlgebrizerCb& right_scan_algebrizer_cb,
      std::vector<FilterConjunctInfo*>* active_conjuncts);
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeFilterScan(
      const ResolvedFilterScan* filter_scan,
      std::vector<FilterConjunctInfo*>* active_conjuncts);
  zetasql_base::StatusOr<std::unique_ptr<AggregateOp>> AlgebrizeAggregateScanBase(
      const ResolvedAggregateScanBase* aggregate_scan,
      bool allow_unused_columns);
  zetasql_base::StatusOr<std::unique_ptr<AggregateOp>> AlgebrizeAggregateScan(
      const ResolvedAggregateScan* aggregate_scan);
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeSetOperationScan(
      const ResolvedSetOperationScan* set_scan);
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeUnionScan(
      const ResolvedSetOperationScan* set_scan);
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeExceptIntersectScan(
      const ResolvedSetOperationScan* set_scan);
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeProjectScan(
      const ResolvedProjectScan* resolved_project,
      std::vector<FilterConjunctInfo*>* active_conjuncts);
  // 'limit' and 'offset' may both be NULL or both non-NULL.
  zetasql_base::StatusOr<std::unique_ptr<SortOp>> AlgebrizeOrderByScan(
      const ResolvedOrderByScan* scan, std::unique_ptr<ValueExpr> limit,
      std::unique_ptr<ValueExpr> offset);
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeArrayScan(
      const ResolvedArrayScan* array_scan,
      std::vector<FilterConjunctInfo*>* active_conjuncts);
  // Algebrizes 'array_scan', ignoring any input scan or join condition.
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeArrayScanWithoutJoin(
      const ResolvedArrayScan* array_scan,
      std::vector<FilterConjunctInfo*>* active_conjuncts);
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeLimitOffsetScan(
      const ResolvedLimitOffsetScan* scan);
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeWithScan(
      const ResolvedWithScan* scan);
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeWithRefScan(
      const ResolvedWithRefScan* scan);

  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeTableScan(
      const ResolvedTableScan* table_scan,
      std::vector<FilterConjunctInfo*>* active_conjuncts);

  // Maps a ResolvedColumn from a table scan to its corresponding Variable and
  // index in the scan (not the Table).
  using TableScanColumnInfoMap =
      absl::flat_hash_map<ResolvedColumn, std::pair<VariableId, int>>;

  // If 'conjunct_info' can be represented using ColumnFilterArgs with the
  // columns in 'column_info_map', appends them to 'and_filters'.
  zetasql_base::Status TryAlgebrizeFilterConjunctAsColumnFilterArgs(
      const TableScanColumnInfoMap& column_info_map,
      const FilterConjunctInfo& conjunct_info,
      std::vector<std::unique_ptr<ColumnFilterArg>>* and_filters);

  // Algebrizes the resolved AST for an AnalyticScan. The AnalyticScan is
  // converted to a sequence of AnalyticOp, one per analytic function group.
  // For each analytic function group, a SortOp is also created if it contains
  // partitioning and ordering expressions, even when the input relation has
  // been already sorted by those expressions.
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeAnalyticScan(
      const ResolvedAnalyticScan* analytic_scan);

  // Returns an AnalyticOp for 'analytic_group'. A SortOp is also created under
  // the AnalyticOp if the partitioning or ordering expressions are not
  // empty. 'input_resolved_columns' contains the input columns including the
  // analytic columns created by the preceding analytic function
  // groups. 'input_is_from_same_analytic_scan' must be true if 'analytic_group'
  // and 'input_relation_op' correspond to the same AnalyticScan resolved AST
  // node.
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeAnalyticFunctionGroup(
      const std::set<ResolvedColumn>& input_resolved_columns,
      const ResolvedAnalyticFunctionGroup* analytic_group,
      std::unique_ptr<RelationalOp> input_relation_op,
      bool input_is_from_same_analytic_scan);

  // Returns 'input_relation_op' if all the partitioning and ordering
  // expressions in 'analytic_group' are correlated column references.
  // Otherwise, creates and returns a SortOp on top of 'input_relation_op' with
  // non-correlated partitioning and ordering expressions as order keys.
  // 'input_resolved_columns' contains the input columns produced by
  // 'input_relation_op'. If 'require_stable_sort' is true, then any SortOp
  // created performs a stable sort over its input.
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
  MaybeCreateSortForAnalyticOperator(
      const std::set<ResolvedColumn>& input_resolved_columns,
      const ResolvedAnalyticFunctionGroup* analytic_group,
      std::unique_ptr<RelationalOp> input_relation_op,
      bool require_stable_sort);

  // Converts each ResolvedOrderByItem to a KeyArg.
  // If 'drop_correlated_columns' is true, the output 'order_by_keys' does not
  // include the KeyArg for a ResolvedOrderByItem if it refers to a correlated
  // column.
  // If 'create_new_ids' is true, we create a new VariableId for each KeyArg
  // and update the class member variable 'column_to_variable_' for the
  // referenced column; otherwise, the KeyArg uses the existing VariableId of
  // the referenced column.
  // 'column_to_id_map' maps for each order by column from its column id to its
  // initial VariableId before any change has been made in this function.
  zetasql_base::Status AlgebrizeOrderByItems(
      bool drop_correlated_columns, bool create_new_ids,
      const std::vector<std::unique_ptr<const ResolvedOrderByItem>>&
          order_by_items,
      absl::flat_hash_map<int, VariableId>* column_to_id_map,
      std::vector<std::unique_ptr<KeyArg>>* order_by_keys);

  // Converts each non-correlated ResolvedColumnRef in 'partition_by' to a
  // KeyArg with an ascending order.
  // 'column_to_id_map' collects the ids of the referenced columns and their
  // VariableIds.
  zetasql_base::Status AlgebrizePartitionExpressions(
      const ResolvedWindowPartitioning* partition_by,
      absl::flat_hash_map<int, VariableId>* column_to_id_map,
      std::vector<std::unique_ptr<KeyArg>>* partition_by_keys);

  // Converts a ResolvedAnalyticFunctionCall to an AnalyticArg.
  zetasql_base::StatusOr<std::unique_ptr<AnalyticArg>> AlgebrizeAnalyticFunctionCall(
      const VariableId& variable,
      const ResolvedAnalyticFunctionCall* analytic_function_call);

  // Converts a ResolvedWindowFrame to a WindowFrameArg.
  zetasql_base::StatusOr<std::unique_ptr<WindowFrameArg>> AlgebrizeWindowFrame(
      const ResolvedWindowFrame* window_frame);

  // Converts a ResolvedWindowFrameExpr to a WindowFrameBoundaryArg.
  zetasql_base::StatusOr<std::unique_ptr<WindowFrameBoundaryArg>>
  AlgebrizeWindowFrameExpr(const ResolvedWindowFrameExpr* window_frame_expr);

  // Algebrize the resolved AST for a scan.
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeScan(
      const ResolvedScan* scan,
      std::vector<FilterConjunctInfo*>* active_conjuncts);

  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> AlgebrizeScan(
      const ResolvedScan* scan);

  // 'output_columns' are needed to compensate for the extra implicit
  // 'ProjectScan' in top-level queries.
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeRootScanAsValueExpr(
      const ResolvedColumnList& output_columns, bool is_value_table,
      const ResolvedScan* scan);

  // Populates 'output_column_list', 'output_column_names', and
  // 'output_column_variables' according to the output of 'query' and returns a
  // RelationalOp corresponding to the scan (which may have extra variables).
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
  AlgebrizeQueryStatementAsRelation(
      const ResolvedQueryStmt* query, ResolvedColumnList* output_column_list,
      std::vector<std::string>* output_column_names,
      std::vector<VariableId>* output_column_variables);

  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeDMLStatement(
      const ResolvedStatement* ast_root);

  // Populates the ResolvedScanMap and the ResolvedExprMap corresponding to
  // 'ast_root', which must be a DML statement. If the DML statement is
  // top-level (non-nested), then 'resolved_table_scan' must not be NULL and
  // this method populates it with the ResolvedTableScan corresponding to the
  // table being modified. If the DML statement is nested, then
  // 'resolved_table_scan' must be NULL. Also adds any placeholder columns
  // (i.e., ResolvedColumns that are defined in the subtree rooted at
  // 'ast_root') to 'column_to_variable_'.
  zetasql_base::Status AlgebrizeDescendantsOfDMLStatement(
      const ResolvedStatement* ast_root, ResolvedScanMap* resolved_scan_map,
      ResolvedExprMap* resolved_expr_map,
      const ResolvedTableScan** resolved_table_scan);

  // Populates the ResolvedScanMap and the ResolvedExprMap corresponding to
  // 'update_item', which must be a DML statement. Also adds any placeholder
  // columns (i.e., ResolvedColumns that are defined in the subtree rooted at
  // 'ast_root') to 'column_to_variable_'.
  zetasql_base::Status AlgebrizeDescendantsOfUpdateItem(
      const ResolvedUpdateItem* update_item, ResolvedScanMap* resolved_scan_map,
      ResolvedExprMap* resolved_expr_map);

  // Adds the entry corresponding to 'resolved_scan' to 'resolved_scan_map'
  // (whose key is 'resolved_scan' and whose value is the algebrized scan). Note
  // that the map does not own the ResolvedScan nodes.
  ::zetasql_base::Status PopulateResolvedScanMap(const ResolvedScan* resolved_scan,
                                         ResolvedScanMap* resolved_scan_map);

  // Adds the entry corresponding to 'resolved_expr' to 'resolved_expr_map'
  // (whose key is 'resolved_expr' and whose value is the algebrized
  // expression). Note that the map does not own the ResolvedExpr nodes.
  zetasql_base::Status PopulateResolvedExprMap(const ResolvedExpr* resolved_expr,
                                       ResolvedExprMap* resolved_expr_map);

  // Given a list of ResolvedComputedColumn and a column_id, return in
  // (*definition) the expression that defines that column, or nullptr if not
  // found.
  bool FindColumnDefinition(
      const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
          expr_list,
      int column_id, const ResolvedExpr** definition);

  // Cap the algebra for a relation in a struct with a ArrayNestExpr.
  zetasql_base::StatusOr<std::unique_ptr<ArrayNestExpr>> NestRelationInStruct(
      const ResolvedColumnList& output_columns,
      std::unique_ptr<RelationalOp> relation, bool is_with_table);
  // Cap the algebra for a relation with a ArrayNestExpr. This is used to
  // encapsulate the result of a subquery expression which is always a single
  // column (but may be a struct containing multiple columns).
  zetasql_base::StatusOr<std::unique_ptr<ArrayNestExpr>> NestSingleColumnRelation(
      const ResolvedColumnList& output_columns,
      std::unique_ptr<RelationalOp> relation, bool is_with_table);

  // Creates a scan operator iterating over 'table_expr'.
  ::zetasql_base::StatusOr<std::unique_ptr<ArrayScanOp>> CreateScanOfTableAsArray(
      const ResolvedScan* scan, bool is_value_table,
      std::unique_ptr<ValueExpr> table_expr);

  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeIf(
      const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args);
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeIfNull(
      const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args);
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeNullIf(
      const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args);
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeCoalesce(
      const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args);
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeCaseNoValue(
      const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args);
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeCaseWithValue(
      const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args);
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeNotEqual(
      std::vector<std::unique_ptr<ValueExpr>> args);
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeIn(
      const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args);
  zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> AlgebrizeBetween(
      const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args);

  // If the presence of 'conjunct_info' above the join allows us to remove the
  // outer join from one or both sides of 'join_kind', updates 'join_kind'
  // accordingly.
  static zetasql_base::Status NarrowJoinKindForFilterConjunct(
      const FilterConjunctInfo& conjunct_info,
      const absl::flat_hash_set<ResolvedColumn>& left_output_columns,
      const absl::flat_hash_set<ResolvedColumn>& right_output_columns,
      JoinOp::JoinKind* join_kind);

  // For a conjunct above the join, populates the following:
  // - 'push_down_to_join_condition' with true if the push down should stop at
  //    the join condition.
  // - 'push_down_to_left_input'/'push_down_to_right_input' if the push down can
  //    go to the left/right input.
  zetasql_base::Status CanPushFilterConjunctIntoJoin(
      const FilterConjunctInfo& conjunct_info, JoinOp::JoinKind join_kind,
      const absl::flat_hash_set<ResolvedColumn>& left_output_columns,
      const absl::flat_hash_set<ResolvedColumn>& right_output_columns,
      bool* push_down_to_join_condition, bool* push_down_to_left_input,
      bool* push_down_to_right_input);

  // Populates the output arguments based on whether/where the filter conjunct
  // can be pushed down, assuming it is currently in the join condition of an
  // inner join.
  zetasql_base::Status CanPushFilterConjunctDownFromInnerJoinCondition(
      const FilterConjunctInfo& conjunct_info,
      const absl::flat_hash_set<ResolvedColumn>& left_output_columns,
      const absl::flat_hash_set<ResolvedColumn>& right_output_columns,
      bool* push_down_to_left_input, bool* push_down_to_right_input);

  // Adds each entry in 'conjuncts_with_push_down' (all of which must be
  // non-redundant) that can be represented with a HashJoinEqualityExprs object
  // to 'hash_join_equality_exprs'.  'conjuncts_with_push_down' is passed by
  // pointer because we mark any conjuncts added to 'hash_join_equality_exprs'
  // as redundant. The iteration is done in reverse order because
  // 'conjuncts_with_push_down' is interpreted as a stack.
  zetasql_base::Status AlgebrizeJoinConditionForHashJoin(
      const absl::flat_hash_set<ResolvedColumn>& left_output_columns,
      const absl::flat_hash_set<ResolvedColumn>& right_output_columns,
      std::vector<FilterConjunctInfo*>* conjuncts_with_push_down,
      std::vector<JoinOp::HashJoinEqualityExprs>* hash_join_equality_exprs);

  // If 'conjunct_info' can be represented by a HashJoinEqualityExprs, populates
  // 'equality_exprs'. Else returns false.
  zetasql_base::StatusOr<bool> TryAlgebrizeFilterConjunctAsHashJoinEqualityExprs(
      const FilterConjunctInfo& conjunct_info,
      const absl::flat_hash_set<ResolvedColumn>& left_output_columns,
      const absl::flat_hash_set<ResolvedColumn>& right_output_columns,
      int num_previous_equality_exprs,
      JoinOp::HashJoinEqualityExprs* equality_exprs);

  // Creates a new variable for each column and returns a vector of arguments,
  // each assigning the new variable from a DerefExpr of the old variable.
  ::zetasql_base::Status RemapJoinColumns(
      const ResolvedColumnList& columns,
      std::vector<std::unique_ptr<ExprArg>>* output);

  // If 'algebrizer_options_.push_down_filters' is false, returns
  // 'input'. Otherwise returns a RelationalOp that applies the non-redundant
  // entries in 'active_conjuncts' to 'input', marking everything in
  // 'active_conjuncts' as redundant.
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> MaybeApplyFilterConjuncts(
      std::unique_ptr<RelationalOp> input,
      std::vector<FilterConjunctInfo*>* active_conjuncts);

  // Returns a RelationalOp corresponding to 'input' that applies
  // 'algebrized_conjuncts' as filters.
  zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> ApplyAlgebrizedFilterConjuncts(
      std::unique_ptr<RelationalOp> input,
      std::vector<std::unique_ptr<ValueExpr>> algebrized_conjuncts);

  // Represents a named or positional parameter.
  class Parameter {
   public:
    explicit Parameter(const ResolvedParameter& param) {
      if (param.position() == 0) {
        position_or_name_ = param.name();
      } else {
        position_or_name_ = param.position();
      }
    }

    bool operator==(const Parameter& other) const {
      return position_or_name_ == other.position_or_name_;
    }

    template <typename H>
    friend H AbslHashValue(H h, const Parameter& p) {
      return H::combine(std::move(h), p.position_or_name_);
    }

    std::string DebugString() const {
      if (absl::holds_alternative<int>(position_or_name_)) {
        return absl::StrCat("$", *absl::get_if<int>(&position_or_name_));
      }
      return absl::StrCat("$", *absl::get_if<std::string>(&position_or_name_));
    }

   private:
    absl::variant<int, std::string> position_or_name_;
  };

  // Represents a column or a parameter (or a ResolvedExpressionColumn, which in
  // some ways is like a column and in some ways is like a parameter).
  class ColumnOrParameter {
   public:
    ColumnOrParameter() {}

    explicit ColumnOrParameter(const ResolvedColumn& column)
        : column_or_param_(column) {}

    explicit ColumnOrParameter(const ResolvedParameter& param)
        : column_or_param_(Parameter(param)) {}

    explicit ColumnOrParameter(const ResolvedExpressionColumn& column)
        : column_or_param_(column.name()) {}

    bool operator==(const ColumnOrParameter& other) const {
      return column_or_param_ == other.column_or_param_;
    }

    template <typename H>
    friend H AbslHashValue(H h, const ColumnOrParameter& c) {
      return H::combine(std::move(h), c.column_or_param_);
    }

    std::string DebugString() const {
      if (absl::holds_alternative<ResolvedColumn>(column_or_param_)) {
        return absl::get_if<ResolvedColumn>(&column_or_param_)->DebugString();
      }
      if (absl::holds_alternative<Parameter>(column_or_param_)) {
        return absl::get_if<Parameter>(&column_or_param_)->DebugString();
      }
      return *absl::get_if<std::string>(&column_or_param_);
    }

   private:
    // Stores a ResolvedColumn, a Parameter (for a ResolvedParameter), or the
    // name of a ResolvedExpressionColumn.
    using StorageType = absl::variant<ResolvedColumn, Parameter, std::string>;
    StorageType column_or_param_;
  };

  class ProtoOrStructField {
   public:
    enum Kind { PROTO_FIELD, STRUCT_FIELD };

    ProtoOrStructField(Kind kind, int tag_or_field_idx)
        : kind_(kind), tag_or_field_idx_(tag_or_field_idx) {}

    Kind kind() const { return kind_; }

    // Requires kind() == PROTO_FIELD.
    int tag_number() const { return tag_or_field_idx_; }

    // Requires kind() == STRUCT_FIELD.
    int field_idx() const { return tag_or_field_idx_; }

    bool operator==(const ProtoOrStructField& other) const {
      return kind_ == other.kind() &&
             tag_or_field_idx_ == other.tag_or_field_idx_;
    }

    template <typename H>
    friend H AbslHashValue(H h, const ProtoOrStructField& f) {
      h = H::combine(std::move(h), f.kind_);
      return H::combine(std::move(h), f.tag_or_field_idx_);
    }

    std::string DebugString() const {
      switch (kind_) {
        case PROTO_FIELD:
          return absl::StrCat("proto_", tag_number());
        case STRUCT_FIELD:
          return absl::StrCat("struct_", field_idx());
      }
    }

   private:
    Kind kind_;
    int tag_or_field_idx_ = 0;
    // Allow copy/move/assign.
  };

  // Represents an expression of the form
  // proto_column_or_parameter.field_with_tag1.field_with_tag2...; extension
  // fields are allowed.
  struct SharedProtoFieldPath {
    ColumnOrParameter column_or_param;
    // TODO: Consider allowing struct field names at the start of the
    // path to optimize for more cases.
    std::vector<ProtoOrStructField> field_path;

    bool operator==(const SharedProtoFieldPath& other) const {
      return column_or_param == other.column_or_param &&
             field_path == other.field_path;
    }

    template <typename H>
    friend H AbslHashValue(H h, const SharedProtoFieldPath& p) {
      h = H::combine(std::move(h), p.column_or_param);
      return H::combine(std::move(h), p.field_path);
    }

    std::string DebugString() const;
  };

  // Adds a FieldRegistry to 'get_proto_field_caches_' and returns the
  // corresponding pointer. If 'id' is set, also updates
  // 'proto_field_registry_map_'.
  zetasql_base::StatusOr<ProtoFieldRegistry*> AddProtoFieldRegistry(
      const absl::optional<SharedProtoFieldPath>& id);

  // Adds a ProtoFieldReader corresponding to 'access_info' and 'registry' to
  // 'get_proto_field_readers_' and returns the corresponding pointer. If 'id'
  // is set and 'access_info' represents a proto-valued field, also updates
  // 'get_proto_field_reader_map_'.
  zetasql_base::StatusOr<ProtoFieldReader*> AddProtoFieldReader(
      const absl::optional<SharedProtoFieldPath>& id,
      const ProtoFieldAccessInfo& access_info, ProtoFieldRegistry* registry);

  // LanguageOption to use when algebrizing.
  const LanguageOptions language_options_;
  const AlgebrizerOptions algebrizer_options_;

  // Maintains the mapping between column ids and variables.
  //
  // Since the same ResolvedColumn can appear in multiple places (e.g., the
  // column lists of multiple scan nodes), the Algebrizer makes extensive use of
  // ColumnToVariableMapping::GetVariableNameFromColumn() to either retrieve a
  // column or add it if it hasn't been added already. However, there are also
  // places where the Algebrizer instead calls
  // ColumnToVariableMapping::AssignNewVariableToColumn() to forcibly allocate a
  // new variable for a column that may have already been added; this technique
  // is typically used where an operator has both an input and an output
  // corresponding to a particular ResolvedColumn. Finally, there is also at
  // least one place where the Algebrizer explicitly requires that a variable
  // corresponding to a ResolvedColumn has already been added: when resolving a
  // RESOLVED_COLUMN_REF expression in the case where the Algebrizer is
  // resolving a ZetaSQL statement (not a standalone expression).
  std::unique_ptr<ColumnToVariableMapping> column_to_variable_;
  // Generates variable names corresponding to query parameters or columns.
  // Owned by 'column_to_variable_'.
  VariableGenerator* variable_gen_;
  // Maps parameters to variables for named parameters or else contains a list
  // of positional parameters. Not owned.
  Parameters* parameters_;
  ParameterMap* column_map_;  // Maps columns to variables. Not owned.

  // Maps system variables to variable ids.  Not owned.
  SystemVariablesAlgebrizerMap* system_variables_map_;

  // Maps named WITH subquery to an argument (variable, ValueExpr). Used to
  // algebrize WithRef scans referencing named subqueries.
  absl::flat_hash_map<std::string, ExprArg*> with_map_;  // Not owned.
  // Vector of LetOp/LetExpr assignments we need to apply for WITH clauses in
  // the query.
  std::vector<std::unique_ptr<ExprArg>> with_subquery_let_assignments_;

  // Owns all the ProtoFieldRegistries created by the algebrizer.
  std::vector<std::unique_ptr<ProtoFieldRegistry>> proto_field_registries_;

  // Owns all the ProtoFieldReaders created by the algebrizer.
  std::vector<std::unique_ptr<ProtoFieldReader>> get_proto_field_readers_;

  // If 'algebrizer_options_.consolidate_proto_field_accesses' is true, contains
  // every GetProtoFieldExpr::FieldRegistry whose proto-valued expression can be
  // represented with a ProtoColumnAndFieldPath. The pointers are owned by
  // 'proto_field_registries_'.
  //
  // For example, the expression proto_column.a.b would result in:
  //
  // GetProtoFieldExpr
  // - GetProtoFieldExpr (corresponding to proto_column.a)
  //   - ValueExpr (corresponding to proto_column)
  //   - ProtoFieldReader (in 'get_proto_field_reader_map_'
  //                       with key proto_column.a)
  //     - ProtoFieldAccessInfo (corresponding to a)
  //     - FieldRegistry (in 'proto_field_registry_map_' with key
  //                      proto_column)
  // - ProtoFieldReader (in 'get_proto_field_reader_map_'
  //                     with key proto_column.a.b)
  //    - ProtoFieldAccessInfo (corresponding to b)
  //    - FieldRegistry (in 'proto_field_registry_map_' with key
  //                     proto_column.a)
  //
  // For more details, see the class comment for GetProtoFieldExpr in
  // operator.h.
  absl::flat_hash_map<SharedProtoFieldPath, ProtoFieldRegistry*>
      proto_field_registry_map_;

  // If 'algebrizer_options_.consolidate_proto_field_accesses' is true, contains
  // the ProtoFieldReader for every GetProtoFieldExpr node with proto type that
  // represents an expression that can be represented with a
  // ProtoColumnAndFieldPath. The pointers are owned by
  // 'get_proto_field_readers_'. Note that all the keys in this map have
  // non-empty tag_number_paths. We only cache proto-valued nodes because it's
  // possible to have two different accesses to a non-message field that have
  // slightly different meaning (e.g., get_has_bit = true vs. false).
  //
  // For more details, see the class comment for GetProtoFieldExpr in
  // operator.h.
  absl::flat_hash_map<SharedProtoFieldPath, ProtoFieldReader*>
      get_proto_field_reader_map_;

  TypeFactory* type_factory_;  // Not owned.

  // For generating unique column names.
  int next_column_;
};

}  // namespace zetasql

#endif  // ZETASQL_REFERENCE_IMPL_ALGEBRIZER_H_
