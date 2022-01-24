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

#ifndef ZETASQL_RESOLVED_AST_SQL_BUILDER_H_
#define ZETASQL_RESOLVED_AST_SQL_BUILDER_H_

#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/public/analyzer.h"  // For QueryParametersMap
#include "zetasql/public/catalog.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/query_expression.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include <cstdint>
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {

class QueryExpression;

// SQLBuilder takes the ZetaSQL Resolved ASTs and generates its equivalent SQL
// form.
//
// Usage:
//   SQLBuilder sql_builder;
//   const ResolvedQueryStmt* node = ...;
//   node->Accept(&sql_builder);
//   const std::string& unparsed_sql = sql_builder.sql();
//
//   NOTE: SQL for the node gets appended onto the SQLBuilder's sql.
//
// Be wary of using SQLBuilder with positional query parameters; there is no
// guarantee that they will appear in the same order as their positions in the
// resolved AST.
class SQLBuilder : public ResolvedASTVisitor {
 public:
  // Options to use when generating SQL.
  struct SQLBuilderOptions {
    SQLBuilderOptions() {
      // PRODUCT_EXTERNAL is the default product mode.
      language_options.set_product_mode(PRODUCT_EXTERNAL);
    }
    explicit SQLBuilderOptions(ProductMode product_mode) {
      language_options.set_product_mode(product_mode);
    }
    explicit SQLBuilderOptions(const LanguageOptions& language_options)
        : language_options(language_options) {}
    ~SQLBuilderOptions() {}

    // Language options included enabled/disabled features and product mode,
    // whether the generated SQL is for INTERNAL or EXTERNAL mode.  For example,
    // EXTERNAL mode supports FLOAT64 only as a type name, not DOUBLE
    // (INTERNAL supports both as type names).
    LanguageOptions language_options;

    // Undeclared query parameters get wrapped in explicit CASTs. This is needed
    // because explicit CASTs may get dropped from the resolved AST upon parsing
    // but may be required for inferring the type of the undeclared parameters.
    // This member can be copied from AnalyzerOutput::undeclared_parameters().
    QueryParametersMap undeclared_parameters;

    // Same as above, but applies to undeclared positional parameters.
    // This member can be copied from
    // AnalyzerOutput::undeclared_positional_parameters().
    std::vector<const Type*> undeclared_positional_parameters;

    // Controls how the SQLBuilder outputs positional parameters. By default,
    // they are converted to ?. In named mode, which allows the caller to
    // sanity-check where they appear in the query, positional parameters are
    // converted to parameters named @param1, @param2, and so on in
    // correspondence with the position of the parameter.
    //
    // Warning: there is no attempt made to verify that the ? in the output
    // appear in the same order of the positional parameters in the resolved
    // AST.
    enum PositionalParameterOutputMode {
      kQuestionMark = 0,
      kNamed = 1,
    };
    PositionalParameterOutputMode positional_parameter_mode = kQuestionMark;
  };

  explicit SQLBuilder(const SQLBuilderOptions& options = SQLBuilderOptions());
  SQLBuilder(const SQLBuilder&) = delete;
  SQLBuilder& operator=(const SQLBuilder&) = delete;
  ~SQLBuilder() override;

  // Visit all nodes in the AST tree and accumulate SQL to be returned via sql()
  // call, below.
  absl::Status Process(const ResolvedNode& ast);

  // Returns the sql string for the last visited ResolvedAST.
  std::string sql();

  // Visit methods for types of ResolvedStatement.
  absl::Status VisitResolvedQueryStmt(const ResolvedQueryStmt* node) override;
  absl::Status VisitResolvedExplainStmt(
      const ResolvedExplainStmt* node) override;
  absl::Status VisitResolvedCreateDatabaseStmt(
      const ResolvedCreateDatabaseStmt* node) override;
  absl::Status VisitResolvedCreateIndexStmt(
    const ResolvedCreateIndexStmt* node) override;
  absl::Status VisitResolvedCreateModelStmt(
      const ResolvedCreateModelStmt* node) override;
  absl::Status VisitResolvedCreateSchemaStmt(
      const ResolvedCreateSchemaStmt* node) override;
  absl::Status VisitResolvedCreateTableStmt(
      const ResolvedCreateTableStmt* node) override;
  absl::Status VisitResolvedCreateSnapshotTableStmt(
      const ResolvedCreateSnapshotTableStmt* node) override;
  absl::Status VisitResolvedCreateTableAsSelectStmt(
      const ResolvedCreateTableAsSelectStmt* node) override;
  absl::Status VisitResolvedCreateViewStmt(
      const ResolvedCreateViewStmt* node) override;
  absl::Status VisitResolvedCreateMaterializedViewStmt(
      const ResolvedCreateMaterializedViewStmt* node) override;
  absl::Status VisitResolvedCreateExternalTableStmt(
      const ResolvedCreateExternalTableStmt* node) override;
  absl::Status VisitResolvedCreatePrivilegeRestrictionStmt(
      const ResolvedCreatePrivilegeRestrictionStmt* node) override;
  absl::Status VisitResolvedCreateRowAccessPolicyStmt(
      const ResolvedCreateRowAccessPolicyStmt* node) override;
  absl::Status VisitResolvedCreateConstantStmt(
      const ResolvedCreateConstantStmt* node) override;
  absl::Status VisitResolvedCreateFunctionStmt(
      const ResolvedCreateFunctionStmt* node) override;
  absl::Status VisitResolvedCreateTableFunctionStmt(
      const ResolvedCreateTableFunctionStmt* node) override;
  absl::Status VisitResolvedCreateProcedureStmt(
      const ResolvedCreateProcedureStmt* node) override;
  absl::Status VisitResolvedArgumentDef(
      const ResolvedArgumentDef* node) override;
  absl::Status VisitResolvedArgumentRef(
      const ResolvedArgumentRef* node) override;
  absl::Status VisitResolvedCloneDataStmt(
      const ResolvedCloneDataStmt* node) override;
  absl::Status VisitResolvedExportDataStmt(
      const ResolvedExportDataStmt* node) override;
  absl::Status VisitResolvedExportModelStmt(
      const ResolvedExportModelStmt* node) override;
  absl::Status VisitResolvedCallStmt(const ResolvedCallStmt* node) override;
  absl::Status VisitResolvedDefineTableStmt(
      const ResolvedDefineTableStmt* node) override;
  absl::Status VisitResolvedDescribeStmt(
      const ResolvedDescribeStmt* node) override;
  absl::Status VisitResolvedShowStmt(
      const ResolvedShowStmt* node) override;
  absl::Status VisitResolvedBeginStmt(const ResolvedBeginStmt* node) override;
  absl::Status VisitResolvedSetTransactionStmt(
      const ResolvedSetTransactionStmt* node) override;
  absl::Status VisitResolvedCommitStmt(const ResolvedCommitStmt* node) override;
  absl::Status VisitResolvedRollbackStmt(
      const ResolvedRollbackStmt* node) override;
  absl::Status VisitResolvedStartBatchStmt(
      const ResolvedStartBatchStmt* node) override;
  absl::Status VisitResolvedRunBatchStmt(
      const ResolvedRunBatchStmt* node) override;
  absl::Status VisitResolvedAbortBatchStmt(
      const ResolvedAbortBatchStmt* node) override;
  absl::Status VisitResolvedDeleteStmt(
      const ResolvedDeleteStmt* node) override;
  absl::Status VisitResolvedDropStmt(
      const ResolvedDropStmt* node) override;
  absl::Status VisitResolvedDropFunctionStmt(
      const ResolvedDropFunctionStmt* node) override;
  absl::Status VisitResolvedDropTableFunctionStmt(
      const ResolvedDropTableFunctionStmt* node) override;
  absl::Status VisitResolvedDropMaterializedViewStmt(
      const ResolvedDropMaterializedViewStmt* node) override;
  absl::Status VisitResolvedDropPrivilegeRestrictionStmt(
      const ResolvedDropPrivilegeRestrictionStmt* node) override;
  absl::Status VisitResolvedDropRowAccessPolicyStmt(
      const ResolvedDropRowAccessPolicyStmt* node) override;
  absl::Status VisitResolvedDropSnapshotTableStmt(
      const ResolvedDropSnapshotTableStmt* node) override;
  absl::Status VisitResolvedDropSearchIndexStmt(
      const ResolvedDropSearchIndexStmt* node) override;
  absl::Status VisitResolvedTruncateStmt(
      const ResolvedTruncateStmt* node) override;
  absl::Status VisitResolvedUpdateStmt(
      const ResolvedUpdateStmt* node) override;
  absl::Status VisitResolvedInsertStmt(
      const ResolvedInsertStmt* node) override;
  absl::Status VisitResolvedMergeStmt(const ResolvedMergeStmt* node) override;
  absl::Status VisitResolvedMergeWhen(const ResolvedMergeWhen* node) override;
  absl::Status VisitResolvedGrantStmt(
      const ResolvedGrantStmt* node) override;
  absl::Status VisitResolvedRevokeStmt(
      const ResolvedRevokeStmt* node) override;
  absl::Status VisitResolvedAlterDatabaseStmt(
      const ResolvedAlterDatabaseStmt* node) override;
  absl::Status VisitResolvedAlterPrivilegeRestrictionStmt(
      const ResolvedAlterPrivilegeRestrictionStmt* node) override;
  absl::Status VisitResolvedAlterRowAccessPolicyStmt(
      const ResolvedAlterRowAccessPolicyStmt* node) override;
  absl::Status VisitResolvedAlterAllRowAccessPoliciesStmt(
      const ResolvedAlterAllRowAccessPoliciesStmt* node) override;
  absl::Status VisitResolvedAlterSchemaStmt(
      const ResolvedAlterSchemaStmt* node) override;
  absl::Status VisitResolvedAlterTableSetOptionsStmt(
      const ResolvedAlterTableSetOptionsStmt* node) override;
  absl::Status VisitResolvedAlterTableStmt(
      const ResolvedAlterTableStmt* node) override;
  absl::Status VisitResolvedAlterViewStmt(
      const ResolvedAlterViewStmt* node) override;
  absl::Status VisitResolvedAlterMaterializedViewStmt(
      const ResolvedAlterMaterializedViewStmt* node) override;
  absl::Status VisitResolvedRenameStmt(
      const ResolvedRenameStmt* node) override;
  absl::Status VisitResolvedImportStmt(const ResolvedImportStmt* node) override;
  absl::Status VisitResolvedModuleStmt(const ResolvedModuleStmt* node) override;
  absl::Status VisitResolvedAnalyzeStmt(
      const ResolvedAnalyzeStmt* node) override;
  absl::Status VisitResolvedAssertStmt(const ResolvedAssertStmt* node) override;
  absl::Status VisitResolvedAssignmentStmt(
      const ResolvedAssignmentStmt* node) override;
  absl::Status VisitResolvedExecuteImmediateStmt(
      const ResolvedExecuteImmediateStmt* node) override;

  // Visit methods for types of ResolvedExpr.
  absl::Status VisitResolvedExpressionColumn(
      const ResolvedExpressionColumn* node) override;
  absl::Status VisitResolvedLiteral(const ResolvedLiteral* node) override;
  absl::Status VisitResolvedConstant(const ResolvedConstant* node) override;
  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override;
  absl::Status VisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall* node) override;
  absl::Status VisitResolvedAnalyticFunctionCall(
      const ResolvedAnalyticFunctionCall* node) override;
  absl::Status VisitResolvedInlineLambda(
      const ResolvedInlineLambda* node) override;
  absl::Status VisitResolvedGetProtoField(
      const ResolvedGetProtoField* node) override;
  absl::Status VisitResolvedFlatten(const ResolvedFlatten* node) override;
  absl::Status VisitResolvedFilterField(
      const ResolvedFilterField* node) override;
  absl::Status VisitResolvedReplaceField(
      const ResolvedReplaceField* node) override;
  absl::Status VisitResolvedFlattenedArg(
      const ResolvedFlattenedArg* node) override;
  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* node) override;
  absl::Status VisitResolvedCast(const ResolvedCast* node) override;
  absl::Status VisitResolvedColumnHolder(
      const ResolvedColumnHolder* node) override;
  absl::Status VisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr* node) override;
  absl::Status VisitResolvedLetExpr(const ResolvedLetExpr* node) override;
  absl::Status VisitResolvedTableAndColumnInfo(
      const ResolvedTableAndColumnInfo* node) override;
  absl::Status VisitResolvedOption(const ResolvedOption* node) override;
  absl::Status VisitResolvedParameter(const ResolvedParameter* node) override;
  absl::Status VisitResolvedSystemVariable(
      const ResolvedSystemVariable* node) override;
  absl::Status VisitResolvedMakeProto(const ResolvedMakeProto* node) override;
  absl::Status VisitResolvedMakeProtoField(
      const ResolvedMakeProtoField* node) override;
  absl::Status VisitResolvedMakeStruct(const ResolvedMakeStruct* node) override;
  absl::Status VisitResolvedGetStructField(
      const ResolvedGetStructField* node) override;
  absl::Status VisitResolvedGetJsonField(
      const ResolvedGetJsonField* node) override;
  absl::Status VisitResolvedOrderByItem(
      const ResolvedOrderByItem* node) override;
  absl::Status VisitResolvedComputedColumn(
      const ResolvedComputedColumn* node) override;
  absl::Status VisitResolvedAssertRowsModified(
      const ResolvedAssertRowsModified* node) override;
  absl::Status VisitResolvedReturningClause(
      const ResolvedReturningClause* node) override;
  absl::Status VisitResolvedDMLDefault(const ResolvedDMLDefault* node) override;
  absl::Status VisitResolvedDMLValue(const ResolvedDMLValue* node) override;
  absl::Status VisitResolvedInsertRow(const ResolvedInsertRow* node) override;
  absl::Status VisitResolvedUpdateItem(const ResolvedUpdateItem* node) override;
  absl::Status VisitResolvedUpdateArrayItem(
      const ResolvedUpdateArrayItem* node) override;
  absl::Status VisitResolvedPrivilege(const ResolvedPrivilege* node) override;
  absl::Status VisitResolvedAggregateHavingModifier(
      const ResolvedAggregateHavingModifier* node) override;

  // Visit methods for types of ResolvedScan.
  absl::Status VisitResolvedAnalyticScan(
      const ResolvedAnalyticScan* node) override;
  absl::Status VisitResolvedTableScan(const ResolvedTableScan* node) override;
  absl::Status VisitResolvedProjectScan(
      const ResolvedProjectScan* node) override;
  absl::Status VisitResolvedTVFScan(const ResolvedTVFScan* node) override;
  absl::Status VisitResolvedRelationArgumentScan(
      const ResolvedRelationArgumentScan* node) override;
  absl::Status VisitResolvedFilterScan(const ResolvedFilterScan* node) override;
  absl::Status VisitResolvedJoinScan(const ResolvedJoinScan* node) override;
  absl::Status VisitResolvedArrayScan(const ResolvedArrayScan* node) override;
  absl::Status VisitResolvedLimitOffsetScan(
      const ResolvedLimitOffsetScan* node) override;
  absl::Status VisitResolvedSetOperationScan(
      const ResolvedSetOperationScan* node) override;
  absl::Status VisitResolvedOrderByScan(
      const ResolvedOrderByScan* node) override;
  absl::Status VisitResolvedAggregateScan(
      const ResolvedAggregateScan* node) override;
  absl::Status VisitResolvedAnonymizedAggregateScan(
      const ResolvedAnonymizedAggregateScan* node) override;
  absl::Status VisitResolvedRecursiveScan(
      const ResolvedRecursiveScan* node) override;
  absl::Status VisitResolvedWithScan(const ResolvedWithScan* node) override;
  absl::Status VisitResolvedRecursiveRefScan(
      const ResolvedRecursiveRefScan* node) override;
  absl::Status VisitResolvedWithRefScan(
      const ResolvedWithRefScan* node) override;
  absl::Status VisitResolvedSampleScan(
      const ResolvedSampleScan* node) override;
  absl::Status VisitResolvedSingleRowScan(
      const ResolvedSingleRowScan* node) override;
  absl::Status VisitResolvedPivotScan(const ResolvedPivotScan* node) override;
  absl::Status VisitResolvedUnpivotScan(
      const ResolvedUnpivotScan* node) override;
  absl::Status VisitResolvedGroupRowsScan(
      const ResolvedGroupRowsScan* node) override;

  // Visit methods for analytic functions related nodes.
  absl::Status VisitResolvedAnalyticFunctionGroup(
      const ResolvedAnalyticFunctionGroup* node) override;
  absl::Status VisitResolvedWindowPartitioning(
      const ResolvedWindowPartitioning* node) override;
  absl::Status VisitResolvedWindowOrdering(
      const ResolvedWindowOrdering* node) override;
  absl::Status VisitResolvedWindowFrame(
      const ResolvedWindowFrame* node) override;
  absl::Status VisitResolvedWindowFrameExpr(
      const ResolvedWindowFrameExpr* node) override;
  absl::Status VisitResolvedCreateEntityStmt(
      const ResolvedCreateEntityStmt* node) override;
  absl::Status VisitResolvedAlterEntityStmt(
      const ResolvedAlterEntityStmt* node) override;
  absl::Status VisitResolvedAuxLoadDataStmt(
      const ResolvedAuxLoadDataStmt* node) override;

  absl::Status DefaultVisit(const ResolvedNode* node) override;

 protected:
  // Holds SQL text of nodes representing expressions/subqueries and
  // QueryExpression for scan nodes.
  struct QueryFragment {
    explicit QueryFragment(const ResolvedNode* node, std::string text)
        : node(node), text(std::move(text)) {}

    // Takes ownership of the <query_expression>.
    explicit QueryFragment(const ResolvedNode* node,
                           QueryExpression* query_expression)
        : node(node), query_expression(query_expression) {}

    QueryFragment(const QueryFragment&) = delete;
    QueryFragment operator=(const QueryFragment&) = delete;

    std::string GetSQL() const;

    // Associated resolved node tree for the QueryFragment.
    const ResolvedNode* node = nullptr;

    // Populated if QueryFragment is created from a QueryExpression.
    std::unique_ptr<QueryExpression> query_expression;

   private:
    // Associated sql text for the QueryFragment.
    const std::string text;
  };

  // Dumps all the QueryFragment in query_fragments_ (if any).
  void DumpQueryFragmentStack();

  // Stack operations on query_fragments_.
  void PushQueryFragment(std::unique_ptr<QueryFragment> query_fragment);

  std::unique_ptr<QueryFragment> PopQueryFragment();

  // Helper functions which creates QueryFragment from the passed params and
  // push it on query_fragments_.
  void PushQueryFragment(const ResolvedNode* node, const std::string& text);
  void PushQueryFragment(const ResolvedNode* node,
                         QueryExpression* query_expression);

  // Helper function which Visits the given <node> and returns the corresponding
  // QueryFragment generated.
  absl::StatusOr<std::unique_ptr<QueryFragment>> ProcessNode(
      const ResolvedNode* node);
  absl::StatusOr<std::string> ProcessExecuteImmediateArgument(
      const ResolvedExecuteImmediateArgument* node);

  // Wraps the given <query_expression> corresponding to the scan <node> as a
  // subquery to the from clause, mutating the <query_expression> subsequently.
  absl::Status WrapQueryExpression(const ResolvedScan* node,
                                   QueryExpression* query_expression);

  // Helper function which fetches <select_list> for columns present in
  // <column_list>. <parent_scan> is optional; if present, it points to the
  // ResolvedScan for which we are currently fetching a select list.
  absl::Status GetSelectList(
      const ResolvedColumnList& column_list,
      const std::map<int64_t, const ResolvedExpr*>& col_to_expr_map,
      const ResolvedScan* parent_scan, QueryExpression* query_expression,
      std::vector<std::pair<std::string, std::string>>* select_list);
  // This overload does not expects a <col_to_expr_map> which is needed only
  // when select expressions are present in the column_list.
  absl::Status GetSelectList(
      const ResolvedColumnList& column_list, QueryExpression* query_expression,
      std::vector<std::pair<std::string, std::string>>* select_list);

  // Adds select_list for columns present in <column_list> inside the
  // <query_expression>, if not present.
  absl::Status AddSelectListIfNeeded(const ResolvedColumnList& column_list,
                                     QueryExpression* query_expression);

  // Merges the <type> and the <annotations> trees and prints the column
  // schema to <text>.
  absl::Status AppendColumnSchema(
      const Type* type, bool is_hidden,
      const ResolvedColumnAnnotations* annotations,
      const ResolvedGeneratedColumnInfo* generated_column_info,
      const ResolvedColumnDefaultValue* default_value, std::string* text);

  absl::StatusOr<std::string> GetHintListString(
      const std::vector<std::unique_ptr<const ResolvedOption>>& hint_list);

  absl::Status AppendCloneDataSource(const ResolvedScan* source,
                                     std::string* sql);

  // Always append a (possibly empty) OPTIONS clause.
  absl::Status AppendOptions(
      const std::vector<std::unique_ptr<const ResolvedOption>>& option_list,
      std::string* sql);
  // Only append an OPTIONS clause if there is at least one option.
  absl::Status AppendOptionsIfPresent(
      const std::vector<std::unique_ptr<const ResolvedOption>>& option_list,
      std::string* sql);

  absl::Status AppendHintsIfPresent(
      const std::vector<std::unique_ptr<const ResolvedOption>>& hint_list,
      std::string* text);

  void PushSQLForQueryExpression(const ResolvedNode* node,
                                 QueryExpression* query_expression);

  // Returns fully qualified path to access the column (in scope of the caller).
  // i.e. <scan_alias>.<column_alias>
  std::string GetColumnPath(const ResolvedColumn& column);

  // Returns the alias to be used to select the column.
  std::string GetColumnAlias(const ResolvedColumn& column);

  // Create a new alias to be used for the column, replacing any existing alias.
  std::string UpdateColumnAlias(const ResolvedColumn& column);

  // Returns the sql text associated with a left/right join scan. Adds explicit
  // scan alias if necessary.
  absl::StatusOr<std::string> GetJoinOperand(const ResolvedScan* scan);

  // Helper function which fetches the list of function arguments
  absl::StatusOr<std::string> GetFunctionArgListString(
      const std::vector<std::string>& arg_name_list,
      const FunctionSignature& signature);

  // Fetches the scan alias corresponding to the given scan node. If not
  // present, assigns a new unique alias to the scan node.
  std::string GetScanAlias(const ResolvedScan* scan);

  // Like GetScanAlias but for a Table, to be able to assign different aliases
  // to ResolvedTableScan and its underlying Table.
  std::string GetTableAlias(const Table* table);

  // Helper for the above. Keep generating aliases until find one that does not
  // conflict with a column name.
  std::string MakeNonconflictingAlias(const std::string& name);

  // Checks whether the table can be used without an explicit "AS" clause, which
  // will use the last component of its table name as its alias. If we have two
  // tables with the same local name in two catalogs/nested catalogs, then we
  // cannot use both without an explicit alias (for example, S.T vs. T, or
  // S1.T vs. S2.T).
  bool CanTableBeUsedWithImplicitAlias(const Table* table);

  // Returns a unique incremental id each time this is called.
  int64_t GetUniqueId();

  // Get the sql for CreateViewStatement/CreateMaterializedViewStatement.
  absl::Status GetCreateViewStatement(const ResolvedCreateViewBase* node,
                                      bool is_value_table,
                                      const std::string& view_type);

  // Get the first part of the syntax for a CREATE command, including statement
  // hints, the <object_type> (e.g. "TABLE") and name, and CREATE modifiers,
  // and a trailing space.
  absl::Status GetCreateStatementPrefix(const ResolvedCreateStatement* node,
                                        const std::string& object_type,
                                        std::string* sql);

  // If the view was created with explicit column names,
  // prints the column names.
  void GetOptionalColumnNameList(const ResolvedCreateViewBase* node,
                                 std::string* sql);

  // Appends PARTITION BY or CLUSTER BY expressions to the provided string, not
  // including the "PARTITION BY " or "CLUSTER BY " prefix.
  absl::Status GetPartitionByListString(
      const std::vector<std::unique_ptr<const ResolvedExpr>>& partition_by_list,
      std::string* sql);

  // Helper function to get corresponding SQL for a list of TableAndColumnInfo
  // to be analyzed in ANALYZE STATEMENT.
  absl::Status GetTableAndColumnInfoList(
      const std::vector<std::unique_ptr<const ResolvedTableAndColumnInfo>>&
          table_and_column_info_list,
      std::string* sql);

  static std::string GetOptionalObjectType(const std::string& object_type);

  // Get the "<privilege_list> ON <object_type> <name_path>" part of a GRANT or
  // REVOKE statement.
  absl::Status GetPrivilegesString(const ResolvedGrantOrRevokeStmt* node,
                                   std::string* sql);

  // Helper functions to save the <path> used to access the column later.
  void SetPathForColumn(const ResolvedColumn& column, const std::string& path);
  void SetPathForColumnList(const ResolvedColumnList& column_list,
                            const std::string& scan_alias);
  absl::Status SetPathForColumnsInScan(const ResolvedScan* scan,
                                       const std::string& alias);

  // Helper functions to set up ResolvedColumn path in returning clause.
  absl::Status SetPathForColumnsInReturningExpr(const ResolvedExpr* expr);

  // Helper function to ensure:
  // - Columns in <output_column_list> matches 1:1 in order with
  //   <query_expression> select-list.
  // - Column aliases in <output_column_list> (if not internal) matches the
  //   <query_expression> select-list.
  absl::Status MatchOutputColumns(
      const std::vector<std::unique_ptr<const ResolvedOutputColumn>>&
          output_column_list,
      const ResolvedScan* query, QueryExpression* query_expression);

  // Helper function which process the given scan <query> and returns the
  // corresponding QueryExpression. Also ensures that the query produces columns
  // matching the order and aliases in <output_column_list>.
  // Caller owns the returned QueryExpression.
  absl::StatusOr<QueryExpression*> ProcessQuery(
      const ResolvedScan* query,
      const std::vector<std::unique_ptr<const ResolvedOutputColumn>>&
          output_column_list);

  static absl::StatusOr<absl::string_view> GetNullHandlingModifier(
      ResolvedNonScalarFunctionCallBase::NullHandlingModifier kind);

  absl::StatusOr<std::string> GetSQL(const Value& value, ProductMode mode,
                                     bool is_constant_value = false);

  // Helper function to return corresponding SQL for a list of
  // ResolvedUpdateItems.
  absl::StatusOr<std::string> GetUpdateItemListSQL(
      const std::vector<std::unique_ptr<const ResolvedUpdateItem>>&
          update_item_list);

  // Helper function to return corresponding SQL for a list of columns to be
  // inserted.
  std::string GetInsertColumnListSQL(
      const std::vector<ResolvedColumn>& insert_column_list) const;

  absl::Status ProcessWithPartitionColumns(
      std::string* sql, const ResolvedWithPartitionColumns* node);

  absl::StatusOr<std::string> ProcessCreateTableStmtBase(
      const ResolvedCreateTableStmtBase* node, bool process_column_definitions,
      const std::string& table_type);

  // Helper function for adding SQL for aggregate and group by lists.
  absl::Status ProcessAggregateScanBase(
      const ResolvedAggregateScanBase* node,
      const std::vector<int>& rollup_column_id_list,
      QueryExpression* query_expression);

  // Helper function to return corresponding SQL for ResolvedAlterAction
  absl::StatusOr<std::string> GetAlterActionSQL(
      const std::vector<std::unique_ptr<const ResolvedAlterAction>>&
          alter_action_list);

  // Helper function to return corresponding SQL for ResolvedAlterObjectStmt
  absl::Status GetResolvedAlterObjectStmtSQL(
      const ResolvedAlterObjectStmt* node, absl::string_view object_kind);

  // Helper function to return corresponding SQL from the grantee list of
  // GRANT, REVOKE, CREATE/ALTER ROW POLICY statements.
  absl::StatusOr<std::string> GetGranteeListSQL(
      const std::string& prefix, const std::vector<std::string>& grantee_list,
      const std::vector<std::unique_ptr<const ResolvedExpr>>&
          grantee_expr_list);
  // Helper function to append table_element, including column_schema and
  // table_constraints, to sql.
  absl::Status ProcessTableElementsBase(
      std::string* sql,
      const std::vector<std::unique_ptr<const ResolvedColumnDefinition>>&
          column_definition_list,
      const ResolvedPrimaryKey* resolved_primary_key,
      const std::vector<std::unique_ptr<const ResolvedForeignKey>>&
          foreign_key_list,
      const std::vector<std::unique_ptr<const ResolvedCheckConstraint>>&
          check_constraint_list);
  // Helper function to append foreign key table constraint.
  absl::StatusOr<std::string> ProcessForeignKey(
      const ResolvedForeignKey* foreign_key, bool is_if_not_exists);
  // Helper function to append primary key table constraint.
  absl::StatusOr<std::string> ProcessPrimaryKey(
      const ResolvedPrimaryKey* primary_key);
  std::string ComputedColumnAliasDebugString() const;

  // If we have a recursive view, sets up internal data structures in
  // preparation for generating SQL text for a recursive view, so that the
  // columns and table references to the recursive table are correct.
  //
  // If 'node->query()' is not a ResolvedRecursiveScan, this function is a
  // no-op.
  absl::Status MaybeSetupRecursiveView(const ResolvedCreateViewBase* node);

  // A stack of QueryFragment kept to parallel the Visit call stack. Stores
  // return values of each Visit call which are then popped/used by the caller
  // of Accept.
  // NOTE: Using deque makes it possible to look at the entire stack, useful for
  // debugging purposes.
  std::deque<std::unique_ptr<QueryFragment>> query_fragments_;

  // Stores the path used to access the resolved columns, keyed by column_id.
  // This map needs to updated to reflect the path of the column in the current
  // context (e.g. when inside or outside a join or subquery) as we visit nodes
  // up the tree.
  std::map<int /* column_id */, std::string> column_paths_;

  // Stores the unique alias we generate for the ResolvedComputedColumn.
  // These aliases are mostly used to replace the internal column names assigned
  // to these columns in the resolved tree. The key is column_id here.
  std::map<int /* column_id */, std::string> computed_column_alias_;

  // Stores the unique scan_aliases assigned for the corresponding scan nodes.
  absl::flat_hash_map<const ResolvedScan*, std::string> scan_alias_map_;

  // Stores the unique aliases assigned to tables.
  absl::flat_hash_map<const Table*, std::string> table_alias_map_;

  // Stores the tables that have been used without an explicit alias.
  // This is a map from the implicit alias to the identifier (as returned by
  // TableToIdentifierLiteral).
  // This is because if we have two tables in different nested catalogs S1.T
  // and S2.T, they cannot both use the same implicit alias "t" and one of them
  // needs to be explicitly aliased.
  absl::flat_hash_map<std::string, std::string> tables_with_implicit_alias_;

  // Stores the target table alias for DML Returning statements.
  std::string returning_table_alias_;
  // Expected position of the next unparsed positional query parameter. Used for
  // validation only.
  int expected_next_parameter_position_ = 1;

  // Used to generate a unique alias id.
  zetasql_base::SequenceNumber scan_id_sequence_;

  // Stores the sql text of the columns, keyed by column_id and populated in
  // AggregateScan, AnalyticScan, and TVFScan. We refer to the sql text in its
  // parent ProjectScan (while building the select_list) and in
  // ResolvedColumnRef pointing to the column.
  // NOTE: This map is cleared every time we visit a ProjectScan, once it has
  // processed all the columns of its child scan.
  std::map<int /* column_id */, std::string> pending_columns_;

  // Holds sql text for node representing an analytic function.
  class AnalyticFunctionInfo;

  // Sql for an analytic function is populated in two places.
  // AnalyticFunctionCall fills in the function-specific parts (i.e. function
  // call, window frame) and AnalyticFunctionGroup fills in the group-specific
  // parts (i.e. partition by, order by).
  //
  // <pending_analytic_function_> stores the return value (in
  // AnalyticFunctionInfo) for each Visit call to ResolvedAnalyticFunctionCall
  // which is then popped/used by the caller of Accept,
  // i.e. ResolvedAnalyticFunctionGroup.
  //
  // NOTE: We cannot use QueryFragment for this purpose. As in the sql of an
  // analytic function, the function-specific and group-specific parts don't go
  // in a left-right order where we could simply append/prepend the sql built
  // from a child node (i.e. AnalyticFunctionCall) to the sql built from a
  // parent node (i.e. AnalyticFunctionGroup).
  std::unique_ptr<AnalyticFunctionInfo> pending_analytic_function_;

  // Stores the WithScan corresponding to the with-query-name. Used in
  // WithRefScan to extract the column aliases of the relevant WithScan.
  std::map<std::string /* with_query_name */,
           const ResolvedScan* /* with_subquery */>
      with_query_name_to_scan_;

  // A stack of stacks. An element of an inner stack corresponds to a
  // ResolvedUpdateItem node that is currently being processed, and stores the
  // target SQL for that node and the array offset SQL (empty if not applicable)
  // of the ResolvedUpdateArrayItem child currently being processed. We start a
  // new stack just before we process a ResolvedUpdateItem node from a
  // ResolvedUpdateStmt (as opposed to a ResolvedUpdateArrayItem).
  //
  // Stacks are added/removed in GetUpdateItemListSQL. Target SQL is set in
  // VisitResolvedUpdateItem. Offset SQL is set in
  // VisitResolvedUpdateArrayItem. An inner stack is used to construct a target
  // in VisitResolvedUpdateItem when there are no ResolvedUpdateArrayItem
  // children.
  std::deque<std::deque<
      std::pair<std::string /* target_sql */, std::string /* offset_sql */>>>
      update_item_targets_and_offsets_;

  // A stack of dml target paths kept to parallel the nesting of dml statements.
  // Populated in VisitResolvedUpdateItem and used in
  // VisitResolved<DMLType>Statement to refer the corresponding target path.
  std::deque<
      std::pair<std::string /* target_path */, std::string /* target_alias */>>
      nested_dml_targets_;

  // All column names referenced in the query.
  absl::flat_hash_set<std::string> col_ref_names_;

  std::string sql_;

  // Options for building SQL.
  SQLBuilderOptions options_;

  // True if we are unparsing the LHS of a SET statement.
  bool in_set_lhs_ = false;

  struct RecursiveQueryInfo {
    // Text to be inserted into the generated query to refer to the recursive
    // table being referenced. If backticks are needed, this should already be
    // present in the query name.
    std::string query_name;

    // The recursive scan being referenced.
    const ResolvedRecursiveScan* scan;
  };

  // Stack of names of recursive queries being defined. Any
  // ResolvedRecursiveRefScan node must refer to the query at the top of the
  // stack.
  std::stack<RecursiveQueryInfo> recursive_query_info_;

 private:
  // Helper function to perform operation on value table's column for
  // ResolvedTableScan.
  virtual absl::Status AddValueTableAliasForVisitResolvedTableScan(
      absl::string_view table_alias, const ResolvedColumn& column,
      std::vector<std::pair<std::string, std::string>>* select_list);

  // Returns a ZetaSQL identifier literal for a table's name.
  // The output will be quoted (with backticks) and escaped if necessary.
  virtual std::string TableToIdentifierLiteral(const Table* table);

  // Converts a table name string to a ZetaSQL identifier literal.
  // The output will be quoted (with backticks) and escaped if necessary.
  virtual std::string TableNameToIdentifierLiteral(
      absl::string_view table_name);

  // Returns the name alias for a table in VisitResolvedTableScan and appends to
  // the <from> string if necessary.
  virtual std::string GetTableAliasForVisitResolvedTableScan(
      const ResolvedTableScan& node, std::string* from);

  // Returns a new unique alias name. The default implementation generates the
  // name as "a_<id>".
  virtual std::string GenerateUniqueAliasName();
};

}  // namespace zetasql

#endif  // ZETASQL_RESOLVED_AST_SQL_BUILDER_H_
