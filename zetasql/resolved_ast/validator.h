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

#ifndef ZETASQL_RESOLVED_AST_VALIDATOR_H_
#define ZETASQL_RESOLVED_AST_VALIDATOR_H_

#include <functional>
#include <memory>
#include <set>
#include <vector>

#include "zetasql/public/language_options.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/container/flat_hash_set.h"
#include "zetasql/base/status.h"

namespace zetasql {

// Used to validate generated Resolved AST structures.
//  * verifies that any column reference  within the resolved tree should be
//    either from the column_list of one of the child nodes of the parent scan
//    or columns added in this node.
class Validator {
 public:
  Validator();
  explicit Validator(const LanguageOptions& language_options);
  Validator(const Validator&) = delete;
  Validator& operator=(const Validator&) = delete;
  ~Validator();

  zetasql_base::Status ValidateResolvedStatement(const ResolvedStatement* statement);

  zetasql_base::Status ValidateStandaloneResolvedExpr(const ResolvedExpr* expr) const;

 private:
  const LanguageOptions language_options_;

  // Statements.
  zetasql_base::Status ValidateResolvedQueryStmt(const ResolvedQueryStmt* query) const;
  zetasql_base::Status ValidateResolvedCreateDatabaseStmt(
      const ResolvedCreateDatabaseStmt* stmt) const;
  zetasql_base::Status ValidateResolvedIndexStmt(
      const ResolvedCreateIndexStmt* stmt) const;
  zetasql_base::Status ValidateResolvedCreateModelStmt(
      const ResolvedCreateModelStmt* stmt) const;
  zetasql_base::Status ValidateResolvedCreateTableStmt(
      const ResolvedCreateTableStmt* stmt) const;
  zetasql_base::Status ValidateResolvedGeneratedColumnInfo(
      const ResolvedColumnDefinition* column_definition,
      const std::set<ResolvedColumn>& visible_columns) const;
  zetasql_base::Status ValidateResolvedCreateTableAsSelectStmt(
      const ResolvedCreateTableAsSelectStmt* stmt) const;
  zetasql_base::Status ValidateResolvedCreateViewStmt(
      const ResolvedCreateViewStmt* stmt) const;
  zetasql_base::Status ValidateResolvedCreateMaterializedViewStmt(
      const ResolvedCreateMaterializedViewStmt* stmt) const;
  zetasql_base::Status ValidateResolvedCreateExternalTableStmt(
      const ResolvedCreateExternalTableStmt* stmt) const;
  zetasql_base::Status ValidateResolvedCreateRowPolicyStmt(
      const ResolvedCreateRowPolicyStmt* stmt) const;
  zetasql_base::Status ValidateResolvedCreateConstantStmt(
      const ResolvedCreateConstantStmt* stmt);
  zetasql_base::Status ValidateResolvedCreateFunctionStmt(
      const ResolvedCreateFunctionStmt* stmt);
  zetasql_base::Status ValidateResolvedCreateTableFunctionStmt(
      const ResolvedCreateTableFunctionStmt* stmt);
  zetasql_base::Status ValidateResolvedCreateProcedureStmt(
      const ResolvedCreateProcedureStmt* stmt);
  zetasql_base::Status ValidateResolvedExportDataStmt(
      const ResolvedExportDataStmt* stmt) const;
  zetasql_base::Status ValidateResolvedCallStmt(const ResolvedCallStmt* stmt) const;
  zetasql_base::Status ValidateResolvedDefineTableStmt(
      const ResolvedDefineTableStmt* stmt) const;
  zetasql_base::Status ValidateResolvedDescribeStmt(
      const ResolvedDescribeStmt* stmt) const;
  zetasql_base::Status ValidateResolvedShowStmt(
      const ResolvedShowStmt* stmt) const;
  zetasql_base::Status ValidateResolvedBeginStmt(const ResolvedBeginStmt* stmt) const;
  zetasql_base::Status ValidateResolvedSetTransactionStmt(
      const ResolvedSetTransactionStmt* stmt) const;
  zetasql_base::Status ValidateResolvedCommitStmt(const ResolvedCommitStmt* stmt) const;
  zetasql_base::Status ValidateResolvedRollbackStmt(
      const ResolvedRollbackStmt* stmt) const;
  zetasql_base::Status ValidateResolvedStartBatchStmt(
      const ResolvedStartBatchStmt* stmt) const;
  zetasql_base::Status ValidateResolvedRunBatchStmt(
      const ResolvedRunBatchStmt* stmt) const;
  zetasql_base::Status ValidateResolvedAbortBatchStmt(
      const ResolvedAbortBatchStmt* stmt) const;
  zetasql_base::Status ValidateResolvedDropStmt(
      const ResolvedDropStmt* stmt) const;
  zetasql_base::Status ValidateResolvedDropMaterializedViewStmt(
      const ResolvedDropMaterializedViewStmt* stmt) const;
  zetasql_base::Status ValidateResolvedDropFunctionStmt(
      const ResolvedDropFunctionStmt* stmt) const;
  zetasql_base::Status ValidateResolvedDropRowPolicyStmt(
      const ResolvedDropRowPolicyStmt* stmt) const;
  zetasql_base::Status ValidateResolvedGrantStmt(
      const ResolvedGrantStmt* stmt) const;
  zetasql_base::Status ValidateResolvedRevokeStmt(
      const ResolvedRevokeStmt* stmt) const;
  zetasql_base::Status ValidateResolvedAlterRowPolicyStmt(
      const ResolvedAlterRowPolicyStmt* stmt) const;
  zetasql_base::Status ValidateResolvedAlterTableSetOptionsStmt(
      const ResolvedAlterTableSetOptionsStmt* stmt) const;
  zetasql_base::Status ValidateResolvedRenameStmt(
      const ResolvedRenameStmt* stmt) const;
  zetasql_base::Status ValidateResolvedImportStmt(const ResolvedImportStmt* stmt) const;
  zetasql_base::Status ValidateResolvedModuleStmt(const ResolvedModuleStmt* stmt) const;
  zetasql_base::Status ValidateResolvedAssertStmt(const ResolvedAssertStmt* stmt) const;

  // DML Statements, which can also be used as nested operations inside UPDATEs.
  // When used nested, they take a non-NULL <array_element_column> and
  // <outer_visible_columns>.
  zetasql_base::Status ValidateResolvedInsertStmt(
      const ResolvedInsertStmt* stmt,
      const std::set<ResolvedColumn>* outer_visible_columns = nullptr,
      const ResolvedColumn* array_element_column = nullptr) const;
  zetasql_base::Status ValidateResolvedDeleteStmt(
      const ResolvedDeleteStmt* stmt,
      const std::set<ResolvedColumn>* outer_visible_columns = nullptr,
      const ResolvedColumn* array_element_column = nullptr) const;
  zetasql_base::Status ValidateResolvedUpdateStmt(
      const ResolvedUpdateStmt* stmt,
      const std::set<ResolvedColumn>* outer_visible_columns = nullptr,
      const ResolvedColumn* array_element_column = nullptr) const;

  // Can occur as a child of a ResolvedUpdateStmt or a
  // ResolvedUpdateArrayItem. In the latter case, <array_element_column> is
  // non-NULL and is in <target_visible_columns> but not
  // <offset_and_where_visible_columns>.
  zetasql_base::Status ValidateResolvedUpdateItem(
      const ResolvedUpdateItem* item, bool allow_nested_statements,
      const ResolvedColumn* array_element_column,
      const std::set<ResolvedColumn>& target_visible_columns,
      const std::set<ResolvedColumn>& offset_and_where_visible_columns) const;

  // <element_column> is not in <target_visible_columns> or
  // <offset_and_where_visible_columns>
  zetasql_base::Status ValidateResolvedUpdateArrayItem(
      const ResolvedUpdateArrayItem* item, const ResolvedColumn& element_column,
      const std::set<ResolvedColumn>& target_visible_columns,
      const std::set<ResolvedColumn>& offset_and_where_visible_columns) const;

  // Merge statement is not supported in nested-DML.
  zetasql_base::Status ValidateResolvedMergeStmt(const ResolvedMergeStmt* stmt) const;
  // The source_visible_columns and target_visible_columns are visible columns
  // from source and target tables.
  // The all_visible_columns is union of source and target columns. Passed as a
  // parameter to avoid re-computing every time.
  zetasql_base::Status ValidateResolvedMergeWhen(
      const ResolvedMergeWhen* merge_when,
      const std::set<ResolvedColumn>& all_visible_columns,
      const std::set<ResolvedColumn>& source_visible_columns,
      const std::set<ResolvedColumn>& target_visible_columns) const;

  zetasql_base::Status ValidateResolvedTruncateStmt(
      const ResolvedTruncateStmt* stmt) const;

  // Templated common code for all DML statements.
  template <class STMT>
  zetasql_base::Status ValidateResolvedDMLStmt(
      const STMT* stmt,
      const ResolvedColumn* array_element_column,
      std::set<ResolvedColumn>* visible_columns) const;

  // Validation calls for various subtypes of ResolvedScan operations.
  zetasql_base::Status ValidateResolvedScan(
      const ResolvedScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedAggregateScanBase(
      const ResolvedAggregateScanBase* scan,
      const std::set<ResolvedColumn>& visible_parameters,
      std::set<ResolvedColumn>* input_scan_visible_columns) const;
  zetasql_base::Status ValidateResolvedAggregateScan(
      const ResolvedAggregateScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedTableScan(
      const ResolvedTableScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedJoinScan(
      const ResolvedJoinScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedArrayScan(
      const ResolvedArrayScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedFilterScan(
      const ResolvedFilterScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedSetOperationScan(
      const ResolvedSetOperationScan* set_op_scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedProjectScan(
      const ResolvedProjectScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedTVFScan(
      const ResolvedTVFScan* resolved_tvf_scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedRelationArgumentScan(
      const ResolvedRelationArgumentScan* arg_ref,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedOrderByScan(
      const ResolvedOrderByScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedLimitOffsetScan(
      const ResolvedLimitOffsetScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedAnalyticScan(
      const ResolvedAnalyticScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  zetasql_base::Status ValidateResolvedSampleScan(
      const ResolvedSampleScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;

  // For a scan with is_ordered=true, validate that this scan can legally
  // produce ordered output.
  zetasql_base::Status ValidateResolvedScanOrdering(const ResolvedScan* scan) const;

  zetasql_base::Status ValidateResolvedWithScan(
      const ResolvedWithScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;

  zetasql_base::Status ValidateResolvedAggregateComputedColumn(
      const ResolvedComputedColumn* computed_column,
      const std::set<ResolvedColumn>& input_scan_visible_columns,
      const std::set<ResolvedColumn>& visible_parameters) const;

  // Verifies that all the internal references in <expr> are present in
  // the <visible_columns> scope.
  zetasql_base::Status ValidateResolvedExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedExpr* expr) const;

  zetasql_base::Status ValidateResolvedGetProtoFieldExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedGetProtoField* get_proto_field) const;

  zetasql_base::Status ValidateResolvedReplaceField(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedReplaceField* replace_field) const;

  zetasql_base::Status ValidateResolvedSubqueryExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedSubqueryExpr* resolved_subquery_expr) const;

  // Verifies that all the internal references in <expr_list> are present
  // in the <visible_columns> scope.
  zetasql_base::Status ValidateResolvedExprList(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const std::vector<std::unique_ptr<const ResolvedExpr>>& expr_list) const;

  zetasql_base::Status ValidateResolvedComputedColumn(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedComputedColumn* computed_column) const;

  zetasql_base::Status ValidateResolvedComputedColumnList(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
          computed_column_list) const;

  zetasql_base::Status ValidateResolvedOutputColumn(
      const std::set<ResolvedColumn>& visible_columns,
      const ResolvedOutputColumn* output_column) const;

  zetasql_base::Status ValidateResolvedOutputColumnList(
      const std::vector<ResolvedColumn>& visible_columns,
      const std::vector<std::unique_ptr<const ResolvedOutputColumn>>&
          output_column_list,
      bool is_value_table) const;
  zetasql_base::Status ValidateResolvedCreateTableStmtBase(
      const ResolvedCreateTableStmtBase* stmt) const;

  zetasql_base::Status ValidateResolvedCast(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedCast* resolved_cast) const;

  zetasql_base::Status ValidateResolvedConstant(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedConstant* resolved_constant) const;

  zetasql_base::Status ValidateResolvedFunctionCallBase(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedFunctionCallBase* resolved_function_call) const;

  zetasql_base::Status ValidateHintList(
      const std::vector<std::unique_ptr<const ResolvedOption>>& hint_list)
      const;

  zetasql_base::Status ValidateColumnAnnotations(
      const ResolvedColumnAnnotations* annotations) const;

  // Verifies that only one of the parameter name and position is set.
  zetasql_base::Status ValidateResolvedParameter(
      const ResolvedParameter* resolved_param) const;

  zetasql_base::Status ValidateResolvedTVFArgument(
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedTVFArgument* resolved_tvf_arg) const;

  // Validates TVF relation argument schema against the required input schema
  // in function signature.
  zetasql_base::Status ValidateRelationSchemaInResolvedTVFArgument(
      const TVFRelation& required_input_schema,
      const TVFRelation& input_relation,
      const ResolvedTVFArgument* resolved_tvf_arg) const;

  zetasql_base::Status CheckColumnIsPresentInColumnSet(
      const ResolvedColumn& column,
      const std::set<ResolvedColumn>& visible_columns) const;

  // Verifies that the scan column list only contains column from the visible
  // set.
  zetasql_base::Status CheckColumnList(const ResolvedScan* scan,
                               const std::set<ResolvedColumn>& visible_columns)
      const;

  zetasql_base::Status AddColumnList(const ResolvedColumnList& column_list,
                             std::set<ResolvedColumn>* visible_columns) const;
  zetasql_base::Status AddColumnFromComputedColumn(
      const ResolvedComputedColumn* computed_column,
      std::set<ResolvedColumn>* visible_columns) const;
  zetasql_base::Status AddColumnsFromComputedColumnList(
      const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
          computed_column_list,
      std::set<ResolvedColumn>* visible_columns) const;

  zetasql_base::Status ValidateResolvedAnalyticFunctionGroup(
      const ResolvedAnalyticFunctionGroup* group,
      const std::set<ResolvedColumn>& input_visible_columns,
      const std::set<ResolvedColumn>& visible_parameters) const;

  zetasql_base::Status ValidateResolvedWindowFrame(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedWindowOrdering* window_ordering,
      const ResolvedWindowFrame* window_frame) const;

  zetasql_base::Status ValidateResolvedWindowFrameExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedWindowOrdering* window_ordering,
      const ResolvedWindowFrame::FrameUnit& frame_unit,
      const ResolvedWindowFrameExpr* window_frame_expr) const;

  zetasql_base::Status ValidateResolvedWindowFrameExprType(
      const ResolvedWindowFrame::FrameUnit& frame_unit,
      const ResolvedExpr* window_ordering_expr,
      const ResolvedExpr& window_frame_expr) const;

  zetasql_base::Status ValidateResolvedAlterObjectStmt(
      const ResolvedAlterObjectStmt* stmt) const;

  zetasql_base::Status ValidateResolvedAlterAction(
      const ResolvedAlterAction* action) const;

  // Check that <expr> contains only ColumnRefs, GetProtoField and
  // GetStructField expressions. Sets 'ref' to point to the leaf
  // ResolvedColumnRef.
  zetasql_base::Status CheckExprIsPath(const ResolvedExpr* expr,
                               const ResolvedColumnRef** ref) const;

  // Validates whether <expr> is a literal or a parameter. In either case, it
  // should be of type int64_t.
  zetasql_base::Status ValidateArgumentIsInt64Constant(const ResolvedExpr* expr) const;

  // Which ArgumentKinds are allowed in the current expression.
  // Set using scoped VarSetters.
  typedef absl::flat_hash_set<ResolvedArgumentDefEnums::ArgumentKind>
      ArgumentKindSet;
  ArgumentKindSet allowed_argument_kinds_;

  // This points to the current CREATE TABLE FUNCTION statement being validated,
  // or null if no such statement is currently being validated.
  const ResolvedCreateTableFunctionStmt* current_create_table_function_stmt_ =
      nullptr;
};

}  // namespace zetasql

#endif  // ZETASQL_RESOLVED_AST_VALIDATOR_H_
