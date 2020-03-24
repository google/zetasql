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

  absl::Status ValidateResolvedStatement(const ResolvedStatement* statement);

  absl::Status ValidateStandaloneResolvedExpr(const ResolvedExpr* expr) const;

 private:
  const LanguageOptions language_options_;

  // Statements.
  absl::Status ValidateResolvedQueryStmt(const ResolvedQueryStmt* query) const;
  absl::Status ValidateResolvedCreateDatabaseStmt(
      const ResolvedCreateDatabaseStmt* stmt) const;
  absl::Status ValidateResolvedIndexStmt(
      const ResolvedCreateIndexStmt* stmt) const;
  absl::Status ValidateResolvedCreateModelStmt(
      const ResolvedCreateModelStmt* stmt) const;
  absl::Status ValidateResolvedCreateTableStmt(
      const ResolvedCreateTableStmt* stmt) const;
  absl::Status ValidateResolvedGeneratedColumnInfo(
      const ResolvedColumnDefinition* column_definition,
      const std::set<ResolvedColumn>& visible_columns) const;
  absl::Status ValidateResolvedCreateTableAsSelectStmt(
      const ResolvedCreateTableAsSelectStmt* stmt) const;
  absl::Status ValidateResolvedCreateViewStmt(
      const ResolvedCreateViewStmt* stmt) const;
  absl::Status ValidateResolvedCreateMaterializedViewStmt(
      const ResolvedCreateMaterializedViewStmt* stmt) const;
  absl::Status ValidateResolvedCreateExternalTableStmt(
      const ResolvedCreateExternalTableStmt* stmt) const;
  absl::Status ValidateResolvedCreateRowAccessPolicyStmt(
      const ResolvedCreateRowAccessPolicyStmt* stmt) const;
  absl::Status ValidateResolvedCreateConstantStmt(
      const ResolvedCreateConstantStmt* stmt);
  absl::Status ValidateResolvedCreateFunctionStmt(
      const ResolvedCreateFunctionStmt* stmt);
  absl::Status ValidateResolvedCreateTableFunctionStmt(
      const ResolvedCreateTableFunctionStmt* stmt);
  absl::Status ValidateResolvedCreateProcedureStmt(
      const ResolvedCreateProcedureStmt* stmt);
  absl::Status ValidateResolvedExportDataStmt(
      const ResolvedExportDataStmt* stmt) const;
  absl::Status ValidateResolvedCallStmt(const ResolvedCallStmt* stmt) const;
  absl::Status ValidateResolvedDefineTableStmt(
      const ResolvedDefineTableStmt* stmt) const;
  absl::Status ValidateResolvedDescribeStmt(
      const ResolvedDescribeStmt* stmt) const;
  absl::Status ValidateResolvedShowStmt(
      const ResolvedShowStmt* stmt) const;
  absl::Status ValidateResolvedBeginStmt(const ResolvedBeginStmt* stmt) const;
  absl::Status ValidateResolvedSetTransactionStmt(
      const ResolvedSetTransactionStmt* stmt) const;
  absl::Status ValidateResolvedCommitStmt(const ResolvedCommitStmt* stmt) const;
  absl::Status ValidateResolvedRollbackStmt(
      const ResolvedRollbackStmt* stmt) const;
  absl::Status ValidateResolvedStartBatchStmt(
      const ResolvedStartBatchStmt* stmt) const;
  absl::Status ValidateResolvedRunBatchStmt(
      const ResolvedRunBatchStmt* stmt) const;
  absl::Status ValidateResolvedAbortBatchStmt(
      const ResolvedAbortBatchStmt* stmt) const;
  absl::Status ValidateResolvedDropStmt(
      const ResolvedDropStmt* stmt) const;
  absl::Status ValidateResolvedDropMaterializedViewStmt(
      const ResolvedDropMaterializedViewStmt* stmt) const;
  absl::Status ValidateResolvedDropFunctionStmt(
      const ResolvedDropFunctionStmt* stmt) const;
  absl::Status ValidateResolvedDropRowAccessPolicyStmt(
      const ResolvedDropRowAccessPolicyStmt* stmt) const;
  absl::Status ValidateResolvedGrantStmt(
      const ResolvedGrantStmt* stmt) const;
  absl::Status ValidateResolvedRevokeStmt(
      const ResolvedRevokeStmt* stmt) const;
  absl::Status ValidateResolvedGrantToAction(
      const ResolvedGrantToAction* stmt) const;
  absl::Status ValidateResolvedFilterUsingAction(
      const ResolvedFilterUsingAction* stmt) const;
  absl::Status ValidateResolvedRevokeFromAction(
      const ResolvedRevokeFromAction* stmt) const;
  absl::Status ValidateResolvedRenameToAction(
      const ResolvedRenameToAction* stmt) const;
  absl::Status ValidateResolvedRowAccessPolicyAlterAction(
      const ResolvedAlterAction* action,
      const std::set<ResolvedColumn>& visible_columns) const;
  absl::Status ValidateResolvedAlterRowAccessPolicyStmt(
      const ResolvedAlterRowAccessPolicyStmt* stmt) const;
  absl::Status ValidateResolvedAlterTableSetOptionsStmt(
      const ResolvedAlterTableSetOptionsStmt* stmt) const;
  absl::Status ValidateResolvedRenameStmt(
      const ResolvedRenameStmt* stmt) const;
  absl::Status ValidateResolvedImportStmt(const ResolvedImportStmt* stmt) const;
  absl::Status ValidateResolvedModuleStmt(const ResolvedModuleStmt* stmt) const;
  absl::Status ValidateResolvedAssertStmt(const ResolvedAssertStmt* stmt) const;
  absl::Status ValidateResolvedAssignmentStmt(
      const ResolvedAssignmentStmt* stmt) const;

  // DML Statements, which can also be used as nested operations inside UPDATEs.
  // When used nested, they take a non-NULL <array_element_column> and
  // <outer_visible_columns>.
  absl::Status ValidateResolvedInsertStmt(
      const ResolvedInsertStmt* stmt,
      const std::set<ResolvedColumn>* outer_visible_columns = nullptr,
      const ResolvedColumn* array_element_column = nullptr) const;
  absl::Status ValidateResolvedDeleteStmt(
      const ResolvedDeleteStmt* stmt,
      const std::set<ResolvedColumn>* outer_visible_columns = nullptr,
      const ResolvedColumn* array_element_column = nullptr) const;
  absl::Status ValidateResolvedUpdateStmt(
      const ResolvedUpdateStmt* stmt,
      const std::set<ResolvedColumn>* outer_visible_columns = nullptr,
      const ResolvedColumn* array_element_column = nullptr) const;

  // Can occur as a child of a ResolvedUpdateStmt or a
  // ResolvedUpdateArrayItem. In the latter case, <array_element_column> is
  // non-NULL and is in <target_visible_columns> but not
  // <offset_and_where_visible_columns>.
  absl::Status ValidateResolvedUpdateItem(
      const ResolvedUpdateItem* item, bool allow_nested_statements,
      const ResolvedColumn* array_element_column,
      const std::set<ResolvedColumn>& target_visible_columns,
      const std::set<ResolvedColumn>& offset_and_where_visible_columns) const;

  // <element_column> is not in <target_visible_columns> or
  // <offset_and_where_visible_columns>
  absl::Status ValidateResolvedUpdateArrayItem(
      const ResolvedUpdateArrayItem* item, const ResolvedColumn& element_column,
      const std::set<ResolvedColumn>& target_visible_columns,
      const std::set<ResolvedColumn>& offset_and_where_visible_columns) const;

  // Merge statement is not supported in nested-DML.
  absl::Status ValidateResolvedMergeStmt(const ResolvedMergeStmt* stmt) const;
  // The source_visible_columns and target_visible_columns are visible columns
  // from source and target tables.
  // The all_visible_columns is union of source and target columns. Passed as a
  // parameter to avoid re-computing every time.
  absl::Status ValidateResolvedMergeWhen(
      const ResolvedMergeWhen* merge_when,
      const std::set<ResolvedColumn>& all_visible_columns,
      const std::set<ResolvedColumn>& source_visible_columns,
      const std::set<ResolvedColumn>& target_visible_columns) const;

  absl::Status ValidateResolvedTruncateStmt(
      const ResolvedTruncateStmt* stmt) const;

  // Templated common code for all DML statements.
  template <class STMT>
  absl::Status ValidateResolvedDMLStmt(
      const STMT* stmt,
      const ResolvedColumn* array_element_column,
      std::set<ResolvedColumn>* visible_columns) const;

  // Validation calls for various subtypes of ResolvedScan operations.
  absl::Status ValidateResolvedScan(
      const ResolvedScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedAggregateScanBase(
      const ResolvedAggregateScanBase* scan,
      const std::set<ResolvedColumn>& visible_parameters,
      std::set<ResolvedColumn>* input_scan_visible_columns) const;
  absl::Status ValidateResolvedAggregateScan(
      const ResolvedAggregateScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedTableScan(
      const ResolvedTableScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedJoinScan(
      const ResolvedJoinScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedArrayScan(
      const ResolvedArrayScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedFilterScan(
      const ResolvedFilterScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedSetOperationScan(
      const ResolvedSetOperationScan* set_op_scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedProjectScan(
      const ResolvedProjectScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedTVFScan(
      const ResolvedTVFScan* resolved_tvf_scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedRelationArgumentScan(
      const ResolvedRelationArgumentScan* arg_ref,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedOrderByScan(
      const ResolvedOrderByScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedLimitOffsetScan(
      const ResolvedLimitOffsetScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedAnalyticScan(
      const ResolvedAnalyticScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;
  absl::Status ValidateResolvedSampleScan(
      const ResolvedSampleScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;

  // For a scan with is_ordered=true, validate that this scan can legally
  // produce ordered output.
  absl::Status ValidateResolvedScanOrdering(const ResolvedScan* scan) const;

  absl::Status ValidateResolvedWithScan(
      const ResolvedWithScan* scan,
      const std::set<ResolvedColumn>& visible_parameters) const;

  absl::Status ValidateResolvedAggregateComputedColumn(
      const ResolvedComputedColumn* computed_column,
      const std::set<ResolvedColumn>& input_scan_visible_columns,
      const std::set<ResolvedColumn>& visible_parameters) const;

  // Verifies that all the internal references in <expr> are present in
  // the <visible_columns> scope.
  absl::Status ValidateResolvedExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedExpr* expr) const;

  absl::Status ValidateResolvedGetProtoFieldExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedGetProtoField* get_proto_field) const;

  absl::Status ValidateResolvedReplaceField(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedReplaceField* replace_field) const;

  absl::Status ValidateResolvedSubqueryExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedSubqueryExpr* resolved_subquery_expr) const;

  // Verifies that all the internal references in <expr_list> are present
  // in the <visible_columns> scope.
  absl::Status ValidateResolvedExprList(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const std::vector<std::unique_ptr<const ResolvedExpr>>& expr_list) const;

  absl::Status ValidateResolvedComputedColumn(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedComputedColumn* computed_column) const;

  absl::Status ValidateResolvedComputedColumnList(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
          computed_column_list) const;

  absl::Status ValidateResolvedOutputColumn(
      const std::set<ResolvedColumn>& visible_columns,
      const ResolvedOutputColumn* output_column) const;

  absl::Status ValidateResolvedOutputColumnList(
      const std::vector<ResolvedColumn>& visible_columns,
      const std::vector<std::unique_ptr<const ResolvedOutputColumn>>&
          output_column_list,
      bool is_value_table) const;
  absl::Status ValidateResolvedCreateTableStmtBase(
      const ResolvedCreateTableStmtBase* stmt) const;

  absl::Status ValidateResolvedCast(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedCast* resolved_cast) const;

  absl::Status ValidateResolvedConstant(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedConstant* resolved_constant) const;

  absl::Status ValidateResolvedFunctionCallBase(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedFunctionCallBase* resolved_function_call) const;

  absl::Status ValidateHintList(
      const std::vector<std::unique_ptr<const ResolvedOption>>& hint_list)
      const;

  absl::Status ValidateColumnAnnotations(
      const ResolvedColumnAnnotations* annotations) const;

  // Verifies that only one of the parameter name and position is set.
  absl::Status ValidateResolvedParameter(
      const ResolvedParameter* resolved_param) const;

  absl::Status ValidateResolvedTVFArgument(
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedTVFArgument* resolved_tvf_arg) const;

  // Validates TVF relation argument schema against the required input schema
  // in function signature.
  absl::Status ValidateRelationSchemaInResolvedTVFArgument(
      const TVFRelation& required_input_schema,
      const TVFRelation& input_relation,
      const ResolvedTVFArgument* resolved_tvf_arg) const;

  absl::Status CheckColumnIsPresentInColumnSet(
      const ResolvedColumn& column,
      const std::set<ResolvedColumn>& visible_columns) const;

  // Verifies that the scan column list only contains column from the visible
  // set.
  absl::Status CheckColumnList(const ResolvedScan* scan,
                               const std::set<ResolvedColumn>& visible_columns)
      const;

  absl::Status AddColumnList(const ResolvedColumnList& column_list,
                             std::set<ResolvedColumn>* visible_columns) const;
  absl::Status AddColumnFromComputedColumn(
      const ResolvedComputedColumn* computed_column,
      std::set<ResolvedColumn>* visible_columns) const;
  absl::Status AddColumnsFromComputedColumnList(
      const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
          computed_column_list,
      std::set<ResolvedColumn>* visible_columns) const;

  absl::Status ValidateResolvedAnalyticFunctionGroup(
      const ResolvedAnalyticFunctionGroup* group,
      const std::set<ResolvedColumn>& input_visible_columns,
      const std::set<ResolvedColumn>& visible_parameters) const;

  absl::Status ValidateResolvedWindowFrame(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedWindowOrdering* window_ordering,
      const ResolvedWindowFrame* window_frame) const;

  absl::Status ValidateResolvedWindowFrameExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedWindowOrdering* window_ordering,
      const ResolvedWindowFrame::FrameUnit& frame_unit,
      const ResolvedWindowFrameExpr* window_frame_expr) const;

  absl::Status ValidateResolvedWindowFrameExprType(
      const ResolvedWindowFrame::FrameUnit& frame_unit,
      const ResolvedExpr* window_ordering_expr,
      const ResolvedExpr& window_frame_expr) const;

  absl::Status ValidateResolvedAlterObjectStmt(
      const ResolvedAlterObjectStmt* stmt) const;

  absl::Status ValidateResolvedAlterAction(
      const ResolvedAlterAction* action) const;

  absl::Status ValidateResolvedExecuteImmediateStmt(
      const ResolvedExecuteImmediateStmt* stmt) const;

  // Check that <expr> contains only ColumnRefs, GetProtoField and
  // GetStructField expressions. Sets 'ref' to point to the leaf
  // ResolvedColumnRef.
  absl::Status CheckExprIsPath(const ResolvedExpr* expr,
                               const ResolvedColumnRef** ref) const;

  // Validates whether <expr> is a literal or a parameter. In either case, it
  // should be of type int64_t.
  absl::Status ValidateArgumentIsInt64Constant(const ResolvedExpr* expr) const;

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
