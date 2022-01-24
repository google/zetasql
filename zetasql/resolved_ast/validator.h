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
#include "zetasql/base/status_builder.h"
namespace zetasql {

// Options to disable certain validations. Options are used to retroactively
// add validations of invariants even if some client code still needs to be
// cleaned up. A non-default ValidatorOptions in client code signals a cleanup
// is needed to resolve broken invariants.
struct ValidatorOptions {
  // When set to false disables checking that all parameter columns of a
  // ResolvedSubquery are referenced somewhere in that subquery.
  bool validate_no_unreferenced_subquery_params = true;
};

// Used to validate generated Resolved AST structures.
//  * verifies that any column reference  within the resolved tree should be
//    either from the column_list of one of the child nodes of the parent scan
//    or columns added in this node.
class Validator {
 public:
  Validator();
  explicit Validator(const LanguageOptions& language_options,
                     ValidatorOptions validator_options = {});
  Validator(const Validator&) = delete;
  Validator& operator=(const Validator&) = delete;
  ~Validator();

  absl::Status ValidateResolvedStatement(const ResolvedStatement* statement);

  absl::Status ValidateStandaloneResolvedExpr(const ResolvedExpr* expr);

 private:
  // Statements.
  absl::Status ValidateResolvedStatementInternal(
      const ResolvedStatement* statement);
  absl::Status ValidateResolvedQueryStmt(const ResolvedQueryStmt* query);
  absl::Status ValidateResolvedCreateDatabaseStmt(
      const ResolvedCreateDatabaseStmt* stmt);
  absl::Status ValidateResolvedIndexStmt(const ResolvedCreateIndexStmt* stmt);
  absl::Status ValidateResolvedCreateModelStmt(
      const ResolvedCreateModelStmt* stmt);
  absl::Status ValidateResolvedCreateSnapshotTableStmt(
      const ResolvedCreateSnapshotTableStmt* stmt);
  absl::Status ValidateResolvedCreateTableStmt(
      const ResolvedCreateTableStmt* stmt);
  absl::Status ValidateResolvedCloneDataSource(const ResolvedScan* source,
                                               const Table* target = nullptr);
  absl::Status ValidateSingleCloneDataSource(const ResolvedScan* source,
                                             const Table* target);
  absl::Status ValidateResolvedGeneratedColumnInfo(
      const ResolvedColumnDefinition* column_definition,
      const std::set<ResolvedColumn>& visible_columns);
  absl::Status ValidateResolvedCreateTableAsSelectStmt(
      const ResolvedCreateTableAsSelectStmt* stmt);
  absl::Status ValidateResolvedCreateViewStmt(
      const ResolvedCreateViewStmt* stmt);
  absl::Status ValidateResolvedCreateMaterializedViewStmt(
      const ResolvedCreateMaterializedViewStmt* stmt);
  absl::Status ValidateResolvedCreateExternalTableStmt(
      const ResolvedCreateExternalTableStmt* stmt);
  absl::Status ValidateResolvedCreatePrivilegeRestrictionStmt(
      const ResolvedCreatePrivilegeRestrictionStmt* stmt);
  absl::Status ValidateResolvedCreateRowAccessPolicyStmt(
      const ResolvedCreateRowAccessPolicyStmt* stmt);
  absl::Status ValidateResolvedCreateConstantStmt(
      const ResolvedCreateConstantStmt* stmt);
  absl::Status ValidateResolvedCreateFunctionStmt(
      const ResolvedCreateFunctionStmt* stmt);
  absl::Status ValidateResolvedCreateTableFunctionStmt(
      const ResolvedCreateTableFunctionStmt* stmt);
  absl::Status ValidateResolvedCreateProcedureStmt(
      const ResolvedCreateProcedureStmt* stmt);
  absl::Status ValidateResolvedCreateEntityStmt(
      const ResolvedCreateEntityStmt* stmt);
  absl::Status ValidateResolvedAlterEntityStmt(
      const ResolvedAlterEntityStmt* stmt);
  absl::Status ValidateResolvedCloneDataStmt(
      const ResolvedCloneDataStmt* stmt);
  absl::Status ValidateResolvedExportDataStmt(
      const ResolvedExportDataStmt* stmt);
  absl::Status ValidateResolvedExportModelStmt(
      const ResolvedExportModelStmt* stmt);
  absl::Status ValidateResolvedCallStmt(const ResolvedCallStmt* stmt);
  absl::Status ValidateResolvedDefineTableStmt(
      const ResolvedDefineTableStmt* stmt);
  absl::Status ValidateResolvedDescribeStmt(const ResolvedDescribeStmt* stmt);
  absl::Status ValidateResolvedShowStmt(const ResolvedShowStmt* stmt);
  absl::Status ValidateResolvedBeginStmt(const ResolvedBeginStmt* stmt);
  absl::Status ValidateResolvedSetTransactionStmt(
      const ResolvedSetTransactionStmt* stmt);
  absl::Status ValidateResolvedCommitStmt(const ResolvedCommitStmt* stmt);
  absl::Status ValidateResolvedRollbackStmt(const ResolvedRollbackStmt* stmt);
  absl::Status ValidateResolvedStartBatchStmt(
      const ResolvedStartBatchStmt* stmt);
  absl::Status ValidateResolvedRunBatchStmt(const ResolvedRunBatchStmt* stmt);
  absl::Status ValidateResolvedAbortBatchStmt(
      const ResolvedAbortBatchStmt* stmt);
  absl::Status ValidateResolvedDropStmt(const ResolvedDropStmt* stmt);
  absl::Status ValidateResolvedDropMaterializedViewStmt(
      const ResolvedDropMaterializedViewStmt* stmt);
  absl::Status ValidateResolvedDropFunctionStmt(
      const ResolvedDropFunctionStmt* stmt);
  absl::Status ValidateResolvedDropTableFunctionStmt(
      const ResolvedDropTableFunctionStmt* stmt);
  absl::Status ValidateResolvedDropPrivilegeRestrictionStmt(
      const ResolvedDropPrivilegeRestrictionStmt* stmt);
  absl::Status ValidateResolvedDropRowAccessPolicyStmt(
      const ResolvedDropRowAccessPolicyStmt* stmt);
  absl::Status ValidateResolvedDropSnapshotTableStmt(
      const ResolvedDropSnapshotTableStmt* stmt);
  absl::Status ValidateResolvedDropSearchIndexStmt(
      const ResolvedDropSearchIndexStmt* stmt);
  absl::Status ValidateResolvedGrantStmt(const ResolvedGrantStmt* stmt);
  absl::Status ValidateResolvedRevokeStmt(const ResolvedRevokeStmt* stmt);
  absl::Status ValidateResolvedGrantToAction(const ResolvedGrantToAction* stmt);
  absl::Status ValidateResolvedFilterUsingAction(
      const ResolvedFilterUsingAction* stmt);
  absl::Status ValidateResolvedRevokeFromAction(
      const ResolvedRevokeFromAction* stmt);
  absl::Status ValidateResolvedRenameToAction(
      const ResolvedRenameToAction* stmt);
  absl::Status ValidateResolvedRowAccessPolicyAlterAction(
      const ResolvedAlterAction* action,
      const std::set<ResolvedColumn>& visible_columns);
  absl::Status ValidateResolvedAlterPrivilegeRestrictionStmt(
      const ResolvedAlterPrivilegeRestrictionStmt* stmt);
  absl::Status ValidateResolvedAlterRowAccessPolicyStmt(
      const ResolvedAlterRowAccessPolicyStmt* stmt);
  absl::Status ValidateResolvedAlterAllRowAccessPoliciesStmt(
      const ResolvedAlterAllRowAccessPoliciesStmt* stmt);
  absl::Status ValidateResolvedAlterSchemaStmt(
      const ResolvedAlterSchemaStmt* stmt);
  absl::Status ValidateResolvedAlterTableSetOptionsStmt(
      const ResolvedAlterTableSetOptionsStmt* stmt);
  absl::Status ValidateResolvedRenameStmt(const ResolvedRenameStmt* stmt);
  absl::Status ValidateResolvedImportStmt(const ResolvedImportStmt* stmt);
  absl::Status ValidateResolvedModuleStmt(const ResolvedModuleStmt* stmt);
  absl::Status ValidateResolvedAnalyzeStmt(const ResolvedAnalyzeStmt* stmt);
  absl::Status ValidateResolvedAssertStmt(const ResolvedAssertStmt* stmt);
  absl::Status ValidateResolvedAssignmentStmt(
      const ResolvedAssignmentStmt* stmt);

  // DML Statements, which can also be used as nested operations inside UPDATEs.
  // When used nested, they take a non-NULL <array_element_column> and
  // <outer_visible_columns>.
  absl::Status ValidateResolvedInsertStmt(
      const ResolvedInsertStmt* stmt,
      const std::set<ResolvedColumn>* outer_visible_columns = nullptr,
      const ResolvedColumn* array_element_column = nullptr);
  absl::Status ValidateResolvedDeleteStmt(
      const ResolvedDeleteStmt* stmt,
      const std::set<ResolvedColumn>* outer_visible_columns = nullptr,
      const ResolvedColumn* array_element_column = nullptr);
  absl::Status ValidateResolvedUpdateStmt(
      const ResolvedUpdateStmt* stmt,
      const std::set<ResolvedColumn>* outer_visible_columns = nullptr,
      const ResolvedColumn* array_element_column = nullptr);

  // Can occur as a child of a ResolvedUpdateStmt or a
  // ResolvedUpdateArrayItem. In the latter case, <array_element_column> is
  // non-NULL and is in <target_visible_columns> but not
  // <offset_and_where_visible_columns>.
  absl::Status ValidateResolvedUpdateItem(
      const ResolvedUpdateItem* item, bool allow_nested_statements,
      const ResolvedColumn* array_element_column,
      const std::set<ResolvedColumn>& target_visible_columns,
      const std::set<ResolvedColumn>& offset_and_where_visible_columns);

  // <element_column> is not in <target_visible_columns> or
  // <offset_and_where_visible_columns>
  absl::Status ValidateResolvedUpdateArrayItem(
      const ResolvedUpdateArrayItem* item, const ResolvedColumn& element_column,
      const std::set<ResolvedColumn>& target_visible_columns,
      const std::set<ResolvedColumn>& offset_and_where_visible_columns);

  // Merge statement is not supported in nested-DML.
  absl::Status ValidateResolvedMergeStmt(const ResolvedMergeStmt* stmt);
  // The source_visible_columns and target_visible_columns are visible columns
  // from source and target tables.
  // The all_visible_columns is union of source and target columns. Passed as a
  // parameter to avoid re-computing every time.
  absl::Status ValidateResolvedMergeWhen(
      const ResolvedMergeWhen* merge_when,
      const std::set<ResolvedColumn>& all_visible_columns,
      const std::set<ResolvedColumn>& source_visible_columns,
      const std::set<ResolvedColumn>& target_visible_columns);

  absl::Status ValidateResolvedTruncateStmt(const ResolvedTruncateStmt* stmt);

  // Templated common code for all DML statements.
  template <class STMT>
  absl::Status ValidateResolvedDMLStmt(
      const STMT* stmt, const ResolvedColumn* array_element_column,
      std::set<ResolvedColumn>* visible_columns);

  // Validation calls for various subtypes of ResolvedScan operations.
  // If we are validating a potentially-recursive query, <recursive_query_name>
  // specifies the query name. Otherwise, <recursive_query_name> is empty.
  absl::Status ValidateResolvedScan(
      const ResolvedScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedAggregateScanBase(
      const ResolvedAggregateScanBase* scan,
      const std::set<ResolvedColumn>& visible_parameters,
      std::set<ResolvedColumn>* input_scan_visible_columns);
  absl::Status ValidateResolvedAggregateScan(
      const ResolvedAggregateScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedAnonymizedAggregateScan(
      const ResolvedAnonymizedAggregateScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedTableScan(
      const ResolvedTableScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedJoinScan(
      const ResolvedJoinScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedArrayScan(
      const ResolvedArrayScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedFilterScan(
      const ResolvedFilterScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedSetOperationScan(
      const ResolvedSetOperationScan* set_op_scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedSetOperationItem(
      const ResolvedSetOperationItem* input_item,
      const ResolvedColumnList& output_column_list,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedProjectScan(
      const ResolvedProjectScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedTVFScan(
      const ResolvedTVFScan* resolved_tvf_scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedRelationArgumentScan(
      const ResolvedRelationArgumentScan* arg_ref,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedOrderByScan(
      const ResolvedOrderByScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedLimitOffsetScan(
      const ResolvedLimitOffsetScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedAnalyticScan(
      const ResolvedAnalyticScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);
  absl::Status ValidateResolvedSampleScan(
      const ResolvedSampleScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);

  // For a scan with is_ordered=true, validate that this scan can legally
  // produce ordered output.
  absl::Status ValidateResolvedScanOrdering(const ResolvedScan* scan);

  absl::Status ValidateResolvedWithScan(
      const ResolvedWithScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);

  absl::Status ValidateGroupRowsScan(const ResolvedGroupRowsScan* scan);

  absl::Status ValidateResolvedReturningClause(
      const ResolvedReturningClause* returning,
      std::set<ResolvedColumn>& visible_columns);

  absl::Status ValidateOrderByAndLimitClausesOfAggregateFunctionCall(
      const std::set<ResolvedColumn>& input_scan_visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedAggregateFunctionCall* aggregate_function_call);

  absl::Status ValidateResolvedAggregateComputedColumn(
      const ResolvedComputedColumn* computed_column,
      const std::set<ResolvedColumn>& input_scan_visible_columns,
      const std::set<ResolvedColumn>& visible_parameters);

  // Verifies that all the internal references in <expr> are present in
  // the <visible_columns> scope.
  absl::Status ValidateResolvedExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedExpr* expr);

  absl::Status ValidateResolvedAggregateFunctionCall(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedAggregateFunctionCall* aggregate_function);

  absl::Status ValidateResolvedAnalyticFunctionCall(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedAnalyticFunctionCall* call);

  absl::Status ValidateResolvedGetProtoFieldExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedGetProtoField* get_proto_field);

  absl::Status ValidateResolvedGetJsonFieldExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedGetJsonField* get_json_field);

  absl::Status ValidateResolvedFlatten(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedFlatten* flatten);
  absl::Status ValidateResolvedFlattenedArg(
      const ResolvedFlattenedArg* flattened_arg);

  absl::Status ValidateResolvedReplaceField(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedReplaceField* replace_field);

  absl::Status ValidateResolvedInlineLambda(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedInlineLambda* resolved_lambda);

  absl::Status ValidateResolvedSubqueryExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedSubqueryExpr* resolved_subquery_expr);

  absl::Status ValidateResolvedLetExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedLetExpr* resolved_let_expr);

  // Verifies that all the internal references in <expr_list> are present
  // in the <visible_columns> scope.
  absl::Status ValidateResolvedExprList(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const std::vector<std::unique_ptr<const ResolvedExpr>>& expr_list);

  absl::Status ValidateResolvedFunctionArgumentList(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const std::vector<std::unique_ptr<const ResolvedFunctionArgument>>&
          expr_list);

  absl::Status ValidateResolvedComputedColumn(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedComputedColumn* computed_column);

  absl::Status ValidateResolvedComputedColumnList(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
          computed_column_list);

  absl::Status ValidateResolvedOutputColumn(
      const std::set<ResolvedColumn>& visible_columns,
      const ResolvedOutputColumn* output_column);

  absl::Status ValidateResolvedOutputColumnList(
      const std::vector<ResolvedColumn>& visible_columns,
      const std::vector<std::unique_ptr<const ResolvedOutputColumn>>&
          output_column_list,
      bool is_value_table);

  absl::Status ValidateResolvedCreateSchemaStmt(
      const ResolvedCreateSchemaStmt* stmt);

  absl::Status ValidateResolvedCreateTableStmtBase(
      const ResolvedCreateTableStmtBase* stmt,
      std::set<ResolvedColumn>* visible_columns);

  absl::Status ValidateResolvedCast(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedCast* resolved_cast);

  absl::Status ValidateResolvedConstant(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedConstant* resolved_constant);

  absl::Status ValidateResolvedFilterField(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedFilterField* filter_field);

  absl::Status ValidateResolvedFunctionCallBase(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedFunctionCallBase* resolved_function_call);

  absl::Status ValidateHintList(
      const std::vector<std::unique_ptr<const ResolvedOption>>& hint_list);

  absl::Status ValidateResolvedTableAndColumnInfo(
      const ResolvedTableAndColumnInfo* table_and_column_info);

  absl::Status ValidateResolvedTableAndColumnInfoList(
      const std::vector<std::unique_ptr<const ResolvedTableAndColumnInfo>>&
          table_and_column_info_list);

  absl::Status ValidateCollateExpr(
      const ResolvedExpr* resolved_collate);

  absl::Status ValidateColumnAnnotations(
      const ResolvedColumnAnnotations* annotations);

  absl::Status ValidateUpdatedAnnotations(
      const ResolvedColumnAnnotations* annotations);

  absl::Status ValidateColumnDefinitions(
      const std::vector<std::unique_ptr<const ResolvedColumnDefinition>>&
          column_definitions,
      std::set<ResolvedColumn>* visible_columns);

  // When <skip_check_type_match> is true, skip checking default value type
  // can be coerced to column type. This is used when validating ALTER COLUMN IF
  // EXIST column SET DEFAULT.
  absl::Status ValidateResolvedColumnDefaultValue(
      const ResolvedColumnDefaultValue* default_value, const Type* column_type,
      bool skip_check_type_match = false);

  absl::Status ValidatePercentArgument(const ResolvedExpr* expr);

  // Verifies that only one of the parameter name and position is set.
  absl::Status ValidateResolvedParameter(
      const ResolvedParameter* resolved_param);

  absl::Status ValidateResolvedFunctionArgument(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedFunctionArgument* resolved_arg);

  // Validates TVF relation argument schema against the required input schema
  // in function signature.
  absl::Status ValidateRelationSchemaInResolvedFunctionArgument(
      const TVFRelation& required_input_schema,
      const TVFRelation& input_relation,
      const ResolvedFunctionArgument* resolved_arg);

  absl::Status CheckColumnIsPresentInColumnSet(
      const ResolvedColumn& column,
      const std::set<ResolvedColumn>& visible_columns);

  // Verifies that the scan column list only contains column from the visible
  // set.
  absl::Status CheckColumnList(const ResolvedScan* scan,
                               const std::set<ResolvedColumn>& visible_columns);

  absl::Status AddColumnList(const ResolvedColumnList& column_list,
                             std::set<ResolvedColumn>* visible_columns);
  absl::Status AddColumnList(
      const ResolvedColumnList& column_list,
      absl::flat_hash_set<ResolvedColumn>* visible_columns);
  absl::Status AddColumn(const ResolvedColumn& column,
                         absl::flat_hash_set<ResolvedColumn>* visible_columns);
  absl::Status AddColumnFromComputedColumn(
      const ResolvedComputedColumn* computed_column,
      std::set<ResolvedColumn>* visible_columns);
  absl::Status AddColumnsFromComputedColumnList(
      const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
          computed_column_list,
      std::set<ResolvedColumn>* visible_columns);

  absl::Status ValidateResolvedOrderByItem(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedOrderByItem* item);

  absl::Status ValidateResolvedAnalyticFunctionGroup(
      const ResolvedAnalyticFunctionGroup* group,
      const std::set<ResolvedColumn>& input_visible_columns,
      const std::set<ResolvedColumn>& visible_parameters);

  absl::Status ValidateResolvedWindowFrame(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedWindowOrdering* window_ordering,
      const ResolvedWindowFrame* window_frame);

  absl::Status ValidateResolvedWindowFrameExpr(
      const std::set<ResolvedColumn>& visible_columns,
      const std::set<ResolvedColumn>& visible_parameters,
      const ResolvedWindowOrdering* window_ordering,
      const ResolvedWindowFrame::FrameUnit& frame_unit,
      const ResolvedWindowFrameExpr* window_frame_expr);

  absl::Status ValidateResolvedWindowFrameExprType(
      const ResolvedWindowFrame::FrameUnit& frame_unit,
      const ResolvedExpr* window_ordering_expr,
      const ResolvedExpr& window_frame_expr);

  absl::Status ValidateResolvedAlterObjectStmt(
      const ResolvedAlterObjectStmt* stmt);

  absl::Status ValidateResolvedAlterAction(const ResolvedAlterAction* action);

  absl::Status ValidateResolvedExecuteImmediateStmt(
      const ResolvedExecuteImmediateStmt* stmt);

  absl::Status ValidateResolvedRecursiveScan(
      const ResolvedRecursiveScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);

  absl::Status ValidateResolvedRecursiveRefScan(
      const ResolvedRecursiveRefScan* scan);

  absl::Status ValidateResolvedPivotScan(
      const ResolvedPivotScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);

  absl::Status ValidateResolvedUnpivotScan(
      const ResolvedUnpivotScan* scan,
      const std::set<ResolvedColumn>& visible_parameters);

  absl::Status ValidateResolvedWithPartitionColumns(
      const ResolvedWithPartitionColumns* with_partition_columns,
      std::set<ResolvedColumn>* visible_columns);

  absl::Status ValidateResolvedForeignKey(
      const ResolvedForeignKey* foreign_key,
      const std::vector<const Type*> column_types,
      absl::flat_hash_set<std::string>* constraint_names);

  absl::Status ValidateResolvedPrimaryKey(
      const std::vector<const Type*>& resolved_column_types,
      const ResolvedPrimaryKey* primary_key,
      absl::flat_hash_set<std::string>* constraint_names);

  absl::Status ValidateAddForeignKeyAction(
      const ResolvedAddConstraintAction* action,
      absl::flat_hash_set<std::string>* constraint_names);

  absl::Status ValidateAddPrimaryKeyAction(
      const ResolvedAddConstraintAction* action,
      absl::flat_hash_set<std::string>* constraint_names);

  absl::Status ValidateResolvedAuxLoadDataStmt(
      const ResolvedAuxLoadDataStmt* stmt);

  // Checks that <expr> contains only ColumnRefs, GetProtoField, GetStructField
  // and GetJsonField expressions. Sets 'ref' to point to the leaf
  // ResolvedColumnRef.
  absl::Status CheckExprIsPath(const ResolvedExpr* expr,
                               const ResolvedColumnRef** ref);

  // Validates whether <expr> is a literal or a parameter. In either case, it
  // should be of type int64_t.
  absl::Status ValidateArgumentIsInt64Constant(const ResolvedExpr* expr);

  absl::Status ValidateGenericArgumentsAgainstConcreteArguments(
      const ResolvedFunctionCallBase* resolved_function_call,
      const FunctionSignature& signature);

  // Validates that CheckUniqueColumnId() was never previously called with the
  // same column id as that of <column>.
  absl::Status CheckUniqueColumnId(const ResolvedColumn& column);

  absl::Status ValidateCompatibleSchemaForClone(const Table* source,
                                                const Table* target);

  absl::Status CheckFunctionArgumentType(
      const FunctionArgumentTypeList& argument_type_list,
      absl::string_view statement_type);

  // Replacement for ::zetasql_base::InternalErrorBuilder(), which also records the
  // context of the error for use in the tree dump.
  zetasql_base::StatusBuilder InternalErrorBuilder() {
    RecordContext();
    return ::zetasql_base::InternalErrorBuilder();
  }

  std::string RecordContext();

  // Clears internal Validator state from prior validation.
  void Reset();

  // Which ArgumentKinds are allowed in the current expression.
  // Set using scoped VarSetters.
  typedef absl::flat_hash_set<ResolvedArgumentDefEnums::ArgumentKind>
      ArgumentKindSet;

  // Helper class to push a node onto the context stack in the constructor and
  // pop the same node off the context stack in the destructor.
  //
  // An instance of this class should be created at the start of each
  // ValidateXXX() function. If an error occurs, the top of the stack, at the
  // point of the failed VALIDATOR_RET_CHECK() indicates the subtree that failed
  // validation, and will be emphasized in the tree dump.
  class PushErrorContext {
   public:
    PushErrorContext(Validator* validator, const ResolvedNode* node)
        : validator_(validator), node_(node) {
      if (node != nullptr) {
        validator->context_stack_.push_back(node);
      }
    }
    ~PushErrorContext() {
      if (node_ != nullptr) {
        ZETASQL_DCHECK(!validator_->context_stack_.empty());
        ZETASQL_DCHECK_EQ(validator_->context_stack_.back(), node_);
        validator_->context_stack_.pop_back();
      }
    }

   private:
    Validator* validator_;
    const ResolvedNode* node_;
  };

  struct RecursiveScanInfo {
    explicit RecursiveScanInfo(const ResolvedRecursiveScan* scan) {
      this->scan = scan;
    }

    const ResolvedRecursiveScan* scan;
    bool saw_recursive_ref = false;
  };

  // Options that disable validation of certain invariants.
  const ValidatorOptions options_;

  ArgumentKindSet allowed_argument_kinds_;

  // This points to the current CREATE TABLE FUNCTION statement being validated,
  // or null if no such statement is currently being validated.
  const ResolvedCreateTableFunctionStmt* current_create_table_function_stmt_ =
      nullptr;

  const LanguageOptions language_options_;

  // The number of nested "recursive contexts" we are in. A "recursive context"
  // is either a WITH entry of a recursive WITH or the body of a recursive view.
  // A ResolvedRecursiveScan node is legal only if this value is > 0.
  int nested_recursive_context_count_ = 0;

  // A stack of ResolvedRecursiveScans we are currently inside the recursive
  // term of. Used to ensure that each ResolvedRecursiveRefScan matches up
  // with a ResolvedRecursiveScan.
  std::vector<RecursiveScanInfo> nested_recursive_scans_;

  // Pre-aggregation columns, reflecting the columns available from the related
  // FROM clause. Captured by WITH GROUP_ROWS expression, to be used by
  // GROUP_ROWS() function in it.
  std::optional<std::set<ResolvedColumn>> input_columns_for_group_rows_;

  // List of column ids seen so far. Used to ensure that every unique column
  // has a distinct id.
  absl::flat_hash_set<int> column_ids_seen_;

  // The node at the top of the stack is the innermost node being validated.
  std::vector<const ResolvedNode*> context_stack_;

  // If validation fails, this is set to the innermost element being validated
  // (context_stack_.back()) at the point of the error. This allows the record
  // of the node that caused the error to be retained, even as various helper
  // functions terminate. At the root level (ValidateStandaloneExpression()/
  // ValidateResolvedStatement()), this node will be annotated in the tree dump.
  //
  // If no validation error has yet occurred, this value is nullptr.
  const ResolvedNode* error_context_ = nullptr;
};

}  // namespace zetasql

#endif  // ZETASQL_RESOLVED_AST_VALIDATOR_H_
