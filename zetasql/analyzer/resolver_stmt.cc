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

// This file contains the implementation of Statement-related resolver methods
// from resolver.h (except DML statements, which are in resolver_dml.cc).
#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/base/varsetter.h"
#include "zetasql/analyzer/column_cycle_detector.h"
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/function_resolver.h"
#include "zetasql/analyzer/input_argument_type_resolver_helper.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/analyzer/recursive_queries.h"
#include "zetasql/analyzer/resolver.h"
// This includes common macro definitions to define in the resolver cc files.
#include "zetasql/analyzer/resolver_common_inl.h"
#include "zetasql/common/errors.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/parser/unparser.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/annotation/collation.h"
#include "zetasql/public/cast.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/deprecation_warning.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/procedure.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/scripting/parsed_script.h"
#include "zetasql/base/case.h"
#include "absl/base/casts.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/meta/type_traits.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/strings/substitute.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

constexpr bool kExplicitColumn = true;

bool AllowsExternalSource(ASTNodeKind node_kind) {
  return node_kind == ASTNodeKind::AST_CREATE_EXTERNAL_TABLE_STATEMENT
      || node_kind == ASTNodeKind::AST_AUX_LOAD_DATA_STATEMENT;
}

bool CompatibleForClone(const ResolvedColumn& c1, const ResolvedColumn& c2) {
  return c1.name_id().CaseEquals(c2.name_id()) && c1.type()->Equals(c2.type());
}

absl::StatusOr<const Table*> ExtractCloneSourceTable(const ResolvedScan& scan) {
  switch (scan.node_kind()) {
    case RESOLVED_TABLE_SCAN:
      return scan.GetAs<ResolvedTableScan>()->table();
    case RESOLVED_FILTER_SCAN:
      return ExtractCloneSourceTable(
          *scan.GetAs<ResolvedFilterScan>()->input_scan());
    default:
      return ::zetasql_base::InternalErrorBuilder()
             << "Invalid scan table from clone data source: "
             << scan.node_kind_string();
  }
}
}  // namespace

// These are constant identifiers used mostly for generated column or table
// names.  We use a single IdString for each so we never have to allocate
// or copy these strings again.
STATIC_IDSTRING(kCreateAsId, "$create_as");
STATIC_IDSTRING(kQueryId, "$query");
STATIC_IDSTRING(kViewId, "$view");
STATIC_IDSTRING(kCreateAsCastId, "$create_as_cast");

absl::Status Resolver::ResolveStatement(
    absl::string_view sql, const ASTStatement* statement,
    std::unique_ptr<const ResolvedStatement>* output) {
  Reset(sql);

  std::unique_ptr<ResolvedStatement> stmt;

  // If we have a HintedStatement, unwrap it and save the Hint for
  // resolving later.
  const ASTHint* hint = nullptr;
  if (statement->node_kind() == AST_HINTED_STATEMENT) {
    const ASTHintedStatement* hinted_statement =
        statement->GetAsOrDie<ASTHintedStatement>();
    hint = hinted_statement->hint();
    statement = hinted_statement->statement();
    ZETASQL_RET_CHECK(hint != nullptr);
  }

  if (analyzer_options().statement_context() == CONTEXT_MODULE) {
    // We are resolving statements inside a module, which restricts the
    // set of supported statement kinds.
    switch (statement->node_kind()) {
      case AST_CREATE_CONSTANT_STATEMENT:
      case AST_CREATE_FUNCTION_STATEMENT:
      case AST_CREATE_TABLE_FUNCTION_STATEMENT:
      case AST_IMPORT_STATEMENT:
      case AST_MODULE_STATEMENT:
        // These statements are supported inside of modules.
        break;
      default:
        // These statements are not supported inside of modules (although
        // some will eventually get support, such as CREATE TABLE FUNCTION).
        return MakeSqlErrorAt(statement)
            << "Statement not supported inside modules: "
            << statement->GetNodeKindString();
    }
  }

  if (statement->node_kind() != AST_QUERY_STATEMENT) {
    if (!analyzer_options().get_target_column_types().empty()) {
      return MakeSqlErrorAt(statement)
             << "Unexpected statement type, expected query "
                "statement because output columns are required";
    }
  }

  switch (statement->node_kind()) {
    case AST_QUERY_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_QUERY_STMT)) {
        std::shared_ptr<const NameList> name_list;
        ZETASQL_RETURN_IF_ERROR(ResolveQueryStatement(
            static_cast<const ASTQueryStatement*>(statement), &stmt, &name_list));
      }
      break;

    case AST_EXPLAIN_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_EXPLAIN_STMT)) {
        const ASTExplainStatement* explain =
            static_cast<const ASTExplainStatement*>(statement);
        if (explain->statement()->node_kind() ==
            AST_EXPLAIN_STATEMENT) {
          return MakeSqlErrorAt(explain) << "EXPLAIN EXPLAIN is not allowed";
        }
        std::unique_ptr<const ResolvedStatement> inner_statement;
        ZETASQL_RETURN_IF_ERROR(
            ResolveStatement(sql, explain->statement(), &inner_statement));
        stmt = MakeResolvedExplainStmt(std::move(inner_statement));
      }
      break;

    case AST_CREATE_INDEX_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_CREATE_INDEX_STMT)) {
        ZETASQL_RETURN_IF_ERROR((ResolveCreateIndexStatement(
            statement->GetAsOrDie<ASTCreateIndexStatement>(), &stmt)));
      }
      break;

    case AST_CREATE_TABLE_STATEMENT: {
      const auto* create_stmt =
          statement->GetAsOrDie<ASTCreateTableStatement>();
      const ResolvedNodeKind node_kind = create_stmt->query() == nullptr ?
          RESOLVED_CREATE_TABLE_STMT : RESOLVED_CREATE_TABLE_AS_SELECT_STMT;
      if (language().SupportsStatementKind(node_kind)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateTableStatement(create_stmt, &stmt));
      }
      break;
    }
    case AST_CREATE_MODEL_STATEMENT: {
      if (language().SupportsStatementKind(RESOLVED_CREATE_MODEL_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateModelStatement(
            statement->GetAsOrDie<ASTCreateModelStatement>(), &stmt));
      }
      break;
    }
    case AST_CREATE_VIEW_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_CREATE_VIEW_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateViewStatement(
            statement->GetAsOrDie<ASTCreateViewStatement>(), &stmt));
      }
      break;

    case AST_CREATE_MATERIALIZED_VIEW_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_CREATE_MATERIALIZED_VIEW_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateMaterializedViewStatement(
            statement->GetAsOrDie<ASTCreateMaterializedViewStatement>(),
            &stmt));
      }
      break;

    case AST_CREATE_SNAPSHOT_TABLE_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_CREATE_SNAPSHOT_TABLE_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateSnapshotTableStatement(
            statement->GetAsOrDie<ASTCreateSnapshotTableStatement>(), &stmt));
      }
      break;

    case AST_CREATE_EXTERNAL_TABLE_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_CREATE_EXTERNAL_TABLE_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateExternalTableStatement(
            statement->GetAsOrDie<ASTCreateExternalTableStatement>(), &stmt));
      }
      break;

    case AST_CREATE_PRIVILEGE_RESTRICTION_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_CREATE_PRIVILEGE_RESTRICTION_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreatePrivilegeRestrictionStatement(
            statement->GetAsOrDie<ASTCreatePrivilegeRestrictionStatement>(),
            &stmt));
      }
      break;

    case AST_CREATE_ROW_ACCESS_POLICY_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_CREATE_ROW_ACCESS_POLICY_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateRowAccessPolicyStatement(
            statement->GetAsOrDie<ASTCreateRowAccessPolicyStatement>(), &stmt));
      }
      break;

    case AST_CREATE_CONSTANT_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_CREATE_CONSTANT_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateConstantStatement(
            statement->GetAsOrDie<ASTCreateConstantStatement>(), &stmt));
      }
      break;

    case AST_CREATE_FUNCTION_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_CREATE_FUNCTION_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateFunctionStatement(
            statement->GetAsOrDie<ASTCreateFunctionStatement>(), &stmt));
      }
      break;

    case AST_CREATE_TABLE_FUNCTION_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_CREATE_TABLE_FUNCTION_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateTableFunctionStatement(
            statement->GetAsOrDie<ASTCreateTableFunctionStatement>(), &stmt));
      }
      break;

    case AST_CREATE_ENTITY_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_CREATE_ENTITY_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateEntityStatement(
            statement->GetAsOrDie<ASTCreateEntityStatement>(), &stmt));
      }
      break;

    case AST_ALTER_ENTITY_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_ALTER_ENTITY_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAlterEntityStatement(
            statement->GetAsOrDie<ASTAlterEntityStatement>(), &stmt));
      }
      break;

    case AST_CLONE_DATA_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_CLONE_DATA_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCloneDataStatement(
            statement->GetAsOrDie<ASTCloneDataStatement>(), &stmt));
      }
      break;

    case AST_EXPORT_DATA_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_EXPORT_DATA_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveExportDataStatement(
            statement->GetAsOrDie<ASTExportDataStatement>(), &stmt));
      }
      break;

    case AST_EXPORT_MODEL_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_EXPORT_MODEL_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveExportModelStatement(
            statement->GetAsOrDie<ASTExportModelStatement>(), &stmt));
      }
      break;

    case AST_CALL_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_CALL_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCallStatement(
            statement->GetAsOrDie<ASTCallStatement>(), &stmt));
      }
      break;

    case AST_DEFINE_TABLE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_DEFINE_TABLE_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveDefineTableStatement(
            statement->GetAsOrDie<ASTDefineTableStatement>(), &stmt));
      }
      break;

    case AST_DESCRIBE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_DESCRIBE_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveDescribeStatement(
            statement->GetAsOrDie<ASTDescribeStatement>(), &stmt));
      }
      break;

    case AST_SHOW_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_SHOW_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveShowStatement(
            statement->GetAsOrDie<ASTShowStatement>(), &stmt));
      }
      break;

    case AST_BEGIN_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_BEGIN_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveBeginStatement(
            statement->GetAsOrDie<ASTBeginStatement>(), &stmt));
      }
      break;

    case AST_SET_TRANSACTION_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_SET_TRANSACTION_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveSetTransactionStatement(
            statement->GetAsOrDie<ASTSetTransactionStatement>(), &stmt));
      }
      break;

    case AST_COMMIT_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_COMMIT_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCommitStatement(
            statement->GetAsOrDie<ASTCommitStatement>(), &stmt));
      }
      break;

    case AST_ROLLBACK_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_ROLLBACK_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveRollbackStatement(
            statement->GetAsOrDie<ASTRollbackStatement>(), &stmt));
      }
      break;

    case AST_START_BATCH_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_START_BATCH_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveStartBatchStatement(
            statement->GetAsOrDie<ASTStartBatchStatement>(), &stmt));
      }
      break;

    case AST_RUN_BATCH_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_RUN_BATCH_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveRunBatchStatement(
            statement->GetAsOrDie<ASTRunBatchStatement>(), &stmt));
      }
      break;

    case AST_ABORT_BATCH_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_ABORT_BATCH_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAbortBatchStatement(
            statement->GetAsOrDie<ASTAbortBatchStatement>(), &stmt));
      }
      break;

    case AST_DELETE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_DELETE_STMT)) {
        std::unique_ptr<ResolvedDeleteStmt> resolved_delete_stmt;
        ZETASQL_RETURN_IF_ERROR(
            ResolveDeleteStatement(statement->GetAsOrDie<ASTDeleteStatement>(),
                                   &resolved_delete_stmt));
        stmt = std::move(resolved_delete_stmt);
      }
      break;

    case AST_DROP_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_DROP_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveDropStatement(
            statement->GetAsOrDie<ASTDropStatement>(), &stmt));
      }
      break;

    case AST_DROP_ENTITY_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_DROP_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveDropEntityStatement(
            statement->GetAsOrDie<ASTDropEntityStatement>(), &stmt));
      }
      break;

    case AST_DROP_FUNCTION_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_DROP_FUNCTION_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveDropFunctionStatement(
            statement->GetAsOrDie<ASTDropFunctionStatement>(), &stmt));
      }
      break;

    case AST_DROP_TABLE_FUNCTION_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_DROP_TABLE_FUNCTION_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveDropTableFunctionStatement(
            statement->GetAsOrDie<ASTDropTableFunctionStatement>(), &stmt));
      }
      break;

    case AST_DROP_PRIVILEGE_RESTRICTION_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_DROP_PRIVILEGE_RESTRICTION_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveDropPrivilegeRestrictionStatement(
            statement->GetAsOrDie<ASTDropPrivilegeRestrictionStatement>(),
            &stmt));
      }
      break;

    case AST_DROP_ROW_ACCESS_POLICY_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_DROP_ROW_ACCESS_POLICY_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveDropRowAccessPolicyStatement(
            statement->GetAsOrDie<ASTDropRowAccessPolicyStatement>(), &stmt));
      }
      break;

    case AST_DROP_ALL_ROW_ACCESS_POLICIES_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_DROP_ROW_ACCESS_POLICY_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveDropAllRowAccessPoliciesStatement(
            statement->GetAsOrDie<ASTDropAllRowAccessPoliciesStatement>(),
            &stmt));
      }
      break;

    case AST_DROP_MATERIALIZED_VIEW_STATEMENT:
      if (language().SupportsStatementKind(
          RESOLVED_DROP_MATERIALIZED_VIEW_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveDropMaterializedViewStatement(
            statement->GetAsOrDie<ASTDropMaterializedViewStatement>(), &stmt));
      }
      break;

    case AST_DROP_SNAPSHOT_TABLE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_DROP_SNAPSHOT_TABLE_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveDropSnapshotTableStatement(
            statement->GetAsOrDie<ASTDropSnapshotTableStatement>(), &stmt));
      }
      break;

    case AST_DROP_SEARCH_INDEX_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_DROP_SEARCH_INDEX_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveDropSearchIndexStatement(
            statement->GetAsOrDie<ASTDropSearchIndexStatement>(), &stmt));
      }
      break;

    case AST_INSERT_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_INSERT_STMT)) {
        std::unique_ptr<ResolvedInsertStmt> resolved_insert_stmt;
        ZETASQL_RETURN_IF_ERROR(
            ResolveInsertStatement(statement->GetAsOrDie<ASTInsertStatement>(),
                                   &resolved_insert_stmt));
        stmt = std::move(resolved_insert_stmt);
      }
      break;

    case AST_UPDATE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_UPDATE_STMT)) {
        std::unique_ptr<ResolvedUpdateStmt> resolved_update_stmt;
        ZETASQL_RETURN_IF_ERROR(
            ResolveUpdateStatement(statement->GetAsOrDie<ASTUpdateStatement>(),
                                   &resolved_update_stmt));
        stmt = std::move(resolved_update_stmt);
      }
      break;

    case AST_TRUNCATE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_TRUNCATE_STMT)) {
        std::unique_ptr<ResolvedTruncateStmt> resolved_truncate_stmt;
        ZETASQL_RETURN_IF_ERROR(ResolveTruncateStatement(
            statement->GetAsOrDie<ASTTruncateStatement>(),
            &resolved_truncate_stmt));
        stmt = std::move(resolved_truncate_stmt);
      }
      break;

    case AST_MERGE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_MERGE_STMT)) {
        std::unique_ptr<ResolvedMergeStmt> resolved_merge_stmt;
        ZETASQL_RETURN_IF_ERROR(ResolveMergeStatement(
            statement->GetAsOrDie<ASTMergeStatement>(), &resolved_merge_stmt));
        stmt = std::move(resolved_merge_stmt);
      }
      break;

    case AST_GRANT_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_GRANT_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveGrantStatement(
            statement->GetAsOrDie<ASTGrantStatement>(), &stmt));
      }
      break;

    case AST_REVOKE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_REVOKE_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveRevokeStatement(
            statement->GetAsOrDie<ASTRevokeStatement>(), &stmt));
      }
      break;
    case AST_ALTER_DATABASE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_ALTER_DATABASE_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAlterDatabaseStatement(
            statement->GetAsOrDie<ASTAlterDatabaseStatement>(), &stmt));
      }
      break;
    case AST_ALTER_PRIVILEGE_RESTRICTION_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_ALTER_PRIVILEGE_RESTRICTION_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAlterPrivilegeRestrictionStatement(
            statement->GetAsOrDie<ASTAlterPrivilegeRestrictionStatement>(),
            &stmt));
      }
      break;
    case AST_ALTER_ROW_ACCESS_POLICY_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_ALTER_ROW_ACCESS_POLICY_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAlterRowAccessPolicyStatement(
            statement->GetAsOrDie<ASTAlterRowAccessPolicyStatement>(), &stmt));
      }
      break;
    case AST_ALTER_ALL_ROW_ACCESS_POLICIES_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_ALTER_ALL_ROW_ACCESS_POLICIES_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAlterAllRowAccessPoliciesStatement(
            statement->GetAsOrDie<ASTAlterAllRowAccessPoliciesStatement>(),
            &stmt));
      }
      break;
    case AST_ALTER_SCHEMA_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_ALTER_SCHEMA_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAlterSchemaStatement(
            statement->GetAsOrDie<ASTAlterSchemaStatement>(), &stmt));
      }
      break;
    case AST_ALTER_TABLE_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_ALTER_TABLE_SET_OPTIONS_STMT) ||
          language().SupportsStatementKind(RESOLVED_ALTER_TABLE_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAlterTableStatement(
            statement->GetAsOrDie<ASTAlterTableStatement>(), &stmt));
      }
      break;
    case AST_ALTER_VIEW_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_ALTER_VIEW_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAlterViewStatement(
            statement->GetAsOrDie<ASTAlterViewStatement>(), &stmt));
      }
      break;
    case AST_ALTER_MATERIALIZED_VIEW_STATEMENT:
      if (language().SupportsStatementKind(
              RESOLVED_ALTER_MATERIALIZED_VIEW_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAlterMaterializedViewStatement(
            statement->GetAsOrDie<ASTAlterMaterializedViewStatement>(), &stmt));
      }
      break;
    case AST_RENAME_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_RENAME_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveRenameStatement(
            statement->GetAsOrDie<ASTRenameStatement>(), &stmt));
      }
      break;
    case AST_IMPORT_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_IMPORT_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveImportStatement(
            statement->GetAsOrDie<ASTImportStatement>(), &stmt));
      }
      break;
    case AST_MODULE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_MODULE_STMT)) {
        // TODO: Add a check for in-module statement context here
        //   if (analyzer_options().statement_context() != CONTEXT_MODULE)
        // and provide an error if we are not in-module statement context.
        // Adding this check has a dependency on PDQL-to-ZetaSQL translator
        // code, which must enable module context before we can add this
        // check.
        if (!language().LanguageFeatureEnabled(FEATURE_EXPERIMENTAL_MODULES)) {
          return MakeSqlErrorAt(statement)
              << "The MODULEs feature is not supported";
        }
        ZETASQL_RETURN_IF_ERROR(ResolveModuleStatement(
            statement->GetAsOrDie<ASTModuleStatement>(), &stmt));
      }
      break;
    case AST_CREATE_DATABASE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_CREATE_DATABASE_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateDatabaseStatement(
            statement->GetAsOrDie<ASTCreateDatabaseStatement>(), &stmt));
      }
      break;
    case AST_CREATE_SCHEMA_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_CREATE_SCHEMA_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateSchemaStatement(
            statement->GetAsOrDie<ASTCreateSchemaStatement>(), &stmt));
      }
      break;
    case AST_ANALYZE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_ANALYZE_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAnalyzeStatement(
            statement->GetAsOrDie<ASTAnalyzeStatement>(), &stmt));
      }
      break;
    case AST_ASSERT_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_ASSERT_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAssertStatement(
            statement->GetAsOrDie<ASTAssertStatement>(), &stmt));
      }
      break;
    case AST_CREATE_PROCEDURE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_CREATE_PROCEDURE_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveCreateProcedureStatement(
            statement->GetAsOrDie<ASTCreateProcedureStatement>(), &stmt));
      }
      break;
    case AST_EXECUTE_IMMEDIATE_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_EXECUTE_IMMEDIATE_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveExecuteImmediateStatement(
            statement->GetAsOrDie<ASTExecuteImmediateStatement>(), &stmt));
      }
      break;
    case AST_SYSTEM_VARIABLE_ASSIGNMENT:
      if (language().SupportsStatementKind(RESOLVED_ASSIGNMENT_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveSystemVariableAssignment(
            statement->GetAsOrDie<ASTSystemVariableAssignment>(), &stmt));
      }
      break;
    case AST_AUX_LOAD_DATA_STATEMENT:
      if (language().SupportsStatementKind(RESOLVED_AUX_LOAD_DATA_STMT)) {
        ZETASQL_RETURN_IF_ERROR(ResolveAuxLoadDataStatement(
            static_cast<const ASTAuxLoadDataStatement*>(statement), &stmt));
      }
      break;
    default:
      break;
  }

  if (stmt == nullptr) {
    // This statement is not currently supported so we return an error here.
    return MakeSqlErrorAt(statement)
           << "Statement not supported: " << statement->GetNodeKindString();
  }

  ZETASQL_RETURN_IF_ERROR(ValidateUndeclaredParameters(stmt.get()));

  if (hint != nullptr) {
    // Add in statement hints if we started with a HintedStatement.
    ZETASQL_RETURN_IF_ERROR(ResolveHintsForNode(hint, stmt.get()));
  }

  ZETASQL_RETURN_IF_ERROR(PruneColumnLists(stmt.get()));
  ZETASQL_RETURN_IF_ERROR(SetColumnAccessList(stmt.get()));
  *output = std::move(stmt);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveQueryStatement(
    const ASTQueryStatement* query_stmt,
    std::unique_ptr<ResolvedStatement>* output_stmt,
    std::shared_ptr<const NameList>* output_name_list) {
  std::unique_ptr<const ResolvedScan> resolved_scan;
  ZETASQL_RETURN_IF_ERROR(ResolveQuery(
      query_stmt->query(), empty_name_scope_.get(), kQueryId,
      /*is_outer_query=*/true, &resolved_scan, output_name_list));

  // Sanity check: WITH aliases get unregistered as they go out of scope.
  ZETASQL_RET_CHECK(named_subquery_map_.empty());

  // Generate the user-visible output_column_list.
  // TODO Generate better user-visible names for anonymous columns.
  std::vector<std::unique_ptr<const ResolvedOutputColumn>> output_column_list;
  for (const NamedColumn& named_column : (*output_name_list)->columns()) {
    // Ownership of ResolvedOutputColumn transferred to ResolvedQueryStmt below.
    // TODO Add IdString support to resolved AST classes like
    // ResolvedOutputColumn so we don't have to call ToString and copy here.
    output_column_list.push_back(MakeResolvedOutputColumn(
        named_column.name.ToString(), named_column.column));
  }

  *output_stmt = MakeResolvedQueryStmt(std::move(output_column_list),
                                       (*output_name_list)->is_value_table(),
                                       std::move(resolved_scan));
  return absl::OkStatus();
}

static ResolvedCreateStatement::CreateScope ToResolvedCreateScope(
    ASTCreateStatement::Scope scope) {
  switch (scope) {
    case ASTCreateStatement::DEFAULT_SCOPE:
      return ResolvedCreateStatementEnums::CREATE_DEFAULT_SCOPE;
    case ASTCreateStatement::TEMPORARY:
      return ResolvedCreateStatementEnums::CREATE_TEMP;
    case ASTCreateStatement::PUBLIC:
      return ResolvedCreateStatementEnums::CREATE_PUBLIC;
    case ASTCreateStatement::PRIVATE:
      return ResolvedCreateStatementEnums::CREATE_PRIVATE;
  }
}

absl::Status Resolver::ResolveCreateStatementLikeTableName(
    const ASTPathExpression* like_table_name,
    const IdString& table_name_id_string,
    std::vector<std::unique_ptr<const ResolvedColumnDefinition>>*
        column_definition_list,
    const Table** like_table) {
  const Table* found_like_table;
  ZETASQL_RETURN_IF_ERROR(FindTable(like_table_name, &found_like_table));

  for (int i = 0; i < found_like_table->NumColumns(); ++i) {
    const Column* column = found_like_table->GetColumn(i);
    IdString column_name = MakeIdString(column->Name());
    const ResolvedColumn resolved_column = ResolvedColumn(
        AllocateColumnId(), table_name_id_string, column_name,
        AnnotatedType(column->GetType(), column->GetTypeAnnotationMap()));
    column_definition_list->push_back(MakeResolvedColumnDefinition(
        column_name.ToString(), resolved_column.type(),
        /*annotations=*/nullptr, /*is_hidden=*/false, resolved_column,
        /*generated_column_info=*/nullptr, /*default_value=*/nullptr));
  }
  *like_table = found_like_table;
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateStatementOptions(
    const ASTCreateStatement* ast_statement, absl::string_view statement_type,
    ResolvedCreateStatement::CreateScope* create_scope,
    ResolvedCreateStatement::CreateMode* create_mode) const {
  *create_scope = ResolvedCreateStatement::CREATE_DEFAULT_SCOPE;
  *create_mode = ResolvedCreateStatement::CREATE_DEFAULT;

  // Check for validity of the create mode context.  Only the default mode is
  // allowed in modules.
  // TODO: Set the error location to the specific offending clause.
  if (ast_statement->is_or_replace() && ast_statement->is_if_not_exists()) {
    return MakeSqlErrorAt(ast_statement)
           << statement_type
           << " cannot have both OR REPLACE and IF NOT EXISTS";
  }
  if (ast_statement->is_or_replace()) {
    if (analyzer_options().statement_context() == CONTEXT_MODULE) {
      return MakeSqlErrorAt(ast_statement)
             << "Modules do not support CREATE OR REPLACE for "
             << statement_type;
    }
    *create_mode = ResolvedCreateStatement::CREATE_OR_REPLACE;
  } else if (ast_statement->is_if_not_exists()) {
    *create_mode = ResolvedCreateStatement::CREATE_IF_NOT_EXISTS;
    if (analyzer_options().statement_context() == CONTEXT_MODULE) {
      return MakeSqlErrorAt(ast_statement)
             << "Modules do not support IF NOT EXISTS for " << statement_type;
    }
  }

  // Check for validity of the create scope context.  If the PUBLIC/PRIVATE
  // feature is enabled, then TEMP or DEFAULT scopes are invalid.  If
  // the PUBLIC/PRIVATE feature is not enabled, then PUBLIC/PRIVATE are
  // disallowed.
  switch (ast_statement->scope()) {
    case ASTCreateStatement::DEFAULT_SCOPE:
    case ASTCreateStatement::TEMPORARY:
      if (analyzer_options().statement_context() == CONTEXT_MODULE) {
        return MakeSqlErrorAt(ast_statement)
            << statement_type
            << " requires the PUBLIC or PRIVATE modifier when used inside "
               "a module";
      }
      break;
    case ASTCreateStatement::PUBLIC:
    case ASTCreateStatement::PRIVATE:
      if (analyzer_options().statement_context() == CONTEXT_DEFAULT) {
        const std::string suffix =
            (language().LanguageFeatureEnabled(FEATURE_EXPERIMENTAL_MODULES)
                 ? "only supported inside modules"
                 : "not supported");
        return MakeSqlErrorAt(ast_statement)
            << statement_type << " with PUBLIC or PRIVATE modifiers is "
            << suffix;
      }
      break;
  }
  *create_scope = ToResolvedCreateScope(ast_statement->scope());

  return absl::OkStatus();
}

absl::Status Resolver::ValidateColumnAttributeList(
    const ASTColumnAttributeList* attribute_list) const {
  if (attribute_list == nullptr) {
    return absl::OkStatus();
  }
  absl::flat_hash_set<ASTNodeKind> attribute_set;
  for (const ASTColumnAttribute* attribute : attribute_list->values()) {
    if (attribute->node_kind() != ASTNodeKind::AST_FOREIGN_KEY_COLUMN_ATTRIBUTE
        && !zetasql_base::InsertIfNotPresent(&attribute_set, attribute->node_kind())) {
      return MakeSqlErrorAt(attribute)
             << "The " << attribute->SingleNodeSqlString()
             << " attribute was specified multiple times";
    }
    if (attribute->node_kind() == AST_NOT_NULL_COLUMN_ATTRIBUTE &&
        !language().LanguageFeatureEnabled(FEATURE_CREATE_TABLE_NOT_NULL)) {
      return MakeSqlErrorAt(attribute) << "NOT NULL constraint is unsupported";
    }
  }
  return absl::OkStatus();
}

static ResolvedGeneratedColumnInfoEnums::StoredMode ConvertStoredMode(
    ASTGeneratedColumnInfo::StoredMode stored_mode) {
  switch (stored_mode) {
    case ASTGeneratedColumnInfo::NON_STORED:
      return ResolvedGeneratedColumnInfoEnums::NON_STORED;
    case ASTGeneratedColumnInfo::STORED:
      return ResolvedGeneratedColumnInfoEnums::STORED;
    case ASTGeneratedColumnInfo::STORED_VOLATILE:
      return ResolvedGeneratedColumnInfoEnums::STORED_VOLATILE;
  }
}

absl::Status Resolver::ResolveGeneratedColumnInfo(
    const ASTGeneratedColumnInfo* ast_generated_column,
    const NameList& column_name_list, const Type* opt_type,
    std::unique_ptr<ResolvedGeneratedColumnInfo>* output) {
  static constexpr char kComputedColumn[] = "computed column expression";
  const ResolvedGeneratedColumnInfoEnums::StoredMode stored_mode =
      ConvertStoredMode(ast_generated_column->stored_mode());

  zetasql_base::VarSetter<bool> setter_func(
      &analyzing_nonvolatile_stored_expression_columns_,
      stored_mode == ResolvedGeneratedColumnInfoEnums::STORED);
  std::unique_ptr<const ResolvedExpr> resolved_expression;
  const std::shared_ptr<const NameScope> target_scope =
      std::make_shared<NameScope>(column_name_list);
  ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(ast_generated_column->expression(),
                                    target_scope.get(), kComputedColumn,
                                    &resolved_expression));

  // If the type is provided, compare the declared type with the type of the
  // expression.
  if (opt_type != nullptr) {
    // Add coercion if necessary
    ZETASQL_RETURN_IF_ERROR(
        CoerceExprToType(ast_generated_column, opt_type, kImplicitAssignment,
                         "Generated column expression has type $1 which cannot "
                         "be assigned to column type $0",
                         &resolved_expression));
  }

  *output = MakeResolvedGeneratedColumnInfo(std::move(resolved_expression),
                                            stored_mode);

  return absl::OkStatus();
}

absl::Status Resolver::ResolveColumnDefaultExpression(
    const ASTExpression* ast_column_default, const Type* opt_type,
    bool skip_type_match_check,
    std::unique_ptr<ResolvedColumnDefaultValue>* default_value) {
  static constexpr char kDefaultColumn[] = "a column default expression";
  ZETASQL_RET_CHECK(default_expr_access_error_name_scope_.has_value());
  ZETASQL_RET_CHECK_NE(default_expr_access_error_name_scope_.value(), nullptr);
  std::unique_ptr<const ResolvedExpr> resolved_expression;
  ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(
      ast_column_default, default_expr_access_error_name_scope_.value(),
      kDefaultColumn, &resolved_expression));

  if (!skip_type_match_check) {
    if (opt_type == nullptr) {
      return MakeSqlErrorAt(ast_column_default)
             << "A column with default expression must have an explicit type.";
    }
    ZETASQL_RETURN_IF_ERROR(
        CoerceExprToType(ast_column_default, opt_type, kImplicitAssignment,
                         "Column default expression has type $1 "
                         "which cannot be assigned to column type $0",
                         &resolved_expression));
  }

  // Extract default value SQL string from AST.
  const ParseLocationRange& ast_default_expression_range =
      ast_column_default->GetParseLocationRange();
  ZETASQL_RET_CHECK_GE(sql_.length(),
               ast_default_expression_range.end().GetByteOffset())
      << sql_;
  absl::string_view sql = absl::ClippedSubstr(
      sql_, ast_default_expression_range.start().GetByteOffset(),
      ast_default_expression_range.end().GetByteOffset() -
          ast_default_expression_range.start().GetByteOffset());

  *default_value = MakeResolvedColumnDefaultValue(
      std::move(resolved_expression), std::string(sql));

  return absl::OkStatus();
}

absl::Status Resolver::ResolveColumnDefinitionList(
    IdString table_name_id_string,
    const absl::Span<const ASTColumnDefinition* const> ast_column_definitions,
    std::vector<std::unique_ptr<const ResolvedColumnDefinition>>*
        column_definition_list,
    ColumnIndexMap* column_indexes) {
  NameList column_name_list;

  std::unordered_map<IdString, const ASTColumnDefinition*, IdStringHash>
      id_to_column_definition_map;
  absl::node_hash_map<IdString, std::unique_ptr<const ResolvedColumnDefinition>,
                      IdStringHash>
      id_to_column_def_map;

  std::vector<const ASTColumnDefinition*> columns_with_expressions;
  std::vector<const ASTColumnDefinition*> columns_with_default_value;
  for (const auto& column : ast_column_definitions) {
    const IdString column_name = column->name()->GetAsIdString();
    zetasql_base::InsertIfNotPresent(&id_to_column_definition_map, column_name, column);
    if (!zetasql_base::InsertIfNotPresent(column_indexes, column_name,
                                 column_indexes->size())) {
      return MakeSqlErrorAt(column)
             << "Duplicate column name " << column_name << " in CREATE TABLE";
    }

    if (column->schema()->generated_column_info() != nullptr) {
      columns_with_expressions.push_back(column);
    } else if (column->schema()->default_expression() != nullptr) {
      columns_with_default_value.push_back(column);
    } else {
      // Resolve all non-generated columns, saving the remaining ones for later.
      // This is an optimization to make column resolution for generated columns
      // require a smaller number of retries.
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<const ResolvedColumnDefinition> column_definition,
          ResolveColumnDefinitionNoCache(column, table_name_id_string,
                                         &column_name_list));

      ZETASQL_RET_CHECK(id_to_column_def_map
                    .emplace(column_name, std::move(column_definition))
                    .second)
          << column_name;
    }
  }

  if (!columns_with_default_value.empty()) {
    std::vector<IdString> column_names;
    for (const auto& column : ast_column_definitions) {
      column_names.push_back(column->name()->GetAsIdString());
    }
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<NameScope> access_error_name_scope,
                     CreateNameScopeWithAccessErrorForDefaultExpr(
                         table_name_id_string, column_names));
    zetasql_base::VarSetter<absl::optional<const NameScope*>> var_setter(
        &default_expr_access_error_name_scope_, access_error_name_scope.get());
    // Resolve all columns with default expressions.
    for (const auto& column : columns_with_default_value) {
      const IdString column_name = column->name()->GetAsIdString();
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<const ResolvedColumnDefinition> column_definition,
          ResolveColumnDefinitionNoCache(column, table_name_id_string,
                                         &column_name_list));
      ZETASQL_RET_CHECK(column_definition->default_value() != nullptr);
      ZETASQL_RET_CHECK(id_to_column_def_map
                    .emplace(column_name, std::move(column_definition))
                    .second)
          << column_name;
    }
  }

  // Resolve generated columns.
  for (const auto& column : columns_with_expressions) {
    ColumnCycleDetector generated_column_cycle_detector(column);
    zetasql_base::VarSetter<ColumnCycleDetector*> setter_cycle_detector(
        &generated_column_cycle_detector_, &generated_column_cycle_detector);
    ZETASQL_RETURN_IF_ERROR(ResolveColumnDefinition(
        id_to_column_definition_map, &id_to_column_def_map, column,
        table_name_id_string, &column_name_list));
  }

  // Add the columns to column_definition_list in the proper order. It will also
  // transfer ownership of the ResolvedColumnDefinition from the
  // std::unordered_map to the std::vector.
  for (const auto& column : ast_column_definitions) {
    const IdString column_name = column->name()->GetAsIdString();
    auto it = id_to_column_def_map.find(column_name);
    ZETASQL_RET_CHECK(it != id_to_column_def_map.end()) << column_name;
    ZETASQL_RET_CHECK(it->second.get() != nullptr) << column_name;
    column_definition_list->push_back(std::move(it->second));
  }

  return absl::OkStatus();
}

absl::Status Resolver::ResolveColumnDefinition(
    const std::unordered_map<IdString, const ASTColumnDefinition*,
                             IdStringHash>& id_to_column_definition_map,
    absl::node_hash_map<IdString,
                        std::unique_ptr<const ResolvedColumnDefinition>,
                        IdStringHash>* id_to_column_def_map,
    const ASTColumnDefinition* column, const IdString& table_name_id_string,
    NameList* column_name_list) {
  ZETASQL_RET_CHECK(generated_column_cycle_detector_ != nullptr);

  const IdString column_name = column->name()->GetAsIdString();
  absl::Status finish_column_status;
  while (finish_column_status.ok()) {
    // Look if this column was already resolved.
    if (id_to_column_def_map->find(column_name) !=
        id_to_column_def_map->end()) {
      return absl::OkStatus();
    }

    ZETASQL_RETURN_IF_ERROR(
        generated_column_cycle_detector_->VisitNewColumn(column_name));
    auto finish_current_col = absl::MakeCleanup([this, &finish_column_status] {
      finish_column_status.Update(
          generated_column_cycle_detector_->FinishCurrentColumn());
    });

    // Clear any unresolved_column_name and try to resolve.
    unresolved_column_name_in_generated_column_.clear();
    absl::StatusOr<std::unique_ptr<const ResolvedColumnDefinition>> status_or =
        ResolveColumnDefinitionNoCache(column, table_name_id_string,
                                       column_name_list);
    if (status_or.ok()) {
      ZETASQL_RET_CHECK(id_to_column_def_map
                    ->emplace(column_name, std::move(status_or.value()))
                    .second)
          << column_name;
      return absl::OkStatus();
    }

    // If there is no unresolved column, this is another type of error, so we
    // return it.
    if (unresolved_column_name_in_generated_column_.empty()) {
      return status_or.status();
    }
    // Column was not resolved yet, so let's look for the table element.
    // When there is a unresolved column in
    // unresolved_column_name_in_generated_column_ the status_or will be
    // INVALID_ARGUMENT.
    ZETASQL_RET_CHECK_EQ(status_or.status().code(), absl::StatusCode::kInvalidArgument);
    const ASTColumnDefinition* const* column_reference =
        zetasql_base::FindOrNull(id_to_column_definition_map,
                        unresolved_column_name_in_generated_column_);
    if (column_reference == nullptr) {
      return status_or.status();
    }
    // ResolvedColumnDefinition was not cached, but ASTTableElement was found,
    // so let's try resolving it.
    // The below code is explicitly overriding a !ok() status, that's how the
    // column resolution works in generated columns, it will try resolving, if
    // it finds a column not resolved yet, it returns !ok(), tries to resolve
    // the missing dependency (unresolved_column_name_in_generated_column_)
    // in the next statement, and if succeeded it continues the resolution.

    // Use the new error message as the old one might be something like
    // "Cannot find column a", whereas the new one will be "Cycle
    // detected".
    finish_column_status = ResolveColumnDefinition(
        id_to_column_definition_map, id_to_column_def_map, *column_reference,
        table_name_id_string, column_name_list);

    // If ResolveColumnDefinition() succeeded, the sub-expression was
    // successfully evaluated, so let's try to resolve 'column_name' again on
    // the next iteration of the loop.
  }

  return finish_column_status;
}

absl::StatusOr<std::unique_ptr<const ResolvedColumnDefinition>>
Resolver::ResolveColumnDefinitionNoCache(const ASTColumnDefinition* column,
                                         const IdString& table_name_id_string,
                                         NameList* column_name_list) {
  const Type* type = nullptr;
  std::unique_ptr<const ResolvedColumnAnnotations> annotations;
  std::unique_ptr<ResolvedGeneratedColumnInfo> generated_column_info;
  std::unique_ptr<ResolvedColumnDefaultValue> default_value;
  // std::unique_ptr<const ResolvedExpr> column_default_expr;
  // std::string column_default_expr_sql;
  ZETASQL_RETURN_IF_ERROR(ResolveColumnSchema(column->schema(), *column_name_list,
                                      &type, &annotations,
                                      &generated_column_info, &default_value));

  // Update column_name_list so that it can be used by the
  // ResolveGeneratedColumnInfo().
  const IdString column_name = column->name()->GetAsIdString();
  ResolvedColumn defined_column(AllocateColumnId(), table_name_id_string,
                                column_name, type);
  ZETASQL_RETURN_IF_ERROR(column_name_list->AddColumn(column_name, defined_column,
                                              /* is_explicit = */ true));

  std::unique_ptr<const ResolvedColumnDefinition> resolved_column =
      MakeResolvedColumnDefinition(
          column_name.ToString(), type, std::move(annotations),
          column->schema()->ContainsAttribute(AST_HIDDEN_COLUMN_ATTRIBUTE),
          defined_column, std::move(generated_column_info),
          std::move(default_value));
  return resolved_column;
}

absl::Status Resolver::ResolveColumnSchema(
    const ASTColumnSchema* schema, const NameList& column_name_list,
    const Type** resolved_type,
    std::unique_ptr<const ResolvedColumnAnnotations>* annotations,
    std::unique_ptr<ResolvedGeneratedColumnInfo>* generated_column_info,
    std::unique_ptr<ResolvedColumnDefaultValue>* default_value) {
  const ASTColumnAttributeList* attributes = schema->attributes();
  if (annotations == nullptr) {
    if (attributes != nullptr) {
      return MakeSqlErrorAt(attributes)
          << "Nested column attributes are unsupported";
    }
    if (schema->options_list() != nullptr) {
      return MakeSqlErrorAt(schema->options_list())
          << "Nested column options are unsupported";
    }
  }
  ZETASQL_RETURN_IF_ERROR(ValidateColumnAttributeList(attributes));
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_column_options;
  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(schema->options_list(), &resolved_column_options));
  std::vector<std::unique_ptr<const ResolvedColumnAnnotations>>
      child_annotation_list;
  const bool enable_nested_annotations =
      annotations != nullptr &&
          language().LanguageFeatureEnabled(
              FEATURE_CREATE_TABLE_FIELD_ANNOTATIONS);
  const ASTTypeParameterList* ast_type_parameters = schema->type_parameters();
  if (ast_type_parameters != nullptr &&
      !language().LanguageFeatureEnabled(FEATURE_PARAMETERIZED_TYPES)) {
    return MakeSqlErrorAt(ast_type_parameters)
           << "Parameterized types are not supported";
  }

  const ASTCollate* ast_collate = schema->collate();
  if (ast_collate != nullptr &&
      !language().LanguageFeatureEnabled(FEATURE_V_1_3_COLLATION_SUPPORT)) {
    return MakeSqlErrorAt(ast_collate)
           << "COLLATE is not supported";
  }

  switch (schema->node_kind()) {
    case AST_SIMPLE_COLUMN_SCHEMA: {
      const auto* simple_schema = schema->GetAsOrDie<ASTSimpleColumnSchema>();
      ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsType(
          simple_schema->type_name(),
          /* is_single_identifier = */ false,
          resolved_type));
      break;
    }

    case AST_ARRAY_COLUMN_SCHEMA: {
      const auto* array_schema = schema->GetAsOrDie<ASTArrayColumnSchema>();
      const Type* resolved_element_type = nullptr;
      std::unique_ptr<const ResolvedColumnAnnotations> element_annotations;

      ZETASQL_RETURN_IF_ERROR(ResolveColumnSchema(
          array_schema->element_schema(), NameList(), &resolved_element_type,
          enable_nested_annotations ? &element_annotations : nullptr,
          /*generated_column_info=*/nullptr, /*default_value=*/nullptr));

      if (resolved_element_type->IsArray()) {
        return MakeSqlErrorAt(array_schema)
            << "Arrays of arrays are not supported";
      }
      ZETASQL_RETURN_IF_ERROR(type_factory_->MakeArrayType(resolved_element_type,
                                                   resolved_type));
      if (element_annotations != nullptr) {
        child_annotation_list.push_back(std::move(element_annotations));
      }
      break;
    }

    case AST_STRUCT_COLUMN_SCHEMA: {
      const auto* struct_schema = schema->GetAsOrDie<ASTStructColumnSchema>();
      std::vector<StructType::StructField> struct_fields;

      int index = 0;
      for (const ASTStructColumnField* struct_field :
           struct_schema->struct_fields()) {
        const Type* field_type;
        std::unique_ptr<const ResolvedColumnAnnotations> field_annotations;
        ZETASQL_RETURN_IF_ERROR(ResolveColumnSchema(
            struct_field->schema(), NameList(), &field_type,
            enable_nested_annotations ? &field_annotations : nullptr,
            /*generated_column_info=*/nullptr,
            /*default_value=*/nullptr));

        struct_fields.emplace_back(
            struct_field->name() != nullptr ?
                struct_field->name()->GetAsString() : "",
            field_type);
        if (field_annotations != nullptr) {
          child_annotation_list.resize(index + 1);
          child_annotation_list[index] = std::move(field_annotations);
        }
        ++index;
      }

      ZETASQL_RETURN_IF_ERROR(type_factory_->MakeStructType(struct_fields,
                                                    resolved_type));
      for (std::unique_ptr<const ResolvedColumnAnnotations>& child :
          child_annotation_list) {
        if (child == nullptr) {
          // All children must be non-null, otherwise Resolver::PruneColumnLists
          // will crash when calling GetDescendantsSatisfying.
          child = MakeResolvedColumnAnnotations();
        }
      }
      break;
    }
    case AST_INFERRED_TYPE_COLUMN_SCHEMA: {
      ZETASQL_RET_CHECK(ast_type_parameters == nullptr)
          << "Type inferred from generation clause can't have type parameters";
      break;
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << schema->DebugString();
  }

  // Resolve type parameters if there are any.
  TypeParameters resolved_type_parameters;
  if (ast_type_parameters != nullptr) {
    // Resolve each type parameter value for the simple type.
    ZETASQL_ASSIGN_OR_RETURN(
        std::vector<TypeParameterValue> resolved_type_parameter_literals,
        ResolveParameterLiterals(*ast_type_parameters));
    absl::StatusOr<TypeParameters> resolved_type_parameters_or_error =
        (*resolved_type)
            ->ValidateAndResolveTypeParameters(resolved_type_parameter_literals,
                                               product_mode());
    if (!resolved_type_parameters_or_error.ok()) {
      // We assume INVALID_ARGUMENT is never returned by
      // ValidateAndResolveTypeParameters for reasons other than MakeSqlError().
      if (absl::IsInvalidArgument(resolved_type_parameters_or_error.status())) {
        return MakeSqlErrorAt(ast_type_parameters)
               << resolved_type_parameters_or_error.status().message();
      }
      return resolved_type_parameters_or_error.status();
    }
    resolved_type_parameters = *resolved_type_parameters_or_error;
  }

  std::unique_ptr<const ResolvedExpr> resolved_collation;
  if (ast_collate != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateAndResolveCollate(
        ast_collate, ast_collate, *resolved_type, &resolved_collation));
  }

  if (schema->generated_column_info() != nullptr) {
    ZETASQL_RET_CHECK(schema->default_expression() == nullptr);
    ZETASQL_RET_CHECK(generated_column_info != nullptr);
    if (!language().LanguageFeatureEnabled(FEATURE_V_1_2_GENERATED_COLUMNS)) {
      return MakeSqlErrorAt(schema->generated_column_info())
             << "Generated columns are not supported";
    }
    ZETASQL_RETURN_IF_ERROR(ResolveGeneratedColumnInfo(schema->generated_column_info(),
                                               column_name_list, *resolved_type,
                                               generated_column_info));
    if (*resolved_type == nullptr) {
      // Propagates the type from the expression into the ColumnDefinition.
      *resolved_type = (*generated_column_info)->expression()->type();
    }
  }

  if (schema->default_expression() != nullptr) {
    ZETASQL_RET_CHECK(schema->generated_column_info() == nullptr);
    ZETASQL_RET_CHECK(default_value != nullptr);
    if (!language().LanguageFeatureEnabled(
            FEATURE_V_1_3_COLUMN_DEFAULT_VALUE)) {
      return MakeSqlErrorAt(schema->generated_column_info())
             << "Column DEFAULT value is not supported";
    }
    ZETASQL_RETURN_IF_ERROR(ResolveColumnDefaultExpression(
        schema->default_expression(), *resolved_type,
        /*skip_type_match_check=*/false, default_value));
  }

  const bool not_null =
      schema->ContainsAttribute(AST_NOT_NULL_COLUMN_ATTRIBUTE);

  if ((resolved_collation != nullptr) || not_null ||
      !resolved_column_options.empty() || !child_annotation_list.empty() ||
      !resolved_type_parameters.IsEmpty()) {
    ZETASQL_RET_CHECK(annotations != nullptr);
    *annotations = MakeResolvedColumnAnnotations(
        std::move(resolved_collation), not_null,
        std::move(resolved_column_options), std::move(child_annotation_list),
        resolved_type_parameters);
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolvePrimaryKey(
    const ColumnIndexMap& column_indexes,
    const ASTPrimaryKey* ast_primary_key,
    std::unique_ptr<ResolvedPrimaryKey>* resolved_primary_key) {
  ZETASQL_RET_CHECK(!column_indexes.empty());
  if (!analyzer_options_.language().LanguageFeatureEnabled(
          FEATURE_UNENFORCED_PRIMARY_KEYS) &&
      !ast_primary_key->enforced()) {
    return MakeSqlErrorAt(ast_primary_key)
           << "NOT ENFORCED primary key table constraints are unsupported";
  }
  std::vector<int> column_index_list;
  std::vector<std::string> column_name_list;
  if (ast_primary_key->column_list() != nullptr) {
    std::set<IdString, IdStringCaseLess> used_primary_key_columns;
    for (const auto& identifier :
         ast_primary_key->column_list()->identifiers()) {
      const IdString primary_key_column = identifier->GetAsIdString();
      if (zetasql_base::ContainsKey(used_primary_key_columns, primary_key_column)) {
        return MakeSqlErrorAt(identifier) << "Duplicate column "
            << primary_key_column << " specified in PRIMARY KEY of CREATE "
            << "TABLE";
      }
      used_primary_key_columns.insert(primary_key_column);
      column_name_list.push_back(primary_key_column.ToString());
      if (!column_indexes.empty()) {
        const int* column_index =
            zetasql_base::FindOrNull(column_indexes, primary_key_column);
        if (column_index == nullptr) {
          return MakeSqlErrorAt(identifier)
                 << "Unsupported primary key column " << primary_key_column
                 << " either does not exist or is a pseudocolumn";
        }

        column_index_list.push_back(*column_index);
      }
    }
  }
  std::vector<std::unique_ptr<const ResolvedOption>> options;
  ZETASQL_RETURN_IF_ERROR(ResolveOptionsList(ast_primary_key->options_list(),
                                     &options));

  std::string constraint_name;
  if (ast_primary_key->constraint_name() != nullptr) {
    constraint_name = ast_primary_key->constraint_name()->GetAsString();
  }

  *resolved_primary_key =
      MakeResolvedPrimaryKey(column_index_list, std::move(options),
                             /*unenforced=*/!ast_primary_key->enforced(),
                             constraint_name, column_name_list);
  return absl::OkStatus();
}

absl::Status Resolver::ResolvePrimaryKey(
    const absl::Span<const ASTTableElement* const> table_elements,
    const ColumnIndexMap& column_indexes,
    std::unique_ptr<ResolvedPrimaryKey>* resolved_primary_key) {
  const std::string multiple_primary_keys_error(
      "Multiple PRIMARY KEY definitions found in CREATE TABLE");

  for (const auto& table_element : table_elements) {
    if (table_element->node_kind() == AST_PRIMARY_KEY) {
      if (*resolved_primary_key != nullptr) {
        return MakeSqlErrorAt(table_element) << multiple_primary_keys_error;
      }
      ZETASQL_RETURN_IF_ERROR(ResolvePrimaryKey(
          column_indexes, static_cast<const ASTPrimaryKey*>(table_element),
          resolved_primary_key));
    } else if (table_element->node_kind() == AST_COLUMN_DEFINITION) {
      const auto* column = static_cast<const ASTColumnDefinition*>(table_element);
      std::vector<const ASTPrimaryKeyColumnAttribute*> primary_key =
          column->schema()->FindAttributes<ASTPrimaryKeyColumnAttribute>(
              AST_PRIMARY_KEY_COLUMN_ATTRIBUTE);
      if (!primary_key.empty()) {
        if (*resolved_primary_key != nullptr || primary_key.size() > 1) {
          return MakeSqlErrorAt(column) << multiple_primary_keys_error;
        }
        const ASTPrimaryKeyColumnAttribute* attribute = primary_key.front();
        if (!analyzer_options_.language().LanguageFeatureEnabled(
                FEATURE_UNENFORCED_PRIMARY_KEYS) &&
            !attribute->enforced()) {
          return MakeSqlErrorAt(attribute) << "NOT ENFORCED primary key column "
                                              "constraints are unsupported";
        }
        const int* column_index =
            zetasql_base::FindOrNull(column_indexes, column->name()->GetAsIdString());
        ZETASQL_RET_CHECK(column_index != nullptr);

        // When a column definition is marked as the primary key, the primary
        // key constraint cannot be named.
        *resolved_primary_key = MakeResolvedPrimaryKey(
            {*column_index}, /*option_list=*/{},
            /*unenforced=*/!attribute->enforced(),
            /*constraint_name=*/"", {column->name()->GetAsString()});
      }
    }
  }

  return absl::OkStatus();
}

absl::Status Resolver::ResolveForeignKeys(
    const absl::Span<const ASTTableElement* const> ast_table_elements,
    const ColumnIndexMap& column_indexes,
    const std::vector<std::unique_ptr<const ResolvedColumnDefinition>>&
        column_definitions,
    std::set<std::string, zetasql_base::CaseLess>* constraint_names,
    std::vector<std::unique_ptr<const ResolvedForeignKey>>* foreign_key_list) {
  std::vector<const Type*> column_types;
  column_types.reserve(column_definitions.size());
  for (const auto& column : column_definitions) {
    column_types.push_back(column->type());
  }
  for (const auto& ast_table_element : ast_table_elements) {
    std::vector<std::unique_ptr<ResolvedForeignKey>> foreign_keys;
    std::vector<const ASTNode*> ast_foreign_key_nodes;
    if (ast_table_element->node_kind() == AST_FOREIGN_KEY) {
      ZETASQL_RETURN_IF_ERROR(ResolveForeignKeyTableConstraint(
          column_indexes, column_types,
          static_cast<const ASTForeignKey*>(ast_table_element), &foreign_keys));
      ast_foreign_key_nodes.push_back(ast_table_element);
    } else if (ast_table_element->node_kind() == AST_COLUMN_DEFINITION) {
      const auto* column =
          static_cast<const ASTColumnDefinition*>(ast_table_element);
      std::vector<const ASTForeignKeyColumnAttribute*> attributes =
          column->schema()->FindAttributes<ASTForeignKeyColumnAttribute>(
              AST_FOREIGN_KEY_COLUMN_ATTRIBUTE);
      for (const auto& attribute : attributes) {
        ZETASQL_RETURN_IF_ERROR(ResolveForeignKeyColumnConstraint(
            column_indexes, column_types, column, attribute, &foreign_keys));
        ast_foreign_key_nodes.push_back(attribute);
      }
    }
    ZETASQL_DCHECK_EQ(foreign_keys.size(), ast_foreign_key_nodes.size());
    for (int i = 0; i < foreign_keys.size(); i++) {
      auto& foreign_key = foreign_keys[i];
      const auto& constraint_name = foreign_key->constraint_name();
      if (!constraint_name.empty() &&
          !constraint_names->insert(constraint_name).second) {
        return MakeSqlErrorAt(ast_foreign_key_nodes[i])
               << "Duplicate constraint name " << constraint_name;
      }
      foreign_key_list->push_back(std::move(foreign_key));
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveForeignKeyColumnConstraint(
    const ColumnIndexMap& column_indexes,
    const std::vector<const Type*>& column_types,
    const ASTColumnDefinition* ast_column_definition,
    const ASTForeignKeyColumnAttribute* ast_foreign_key,
    std::vector<std::unique_ptr<ResolvedForeignKey>>* resolved_foreign_keys) {
  if (!language().LanguageFeatureEnabled(FEATURE_FOREIGN_KEYS)) {
    return MakeSqlErrorAt(ast_foreign_key) << "Foreign keys are not supported";
  }
  auto foreign_key = MakeResolvedForeignKey();

  // CONSTRAINT name.
  if (ast_foreign_key->constraint_name() != nullptr) {
    foreign_key->set_constraint_name(
        ast_foreign_key->constraint_name()->GetAsString());
  }

  // REFERENCES table referenced_columns.
  if (ast_foreign_key->reference()->column_list()->identifiers().size() != 1) {
    return MakeSqlErrorAt(ast_foreign_key->reference()->column_list())
        << "Foreign key definition must include exactly one column name";
  }
  const ASTIdentifier* ast_referencing_column_identifiers[]
      {ast_column_definition->name()};
  ZETASQL_RETURN_IF_ERROR(ResolveForeignKeyReference(
      column_indexes, column_types,
      absl::MakeSpan(ast_referencing_column_identifiers, 1),
      ast_foreign_key->reference(), foreign_key.get()));

  resolved_foreign_keys->push_back(std::move(foreign_key));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveForeignKeyTableConstraint(
    const ColumnIndexMap& column_indexes,
    const std::vector<const Type*>& column_types,
    const ASTForeignKey* ast_foreign_key,
    std::vector<std::unique_ptr<ResolvedForeignKey>>* resolved_foreign_keys) {
  if (!language().LanguageFeatureEnabled(FEATURE_FOREIGN_KEYS)) {
    return MakeSqlErrorAt(ast_foreign_key) << "Foreign keys are not supported";
  }
  auto foreign_key = MakeResolvedForeignKey();

  // CONSTRAINT name.
  if (ast_foreign_key->constraint_name() != nullptr) {
    foreign_key->set_constraint_name(
        ast_foreign_key->constraint_name()->GetAsString());
  }

  // FOREIGN KEY referencing_columns REFERENCES table referenced_columns.
  ZETASQL_RETURN_IF_ERROR(ResolveForeignKeyReference(
      column_indexes, column_types,
      ast_foreign_key->column_list()->identifiers(),
      ast_foreign_key->reference(), foreign_key.get()));

  // OPTIONS options.
  std::vector<std::unique_ptr<const ResolvedOption>> options;
  ZETASQL_RETURN_IF_ERROR(ResolveOptionsList(ast_foreign_key->options_list(),
                                     &options));
  for (auto& option : options) {
    foreign_key->add_option_list(std::move(option));
  }

  resolved_foreign_keys->push_back(std::move(foreign_key));
  return absl::OkStatus();
}

static ResolvedForeignKey::MatchMode GetForeignKeyMatchMode(
    ASTForeignKeyReference::Match ast_match_mode) {
  switch (ast_match_mode) {
    case ASTForeignKeyReference::SIMPLE:
      return ResolvedForeignKey::SIMPLE;
    case ASTForeignKeyReference::FULL:
      return ResolvedForeignKey::FULL;
    case ASTForeignKeyReference::NOT_DISTINCT:
      return ResolvedForeignKey::NOT_DISTINCT;
  }
}

static ResolvedForeignKey::ActionOperation GetForeignKeyActionOperation(
    ASTForeignKeyActions::Action ast_actions) {
  switch (ast_actions) {
    case ASTForeignKeyActions::NO_ACTION:
      return ResolvedForeignKey::NO_ACTION;
    case ASTForeignKeyActions::RESTRICT:
      return ResolvedForeignKey::RESTRICT;
    case ASTForeignKeyActions::CASCADE:
      return ResolvedForeignKey::CASCADE;
    case ASTForeignKeyActions::SET_NULL:
      return ResolvedForeignKey::SET_NULL;
  }
}

absl::Status Resolver::ResolveForeignKeyReference(
    const ColumnIndexMap& column_indexes,
    const std::vector<const Type*>& column_types,
    const absl::Span<const ASTIdentifier* const>
        ast_referencing_column_identifiers,
    const ASTForeignKeyReference* ast_foreign_key_reference,
    ResolvedForeignKey* foreign_key) {
  const auto* ast_referenced_columns = ast_foreign_key_reference->column_list();
  const auto ast_referenced_column_identifiers =
      ast_referenced_columns->identifiers();

  ZETASQL_RET_CHECK(!ast_referencing_column_identifiers.empty());
  if (ast_referencing_column_identifiers.size()
      != ast_referenced_column_identifiers.size()) {
    return MakeSqlErrorAt(ast_foreign_key_reference->column_list())
        << "Number of foreign columns does not match the number of "
        << "referenced columns";
  }

  const Table* referenced_table = nullptr;
  ZETASQL_RETURN_IF_ERROR(FindTable(
      ast_foreign_key_reference->table_name(), &referenced_table));
  foreign_key->set_referenced_table(referenced_table);
  const std::string referenced_table_name(referenced_table->Name());

  std::set<std::string, zetasql_base::CaseLess>
      referencing_column_names;
  std::set<std::string, zetasql_base::CaseLess>
      referenced_column_names;
  for (int i = 0; i < ast_referencing_column_identifiers.size(); i++) {
    const auto* ast_referencing_column_identifier =
        ast_referencing_column_identifiers[i];
    const auto* ast_referenced_column_identifier =
        ast_referenced_column_identifiers[i];
    const auto referencing_column_name(
        ast_referencing_column_identifier->GetAsString());
    const auto referenced_column_name(
        ast_referenced_column_identifier->GetAsString());
    if (!referencing_column_names.insert(referencing_column_name).second) {
      return MakeSqlErrorAt(ast_referencing_column_identifier)
          << "Duplicate foreign key column name";
    }
    if (!referenced_column_names.insert(referenced_column_name).second) {
      return MakeSqlErrorAt(ast_referenced_column_identifier)
          << "Duplicate referenced column name";
    }

    const int* referencing_column_offset = zetasql_base::FindOrNull(
        column_indexes, ast_referencing_column_identifier->GetAsIdString());
    // The offset may be null if the referecing column is a pseudocolumn.
    if (referencing_column_offset == nullptr) {
      return MakeSqlErrorAt(ast_referencing_column_identifier)
             << "Unsupported foreign key column "
             << ast_referencing_column_identifier->GetAsIdString()
             << " either does not exist or is a pseudocolumn";
    }
    ZETASQL_RET_CHECK(*referencing_column_offset < column_types.size());
    foreign_key->add_referencing_column_offset_list(
        *referencing_column_offset);

    int referenced_column_offset = -1;
    bool duplicate_referenced_column_name = true;
    FindColumnIndex(referenced_table, referenced_column_name,
                    &referenced_column_offset,
                    &duplicate_referenced_column_name);
    if (referenced_column_offset < 0) {
      return MakeSqlErrorAt(ast_referenced_column_identifier)
          << "Column name " << referenced_column_name
          << " not found in " << referenced_table_name;
    }
    if (duplicate_referenced_column_name) {
      return MakeSqlErrorAt(ast_referenced_column_identifier)
          << "Column name " << referenced_column_name
          << " found more than once in " << referenced_table_name;
    }
    foreign_key->add_referenced_column_offset_list(referenced_column_offset);

    const Type* referencing_type = column_types[*referencing_column_offset];
    const Type* referenced_type =
        referenced_table->GetColumn(referenced_column_offset)->GetType();
    ZETASQL_ASSIGN_OR_RETURN(const bool supports_equality,
                     SupportsEquality(referencing_type, referenced_type));
    if (!supports_equality) {
      if (!referencing_type->SupportsEquality(analyzer_options_.language())) {
        return MakeSqlErrorAt(ast_referencing_column_identifier)
            << "The type of the referencing column " << referencing_column_name
            << " does not support equality ('=') and therefore is not"
            << " compatible with foreign keys";
      }
      return MakeSqlErrorAt(ast_referenced_column_identifier)
          << "Referenced column " << referenced_column_name
          << " from " << referenced_table_name
          << " is not compatible with the referencing column "
          << referencing_column_name;
    }
  }

  foreign_key->set_match_mode(GetForeignKeyMatchMode(
      ast_foreign_key_reference->match()));
  foreign_key->set_update_action(GetForeignKeyActionOperation(
      ast_foreign_key_reference->actions()->update_action()));
  foreign_key->set_delete_action(GetForeignKeyActionOperation(
      ast_foreign_key_reference->actions()->delete_action()));
  foreign_key->set_enforced(ast_foreign_key_reference->enforced());

  for (const auto* column : ast_referencing_column_identifiers) {
    foreign_key->add_referencing_column_list(column->GetAsString());
  }

  return absl::OkStatus();
}

absl::Status Resolver::ResolveCheckConstraints(
    absl::Span<const ASTTableElement* const> ast_table_elements,
    const NameScope& name_scope,
    std::set<std::string, zetasql_base::CaseLess>* constraint_names,
    std::vector<std::unique_ptr<const ResolvedCheckConstraint>>*
        check_constraint_list) {
  static constexpr char kCheckConstraintClause[] = "CHECK constraint";
  for (const auto* table_element : ast_table_elements) {
    if (table_element->node_kind() == AST_CHECK_CONSTRAINT) {
      if (!language().LanguageFeatureEnabled(FEATURE_CHECK_CONSTRAINT)) {
        return MakeSqlErrorAt(table_element)
               << "CHECK constraints are not supported";
      }
      const auto* ast_check_constraint =
          table_element->GetAsOrDie<ASTCheckConstraint>();
      std::string constraint_name;
      if (ast_check_constraint->constraint_name() != nullptr) {
        constraint_name =
            ast_check_constraint->constraint_name()->GetAsString();
        if (!constraint_name.empty() &&
            !constraint_names->insert(constraint_name).second) {
          return MakeSqlErrorAt(table_element)
                 << "Duplicate constraint name " << constraint_name;
        }
      }
      std::unique_ptr<const ResolvedExpr> resolved_expr;
      {
        zetasql_base::VarSetter<bool> setter(&analyzing_check_constraint_expression_, true);
        ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(ast_check_constraint->expression(),
                                          &name_scope, kCheckConstraintClause,
                                          &resolved_expr));
      }
      if (!resolved_expr->type()->IsBool()) {
        return MakeSqlErrorAt(ast_check_constraint->expression())
               << "CHECK constraint expects a boolean expression";
      }
      std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
      ZETASQL_RETURN_IF_ERROR(ResolveOptionsList(ast_check_constraint->options_list(),
                                         &resolved_options));

      auto resolved_check_constraint = MakeResolvedCheckConstraint(
          constraint_name, std::move(resolved_expr),
          ast_check_constraint->is_enforced(), std::move(resolved_options));
      check_constraint_list->push_back(std::move(resolved_check_constraint));
    }
  }

  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateTablePartitionByList(
    absl::Span<const ASTExpression* const> expressions,
    PartitioningKind partitioning_kind, const NameScope& name_scope,
    QueryResolutionInfo* query_info,
    std::vector<std::unique_ptr<const ResolvedExpr>>* partition_by_list_out) {
  ZETASQL_RET_CHECK(!expressions.empty());
  const char* const clause_name =
      (partitioning_kind == PartitioningKind::PARTITION_BY) ? "PARTITION BY"
                                                            : "CLUSTER BY";

  ExprResolutionInfo resolution_info(
      &name_scope, /*aggregate_name_scope_in=*/&name_scope,
      /*allows_aggregation_in=*/false, /*allows_analytic_in=*/false,
      /*use_post_grouping_columns_in=*/false, clause_name, query_info);

  for (const ASTExpression* expression : expressions) {
    std::unique_ptr<const ResolvedExpr> resolved_expression;
    {
      ZETASQL_RET_CHECK(analyzing_partition_by_clause_name_ == nullptr);
      zetasql_base::VarSetter<const char*> setter(&analyzing_partition_by_clause_name_,
                                    clause_name);
      ZETASQL_RETURN_IF_ERROR(
          ResolveExpr(expression, &resolution_info, &resolved_expression));
    }
    if (resolved_expression->type()->IsFloatingPoint()) {
      return MakeSqlErrorAt(expression)
             << clause_name << " expression may not be a floating point type";
    }
    // We do not support partition by Geography, but we allow clustering by it.
    // Clustering is possible because clustering actually uses ordering, not
    // grouping. Still, Geography does not support ordering either - we cannot
    // say if g1 < g2. We can however map geography to an index (S2 CellId id),
    // and use it for clustering in a meaningful way. The use case does not seem
    // to be large enough to call for adding Type::SupportsClustering.
    const bool is_geography_clustering =
        partitioning_kind == PartitioningKind::CLUSTER_BY &&
        resolved_expression->type()->IsGeography();
    std::string no_grouping_type;
    if (!is_geography_clustering &&
        !TypeSupportsGrouping(resolved_expression->type(), &no_grouping_type)) {
      return MakeSqlErrorAt(expression)
             << clause_name << " expression must be groupable, but type is "
             << no_grouping_type;
    }
    ZETASQL_ASSIGN_OR_RETURN(bool resolved_expression_is_constant_expr,
                     IsConstantExpression(resolved_expression.get()));
    if (resolved_expression_is_constant_expr) {
      return MakeSqlErrorAt(expression)
             << clause_name << " expression must not be constant";
    }
    partition_by_list_out->push_back(std::move(resolved_expression));
  }

  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateDatabaseStatement(
    const ASTCreateDatabaseStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(ast_statement->options_list(), &resolved_options));
  const std::vector<std::string> database_name =
      ast_statement->name()->ToIdentifierVector();
  *output = MakeResolvedCreateDatabaseStmt(database_name,
                                           std::move(resolved_options));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateSchemaStatement(
    const ASTCreateSchemaStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  std::unique_ptr<const ResolvedExpr> resolved_collation;
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  if (ast_statement->collate() != nullptr) {
    if (!language().LanguageFeatureEnabled(FEATURE_V_1_3_COLLATION_SUPPORT)) {
      return MakeSqlErrorAt(ast_statement->collate())
           << "CREATE SCHEMA with DEFAULT COLLATE is not supported";
    }
    ZETASQL_RETURN_IF_ERROR(ValidateAndResolveDefaultCollate(
        /*ast_collate=*/ast_statement->collate(),
        /*ast_location=*/ast_statement->collate(), &resolved_collation));
  }
  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(ast_statement, "CREATE SCHEMA",
                                                &create_scope, &create_mode));
  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(ast_statement->options_list(), &resolved_options));
  *output = MakeResolvedCreateSchemaStmt(
      ast_statement->name()->ToIdentifierVector(), create_scope, create_mode,
      std::move(resolved_collation), std::move(resolved_options));
  return absl::OkStatus();
}

absl::Status Resolver::ValidateIndexKeyExpressionForCreateSearchIndex(
    const ASTOrderingExpression& ordering_expression,
    const ResolvedExpr& resolved_expr) {
  if (ordering_expression.ordering_spec() !=
      ASTOrderingExpression::UNSPECIFIED) {
    return MakeSqlErrorAt(&ordering_expression)
           << "Key expression with ASC or DESC option for "
           << "CREATE SEARCH INDEX is not allowed";
  }
  if (ordering_expression.null_order() != nullptr) {
    return MakeSqlErrorAt(&ordering_expression)
           << "Key expression with NULL order option for "
           << "CREATE SEARCH INDEX is not allowed";
  }

  if (resolved_expr.node_kind() != RESOLVED_COLUMN_REF) {
    return MakeSqlErrorAt(&ordering_expression)
           << "CREATE SEARCH INDEX does not yet support expressions to define "
           << "index keys, only column name is supported";
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateIndexStatement(
    const ASTCreateIndexStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  const ASTPathExpression* table_path = ast_statement->table_name();
  IdString table_alias;
  const ASTNode* table_alias_location = nullptr;
  bool has_explicit_table_name_alias = true;
  if (ast_statement->optional_table_alias() != nullptr) {
    table_alias = ast_statement->optional_table_alias()->GetAsIdString();
    table_alias_location = ast_statement->optional_table_alias();
  } else {
    has_explicit_table_name_alias = false;
    table_alias = GetAliasForExpression(table_path);
    table_alias_location = ast_statement->table_name();
  }

  std::shared_ptr<const NameList> target_name_list(new NameList);
  std::unique_ptr<const ResolvedTableScan> resolved_table_scan;
  ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsTableScan(
      table_path, table_alias, has_explicit_table_name_alias,
      /*alias_location=*/table_alias_location, /*hints=*/nullptr,
      /*for_system_time=*/nullptr, empty_name_scope_.get(),
      &resolved_table_scan, &target_name_list));

  NameList current_name_list;
  ZETASQL_RETURN_IF_ERROR(current_name_list.MergeFrom(*target_name_list, table_path));

  std::vector<std::unique_ptr<const ResolvedUnnestItem>> resolved_unnest_items;
  // Resolve the unnest expressions if there are any.
  if (ast_statement->optional_index_unnest_expression_list() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveIndexUnnestExpressions(
        ast_statement->optional_index_unnest_expression_list(),
        &current_name_list, &resolved_unnest_items));
  }
  NameScope name_scope(current_name_list);
  // Resolve the referred columns in index keys or storing columns.
  std::set<IdString, IdStringCaseLess> resolved_columns;
  std::vector<std::unique_ptr<const ResolvedIndexItem>> resolved_index_items;
  // Resolve the computed columns in the index.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      resolved_computed_columns;
  bool index_all_columns = false;
  for (const auto* ordering_expression :
       ast_statement->index_item_list()->ordering_expressions()) {
    if (ordering_expression->expression()->node_kind() ==
        AST_INDEX_ALL_COLUMNS) {
      if (!ast_statement->is_search()) {
        return MakeSqlErrorAt(ordering_expression)
               << "'ALL COLUMNS' is not a supported index key expression for "
               << "CREATE INDEX statement";
      }
      ZETASQL_RET_CHECK(
          ast_statement->index_item_list()->ordering_expressions().size() == 1);
      index_all_columns = true;
      break;
    }
    if (ordering_expression->expression()->node_kind() != AST_PATH_EXPRESSION) {
      return MakeSqlErrorAt(ordering_expression)
             << "Non-path index key expression for CREATE INDEX is not "
             << "supported yet";
    }
    const auto* path_expression =
        ordering_expression->expression()->GetAsOrDie<ASTPathExpression>();

    ZETASQL_RET_CHECK_GT(path_expression->num_names(), 0);
    std::unique_ptr<const ResolvedExpr> resolved_expr;
    ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(path_expression, &name_scope,
                                      /*clause_name=*/"INDEX Key Items",
                                      &resolved_expr));
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExprForCreateIndex(
        ast_statement, path_expression, &resolved_columns,
        resolved_expr.get()));
    if (ast_statement->is_search()) {
      ZETASQL_RETURN_IF_ERROR(ValidateIndexKeyExpressionForCreateSearchIndex(
          *ordering_expression, *resolved_expr.get()));
    }
    switch (resolved_expr->node_kind()) {
      case RESOLVED_COLUMN_REF: {
        // If resolved_expr is already resolved_column_ref, we can use that
        // directly.
        const ResolvedColumnRef* column_ref =
            resolved_expr.release()->GetAs<ResolvedColumnRef>();
        resolved_index_items.push_back(MakeResolvedIndexItem(
            absl::WrapUnique(column_ref), ordering_expression->descending()));
        break;
      }
      case RESOLVED_GET_PROTO_FIELD:
      case RESOLVED_GET_STRUCT_FIELD: {
        ResolvedColumn resolved_column(
            AllocateColumnId(), /*table_name=*/table_alias,
            GetAliasForExpression(path_expression), resolved_expr->type());
        resolved_computed_columns.push_back(MakeResolvedComputedColumn(
            resolved_column, std::move(resolved_expr)));
        std::unique_ptr<ResolvedColumnRef> column_ref;
        column_ref = MakeColumnRef(resolved_column);
        resolved_index_items.push_back(MakeResolvedIndexItem(
            std::move(column_ref), ordering_expression->descending()));
        break;
      }
      case RESOLVED_MAKE_STRUCT: {
        // The path expression is resolved as a MakeStruct. This only happens
        // when the path is the table name of the SQL table. We error out in
        // this case. Note that this case should be very rare though.
        ZETASQL_RET_CHECK(!resolved_table_scan->table()->IsValueTable());
        return MakeSqlErrorAt(ordering_expression)
               << "Index key " << path_expression->ToIdentifierPathString()
               << " is on the whole row of a SQL table, which is not supported";
      }
      default:
        // TODO: Implement general expression support in index
        // keys.
        ZETASQL_RET_CHECK_FAIL() << "Index key "
                         << path_expression->ToIdentifierPathString()
                         << " is resolved as "
                         << resolved_expr->node_kind_string();
    }
  }

  std::vector<std::unique_ptr<const ResolvedExpr>> resolved_index_storing_items;
  if (ast_statement->optional_index_storing_expressions() != nullptr) {
    zetasql_base::VarSetter<bool> setter_func(
        &analyzing_nonvolatile_stored_expression_columns_, true);
    zetasql_base::VarSetter<absl::string_view> setter(
        &disallowing_query_parameters_with_error_,
        "Query parameters cannot be used inside expressions of CREATE INDEX "
        "statement");
    for (const ASTExpression* ast_expression :
         ast_statement->optional_index_storing_expressions()->expressions()) {
      std::unique_ptr<const ResolvedExpr> resolved_expr;
      ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(ast_expression, &name_scope,
                                        /*clause_name=*/"INDEX STORING Items",
                                        &resolved_expr));

      ZETASQL_RETURN_IF_ERROR(ValidateResolvedExprForCreateIndex(
          ast_statement, ast_expression, &resolved_columns,
          resolved_expr.get()));
      resolved_index_storing_items.push_back(std::move(resolved_expr));
    }
  }

  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(
      ast_statement, "CREATE INDEX", &create_scope, &create_mode));
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(ast_statement->options_list(), &resolved_options));

  const std::vector<std::string> index_name =
      ast_statement->name()->ToIdentifierVector();
  const std::vector<std::string> table_name =
      ast_statement->table_name()->ToIdentifierVector();

  *output = MakeResolvedCreateIndexStmt(
      index_name, create_scope, create_mode, table_name,
      std::move(resolved_table_scan), ast_statement->is_unique(),
      ast_statement->is_search(), index_all_columns,
      std::move(resolved_index_items), std::move(resolved_index_storing_items),
      std::move(resolved_options), std::move(resolved_computed_columns),
      std::move(resolved_unnest_items));
  return absl::OkStatus();
}

absl::Status Resolver::ValidateResolvedExprForCreateIndex(
    const ASTCreateIndexStatement* ast_statement,
    const ASTExpression* ast_expression,
    std::set<IdString, IdStringCaseLess>* resolved_columns,
    const ResolvedExpr* resolved_expr) {
  // For simple expressions such as ResolvedColumnRef we are going to make
  // sure the column names are not used more than once.
  // TODO: Verify that we don't duplicate expressions either. We could
  // probably use IsSameFieldPath and/or IsSameExpressionForGroupBy for that.
  if (resolved_expr->node_kind() == RESOLVED_COLUMN_REF) {
    const ResolvedColumnRef* column_ref =
        resolved_expr->GetAs<ResolvedColumnRef>();
    if (zetasql_base::ContainsKey(*resolved_columns, column_ref->column().name_id())) {
      return MakeSqlErrorAt(ast_expression)
             << "Column " << column_ref->column().name()
             << " found multiple times in "
             << ast_statement->name()->ToIdentifierPathString();
    }
    resolved_columns->insert(column_ref->column().name_id());
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveIndexUnnestExpressions(
    const ASTIndexUnnestExpressionList* unnest_expression_list,
    NameList* name_list,
    std::vector<std::unique_ptr<const ResolvedUnnestItem>>*
        resolved_unnest_items) {
  ZETASQL_RET_CHECK(unnest_expression_list != nullptr);
  ZETASQL_RET_CHECK(name_list != nullptr);
  ZETASQL_RET_CHECK(resolved_unnest_items != nullptr);

  for (const ASTUnnestExpressionWithOptAliasAndOffset*
           unnest_expression_with_alias_and_offset :
       unnest_expression_list->unnest_expressions()) {
    const ASTUnnestExpression* unnest_expr =
        unnest_expression_with_alias_and_offset->unnest_expression();
    ZETASQL_RET_CHECK(unnest_expr != nullptr);
    if (unnest_expr->expression()->node_kind() != AST_PATH_EXPRESSION) {
      return MakeSqlErrorAt(unnest_expr->expression())
             << "Non-path unnest expression for CREATE INDEX is not "
                "supported yet";
    }

    NameScope name_scope(*name_list);
    std::unique_ptr<const ResolvedExpr> resolved_unnest_expr;
    ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(unnest_expr->expression(), &name_scope,
                                      /*clause_name=*/"UNNEST",
                                      &resolved_unnest_expr));
    const Type* unnest_expr_type = resolved_unnest_expr->type();
    if (!unnest_expr_type->IsArray()) {
      return MakeSqlErrorAt(unnest_expr->expression())
             << "Values referenced in UNNEST must be arrays. "
             << "UNNEST contains expression of type "
             << unnest_expr_type->ShortTypeName(product_mode());
    }

    IdString alias_name;
    const ASTNode* alias_location = nullptr;
    const ASTAlias* ast_alias =
        unnest_expression_with_alias_and_offset->optional_alias();
    if (ast_alias != nullptr) {
      alias_name = ast_alias->GetAsIdString();
      alias_location = ast_alias;
    } else {
      alias_name = GetAliasForExpression(unnest_expr->expression());
      alias_location = unnest_expr;
    }
    ZETASQL_RET_CHECK(!alias_name.empty());

    const ResolvedColumn array_element_column(
        AllocateColumnId(),
        /*table_name=*/kArrayId, /*name=*/alias_name,
        unnest_expr_type->AsArray()->element_type());
    std::shared_ptr<NameList> new_name_list(new NameList);
    ZETASQL_RETURN_IF_ERROR(name_list->AddValueTableColumn(
        alias_name, array_element_column, alias_location));

    std::unique_ptr<ResolvedColumnHolder> array_position_column;
    if (unnest_expression_with_alias_and_offset->optional_with_offset() !=
        nullptr) {
      const ASTWithOffset* with_offset =
          unnest_expression_with_alias_and_offset->optional_with_offset();
      // If the alias is NULL, we get "offset" as an implicit alias.
      const ASTAlias* with_offset_alias = with_offset->alias();
      const IdString offset_alias =
          (with_offset_alias == nullptr ? kOffsetAlias
                                        : with_offset_alias->GetAsIdString());

      const ResolvedColumn column(AllocateColumnId(),
                                  /*table_name=*/kArrayOffsetId,
                                  /*name=*/offset_alias,
                                  type_factory_->get_int64());
      array_position_column = MakeResolvedColumnHolder(column);

      // We add the offset column as a value table column so its name acts
      // like a range variable and we get an error if it conflicts with
      // other range variables in the CREATE INDEX target. Note this behavior is
      // consistent with how we handle offset alias name scoping in the FROM
      // clause of a query.
      ZETASQL_RETURN_IF_ERROR(name_list->AddValueTableColumn(
          offset_alias, array_position_column->column(),
          with_offset_alias != nullptr
              ? absl::implicit_cast<const ASTNode*>(with_offset_alias)
              : with_offset));
    }
    resolved_unnest_items->push_back(MakeResolvedUnnestItem(
        std::move(resolved_unnest_expr), array_element_column,
        std::move(array_position_column)));
    ZETASQL_RETURN_IF_ERROR(name_list->MergeFrom(
        *new_name_list, unnest_expression_with_alias_and_offset));
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateModelStatement(
    const ASTCreateModelStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  // Resolve base create statement.
  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(ast_statement, "CREATE MODEL",
                                                &create_scope, &create_mode));

  // Resolve the query.
  const ASTQuery* query = ast_statement->query();
  if (query == nullptr) {
    return MakeSqlErrorAt(ast_statement)
           << "The AS SELECT clause is required for CREATE MODEL";
  }
  bool is_value_table = false;
  std::unique_ptr<const ResolvedScan> query_scan;
  std::vector<std::unique_ptr<const ResolvedOutputColumn>>
      query_output_column_list;
  const std::vector<IdString> table_name_id =
      ast_statement->name()->ToIdStringVector();
  std::vector<std::unique_ptr<const ResolvedColumnDefinition>>
      transform_input_column_list;
  const ASTTransformClause* transform_clause =
      ast_statement->transform_clause();
  std::vector<std::unique_ptr<const ResolvedColumnDefinition>>*
      column_definition_list_ptr =
          transform_clause == nullptr ? nullptr : &transform_input_column_list;
  ZETASQL_RETURN_IF_ERROR(ResolveQueryAndOutputColumns(
      query, /*object_type=*/"MODEL", /*is_recursive_view=*/false,
      table_name_id, kCreateAsId, /*explicit_column_list=*/nullptr, &query_scan,
      &is_value_table, &query_output_column_list, column_definition_list_ptr));

  // Resolve transform list.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>> transform_list;
  std::vector<std::unique_ptr<const ResolvedOutputColumn>>
      transform_output_column_list;
  std::vector<std::unique_ptr<const ResolvedAnalyticFunctionGroup>>
      transform_analytic_function_group_list;
  if (transform_clause != nullptr) {
    ZETASQL_DCHECK_EQ(query_output_column_list.size(),
              transform_input_column_list.size());
    std::shared_ptr<NameList> query_column_definition_name_list(new NameList);
    query_column_definition_name_list->ReserveColumns(
        static_cast<int>(transform_input_column_list.size()));
    for (const auto& column_definition : transform_input_column_list) {
      ZETASQL_RETURN_IF_ERROR(query_column_definition_name_list->AddColumn(
          MakeIdString(column_definition->name()), column_definition->column(),
          /*is_explicit=*/true));
    }
    std::unique_ptr<const NameScope> from_scan_scope(new NameScope(
        empty_name_scope_.get(), query_column_definition_name_list));
    ZETASQL_RETURN_IF_ERROR(ResolveModelTransformSelectList(
        from_scan_scope.get(), transform_clause->select_list(),
        query_column_definition_name_list, &transform_list,
        &transform_output_column_list,
        &transform_analytic_function_group_list));
  }

  // Resolve options.
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(ast_statement->options_list(), &resolved_options));

  const std::vector<std::string> table_name =
      ast_statement->name()->ToIdentifierVector();
  *output = MakeResolvedCreateModelStmt(
      table_name, create_scope, create_mode, std::move(resolved_options),
      std::move(query_output_column_list), std::move(query_scan),
      std::move(transform_input_column_list), std::move(transform_list),
      std::move(transform_output_column_list),
      std::move(transform_analytic_function_group_list));

  return absl::OkStatus();
}

absl::Status Resolver::ResolveWithPartitionColumns(
    const ASTWithPartitionColumnsClause* with_partition_columns_clause,
    const IdString table_name_id_string, ColumnIndexMap* column_indexes,
    std::unique_ptr<const ResolvedWithPartitionColumns>*
        resolved_with_partition_columns) {
  std::vector<std::unique_ptr<const ResolvedColumnDefinition>>
      with_partition_column_definition_list;
  std::vector<const ASTColumnDefinition*> with_partition_column_definitions;
  if (with_partition_columns_clause->table_element_list() != nullptr) {
    for (const ASTTableElement* table_element :
         with_partition_columns_clause->table_element_list()->elements()) {
      switch (table_element->node_kind()) {
        case AST_COLUMN_DEFINITION: {
          const auto* column =
              static_cast<const ASTColumnDefinition*>(table_element);
          // Disallow annotations since constraints are not supported as part
          // of with partition column clause.
          if (column->schema()->attributes() != nullptr &&
              !column->schema()->attributes()->values().empty()) {
            return MakeSqlErrorAt(table_element)
                   << "column annotations are unsupported in "
                   << with_partition_columns_clause->table_element_list()
                          ->GetNodeKindString();
          }
          with_partition_column_definitions.push_back(column);
          break;
        }
        case AST_PRIMARY_KEY:
        case AST_FOREIGN_KEY:
        case AST_CHECK_CONSTRAINT:
          // Constraints are not supported as part with partition column clause.
          return MakeSqlErrorAt(table_element)
                 << table_element->GetNodeKindString() << " is unsupported";
          break;
        default:
          ZETASQL_RET_CHECK(false) << "Unsupported table element "
                           << table_element->GetNodeKindString();
      }
    }
  }
  if (!with_partition_column_definitions.empty()) {
    ZETASQL_RETURN_IF_ERROR(ResolveColumnDefinitionList(
        table_name_id_string, with_partition_column_definitions,
        &with_partition_column_definition_list, column_indexes));
  }

  if (with_partition_columns_clause != nullptr) {
    *resolved_with_partition_columns = MakeResolvedWithPartitionColumns(
        std::move(with_partition_column_definition_list));
  }
  return absl::OkStatus();
}

absl::Status
Resolver::ResolveCreateTableStatementBaseProperties::GetVisibleColumnNames(
    NameList* column_names) const {
  if (column_definition_list.empty()) {
    // No explicit column def
    for (const ResolvedColumn& col : default_visible_columns) {
      ZETASQL_RETURN_IF_ERROR(
          column_names->AddColumn(col.name_id(), col, kExplicitColumn));
    }
  } else {
    for (const auto& c : column_definition_list) {
      ZETASQL_RETURN_IF_ERROR(column_names->AddColumn(c->column().name_id(),
                                              c->column(), kExplicitColumn));
    }
  }
  return absl::OkStatus();
}

absl::Status
Resolver::ResolveCreateTableStatementBaseProperties::WithPartitionColumnNames(
    NameList* column_names) const {
  if (with_partition_columns != nullptr) {
    for (const auto& c : with_partition_columns->column_definition_list()) {
      NameTarget found;
      if (!column_names->LookupName(c->column().name_id(), &found)) {
        ZETASQL_RETURN_IF_ERROR(column_names->AddColumn(c->column().name_id(),
                                                c->column(), kExplicitColumn));
      }
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateTableStmtBaseProperties(
    const ASTCreateTableStmtBase* ast_statement,
    absl::string_view statement_type, const ASTPathExpression* like_table_name,
    const ASTQuery* query, const ASTCollate* collate,
    const ASTPartitionBy* partition_by, const ASTClusterBy* cluster_by,
    const ASTWithPartitionColumnsClause* with_partition_columns_clause,
    const ASTWithConnectionClause* with_connection_clause,
    const ResolveCreateTableStmtBasePropertiesArgs&
        resolved_properties_control_args,
    ResolveCreateTableStatementBaseProperties* statement_base_properties) {
  const ASTTableElementList* table_element_list =
      ast_statement->table_element_list();
  std::vector<const ASTColumnDefinition*> ast_column_definitions;
  const IdString table_name_id_string =
      MakeIdString(ast_statement->name()->ToIdentifierPathString());
  std::vector<IdString> table_name_id =
      ast_statement->name()->ToIdStringVector();
  bool has_primary_key = false;
  bool has_foreign_key = false;
  bool has_check_constraint = false;

  // Sanity check for duplicate constraint names. Constraint names are required
  // to be unique within the containing schema. But ZetaSQL cannot enforce
  // this because it resolves a single statement at a time (in this case a
  // single CREATE TABLE statement). Engines are supposed to implement stricter
  // checks for uniqueness at schema level.
  std::set<std::string, zetasql_base::CaseLess> constraint_names;
  if (table_element_list != nullptr) {
    if (!resolved_properties_control_args.table_element_list_enabled) {
      return MakeSqlErrorAt(table_element_list)
          << statement_type << " with column definition list is unsupported";
    }
    for (const ASTTableElement* table_element :
         table_element_list->elements()) {
      switch (table_element->node_kind()) {
        case AST_COLUMN_DEFINITION:
          ast_column_definitions.push_back(
              static_cast<const ASTColumnDefinition*>(table_element));
          break;
        case AST_PRIMARY_KEY:
          has_primary_key = true;
          break;
        case AST_FOREIGN_KEY:
          has_foreign_key = true;
          break;
        case AST_CHECK_CONSTRAINT:
          has_check_constraint = true;
          break;
        default:
          ZETASQL_RET_CHECK(false) << "Unsupported table element "
                           << table_element->GetNodeKindString();
      }
    }
  }

  ColumnIndexMap column_indexes;
  if (!ast_column_definitions.empty()) {
    if (like_table_name != nullptr) {
      return MakeSqlErrorAt(like_table_name)
             << "Cannot have both column definition list and LIKE";
    }
    ZETASQL_RETURN_IF_ERROR(ResolveColumnDefinitionList(
        table_name_id_string, ast_column_definitions,
        &statement_base_properties->column_definition_list, &column_indexes));
    ZETASQL_RETURN_IF_ERROR(ResolvePrimaryKey(table_element_list->elements(),
                                      column_indexes,
                                      &statement_base_properties->primary_key));
    ZETASQL_RETURN_IF_ERROR(ResolveForeignKeys(
        table_element_list->elements(), column_indexes,
        statement_base_properties->column_definition_list, &constraint_names,
        &statement_base_properties->foreign_key_list));
  }

  // TODO: primary key and foreign key constraints do not
  // work without explicit column definitions in CTAS statement. To fix, the
  // output column list from SELECT clause need to be provided to resolve column
  // names in these constraints.
  if ((query != nullptr ||
       AllowsExternalSource(ast_statement->node_kind())) &&
      ast_column_definitions.empty() && has_primary_key) {
    return MakeSqlErrorAt(ast_statement)
           << "Primary key definition is only allowed with explicit column "
              "definitions in "
           << statement_type;
  }
  if ((query != nullptr ||
       AllowsExternalSource(ast_statement->node_kind())) &&
      ast_column_definitions.empty() && has_foreign_key) {
    return MakeSqlErrorAt(ast_statement)
           << "Foreign key definition is only allowed with explicit column "
              "definitions in "
           << statement_type;
  }

  if (collate != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateAndResolveDefaultCollate(
        collate, collate, &statement_base_properties->collation));
  }

  if (like_table_name != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementLikeTableName(
        like_table_name, table_name_id_string,
        &statement_base_properties->column_definition_list,
        &statement_base_properties->like_table));
  }

  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(
      ast_statement, statement_type, &statement_base_properties->create_scope,
      &statement_base_properties->create_mode));

  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(ast_statement->options_list(),
                         &statement_base_properties->resolved_options));

  statement_base_properties->is_value_table = false;

  // Resolve the query, if any, before resolving the PARTITION BY and
  // CLUSTER BY clauses. These clauses may reference the columns in the output
  // of the query. ZETASQL_CHECK constraints may also reference these columns.
  if (query != nullptr) {
    if (!(statement_base_properties->column_definition_list).empty()) {
      ZETASQL_RETURN_IF_ERROR(ResolveAndAdaptQueryAndOutputColumns(
          query, like_table_name, table_element_list, ast_column_definitions,
          statement_base_properties->column_definition_list,
          &statement_base_properties->query_scan,
          &statement_base_properties->output_column_list));
    } else {
      ZETASQL_RETURN_IF_ERROR(ResolveQueryAndOutputColumns(
          query, "TABLE", /*is_recursive_view=*/false, table_name_id,
          kCreateAsId, /*explicit_column_list=*/nullptr,
          &statement_base_properties->query_scan,
          &statement_base_properties->is_value_table,
          &statement_base_properties->output_column_list,
          &statement_base_properties->column_definition_list));
    }
  }

  statement_base_properties->table_name =
      ast_statement->name()->ToIdentifierVector();
  QueryResolutionInfo query_info(this);
  NameList create_table_names;
  ZETASQL_RETURN_IF_ERROR(
      statement_base_properties->GetVisibleColumnNames(&create_table_names));

  if (partition_by != nullptr || cluster_by != nullptr ||
      has_check_constraint) {
    // Set up the name scope for the table columns, which may appear in
    // PARTITION BY and CLUSTER BY expressions, or ZETASQL_CHECK constraint
    // expressions. The column definition list is populated even for CREATE
    // TABLE AS statements with no explicit list.

    // Populate pseudo-columns for the table, if any.
    if (analyzer_options().ddl_pseudo_columns_callback() != nullptr) {
      std::vector<std::pair<std::string, const Type*>> ddl_pseudo_columns;
      std::map<std::string, const Type*> ddl_pseudo_columns_map;
      std::vector<const ResolvedOption*> option_ptrs;
      option_ptrs.reserve((statement_base_properties->resolved_options).size());
      for (const auto& option : statement_base_properties->resolved_options) {
        option_ptrs.push_back(option.get());
      }
      ZETASQL_RETURN_IF_ERROR(analyzer_options().ddl_pseudo_columns_callback()(
          statement_base_properties->table_name, option_ptrs,
          &ddl_pseudo_columns));
      for (const auto& name_and_type : ddl_pseudo_columns) {
        ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(
            &ddl_pseudo_columns_map, absl::AsciiStrToLower(name_and_type.first),
            name_and_type.second))
            << "Found duplicate DDL pseudo-column '" << name_and_type.first
            << "' for table "
            << absl::StrJoin(statement_base_properties->table_name, ".");
      }
      const IdString table_name_id_string =
          MakeIdString(ast_statement->name()->ToIdentifierPathString());
      for (const auto& ddl_pseudo_column : ddl_pseudo_columns_map) {
        const IdString pseudo_column_name =
            MakeIdString(ddl_pseudo_column.first);
        const ResolvedColumn pseudo_column(
            AllocateColumnId(), table_name_id_string, pseudo_column_name,
            ddl_pseudo_column.second);
        ZETASQL_RETURN_IF_ERROR(create_table_names.AddPseudoColumn(
            pseudo_column_name, pseudo_column, ast_statement));
        (statement_base_properties->pseudo_column_list)
            .push_back(pseudo_column);
      }
    }

    if (has_check_constraint) {
      // WITH PARTITION COLUMNS names are not visible to constraints.
      ZETASQL_RETURN_IF_ERROR(ResolveCheckConstraints(
          table_element_list->elements(), NameScope(create_table_names),
          &constraint_names,
          &statement_base_properties->check_constraint_list));
    }
  }

  // In the end, resolve WithPartitionColumns so that all the columns resolved
  // from the WithPartitionColumns are not available for resolving constraints.
  // We still use same column_indexes as table element to identify duplicate
  // column names.
  if (with_partition_columns_clause != nullptr) {
    if (!AllowsExternalSource(ast_statement->node_kind()) ||
        !language().LanguageFeatureEnabled(
            FEATURE_CREATE_EXTERNAL_TABLE_WITH_PARTITION_COLUMNS)) {
      return MakeSqlErrorAt(with_partition_columns_clause)
             << "WITH PARTITION COLUMNS clause is unsupported for "
             << statement_type;
    }
    ZETASQL_RETURN_IF_ERROR(ResolveWithPartitionColumns(
        with_partition_columns_clause, table_name_id_string, &column_indexes,
        &statement_base_properties->with_partition_columns));

    // WITH PARTITION COLUMNS names are visible to PARTITION BY and CLUSTER BY.
    ZETASQL_RETURN_IF_ERROR(statement_base_properties->WithPartitionColumnNames(
        &create_table_names));
  }
  if (with_connection_clause != nullptr) {
    if (!AllowsExternalSource(ast_statement->node_kind()) ||
        !language().LanguageFeatureEnabled(
            FEATURE_CREATE_EXTERNAL_TABLE_WITH_CONNECTION)) {
      return MakeSqlErrorAt(with_connection_clause)
             << "WITH CONNECTION clause is unsupported for " << statement_type;
    }
    ZETASQL_RETURN_IF_ERROR(ResolveConnection(
        with_connection_clause->connection_clause()->connection_path(),
        &statement_base_properties->connection));
  }

  if (partition_by != nullptr) {
    // The parser should reject hints on PARTITION BY.
    ZETASQL_RET_CHECK(partition_by->hint() == nullptr);
    ZETASQL_RETURN_IF_ERROR(ResolveCreateTablePartitionByList(
        partition_by->partitioning_expressions(),
        PartitioningKind::PARTITION_BY, NameScope(create_table_names),
        &query_info, &statement_base_properties->partition_by_list));
  }

  if (cluster_by != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveCreateTablePartitionByList(
        cluster_by->clustering_expressions(), PartitioningKind::CLUSTER_BY,
        NameScope(create_table_names), &query_info,
        &statement_base_properties->cluster_by_list));
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateTableStatement(
    const ASTCreateTableStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  const ASTQuery* query = ast_statement->query();
  const std::string statement_type =
      query == nullptr ? "CREATE TABLE" : "CREATE TABLE AS SELECT";
  ResolveCreateTableStatementBaseProperties statement_base_properties;
  ResolveCreateTableStmtBasePropertiesArgs resolved_properties_control_args = {
      // table_elements are enabled for "CREATE TABLE" statement or controlled
      // by language feature in case of "CREATE TABLE AS SELECT".
      query == nullptr || language().LanguageFeatureEnabled(
                              FEATURE_CREATE_TABLE_AS_SELECT_COLUMN_LIST)};

  if (ast_statement->collate() != nullptr &&
      !language().LanguageFeatureEnabled(FEATURE_V_1_3_COLLATION_SUPPORT)) {
    return MakeSqlErrorAt(ast_statement->collate())
           << statement_type << " with DEFAULT COLLATE is not supported";
  }

  if (ast_statement->like_table_name() != nullptr &&
      !language().LanguageFeatureEnabled(FEATURE_CREATE_TABLE_LIKE)) {
    return MakeSqlErrorAt(ast_statement->like_table_name())
           << "CREATE TABLE LIKE is not supported";
  }

  if (ast_statement->clone_data_source() != nullptr &&
      !language().LanguageFeatureEnabled(FEATURE_CREATE_TABLE_CLONE)) {
    return MakeSqlErrorAt(ast_statement->clone_data_source())
           << "CREATE TABLE CLONE is not supported";
  }

  if (ast_statement->copy_data_source() != nullptr &&
      !language().LanguageFeatureEnabled(FEATURE_CREATE_TABLE_COPY)) {
    return MakeSqlErrorAt(ast_statement->copy_data_source())
           << "CREATE TABLE COPY is not supported";
  }
  if (ast_statement->like_table_name() != nullptr &&
      ast_statement->clone_data_source() != nullptr) {
    return MakeSqlErrorAt(ast_statement)
           << "CLONE and LIKE cannot both be used for CREATE TABLE";
  }
  if (ast_statement->like_table_name() != nullptr &&
      ast_statement->copy_data_source() != nullptr) {
    return MakeSqlErrorAt(ast_statement)
           << "COPY and LIKE cannot both be used for CREATE TABLE";
  }
  if (ast_statement->clone_data_source() != nullptr &&
      ast_statement->copy_data_source() != nullptr) {
    return MakeSqlErrorAt(ast_statement)
           << "COPY and CLONE cannot both be used for CREATE TABLE";
  }

  if (ast_statement->partition_by() != nullptr &&
      !language().LanguageFeatureEnabled(FEATURE_CREATE_TABLE_PARTITION_BY)) {
    return MakeSqlErrorAt(ast_statement->partition_by())
           << statement_type << " with PARTITION BY is unsupported";
  }
  if (ast_statement->cluster_by() != nullptr &&
      !language().LanguageFeatureEnabled(FEATURE_CREATE_TABLE_CLUSTER_BY)) {
    return MakeSqlErrorAt(ast_statement->cluster_by())
           << statement_type << " with CLUSTER BY is unsupported";
  }
  if (ast_statement->clone_data_source() != nullptr) {
    if (query != nullptr) {
      return MakeSqlErrorAt(query)
             << "CREATE TABLE CLONE cannot be used together with AS SELECT";
    }
    if (ast_statement->partition_by() != nullptr) {
      return MakeSqlErrorAt(ast_statement->partition_by())
             << "CREATE TABLE CLONE cannot be used together with PARTITION BY";
    }
    if (ast_statement->cluster_by() != nullptr) {
      return MakeSqlErrorAt(ast_statement->cluster_by())
             << "CREATE TABLE CLONE cannot be used together with CLUSTER BY";
    }
  }
  if (ast_statement->copy_data_source() != nullptr) {
    if (query != nullptr) {
      return MakeSqlErrorAt(query)
             << "CREATE TABLE COPY cannot be used together with AS SELECT";
    }
    if (ast_statement->partition_by() != nullptr) {
      return MakeSqlErrorAt(ast_statement->partition_by())
             << "CREATE TABLE COPY cannot be used together with PARTITION BY";
    }
    if (ast_statement->cluster_by() != nullptr) {
      return MakeSqlErrorAt(ast_statement->cluster_by())
             << "CREATE TABLE COPY cannot be used together with CLUSTER BY";
    }
  }
  ZETASQL_RETURN_IF_ERROR(ResolveCreateTableStmtBaseProperties(
      ast_statement, statement_type, ast_statement->like_table_name(), query,
      ast_statement->collate(), ast_statement->partition_by(),
      ast_statement->cluster_by(),
      /* with_partition_columns_clause = */ nullptr,
      /* with_connection_clause = */ nullptr, resolved_properties_control_args,
      &statement_base_properties));

  if (query != nullptr) {
    *output = MakeResolvedCreateTableAsSelectStmt(
        statement_base_properties.table_name,
        statement_base_properties.create_scope,
        statement_base_properties.create_mode,
        std::move(statement_base_properties.resolved_options),
        std::move(statement_base_properties.column_definition_list),
        statement_base_properties.pseudo_column_list,
        std::move(statement_base_properties.primary_key),
        std::move(statement_base_properties.foreign_key_list),
        std::move(statement_base_properties.check_constraint_list),
        statement_base_properties.is_value_table,
        statement_base_properties.like_table,
        std::move(statement_base_properties.collation),
        std::move(statement_base_properties.partition_by_list),
        std::move(statement_base_properties.cluster_by_list),
        std::move(statement_base_properties.output_column_list),
        std::move(statement_base_properties.query_scan));
  } else {
    if (ast_statement->clone_data_source() != nullptr) {
      if (!statement_base_properties.column_definition_list.empty()) {
        return MakeSqlErrorAt(ast_statement->table_element_list())
               << "CREATE TABLE CLONE cannot define column list";
      }
      ZETASQL_RETURN_IF_ERROR(ResolveDataSourceForCopyOrClone(
          ast_statement->clone_data_source(),
          &statement_base_properties.clone_from));
      ZETASQL_RET_CHECK(!statement_base_properties.clone_from->column_list().empty());
    } else if (ast_statement->copy_data_source() != nullptr) {
      if (!statement_base_properties.column_definition_list.empty()) {
        return MakeSqlErrorAt(ast_statement->table_element_list())
               << "CREATE TABLE COPY cannot define column list";
      }
      ZETASQL_RETURN_IF_ERROR(ResolveDataSourceForCopyOrClone(
          ast_statement->copy_data_source(),
          &statement_base_properties.copy_from));
      ZETASQL_RET_CHECK(!statement_base_properties.copy_from->column_list().empty());
    } else if (ast_statement->like_table_name() == nullptr &&
               statement_base_properties.column_definition_list.empty()) {
      return MakeSqlErrorAt(ast_statement)
             << "No column definitions in " << statement_type;
    }
    *output = MakeResolvedCreateTableStmt(
        statement_base_properties.table_name,
        statement_base_properties.create_scope,
        statement_base_properties.create_mode,
        std::move(statement_base_properties.resolved_options),
        std::move(statement_base_properties.column_definition_list),
        statement_base_properties.pseudo_column_list,
        std::move(statement_base_properties.primary_key),
        std::move(statement_base_properties.foreign_key_list),
        std::move(statement_base_properties.check_constraint_list),
        statement_base_properties.is_value_table,
        statement_base_properties.like_table,
        std::move(statement_base_properties.collation),
        std::move(statement_base_properties.clone_from),
        std::move(statement_base_properties.copy_from),
        std::move(statement_base_properties.partition_by_list),
        std::move(statement_base_properties.cluster_by_list));
  }

  // Populate the location information for the table name referred in FROM
  // clause.
  MaybeRecordParseLocation(ast_statement->name(), (*output).get());

  return absl::OkStatus();
}

absl::Status Resolver::ResolveDataSourceForCopyOrClone(
    const ASTTableDataSource* data_source,
    std::unique_ptr<const ResolvedScan>* output) {
  std::shared_ptr<const NameList> output_name_list;
  std::unique_ptr<const ResolvedTableScan> table_scan;
  ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsTableScan(
      data_source->path_expr(), GetAliasForExpression(data_source->path_expr()),
      /*has_explicit_alias=*/false,
      data_source->path_expr(), /*hints=*/nullptr,
      data_source->for_system_time(), empty_name_scope_.get(), &table_scan,
      &output_name_list));
  if (table_scan->table()->IsValueTable()) {
    return MakeSqlErrorAt(data_source)
           << "Cannot copy from value table: " << table_scan->table()->Name();
  }
  *output = std::move(table_scan);
  NameScope name_scope(empty_name_scope_.get(), output_name_list);
  if (data_source->where_clause() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveWhereClauseAndCreateScan(data_source->where_clause(),
                                                    &name_scope, output));
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveQueryAndOutputColumns(
    const ASTQuery* query, absl::string_view object_type, bool recursive_view,
    const std::vector<IdString>& table_name, IdString internal_table_name,
    const ASTColumnList* explicit_column_list,
    std::unique_ptr<const ResolvedScan>* query_scan, bool* is_value_table,
    std::vector<std::unique_ptr<const ResolvedOutputColumn>>*
        output_column_list,
    std::vector<std::unique_ptr<const ResolvedColumnDefinition>>*
        column_definition_list) {
  std::shared_ptr<const NameList> query_name_list;

  if (recursive_view) {
    ZETASQL_ASSIGN_OR_RETURN(const ASTSetOperation* recursive_union,
                     GetRecursiveUnion(query));
    SetOperationResolver setop_resolver(recursive_union, this);
    ZETASQL_RETURN_IF_ERROR(setop_resolver.ResolveRecursive(
        empty_name_scope_.get(), table_name, internal_table_name, query_scan,
        &query_name_list));
  } else {
    ZETASQL_RETURN_IF_ERROR(ResolveQuery(query, empty_name_scope_.get(),
                                 internal_table_name, /*is_outer_query=*/true,
                                 query_scan, &query_name_list));
  }
  *is_value_table = query_name_list->is_value_table();

  const int num_output_columns = query_name_list->num_columns();
  IdStringHashSetCase column_names;
  column_names.reserve(num_output_columns);
  auto check_duplicate_column_name =
      [&column_names, object_type](
          const ASTNode* ast, const IdString& column_name) -> absl::Status {
    if (!zetasql_base::InsertIfNotPresent(&column_names, column_name)) {
      return MakeSqlErrorAt(ast)
             << "CREATE " << object_type << " has columns with duplicate name "
             << ToIdentifierLiteral(column_name);
    }
    return absl::OkStatus();
  };
  const IdString table_name_id_string = MakeIdString(
      absl::StrJoin(table_name, ".", [](std::string* out, IdString s) {
        absl::StrAppend(out, ToIdentifierLiteral(s.ToStringView()));
      }));
  if (explicit_column_list != nullptr) {  // With explicit column names.
    if (explicit_column_list->identifiers().size() != num_output_columns) {
      return MakeSqlErrorAt(explicit_column_list)
             << "The number of view column names ("
             << explicit_column_list->identifiers().size()
             << ") must be equal to the number of "
                "columns from the view query ("
             << num_output_columns << ")";
    }
    for (int i = 0; i < num_output_columns; i++) {
      const ASTIdentifier* col = explicit_column_list->identifiers()[i];
      const IdString column_name = col->GetAsIdString();
      if (IsInternalAlias(column_name)) {
        return MakeSqlErrorAt(col) << "Illegal column name: " << column_name;
      }
      ZETASQL_RETURN_IF_ERROR(check_duplicate_column_name(col, column_name));
      output_column_list->push_back(MakeResolvedOutputColumn(
          column_name.ToString(), query_name_list->column(i).column));
    }
  } else {  // Without explicit column names.
    for (int i = 0; i < num_output_columns; ++i) {
      const NamedColumn& named_column = query_name_list->column(i);
      if (!(*is_value_table)) {
        if (IsInternalAlias(named_column.name)) {
          return MakeSqlErrorAt(query)
                 << "CREATE " << object_type
                 << " columns must be named, but column " << (i + 1)
                 << " has no name";
        }
        ZETASQL_RETURN_IF_ERROR(check_duplicate_column_name(query, named_column.name));
      }
      output_column_list->push_back(MakeResolvedOutputColumn(
          named_column.name.ToString(), named_column.column));
    }
  }
  if (column_definition_list != nullptr) {
    for (int i = 0; i < num_output_columns; ++i) {
      const NamedColumn& named_column = query_name_list->column(i);
      ResolvedColumn defined_column(AllocateColumnId(), table_name_id_string,
                                    named_column.name,
                                    named_column.column.annotated_type());
      std::unique_ptr<ResolvedColumnAnnotations> column_annotations;
      if (language().LanguageFeatureEnabled(FEATURE_V_1_3_COLLATION_SUPPORT)) {
        ZETASQL_ASSIGN_OR_RETURN(column_annotations,
                         MakeResolvedColumnAnnotationsWithCollation(
                             named_column.column.type_annotation_map()));
      }

      column_definition_list->push_back(MakeResolvedColumnDefinition(
          output_column_list->at(i)->name(), named_column.column.type(),
          /*annotations=*/std::move(column_annotations), /*is_hidden=*/false,
          defined_column,
          /*generated_column_info=*/nullptr, /*default_value=*/nullptr));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<ResolvedColumnAnnotations>>
Resolver::MakeResolvedColumnAnnotationsWithCollation(
    const AnnotationMap* type_annotation_map) {
  std::unique_ptr<ResolvedColumnAnnotations> column_annotations;
  if (type_annotation_map != nullptr &&
      type_annotation_map->Has<CollationAnnotation>()) {
    std::unique_ptr<const ResolvedLiteral> collation_name_expr;
    const SimpleValue* collation_name_value =
        type_annotation_map->GetAnnotation(CollationAnnotation::GetId());
    if (collation_name_value != nullptr) {
      ZETASQL_RET_CHECK(collation_name_value->has_string_value());
      ZETASQL_RET_CHECK(!collation_name_value->string_value().empty());
      collation_name_expr = MakeResolvedLiteralWithoutLocation(
          Value::StringValue(collation_name_value->string_value()));
    }
    std::vector<std::unique_ptr<const ResolvedColumnAnnotations>> child_list;
    if (type_annotation_map->IsArrayMap()) {
      const AnnotationMap* element_map =
          type_annotation_map->AsArrayMap()->element();
      if (element_map != nullptr && element_map->Has<CollationAnnotation>()) {
        ZETASQL_ASSIGN_OR_RETURN(
            auto element_annotation,
            MakeResolvedColumnAnnotationsWithCollation(element_map));
        child_list.push_back(std::move(element_annotation));
      }
    } else if (type_annotation_map->IsStructMap()) {
      int last_non_empty_index = -1;
      for (int i = 0; i < type_annotation_map->AsStructMap()->num_fields();
           i++) {
        std::unique_ptr<ResolvedColumnAnnotations> field_annotation;
        const AnnotationMap* field_map =
            type_annotation_map->AsStructMap()->field(i);
        if (field_map != nullptr && field_map->Has<CollationAnnotation>()) {
          last_non_empty_index = i;
          ZETASQL_ASSIGN_OR_RETURN(
              field_annotation,
              MakeResolvedColumnAnnotationsWithCollation(field_map));
        } else {
          // All children must be non-null, otherwise Resolver::PruneColumnLists
          // will crash when calling GetDescendantsSatisfying.
          field_annotation = MakeResolvedColumnAnnotations();
        }
        child_list.push_back(std::move(field_annotation));
      }
      // Shorten the child_list to be able to hold the last non-empty element.
      child_list.resize(last_non_empty_index + 1);
    }
    column_annotations = MakeResolvedColumnAnnotations(
        std::move(collation_name_expr), /*not_null=*/false, /*option_list=*/{},
        std::move(child_list), TypeParameters());
  }
  return column_annotations;
}

absl::Status Resolver::ResolveAndAdaptQueryAndOutputColumns(
    const ASTQuery* query, const ASTPathExpression* like_table_name,
    const ASTTableElementList* table_element_list,
    absl::Span<const ASTColumnDefinition* const> ast_column_definitions,
    std::vector<std::unique_ptr<const ResolvedColumnDefinition>>&
        column_definition_list,
    std::unique_ptr<const ResolvedScan>* query_scan,
    std::vector<std::unique_ptr<const ResolvedOutputColumn>>*
        output_column_list) {
  std::shared_ptr<const NameList> query_name_list;
  ZETASQL_RETURN_IF_ERROR(
      ResolveQuery(query, empty_name_scope_.get(),
                   kCreateAsId, /*is_outer_query=*/true,
                   query_scan, &query_name_list));
  const int num_output_columns = query_name_list->num_columns();
  if (like_table_name != nullptr) {
    if (num_output_columns != column_definition_list.size()) {
      return MakeSqlErrorAt(like_table_name)
             << "The number of columns in the LIKE table does not "
                "match the number of columns produced by the query";
    }
  } else {
    if (query_name_list->is_value_table()) {
      return MakeSqlErrorAt(table_element_list)
             << "Column definition list cannot be specified when creating "
                "a value table";
    }

    if (num_output_columns != column_definition_list.size()) {
      return MakeSqlErrorAt(table_element_list)
             << "The number of columns in the column definition list does not "
                "match the number of columns produced by the query";
    }

    ZETASQL_RET_CHECK_EQ(ast_column_definitions.size(), num_output_columns);
  }

  ResolvedColumnList output_columns;
  ResolvedColumnList desired_output_columns;
  output_columns.reserve(num_output_columns);
  desired_output_columns.reserve(num_output_columns);
  UntypedLiteralMap untyped_literal_map(query_scan->get());
  for (int i = 0; i < num_output_columns; ++i) {
    const NamedColumn& named_column = query_name_list->column(i);
    const Type* output_type = named_column.column.type();
    IdString column_name = column_definition_list[i]->column().name_id();
    const Type* defined_type = column_definition_list[i]->type();
    SignatureMatchResult unused;
    if (!coercer_.AssignableTo(InputArgumentType(output_type), defined_type,
                               /* is_explicit = */ false, &unused) &&
        untyped_literal_map.Find(named_column.column) == nullptr) {
      const std::string error_message =
          absl::StrCat(ToIdentifierLiteral(column_name), "' has type ",
                       defined_type->ShortTypeName(product_mode()),
                       " which cannot be coerced from query output type ",
                       output_type->ShortTypeName(product_mode()));
      if (like_table_name != nullptr) {
        return MakeSqlErrorAt(like_table_name)
               << like_table_name->ToIdentifierPathString() << "'s column '"
               << error_message;
      }
      return MakeSqlErrorAt(ast_column_definitions[i])
             << "Column '" << error_message;
    }
    desired_output_columns.emplace_back(
        named_column.column.column_id(), kCreateAsId,
        column_name, defined_type);
    output_columns.push_back(named_column.column);
  }
  ZETASQL_RETURN_IF_ERROR(CreateWrapperScanWithCasts(
      query, desired_output_columns, kCreateAsCastId, query_scan,
      &output_columns));
  for (int i = 0; i < query_name_list->num_columns(); ++i) {
    output_column_list->push_back(MakeResolvedOutputColumn(
        column_definition_list[i]->column().name(), output_columns[i]));
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateViewStatementBaseProperties(
    const ASTCreateViewStatementBase* ast_statement,
    absl::string_view statement_type, absl::string_view object_type,
    std::vector<std::string>* table_name,
    ResolvedCreateStatement::CreateScope* create_scope,
    ResolvedCreateStatement::CreateMode* create_mode,
    ResolvedCreateStatementEnums::SqlSecurity* sql_security,
    std::vector<std::unique_ptr<const ResolvedOption>>* resolved_options,
    std::vector<std::unique_ptr<const ResolvedOutputColumn>>*
        output_column_list,
    std::vector<std::unique_ptr<const ResolvedColumnDefinition>>*
        column_definition_list,
    std::unique_ptr<const ResolvedScan>* query_scan, std::string* view_sql,
    bool* is_value_table, bool* recursive) {
  *recursive = ast_statement->recursive();
  bool actually_recursive = false;
  if (ast_statement->recursive()) {
    if (!language().LanguageFeatureEnabled(FEATURE_V_1_3_WITH_RECURSIVE)) {
      return MakeSqlErrorAt(ast_statement)
             << "Recursive views are not supported";
    }

    ZETASQL_ASSIGN_OR_RETURN(actually_recursive, IsViewSelfRecursive(ast_statement));
  }
  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(ast_statement, statement_type,
                                                create_scope, create_mode));

  ZETASQL_RET_CHECK(ast_statement->name() != nullptr);
  const std::vector<IdString> table_name_id =
      ast_statement->name()->ToIdStringVector();
  *table_name = ast_statement->name()->ToIdentifierVector();

  ZETASQL_RETURN_IF_ERROR(ResolveOptionsList(ast_statement->options_list(),
                                     resolved_options));

  *is_value_table = false;
  {
    zetasql_base::VarSetter<absl::string_view> setter(
        &disallowing_query_parameters_with_error_,
        "Query parameters cannot be used inside SQL view bodies");
    ZETASQL_RETURN_IF_ERROR(ResolveQueryAndOutputColumns(
        ast_statement->query(), object_type, actually_recursive, table_name_id,
        kViewId, ast_statement->column_list(), query_scan, is_value_table,
        output_column_list, column_definition_list));
  }

  const ParseLocationRange& ast_query_range =
      ast_statement->query()->GetParseLocationRange();
  ZETASQL_RET_CHECK_GE(sql_.length(), ast_query_range.end().GetByteOffset()) << sql_;
  absl::string_view sql =
      absl::ClippedSubstr(sql_, ast_query_range.start().GetByteOffset(),
                          ast_query_range.end().GetByteOffset() -
                              ast_query_range.start().GetByteOffset());
  *view_sql = std::string(sql);

  *sql_security = static_cast<ResolvedCreateStatementEnums::SqlSecurity>(
      ast_statement->sql_security());

  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateViewStatement(
    const ASTCreateViewStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  ResolvedCreateStatementEnums::SqlSecurity sql_security;
  std::vector<std::string> table_name;
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  std::unique_ptr<const ResolvedScan> query_scan;
  std::vector<std::unique_ptr<const ResolvedOutputColumn>> output_column_list;
  std::string view_sql;
  bool is_value_table = false;
  bool recursive = false;
  ZETASQL_RETURN_IF_ERROR(ResolveCreateViewStatementBaseProperties(
      ast_statement, /*statement_type=*/"CREATE VIEW", /*object_type=*/"VIEW",
      &table_name, &create_scope, &create_mode, &sql_security,
      &resolved_options, &output_column_list,
      /*column_definition_list=*/nullptr, &query_scan, &view_sql,
      &is_value_table, &recursive));

  if (!language().LanguageFeatureEnabled(FEATURE_CREATE_VIEW_WITH_COLUMN_LIST)
      && ast_statement->column_list() != nullptr) {
    return MakeSqlErrorAt(ast_statement->column_list())
           << "CREATE VIEW with explicit column list is not supported";
  }
  *output = MakeResolvedCreateViewStmt(
      table_name, create_scope, create_mode, std::move(resolved_options),
      std::move(output_column_list), ast_statement->column_list() != nullptr,
      std::move(query_scan), view_sql, sql_security, is_value_table, recursive);

  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateMaterializedViewStatement(
    const ASTCreateMaterializedViewStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  const std::string statement_type = "CREATE MATERIALIZED VIEW";
  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  ResolvedCreateStatementEnums::SqlSecurity sql_security;
  std::vector<std::string> table_name;
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  std::unique_ptr<const ResolvedScan> query_scan;
  std::vector<std::unique_ptr<const ResolvedOutputColumn>> output_column_list;
  std::vector<std::unique_ptr<const ResolvedColumnDefinition>>
      column_definition_list;
  std::string view_sql;
  bool is_value_table = false;
  bool recursive = false;

  ZETASQL_RETURN_IF_ERROR(ResolveCreateViewStatementBaseProperties(
      ast_statement, statement_type,
      /*object_type=*/"MATERIALIZED VIEW", &table_name, &create_scope,
      &create_mode, &sql_security, &resolved_options, &output_column_list,
      &column_definition_list, &query_scan, &view_sql, &is_value_table,
      &recursive));

  // Set up the name scope for the table columns, which may appear in
  // PARTITION BY and CLUSTER BY expressions.
  NameList create_names;
  for (const std::unique_ptr<const ResolvedColumnDefinition>&
           column_definition : column_definition_list) {
    ZETASQL_RETURN_IF_ERROR(create_names.AddColumn(
        column_definition->column().name_id(), column_definition->column(),
        /*is_explicit=*/true));
    RecordColumnAccess(column_definition->column());
  }
  const NameScope name_scope(create_names);
  QueryResolutionInfo query_info(this);

  std::vector<std::unique_ptr<const ResolvedExpr>> partition_by_list;
  std::vector<std::unique_ptr<const ResolvedExpr>> cluster_by_list;
  if (ast_statement->partition_by() != nullptr) {
    if (!language().LanguageFeatureEnabled(
            FEATURE_CREATE_MATERIALIZED_VIEW_PARTITION_BY)) {
      return MakeSqlErrorAt(ast_statement->partition_by())
             << statement_type << " with PARTITION BY is unsupported";
    }
    // The parser should reject hints on PARTITION BY.
    ZETASQL_RET_CHECK(ast_statement->partition_by()->hint() == nullptr);
    ZETASQL_RETURN_IF_ERROR(ResolveCreateTablePartitionByList(
        ast_statement->partition_by()->partitioning_expressions(),
        PartitioningKind::PARTITION_BY, name_scope, &query_info,
        &partition_by_list));
  }
  if (ast_statement->cluster_by() != nullptr) {
    // The parser should reject hints on CLUSTER BY.
    if (!language().LanguageFeatureEnabled(
            FEATURE_CREATE_MATERIALIZED_VIEW_CLUSTER_BY)) {
      return MakeSqlErrorAt(ast_statement->cluster_by())
             << statement_type << " with CLUSTER BY is unsupported";
    }
    ZETASQL_RETURN_IF_ERROR(ResolveCreateTablePartitionByList(
        ast_statement->cluster_by()->clustering_expressions(),
        PartitioningKind::CLUSTER_BY, name_scope, &query_info,
        &cluster_by_list));
  }

  if (!language().LanguageFeatureEnabled(
          FEATURE_CREATE_VIEW_WITH_COLUMN_LIST) &&
      ast_statement->column_list() != nullptr) {
    return MakeSqlErrorAt(ast_statement->column_list())
           << "CREATE MATERIALIZED VIEW with explicit column list is "
              "not supported";
  }
  *output = MakeResolvedCreateMaterializedViewStmt(
      table_name, create_scope, create_mode, std::move(resolved_options),
      std::move(output_column_list),
      ast_statement->column_list() != nullptr,
      std::move(query_scan), view_sql,
      sql_security, is_value_table, recursive,
      std::move(column_definition_list), std::move(partition_by_list),
      std::move(cluster_by_list));

  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateExternalTableStatement(
    const ASTCreateExternalTableStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  const std::string statement_type = "CREATE EXTERNAL TABLE";
  ResolveCreateTableStatementBaseProperties statement_base_properties;
  /* CREATE EXTERNAL TABLE does not support query
   * (CREATE EXTERNAL TABLE AS SELECT) presently but
   * ResolveCreateTableStmtBaseProperties needs these parameters to resolve
   * partition by, cluster by and check constraint so provide the parameter to
   * the ResolveCreateTableStmtBaseProperties and then ignore the results.
   */
  std::unique_ptr<const ResolvedScan> query_scan;
  std::vector<std::unique_ptr<const ResolvedOutputColumn>> output_column_list;
  ResolveCreateTableStmtBasePropertiesArgs resolved_properties_control_args = {
      language().LanguageFeatureEnabled(
          FEATURE_CREATE_EXTERNAL_TABLE_WITH_TABLE_ELEMENT_LIST)};

  if (ast_statement->like_table_name() != nullptr) {
    return MakeSqlErrorAt(ast_statement->like_table_name())
           << statement_type << " LIKE is not supported";
  }

  if (ast_statement->collate() != nullptr &&
      !language().LanguageFeatureEnabled(FEATURE_V_1_3_COLLATION_SUPPORT)) {
    return MakeSqlErrorAt(ast_statement->collate())
           << statement_type << " with DEFAULT COLLATE is not supported";
  }

  ZETASQL_RETURN_IF_ERROR(ResolveCreateTableStmtBaseProperties(
      ast_statement, statement_type, /* like_table_name = */ nullptr,
      /* query = */ nullptr, ast_statement->collate(),
      /* partition_by = */ nullptr, /* cluster_by = */ nullptr,
      ast_statement->with_partition_columns_clause(),
      ast_statement->with_connection_clause(), resolved_properties_control_args,
      &statement_base_properties));

  *output = MakeResolvedCreateExternalTableStmt(
      statement_base_properties.table_name,
      statement_base_properties.create_scope,
      statement_base_properties.create_mode,
      std::move(statement_base_properties.resolved_options),
      std::move(statement_base_properties.column_definition_list),
      statement_base_properties.pseudo_column_list,
      std::move(statement_base_properties.primary_key),
      std::move(statement_base_properties.foreign_key_list),
      std::move(statement_base_properties.check_constraint_list),
      statement_base_properties.is_value_table,
      statement_base_properties.like_table,
      std::move(statement_base_properties.collation),
      std::move(statement_base_properties.with_partition_columns),
      std::move(statement_base_properties.connection));

  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateSnapshotTableStatement(
    const ASTCreateSnapshotTableStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  constexpr absl::string_view statement_type = "CREATE SNAPSHOT TABLE";
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  std::unique_ptr<const ResolvedScan> clone_from;

  if (!language().LanguageFeatureEnabled(FEATURE_CREATE_SNAPSHOT_TABLE)) {
    return MakeSqlErrorAt(ast_statement)
           << "CREATE SNAPSHOT TABLE is not supported";
  }

  ZETASQL_RET_CHECK(ast_statement->name() != nullptr);
  const std::vector<IdString> table_name_id =
      ast_statement->name()->ToIdStringVector();
  std::vector<std::string> table_name =
      ast_statement->name()->ToIdentifierVector();

  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(ast_statement, statement_type,
                                                &create_scope, &create_mode));

  // CLONE clause is required for snapshots.
  ZETASQL_RET_CHECK(ast_statement->clone_data_source() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ResolveDataSourceForCopyOrClone(
      ast_statement->clone_data_source(), &clone_from));
  ZETASQL_RET_CHECK(!clone_from->column_list().empty());

  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(ast_statement->options_list(), &resolved_options));

  *output = MakeResolvedCreateSnapshotTableStmt(
      table_name, create_scope, create_mode, std::move(clone_from),
      std::move(resolved_options));

  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateConstantStatement(
    const ASTCreateConstantStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {

  // Get the scope and the mode of the CREATE CONSTANT statement.
  // Inside modules, neither CREATE OR REPLACE nor CREATE IF NOT EXISTS are
  // allowed.
  // TODO: Make ModuleCatalog::PerformCommonCreateStatementValidation()
  // public and static, and invoke it here for consistent validation?
  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(
      ast_statement, "CREATE CONSTANT", &create_scope, &create_mode));

  // CREATE CONSTANT must use a simple name inside modules. Name paths of length
  // > 1 are not allowed because the path prefix refers to nested catalogs; such
  // a constant should be defined in the corresponding module.
  const std::vector<std::string> name_path(
      ast_statement->name()->ToIdentifierVector());
  if (name_path.size() != 1 &&
      analyzer_options().statement_context() == CONTEXT_MODULE) {
    return MakeSqlErrorAt(ast_statement)
           << "Modules do not support creating functions with multi-part names "
              "or in nested catalogs: "
           << ast_statement->name()->ToIdentifierPathString();
  }

  // Resolve the constant expression.
  std::unique_ptr<const ResolvedExpr> resolved_expr;
  ZETASQL_RETURN_IF_ERROR(
      ResolveScalarExpr(ast_statement->expr(), empty_name_scope_.get(),
                        "definition of named constant", &resolved_expr));

  // Return a resolved statement.
  *output = MakeResolvedCreateConstantStmt(name_path, create_scope, create_mode,
                                           std::move(resolved_expr));
  MaybeRecordParseLocation(ast_statement->name(), output->get());

  return absl::OkStatus();
}

// Get an appropriate string to identify a create scope in an error message.
static std::string CreateScopeErrorString(
    ResolvedCreateStatement::CreateScope create_scope) {
  switch (create_scope) {
    case ResolvedCreateStatement::CREATE_PUBLIC:
      return "PUBLIC";
    case ResolvedCreateStatement::CREATE_PRIVATE:
      return "PRIVATE";
    case ResolvedCreateStatement::CREATE_TEMP:
      return "TEMP";
    case ResolvedCreateStatement::CREATE_DEFAULT_SCOPE:
      ZETASQL_LOG(FATAL) << "Unexpected error scope default.";
  }
}

static absl::Status FailIfContainsParameterExpr(const ASTNode* node,
                                                absl::string_view entity_type,
                                                absl::string_view entity_name) {
  if (node == nullptr) {
    return absl::OkStatus();
  }
  std::vector<const ASTNode*> found_nodes;
  node->GetDescendantSubtreesWithKinds({AST_PARAMETER_EXPR}, &found_nodes);
  if (found_nodes.empty()) {
    return absl::OkStatus();
  }
  const ASTParameterExpr* parameter_expr =
      found_nodes.front()->GetAsOrDie<ASTParameterExpr>();
  std::string unparsed_parameter_expr;
  parser::Unparser unparser(&unparsed_parameter_expr);
  unparser.visitASTParameterExpr(parameter_expr, /*data=*/nullptr);
  unparser.FlushLine();
  absl::StripAsciiWhitespace(&unparsed_parameter_expr);
  return MakeSqlErrorAt(parameter_expr) << absl::Substitute(
             "Query parameter is not allowed in the body of $0 '$1': $2",
             entity_type, entity_name, unparsed_parameter_expr);
}

static ResolvedCreateStatementEnums::DeterminismLevel ConvertDeterminismLevel(
    ASTCreateFunctionStmtBase::DeterminismLevel level) {
  switch (level) {
    case ASTCreateFunctionStmtBase::DETERMINISM_UNSPECIFIED:
      return ResolvedCreateStatementEnums::DETERMINISM_UNSPECIFIED;
    case ASTCreateFunctionStmtBase::DETERMINISTIC:
      return ResolvedCreateStatementEnums::DETERMINISM_DETERMINISTIC;
    case ASTCreateFunctionStmtBase::NOT_DETERMINISTIC:
      return ResolvedCreateStatementEnums::DETERMINISM_NOT_DETERMINISTIC;
    case ASTCreateFunctionStmtBase::IMMUTABLE:
      return ResolvedCreateStatementEnums::DETERMINISM_IMMUTABLE;
    case ASTCreateFunctionStmtBase::STABLE:
      return ResolvedCreateStatementEnums::DETERMINISM_STABLE;
    case ASTCreateFunctionStmtBase::VOLATILE:
      return ResolvedCreateStatementEnums::DETERMINISM_VOLATILE;
  }
}

absl::Status Resolver::ResolveCreateFunctionStatement(
    const ASTCreateFunctionStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(
      ast_statement, "CREATE FUNCTION", &create_scope, &create_mode));

  const bool is_aggregate = ast_statement->is_aggregate();
  if (is_aggregate &&
      !language().LanguageFeatureEnabled(FEATURE_CREATE_AGGREGATE_FUNCTION)) {
    return MakeSqlErrorAt(ast_statement)
           << "Aggregate functions are not supported";
  }

  std::vector<std::string> function_name;
  auto arg_info = absl::make_unique<FunctionArgumentInfo>();
  ZETASQL_RETURN_IF_ERROR(ResolveFunctionDeclaration(
      ast_statement->function_declaration(),
      is_aggregate ? ResolveFunctionDeclarationType::AGGREGATE_FUNCTION
                   : ResolveFunctionDeclarationType::SCALAR_FUNCTION,
      &function_name, arg_info.get()));
  const Type* return_type = nullptr;
  const bool has_explicit_return_type = ast_statement->return_type() != nullptr;
  bool has_return_type = false;
  if (has_explicit_return_type) {
    ZETASQL_RETURN_IF_ERROR(ResolveType(ast_statement->return_type(),
                                "function signatures", &return_type,
                                /*resolved_type_params=*/nullptr));
    has_return_type = true;
  }

  const ASTIdentifier* function_language = ast_statement->language();
  const ASTStringLiteral* code = ast_statement->code();
  const ASTSqlFunctionBody* sql_function_body =
      ast_statement->sql_function_body();
  const bool is_remote =
      ast_statement->is_remote() ||
      (function_language != nullptr &&
       absl::AsciiStrToUpper(function_language->GetAsString()) == "REMOTE");

  if (!is_remote && function_language == nullptr &&
      sql_function_body == nullptr) {
    return MakeSqlErrorAt(ast_statement)
           << "Function must specify LANGUAGE or have a SQL body in "
              "parentheses";
  }
  if (function_language != nullptr && sql_function_body != nullptr) {
    if (sql_function_body->expression()->node_kind() ==
            AST_STRING_LITERAL) {
      // Try to be helpful if someone writes AS ("""body""") with a string
      // body enclosed in parentheses.
      return MakeSqlErrorAt(sql_function_body)
             << "Function body should not be enclosed in ( ) for non-SQL "
             << "functions";
    } else {
      return MakeSqlErrorAt(ast_statement)
             << "Function cannot specify a LANGUAGE and include a SQL body";
    }
  }
  const bool is_sql_function = (sql_function_body != nullptr);
  if (!is_sql_function && !has_return_type) {
    return MakeSqlErrorAt(ast_statement)
           << "Non-SQL functions must specify a return type";
  }
  const std::string language_string =
      is_remote ? "REMOTE"
                : (is_sql_function ? "SQL" : function_language->GetAsString());
  if (zetasql_base::CaseEqual(language_string, "SQL") && !is_sql_function) {
    return MakeSqlErrorAt(ast_statement->language())
           << "To write SQL functions, omit the LANGUAGE clause "
           << "and write the function body using 'AS (expression)'";
  }

  std::string code_string;
  if (code != nullptr) {
    code_string = code->string_value();
  }

  std::unique_ptr<const ResolvedExpr> resolved_expr;
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      resolved_aggregate_exprs;

  ZETASQL_RETURN_IF_ERROR(FailIfContainsParameterExpr(
      sql_function_body, "SQL function",
      ast_statement->function_declaration()->name()->ToIdentifierPathString()));

  // Resolve the SQL function body if this function declaration includes one and
  // the function declaration does not contain any templated arguments.
  if (sql_function_body != nullptr &&
      !arg_info->contains_templated_arguments()) {
    ZETASQL_RET_CHECK(is_sql_function);
    // Set the argument info member variable in Resolver so that arguments are
    // in scope for the function body. The scoped_reset will set the variable
    // back to nullptr after resolving the function body.
    auto scoped_reset = SetArgumentInfo(arg_info.get());
    {
      if (!is_aggregate) {
        // This branch disallows aggregates inside function definitions.
        ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(
            sql_function_body->expression(), empty_name_scope_.get(),
            language().LanguageFeatureEnabled(FEATURE_CREATE_AGGREGATE_FUNCTION)
                ? "SQL function body for non-AGGREGATE function"
                : "SQL function body",
            &resolved_expr));
      } else {
        // We use a QueryResolutionInfo to capture aggregate expressions
        // inside the function body.
        QueryResolutionInfo query_info(this);
        ExprResolutionInfo expr_info(
            empty_name_scope_.get() /* name_scope */,
            empty_name_scope_.get() /* aggregate_name_scope */,
            /*allows_aggregation_in=*/true,
            /*allows_analytic_in=*/false,
            /*use_post_grouping_columns_in=*/false,
            "SQL function body",
            &query_info);

        ZETASQL_RETURN_IF_ERROR(
            ResolveExpr(sql_function_body->expression(),
                        &expr_info, &resolved_expr));

        ZETASQL_RET_CHECK(!expr_info.has_analytic);
        ZETASQL_RETURN_IF_ERROR(
            FunctionResolver::CheckCreateAggregateFunctionProperties(
                *resolved_expr, sql_function_body->expression(), &expr_info,
                &query_info));
        if (expr_info.has_aggregation) {
          ZETASQL_RET_CHECK(is_aggregate);
          resolved_aggregate_exprs =
              query_info.release_aggregate_columns_to_compute();
        }
      }
    }

    const Type* function_body_type = resolved_expr->type();
    if (!has_return_type) {
      return_type = function_body_type;
      has_return_type = true;
    } else {
      ZETASQL_RETURN_IF_ERROR(CoerceExprToType(
          sql_function_body->expression(), return_type, kImplicitCoercion,
          "Function declared to return $0 but the function body produces "
          "incompatible type $1",
          &resolved_expr));
    }
  } else if (is_sql_function && !arg_info->contains_templated_arguments()) {
    return MakeSqlErrorAt(ast_statement)
           << "SQL function must have a non-empty body";
  }

  FunctionSignatureOptions signature_options;
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<FreestandingDeprecationWarning>
          additional_deprecation_warnings,
      StatusesToDeprecationWarnings(
          // We always use ERROR_MESSAGE_WITH_PAYLOAD because we don't want the
          // contents of the function signature to depend on
          // AnalyzerOptions.error_message_mode().
          ConvertInternalErrorLocationsAndAdjustErrorStrings(
              ERROR_MESSAGE_WITH_PAYLOAD, sql_, deprecation_warnings_),
          sql_));
  signature_options.set_additional_deprecation_warnings(
      additional_deprecation_warnings);
  if (language().LanguageFeatureEnabled(FEATURE_V_1_3_COLLATION_SUPPORT)) {
    // User defined function should disallow collation on function arguments.
    // This constraint is temporary and we should support it later through some
    // kind of language extensions.
    signature_options.set_rejects_collation();
  }

  std::unique_ptr<FunctionSignature> signature;
  if (has_return_type) {
    signature = absl::make_unique<FunctionSignature>(
        return_type, arg_info->SignatureArguments(), /*context_id=*/0,
        signature_options);
  } else {
    const FunctionArgumentType any_type(ARG_TYPE_ARBITRARY,
                                        /*num_occurrences=*/1);
    signature = absl::make_unique<FunctionSignature>(
        any_type, arg_info->SignatureArguments(), /*context_id=*/0,
        signature_options);
  }

  // Option resolution is done with an empty namescope. That includes any
  // function arguments. Function arguments have values at function invocation
  // time, but not at function create time when options are evaluated.
  ZETASQL_RET_CHECK_EQ(function_argument_info_, nullptr);
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(ResolveOptionsList(ast_statement->options_list(),
                                     &resolved_options));

  // If the function has a SQL function body, copy the body SQL to the code
  // field.
  if (sql_function_body != nullptr) {
    const ParseLocationRange& range =
        sql_function_body->GetParseLocationRange();
    ZETASQL_RET_CHECK_GE(sql_.length(), range.end().GetByteOffset()) << sql_;
    absl::string_view sql_body = absl::ClippedSubstr(
        sql_, range.start().GetByteOffset(),
        range.end().GetByteOffset() - range.start().GetByteOffset());
    // The parser includes the outer parentheses in the sql_function_body rule
    // and thus in the parse range. We remove those parentheses here since they
    // are not part of the expression, but we also assert that they exist to
    // defend against changes in the parser that might otherwise break us.
    ZETASQL_RET_CHECK(absl::ConsumePrefix(&sql_body, "("));
    ZETASQL_RET_CHECK(absl::ConsumeSuffix(&sql_body, ")"));
    sql_body = absl::StripAsciiWhitespace(sql_body);
    code_string = std::string(sql_body);
  }

  if (!has_return_type) {
    ZETASQL_RET_CHECK(arg_info->contains_templated_arguments())
        << ast_statement->DebugString();
    // TODO: The return type is unused for templated function
    // declarations since the FunctionSignature contains the relevant
    // information instead. Remove this part when the 'return_type' field is
    // removed from the ResolvedCreateFunctionStmt.
    return_type = types::EmptyStructType();
  }

  auto sql_security = static_cast<ResolvedCreateStatementEnums::SqlSecurity>(
      ast_statement->sql_security());

  if (create_scope != ResolvedCreateStatementEnums::CREATE_DEFAULT_SCOPE &&
      sql_security != ResolvedCreateStatementEnums::SQL_SECURITY_UNSPECIFIED) {
    return MakeSqlErrorAt(ast_statement)
           << "SQL SECURITY clause is not supported on statements with the "
           << CreateScopeErrorString(create_scope) << " modifier.";
  }

  std::unique_ptr<const ResolvedConnection> resolved_connection;

  const ASTWithConnectionClause* with_connection =
      ast_statement->with_connection_clause();
  // If REMOTE keyword is used in CREATE FUNCTION
  if (ast_statement->is_remote()) {
    if (!language().LanguageFeatureEnabled(FEATURE_V_1_3_REMOTE_FUNCTION)) {
      return MakeSqlErrorAt(ast_statement)
             << "Creating remote functions is not supported";
    }

    if (function_language != nullptr) {
      // TODO: Improve the error message. Currently the error points
      // at language identifier. It would be better for the error to point at
      // LANGUAGE, but its location is not available from function_language.
      return MakeSqlErrorAt(function_language)
             << "REMOTE function cannot specify a LANGUAGE";
    }

    if (with_connection != nullptr) {
      ZETASQL_RETURN_IF_ERROR(ResolveConnection(
          with_connection->connection_clause()->connection_path(),
          &resolved_connection));
    }
  } else if (with_connection != nullptr) {
    if (!language().LanguageFeatureEnabled(FEATURE_V_1_3_REMOTE_FUNCTION)) {
      return MakeSqlErrorAt(with_connection)
             << "WITH CONNECTION clause is not supported";
    }

    if (function_language != nullptr &&
        zetasql_base::CaseEqual(function_language->GetAsString(), "REMOTE")) {
      return MakeSqlErrorAt(with_connection)
             << "To create a remote function using a connection, use 'CREATE "
                "FUNCTION ... REMOTE WITH CONNECTION <connection>', instead "
                "of using LANGUAGE clause";
    }

    return MakeSqlErrorAt(with_connection)
           << "WITH CONNECTION clause should be preceded by keyword REMOTE "
              "and can't be used together with LANGUAGE clause";
  }

  // If REMOTE keyword is used or LANGUAGE is set to "REMOTE" and the feature is
  // enabled.
  if (is_remote &&
      language().LanguageFeatureEnabled(FEATURE_V_1_3_REMOTE_FUNCTION)) {
    // Following checks are skipped for the current consumers without the
    // feature enabled but using LANGUAGE REMOTE. They may have their own
    // checking logic and throw their own errors.
    if (sql_function_body != nullptr || code != nullptr) {
      return MakeSqlErrorAt(ast_statement)
             << "REMOTE function cannot include AS clause";
    }

    if (sql_security !=
        ResolvedCreateStatementEnums::SQL_SECURITY_UNSPECIFIED) {
      return MakeSqlErrorAt(ast_statement)
             << "REMOTE function cannot include SQL SECURITY clause";
    }
  }

  *output = MakeResolvedCreateFunctionStmt(
      function_name, create_scope, create_mode, has_explicit_return_type,
      return_type, arg_info->ArgumentNames(), *signature, is_aggregate,
      language_string, code_string, std::move(resolved_aggregate_exprs),
      std::move(resolved_expr), std::move(resolved_options), sql_security,
      ConvertDeterminismLevel(ast_statement->determinism_level()), is_remote,
      std::move(resolved_connection));
  MaybeRecordParseLocation(ast_statement->function_declaration()->name(),
                           output->get());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateTableFunctionStatement(
    const ASTCreateTableFunctionStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  if (!language().LanguageFeatureEnabled(FEATURE_CREATE_TABLE_FUNCTION)) {
    return MakeSqlErrorAt(ast_statement)
        << "Creating table-valued functions is not supported";
  }

  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(
      ast_statement, "CREATE TABLE FUNCTION", &create_scope, &create_mode));

  std::vector<std::string> function_name;
  auto arg_info = absl::make_unique<FunctionArgumentInfo>();
  ZETASQL_RETURN_IF_ERROR(
      ResolveFunctionDeclaration(ast_statement->function_declaration(),
                                 ResolveFunctionDeclarationType::TABLE_FUNCTION,
                                 &function_name, arg_info.get()));
  const bool has_explicit_return_schema =
      ast_statement->return_tvf_schema() != nullptr;
  TVFRelation return_tvf_relation({});
  bool has_return_tvf_relation = false;
  if (has_explicit_return_schema) {
    ZETASQL_RETURN_IF_ERROR(ResolveTVFSchema(
        ast_statement->return_tvf_schema(),
        ResolveTVFSchemaCheckPropertiesType::INVALID_OUTPUT_SCHEMA,
        &return_tvf_relation));
    has_return_tvf_relation = true;
  }

  const ASTIdentifier* language = ast_statement->language();
  const ASTStringLiteral* code = ast_statement->code();
  const ASTQuery* query = ast_statement->query();
  if (language != nullptr && query != nullptr) {
    return MakeSqlErrorAt(ast_statement)
        << "Function cannot specify a LANGUAGE and include a SQL body";
  }
  if (language != nullptr && ast_statement->return_tvf_schema() == nullptr) {
    return MakeSqlErrorAt(ast_statement)
        << "Non-SQL functions must specify a return type";
  }
  if (language == nullptr && code != nullptr) {
    return MakeSqlErrorAt(ast_statement)
        << "Function cannot specify a literal string body without a LANGUAGE";
  }

  std::string language_string = "UNDECLARED";
  if (language != nullptr) {
    language_string = language->GetAsString();
    if (zetasql_base::CaseEqual(language_string, "SQL")) {
      return MakeSqlErrorAt(ast_statement->language())
             << "To write SQL table-valued functions, omit the LANGUAGE clause "
             << "and write the function body using 'AS SELECT ...'";
    }
  }
  if (query != nullptr) {
    ZETASQL_RET_CHECK_EQ(language, nullptr);
    language_string = "SQL";
  }

  std::string code_string;
  if (code != nullptr) {
    code_string = code->string_value();
  }

  std::unique_ptr<const ResolvedScan> resolved_query;
  std::vector<std::unique_ptr<const ResolvedOutputColumn>>
      resolved_output_column_list;

  ZETASQL_RETURN_IF_ERROR(FailIfContainsParameterExpr(
      query, "SQL function",
      ast_statement->function_declaration()->name()->ToIdentifierPathString()));

  // Resolve the SQL function body if this function declaration includes one
  // and the function declaration does not contain any templated arguments.
  if (query != nullptr && !arg_info->contains_templated_arguments()) {
    for (const ASTFunctionParameter* param :
         ast_statement->function_declaration()
             ->parameters()->parameter_entries()) {
      if (param->IsTemplated()) {
        return UnsupportedArgumentError(
            *param, "CREATE TABLE FUNCTION declarations with SQL bodies");
      }
    }
    ZETASQL_RET_CHECK_EQ(language, nullptr);
    std::shared_ptr<const NameList> tvf_body_name_list;
    {
      // Set the argument info member variable in Resolver so that arguments are
      // in scope for the function body. The scoped_reset will set the variable
      // back to nullptr after resolving the function body.
      auto scoped_reset = SetArgumentInfo(arg_info.get());
      ZETASQL_RETURN_IF_ERROR(ResolveQuery(query, empty_name_scope_.get(), kQueryId,
                                   /*is_outer_query=*/false, &resolved_query,
                                   &tvf_body_name_list));
    }
    for (const NamedColumn& column : tvf_body_name_list->columns()) {
      resolved_output_column_list.emplace_back(
          MakeResolvedOutputColumn(column.name.ToString(), column.column));
    }

    // Resolve the output schema of the table-valued function. This builds a
    // TVFRelation object representing the output schema of the table-valued
    // function.
    //
    // If the function does not include a "RETURNS TABLE" section, automatically
    // generate this output schema from the output column list of the function's
    // SQL body.
    //
    // Otherwise, compare the explicit output schema from the "RETURNS TABLE"
    // section against the implicit output schema from the output column list of
    // the function's SQL body, returning an error if they are not compatible or
    // inserting a ResolvedProjectScan if they are coercible but not equal.
    if (has_explicit_return_schema) {
      ZETASQL_RETURN_IF_ERROR(CheckSQLBodyReturnTypesAndCoerceIfNeeded(
          ast_statement, return_tvf_relation, tvf_body_name_list.get(),
          &resolved_query, &resolved_output_column_list));
    } else {
      has_return_tvf_relation = true;
      if (tvf_body_name_list->is_value_table()) {
        ZETASQL_RET_CHECK_EQ(1, tvf_body_name_list->num_columns());
        return_tvf_relation = TVFRelation::ValueTable(
            tvf_body_name_list->column(0).column.type());
      } else {
        std::vector<TVFRelation::Column> return_tvf_relation_columns;
        return_tvf_relation_columns.reserve(tvf_body_name_list->num_columns());
        for (const NamedColumn& tvf_body_name_list_column :
             tvf_body_name_list->columns()) {
          if (IsInternalAlias(tvf_body_name_list_column.name)) {
            return MakeSqlErrorAt(ast_statement->query())
                << "Table-valued function SQL body without a RETURNS TABLE "
                << "clause is missing one or more explicit output column names";
          }
          return_tvf_relation_columns.emplace_back(
              tvf_body_name_list_column.name.ToString(),
              tvf_body_name_list_column.column.type());
        }
        return_tvf_relation = TVFRelation(return_tvf_relation_columns);
      }
    }
  }

  FunctionSignatureOptions signature_options;
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<FreestandingDeprecationWarning>
          additional_deprecation_warnings,
      StatusesToDeprecationWarnings(
          // We always use ERROR_MESSAGE_WITH_PAYLOAD because we don't want the
          // contents of the function signature to depend on
          // AnalyzerOptions.error_message_mode().
          ConvertInternalErrorLocationsAndAdjustErrorStrings(
              ERROR_MESSAGE_WITH_PAYLOAD, sql_, deprecation_warnings_),
          sql_));
  signature_options.set_additional_deprecation_warnings(
      additional_deprecation_warnings);

  FunctionSignature signature(
      has_return_tvf_relation
          ? FunctionArgumentType::RelationWithSchema(
                return_tvf_relation,
                /*extra_relation_input_columns_allowed=*/true)
          : FunctionArgumentType::AnyRelation(),
      arg_info->SignatureArguments(), /*context_id=*/0, signature_options);

  auto sql_security = static_cast<ResolvedCreateStatementEnums::SqlSecurity>(
      ast_statement->sql_security());

  if (create_scope != ResolvedCreateStatementEnums::CREATE_DEFAULT_SCOPE &&
      sql_security != ResolvedCreateStatementEnums::SQL_SECURITY_UNSPECIFIED) {
    return MakeSqlErrorAt(ast_statement)
           << "SQL SECURITY clause is not supported on statements with the "
           << CreateScopeErrorString(create_scope) << " modifier.";
  }

  // Option resolution is done with an empty namescope. That includes any
  // function arguments. Function arguments have values at function invocation
  // time, but not at function create time when options are evaluated.
  ZETASQL_RET_CHECK_EQ(function_argument_info_, nullptr);
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(ast_statement->options_list(), &resolved_options));

  // If the function has a SQL statement body, copy the body SQL to the code
  // field.
  if (ast_statement->query() != nullptr) {
    const ParseLocationRange& range =
        ast_statement->query()->GetParseLocationRange();
    code_string = std::string(sql_.substr(
        range.start().GetByteOffset(),
        range.end().GetByteOffset() - range.start().GetByteOffset()));
  }
  *output = MakeResolvedCreateTableFunctionStmt(
      function_name, create_scope, create_mode, arg_info->ArgumentNames(),
      signature, has_explicit_return_schema, std::move(resolved_options),
      language_string, code_string, std::move(resolved_query),
      std::move(resolved_output_column_list),
      return_tvf_relation.is_value_table(), sql_security);
  MaybeRecordParseLocation(ast_statement->function_declaration()->name(),
                           output->get());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveTVFSchema(
    const ASTTVFSchema* ast_tvf_schema,
    ResolveTVFSchemaCheckPropertiesType check_type, TVFRelation* tvf_relation) {
  // Check the columns of the parsed schema to see which ones have names.
  // If there is exactly one column and it has no name, then this schema
  // represents a value table. Otherwise, all columns must have names.
  if (ast_tvf_schema->columns().size() == 1 &&
      ast_tvf_schema->columns()[0]->name() == nullptr) {
    ZETASQL_RET_CHECK(ast_tvf_schema->columns()[0]->type() != nullptr);
    const Type* resolved_type = nullptr;
    ZETASQL_RETURN_IF_ERROR(ResolveType(ast_tvf_schema->columns()[0]->type(),
                                "table function signatures", &resolved_type,
                                /*resolved_type_params=*/nullptr));
    TVFRelation::Column column("", resolved_type);
    RecordTVFRelationColumnParseLocationsIfPresent(
        *ast_tvf_schema->columns()[0], &column);
    *tvf_relation = TVFRelation::ValueTable(column);
    return absl::OkStatus();
  }
  std::vector<TVFRelation::Column> tvf_relation_columns;
  for (const ASTTVFSchemaColumn* ast_tvf_schema_column :
       ast_tvf_schema->columns()) {
    const Type* resolved_type = nullptr;
    ZETASQL_RETURN_IF_ERROR(ResolveType(ast_tvf_schema_column->type(),
                                "table function signatures", &resolved_type,
                                /*resolved_type_params=*/nullptr));
    std::string name;
    if (ast_tvf_schema_column->name() != nullptr) {
      name = ast_tvf_schema_column->name()->GetAsString();
    }
    TVFRelation::Column column(name, resolved_type);
    RecordTVFRelationColumnParseLocationsIfPresent(*ast_tvf_schema_column,
                                                   &column);
    tvf_relation_columns.push_back(column);
  }
  *tvf_relation = TVFRelation(tvf_relation_columns);

  if (check_type != ResolveTVFSchemaCheckPropertiesType::SKIP_CHECKS) {
    // Check that the column names appear as expected. If this is a value table,
    // then there should be exactly one column with no name. Otherwise, if this
    // is not a value table, then all columns should have names.
    const bool is_invalid_value_table = tvf_relation->is_value_table() &&
                                        (tvf_relation->num_columns() != 1 ||
                                         tvf_relation->column(0).name.empty());
    const bool is_invalid_non_value_table =
        tvf_relation->num_columns() != 1 &&
        std::any_of(tvf_relation->columns().begin(),
                    tvf_relation->columns().end(),
                    [](const TVFRelation::Column& column) {
                      return column.name.empty();
                    });
    if (is_invalid_value_table || is_invalid_non_value_table) {
      if (check_type ==
          ResolveTVFSchemaCheckPropertiesType::INVALID_TABLE_ARGUMENT) {
        return MakeSqlErrorAt(ast_tvf_schema)
            << "Invalid table argument for table-valued function: each column "
            << "requires a name, unless there is exactly one unnamed column, "
            << "in which case the function argument is a value table";
      } else {
        return MakeSqlErrorAt(ast_tvf_schema)
            << "Invalid table-valued function output schema: each column "
            << "requires a name, unless there is exactly one unnamed column, "
            << "in which case the function returns a value table";
      }
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::CheckSQLBodyReturnTypesAndCoerceIfNeeded(
    const ASTNode* statement_location,
    const TVFRelation& return_tvf_relation, const NameList* tvf_body_name_list,
    std::unique_ptr<const ResolvedScan>* resolved_query,
    std::vector<std::unique_ptr<const ResolvedOutputColumn>>*
        resolved_output_column_list) {
  // Return an error if the table function signature includes a column name
  // that was not included in the SQL function body, or if the included
  // column has a type that is not equal or coercible to the required type.
  const int num_required_cols = return_tvf_relation.num_columns();
  const int num_provided_cols = tvf_body_name_list->num_columns();
  std::map<std::string, int, zetasql_base::CaseLess>
      provided_col_name_to_idx;
  std::set<std::string> duplicate_col_names;
  for (int provided_col_idx = 0;
       provided_col_idx < tvf_body_name_list->num_columns();
       ++provided_col_idx) {
    const std::string provided_col_name =
        tvf_body_name_list->column(provided_col_idx).name.ToString();
    if (!zetasql_base::InsertIfNotPresent(&provided_col_name_to_idx, provided_col_name,
                                 provided_col_idx)) {
      zetasql_base::InsertIfNotPresent(&duplicate_col_names, provided_col_name);
    }
  }
  bool add_projection_to_rearrange_provided_col_names = false;
  for (int required_col_idx = 0; required_col_idx < num_required_cols;
       ++required_col_idx) {
    const std::string& required_col_name =
        return_tvf_relation.column(required_col_idx).name;
    const Type* required_col_type =
        return_tvf_relation.column(required_col_idx).type;
    int provided_col_idx = 0;
    if (!return_tvf_relation.is_value_table() &&
        !tvf_body_name_list->is_value_table()) {
      provided_col_idx =
          zetasql_base::FindWithDefault(provided_col_name_to_idx, required_col_name, -1);
      if (provided_col_idx == -1) {
        const std::string error = absl::StrCat(
            "Required column name ", required_col_name,
            " not returned from SQL body of CREATE TABLE FUNCTION statement");
        if (statement_location != nullptr) {
          return MakeSqlErrorAt(statement_location) << error;
        } else {
          return MakeSqlError() << error;
        }
      } else if (zetasql_base::ContainsKey(duplicate_col_names, required_col_name)) {
        const std::string error =
            absl::StrCat("Required column name ", required_col_name,
                         " returned multiple times from SQL body of "
                         "CREATE TABLE FUNCTION statement");
        if (statement_location != nullptr) {
          return MakeSqlErrorAt(statement_location) << error;
        } else {
          return MakeSqlError() << error;
        }
      }
    }
    const Type* provided_col_type =
        tvf_body_name_list->column(provided_col_idx).column.type();
    SignatureMatchResult signature_match_result;
    if (!coercer_.CoercesTo(InputArgumentType(provided_col_type),
                            required_col_type, /*is_explicit=*/false,
                            &signature_match_result)) {
      std::string column_description;
      if (return_tvf_relation.is_value_table()) {
        const std::string error = absl::StrCat(
            "Value-table column for the output table of a CREATE TABLE "
            "FUNCTION statement has type ",
            required_col_type->ShortTypeName(product_mode()),
            ", but the SQL body provides incompatible type ",
            provided_col_type->ShortTypeName(product_mode()),
            " for the value-table column");
        if (statement_location != nullptr) {
          return MakeSqlErrorAt(statement_location) << error;
        } else {
          return MakeSqlError() << error;
        }
      } else {
        const std::string error = absl::StrCat(
            "Column ", required_col_name,
            " for the output table of a CREATE TABLE FUNCTION statement "
            "has type ",
            required_col_type->ShortTypeName(product_mode()),
            ", but the SQL body provides incompatible type ",
            provided_col_type->ShortTypeName(product_mode()),
            " for this column");
        if (statement_location != nullptr) {
          return MakeSqlErrorAt(statement_location) << error;
        } else {
          return MakeSqlError() << error;
        }
      }
    }
    if (provided_col_idx != required_col_idx ||
        !provided_col_type->Equals(required_col_type)) {
      add_projection_to_rearrange_provided_col_names = true;
    }
  }
  if (num_required_cols != num_provided_cols) {
    add_projection_to_rearrange_provided_col_names = true;
  }
  if (!return_tvf_relation.is_value_table() &&
      tvf_body_name_list->is_value_table()) {
    add_projection_to_rearrange_provided_col_names = true;
  }

  // If the names and types of the columns returned by the SQL function body are
  // in a different order than the required column list specified in the
  // function declaration "RETURNS TABLE" clause, or if the returned column
  // types are different but coercible to the required column types, add a
  // projection to corece and/or rearrange the provided columns to match the
  // required columns.
  if (add_projection_to_rearrange_provided_col_names) {
    resolved_output_column_list->clear();
    const IdString new_project_alias = AllocateSubqueryName();
    std::vector<ResolvedColumn> new_column_list;
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        new_project_columns;
    new_column_list.reserve(num_required_cols);
    new_project_columns.reserve(num_required_cols);
    for (int required_col_idx = 0; required_col_idx < num_required_cols;
         ++required_col_idx) {
      const std::string& required_col_name =
          return_tvf_relation.column(required_col_idx).name;
      const Type* required_col_type =
          return_tvf_relation.column(required_col_idx).type;
      int provided_col_idx;
      if (return_tvf_relation.is_value_table() ||
          tvf_body_name_list->is_value_table()) {
        provided_col_idx = 0;
      } else {
        provided_col_idx = zetasql_base::FindWithDefault(provided_col_name_to_idx,
                                                required_col_name, -1);
      }
      ZETASQL_RET_CHECK_NE(-1, provided_col_idx) << "\"" << required_col_name << "\"";
      const ResolvedColumn& provided_col =
          tvf_body_name_list->column(provided_col_idx).column;
      const Type* provided_col_type = provided_col.type();
      // Here we compare the provided and required column types. If they are
      // Equals, then we accept the provided column as-is. Otherwise, if they
      // are implicitly coercible, we add a ResolvedCast to the required type.
      // This includes when the two types are Equivalent but not Equals.
      if (provided_col_type->Equals(required_col_type)) {
        new_column_list.push_back(provided_col);
      } else {
        new_column_list.emplace_back(AllocateColumnId(), new_project_alias,
                                     provided_col.name_id(), required_col_type);
        std::unique_ptr<const ResolvedExpr> resolved_cast =
            MakeColumnRef(provided_col, /*is_correlated=*/false);
        ZETASQL_RETURN_IF_ERROR(ResolveCastWithResolvedArgument(
            statement_location, required_col_type,
            /*return_null_on_error=*/false, &resolved_cast));
        new_project_columns.push_back(MakeResolvedComputedColumn(
            new_column_list.back(), std::move(resolved_cast)));
        RecordColumnAccess(new_column_list.back());
      }
      if (!return_tvf_relation.is_value_table() &&
          tvf_body_name_list->is_value_table()) {
        resolved_output_column_list->emplace_back(MakeResolvedOutputColumn(
            return_tvf_relation.column(0).name, new_column_list.back()));
      } else {
        resolved_output_column_list->emplace_back(MakeResolvedOutputColumn(
            new_column_list.back().name(), new_column_list.back()));
      }
    }
    *resolved_query =
        MakeResolvedProjectScan(new_column_list, std::move(new_project_columns),
                                std::move(*resolved_query));
  }
  return absl::OkStatus();
}

absl::Status Resolver::UnsupportedArgumentError(
    const ASTFunctionParameter& argument, const std::string& context) {
  if (argument.IsTemplated()) {
    // Templated arguments are only allowed when the language option is
    // enabled.
    if (!language().LanguageFeatureEnabled(FEATURE_TEMPLATE_FUNCTIONS)) {
      return MakeSqlErrorAt(argument.templated_parameter_type())
             << "Functions with templated arguments are not supported";
    }
    // We should only get here for templated arguments where an alias is
    // present. For example, "CREATE FUNCTION FOO (name ANY TYPE AS type_alias)"
    // should return an error since the alias does not make sense in this
    // context.
    if (argument.alias() != nullptr) {
      return MakeSqlErrorAt(argument.alias())
             << "Templated arguments with type aliases are not supported yet";
    }
    // Templated arguments other than ANY TYPE or ANY TABLE are not supported
    // yet.
    switch (argument.templated_parameter_type()->kind()) {
      case ASTTemplatedParameterType::ANY_TYPE:
      case ASTTemplatedParameterType::ANY_TABLE:
        break;
      default:
        return MakeSqlErrorAt(argument.templated_parameter_type())
               << "Templated arguments other than ANY TYPE or ANY TABLE in "
               << context << " are not supported yet";
    }
    return MakeSqlErrorAt(argument.templated_parameter_type())
           << "Templated arguments in " << context << " are not supported yet";
  }
  // We should only get here for non-templated arguments where a type alias
  // is present.
  ZETASQL_RET_CHECK(argument.alias() != nullptr);
  return MakeSqlErrorAt(argument.alias())
         << "Non-templated arguments in " << context
         << " do not support type aliases";
}

absl::Status Resolver::ResolveCreateProcedureStatement(
    const ASTCreateProcedureStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(
      ast_statement,
      /*statement_type=*/"CREATE PROCEDURE", &create_scope, &create_mode));

  const std::vector<std::string> procedure_name =
      ast_statement->name()->ToIdentifierVector();
  auto arg_info = absl::make_unique<FunctionArgumentInfo>();
  ZETASQL_RETURN_IF_ERROR(ResolveFunctionParameters(
      ast_statement->parameters(), ResolveFunctionDeclarationType::PROCEDURE,
      arg_info.get()));

  auto signature = absl::make_unique<FunctionSignature>(
      FunctionArgumentType(ARG_TYPE_VOID), arg_info->SignatureArguments(),
      /*context_id=*/0);

  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(ResolveOptionsList(ast_statement->options_list(),
                                     &resolved_options));

  ZETASQL_RETURN_IF_ERROR(FailIfContainsParameterExpr(
      ast_statement->body(), "procedure",
      ast_statement->name()->ToIdentifierPathString()));

  // Copy procedure body from BEGIN <statement_list> END block
  const ParseLocationRange& range =
      ast_statement->body()->GetParseLocationRange();
  ZETASQL_RET_CHECK_GE(sql_.length(), range.end().GetByteOffset()) << sql_;
  absl::string_view procedure_body =
      sql_.substr(range.start().GetByteOffset(),
                  range.end().GetByteOffset() - range.start().GetByteOffset());

  // Validates procedure body. See ParsedScript::Create() for the list of checks
  // being done.
  ParsedScript::ArgumentTypeMap arguments_map;
  for (const ASTFunctionParameter* function_param :
       ast_statement->parameters()->parameter_entries()) {
    // Always use nullptr as type of variable, for type is not used during
    // validation and getting correct type here is lengthy.
    arguments_map[function_param->name()->GetAsIdString()] = nullptr;
  }
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ParsedScript> parsed_script,
      ParsedScript::CreateForRoutine(sql_, ast_statement->body(),
                                     analyzer_options_.error_message_mode(),
                                     std::move(arguments_map)));
  ZETASQL_RETURN_IF_ERROR(parsed_script->CheckQueryParameters(absl::nullopt));

  *output = MakeResolvedCreateProcedureStmt(
      procedure_name, create_scope, create_mode, arg_info->ArgumentNames(),
      *signature, std::move(resolved_options), std::string(procedure_body));
  MaybeRecordParseLocation(ast_statement->name(), output->get());
  return absl::OkStatus();
}

static FunctionEnums::ProcedureArgumentMode GetProcedureArgumentMode(
    ASTFunctionParameter::ProcedureParameterMode mode) {
  switch (mode) {
    case ASTFunctionParameter::ProcedureParameterMode::IN:
      return FunctionEnums::IN;
    case ASTFunctionParameter::ProcedureParameterMode::OUT:
      return FunctionEnums::OUT;
    case ASTFunctionParameter::ProcedureParameterMode::INOUT:
      return FunctionEnums::INOUT;
    case ASTFunctionParameter::ProcedureParameterMode::NOT_SET:
      return FunctionEnums::NOT_SET;
  }
}

absl::Status Resolver::ResolveFunctionDeclaration(
    const ASTFunctionDeclaration* function_declaration,
    ResolveFunctionDeclarationType function_type,
    std::vector<std::string>* function_name, FunctionArgumentInfo* arg_info) {
  ZETASQL_RET_CHECK(function_declaration->name() != nullptr);
  *function_name = function_declaration->name()->ToIdentifierVector();
  return ResolveFunctionParameters(function_declaration->parameters(),
                                   function_type, arg_info);
}

absl::Status Resolver::ResolveFunctionParameters(
    const ASTFunctionParameters* ast_function_parameters,
    ResolveFunctionDeclarationType function_type,
    FunctionArgumentInfo* arg_info) {
  bool default_arg_exists = false;
  for (const ASTFunctionParameter* function_param :
       ast_function_parameters->parameter_entries()) {
    ZETASQL_RET_CHECK(function_param != nullptr);
    // Null parameter name is allowed by grammar for DROP statements, but
    // parameter names must be provided for function declarations.
    if (function_param->name() == nullptr) {
      return MakeSqlErrorAt(function_param)
             << "Parameters in function declarations must include both name "
                "and type";
    }
    const bool is_any_type_arg =
        function_param->IsTemplated() &&
        (function_param->templated_parameter_type()->kind() ==
         ASTTemplatedParameterType::ANY_TYPE);
    const bool is_any_table_arg =
        function_param->IsTemplated() &&
        (function_param->templated_parameter_type()->kind() ==
         ASTTemplatedParameterType::ANY_TABLE);
    if (function_param->alias() != nullptr) {
      // TODO: When we support type aliases, ensure that an error
      // is provided if two types are given the same alias.
      return UnsupportedArgumentError(*function_param,
                                       "function declarations");
    } else if (function_param->IsTemplated()) {
      // "ANY TYPE" and "ANY TABLE" types are supported in procedure or when
      // language feature is enabled for function.
      if ((function_type != ResolveFunctionDeclarationType::PROCEDURE &&
           !language().LanguageFeatureEnabled(FEATURE_TEMPLATE_FUNCTIONS)) ||
          (!is_any_type_arg && !is_any_table_arg)) {
        return UnsupportedArgumentError(*function_param,
                                         "function declarations");
      }
    }
    if (function_type == ResolveFunctionDeclarationType::PROCEDURE &&
        function_param->IsTableParameter() &&
        (function_param->procedure_parameter_mode() ==
             ASTFunctionParameter::ProcedureParameterMode::OUT ||
         function_param->procedure_parameter_mode() ==
             ASTFunctionParameter::ProcedureParameterMode::INOUT)) {
      return MakeSqlErrorAt(function_param)
             << "Table parameters cannot have OUT or INOUT mode";
    }
    if (function_type != ResolveFunctionDeclarationType::AGGREGATE_FUNCTION &&
        function_param->is_not_aggregate()) {
      return MakeSqlErrorAt(function_param)
             << "Parameters can only be marked NOT AGGREGATE in "
                "functions created with CREATE AGGREGATE FUNCTION";
    }
    FunctionArgumentTypeOptions argument_type_options;
    RecordArgumentParseLocationsIfPresent(*function_param,
                                          &argument_type_options);
    argument_type_options.set_procedure_argument_mode(
        GetProcedureArgumentMode(function_param->procedure_parameter_mode()));
    argument_type_options.set_argument_name(
        function_param->name()->GetAsString());
    ResolvedArgumentDef::ArgumentKind arg_kind;
    if (function_type == ResolveFunctionDeclarationType::AGGREGATE_FUNCTION) {
      if (function_param->is_not_aggregate()) {
        arg_kind = ResolvedArgumentDef::NOT_AGGREGATE;
        argument_type_options.set_is_not_aggregate(true);
      } else {
        arg_kind = ResolvedArgumentDef::AGGREGATE;
      }
    } else {
      arg_kind = ResolvedArgumentDef::SCALAR;
    }
    const IdString name = function_param->name()->GetAsIdString();
    if (arg_info->HasArg(name)) {
      return MakeSqlErrorAt(function_param->name())
          << "Duplicate argument name " << name;
    }
    if (function_param->IsTableParameter()) {
      if (function_type != ResolveFunctionDeclarationType::TABLE_FUNCTION &&
          function_type != ResolveFunctionDeclarationType::PROCEDURE) {
        return MakeSqlErrorAt(function_param)
               << "TABLE parameters are not allowed in CREATE FUNCTION "
                  "statement";
      }
      if (function_param->default_value() != nullptr) {
        return MakeSqlErrorAt(function_param)
            << "TABLE parameters are not allowed to have default values";
      }
      if (default_arg_exists) {
        return MakeSqlErrorAt(function_param)
               << "Function parameter with a default value cannot be followed "
                  "by non-default parameters";
      }
      if (is_any_table_arg) {
        ZETASQL_RETURN_IF_ERROR(arg_info->AddRelationArg(
            name, FunctionArgumentType(ARG_TYPE_RELATION, argument_type_options,
                                       /*num_occurrences=*/1)));
        continue;
      }
      ZETASQL_RET_CHECK(function_param->type() == nullptr);
      auto resolved_tvf_relation =
          std::make_shared<TVFRelation>(TVFRelation::ColumnList{});
      ZETASQL_RETURN_IF_ERROR(ResolveTVFSchema(
          function_param->tvf_schema(),
          ResolveTVFSchemaCheckPropertiesType::INVALID_TABLE_ARGUMENT,
          resolved_tvf_relation.get()));
      argument_type_options.set_relation_input_schema(
          std::move(resolved_tvf_relation));
      argument_type_options.set_extra_relation_input_columns_allowed(true);
      RecordArgumentParseLocationsIfPresent(*function_param,
                                            &argument_type_options);
      ZETASQL_RETURN_IF_ERROR(arg_info->AddRelationArg(
          name,
          FunctionArgumentType(ARG_TYPE_RELATION, argument_type_options)));
    } else {
      absl::optional<Value> default_value;
      bool default_value_has_explicit_type = false;
      if (function_param->default_value() != nullptr) {
        if (!language().LanguageFeatureEnabled(
                FEATURE_FUNCTION_ARGUMENTS_WITH_DEFAULTS)) {
          return MakeSqlErrorAt(function_param->default_value())
              << "Function arguments with DEFAULT values are not supported";
        }
        std::unique_ptr<const ResolvedExpr> resolved_expr;
        ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(
            function_param->default_value(), empty_name_scope_.get(),
            "function parameter", &resolved_expr));
        if (resolved_expr == nullptr ||
            resolved_expr->node_kind() != RESOLVED_LITERAL) {
          return MakeSqlErrorAt(function_param->default_value())
                 << "Function parameter default value must be a literal";
        }
        const ResolvedLiteral* resolved_literal =
            resolved_expr->GetAs<ResolvedLiteral>();
        ZETASQL_RET_CHECK(resolved_literal != nullptr);

        default_value_has_explicit_type = resolved_literal->has_explicit_type();
        default_value = resolved_literal->value();
        default_arg_exists = true;
      } else if (default_arg_exists) {
        return MakeSqlErrorAt(function_param)
               << "Function parameter with a default value cannot be followed "
                  "by non-default parameters";
      }

      if (is_any_type_arg) {
        if (default_value.has_value()) {
          argument_type_options.set_default(std::move(*default_value));
          argument_type_options.set_cardinality(FunctionArgumentType::OPTIONAL);
        }
        ZETASQL_RETURN_IF_ERROR(arg_info->AddScalarArg(
            name, arg_kind,
            FunctionArgumentType(ARG_TYPE_ARBITRARY, argument_type_options,
                                 /*num_occurrences=*/1)));
        continue;
      }

      ZETASQL_RET_CHECK(function_param->type() != nullptr);
      const Type* resolved_type = nullptr;
      ZETASQL_RETURN_IF_ERROR(ResolveType(function_param->type(), "function arguments",
                                  &resolved_type,
                                  /*resolved_type_params=*/nullptr));
      ZETASQL_RET_CHECK(resolved_type != nullptr) << function_param->DebugString();
      if (default_value.has_value()) {
        if (!resolved_type->Equals(default_value->type())) {
          if (!default_value_has_explicit_type && default_value->is_null()) {
              // Special handling for untyped literal NULL, as <default_value>
              // is having its type as INT64 now.
              default_value = Value::Null(resolved_type);
          } else if (!default_value_has_explicit_type &&
                     default_value->is_empty_array() &&
                     resolved_type->IsArray()) {
            // Special handling for untyped empty array, as <default_value>
            // is having its type as ARRAY<INT64> now.
            default_value =
                Value::Array(resolved_type->AsArray(), /*values=*/{});
          } else {
            InputArgumentType arg(*default_value,
                                  /*is_default_argument_value=*/true);
            SignatureMatchResult match_result;
            if (!coercer_.AssignableTo(arg, resolved_type, /*is_explicit=*/true,
                                       &match_result)) {
              return MakeSqlErrorAt(function_param->default_value())
                     << "Default argument value does not match the argument "
                        "type. Got: "
                     << default_value->type()->ShortTypeName(
                            analyzer_options_.language().product_mode())
                     << ", expected: "
                     << resolved_type->ShortTypeName(
                            analyzer_options_.language().product_mode());
            }
            ZETASQL_ASSIGN_OR_RETURN(
                default_value,
                CastValue(*default_value, analyzer_options_.default_time_zone(),
                          analyzer_options_.language(), resolved_type),
                _.Attach(GetErrorLocationPoint(function_param->default_value(),
                                               /*include_leftmost_child=*/true)
                             .ToInternalErrorLocation()));
          }
        }
        argument_type_options.set_default(std::move(*default_value));
        argument_type_options.set_cardinality(FunctionArgumentType::OPTIONAL);
      }
      ZETASQL_RETURN_IF_ERROR(arg_info->AddScalarArg(
          name, arg_kind,
          FunctionArgumentType(resolved_type, argument_type_options)));
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveTableAndPredicate(
    const ASTPathExpression* table_path, const ASTExpression* predicate,
    const char* clause_name,
    std::unique_ptr<const ResolvedTableScan>* resolved_table_scan,
    std::unique_ptr<const ResolvedExpr>* resolved_predicate,
    std::string* predicate_str) {
  ZETASQL_RET_CHECK(table_path != nullptr);
  const IdString alias = GetAliasForExpression(table_path);
  const ASTNode* alias_location = table_path;

  std::shared_ptr<const NameList> target_name_list(new NameList);
  ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsTableScan(
      table_path, alias, /*has_explicit_alias=*/false, alias_location,
      /*hints=*/nullptr, /*for_system_time=*/nullptr, empty_name_scope_.get(),
      resolved_table_scan, &target_name_list));
  ZETASQL_RET_CHECK(target_name_list->HasRangeVariable(alias));

  const std::shared_ptr<const NameScope> target_scope(
      new NameScope(/*previous_scope=*/nullptr, target_name_list));

  if (predicate != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(predicate, target_scope.get(),
                                      clause_name, resolved_predicate));
    ZETASQL_RETURN_IF_ERROR(CoerceExprToBool(predicate,
                                          "USING clause", resolved_predicate));

    if (predicate_str != nullptr) {
      // Extract the string form of predicate expression using ZetaSQL
      // unparser.
      parser::Unparser predicate_unparser(predicate_str);
      predicate->Accept(&predicate_unparser, /*data=*/nullptr);
      predicate_unparser.FlushLine();
      absl::StripAsciiWhitespace(predicate_str);
    }
  }

  return absl::OkStatus();
}

absl::Status Resolver::AddGranteeToExpressionList(
    const ASTExpression* grantee,
    std::vector<std::unique_ptr<const ResolvedExpr>>* grantee_expr_list) {
  ZETASQL_RET_CHECK(grantee->node_kind() == AST_PARAMETER_EXPR ||
            grantee->node_kind() == AST_STRING_LITERAL ||
            grantee->node_kind() == AST_SYSTEM_VARIABLE_EXPR)
      << grantee->DebugString();
  std::unique_ptr<const ResolvedExpr> grantee_expr;
  const NameScope empty_name_scope;
  // Since we are resolving an expression that is a literal or parameter,
  // we use an empty NameScope() here for resolution
  ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(grantee, &empty_name_scope,
                                    /*clause_name=*/"GRANTEE LIST",
                                    &grantee_expr));
  if (!analyzer_options_.allow_undeclared_parameters() &&
      !grantee_expr->type()->IsString()) {
    // Since the parser only allows parameters and STRING literals, and
    // the type is not STRING, then this must be a parameter expression.
    return MakeSqlErrorAt(grantee)
           << "Query parameters in the GRANTEE list must be STRING type";
  }
  grantee_expr_list->push_back(std::move(grantee_expr));
  return absl::OkStatus();
}

absl::Status Resolver::AddGranteeToList(
    const ASTExpression* grantee, std::vector<std::string>* grantee_list) {
  if (grantee->node_kind() == AST_PARAMETER_EXPR) {
    return MakeSqlErrorAt(grantee)
           << "The GRANTEE list only supports string literals, not parameters";
  } else if (grantee->node_kind() == AST_SYSTEM_VARIABLE_EXPR) {
    return MakeSqlErrorAt(grantee)
           << "The GRANTEE list only supports string literals, not system "
           << "variables";
  } else {
    ZETASQL_RET_CHECK(grantee->node_kind() == AST_STRING_LITERAL)
        << grantee->DebugString();
  }
  grantee_list->push_back(
      grantee->GetAsOrDie<ASTStringLiteral>()->string_value());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveGranteeList(
    const ASTGranteeList* ast_grantee_list,
    std::vector<std::string>* grantee_list,
    std::vector<std::unique_ptr<const ResolvedExpr>>* grantee_expr_list) {
  for (const ASTExpression* grantee : ast_grantee_list->grantee_list()) {
    if (language().LanguageFeatureEnabled(FEATURE_PARAMETERS_IN_GRANTEE_LIST)) {
      ZETASQL_RETURN_IF_ERROR(AddGranteeToExpressionList(grantee, grantee_expr_list));
    } else {
      ZETASQL_RETURN_IF_ERROR(AddGranteeToList(grantee, grantee_list));
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreatePrivilegeRestrictionStatement(
    const ASTCreatePrivilegeRestrictionStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  ZETASQL_RET_CHECK(ast_statement->name_path() != nullptr);

  // The statement type is used for error messaging.
  constexpr absl::string_view statement_type = "CREATE PRIVILEGE RESTRICTION";
  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;

  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(ast_statement, statement_type,
                                                &create_scope, &create_mode));

  ZETASQL_RET_CHECK(ast_statement->is_default_scope());

  // The parser ensures that no lifetime modifier is specified on
  // CREATE PRIVILEGE RESTRICTION statements.
  ZETASQL_RET_CHECK(!ast_statement->is_temp())
      << absl::StrFormat("CREATE TEMP %s is not supported", statement_type);

  std::vector<std::unique_ptr<const ResolvedExpr>> restrictee_expr_list;
  if (ast_statement->restrict_to() != nullptr) {
    // We reuse ResolveGranteeList because restrictee_list and grantee_list have
    // the same underlying types and are resolved the same way.
    if (ast_statement->restrict_to()->restrictee_list() != nullptr) {
      for (const ASTExpression* grantee :
           ast_statement->restrict_to()->restrictee_list()->grantee_list()) {
        ZETASQL_RETURN_IF_ERROR(
            AddGranteeToExpressionList(grantee, &restrictee_expr_list));
      }
    }
  }

  for (const ASTPrivilege* privilege :
       ast_statement->privileges()->privileges()) {
    if (privilege->paths() == nullptr ||
        privilege->paths()->path_expression_list().empty()) {
      return MakeSqlErrorAt(privilege) << "Expected privilege to contain paths";
    }
  }

  const ASTPathExpression* table_path = ast_statement->name_path();
  const IdString alias = GetAliasForExpression(table_path);
  std::unique_ptr<const ResolvedTableScan> resolved_table_scan;
  std::shared_ptr<const NameList> name_list;
  ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsTableScan(
      table_path, alias, /*has_explicit_alias=*/false,
      /*alias_location=*/table_path, /*hints=*/nullptr,
      /*for_system_time=*/nullptr, empty_name_scope_.get(),
      &resolved_table_scan, &name_list));
  const std::shared_ptr<const NameScope> name_scope =
      std::make_shared<NameScope>(/*previous_scope=*/nullptr, name_list);

  std::vector<std::unique_ptr<const ResolvedPrivilege>> column_privilege_list;
  ZETASQL_RETURN_IF_ERROR(ResolvePrivileges(ast_statement->privileges(),
                                    name_scope.get(),
                                    /*enable_nested_field_privileges=*/true,
                                    statement_type, &column_privilege_list));

  *output = MakeResolvedCreatePrivilegeRestrictionStmt(
      ast_statement->name_path()->ToIdentifierVector(), create_scope,
      create_mode, std::move(column_privilege_list),
      ast_statement->object_type()->GetAsString(),
      std::move(restrictee_expr_list));

  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateRowAccessPolicyStatement(
    const ASTCreateRowAccessPolicyStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  bool has_grant_clause = ast_statement->grant_to() != nullptr;
  bool has_access_keyword = ast_statement->has_access_keyword();
  bool has_grant_keyword_and_parens =
      has_grant_clause &&
      ast_statement->grant_to()->has_grant_keyword_and_parens();
  bool has_filter_keyword = ast_statement->filter_using()->has_filter_keyword();

  // We construct the correct row policy object name for error messaging based
  // on the provided syntax.
  std::string row_policy_object_name =
      ast_statement->has_access_keyword() ? "ROW ACCESS POLICY" : "ROW POLICY";
  std::string statement_name = absl::StrCat("CREATE ", row_policy_object_name);
  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(ast_statement, statement_name,
                                                &create_scope, &create_mode));

  // The parser ensures that no lifetime modifier is specified on
  // CREATE ROW ACCESS POLICY statements.
  ZETASQL_RET_CHECK(ast_statement->is_default_scope());

  // If this feature is disabled, then the new syntax must be used.
  if (!language().LanguageFeatureEnabled(
          FEATURE_ALLOW_LEGACY_ROW_ACCESS_POLICY_SYNTAX)) {
    if (!has_access_keyword) {
      return MakeSqlErrorAt(ast_statement)
             << "Expected keyword ACCESS between ROW and POLICY";
    }
    if (has_grant_clause && !has_grant_keyword_and_parens) {
      return MakeSqlErrorAt(ast_statement->grant_to())
             << "Expected keyword GRANT before TO";
    }
    if (!has_filter_keyword) {
      return MakeSqlErrorAt(ast_statement->filter_using())
             << "Expected keyword FILTER before USING";
    }
    if (ast_statement->name() == nullptr && !has_grant_clause) {
      return MakeSqlErrorAt(ast_statement)
             << "Omitting the GRANT TO clause is not supported for unnamed row "
                "access policies";
    }
  } else {
    // In the old syntax, the "[GRANT] TO" clause is required.
    if (!has_grant_clause) {
      return MakeSqlErrorAt(ast_statement)
             << "Missing TO <grantee_list> clause";
    }
  }

  std::string policy_name;
  if (ast_statement->name() != nullptr) {
    policy_name = ast_statement->name()->GetAsString();
  } else if (create_mode != ResolvedCreateStatement::CREATE_DEFAULT) {
    return MakeSqlErrorAt(ast_statement)
           << statement_name << " with "
           << ResolvedCreateStatementEnums::CreateMode_Name(create_mode)
           << " but policy name not specified";
  }

  const ASTPathExpression* target_path = ast_statement->target_path();
  std::unique_ptr<const ResolvedTableScan> resolved_table_scan;
  std::unique_ptr<const ResolvedExpr> resolved_predicate;
  std::string predicate_str;

  ZETASQL_RETURN_IF_ERROR(ResolveTableAndPredicate(
      target_path, ast_statement->filter_using()->predicate(),
      absl::StrCat(statement_name, " statement").c_str(), &resolved_table_scan,
      &resolved_predicate, &predicate_str));

  std::vector<std::string> grantee_list;
  std::vector<std::unique_ptr<const ResolvedExpr>> grantee_expr_list;
  if (has_grant_clause) {
    ZETASQL_RETURN_IF_ERROR(
        ResolveGranteeList(ast_statement->grant_to()->grantee_list(),
                           &grantee_list, &grantee_expr_list));
  }

  ZETASQL_RET_CHECK(!ast_statement->is_temp()) << absl::StrFormat(
      "CREATE TEMP %s is not supported", row_policy_object_name);

  *output = MakeResolvedCreateRowAccessPolicyStmt(
      create_mode, policy_name, target_path->ToIdentifierVector(), grantee_list,
      std::move(grantee_expr_list), std::move(resolved_table_scan),
      std::move(resolved_predicate), predicate_str);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterPrivilegeRestrictionStatement(
    const ASTAlterPrivilegeRestrictionStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  // If the table is not found, an error is returned. path() should never be
  // null here because the PRIVILEGE branch of the alter_action rule requires
  // a nonempty path.
  ZETASQL_RET_CHECK(ast_statement->path() != nullptr);

  // The statement type is used for error messaging.
  constexpr absl::string_view statement_type = "ALTER PRIVILEGE RESTRICTION";

  std::vector<std::unique_ptr<const ResolvedAlterAction>>
      resolved_alter_actions;
  const ASTAlterActionList* action_list = ast_statement->action_list();
  for (const ASTAlterAction* const action : action_list->actions()) {
    std::unique_ptr<const ResolvedAlterAction> alter_action;
    switch (action->node_kind()) {
      case AST_RESTRICT_TO_CLAUSE: {
        auto* restrict_to = action->GetAs<ASTRestrictToClause>();

        std::vector<std::unique_ptr<const ResolvedExpr>> restrictee_expr_list;
        // We reuse ResolveGranteeList because restrictee_list and
        // grantee_list have the same underlying types and are resolved the
        // same way.
        for (const ASTExpression* grantee :
             restrict_to->restrictee_list()->grantee_list()) {
          ZETASQL_RETURN_IF_ERROR(
              AddGranteeToExpressionList(grantee, &restrictee_expr_list));
        }

        alter_action =
            MakeResolvedRestrictToAction(std::move(restrictee_expr_list));
        break;
      }
      case AST_ADD_TO_RESTRICTEE_LIST_CLAUSE: {
        auto* add_restrictees = action->GetAs<ASTAddToRestricteeListClause>();
        std::vector<std::unique_ptr<const ResolvedExpr>> restrictee_expr_list;
        // We reuse ResolveGranteeList because restrictee_list and
        // grantee_list have the same underlying types and are resolved the
        // same way.
        if (add_restrictees->restrictee_list() != nullptr) {
          for (const ASTExpression* grantee :
               add_restrictees->restrictee_list()->grantee_list()) {
            ZETASQL_RETURN_IF_ERROR(
                AddGranteeToExpressionList(grantee, &restrictee_expr_list));
          }
        }
        alter_action = MakeResolvedAddToRestricteeListAction(
            add_restrictees->is_if_not_exists(),
            std::move(restrictee_expr_list));
        break;
      }
      case AST_REMOVE_FROM_RESTRICTEE_LIST_CLAUSE: {
        auto* remove_restrictees =
            action->GetAs<ASTRemoveFromRestricteeListClause>();
        std::vector<std::unique_ptr<const ResolvedExpr>> restrictee_expr_list;
        // We reuse ResolveGranteeList because restrictee_list and
        // grantee_list have the same underlying types and are resolved the
        // same way.
        for (const ASTExpression* grantee :
             remove_restrictees->restrictee_list()->grantee_list()) {
          ZETASQL_RETURN_IF_ERROR(
              AddGranteeToExpressionList(grantee, &restrictee_expr_list));
        }
        alter_action = MakeResolvedRemoveFromRestricteeListAction(
            remove_restrictees->is_if_exists(),
            std::move(restrictee_expr_list));
        break;
      }
      default:
        return MakeSqlErrorAt(action)
               << "ALTER PRIVILEGE RESTRICTION doesn't support "
               << action->GetNodeKindString() << " action.";
    }
    resolved_alter_actions.push_back(std::move(alter_action));
  }

  for (const ASTPrivilege* privilege :
       ast_statement->privileges()->privileges()) {
    if (privilege->paths() == nullptr ||
        privilege->paths()->path_expression_list().empty()) {
      return MakeSqlErrorAt(privilege) << "Expected privilege to contain paths";
    }
  }

  const ASTPathExpression* table_path = ast_statement->path();
  const IdString alias = GetAliasForExpression(table_path);
  std::unique_ptr<const ResolvedTableScan> resolved_table_scan;
  std::shared_ptr<const NameList> name_list;
  ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsTableScan(
      table_path, alias, /*has_explicit_alias=*/false,
      /*alias_location=*/table_path, /*hints=*/nullptr,
      /*for_system_time=*/nullptr, empty_name_scope_.get(),
      &resolved_table_scan, &name_list));
  const std::shared_ptr<const NameScope> name_scope =
      std::make_shared<NameScope>(/*previous_scope=*/nullptr, name_list);

  std::vector<std::unique_ptr<const ResolvedPrivilege>> column_privilege_list;
  ZETASQL_RETURN_IF_ERROR(ResolvePrivileges(ast_statement->privileges(),
                                    name_scope.get(),
                                    /*enable_nested_field_privileges=*/true,
                                    statement_type, &column_privilege_list));

  *output = MakeResolvedAlterPrivilegeRestrictionStmt(
      ast_statement->path()->ToIdentifierVector(),
      std::move(resolved_alter_actions), ast_statement->is_if_exists(),
      std::move(column_privilege_list),
      ast_statement->object_type()->GetAsString());

  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterRowAccessPolicyStatement(
    const ASTAlterRowAccessPolicyStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  // path() should never be null here because the single ROW branch of the
  // alter_action rule requires a nonempty path.
  ZETASQL_RET_CHECK(ast_statement->path() != nullptr);

  std::unique_ptr<const ResolvedTableScan> resolved_table_scan;
  std::vector<std::unique_ptr<const ResolvedAlterAction>>
      resolved_alter_actions;
  const ASTAlterActionList* action_list = ast_statement->action_list();
  absl::flat_hash_map<ASTNodeKind, std::unique_ptr<const ResolvedAlterAction>*>
      action_map;
  for (const ASTAlterAction* const action : action_list->actions()) {
    std::unique_ptr<const ResolvedAlterAction>*& existing_action =
        action_map[action->node_kind()];
    std::unique_ptr<const ResolvedAlterAction> alter_action;
    switch (action->node_kind()) {
      case AST_GRANT_TO_CLAUSE: {
        if (existing_action != nullptr) {
          return MakeSqlErrorAt(action)
                 << "Multiple GRANT TO actions are not supported";
        }
        auto* grant_to = action->GetAs<ASTGrantToClause>();
        std::vector<std::string> grantee_list;
        std::vector<std::unique_ptr<const ResolvedExpr>> grantee_expr_list;
        ZETASQL_RETURN_IF_ERROR(ResolveGranteeList(grant_to->grantee_list(),
                                           &grantee_list, &grantee_expr_list));
        alter_action = MakeResolvedGrantToAction(std::move(grantee_expr_list));
      } break;
      case AST_FILTER_USING_CLAUSE: {
        if (existing_action != nullptr) {
          return MakeSqlErrorAt(action)
                 << "Multiple FILTER USING actions are not supported";
        }
        auto* filter_using = action->GetAs<ASTFilterUsingClause>();
        std::unique_ptr<const ResolvedExpr> resolved_predicate;
        std::string predicate_str;
        ZETASQL_RETURN_IF_ERROR(ResolveTableAndPredicate(
            ast_statement->path(), filter_using->predicate(),
            "ALTER ROW ACCESS POLICY FILTER USING action", &resolved_table_scan,
            &resolved_predicate, &predicate_str));
        alter_action = MakeResolvedFilterUsingAction(
            std::move(resolved_predicate), predicate_str);
      } break;
      case AST_REVOKE_FROM_CLAUSE: {
        if (existing_action != nullptr) {
          return MakeSqlErrorAt(action)
                 << "Multiple REVOKE FROM actions are not supported";
        }
        auto* revoke_from = action->GetAs<ASTRevokeFromClause>();
        if (revoke_from->is_revoke_from_all()) {
          if (action_map.contains(AST_GRANT_TO_CLAUSE)) {
            return MakeSqlErrorAt(action) << "REVOKE FROM ALL action after "
                                             "GRANT TO action is not supported";
          }
        }
        std::vector<std::unique_ptr<const ResolvedExpr>> revokee_expr_list;
        if (!revoke_from->is_revoke_from_all()) {
          std::vector<std::string> revokee_list;
          ZETASQL_RETURN_IF_ERROR(ResolveGranteeList(revoke_from->revoke_from_list(),
                                             &revokee_list,
                                             &revokee_expr_list));
        }
        alter_action = MakeResolvedRevokeFromAction(
            std::move(revokee_expr_list), revoke_from->is_revoke_from_all());
      } break;
      case AST_RENAME_TO_CLAUSE: {
        if (existing_action != nullptr) {
          return MakeSqlErrorAt(action)
                 << "Multiple RENAME TO actions are not supported";
        }
        auto* rename_to = action->GetAs<ASTRenameToClause>();
        ZETASQL_RET_CHECK(rename_to->new_name());
        ZETASQL_RET_CHECK(rename_to->new_name()->num_names() == 1);
        alter_action = MakeResolvedRenameToAction(
            rename_to->new_name()->ToIdentifierVector());
      } break;
      default:
        return MakeSqlErrorAt(action)
               << "ALTER ROW ACCESS POLICY doesn't support "
               << action->GetNodeKindString() << " action.";
    }
    resolved_alter_actions.push_back(std::move(alter_action));
    existing_action = &resolved_alter_actions.back();
  }

  if (resolved_table_scan == nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveTableAndPredicate(
        ast_statement->path(), nullptr, "ALTER ROW ACCESS POLICY statement",
        &resolved_table_scan, nullptr, nullptr));
  }

  *output = MakeResolvedAlterRowAccessPolicyStmt(
      ast_statement->path()->ToIdentifierVector(),
      std::move(resolved_alter_actions), ast_statement->is_if_exists(),
      ast_statement->name()->GetAsString(), std::move(resolved_table_scan));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterAllRowAccessPoliciesStatement(
      const ASTAlterAllRowAccessPoliciesStatement* ast_statement,
      std::unique_ptr<ResolvedStatement>* output) {
  ZETASQL_RET_CHECK(ast_statement->table_name_path() != nullptr);
  ZETASQL_RET_CHECK(ast_statement->alter_action() != nullptr);

  if (ast_statement->alter_action()->node_kind() != AST_REVOKE_FROM_CLAUSE) {
    return MakeSqlErrorAt(ast_statement->alter_action()) <<
        "ALTER ALL ROW ACCESS POLICIES only supports REVOKE FROM";
  }

  const ASTPathExpression* table_path = ast_statement->table_name_path();
  const IdString alias = GetAliasForExpression(table_path);

  std::unique_ptr<const ResolvedTableScan> resolved_table_scan;
  std::shared_ptr<const NameList> name_list;
  ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsTableScan(
      table_path, alias, /*has_explicit_alias=*/ false,
      /*alias_location=*/ table_path, /*hints=*/ nullptr,
      /*for_system_time=*/ nullptr, empty_name_scope_.get(),
      &resolved_table_scan, &name_list));

  const ASTRevokeFromClause* revoke_from_action =
      ast_statement->alter_action()->GetAs<ASTRevokeFromClause>();
  std::vector<std::string> revokee_list;
  std::vector<std::unique_ptr<const ResolvedExpr>> revokee_expr_list;
  if (!revoke_from_action->is_revoke_from_all()) {
    ZETASQL_RETURN_IF_ERROR(ResolveGranteeList(revoke_from_action->revoke_from_list(),
                                       &revokee_list, &revokee_expr_list));
  }

  std::vector<std::unique_ptr<const ResolvedAlterAction>>
      resolved_alter_actions;
  std::unique_ptr<const ResolvedAlterAction> revoke_action =
      MakeResolvedRevokeFromAction(std::move(revokee_expr_list),
                                   revoke_from_action->is_revoke_from_all());
  resolved_alter_actions.push_back(std::move(revoke_action));

  *output = MakeResolvedAlterAllRowAccessPoliciesStmt(
      table_path->ToIdentifierVector(), std::move(resolved_alter_actions),
      /*is_if_exists=*/false, std::move(resolved_table_scan));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCloneDataStatement(
    const ASTCloneDataStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  if (!language().LanguageFeatureEnabled(FEATURE_CLONE_DATA)) {
    return MakeSqlErrorAt(ast_statement) << "CLONE DATA not supported";
  }
  std::shared_ptr<const NameList> output_name_list;
  std::unique_ptr<const ResolvedTableScan> target_table;
  ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsTableScan(
      ast_statement->target_path(),
      GetAliasForExpression(ast_statement->target_path()),
      /*has_explicit_alias=*/false, ast_statement->target_path(),
      /*hints=*/nullptr,
      /*for_system_time=*/nullptr, empty_name_scope_.get(), &target_table,
      &output_name_list));
  if (target_table->table()->IsValueTable()) {
    return MakeSqlErrorAt(ast_statement->target_path())
           << "Cannot clone into a value table: "
           << target_table->table()->Name();
  }
  ZETASQL_RET_CHECK_NE(ast_statement->data_source_list(), nullptr);
  ZETASQL_RET_CHECK_GT(ast_statement->data_source_list()->data_sources().size(), 0);
  std::vector<std::unique_ptr<const ResolvedScan>> scans;
  scans.reserve(ast_statement->data_source_list()->data_sources().size());
  absl::flat_hash_set<std::string> source_table_names;
  for (const ASTCloneDataSource* data_source :
       ast_statement->data_source_list()->data_sources()) {
    std::unique_ptr<const ResolvedScan> scan;
    ZETASQL_RETURN_IF_ERROR(ResolveDataSourceForCopyOrClone(data_source, &scan));
    ZETASQL_ASSIGN_OR_RETURN(const Table* source_table, ExtractCloneSourceTable(*scan));
    if (source_table->Name() == target_table->table()->Name()) {
        return MakeSqlErrorAt(data_source)
               << "Source table cannot be the same as destination table";
    }
    if (!source_table_names.insert(source_table->Name()).second) {
        return MakeSqlErrorAt(data_source)
               << "Cannot clone from the same source table twice";
    }
    if (scan->column_list().size() != target_table->column_list().size()) {
      return MakeSqlErrorAt(data_source)
             << "Source table "
             << data_source->path_expr()->ToIdentifierPathString()
             << " has different number of columns than destination table "
             << ast_statement->target_path()->ToIdentifierPathString()
             << ": " << scan->column_list().size() << " vs. "
             << target_table->column_list().size();
    }
    for (int i = 0; i < scan->column_list().size(); i++) {
      if (!CompatibleForClone(scan->column_list()[i],
                              target_table->column_list()[i])) {
        return MakeSqlErrorAt(data_source)
               << "Incompatible column "
               << ast_statement->target_path()->ToIdentifierPathString() << "."
               << target_table->column_list()[i].name() << " ("
               << target_table->column_list()[i].type()->ShortTypeName(
                      PRODUCT_EXTERNAL)
               << ") vs. " << data_source->path_expr()->ToIdentifierPathString()
               << "." << scan->column_list()[i].name() << " ("
               << scan->column_list()[i].type()->ShortTypeName(PRODUCT_EXTERNAL)
               << ")";
      }
    }
    scans.push_back(std::move(scan));
  }
  if (scans.size() == 1) {
    *output =
        MakeResolvedCloneDataStmt(std::move(target_table), std::move(scans[0]));
  } else {
    // Clone the column list so that the columns in the inner set operation used
    // for the non-recursive term have unique ids.
    const IdString output_table_name =
        MakeIdString(target_table->table()->Name());
    std::vector<ResolvedColumn> output_columns;
    for (const auto& column : scans[0]->column_list()) {
      output_columns.push_back(ResolvedColumn(
          AllocateColumnId(), output_table_name,
          column.name_id(), column.type()));
    }
    std::vector<std::unique_ptr<ResolvedSetOperationItem>> set_items;
    set_items.reserve(scans.size());
    for (auto& scan : scans) {
      std::vector<ResolvedColumn> column_list = scan->column_list();
      set_items.push_back(
          MakeResolvedSetOperationItem(std::move(scan), column_list));
    }
    *output = MakeResolvedCloneDataStmt(
        std::move(target_table),
        MakeResolvedSetOperationScan(output_columns,
                                     ResolvedSetOperationScan::UNION_ALL,
                                     std::move(set_items)));
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveExportDataStatement(
    const ASTExportDataStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  std::unique_ptr<const ResolvedScan> query_scan;
  std::shared_ptr<const NameList> query_name_list;
  ZETASQL_RETURN_IF_ERROR(
      ResolveQuery(ast_statement->query(), empty_name_scope_.get(),
                   kCreateAsId, /*is_outer_query=*/true,
                   &query_scan, &query_name_list));

  std::unique_ptr<const ResolvedConnection> resolved_connection;
  if (ast_statement->with_connection_clause() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveConnection(ast_statement->with_connection_clause()
                                          ->connection_clause()
                                          ->connection_path(),
                                      &resolved_connection));
  }

  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(ResolveOptionsList(ast_statement->options_list(),
                                     &resolved_options));

  std::vector<std::unique_ptr<const ResolvedOutputColumn>> output_column_list;
  for (int i = 0; i < query_name_list->num_columns(); ++i) {
    const NamedColumn& named_column = query_name_list->column(i);
    output_column_list.push_back(MakeResolvedOutputColumn(
        named_column.name.ToString(), named_column.column));
  }

  *output = MakeResolvedExportDataStmt(
      std::move(resolved_connection), std::move(resolved_options),
      std::move(output_column_list), query_name_list->is_value_table(),
      std::move(query_scan));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveExportModelStatement(
    const ASTExportModelStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  std::vector<std::string> model_name_path =
      ast_statement->model_name_path()->ToIdentifierVector();

  std::unique_ptr<const ResolvedConnection> resolved_connection;
  if (ast_statement->with_connection_clause() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ResolveConnection(ast_statement->with_connection_clause()
                                          ->connection_clause()
                                          ->connection_path(),
                                      &resolved_connection));
  }

  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(ast_statement->options_list(), &resolved_options));

  *output = MakeResolvedExportModelStmt(model_name_path,
                                        std::move(resolved_connection),
                                        std::move(resolved_options));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCallStatement(
    const ASTCallStatement* ast_call,
    std::unique_ptr<ResolvedStatement>* output) {
  // Lookup into the catalog to get the Procedure definition.
  const std::string name_string =
      ast_call->procedure_name()->ToIdentifierPathString();
  const Procedure* procedure_catalog_entry = nullptr;
  const absl::Status find_status = catalog_->FindProcedure(
      ast_call->procedure_name()->ToIdentifierVector(),
      &procedure_catalog_entry, analyzer_options_.find_options());
  if (find_status.code() == absl::StatusCode::kNotFound) {
    return MakeSqlErrorAt(ast_call->procedure_name())
        << "Procedure not found: " << name_string;
  }
  ZETASQL_RETURN_IF_ERROR(find_status);

  const FunctionSignature& signature = procedure_catalog_entry->signature();
  // Resolve the Procedure arguments.
  int num_args = ast_call->arguments().size();
  std::vector<std::unique_ptr<const ResolvedExpr>> resolved_args_exprs(
      num_args);
  std::vector<InputArgumentType> input_arg_types(num_args);
  for (int i = 0; i < num_args; ++i) {
    const ASTTVFArgument* ast_tvf_argument = ast_call->arguments()[i];
    // TODO: support resolving table/model/connection clause.
    if (ast_tvf_argument->table_clause() || ast_tvf_argument->model_clause() ||
        ast_tvf_argument->connection_clause()) {
      return MakeSqlErrorAt(ast_tvf_argument)
             << (ast_tvf_argument->table_clause()
                     ? "Table"
                     : (ast_tvf_argument->connection_clause() ? "Connection"
                                                              : "Model"))
             << " typed argument is not supported";
    }
    std::unique_ptr<const ResolvedExpr> expr;
    ZETASQL_RETURN_IF_ERROR(
        ResolveStandaloneExpr(sql_, ast_tvf_argument->expr(), &expr));
    input_arg_types[i] = GetInputArgumentTypeForExpr(expr.get());
    resolved_args_exprs[i] = std::move(expr);
  }

  FunctionResolver function_resolver(catalog_, type_factory_, this);
  std::unique_ptr<FunctionSignature> result_signature;
  SignatureMatchResult signature_match_result;

  std::vector<const ASTNode*> arg_locations =
      ToLocations(ast_call->arguments());
  ZETASQL_ASSIGN_OR_RETURN(
      const bool is_match,
      function_resolver.SignatureMatches(
          arg_locations, input_arg_types, signature,
          /* allow_argument_coercion=*/true, /*name_scope=*/nullptr,
          &result_signature, &signature_match_result,
          /*arg_overrides=*/nullptr));
  if (!is_match) {
    return MakeSqlErrorAt(ast_call->procedure_name())
           << Function::GetGenericNoMatchingFunctionSignatureErrorMessage(
                  absl::StrCat("procedure ", name_string), input_arg_types,
                  language().product_mode())
           << ". Supported signature: "
           << procedure_catalog_entry->GetSupportedSignatureUserFacingText(
                  language().product_mode());
  }

  // Add casts or coerce literals for procedure arguments.
  ZETASQL_RET_CHECK(result_signature->IsConcrete()) << ast_call->DebugString();
  for (int i = 0; i < num_args; ++i) {
    const Type* target_type = result_signature->ConcreteArgumentType(i);
    ZETASQL_RETURN_IF_ERROR(CoerceExprToType(ast_call->arguments()[i], target_type,
                                     kExplicitCoercion,
                                     &resolved_args_exprs[i]));
  }

  *output = MakeResolvedCallStmt(procedure_catalog_entry, *result_signature,
                                 std::move(resolved_args_exprs));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDefineTableStatement(
    const ASTDefineTableStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  const std::vector<std::string> table_name =
      ast_statement->name()->ToIdentifierVector();
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(ResolveOptionsList(ast_statement->options_list(),
                                     &resolved_options));

  *output =
      MakeResolvedDefineTableStmt(table_name, std::move(resolved_options));
  MaybeRecordParseLocation(ast_statement->name(), (*output).get());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDescribeStatement(
    const ASTDescribeStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  const std::vector<std::string> name =
      ast_statement->name()->ToIdentifierVector();
  const ASTIdentifier* object_type = ast_statement->optional_identifier();
  const ASTPathExpression* from_name = ast_statement->optional_from_name();
  *output = MakeResolvedDescribeStmt(
      object_type == nullptr ? "" : object_type->GetAsString(), name,
      from_name == nullptr ? std::vector<std::string>{}
                           : from_name->ToIdentifierVector());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveShowStatement(
    const ASTShowStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  const ASTIdentifier* identifier = ast_statement->identifier();
  ZETASQL_RET_CHECK(identifier != nullptr);
  const ASTPathExpression* optional_name = ast_statement->optional_name();
  const ASTStringLiteral* optional_like_string =
      ast_statement->optional_like_string();
  std::unique_ptr<const ResolvedLiteral> like_string_literal;
  if (optional_like_string != nullptr) {
    like_string_literal = MakeResolvedLiteralWithoutLocation(
        Value::String(optional_like_string->string_value()));
  }

  *output = MakeResolvedShowStmt(
      identifier->GetAsString(),
      optional_name == nullptr
          ? std::vector<std::string>()
          : optional_name->ToIdentifierVector() /* name_path */,
      std::move(like_string_literal));
  return absl::OkStatus();
}

static absl::Status ParseModeListElements(
    absl::Span<const ASTTransactionMode* const> modes,
    ResolvedBeginStmtEnums::ReadWriteMode* read_write_mode,
    std::vector<std::string>* isolation_level) {
  *read_write_mode = ResolvedBeginStmtEnums::MODE_UNSPECIFIED;
  isolation_level->clear();
  bool seen_read_write_mode = false;
  bool seen_isolation_level = false;
  for (const ASTTransactionMode* mode : modes) {
    switch (mode->node_kind()) {
      case AST_TRANSACTION_READ_WRITE_MODE: {
        if (seen_read_write_mode) {
          return MakeSqlErrorAt(mode)
                 << "Can only specify 'READ ONLY' or 'READ WRITE' once";
        }
        seen_read_write_mode = true;
        switch (mode->GetAsOrDie<ASTTransactionReadWriteMode>()->mode()) {
          case ASTTransactionReadWriteMode::INVALID:
            return MakeSqlErrorAt(mode) << "Invalid mode";
          case ASTTransactionReadWriteMode::READ_ONLY:
            *read_write_mode = ResolvedBeginStmtEnums::MODE_READ_ONLY;
            break;
          case ASTTransactionReadWriteMode::READ_WRITE:
            *read_write_mode = ResolvedBeginStmtEnums::MODE_READ_WRITE;
            break;
        }
        break;
      }
      case AST_TRANSACTION_ISOLATION_LEVEL: {
        if (seen_isolation_level) {
          return MakeSqlErrorAt(mode)
                 << "Can only specify 'ISOLATION LEVEL' a single time";
        }
        seen_isolation_level = true;
        const ASTTransactionIsolationLevel* iso =
            mode->GetAsOrDie<ASTTransactionIsolationLevel>();
        const ASTIdentifier* identifier1 = iso->identifier1();
        const ASTIdentifier* identifier2 = iso->identifier2();
        if (identifier1 != nullptr) {
          isolation_level->push_back(identifier1->GetAsString());
          if (identifier2 != nullptr) {
            isolation_level->push_back(identifier2->GetAsString());
          }
        }
        break;
      }
      default:
        return MakeSqlErrorAt(mode) << "Unknown transaction_mode";
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveBeginStatement(
    const ASTBeginStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  absl::Span<const ASTTransactionMode* const> modes;
  if (ast_statement->mode_list() != nullptr) {
    modes = ast_statement->mode_list()->elements();
  }

  ResolvedBeginStmtEnums::ReadWriteMode read_write_mode;
  std::vector<std::string> isolation_level;
  ZETASQL_RETURN_IF_ERROR(
      ParseModeListElements(modes, &read_write_mode, &isolation_level));
  *output = MakeResolvedBeginStmt(read_write_mode, isolation_level);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveSetTransactionStatement(
    const ASTSetTransactionStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  ResolvedBeginStmtEnums::ReadWriteMode read_write_mode;
  std::vector<std::string> isolation_level;
  ZETASQL_RETURN_IF_ERROR(ParseModeListElements(ast_statement->mode_list()->elements(),
                                        &read_write_mode, &isolation_level));
  *output = MakeResolvedSetTransactionStmt(read_write_mode, isolation_level);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCommitStatement(
    const ASTCommitStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  *output = MakeResolvedCommitStmt();
  return absl::OkStatus();
}

absl::Status Resolver::ResolveRollbackStatement(
    const ASTRollbackStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  *output = MakeResolvedRollbackStmt();
  return absl::OkStatus();
}

absl::Status Resolver::ResolveStartBatchStatement(
    const ASTStartBatchStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  const ASTIdentifier* batch_type = ast_statement->batch_type();
  *output = MakeResolvedStartBatchStmt(
      batch_type == nullptr ? "" : batch_type->GetAsString());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveRunBatchStatement(
    const ASTRunBatchStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  *output = MakeResolvedRunBatchStmt();
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAbortBatchStatement(
    const ASTAbortBatchStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  *output = MakeResolvedAbortBatchStmt();
  return absl::OkStatus();
}

static ResolvedDropStmtEnums::DropMode ConvertDropMode(
    ASTDropStatement::DropMode drop_mode) {
  switch (drop_mode) {
    case ASTDropStatement::DropMode::DROP_MODE_UNSPECIFIED:
      return ResolvedDropStmtEnums::DROP_MODE_UNSPECIFIED;
    case ASTDropStatement::DropMode::RESTRICT:
      return ResolvedDropStmtEnums::RESTRICT;
    case ASTDropStatement::DropMode::CASCADE:
      return ResolvedDropStmtEnums::CASCADE;
  }
}

absl::Status Resolver::ResolveDropStatement(
    const ASTDropStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  const std::vector<std::string> name =
      ast_statement->name()->ToIdentifierVector();
  *output = MakeResolvedDropStmt(
      std::string(SchemaObjectKindToName(ast_statement->schema_object_kind())),
      ast_statement->is_if_exists(), name,
      ConvertDropMode(ast_statement->drop_mode()));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDropEntityStatement(
    const ASTDropEntityStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  const std::vector<std::string> name =
      ast_statement->name()->ToIdentifierVector();
  *output = MakeResolvedDropStmt(ast_statement->entity_type()->GetAsString(),
                                 ast_statement->is_if_exists(), name,
                                 ResolvedDropStmtEnums::DROP_MODE_UNSPECIFIED);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDropFunctionStatement(
    const ASTDropFunctionStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  std::unique_ptr<const ResolvedArgumentList> arguments;
  std::unique_ptr<ResolvedFunctionSignatureHolder> signature;
  bool contains_table_args = false;
  if (ast_statement->parameters() != nullptr) {
    FunctionArgumentTypeList signature_arguments;
    std::vector<std::unique_ptr<const ResolvedArgumentDef>> resolved_args;
    for (const ASTFunctionParameter* function_param :
        ast_statement->parameters()->parameter_entries()) {
      ZETASQL_RET_CHECK(function_param != nullptr);
      const bool is_any_table_arg =
          function_param->IsTemplated() &&
          (function_param->templated_parameter_type()->kind() ==
           ASTTemplatedParameterType::ANY_TABLE);
      if (function_param->alias() != nullptr ||
          (function_param->IsTemplated() && !is_any_table_arg)) {
        // TODO: When we support type aliases, ensure that an error
        // is provided if two types are given the same alias.
        return UnsupportedArgumentError(*function_param, "DROP FUNCTION");
      }
      if (function_param->IsTableParameter()) {
        contains_table_args = true;
        if (is_any_table_arg) {
          signature_arguments.push_back(FunctionArgumentType::AnyRelation());
          continue;
        }
        ZETASQL_RET_CHECK(function_param->tvf_schema() != nullptr);
        TVFRelation resolved_tvf_relation({});
        ZETASQL_RETURN_IF_ERROR(
            ResolveTVFSchema(function_param->tvf_schema(),
                             ResolveTVFSchemaCheckPropertiesType::SKIP_CHECKS,
                             &resolved_tvf_relation));
        signature_arguments.push_back(
            FunctionArgumentType::RelationWithSchema(
                resolved_tvf_relation,
                /*extra_relation_input_columns_allowed=*/false));
      } else {
        const Type* resolved_type;
        ZETASQL_RETURN_IF_ERROR(ResolveType(function_param->type(),
                                    "function arguments", &resolved_type,
                                    /*resolved_type_params=*/nullptr));
        // Argument names are ignored for DROP FUNCTION statements, and are
        // always provided as an empty string.  Argument kinds are also ignored.
        auto argument_def = MakeResolvedArgumentDef(
            "", resolved_type, ResolvedArgumentDef::SCALAR);
        resolved_args.push_back(std::move(argument_def));
        signature_arguments.emplace_back(resolved_type);
      }
    }
    // If this DROP FUNCTION statement contained table arguments for
    // table-valued functions, clear 'arguments' as we match against
    // table-valued functions by function name only (since they do not allow
    // overloads).
    if (contains_table_args) {
      resolved_args.clear();
    }
    arguments = MakeResolvedArgumentList(std::move(resolved_args));
    signature = MakeResolvedFunctionSignatureHolder(
        FunctionSignature{{ARG_TYPE_VOID} /* return_type */,
                          signature_arguments,
                          /*context_ptr=*/nullptr});
  }
  *output =
      MakeResolvedDropFunctionStmt(ast_statement->is_if_exists(),
                                   ast_statement->name()->ToIdentifierVector(),
                                   std::move(arguments), std::move(signature));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDropTableFunctionStatement(
    const ASTDropTableFunctionStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  if (!language().LanguageFeatureEnabled(FEATURE_CREATE_TABLE_FUNCTION)) {
    return MakeSqlErrorAt(ast_statement)
        << "Dropping table-valued functions is not supported";
  }
  *output =
      MakeResolvedDropTableFunctionStmt(
          ast_statement->is_if_exists(),
          ast_statement->name()->ToIdentifierVector());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDropPrivilegeRestrictionStatement(
    const ASTDropPrivilegeRestrictionStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  ZETASQL_RET_CHECK(ast_statement->name_path() != nullptr);

  // The statement type is used for error messaging.
  constexpr absl::string_view statement_type = "DROP PRIVILEGE RESTRICTION";

  for (const ASTPrivilege* privilege :
       ast_statement->privileges()->privileges()) {
    if (privilege->paths() == nullptr ||
        privilege->paths()->path_expression_list().empty()) {
      return MakeSqlErrorAt(privilege) << "Expected privilege to contain paths";
    }
  }

  const ASTPathExpression* table_path = ast_statement->name_path();
  const IdString alias = GetAliasForExpression(table_path);
  std::unique_ptr<const ResolvedTableScan> resolved_table_scan;
  std::shared_ptr<const NameList> name_list;
  ZETASQL_RETURN_IF_ERROR(ResolvePathExpressionAsTableScan(
      table_path, alias, /*has_explicit_alias=*/false,
      /*alias_location=*/table_path, /*hints=*/nullptr,
      /*for_system_time=*/nullptr, empty_name_scope_.get(),
      &resolved_table_scan, &name_list));
  const std::shared_ptr<const NameScope> name_scope =
      std::make_shared<NameScope>(/*previous_scope=*/nullptr, name_list);

  std::vector<std::unique_ptr<const ResolvedPrivilege>> column_privilege_list;
  ZETASQL_RETURN_IF_ERROR(ResolvePrivileges(ast_statement->privileges(),
                                    name_scope.get(),
                                    /*enable_nested_field_privileges=*/true,
                                    statement_type, &column_privilege_list));

  *output = MakeResolvedDropPrivilegeRestrictionStmt(
      ast_statement->object_type()->GetAsString(),
      ast_statement->is_if_exists(),
      ast_statement->name_path()->ToIdentifierVector(),
      std::move(column_privilege_list));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDropRowAccessPolicyStatement(
    const ASTDropRowAccessPolicyStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  *output = MakeResolvedDropRowAccessPolicyStmt(
      /*is_drop_all=*/false, ast_statement->is_if_exists(),
      ast_statement->name()->GetAsString(),
      ast_statement->table_name()->ToIdentifierVector());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDropAllRowAccessPoliciesStatement(
    const ASTDropAllRowAccessPoliciesStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  if (!language().LanguageFeatureEnabled(
          FEATURE_ALLOW_LEGACY_ROW_ACCESS_POLICY_SYNTAX) &&
      !ast_statement->has_access_keyword()) {
    return MakeSqlErrorAt(ast_statement)
           << "Expected keyword ACCESS between ROW and POLICY";
  }
  *output = MakeResolvedDropRowAccessPolicyStmt(
      /*is_drop_all=*/true,
      /*is_if_exists=*/false, /*name=*/"",
      ast_statement->table_name()->ToIdentifierVector());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDropMaterializedViewStatement(
    const ASTDropMaterializedViewStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  *output = MakeResolvedDropMaterializedViewStmt(
      ast_statement->is_if_exists(),
      ast_statement->name()->ToIdentifierVector());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDropSnapshotTableStatement(
    const ASTDropSnapshotTableStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  *output = MakeResolvedDropSnapshotTableStmt(
      ast_statement->is_if_exists(),
      ast_statement->name()->ToIdentifierVector());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDropSearchIndexStatement(
    const ASTDropSearchIndexStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  // The parser would fail if the index name was missing.
  ZETASQL_RET_CHECK(ast_statement->name() != nullptr);

  if (ast_statement->name()->names().size() != 1) {
    return MakeSqlErrorAt(ast_statement->name())
           << "The DROP SEARCH INDEX statement requires an index name, not a "
              "path";
  }
  std::vector<std::string> table_name;
  if (ast_statement->table_name() != nullptr) {
    table_name = ast_statement->table_name()->ToIdentifierVector();
  }
  *output = MakeResolvedDropSearchIndexStmt(
      ast_statement->is_if_exists(),
      ast_statement->name()->first_name()->GetAsString(), table_name);
  return absl::OkStatus();
}
absl::Status Resolver::ResolveAlterViewStatement(
    const ASTAlterViewStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  bool has_only_set_options_action = true;
  std::vector<std::unique_ptr<const ResolvedAlterAction>> alter_actions;
  ZETASQL_RETURN_IF_ERROR(ResolveAlterActions(ast_statement, "VIEW", output,
                                      &has_only_set_options_action,
                                      &alter_actions));

  // path() should never be null here because the drop_statement grammar rule
  // requires a nonempty path.
  ZETASQL_RET_CHECK(ast_statement->path() != nullptr);
  *output = MakeResolvedAlterViewStmt(
      ast_statement->path()->ToIdentifierVector(), std::move(alter_actions),
      ast_statement->is_if_exists());

  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterMaterializedViewStatement(
    const ASTAlterMaterializedViewStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  bool has_only_set_options_action = true;
  std::vector<std::unique_ptr<const ResolvedAlterAction>> alter_actions;
  ZETASQL_RETURN_IF_ERROR(ResolveAlterActions(ast_statement, "MATERIALIZED VIEW",
                                      output, &has_only_set_options_action,
                                      &alter_actions));

  // path() should never be null here because the ALTER MATERIALIZED VIEW
  // grammar rule requires a nonempty path.
  ZETASQL_RET_CHECK(ast_statement->path() != nullptr);
  *output = MakeResolvedAlterMaterializedViewStmt(
      ast_statement->path()->ToIdentifierVector(), std::move(alter_actions),
      ast_statement->is_if_exists());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveRenameStatement(
    const ASTRenameStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  const std::vector<std::string> old_name =
      ast_statement->old_name()->ToIdentifierVector();
  const std::vector<std::string> new_name =
      ast_statement->new_name()->ToIdentifierVector();
  const ASTIdentifier* object_type = ast_statement->identifier();
  ZETASQL_RET_CHECK(object_type != nullptr);
  *output =
      MakeResolvedRenameStmt(object_type->GetAsString(), old_name, new_name);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveImportStatement(
    const ASTImportStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  std::vector<std::string> name_path;
  std::vector<std::string> alias_path;
  std::vector<std::string> into_alias_path;
  std::string file_path;

  ParseLocationRange import_path_location_range;
  ResolvedImportStmt::ImportKind kind;
  switch (ast_statement->import_kind()) {
    case ASTImportStatement::MODULE:
      if (!language().LanguageFeatureEnabled(FEATURE_EXPERIMENTAL_MODULES)) {
        return MakeSqlErrorAt(ast_statement)
            << "The MODULEs feature is not supported, including IMPORT "
               "MODULE";
      }

      if (ast_statement->name() != nullptr) {
        name_path = ast_statement->name()->ToIdentifierVector();
      } else {
        ZETASQL_RET_CHECK(ast_statement->string_value() != nullptr);
        return MakeSqlErrorAt(ast_statement->string_value())
            << "The IMPORT MODULE statement requires a path expression";
      }

      // Currently, the parser only supports single part aliases.  But the
      // spec allows multi-part aliases, so multi-part aliases are supported
      // in the ResolvedAST.
      if (ast_statement->alias() != nullptr) {
        alias_path.push_back(ast_statement->alias()->GetAsString());
      }

      if (ast_statement->into_alias() != nullptr) {
        return MakeSqlErrorAt(ast_statement->into_alias())
            << "The IMPORT MODULE statement does not support INTO alias; "
            << "use AS alias instead";
      }

      // The IMPORT MODULE statement does not currently allow an 'alias_path'
      // with more than one name.  This is enforced when populating
      // 'alias_path' above.
      ZETASQL_RET_CHECK_LE(alias_path.size(), 1);
      // If 'alias_path' is empty, then an implicit alias is used that
      // matches the last name in 'name_path'.
      if (alias_path.empty()) {
        alias_path.push_back(name_path.back());
      }
      kind = ResolvedImportStmt::MODULE;
      import_path_location_range =
          ast_statement->name()->GetParseLocationRange();
      break;
    case ASTImportStatement::PROTO:
      if (ast_statement->string_value() != nullptr) {
        file_path = ast_statement->string_value()->string_value();
        if (file_path.empty()) {
          return MakeSqlErrorAt(ast_statement->string_value())
                 << "The IMPORT PROTO statement requires a non-empty string "
                    "literal";
        }
      }
      // Check invalid parameters because they are not checked during parsing.
      if (ast_statement->name() != nullptr) {
        return MakeSqlErrorAt(ast_statement->name())
               << "The IMPORT PROTO statement requires a string literal";
      }
      if (ast_statement->alias() != nullptr) {
        return MakeSqlErrorAt(ast_statement->alias())
               << "The IMPORT PROTO statement does not support AS alias; "
               << "use INTO alias instead";
      }
      if (ast_statement->into_alias() != nullptr) {
        into_alias_path.push_back(ast_statement->into_alias()->GetAsString());
      }
      kind = ResolvedImportStmt::PROTO;
      import_path_location_range =
          ast_statement->string_value()->GetParseLocationRange();
      break;
  }

  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(ast_statement->options_list(), &resolved_options));

  // ResolvedStatement populates either name_path or file_path but not both.
  ZETASQL_RET_CHECK(name_path.empty() || file_path.empty())
      << "ResolveImportStatement populates either name_path or file_path but "
         "not both";
  *output =
      MakeResolvedImportStmt(kind, name_path, file_path, alias_path,
                             into_alias_path, std::move(resolved_options));
  if (analyzer_options_.parse_location_record_type() !=
      PARSE_LOCATION_RECORD_NONE) {
    (*output)->SetParseLocationRange(import_path_location_range);
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveModuleStatement(
    const ASTModuleStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  if (!language().LanguageFeatureEnabled(FEATURE_EXPERIMENTAL_MODULES)) {
    return MakeSqlErrorAt(ast_statement)
           << "The MODULEs feature is not supported";
  }
  const std::vector<std::string> name_path =
      ast_statement->name()->ToIdentifierVector();

  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(ast_statement->options_list(), &resolved_options));

  *output = MakeResolvedModuleStmt(name_path, std::move(resolved_options));
  MaybeRecordParseLocation(ast_statement->name(), output->get());

  return absl::OkStatus();
}

absl::Status Resolver::ResolvePrivileges(
    const ASTPrivileges* ast_privileges, const NameScope* name_scope,
    bool enable_nested_field_privileges, absl::string_view statement_type,
    std::vector<std::unique_ptr<const ResolvedPrivilege>>* privilege_list) {
  ZETASQL_RET_CHECK(privilege_list->empty());
  if (!ast_privileges->is_all_privileges()) {
    for (const ASTPrivilege* privilege : ast_privileges->privileges()) {
      std::vector<std::unique_ptr<ResolvedObjectUnit>> unit_list;
      if (privilege->paths() != nullptr) {
        for (const ASTPathExpression* path :
             privilege->paths()->path_expression_list()) {
          if (name_scope != nullptr) {
            // The path is resolved to validate that it exists.
            std::unique_ptr<const ResolvedExpr> resolved_path_expression;
            ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(path, name_scope,
                                              /*clause_name=*/"UNIT LIST",
                                              &resolved_path_expression));
          }

          if (!enable_nested_field_privileges && path->num_names() > 1) {
            return MakeSqlErrorAt(path)
                   << "Privileges on nested fields are not supported in "
                   << statement_type;
          }

          unit_list.push_back(
              MakeResolvedObjectUnit(path->ToIdentifierVector()));
        }
      }

      privilege_list->push_back(MakeResolvedPrivilege(
          privilege->privilege_action()->GetAsString(), std::move(unit_list)));
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveGrantStatement(
    const ASTGrantStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  // The statement type is used for error messaging.
  constexpr absl::string_view statement_type = "GRANT";
  std::vector<std::unique_ptr<const ResolvedPrivilege>> privilege_list;
  ZETASQL_RETURN_IF_ERROR(ResolvePrivileges(ast_statement->privileges(),
                                    /*name_scope=*/nullptr,
                                    /*enable_nested_field_privileges=*/false,
                                    statement_type, &privilege_list));

  std::vector<std::string> grantee_list;
  std::vector<std::unique_ptr<const ResolvedExpr>> grantee_expr_list;
  ZETASQL_RETURN_IF_ERROR(ResolveGranteeList(ast_statement->grantee_list(),
                                     &grantee_list, &grantee_expr_list));

  const ASTIdentifier* object_type = ast_statement->target_type();
  *output = MakeResolvedGrantStmt(
      std::move(privilege_list),
      object_type == nullptr ? "" : object_type->GetAsString(),
      ast_statement->target_path()->ToIdentifierVector(),
      grantee_list, std::move(grantee_expr_list));

  return absl::OkStatus();
}

absl::Status Resolver::ResolveRevokeStatement(
    const ASTRevokeStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  // The statement type is used for error messaging.
  constexpr absl::string_view statement_type = "REVOKE";
  std::vector<std::unique_ptr<const ResolvedPrivilege>> privilege_list;
  ZETASQL_RETURN_IF_ERROR(ResolvePrivileges(ast_statement->privileges(),
                                    /*name_scope=*/nullptr,
                                    /*enable_nested_field_privileges=*/false,
                                    statement_type, &privilege_list));

  std::vector<std::string> grantee_list;
  std::vector<std::unique_ptr<const ResolvedExpr>> grantee_expr_list;
  ZETASQL_RETURN_IF_ERROR(ResolveGranteeList(ast_statement->grantee_list(),
                                     &grantee_list, &grantee_expr_list));

  const ASTIdentifier* object_type = ast_statement->target_type();
  *output = MakeResolvedRevokeStmt(
      std::move(privilege_list),
      object_type == nullptr ? "" : object_type->GetAsString(),
      ast_statement->target_path()->ToIdentifierVector(),
      grantee_list, std::move(grantee_expr_list));

  return absl::OkStatus();
}

absl::Status Resolver::ResolveAnalyzeStatement(
    const ASTAnalyzeStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(ast_statement->options_list(), &resolved_options));
  std::vector<std::unique_ptr<const ResolvedTableAndColumnInfo>>
      resolved_table_and_column_info_list;
  ZETASQL_RETURN_IF_ERROR(
      ResolveTableAndColumnInfoList(ast_statement->table_and_column_info_list(),
                                    &resolved_table_and_column_info_list));
  *output =
      MakeResolvedAnalyzeStmt(std::move(resolved_options),
                              std::move(resolved_table_and_column_info_list));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAssertStatement(
    const ASTAssertStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  std::unique_ptr<const ResolvedExpr> resolved_expr;
  ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(ast_statement->expr(),
                                    empty_name_scope_.get(),
                                    "Expression clause", &resolved_expr));

  ZETASQL_RETURN_IF_ERROR(CoerceExprToBool(
      ast_statement->expr(), "ASSERT expression", &resolved_expr));

  const ASTStringLiteral* description = ast_statement->description();
  *output = MakeResolvedAssertStmt(
      std::move(resolved_expr),
      description == nullptr ? "" : description->string_value());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveExecuteImmediateStatement(
    const ASTExecuteImmediateStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  using IdentifierSet =
      absl::flat_hash_set<IdString, IdStringCaseHash, IdStringCaseEqualFunc>;
  QueryResolutionInfo query_info(this);
  ExprResolutionInfo expr_info(
      /*name_scope_in=*/empty_name_scope_.get(),
      /*aggregate_name_scope_in=*/empty_name_scope_.get(),
      /*allows_aggregation_in=*/false,
      /*allows_analytic_in=*/false,
      /*use_post_grouping_columns_in=*/false, "SQL EXECUTE IMMEDIATE",
      &query_info);
  std::unique_ptr<const ResolvedExpr> sql;
  ZETASQL_RETURN_IF_ERROR(ResolveExpr(ast_statement->sql(), &expr_info, &sql));
  ZETASQL_RETURN_IF_ERROR(CoerceExprToType(
      ast_statement->sql(), type_factory_->get_string(), kImplicitCoercion,
      "Dynamic SQL should return type $0, but returns $1", &sql));

  std::vector<std::string> into_identifiers;
  if (ast_statement->into_clause()) {
    const ASTExecuteIntoClause* into_clause = ast_statement->into_clause();
    // It's an error if the same variable is referenced multiple times in an
    // INTO clause. This behavior follows the precedent set by procedures.
    IdentifierSet seen_identifiers;
    for (const ASTIdentifier* identifier :
         into_clause->identifiers()->identifier_list()) {
      if (!seen_identifiers.insert(identifier->GetAsIdString()).second) {
        return MakeSqlErrorAt(identifier)
               << "The same parameter cannot be assigned "
                  "multiple times in an INTO clause: "
               << identifier->GetAsString();
      }
      into_identifiers.push_back(identifier->GetAsString());
    }
  }

  std::vector<std::unique_ptr<const ResolvedExecuteImmediateArgument>>
      using_arguments;
  if (ast_statement->using_clause()) {
    const ASTExecuteUsingClause* using_clause = ast_statement->using_clause();
    absl::flat_hash_set<IdString, IdStringCaseHash, IdStringCaseEqualFunc>
        seen_identifiers;
    // After parsing, we're guaranteed at least one argument.
    bool expecting_names = using_clause->arguments().at(0)->alias() != nullptr;
    for (const ASTExecuteUsingArgument* const argument :
         using_clause->arguments()) {
      if (const bool has_name = argument->alias() != nullptr;
          expecting_names != has_name) {
        return MakeSqlErrorAt(argument)
               << "Cannot mix named and positional parameters";
      }
      if (expecting_names &&
          !seen_identifiers.insert(argument->alias()->GetAsIdString()).second) {
        return MakeSqlErrorAt(argument->alias())
               << "The same parameter cannot be assigned "
                  "multiple times in a USING clause: "
               << argument->alias()->GetAsString();
      }
      std::unique_ptr<const ResolvedExecuteImmediateArgument> resolved_argument;
      ZETASQL_RETURN_IF_ERROR(ResolveExecuteImmediateArgument(argument, &expr_info,
                                                      &resolved_argument));
      using_arguments.push_back(std::move(resolved_argument));
    }
  }

  *output = MakeResolvedExecuteImmediateStmt(std::move(sql), into_identifiers,
                                             std::move(using_arguments));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveCreateEntityStatement(
    const ASTCreateEntityStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  ResolvedCreateStatement::CreateScope create_scope;
  ResolvedCreateStatement::CreateMode create_mode;
  ZETASQL_RETURN_IF_ERROR(ResolveCreateStatementOptions(
      ast_statement,
      /*statement_type=*/
      absl::StrCat("CREATE ", ast_statement->type()->GetAsString()),
      &create_scope, &create_mode));

  std::vector<std::unique_ptr<const ResolvedOption>> options;
  ZETASQL_RETURN_IF_ERROR(ResolveOptionsList(ast_statement->options_list(), &options));
  std::string entity_body_json;
  if (ast_statement->json_body() != nullptr) {
    // TODO: Use ResolveExpr() once JSON goes GA.
    ZETASQL_ASSIGN_OR_RETURN(auto json_literal,
                     ResolveJsonLiteral(ast_statement->json_body()));
    entity_body_json = json_literal->value().json_string();
  }
  std::string entity_body_text;
  if (ast_statement->text_body() != nullptr) {
    entity_body_text = ast_statement->text_body()->string_value();
  }
  if (!entity_body_text.empty() && !entity_body_json.empty()) {
    return MakeSqlErrorAt(ast_statement)
           << "CREATE ENTITY should have at most one JSON or TEXT body literal";
  }

  auto resolved_stmt = MakeResolvedCreateEntityStmt(
      ast_statement->name()->ToIdentifierVector(), create_scope, create_mode,
      ast_statement->type()->GetAsString(), std::move(entity_body_json),
      std::move(entity_body_text), std::move(options));
  *output = std::move(resolved_stmt);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveSystemVariableAssignment(
    const ASTSystemVariableAssignment* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  std::unique_ptr<const ResolvedExpr> target;
  ExprResolutionInfo expr_resolution_info(empty_name_scope_.get(),
                                          "SET statement");
  ZETASQL_RETURN_IF_ERROR(ResolveSystemVariableExpression(
      ast_statement->system_variable(), &expr_resolution_info, &target));

  std::unique_ptr<const ResolvedExpr> resolved_expr;
  ZETASQL_RETURN_IF_ERROR(ResolveScalarExpr(ast_statement->expression(),
                                    empty_name_scope_.get(), "SET statement",
                                    &resolved_expr));

  ZETASQL_RETURN_IF_ERROR(CoerceExprToType(ast_statement->expression(), target->type(),
                                   kImplicitAssignment, &resolved_expr));

  std::unique_ptr<ResolvedAssignmentStmt> result =
      MakeResolvedAssignmentStmt(std::move(target), std::move(resolved_expr));
  *output = std::unique_ptr<ResolvedStatement>(std::move(result));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAuxLoadDataStatement(
    const ASTAuxLoadDataStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  ResolvedAuxLoadDataStmtEnums::InsertionMode insertion_mode =
      ast_statement->insertion_mode() == ASTAuxLoadDataStatement::OVERWRITE
          ? ResolvedAuxLoadDataStmtEnums::OVERWRITE
          : ResolvedAuxLoadDataStmtEnums::APPEND;
  ResolveCreateTableStatementBaseProperties statement_base_properties;
  const Table* table;
  ZETASQL_RETURN_IF_ERROR(FindTable(ast_statement->name(), &table));
  const IdString table_name_id_string =
      MakeIdString(ast_statement->name()->ToIdentifierPathString());
  for (int i = 0; i < table->NumColumns(); i++) {
    const Column* column = table->GetColumn(i);
    IdString column_name = MakeIdString(column->Name());
    statement_base_properties.default_visible_columns.emplace_back(
        AllocateColumnId(), table_name_id_string, column_name,
        AnnotatedType(column->GetType(), column->GetTypeAnnotationMap()));
  }
  ZETASQL_RETURN_IF_ERROR(ResolveCreateTableStmtBaseProperties(
      ast_statement, "LOAD", /*like_table_name=*/nullptr, /*query=*/nullptr,
      ast_statement->collate(), ast_statement->partition_by(),
      ast_statement->cluster_by(),
      ast_statement->with_partition_columns_clause(),
      ast_statement->with_connection_clause(),
      {.table_element_list_enabled = true}, &statement_base_properties));

  std::vector<std::unique_ptr<const ResolvedOption>> from_files_options_list;
  ZETASQL_RETURN_IF_ERROR(ResolveOptionsList(
      ast_statement->from_files()->options_list(), &from_files_options_list));
  NameList columns;
  ZETASQL_RETURN_IF_ERROR(statement_base_properties.GetVisibleColumnNames(&columns));
  ZETASQL_RETURN_IF_ERROR(statement_base_properties.WithPartitionColumnNames(&columns));

  std::vector<std::unique_ptr<const ResolvedOutputColumn>> output_column_list;
  for (const ResolvedColumn& c : columns.GetResolvedColumns()) {
    RecordColumnAccess(c);
    output_column_list.push_back(MakeResolvedOutputColumn(c.name(), c));
  }
  *output = MakeResolvedAuxLoadDataStmt(
      insertion_mode, statement_base_properties.table_name,
      std::move(output_column_list),
      std::move(statement_base_properties.column_definition_list),
      std::move(statement_base_properties.pseudo_column_list),
      std::move(statement_base_properties.primary_key),
      std::move(statement_base_properties.foreign_key_list),
      std::move(statement_base_properties.check_constraint_list),
      std::move(statement_base_properties.partition_by_list),
      std::move(statement_base_properties.cluster_by_list),
      std::move(statement_base_properties.resolved_options),
      std::move(statement_base_properties.with_partition_columns),
      std::move(statement_base_properties.connection),
      std::move(from_files_options_list));
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<NameScope>>
Resolver::CreateNameScopeWithAccessErrorForDefaultExpr(
    IdString table_name_id_string, std::vector<IdString>& all_column_names) {
  IdStringHashMapCase<NameTarget> error_name_targets;
  for (const IdString column_name : all_column_names) {
    // Create a placeholder column. As these columns generates error upon
    // access, we are using placeholder values for column_id and type.
    ResolvedColumn defined_column(/*column_id=*/1, table_name_id_string,
                                  column_name, types::Int64Type());
    NameTarget name_target(defined_column, /*is_explicit=*/true);
    name_target.SetAccessError(
        NameTarget::EXPLICIT_COLUMN,
        "Default value expressions cannot reference columns");
    ZETASQL_RET_CHECK(
        zetasql_base::InsertIfNotPresent(&error_name_targets, column_name, name_target));
  }
  std::unique_ptr<NameScope> target_scope;
  std::shared_ptr<NameScope> empty_scope = std::make_shared<NameScope>();
  ZETASQL_RETURN_IF_ERROR(empty_scope->CopyNameScopeWithOverridingNameTargets(
      error_name_targets, &target_scope));
  return target_scope;
}

}  // namespace zetasql
