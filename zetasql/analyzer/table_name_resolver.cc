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

#include "zetasql/analyzer/table_name_resolver.h"

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/base/case.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

// TODO This implementation probably doesn't cover all edge cases for
// table name extraction.  It should be tested more and tuned for the final
// name scoping rules once those are implemented in the full resolver.

namespace zetasql {
namespace table_name_resolver {
namespace {

// Each instance should be used only once.
class TableNameResolver {
 public:
  // <*analyzer_options> must outlive the created TableNameResolver. It must
  // have all arenas initialized.
  // If 'type_factory' and 'catalog' are not null, their contents must
  // outlive the created TableNameResolver as well.
  //
  TableNameResolver(
      absl::string_view sql, const AnalyzerOptions* analyzer_options,
      TypeFactory* type_factory, Catalog* catalog, TableNamesSet* table_names,
      TableResolutionTimeInfoMap* table_resolution_time_info_map)
    : sql_(sql), analyzer_options_(analyzer_options),
      for_system_time_as_of_feature_enabled_(
          analyzer_options->language().LanguageFeatureEnabled(
              FEATURE_V_1_1_FOR_SYSTEM_TIME_AS_OF)),
      type_factory_(type_factory), catalog_(catalog),
      table_names_(table_names),
      table_resolution_time_info_map_(table_resolution_time_info_map) {
    DCHECK(analyzer_options_->AreAllArenasInitialized());
  }

  TableNameResolver(const TableNameResolver&) = delete;
  TableNameResolver& operator=(const TableNameResolver&) = delete;

  zetasql_base::Status FindTableNamesAndTemporalReferences(
      const ASTStatement& statement);

  zetasql_base::Status FindTableNames(const ASTScript& script);

 private:
  typedef std::set<std::string> AliasSet;  // Always lowercase.

  zetasql_base::Status FindInStatement(const ASTStatement* statement,
                               const AliasSet& visible_aliases);

  zetasql_base::Status FindInStatementList(const ASTStatementListBase* statement_list);

  // Consumes either an ASTScript, ASTStatementList, or ASTScriptStatement.
  zetasql_base::Status FindInScriptNode(const ASTNode* node);

  zetasql_base::Status FindInQueryStatement(const ASTQueryStatement* statement,
                                    const AliasSet& visible_aliases);

  zetasql_base::Status FindInCreateViewStatement(
      const ASTCreateViewStatement* statement,
      const AliasSet& visible_aliases);

  zetasql_base::Status FindInCreateMaterializedViewStatement(
      const ASTCreateMaterializedViewStatement* statement,
      const AliasSet& visible_aliases);

  zetasql_base::Status FindInExportDataStatement(
      const ASTExportDataStatement* statement,
      const AliasSet& visible_aliases);

  zetasql_base::Status FindInDeleteStatement(const ASTDeleteStatement* statement,
                                     const AliasSet& orig_visible_aliases);

  zetasql_base::Status FindInTruncateStatement(const ASTTruncateStatement* statement,
                                       const AliasSet& orig_visible_aliases);

  zetasql_base::Status FindInInsertStatement(const ASTInsertStatement* statement,
                                     const AliasSet& orig_visible_aliases);

  zetasql_base::Status FindInUpdateStatement(const ASTUpdateStatement* statement,
                                     const AliasSet& orig_visible_aliases);

  zetasql_base::Status FindInMergeStatement(const ASTMergeStatement* statement,
                                    const AliasSet& visible_aliases);

  zetasql_base::Status FindInQuery(const ASTQuery* query,
                           const AliasSet& orig_visible_aliases);

  zetasql_base::Status FindInQueryExpression(const ASTQueryExpression* query_expr,
                                     const ASTOrderBy* order_by,
                                     const AliasSet& visible_aliases);

  zetasql_base::Status FindInSelect(const ASTSelect* select, const ASTOrderBy* order_by,
                            const AliasSet& orig_visible_aliases);

  zetasql_base::Status FindInSetOperation(const ASTSetOperation* set_operation,
                                  const AliasSet& visible_aliases);

  // When resolving the FROM clause, <external_visible_aliases> is the set
  // of names visible in the query without any names from the FROM clause.
  // <local_visible_aliases> includes all names visible in
  // <external_visible_alaises> plus names earlier in the same FROM clause
  // that are visible.  See corresponding methods in resolver.cc.
  zetasql_base::Status FindInTableExpression(const ASTTableExpression* table_expr,
                                     const AliasSet& external_visible_aliases,
                                     AliasSet* local_visible_aliases);

  zetasql_base::Status FindInJoin(const ASTJoin* join,
                          const AliasSet& external_visible_aliases,
                          AliasSet* local_visible_aliases);

  zetasql_base::Status FindInParenthesizedJoin(
      const ASTParenthesizedJoin* parenthesized_join,
      const AliasSet& external_visible_aliases,
      AliasSet* local_visible_aliases);

  zetasql_base::Status FindInTVF(
      const ASTTVF* tvf,
      const AliasSet& external_visible_aliases,
      AliasSet* local_visible_aliases);

  zetasql_base::Status FindInTableSubquery(
      const ASTTableSubquery* table_subquery,
      const AliasSet& external_visible_aliases,
      AliasSet* local_visible_aliases);

  zetasql_base::Status FindInTablePathExpression(
      const ASTTablePathExpression* table_ref,
      AliasSet* visible_aliases);

  // Traverse all expressions attached as descendants of <root>.
  // Unlike other methods above, may be called with NULL.
  zetasql_base::Status FindInExpressionsUnder(const ASTNode* root,
                                      const AliasSet& visible_aliases);

  // Traverse all options_list node as descendants of <root>.
  // May be called with NULL.
  zetasql_base::Status FindInOptionsListUnder(const ASTNode* root,
                                      const AliasSet& visible_aliases);

  // Root level SQL statement we are extracting table names or temporal
  // references from.
  const absl::string_view sql_;

  const AnalyzerOptions* analyzer_options_;  // Not owned.

  const bool for_system_time_as_of_feature_enabled_;

  TypeFactory* type_factory_;

  Catalog* catalog_;

  // The set of table names we are building up in this call to FindTables.
  // NOTE: The raw pointer is not owned.  We just cache the output parameter
  // to FindTables/FindTableNamesAndTemporalReferences to simplify sharing
  // across recursive calls.
  TableNamesSet* table_names_ = nullptr;

  // The set of temporal table references we are building up
  // in this call to FindTemporalTableReferencess.
  // NOTE: The raw pointer is not owned.  We just cache the output parameter
  // to FindTableNamesAndTemporalReferences to simplify sharing
  // across recursive calls.
  TableResolutionTimeInfoMap* table_resolution_time_info_map_ = nullptr;

  // The set of WITH aliases defined in the current statement.
  AliasSet with_aliases_;
};

zetasql_base::Status TableNameResolver::FindTableNamesAndTemporalReferences(
    const ASTStatement& statement) {
  table_names_->clear();
  if (table_resolution_time_info_map_ != nullptr) {
    ZETASQL_RET_CHECK_EQ((type_factory_ == nullptr), (catalog_ == nullptr));
    table_resolution_time_info_map_->clear();
  }

  ZETASQL_RETURN_IF_ERROR(FindInStatement(&statement, {} /* visible_aliases */));

  ZETASQL_RET_CHECK(with_aliases_.empty());  // Sanity check - these should get popped.
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindTableNames(const ASTScript& script) {
  table_names_->clear();
  ZETASQL_RETURN_IF_ERROR(FindInScriptNode(&script));
  ZETASQL_RET_CHECK(with_aliases_.empty());  // Sanity check - these should get popped.
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInScriptNode(const ASTNode* node) {
  for (int i = 0; i < node->num_children(); ++i) {
    const ASTNode* child = node->child(i);
    if (child->IsExpression()) {
      ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(child, /*visible_aliases=*/{}));
    } else if (child->IsSqlStatement()) {
      ZETASQL_RETURN_IF_ERROR(FindInStatement(child->GetAs<ASTStatement>(),
                                      /*visible_aliases=*/{}));
    }
    ZETASQL_RETURN_IF_ERROR(FindInScriptNode(child));
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInStatement(
    const ASTStatement* statement,
    const AliasSet& visible_aliases) {
  // Find table name under OPTIONS (...) clause for any type of statement.
  ZETASQL_RETURN_IF_ERROR(FindInOptionsListUnder(statement, visible_aliases));
  switch (statement->node_kind()) {
    case AST_QUERY_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_QUERY_STMT)) {
        return FindInQueryStatement(
            static_cast<const ASTQueryStatement*>(statement), visible_aliases);
      }
      break;

    case AST_EXPLAIN_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_EXPLAIN_STMT)) {
        const ASTExplainStatement* explain =
            static_cast<const ASTExplainStatement*>(statement);
        return FindInStatement(explain->statement(), visible_aliases);
      }
      break;

    case AST_CREATE_DATABASE_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_CREATE_DATABASE_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_CREATE_INDEX_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_CREATE_INDEX_STMT)) {
        const ASTCreateIndexStatement* create_index =
            static_cast<const ASTCreateIndexStatement*>(statement);
        zetasql_base::InsertIfNotPresent(
            table_names_, create_index->table_name()->ToIdentifierVector());
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_CREATE_TABLE_STATEMENT: {
      const ASTQuery* query =
          statement->GetAs<ASTCreateTableStatement>()->query();
      if (query == nullptr) {
        if (analyzer_options_->language().SupportsStatementKind(
                RESOLVED_CREATE_TABLE_STMT)) {
          return ::zetasql_base::OkStatus();
        }
      } else {
        if (analyzer_options_->language().SupportsStatementKind(
                RESOLVED_CREATE_TABLE_AS_SELECT_STMT)) {
          return FindInQuery(query, visible_aliases);
        }
      }
      break;
    }
    case AST_CREATE_MODEL_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_CREATE_MODEL_STMT)) {
        const ASTQuery* query =
            statement->GetAs<ASTCreateModelStatement>()->query();
        if (query == nullptr) {
          return ::zetasql_base::OkStatus();
        }
        return FindInQuery(query, visible_aliases);
      }
      break;
    case AST_CREATE_VIEW_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_CREATE_VIEW_STMT)) {
        return FindInCreateViewStatement(
            statement->GetAs<ASTCreateViewStatement>(), visible_aliases);
      }
      break;
    case AST_CREATE_MATERIALIZED_VIEW_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_CREATE_MATERIALIZED_VIEW_STMT)) {
        return FindInCreateMaterializedViewStatement(
            statement->GetAs<ASTCreateMaterializedViewStatement>(),
            visible_aliases);
      }
      break;

    case AST_CREATE_EXTERNAL_TABLE_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_CREATE_EXTERNAL_TABLE_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_CREATE_ROW_ACCESS_POLICY_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_CREATE_ROW_ACCESS_POLICY_STMT)) {
        const ASTCreateRowAccessPolicyStatement* stmt =
            statement->GetAsOrDie<ASTCreateRowAccessPolicyStatement>();
        zetasql_base::InsertIfNotPresent(table_names_,
                                stmt->target_path()->ToIdentifierVector());
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_CREATE_CONSTANT_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_CREATE_CONSTANT_STMT)) {
        ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(
            static_cast<const ASTCreateConstantStatement*>(statement)->expr(),
            visible_aliases));
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_CREATE_FUNCTION_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_CREATE_FUNCTION_STMT)) {
        ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(
            static_cast<const ASTCreateFunctionStatement*>(statement)
                ->sql_function_body(),
            visible_aliases));
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_CREATE_TABLE_FUNCTION_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_CREATE_TABLE_FUNCTION_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_CREATE_PROCEDURE_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_CREATE_PROCEDURE_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_EXPORT_DATA_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_EXPORT_DATA_STMT)) {
        return FindInExportDataStatement(
            statement->GetAs<ASTExportDataStatement>(), visible_aliases);
      }
      break;

    case AST_CALL_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_CALL_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_DEFINE_TABLE_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_DEFINE_TABLE_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_DESCRIBE_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_DESCRIBE_STMT)) {
        // Note that for a DESCRIBE TABLE statement, the table name is not
        // inserted into table_names_. Engines that need to know about a table
        // referenced by DESCRIBE TABLE should handle that themselves.
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_SHOW_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_SHOW_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_BEGIN_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_BEGIN_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_SET_TRANSACTION_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_SET_TRANSACTION_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_COMMIT_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_COMMIT_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_ROLLBACK_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_ROLLBACK_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_START_BATCH_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_START_BATCH_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_RUN_BATCH_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_RUN_BATCH_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_ABORT_BATCH_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_ABORT_BATCH_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_DELETE_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_DELETE_STMT)) {
        return FindInDeleteStatement(statement->GetAs<ASTDeleteStatement>(),
                                     visible_aliases);
      }
      break;

    case AST_DROP_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_DROP_STMT)) {
        // Note that for a DROP TABLE statement, the table name is not
        // inserted into table_names_. Engines that need to know about a table
        // referenced by DROP TABLE should handle that themselves.
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_TRUNCATE_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_TRUNCATE_STMT)) {
        return FindInTruncateStatement(
            statement->GetAsOrDie<ASTTruncateStatement>(), visible_aliases);
      }
      break;

    case AST_DROP_MATERIALIZED_VIEW_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
          RESOLVED_DROP_MATERIALIZED_VIEW_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_DROP_FUNCTION_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_DROP_FUNCTION_STMT)) {
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_DROP_ROW_ACCESS_POLICY_STATEMENT:
    case AST_DROP_ALL_ROW_ACCESS_POLICIES_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_DROP_ROW_ACCESS_POLICY_STMT)) {
        // Note that for a DROP [ALL] ROW (ACCESS POLICY|[ACCESS] POLICIES)
        // statement, the table name is not inserted into table_names_. Engines
        // that need to know about the target table should handle that
        // themselves.
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_RENAME_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_RENAME_STMT)) {
        // Note that for a RENAME TABLE statement, the table names are not
        // inserted into table_names_. Engines that need to know about a table
        // referenced by RENAME TABLE should handle that themselves.
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_INSERT_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_INSERT_STMT)) {
        return FindInInsertStatement(statement->GetAs<ASTInsertStatement>(),
                                     visible_aliases);
      }
      break;

    case AST_UPDATE_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_UPDATE_STMT)) {
        return FindInUpdateStatement(statement->GetAs<ASTUpdateStatement>(),
                                     visible_aliases);
      }
      break;

    case AST_MERGE_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_MERGE_STMT)) {
        return FindInMergeStatement(statement->GetAs<ASTMergeStatement>(),
                                    visible_aliases);
      }
      break;

    case AST_GRANT_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_GRANT_STMT)) {
        // Note that for a GRANT statement, the table name is not inserted
        // into table_names_. Engines that need to know about a table
        // referenced by GRANT statement should handle that themselves.
        return ::zetasql_base::OkStatus();
      }
      break;

    case AST_REVOKE_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_REVOKE_STMT)) {
        // Note that for a REVOKE statement, the table name is not inserted
        // into table_names_. Engines that need to know about a table
        // referenced by REVOKE statement should handle that themselves.
        return ::zetasql_base::OkStatus();
      }
      break;
    case AST_ALTER_ROW_POLICY_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_ALTER_ROW_POLICY_STMT)) {
        const ASTAlterRowPolicyStatement* stmt =
            statement->GetAs<ASTAlterRowPolicyStatement>();
        zetasql_base::InsertIfNotPresent(table_names_,
                                stmt->target_path()->ToIdentifierVector());
        return ::zetasql_base::OkStatus();
      }
      break;
    case AST_ALTER_TABLE_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_ALTER_TABLE_SET_OPTIONS_STMT) ||
          analyzer_options_->language().SupportsStatementKind(
              RESOLVED_ALTER_TABLE_STMT)) {
        // Note that for a ALTER TABLE statement, the table name is not
        // inserted into table_names_. Engines that need to know about a table
        // referenced by ALTER TABLE should handle that themselves.
        return ::zetasql_base::OkStatus();
      }
      break;
    case AST_ALTER_VIEW_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_ALTER_VIEW_STMT)) {
        // Note that for a ALTER VIEW statement, the table name is not
        // inserted into table_names_. Engines that need to know about a table
        // referenced by ALTER VIEW should handle that themselves.
        return ::zetasql_base::OkStatus();
      }
      break;
    case AST_ALTER_MATERIALIZED_VIEW_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_ALTER_MATERIALIZED_VIEW_STMT)) {
        // Note that for a ALTER MATERIALIZED VIEW statement, the table name is
        // not inserted into table_names_. Engines that need to know about a
        // table referenced by ALTER MATERIALIZED VIEW should handle that
        // themselves.
        return ::zetasql_base::OkStatus();
      }
      break;
    case AST_HINTED_STATEMENT:
      return FindInStatement(
          statement->GetAs<ASTHintedStatement>()->statement(),
          visible_aliases);
    case AST_IMPORT_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_IMPORT_STMT)) {
        // There are no table names in an IMPORT statement.
        return ::zetasql_base::OkStatus();
      }
      break;
    case AST_MODULE_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_MODULE_STMT)) {
        // There are no table names in a MODULE statement.
        return ::zetasql_base::OkStatus();
      }
      break;
    case AST_ASSERT_STATEMENT:
      if (analyzer_options_->language().SupportsStatementKind(
              RESOLVED_ASSERT_STMT)) {
        return FindInExpressionsUnder(
            statement->GetAs<ASTAssertStatement>()->expr(), visible_aliases);
      }
      break;
    default:
      break;
  }

  // This statement is not currently supported so we return an error here.
  return MakeSqlErrorAt(statement)
         << "Statement not supported: " << statement->GetNodeKindString();
}

zetasql_base::Status TableNameResolver::FindInQueryStatement(
    const ASTQueryStatement* statement,
    const AliasSet& visible_aliases) {
  return FindInQuery(statement->query(), visible_aliases);
}

zetasql_base::Status TableNameResolver::FindInCreateViewStatement(
    const ASTCreateViewStatement* statement,
    const AliasSet& visible_aliases) {
  return FindInQuery(statement->query(), visible_aliases);
}

zetasql_base::Status TableNameResolver::FindInCreateMaterializedViewStatement(
    const ASTCreateMaterializedViewStatement* statement,
    const AliasSet& visible_aliases) {
  return FindInQuery(statement->query(), visible_aliases);
}

zetasql_base::Status TableNameResolver::FindInExportDataStatement(
    const ASTExportDataStatement* statement,
    const AliasSet& visible_aliases) {
  return FindInQuery(statement->query(), visible_aliases);
}

zetasql_base::Status TableNameResolver::FindInDeleteStatement(
    const ASTDeleteStatement* statement, const AliasSet& orig_visible_aliases) {
  AliasSet visible_aliases = orig_visible_aliases;

  ZETASQL_ASSIGN_OR_RETURN(const ASTPathExpression* path_expr,
                   statement->GetTargetPathForNonNested());
  std::vector<std::string> path = path_expr->ToIdentifierVector();
  if (!zetasql_base::ContainsKey(visible_aliases, absl::AsciiStrToLower(path[0]))) {
    zetasql_base::InsertIfNotPresent(table_names_, path);
    zetasql_base::InsertIfNotPresent(&visible_aliases,
                            absl::AsciiStrToLower(path.back()));
  }

  ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(statement->where(), visible_aliases));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInTruncateStatement(
    const ASTTruncateStatement* statement,
    const AliasSet& orig_visible_aliases) {
  AliasSet visible_aliases = orig_visible_aliases;

  ZETASQL_ASSIGN_OR_RETURN(const ASTPathExpression* path_expr,
                   statement->GetTargetPathForNonNested());
  std::vector<std::string> path = path_expr->ToIdentifierVector();
  if (!zetasql_base::ContainsKey(visible_aliases, absl::AsciiStrToLower(path[0]))) {
    zetasql_base::InsertIfNotPresent(table_names_, path);
    zetasql_base::InsertIfNotPresent(&visible_aliases,
                            absl::AsciiStrToLower(path.back()));
  }

  return FindInExpressionsUnder(statement->where(), visible_aliases);
}

zetasql_base::Status TableNameResolver::FindInInsertStatement(
    const ASTInsertStatement* statement, const AliasSet& orig_visible_aliases) {
  AliasSet visible_aliases = orig_visible_aliases;

  ZETASQL_ASSIGN_OR_RETURN(const ASTPathExpression* path_expr,
                   statement->GetTargetPathForNonNested());
  std::vector<std::string> path = path_expr->ToIdentifierVector();
  if (!zetasql_base::ContainsKey(visible_aliases, absl::AsciiStrToLower(path[0]))) {
    zetasql_base::InsertIfNotPresent(table_names_, path);
    zetasql_base::InsertIfNotPresent(&visible_aliases,
                            absl::AsciiStrToLower(path.back()));
  }

  if (statement->rows() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(statement->rows(), visible_aliases));
  }

  if (statement->query() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(FindInQuery(statement->query(), visible_aliases));
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInUpdateStatement(
    const ASTUpdateStatement* statement, const AliasSet& orig_visible_aliases) {
  AliasSet visible_aliases = orig_visible_aliases;

  ZETASQL_ASSIGN_OR_RETURN(const ASTPathExpression* path_expr,
                   statement->GetTargetPathForNonNested());
  const std::vector<std::string> path = path_expr->ToIdentifierVector();

  if (!zetasql_base::ContainsKey(visible_aliases, absl::AsciiStrToLower(path[0]))) {
    zetasql_base::InsertIfNotPresent(table_names_, path);
    const std::string alias = statement->alias() == nullptr
                             ? path.back()
                             : statement->alias()->GetAsString();
    zetasql_base::InsertIfNotPresent(&visible_aliases, absl::AsciiStrToLower(alias));
  }

  if (statement->from_clause() != nullptr) {
    ZETASQL_RET_CHECK(statement->from_clause()->table_expression() != nullptr);
    ZETASQL_RETURN_IF_ERROR(FindInTableExpression(
        statement->from_clause()->table_expression(),
        orig_visible_aliases,
        &visible_aliases));
  }

  ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(statement->where(), visible_aliases));
  ZETASQL_RETURN_IF_ERROR(
      FindInExpressionsUnder(statement->update_item_list(), visible_aliases));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInMergeStatement(
    const ASTMergeStatement* statement, const AliasSet& visible_aliases) {
  AliasSet all_visible_aliases = visible_aliases;

  const ASTPathExpression* path_expr = statement->target_path();
  std::vector<std::string> path = path_expr->ToIdentifierVector();
  if (!zetasql_base::ContainsKey(all_visible_aliases, absl::AsciiStrToLower(path[0]))) {
    zetasql_base::InsertIfNotPresent(table_names_, path);
    zetasql_base::InsertIfNotPresent(&all_visible_aliases,
                            absl::AsciiStrToLower(path.back()));
  }

  ZETASQL_RETURN_IF_ERROR(FindInTableExpression(statement->table_expression(),
                                        visible_aliases, &all_visible_aliases));
  ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(statement->merge_condition(),
                                         all_visible_aliases));
  ZETASQL_RETURN_IF_ERROR(
      FindInExpressionsUnder(statement->when_clauses(), all_visible_aliases));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInQuery(
    const ASTQuery* query,
    const AliasSet& orig_visible_aliases) {

  AliasSet visible_aliases = orig_visible_aliases;

  AliasSet old_with_aliases;
  if (query->with_clause() != nullptr) {
    // Record the set of WITH aliases visible in the outer scope so we can
    // restore that after processing the local query.
    old_with_aliases = with_aliases_;

    for (const ASTWithClauseEntry* with_entry : query->with_clause()->with()) {
      ZETASQL_RETURN_IF_ERROR(FindInQuery(with_entry->query(), visible_aliases));

      const std::string with_alias =
          absl::AsciiStrToLower(with_entry->alias()->GetAsString());
      zetasql_base::InsertIfNotPresent(&visible_aliases, with_alias);
      zetasql_base::InsertIfNotPresent(&with_aliases_, with_alias);
    }
  }

  ZETASQL_RETURN_IF_ERROR(FindInQueryExpression(query->query_expr(),
                                        query->order_by(),
                                        visible_aliases));

  // Restore outer WITH alias set if we modified it.
  if (query->with_clause() != nullptr) {
    with_aliases_ = old_with_aliases;
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInQueryExpression(
    const ASTQueryExpression* query_expr,
    const ASTOrderBy* order_by,
    const AliasSet& visible_aliases) {
  switch (query_expr->node_kind()) {
    case AST_SELECT:
      ZETASQL_RETURN_IF_ERROR(
          FindInSelect(query_expr->GetAs<ASTSelect>(),
                       order_by,
                       visible_aliases));
      break;
    case AST_SET_OPERATION:
      ZETASQL_RETURN_IF_ERROR(
          FindInSetOperation(query_expr->GetAs<ASTSetOperation>(),
                             visible_aliases));
      break;
    case AST_QUERY:
      ZETASQL_RETURN_IF_ERROR(
          FindInQuery(query_expr->GetAs<ASTQuery>(), visible_aliases));
      break;
    default:
      return MakeSqlErrorAt(query_expr)
             << "Unhandled query_expr:\n" << query_expr->DebugString();
  }

  if (query_expr->node_kind() != AST_SELECT) {
    ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(order_by, visible_aliases));
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInSelect(
    const ASTSelect* select,
    const ASTOrderBy* order_by,
    const AliasSet& orig_visible_aliases) {
  AliasSet visible_aliases = orig_visible_aliases;
  if (select->from_clause() != nullptr) {
    ZETASQL_RET_CHECK(select->from_clause()->table_expression() != nullptr);
    ZETASQL_RETURN_IF_ERROR(FindInTableExpression(
        select->from_clause()->table_expression(),
        orig_visible_aliases,
        &visible_aliases));
  }
  ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(select->select_list(),
                                         visible_aliases));
  ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(select->where_clause(),
                                         visible_aliases));
  ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(select->group_by(), visible_aliases));
  ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(select->having(), visible_aliases));
  ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(order_by, visible_aliases));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInSetOperation(
    const ASTSetOperation* set_operation,
    const AliasSet& visible_aliases) {
  for (const ASTQueryExpression* input : set_operation->inputs()) {
    ZETASQL_RETURN_IF_ERROR(FindInQueryExpression(input, nullptr /* order_by */,
                                          visible_aliases));
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInTableExpression(
    const ASTTableExpression* table_expr,
    const AliasSet& external_visible_aliases,
    AliasSet* local_visible_aliases) {
  switch (table_expr->node_kind()) {
    case AST_TABLE_PATH_EXPRESSION:
      return FindInTablePathExpression(
          table_expr->GetAs<ASTTablePathExpression>(), local_visible_aliases);

    case AST_TABLE_SUBQUERY:
      return FindInTableSubquery(
          table_expr->GetAs<ASTTableSubquery>(),
          external_visible_aliases, local_visible_aliases);

    case AST_JOIN:
      return FindInJoin(table_expr->GetAs<ASTJoin>(),
                        external_visible_aliases, local_visible_aliases);

    case AST_PARENTHESIZED_JOIN:
      return FindInParenthesizedJoin(table_expr->GetAs<ASTParenthesizedJoin>(),
                                     external_visible_aliases,
                                     local_visible_aliases);

    case AST_TVF:
      return FindInTVF(table_expr->GetAs<ASTTVF>(), external_visible_aliases,
                       local_visible_aliases);
    default:
      return MakeSqlErrorAt(table_expr)
             << "Unhandled node type in from clause: "
             << table_expr->GetNodeKindString();
  }
}

zetasql_base::Status TableNameResolver::FindInJoin(
    const ASTJoin* join,
    const AliasSet& external_visible_aliases,
    AliasSet* local_visible_aliases) {
  ZETASQL_RETURN_IF_ERROR(FindInTableExpression(join->lhs(), external_visible_aliases,
                                        local_visible_aliases));
  ZETASQL_RETURN_IF_ERROR(FindInTableExpression(join->rhs(), external_visible_aliases,
                                        local_visible_aliases));
  ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(join->on_clause(),
                                         *local_visible_aliases));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInParenthesizedJoin(
    const ASTParenthesizedJoin* parenthesized_join,
    const AliasSet& external_visible_aliases, AliasSet* local_visible_aliases) {
  const ASTJoin* join = parenthesized_join->join();
  // In parenthesized joins, we can't see names from outside the parentheses.
  std::unique_ptr<AliasSet> join_visible_aliases(
      new AliasSet(external_visible_aliases));
  ZETASQL_RETURN_IF_ERROR(FindInJoin(join, external_visible_aliases,
                             join_visible_aliases.get()));
  for (const std::string& alias : *join_visible_aliases) {
    zetasql_base::InsertIfNotPresent(local_visible_aliases, alias);
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInTVF(
    const ASTTVF* tvf,
    const AliasSet& external_visible_aliases, AliasSet* local_visible_aliases) {
  // The 'tvf' here is the TVF parse node. Each TVF argument may be a scalar, a
  // relation, or a TABLE clause. We've parsed all of the TVF arguments as
  // expressions by this point, so the FindInExpressionsUnder call will descend
  // into the relation arguments as expression subqueries. For TABLE clause
  // arguments, we add each named table to the set of referenced table names in
  // a separate step.
  //
  // Note about correlation: if a TVF argument is a scalar, it should resolve
  // like a correlated subquery and be able to see 'local_visible_aliases'. On
  // the other hand, if the argument is a relation, it should be uncorrelated,
  // and so those aliases should not be visible. Because we don't know whether
  // the argument should be a scalar or a relation yet, we allow correlation
  // here and examine the arguments again during resolving.
  ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(tvf, *local_visible_aliases));
  for (const ASTTVFArgument* arg : tvf->argument_entries()) {
    if (arg->table_clause() != nullptr) {
      if (arg->table_clause()->table_path() != nullptr &&
          !zetasql_base::ContainsKey(with_aliases_,
                            absl::AsciiStrToLower(arg->table_clause()
                                                      ->table_path()
                                                      ->first_name()
                                                      ->GetAsString()))) {
        zetasql_base::InsertIfNotPresent(
            table_names_,
            arg->table_clause()->table_path()->ToIdentifierVector());
      }
      if (arg->table_clause()->tvf() != nullptr) {
        ZETASQL_RETURN_IF_ERROR(FindInTVF(arg->table_clause()->tvf(),
                                  external_visible_aliases,
                                  local_visible_aliases));
      }
    }
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInTableSubquery(
    const ASTTableSubquery* table_subquery,
    const AliasSet& external_visible_aliases,
    AliasSet* local_visible_aliases) {

  // A table subquery doesn't see any aliases for preceding scans in the
  // from clause.  It can only see the external aliases and WITH aliases.
  AliasSet subquery_visible_aliases = external_visible_aliases;
  for (const std::string& alias : with_aliases_) {
    zetasql_base::InsertIfNotPresent(&subquery_visible_aliases, alias);
  }
  ZETASQL_RETURN_IF_ERROR(FindInQuery(table_subquery->subquery(),
                              subquery_visible_aliases));

  if (table_subquery->alias() != nullptr) {
    zetasql_base::InsertIfNotPresent(
        local_visible_aliases,
        absl::AsciiStrToLower(table_subquery->alias()->GetAsString()));
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInTablePathExpression(
    const ASTTablePathExpression* table_ref,
    AliasSet* visible_aliases) {

  std::string alias;
  if (table_ref->alias() != nullptr) {
    alias = table_ref->alias()->GetAsString();
  }

  if (table_ref->path_expr() != nullptr) {
    const ASTPathExpression* path_expr = table_ref->path_expr();
    std::vector<std::string> path = path_expr->ToIdentifierVector();
    ZETASQL_RET_CHECK(!path.empty());

    // Single identifiers are always table names, not range variable references,
    // but could be WITH table references.
    // For paths, check if the first identifier is a known alias.
    const std::string first_identifier = absl::AsciiStrToLower(path[0]);
    if (path.size() == 1
            ? !zetasql_base::ContainsKey(with_aliases_, first_identifier)
            : !zetasql_base::ContainsKey(*visible_aliases, first_identifier)) {
      zetasql_base::InsertIfNotPresent(table_names_, path);
      if (table_resolution_time_info_map_ != nullptr) {
        // Lookup for or insert a set of temporal expressions for 'path'.
        TableResolutionTimeInfo& temporal_expressions_set =
            (*table_resolution_time_info_map_)[path];

        const ASTForSystemTime* for_system_time = table_ref->for_system_time();
        if (for_system_time != nullptr) {
          if (!for_system_time_as_of_feature_enabled_) {
            return MakeSqlErrorAt(for_system_time)
                   << "FOR SYSTEM_TIME AS OF is not supported";
          }

          const ASTExpression* expr = for_system_time->expression();
          ZETASQL_RET_CHECK(expr != nullptr);
          std::unique_ptr<const AnalyzerOutput> analyzed;

          if (catalog_ != nullptr) {
            ZETASQL_RETURN_IF_ERROR(::zetasql::AnalyzeExpressionFromParserAST(
                *expr, *analyzer_options_, sql_, type_factory_, catalog_,
                &analyzed));
          }
          temporal_expressions_set.exprs.push_back({expr, std::move(analyzed)});
        } else {
          temporal_expressions_set.has_default_resolution_time = true;
        }
      }
    }

    if (alias.empty()) {
      alias = path.back();
    }
  }

  ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(table_ref->unnest_expr(),
                                         *visible_aliases));

  if (!alias.empty()) {
    visible_aliases->insert(absl::AsciiStrToLower(alias));
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInExpressionsUnder(
    const ASTNode* root,
    const AliasSet& visible_aliases) {
  if (root == nullptr) return ::zetasql_base::OkStatus();

  // The only thing that matters inside expressions are expression subqueries,
  // which can be either ASTExpressionSubquery or ASTIn, both of which have
  // the subquery in an ASTQuery child.
  std::vector<const ASTNode*> subquery_nodes;
  root->GetDescendantSubtreesWithKinds({AST_QUERY}, &subquery_nodes);

  for (const ASTNode* subquery_node : subquery_nodes) {
    ZETASQL_RETURN_IF_ERROR(FindInQuery(subquery_node->GetAs<ASTQuery>(),
                                visible_aliases));
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status TableNameResolver::FindInOptionsListUnder(
    const ASTNode* root,
    const AliasSet& visible_aliases) {
  if (root == nullptr) return ::zetasql_base::OkStatus();

  std::vector<const ASTNode*> options_list_nodes;
  root->GetDescendantSubtreesWithKinds({AST_OPTIONS_LIST}, &options_list_nodes);

  for (const ASTNode* options_list : options_list_nodes) {
    ZETASQL_RETURN_IF_ERROR(FindInExpressionsUnder(options_list, visible_aliases));
  }
  return ::zetasql_base::OkStatus();
}
}  // namespace

zetasql_base::Status FindTableNamesAndResolutionTime(
    absl::string_view sql, const ASTStatement& statement,
    const AnalyzerOptions& analyzer_options, TypeFactory* type_factory,
    Catalog* catalog, TableNamesSet* table_names,
    TableResolutionTimeInfoMap* table_resolution_time_info_map) {
  return TableNameResolver(sql, &analyzer_options, type_factory, catalog,
                           table_names, table_resolution_time_info_map)
      .FindTableNamesAndTemporalReferences(statement);
}

zetasql_base::Status FindTableNamesInScript(absl::string_view sql,
                                    const ASTScript& script,
                                    const AnalyzerOptions& analyzer_options,
                                    TableNamesSet* table_names) {
  return TableNameResolver(sql, &analyzer_options, /*type_factory=*/nullptr,
                           /*catalog=*/nullptr, table_names,
                           /*table_resolution_time_info_map=*/nullptr)
      .FindTableNames(script);
}

}  // namespace table_name_resolver
}  // namespace zetasql
