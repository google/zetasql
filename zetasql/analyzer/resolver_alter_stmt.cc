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

// This file contains the implementation of ALTER related resolver
// methods from resolver.h.
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/base/varsetter.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {
bool OptionsPresent(const ResolvedColumnAnnotations* annotations) {
  if (annotations != nullptr) {
    if (annotations->option_list_size() > 0) {
      return true;
    }
    for (int i = 0; i < annotations->child_list_size(); ++i) {
      if (OptionsPresent(annotations->child_list(i))) {
        return true;
      }
    }
  }
  return false;
}
bool NotNullPresent(
    const ResolvedColumnAnnotations* annotations) {
  if (annotations != nullptr) {
    if (annotations->not_null()) {
      return true;
    }
    for (int i = 0; i < annotations->child_list_size(); ++i) {
      if (NotNullPresent(annotations->child_list(i))) {
        return true;
      }
    }
  }
  return false;
}
}  // namespace

absl::Status Resolver::ResolveAlterActions(
    const ASTAlterStatementBase* ast_statement,
    absl::string_view alter_statement_kind,
    std::unique_ptr<ResolvedStatement>* output,
    bool* has_only_set_options_action,
    std::vector<std::unique_ptr<const ResolvedAlterAction>>* alter_actions) {
  // path() can be null iff ALLOW_MISSING_PATH_EXPRESSION_IN_ALTER_DDL is set.
  ZETASQL_RET_CHECK(language().LanguageFeatureEnabled(
                FEATURE_ALLOW_MISSING_PATH_EXPRESSION_IN_ALTER_DDL) ||
            ast_statement->path() != nullptr);
  // The default empty value of IdString is handled in all downstream paths.
  IdString table_name_id_string;
  if (ast_statement->path() != nullptr) {
    table_name_id_string =
        MakeIdString(ast_statement->path()->ToIdentifierPathString());
  }

  IdStringSetCase new_columns, column_to_drop;

  // <columns_to_rename> is used to store old column names of columns renamed by
  // RENAME COLUMN. If a column is renamed multiple times in same statement,
  // the column's original name (the column name in catalog) will be stored in
  // <columns_to_rename>.
  IdStringSetCase columns_to_rename;

  // This <columns_rename_map> map takes the new column name as key and the old
  // column name as value. If a column is renamed multiple times in the same
  // statement, the column's original name (the column name in the catalog) will
  // be stored in the map as the value.
  IdStringHashMapCase<IdString> columns_rename_map;
  const Table* altered_table = nullptr;
  bool existing_rename_to_action = false;

  // If the path expression is null the language feature is enabled (RET_CHECK
  // at the start of this function).
  absl::Status table_status;
  if (ast_statement->path() != nullptr) {
    // Some engines do not add all the referenced tables into the catalog. Thus,
    // if the lookup here fails it does not necessarily mean that the table does
    // not exist.
    table_status = FindTable(ast_statement->path(), &altered_table);
  } else {  // Path is missing. This may or may not be a problem, but we set the
            // error message here regardless; the consumer of table_status can
            // decide what to do with it.

    // alter_entities could be intentionally missing the path so we need
    // to generate a potentially-user-facing error.
    if (auto alter_entity =
            ast_statement->GetAsOrNull<ASTAlterEntityStatement>()) {
      std::string entity_type =
          absl::AsciiStrToUpper(alter_entity->type()->GetAsString());
      table_status = MakeSqlErrorAt(ast_statement)
                     << "ALTER " << entity_type
                     << " statements must have a path expression between "
                     << entity_type << " and the first alter action.";
    } else {
      // The only type of alter statement that should be able to exhibit a
      // missing path is alter_entity. Anything else is a bug, so generate an
      // internal error.
      table_status = absl::Status(
          absl::StatusCode::kInternal,
          absl::StrFormat("Path missing on non-alter_entity ALTER statement of "
                          "node kind %s. ZetaSQL grammar bug?",
                          ast_statement->GetNodeKindString()));
    }
  }

  *has_only_set_options_action = true;
  bool already_added_primary_key = false;
  bool has_rename_column = false;
  bool has_non_rename_column_action = false;
  const ASTAlterActionList* action_list = ast_statement->action_list();
  for (const ASTAlterAction* const action : action_list->actions()) {
    if (action->node_kind() != AST_SET_OPTIONS_ACTION) {
      *has_only_set_options_action = false;
    }
    if (action->node_kind() == AST_RENAME_COLUMN_ACTION) {
      if (has_non_rename_column_action) {
        return MakeSqlErrorAt(action)
               << "ALTER " << alter_statement_kind
               << " does not support combining "
               << action->GetSQLForAlterAction() << " with other alter actions";
      }
      has_rename_column = true;
    } else {
      if (has_rename_column) {
        return MakeSqlErrorAt(action)
               << "ALTER " << alter_statement_kind
               << " does not support combining "
               << action->GetSQLForAlterAction() << " with RENAME COLUMN";
      }
      has_non_rename_column_action = true;
    }
    switch (action->node_kind()) {
      case AST_SET_OPTIONS_ACTION: {
        std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
        ZETASQL_RETURN_IF_ERROR(ResolveOptionsList(
            action->GetAsOrDie<ASTSetOptionsAction>()->options_list(),
            &resolved_options));
        alter_actions->push_back(
            MakeResolvedSetOptionsAction(std::move(resolved_options)));
      } break;
      case AST_ADD_CONSTRAINT_ACTION:
      case AST_DROP_CONSTRAINT_ACTION: {
        if (!ast_statement->is_if_exists()) {
          ZETASQL_RETURN_IF_ERROR(table_status);
        }
        if (action->node_kind() == AST_ADD_CONSTRAINT_ACTION) {
          const auto* constraint = action->GetAsOrDie<ASTAddConstraintAction>();
          auto constraint_kind = constraint->constraint()->node_kind();
          if (constraint_kind == AST_PRIMARY_KEY) {
            if (already_added_primary_key) {
              return MakeSqlErrorAt(action)
                     << "ALTER TABLE only supports one ADD PRIMARY KEY action";
            }
            already_added_primary_key = true;
          }

          std::unique_ptr<const ResolvedAddConstraintAction>
              resolved_alter_action;
          ZETASQL_RETURN_IF_ERROR(ResolveAddConstraintAction(altered_table,
                                                     ast_statement, constraint,
                                                     &resolved_alter_action));
          alter_actions->push_back(std::move(resolved_alter_action));
        } else {
          const auto* constraint =
              action->GetAsOrDie<ASTDropConstraintAction>();
          alter_actions->push_back(MakeResolvedDropConstraintAction(
              constraint->is_if_exists(),
              constraint->constraint_name()->GetAsString()));
        }
      } break;
      case AST_DROP_PRIMARY_KEY_ACTION: {
        if (!ast_statement->is_if_exists()) {
          ZETASQL_RETURN_IF_ERROR(table_status);
        }
        alter_actions->push_back(MakeResolvedDropPrimaryKeyAction(
            action->GetAsOrDie<ASTDropPrimaryKeyAction>()->is_if_exists()));
      } break;
      case AST_ALTER_CONSTRAINT_ENFORCEMENT_ACTION:
        return MakeSqlErrorAt(action)
               << "ALTER CONSTRAINT ENFORCED/NOT ENFORCED is not supported";
      case AST_ALTER_CONSTRAINT_SET_OPTIONS_ACTION:
        return MakeSqlErrorAt(action)
               << "ALTER CONSTRAINT SET OPTIONS is not supported";
      case AST_ADD_COLUMN_ACTION:
      case AST_DROP_COLUMN_ACTION:
      case AST_RENAME_COLUMN_ACTION:
      case AST_ALTER_COLUMN_TYPE_ACTION: {
        if (ast_statement->node_kind() != AST_ALTER_TABLE_STATEMENT) {
          // Views, models, etc don't support ADD/DROP/RENAME/SET DATA TYPE
          // columns.
          return MakeSqlErrorAt(action)
                 << "ALTER " << alter_statement_kind << " does not support "
                 << action->GetSQLForAlterAction();
        }
        if (!ast_statement->is_if_exists()) {
          ZETASQL_RETURN_IF_ERROR(table_status);
        }
        std::unique_ptr<const ResolvedAlterAction> resolved_action;
        switch (action->node_kind()) {
          case AST_ADD_COLUMN_ACTION: {
            ZETASQL_RETURN_IF_ERROR(ResolveAddColumnAction(
                table_name_id_string, altered_table,
                action->GetAsOrDie<ASTAddColumnAction>(), &new_columns,
                &column_to_drop, &resolved_action));
          } break;
          case AST_DROP_COLUMN_ACTION: {
            ZETASQL_RETURN_IF_ERROR(ResolveDropColumnAction(
                altered_table, action->GetAsOrDie<ASTDropColumnAction>(),
                &new_columns, &column_to_drop, &resolved_action));
          } break;
          case AST_ALTER_COLUMN_TYPE_ACTION: {
            if (language().LanguageFeatureEnabled(
                    FEATURE_ALTER_COLUMN_SET_DATA_TYPE)) {
              ZETASQL_RETURN_IF_ERROR(ResolveAlterColumnTypeAction(
                  table_name_id_string, altered_table,
                  action->GetAsOrDie<ASTAlterColumnTypeAction>(),
                  &resolved_action));
            } else {
              return MakeSqlErrorAt(action)
                     << "ALTER " << alter_statement_kind << " does not support "
                     << action->GetSQLForAlterAction();
            }
          } break;
          case AST_RENAME_COLUMN_ACTION: {
            if (language().LanguageFeatureEnabled(
                    FEATURE_ALTER_TABLE_RENAME_COLUMN) &&
                action->node_kind() == AST_RENAME_COLUMN_ACTION) {
              ZETASQL_RETURN_IF_ERROR(ResolveRenameColumnAction(
                  altered_table, action->GetAsOrDie<ASTRenameColumnAction>(),
                  &columns_to_rename, &columns_rename_map, &resolved_action));
            } else {
              return MakeSqlErrorAt(action)
                     << "ALTER " << alter_statement_kind << " does not support "
                     << action->GetSQLForAlterAction();
            }
          } break;
          default:
            return MakeSqlErrorAt(action)
                   << "ALTER " << alter_statement_kind << " does not support "
                   << action->GetSQLForAlterAction();
        }
        alter_actions->push_back(std::move(resolved_action));
      } break;
      case AST_SET_AS_ACTION: {
        if (ast_statement->node_kind() != AST_ALTER_ENTITY_STATEMENT) {
          return MakeSqlErrorAt(action)
                 << "ALTER " << alter_statement_kind << " does not support "
                 << action->GetSQLForAlterAction();
        }
        const auto* set_as_action = action->GetAsOrDie<ASTSetAsAction>();
        std::string entity_body_json;
        if (set_as_action->json_body() != nullptr) {
          // TODO: Use ResolveExpr() after JSON type goes public.
          ZETASQL_ASSIGN_OR_RETURN(auto json_literal,
                           ResolveJsonLiteral(set_as_action->json_body()));
          entity_body_json = json_literal->value().json_string();
        }
        std::string entity_body_text;
        if (set_as_action->text_body() != nullptr) {
          entity_body_text = set_as_action->text_body()->string_value();
        }
        if (entity_body_json.empty() && entity_body_text.empty()) {
          return MakeSqlErrorAt(action)
                 << "ALTER SET AS requires JSON or TEXT body literal";
        }
        if (!entity_body_text.empty() && !entity_body_json.empty()) {
          return MakeSqlErrorAt(ast_statement)
                 << "ALTER SET AS should have exactly one JSON or TEXT body "
                    "literal";
        }
        std::unique_ptr<const ResolvedAlterAction> resolved_action =
            MakeResolvedSetAsAction(entity_body_json, entity_body_text);
        alter_actions->push_back(std::move(resolved_action));
      } break;
      case AST_RENAME_TO_CLAUSE: {
        if (ast_statement->node_kind() != AST_ALTER_TABLE_STATEMENT) {
          // only rename table is supported
          return MakeSqlErrorAt(action)
                 << "ALTER " << alter_statement_kind << " does not support "
                 << action->GetSQLForAlterAction();
        }
        if (existing_rename_to_action) {
          return MakeSqlErrorAt(action)
                 << "Multiple RENAME TO actions are not supported";
        }
        existing_rename_to_action = true;
        auto* rename_to = action->GetAsOrDie<ASTRenameToClause>();
        std::unique_ptr<const ResolvedAlterAction> resolved_action =
            MakeResolvedRenameToAction(
                rename_to->new_name()->ToIdentifierVector());
        alter_actions->push_back(std::move(resolved_action));
        break;
      }
      case AST_ALTER_COLUMN_OPTIONS_ACTION:
      case AST_ALTER_COLUMN_DROP_NOT_NULL_ACTION: {
        if (ast_statement->node_kind() != AST_ALTER_TABLE_STATEMENT) {
          // Views, models, etc don't support ALTER COLUMN ... SET OPTIONS/DROP
          // NOT NULL ...
          return MakeSqlErrorAt(action)
                 << "ALTER " << alter_statement_kind << " does not support "
                 << action->GetSQLForAlterAction();
        }
        if (!ast_statement->is_if_exists()) {
          ZETASQL_RETURN_IF_ERROR(table_status);
        }
        std::unique_ptr<const ResolvedAlterAction> resolved_action;
        if (action->node_kind() == AST_ALTER_COLUMN_OPTIONS_ACTION) {
          ZETASQL_RETURN_IF_ERROR(ResolveAlterColumnOptionsAction(
              altered_table, action->GetAsOrDie<ASTAlterColumnOptionsAction>(),
              &resolved_action));
        } else if (action->node_kind() ==
                   AST_ALTER_COLUMN_DROP_NOT_NULL_ACTION) {
          ZETASQL_RETURN_IF_ERROR(ResolveAlterColumnDropNotNullAction(
              altered_table,
              action->GetAsOrDie<ASTAlterColumnDropNotNullAction>(),
              &resolved_action));
        }
        alter_actions->push_back(std::move(resolved_action));
      } break;
      case AST_ALTER_COLUMN_SET_DEFAULT_ACTION:
      case AST_ALTER_COLUMN_DROP_DEFAULT_ACTION: {
        if (!language().LanguageFeatureEnabled(
                FEATURE_V_1_3_COLUMN_DEFAULT_VALUE)) {
          return MakeSqlErrorAt(action)
                 << "Column default value is not supported";
        }
        if (ast_statement->node_kind() != AST_ALTER_TABLE_STATEMENT) {
          // Views, models, etc don't support ALTER COLUMN ... SET/DROP DEFAULT
          return MakeSqlErrorAt(action)
                 << "ALTER " << alter_statement_kind << " does not support "
                 << action->GetSQLForAlterAction();
        }
        if (!ast_statement->is_if_exists()) {
          ZETASQL_RETURN_IF_ERROR(table_status);
        }
        std::unique_ptr<const ResolvedAlterAction> resolved_action;
        if (action->node_kind() == AST_ALTER_COLUMN_SET_DEFAULT_ACTION) {
          ZETASQL_RETURN_IF_ERROR(ResolveAlterColumnSetDefaultAction(
              table_name_id_string, altered_table,
              action->GetAsOrDie<ASTAlterColumnSetDefaultAction>(),
              &resolved_action));
        } else {
          // action->node_kind() == AST_ALTER_COLUMN_DROP_DEFAULT_ACTION
          ZETASQL_RETURN_IF_ERROR(ResolveAlterColumnDropDefaultAction(
              altered_table,
              action->GetAsOrDie<ASTAlterColumnDropDefaultAction>(),
              &resolved_action));
        }
        alter_actions->push_back(std::move(resolved_action));
      } break;
      case AST_SET_COLLATE_CLAUSE: {
        if (!language().LanguageFeatureEnabled(
                FEATURE_V_1_3_COLLATION_SUPPORT) ||
            (ast_statement->node_kind() != AST_ALTER_TABLE_STATEMENT &&
             ast_statement->node_kind() != AST_ALTER_SCHEMA_STATEMENT)) {
          // AST_SET_COLLATE_CLAUSE supports ALTER TABLE and ALTER SCHEMA
          // statement.
          return MakeSqlErrorAt(action)
                 << "ALTER " << alter_statement_kind << " does not support "
                 << action->GetSQLForAlterAction();
        }
        std::unique_ptr<const ResolvedAlterAction> resolved_alter_action;
        const auto* collate_clause = action->GetAsOrDie<ASTSetCollateClause>();
        ZETASQL_RETURN_IF_ERROR(
            ResolveSetCollateClause(collate_clause, &resolved_alter_action));
        alter_actions->push_back(std::move(resolved_alter_action));
      } break;
      default:
        return MakeSqlErrorAt(action)
               << "ALTER " << alter_statement_kind << " does not support "
               << action->GetSQLForAlterAction();
    }
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterDatabaseStatement(
    const ASTAlterDatabaseStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  bool has_only_set_options_action = true;
  std::vector<std::unique_ptr<const ResolvedAlterAction>>
      resolved_alter_actions;
  ZETASQL_RETURN_IF_ERROR(ResolveAlterActions(ast_statement, "DATABASE", output,
                                      &has_only_set_options_action,
                                      &resolved_alter_actions));
  // path() should never be null here because DATABASE is a schema_object_kind
  // not a generic_entity_type.
  ZETASQL_RET_CHECK(ast_statement->path() != nullptr);
  *output = MakeResolvedAlterDatabaseStmt(
      ast_statement->path()->ToIdentifierVector(),
      std::move(resolved_alter_actions), ast_statement->is_if_exists());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterSchemaStatement(
    const ASTAlterSchemaStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  bool has_only_set_options_action = true;
  std::vector<std::unique_ptr<const ResolvedAlterAction>>
      resolved_alter_actions;
  ZETASQL_RETURN_IF_ERROR(ResolveAlterActions(ast_statement, "SCHEMA", output,
                                      &has_only_set_options_action,
                                      &resolved_alter_actions));

  // path() should never be null here because SCHEMA is a schema_object_kind
  // not a generic_entity_type.
  ZETASQL_RET_CHECK(ast_statement->path() != nullptr);
  *output = MakeResolvedAlterSchemaStmt(
      ast_statement->path()->ToIdentifierVector(),
      std::move(resolved_alter_actions), ast_statement->is_if_exists());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterTableStatement(
    const ASTAlterTableStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  bool has_only_set_options_action = true;
  std::vector<std::unique_ptr<const ResolvedAlterAction>>
      resolved_alter_actions;
  ZETASQL_RETURN_IF_ERROR(ResolveAlterActions(ast_statement, "TABLE", output,
                                      &has_only_set_options_action,
                                      &resolved_alter_actions));

  // path() should never be null here because TABLE is a
  // table_or_table_function not a generic_entity_type.
  ZETASQL_RET_CHECK(ast_statement->path() != nullptr);
  std::unique_ptr<ResolvedAlterTableStmt> alter_statement =
      MakeResolvedAlterTableStmt(ast_statement->path()->ToIdentifierVector(),
                                 std::move(resolved_alter_actions),
                                 ast_statement->is_if_exists());

  // TODO: deprecate ResolvedAlterTableSetOptionsStmt
  // To support legacy code, form ResolvedAlterTableSetOptionsStmt here
  // if RESOLVED_ALTER_TABLE_SET_OPTIONS_STMT is enabled
  const bool legacy_support =
      language().SupportsStatementKind(RESOLVED_ALTER_TABLE_SET_OPTIONS_STMT);
  const bool alter_support =
      language().SupportsStatementKind(RESOLVED_ALTER_TABLE_STMT);
  if (has_only_set_options_action && legacy_support) {
    // Converts the action list with potentially multiple SET OPTIONS actions
    // to a single list of options.
    std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
    const ASTAlterActionList* action_list = ast_statement->action_list();
    for (const ASTAlterAction* const action : action_list->actions()) {
      ZETASQL_RETURN_IF_ERROR(ResolveOptionsList(
          action->GetAsOrDie<ASTSetOptionsAction>()->options_list(),
          &resolved_options));
    }
    *output = MakeResolvedAlterTableSetOptionsStmt(
        alter_statement->name_path(), std::move(resolved_options),
        ast_statement->is_if_exists());
  } else if (!has_only_set_options_action && legacy_support && !alter_support) {
    return MakeSqlErrorAt(ast_statement)
           << "ALTER TABLE supports only the SET OPTIONS action";
  } else if (!alter_support) {
    return MakeSqlErrorAt(ast_statement) << "ALTER TABLE is not supported";
  } else {
    *output = std::move(alter_statement);
  }
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAddColumnAction(
    IdString table_name_id_string, const Table* table,
    const ASTAddColumnAction* action, IdStringSetCase* new_columns,
    IdStringSetCase* columns_to_drop,
    std::unique_ptr<const ResolvedAlterAction>* alter_action) {
  ZETASQL_DCHECK(*alter_action == nullptr);

  const ASTColumnDefinition* column = action->column_definition();

  const IdString column_name = column->name()->GetAsIdString();
  if (!new_columns->insert(column_name).second) {
    return MakeSqlErrorAt(action->column_definition()->name())
           << "Duplicate column name " << column_name
           << " in ALTER TABLE ADD COLUMN";
  }

  // Check that ASTAddColumnAction does not contain various fields for which we
  // don't have corresponding properties in ResolvedAlterAction yet.
  // TODO: add corresponding properties and support.
  if (action->fill_expression() != nullptr) {
    return MakeSqlErrorAt(action->fill_expression())
           << "ALTER TABLE ADD COLUMN with FILL USING is not supported yet";
  }
  if (column->schema()->generated_column_info() != nullptr) {
    return MakeSqlErrorAt(action->column_definition()->name())
           << "ALTER TABLE ADD COLUMN does not support generated columns yet";
  }
  if (column->schema()->ContainsAttribute(AST_PRIMARY_KEY_COLUMN_ATTRIBUTE)) {
    return MakeSqlErrorAt(action->column_definition()->name())
           << "ALTER TABLE ADD COLUMN does not support primary key attribute"
           << " (column: " << column_name << ")";
  }
  if (column->schema()->ContainsAttribute(AST_FOREIGN_KEY_COLUMN_ATTRIBUTE)) {
    return MakeSqlErrorAt(action->column_definition()->name())
           << "ALTER TABLE ADD COLUMN does not support foreign key attribute"
           << " (column: " << column_name << ")";
  }
  if (action->column_position() != nullptr) {
    return MakeSqlErrorAt(action->column_position())
           << "ALTER TABLE ADD COLUMN with column position is not supported"
           << " (column: " << column_name << ")";
  }
  // Check the column does not exist, unless it was just deleted by DROP COLUMN.
  if (table != nullptr && !action->is_if_not_exists() &&
      columns_to_drop->find(column_name) == columns_to_drop->end()) {
    if (table->FindColumnByName(column_name.ToString()) != nullptr) {
      return MakeSqlErrorAt(action->column_definition()->name())
             << "Column already exists: " << column_name;
    }
  }

  NameList column_name_list;
  std::unique_ptr<const ResolvedColumnDefinition> column_definition;
  if (column->schema()->default_expression() != nullptr) {
    // Collect all active columns, include existing and new columns, minus
    // dropped columns.
    std::vector<IdString> all_column_names;
    for (int i = 0; i < table->NumColumns(); ++i) {
      const IdString column_name = MakeIdString(table->GetColumn(i)->Name());
      if (columns_to_drop->find(column_name) == columns_to_drop->end()) {
        all_column_names.push_back(column_name);
      }
    }
    for (const IdString new_column_name : *new_columns) {
      if (columns_to_drop->find(new_column_name) == columns_to_drop->end()) {
        all_column_names.push_back(new_column_name);
      }
    }
    // Add all column names to name scope with access error, so default value
    // can't reference these columns.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<NameScope> access_error_name_scope,
                     CreateNameScopeWithAccessErrorForDefaultExpr(
                         table_name_id_string, all_column_names));
    // Set default_expr_access_error_name_scope_ activates all default value
    // validation logic, which relies on the existence of this variable.
    zetasql_base::VarSetter<absl::optional<const NameScope*>> var_setter(
        &default_expr_access_error_name_scope_, access_error_name_scope.get());
    // We can't move ResolveColumnDefinitionNoCache() out of the block, because
    // default_expr_access_error_name_scope_ is only scoped in the block.
    ZETASQL_ASSIGN_OR_RETURN(column_definition,
                     ResolveColumnDefinitionNoCache(
                         column, table_name_id_string, &column_name_list));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(column_definition,
                     ResolveColumnDefinitionNoCache(
                         column, table_name_id_string, &column_name_list));
  }

  *alter_action = MakeResolvedAddColumnAction(action->is_if_not_exists(),
                                              std::move(column_definition));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDropColumnAction(
    const Table* table, const ASTDropColumnAction* action,
    IdStringSetCase* new_columns, IdStringSetCase* columns_to_drop,
    std::unique_ptr<const ResolvedAlterAction>* alter_action) {
  ZETASQL_DCHECK(*alter_action == nullptr);

  const IdString column_name = action->column_name()->GetAsIdString();
  if (!columns_to_drop->insert(column_name).second) {
    return MakeSqlErrorAt(action->column_name())
           << "ALTER TABLE DROP COLUMN cannot drop column " << column_name
           << " multiple times";
  }
  if (new_columns->find(column_name) != new_columns->end()) {
    return MakeSqlErrorAt(action->column_name())
           << "Column " << column_name
           << " cannot be added and dropped by the same ALTER TABLE statement";
  }

  // If the table is present, verify that the column exists and can be dropped.
  if (table != nullptr) {
    const Column* column = table->FindColumnByName(column_name.ToString());
    if (column == nullptr && !action->is_if_exists()) {
      return MakeSqlErrorAt(action->column_name())
             << "Column not found: " << column_name;
    }
    if (column != nullptr && column->IsPseudoColumn()) {
      return MakeSqlErrorAt(action->column_name())
             << "ALTER TABLE DROP COLUMN cannot drop pseudo-column "
             << column_name;
    }
  }

  *alter_action = MakeResolvedDropColumnAction(action->is_if_exists(),
                                               column_name.ToString());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveRenameColumnAction(
    const Table* table, const ASTRenameColumnAction* action,
    IdStringSetCase* columns_to_rename,
    IdStringHashMapCase<IdString>* columns_rename_map,
    std::unique_ptr<const ResolvedAlterAction>* alter_action) {
  ZETASQL_RET_CHECK(*alter_action == nullptr);

  const IdString new_column_name = action->new_column_name()->GetAsIdString();
  if (zetasql_base::ContainsKey(*columns_rename_map, new_column_name)) {
    return MakeSqlErrorAt(action->new_column_name())
           << "Another column was renamed to " << new_column_name
           << " in a previous command of the same ALTER TABLE statement";
  }
  // Check the new column name does not exist, unless it was renamed by RENAME
  // COLUMN.
  if (table != nullptr &&
      !zetasql_base::ContainsKey(*columns_to_rename, new_column_name) &&
      table->FindColumnByName(new_column_name.ToString()) != nullptr) {
    return MakeSqlErrorAt(action->new_column_name())
           << "Column already exists: " << new_column_name;
  }

  // Check the existence of old column name:
  // 1. Check if there is a column renamed to this old column name.
  // 2. Check if the column has been renamed in previous actions of the same
  // statement.
  // 3. Check if the column name exists in the table.
  const IdString old_column_name = action->column_name()->GetAsIdString();
  if (table != nullptr) {
    if (zetasql_base::ContainsKey(*columns_rename_map, old_column_name)) {
      // This case is renaming a column that was already renamed, e.g., RENAME a
      // TO b, RENAME b TO c. In this case, we replace key-value pair
      // {old_column_name, catalog_column_name} with {new_column_name,
      // catalog_column_name} in columns_rename_map. Here catalog_column_name
      // represents the actual column name in table before the ALTER statement
      // happens.
      const IdString catalog_column_name =
          (*columns_rename_map)[old_column_name];
      columns_rename_map->erase(old_column_name);
      columns_rename_map->insert({new_column_name, catalog_column_name});
    } else if (zetasql_base::ContainsKey(*columns_to_rename, old_column_name)) {
      // This case is renaming a table column that has already been renamed,
      // e.g., RENAME a TO b, RENAME a TO c
      return MakeSqlErrorAt(action->column_name())
             << "Column " << old_column_name
             << " has been renamed in a previous alter action";
    } else {
      // Verify that the old column exists and can be renamed.
      const Column* column =
          table->FindColumnByName(old_column_name.ToString());
      if (column == nullptr && !action->is_if_exists()) {
        return MakeSqlErrorAt(action->column_name())
               << "ALTER TABLE RENAME COLUMN not found: " << old_column_name;
      }
      if (column != nullptr && column->IsPseudoColumn()) {
        return MakeSqlErrorAt(action->column_name())
               << "ALTER TABLE RENAME COLUMN cannot rename pseudo-column "
               << old_column_name;
      }
      columns_rename_map->insert({new_column_name, old_column_name});
      columns_to_rename->insert(old_column_name);
    }
  }

  *alter_action = MakeResolvedRenameColumnAction(action->is_if_exists(),
                                                 old_column_name.ToString(),
                                                 new_column_name.ToString());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterColumnTypeAction(
    IdString table_name_id_string, const Table* table,
    const ASTAlterColumnTypeAction* action,
    std::unique_ptr<const ResolvedAlterAction>* alter_action) {
  ZETASQL_RET_CHECK(*alter_action == nullptr);

  const IdString column_name = action->column_name()->GetAsIdString();

  std::unique_ptr<ResolvedColumnRef> column_reference;
  const Type* resolved_type = nullptr;
  TypeParameters type_parameters;
  std::unique_ptr<const ResolvedColumnAnnotations> annotations;

  if (table != nullptr) {
    const Column* column = table->FindColumnByName(column_name.ToString());
    if (column == nullptr && !action->is_if_exists()) {
      return MakeSqlErrorAt(action) << "Column not found: " << column_name;
    }
    if (column != nullptr && column->IsPseudoColumn()) {
      return MakeSqlErrorAt(action->column_name())
             << "ALTER TABLE ALTER COLUMN SET DATA TYPE cannot change the type "
             << "of pseudo-column " << column_name;
    }
    if (column != nullptr) {
      const ResolvedColumn resolved_column(AllocateColumnId(),
                                           table_name_id_string, column_name,
                                           column->GetType());
      column_reference = MakeColumnRef(resolved_column);
    }

    NameList column_name_list;
    std::unique_ptr<ResolvedGeneratedColumnInfo> generated_column_info;
    std::unique_ptr<ResolvedColumnDefaultValue> column_default_value;

    ZETASQL_RETURN_IF_ERROR(
        ResolveColumnSchema(action->schema(), column_name_list, &resolved_type,
                            &annotations, &generated_column_info,
                            &column_default_value));

    // Parser already made sure SET DATA TYPE won't have generated column, or
    // default value expression. That's why we throw ZETASQL_RET_CHECK here.
    ZETASQL_RET_CHECK(generated_column_info == nullptr);
    ZETASQL_RET_CHECK(column_default_value == nullptr);

    if (annotations != nullptr) {
      // OPTIONS not allowed.
      if (OptionsPresent(annotations.get())) {
        return MakeSqlErrorAt(action->schema())
               << "For ALTER TABLE ALTER COLUMN SET DATA TYPE, the updated "
               << "data type cannot contain OPTIONS";
      }
      // NOT NULL not allowed.
      if (NotNullPresent(annotations.get())) {
        return MakeSqlErrorAt(action->schema())
               << "For ALTER TABLE ALTER COLUMN SET DATA TYPE, the updated "
               << "data type cannot contain NOT NULL";
      }

      ZETASQL_ASSIGN_OR_RETURN(type_parameters,
                       annotations->GetFullTypeParameters(resolved_type));
    }
  }

  SignatureMatchResult result;
  if (column_reference != nullptr) {
    const Type* existing_type = column_reference->column().type();

    // TODO: Check CONVERT USING expression when parser adds it.
    // Note that we cannot check that a NUMERIC(P,S) column can't increase S by
    // more than P was increased because the type parameters aren't in the
    // catalog.
    if (!coercer_.AssignableTo(InputArgumentType(existing_type), resolved_type,
                               /*is_explicit=*/false, &result)) {
      return MakeSqlErrorAt(action)
             << "ALTER TABLE ALTER COLUMN SET DATA TYPE "
             << "requires that the existing column type ("
             << column_reference->column().type()->TypeName(
                    language().product_mode())
             << ") is assignable to the new type ("
             << resolved_type->ShortTypeName(language().product_mode()) << ")";
    }
  }

  *alter_action = MakeResolvedAlterColumnSetDataTypeAction(
      action->is_if_exists(), column_name.ToString(), resolved_type,
      type_parameters, std::move(annotations));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterColumnOptionsAction(
    const Table* table, const ASTAlterColumnOptionsAction* action,
    std::unique_ptr<const ResolvedAlterAction>* alter_action) {
  ZETASQL_RET_CHECK(*alter_action == nullptr);
  const IdString column_name = action->column_name()->GetAsIdString();
  // If the table is present, verify that the column exists and can be modified.
  if (table != nullptr) {
    const Column* column = table->FindColumnByName(column_name.ToString());
    if (column == nullptr) {
      if (action->is_if_exists()) {
        // Silently ignore the NOT FOUND error since this is a ALTER COLUMN IF
        // EXISTS action.
      } else {
        return MakeSqlErrorAt(action->column_name())
               << "Column not found: " << column_name;
      }
    } else if (column->IsPseudoColumn()) {
      return MakeSqlErrorAt(action->column_name())
             << "ALTER COLUMN SET OPTIONS not supported "
             << "for pseudo-column " << column_name;
    }
  }
  std::vector<std::unique_ptr<const ResolvedOption>> resolved_options;
  ZETASQL_RETURN_IF_ERROR(
      ResolveOptionsList(action->options_list(), &resolved_options));
  *alter_action = MakeResolvedAlterColumnOptionsAction(
      action->is_if_exists(), column_name.ToString(),
      std::move(resolved_options));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterColumnDropNotNullAction(
    const Table* table, const ASTAlterColumnDropNotNullAction* action,
    std::unique_ptr<const ResolvedAlterAction>* alter_action) {
  ZETASQL_RET_CHECK(*alter_action == nullptr);
  const IdString column_name = action->column_name()->GetAsIdString();
  std::unique_ptr<ResolvedColumnRef> column_reference;
  // If the table is present, verify that the column exists and can be modified.
  if (table != nullptr) {
    const Column* column = table->FindColumnByName(column_name.ToString());
    if (column == nullptr) {
      if (action->is_if_exists()) {
        // Silently ignore the NOT FOUND error since this is a ALTER COLUMN IF
        // EXISTS action.
      } else {
        return MakeSqlErrorAt(action->column_name())
               << "Column not found: " << column_name;
      }
    } else if (column->IsPseudoColumn()) {
      return MakeSqlErrorAt(action->column_name())
             << "ALTER COLUMN DROP NOT NULL not supported for pseudo-column "
             << column_name;
    }
  }
  *alter_action = MakeResolvedAlterColumnDropNotNullAction(
      action->is_if_exists(), column_name.ToString());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterColumnDropDefaultAction(
    const Table* table, const ASTAlterColumnDropDefaultAction* action,
    std::unique_ptr<const ResolvedAlterAction>* alter_action) {
  ZETASQL_RET_CHECK(*alter_action == nullptr);
  const IdString column_name = action->column_name()->GetAsIdString();
  std::unique_ptr<ResolvedColumnRef> column_reference;
  // If the table is present, verify that the column exists and can be modified.
  if (table != nullptr) {
    const Column* column = table->FindColumnByName(column_name.ToString());
    if (column == nullptr) {
      if (action->is_if_exists()) {
        // Silently ignore the NOT FOUND error since this is a ALTER COLUMN IF
        // EXISTS action.
      } else {
        return MakeSqlErrorAt(action->column_name())
               << "Column not found: " << column_name;
      }
    } else if (column->IsPseudoColumn()) {
      return MakeSqlErrorAt(action->column_name())
             << "ALTER COLUMN DROP DEFAULT is not supported for pseudo-column "
             << column_name;
    }
  }
  *alter_action = MakeResolvedAlterColumnDropDefaultAction(
      action->is_if_exists(), column_name.ToString());
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterEntityStatement(
    const ASTAlterEntityStatement* ast_statement,
    std::unique_ptr<ResolvedStatement>* output) {
  bool has_only_set_options_action = true;
  std::vector<std::unique_ptr<const ResolvedAlterAction>>
      resolved_alter_actions;
  ZETASQL_RETURN_IF_ERROR(ResolveAlterActions(
      ast_statement, ast_statement->type()->GetAsString(), output,
      &has_only_set_options_action, &resolved_alter_actions));
  if (ast_statement->path() == nullptr) {
    *output = MakeResolvedAlterEntityStmt(std::move(resolved_alter_actions),
                                          ast_statement->is_if_exists(),
                                          ast_statement->type()->GetAsString());
  } else {
    *output = MakeResolvedAlterEntityStmt(
        ast_statement->path()->ToIdentifierVector(),
        std::move(resolved_alter_actions), ast_statement->is_if_exists(),
        ast_statement->type()->GetAsString());
  }

  return absl::OkStatus();
}

absl::Status Resolver::ResolveAddForeignKey(
    const Table* referencing_table, const ASTAlterStatementBase* alter_stmt,
    const ASTAddConstraintAction* alter_action,
    std::unique_ptr<const ResolvedAddConstraintAction>* resolved_alter_action) {
  if (!language().LanguageFeatureEnabled(FEATURE_FOREIGN_KEYS)) {
    return MakeSqlErrorAt(alter_action) << "FOREIGN KEY is not supported";
  }

  // <referencing_table> may be null if the target table does not exist. In
  // that case, we return an error for ALTER TABLE and optimistically assume
  // schemas match for ALTER TABLE IF EXISTS.

  // The caller should have already verified this for us.
  ZETASQL_RET_CHECK(referencing_table != nullptr || alter_stmt->is_if_exists());

  const ASTForeignKey* foreign_key =
      alter_action->constraint()->GetAsOrDie<ASTForeignKey>();

  ColumnIndexMap column_indexes;
  std::vector<const Type*> column_types;
  if (referencing_table != nullptr) {
    for (int i = 0; i < referencing_table->NumColumns(); i++) {
      const Column* column = referencing_table->GetColumn(i);
      ZETASQL_RET_CHECK(column != nullptr);
      column_indexes[id_string_pool_->Make(column->Name())] = i;
      column_types.push_back(column->GetType());
    }
  } else {
    // If the referencing table does not exist, then we use the referenced
    // columns' types. We also include the referencing columns' names in the
    // resolved node so that SQL builders can reconstruct the original SQL.
    const Table* referenced_table;
    ZETASQL_RETURN_IF_ERROR(
        FindTable(foreign_key->reference()->table_name(), &referenced_table));
    for (const ASTIdentifier* column_name :
         foreign_key->reference()->column_list()->identifiers()) {
      const Column* column =
          referenced_table->FindColumnByName(column_name->GetAsString());
      if (column == nullptr) {
        return MakeSqlErrorAt(column_name)
               << "Column " << column_name->GetAsString()
               << " not found in table " << referenced_table->Name();
      }
      column_types.push_back(column->GetType());
    }

    // Column indexes for referencing columns are fake and assigned based on
    // their appearance in the constraint DDL.
    for (int i = 0; i < foreign_key->column_list()->identifiers().size(); i++) {
      const ASTIdentifier* referencing_column =
          foreign_key->column_list()->identifiers().at(i);
      column_indexes.insert({referencing_column->GetAsIdString(), i});
    }
  }

  std::vector<std::unique_ptr<ResolvedForeignKey>> foreign_keys;
  ZETASQL_RETURN_IF_ERROR(ResolveForeignKeyTableConstraint(column_indexes, column_types,
                                                   foreign_key, &foreign_keys));
  ZETASQL_RET_CHECK(foreign_keys.size() == 1);
  *resolved_alter_action = MakeResolvedAddConstraintAction(
      alter_action->is_if_not_exists(), std::move(foreign_keys[0]),
      referencing_table);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAddPrimaryKey(
    const Table* target_table, const ASTAlterStatementBase* alter_stmt,
    const ASTAddConstraintAction* alter_action,
    std::unique_ptr<const ResolvedAddConstraintAction>* resolved_alter_action) {
  if (target_table == nullptr) {
    if (language().LanguageFeatureEnabled(
            FEATURE_ALLOW_MISSING_PATH_EXPRESSION_IN_ALTER_DDL)) {
      // This could be valid user input. There's nothing useful we can do
      // though, so just generate a syntax error.
      return MakeSqlErrorAt(alter_action)
             << "A path_expression is required for ADD PRIMARY KEY actions";
    } else {  // Maintain old behavior.
      // The parser should have already verified this for us. We either have a
      // table or the action uses IF EXISTS.
      ZETASQL_RET_CHECK(target_table != nullptr || alter_stmt->is_if_exists());
    }
  }
  const ASTPrimaryKey* ast_primary_key =
      alter_action->constraint()->GetAsOrDie<ASTPrimaryKey>();

  ColumnIndexMap column_indexes;
  if (target_table != nullptr) {
    for (int i = 0; i < target_table->NumColumns(); i++) {
      const Column* column = target_table->GetColumn(i);
      ZETASQL_RET_CHECK(column != nullptr);
      if (!column->IsPseudoColumn()) {
        column_indexes[id_string_pool_->Make(column->Name())] = i;
      }
    }
  }

  std::unique_ptr<ResolvedPrimaryKey> primary_key;
  ZETASQL_RETURN_IF_ERROR(
      ResolvePrimaryKey(column_indexes, ast_primary_key, &primary_key));

  *resolved_alter_action = MakeResolvedAddConstraintAction(
      alter_action->is_if_not_exists(), std::move(primary_key), target_table);
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAddConstraintAction(
    const Table* referencing_table, const ASTAlterStatementBase* alter_stmt,
    const ASTAddConstraintAction* alter_action,
    std::unique_ptr<const ResolvedAddConstraintAction>* resolved_alter_action) {
  auto constraint_kind = alter_action->constraint()->node_kind();
  if (constraint_kind == AST_CHECK_CONSTRAINT &&
      !language().LanguageFeatureEnabled(FEATURE_CHECK_CONSTRAINT)) {
    return MakeSqlErrorAt(alter_action) << "CHECK CONSTRAINT is not supported";
  } else if (constraint_kind == AST_FOREIGN_KEY) {
    return ResolveAddForeignKey(referencing_table, alter_stmt, alter_action,
                                resolved_alter_action);
  } else if (constraint_kind == AST_PRIMARY_KEY) {
    return ResolveAddPrimaryKey(referencing_table, alter_stmt, alter_action,
                                resolved_alter_action);
  }

  return MakeSqlErrorAt(alter_action)
         << "ALTER TABLE ADD CONSTRAINT is not implemented";
}
absl::Status Resolver::ResolveSetCollateClause(
    const ASTSetCollateClause* action,
    std::unique_ptr<const ResolvedAlterAction>* alter_action) {
  std::unique_ptr<const ResolvedExpr> resolved_collation;
  ZETASQL_RETURN_IF_ERROR(ValidateAndResolveDefaultCollate(
      action->collate(), action->collate(), &resolved_collation));
  *alter_action = MakeResolvedSetCollateClause(std::move(resolved_collation));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveAlterColumnSetDefaultAction(
    IdString table_name_id_string, const Table* table,
    const ASTAlterColumnSetDefaultAction* action,
    std::unique_ptr<const ResolvedAlterAction>* alter_action) {
  ZETASQL_RET_CHECK(*alter_action == nullptr);
  const IdString column_name = action->column_name()->GetAsIdString();
  const Type* column_type = nullptr;
  const Column* column = nullptr;
  bool skip_type_match_check = false;
  std::vector<IdString> column_names;
  // If the table is present, verify that the column exists and can be modified.
  // Also collects existing column information.
  if (table != nullptr) {
    column = table->FindColumnByName(column_name.ToString());
    if (column == nullptr) {
      if (action->is_if_exists()) {
        // Silently ignore the NOT FOUND error since this is a ALTER COLUMN IF
        // EXISTS action.
        skip_type_match_check = true;
      } else {
        return MakeSqlErrorAt(action->column_name())
               << "Column not found: " << column_name;
      }
    } else {
      if (column->IsPseudoColumn()) {
        return MakeSqlErrorAt(action->column_name())
               << "ALTER COLUMN SET DEFAULT is not supported "
               << "for pseudo-column " << column_name;
      }
      column_type = column->GetType();
    }

    // Collect all columns in the table.
    for (int i = 0; i < table->NumColumns(); ++i) {
      const IdString column_name = MakeIdString(table->GetColumn(i)->Name());
      column_names.push_back(column_name);
    }
  } else {
    skip_type_match_check = true;
  }

  std::unique_ptr<ResolvedColumnDefaultValue> resolved_default_value;
  // Add all column names to name scope with access error, so default value
  // can't reference these columns.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<NameScope> access_error_name_scope,
                   CreateNameScopeWithAccessErrorForDefaultExpr(
                       table_name_id_string, column_names));
  // Set default_expr_access_error_name_scope_ activates all default value
  // validation logic, which relies on the existence of this variable.
  zetasql_base::VarSetter<absl::optional<const NameScope*>> var_setter(
      &default_expr_access_error_name_scope_, access_error_name_scope.get());

  ZETASQL_RETURN_IF_ERROR(ResolveColumnDefaultExpression(
      action->default_expression(), column_type, skip_type_match_check,
      &resolved_default_value));

  *alter_action = MakeResolvedAlterColumnSetDefaultAction(
      action->is_if_exists(), column_name.ToString(),
      std::move(resolved_default_value));

  return absl::OkStatus();
}

}  // namespace zetasql
