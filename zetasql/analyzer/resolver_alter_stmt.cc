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

// This file contains the implementation of ALTER TABLE-related resolver
// methods from resolver.h.
#include <map>
#include <memory>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/id_string.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

absl::Status Resolver::ResolveAlterActions(
    const ASTAlterStatementBase* ast_statement,
    absl::string_view alter_statement_kind,
    std::unique_ptr<ResolvedStatement>* output,
    bool* has_only_set_options_action,
    std::vector<std::unique_ptr<const ResolvedAlterAction>>* alter_actions) {
  ZETASQL_RET_CHECK(ast_statement->path() != nullptr);
  const IdString table_name_id_string =
      MakeIdString(ast_statement->path()->ToIdentifierPathString());

  IdStringSetCase new_columns, column_to_drop;
  const Table* altered_table = nullptr;
  // We keep status, but don't fail unless we have ADD/DROP.
  absl::Status table_status = FindTable(ast_statement->path(), &altered_table);

  *has_only_set_options_action = true;
  const ASTAlterActionList* action_list = ast_statement->action_list();
  for (const ASTAlterAction* const action : action_list->actions()) {
    if (action->node_kind() != AST_SET_OPTIONS_ACTION) {
      *has_only_set_options_action = false;
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
      case AST_ADD_CONSTRAINT_ACTION: {
        auto constraint_kind = action->GetAsOrDie<ASTAddConstraintAction>()
                                   ->constraint()
                                   ->node_kind();
        if (constraint_kind == AST_CHECK_CONSTRAINT &&
            !language().LanguageFeatureEnabled(FEATURE_CHECK_CONSTRAINT)) {
          return MakeSqlErrorAt(action) << "CHECK CONSTRAINT is not supported";
        }
        if (constraint_kind == AST_FOREIGN_KEY &&
            !language().LanguageFeatureEnabled(FEATURE_FOREIGN_KEYS)) {
          return MakeSqlErrorAt(action) << "FOREIGN KEY is not supported";
        }
        return MakeSqlErrorAt(action)
               << "ALTER TABLE ADD CONSTRAINT is not implemented";
      }
      case AST_DROP_CONSTRAINT_ACTION:
        return MakeSqlErrorAt(action) << "DROP CONSTRAINT is not supported";
      case AST_ALTER_CONSTRAINT_ENFORCEMENT_ACTION:
        return MakeSqlErrorAt(action)
               << "ALTER CONSTRAINT ENFORCED/NOT ENFORCED is not supported";
      case AST_ALTER_CONSTRAINT_SET_OPTIONS_ACTION:
        return MakeSqlErrorAt(action)
               << "ALTER CONSTRAINT SET OPTIONS is not supported";
      case AST_ADD_COLUMN_ACTION:
      case AST_DROP_COLUMN_ACTION: {
        if (ast_statement->node_kind() != AST_ALTER_TABLE_STATEMENT) {
          // Views, models, etc don't support ADD/DROP columns.
          return MakeSqlErrorAt(action)
                 << "ALTER " << alter_statement_kind << " does not support "
                 << action->GetSQLForAlterAction();
        }
        if (!ast_statement->is_if_exists()) {
          ZETASQL_RETURN_IF_ERROR(table_status);
        }
        std::unique_ptr<const ResolvedAlterAction> resolved_action;
        if (action->node_kind() == AST_ADD_COLUMN_ACTION) {
          ZETASQL_RETURN_IF_ERROR(ResolveAddColumnAction(
              table_name_id_string, altered_table,
              action->GetAsOrDie<ASTAddColumnAction>(), &new_columns,
              &column_to_drop, &resolved_action));
        } else {
          ZETASQL_RETURN_IF_ERROR(ResolveDropColumnAction(
              table_name_id_string, altered_table,
              action->GetAsOrDie<ASTDropColumnAction>(), &new_columns,
              &column_to_drop, &resolved_action));
        }
        alter_actions->push_back(std::move(resolved_action));
      } break;
      default:
        return MakeSqlErrorAt(action)
               << "ALTER " << alter_statement_kind << " doesn't support "
               << action->GetNodeKindString() << " action.";
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
  *output = MakeResolvedAlterDatabaseStmt(
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
  DCHECK(*alter_action == nullptr);

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
      return MakeSqlErrorAt(action) << "Column already exists: " << column_name;
    }
  }

  NameList column_name_list;
  // We don't support fill expression, so can use cheaper method
  // ResolveColumnDefinitionNoCache to resolve columns.
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedColumnDefinition> column_definition,
      ResolveColumnDefinitionNoCache(column, table_name_id_string,
                                     &column_name_list));

  *alter_action = MakeResolvedAddColumnAction(action->is_if_not_exists(),
                                              std::move(column_definition));
  return absl::OkStatus();
}

absl::Status Resolver::ResolveDropColumnAction(
    IdString table_name_id_string, const Table* table,
    const ASTDropColumnAction* action, IdStringSetCase* new_columns,
    IdStringSetCase* columns_to_drop,
    std::unique_ptr<const ResolvedAlterAction>* alter_action) {
  DCHECK(*alter_action == nullptr);

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

  std::unique_ptr<ResolvedColumnRef> column_reference;
  if (table != nullptr) {
    const Column* column = table->FindColumnByName(column_name.ToString());
    if (column == nullptr && !action->is_if_exists()) {
      return MakeSqlErrorAt(action) << "Column not found: " << column_name;
    }
    if (column != nullptr && column->IsPseudoColumn()) {
      return MakeSqlErrorAt(action) << "ALTER TABLE DROP COLUMN cannot drop "
                                    << "pseudo-column " << column_name;
    }
    if (column != nullptr) {
      const ResolvedColumn resolved_column(AllocateColumnId(),
                                           table_name_id_string, column_name,
                                           column->GetType());
      column_reference =
          MakeResolvedColumnRef(resolved_column.type(), resolved_column, false);
    }
  }

  *alter_action = MakeResolvedDropColumnAction(action->is_if_exists(),
                                               column_name.ToString(),
                                               std::move(column_reference));
  return absl::OkStatus();
}

}  // namespace zetasql
