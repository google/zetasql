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

#include "zetasql/public/parse_helpers.h"

#include <memory>

#include "zetasql/base/logging.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"

namespace zetasql {

zetasql_base::Status IsValidStatementSyntax(absl::string_view sql,
                                    ErrorMessageMode error_message_mode) {
  std::unique_ptr<ParserOutput> parser_output;
  // Nothing in ParserOptions affects syntax, so use the default ParserOptions.
  const zetasql_base::Status parse_status =
      ParseStatement(sql, ParserOptions(), &parser_output);
  return MaybeUpdateErrorFromPayload(error_message_mode, sql, parse_status);
}

zetasql_base::Status IsValidNextStatementSyntax(ParseResumeLocation* resume_location,
                                        ErrorMessageMode error_message_mode,
                                        bool* at_end_of_input) {
  std::unique_ptr<ParserOutput> parser_output;
  // Nothing in ParserOptions affects syntax, so use the default ParserOptions.
  const zetasql_base::Status parse_status = ParseNextStatement(
      resume_location, ParserOptions(), &parser_output, at_end_of_input);
  return MaybeUpdateErrorFromPayload(error_message_mode,
                                     resume_location->input(), parse_status);
}

ResolvedNodeKind GetStatementKind(const std::string& input) {
  return GetNextStatementKind(ParseResumeLocation::FromStringView(input));
}

ResolvedNodeKind GetStatementKind(ASTNodeKind node_kind) {
  switch (node_kind) {
    case AST_QUERY_STATEMENT:
      return RESOLVED_QUERY_STMT;
    case AST_EXPLAIN_STATEMENT:
      return RESOLVED_EXPLAIN_STMT;
    case AST_EXPORT_DATA_STATEMENT:
      return RESOLVED_EXPORT_DATA_STMT;
    case AST_CALL_STATEMENT:
      return RESOLVED_CALL_STMT;
    case AST_CREATE_CONSTANT_STATEMENT:
      return RESOLVED_CREATE_CONSTANT_STMT;
    case AST_CREATE_DATABASE_STATEMENT:
      return RESOLVED_CREATE_DATABASE_STMT;
    case AST_CREATE_FUNCTION_STATEMENT:
      return RESOLVED_CREATE_FUNCTION_STMT;
    case AST_CREATE_INDEX_STATEMENT:
      return RESOLVED_CREATE_INDEX_STMT;
    case AST_CREATE_MODEL_STATEMENT:
      return RESOLVED_CREATE_MODEL_STMT;
    case AST_CREATE_TABLE_FUNCTION_STATEMENT:
      return RESOLVED_CREATE_TABLE_FUNCTION_STMT;
    case AST_CREATE_PROCEDURE_STATEMENT:
      return RESOLVED_CREATE_PROCEDURE_STMT;
    case AST_CREATE_VIEW_STATEMENT:
      return RESOLVED_CREATE_VIEW_STMT;
    case AST_CREATE_MATERIALIZED_VIEW_STATEMENT:
      return RESOLVED_CREATE_MATERIALIZED_VIEW_STMT;
    case AST_CREATE_TABLE_STATEMENT:
      return RESOLVED_CREATE_TABLE_STMT;
    case AST_CREATE_EXTERNAL_TABLE_STATEMENT:
      return RESOLVED_CREATE_EXTERNAL_TABLE_STMT;
    case AST_CREATE_ROW_ACCESS_POLICY_STATEMENT:
      return RESOLVED_CREATE_ROW_ACCESS_POLICY_STMT;
    case AST_DEFINE_TABLE_STATEMENT:
      return RESOLVED_DEFINE_TABLE_STMT;
    case AST_DELETE_STATEMENT:
      return RESOLVED_DELETE_STMT;
    case AST_INSERT_STATEMENT:
      return RESOLVED_INSERT_STMT;
    case AST_UPDATE_STATEMENT:
      return RESOLVED_UPDATE_STMT;
    case AST_MERGE_STATEMENT:
      return RESOLVED_MERGE_STMT;
    case AST_TRUNCATE_STATEMENT:
      return RESOLVED_TRUNCATE_STMT;
    case AST_DESCRIBE_STATEMENT:
      return RESOLVED_DESCRIBE_STMT;
    case AST_SHOW_STATEMENT:
      return RESOLVED_SHOW_STMT;
    case AST_BEGIN_STATEMENT:
      return RESOLVED_BEGIN_STMT;
    case AST_SET_TRANSACTION_STATEMENT:
      return RESOLVED_SET_TRANSACTION_STMT;
    case AST_COMMIT_STATEMENT:
      return RESOLVED_COMMIT_STMT;
    case AST_ROLLBACK_STATEMENT:
      return RESOLVED_ROLLBACK_STMT;
    case AST_START_BATCH_STATEMENT:
      return RESOLVED_START_BATCH_STMT;
    case AST_RUN_BATCH_STATEMENT:
      return RESOLVED_RUN_BATCH_STMT;
    case AST_ABORT_BATCH_STATEMENT:
      return RESOLVED_ABORT_BATCH_STMT;
    case AST_DROP_STATEMENT:
      return RESOLVED_DROP_STMT;
    case AST_DROP_FUNCTION_STATEMENT:
      return RESOLVED_DROP_FUNCTION_STMT;
    case AST_DROP_ROW_ACCESS_POLICY_STATEMENT:
      return RESOLVED_DROP_ROW_ACCESS_POLICY_STMT;
    case AST_DROP_ALL_ROW_ACCESS_POLICIES_STATEMENT:
      return RESOLVED_DROP_ROW_ACCESS_POLICY_STMT;
    case AST_DROP_MATERIALIZED_VIEW_STATEMENT:
      return RESOLVED_DROP_MATERIALIZED_VIEW_STMT;
    case AST_GRANT_STATEMENT:
      return RESOLVED_GRANT_STMT;
    case AST_REVOKE_STATEMENT:
      return RESOLVED_REVOKE_STMT;
    case AST_ALTER_TABLE_STATEMENT:
      return RESOLVED_ALTER_TABLE_STMT;
    case AST_ALTER_VIEW_STATEMENT:
      return RESOLVED_ALTER_VIEW_STMT;
    case AST_ALTER_MATERIALIZED_VIEW_STATEMENT:
      return RESOLVED_ALTER_MATERIALIZED_VIEW_STMT;
    case AST_ALTER_ROW_POLICY_STATEMENT:
      return RESOLVED_ALTER_ROW_POLICY_STMT;
    case AST_RENAME_STATEMENT:
      return RESOLVED_RENAME_STMT;
    case AST_IMPORT_STATEMENT:
      return RESOLVED_IMPORT_STMT;
    case AST_MODULE_STATEMENT:
      return RESOLVED_MODULE_STMT;
    case AST_ASSERT_STATEMENT:
      return RESOLVED_ASSERT_STMT;
    default:
      break;
  }
  VLOG(1) << "Unrecognized parse node kind: "
          << ASTNode::NodeKindToString(node_kind);
  return RESOLVED_LITERAL;
}

ResolvedNodeKind GetNextStatementKind(
    const ParseResumeLocation& resume_location) {
  bool statement_is_ctas = false;
  ASTNodeKind node_kind =
      ParseNextStatementKind(resume_location, &statement_is_ctas);
  return statement_is_ctas ?
      RESOLVED_CREATE_TABLE_AS_SELECT_STMT : GetStatementKind(node_kind);
}

}  // namespace zetasql
