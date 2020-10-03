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

#ifndef ZETASQL_PARSER_STATEMENT_PROPERTIES_H_
#define ZETASQL_PARSER_STATEMENT_PROPERTIES_H_

#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"

namespace zetasql {
namespace parser {

struct ASTStatementProperties {
  // The parse node kind of the statement.
  ASTNodeKind node_kind = kUnknownASTNodeKind;

  // Whether or not the statement is CREATE TABLE AS SELECT.  Only applies
  // if <node_kind> is AST_CREATE_TABLE_STATEMENT.
  bool is_create_table_as_select = false;

  // The create scope of the statement (i.e. TEMP, DEFAULT, etc.).  Only
  // applies if <node_kind> is AST_CREATE_*.
  zetasql::ASTCreateStatement::Scope create_scope =
      zetasql::ASTCreateStatement::DEFAULT_SCOPE;

  // Statement level hints, if any.  Not owned.
  ASTNode* statement_level_hints = nullptr;
};

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_STATEMENT_PROPERTIES_H_
