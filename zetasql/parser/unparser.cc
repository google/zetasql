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

#include "zetasql/parser/unparser.h"

#include <ctype.h>

#include <set>
#include <utility>
#include <deque>

#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.h"
#include "zetasql/public/parse_location.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/match.h"
#include "absl/strings/strip.h"
#include "zetasql/base/map_util.h"

namespace zetasql {

std::string Unparse(const ASTNode* node) {
  std::string unparsed_;
  parser::Unparser unparser(&unparsed_);
  node->Accept(&unparser, nullptr);
  unparser.FlushLine();
  return unparsed_;
}

std::string UnparseWithComments(const ASTNode* node, std::deque<std::pair<std::string,
                                ParseLocationPoint>> parse_tokens) {
  std::string unparsed_;
  parser::Unparser unparser(&unparsed_);
  // Print comments by visitors and pop.
  node->Accept(&unparser, &parse_tokens);
  // Emit left comments in parse_tokens.
  for (const auto& parse_token : parse_tokens) {
    unparser.print(parse_token.first);
  }
  unparser.FlushLine();
  return unparsed_;
}

namespace parser {

// Formatter ---------------------------------------------------------
// Indent 2 spaces by default.
static const int kDefaultNumIndentSpaces = 2;
static const int kNumColumnLimit = 100;

void Formatter::Indent() {
  absl::StrAppend(&indentation_, std::string(kDefaultNumIndentSpaces, ' '));
}

void Formatter::Dedent() {
  CHECK_GE(indentation_.size(), kDefaultNumIndentSpaces)
      << "Impossible to dedent: has reached to the beginning of the line.";
  indentation_.resize(indentation_.size() - kDefaultNumIndentSpaces);
}

void Formatter::Format(absl::string_view s) {
  if (s.empty()) return;
  if (buffer_.empty()) {
    // This is treated the same as the case below when starting a new line.
    absl::StrAppend(&buffer_, indentation_, s);
    indentation_length_in_buffer_ = indentation_.size();
  } else {
    // Formats according to the last char in buffer_ and first char in s.
    char last_char = buffer_.back();
    switch (last_char) {
      case '\n':
        // Prepends indentation when starting a new line.
        absl::StrAppend(&buffer_, indentation_, s);
        indentation_length_in_buffer_ = indentation_.size();
        break;
      case '(':
      case '[':
      case '@':
      case '.':
      case '~':
      case ' ':
        // When seeing these characters, appends s directly.
        absl::StrAppend(&buffer_, s);
        break;
      default:
        {
          char curr_char = s[0];
          if (curr_char == '(') {
            // Inserts a space if last token is a separator, otherwise regards
            // it as a function call.
            if (LastTokenIsSeparator()) {
              absl::StrAppend(&buffer_, " ", s);
            } else {
              absl::StrAppend(&buffer_, s);
            }
          } else if (
              curr_char == ')' ||
              curr_char == '[' ||
              curr_char == ']' ||
              // To avoid case like "SELECT 1e10,.1e10".
              (curr_char == '.' && last_char != ',') ||
              curr_char == ',') {
            // If s starts with these characters, appends s directly.
            absl::StrAppend(&buffer_, s);
          } else {
            // By default, separate s from anything before with a space.
            absl::StrAppend(&buffer_, " ", s);
          }
          break;
        }
    }
  }

  if (buffer_.size() >= indentation_length_in_buffer_ + kNumColumnLimit &&
      LastTokenIsSeparator()) {
    FlushLine();
  }
}

void Formatter::FormatLine(absl::string_view s) {
  Format(s);
  FlushLine();
}

bool Formatter::LastTokenIsSeparator() {
  // These are keywords emitted in uppercase in Unparser, so don't need to make
  // them case insensitive.
  static const std::set<std::string>& kWordSperarator =
      *new std::set<std::string>({"AND", "OR", "ON", "IN"});
  static const std::set<char>& kNonWordSperarator =
      *new std::set<char>({',', '<', '>', '-', '+', '=', '*', '/', '%'});
  if (buffer_.empty()) return false;
  // When last token is not a word.
  if (!isalnum(buffer_.back())) {
    return zetasql_base::ContainsKey(kNonWordSperarator, buffer_.back());
  }

  int last_token_index = buffer_.size() - 1;
  while (last_token_index >= 0 && isalnum(buffer_[last_token_index])) {
    --last_token_index;
  }
  std::string last_token = buffer_.substr(last_token_index + 1);
  return zetasql_base::ContainsKey(kWordSperarator, last_token);
}

void Formatter::FlushLine() {
  if ((unparsed_->empty() || unparsed_->back() == '\n') && buffer_.empty()) {
    return;
  }
  absl::StrAppend(unparsed_, buffer_, "\n");
  buffer_.clear();
}

// Unparser -------------------------------------------------------------------

// Helper functions.
void Unparser::PrintOpenParenIfNeeded(const ASTNode* node) {
  DCHECK(node->IsExpression() || node->IsQueryExpression())
      << "Parenthesization is not allowed for " << node->GetNodeKindString();
  if (node->IsExpression() &&
      node->GetAsOrDie<ASTExpression>()->parenthesized()) {
    print("(");
  } else if (node->IsQueryExpression() &&
             node->GetAsOrDie<ASTQueryExpression>()->parenthesized()) {
    print("(");
  } else if (node->node_kind() == AST_SYSTEM_VARIABLE_EXPR &&
             node->parent() != nullptr &&
             node->parent()->node_kind() == AST_DOT_IDENTIFIER) {
    // "." takes precedence over "@@".
    print("(");
  }
}

void Unparser::PrintCloseParenIfNeeded(const ASTNode* node) {
  DCHECK(node->IsExpression() || node->IsQueryExpression())
      << "Parenthesization is not allowed for " << node->GetNodeKindString();
  if (node->IsExpression() &&
      node->GetAsOrDie<ASTExpression>()->parenthesized()) {
    print(")");
  } else if (node->IsQueryExpression() &&
             node->GetAsOrDie<ASTQueryExpression>()->parenthesized()) {
    print(")");
  } else if (node->node_kind() == AST_SYSTEM_VARIABLE_EXPR &&
             node->parent() != nullptr &&
             node->parent()->node_kind() == AST_DOT_IDENTIFIER) {
    // "." takes precedence over "@@".
    print(")");
  }
}

void Unparser::UnparseLeafNode(const ASTLeaf* leaf_node) {
  print(leaf_node->image());
}

void Unparser::UnparseChildrenWithSeparator(const ASTNode* node, void* data,
                                            const std::string& separator,
                                            bool break_line) {
  UnparseChildrenWithSeparator(node, data, 0, node->num_children(), separator,
                               break_line);
}

// Unparse children of <node> from indices in the range [<begin>, <end>)
// putting <separator> between them.
void Unparser::UnparseChildrenWithSeparator(const ASTNode* node, void* data,
                                            int begin, int end,
                                            const std::string& separator,
                                            bool break_line) {
  for (int i = begin; i < end; i++) {
    if (i > begin) {
      print(separator);
      if (break_line) {
        println();
      }
    }
    node->child(i)->Accept(this, data);
  }
}

// PrintCommentsPassedBy prints comments if they are before the given ParseLocationPoint
// and returns if comments are emitted.
bool Unparser::PrintCommentsPassedBy(const ParseLocationPoint point, void* data) {
  if (data == nullptr) {
    return false;
  }
  auto parse_tokens = static_cast<std::deque<std::pair<std::string, ParseLocationPoint>>*>(data);
  if (parse_tokens == nullptr) {
    return false;
  }
  bool emitted = false;
  const int size = parse_tokens->size();
  for (int i = 0; i < size; i++) {
    if (parse_tokens->front().second < point) {
      absl::string_view comment_string_view(parse_tokens->front().first);
      absl::ConsumeSuffix(&comment_string_view, "\r\n");
      absl::ConsumeSuffix(&comment_string_view, "\r");
      absl::ConsumeSuffix(&comment_string_view, "\n");
      std::string comment_string = std::string(comment_string_view);
      parse_tokens->pop_front();

      // println if multi-line comments
      if (!emitted && i + 1 < size) {
        if (parse_tokens->front().second < point) {
          FlushLine();
        }
      }

      println(comment_string);
      emitted = true;
    } else {
      break;
    }
  }
  return emitted;
}

// Visitor implementation.

void Unparser::visitASTHintedStatement(const ASTHintedStatement* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTExplainStatement(const ASTExplainStatement* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("EXPLAIN");
  node->statement()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTQueryStatement(const ASTQueryStatement* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  visitASTQuery(node->query(), data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTFunctionParameter(
    const ASTFunctionParameter* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(ASTFunctionParameter::ProcedureParameterModeToString(
      node->procedure_parameter_mode()));
  if (node->name() != nullptr) {
    node->name()->Accept(this, data);
  }
  if (node->type() != nullptr) {
    node->type()->Accept(this, data);
  }
  if (node->templated_parameter_type() != nullptr) {
    node->templated_parameter_type()->Accept(this, data);
  }
  if (node->tvf_schema() != nullptr) {
    node->tvf_schema()->Accept(this, data);
  }
  if (node->alias() != nullptr) {
    node->alias()->Accept(this, data);
  }
  if (node->is_not_aggregate()) {
    print("NOT AGGREGATE");
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTTemplatedParameterType(
    const ASTTemplatedParameterType* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  switch (node->kind()) {
    case ASTTemplatedParameterType::UNINITIALIZED:
      print("UNINITIALIZED");
      break;
    case ASTTemplatedParameterType::ANY_TYPE:
      print("ANY TYPE");
      break;
    case ASTTemplatedParameterType::ANY_PROTO:
      print("ANY PROTO");
      break;
    case ASTTemplatedParameterType::ANY_ENUM:
      print("ANY ENUM");
      break;
    case ASTTemplatedParameterType::ANY_STRUCT:
      print("ANY STRUCT");
      break;
    case ASTTemplatedParameterType::ANY_ARRAY:
      print("ANY ARRAY");
      break;
    case ASTTemplatedParameterType::ANY_TABLE:
      print("ANY TABLE");
      break;
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTFunctionParameters(
    const ASTFunctionParameters* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("(");
  for (int i = 0; i < node->num_children(); ++i) {
    if (i > 0) {
      print(", ");
    }
    node->child(i)->Accept(this, data);
  }
  print(")");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTFunctionDeclaration(
    const ASTFunctionDeclaration* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->name()->Accept(this, data);
  node->parameters()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTSqlFunctionBody(
    const ASTSqlFunctionBody* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->expression()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTTableClause(const ASTTableClause* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("TABLE ");
  if (node->table_path() != nullptr) {
    node->table_path()->Accept(this, data);
  }
  if (node->tvf() != nullptr) {
    node->tvf()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTModelClause(const ASTModelClause* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("MODEL ");
  node->model_path()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTConnectionClause(const ASTConnectionClause* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("CONNECTION ");
  node->connection_path()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTTVF(const ASTTVF* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->name()->Accept(this, data);
  print("(");
  UnparseVectorWithSeparator(node->argument_entries(), data, ",");
  print(")");
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  if (node->alias() != nullptr) {
    node->alias()->Accept(this, data);
  }
  if (node->sample() != nullptr) {
    node->sample()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTTVFArgument(const ASTTVFArgument* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->expr() != nullptr) {
    node->expr()->Accept(this, data);
  }
  if (node->table_clause() != nullptr) {
    node->table_clause()->Accept(this, data);
  }
  if (node->model_clause() != nullptr) {
    node->model_clause()->Accept(this, data);
  }
  if (node->connection_clause() != nullptr) {
    node->connection_clause()->Accept(this, data);
  }
  if (node->descriptor() != nullptr) {
    node->descriptor()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTTVFSchema(const ASTTVFSchema* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("TABLE<");
  UnparseChildrenWithSeparator(node, data, ",");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  print(">");
}

void Unparser::visitASTTVFSchemaColumn(const ASTTVFSchemaColumn* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseChildrenWithSeparator(node, data, "");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

static std::string GetCreateStatementPrefix(
    const ASTCreateStatement* node, const std::string& create_object_type) {
  std::string output("CREATE");
  if (node->is_or_replace()) absl::StrAppend(&output, " OR REPLACE");
  if (node->is_private()) absl::StrAppend(&output, " PRIVATE");
  if (node->is_public()) absl::StrAppend(&output, " PUBLIC");
  if (node->is_temp()) absl::StrAppend(&output, " TEMP");
  auto create_view = node->GetAsOrNull<ASTCreateViewStatementBase>();
  if (create_view != nullptr && create_view->recursive()) {
    absl::StrAppend(&output, " RECURSIVE");
  }
  absl::StrAppend(&output, " ", create_object_type);
  if (node->is_if_not_exists()) absl::StrAppend(&output, " IF NOT EXISTS");
  return output;
}

void Unparser::visitASTCreateConstantStatement(
    const ASTCreateConstantStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(GetCreateStatementPrefix(node, "CONSTANT"));
  node->name()->Accept(this, data);
  print("=");
  node->expr()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCreateDatabaseStatement(
    const ASTCreateDatabaseStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("CREATE DATABASE");
  node->name()->Accept(this, data);
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCreateFunctionStatement(
    const ASTCreateFunctionStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  const std::string create_object_type =
      absl::StrCat((node->is_aggregate() ? "AGGREGATE " : ""), "FUNCTION");
  print(GetCreateStatementPrefix(node, create_object_type));
  node->function_declaration()->Accept(this, data);
  println();
  if (node->return_type() != nullptr) {
    print("RETURNS");
    node->return_type()->Accept(this, data);
  }
  if (node->sql_security() != ASTCreateStatement::SQL_SECURITY_UNSPECIFIED) {
    print(node->GetSqlForSqlSecurity());
  }
  if (node->determinism_level() !=
      ASTCreateFunctionStmtBase::DETERMINISM_UNSPECIFIED) {
    print(node->GetSqlForDeterminismLevel());
  }
  if (node->language() != nullptr) {
    print("LANGUAGE");
    node->language()->Accept(this, data);
  }
  if (node->code() != nullptr) {
    print("AS");
    node->code()->Accept(this, data);
  } else if (node->sql_function_body() != nullptr) {
    println("AS (");
    {
      Formatter::Indenter indenter(&formatter_);
      node->sql_function_body()->Accept(this, data);
    }
    println();
    println(")");
  }
  if (node->options_list() != nullptr) {
    println("OPTIONS");
    Formatter::Indenter indenter(&formatter_);
    node->options_list()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCreateSchemaStatement(
    const ASTCreateSchemaStatement* node, void* data) {
  print(GetCreateStatementPrefix(node, "SCHEMA"));
  node->name()->Accept(this, data);
  if (node->options_list() != nullptr) {
    println();
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
}

void Unparser::visitASTCreateTableFunctionStatement(
    const ASTCreateTableFunctionStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(GetCreateStatementPrefix(node, "TABLE FUNCTION"));
  node->function_declaration()->Accept(this, data);
  println();
  if (node->return_tvf_schema() != nullptr &&
      !node->return_tvf_schema()->columns().empty()) {
    print("RETURNS");
    node->return_tvf_schema()->Accept(this, data);
  }
  if (node->sql_security() != ASTCreateStatement::SQL_SECURITY_UNSPECIFIED) {
    print(node->GetSqlForSqlSecurity());
  }
  if (node->options_list() != nullptr) {
    println("OPTIONS");
    Formatter::Indenter indenter(&formatter_);
    node->options_list()->Accept(this, data);
  }
  if (node->language() != nullptr) {
    print("LANGUAGE");
    node->language()->Accept(this, data);
  }
  if (node->code() != nullptr) {
    print("AS");
    node->code()->Accept(this, data);
  } else if (node->query() != nullptr) {
    println("AS");
    {
      Formatter::Indenter indenter(&formatter_);
      node->query()->Accept(this, data);
    }
    println();
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCreateTableStatement(
    const ASTCreateTableStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(GetCreateStatementPrefix(node, "TABLE"));
  node->name()->Accept(this, data);
  if (node->table_element_list() != nullptr) {
    println();
    node->table_element_list()->Accept(this, data);
  }
  if (node->partition_by() != nullptr) {
    node->partition_by()->Accept(this, data);
  }
  if (node->cluster_by() != nullptr) {
    node->cluster_by()->Accept(this, data);
  }
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  if (node->query() != nullptr) {
    println("AS");
    node->query()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCreateEntityStatement(
    const ASTCreateEntityStatement* node, void* data) {
  print(GetCreateStatementPrefix(node, node->type()->GetAsString()));
  node->name()->Accept(this, data);
  if (node->options_list() != nullptr) {
    println();
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  if (node->json_body() != nullptr) {
    println();
    print("AS JSON");
    node->json_body()->Accept(this, data);
  }
}

void Unparser::visitASTAlterEntityStatement(const ASTAlterEntityStatement* node,
                                            void* data) {
  print("ALTER ");
  node->type()->Accept(this, data);
  VisitAlterStatementBase(node, data);
}

void Unparser::visitASTCreateModelStatement(const ASTCreateModelStatement* node,
                                            void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(GetCreateStatementPrefix(node, "MODEL"));
  node->name()->Accept(this, data);
  if (node->transform_clause() != nullptr) {
    print("TRANSFORM");
    node->transform_clause()->Accept(this, data);
  }
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  if (node->query() != nullptr) {
    println("AS");
    node->query()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTTableElementList(const ASTTableElementList* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",", true /* break_line */);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  println();
  print(")");
}

void Unparser::visitASTNotNullColumnAttribute(
    const ASTNotNullColumnAttribute* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("NOT NULL");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTHiddenColumnAttribute(
    const ASTHiddenColumnAttribute* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("HIDDEN");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTPrimaryKeyColumnAttribute(
    const ASTPrimaryKeyColumnAttribute* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("PRIMARY KEY");
  if (!node->enforced()) {
    print("NOT ENFORCED");
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTForeignKeyColumnAttribute(
    const ASTForeignKeyColumnAttribute* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->constraint_name() != nullptr) {
    print("CONSTRAINT");
    node->constraint_name()->Accept(this, data);
  }
  node->reference()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTColumnAttributeList(
    const ASTColumnAttributeList* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseChildrenWithSeparator(node, data, "", /*break_line=*/false);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTColumnDefinition(const ASTColumnDefinition* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->name() != nullptr) {
    node->name()->Accept(this, data);
  }
  if (node->schema() != nullptr) {
    node->schema()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCreateViewStatement(
    const ASTCreateViewStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(GetCreateStatementPrefix(node, "VIEW"));
  node->name()->Accept(this, data);
  if (node->sql_security() != ASTCreateStatement::SQL_SECURITY_UNSPECIFIED) {
    print(node->GetSqlForSqlSecurity());
  }
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  println("AS");
  node->query()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCreateMaterializedViewStatement(
    const ASTCreateMaterializedViewStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("CREATE");

  if (node->is_or_replace()) print("OR REPLACE");
  print("MATERIALIZED");
  if (node->recursive()) {
    print("RECURSIVE");
  }
  print("VIEW");
  if (node->is_if_not_exists()) {
    print("IF NOT EXISTS");
  }

  node->name()->Accept(this, data);
  if (node->sql_security() != ASTCreateStatement::SQL_SECURITY_UNSPECIFIED) {
    print(node->GetSqlForSqlSecurity());
  }
  if (node->partition_by() != nullptr) {
    node->partition_by()->Accept(this, data);
  }
  if (node->cluster_by() != nullptr) {
    node->cluster_by()->Accept(this, data);
  }
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  println("AS");
  node->query()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTWithPartitionColumnsClause(
    const ASTWithPartitionColumnsClause* node, void* data) {
  print("WITH PARTITION COLUMNS");
  if (node->table_element_list() != nullptr) {
    node->table_element_list()->Accept(this, data);
  }
}

void Unparser::visitASTCreateExternalTableStatement(
    const ASTCreateExternalTableStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(GetCreateStatementPrefix(node, "EXTERNAL TABLE"));
  node->name()->Accept(this, data);
  if (node->table_element_list() != nullptr) {
    println();
    node->table_element_list()->Accept(this, data);
  }
  if (node->with_partition_columns_clause() != nullptr) {
    node->with_partition_columns_clause()->Accept(this, data);
  }
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTGrantToClause(const ASTGrantToClause* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->has_grant_keyword_and_parens()) {
    print("GRANT");
  }
  print("TO ");
  if (node->has_grant_keyword_and_parens()) {
    print("(");
  }
  node->grantee_list()->Accept(this, data);
  if (node->has_grant_keyword_and_parens()) {
    print(")");
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTFilterUsingClause(const ASTFilterUsingClause* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->has_filter_keyword()) {
    print("FILTER");
  }
  print("USING (");
  node->predicate()->Accept(this, data);
  print(")");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCreateRowAccessPolicyStatement(
    const ASTCreateRowAccessPolicyStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("CREATE");
  if (node->is_or_replace()) {
    print("OR REPLACE");
  }
  print("ROW");
  if (node->has_access_keyword()) {
    print("ACCESS");
  }
  print("POLICY");
  if (node->is_if_not_exists()) print("IF NOT EXISTS");
  if (node->name() != nullptr) {
    node->name()->Accept(this, data);
  }
  print("ON");
  node->target_path()->Accept(this, data);
  if (node->grant_to() != nullptr) {
    node->grant_to()->Accept(this, data);
  }
  node->filter_using()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTExportDataStatement(
    const ASTExportDataStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("EXPORT DATA");
  if (node->with_connection_clause() != nullptr) {
    print("WITH");
    node->with_connection_clause()->Accept(this, data);
  }

  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  println("AS");
  node->query()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTExportModelStatement(const ASTExportModelStatement* node,
                                            void* data) {
  print("EXPORT MODEL");
  if (node->model_name_path() != nullptr) {
    node->model_name_path()->Accept(this, data);
  }

  if (node->with_connection_clause() != nullptr) {
    print("WITH");
    node->with_connection_clause()->Accept(this, data);
  }

  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
}

void Unparser::visitASTWithConnectionClause(const ASTWithConnectionClause* node,
                                            void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->connection_clause()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCallStatement(
    const ASTCallStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("CALL");
  node->procedure_name()->Accept(this, data);
  print("(");
  UnparseVectorWithSeparator(node->arguments(), data, ",");
  print(")");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDefineTableStatement(
    const ASTDefineTableStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("DEFINE TABLE");
  node->name()->Accept(this, data);
  node->options_list()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDescribeStatement(const ASTDescribeStatement* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("DESCRIBE");
  if (node->optional_identifier() != nullptr) {
    node->optional_identifier()->Accept(this, data);
  }
  node->name()->Accept(this, data);
  if (node->optional_from_name() != nullptr) {
    print("FROM");
    node->optional_from_name()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDescriptorColumn(const ASTDescriptorColumn* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->name()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDescriptorColumnList(const ASTDescriptorColumnList* node,
                                            void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseChildrenWithSeparator(node, data, ", ");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDescriptor(const ASTDescriptor* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("DESCRIPTOR(");
  node->columns()->Accept(this, data);
  print(")");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTShowStatement(const ASTShowStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("SHOW");
  node->identifier()->Accept(this, data);
  if (node->optional_name() != nullptr) {
    print("FROM");
    node->optional_name()->Accept(this, data);
  }
  if (node->optional_like_string() != nullptr) {
    print("LIKE");
    node->optional_like_string()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTBeginStatement(
    const ASTBeginStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("BEGIN TRANSACTION");
  if (node->mode_list() != nullptr) {
    node->mode_list()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTTransactionIsolationLevel(
    const ASTTransactionIsolationLevel* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->identifier1() != nullptr) {
    print("ISOLATION LEVEL");
    node->identifier1()->Accept(this, data);
  }
  if (node->identifier2() != nullptr) {
    node->identifier2()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTTransactionReadWriteMode(
    const ASTTransactionReadWriteMode* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  switch (node->mode()) {
    case ASTTransactionReadWriteMode::READ_ONLY:
      print("READ ONLY");
      break;
    case ASTTransactionReadWriteMode::READ_WRITE:
      print("READ WRITE");
      break;
    case ASTTransactionReadWriteMode::INVALID:
      LOG(DFATAL) << "invalid read write mode";
      break;
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTTransactionModeList(const ASTTransactionModeList* node,
                                           void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  bool first = true;
  for (const ASTTransactionMode* mode : node->elements()) {
    if (!first) {
      print(",");
    } else {
      first = false;
    }
    mode->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTSetTransactionStatement(
    const ASTSetTransactionStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("SET TRANSACTION");
  node->mode_list()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCommitStatement(const ASTCommitStatement* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("COMMIT");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTRollbackStatement(const ASTRollbackStatement* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ROLLBACK");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStartBatchStatement(const ASTStartBatchStatement* node,
                                           void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("START BATCH");
  if (node->batch_type() != nullptr) {
    node->batch_type()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTRunBatchStatement(const ASTRunBatchStatement* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("RUN BATCH");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAbortBatchStatement(const ASTAbortBatchStatement* node,
                                           void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ABORT BATCH");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDropStatement(const ASTDropStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("DROP");
  print(SchemaObjectKindToName(node->schema_object_kind()));
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
  print(node->GetSQLForDropMode(node->drop_mode()));
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDropEntityStatement(const ASTDropEntityStatement* node,
                                           void* data) {
  print("DROP ");
  node->entity_type()->Accept(this, data);
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
}

void Unparser::visitASTDropFunctionStatement(
    const ASTDropFunctionStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("DROP FUNCTION");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
  if (node->parameters() != nullptr) {
    node->parameters()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDropRowAccessPolicyStatement(
    const ASTDropRowAccessPolicyStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("DROP ROW ACCESS POLICY");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
  print("ON");
  node->table_name()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDropAllRowAccessPoliciesStatement(
    const ASTDropAllRowAccessPoliciesStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("DROP ALL ROW");
  if (node->has_access_keyword()) {
    print("ACCESS");
  }
  print("POLICIES ON");
  node->table_name()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDropMaterializedViewStatement(
    const ASTDropMaterializedViewStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("DROP MATERIALIZED VIEW");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTRenameStatement(const ASTRenameStatement* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("RENAME");
  if (node->identifier() != nullptr) {
    node->identifier()->Accept(this, data);
  }
  node->old_name()->Accept(this, data);
  print("TO");
  node->new_name()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTImportStatement(const ASTImportStatement* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("IMPORT");
  if (node->import_kind() == ASTImportStatement::MODULE) {
    print("MODULE");
  } else if (node->import_kind() == ASTImportStatement::PROTO) {
    print("PROTO");
  } else {
    print("<invalid import type>");
  }

  if (node->name() != nullptr) {
    node->name()->Accept(this, data);
  }
  if (node->string_value() != nullptr) {
    node->string_value()->Accept(this, data);
  }
  if (node->alias() != nullptr) {
    node->alias()->Accept(this, data);
  }
  if (node->into_alias() != nullptr) {
    node->into_alias()->Accept(this, data);
  }
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTModuleStatement(const ASTModuleStatement* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("MODULE");
  node->name()->Accept(this, data);
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTWithClause(const ASTWithClause* node,
                                  void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->recursive()) {
    println("WITH RECURSIVE");
  } else {
    println("WITH");
  }

  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
}

void Unparser::visitASTWithClauseEntry(const ASTWithClauseEntry* node,
                                       void *data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  node->alias()->Accept(this, data);
  println("AS (");
  {
    Formatter::Indenter indenter(&formatter_);
    visitASTQuery(node->query(), data);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  println();
  print(")");
}

void Unparser::visitASTQuery(const ASTQuery* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  if (node->is_nested()) {
    println();
    print("(");
    {
      Formatter::Indenter indenter(&formatter_);
      visitASTChildren(node, data);
      PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
    }
    println();
    print(")");
  } else {
    visitASTChildren(node, data);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTSetOperation(const ASTSetOperation* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);

  int start = node->hint() == nullptr ? 0 : 1;

  for (int i = start; i < node->num_children(); ++i) {
    if (i > start) {
      if (i == start + 1) {
        const auto& pair = node->GetSQLForOperationPair();
        print(pair.first);
        if (node->hint()) {
          node->hint()->Accept(this, data);
        }
        print(pair.second);
      } else {
        print(node->GetSQLForOperation());
      }
    }
    node->child(i)->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTSetAsAction(const ASTSetAsAction* node, void* data) {
  print("SET AS JSON");
  node->body()->Accept(this, data);
}

void Unparser::visitASTSelect(const ASTSelect* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  println();
  print("SELECT");
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  if (node->distinct()) {
    print("DISTINCT");
  }

  for (int i = 0; i < node->num_children(); ++i) {
    const ASTNode* child = node->child(i);
    if (child != node->hint()
        ) {
      child->Accept(this, data);
    }
  }

  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);

  println();
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTSelectAs(const ASTSelectAs* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->as_mode() != ASTSelectAs::TYPE_NAME) {
    print(absl::StrCat(
        "AS ", node->as_mode() == ASTSelectAs::VALUE ? "VALUE" : "STRUCT"));
  } else {
    print("AS");
  }
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTSelectList(const ASTSelectList* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",", true /* break_line */);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
}

void Unparser::visitASTSelectColumn(const ASTSelectColumn* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAlias(const ASTAlias* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(absl::StrCat("AS ",
                     ToIdentifierLiteral(node->identifier()->GetAsIdString())));
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTIntoAlias(const ASTIntoAlias* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(absl::StrCat("INTO ",
                     ToIdentifierLiteral(node->identifier()->GetAsIdString())));
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTFromClause(const ASTFromClause* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  println("FROM");
  {
    Formatter::Indenter indenter(&formatter_);
    visitASTChildren(node, data);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
}

void Unparser::visitASTTransformClause(const ASTTransformClause* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println("(");
  visitASTChildren(node, data);
  println(")");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTWithOffset(const ASTWithOffset* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("WITH OFFSET");
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTUnnestExpression(const ASTUnnestExpression* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("UNNEST(");
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  print(")");
}

void Unparser::visitASTUnnestExpressionWithOptAliasAndOffset(
    const ASTUnnestExpressionWithOptAliasAndOffset* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTTablePathExpression(
    const ASTTablePathExpression* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTForSystemTime(const ASTForSystemTime* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("FOR SYSTEM_TIME AS OF ");
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTTableSubquery(
    const ASTTableSubquery* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTJoin(const ASTJoin* node, void* data) {
  node->child(0)->Accept(this, data);

  // Print comments after accepting child 0 because ASTJoin's
  // position is after the child 0.
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);

  if (node->join_type() == ASTJoin::COMMA) {
    print(",");
  } else {
    println();
    if (node->natural()) {
      print("NATURAL");
    }
    print(node->GetSQLForJoinType());
    print(node->GetSQLForJoinHint());

    print("JOIN");
  }
  println();

  // This will print hints, the rhs, and the ON or USING clause.
  for (int i = 1; i < node->num_children(); i++) {
    node->child(i)->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTParenthesizedJoin(const ASTParenthesizedJoin* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  println("(");
  {
    Formatter::Indenter indenter(&formatter_);
    node->join()->Accept(this, data);
  }
  println();
  print(")");

  if (node->sample_clause() != nullptr) {
    node->sample_clause()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTOnClause(const ASTOnClause* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("ON");
  {
    Formatter::Indenter indenter(&formatter_);
    visitASTChildren(node, data);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
}

void Unparser::visitASTOnOrUsingClauseList(const ASTOnOrUsingClauseList* node,
                                           void* data) {
  for (const ASTNode* clause : node->on_or_using_clause_list()) {
    clause->Accept(this, data);
    println();
  }
}

void Unparser::visitASTUsingClause(const ASTUsingClause* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("USING(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
}

void Unparser::visitASTWhereClause(const ASTWhereClause* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  println("WHERE");
  {
    Formatter::Indenter indenter(&formatter_);
    visitASTChildren(node, data);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
}

void Unparser::visitASTRollup(const ASTRollup* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ROLLUP(");
  UnparseVectorWithSeparator(node->expressions(), data, ",");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  print(")");
}

void Unparser::visitASTGroupingItem(const ASTGroupingItem* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->expression() != nullptr) {
    DCHECK(node->rollup() == nullptr);
    node->expression()->Accept(this, data);
  } else {
    DCHECK(node->rollup() != nullptr);
    node->rollup()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTGroupBy(const ASTGroupBy* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("GROUP");
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  println("BY");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->grouping_items(), data, ",", true /* break_line */);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
}

void Unparser::visitASTHaving(const ASTHaving* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("HAVING");
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCollate(const ASTCollate* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("COLLATE");
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTOrderBy(const ASTOrderBy* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("ORDER");
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  print("BY");
  UnparseVectorWithSeparator(node->ordering_expressions(), data, ",");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTNullOrder(const ASTNullOrder* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->nulls_first()) {
    print("NULLS FIRST");
  } else {
    print("NULLS LAST");
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTOrderingExpression(const ASTOrderingExpression* node,
                                          void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->expression()->Accept(this, data);
  if (node->collate()) node->collate()->Accept(this, data);
  if (node->descending()) print("DESC");
  if (node->null_order()) node->null_order()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTLimitOffset(const ASTLimitOffset* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("LIMIT");
  UnparseChildrenWithSeparator(node, data, "OFFSET");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTHavingModifier(const ASTHavingModifier* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("HAVING ");
  if (node->modifier_kind() == ASTHavingModifier::ModifierKind::MAX) {
    print("MAX");
  } else {
    print("MIN");
  }
  node->expr()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTIdentifier(const ASTIdentifier* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(ToIdentifierLiteral(node->GetAsIdString()));
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTNewConstructorArg(const ASTNewConstructorArg* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->expression()->Accept(this, data);
  if (node->optional_identifier() != nullptr) {
    print("AS ");
    node->optional_identifier()->Accept(this, data);
  }
  if (node->optional_path_expression() != nullptr) {
    print("AS (");
    node->optional_path_expression()->Accept(this, data);
    print(")");
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTNewConstructor(const ASTNewConstructor* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("NEW");
  node->type_name()->Accept(this, data);
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->arguments(), data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
}

void Unparser::visitASTInferredTypeColumnSchema(
    const ASTInferredTypeColumnSchema* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseColumnSchema(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTArrayConstructor(const ASTArrayConstructor* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->type() != nullptr) {
    node->type()->Accept(this, data);
  } else {
    print("ARRAY");
  }
  print("[");
  UnparseVectorWithSeparator(node->elements(), data, ",");
  print("]");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStructConstructorArg(const ASTStructConstructorArg* node,
                                            void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStructConstructorWithParens(
    const ASTStructConstructorWithParens* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->field_expressions(), data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
}

void Unparser::visitASTStructConstructorWithKeyword(
    const ASTStructConstructorWithKeyword* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->struct_type() != nullptr) {
    node->struct_type()->Accept(this, data);
  } else {
    print("STRUCT");
  }
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->fields(), data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
}

void Unparser::visitASTIntLiteral(const ASTIntLiteral* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseLeafNode(node);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTNumericLiteral(
    const ASTNumericLiteral* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("NUMERIC");
  UnparseLeafNode(node);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTBigNumericLiteral(const ASTBigNumericLiteral* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("BIGNUMERIC");
  UnparseLeafNode(node);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTJSONLiteral(const ASTJSONLiteral* node, void* data) {
  print("JSON");
  UnparseLeafNode(node);
}

void Unparser::visitASTFloatLiteral(const ASTFloatLiteral* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseLeafNode(node);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStringLiteral(const ASTStringLiteral* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseLeafNode(node);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTBytesLiteral(const ASTBytesLiteral* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseLeafNode(node);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTBooleanLiteral(const ASTBooleanLiteral* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseLeafNode(node);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTNullLiteral(const ASTNullLiteral* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseLeafNode(node);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDateOrTimeLiteral(const ASTDateOrTimeLiteral* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(Type::TypeKindToString(node->type_kind(), PRODUCT_INTERNAL));
  UnparseChildrenWithSeparator(node, data, "");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStar(const ASTStar* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseLeafNode(node);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStarExceptList(const ASTStarExceptList* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",", true /* break_line */);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
}

void Unparser::visitASTStarReplaceItem(const ASTStarReplaceItem* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseChildrenWithSeparator(node, data, "AS");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStarModifiers(const ASTStarModifiers* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->except_list() != nullptr) {
    println();
    {
      Formatter::Indenter indenter(&formatter_);
      println("EXCEPT (");
      node->except_list()->Accept(this, data);
      println();
      print(")");
    }
  }
  if (!node->replace_items().empty()) {
    println();
    {
      Formatter::Indenter indenter(&formatter_);
      println("REPLACE (");
      {
        Formatter::Indenter indenter(&formatter_);
        UnparseVectorWithSeparator(node->replace_items(), data, ",");
      }
      println();
      print(")");
    }
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStarWithModifiers(const ASTStarWithModifiers* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("*");
  node->modifiers()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTPathExpression(const ASTPathExpression* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  UnparseChildrenWithSeparator(node, data, ".");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTParameterExpr(const ASTParameterExpr* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->name() == nullptr) {
    print("?");
  } else {
    print("@");
    visitASTChildren(node, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTSystemVariableExpr(const ASTSystemVariableExpr* node,
                                          void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  print("@@");
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTIntervalExpr(const ASTIntervalExpr* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("INTERVAL");
  UnparseChildrenWithSeparator(node, data, "");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDotIdentifier(const ASTDotIdentifier* node,
                                     void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  node->expr()->Accept(this, data);
  print(".");
  node->name()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTDotGeneralizedField(const ASTDotGeneralizedField* node,
                                           void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  node->expr()->Accept(this, data);
  print(".(");
  node->path()->Accept(this, data);
  print(")");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTDotStar(const ASTDotStar* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->expr()->Accept(this, data);
  print(".*");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDotStarWithModifiers(
    const ASTDotStarWithModifiers* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->expr()->Accept(this, data);
  print(".*");
  node->modifiers()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTOrExpr(const ASTOrExpr* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  UnparseChildrenWithSeparator(node, data, "OR");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTAndExpr(const ASTAndExpr* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  UnparseChildrenWithSeparator(node, data, "AND");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTUnaryExpression(const ASTUnaryExpression* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  // Nested unary expressions like "--1" would be printed as " - - 1"
  // by the formatter and not "-- 1", as the latter would be detected
  // as partial line comment by the parser.
  PrintOpenParenIfNeeded(node);
  print(node->GetSQLForOperator());
  node->operand()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTCastExpression(const ASTCastExpression* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(node->is_safe_cast() ? "SAFE_CAST(" : "CAST(");
  UnparseChildrenWithSeparator(node, data, "AS");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  print(")");
}

void Unparser::visitASTExtractExpression(const ASTExtractExpression* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("EXTRACT(");
  node->lhs_expr()->Accept(this, data);
  print("FROM");
  node->rhs_expr()->Accept(this, data);
  if (node->time_zone_expr() != nullptr) {
    print("AT TIME ZONE");
    node->time_zone_expr()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  print(")");
}

void Unparser::visitASTCollateExpression(const ASTCollateExpression* node,
                                         void* data) {
  print("COLLATE(");
  node->expr()->Accept(this, data);
  print(", ");
  node->collation_spec()->Accept(this, data);
  print(")");
}

void Unparser::visitASTCaseNoValueExpression(
    const ASTCaseNoValueExpression* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("CASE");
  int i;
  {
    Formatter::Indenter indenter(&formatter_);
    for (i = 0; i < node->num_children() - 1; i += 2) {
      println();
      print("WHEN");
      node->child(i)->Accept(this, data);
      print("THEN");
      node->child(i + 1)->Accept(this, data);
    }
    if (i < node->num_children()) {
      println();
      print("ELSE");
      node->child(i)->Accept(this, data);
    }
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  println();
  print("END");
}

void Unparser::visitASTCaseValueExpression(const ASTCaseValueExpression* node,
                                           void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("CASE");
  node->child(0)->Accept(this, data);
  int i;
  {
    Formatter::Indenter indenter(&formatter_);
    for (i = 1; i < node->num_children() - 1; i += 2) {
      println();
      print("WHEN");
      node->child(i)->Accept(this, data);
      PrintCommentsPassedBy(node->child(i + 1)->GetParseLocationRange().start(), data);
      print("THEN");
      node->child(i + 1)->Accept(this, data);
    }
    if (i < node->num_children()) {
      println();
      print("ELSE");
      node->child(i)->Accept(this, data);
    }
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  println();
  print("END");
}

void Unparser::visitASTBinaryExpression(const ASTBinaryExpression* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  UnparseChildrenWithSeparator(node, data, node->GetSQLForOperator());
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTBitwiseShiftExpression(
    const ASTBitwiseShiftExpression* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  UnparseChildrenWithSeparator(node, data, node->is_left_shift() ? "<<" : ">>");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTInExpression(const ASTInExpression* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  node->lhs()->Accept(this, data);
  print(absl::StrCat(node->is_not() ? "NOT " : "", "IN"));
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  if (node->query() != nullptr) {
    print("(");
    {
      Formatter::Indenter indenter(&formatter_);
      node->query()->Accept(this, data);
    }
    print(")");
  }
  if (node->in_list() != nullptr) {
    node->in_list()->Accept(this, data);
  }
  if (node->unnest_expr() != nullptr) {
    node->unnest_expr()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTInList(const ASTInList* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
}

void Unparser::visitASTBetweenExpression(const ASTBetweenExpression* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  node->child(0)->Accept(this, data);
  print(absl::StrCat(node->is_not() ? "NOT " : "", "BETWEEN"));
  UnparseChildrenWithSeparator(node, data, 1, node->num_children(), "AND");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTFunctionCall(const ASTFunctionCall* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  node->function()->Accept(this, data);
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    if (node->distinct()) print("DISTINCT");
    UnparseVectorWithSeparator(node->arguments(), data, ",");
    switch (node->null_handling_modifier()) {
      case ASTFunctionCall::DEFAULT_NULL_HANDLING:
        break;
      case ASTFunctionCall::IGNORE_NULLS:
        print("IGNORE NULLS");
        break;
      case ASTFunctionCall::RESPECT_NULLS:
        print("RESPECT NULLS");
        break;
      // No "default:". Let the compilation fail in case an entry is added to
      // the enum without being handled here.
    }
    if (node->having_modifier() != nullptr) {
      node->having_modifier()->Accept(this, data);
    }
    if (node->order_by() != nullptr) {
      node->order_by()->Accept(this, data);
    }
    if (node->limit_offset() != nullptr) {
      node->limit_offset()->Accept(this, data);
    }
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTArrayElement(const ASTArrayElement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  node->array()->Accept(this, data);
  print("[");
  node->position()->Accept(this, data);
  print("]");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTExpressionSubquery(const ASTExpressionSubquery* node,
                                          void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(ASTExpressionSubquery::ModifierToString(node->modifier()));
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    node->query()->Accept(this, data);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
}

void Unparser::visitASTHint(const ASTHint* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->num_shards_hint() != nullptr) {
    print("@");
    node->num_shards_hint()->Accept(this, data);
  }

  if (!node->hint_entries().empty()) {
    print("@{");
    UnparseVectorWithSeparator(node->hint_entries(), data, ",");
    print("}");
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTHintEntry(const ASTHintEntry* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->qualifier() != nullptr) {
    node->qualifier()->Accept(this, data);
    print(".");
  }
  node->name()->Accept(this, data);
  print("=");
  node->value()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTOptionsList(const ASTOptionsList* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
}

void Unparser::visitASTOptionsEntry(const ASTOptionsEntry* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseChildrenWithSeparator(node, data, "=");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTSimpleType(const ASTSimpleType* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTArrayType(const ASTArrayType* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ARRAY<");
  node->element_type()->Accept(this, data);
  print(">");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStructType(const ASTStructType* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("STRUCT<");
  UnparseChildrenWithSeparator(node, data, ",");
  print(">");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStructField(const ASTStructField* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseChildrenWithSeparator(node, data, "");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTSimpleColumnSchema(const ASTSimpleColumnSchema* node,
                                          void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->type_name()->Accept(this, data);
  UnparseColumnSchema(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTArrayColumnSchema(const ASTArrayColumnSchema* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ARRAY<");
  node->element_schema()->Accept(this, data);
  print(">");
  UnparseColumnSchema(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStructColumnSchema(const ASTStructColumnSchema* node,
                                          void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("STRUCT<");
  UnparseVectorWithSeparator(node->struct_fields(), data, ",");
  print(">");
  UnparseColumnSchema(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTGeneratedColumnInfo(const ASTGeneratedColumnInfo* node,
                                           void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->is_on_write()) {
    print("GENERATED ON WRITE");
  }
  print("AS (");
  DCHECK(node->expression() != nullptr);
  node->expression()->Accept(this, data);
  print(")");
  if (node->is_stored()) {
    print("STORED");
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStructColumnField(const ASTStructColumnField* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseChildrenWithSeparator(node, data, "");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::UnparseColumnSchema(const ASTColumnSchema* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->generated_column_info() != nullptr) {
    node->generated_column_info()->Accept(this, data);
  }
  if (node->attributes() != nullptr) {
    node->attributes()->Accept(this, data);
  }
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    Formatter::Indenter indenter(&formatter_);
    node->options_list()->Accept(this, data);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAnalyticFunctionCall(const ASTAnalyticFunctionCall* node,
                                            void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  PrintOpenParenIfNeeded(node);
  if (node->function() != nullptr) {
    node->function()->Accept(this, data);
  } else {
    node->function_with_group_rows()->Accept(this, data);
  }
  print("OVER (");
  {
    Formatter::Indenter indenter(&formatter_);
    node->window_spec()->Accept(this, data);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTFunctionCallWithGroupRows(
    const ASTFunctionCallWithGroupRows* node, void* data) {
  PrintOpenParenIfNeeded(node);
  node->function()->Accept(this, data);
  print("WITH GROUP_ROWS (");
  {
    Formatter::Indenter indenter(&formatter_);
    node->subquery()->Accept(this, data);
  }
  print(")");
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTWindowClause(const ASTWindowClause* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("WINDOW");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->windows(), data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
}

void Unparser::visitASTWindowDefinition(
    const ASTWindowDefinition* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->name()->Accept(this, data);
  print("AS (");
  node->window_spec()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  print(")");
}

void Unparser::visitASTWindowSpecification(
    const ASTWindowSpecification* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseChildrenWithSeparator(node, data, "");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTPartitionBy(const ASTPartitionBy* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("PARTITION");
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  print("BY");
  UnparseVectorWithSeparator(node->partitioning_expressions(), data, ",");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTClusterBy(const ASTClusterBy* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("CLUSTER BY");
  UnparseVectorWithSeparator(node->clustering_expressions(), data, ",");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTWindowFrame(const ASTWindowFrame* node,
                                   void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(node->GetFrameUnitString());
  if (nullptr != node->end_expr()) {
    print("BETWEEN");
  }
  node->start_expr()->Accept(this, data);
  if (nullptr != node->end_expr()) {
    print("AND");
    node->end_expr()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTWindowFrameExpr(
    const ASTWindowFrameExpr* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  switch (node->boundary_type()) {
    case ASTWindowFrameExpr::UNBOUNDED_PRECEDING:
    case ASTWindowFrameExpr::CURRENT_ROW:
    case ASTWindowFrameExpr::UNBOUNDED_FOLLOWING:
      print(node->GetBoundaryTypeString());
      break;
    case ASTWindowFrameExpr::OFFSET_PRECEDING:
      node->expression()->Accept(this, data);
      print("PRECEDING");
      break;
    case ASTWindowFrameExpr::OFFSET_FOLLOWING:
      node->expression()->Accept(this, data);
      print("FOLLOWING");
      break;
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDefaultLiteral(const ASTDefaultLiteral* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("DEFAULT");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAssertStatement(const ASTAssertStatement* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ASSERT");
  node->expr()->Accept(this, data);
  if (node->description() != nullptr) {
    print("AS");
    node->description()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAssertRowsModified(const ASTAssertRowsModified* node,
                                          void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("ASSERT_ROWS_MODIFIED");
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDeleteStatement(const ASTDeleteStatement* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("DELETE");
  // GetTargetPathForNested() is strictly more general than "ForNonNested()".
  node->GetTargetPathForNested()->Accept(this, data);
  if (node->alias() != nullptr) {
    node->alias()->Accept(this, data);
  }
  if (node->offset() != nullptr) {
    node->offset()->Accept(this, data);
  }
  if (node->where() != nullptr) {
    println();
    println("WHERE");
    {
      Formatter::Indenter indenter(&formatter_);
      node->where()->Accept(this, data);
      if (node->assert_rows_modified() == nullptr) {
        PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
      }
    }
  }
  if (node->assert_rows_modified() != nullptr) {
    node->assert_rows_modified()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTColumnList(const ASTColumnList* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
}

void Unparser::visitASTInsertValuesRow(const ASTInsertValuesRow* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
}

void Unparser::visitASTInsertValuesRowList(const ASTInsertValuesRowList* node,
                                           void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("VALUES");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
}

void Unparser::visitASTInsertStatement(const ASTInsertStatement* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("INSERT");
  if (node->insert_mode() != ASTInsertStatement::DEFAULT_MODE) {
    print("OR");
    print(node->GetSQLForInsertMode());
  }
  print("INTO");
  // GetTargetPathForNested() is strictly more general than "ForNonNested()".
  node->GetTargetPathForNested()->Accept(this, data);

  if (node->column_list() != nullptr) {
    node->column_list()->Accept(this, data);
  }

  println();

  if (node->rows() != nullptr) {
    node->rows()->Accept(this, data);
  }

  if (node->query() != nullptr) {
    node->query()->Accept(this, data);
  }

  if (node->assert_rows_modified() != nullptr) {
    node->assert_rows_modified()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTUpdateSetValue(const ASTUpdateSetValue* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseChildrenWithSeparator(node, data, "=");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTUpdateItem(const ASTUpdateItem* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  // If we don't have set_value, we have one of the statement types, which
  // require parentheses.
  if (node->set_value() == nullptr) {
    println();
    println("(");
    {
      Formatter::Indenter indenter(&formatter_);
      visitASTChildren(node, data);
      PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
    }
    println();
    print(")");
  } else {
    visitASTChildren(node, data);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
}

void Unparser::visitASTUpdateItemList(const ASTUpdateItemList* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseChildrenWithSeparator(node, data, ",", true /* break_line */);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTUpdateStatement(const ASTUpdateStatement* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("UPDATE");
  // GetTargetPathForNested() is strictly more general than "ForNonNested()".
  node->GetTargetPathForNested()->Accept(this, data);
  if (node->alias() != nullptr) {
    node->alias()->Accept(this, data);
  }
  if (node->offset() != nullptr) {
    node->offset()->Accept(this, data);
  }
  println();
  println("SET");
  {
    Formatter::Indenter indenter(&formatter_);
    node->update_item_list()->Accept(this, data);
  }
  if (node->from_clause() != nullptr) {
    node->from_clause()->Accept(this, data);
  }
  if (node->where() != nullptr) {
    println();
    println("WHERE");
    {
      Formatter::Indenter indenter(&formatter_);
      node->where()->Accept(this, data);
    }
  }
  if (node->assert_rows_modified() != nullptr) {
    node->assert_rows_modified()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTTruncateStatement(const ASTTruncateStatement* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("TRUNCATE TABLE");

  node->target_path()->Accept(this, data);

  if (node->where() != nullptr) {
    println();
    println("WHERE");
    {
      Formatter::Indenter indenter(&formatter_);
      node->where()->Accept(this, data);
      PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
    }
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTMergeAction(const ASTMergeAction* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  switch (node->action_type()) {
    case ASTMergeAction::INSERT:
      print("INSERT");
      if (node->insert_column_list() != nullptr) {
        node->insert_column_list()->Accept(this, data);
      }
      println();
      DCHECK(node->insert_row() != nullptr);
      if (!node->insert_row()->values().empty()) {
        println("VALUES");
        {
          Formatter::Indenter indenter(&formatter_);
          node->insert_row()->Accept(this, data);
        }
      } else {
        println("ROW");
      }
      break;
    case ASTMergeAction::UPDATE:
      print("UPDATE");
      println();
      println("SET");
      {
        Formatter::Indenter indenter(&formatter_);
        node->update_item_list()->Accept(this, data);
      }
      break;
    case ASTMergeAction::DELETE:
      print("DELETE");
      break;
    case ASTMergeAction::NOT_SET:
      LOG(DFATAL) << "Merge clause action type is not set";
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTMergeWhenClause(const ASTMergeWhenClause* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  const ASTMergeAction* action = node->action();
  switch (node->match_type()) {
    case ASTMergeWhenClause::MATCHED:
      print("WHEN MATCHED");
      break;
    case ASTMergeWhenClause::NOT_MATCHED_BY_SOURCE:
      print("WHEN NOT MATCHED BY SOURCE");
      break;
    case ASTMergeWhenClause::NOT_MATCHED_BY_TARGET:
      print("WHEN NOT MATCHED BY TARGET");
      break;
    case ASTMergeWhenClause::NOT_SET:
      LOG(DFATAL) << "Match type of merge match clause is not set.";
  }
  if (node->search_condition() != nullptr) {
    print("AND");
    node->search_condition()->Accept(this, data);
  }
  print("THEN");
  Formatter::Indenter indenter(&formatter_);
  action->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTMergeWhenClauseList(const ASTMergeWhenClauseList* node,
                                           void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  UnparseChildrenWithSeparator(node, data, "", true /* break_line */);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTMergeStatement(const ASTMergeStatement* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println();
  print("MERGE INTO");
  node->target_path()->Accept(this, data);
  if (node->alias() != nullptr) {
    node->alias()->Accept(this, data);
  }
  println();
  print("USING");
  node->table_expression()->Accept(this, data);
  println();
  print("ON");
  node->merge_condition()->Accept(this, data);
  node->when_clauses()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTPrimaryKey(const ASTPrimaryKey* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("PRIMARY KEY");
  if (node->column_list() == nullptr) {
    print("()");
  } else {
    node->column_list()->Accept(this, data);
  }
  if (!node->enforced()) {
    print("NOT ENFORCED");
  }
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    Formatter::Indenter indenter(&formatter_);
    node->options_list()->Accept(this, data);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTPrivilege(const ASTPrivilege* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->privilege_action()->Accept(this, data);
  if (node->column_list() != nullptr) {
    node->column_list()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTPrivileges(const ASTPrivileges* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->is_all_privileges()) {
    print("ALL PRIVILEGES");
  } else {
    UnparseChildrenWithSeparator(node, data, ",");
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTGranteeList(const ASTGranteeList* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseChildrenWithSeparator(node, data, ",");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTGrantStatement(const ASTGrantStatement* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("GRANT");
  node->privileges()->Accept(this, data);
  print("ON");
  if (node->target_type() != nullptr) {
    node->target_type()->Accept(this, data);
  }
  node->target_path()->Accept(this, data);
  print("TO");
  node->grantee_list()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTRevokeStatement(const ASTRevokeStatement* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("REVOKE");
  node->privileges()->Accept(this, data);
  print("ON");
  if (node->target_type() != nullptr) {
    node->target_type()->Accept(this, data);
  }
  node->target_path()->Accept(this, data);
  print("FROM");
  node->grantee_list()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTRepeatableClause(const ASTRepeatableClause* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("REPEATABLE (");
  node->argument()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  print(")");
}

void Unparser::visitASTReplaceFieldsArg(const ASTReplaceFieldsArg* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->expression()->Accept(this, data);
  print("AS ");
  node->path_expression()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTReplaceFieldsExpression(
    const ASTReplaceFieldsExpression* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("REPLACE_FIELDS(");
  node->expr()->Accept(this, data);
  print(", ");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->arguments(), data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
}

void Unparser::visitASTFilterFieldsArg(const ASTFilterFieldsArg* node,
                                       void* data) {
  std::string path_expression = Unparse(node->path_expression());
  DCHECK_EQ(path_expression.back(), '\n');
  path_expression.pop_back();
  print(absl::StrCat(node->GetSQLForOperator(), path_expression));
}

void Unparser::visitASTFilterFieldsExpression(
    const ASTFilterFieldsExpression* node, void* data) {
  print("FILTER_FIELDS(");
  node->expr()->Accept(this, data);
  print(", ");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->arguments(), data, ",");
  }
  print(")");
}

void Unparser::visitASTSampleSize(const ASTSampleSize* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->size()->Accept(this, data);
  print(node->GetSQLForUnit());
  if (node->partition_by() != nullptr) {
    node->partition_by()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTSampleSuffix(const ASTSampleSuffix* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->weight() != nullptr) {
    node->weight()->Accept(this, data);
  }
  if (node->repeat() != nullptr) {
    node->repeat()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTWithWeight(const ASTWithWeight* node, void *data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("WITH WEIGHT");
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTSampleClause(const ASTSampleClause* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("TABLESAMPLE");
  node->sample_method()->Accept(this, data);
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    node->sample_size()->Accept(this, data);
  }
  print(")");
  if (node->sample_suffix()) {
    node->sample_suffix()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::VisitAlterStatementBase(const ASTAlterStatementBase* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->path()->Accept(this, data);
  node->action_list()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAlterMaterializedViewStatement(
    const ASTAlterMaterializedViewStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ALTER MATERIALIZED VIEW");
  VisitAlterStatementBase(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAlterDatabaseStatement(
    const ASTAlterDatabaseStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ALTER DATABASE");
  VisitAlterStatementBase(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAlterTableStatement(const ASTAlterTableStatement* node,
                                           void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ALTER TABLE");
  VisitAlterStatementBase(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAlterViewStatement(const ASTAlterViewStatement* node,
                                          void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ALTER VIEW");
  VisitAlterStatementBase(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTSetOptionsAction(const ASTSetOptionsAction* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("SET OPTIONS");
  node->options_list()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::VisitCheckConstraintSpec(const ASTCheckConstraint* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("CHECK");
  print("(");
  node->expression()->Accept(this, data);
  print(")");
  if (!node->is_enforced()) {
    print("NOT");
  }
  print("ENFORCED");
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAddConstraintAction(const ASTAddConstraintAction* node,
                                           void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ADD");
  auto* constraint = node->constraint();
  if (constraint->constraint_name() != nullptr) {
    print("CONSTRAINT");
    if (node->is_if_not_exists()) {
      print("IF NOT EXISTS");
    }
    constraint->constraint_name()->Accept(this, data);
  }
  auto node_kind = constraint->node_kind();
  if (node_kind == AST_CHECK_CONSTRAINT) {
    VisitCheckConstraintSpec(constraint->GetAs<ASTCheckConstraint>(), data);
  } else if (node_kind == AST_FOREIGN_KEY) {
    VisitForeignKeySpec(constraint->GetAs<ASTForeignKey>(), data);
  } else {
    LOG(FATAL) << "Unknown constraint node kind: "
               << ASTNode::NodeKindToString(node_kind);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDropConstraintAction(const ASTDropConstraintAction* node,
                                            void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("DROP CONSTRAINT");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->constraint_name()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAlterConstraintEnforcementAction(
    const ASTAlterConstraintEnforcementAction* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ALTER CONSTRAINT");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->constraint_name()->Accept(this, data);
  if (!node->is_enforced()) {
    print("NOT");
  }
  print("ENFORCED");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAlterConstraintSetOptionsAction(
    const ASTAlterConstraintSetOptionsAction* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ALTER CONSTRAINT");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->constraint_name()->Accept(this, data);
  print("SET OPTIONS");
  node->options_list()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAddColumnAction(const ASTAddColumnAction* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ADD COLUMN");
  if (node->is_if_not_exists()) {
    print("IF NOT EXISTS");
  }
  node->column_definition()->Accept(this, data);
  if (node->column_position()) {
    node->column_position()->Accept(this, data);
  }
  if (node->fill_expression()) {
    print("FILL USING");
    node->fill_expression()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTColumnPosition(const ASTColumnPosition* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(node->type() == ASTColumnPosition::PRECEDING ? "PRECEDING"
                                                     : "FOLLOWING");
  node->identifier()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTDropColumnAction(const ASTDropColumnAction* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("DROP COLUMN");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->column_name()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAlterColumnOptionsAction(
    const ASTAlterColumnOptionsAction* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ALTER COLUMN");
  node->column_name()->Accept(this, data);
  print("SET OPTIONS");
  node->options_list()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAlterColumnTypeAction(
    const ASTAlterColumnTypeAction* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ALTER COLUMN");
  node->column_name()->Accept(this, data);
  print("SET DATA TYPE");
  node->schema()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTRevokeFromClause(const ASTRevokeFromClause* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("REVOKE FROM ");
  if (node->is_revoke_from_all()) {
    print("ALL");
  } else {
    print("(");
    node->revoke_from_list()->Accept(this, data);
    print(")");
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTRenameToClause(const ASTRenameToClause* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("RENAME TO");
  node->new_name()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAlterActionList(const ASTAlterActionList* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  Formatter::Indenter indenter(&formatter_);
  UnparseChildrenWithSeparator(node, data, ",");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAlterRowAccessPolicyStatement(
    const ASTAlterRowAccessPolicyStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ALTER ROW ACCESS POLICY");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
  print("ON");
  node->path()->Accept(this, data);
  node->action_list()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAlterAllRowAccessPoliciesStatement(
    const ASTAlterAllRowAccessPoliciesStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ALTER ALL ROW ACCESS POLICIES ON");
  node->table_name_path()->Accept(this, data);
  node->alter_action()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCreateIndexStatement(const ASTCreateIndexStatement* node,
                                            void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("CREATE");
  if (node->is_or_replace()) print("OR REPLACE");
  if (node->is_unique()) print("UNIQUE");
  print("INDEX");
  if (node->is_if_not_exists()) print("IF NOT EXISTS");
  node->name()->Accept(this, data);
  print("ON");
  node->table_name()->Accept(this, data);
  if (node->optional_table_alias() != nullptr) {
    node->optional_table_alias()->Accept(this, data);
  }
  if (node->optional_index_unnest_expression_list() != nullptr) {
    println();
    node->optional_index_unnest_expression_list()->Accept(this, data);
    println();
  }
  node->index_item_list()->Accept(this, data);
  if (node->optional_index_storing_expressions() != nullptr) {
    println();
    node->optional_index_storing_expressions()->Accept(this, data);
  }
  if (node->options_list() != nullptr) {
    println();
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTStatementList(const ASTStatementList* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  for (const ASTStatement* statement : node->statement_list()) {
    statement->Accept(this, data);
    println(";");
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTElseifClause(const ASTElseifClause* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ELSEIF");
  node->condition()->Accept(this, data);
  print("THEN");
  {
    Formatter::Indenter indenter(&formatter_);
    node->body()->Accept(this, data);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  println();
}

void Unparser::visitASTElseifClauseList(const ASTElseifClauseList* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  for (const ASTElseifClause* else_if_clause : node->elseif_clauses()) {
    else_if_clause->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTIfStatement(const ASTIfStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("IF");
  node->condition()->Accept(this, data);
  println("THEN");
  {
    Formatter::Indenter indenter(&formatter_);
    node->then_list()->Accept(this, data);
  }
  if (node->elseif_clauses() != nullptr) {
    node->elseif_clauses()->Accept(this, data);
  }
  if (node->else_list() != nullptr) {
    PrintCommentsPassedBy(node->else_list()->GetParseLocationRange().start(), data);
    println();
    println("ELSE");
    Formatter::Indenter indenter(&formatter_);
    node->else_list()->Accept(this, data);
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  println();
  print("END IF");
}

void Unparser::visitASTBeginEndBlock(const ASTBeginEndBlock* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println("BEGIN");
  {
    Formatter::Indenter indenter(&formatter_);
    node->statement_list_node()->Accept(this, data);
  }
  if (node->handler_list() != nullptr) {
    node->handler_list()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  println("END");
}

void Unparser::visitASTIndexItemList(const ASTIndexItemList* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("(");
  UnparseVectorWithSeparator(node->ordering_expressions(), data, ",");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  print(")");
}

void Unparser::visitASTIndexStoringExpressionList(
    const ASTIndexStoringExpressionList* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("STORING(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->expressions(), data, ",");
    PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
  }
  print(")");
}

void Unparser::visitASTIndexUnnestExpressionList(
    const ASTIndexUnnestExpressionList* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseVectorWithSeparator(node->unnest_expressions(), data, "");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::VisitForeignKeySpec(const ASTForeignKey* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("FOREIGN KEY");
  node->column_list()->Accept(this, data);
  node->reference()->Accept(this, data);
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTForeignKey(const ASTForeignKey* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->constraint_name() != nullptr) {
    print("CONSTRAINT");
    node->constraint_name()->Accept(this, data);
  }
  VisitForeignKeySpec(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTForeignKeyReference(
    const ASTForeignKeyReference* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("REFERENCES");
  node->table_name()->Accept(this, data);
  node->column_list()->Accept(this, data);
  print("MATCH");
  print(node->GetSQLForMatch());
  node->actions()->Accept(this, data);
  if (!node->enforced()) {
    print("NOT");
  }
  print("ENFORCED");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTForeignKeyActions(
    const ASTForeignKeyActions* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("ON UPDATE");
  print(ASTForeignKeyActions::GetSQLForAction(node->update_action()));
  print("ON DELETE");
  print(ASTForeignKeyActions::GetSQLForAction(node->delete_action()));
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCheckConstraint(const ASTCheckConstraint* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->constraint_name() != nullptr) {
    print("CONSTRAINT");
    node->constraint_name()->Accept(this, data);
  }
  VisitCheckConstraintSpec(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTIdentifierList(const ASTIdentifierList* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  UnparseVectorWithSeparator(node->identifier_list(), data, ", ");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTVariableDeclaration(const ASTVariableDeclaration* node,
                                           void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("DECLARE");
  node->variable_list()->Accept(this, data);
  if (node->type() != nullptr) {
    node->type()->Accept(this, data);
  }
  if (node->default_value() != nullptr) {
    print("DEFAULT");
    node->default_value()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTSingleAssignment(const ASTSingleAssignment* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("SET");
  node->variable()->Accept(this, data);
  print("=");
  node->expression()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTParameterAssignment(const ASTParameterAssignment* node,
                                           void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("SET");
  node->parameter()->Accept(this, data);
  print("=");
  node->expression()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTSystemVariableAssignment(
    const ASTSystemVariableAssignment* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("SET");
  node->system_variable()->Accept(this, data);
  print("=");
  node->expression()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTAssignmentFromStruct(const ASTAssignmentFromStruct* node,
                                  void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("SET");
  print("(");
  for (const ASTIdentifier* variable : node->variables()->identifier_list()) {
    variable->Accept(this, data);
    if (variable != node->variables()->identifier_list().back()) {
      print(",");
    }
  }
  print(")");
  print("=");
  node->struct_expression()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTWhileStatement(const ASTWhileStatement* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  if (node->condition() != nullptr) {
    print("WHILE");
    node->condition()->Accept(this, data);
    println("DO");
    {
      Formatter::Indenter indenter(&formatter_);
      node->body()->Accept(this, data);
      PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
    }
    print("END");
    print("WHILE");
  } else {
    println("LOOP");
    {
      Formatter::Indenter indenter(&formatter_);
      node->body()->Accept(this, data);
      PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
    }
    print("END");
    print("LOOP");
  }
}

void Unparser::visitASTScript(const ASTScript* node, void* data) {
  node->statement_list_node()->Accept(this, data);
}

void Unparser::visitASTBreakStatement(const ASTBreakStatement* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(node->GetKeywordText());
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTContinueStatement(const ASTContinueStatement* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(node->GetKeywordText());
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTReturnStatement(const ASTReturnStatement* node,
                                       void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("RETURN");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTCreateProcedureStatement(
    const ASTCreateProcedureStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print(GetCreateStatementPrefix(node, "PROCEDURE"));
  node->name()->Accept(this, data);
  node->parameters()->Accept(this, data);
  println();
  if (node->options_list() != nullptr) {
    println("OPTIONS");
    Formatter::Indenter indenter(&formatter_);
    node->options_list()->Accept(this, data);
    println();
  }
  node->begin_end_block()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTNamedArgument(const ASTNamedArgument* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  node->name()->Accept(this, data);
  print(" => ");
  node->expr()->Accept(this, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTLambda(const ASTLambda* node, void* data) {
  const ASTExpression* argument_list = node->argument_list();
  // Check if the parameter list expression will print the parentheses.
  const bool already_parenthesized =
      argument_list->parenthesized() ||
      argument_list->node_kind() == AST_STRUCT_CONSTRUCTOR_WITH_PARENS;
  if (!already_parenthesized) {
    print("(");
  }
  node->argument_list()->Accept(this, data);
  if (!already_parenthesized) {
    print(")");
  }
  print("-> ");
  node->body()->Accept(this, data);
}

void Unparser::visitASTExceptionHandler(const ASTExceptionHandler* node,
                                        void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("WHEN ERROR THEN");
  Formatter::Indenter indenter(&formatter_);
  node->statement_list()->Accept(this, data);
  println();
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}
void Unparser::visitASTExceptionHandlerList(const ASTExceptionHandlerList* node,
                                            void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  println("EXCEPTION");
  for (const ASTExceptionHandler* handler : node->exception_handler_list()) {
    handler->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}
void Unparser::visitASTExecuteIntoClause(const ASTExecuteIntoClause* node,
                                         void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("INTO");
  UnparseChildrenWithSeparator(node, data, ", ");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}
void Unparser::visitASTExecuteUsingArgument(const ASTExecuteUsingArgument* node,
                                            void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  visitASTChildren(node, data);
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}
void Unparser::visitASTExecuteUsingClause(const ASTExecuteUsingClause* node,
                                          void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("USING");
  UnparseChildrenWithSeparator(node, data, ", ");
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}
void Unparser::visitASTExecuteImmediateStatement(
    const ASTExecuteImmediateStatement* node, void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("EXECUTE IMMEDIATE");
  node->sql()->Accept(this, data);
  if (node->into_clause() != nullptr) {
    node->into_clause()->Accept(this, data);
  }
  if (node->using_clause() != nullptr) {
    node->using_clause()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

void Unparser::visitASTRaiseStatement(const ASTRaiseStatement* node,
                                      void* data) {
  PrintCommentsPassedBy(node->GetParseLocationRange().start(), data);
  print("RAISE");
  if (node->message() != nullptr) {
    print("USING MESSAGE =");
    node->message()->Accept(this, data);
  }
  PrintCommentsPassedBy(node->GetParseLocationRange().end(), data);
}

}  // namespace parser
}  // namespace zetasql
