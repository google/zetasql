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
#include <string>
#include <utility>

#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.h"
#include "zetasql/base/case.h"
#include "absl/flags/flag.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/map_util.h"

ABSL_DECLARE_FLAG(bool, output_asc_explicitly);

namespace zetasql {

std::string Unparse(const ASTNode* node) {
  std::string unparsed_;
  parser::Unparser unparser(&unparsed_);
  node->Accept(&unparser, nullptr);
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
  ZETASQL_CHECK_GE(indentation_.size(), kDefaultNumIndentSpaces)
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
          if (last_was_single_char_unary_) {
            absl::StrAppend(&buffer_, s);
          } else if (curr_char == '(') {
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

  // Reset last_was_single_char_unary_. If this was called with a unary it'll
  // get set after this returns.
  last_was_single_char_unary_ = false;
}

void Formatter::FormatLine(absl::string_view s) {
  Format(s);
  FlushLine();
}

void Formatter::AddUnary(absl::string_view s) {
  if (last_was_single_char_unary_ && absl::EndsWith(buffer_, "-") && s == "-") {
    // Pretend it's not a unary so we don't get '--' which is a comment.
    last_was_single_char_unary_ = false;
  }
  Format(s);
  last_was_single_char_unary_ = s.size() == 1;
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
  ZETASQL_DCHECK(node->IsExpression() || node->IsQueryExpression())
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
  ZETASQL_DCHECK(node->IsExpression() || node->IsQueryExpression())
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
      if (break_line) {
        println(separator);
      } else {
        print(separator);
      }
    }
    node->child(i)->Accept(this, data);
  }
}

// Visitor implementation.

void Unparser::visitASTHintedStatement(const ASTHintedStatement* node,
                                       void* data) {
  visitASTChildren(node, data);
}

void Unparser::visitASTExplainStatement(const ASTExplainStatement* node,
                                        void* data) {
  print("EXPLAIN");
  node->statement()->Accept(this, data);
}

void Unparser::visitASTQueryStatement(const ASTQueryStatement* node,
                                      void* data) {
  visitASTQuery(node->query(), data);
}

void Unparser::visitASTFunctionParameter(
    const ASTFunctionParameter* node, void* data) {
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
  if (node->default_value() != nullptr) {
    print("DEFAULT");
    node->default_value()->Accept(this, data);
  }
  if (node->is_not_aggregate()) {
    print("NOT AGGREGATE");
  }
}

void Unparser::visitASTTemplatedParameterType(
    const ASTTemplatedParameterType* node, void* data) {
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
}

void Unparser::visitASTFunctionParameters(
    const ASTFunctionParameters* node, void* data) {
  print("(");
  for (int i = 0; i < node->num_children(); ++i) {
    if (i > 0) {
      print(", ");
    }
    node->child(i)->Accept(this, data);
  }
  print(")");
}

void Unparser::visitASTFunctionDeclaration(
    const ASTFunctionDeclaration* node, void* data) {
  node->name()->Accept(this, data);
  node->parameters()->Accept(this, data);
}

void Unparser::visitASTSqlFunctionBody(
    const ASTSqlFunctionBody* node, void* data) {
  node->expression()->Accept(this, data);
}

void Unparser::visitASTTableClause(const ASTTableClause* node, void* data) {
  print("TABLE ");
  if (node->table_path() != nullptr) {
    node->table_path()->Accept(this, data);
  }
  if (node->tvf() != nullptr) {
    node->tvf()->Accept(this, data);
  }
}

void Unparser::visitASTModelClause(const ASTModelClause* node, void* data) {
  print("MODEL ");
  node->model_path()->Accept(this, data);
}

void Unparser::visitASTConnectionClause(const ASTConnectionClause* node,
                                        void* data) {
  print("CONNECTION ");
  node->connection_path()->Accept(this, data);
}

void Unparser::visitASTTVF(const ASTTVF* node, void* data) {
  node->name()->Accept(this, data);
  print("(");
  UnparseVectorWithSeparator(node->argument_entries(), data, ",");
  print(")");
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  if (node->pivot_clause() != nullptr) {
    node->pivot_clause()->Accept(this, data);
  }
  if (node->unpivot_clause() != nullptr) {
    node->unpivot_clause()->Accept(this, data);
  }
  if (node->alias() != nullptr) {
    node->alias()->Accept(this, data);
  }
  if (node->sample() != nullptr) {
    node->sample()->Accept(this, data);
  }
}

void Unparser::visitASTTVFArgument(const ASTTVFArgument* node, void* data) {
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
}

void Unparser::visitASTTVFSchema(const ASTTVFSchema* node, void* data) {
  print("TABLE<");
  UnparseChildrenWithSeparator(node, data, ",");
  print(">");
}

void Unparser::visitASTTVFSchemaColumn(const ASTTVFSchemaColumn* node,
                                       void* data) {
  UnparseChildrenWithSeparator(node, data, "");
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
  print(GetCreateStatementPrefix(node, "CONSTANT"));
  node->name()->Accept(this, data);
  print("=");
  node->expr()->Accept(this, data);
}

void Unparser::visitASTCreateDatabaseStatement(
    const ASTCreateDatabaseStatement* node, void* data) {
  print("CREATE DATABASE");
  node->name()->Accept(this, data);
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
}

void Unparser::visitASTCreateFunctionStatement(
    const ASTCreateFunctionStatement* node, void* data) {
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
  if (node->is_remote()) {
    print("REMOTE");
  }
  if (node->with_connection_clause() != nullptr) {
    node->with_connection_clause()->Accept(this, data);
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
}

void Unparser::visitASTCreateSchemaStatement(
    const ASTCreateSchemaStatement* node, void* data) {
  print(GetCreateStatementPrefix(node, "SCHEMA"));
  node->name()->Accept(this, data);
  if (node->collate() != nullptr) {
    print("DEFAULT");
    visitASTCollate(node->collate(), data);
  }
  if (node->options_list() != nullptr) {
    println();
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
}

void Unparser::visitASTCreateTableFunctionStatement(
    const ASTCreateTableFunctionStatement* node, void* data) {
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
}

void Unparser::visitASTAuxLoadDataFromFilesOptionsList(
    const ASTAuxLoadDataFromFilesOptionsList* node, void* data) {
  node->options_list()->Accept(this, data);
}

void Unparser::visitASTAuxLoadDataStatement(
    const ASTAuxLoadDataStatement* node, void* data) {
  print("LOAD DATA");
  switch (node->insertion_mode()) {
    case ASTAuxLoadDataStatement::InsertionMode::APPEND:
      print("INTO");
      break;
    case ASTAuxLoadDataStatement::InsertionMode::OVERWRITE:
      print("OVERWRITE");
      break;
    default:
      break;
  }
  node->name()->Accept(this, data);
  if (node->table_element_list() != nullptr) {
    node->table_element_list()->Accept(this, data);
  }
  if (node->collate() != nullptr) {
    visitASTCollate(node->collate(), data);
  }
  if (node->partition_by() != nullptr) {
    println();
    node->partition_by()->Accept(this, data);
  }
  if (node->cluster_by() != nullptr) {
    println();
    node->cluster_by()->Accept(this, data);
  }
  if (node->options_list() != nullptr) {
    println();
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  println();
  print("FROM FILES");
  node->from_files()->Accept(this, data);
  if (node->with_partition_columns_clause() != nullptr) {
    println();
    node->with_partition_columns_clause()->Accept(this, data);
  }
  if (node->with_connection_clause() != nullptr) {
    println();
    node->with_connection_clause()->Accept(this, data);
  }
}

void Unparser::visitASTCreateTableStatement(
    const ASTCreateTableStatement* node, void* data) {
  print(GetCreateStatementPrefix(node, "TABLE"));
  node->name()->Accept(this, data);
  if (node->table_element_list() != nullptr) {
    println();
    node->table_element_list()->Accept(this, data);
  }
  if (node->collate() != nullptr) {
    print("DEFAULT");
    visitASTCollate(node->collate(), data);
  }
  if (node->like_table_name() != nullptr) {
    println("LIKE");
    node->like_table_name()->Accept(this, data);
  }
  if (node->clone_data_source() != nullptr) {
    println("CLONE");
    node->clone_data_source()->Accept(this, data);
  }
  if (node->copy_data_source() != nullptr) {
    println("COPY");
    node->copy_data_source()->Accept(this, data);
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
    print("AS ");
    node->json_body()->Accept(this, data);
  }
  if (node->text_body() != nullptr) {
    println();
    print("AS");
    node->text_body()->Accept(this, data);
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
}

void Unparser::visitASTTableElementList(const ASTTableElementList* node,
                                        void* data) {
  println("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",", true /* break_line */);
  }
  println();
  print(")");
}

void Unparser::visitASTNotNullColumnAttribute(
    const ASTNotNullColumnAttribute* node, void* data) {
  print("NOT NULL");
}

void Unparser::visitASTHiddenColumnAttribute(
    const ASTHiddenColumnAttribute* node, void* data) {
  print("HIDDEN");
}

void Unparser::visitASTPrimaryKeyColumnAttribute(
    const ASTPrimaryKeyColumnAttribute* node, void* data) {
  print("PRIMARY KEY");
  if (!node->enforced()) {
    print("NOT ENFORCED");
  }
}

void Unparser::visitASTForeignKeyColumnAttribute(
    const ASTForeignKeyColumnAttribute* node, void* data) {
  if (node->constraint_name() != nullptr) {
    print("CONSTRAINT");
    node->constraint_name()->Accept(this, data);
  }
  node->reference()->Accept(this, data);
}

void Unparser::visitASTColumnAttributeList(
    const ASTColumnAttributeList* node, void* data) {
  UnparseChildrenWithSeparator(node, data, "", /*break_line=*/false);
}

void Unparser::visitASTColumnDefinition(const ASTColumnDefinition* node,
                                        void* data) {
  if (node->name() != nullptr) {
    node->name()->Accept(this, data);
  }
  if (node->schema() != nullptr) {
    node->schema()->Accept(this, data);
  }
}

void Unparser::visitASTCreateViewStatement(
    const ASTCreateViewStatement* node, void* data) {
  print(GetCreateStatementPrefix(node, "VIEW"));
  node->name()->Accept(this, data);
  if (node->column_list() != nullptr) {
    node->column_list()->Accept(this, data);
  }
  if (node->sql_security() != ASTCreateStatement::SQL_SECURITY_UNSPECIFIED) {
    print(node->GetSqlForSqlSecurity());
  }
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  println("AS");
  node->query()->Accept(this, data);
}

void Unparser::visitASTCreateMaterializedViewStatement(
    const ASTCreateMaterializedViewStatement* node, void* data) {
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
  if (node->column_list() != nullptr) {
    node->column_list()->Accept(this, data);
  }
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
  print(GetCreateStatementPrefix(node, "EXTERNAL TABLE"));
  node->name()->Accept(this, data);
  if (node->table_element_list() != nullptr) {
    println();
    node->table_element_list()->Accept(this, data);
  }
  if (node->collate() != nullptr) {
    print("DEFAULT");
    visitASTCollate(node->collate(), data);
  }
  if (node->like_table_name() != nullptr) {
    println("LIKE");
    node->like_table_name()->Accept(this, data);
  }
  if (node->with_partition_columns_clause() != nullptr) {
    node->with_partition_columns_clause()->Accept(this, data);
  }
  if (node->with_connection_clause() != nullptr) {
    node->with_connection_clause()->Accept(this, data);
  }
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
}

void Unparser::visitASTCreateSnapshotTableStatement(
    const ASTCreateSnapshotTableStatement* node, void* data) {
  print("CREATE");
  if (node->is_or_replace()) print("OR REPLACE");
  print("SNAPSHOT TABLE");
  if (node->is_if_not_exists()) print("IF NOT EXISTS");
  node->name()->Accept(this, data);
  print("CLONE");
  node->clone_data_source()->Accept(this, data);
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
}

void Unparser::visitASTGrantToClause(const ASTGrantToClause* node, void* data) {
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
}

void Unparser::visitASTRestrictToClause(const ASTRestrictToClause* node,
                                        void* data) {
  print("RESTRICT TO (");
  node->restrictee_list()->Accept(this, data);
  print(")");
}

void Unparser::visitASTAddToRestricteeListClause(
    const ASTAddToRestricteeListClause* node, void* data) {
  print("ADD ");
  if (node->is_if_not_exists()) {
    print("IF NOT EXISTS ");
  }
  print("(");
  node->restrictee_list()->Accept(this, data);
  print(")");
}

void Unparser::visitASTRemoveFromRestricteeListClause(
    const ASTRemoveFromRestricteeListClause* node, void* data) {
  print("REMOVE ");
  if (node->is_if_exists()) {
    print("IF EXISTS ");
  }
  print("(");
  node->restrictee_list()->Accept(this, data);
  print(")");
}

void Unparser::visitASTFilterUsingClause(const ASTFilterUsingClause* node,
                                         void* data) {
  if (node->has_filter_keyword()) {
    print("FILTER");
  }
  print("USING (");
  node->predicate()->Accept(this, data);
  print(")");
}

void Unparser::visitASTCreatePrivilegeRestrictionStatement(
    const ASTCreatePrivilegeRestrictionStatement* node, void* data) {
  print("CREATE");
  if (node->is_or_replace()) {
    print("OR REPLACE");
  }
  print("PRIVILEGE RESTRICTION");
  if (node->is_if_not_exists()) {
    print("IF NOT EXISTS");
  }
  print("ON");
  node->privileges()->Accept(this, data);
  print("ON");
  node->object_type()->Accept(this, data);
  node->name_path()->Accept(this, data);
  if (node->restrict_to() != nullptr) {
    node->restrict_to()->Accept(this, data);
  }
}

void Unparser::visitASTCreateRowAccessPolicyStatement(
    const ASTCreateRowAccessPolicyStatement* node, void* data) {
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
}

void Unparser::visitASTExportDataStatement(
    const ASTExportDataStatement* node, void* data) {
  print("EXPORT DATA");
  if (node->with_connection_clause() != nullptr) {
    node->with_connection_clause()->Accept(this, data);
  }

  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
  println("AS");
  node->query()->Accept(this, data);
}

void Unparser::visitASTExportModelStatement(const ASTExportModelStatement* node,
                                            void* data) {
  print("EXPORT MODEL");
  if (node->model_name_path() != nullptr) {
    node->model_name_path()->Accept(this, data);
  }

  if (node->with_connection_clause() != nullptr) {
    node->with_connection_clause()->Accept(this, data);
  }

  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
}

void Unparser::visitASTWithConnectionClause(const ASTWithConnectionClause* node,
                                            void* data) {
  print("WITH");
  node->connection_clause()->Accept(this, data);
}

void Unparser::visitASTCallStatement(
    const ASTCallStatement* node, void* data) {
  print("CALL");
  node->procedure_name()->Accept(this, data);
  print("(");
  UnparseVectorWithSeparator(node->arguments(), data, ",");
  print(")");
}

void Unparser::visitASTDefineTableStatement(
    const ASTDefineTableStatement* node, void* data) {
  print("DEFINE TABLE");
  node->name()->Accept(this, data);
  node->options_list()->Accept(this, data);
}

void Unparser::visitASTDescribeStatement(const ASTDescribeStatement* node,
                                         void* data) {
  print("DESCRIBE");
  if (node->optional_identifier() != nullptr) {
    node->optional_identifier()->Accept(this, data);
  }
  node->name()->Accept(this, data);
  if (node->optional_from_name() != nullptr) {
    print("FROM");
    node->optional_from_name()->Accept(this, data);
  }
}

void Unparser::visitASTDescriptorColumn(const ASTDescriptorColumn* node,
                                        void* data) {
  node->name()->Accept(this, data);
}

void Unparser::visitASTDescriptorColumnList(const ASTDescriptorColumnList* node,
                                            void* data) {
  UnparseChildrenWithSeparator(node, data, ", ");
}

void Unparser::visitASTDescriptor(const ASTDescriptor* node, void* data) {
  print("DESCRIPTOR(");
  node->columns()->Accept(this, data);
  print(")");
}

void Unparser::visitASTShowStatement(const ASTShowStatement* node, void* data) {
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
}

void Unparser::visitASTBeginStatement(
    const ASTBeginStatement* node, void* data) {
  print("BEGIN TRANSACTION");
  if (node->mode_list() != nullptr) {
    node->mode_list()->Accept(this, data);
  }
}

void Unparser::visitASTTransactionIsolationLevel(
    const ASTTransactionIsolationLevel* node, void* data) {
  if (node->identifier1() != nullptr) {
    print("ISOLATION LEVEL");
    node->identifier1()->Accept(this, data);
  }
  if (node->identifier2() != nullptr) {
    node->identifier2()->Accept(this, data);
  }
}

void Unparser::visitASTTransactionReadWriteMode(
    const ASTTransactionReadWriteMode* node, void* data) {
  switch (node->mode()) {
    case ASTTransactionReadWriteMode::READ_ONLY:
      print("READ ONLY");
      break;
    case ASTTransactionReadWriteMode::READ_WRITE:
      print("READ WRITE");
      break;
    case ASTTransactionReadWriteMode::INVALID:
      ZETASQL_LOG(DFATAL) << "invalid read write mode";
      break;
  }
}

void Unparser::visitASTTransactionModeList(const ASTTransactionModeList* node,
                                           void* data) {
  bool first = true;
  for (const ASTTransactionMode* mode : node->elements()) {
    if (!first) {
      print(",");
    } else {
      first = false;
    }
    mode->Accept(this, data);
  }
}

void Unparser::visitASTSetTransactionStatement(
    const ASTSetTransactionStatement* node, void* data) {
  print("SET TRANSACTION");
  node->mode_list()->Accept(this, data);
}

void Unparser::visitASTCommitStatement(const ASTCommitStatement* node,
                                       void* data) {
  print("COMMIT");
}

void Unparser::visitASTRollbackStatement(const ASTRollbackStatement* node,
                                         void* data) {
  print("ROLLBACK");
}

void Unparser::visitASTStartBatchStatement(const ASTStartBatchStatement* node,
                                           void* data) {
  print("START BATCH");
  if (node->batch_type() != nullptr) {
    node->batch_type()->Accept(this, data);
  }
}

void Unparser::visitASTRunBatchStatement(const ASTRunBatchStatement* node,
                                         void* data) {
  print("RUN BATCH");
}

void Unparser::visitASTAbortBatchStatement(const ASTAbortBatchStatement* node,
                                           void* data) {
  print("ABORT BATCH");
}

void Unparser::visitASTDropStatement(const ASTDropStatement* node, void* data) {
  print("DROP");
  print(SchemaObjectKindToName(node->schema_object_kind()));
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
  print(node->GetSQLForDropMode(node->drop_mode()));
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
  print("DROP FUNCTION");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
  if (node->parameters() != nullptr) {
    node->parameters()->Accept(this, data);
  }
}

void Unparser::visitASTDropTableFunctionStatement(
    const ASTDropTableFunctionStatement* node, void* data) {
  print("DROP TABLE FUNCTION");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
}

void Unparser::visitASTDropPrivilegeRestrictionStatement(
    const ASTDropPrivilegeRestrictionStatement* node, void* data) {
  print("DROP PRIVILEGE RESTRICTION");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  print("ON");
  node->privileges()->Accept(this, data);
  print("ON");
  node->object_type()->Accept(this, data);
  node->name_path()->Accept(this, data);
}

void Unparser::visitASTDropRowAccessPolicyStatement(
    const ASTDropRowAccessPolicyStatement* node, void* data) {
  print("DROP ROW ACCESS POLICY");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
  print("ON");
  node->table_name()->Accept(this, data);
}

void Unparser::visitASTDropAllRowAccessPoliciesStatement(
    const ASTDropAllRowAccessPoliciesStatement* node, void* data) {
  print("DROP ALL ROW");
  if (node->has_access_keyword()) {
    print("ACCESS");
  }
  print("POLICIES ON");
  node->table_name()->Accept(this, data);
}

void Unparser::visitASTDropSearchIndexStatement(
    const ASTDropSearchIndexStatement* node, void* data) {
  print("DROP SEARCH INDEX");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
  if (node->table_name() != nullptr) {
    print("ON");
    node->table_name()->Accept(this, data);
  }
}

void Unparser::visitASTDropMaterializedViewStatement(
    const ASTDropMaterializedViewStatement* node, void* data) {
  print("DROP MATERIALIZED VIEW");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
}

void Unparser::visitASTDropSnapshotTableStatement(
    const ASTDropSnapshotTableStatement* node, void* data) {
  print("DROP SNAPSHOT TABLE");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
}

void Unparser::visitASTRenameStatement(const ASTRenameStatement* node,
                                       void* data) {
  print("RENAME");
  if (node->identifier() != nullptr) {
    node->identifier()->Accept(this, data);
  }
  node->old_name()->Accept(this, data);
  print("TO");
  node->new_name()->Accept(this, data);
}

void Unparser::visitASTImportStatement(const ASTImportStatement* node,
                                       void* data) {
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
}

void Unparser::visitASTModuleStatement(const ASTModuleStatement* node,
                                       void* data) {
  print("MODULE");
  node->name()->Accept(this, data);
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
}

void Unparser::visitASTWithClause(const ASTWithClause* node,
                                  void* data) {
  if (node->recursive()) {
    println("WITH RECURSIVE");
  } else {
    println("WITH");
  }
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
  }
}

void Unparser::visitASTWithClauseEntry(const ASTWithClauseEntry* node,
                                       void *data) {
  println();
  node->alias()->Accept(this, data);
  println("AS (");
  {
    Formatter::Indenter indenter(&formatter_);
    visitASTQuery(node->query(), data);
  }
  println();
  print(")");
}

void Unparser::visitASTQuery(const ASTQuery* node, void* data) {
  PrintOpenParenIfNeeded(node);
  if (node->is_nested()) {
    println();
    print("(");
    {
      Formatter::Indenter indenter(&formatter_);
      visitASTChildren(node, data);
    }
    println();
    print(")");
  } else {
    visitASTChildren(node, data);
  }
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTSetOperation(const ASTSetOperation* node, void* data) {
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
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTSetAsAction(const ASTSetAsAction* node, void* data) {
  print("SET AS ");
  if (node->json_body() != nullptr) {
    node->json_body()->Accept(this, data);
  }
  if (node->text_body() != nullptr) {
    node->text_body()->Accept(this, data);
  }
}

void Unparser::visitASTSelect(const ASTSelect* node, void* data) {
  PrintOpenParenIfNeeded(node);
  println();
  print("SELECT");
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  if (node->anonymization_options() != nullptr) {
    print("WITH ANONYMIZATION OPTIONS");
    node->anonymization_options()->Accept(this, data);
  }
  if (node->distinct()) {
    print("DISTINCT");
  }

  // Visit all children except hint() and anonymization_options, which we
  // processed above.  We can't just use visitASTChildren(node, data) because
  // we need to insert the DISTINCT modifier after the hint and anonymization
  // nodes and before everything else.
  for (int i = 0; i < node->num_children(); ++i) {
    const ASTNode* child = node->child(i);
    if (child != node->hint() && child != node->anonymization_options()) {
      child->Accept(this, data);
    }
  }

  println();
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTSelectAs(const ASTSelectAs* node, void* data) {
  if (node->as_mode() != ASTSelectAs::TYPE_NAME) {
    print(absl::StrCat(
        "AS ", node->as_mode() == ASTSelectAs::VALUE ? "VALUE" : "STRUCT"));
  } else {
    print("AS");
  }
  visitASTChildren(node, data);
}

void Unparser::visitASTSelectList(const ASTSelectList* node, void* data) {
  println();
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",", true /* break_line */);
  }
}

void Unparser::visitASTSelectColumn(const ASTSelectColumn* node, void* data) {
  visitASTChildren(node, data);
}

void Unparser::visitASTAlias(const ASTAlias* node, void* data) {
  print(absl::StrCat("AS ",
                     ToIdentifierLiteral(node->identifier()->GetAsIdString())));
}

void Unparser::visitASTIntoAlias(const ASTIntoAlias* node, void* data) {
  print(absl::StrCat("INTO ",
                     ToIdentifierLiteral(node->identifier()->GetAsIdString())));
}

void Unparser::visitASTFromClause(const ASTFromClause* node, void* data) {
  println();
  println("FROM");
  {
    Formatter::Indenter indenter(&formatter_);
    visitASTChildren(node, data);
  }
}

void Unparser::visitASTTransformClause(const ASTTransformClause* node,
                                       void* data) {
  println("(");
  visitASTChildren(node, data);
  println(")");
}

void Unparser::visitASTWithOffset(const ASTWithOffset* node, void* data) {
  print("WITH OFFSET");
  visitASTChildren(node, data);
}

void Unparser::visitASTUnnestExpression(const ASTUnnestExpression* node,
                                        void* data) {
  print("UNNEST(");
  visitASTChildren(node, data);
  print(")");
}

void Unparser::visitASTUnnestExpressionWithOptAliasAndOffset(
    const ASTUnnestExpressionWithOptAliasAndOffset* node, void* data) {
  visitASTChildren(node, data);
}

void Unparser::visitASTTablePathExpression(
    const ASTTablePathExpression* node, void* data) {
  visitASTChildren(node, data);
}

void Unparser::visitASTPathExpressionList(const ASTPathExpressionList* node,
                                          void* data) {
  UnparseVectorWithSeparator(node->path_expression_list(), data, ", ");
}

void Unparser::visitASTForSystemTime(const ASTForSystemTime* node, void* data) {
  print("FOR SYSTEM_TIME AS OF ");
  visitASTChildren(node, data);
}

void Unparser::visitASTTableSubquery(
    const ASTTableSubquery* node, void* data) {
  visitASTChildren(node, data);
}

void Unparser::visitASTJoin(const ASTJoin* node, void* data) {
  node->child(0)->Accept(this, data);

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
}

void Unparser::visitASTParenthesizedJoin(const ASTParenthesizedJoin* node,
                                         void* data) {
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
}

void Unparser::visitASTOnClause(const ASTOnClause* node, void* data) {
  println();
  print("ON");
  {
    Formatter::Indenter indenter(&formatter_);
    visitASTChildren(node, data);
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
  println();
  print("USING(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
  }
  print(")");
}

void Unparser::visitASTWhereClause(const ASTWhereClause* node, void* data) {
  println();
  println("WHERE");
  {
    Formatter::Indenter indenter(&formatter_);
    visitASTChildren(node, data);
  }
}

void Unparser::visitASTRollup(const ASTRollup* node, void* data) {
  print("ROLLUP(");
  UnparseVectorWithSeparator(node->expressions(), data, ",");
  print(")");
}

void Unparser::visitASTGroupingItem(const ASTGroupingItem* node, void* data) {
  if (node->expression() != nullptr) {
    ZETASQL_DCHECK(node->rollup() == nullptr);
    node->expression()->Accept(this, data);
  } else {
    ZETASQL_DCHECK(node->rollup() != nullptr);
    node->rollup()->Accept(this, data);
  }
}

void Unparser::visitASTGroupBy(const ASTGroupBy* node, void* data) {
  println();
  print("GROUP");
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  print("BY");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->grouping_items(), data, ",");
  }
}

void Unparser::visitASTHaving(const ASTHaving* node, void* data) {
  println();
  print("HAVING");
  visitASTChildren(node, data);
}

void Unparser::visitASTQualify(const ASTQualify* node, void* data) {
  println();
  print("QUALIFY");
  visitASTChildren(node, data);
}

void Unparser::visitASTCollate(const ASTCollate* node, void* data) {
  print("COLLATE");
  visitASTChildren(node, data);
}

void Unparser::visitASTOrderBy(const ASTOrderBy* node, void* data) {
  println();
  print("ORDER");
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  print("BY");
  UnparseVectorWithSeparator(node->ordering_expressions(), data, ",");
}

void Unparser::visitASTNullOrder(const ASTNullOrder* node, void* data) {
  if (node->nulls_first()) {
    print("NULLS FIRST");
  } else {
    print("NULLS LAST");
  }
}

void Unparser::visitASTOrderingExpression(const ASTOrderingExpression* node,
                                          void* data) {
  node->expression()->Accept(this, data);
  if (node->collate()) node->collate()->Accept(this, data);
  if (node->descending()) {
    print("DESC");
  } else if (node->ordering_spec() == ASTOrderingExpression::ASC &&
             absl::GetFlag(FLAGS_output_asc_explicitly)) {
    print("ASC");
  }
  if (node->null_order()) node->null_order()->Accept(this, data);
}

void Unparser::visitASTLimitOffset(const ASTLimitOffset* node, void* data) {
  println();
  print("LIMIT");
  UnparseChildrenWithSeparator(node, data, "OFFSET");
}

void Unparser::visitASTHavingModifier(const ASTHavingModifier* node,
                                      void* data) {
  println();
  print("HAVING ");
  if (node->modifier_kind() == ASTHavingModifier::ModifierKind::MAX) {
    print("MAX");
  } else {
    print("MIN");
  }
  node->expr()->Accept(this, data);
}

void Unparser::visitASTClampedBetweenModifier(
    const ASTClampedBetweenModifier* node, void* data) {
  println();
  print("CLAMPED BETWEEN");
  UnparseChildrenWithSeparator(node, data, 0, node->num_children(), "AND");
}

void Unparser::UnparseASTTableDataSource(const ASTTableDataSource* node,
                                         void* data) {
  node->path_expr()->Accept(this, data);
  if (node->for_system_time() != nullptr) {
    println();
    Formatter::Indenter indenter(&formatter_);
    node->for_system_time()->Accept(this, data);
  }
  if (node->where_clause() != nullptr) {
    Formatter::Indenter indenter(&formatter_);
    node->where_clause()->Accept(this, data);
  }
  println();
}

void Unparser::visitASTCloneDataSourceList(const ASTCloneDataSourceList* node,
                                           void* data) {
  UnparseChildrenWithSeparator(node, data, "UNION ALL");
}

void Unparser::visitASTCloneDataStatement(const ASTCloneDataStatement* node,
                                          void* data) {
  print("CLONE DATA INTO");
  node->target_path()->Accept(this, data);
  println();
  print("FROM");
  node->data_source_list()->Accept(this, data);
}

void Unparser::visitASTIdentifier(const ASTIdentifier* node, void* data) {
  print(ToIdentifierLiteral(node->GetAsIdString()));
}

void Unparser::visitASTNewConstructorArg(const ASTNewConstructorArg* node,
                                         void* data) {
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
}

void Unparser::visitASTNewConstructor(const ASTNewConstructor* node,
                                      void* data) {
  print("NEW");
  node->type_name()->Accept(this, data);
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->arguments(), data, ",");
  }
  print(")");
}

void Unparser::visitASTInferredTypeColumnSchema(
    const ASTInferredTypeColumnSchema* node, void* data) {
  UnparseColumnSchema(node, data);
}

void Unparser::visitASTArrayConstructor(const ASTArrayConstructor* node,
                                        void* data) {
  if (node->type() != nullptr) {
    node->type()->Accept(this, data);
  } else {
    print("ARRAY");
  }
  print("[");
  UnparseVectorWithSeparator(node->elements(), data, ",");
  print("]");
}

void Unparser::visitASTStructConstructorArg(const ASTStructConstructorArg* node,
                                            void* data) {
  visitASTChildren(node, data);
}

void Unparser::visitASTStructConstructorWithParens(
    const ASTStructConstructorWithParens* node, void* data) {
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->field_expressions(), data, ",");
  }
  print(")");
}

void Unparser::visitASTStructConstructorWithKeyword(
    const ASTStructConstructorWithKeyword* node, void* data) {
  if (node->struct_type() != nullptr) {
    node->struct_type()->Accept(this, data);
  } else {
    print("STRUCT");
  }
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->fields(), data, ",");
  }
  print(")");
}

void Unparser::visitASTIntLiteral(const ASTIntLiteral* node, void* data) {
  UnparseLeafNode(node);
}

void Unparser::visitASTNumericLiteral(
    const ASTNumericLiteral* node, void* data) {
  print("NUMERIC");
  UnparseLeafNode(node);
}

void Unparser::visitASTBigNumericLiteral(const ASTBigNumericLiteral* node,
                                         void* data) {
  print("BIGNUMERIC");
  UnparseLeafNode(node);
}

void Unparser::visitASTJSONLiteral(const ASTJSONLiteral* node, void* data) {
  print("JSON");
  UnparseLeafNode(node);
}

void Unparser::visitASTFloatLiteral(const ASTFloatLiteral* node, void* data) {
  UnparseLeafNode(node);
}

void Unparser::visitASTStringLiteral(const ASTStringLiteral* node, void* data) {
  UnparseLeafNode(node);
}

void Unparser::visitASTBytesLiteral(const ASTBytesLiteral* node, void* data) {
  UnparseLeafNode(node);
}

void Unparser::visitASTBooleanLiteral(const ASTBooleanLiteral* node,
                                      void* data) {
  UnparseLeafNode(node);
}

void Unparser::visitASTNullLiteral(const ASTNullLiteral* node, void* data) {
  UnparseLeafNode(node);
}

void Unparser::visitASTDateOrTimeLiteral(const ASTDateOrTimeLiteral* node,
                                         void* data) {
  print(Type::TypeKindToString(node->type_kind(), PRODUCT_INTERNAL));
  UnparseChildrenWithSeparator(node, data, "");
}

void Unparser::visitASTStar(const ASTStar* node, void* data) {
  UnparseLeafNode(node);
}

void Unparser::visitASTStarExceptList(const ASTStarExceptList* node,
                                      void* data) {
  UnparseChildrenWithSeparator(node, data, ",");
}

void Unparser::visitASTStarReplaceItem(const ASTStarReplaceItem* node,
                                       void* data) {
  UnparseChildrenWithSeparator(node, data, "AS");
}

void Unparser::visitASTStarModifiers(const ASTStarModifiers* node, void* data) {
  if (node->except_list() != nullptr) {
    print("EXCEPT (");
    node->except_list()->Accept(this, data);
    print(")");
  }
  if (!node->replace_items().empty()) {
    print("REPLACE (");
    UnparseVectorWithSeparator(node->replace_items(), data, ",");
    print(")");
  }
}

void Unparser::visitASTStarWithModifiers(const ASTStarWithModifiers* node,
                                         void* data) {
  print("*");
  node->modifiers()->Accept(this, data);
}

void Unparser::visitASTPathExpression(const ASTPathExpression* node,
                                      void* data) {
  PrintOpenParenIfNeeded(node);
  UnparseChildrenWithSeparator(node, data, ".");
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTParameterExpr(const ASTParameterExpr* node, void* data) {
  if (node->name() == nullptr) {
    print("?");
  } else {
    print("@");
    visitASTChildren(node, data);
  }
}

void Unparser::visitASTSystemVariableExpr(const ASTSystemVariableExpr* node,
                                          void* data) {
  PrintOpenParenIfNeeded(node);
  print("@@");
  visitASTChildren(node, data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTIntervalExpr(const ASTIntervalExpr* node, void* data) {
  print("INTERVAL");
  node->interval_value()->Accept(this, data);
  node->date_part_name()->Accept(this, data);
  if (node->date_part_name_to() != nullptr) {
    print("TO");
    node->date_part_name_to()->Accept(this, data);
  }
}

void Unparser::visitASTDotIdentifier(const ASTDotIdentifier* node,
                                     void* data) {
  PrintOpenParenIfNeeded(node);
  node->expr()->Accept(this, data);
  print(".");
  node->name()->Accept(this, data);
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTDotGeneralizedField(const ASTDotGeneralizedField* node,
                                           void* data) {
  PrintOpenParenIfNeeded(node);
  node->expr()->Accept(this, data);
  print(".(");
  node->path()->Accept(this, data);
  print(")");
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTDotStar(const ASTDotStar* node, void* data) {
  node->expr()->Accept(this, data);
  print(".*");
}

void Unparser::visitASTDotStarWithModifiers(
    const ASTDotStarWithModifiers* node, void* data) {
  node->expr()->Accept(this, data);
  print(".*");
  node->modifiers()->Accept(this, data);
}

void Unparser::visitASTOrExpr(const ASTOrExpr* node, void* data) {
  PrintOpenParenIfNeeded(node);
  UnparseChildrenWithSeparator(node, data, "OR");
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTAndExpr(const ASTAndExpr* node, void* data) {
  PrintOpenParenIfNeeded(node);
  UnparseChildrenWithSeparator(node, data, "AND");
  PrintCloseParenIfNeeded(node);
}

static bool IsPostfix(ASTUnaryExpression::Op op) {
  return op == ASTUnaryExpression::IS_UNKNOWN ||
         op == ASTUnaryExpression::IS_NOT_UNKNOWN;
}

void Unparser::visitASTUnaryExpression(const ASTUnaryExpression* node,
                                       void* data) {
  PrintOpenParenIfNeeded(node);
  if (IsPostfix(node->op())) {
    node->operand()->Accept(this, data);
    formatter_.AddUnary(node->GetSQLForOperator());
  } else {
    formatter_.AddUnary(node->GetSQLForOperator());
    node->operand()->Accept(this, data);
  }
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTFormatClause(const ASTFormatClause *node, void *data) {
  print("FORMAT");
  node->format()->Accept(this, data);
  if (node->time_zone_expr() != nullptr) {
    print("AT TIME ZONE");
    node->time_zone_expr()->Accept(this, data);
  }
}

void Unparser::visitASTCastExpression(const ASTCastExpression* node,
                                      void* data) {
  print(node->is_safe_cast() ? "SAFE_CAST(" : "CAST(");
  node->expr()->Accept(this, data);
  print("AS");
  node->type()->Accept(this, data);
  if (node->format()) {
    node->format()->Accept(this, data);
  }
  print(")");
}

void Unparser::visitASTExtractExpression(const ASTExtractExpression* node,
                                         void* data) {
  print("EXTRACT(");
  node->lhs_expr()->Accept(this, data);
  print("FROM");
  node->rhs_expr()->Accept(this, data);
  if (node->time_zone_expr() != nullptr) {
    print("AT TIME ZONE");
    node->time_zone_expr()->Accept(this, data);
  }
  print(")");
}

void Unparser::visitASTCaseNoValueExpression(
    const ASTCaseNoValueExpression* node, void* data) {
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
  }
  println();
  print("END");
}

void Unparser::visitASTCaseValueExpression(const ASTCaseValueExpression* node,
                                           void* data) {
  print("CASE");
  node->child(0)->Accept(this, data);
  int i;
  {
    Formatter::Indenter indenter(&formatter_);
    for (i = 1; i < node->num_children() - 1; i += 2) {
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
  }
  println();
  print("END");
}

void Unparser::visitASTBinaryExpression(const ASTBinaryExpression* node,
                                        void* data) {
  PrintOpenParenIfNeeded(node);
  UnparseChildrenWithSeparator(node, data, node->GetSQLForOperator());
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTBitwiseShiftExpression(
    const ASTBitwiseShiftExpression* node, void* data) {
  PrintOpenParenIfNeeded(node);
  UnparseChildrenWithSeparator(node, data, node->is_left_shift() ? "<<" : ">>");
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTInExpression(const ASTInExpression* node, void* data) {
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
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTInList(const ASTInList* node, void* data) {
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
  }
  print(")");
}

void Unparser::visitASTLikeExpression(const ASTLikeExpression* node,
                                      void* data) {
  PrintOpenParenIfNeeded(node);
  node->lhs()->Accept(this, data);
  print(absl::StrCat(node->is_not() ? "NOT " : "", "LIKE"));
  node->op()->Accept(this, data);
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
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTAnySomeAllOp(const ASTAnySomeAllOp* node, void* data) {
  switch (node->op()) {
    case ASTAnySomeAllOp::kUninitialized:
      print("UNINITIALIZED");
      break;
    case ASTAnySomeAllOp::kAny:
      print("ANY");
      break;
    case ASTAnySomeAllOp::kSome:
      print("SOME");
      break;
    case ASTAnySomeAllOp::kAll:
      print("ALL");
      break;
  }
}

void Unparser::visitASTBetweenExpression(const ASTBetweenExpression* node,
                                         void* data) {
  PrintOpenParenIfNeeded(node);
  node->child(0)->Accept(this, data);
  print(absl::StrCat(node->is_not() ? "NOT " : "", "BETWEEN"));
  UnparseChildrenWithSeparator(node, data, 1, node->num_children(), "AND");
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTFunctionCall(const ASTFunctionCall* node, void* data) {
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
    if (node->clamped_between_modifier() != nullptr) {
      node->clamped_between_modifier()->Accept(this, data);
    }
    if (node->order_by() != nullptr) {
      node->order_by()->Accept(this, data);
    }
    if (node->limit_offset() != nullptr) {
      node->limit_offset()->Accept(this, data);
    }
  }
  print(")");
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  if (node->with_group_rows() != nullptr) {
    node->with_group_rows()->Accept(this, data);
  }
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTWithGroupRows(const ASTWithGroupRows* node, void* data) {
  print("WITH GROUP_ROWS (");
  {
    Formatter::Indenter indenter(&formatter_);
    node->subquery()->Accept(this, data);
  }
  print(")");
}

void Unparser::visitASTArrayElement(const ASTArrayElement* node, void* data) {
  PrintOpenParenIfNeeded(node);
  node->array()->Accept(this, data);
  print("[");
  node->position()->Accept(this, data);
  print("]");
  PrintCloseParenIfNeeded(node);
}

void Unparser::visitASTExpressionSubquery(const ASTExpressionSubquery* node,
                                          void* data) {
  print(ASTExpressionSubquery::ModifierToString(node->modifier()));
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    node->query()->Accept(this, data);
  }
  print(")");
}

void Unparser::visitASTHint(const ASTHint* node, void* data) {
  if (node->num_shards_hint() != nullptr) {
    print("@");
    node->num_shards_hint()->Accept(this, data);
  }

  if (!node->hint_entries().empty()) {
    print("@{");
    UnparseVectorWithSeparator(node->hint_entries(), data, ",");
    print("}");
  }
}

void Unparser::visitASTHintEntry(const ASTHintEntry* node, void* data) {
  if (node->qualifier() != nullptr) {
    node->qualifier()->Accept(this, data);
    print(".");
  }
  node->name()->Accept(this, data);
  print("=");
  node->value()->Accept(this, data);
}

void Unparser::visitASTOptionsList(const ASTOptionsList* node, void* data) {
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
  }
  print(")");
}

void Unparser::visitASTOptionsEntry(const ASTOptionsEntry* node, void* data) {
  UnparseChildrenWithSeparator(node, data, "=");
}

void Unparser::visitASTMaxLiteral(const ASTMaxLiteral* node, void* data) {
  print("MAX");
}

void Unparser::visitASTTypeParameterList(const ASTTypeParameterList* node,
                                         void* data) {
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
  }
  print(")");
}

void Unparser::visitASTSimpleType(const ASTSimpleType* node, void* data) {
  const ASTPathExpression* type_name = node->type_name();
  // 'INTERVAL' is a reserved keyword, but when it is used as a type name, we
  // want to print it without backticks.
  if (type_name->num_names() == 1 &&
      zetasql_base::CaseEqual(type_name->first_name()->GetAsString(),
                             "interval")) {
    print(type_name->first_name()->GetAsStringView());
  } else {
    visitASTChildren(node, data);
  }
}

void Unparser::visitASTArrayType(const ASTArrayType* node, void* data) {
  print("ARRAY<");
  node->element_type()->Accept(this, data);
  print(">");
  if (node->type_parameters() != nullptr) {
    node->type_parameters()->Accept(this, data);
  }

  if (node->collate() != nullptr) {
    node->collate()->Accept(this, data);
  }
}

void Unparser::visitASTStructType(const ASTStructType* node, void* data) {
  print("STRUCT<");
  UnparseVectorWithSeparator(node->struct_fields(), data, ",");
  print(">");
  if (node->type_parameters() != nullptr) {
    node->type_parameters()->Accept(this, data);
  }

  if (node->collate() != nullptr) {
    node->collate()->Accept(this, data);
  }
}

void Unparser::visitASTStructField(const ASTStructField* node, void* data) {
  UnparseChildrenWithSeparator(node, data, "");
}

void Unparser::visitASTSimpleColumnSchema(const ASTSimpleColumnSchema* node,
                                          void* data) {
  const ASTPathExpression* type_name = node->type_name();
  // 'INTERVAL' is a reserved keyword, but when it is used as a type name, we
  // want to print it without backticks.
  if (type_name->num_names() == 1 &&
      zetasql_base::CaseEqual(type_name->first_name()->GetAsString(),
                             "interval")) {
    print(type_name->first_name()->GetAsString());
  } else {
    type_name->Accept(this, data);
    UnparseColumnSchema(node, data);
  }
}

void Unparser::visitASTArrayColumnSchema(const ASTArrayColumnSchema* node,
                                         void* data) {
  print("ARRAY<");
  node->element_schema()->Accept(this, data);
  print(">");
  UnparseColumnSchema(node, data);
}

void Unparser::visitASTStructColumnSchema(const ASTStructColumnSchema* node,
                                          void* data) {
  print("STRUCT<");
  UnparseVectorWithSeparator(node->struct_fields(), data, ",");
  print(">");
  UnparseColumnSchema(node, data);
}

void Unparser::visitASTGeneratedColumnInfo(const ASTGeneratedColumnInfo* node,
                                           void* data) {
  print("AS (");
  ZETASQL_DCHECK(node->expression() != nullptr);
  node->expression()->Accept(this, data);
  print(")");
  print(node->GetSqlForStoredMode());
}

void Unparser::visitASTStructColumnField(const ASTStructColumnField* node,
                                         void* data) {
  UnparseChildrenWithSeparator(node, data, "");
}

void Unparser::UnparseColumnSchema(const ASTColumnSchema* node, void* data) {
  if (node->type_parameters() != nullptr) {
    node->type_parameters()->Accept(this, data);
  }
  if (node->generated_column_info() != nullptr) {
    node->generated_column_info()->Accept(this, data);
  }
  if (node->default_expression() != nullptr) {
    print("DEFAULT ");
    node->default_expression()->Accept(this, data);
  }
  if (node->collate() != nullptr) {
    visitASTCollate(node->collate(), data);
  }
  if (node->attributes() != nullptr) {
    node->attributes()->Accept(this, data);
  }
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    Formatter::Indenter indenter(&formatter_);
    node->options_list()->Accept(this, data);
  }
}

void Unparser::visitASTAnalyticFunctionCall(const ASTAnalyticFunctionCall* node,
                                            void* data) {
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
  println();
  print("WINDOW");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->windows(), data, ",");
  }
}

void Unparser::visitASTWindowDefinition(
    const ASTWindowDefinition* node, void* data) {
  node->name()->Accept(this, data);
  print("AS (");
  node->window_spec()->Accept(this, data);
  print(")");
}

void Unparser::visitASTWindowSpecification(
    const ASTWindowSpecification* node, void* data) {
  UnparseChildrenWithSeparator(node, data, "");
}

void Unparser::visitASTPartitionBy(const ASTPartitionBy* node, void* data) {
  print("PARTITION");
  if (node->hint() != nullptr) {
    node->hint()->Accept(this, data);
  }
  print("BY");
  UnparseVectorWithSeparator(node->partitioning_expressions(), data, ",");
}

void Unparser::visitASTClusterBy(const ASTClusterBy* node, void* data) {
  print("CLUSTER BY");
  UnparseVectorWithSeparator(node->clustering_expressions(), data, ",");
}

void Unparser::visitASTWindowFrame(const ASTWindowFrame* node,
                                   void* data) {
  print(node->GetFrameUnitString());
  if (nullptr != node->end_expr()) {
    print("BETWEEN");
  }
  node->start_expr()->Accept(this, data);
  if (nullptr != node->end_expr()) {
    print("AND");
    node->end_expr()->Accept(this, data);
  }
}

void Unparser::visitASTWindowFrameExpr(
    const ASTWindowFrameExpr* node, void* data) {
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
}

void Unparser::visitASTDefaultLiteral(const ASTDefaultLiteral* node,
                                      void* data) {
  print("DEFAULT");
}

void Unparser::visitASTAnalyzeStatement(const ASTAnalyzeStatement* node,
                                        void* data) {
  println();
  print("ANALYZE");
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    Formatter::Indenter indenter(&formatter_);
    node->options_list()->Accept(this, data);
  }
  if (node->table_and_column_info_list() != nullptr) {
    Formatter::Indenter indenter(&formatter_);
    node->table_and_column_info_list()->Accept(this, data);
  }
}

void Unparser::visitASTTableAndColumnInfo(const ASTTableAndColumnInfo* node,
                                          void* data) {
  visitASTChildren(node, data);
}

void Unparser::visitASTTableAndColumnInfoList(
    const ASTTableAndColumnInfoList* node, void* data) {
  UnparseChildrenWithSeparator(node, data, ",");
}

void Unparser::visitASTAssertStatement(const ASTAssertStatement* node,
                                       void* data) {
  print("ASSERT");
  node->expr()->Accept(this, data);
  if (node->description() != nullptr) {
    print("AS");
    node->description()->Accept(this, data);
  }
}

void Unparser::visitASTAssertRowsModified(const ASTAssertRowsModified* node,
                                          void* data) {
  println();
  print("ASSERT_ROWS_MODIFIED");
  visitASTChildren(node, data);
}

void Unparser::visitASTReturningClause(const ASTReturningClause* node,
                                       void* data) {
  println();
  print("THEN RETURN");
  if (node->action_alias() != nullptr) {
    print("WITH ACTION");
    print(absl::StrCat("AS ", node->action_alias()->GetAsStringView()));
  }
  node->select_list()->Accept(this, data);
}

void Unparser::visitASTDeleteStatement(const ASTDeleteStatement* node,
                                       void* data) {
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
    }
  }
  if (node->assert_rows_modified() != nullptr) {
    node->assert_rows_modified()->Accept(this, data);
  }
  if (node->returning() != nullptr) {
    node->returning()->Accept(this, data);
  }
}

void Unparser::visitASTColumnList(const ASTColumnList* node, void* data) {
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
  }
  print(")");
}

void Unparser::visitASTInsertValuesRow(const ASTInsertValuesRow* node,
                                       void* data) {
  println();
  print("(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
  }
  print(")");
}

void Unparser::visitASTInsertValuesRowList(const ASTInsertValuesRowList* node,
                                           void* data) {
  print("VALUES");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseChildrenWithSeparator(node, data, ",");
  }
}

void Unparser::visitASTInsertStatement(const ASTInsertStatement* node,
                                       void* data) {
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

  if (node->returning() != nullptr) {
    node->returning()->Accept(this, data);
  }
}

void Unparser::visitASTUpdateSetValue(const ASTUpdateSetValue* node,
                                      void* data) {
  UnparseChildrenWithSeparator(node, data, "=");
}

void Unparser::visitASTUpdateItem(const ASTUpdateItem* node, void* data) {
  // If we don't have set_value, we have one of the statement types, which
  // require parentheses.
  if (node->set_value() == nullptr) {
    println();
    println("(");
    {
      Formatter::Indenter indenter(&formatter_);
      visitASTChildren(node, data);
    }
    println();
    print(")");
  } else {
    visitASTChildren(node, data);
  }
}

void Unparser::visitASTUpdateItemList(const ASTUpdateItemList* node,
                                      void* data) {
  UnparseChildrenWithSeparator(node, data, ",", true /* break_line */);
}

void Unparser::visitASTUpdateStatement(const ASTUpdateStatement* node,
                                       void* data) {
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
  if (node->returning() != nullptr) {
    node->returning()->Accept(this, data);
  }
}

void Unparser::visitASTTruncateStatement(const ASTTruncateStatement* node,
                                         void* data) {
  println();
  print("TRUNCATE TABLE");

  node->target_path()->Accept(this, data);

  if (node->where() != nullptr) {
    println();
    println("WHERE");
    {
      Formatter::Indenter indenter(&formatter_);
      node->where()->Accept(this, data);
    }
  }
}

void Unparser::visitASTMergeAction(const ASTMergeAction* node, void* data) {
  println();
  switch (node->action_type()) {
    case ASTMergeAction::INSERT:
      print("INSERT");
      if (node->insert_column_list() != nullptr) {
        node->insert_column_list()->Accept(this, data);
      }
      println();
      ZETASQL_DCHECK(node->insert_row() != nullptr);
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
      ZETASQL_LOG(DFATAL) << "Merge clause action type is not set";
  }
}

void Unparser::visitASTMergeWhenClause(const ASTMergeWhenClause* node,
                                       void* data) {
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
      ZETASQL_LOG(DFATAL) << "Match type of merge match clause is not set.";
  }
  if (node->search_condition() != nullptr) {
    print("AND");
    node->search_condition()->Accept(this, data);
  }
  print("THEN");
  Formatter::Indenter indenter(&formatter_);
  action->Accept(this, data);
}

void Unparser::visitASTMergeWhenClauseList(const ASTMergeWhenClauseList* node,
                                           void* data) {
  println();
  UnparseChildrenWithSeparator(node, data, "", true /* break_line */);
}

void Unparser::visitASTMergeStatement(const ASTMergeStatement* node,
                                      void* data) {
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
}

void Unparser::visitASTPrimaryKey(const ASTPrimaryKey* node, void* data) {
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
  }
}

void Unparser::visitASTPrivilege(const ASTPrivilege* node, void* data) {
  node->privilege_action()->Accept(this, data);
  if (node->paths() != nullptr) {
    print("(");
    node->paths()->Accept(this, data);
    print(")");
  }
}

void Unparser::visitASTPrivileges(const ASTPrivileges* node, void* data) {
  if (node->is_all_privileges()) {
    print("ALL PRIVILEGES");
  } else {
    UnparseChildrenWithSeparator(node, data, ",");
  }
}

void Unparser::visitASTGranteeList(const ASTGranteeList* node, void* data) {
  UnparseChildrenWithSeparator(node, data, ",");
}

void Unparser::visitASTGrantStatement(const ASTGrantStatement* node,
                                      void* data) {
  print("GRANT");
  node->privileges()->Accept(this, data);
  print("ON");
  if (node->target_type() != nullptr) {
    node->target_type()->Accept(this, data);
  }
  node->target_path()->Accept(this, data);
  print("TO");
  node->grantee_list()->Accept(this, data);
}

void Unparser::visitASTRevokeStatement(const ASTRevokeStatement* node,
                                       void* data) {
  print("REVOKE");
  node->privileges()->Accept(this, data);
  print("ON");
  if (node->target_type() != nullptr) {
    node->target_type()->Accept(this, data);
  }
  node->target_path()->Accept(this, data);
  print("FROM");
  node->grantee_list()->Accept(this, data);
}

void Unparser::visitASTRepeatableClause(const ASTRepeatableClause* node,
                                        void* data) {
  print("REPEATABLE (");
  node->argument()->Accept(this, data);
  print(")");
}

void Unparser::visitASTReplaceFieldsArg(const ASTReplaceFieldsArg* node,
                                        void* data) {
  node->expression()->Accept(this, data);
  print("AS ");
  node->path_expression()->Accept(this, data);
}

void Unparser::visitASTReplaceFieldsExpression(
    const ASTReplaceFieldsExpression* node, void* data) {
  print("REPLACE_FIELDS(");
  node->expr()->Accept(this, data);
  print(", ");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->arguments(), data, ",");
  }
  print(")");
}

void Unparser::visitASTFilterFieldsArg(const ASTFilterFieldsArg* node,
                                       void* data) {
  std::string path_expression = Unparse(node->path_expression());
  ZETASQL_DCHECK_EQ(path_expression.back(), '\n');
  path_expression.pop_back();
  print(absl::StrCat(node->GetSQLForOperator(), path_expression));
}

void Unparser::visitASTPivotExpression(const ASTPivotExpression* node,
                                       void* data) {
  node->expression()->Accept(this, data);
  if (node->alias() != nullptr) {
    node->alias()->Accept(this, data);
  }
}

void Unparser::visitASTPivotExpressionList(const ASTPivotExpressionList* node,
                                           void* data) {
  UnparseChildrenWithSeparator(node, data, ", ");
}

void Unparser::visitASTPivotValue(const ASTPivotValue* node, void* data) {
  node->value()->Accept(this, data);
  if (node->alias() != nullptr) {
    node->alias()->Accept(this, data);
  }
}
void Unparser::visitASTPivotValueList(const ASTPivotValueList* node,
                                      void* data) {
  UnparseChildrenWithSeparator(node, data, ", ");
}

void Unparser::visitASTPivotClause(const ASTPivotClause* node, void* data) {
  print("PIVOT(");
  node->pivot_expressions()->Accept(this, data);
  print("FOR");
  node->for_expression()->Accept(this, data);
  print("IN (");
  node->pivot_values()->Accept(this, data);
  print("))");

  if (node->output_alias() != nullptr) {
    node->output_alias()->Accept(this, data);
  }
}

void Unparser::visitASTUnpivotInItem(const ASTUnpivotInItem* node, void* data) {
  print("(");
  node->unpivot_columns()->Accept(this, data);
  print(")");
  if (node->alias() != nullptr) {
    print("AS");
    node->alias()->Accept(this, data);
  }
}
void Unparser::visitASTUnpivotInItemList(const ASTUnpivotInItemList* node,
                                         void* data) {
  print("(");
  UnparseChildrenWithSeparator(node, data, ", ");
  print(")");
}

void Unparser::visitASTUnpivotInItemLabel(const ASTUnpivotInItemLabel* node,
                                          void* data) {
  node->label()->Accept(this, data);
}

void Unparser::visitASTUnpivotClause(const ASTUnpivotClause* node, void* data) {
  print("UNPIVOT");
  print(node->GetSQLForNullFilter().empty()
            ? ""
            : absl::StrCat(node->GetSQLForNullFilter(), " "));
  print("(");
  if (node->unpivot_output_value_columns()->path_expression_list().size() > 1) {
    print("(");
  }
  node->unpivot_output_value_columns()->Accept(this, data);
  if (node->unpivot_output_value_columns()->path_expression_list().size() > 1) {
    print(")");
  }
  print("FOR");
  node->unpivot_output_name_column()->Accept(this, data);
  print("IN ");
  node->unpivot_in_items()->Accept(this, data);
  print(")");

  if (node->output_alias() != nullptr) {
    node->output_alias()->Accept(this, data);
  }
}

void Unparser::visitASTSampleSize(const ASTSampleSize* node, void* data) {
  node->size()->Accept(this, data);
  print(node->GetSQLForUnit());
  if (node->partition_by() != nullptr) {
    node->partition_by()->Accept(this, data);
  }
}

void Unparser::visitASTSampleSuffix(const ASTSampleSuffix* node, void* data) {
  if (node->weight() != nullptr) {
    node->weight()->Accept(this, data);
  }
  if (node->repeat() != nullptr) {
    node->repeat()->Accept(this, data);
  }
}

void Unparser::visitASTWithWeight(const ASTWithWeight* node, void *data) {
  print("WITH WEIGHT");
  visitASTChildren(node, data);
}

void Unparser::visitASTSampleClause(const ASTSampleClause* node, void* data) {
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
}

void Unparser::VisitAlterStatementBase(const ASTAlterStatementBase* node,
                                       void* data) {
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  // Path may not exist if FEATURE_ALLOW_MISSING_PATH_EXPRESSION_IN_ALTER_DDL
  // was set during parse time.
  if (node->path()) {
    node->path()->Accept(this, data);
  }
  node->action_list()->Accept(this, data);
}

void Unparser::visitASTAlterMaterializedViewStatement(
    const ASTAlterMaterializedViewStatement* node, void* data) {
  print("ALTER MATERIALIZED VIEW");
  VisitAlterStatementBase(node, data);
}

void Unparser::visitASTAlterDatabaseStatement(
    const ASTAlterDatabaseStatement* node, void* data) {
  print("ALTER DATABASE");
  VisitAlterStatementBase(node, data);
}

void Unparser::visitASTAlterSchemaStatement(
    const ASTAlterSchemaStatement* node, void* data) {
  print("ALTER SCHEMA");
  VisitAlterStatementBase(node, data);
}

void Unparser::visitASTAlterTableStatement(const ASTAlterTableStatement* node,
                                           void* data) {
  print("ALTER TABLE");
  VisitAlterStatementBase(node, data);
}

void Unparser::visitASTAlterViewStatement(const ASTAlterViewStatement* node,
                                          void* data) {
  print("ALTER VIEW");
  VisitAlterStatementBase(node, data);
}

void Unparser::visitASTSetOptionsAction(const ASTSetOptionsAction* node,
                                        void* data) {
  print("SET OPTIONS");
  node->options_list()->Accept(this, data);
}

void Unparser::VisitCheckConstraintSpec(const ASTCheckConstraint* node,
                                        void* data) {
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
}

void Unparser::visitASTAddConstraintAction(const ASTAddConstraintAction* node,
                                           void* data) {
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
  } else if (node_kind == AST_PRIMARY_KEY) {
    constraint->GetAs<ASTPrimaryKey>()->Accept(this, data);
  } else {
    ZETASQL_LOG(FATAL) << "Unknown constraint node kind: "
               << ASTNode::NodeKindToString(node_kind);
  }
}

void Unparser::visitASTDropConstraintAction(const ASTDropConstraintAction* node,
                                            void* data) {
  print("DROP CONSTRAINT");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->constraint_name()->Accept(this, data);
}

void Unparser::visitASTDropPrimaryKeyAction(const ASTDropPrimaryKeyAction* node,
                                            void* data) {
  print("DROP PRIMARY KEY");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
}

void Unparser::visitASTAlterConstraintEnforcementAction(
    const ASTAlterConstraintEnforcementAction* node, void* data) {
  print("ALTER CONSTRAINT");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->constraint_name()->Accept(this, data);
  if (!node->is_enforced()) {
    print("NOT");
  }
  print("ENFORCED");
}

void Unparser::visitASTAlterConstraintSetOptionsAction(
    const ASTAlterConstraintSetOptionsAction* node, void* data) {
  print("ALTER CONSTRAINT");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->constraint_name()->Accept(this, data);
  print("SET OPTIONS");
  node->options_list()->Accept(this, data);
}

void Unparser::visitASTAddColumnAction(const ASTAddColumnAction* node,
                                       void* data) {
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
}

void Unparser::visitASTColumnPosition(const ASTColumnPosition* node,
                                      void* data) {
  print(node->type() == ASTColumnPosition::PRECEDING ? "PRECEDING"
                                                     : "FOLLOWING");
  node->identifier()->Accept(this, data);
}

void Unparser::visitASTDropColumnAction(const ASTDropColumnAction* node,
                                        void* data) {
  print("DROP COLUMN");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->column_name()->Accept(this, data);
}

void Unparser::visitASTRenameColumnAction(const ASTRenameColumnAction* node,
                                          void* data) {
  print("RENAME COLUMN");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->column_name()->Accept(this, data);
  print("TO");
  node->new_column_name()->Accept(this, data);
}

void Unparser::visitASTAlterColumnOptionsAction(
    const ASTAlterColumnOptionsAction* node, void* data) {
  print("ALTER COLUMN");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->column_name()->Accept(this, data);
  print("SET OPTIONS");
  node->options_list()->Accept(this, data);
}

void Unparser::visitASTAlterColumnDropNotNullAction(
    const ASTAlterColumnDropNotNullAction* node, void* data) {
  print("ALTER COLUMN");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->column_name()->Accept(this, data);
  print("DROP NOT NULL");
}

void Unparser::visitASTAlterColumnTypeAction(
    const ASTAlterColumnTypeAction* node, void* data) {
  print("ALTER COLUMN");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->column_name()->Accept(this, data);
  print("SET DATA TYPE");
  node->schema()->Accept(this, data);
  if (node->collate() != nullptr) {
    visitASTCollate(node->collate(), data);
  }
}

void Unparser::visitASTAlterColumnSetDefaultAction(
    const ASTAlterColumnSetDefaultAction* node, void* data) {
  print("ALTER COLUMN");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->column_name()->Accept(this, data);
  print("SET DEFAULT");
  node->default_expression()->Accept(this, data);
}

void Unparser::visitASTAlterColumnDropDefaultAction(
    const ASTAlterColumnDropDefaultAction* node, void* data) {
  print("ALTER COLUMN");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->column_name()->Accept(this, data);
  print("DROP DEFAULT");
}

void Unparser::visitASTRevokeFromClause(const ASTRevokeFromClause* node,
                                        void* data) {
  print("REVOKE FROM ");
  if (node->is_revoke_from_all()) {
    print("ALL");
  } else {
    print("(");
    node->revoke_from_list()->Accept(this, data);
    print(")");
  }
}

void Unparser::visitASTRenameToClause(const ASTRenameToClause* node,
                                      void* data) {
  print("RENAME TO");
  node->new_name()->Accept(this, data);
}

void Unparser::visitASTSetCollateClause(const ASTSetCollateClause* node,
                                        void* data) {
  print("SET DEFAULT");
  visitASTCollate(node->collate(), data);
}

void Unparser::visitASTAlterActionList(const ASTAlterActionList* node,
                                       void* data) {
  Formatter::Indenter indenter(&formatter_);
  UnparseChildrenWithSeparator(node, data, ",");
}

void Unparser::visitASTAlterPrivilegeRestrictionStatement(
    const ASTAlterPrivilegeRestrictionStatement* node, void* data) {
  print("ALTER PRIVILEGE RESTRICTION");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  print("ON");
  node->privileges()->Accept(this, data);
  print("ON");
  node->object_type()->Accept(this, data);
  node->path()->Accept(this, data);
  node->action_list()->Accept(this, data);
}

void Unparser::visitASTAlterRowAccessPolicyStatement(
    const ASTAlterRowAccessPolicyStatement* node, void* data) {
  print("ALTER ROW ACCESS POLICY");
  if (node->is_if_exists()) {
    print("IF EXISTS");
  }
  node->name()->Accept(this, data);
  print("ON");
  node->path()->Accept(this, data);
  node->action_list()->Accept(this, data);
}

void Unparser::visitASTAlterAllRowAccessPoliciesStatement(
    const ASTAlterAllRowAccessPoliciesStatement* node, void* data) {
  print("ALTER ALL ROW ACCESS POLICIES ON");
  node->table_name_path()->Accept(this, data);
  node->alter_action()->Accept(this, data);
}

void Unparser::visitASTCreateIndexStatement(const ASTCreateIndexStatement* node,
                                            void* data) {
  print("CREATE");
  if (node->is_or_replace()) print("OR REPLACE");
  if (node->is_unique()) print("UNIQUE");
  if (node->is_search()) print("SEARCH");
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
}

void Unparser::visitASTStatementList(const ASTStatementList* node, void* data) {
  for (const ASTStatement* statement : node->statement_list()) {
    statement->Accept(this, data);
    println(";");
  }
}

void Unparser::visitASTElseifClause(const ASTElseifClause* node, void* data) {
  print("ELSEIF");
  node->condition()->Accept(this, data);
  print("THEN");
  {
    Formatter::Indenter indenter(&formatter_);
    node->body()->Accept(this, data);
  }
  println();
}

void Unparser::visitASTElseifClauseList(const ASTElseifClauseList* node,
                                        void* data) {
  for (const ASTElseifClause* else_if_clause : node->elseif_clauses()) {
    else_if_clause->Accept(this, data);
  }
}

void Unparser::visitASTIfStatement(const ASTIfStatement* node, void* data) {
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
    println();
    println("ELSE");
    Formatter::Indenter indenter(&formatter_);
    node->else_list()->Accept(this, data);
  }
  println();
  print("END IF");
}

void Unparser::visitASTWhenThenClause(const ASTWhenThenClause* node,
                                      void* data) {
  print("WHEN");
  node->condition()->Accept(this, data);
  println("THEN");
  {
    Formatter::Indenter indenter(&formatter_);
    node->body()->Accept(this, data);
  }
  println();
}

void Unparser::visitASTWhenThenClauseList(const ASTWhenThenClauseList* node,
                                          void* data) {
  for (const ASTWhenThenClause* when_then_clause : node->when_then_clauses()) {
    when_then_clause->Accept(this, data);
  }
}

void Unparser::visitASTCaseStatement(
    const ASTCaseStatement* node, void* data) {
  print("CASE");
  if (node->expression() != nullptr) {
    node->expression()->Accept(this, data);
  }
  println();
  node->when_then_clauses()->Accept(this, data);
  if (node->else_list() != nullptr) {
    println();
    println("ELSE");
    Formatter::Indenter indenter(&formatter_);
    node->else_list()->Accept(this, data);
  }
  print("END");
  print("CASE");
}

void Unparser::visitASTBeginEndBlock(const ASTBeginEndBlock* node, void* data) {
  if (node->label() != nullptr) {
    node->label()->Accept(this, data);
    print(":");
  }
  println("BEGIN");
  {
    Formatter::Indenter indenter(&formatter_);
    node->statement_list_node()->Accept(this, data);
  }
  if (node->handler_list() != nullptr) {
    node->handler_list()->Accept(this, data);
  }
  println("END");
  if (node->label() != nullptr) {
    node->label()->Accept(this, data);
  }
}

void Unparser::visitASTIndexAllColumns(const ASTIndexAllColumns* node,
                                       void* data) {
  UnparseLeafNode(node);
}

void Unparser::visitASTIndexItemList(const ASTIndexItemList* node, void* data) {
  print("(");
  UnparseVectorWithSeparator(node->ordering_expressions(), data, ",");
  print(")");
}

void Unparser::visitASTIndexStoringExpressionList(
    const ASTIndexStoringExpressionList* node, void* data) {
  print("STORING(");
  {
    Formatter::Indenter indenter(&formatter_);
    UnparseVectorWithSeparator(node->expressions(), data, ",");
  }
  print(")");
}

void Unparser::visitASTIndexUnnestExpressionList(
    const ASTIndexUnnestExpressionList* node, void* data) {
  UnparseVectorWithSeparator(node->unnest_expressions(), data, "");
}

void Unparser::VisitForeignKeySpec(const ASTForeignKey* node, void* data) {
  print("FOREIGN KEY");
  node->column_list()->Accept(this, data);
  node->reference()->Accept(this, data);
  if (node->options_list() != nullptr) {
    print("OPTIONS");
    node->options_list()->Accept(this, data);
  }
}

void Unparser::visitASTForeignKey(const ASTForeignKey* node, void* data) {
  if (node->constraint_name() != nullptr) {
    print("CONSTRAINT");
    node->constraint_name()->Accept(this, data);
  }
  VisitForeignKeySpec(node, data);
}

void Unparser::visitASTForeignKeyReference(
    const ASTForeignKeyReference* node, void* data) {
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
}

void Unparser::visitASTForeignKeyActions(
    const ASTForeignKeyActions* node, void* data) {
  print("ON UPDATE");
  print(ASTForeignKeyActions::GetSQLForAction(node->update_action()));
  print("ON DELETE");
  print(ASTForeignKeyActions::GetSQLForAction(node->delete_action()));
}

void Unparser::visitASTCheckConstraint(const ASTCheckConstraint* node,
                                       void* data) {
  if (node->constraint_name() != nullptr) {
    print("CONSTRAINT");
    node->constraint_name()->Accept(this, data);
  }
  VisitCheckConstraintSpec(node, data);
}

void Unparser::visitASTIdentifierList(const ASTIdentifierList* node,
                                      void* data) {
  UnparseVectorWithSeparator(node->identifier_list(), data, ", ");
}

void Unparser::visitASTVariableDeclaration(const ASTVariableDeclaration* node,
                                           void* data) {
  print("DECLARE");
  node->variable_list()->Accept(this, data);
  if (node->type() != nullptr) {
    node->type()->Accept(this, data);
  }
  if (node->default_value() != nullptr) {
    print("DEFAULT");
    node->default_value()->Accept(this, data);
  }
}

void Unparser::visitASTSingleAssignment(const ASTSingleAssignment* node,
                                        void* data) {
  print("SET");
  node->variable()->Accept(this, data);
  print("=");
  node->expression()->Accept(this, data);
}

void Unparser::visitASTParameterAssignment(const ASTParameterAssignment* node,
                                           void* data) {
  print("SET");
  node->parameter()->Accept(this, data);
  print("=");
  node->expression()->Accept(this, data);
}

void Unparser::visitASTSystemVariableAssignment(
    const ASTSystemVariableAssignment* node, void* data) {
  print("SET");
  node->system_variable()->Accept(this, data);
  print("=");
  node->expression()->Accept(this, data);
}

void Unparser::visitASTAssignmentFromStruct(const ASTAssignmentFromStruct* node,
                                  void* data) {
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
}

void Unparser::visitASTWhileStatement(const ASTWhileStatement* node,
                                      void* data) {
  if (node->label() != nullptr) {
    node->label()->Accept(this, data);
    print(":");
  }
  if (node->condition() != nullptr) {
    print("WHILE");
    node->condition()->Accept(this, data);
    println("DO");
    {
      Formatter::Indenter indenter(&formatter_);
      node->body()->Accept(this, data);
    }
    print("END");
    print("WHILE");
  } else {
    println("LOOP");
    {
      Formatter::Indenter indenter(&formatter_);
      node->body()->Accept(this, data);
    }
    print("END");
    print("LOOP");
  }
  if (node->label() != nullptr) {
    node->label()->Accept(this, data);
  }
}

void Unparser::visitASTUntilClause(const ASTUntilClause* node,
                                   void* data) {
  print("UNTIL");
  node->condition()->Accept(this, data);
}

void Unparser::visitASTRepeatStatement(const ASTRepeatStatement* node,
                                       void* data) {
  if (node->label() != nullptr) {
    node->label()->Accept(this, data);
    print(":");
  }
  println("REPEAT");
  {
    Formatter::Indenter indenter(&formatter_);
    node->body()->Accept(this, data);
  }
  node->until_clause()->Accept(this, data);
  println();
  print("END");
  print("REPEAT");
  if (node->label() != nullptr) {
    node->label()->Accept(this, data);
  }
}

void Unparser::visitASTForInStatement(const ASTForInStatement* node,
                                      void* data) {
  if (node->label() != nullptr) {
    node->label()->Accept(this, data);
    print(":");
  }
  print("FOR");
  node->variable()->Accept(this, data);
  print("IN");
  print("(");
  node->query()->Accept(this, data);
  println(")");
  println("DO");
  {
    Formatter::Indenter indenter(&formatter_);
    node->body()->Accept(this, data);
  }
  print("END");
  print("FOR");
  if (node->label() != nullptr) {
    node->label()->Accept(this, data);
  }
}

void Unparser::visitASTLabel(const ASTLabel* node, void* data) {
  visitASTChildren(node, data);
}

void Unparser::visitASTScript(const ASTScript* node, void* data) {
  node->statement_list_node()->Accept(this, data);
}

void Unparser::visitASTBreakStatement(const ASTBreakStatement* node,
                                      void* data) {
  print(node->GetKeywordText());
  if (node->label() != nullptr) {
    node->label()->Accept(this, data);
  }
}

void Unparser::visitASTContinueStatement(const ASTContinueStatement* node,
                                         void* data) {
  print(node->GetKeywordText());
  if (node->label() != nullptr) {
    node->label()->Accept(this, data);
  }
}

void Unparser::visitASTReturnStatement(const ASTReturnStatement* node,
                                       void* data) {
  print("RETURN");
}

void Unparser::visitASTCreateProcedureStatement(
    const ASTCreateProcedureStatement* node, void* data) {
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

  // CREATE PROCEDURE statements are constructed so that the body always
  // consists of a single ASTBeginEndBlock statement.
  ZETASQL_DCHECK_EQ(node->body()->statement_list().size(), 1);
  node->body()->statement_list()[0]->Accept(this, data);
}

void Unparser::visitASTNamedArgument(const ASTNamedArgument* node, void* data) {
  node->name()->Accept(this, data);
  print(" => ");
  node->expr()->Accept(this, data);
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
  print("WHEN ERROR THEN");
  Formatter::Indenter indenter(&formatter_);
  node->statement_list()->Accept(this, data);
  println();
}
void Unparser::visitASTExceptionHandlerList(const ASTExceptionHandlerList* node,
                                            void* data) {
  println("EXCEPTION");
  for (const ASTExceptionHandler* handler : node->exception_handler_list()) {
    handler->Accept(this, data);
  }
}
void Unparser::visitASTExecuteIntoClause(const ASTExecuteIntoClause* node,
                                         void* data) {
  print("INTO");
  UnparseChildrenWithSeparator(node, data, ", ");
}
void Unparser::visitASTExecuteUsingArgument(const ASTExecuteUsingArgument* node,
                                            void* data) {
  visitASTChildren(node, data);
}
void Unparser::visitASTExecuteUsingClause(const ASTExecuteUsingClause* node,
                                          void* data) {
  print("USING");
  UnparseChildrenWithSeparator(node, data, ", ");
}
void Unparser::visitASTExecuteImmediateStatement(
    const ASTExecuteImmediateStatement* node, void* data) {
  print("EXECUTE IMMEDIATE");
  node->sql()->Accept(this, data);
  if (node->into_clause() != nullptr) {
    node->into_clause()->Accept(this, data);
  }
  if (node->using_clause() != nullptr) {
    node->using_clause()->Accept(this, data);
  }
}

void Unparser::visitASTRaiseStatement(const ASTRaiseStatement* node,
                                      void* data) {
  print("RAISE");
  if (node->message() != nullptr) {
    print("USING MESSAGE =");
    node->message()->Accept(this, data);
  }
}

}  // namespace parser
}  // namespace zetasql
