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

#include "zetasql/parser/parser.h"

#include <memory>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/parser/bison_parser.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/parser/parse_tree_visitor.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_resume_location.h"
#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

using parser::BisonParserMode;
using parser::BisonParser;

LanguageOptions LegacyDefaultParserLanguageOptions() {
  LanguageOptions options;
  options.EnableLanguageFeature(FEATURE_V_1_3_WITH_GROUP_ROWS);
  options.EnableLanguageFeature(FEATURE_V_1_3_CASE_STMT);
  options.EnableLanguageFeature(FEATURE_V_1_3_REPEAT);
  options.EnableLanguageFeature(FEATURE_V_1_3_FOR_IN);
  options.EnableLanguageFeature(FEATURE_V_1_3_REMOTE_FUNCTION);

  return options;
}

// TODO: Simplify this to 'LanguageOptions()'
ParserOptions::ParserOptions()
    : ParserOptions(LegacyDefaultParserLanguageOptions()) {}

ParserOptions::ParserOptions(std::shared_ptr<IdStringPool> id_string_pool,
                             std::shared_ptr<zetasql_base::UnsafeArena> arena,
                             const LanguageOptions* language_options)
    : arena_(std::move(arena)), id_string_pool_(std::move(id_string_pool)) {
  set_language_options(language_options);
}

ParserOptions::ParserOptions(LanguageOptions language_options)
    : arena_(std::make_shared<zetasql_base::UnsafeArena>(/*block_size=*/4096)),
      id_string_pool_(std::make_shared<IdStringPool>(arena_)),
      language_options_(std::move(language_options)) {}

ParserOptions::ParserOptions(std::shared_ptr<IdStringPool> id_string_pool,
                             std::shared_ptr<zetasql_base::UnsafeArena> arena,
                             LanguageOptions language_options)
    : arena_(std::move(arena)),
      id_string_pool_(std::move(id_string_pool)),
      language_options_(std::move(language_options)) {}

ParserOptions::~ParserOptions() {}

void ParserOptions::CreateDefaultArenasIfNotSet() {
  if (arena_ == nullptr) {
    arena_ = std::make_shared<zetasql_base::UnsafeArena>(/*block_size=*/4096);
  }
  if (id_string_pool_ == nullptr) {
    id_string_pool_ = std::make_shared<IdStringPool>(arena_);
  }
}

ParserOutput::ParserOutput(
    std::shared_ptr<IdStringPool> id_string_pool,
    std::shared_ptr<zetasql_base::UnsafeArena> arena,
    std::vector<std::unique_ptr<ASTNode>> other_allocated_ast_nodes,
    absl::variant<std::unique_ptr<ASTStatement>, std::unique_ptr<ASTScript>,
                  std::unique_ptr<ASTType>, std::unique_ptr<ASTExpression>>
        node)
    : id_string_pool_(std::move(id_string_pool)),
      arena_(std::move(arena)),
      other_allocated_ast_nodes_(std::move(other_allocated_ast_nodes)),
      node_(std::move(node)) {}

ParserOutput::~ParserOutput() {}

absl::Status ParseStatement(absl::string_view statement_string,
                            const ParserOptions& parser_options_in,
                            std::unique_ptr<ParserOutput>* output) {
  ParserOptions parser_options = parser_options_in;
  parser_options.CreateDefaultArenasIfNotSet();

  // TODO: Share implementation with ParseNextStatement. There are
  // subtle differences that make this difficult. One of them is that the error
  // messages from the parser change depending on whether a "next" statement is
  // expected to occur or not.
  BisonParser parser;
  std::unique_ptr<ASTNode> ast_node;
  std::vector<std::unique_ptr<ASTNode>> other_allocated_ast_nodes;
  absl::Status status = parser.Parse(
      BisonParserMode::kStatement, /*filename=*/absl::string_view(),
      statement_string, /*start_byte_offset=*/0,
      parser_options.id_string_pool().get(), parser_options.arena().get(),
      parser_options.language_options(), &ast_node, &other_allocated_ast_nodes,
      /*ast_statement_properties=*/nullptr,
      /*statement_end_byte_offset=*/nullptr);
  ZETASQL_RETURN_IF_ERROR(
      ConvertInternalErrorLocationToExternal(status, statement_string));
  ZETASQL_RET_CHECK(ast_node != nullptr);
  std::unique_ptr<ASTStatement> statement(
      ast_node.release()->GetAsOrDie<ASTStatement>());
  *output = absl::make_unique<ParserOutput>(
      parser_options.id_string_pool(), parser_options.arena(),
      std::move(other_allocated_ast_nodes), std::move(statement));
  return absl::OkStatus();
}

absl::Status ParseScript(absl::string_view script_string,
                         const ParserOptions& parser_options_in,
                         ErrorMessageMode error_message_mode,
                         std::unique_ptr<ParserOutput>* output) {
  ParserOptions parser_options = parser_options_in;
  parser_options.CreateDefaultArenasIfNotSet();

  BisonParser parser;
  std::unique_ptr<ASTNode> ast_node;
  std::vector<std::unique_ptr<ASTNode>> other_allocated_ast_nodes;
  absl::Status status = parser.Parse(
      BisonParserMode::kScript, /*filename=*/absl::string_view(), script_string,
      /*start_byte_offset=*/0, parser_options.id_string_pool().get(),
      parser_options.arena().get(), parser_options.language_options(),
      &ast_node, &other_allocated_ast_nodes,
      /*ast_statement_properties=*/nullptr,
      /*statement_end_byte_offset=*/nullptr);

  std::unique_ptr<ASTScript> script;
  if (status.ok()) {
    ZETASQL_RET_CHECK_EQ(ast_node->node_kind(), AST_SCRIPT);
    script = absl::WrapUnique(ast_node.release()->GetAsOrDie<ASTScript>());
  }
  ZETASQL_RETURN_IF_ERROR(ConvertInternalErrorLocationAndAdjustErrorString(
      error_message_mode, script_string, status));
  *output = absl::make_unique<ParserOutput>(
      parser_options.id_string_pool(), parser_options.arena(),
      std::move(other_allocated_ast_nodes), std::move(script));
  return absl::OkStatus();
}

namespace {
absl::Status ParseNextStatementInternal(ParseResumeLocation* resume_location,
                                        const ParserOptions& parser_options_in,
                                        BisonParserMode mode,
                                        std::unique_ptr<ParserOutput>* output,
                                        bool* at_end_of_input) {
  ParserOptions parser_options = parser_options_in;
  parser_options.CreateDefaultArenasIfNotSet();

  *at_end_of_input = false;
  output->reset();
  ZETASQL_RETURN_IF_ERROR(resume_location->Validate());

  parser::BisonParser parser;
  std::unique_ptr<ASTNode> ast_node;
  std::vector<std::unique_ptr<ASTNode>> other_allocated_ast_nodes;

  int next_statement_byte_offset = 0;

  absl::Status status = parser.Parse(
      mode, resume_location->filename(), resume_location->input(),
      resume_location->byte_position(), parser_options.id_string_pool().get(),
      parser_options.arena().get(), parser_options.language_options(),
      &ast_node, &other_allocated_ast_nodes,
      /*ast_statement_properties=*/nullptr, &next_statement_byte_offset);
  ZETASQL_RETURN_IF_ERROR(
      ConvertInternalErrorLocationToExternal(status, resume_location->input()));

  *at_end_of_input =
      (next_statement_byte_offset == -1 ||
       next_statement_byte_offset == resume_location->input().size());
  if (*at_end_of_input) {
    // Match JavaCC here, even though it doesn't matter at end-of-input.
    next_statement_byte_offset = resume_location->input().size();
  }
  ZETASQL_RET_CHECK(ast_node != nullptr);
  ZETASQL_RET_CHECK(ast_node->IsStatement());
  std::unique_ptr<ASTStatement> statement(
      ast_node.release()->GetAsOrDie<ASTStatement>());
  resume_location->set_byte_position(next_statement_byte_offset);

  *output = absl::make_unique<ParserOutput>(
      parser_options.id_string_pool(), parser_options.arena(),
      std::move(other_allocated_ast_nodes), std::move(statement));
  return absl::OkStatus();
}
}  // namespace

absl::Status ParseNextScriptStatement(ParseResumeLocation* resume_location,
                                      const ParserOptions& parser_options_in,
                                      std::unique_ptr<ParserOutput>* output,
                                      bool* at_end_of_input) {
  return ParseNextStatementInternal(resume_location, parser_options_in,
                                    BisonParserMode::kNextScriptStatement,
                                    output, at_end_of_input);
}

absl::Status ParseNextStatement(ParseResumeLocation* resume_location,
                                const ParserOptions& parser_options_in,
                                std::unique_ptr<ParserOutput>* output,
                                bool* at_end_of_input) {
  return ParseNextStatementInternal(resume_location, parser_options_in,
                                    BisonParserMode::kNextStatement, output,
                                    at_end_of_input);
}

absl::Status ParseType(absl::string_view type_string,
                       const ParserOptions& parser_options_in,
                       std::unique_ptr<ParserOutput>* output) {
  ParserOptions parser_options = parser_options_in;
  parser_options.CreateDefaultArenasIfNotSet();

  parser::BisonParser parser;
  std::unique_ptr<ASTNode> ast_node;
  std::vector<std::unique_ptr<ASTNode>> other_allocated_ast_nodes;
  absl::Status status = parser.Parse(
      BisonParserMode::kType, /* filename = */ absl::string_view(), type_string,
      0 /* offset */, parser_options.id_string_pool().get(),
      parser_options.arena().get(), parser_options.language_options(),
      &ast_node, &other_allocated_ast_nodes,
      /*ast_statement_properties=*/nullptr,
      /*statement_end_byte_offset=*/nullptr);
  ZETASQL_RETURN_IF_ERROR(ConvertInternalErrorLocationToExternal(status, type_string));
  ZETASQL_RET_CHECK(ast_node != nullptr);
  ZETASQL_RET_CHECK(ast_node->IsType());
  std::unique_ptr<ASTType> type(ast_node.release()->GetAsOrDie<ASTType>());

  *output = absl::make_unique<ParserOutput>(
      parser_options.id_string_pool(), parser_options.arena(),
      std::move(other_allocated_ast_nodes), std::move(type));
  return absl::OkStatus();
}

absl::Status ParseExpression(absl::string_view expression_string,
                             const ParserOptions& parser_options_in,
                             std::unique_ptr<ParserOutput>* output) {
  ParserOptions parser_options = parser_options_in;
  parser_options.CreateDefaultArenasIfNotSet();

  parser::BisonParser parser;
  std::unique_ptr<ASTNode> ast_node;
  std::vector<std::unique_ptr<ASTNode>> other_allocated_ast_nodes;
  absl::Status status = parser.Parse(
      BisonParserMode::kExpression, /* filename = */ absl::string_view(),
      expression_string, 0 /* offset */, parser_options.id_string_pool().get(),
      parser_options.arena().get(), parser_options.language_options(),
      &ast_node, &other_allocated_ast_nodes,
      /*ast_statement_properties=*/nullptr,
      /*statement_end_byte_offset=*/nullptr);
  ZETASQL_RETURN_IF_ERROR(
      ConvertInternalErrorLocationToExternal(status, expression_string));
  ZETASQL_RET_CHECK(ast_node != nullptr);
  ZETASQL_RET_CHECK(ast_node->IsExpression());
  std::unique_ptr<ASTExpression> expression(
      ast_node.release()->GetAsOrDie<ASTExpression>());
  *output = absl::make_unique<ParserOutput>(
      parser_options.id_string_pool(), parser_options.arena(),
      std::move(other_allocated_ast_nodes),
      std::move(expression));
  return absl::OkStatus();
}

absl::Status ParseExpression(const ParseResumeLocation& resume_location,
                             const ParserOptions& parser_options_in,
                             std::unique_ptr<ParserOutput>* output) {
  ParserOptions parser_options = parser_options_in;
  parser_options.CreateDefaultArenasIfNotSet();

  parser::BisonParser parser;
  std::unique_ptr<ASTNode> ast_node;
  std::vector<std::unique_ptr<ASTNode>> other_allocated_ast_nodes;
  absl::Status status = parser.Parse(
      BisonParserMode::kExpression, resume_location.filename(),
      resume_location.input(), resume_location.byte_position(),
      parser_options.id_string_pool().get(), parser_options.arena().get(),
      parser_options.language_options(), &ast_node, &other_allocated_ast_nodes,
      /*ast_statement_properties=*/nullptr,
      /*statement_end_byte_offset=*/nullptr);
  ZETASQL_RETURN_IF_ERROR(
      ConvertInternalErrorLocationToExternal(status, resume_location.input()));
  ZETASQL_RET_CHECK(ast_node != nullptr);
  std::unique_ptr<ASTExpression> expression(
      ast_node.release()->GetAsOrDie<ASTExpression>());
  *output = absl::make_unique<ParserOutput>(
      parser_options.id_string_pool(), parser_options.arena(),
      std::move(other_allocated_ast_nodes),
      std::move(expression));
  return absl::OkStatus();
}

ASTNodeKind ParseStatementKind(absl::string_view input,
                               const LanguageOptions& language_options,
                               bool* statement_is_ctas) {
  return ParseNextStatementKind(ParseResumeLocation::FromStringView(input),
                                language_options, statement_is_ctas);
}

ASTNodeKind ParseNextStatementKind(const ParseResumeLocation& resume_location,
                                   const LanguageOptions& language_options,
                                   bool* next_statement_is_ctas) {
  ZETASQL_DCHECK_OK(resume_location.Validate());

  parser::BisonParser parser;
  IdStringPool id_string_pool;
  zetasql_base::UnsafeArena arena(/*block_size=*/1024);
  std::vector<std::unique_ptr<ASTNode>> other_allocated_ast_nodes;
  parser::ASTStatementProperties ast_statement_properties;
  parser
      .Parse(BisonParserMode::kNextStatementKind, resume_location.filename(),
             resume_location.input(), resume_location.byte_position(),
             &id_string_pool, &arena, language_options, /*output=*/nullptr,
             &other_allocated_ast_nodes, &ast_statement_properties,
             /*statement_end_byte_offset=*/nullptr)
      .IgnoreError();
  *next_statement_is_ctas = ast_statement_properties.is_create_table_as_select;
  return ast_statement_properties.node_kind;
}

absl::Status ParseNextStatementProperties(
    const ParseResumeLocation& resume_location,
    const ParserOptions& parser_options,
    std::vector<std::unique_ptr<ASTNode>>* allocated_ast_nodes,
    parser::ASTStatementProperties* ast_statement_properties) {
  ZETASQL_RETURN_IF_ERROR(resume_location.Validate());
  ZETASQL_RET_CHECK(parser_options.AllArenasAreInitialized());

  parser::BisonParser parser;

  // Note that we ignore parse errors since they may occur after we have
  // already determined some of the node properties, and we want to return
  // whatever information we have in that case.  This is also consistent
  // with Parse[Next]StatementKind() above.
  parser.Parse(
      BisonParserMode::kNextStatementKind, resume_location.filename(),
      resume_location.input(), resume_location.byte_position(),
      parser_options.id_string_pool().get(),
      parser_options.arena().get(), parser_options.language_options(),
      /*output=*/nullptr, allocated_ast_nodes,
      ast_statement_properties, /*statement_end_byte_offset=*/nullptr)
          .IgnoreError();
  return absl::OkStatus();
}

}  // namespace zetasql
