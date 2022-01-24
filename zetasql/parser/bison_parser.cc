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

#include "zetasql/parser/bison_parser.h"

#include <cctype>
#include <cstdint>
#include <set>
#include <string>
#include <utility>

#include "zetasql/common/errors.h"
#include "zetasql/common/utf_util.h"
#include "zetasql/parser/bison_parser.bison.h"
#include "zetasql/parser/keywords.h"
#include "zetasql/public/id_string.h"
#include <cstdint>
#include "absl/cleanup/cleanup.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "re2/re2.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"



namespace zetasql {
namespace parser {

// Bison parser return values.
//
// Source:
// https://www.gnu.org/software/bison/manual/html_node/Parser-Function.html#Parser-Function
constexpr int kBisonParseSuccess = 0;
constexpr int kBisonParseError = 1;
constexpr int kBisonMemoryExhausted = 2;

BisonParser::BisonParser() {}
BisonParser::~BisonParser() {}

static std::string GetBisonParserModeName(BisonParserMode mode) {
  switch (mode) {
    case BisonParserMode::kExpression:
      return "expression";
    case BisonParserMode::kType:
      return "type";
    case BisonParserMode::kStatement:
    case BisonParserMode::kNextStatement:
    case BisonParserMode::kNextScriptStatement:
    case BisonParserMode::kNextStatementKind:
      return "statement";
    case BisonParserMode::kScript:
      return "script";
    case BisonParserMode::kTokenizer:
    case BisonParserMode::kTokenizerPreserveComments:
      ZETASQL_LOG(FATAL) << "CleanUpBisonError called in tokenizer mode";
  }
}

// The longest literal value that we're willing to echo in an error message.
// This value is in bytes, not characters. It includes the quotes. The value was
// chosen so that "Unexpected string literal" messages typically still fit on
// one line. (Not an 80-column line, but a realistic line in a query UI.)
//
// When you change this limit, also adjust the test literals in
// parser_errors.test.
static constexpr int kMaxErrorLiteralLength = 50;

static std::string ShortenStringLiteralForError(absl::string_view literal) {
  if (literal.size() > kMaxErrorLiteralLength) {
    // Shorten by inserting "..." before the final quotes.
    int num_end_quotes = 1;
    if (absl::StartsWith(literal, "\"\"\"") ||
        absl::StartsWith(literal, "\'\'\'") ||
        absl::StartsWithIgnoreCase(literal, "r\"\"\"") ||
        absl::StartsWithIgnoreCase(literal, "r\'\'\'")) {
      num_end_quotes = 3;
    }
    int excerpt_size = kMaxErrorLiteralLength - num_end_quotes;
    // If we can't remove at least four bytes in addition to the quotes, then
    // we're not narrowing the output at all. If that happens, just keep the
    // original. The net effect is that all narrowed literals have the same
    // length in bytes, but we'll let some literals through that are just over
    // the limit. (Note that the constant size is in bytes, not characters. This
    // is not that important.)
    if (excerpt_size >
        static_cast<int64_t>(literal.size()) - num_end_quotes - 4) {
      return std::string(literal);
    }
    // Don't cut the string off in the middle of a multibyte character.
    while (!IsWellFormedUTF8(literal.substr(0, excerpt_size))) {
      --excerpt_size;
    }
    return absl::StrCat(
        literal.substr(0, excerpt_size), "...",
        absl::ClippedSubstr(literal, literal.size() - num_end_quotes,
                            num_end_quotes));
  }
  return std::string(literal);
}

static std::string ShortenBytesLiteralForError(absl::string_view literal) {
  if (literal.size() < kMaxErrorLiteralLength) {
    return std::string(literal);
  }
  // Shorten by inserting "..." before the final quotes.
  int num_end_quotes = 1;
  if (absl::StartsWithIgnoreCase(literal, "b\"\"\"") ||
      absl::StartsWithIgnoreCase(literal, "rb\"\"\"") ||
      absl::StartsWithIgnoreCase(literal, "br\"\"\"") ||
      absl::StartsWithIgnoreCase(literal, "b\'\'\'") ||
      absl::StartsWithIgnoreCase(literal, "rb\'\'\'") ||
      absl::StartsWithIgnoreCase(literal, "br\'\'\'")) {
    num_end_quotes = 3;
  }
  const int excerpt_size = kMaxErrorLiteralLength - num_end_quotes;
  // If we can't remove at least four bytes in addition to the quotes, then
  // we're not narrowing the output at all. If that happens, just keep the
  // original. The net effect is that all narrowed literals have the same
  // length in bytes, but we'll let some literals through that are just over
  // the limit.
  if (excerpt_size >
      static_cast<int64_t>(literal.size()) - num_end_quotes - 4) {
    return std::string(literal);
  }
  return absl::StrCat(
      literal.substr(0, excerpt_size), "...",
      absl::ClippedSubstr(literal, literal.size() - num_end_quotes,
                          num_end_quotes));
}

// Generates an error message for a Bison syntax error at location
// 'error_location' based on the Bison-generated error message
// 'bison_error_message'. The other arguments should match those passed into
// BisonParser::Parse(). It is required that 'bison_error_message' is the actual
// error message produced by the bison parser for the given inputs.
static absl::StatusOr<std::string> GenerateImprovedBisonSyntaxError(
    const LanguageOptions& language_options, ParseLocationPoint error_location,
    absl::string_view bison_error_message, BisonParserMode mode,
    absl::string_view input, int start_offset) {
  // Bison error messages are always of the form "syntax error, unexpected X,
  // expecting Y", where Y may be of the form "A" or "A or B" or "A or B or C".
  // It will use $end to indicate "end of input". We don't want to have the text
  // X because we can generate a better description ourselves. However, we do
  // want to have the expectations from 'bison_error_message', because they may
  // be useful.
  static LazyRE2 re_expectations = {
      "syntax error, unexpected .*, expecting (.*)"};
  // If there's no match, then 'expectations_string' will be empty.
  std::string expectations_string;
  RE2::FullMatch(bison_error_message, *re_expectations, &expectations_string);

  // Transform the individual expectations, because Bison gives some weird
  // output for some of them.
  std::vector<std::string> expectations =
      absl::StrSplit(expectations_string, " or ", absl::SkipEmpty());
  for (std::string& expectation : expectations) {
    if (expectation.size() == 1 && !isalpha(expectation[0])) {
      expectation = absl::StrCat("\"", expectation, "\"");
    }
    if (absl::StrContains(expectation, " for ")) {
      // There are several tokens of the form "KEYWORD for CONTEXT" that the
      // parser knows about but that we shouldn't expose externally. Strip off
      // the " for CONTEXT" bit.
      expectation = expectation.substr(0, expectation.find(" for "));
    }
    // These are a single token in the Bison tokenizer but we treat them as two
    // tokens externally.
    if (expectation == "\".*\"") {
      expectation = "\".\"";
    } else if (expectation == "\"@{\"") {
      expectation = "\"@\"";
    } else if (expectation == "@@") {
      expectation = "\"@@\"";
    }
    // If it looks like an uppercased keyword, say it's a keyword. All other
    // things such as string literals are described in lower case in the Bison
    // parser's token names.
    static LazyRE2 re_keyword = {"[A-Z]+"};
    if (RE2::FullMatch(expectation, *re_keyword)) {
      expectation = absl::StrCat("keyword ", expectation);
    }
  }
  // Deduplicate expectations. Transformations may have introduced duplicates.
  std::set<std::string> expectations_set(expectations.begin(),
                                         expectations.end());

  // Remove an expectation that is sure to raise question marks with most users.
  // Positional parameters ("?") are only there for some specific client
  // libraries outside Google.
  expectations_set.erase("\"?\"");
  // When the user has entered another token, they don't intend to end the
  // statement, so giving that as an alternative option makes little sense.
  // Bison also tends to give it as the only option when there is an unexpected
  // token in the middle of an expression, even though there are other possible
  // continuations. This may be triggered by the fact that we use operator
  // precedence parsing.
  expectations_set.erase("$end");

  // TODO: Make this conditional on the language features that are
  // enabled, and remove other elements from the expectations that are not
  // supported by the enabled language features.

  // Re-parse the input so that we can get the token at the error location. We
  // start the tokenizer in kTokenizer mode because we don't need to get a bogus
  // token at the start to indicate the statement type. That token interferes
  // with errors at offset 0.
  auto tokenizer = absl::make_unique<ZetaSqlFlexTokenizer>(
      BisonParserMode::kTokenizer, error_location.filename(), input,
      start_offset, language_options);
  ParseLocationRange token_location;
  int token = -1;
  while (token != 0) {
    ZETASQL_RETURN_IF_ERROR(tokenizer->GetNextToken(&token_location, &token));
    // Bison always returns parse errors at token boundaries, so this should
    // never happen.
    ZETASQL_RET_CHECK_GE(error_location.GetByteOffset(),
                 token_location.start().GetByteOffset());
    if (token == 0 || error_location.GetByteOffset() ==
                          token_location.start().GetByteOffset()) {
      const absl::string_view token_text =
          absl::ClippedSubstr(input, token_location.start().GetByteOffset(),
                              token_location.end().GetByteOffset() -
                                  token_location.start().GetByteOffset());
      std::string actual_token_description;
      if (token == 0) {
        // The error location was at end-of-input, so this is an
        // unexpected-end-of error.
        actual_token_description =
            absl::StrCat("end of ", GetBisonParserModeName(mode));
      } else if (token ==
                 zetasql_bison_parser::BisonParserImpl::token::KW_OVER) {
        // When the OVER keyword is used in the wrong place, we tell the user
        // exactly where it can be used.
        return std::string(
            "Syntax error: OVER keyword must follow a function call");
      } else if (const KeywordInfo* keyword_info =
                     GetKeywordInfoForBisonToken(token)) {
        actual_token_description =
            absl::StrCat("keyword ", keyword_info->keyword());
      } else if (token == zetasql_bison_parser::BisonParserImpl::token::
                              STRING_LITERAL) {
        // Escape physical newlines, to avoid multiline error messages. (Note
        // that this is technically incorrect for raw string literals.)
        std::string escaped_token_text = std::string(token_text);
        absl::StrReplaceAll({{"\r", "\\r"}}, &escaped_token_text);
        absl::StrReplaceAll({{"\n", "\\n"}}, &escaped_token_text);
        actual_token_description =
            absl::StrCat("string literal ",
                         ShortenStringLiteralForError(escaped_token_text));
      } else if (token == zetasql_bison_parser::BisonParserImpl::token::
                              BYTES_LITERAL) {
        // Escape physical newlines, to avoid multiline error messages. (Note
        // that this is technically incorrect for raw bytes literals.)
        std::string escaped_token_text = std::string(token_text);
        absl::StrReplaceAll({{"\r", "\\r"}}, &escaped_token_text);
        absl::StrReplaceAll({{"\n", "\\n"}}, &escaped_token_text);
        actual_token_description = absl::StrCat(
            "bytes literal ", ShortenBytesLiteralForError(escaped_token_text));
      } else if (token == zetasql_bison_parser::BisonParserImpl::token::
                              INTEGER_LITERAL) {
        actual_token_description =
            absl::StrCat("integer literal \"", token_text, "\"");
      } else if (token == zetasql_bison_parser::BisonParserImpl::token::
                              FLOATING_POINT_LITERAL) {
        actual_token_description =
            absl::StrCat("floating point literal \"", token_text, "\"");
      } else if (token ==
                 zetasql_bison_parser::BisonParserImpl::token::IDENTIFIER) {
        if (token_text[0] == '`') {
          // Don't put extra quotes around an already-backquoted identifier.
          actual_token_description = absl::StrCat("identifier ", token_text);
        } else {
          actual_token_description =
              absl::StrCat("identifier \"", token_text, "\"");
        }
      } else if (token == ';') {
        // The ";" token includes trailing whitespace, and we don't want to
        // echo that back.
        actual_token_description = "\";\"";
      } else if (token ==
                 zetasql_bison_parser::BisonParserImpl::token::KW_OPEN_HINT) {
        // This is a single token for "@{", but we want to expose this as "@"
        // externally.
        actual_token_description = "\"@\"";
      } else if (token ==
                 zetasql_bison_parser::BisonParserImpl::token::KW_DOUBLE_AT) {
        actual_token_description = "\"@@\"";
      } else if (token ==
                 zetasql_bison_parser::BisonParserImpl::token::KW_DOT_STAR) {
        // This is a single token for ".*", but we want to expose this as "."
        // externally.
        actual_token_description = "\".\"";
      } else {
        actual_token_description = absl::StrCat("\"", token_text, "\"");
      }
      if (!expectations_set.empty()) {
        return absl::StrCat("Syntax error: Expected ",
                            absl::StrJoin(expectations_set, " or "),
                            " but got ", actual_token_description);
      }
      return absl::StrCat("Syntax error: Unexpected ",
                          actual_token_description);
    }
  }
  ZETASQL_LOG(DFATAL) << "Syntax error location not found in input";
  return MakeSqlErrorAtPoint(error_location) << bison_error_message;
}

absl::Status BisonParser::Parse(
    BisonParserMode mode, absl::string_view filename, absl::string_view input,
    int start_byte_offset, IdStringPool* id_string_pool, zetasql_base::UnsafeArena* arena,
    const LanguageOptions& language_options, std::unique_ptr<ASTNode>* output,
    std::vector<std::unique_ptr<ASTNode>>* other_allocated_ast_nodes,
    ASTStatementProperties* ast_statement_properties,
    int* statement_end_byte_offset) {
  id_string_pool_ = id_string_pool;
  arena_ = arena;
  language_options_ = &language_options;
  allocated_ast_nodes_ =
      absl::make_unique<std::vector<std::unique_ptr<ASTNode>>>();
  auto clean_up_allocated_ast_nodes =
      absl::MakeCleanup([&] { allocated_ast_nodes_.reset(); });
  // We must have the filename outlive the <resume_location>, since the
  // parse tree ASTNodes will reference it in their ParseLocationRanges.
  // So we allocate a new filename from the ParserOptions IdStringPool,
  // since that will be given to the ParserOutput and the filename allocated
  // there will outlive the ASTNodes that reference it.
  filename_ = id_string_pool->Make(filename);
  input_ = input;
  tokenizer_ = absl::make_unique<ZetaSqlFlexTokenizer>(
      mode, filename_.ToStringView(), input_, start_byte_offset,
      language_options);
  ASTNode* output_node = nullptr;
  std::string error_message;
  ParseLocationPoint error_location;
  bool move_error_location_past_whitespace = false;
  std::vector<ASTNode*> nodes_requiring_init_fields;
  zetasql_bison_parser::BisonParserImpl bison_parser_impl(
      tokenizer_.get(), this, &output_node, ast_statement_properties,
      &error_message, &error_location, &move_error_location_past_whitespace,
      statement_end_byte_offset);
  const int parse_status_code = bison_parser_impl.parse();
  if (parse_status_code == kBisonParseSuccess &&
      tokenizer_->GetOverrideError().ok()) {
    // Make sure InitFields() is called for all ASTNodes that were created.
    // We don't use the result of InitFields() in the grammar itself, so we
    // don't need to do this during parsing.
    for (const auto& ast_node : *allocated_ast_nodes_) {
      ast_node->InitFields();
    }

    if (mode != BisonParserMode::kNextStatementKind) {
      ZETASQL_RET_CHECK(output_node != nullptr);
      *output = nullptr;
      // Move 'output_node' out of 'allocated_ast_nodes_' and into '*output'.
      for (int i = allocated_ast_nodes_->size() - 1; i >= 0; --i) {
        if ((*allocated_ast_nodes_)[i].get() == output_node) {
          *output = std::move((*allocated_ast_nodes_)[i]);
          // There's no need to erase the entry in allocated_ast_nodes_.
          break;
        }
      }
      ZETASQL_RET_CHECK_EQ(output->get(), output_node);
    }
    *other_allocated_ast_nodes = std::move(*allocated_ast_nodes_);
    return absl::OkStatus();
  }
  // The tokenizer's error overrides the parser's error.
  ZETASQL_RETURN_IF_ERROR(tokenizer_->GetOverrideError());
  if (parse_status_code == kBisonParseError) {
    if (move_error_location_past_whitespace) {
      // In rare cases we manually generate parse errors when tokens are
      // missing. In those cases we typically don't have the position of the
      // next token available, and the parser can request that the error
      // location be moved past any whitespace onto the next token.
      ZetaSqlFlexTokenizer skip_whitespace_tokenizer(
          BisonParserMode::kTokenizer, filename_.ToStringView(), input_,
          error_location.GetByteOffset(), this->language_options());
      ParseLocationRange next_token_location;
      int token;
      ZETASQL_RETURN_IF_ERROR(
          skip_whitespace_tokenizer.GetNextToken(&next_token_location, &token));
      // Ignore the token. We only care about its starting location.
      error_location = next_token_location.start();
    }
    // Bison returns error messages that start with "syntax error, ". The parser
    // logic itself will return an empty error message if it wants to generate
    // a simple "Unexpected X" error.
    if (absl::StartsWith(error_message, "syntax error, ") ||
        error_message.empty()) {
      // This was a Bison-generated syntax error. Generate a message that is to
      // our own liking.
      ZETASQL_ASSIGN_OR_RETURN(error_message,
                       GenerateImprovedBisonSyntaxError(
                           language_options, error_location, error_message,
                           mode, input_, start_byte_offset));
    }
    return MakeSqlErrorAtPoint(error_location) << error_message;
  }
  // The Bison C++ parser skeleton doesn't actually return this code. We handle
  // it here for completeness in case it starts returning this code at some
  // point.
  if (parse_status_code == kBisonMemoryExhausted) {
    return MakeSqlError() << "Input too large";
  }
  ZETASQL_RET_CHECK_FAIL() << "Parser returned undefined return code "
                   << parse_status_code;
}

absl::string_view BisonParser::GetFirstTokenOfNode(
    zetasql_bison_parser::location& bison_location) const {
  absl::string_view text = GetInputText(bison_location);
  for (int i = 0; i < text.size(); i++) {
    if (absl::ascii_isblank(text[i])) {
      return text.substr(0, i);
    }
  }
  return text;
}

}  // namespace parser
}  // namespace zetasql
