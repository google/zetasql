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

#include "zetasql/parser/flex_tokenizer.h"

#include <cctype>
#include <ios>
#include <memory>
#include <sstream>

#include "zetasql/common/errors.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/bison_token_codes.h"
#include "zetasql/parser/flex_istream.h"
#include "zetasql/parser/keywords.h"
#include "zetasql/parser/macros/token_with_location.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "absl/flags/flag.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_macros.h"

#ifndef ZETASQL_TOKEN_DISMABIGUATOR
// TODO: The end state is to turn on everywhere and remove this
// flag. Before that, we'll turn on this feature in test environment and soak
// for a while. Then roll out to Evenflow prod instances and eventually
// deprecate this flag.
ABSL_FLAG(bool, zetasql_use_customized_flex_istream, true,
          "If true, use customized StringStreamWithSentinel to read input.");
#endif

namespace zetasql {
namespace parser {
// Include the helpful type aliases in the namespace within the C++ file so
// that they are useful for free helper functions as well as class member
// functions.
using Token = TokenKinds;
using TokenKind = int;
using Location = ParseLocationRange;
using TokenWithLocation = macros::TokenWithLocation;

absl::StatusOr<TokenKind> ZetaSqlFlexTokenizer::GetNextToken(
    Location* location) {
  int token = GetNextTokenFlexImpl(location);
  if (!override_error_.ok()) {
    return override_error_;
  }
  num_lexical_tokens_++;
  prev_flex_token_ = token;
  return token;
}

static absl::Status MakeError(absl::string_view error_message,
                              const Location& yylloc) {
  return MakeSqlErrorAtPoint(yylloc.start()) << error_message;
}

absl::Status ZetaSqlFlexTokenizer::GetNextToken(ParseLocationRange* location,
                                                  TokenKind* token) {
  ZETASQL_ASSIGN_OR_RETURN(*token, GetNextToken(location));
  return override_error_;
}

bool ZetaSqlFlexTokenizer::IsDotGeneralizedIdentifierPrefixToken(
    TokenKind bison_token) const {
  if (bison_token == Token::IDENTIFIER || bison_token == ')' ||
      bison_token == ']' || bison_token == '?') {
    return true;
  }
  const KeywordInfo* keyword_info = GetKeywordInfoForBisonToken(bison_token);
  if (keyword_info == nullptr) {
    return false;
  }
  return !IsReservedKeyword(keyword_info->keyword());
}

bool ZetaSqlFlexTokenizer::IsReservedKeyword(absl::string_view text) const {
  return language_options_.IsReservedKeyword(text);
}

int ZetaSqlFlexTokenizer::GetIdentifierLength(absl::string_view text) {
  if (text[0] == '`') {
    // Identifier is backquoted. Find the closing quote, accounting for escape
    // sequences.
    for (int i = 1; i < text.size(); ++i) {
      switch (text[i]) {
        case '\\':
          // Next character is a literal - ignore it
          ++i;
          continue;
        case '`':
          // Reached the end of the backquoted string
          return i + 1;
        default:
          break;
      }
    }
    // Backquoted identifier is not closed. For lexer purposes, assume the
    // identifier portion spans the entire text. An error will be issued later.
    return static_cast<int>(text.size());
  }

  // The identifier is not backquoted - the identifier terminates at the first
  // character that is not either a letter, digit, or underscore.
  if (!isalpha(text[0]) && text[0] != '_') {
    return 0;
  }
  for (int i = 1; i < text.size(); ++i) {
    if (!isalnum(text[i]) && text[i] != '_') {
      return i;
    }
  }

  return static_cast<int>(text.size());
}

// Returns true if the given parser mode requires a start token.
// Any new mode should have its own start token.
static bool ModeRequiresStartToken(BisonParserMode mode) {
  switch (mode) {
    case BisonParserMode::kStatement:
    case BisonParserMode::kScript:
    case BisonParserMode::kNextStatement:
    case BisonParserMode::kNextScriptStatement:
    case BisonParserMode::kNextStatementKind:
    case BisonParserMode::kExpression:
    case BisonParserMode::kType:
      return true;
    case BisonParserMode::kTokenizer:
    case BisonParserMode::kTokenizerPreserveComments:
    case BisonParserMode::kMacroBody:
      return false;
  }
}

static bool ShouldTerminateAfterNextStatement(BisonParserMode mode) {
  return mode == BisonParserMode::kNextStatement ||
         mode == BisonParserMode::kNextScriptStatement ||
         mode == BisonParserMode::kNextStatementKind;
}

ZetaSqlFlexTokenizer::ZetaSqlFlexTokenizer(
    BisonParserMode mode, absl::string_view filename, absl::string_view input,
    int start_offset, const LanguageOptions& language_options)
    : filename_(filename),
      input_(input),
      start_offset_(start_offset),
      input_size_(static_cast<int>(input.size())),
      generate_custom_mode_start_token_(ModeRequiresStartToken(mode)),
      terminate_after_statement_(ShouldTerminateAfterNextStatement(mode)),
      preserve_comments_(mode == BisonParserMode::kTokenizerPreserveComments),
      language_options_(language_options) {
  if (absl::GetFlag(FLAGS_zetasql_use_customized_flex_istream)) {
    input_stream_ = std::make_unique<StringStreamWithSentinel>(input);
  } else {
    input_stream_ = std::make_unique<std::istringstream>(
        absl::StrCat(input, kEofSentinelInput));
  }
  // Seek the stringstream to the start_offset, and then instruct flex to read
  // from the stream. (Flex has the ability to read multiple consecutive
  // streams, but we only ever feed it one.)
  input_stream_->seekg(start_offset, std::ios_base::beg);
  switch_streams(/*new_in=*/input_stream_.get(), /*new_out=*/nullptr);
}

void ZetaSqlFlexTokenizer::SetOverrideError(const Location& yylloc,
                                              absl::string_view error_message) {
  override_error_ = MakeError(error_message, yylloc);
}

void ZetaSqlFlexTokenizer::LexerError(const char* msg) {
  override_error_ = MakeSqlError() << msg;
}

bool ZetaSqlFlexTokenizer::AreMacrosEnabled() const {
  return language_options_.LanguageFeatureEnabled(FEATURE_V_1_4_SQL_MACROS);
}

bool ZetaSqlFlexTokenizer::EnforceStrictMacros() const {
  return language_options_.LanguageFeatureEnabled(
      FEATURE_V_1_4_ENFORCE_STRICT_MACROS);
}

bool ZetaSqlFlexTokenizer::AreAlterArrayOptionsEnabled() const {
  return language_options_.LanguageFeatureEnabled(
      FEATURE_ENABLE_ALTER_ARRAY_OPTIONS);
}

}  // namespace parser
}  // namespace zetasql
