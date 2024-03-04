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

#include "zetasql/parser/token_disambiguator.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <variant>

#include "zetasql/base/arena.h"
#include "zetasql/common/errors.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/bison_token_codes.h"
#include "zetasql/parser/flex_tokenizer.h"
#include "zetasql/parser/macros/flex_token_provider.h"
#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/macros/macro_expander.h"
#include "zetasql/parser/macros/token_with_location.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/base/check.h"
#include "absl/log/log.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// Implementation of the wrapper calls forward-declared in parser_internal.h.
// This workaround is to avoid creating an interface and incurring a v-table
// lookup on every token.
namespace parser_internal {
using zetasql::parser::BisonParserMode;
using zetasql::parser::DisambiguatorLexer;

void SetForceTerminate(DisambiguatorLexer* disambiguator, int* end_offset) {
  return disambiguator->SetForceTerminate(end_offset);
}
void PushBisonParserMode(DisambiguatorLexer* disambiguator,
                         BisonParserMode mode) {
  return disambiguator->PushBisonParserMode(mode);
}
void PopBisonParserMode(DisambiguatorLexer* disambiguator) {
  return disambiguator->PopBisonParserMode();
}
int GetNextToken(DisambiguatorLexer* disambiguator, absl::string_view* text,
                 ParseLocationRange* location) {
  return disambiguator->GetNextToken(text, location);
}
}  // namespace parser_internal

namespace parser {
// Include the helpful type aliases in the namespace within the C++ file so
// that they are useful for free helper functions as well as class member
// functions.
using Token = TokenKinds;
using TokenKind = int;
using Location = ParseLocationRange;
using TokenWithLocation = macros::TokenWithLocation;
using FlexTokenProvider = macros::FlexTokenProvider;
using MacroCatalog = macros::MacroCatalog;
using MacroExpander = macros::MacroExpander;
using MacroExpanderBase = macros::MacroExpanderBase;
using TokenWithLocation = macros::TokenWithLocation;

static bool IsReservedKeywordToken(TokenKind token) {
  // We need to add sentinels before and after each block of keywords to make
  // this safe.
  return token > Token::SENTINEL_RESERVED_KW_START &&
         token < Token::SENTINEL_RESERVED_KW_END;
}

static bool IsNonreservedKeywordToken(TokenKind token) {
  // We need to add sentinels before and after each block of keywords to make
  // this safe.
  return token > Token::SENTINEL_NONRESERVED_KW_START &&
         token < Token::SENTINEL_NONRESERVED_KW_END;
}

static absl::Status MakeError(absl::string_view error_message,
                              const Location& yylloc) {
  return MakeSqlErrorAtPoint(yylloc.start()) << error_message;
}

static TokenKind DisambiguateModeStartToken(BisonParserMode mode) {
  switch (mode) {
    case BisonParserMode::kStatement:
      return Token::MODE_STATEMENT;
    case BisonParserMode::kScript:
      return Token::MODE_SCRIPT;
    case BisonParserMode::kNextStatement:
      return Token::MODE_NEXT_STATEMENT;
    case BisonParserMode::kNextScriptStatement:
      return Token::MODE_NEXT_SCRIPT_STATEMENT;
    case BisonParserMode::kNextStatementKind:
      return Token::MODE_NEXT_STATEMENT_KIND;
    case BisonParserMode::kExpression:
      return Token::MODE_EXPRESSION;
    case BisonParserMode::kType:
      return Token::MODE_TYPE;

    case BisonParserMode::kTokenizer:
    case BisonParserMode::kTokenizerPreserveComments:
    case BisonParserMode::kMacroBody:
      ABSL_LOG(FATAL) << "BisonParserMode: " << static_cast<int>(mode)
                 << " should not disambiguate CUSTOM_START_TOKEN";
      return -1;

    default:
      ABSL_LOG(FATAL) << "Unkonwn BisonParserMode: " << static_cast<int>(mode);
      return -1;
  }
}

// The token disambiguation rules are allowed to see a fixed-length sequence of
// tokens produced by the lexical rules in flex_tokenzer.h and may change the
// kind of `token` based on the kinds of the other tokens in the window.
//
// For now, the window available is:
//   [token, Lookahead1()]
//
// `token` is the token that is about to be dispensed to the consuming
//     component.
// `Lookahead1()` is the next token that will be disambiguated on the subsequent
//     call to GetNextToken.
//
// USE WITH CAUTION:
// For any given sequence of tokens, there may be many different shift/reduce
// sequences in the parser that "accept" that token sequence. It's critical
// when adding a token disambiguation rule that all parts of the grammar that
// accept the sequence of tokens are identified to verify that changing the kind
// of `token` does not break any unanticipated cases where that sequence would
// currently be accepted.
TokenKind DisambiguatorLexer::ApplyTokenDisambiguation(
    TokenKind token, const Location& location) {
  switch (mode_) {
    case BisonParserMode::kTokenizer:
    case BisonParserMode::kTokenizerPreserveComments:
      // Tokenizer modes are used to extract tokens for error messages among
      // other things. The rules below are mostly intended to support the bison
      // parser, and aren't necessary in tokenizer mode.
      // For keywords that have context-dependent variations, return the
      // "standard" one.
      switch (token) {
        case Token::KW_AND_FOR_BETWEEN:
          return Token::KW_AND;
        case Token::KW_FULL_IN_SET_OP:
          return Token::KW_FULL;
        case Token::KW_LEFT_IN_SET_OP:
          return Token::KW_LEFT;
        case Token::KW_DEFINE_FOR_MACROS:
          return Token::KW_DEFINE;
        case Token::KW_OPEN_HINT:
        case Token::KW_OPEN_INTEGER_HINT:
          return '@';
        default:
          return token;
      }
    case BisonParserMode::kMacroBody:
      switch (token) {
        case ';':
        case Token::YYEOF:
          return token;
        default:
          return Token::MACRO_BODY_TOKEN;
      }
    default:
      break;
  }

  switch (token) {
    case Token::CUSTOM_MODE_START: {
      return DisambiguateModeStartToken(mode_);
    }
    case Token::KW_NOT: {
      // This returns a different token because returning KW_NOT would confuse
      // the operator precedence parsing. Boolean NOT has a different
      // precedence than NOT BETWEEN/IN/LIKE/DISTINCT.
      switch (Lookahead1(location)) {
        case Token::KW_BETWEEN:
        case Token::KW_IN:
        case Token::KW_LIKE:
        case Token::KW_DISTINCT:
          return Token::KW_NOT_SPECIAL;
        default:
          break;
      }
      break;
    }
    case Token::KW_WITH: {
      // The WITH expression uses a function-call like syntax and is followed by
      // the open parenthesis.
      if (Lookahead1(location) == '(') {
        return Token::KW_WITH_STARTING_WITH_EXPRESSION;
      }
      break;
    }
    case Token::KW_EXCEPT: {
      // EXCEPT is used in two locations of the language. And when the parser is
      // exploding the rules it detects that two rules can be used for the same
      // syntax.
      //
      // This rule generates a special token for an EXCEPT that is followed by a
      // hint, ALL or DISTINCT which is distinctly the set operator use.
      switch (Lookahead1(location)) {
        case '(':
          // This is the SELECT * EXCEPT (column...) case.
          return Token::KW_EXCEPT;
        case Token::KW_ALL:
        case Token::KW_DISTINCT:
        case Token::KW_OPEN_HINT:
        case Token::KW_OPEN_INTEGER_HINT:
          // This is the {query} EXCEPT {opt_hint} ALL|DISTINCT {query} case.
          return Token::KW_EXCEPT_IN_SET_OP;
        default:
          return SetOverrideErrorAndReturnEof(
              "EXCEPT must be followed by ALL, DISTINCT, or \"(\"", location);
      }
      break;
    }
    case Token::KW_FULL: {
      // If FULL is used in set operations, return KW_FULL_IN_SET_OP instead.
      TokenKind lookahead = Lookahead1(location) == Token::KW_OUTER
                                ? Lookahead2(location)
                                : Lookahead1(location);
      switch (lookahead) {
        case Token::KW_UNION:
        case Token::KW_INTERSECT:
        case Token::KW_EXCEPT:
          return Token::KW_FULL_IN_SET_OP;
      }
      break;
    }
    case Token::KW_QUALIFY_NONRESERVED: {
      if (IsReservedKeyword("QUALIFY")) {
        return Token::KW_QUALIFY_RESERVED;
      }
      break;
    }
    default: {
      break;
    }
  }

  return token;
}

TokenKind DisambiguatorLexer::SetOverrideErrorAndReturnEof(
    absl::string_view error_message, const Location& error_location) {
  override_error_ = MakeError(error_message, error_location);
  return Token::YYEOF;
}

namespace {
class NoOpExpander : public MacroExpanderBase {
 public:
  explicit NoOpExpander(std::unique_ptr<FlexTokenProvider> token_provider)
      : token_provider_(std::move(token_provider)) {}
  absl::StatusOr<TokenWithLocation> GetNextToken() override {
    return token_provider_->ConsumeNextToken();
  }
  int num_unexpanded_tokens_consumed() const override {
    return token_provider_->num_consumed_tokens();
  }

 private:
  std::unique_ptr<FlexTokenProvider> token_provider_;
};
}  // namespace

// Retrieve the next token, and split absl::StatusOr<TokenWithLocation> into
// separate variables for compatibility with Bison. If the status is not ok(),
// the token is set to YYEOF and location is not updated.
static void ConsumeNextToken(MacroExpanderBase& expander,
                             TokenWithLocation& token_with_location,
                             absl::Status& status) {
  absl::StatusOr<TokenWithLocation> next_token = expander.GetNextToken();
  if (next_token.ok()) {
    token_with_location = *next_token;
  } else {
    token_with_location.kind = Token::YYEOF;
    status = std::move(next_token.status());
  }
}

int DisambiguatorLexer::Lookahead1(const Location& current_token_location) {
  if (lookahead_1_.has_value()) {
    return lookahead_1_->kind;
  }

  lookahead_1_ = {.kind = Token::YYEOF,
                  .location = current_token_location,
                  .text = "",
                  .preceding_whitespaces = ""};
  if (force_terminate_) {
    return lookahead_1_->kind;
  }

  ConsumeNextToken(*macro_expander_, *lookahead_1_, override_error_);
  return lookahead_1_->kind;
}

int DisambiguatorLexer::Lookahead2(const Location& current_token_location) {
  if (lookahead_2_.has_value()) {
    // If `force_terminate_` is true, the token kind stored in `lookahead_2_`
    // has been overwritten as YYEOF.
    return lookahead_2_->kind;
  }
  if (force_terminate_) {
    return Token::YYEOF;
  }
  if (!lookahead_1_.has_value()) {
    Lookahead1(current_token_location);
  }
  lookahead_2_ = {.kind = Token::YYEOF,
                  .location = lookahead_1_->location,
                  .text = "",
                  .preceding_whitespaces = ""};
  ConsumeNextToken(*macro_expander_, *lookahead_2_, override_error_);
  return lookahead_2_->kind;
}

// Returns the next token id, returning its location in 'yylloc'. On input,
// 'yylloc' must be the location of the previous token that was returned.
TokenKind DisambiguatorLexer::GetNextToken(absl::string_view* text,
                                           Location* yylloc) {
  if (lookahead_1_.has_value()) {
    if (prev_token_.has_value()) {
      ABSL_DCHECK_EQ(prev_token_->location, *yylloc);
    }
    // Get the next token from the lookahead buffer and advance the buffer. If
    // force_terminate_ was set, we still need the location from the buffer,
    // with Token::YYEOF as the token. Calling SetForceTerminate() should have
    // updated the lookahead.
    prev_token_.swap(lookahead_1_);
    lookahead_1_.swap(lookahead_2_);
    lookahead_2_.reset();
  } else if (force_terminate_) {
    return Token::YYEOF;
  } else {
    // The lookahead buffer is empty, so get a token from the underlying lexer.
    ConsumeNextToken(*macro_expander_, prev_token_.emplace(), override_error_);
  }
  *yylloc = prev_token_->location;
  *text = prev_token_->text;
  return ApplyTokenDisambiguation(prev_token_->kind, *yylloc);
}

static const MacroCatalog* empty_macro_catalog() {
  static MacroCatalog* empty_macro_catalog = new MacroCatalog();
  return empty_macro_catalog;
}

absl::StatusOr<std::unique_ptr<DisambiguatorLexer>> DisambiguatorLexer::Create(
    BisonParserMode mode, absl::string_view filename, absl::string_view input,
    int start_offset, const LanguageOptions& language_options,
    const macros::MacroCatalog* macro_catalog, zetasql_base::UnsafeArena* arena) {
  auto token_provider = std::make_unique<FlexTokenProvider>(
      mode, filename, input, start_offset, language_options);

  std::unique_ptr<MacroExpanderBase> macro_expander;
  if (language_options.LanguageFeatureEnabled(FEATURE_V_1_4_SQL_MACROS)) {
    if (macro_catalog == nullptr) {
      macro_catalog = empty_macro_catalog();
    }
    macro_expander = std::make_unique<MacroExpander>(
        std::move(token_provider), *macro_catalog, arena, ErrorMessageOptions{},
        /*parent_location=*/nullptr);
  } else {
    ZETASQL_RET_CHECK(macro_catalog == nullptr);
    macro_expander = std::make_unique<NoOpExpander>(std::move(token_provider));
  }
  return absl::WrapUnique(new DisambiguatorLexer(mode, language_options,
                                                 std::move(macro_expander)));
}

DisambiguatorLexer::DisambiguatorLexer(
    BisonParserMode mode, const LanguageOptions& language_options,
    std::unique_ptr<MacroExpanderBase> expander)
    : mode_(mode),
      language_options_(language_options),
      macro_expander_(std::move(expander)) {}

int64_t DisambiguatorLexer::num_lexical_tokens() const {
  return macro_expander_->num_unexpanded_tokens_consumed();
}

// TODO: this overload should also be updated to return the image, and
// all callers should be updated. In fact, all callers should simply use
// TokenWithLocation, and maybe have the image attached there.
absl::Status DisambiguatorLexer::GetNextToken(ParseLocationRange* location,
                                              TokenKind* token) {
  absl::string_view image;
  *token = GetNextToken(&image, location);
  return GetOverrideError();
}

void DisambiguatorLexer::SetForceTerminate(int* end_byte_offset) {
  // Ensure that the lookahead buffers immediately reflects the termination,
  // but only after making sure to seek to the end offset if all that's left
  // is whitespace.
  bool lookahead_ok = false;
  Location last_returned_location =
      prev_token_.has_value() ? prev_token_->location : Location();
  if (!lookahead_1_.has_value()) {
    // Quick fix for b/325081404 to unblock TAP. Further investigation of the
    // invariant needed.
    // TODO: investigate why that case is calling
    // SetForceTerminate() when it already hit an error.
    absl::Status original_status;
    std::swap(override_error_, original_status);

    Lookahead1(last_returned_location);
    lookahead_ok = override_error_.ok();

    // Restore the old error.
    // If we hit an error on the lookahead, this is a problem on the next
    // statement, not the current one. We should not let it take effect.
    std::swap(override_error_, original_status);
  }
  if (end_byte_offset != nullptr) {
    // If the next token is YYEOF, update the end_byte_offset to the end.
    *end_byte_offset = lookahead_1_->kind == Token::YYEOF && lookahead_ok
                           ? lookahead_1_->location.end().GetByteOffset()
                           : last_returned_location.end().GetByteOffset();
  }
  force_terminate_ = true;
  // Ensure that the lookahead buffers immediately reflects the termination.
  if (lookahead_1_.has_value()) {
    lookahead_1_->kind = Token::YYEOF;
  }
  if (lookahead_2_.has_value()) {
    lookahead_2_->kind = Token::YYEOF;
  }
}

void DisambiguatorLexer::PushBisonParserMode(BisonParserMode mode) {
  restore_modes_.push(mode_);
  mode_ = mode;
}

void DisambiguatorLexer::PopBisonParserMode() {
  ABSL_DCHECK(!restore_modes_.empty());
  mode_ = restore_modes_.top();
  restore_modes_.pop();
}

}  // namespace parser
}  // namespace zetasql
