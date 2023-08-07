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

#include <ios>
#include <memory>
#include <sstream>

#include "zetasql/common/errors.h"
#include "zetasql/parser/bison_parser.bison.h"
#include "zetasql/parser/flex_istream.h"
#include "zetasql/parser/keywords.h"
#include "zetasql/parser/location.hh"
#include "zetasql/public/parse_location.h"
#include "absl/flags/flag.h"

// TODO: The end state is to turn on everywhere and remove this
// flag. Before that, we'll turn on this feature in test environment and soak
// for a while. Then roll out to Evenflow prod instances and eventually
// deprecate this flag.
ABSL_FLAG(bool, zetasql_use_customized_flex_istream, true,
          "If true, use customized StringStreamWithSentinel to read input.");

namespace zetasql {
namespace parser {
// Include the helpful type aliases in the namespace within the C++ file so
// that they are useful for free helper functions as well as class member
// functions.
using Token = zetasql_bison_parser::BisonParserImpl::token;
using TokenKind = int;
using Location = zetasql_bison_parser::location;

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

// The token disambiguation rules are allowed to see a fixed-length sequence of
// tokens produced by the lexical rules in flex_tokenzer.h and may change the
// kind of `token` based on the kinds of the other tokens in the window.
//
// For now, the window available is:
//   [prev_dispensed_token_, token, Lookahead1()]
//
// `prev_dispensed_token_` is the token most recently dispensed to the consuming
//     component (usually the parser).
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
TokenKind ZetaSqlFlexTokenizer::ApplyTokenDisambiguation(
    TokenKind token, const Location& location) {
  switch (mode_) {
    case BisonParserMode::kTokenizer:
    case BisonParserMode::kTokenizerPreserveComments:
      // Tokenizer modes are used to extract tokens for error messages among
      // other things. The rules below are mostly intended to support the bison
      // parser, and aren't necessary in tokenizer mode.
      return token;
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

  switch (prev_dispensed_token_) {
    case '@':
    case Token::KW_DOUBLE_AT:
      // The only place in the grammar that '@' or KW_DOUBLE_AT appear is in
      // rules for query parameters and system variables respectively. And
      // macros rules, but those accept all tokens. For both query parameters
      // and system variables, the following token is always treated as an
      // identifier even if it is otherwise a reserved keyword.
      //
      // This rule results in minor improvements to both the implemented parser
      // (the generated code) in terms of number of states and the grammar file
      // by eliminating a few lines. The significant improvement is greatly
      // reducing the complexity of analyzing subsequent rules added here.
      // Without this rule, any reserved keyword can be the last token in an
      // expression and can thus be followed by many potential sequences of
      // reduces. Even though that analysis can only be reliably done by a tool,
      // reducing the number of paths the tool needs to explore reduces the tool
      // runtime from hours to minutes.
      //
      // The negative side of this rule, like similar rules embedded in the
      // lexer's regexps, is that it effectively means `@` or `@@` operator
      // followed by a keyword will interpret the keyword as an identifier in
      // *all contexts*. That prevents using `@` or `@@` as other operators.
      //
      // It looks like we intended to support `SELECT @param_name`, but
      // accidentally supported `SELECT @  param_name` as well. If we deprecate
      // and remove the ability to put whitespace in a parameter reference, then
      // we should probably remove this rule and instead change the lexer to
      // directly produce tokens for parameter and system variable names
      // directly.
      // TODO: Remove this rule and add lex parameter and system
      //     variable references as their own token kinds.
      if (IsReservedKeywordToken(token) || IsNonreservedKeywordToken(token)) {
        // The dot-identifier mini-parser is triggered based on the value
        // of prev_flex_token_ being identifier or non-reserved keyword. We
        // don't want to update prev_flex_token_ from this code since it might
        // actually reflect the lookahead token and not the current token (for
        // the case where the lookahead is populated). The long term healthy
        // solution for this is to pull the lexer modes forward into this layer,
        // but for now we engage directly with them.
        if (IsReservedKeywordToken(token) && Lookahead1(location) == '.') {
          yy_push_state(/*DOT_IDENTIFIER*/ 1);
        }
        return Token::IDENTIFIER;
      }
      break;
    default:
      break;
  }

  switch (token) {
    case Token::KW_NOT:
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
    case Token::KW_WITH:
      // The WITH expression uses a function-call like syntax and is followed by
      // the open parenthesis.
      if (Lookahead1(location) == '(') {
        return Token::KW_WITH_STARTING_WITH_EXPRESSION;
      }
      break;
    case Token::KW_EXCEPT:
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
          SetOverrideError(
              location, "EXCEPT must be followed by ALL, DISTINCT, or \"(\"");
          break;
      }
      break;
    default:
      break;
  }

  return token;
}

int ZetaSqlFlexTokenizer::Lookahead1(const Location& current_token_location) {
  if (!lookahead_1_.has_value()) {
    lookahead_1_ = {.token = 0, .token_location = current_token_location};
    prev_flex_token_ = GetNextTokenFlexImpl(&lookahead_1_->token_location);
    lookahead_1_->token = prev_flex_token_;
  }
  return lookahead_1_->token;
}

// Returns the next token id, returning its location in 'yylloc'. On input,
// 'yylloc' must be the location of the previous token that was returned.
int ZetaSqlFlexTokenizer::GetNextTokenFlex(Location* yylloc) {
  TokenKind token = 0;
  if (lookahead_1_.has_value()) {
    // Get the next token from the lookahead buffer and advance the buffer. If
    // force_terminate_ was set, we still need the location from the buffer,
    // with Token::YYEOF as the token.
    token = force_terminate_ ? Token::YYEOF : lookahead_1_->token;
    *yylloc = lookahead_1_->token_location;
    lookahead_1_.reset();
  } else {
    // The lookahead buffer is empty, so get a token from the underlying lexer.
    prev_flex_token_ = GetNextTokenFlexImpl(yylloc);
    token = prev_flex_token_;
  }
  token = ApplyTokenDisambiguation(token, *yylloc);
  if (override_error_.ok()) {
    num_lexical_tokens_++;
  }
  prev_dispensed_token_ = token;
  return token;
}

absl::Status ZetaSqlFlexTokenizer::GetNextToken(ParseLocationRange* location,
                                                  TokenKind* token) {
  Location bison_location;
  bison_location.begin.column = location->start().GetByteOffset();
  bison_location.end.column = location->end().GetByteOffset();
  *token = GetNextTokenFlex(&bison_location);
  location->set_start(
      ParseLocationPoint::FromByteOffset(filename_,
                                         bison_location.begin.column));
  location->set_end(
      ParseLocationPoint::FromByteOffset(filename_,
                                         bison_location.end.column));
  return override_error_;
}

void ZetaSqlFlexTokenizer::SetForceTerminate() {
  force_terminate_ = true;
  // Ensure that the lookahead buffer immediately reflects the termination.
  if (lookahead_1_.has_value()) {
    lookahead_1_->token = Token::YYEOF;
  }
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

ZetaSqlFlexTokenizer::ZetaSqlFlexTokenizer(
    BisonParserMode mode, absl::string_view filename, absl::string_view input,
    int start_offset, const LanguageOptions& language_options)
    : filename_(filename),
      start_offset_(start_offset),
      input_size_(static_cast<int>(input.size())),
      mode_(mode),
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
  override_error_ = MakeSqlErrorAtPoint(ParseLocationPoint::FromByteOffset(
                        filename_, yylloc.begin.column))
                    << error_message;
}

void ZetaSqlFlexTokenizer::LexerError(const char* msg) {
  override_error_ = MakeSqlError() << msg;
}

bool ZetaSqlFlexTokenizer::AreMacrosEnabled() const {
  return language_options_.LanguageFeatureEnabled(FEATURE_V_1_4_SQL_MACROS);
}

void ZetaSqlFlexTokenizer::PushBisonParserMode(BisonParserMode mode) {
  restore_modes_.push(mode_);
  mode_ = mode;
}

void ZetaSqlFlexTokenizer::PopBisonParserMode() {
  ABSL_DCHECK(!restore_modes_.empty());
  mode_ = restore_modes_.top();
  restore_modes_.pop();
}

}  // namespace parser
}  // namespace zetasql
