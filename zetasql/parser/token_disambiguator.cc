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
#include <optional>
#include <utility>

#include "zetasql/base/arena.h"
#include "zetasql/common/errors.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/bison_token_codes.h"
#include "zetasql/parser/macros/flex_token_provider.h"
#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/macros/macro_expander.h"
#include "zetasql/parser/macros/token_with_location.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "absl/container/flat_hash_map.h"
#include "zetasql/base/check.h"
#include "absl/log/log.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
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
using TokenKind = int;

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
absl::Status OverrideNextTokenLookback(DisambiguatorLexer* disambiguator,
                                       bool parser_lookahead_is_empty,
                                       TokenKind expected_next_token,
                                       TokenKind lookback_token) {
  return disambiguator->OverrideNextTokenLookback(
      parser_lookahead_is_empty, expected_next_token, lookback_token);
}

absl::Status OverrideCurrentTokenLookback(DisambiguatorLexer* disambiguator,
                                          TokenKind new_token_kind) {
  return disambiguator->OverrideCurrentTokenLookback(new_token_kind);
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
using DiagnosticOptions = macros::DiagnosticOptions;
using FlexTokenProvider = macros::FlexTokenProvider;
using MacroCatalog = macros::MacroCatalog;
using MacroExpander = macros::MacroExpander;
using MacroExpanderBase = macros::MacroExpanderBase;
using TokenWithLocation = macros::TokenWithLocation;

static constexpr char kInTemplatedType = '<';

static bool IsLookbackToken(TokenKind token) {
  return token > Token::SENTINEL_LB_TOKEN_START &&
         token < Token::SENTINEL_LB_TOKEN_END;
}

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

static bool IsKeywordToken(TokenKind token) {
  return IsReservedKeywordToken(token) || IsNonreservedKeywordToken(token);
}

static bool IsIdentifierOrKeyword(TokenKind token) {
  switch (token) {
    case Token::IDENTIFIER:
    case Token::EXP_IN_FLOAT_NO_SIGN:
    case Token::STANDALONE_EXPONENT_SIGN:
      return true;
    default:
      return IsKeywordToken(token);
  }
}

static bool IsIdentifierOrNonreservedKeyword(TokenKind token) {
  switch (token) {
    case Token::IDENTIFIER:
    case Token::EXP_IN_FLOAT_NO_SIGN:
    case Token::STANDALONE_EXPONENT_SIGN:
      return true;
    default:
      return IsNonreservedKeywordToken(token);
  }
}

// Returns whether `token` is a keyword or an unquoted identifier.
static bool IsKeywordOrUnquotedIdentifier(const TokenWithLocation& token) {
  switch (token.kind) {
    case Token::EXP_IN_FLOAT_NO_SIGN:
    case Token::STANDALONE_EXPONENT_SIGN:
      return true;
    case Token::IDENTIFIER:
      ABSL_DCHECK(!token.text.empty());
      return token.text.front() != '`';
    default:
      return IsKeywordToken(token.kind);
  }
}

static absl::Status MakeError(absl::string_view error_message,
                              const Location& yylloc) {
  return MakeSqlErrorAtPoint(yylloc.start()) << error_message;
}

static bool IsValidPreviousTokenBeforeScriptLabel(
    const TokenKind previous_token) {
  switch (previous_token) {
    case ';':
    case Token::LB_END_OF_STATEMENT_LEVEL_HINT:
    case Token::LB_OPEN_STATEMENT_BLOCK:
    case Token::LB_BEGIN_AT_STATEMENT_START:
    case Token::KW_ELSE:
    case Token::KW_THEN:
    case Token::MODE_NEXT_SCRIPT_STATEMENT:
    case Token::MODE_NEXT_STATEMENT_KIND:
    case Token::MODE_SCRIPT:
      return true;
    default:
      return false;
  }
}

static bool IsValidPreviousTokenToSqlStatement(TokenKind token) {
  switch (token) {
    case ';':
    case Token::LB_EXPLAIN_SQL_STATEMENT:
    case Token::LB_END_OF_STATEMENT_LEVEL_HINT:
    case Token::LB_OPEN_STATEMENT_BLOCK:
    case Token::LB_BEGIN_AT_STATEMENT_START:
    case Token::KW_ELSE:
    case Token::KW_THEN:
    case Token::MODE_NEXT_SCRIPT_STATEMENT:
    case Token::MODE_NEXT_STATEMENT_KIND:
    case Token::MODE_NEXT_STATEMENT:
    case Token::MODE_SCRIPT:
    case Token::MODE_STATEMENT:
      return true;
    default:
      return false;
  }
}

// Returns whether the token in `token_with_location` is a SCRIPT_LABEL token.
// It is a script label token if:
// - `IsValidPreviousTokenBeforeScriptLabel(previous_token)` returns true.
// - The token itself is a keyword or identifier.
// - The token is followed by a colon, and the followed by one of the tokens in
//   [BEGIN, WHILE, LOOP, REPEAT, FOR].
//
// `previous_token`: the token the disambiguator sees before
// `token_with_location`.
static bool IsScriptLabel(TokenKind lookback,
                          const TokenWithLocation& token_with_location,
                          TokenKind lookahead_1, TokenKind lookahead_2) {
  if (!IsValidPreviousTokenBeforeScriptLabel(lookback)) {
    return false;
  }
  const TokenKind token = token_with_location.kind;
  if (!IsIdentifierOrKeyword(token)) {
    return false;
  }
  if (lookahead_1 != ':') {
    return false;
  }
  switch (lookahead_2) {
    case Token::KW_BEGIN:
    case Token::KW_WHILE:
    case Token::KW_LOOP:
    case Token::KW_REPEAT:
    case Token::KW_FOR:
      return true;
    default:
      return false;
  }
}

void DisambiguatorLexer::ApplyConditionallyReservedKeywords(TokenKind& kind) {
  switch (kind) {
    case Token::KW_QUALIFY_NONRESERVED:
      if (language_options_.IsReservedKeyword("QUALIFY")) {
        kind = Token::KW_QUALIFY_RESERVED;
      }
      break;
    default:
      break;
  }
}

void DisambiguatorLexer::FetchNextToken(
    const std::optional<TokenWithOverrideError>& current,
    std::optional<TokenWithOverrideError>& next) {
  if (current.has_value() && current->token.kind == Token::YYEOF) {
    // If the current token is already YYEOF, do not continue the fetch.
    // Instead, return the same token directly so that future calls to
    // GetNextToken() and GetOverrideError() return the same token kind and
    // error.
    //
    // This is ok because we do not allow token transformation from YYEOF to
    // non-YYEOF, so `current` will always remain YYEOF.
    next = *current;
    return;
  }
  next.emplace();
  absl::StatusOr<TokenWithLocation> next_token =
      macro_expander_->GetNextToken();
  if (next_token.ok()) {
    next->token = *next_token;
    ApplyConditionallyReservedKeywords(next->token.kind);
    next->error = absl::OkStatus();
  } else {
    next->token.kind = Token::YYEOF;
    // TODO: Correctly update the `slot` token location once the
    // macro expander is updated to return TokenWithOverrideError.
    next->token.location = Location();
    next->error = std::move(next_token.status());
  }
}

// Returns whether `token1` and `token2` are adjacent and `token` precedes
// `token2`.
static bool IsAdjacentPrecedingToken(
    const std::optional<TokenWithOverrideError>& token1,
    const std::optional<TokenWithOverrideError>& token2) {
  if (!token1.has_value() || !token2.has_value()) {
    return false;
  }
  // YYEOF could mean tokens have errors, in which case we do not have the
  // correct location information, so we return false to disallow token fusions.
  if (token1->token.kind == Token::YYEOF ||
      token2->token.kind == Token::YYEOF) {
    return false;
  }
  return token1->token.AdjacentlyPrecedes(token2->token);
}

// Merges the token texts of `token1` and `token2` into a single text. `token1`
// and `token2` must be adjacent.
static absl::string_view GetFusedText(const TokenWithLocation& token1,
                                      const TokenWithLocation& token2) {
  absl::string_view::size_type total_size =
      token1.text.size() + token2.text.size();
  return absl::string_view(token1.text.data(), total_size);
}

// Fuses `token1` and `token2` into a new token with token kind being
// `target_token_kind`. `token1` must precede `token2` and they must be
// adjacent.
static TokenWithLocation FuseTokensIntoTokenKind(
    TokenKind target_token_kind, const TokenWithLocation& token1,
    const TokenWithLocation& token2) {
  ABSL_DCHECK(token1.AdjacentlyPrecedes(token2));
  return {
      .kind = target_token_kind,
      .location = Location(token1.location.start(), token2.location.end()),
      .text = GetFusedText(token1, token2),
      .preceding_whitespaces = token1.preceding_whitespaces,
  };
}

void DisambiguatorLexer::FuseLookahead1IntoCurrent(TokenKind fused_token_kind) {
  ABSL_DCHECK(current_token_.has_value());
  ABSL_DCHECK(IsAdjacentPrecedingToken(current_token_, lookahead_1_));
  current_token_->token = FuseTokensIntoTokenKind(
      fused_token_kind, current_token_->token, lookahead_1_->token);
  lookahead_1_.swap(lookahead_2_);
  lookahead_2_.swap(lookahead_3_);
  FetchNextToken(lookahead_2_, lookahead_3_);
}

// Detects whether a token that could be a literal (in `lookback_token`) is
// followed by an adjacent unquoted IDENTIFIER or non-reserved keyword token (in
// `current_token`). This is sometimes used to generate error messages in cases
// like `SELECT 123abc` where it appears the user missed a whitespace between a
// column value and its alias.
//
// `lookback1` is the token kind for `lookback_token` when it is used as
// lookbacks, which can be different from `lookback_token.kind`. See
// the comment of Lookback1() for more information.
static bool IsLiteralBeforeAdjacentUnquotedIdentifier(
    TokenKind lookback1,
    const std::optional<TokenWithOverrideError>& lookback_token,
    const std::optional<TokenWithOverrideError>& current_token) {
  if (!IsKeywordOrUnquotedIdentifier(current_token->token)) {
    return false;
  }
  if (lookback1 != Token::FLOATING_POINT_LITERAL &&
      lookback1 != Token::INTEGER_LITERAL) {
    return false;
  }
  if (!IsAdjacentPrecedingToken(lookback_token, current_token)) {
    return false;
  }
  // Inputs like "123.abc" are allowed by the lexer and are tokenized into two
  // tokens: FLOATING_POINT_LITERAL ("123.") and IDENTIFIER ("abc"). We preserve
  // the behavior here.
  if (lookback_token.has_value() && lookback_token->token.text.back() == '.') {
    return false;
  }
  return true;
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
    const TokenWithLocation& token_with_location) {
  const TokenKind token = token_with_location.kind;
  const Location& location = token_with_location.location;

  switch (mode_) {
    case BisonParserMode::kTokenizer:
    case BisonParserMode::kTokenizerPreserveComments:
      // Tokenizer modes are used to extract tokens for error messages among
      // other things. The rules below are mostly intended to support the bison
      // parser, and aren't necessary in tokenizer mode.
      // For keywords that have context-dependent variations, return the
      // "standard" one.
      switch (Lookback1()) {
        case Token::LB_DOT_IN_PATH_EXPRESSION:
        case '@':
        case Token::KW_DOUBLE_AT:
          if (IsKeywordToken(token)) {
            // This keyword is used as an identifier.
            return Token::IDENTIFIER;
          }
          break;
      }
      switch (token) {
        case Token::KW_DEFINE_FOR_MACROS:
          return Token::KW_DEFINE;
        case Token::KW_OPEN_HINT:
        case Token::KW_OPEN_INTEGER_HINT:
          return '@';
        // The following token fusions need to be performed even in the
        // `kTokenizer` and `kTokenizerPreserveComments` to make sure the
        // formatter and other clients of GetParseTokens do not have to
        // reimplement fusions of floating point literals.
        case Token::DECIMAL_INTEGER_LITERAL:
        case Token::HEX_INTEGER_LITERAL:
          TransformIntegerLiteral();
          return EmitInvalidLiteralPrecedesIdentifierTokenIfApplicable();
        case Token::FLOATING_POINT_LITERAL:
          return EmitInvalidLiteralPrecedesIdentifierTokenIfApplicable();
        case '.':
          TransformDotSymbol();
          return EmitInvalidLiteralPrecedesIdentifierTokenIfApplicable();
        case Token::EXP_IN_FLOAT_NO_SIGN:
        case Token::STANDALONE_EXPONENT_SIGN:
          return Token::IDENTIFIER;
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

  // The rules in this block are changing state based on lookback overrides.
  // These should happen before any token transformations when we are running
  // in a mode that is driven by the parser.
  switch (Lookback1()) {
    case Token::LB_OPEN_TYPE_TEMPLATE:
      PushState(kInTemplatedType);
      break;
    case Token::LB_CLOSE_TYPE_TEMPLATE:
      bool popped = PopStateIfMatch(kInTemplatedType);
      ABSL_DCHECK(popped);
      break;
  }

  // WARNING: This transformation must come before other transformations for
  // keywords and identifiers because it force-emits the SCRIPT_LABEL token,
  // even if a keyword is present.
  if (IsScriptLabel(Lookback1(), token_with_location, Lookahead1(),
                    Lookahead2())) {
    if (IsReservedKeywordToken(token)) {
      return SetOverrideErrorAndReturnEof(
          absl::StrCat("Reserved keyword '", token_with_location.text,
                       "' may not be used as a label name without backticks"),
          location);
    }
    return Token::SCRIPT_LABEL;
  }

  if (IsLiteralBeforeAdjacentUnquotedIdentifier(Lookback1(), lookback_1_,
                                                current_token_)) {
    // TODO: b/334114221: We should report this error only when inside a
    // select list.
    return SetOverrideErrorAndReturnEof(
        "Syntax error: Missing whitespace between literal and alias", location);
  }

  switch (Lookback1()) {
    case Token::LB_DOT_IN_PATH_EXPRESSION:
    case '@':
    case Token::KW_DOUBLE_AT:
      if (IsKeywordToken(token)) {
        // This keyword is used as an identifier.
        return Token::IDENTIFIER;
      }
      break;
  }

  switch (token) {
    case Token::KW_OPTIONS:
      if (Lookback2() == Token::LB_WITH_IN_SELECT_WITH_OPTIONS &&
          Lookahead1() == '(') {
        return Token::KW_OPTIONS_IN_SELECT_WITH_OPTIONS;
      }
      break;
    case Token::KW_UPDATE:
    case Token::KW_REPLACE:
      if (Lookback1() == Token::KW_INSERT) {
        // The INSERT token is interesting when it starts the INSERT statement
        // whether a top level statement or nested DML. The first few tokens
        // include several unreserved keywords and identifiers
        bool insert_starts_statement =
            IsValidPreviousTokenToSqlStatement(Lookback2()) ||
            Lookback2() == Token::LB_OPEN_NESTED_DML;
        // Ideally we would like to treat UPDATE and REPLACE as if they were
        // reserved keyword here so they work like KW_IGNORE. However, the hard
        // coded mini-parser initially implemented for the Bison parser consumed
        // an entire `generalized_path_expression` and checked whether its text
        // image was 'UPDATE' or 'REPLACE'. That meant `update.whatever` was
        // the path expression, and its image was not simply 'UPDATE', so it
        // was not recognized as the insert mode. This check preserves that
        // behavior.
        bool token_starts_path = Lookahead1() == '.' || Lookahead1() == '[';
        if (insert_starts_statement && !token_starts_path) {
          return token == Token::KW_UPDATE ? Token::KW_UPDATE_AFTER_INSERT
                                           : Token::KW_REPLACE_AFTER_INSERT;
        }
      }
      break;
    case Token::KW_EXPLAIN:
      if (IsValidPreviousTokenToSqlStatement(Lookback1())) {
        current_token_->lookback_override = Token::LB_EXPLAIN_SQL_STATEMENT;
      }
      break;
    case Token::KW_NOT: {
      // This returns a different token because returning KW_NOT would confuse
      // the operator precedence parsing. Boolean NOT has a different
      // precedence than NOT BETWEEN/IN/LIKE/DISTINCT.
      switch (Lookahead1()) {
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
      if (Lookahead1() == KW_GROUP) {
        return Token::KW_WITH_STARTING_WITH_GROUP_ROWS;
      }
      // The WITH expression uses a function-call like syntax and is followed by
      // the open parenthesis and at least one variable definition consisting
      // of an identifier followed by KW_AS.
      if (Lookahead1() == '(' &&
          (IsIdentifierOrNonreservedKeyword(Lookahead2())) &&
          Lookahead3() == KW_AS) {
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
      switch (Lookahead1()) {
        case '(':
          // This is the SELECT * EXCEPT (column...) case.
          return Token::KW_EXCEPT;
        case Token::KW_ALL:
        case Token::KW_DISTINCT:
          // This is the {query} EXCEPT (ALL|DISTINCT) {query} case.
          return Token::KW_EXCEPT_IN_SET_OP;
        case '@':
          switch (Lookahead2()) {
            case Token::DECIMAL_INTEGER_LITERAL:
            case Token::HEX_INTEGER_LITERAL:
            case '{':
              // This is the {query} EXCEPT opt_hint (ALL|DISTINCT) {query}
              // case.
              return Token::KW_EXCEPT_IN_SET_OP;
          }
      }
      return SetOverrideErrorAndReturnEof(
          "EXCEPT must be followed by ALL, DISTINCT, or \"(\"", location);
    }
    case Token::KW_FULL:
    case Token::KW_LEFT: {
      // If FULL or LEFT is used in set operations, return KW_FULL_IN_SET_OP
      // or KW_LEFT_IN_SET_OP instead.
      TokenKind lookahead =
          Lookahead1() == Token::KW_OUTER ? Lookahead2() : Lookahead1();
      switch (lookahead) {
        case Token::KW_UNION:
        case Token::KW_INTERSECT:
        case Token::KW_EXCEPT:
          return token == KW_FULL ? Token::KW_FULL_IN_SET_OP
                                  : Token::KW_LEFT_IN_SET_OP;
      }
      break;
    }
    case Token::KW_SEQUENCE: {
      // Force the KW_SEQUENCEs to IDENTIFIERs if they are followed by
      // KW_CLAMPED to allow the resolution for statements like
      // SELECT some_func(sequence CLAMPED BETWEEN 1 AND 2).
      //
      // Without this transformation, bison believes CLAMPED is a sequence arg
      // and reports "Syntax error: Expected ")" but got keyword BETWEEN".
      //
      // See the comment section "AMBIGUOUS CASE 13: SEQUENCE CLAMPED" in
      // bison_parser.y for more information.
      if (Lookahead1() == Token::KW_CLAMPED) {
        return Token::IDENTIFIER;
      }
      break;
    }
    case '@': {
      switch (Lookahead1()) {
        case Token::DECIMAL_INTEGER_LITERAL:
        case Token::HEX_INTEGER_LITERAL:
          if (Lookahead2() == '@' && Lookahead3() == '{') {
            // This is a hint with both the integer and the key-value list.
            // Like: @5 @{a=b}. We give a special prefix token here so that the
            // parser can handle this case without lookahead. Avoiding lookahead
            // here lets us, in turn, better identify where the statement starts
            // after a statement level hint.
            return Token::OPEN_INTEGER_PREFIX_HINT;
          }
          return Token::KW_OPEN_INTEGER_HINT;
        case '{':
          return Token::KW_OPEN_HINT;
      }
      break;
    }
    case '<': {
      // Adjacent "<" and ">" become "<>".
      if (Lookback1() != Token::KW_STRUCT && Lookahead1() == '>' &&
          IsAdjacentPrecedingToken(current_token_, lookahead_1_)) {
        FuseLookahead1IntoCurrent(Token::KW_NOT_EQUALS_SQL_STYLE);
      }
      return current_token_->token.kind;
    }
    case '>': {
      // Adjacent ">" and ">" become ">>".
      if (!IsInTemplatedTypeState() && Lookahead1() == '>' &&
          IsAdjacentPrecedingToken(current_token_, lookahead_1_)) {
        FuseLookahead1IntoCurrent(Token::KW_SHIFT_RIGHT);
      }
      return current_token_->token.kind;
    }
    case '(': {
      PushState('(');
      break;
    }
    case ')': {
      if (!PopStateIfMatch('(')) {
        // This is an unmatched ')'. We push it onto `state_stack_` to end
        // the kInTemplatedType state, if it exists, to preserve the Flex
        // behavior.
        // TODO: b/333926361 - Report an error directly.
        PushState(')');
      }
      break;
    }
    case Token::DECIMAL_INTEGER_LITERAL:
    case Token::HEX_INTEGER_LITERAL: {
      TransformIntegerLiteral();
      return current_token_->token.kind;
    }
    case '.': {
      return TransformDotSymbol();
    }
    case Token::EXP_IN_FLOAT_NO_SIGN:
    case Token::STANDALONE_EXPONENT_SIGN: {
      return Token::IDENTIFIER;
    }
    // TODO: b/333926361 - If the token is YYEOF without errors, check whether
    // `state_stack_` has '(' and if yes, report an error.
    default: {
      break;
    }
  }

  return token;
}

absl::Status DisambiguatorLexer::OverrideNextTokenLookback(
    bool parser_lookahead_is_empty, TokenKind expected_next_token,
    TokenKind lookback_token) {
  ZETASQL_RET_CHECK(current_token_.has_value()) << "current_token_ not populated.";
  TokenWithOverrideError& next_token =
      parser_lookahead_is_empty ? *lookahead_1_ : *current_token_;
  if (next_token.token.kind != expected_next_token) {
    return absl::OkStatus();
  }
  next_token.lookback_override = lookback_token;
  return absl::OkStatus();
}

bool DisambiguatorLexer::LookbackTokenCanBeBeforeDotInPathExpression(
    TokenKind token_kind) const {
  ABSL_DCHECK(token_kind != Token::EXP_IN_FLOAT_NO_SIGN);
  ABSL_DCHECK(token_kind != Token::STANDALONE_EXPONENT_SIGN);
  switch (token_kind) {
    case Token::IDENTIFIER:
    case ')':
    case ']':
    case '?':
      return true;
    default:
      break;
  }
  return IsNonreservedKeywordToken(token_kind);
}

static bool IsPlusOrMinus(TokenKind token_kind) {
  return token_kind == '+' || token_kind == '-';
}

bool DisambiguatorLexer::FuseExponentPartIntoFloatingPointLiteral() {
  if (!IsAdjacentPrecedingToken(current_token_, lookahead_1_)) {
    return false;
  }
  switch (Lookahead1()) {
    case Token::STANDALONE_EXPONENT_SIGN: {
      // The first token is 'E', check whether it has a sign ('+' or '-') and an
      // integer following it to form floats like "E+10".
      if (!IsPlusOrMinus(Lookahead2()) ||
          !IsAdjacentPrecedingToken(lookahead_1_, lookahead_2_)) {
        return false;
      }
      if (Lookahead3() != Token::DECIMAL_INTEGER_LITERAL ||
          !IsAdjacentPrecedingToken(lookahead_2_, lookahead_3_)) {
        return false;
      }
      // Now we have adjacent tokens that can form the exponential part of a
      // floating point literal, for example "E+10". Fuse the three tokens
      // together.
      FuseLookahead1IntoCurrent(Token::FLOATING_POINT_LITERAL);
      FuseLookahead1IntoCurrent(Token::FLOATING_POINT_LITERAL);
      FuseLookahead1IntoCurrent(Token::FLOATING_POINT_LITERAL);
      return true;
    }
    case Token::EXP_IN_FLOAT_NO_SIGN: {
      // For example "E10".
      FuseLookahead1IntoCurrent(Token::FLOATING_POINT_LITERAL);
      return true;
    }
    default: {
      return false;
    }
  }
}

TokenKind DisambiguatorLexer::TransformDotSymbol() {
  if (LookbackTokenCanBeBeforeDotInPathExpression(Lookback1())) {
    // This dot is part of a path expression, return '.' directly.
    current_token_->lookback_override = Token::LB_DOT_IN_PATH_EXPRESSION;
    return '.';
  }
  if (Lookahead1() == Token::DECIMAL_INTEGER_LITERAL &&
      IsAdjacentPrecedingToken(current_token_, lookahead_1_)) {
    // This dot is the start of a floating point literal, e.g. ".1". Fuse it
    // with the integer literal and potentially an exponent part after it, if it
    // exists.
    FuseLookahead1IntoCurrent(Token::FLOATING_POINT_LITERAL);
    FuseExponentPartIntoFloatingPointLiteral();
    return Token::FLOATING_POINT_LITERAL;
  }
  return '.';
}

void DisambiguatorLexer::TransformIntegerLiteral() {
  TokenKind initial_kind = current_token_->token.kind;
  ABSL_DCHECK(initial_kind == Token::DECIMAL_INTEGER_LITERAL ||
         initial_kind == Token::HEX_INTEGER_LITERAL);

  if (Lookback1() == Token::LB_DOT_IN_PATH_EXPRESSION) {
    // Integer literals, for example "123" or "0x01", and identifiers that start
    // with digits, for example "123abc" are allowed in path expressions.
    if (IsKeywordOrUnquotedIdentifier(lookahead_1_->token) &&
        IsAdjacentPrecedingToken(current_token_, lookahead_1_)) {
      FuseLookahead1IntoCurrent(Token::IDENTIFIER);
    } else {
      current_token_->token.kind = Token::IDENTIFIER;
    }
    return;
  }
  // Converts Token::DECIMAL_INTEGER_LITERAL and Token::HEX_INTEGER_LITERAL to
  // be Token::INTEGER_LITERAL.
  current_token_->token.kind = Token::INTEGER_LITERAL;
  // Decimal integers can be the start of a floating point literal, hex integers
  // cannot.
  if (initial_kind != Token::DECIMAL_INTEGER_LITERAL) {
    return;
  }
  switch (Lookahead1()) {
    case '.':
      if (!IsAdjacentPrecedingToken(current_token_, lookahead_1_)) {
        return;
      }
      // This is a floating point literal, for example "1.". Check whether it
      // has (1) digits after the floating point, and (2) an exponent part.
      FuseLookahead1IntoCurrent(Token::FLOATING_POINT_LITERAL);
      if (Lookahead1() == Token::DECIMAL_INTEGER_LITERAL &&
          IsAdjacentPrecedingToken(current_token_, lookahead_1_)) {
        // The floating point literal has digits after the dot, e.g. "1.1".
        FuseLookahead1IntoCurrent(Token::FLOATING_POINT_LITERAL);
      }
      // Check whether it has an exponent part as well.
      FuseExponentPartIntoFloatingPointLiteral();
      return;
    case Token::EXP_IN_FLOAT_NO_SIGN:
    case Token::STANDALONE_EXPONENT_SIGN:
      // This can be a floating point literal without dot, e.g. "2E10".
      FuseExponentPartIntoFloatingPointLiteral();
      break;
    default:
      break;
  }
}

TokenKind DisambiguatorLexer::SetOverrideErrorAndReturnEof(
    absl::string_view error_message, const Location& error_location) {
  if (!current_token_.has_value()) {
    current_token_.emplace();
  }
  current_token_->token.kind = Token::YYEOF;
  current_token_->error = MakeError(error_message, error_location);
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

TokenKind DisambiguatorLexer::Lookahead1() const {
  return lookahead_1_->token.kind;
}

TokenKind DisambiguatorLexer::Lookahead2() const {
  return lookahead_2_->token.kind;
}

TokenKind DisambiguatorLexer::Lookahead3() const {
  return lookahead_3_->token.kind;
}

void DisambiguatorLexer::PopulateLookaheads() {
  if (!lookahead_1_.has_value()) {
    FetchNextToken(current_token_, lookahead_1_);
  }
  if (!lookahead_2_.has_value()) {
    FetchNextToken(lookahead_1_, lookahead_2_);
  }
  if (!lookahead_3_.has_value()) {
    FetchNextToken(lookahead_2_, lookahead_3_);
  }
}

TokenKind DisambiguatorLexer::Lookback1() const {
  if (lookback_1_.has_value()) {
    if (lookback_1_->lookback_override != kNoToken) {
      return lookback_1_->lookback_override;
    } else {
      return lookback_1_->token.kind;
    }
  }
  return kNoToken;
}

TokenKind DisambiguatorLexer::Lookback2() const {
  if (lookback_2_.has_value()) {
    if (lookback_2_->lookback_override != kNoToken) {
      return lookback_2_->lookback_override;
    } else {
      return lookback_2_->token.kind;
    }
  }
  return kNoToken;
}

TokenKind DisambiguatorLexer::GetNextToken(absl::string_view* text,
                                           Location* yylloc) {
  // Advance the token buffers.
  lookback_2_.swap(lookback_1_);
  lookback_1_.swap(current_token_);
  current_token_.swap(lookahead_1_);
  lookahead_1_.swap(lookahead_2_);
  lookahead_2_.swap(lookahead_3_);
  FetchNextToken(lookahead_2_, lookahead_3_);

  current_token_->token.kind = ApplyTokenDisambiguation(current_token_->token);
  // If the current token is Token::YYEOF after disambiguation, set all the
  // lookaheads to be the same as `current_token_` so that future calls to
  // GetNextToken() and GetOverrideError() return the same token kind and error.
  if (current_token_->token.kind == YYEOF) {
    ResetToEof(*current_token_, lookahead_1_);
    ResetToEof(*current_token_, lookahead_2_);
    ResetToEof(*current_token_, lookahead_3_);
  }

  *text = current_token_->token.text;

  // Location offsets must be valid for the source they refer to.
  // Currently, the parser & analyzer only have the unexpanded source, so we
  // use the unexpanded offset.
  // In the future, the resolver should show the expanded location and where
  // it was expanded from. The expander would have the full location map and
  // the sources of macro definitions as well, so we would not need this
  // adjustment nor the `topmost_invocation_location` at all, since the
  // expander will be able to provide the stack. All layers, however, will
  // need to ask for that mapping.
  *yylloc = current_token_->token.topmost_invocation_location.IsValid()
                ? current_token_->token.topmost_invocation_location
                : current_token_->token.location;

  // LB tokens should only be used in the lookback_override variable. They
  // should never be returned as the current token.
  ABSL_DCHECK(!IsLookbackToken(current_token_->token.kind));
  return current_token_->token.kind;
}

static const MacroCatalog* empty_macro_catalog() {
  static MacroCatalog* empty_macro_catalog = new MacroCatalog();
  return empty_macro_catalog;
}

using StartTokenMap = absl::flat_hash_map<BisonParserMode, TokenKind>;
static const StartTokenMap& GetStartTokenMap() {
  static const StartTokenMap* kStartTokenMap([] {
    using Mode = BisonParserMode;
    StartTokenMap* ret = new StartTokenMap();
    ret->emplace(Mode::kStatement, Token::MODE_STATEMENT);
    ret->emplace(Mode::kScript, Token::MODE_SCRIPT);
    ret->emplace(Mode::kNextStatement, Token::MODE_NEXT_STATEMENT);
    ret->emplace(Mode::kNextScriptStatement, Token::MODE_NEXT_SCRIPT_STATEMENT);
    ret->emplace(Mode::kNextStatementKind, Token::MODE_NEXT_STATEMENT_KIND);
    ret->emplace(Mode::kExpression, Token::MODE_EXPRESSION);
    ret->emplace(Mode::kType, Token::MODE_TYPE);
    return ret;
  }());
  return *kStartTokenMap;
}

static std::optional<TokenWithLocation> MakeStartModeToken(
    BisonParserMode mode, absl::string_view filename, int start_offset) {
  switch (mode) {
    case BisonParserMode::kTokenizer:
    case BisonParserMode::kTokenizerPreserveComments:
    case BisonParserMode::kMacroBody:
      // These modes do no have a start token.
      return std::nullopt;
    default:
      return TokenWithLocation{
          .kind = GetStartTokenMap().at(mode),
          .location = {
              ParseLocationPoint::FromByteOffset(filename, start_offset),
              ParseLocationPoint::FromByteOffset(filename, start_offset)}};
  }
}

absl::StatusOr<std::unique_ptr<DisambiguatorLexer>> DisambiguatorLexer::Create(
    BisonParserMode mode, absl::string_view filename, absl::string_view input,
    int start_offset, const LanguageOptions& language_options,
    const macros::MacroCatalog* macro_catalog, zetasql_base::UnsafeArena* arena) {
  // TODO: take the token_provider as an injected dependency.
  auto token_provider = std::make_unique<FlexTokenProvider>(
      filename, input,
      /*preserve_comments=*/mode == BisonParserMode::kTokenizerPreserveComments,
      start_offset, /*end_offset=*/std::nullopt);

  std::unique_ptr<MacroExpanderBase> macro_expander;
  if (language_options.LanguageFeatureEnabled(FEATURE_V_1_4_SQL_MACROS)) {
    if (macro_catalog == nullptr) {
      macro_catalog = empty_macro_catalog();
    }
    macro_expander = std::make_unique<MacroExpander>(
        std::move(token_provider), language_options, *macro_catalog, arena,
        // TODO: pass the real ErrorMessageOptions.
        DiagnosticOptions{}, /*parent_location=*/nullptr);
  } else {
    ZETASQL_RET_CHECK(macro_catalog == nullptr);
    macro_expander = std::make_unique<NoOpExpander>(std::move(token_provider));
  }
  return absl::WrapUnique(new DisambiguatorLexer(
      mode, MakeStartModeToken(mode, filename, start_offset), language_options,
      std::move(macro_expander)));
}

DisambiguatorLexer::DisambiguatorLexer(
    BisonParserMode mode, std::optional<TokenWithLocation> start_token,
    const LanguageOptions& language_options,
    std::unique_ptr<MacroExpanderBase> expander)
    : mode_(mode),
      language_options_(language_options),
      macro_expander_(std::move(expander)) {
  if (start_token.has_value()) {
    num_inserted_tokens_++;
    lookahead_1_.emplace(TokenWithOverrideError{.token = *start_token});
  }
  // Actively fetch lookaheads.
  PopulateLookaheads();
}

int64_t DisambiguatorLexer::num_lexical_tokens() const {
  return num_inserted_tokens_ +
         macro_expander_->num_unexpanded_tokens_consumed();
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
  if (end_byte_offset != nullptr) {
    if (!current_token_.has_value()) {
      // If no tokens have been returned, set `end_byte_offset` to 0 to indicate
      // nothing has been consumed.
      *end_byte_offset = 0;
    } else if (!current_token_->error.ok()) {
      // The most recently returned token errored, set `end_byte_offset` to -1
      // to terminate the whole parsing. This is because we currently lose the
      // token location information when the underlying macro expander errors.
      *end_byte_offset = -1;
    } else if (Lookahead1IsRealEndOfInput()) {
      // The next token is a real YYEOF, set `end_byte_offset` to the end of the
      // input. For example, the input is ";  ", instead of pointing the end of
      // ';', we want to set the end to include the whitespaces, i.e. the whole
      // ";  ".
      *end_byte_offset = lookahead_1_->token.location.end().GetByteOffset();
    } else {
      // The next token is not end of file, just use the end location of the
      // current token.
      *end_byte_offset = current_token_->token.location.end().GetByteOffset();
    }
  }
  force_terminate_ = true;
  // Ensure that the lookahead buffers immediately reflects the termination so
  // that future calls to Lookahead1 and Lookahead2 correctly return YYEOF
  // rather than the original cached token kind. The error is set to be the same
  // as the error of the most recently returned token.
  TokenWithOverrideError template_token = {.error = GetOverrideError()};
  ResetToEof(template_token, lookahead_1_);
  ResetToEof(template_token, lookahead_2_);
  ResetToEof(template_token, lookahead_3_);
}

void DisambiguatorLexer::ResetToEof(
    const TokenWithOverrideError& template_token,
    std::optional<TokenWithOverrideError>& lookahead) const {
  lookahead = template_token;
  lookahead->token.kind = Token::YYEOF;
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

bool DisambiguatorLexer::Lookahead1IsRealEndOfInput() const {
  if (lookahead_1_->token.kind != Token::YYEOF) {
    return false;
  }
  if (!lookahead_1_->error.ok()) {
    // If lookahead1 errors, the Token::YYEOF does not necessarily indicate
    // the real end of file, so return false.
    return false;
  }
  if (current_token_.has_value() &&
      current_token_->token.kind == Token::YYEOF) {
    // The current token is already YYEOF. If it does not have error, then the
    // lookahead1 is not a real YYEOF because the input has already ended.
    // Otherwise, we don't know whether the next token is the real end of
    // the file or not. Return false in both cases.
    return false;
  }
  return true;
}

absl::Status DisambiguatorLexer::OverrideCurrentTokenLookback(
    TokenKind new_token_kind) {
  ZETASQL_RET_CHECK(current_token_.has_value());
  current_token_->lookback_override = new_token_kind;
  return absl::OkStatus();
}

void DisambiguatorLexer::PushState(StateType state) {
  state_stack_.push(state);
}

bool DisambiguatorLexer::PopStateIfMatch(StateType target_state) {
  if (state_stack_.empty() || state_stack_.top() != target_state) {
    return false;
  }
  state_stack_.pop();
  return true;
}

bool DisambiguatorLexer::IsInTemplatedTypeState() const {
  return !state_stack_.empty() && state_stack_.top() == kInTemplatedType;
}

TokenKind
DisambiguatorLexer::EmitInvalidLiteralPrecedesIdentifierTokenIfApplicable() {
  ABSL_DCHECK(current_token_.has_value());
  TokenKind token = current_token_->token.kind;
  if (token != Token::INTEGER_LITERAL &&
      token != Token::FLOATING_POINT_LITERAL) {
    return token;
  }
  // It's ok for inputs like "1.a" to stay two tokens, "1." and "a".
  if (current_token_->token.text.back() == '.') {
    return token;
  }
  // For example, emit a single token for "1.2abc" so that the callers
  // of GetParseTokens do not blindly insert whitespaces between "1.2"
  // and "abc", which changes the semantic meaning of the input.
  if (IsKeywordOrUnquotedIdentifier(lookahead_1_->token) &&
      IsAdjacentPrecedingToken(current_token_, lookahead_1_)) {
    FuseLookahead1IntoCurrent(
        Token::INVALID_LITERAL_PRECEDING_IDENTIFIER_NO_SPACE);
    return current_token_->token.kind;
  }
  return token;
}

}  // namespace parser
}  // namespace zetasql
