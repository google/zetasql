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

#ifndef ZETASQL_PARSER_LOOKAHEAD_TRANSFORMER_H_
#define ZETASQL_PARSER_LOOKAHEAD_TRANSFORMER_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <stack>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/bison_token_codes.h"
#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/macros/macro_expander.h"
#include "zetasql/parser/macros/token_with_location.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "absl/base/attributes.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {

using TokenKind = int;
using Location = ParseLocationRange;

constexpr TokenKind kNoToken = -2;  // TokenKinds::YYEMPTY

// Represents a token with a possible override_error. We don't use
// absl::StatusOr because the token location is still needed when an error
// occurs.
struct TokenWithOverrideError {
  macros::TokenWithLocation token;

  // When this token becomes a lookback, disambiguation rules should see
  // `lookback_token` and typically ignore `token.kind`.
  TokenKind lookback_override = kNoToken;

  // The lookahead_transformer may want to return an error directly. It does
  // this by returning EOF to the bison parser, which then may or may not spew
  // out its own error message. The BisonParser wrapper then grabs the error
  // from here instead.
  absl::Status error = absl::OkStatus();
};

class LookaheadTransformer final {
 public:
  static absl::StatusOr<std::unique_ptr<LookaheadTransformer>> Create(
      BisonParserMode mode, absl::string_view filename, absl::string_view input,
      int start_offset, const LanguageOptions& language_options,
      const macros::MacroCatalog* macro_catalog, zetasql_base::UnsafeArena* arena);

  // Returns the next token id, returning its location in `yylloc` and image in
  // `text`.
  //
  // Token output rules:
  // - An error is reported through `GetOverrideError()` if the most recently
  //   returned token exists and produces an error.
  //
  // - If `SetForceTerminate()` has been called, subsequent calls to
  //   `GetNextToken()` will always return Token::YYEOF.
  //
  // - If a previous call to `GetNextToken()` returned Token::YYEOF, which can
  //   be because
  //   - `SetForceTerminate()` has been called.
  //   - Or the previous token errors.
  //   - Or the previous token is a real end of input.
  //
  //   all future calls to `GetNextToken()` return Token::YYEOF and
  //   `GetOverrideError()` returns the same error, if any.
  //
  // - If an error occurs, i.e. `GetOverrideError().ok()` is false after the
  //   call, returns Token::YYEOF.
  //
  // Output parameters:
  // - The output parameters are undefined if
  //   - The lookahead_transformer has been SetForceTerminate.
  //   - Or the returned token has an error.
  // - Otherwise, they will be updated with the text and location of the
  //   returned token.
  TokenKind GetNextToken(absl::string_view* text, Location* yylloc);

  // Returns the error associated with the most recent token returned by
  // GetNextToken, and absl::OkStatus() if it does not have errors.
  // TODO: Rename this to GetError().
  absl::Status GetOverrideError() const {
    if (current_token_.has_value()) {
      return current_token_->error;
    }
    return absl::OkStatus();
  }

  // Returns the Bison token id in `token` and the ZetaSQL location
  // in `location`. Returns an error if the tokenizer sets override_error.
  absl::Status GetNextToken(ParseLocationRange* location, TokenKind* token);

  // Ensures that the next token returned will be EOF, even if we're not at the
  // end of the input.
  //
  // `end_byte_offset`, if is not nullptr, will be populated with the end offset
  // for the statement:
  // (1) 0 if no tokens have been returned.
  // (2) -1 if the most recently returned token errors.
  // (3) the end location of the most recently returned token if
  //     - the most recently returned token is Token::YYEOF with no error.
  //     - or the lookahead1 is not a Token::YYEOF or a Token::YYEOF with an
  //       error.
  // (4) the end location of the lookahead1 otherwise, i.e. lookahead1 is a
  //     Token::YYEOF without errors.
  //
  // Explanations:
  // - For (2): We currently lose the token location when the underlying token
  //   errors (see ConsumeNextToken()), so we can only set it to -1 to denote
  //   the end of all the statements.
  //   TODO: b/329291322 - Update the macro expander to return
  //   `TokenWithOverrideError` instead.
  //
  // - For (3): When the input ends in a semicolon, we need the returned
  //   `end_byte_offset` points to the end of the semicolon plus whitespaces,
  //   not just the semicolon.
  //
  // Future calls to `GetNextToken` will always return Token::YYEOF. An error
  // is returned if and only if the most recently returned token, if exists, has
  // an error.
  void SetForceTerminate(int* end_byte_offset);

  // Some sorts of statements need to change the mode after the parser consumes
  // the preamble of the statement. DEFINE MACRO is an example, it wants to
  // consume the macro body as raw tokens.
  void PushBisonParserMode(BisonParserMode mode);
  // Restore the BisonParserMode to its value before the previous Push.
  void PopBisonParserMode();

  // This function is called by the Bison or Textmapper parsers before they
  // (maybe) consume `expected_next_token` and will set an alternative lookback
  // token kind, `lookback_token`, for that token. The alternative token is only
  // available via the Lookback1() function.
  //
  // `parser_lookahead_is_empty` indicates whether the respective parser has
  // pulled the next token into its LA(1) buffer. When
  // `parser_lookahead_is_empty` is false, the expected token is looked for in
  // in `current_token_`. When `parser_lookahead_is_empty` is true, the
  // expected token is looked for in `Lookahead1()`.
  //
  // This context hint is not useful to affect the lexer's choice of token
  // for `expected_next_token` because in the case that the parser's LA(1)
  // buffer if full, not only has the token already left the
  // lookahead_transformer, the parser may have acted on that token kind. This
  // is useful for affecting subsequent tokens beyond `expected_next_token`.
  absl::Status OverrideNextTokenLookback(bool parser_lookahead_is_empty,
                                         TokenKind expected_next_token,
                                         TokenKind lookback_token);

  // This function is called by the parser to set the `lookback_override` field
  // for `current_token_` to be `new_token_kind`. A ZETASQL_RET_CHECK is returned if
  // `current_token_` is std::nullopt.
  absl::Status OverrideCurrentTokenLookback(TokenKind new_token_kind);

  // Returns the number of lexical tokens returned by the underlying tokenizer.
  int64_t num_lexical_tokens() const;

 private:
  using StateType = char;

  LookaheadTransformer(BisonParserMode mode,
                       std::optional<macros::TokenWithLocation> start_token,
                       const LanguageOptions& language_options,
                       std::unique_ptr<macros::MacroExpanderBase> expander);

  LookaheadTransformer(const LookaheadTransformer&) = delete;
  LookaheadTransformer& operator=(const LookaheadTransformer&) = delete;

  // This friend is used by the unit test to help test internals.
  friend class TokenTestThief;

  // Returns the kind for the N+1 token. Requires lookahead buffers have been
  // populated.
  TokenKind Lookahead1() const;

  // Returns the kind for the N+2 token. Requires lookahead buffers have been
  // populated.
  TokenKind Lookahead2() const;

  // Returns the kind for the N+3 token. Requires lookahead buffers have been
  // populated.
  TokenKind Lookahead3() const;

  // Lookback to token returned before `curren_token_`. If the
  // `lookback_override` field has been set then that token kind will be
  // returned. Otherwise, this will return the N-1 token as produced after
  // macro expansion.
  //
  // Until one token has been returned, this will return kNoToken.
  TokenKind Lookback1() const;

  // Lookback to token returned before `Lookback1()`. If the
  // `lookback_override` field has been set then that token kind will be
  // returned. Otherwise, this will return the N-2 token as produced after
  // macro expansion.
  //
  // Until two tokens have been returned, this will return kNoToken.
  TokenKind Lookback2() const;

  // Populates the lookahead buffers if they are nullopt.
  void PopulateLookaheads();

  // Reads the next token from `macro_expander_` and writes it into `next`.
  // `current` is the token before `next`. The original content in `next` will
  // be overwritten.
  //
  // When `current` is already YYEOF, this function does not try fetching from
  // the underlying token stream; instead it populates `next` with `current`
  // directly to guarantee future calls to GetNextToken() and GetOverrideError()
  // return the same token kind and error.
  void FetchNextToken(const std::optional<TokenWithOverrideError>& current,
                      std::optional<TokenWithOverrideError>& next);

  // A helper used by FetchNextToken that applies conditionally reserved keyword
  // rules. Its possible to identify conditionally reserved keywords immediately
  // because they are conditioned only on language options and not their
  // context. Identifying conditionally reserved keywords early helps keep
  // lookahead logic simple since we can depend on `IsReservedKeyword` and
  // `IsNonreservedKeyword` without special casing conditionally reserved
  // keywords each time.
  void ApplyConditionallyReservedKeywords(TokenKind& kind);

  // Applies a set of rules based on previous and successive token kinds and if
  // any rule matches, returns the token kind specified by the rule.  Otherwise
  // when no rule matches, returns `token_with_location.kind`.
  // `token_with_location.location` is used to generate error messages for
  // `SetOverrideError`.
  //
  // Requirements on the rules:
  // - Token::YYEOF cannot be transformed into any other tokens.
  // - Rules that compare lookaheads with Token::YYEOF must also check
  //   whether the lookaheads have any errors, because Token::YYEOF can also
  //   indicate an error rather than the real end of file.
  TokenKind ApplyTokenDisambiguation(
      const macros::TokenWithLocation& token_with_location);

  // Sets the field `override_error_` and returns the token kind
  // Token::YYEOF.
  ABSL_MUST_USE_RESULT TokenKind SetOverrideErrorAndReturnEof(
      absl::string_view error_message, const Location& error_location);

  // Returns whether `lookahead_1_` contains a Token::YYEOF that represents the
  // real end of input. For example, if `lookahead_1_` errors, its token kind
  // is Token::YYEOF but it does not represent the real end of input.
  bool Lookahead1IsRealEndOfInput() const;

  // This determines the first token returned to the bison parser, which
  // determines the mode that we'll run in.
  BisonParserMode mode_;
  std::stack<BisonParserMode> restore_modes_;

  const LanguageOptions& language_options_;

  // The underlying macro expander which feeds tokens to this
  // lookahead_transformer.
  std::unique_ptr<macros::MacroExpanderBase> macro_expander_;

  // If this is set to true, the next token returned will be EOF, even if we're
  // not at the end of the input.
  bool force_terminate_ = false;

  // Number of tokens inserted by this layer.
  int num_inserted_tokens_ = 0;

  // Sets the token of `lookahead` to Token::YYEOF even if `lookahead` is
  // nullopt. All other fields, for example, `lookahead->error` and
  // `lookahead->token.location` are set to the same values in `template_token`.
  void ResetToEof(const TokenWithOverrideError& template_token,
                  std::optional<TokenWithOverrideError>& lookahead) const;

  // Returns whether the given `token_kind` can appear before "." in a path
  // expression.
  bool LookbackTokenCanBeBeforeDotInPathExpression(TokenKind token_kind) const;

  // Fuses `lookahead_1_` into `current_token_` with the new token kind
  // `fused_token_kind`. The lookahead buffers are advanced accordingly.
  //
  // Should only be called when `current_token_` has value and
  // `IsAdjacentPrecedingToken(current_token_, lookahead_1_)` returns true.
  void FuseLookahead1IntoCurrent(TokenKind fused_token_kind);

  // Pushes a state onto the stack used by the lookahead_transformer to handle
  // paren-balancing operations. Typically, `state` is a character sort that
  // is an "open" marker (e.g. '('). Internally, the lookahead_transformer
  // tracks opening and closing of chars that are typically balanced (e.g. '('
  // or '[' but not '<' which is used unbalanced for comparison operators). Used
  // in conjunction with `PopStateIfMatch` to find the matching close token.
  void PushState(StateType state);

  // Pops the top of the stack and returns true if the top of `state_stack_`
  // matches `target_state`. Otherwise does nothing and returns false.
  bool PopStateIfMatch(StateType target_state);
  // Returns whether the lookahead_transformer is in the `kInTemplatedType`
  // state.
  bool IsInTemplatedTypeState() const;

  // If the current token is an integer or float literal followed by an
  // identifier without spaces, updates the token kind of the current token to
  // be Token::INVALID_LITERAL_PRECEDING_IDENTIFIER_NO_SPACE and returns it.
  // Otherwise returns the current token kind.
  //
  // This function should only be called when under the `kTokenizer` or
  // `kTokenizerPreserveComments` mode.
  TokenKind EmitInvalidLiteralPrecedesIdentifierTokenIfApplicable();

  // Applies token transformations for `current_token_` and returns the new
  // token kind. Should only be called when `current_token_` holds '.'.
  TokenKind TransformDotSymbol();

  // Applies token transformations for `current_token_`. Should only be called
  // when `current_token_` holds DECIMAL_INTEGER_LITERAL.
  void TransformIntegerLiteral();

  // Tries fusing the lookaheads into the exponential part of a floating point
  // literal. If it succeeds, the exponential part is fused into
  // `current_token_` with token kind FLOATING_POINT_LITERAL and returns true.
  // Otherwise no tokens will change and the function returns false.
  //
  // Should only be called when `current_token_` can be the start of a floating
  // point literal, i.e. "." or INTEGER_LITERAL.
  bool FuseExponentPartIntoFloatingPointLiteral();

  // The token the lookahead_transformer returned the last time when
  // GetNextToken was called.
  std::optional<TokenWithOverrideError> current_token_;

  // The token returned before `current_token_`. TokenWithOverrideError is used
  // instead of just the TokenKind to allow easier swap with `current_token_`.
  std::optional<TokenWithOverrideError> lookback_1_;

  // The token returned before `lookback_1_`. TokenWithOverrideError is used
  // instead of just the TokenKind to allow easier swap with `lookback_1_`.
  std::optional<TokenWithOverrideError> lookback_2_;

  // The lookahead_N_ fields implement the token lookahead buffer. There are a
  // fixed number of fields here, each represented by an optional, rather than a
  // deque or vector because, ideally we only do token disambiguation on small
  // windows (e.g. no more than two or three lookaheads).
  //
  // Note the errors stored in the lookaheads should not be exposed. The
  // lookahead_transformer should only return the errors stored in
  // `current_token_`.
  //
  // Invariants:
  // - If `current_token_` or a lookahead is Token::YYEOF, all further
  //   lookaheads are Token::YYEOF with the same errors.

  // A token in the lookahead buffer.
  // The lookahead buffer slot for token N+1.
  std::optional<TokenWithOverrideError> lookahead_1_;

  // The lookahead buffer slot for token N+2.
  std::optional<TokenWithOverrideError> lookahead_2_;

  // The lookahead buffer slot for token N+3.
  std::optional<TokenWithOverrideError> lookahead_3_;

  // Stores the special symbols that affect the token disambiguation behaviors.
  //
  // If the top of the stack stores:
  // - `kInTemplatedType`: The lookahead_transformer is processing tokens inside
  //   a templated type, so for example it stops recognizing ">>" as
  //   KW_SHIFT_RIGHT but two ">"s.
  // - '(' or ')': The lookahead_transformer is processing tokens inside a pair
  //   of parentheses or after an unpaired ')', mainly used to temporarily leave
  //   the `kInTemplatedType` state.
  std::stack<StateType> state_stack_;
};

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_LOOKAHEAD_TRANSFORMER_H_
