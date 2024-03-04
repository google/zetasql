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

#ifndef ZETASQL_PARSER_TOKEN_DISAMBIGUATOR_H_
#define ZETASQL_PARSER_TOKEN_DISAMBIGUATOR_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <stack>
#include <variant>
#include <vector>

#include "zetasql/base/arena.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/flex_tokenizer.h"
#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/macros/macro_expander.h"
#include "zetasql/parser/macros/token_with_location.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "absl/base/attributes.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {

class DisambiguatorLexer final {
 public:
  using TokenKind = ZetaSqlFlexTokenizer::TokenKind;
  using Location = ZetaSqlFlexTokenizer::Location;

  static absl::StatusOr<std::unique_ptr<DisambiguatorLexer>> Create(
      BisonParserMode mode, absl::string_view filename, absl::string_view input,
      int start_offset, const LanguageOptions& language_options,
      const macros::MacroCatalog* macro_catalog, zetasql_base::UnsafeArena* arena);

  // Returns the next token id, returning its location in 'yylloc' and image in
  // 'text'. On input, 'yylloc' must be the location of the previous token that
  // was returned.
  TokenKind GetNextToken(absl::string_view* text, Location* yylloc);

  // This is the "nice" API for the tokenizer, to be used by GetParseTokens().
  // On input, 'location' must be the location of the previous token that was
  // generated. Returns the Bison token id in 'token' and the ZetaSQL location
  // in 'location'. Returns an error if the tokenizer sets override_error.
  absl::Status GetNextToken(ParseLocationRange* location, int* token);

  // Returns a non-OK error status if the tokenizer encountered an error. This
  // error takes priority over a parser error, because the parser error is
  // always a consequence of the tokenizer error.
  absl::Status GetOverrideError() const { return override_error_; }

  // Ensures that the next token returned will be EOF, even if we're not at the
  // end of the input. The current end offset is returned in `end_byte_offset`
  // if it is not nullptr.
  // Ensures that `lookahead_1_` is filled (and masked as YYEOF), and voids
  // `lookahead_2_`. Lookahead1 is important for when the input ends in a
  // semicolon, because in that case we need the returned `end_byte_offset`
  // points to the end of input, not just the semicolon.
  void SetForceTerminate(int* end_byte_offset);

  // Some sorts of statements need to change the mode after the parser consumes
  // the preamble of the statement. DEFINE MACRO is an example, it wants to
  // consume the macro body as raw tokens.
  void PushBisonParserMode(BisonParserMode mode);
  // Restore the BisonParserMode to its value before the previous Push.
  void PopBisonParserMode();

  // Returns the number of lexical tokens returned by the underlying tokenizer.
  int64_t num_lexical_tokens() const;

 private:
  DisambiguatorLexer(BisonParserMode mode,
                     const LanguageOptions& language_options,
                     std::unique_ptr<macros::MacroExpanderBase> expander);

  DisambiguatorLexer(const DisambiguatorLexer&) = delete;
  DisambiguatorLexer& operator=(const DisambiguatorLexer&) = delete;

  // This friend is used by the unit test to help test internals.
  friend class TokenTestThief;

  // If the N+1 token is already buffered we simply return the token value from
  // the buffer. Otherwise we read the next token from `GetNextTokenFlexImpl`
  // and put it in the lookahead buffer before returning it.
  int Lookahead1(const Location& current_token_location);

  // If the N+2 token is already buffered we simply return the token value from
  // the buffer. Otherwise we ensure the N+1 lookahead is populated then read
  // the next token from `GetNextTokenFlexImpl` and put it in the lookahead
  // buffer before returning it. `current_token_location` is used if we need
  // to populated token N+1.
  int Lookahead2(const Location& current_token_location);

  // Applies a set of rules based on previous and successive token kinds and if
  // any rule matches, returns the token kind specified by the rule.  Otherwise
  // when no rule matches, returns `token`. `location` is used when requesting
  // Lookahead tokens and also to generate error messages for
  // `SetOverrideError`.
  TokenKind ApplyTokenDisambiguation(TokenKind token, const Location& location);

  // Indicates whether macros are enabled.
  bool AreMacrosEnabled() const;

  bool IsReservedKeyword(absl::string_view text) const {
    return language_options_.IsReservedKeyword(text);
  }

  // Sets the field `override_error_` and returns the token kind YYEOF.
  ABSL_MUST_USE_RESULT TokenKind SetOverrideErrorAndReturnEof(
      absl::string_view error_message, const Location& error_location);

  // This determines the first token returned to the bison parser, which
  // determines the mode that we'll run in.
  BisonParserMode mode_;
  std::stack<BisonParserMode> restore_modes_;

  const LanguageOptions& language_options_;

  // The underlying macro expander which feeds tokens to this disambiguator.
  std::unique_ptr<macros::MacroExpanderBase> macro_expander_;

  // If this is set to true, the next token returned will be EOF, even if we're
  // not at the end of the input.
  bool force_terminate_ = false;

  // The disambiguator may want to return an error directly. It does this by
  // returning EOF to the bison parser, which then may or may not spew out its
  // own error message. The BisonParser wrapper then grabs the error from here
  // instead.
  absl::Status override_error_;

  // The previous token returned by the tokenizer.
  std::optional<macros::TokenWithLocation> prev_token_;

  // The lookahead_N_ fields implement the token lookahead buffer. There are a
  // fixed number of fields here, each represented by an optional, rather than a
  // deque or vector because, ideally we only do token disambiguation on small
  // windows (e.g. no more than two or three lookaheads).

  // A token in the lookahead buffer.
  // The lookahead buffer slot for token N+1.
  std::optional<macros::TokenWithLocation> lookahead_1_;

  // The lookahead buffer slot for token N+2.
  std::optional<macros::TokenWithLocation> lookahead_2_;
};

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_TOKEN_DISAMBIGUATOR_H_
