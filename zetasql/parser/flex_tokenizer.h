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

#ifndef ZETASQL_PARSER_FLEX_TOKENIZER_H_
#define ZETASQL_PARSER_FLEX_TOKENIZER_H_

#include <cstdint>
#include <istream>
#include <memory>
#include <optional>
#include <stack>
#include <vector>

// Some contortions to avoid duplicate inclusion of FlexLexer.h in the
// generated flex_tokenizer.flex.cc.
#undef yyFlexLexer
#define yyFlexLexer ZetaSqlFlexTokenizerBase
#include <FlexLexer.h>

#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/location.hh"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "absl/flags/declare.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

ABSL_DECLARE_FLAG(bool, zetasql_use_customized_flex_istream);

namespace zetasql {
namespace parser {

// Flex-based tokenizer for the ZetaSQL Bison parser.
class ZetaSqlFlexTokenizer final : public ZetaSqlFlexTokenizerBase {
 public:
  // Type aliases to improve readability of API.
  using TokenKind = int;
  using Location = zetasql_bison_parser::location;

  // Constructs a simple wrapper around a flex generated tokenizer. 'mode'
  // controls the first token that is returned to the bison parser, which
  // determines the starting production used by the parser.
  // 'filename', 'input', and 'language_options' must outlive this object.
  ZetaSqlFlexTokenizer(BisonParserMode mode, absl::string_view filename,
                         absl::string_view input, int start_offset,
                         const LanguageOptions& language_options);

  ZetaSqlFlexTokenizer(const ZetaSqlFlexTokenizer&) = delete;
  ZetaSqlFlexTokenizer& operator=(const ZetaSqlFlexTokenizer&) = delete;

  // Returns the next token id, returning its location in 'yylloc'. On input,
  // 'yylloc' must be the location of the previous token that was returned.
  TokenKind GetNextTokenFlex(Location* yylloc);

  // This is the "nice" API for the tokenizer, to be used by GetParseTokens().
  // On input, 'location' must be the location of the previous token that was
  // generated. Returns the Bison token id in 'token' and the ZetaSQL location
  // in 'location'. Returns an error if the tokenizer sets override_error.
  absl::Status GetNextToken(ParseLocationRange* location, TokenKind* token);

  // Returns a non-OK error status if the tokenizer encountered an error. This
  // error takes priority over a parser error, because the parser error is
  // always a consequence of the tokenizer error.
  absl::Status GetOverrideError() const { return override_error_; }

  // Ensures that the next token returned will be EOF, even if we're not at the
  // end of the input.
  void SetForceTerminate();

  // Some sorts of statements need to change the mode after the parser consumes
  // the preamble of the statement.  DEFINE MACRO is an example, it wants to
  // consume the macro body as raw tokens.
  void PushBisonParserMode(BisonParserMode mode);
  // Restore the BisonParserMode to its value before the previous Push.
  void PopBisonParserMode();

  // Helper function for determining if the given 'bison_token' followed by "."
  // should trigger the generalized identifier tokenizer mode.
  bool IsDotGeneralizedIdentifierPrefixToken(TokenKind bison_token) const;

  int64_t num_lexical_tokens() const { return num_lexical_tokens_; }

 private:
  // This friend is used by the unit test to help test internals.
  friend class TokenTestThief;

  void SetOverrideError(const Location& yylloc,
                        absl::string_view error_message);

  // This method is implemented by the flex generated tokenizer. On input,
  // 'yylloc' must be the location of the previous token that was returned.
  // Returns the next token id, returning its location in 'yylloc'.
  TokenKind GetNextTokenFlexImpl(Location* yylloc);

  // If the N+1 token is already buffered we simply return the token value from
  // the buffer. Otherwise we read the next token from `GetNextTokenFlexImpl`
  // and put it in the lookahead buffer before returning it.
  int Lookahead1(const Location& current_token_location);

  // Applies a set of rules based on previous and successive token kinds and if
  // any rule matches, returns the token kind specified by the rule.  Otherwise
  // when no rule matches, returns `token`. `location` is used when requesting
  // Lookahead tokens and also to generate error messages for
  // `SetOverrideError`.
  TokenKind ApplyTokenDisambiguation(TokenKind token, const Location& location);

  // This is called by flex when it is wedged.
  void LexerError(const char* msg) override;

  // Given a fragment of text that starts with an identifier, returns the length
  // of just the identifier portion of the text. Backquotes are included in the
  // returned length if the identifier is correctly backquoted.
  int GetIdentifierLength(absl::string_view text);

  bool IsReservedKeyword(absl::string_view text) const;

  bool AreMacrosEnabled() const;

  bool AreAlterArrayOptionsEnabled() const;

  // EOF sentinel input. This is appended to the input and used as a sentinel in
  // the tokenizer. The reason for doing this is that some tokenizer rules
  // try to match trailing context of the form [^...] where "..." is a set of
  // characters that should *not* be present after the token. Unfortunately
  // these rules actually also need to be triggered if, instead of "any
  // character that is not in [...]", there is EOF. For instance, the
  // unterminated comment rule cannot include the last star in "/* abcdef *"
  // because it looks for a * followed by "something that is not a star". To
  // solve this, we add some useless input characters at the end of the stream
  // that are not in any [...] used by affected rules. The useless input
  // characters are never returned as a token; when it is found, we return EOF
  // instead. All "open ended tokens" (unclosed string literal / comment)
  // that would include this bogus character in their location range are not
  // affected because they are all error tokens, and they immediately produce
  // errors that mention only their start location.
  static constexpr char kEofSentinelInput[] = "\n";

  // True only before the first call to lex(). We use an artificial first token
  // to determine which mode the bison parser should run in.
  bool is_first_token_ = true;

  // The kind of the most recently generated token from the flex layer.
  TokenKind prev_flex_token_ = 0;
  // The kind of the most recently dispensed token to the consuming component
  // (usually the last token dispensed to the parser).
  TokenKind prev_dispensed_token_ = 0;

  // The (optional) filename from which the statement is being parsed.
  absl::string_view filename_;

  // The offset in the input of the first byte that is tokenized. This is used
  // to determine the returned location for the first token.
  const int start_offset_ = 0;

  // The length of the input string without the sentinel.
  const int input_size_;
  // An input stream over the input string (of size input_size_) plus the
  // sentinel.
  std::unique_ptr<std::istream> input_stream_;

  // This determines the first token returned to the bison parser, which
  // determines the mode that we'll run in.
  BisonParserMode mode_;
  std::stack<BisonParserMode> restore_modes_;

  // The tokenizer may want to return an error directly. It does this by
  // returning EOF to the bison parser, which then may or may not spew out its
  // own error message. The BisonParser wrapper then grabs the error from the
  // tokenizer instead.
  absl::Status override_error_;

  // If this is set to true, the next token returned will be EOF, even if we're
  // not at the end of the input.
  bool force_terminate_ = false;

  // LanguageOptions passed in from parser, used to decide if reservable
  // keywords are reserved or not.
  const LanguageOptions& language_options_;

  // Count of lexical tokens returned
  int64_t num_lexical_tokens_ = 0;

  // The lookahead_N_ fields implement the token lookahead buffer. There are a
  // fixed number of fields here, each represented by an optional, rather than a
  // deque or vector because, ideally we only do token disambiguation on small
  // windows (e.g. no more than two or three lookaheads).

  // A token in the lookahead buffer.
  struct TokenInfo {
    TokenKind token;
    Location token_location;
  };
  // The lookahead buffer slot for token N+1.
  std::optional<TokenInfo> lookahead_1_;
};

}  // namespace parser
}  // namespace zetasql

// This incantation is necessary because for some reason these functions are not
// generated for ZetaSqlFlexTokenizerBase, but the class does reference them.
inline int ZetaSqlFlexTokenizerBase::yylex() { return 0; }
inline int ZetaSqlFlexTokenizerBase::yywrap() { return 1; }

#endif  // ZETASQL_PARSER_FLEX_TOKENIZER_H_
