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

#include "absl/status/statusor.h"

// Some contortions to avoid duplicate inclusion of FlexLexer.h in the
// generated flex_tokenizer.flex.cc.
#undef yyFlexLexer
#define yyFlexLexer ZetaSqlFlexTokenizerBase
#include <FlexLexer.h>

#include "zetasql/parser/bison_parser_mode.h"
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
  using Location = ParseLocationRange;
  using TokenKind = int;

  // Constructs a simple wrapper around a flex generated tokenizer. 'mode'
  // controls the first token that is returned to the bison parser, which
  // determines the starting production used by the parser.
  // 'filename', 'input', and 'language_options' must outlive this object.
  ZetaSqlFlexTokenizer(BisonParserMode mode, absl::string_view filename,
                         absl::string_view input, int start_offset,
                         const LanguageOptions& language_options);

  ZetaSqlFlexTokenizer(const ZetaSqlFlexTokenizer&) = delete;
  ZetaSqlFlexTokenizer& operator=(const ZetaSqlFlexTokenizer&) = delete;

  absl::StatusOr<TokenKind> GetNextToken(Location* location);

  // This is the "nice" API for the tokenizer, to be used by GetParseTokens().
  // On input, 'location' must be the location of the previous token that was
  // generated. Returns the Bison token id in 'token' and the ZetaSQL location
  // in 'location'. Returns an error if the tokenizer sets override_error.
  absl::Status GetNextToken(ParseLocationRange* location, TokenKind* token);

  // Helper function for determining if the given 'bison_token' followed by "."
  // should trigger the generalized identifier tokenizer mode.
  bool IsDotGeneralizedIdentifierPrefixToken(TokenKind bison_token) const;

  int64_t num_lexical_tokens() const { return num_lexical_tokens_; }

  absl::string_view filename() const { return filename_; }
  absl::string_view input() const { return input_; }

 private:
  void SetOverrideError(const Location& yylloc,
                        absl::string_view error_message);

  // This method is implemented by the flex generated tokenizer. On input,
  // 'yylloc' must be the location of the previous token that was returned.
  // Returns the next token id, returning its location in 'yylloc'.
  TokenKind GetNextTokenFlexImpl(Location* yylloc);

  // This is called by flex when it is wedged.
  void LexerError(const char* msg) override;

  // Given a fragment of text that starts with an identifier, returns the length
  // of just the identifier portion of the text. Backquotes are included in the
  // returned length if the identifier is correctly backquoted.
  int GetIdentifierLength(absl::string_view text);

  bool IsReservedKeyword(absl::string_view text) const;

  bool AreMacrosEnabled() const;
  bool EnforceStrictMacros() const;

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

  // The (optional) filename from which the statement is being parsed.
  absl::string_view filename_;

  // The input where we are tokenizing from. Note that if `start_offset_` is
  // not zero, tokenization starts from the specified offset.
  absl::string_view input_;

  // The offset in the input of the first byte that is tokenized. This is used
  // to determine the returned location for the first token.
  const int start_offset_ = 0;

  // The length of the input string without the sentinel.
  const int input_size_;
  // An input stream over the input string (of size input_size_) plus the
  // sentinel.
  std::unique_ptr<std::istream> input_stream_;

  // When true, returns CUSTOM_MODE_START as the first token before working on
  // the input.
  bool generate_custom_mode_start_token_;

  // If set, the lexer terminates if it encounters a semicolon, instead of
  // continuing into the next statement.
  bool terminate_after_statement_;

  // If set, comments are preserved. Used only in raw tokenization for the
  // formatter.
  bool preserve_comments_;

  // The Flex-generated tokenizer does not work with absl::StatusOr, so it
  // stores the error in this field. GetNextToken() grabs the status from here
  // when returning the result.
  absl::Status override_error_;

  // LanguageOptions passed in from parser, used to decide if reservable
  // keywords are reserved or not.
  const LanguageOptions& language_options_;

  // Count of lexical tokens returned
  int64_t num_lexical_tokens_ = 0;
};

}  // namespace parser
}  // namespace zetasql

// This incantation is necessary because for some reason these functions are not
// generated for ZetaSqlFlexTokenizerBase, but the class does reference them.
inline int ZetaSqlFlexTokenizerBase::yylex() { return 0; }
inline int ZetaSqlFlexTokenizerBase::yywrap() { return 1; }

#endif  // ZETASQL_PARSER_FLEX_TOKENIZER_H_
