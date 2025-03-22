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

#include <istream>
#include <memory>

#include "zetasql/parser/tm_lexer.h"
#include "zetasql/parser/tm_token.h"
#include "zetasql/parser/token_codes.h"
#include "absl/flags/declare.h"

// Some contortions to avoid duplicate inclusion of FlexLexer.h in the
// generated flex_tokenizer.flex.cc.
#undef yyFlexLexer
#define yyFlexLexer ZetaSqlFlexTokenizerBase
#include <FlexLexer.h>

#include "zetasql/public/parse_location.h"
#include "absl/base/macros.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

ABSL_DECLARE_FLAG(bool, use_textmapper_lexer);

namespace zetasql {
namespace parser {

// The legacy Flex-based tokenizer for ZetaSQL. Kept only for validation
// purpose.
class LegacyFlexTokenizer final : public ZetaSqlFlexTokenizerBase {
 public:
  // Constructs a wrapper around a flex generated tokenizer.
  // `filename`, `input` and `language_options` must outlive this object.
  LegacyFlexTokenizer(absl::string_view filename, absl::string_view input,
                      int start_offset);

  LegacyFlexTokenizer(const LegacyFlexTokenizer&) = delete;
  LegacyFlexTokenizer& operator=(const LegacyFlexTokenizer&) = delete;

  absl::StatusOr<Token> GetNextToken(ParseLocationRange* location);

 private:
  void SetOverrideError(const ParseLocationRange& location,
                        absl::string_view error_message);

  // This method is implemented by the flex generated tokenizer. On input,
  // 'yylloc' must be the location of the previous token that was returned.
  // Returns the next token id, returning its location in 'yylloc'.
  Token GetNextTokenFlexImpl(ParseLocationRange* location);

  // The (optional) filename from which the statement is being parsed.
  absl::string_view filename_;

  // The offset in the input of the first byte that is tokenized. This is used
  // to determine the returned location for the first token.
  const int start_offset_ = 0;

  // An input stream over the input string (of size input_size_) plus the
  // sentinel.
  std::unique_ptr<std::istream> input_stream_;

  // The Flex-generated tokenizer does not work with absl::StatusOr, so it
  // stores the error in this field. GetNextToken() grabs the status from here
  // when returning the result.
  absl::Status override_error_;
};

// A wrapper class for the generated TextMapper lexer class with access to
// the private fields of `Lexer`.
// TODO: b/322871843: Find a way to use the `Lexer` class directly, maybe by
// updating the TextMapper template.
class TextMapperTokenizer final : Lexer {
 public:
  TextMapperTokenizer(absl::string_view filename, absl::string_view input,
                      int start_offset);

  TextMapperTokenizer(const TextMapperTokenizer&) = delete;
  TextMapperTokenizer& operator=(const TextMapperTokenizer&) = delete;

  absl::StatusOr<Token> GetNextToken(ParseLocationRange* location);
};

class ZetaSqlTokenizer {
 public:
  ZetaSqlTokenizer(absl::string_view filename, absl::string_view input,
                     int start_offset, bool force_flex);

  ZetaSqlTokenizer(const ZetaSqlTokenizer&) = delete;
  ZetaSqlTokenizer& operator=(const ZetaSqlTokenizer&) = delete;

  absl::StatusOr<Token> GetNextToken(ParseLocationRange* location);

 private:
  // Validates the next TextMapper token is the same as the given flex token
  // (`flex_token`, `flex_token_location`).
  //
  // `text_mapper_tokenizer_` will be initialized if it is nullptr.
  absl::Status ValidateTextMapperToken(
      absl::StatusOr<Token> flex_token,
      const ParseLocationRange& flex_token_location);

  std::unique_ptr<LegacyFlexTokenizer> flex_tokenizer_;
  std::unique_ptr<TextMapperTokenizer> text_mapper_tokenizer_;

  const absl::string_view filename_;
  const absl::string_view input_;
  const int start_offset_;
  const bool force_flex_;
};

using ZetaSqlFlexTokenizer ABSL_DEPRECATED("Inline me!") = ZetaSqlTokenizer;

}  // namespace parser
}  // namespace zetasql

// This incantation is necessary because for some reason these functions are not
// generated for ZetaSqlFlexTokenizerBase, but the class does reference them.
inline int ZetaSqlFlexTokenizerBase::yylex() { return 0; }
inline int ZetaSqlFlexTokenizerBase::yywrap() { return 1; }

#endif  // ZETASQL_PARSER_FLEX_TOKENIZER_H_
