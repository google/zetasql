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

// Some contortions to avoid duplicate inclusion of FlexLexer.h in the
// generated flex_tokenizer.flex.cc.
#undef yyFlexLexer
#define yyFlexLexer ZetaSqlFlexTokenizerBase
#include <FlexLexer.h>

#include "zetasql/public/parse_location.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {

// Flex-based tokenizer for the ZetaSQL Bison parser.
class ZetaSqlFlexTokenizer final : public ZetaSqlFlexTokenizerBase {
 public:
  // Type aliases to improve readability of API.
  using TokenKind = int;

  // Constructs a wrapper around a flex generated tokenizer.
  // `filename`, `input` and `language_options` must outlive this object.
  ZetaSqlFlexTokenizer(absl::string_view filename, absl::string_view input,
                         bool preserve_comments, int start_offset);

  ZetaSqlFlexTokenizer(const ZetaSqlFlexTokenizer&) = delete;
  ZetaSqlFlexTokenizer& operator=(const ZetaSqlFlexTokenizer&) = delete;

  absl::StatusOr<TokenKind> GetNextToken(ParseLocationRange* location);

 private:
  void SetOverrideError(const ParseLocationRange& location,
                        absl::string_view error_message);

  // This method is implemented by the flex generated tokenizer. On input,
  // 'yylloc' must be the location of the previous token that was returned.
  // Returns the next token id, returning its location in 'yylloc'.
  TokenKind GetNextTokenFlexImpl(ParseLocationRange* location);

  // The (optional) filename from which the statement is being parsed.
  absl::string_view filename_;

  // The offset in the input of the first byte that is tokenized. This is used
  // to determine the returned location for the first token.
  const int start_offset_ = 0;

  // An input stream over the input string (of size input_size_) plus the
  // sentinel.
  std::unique_ptr<std::istream> input_stream_;

  // If set, comments are preserved. Used only in raw tokenization for the
  // formatter.
  bool preserve_comments_;

  // The Flex-generated tokenizer does not work with absl::StatusOr, so it
  // stores the error in this field. GetNextToken() grabs the status from here
  // when returning the result.
  absl::Status override_error_;
};

}  // namespace parser
}  // namespace zetasql

// This incantation is necessary because for some reason these functions are not
// generated for ZetaSqlFlexTokenizerBase, but the class does reference them.
inline int ZetaSqlFlexTokenizerBase::yylex() { return 0; }
inline int ZetaSqlFlexTokenizerBase::yywrap() { return 1; }

#endif  // ZETASQL_PARSER_FLEX_TOKENIZER_H_
