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

#ifndef ZETASQL_PARSER_TEXTMAPPER_LEXER_ADAPTER_H_
#define ZETASQL_PARSER_TEXTMAPPER_LEXER_ADAPTER_H_

#include <algorithm>
#include <cstdint>
#include <deque>
#include <memory>
#include <utility>

#include "zetasql/base/arena.h"
#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/lookahead_transformer.h"
#include "zetasql/parser/macros/macro_catalog.h"
#include "zetasql/parser/tm_token.h"
#include "zetasql/parser/token_codes.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "absl/base/attributes.h"
#include "zetasql/base/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {

// The TextMapperLexerAdapter adapts the token stream produced by ZetaSQL's
// intermediate components (macro expansion, lookahead transformer, etc) into
// a stream of tokens that can be consumed by the TextMapper parser.
class TextMapperLexerAdapter {
 public:
  using InputToken = Token;
  using Location = ParseLocationRange;

  explicit TextMapperLexerAdapter(BisonParserMode mode,
                                  absl::string_view filename,
                                  absl::string_view input, int start_offset,
                                  const LanguageOptions& language_options,
                                  const macros::MacroCatalog* macro_catalog,
                                  zetasql_base::UnsafeArena* arena);
  ~TextMapperLexerAdapter();

  TextMapperLexerAdapter(const TextMapperLexerAdapter& other);

  TextMapperLexerAdapter(TextMapperLexerAdapter&& other) = delete;
  TextMapperLexerAdapter& operator=(TextMapperLexerAdapter&& other) = delete;

  ABSL_MUST_USE_RESULT Token Next();

  // Returns the 1-based line number of the last token returned by Next().
  ABSL_MUST_USE_RESULT int64_t Line() const {
    // TODO: Actually implement the logic. With the bison parser, we only
    // generate lines and columns when computing error location, and this
    // function always returned the static line number as it was unused in bison
    // locations, i.e. 1
    return 1;
  }

  // Text returns the substring of the input corresponding to the last token.
  ABSL_MUST_USE_RESULT absl::string_view Text() const
      ABSL_ATTRIBUTE_LIFETIME_BOUND {
    return last_token_.text;
  }

  ABSL_MUST_USE_RESULT const Location& LastTokenLocation() const {
    return last_token_.location;
  }
  LookaheadTransformer& tokenizer() { return *tokenizer_; }

 private:
  struct TextMapperToken {
    Token kind;
    Location location;
    absl::string_view text;
  };
  std::deque<TextMapperToken> queued_tokens_;
  std::shared_ptr<LookaheadTransformer> tokenizer_;
  TextMapperToken last_token_;
};

using Lexer = TextMapperLexerAdapter;

}  // namespace parser

}  // namespace zetasql

#endif  // ZETASQL_PARSER_TEXTMAPPER_LEXER_ADAPTER_H_
