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

#ifndef ZETASQL_PARSER_TOKENIZER_H_
#define ZETASQL_PARSER_TOKENIZER_H_

#include "zetasql/parser/tm_lexer.h"
#include "zetasql/parser/tm_token.h"
#include "zetasql/public/parse_location.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {

// A wrapper class for the generated TextMapper lexer class with access to
// the private fields of `Lexer`.
// TODO: b/322871843 - Rename the file to tokenizer.h.
class ZetaSqlTokenizer final : Lexer {
 public:
  ZetaSqlTokenizer(absl::string_view filename, absl::string_view input,
                     int start_offset);

  ZetaSqlTokenizer(const ZetaSqlTokenizer&) = delete;
  ZetaSqlTokenizer& operator=(const ZetaSqlTokenizer&) = delete;

  absl::StatusOr<Token> GetNextToken(ParseLocationRange* location);
};

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_TOKENIZER_H_
