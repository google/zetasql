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

#include <memory>

#include "zetasql/parser/location.hh"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {

// General interface for tokenizers. DOCUMENT
class Tokenizer {
 public:
  // Type aliases to improve readability of API.
  using Location = zetasql_bison_parser::location;
  using TokenKind = int;
  virtual ~Tokenizer() = default;

  // Returns a fresh instance of this tokenizer, to run on potentially new
  // input. However, language_options, mode, etc, are all the same.
  virtual std::unique_ptr<Tokenizer> GetNewInstance(
      absl::string_view filename, absl::string_view input) const = 0;

  // Returns the next lexical token kind. On input, 'location' must be the
  // location of the previous token that was generated. Returns the Bison token
  // id in 'token' and the ZetaSQL location in 'location'. Returns an error if
  // the tokenizer sets override_error.
  virtual TokenKind GetNextToken(Location* location) = 0;

  // Returns the override error, if set. Multiple custom rules in our lexer
  // set the override error.
  virtual absl::Status GetOverrideError() const = 0;
};

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_TOKENIZER_H_
