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

#ifndef ZETASQL_PARSER_TOKEN_CODES_H_
#define ZETASQL_PARSER_TOKEN_CODES_H_

#include "zetasql/parser/tm_token.h"
#include "absl/base/macros.h"

namespace zetasql {
namespace parser {

using TokenKinds ABSL_DEPRECATED("Inline me!") = Token;

}  // namespace parser

using TokenKinds ABSL_DEPRECATED("Inline me!") = parser::Token;

}  // namespace zetasql

#endif  // ZETASQL_PARSER_TOKEN_CODES_H_
