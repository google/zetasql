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

#ifndef ZETASQL_PARSER_MACROS_TOKEN_WITH_LOCATION_H_
#define ZETASQL_PARSER_MACROS_TOKEN_WITH_LOCATION_H_

#include "zetasql/public/parse_location.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {
namespace macros {

using Location = ParseLocationRange;

// Represents one token in the unexpanded input stream. 'Kind' is the Flex token
// kind, e.g. STRING_LITERAL, IDENTIFIER, KW_SELECT.
// Offsets and 'text' refer to the token *without* any whitespaces, consistent
// with Flex.
// Preceding whitespaces are stored in their own string_view; they
// are used when serializing the end result to preserve the original user
// spacing.
struct TokenWithLocation {
  int kind;
  Location location;
  absl::string_view text;
  absl::string_view preceding_whitespaces;

  int start_offset() const { return location.start().GetByteOffset(); }
  int end_offset() const { return location.end().GetByteOffset(); }
};

}  // namespace macros
}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_MACROS_TOKEN_WITH_LOCATION_H_
