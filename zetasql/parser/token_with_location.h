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

#ifndef ZETASQL_PARSER_TOKEN_WITH_LOCATION_H_
#define ZETASQL_PARSER_TOKEN_WITH_LOCATION_H_

#include "zetasql/parser/token_codes.h"
#include "zetasql/public/parse_location.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {

// Represents one token in the unexpanded input stream. 'Kind' is the lexical
// token kind, e.g. STRING_LITERAL, IDENTIFIER, KW_SELECT.
// Offsets and 'text' refer to the token *without* any whitespaces.
// Preceding whitespaces are stored in their own string_view; they
// are used when serializing the end result to preserve the original user
// spacing.
struct TokenWithLocation {
  Token kind;
  ParseLocationRange location;
  absl::string_view text;
  absl::string_view preceding_whitespaces;

  // TEMPORARY FIELD: Please do not use as it will be removed shortly.
  //
  // Location offsets must be valid for the source they refer to.
  // Currently, the parser & analyzer only have the unexpanded source, so we
  // use the unexpanded offset.
  // In the future, the resolver should show the expanded location and where
  // it was expanded from. The expander would have the full location map and
  // the sources of macro definitions as well, so we would not need this
  // adjustment nor the `topmost_invocation_location` at all, since the
  // expander will be able to provide the stack. All layers, however, will need
  // to ask for that mapping.
  ParseLocationRange topmost_invocation_location;

  int start_offset() const { return location.start().GetByteOffset(); }
  int end_offset() const { return location.end().GetByteOffset(); }

  // Returns whether `other` and `this` are adjacent tokens (no spaces in
  // between) and `this` precedes `other`.
  //
  // If the location of either token is invalid, i.e. with one end smaller than
  // 0, the function call returns false.
  bool AdjacentlyPrecedes(const TokenWithLocation& other) const;
};

}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_TOKEN_WITH_LOCATION_H_
