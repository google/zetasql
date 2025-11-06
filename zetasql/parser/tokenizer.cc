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

#include "zetasql/parser/tokenizer.h"

#include "zetasql/parser/tm_lexer.h"
#include "zetasql/parser/tm_token.h"
#include "zetasql/public/parse_location.h"
#include "absl/flags/flag.h"
#include "zetasql/base/check.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_macros.h"

// TODO: Remove flag when references are gone.
ABSL_FLAG(bool, zetasql_use_customized_flex_istream, true, "Unused");

namespace zetasql {
namespace parser {

absl::StatusOr<Token> ZetaSqlTokenizer::GetNextToken(
    ParseLocationRange* location) {
  Token token = Next();
  *location = LastTokenLocationWithStartOffset();
  ZETASQL_RETURN_IF_ERROR(override_error_);
  return token;
}

ZetaSqlTokenizer::ZetaSqlTokenizer(absl::string_view filename,
                                       absl::string_view input,
                                       int start_offset)
    // We do not use Lexer::Rewind() because its time complexity is
    // O(start_offset). See the comment for `Lexer::start_offset_` in
    // zetasql.tm for more information.
    : Lexer(absl::ClippedSubstr(input, start_offset)) {
  filename_ = filename;
  start_offset_ = start_offset;
}

}  // namespace parser
}  // namespace zetasql
