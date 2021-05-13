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

#include "zetasql/parser/flex_tokenizer.h"

#include "zetasql/parser/bison_parser.bison.h"
#include "zetasql/parser/keywords.h"
#include "zetasql/parser/location.hh"
#include "zetasql/public/parse_location.h"

// TODO: The end state is to turn on everywhere and remove this
// flag. Before that, we'll turn on this feature in test environment and soak
// for a while. Then roll out to Evenflow prod instances and eventually
// deprecate this flag.
ABSL_FLAG(
    bool, zetasql_use_customized_flex_istream, false,
    "If true, use customized StringStreamWithSentinel to read input.");

namespace zetasql {
namespace parser {

absl::Status ZetaSqlFlexTokenizer::GetNextToken(
    ParseLocationRange* location, int* token) {
  zetasql_bison_parser::location bison_location;
  bison_location.begin.column = location->start().GetByteOffset();
  bison_location.end.column = location->end().GetByteOffset();
  *token = GetNextTokenFlex(&bison_location);
  location->set_start(
      ParseLocationPoint::FromByteOffset(filename_,
                                         bison_location.begin.column));
  location->set_end(
      ParseLocationPoint::FromByteOffset(filename_,
                                         bison_location.end.column));
  return override_error_;
}

bool ZetaSqlFlexTokenizer::IsDotGeneralizedIdentifierPrefixToken(
    int bison_token) {
  if (bison_token ==
          zetasql_bison_parser::BisonParserImpl::token::IDENTIFIER ||
      bison_token == ')' || bison_token == ']' || bison_token == '?') {
    return true;
  }
  const KeywordInfo* keyword_info = GetKeywordInfoForBisonToken(bison_token);
  if (keyword_info == nullptr) {
    return false;
  }
  return !keyword_info->IsReserved();
}

}  // namespace parser
}  // namespace zetasql
