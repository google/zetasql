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

#include "zetasql/parser/macros/flex_token_provider.h"

#include <cstddef>
#include <memory>

#include "zetasql/parser/bison_parser_mode.h"
#include "zetasql/parser/bison_token_codes.h"
#include "zetasql/parser/flex_tokenizer.h"
#include "zetasql/parser/macros/token_with_location.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/base/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace parser {
namespace macros {

static absl::string_view GetTextBetween(absl::string_view input, size_t start,
                                        size_t end) {
  ABSL_DCHECK_LE(start, end);
  ABSL_DCHECK_LE(start, input.length());
  size_t len = end - start;
  ABSL_DCHECK_LE(len, input.length());
  return absl::ClippedSubstr(input, start, len);
}

FlexTokenProvider::FlexTokenProvider(BisonParserMode mode,
                                     absl::string_view filename,
                                     absl::string_view input, int start_offset,
                                     const LanguageOptions& language_options)
    : mode_(mode),
      language_options_(language_options),
      tokenizer_(std::make_unique<ZetaSqlFlexTokenizer>(
          mode, filename, input, start_offset, language_options)),
      location_(ParseLocationPoint::FromByteOffset(filename, -1),
                ParseLocationPoint::FromByteOffset(filename, -1)) {}

absl::StatusOr<TokenWithLocation> FlexTokenProvider::GetFlexToken() {
  int last_token_end_offset = location_.end().GetByteOffset();
  if (last_token_end_offset == -1) {
    last_token_end_offset = 0;
  }

  ZETASQL_ASSIGN_OR_RETURN(int token_kind, tokenizer_->GetNextToken(&location_));

  absl::string_view prev_whitespaces;
  absl::string_view input = tokenizer_->input();
  prev_whitespaces = GetTextBetween(input, last_token_end_offset,
                                    location_.start().GetByteOffset());

  return {
      {token_kind, location_, location_.GetTextFrom(input), prev_whitespaces}};
}

}  // namespace macros
}  // namespace parser
}  // namespace zetasql
