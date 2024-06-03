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

#include <ios>
#include <memory>

#include "zetasql/common/errors.h"
#include "zetasql/parser/flex_istream.h"
#include "zetasql/public/parse_location.h"
#include "absl/flags/flag.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_macros.h"

// TODO: Remove flag when references are gone.
ABSL_FLAG(bool, zetasql_use_customized_flex_istream, true, "Unused");

namespace zetasql {
namespace parser {
// Include the helpful type aliases in the namespace within the C++ file so
// that they are useful for free helper functions as well as class member
// functions.
using TokenKind = int;

absl::StatusOr<TokenKind> ZetaSqlFlexTokenizer::GetNextToken(
    ParseLocationRange* location) {
  TokenKind token = GetNextTokenFlexImpl(location);
  if (!override_error_.ok()) {
    return override_error_;
  }
  return token;
}

ZetaSqlFlexTokenizer::ZetaSqlFlexTokenizer(absl::string_view filename,
                                               absl::string_view input,
                                               bool preserve_comments,
                                               int start_offset)
    : filename_(filename),
      start_offset_(start_offset),
      input_size_(static_cast<int>(input.size())),
      input_stream_(std::make_unique<StringViewStream>(input)),
      preserve_comments_(preserve_comments) {
  // Seek the stringstream to the start_offset, and then instruct flex to read
  // from the stream. (Flex has the ability to read multiple consecutive
  // streams, but we only ever feed it one.)
  input_stream_->seekg(start_offset, std::ios_base::beg);
  switch_streams(/*new_in=*/input_stream_.get(), /*new_out=*/nullptr);
}

void ZetaSqlFlexTokenizer::SetOverrideError(
    const ParseLocationRange& location, absl::string_view error_message) {
  override_error_ = MakeSqlErrorAtPoint(location.start()) << error_message;
}

}  // namespace parser
}  // namespace zetasql
