//
// Copyright 2019 ZetaSQL Authors
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

#ifndef ZETASQL_COMMON_UTF_UTIL_H_
#define ZETASQL_COMMON_UTF_UTIL_H_

#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "unicode/utf8.h"

// TODO: Refactor references and add common functions from
// .../public/functions/string.cc. Specific functions are BackwardN, ForwardN,
// and CheckAndCastStrLength.
namespace zetasql {

// Returns the length of `s` that is well formed UTF8. This will return
// `s.length()` if it is completely well formed UTF8.
absl::string_view::size_type SpanWellFormedUTF8(absl::string_view s);

inline bool IsWellFormedUTF8(absl::string_view s) {
  return SpanWellFormedUTF8(s) == s.length();
}

// Returns a well-formed Unicode string. Replaces any ill-formed
// subsequences with the Unicode REPLACEMENT CHARACTER (U+FFFD).
// This is usually rendered as a diamond with a question mark in the middle.
std::string CoerceToWellFormedUTF8(absl::string_view input);

// Truncate the given UTF8 string to ensure it is no more than max_bytes.
// If truncated, attempts to create a well formed unicode string, and append an
// (ascii) ellipsis.  If max_bytes is < 3, no ellipsis is appended.
std::string PrettyTruncateUTF8(absl::string_view input, int max_bytes);

// Verifies that the string length can be represented in a 32-bit signed int and
// returns that value. Fitting in an int32_t is a requirement for icu methods.
ABSL_MUST_USE_RESULT bool CheckAndCastStrLength(absl::string_view str,
                                                int32_t* str_length32);

// Returns the offset needed to forward `str` by `num_code_points` or an empty
// optional if an invalid UTF-8 codepoint is detected.
// Similar to U8_FWD_N, but will detect bad utf codepoints.
absl::optional<int32_t> ForwardN(absl::string_view str, int32_t str_length32,
                               int64_t num_code_points);

}  // namespace zetasql

#endif  // ZETASQL_COMMON_UTF_UTIL_H_
