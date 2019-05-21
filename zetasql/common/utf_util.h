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
#include "unicode/utf8.h"

namespace zetasql {

// Returns the length of `s` that is well formed UTF8. This will return
// `s.length()` if it is completely well formed UTF8.
absl::string_view::size_type SpanWellFormedUTF8(absl::string_view s);

inline bool IsWellFormedUTF8(absl::string_view s) {
  return SpanWellFormedUTF8(s) == s.length();
}

// Returns a well-formed Unicode std::string. Replaces any ill-formed
// subsequences with the Unicode REPLACEMENT CHARACTER (U+FFFD).
// This is usually rendered as a diamond with a question mark in the middle.
std::string CoerceToWellFormedUTF8(absl::string_view input);

// Truncate the given UTF8 std::string to ensure it is no more than max_bytes.
// If truncated, attempts to create a well formed unicode std::string, and append an
// (ascii) ellipsis.  If max_bytes is < 3, no ellipsis is appended.
std::string PrettyTruncateUTF8(absl::string_view input, int max_bytes);

}  // namespace zetasql

#endif  // ZETASQL_COMMON_UTF_UTIL_H_
