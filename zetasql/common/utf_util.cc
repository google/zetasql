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

#include "zetasql/common/utf_util.h"

#include "zetasql/base/logging.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "unicode/utf8.h"

namespace zetasql {

constexpr absl::string_view kReplacementCharacter = "\uFFFD";

static int SpanWellFormedUTF8(const char* s, int length) {
  for (int i = 0; i < length;) {
    int start = i;
    UChar32 c;
    U8_NEXT(s, i, length, c);
    if (c < 0) {
      return start;
    }
  }
  return length;
}

absl::string_view::size_type SpanWellFormedUTF8(absl::string_view s) {
  return static_cast<absl::string_view::size_type>(
      SpanWellFormedUTF8(s.data(), static_cast<int>(s.length())));
}

std::string CoerceToWellFormedUTF8(absl::string_view input) {
  const char* s = input.data();
  size_t length = input.length();
  size_t prev = 0;
  std::string out;
  for (size_t i = 0; i < length;) {
    size_t start = i;
    UChar32 c;
    U8_NEXT(s, i, length, c);
    if (c < 0) {
      if (prev < start) {
        // Append the well-formed span between the last ill-formed sequence
        // (or start of input), and the point just before the current one.
        out.append(s + prev, start - prev);
      }
      out.append(kReplacementCharacter.data(), kReplacementCharacter.size());
      prev = i;
    }
  }
  if (prev < length) {
    // Append any remaining well formed span.
    out.append(s + prev, length - prev);
  }
  return out;
}

std::string PrettyTruncateUTF8(absl::string_view input, int max_bytes) {
  if (max_bytes <= 0) {
    return "";
  }
  if (input.size() <= max_bytes) {
    // Already small enough, take no action.
    return std::string(input);
  }
  const bool append_ellipsis = max_bytes > 3;
  int new_width = append_ellipsis ? max_bytes - 3 : max_bytes;

  const uint8_t* str_ptr = reinterpret_cast<const uint8_t*>(input.data());
  // Handles the edge case that a unicode character is cut in half. Finds
  // a safe width that has complete unicode characters. No (further) truncation
  // occurs for totally invalid unicode.
  U8_SET_CP_START(str_ptr, 0, new_width);

  if (append_ellipsis)
    return absl::StrCat(input.substr(0, new_width), "...");
  else
    return std::string(input.substr(0, new_width));
}

}  // namespace zetasql
