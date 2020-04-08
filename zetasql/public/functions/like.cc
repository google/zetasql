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

#include "zetasql/public/functions/like.h"

#include <stddef.h>
#include <string>

#include "zetasql/base/logging.h"
#include "absl/memory/memory.h"
#include "re2/re2.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

static bool IsRegexSpecialChar(char c) {
  switch (c) {
    case '\\':
    case '.':
    case '*':
    case '?':
    case '+':
    case '^':
    case '$':
    case '|':
    case '(':
    case ')':
    case '[':
    case ']':
    case '{':
    case '}':
      return true;
    default:
      return false;
  }
}

absl::Status CreateLikeRegexpWithOptions(absl::string_view pattern,
                                         const RE2::Options& options,
                                         std::unique_ptr<RE2>* regexp) {
  std::string re_pattern;
  size_t size = pattern.size();
  for (size_t i = 0; i < size; ++i) {
    // We scan the pattern and add it to re_pattern char by char. This
    // works correctly for multibyte characters in UTF-8 encoded strings.
    // Because all their bytes have value greater than 127, they cannot
    // be equal to any characters special-case below and so they get copied
    // to re_pattern verbatim.
    char c = pattern[i];
    switch (c) {
      case '\\':
        if (i + 1 >= size) {
          return absl::Status(absl::StatusCode::kOutOfRange,
                              "LIKE pattern ends with a backslash");
        }
        c = pattern[++i];
        if (IsRegexSpecialChar(c)) {
          re_pattern.push_back('\\');
        }
        re_pattern.push_back(c);
        break;
      case '_':
        re_pattern.append(".");
        break;
      case '%':
        re_pattern.append(".*");
        break;
      default:
        if (IsRegexSpecialChar(c)) {
          re_pattern.push_back('\\');
        }
        re_pattern.push_back(c);
    }
  }

  *regexp = absl::make_unique<RE2>(re_pattern, options);
  if (!(*regexp)->ok()) {
    absl::Status error(absl::StatusCode::kOutOfRange, (*regexp)->error());
    regexp->reset();
    return error;
  }

  return absl::OkStatus();
}

absl::Status CreateLikeRegexp(absl::string_view pattern, TypeKind type,
                              std::unique_ptr<RE2>* regexp) {
  DCHECK(type == TYPE_STRING || type == TYPE_BYTES);
  RE2::Options options;
  options.set_encoding(type == TYPE_STRING ? RE2::Options::EncodingUTF8
                                           : RE2::Options::EncodingLatin1);
  options.set_dot_nl(true);
  return CreateLikeRegexpWithOptions(pattern, options, regexp);
}

}  // namespace functions
}  // namespace zetasql
