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

#include "zetasql/common/unicode_utils.h"

#include <string>

#include "zetasql/base/logging.h"
#include "absl/algorithm/container.h"
#include "absl/flags/flag.h"
#include "absl/log/log.h"
#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "unicode/errorcode.h"
#include "unicode/normalizer2.h"

ABSL_FLAG(bool, zetasql_idstring_allow_unicode_characters, false,
          "Enables case insensitive comparison for idstrings with unicode "
          "characters.");

namespace zetasql {

std::string GetNormalizedAndCasefoldedString(absl::string_view str) {
  if (!absl::GetFlag(FLAGS_zetasql_idstring_allow_unicode_characters) ||
      absl::c_all_of(str, absl::ascii_isascii)) {
    return absl::AsciiStrToLower(str);
  }
  icu::ErrorCode error_code;
  static const icu::Normalizer2* normalizer =
      icu::Normalizer2::getNFKCCasefoldInstance(error_code);
  if (error_code.isFailure()) {
    LOG_EVERY_N_SEC(ERROR, 5)
        << "Failed to get a normalizer instance: " << error_code.errorName()
        << ". Falling back to ASCII lowercase.";
    error_code.reset();
    return absl::AsciiStrToLower(str);
  }
  std::string result;
  result.clear();
  icu::StringByteSink<std::string> sink(&result);
  normalizer->normalizeUTF8(/*options=*/0, str, sink, /*edits=*/nullptr,
                            error_code);
  if (error_code.isFailure()) {
    LOG_EVERY_N_SEC(ERROR, 5)
        << "Failed to normalize input: " << absl::Utf8SafeCHexEscape(str)
        << ". Falling back to ASCII lowercase.";
    result = absl::AsciiStrToLower(str);
    error_code.reset();
  }
  return result;
}

}  // namespace zetasql
