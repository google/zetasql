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

#include "zetasql/public/functions/string.h"

#include <stddef.h>
#include <string.h>

#include <algorithm>
#include <cstdlib>
#include <iterator>
#include <limits>
#include <set>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/utf_util.h"
#include "zetasql/public/functions/normalize_mode.pb.h"
#include "zetasql/public/functions/util.h"
#include "zetasql/base/string_numbers.h"
#include "absl/base/casts.h"
#include <cstdint>
#include "absl/base/optimization.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "absl/strings/substitute.h"
#include "unicode/errorcode.h"
#include "unicode/normalizer2.h"
#include "unicode/ucasemap.h"
#include "unicode/uchar.h"
#include "unicode/uniset.h"
#include "unicode/unistr.h"
#include "unicode/utf8.h"
#include "unicode/utypes.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/stl_util.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

namespace {

constexpr absl::string_view kBadUtf8 = "A string is not valid UTF-8.";

constexpr absl::string_view kUtf8ReplacementChar = "\uFFFD";
constexpr UChar32 kUChar32ReplacementChar = 0xfffd;

const size_t kMaxOutputSize = (1 << 20);  // 1MB
const char kExceededPadOutputSize[] =
    "Output of LPAD/RPAD exceeds max allowed output size of 1MB";
const char kExceededRepeatOutputSize[] =
    "Output of REPEAT exceeds max allowed output size of 1MB";
const char kExceededReplaceOutputSize[] =
    "Output of REPLACE exceeds max allowed output size of 1MB";

// Verifies that the string length can be represented in a 32-bit signed int and
// returns that value. Fitting in an int32_t is a requirement for icu methods.
static bool CheckAndCastStrLength(absl::string_view str, int32_t* str_length32,
                                  absl::Status* error) {
  if (str.length() > std::numeric_limits<int32_t>::max()) {
    return internal::UpdateError(
        error,
        absl::Substitute("input string size too large $0", str.length()));
  }
  *str_length32 = static_cast<int32_t>(str.length());
  return true;
}

static int32_t ClampToInt32Max(int64_t i) {
  return i > std::numeric_limits<int32_t>::max()
             ? std::numeric_limits<int32_t>::max()
             : static_cast<int32_t>(i);
}

static bool GlobalStringReplace(absl::string_view s, absl::string_view oldsub,
                                absl::string_view newsub, std::string* res,
                                absl::Status* error) {
  if (oldsub.empty()) {
    if (s.length() > kMaxOutputSize) {
      return internal::UpdateError(error, kExceededReplaceOutputSize);
    }
    res->append(s.data(), s.length());  // If empty, append the given string.
    return true;
  }

  absl::string_view::size_type start_pos = 0;
  while (true) {
    absl::string_view::size_type pos = s.find(oldsub, start_pos);
    if (pos == absl::string_view::npos) {
      break;
    }
    const size_t total_append_size = (pos - start_pos) + newsub.length();
    if (res->size() + total_append_size > kMaxOutputSize) {
      return internal::UpdateError(error, kExceededReplaceOutputSize);
    }
    res->append(s.data() + start_pos, pos - start_pos);
    res->append(newsub.data(), newsub.length());
    // Start searching again after the "old".
    start_pos = pos + oldsub.length();
  }
  const size_t append_size = s.length() - start_pos;
  if (res->size() + append_size > kMaxOutputSize) {
    return internal::UpdateError(error, kExceededReplaceOutputSize);
  }
  res->append(s.data() + start_pos, append_size);
  return true;
}

// Returns an icu::Normalizer2 instance according to the given <normalize_mode>.
// If the <normalize_mode> is not valid or there is any error in getting the
// normalizer instance, returns a nullptr and sets the <error>.
const icu::Normalizer2* GetNormalizerByMode(NormalizeMode normalize_mode,
                                            absl::Status* error) {
  const icu::Normalizer2* normalizer = nullptr;
  icu::ErrorCode icu_errorcode;
  switch (normalize_mode) {
    case NormalizeMode::NFC:
      normalizer = icu::Normalizer2::getNFCInstance(icu_errorcode);
      break;
    case NormalizeMode::NFD:
      normalizer = icu::Normalizer2::getNFDInstance(icu_errorcode);
      break;
    case NormalizeMode::NFKC:
      normalizer = icu::Normalizer2::getNFKCInstance(icu_errorcode);
      break;
    case NormalizeMode::NFKD:
      normalizer = icu::Normalizer2::getNFKDInstance(icu_errorcode);
      break;
    default:
      error->Update(absl::Status(absl::StatusCode::kInvalidArgument,
                                 "A valid normalize mode is required."));
      return nullptr;
  }
  if (icu_errorcode.isFailure()) {
    error->Update(absl::Status(
        absl::StatusCode::kInternal,
        absl::StrCat("Failed to get a normalizer instance with error: ",
                     icu_errorcode.errorName())));
    return nullptr;
  }
  return normalizer;
}

}  // anonymous namespace

bool Utf8Trimmer::Initialize(absl::string_view to_trim, absl::Status* error) {
  int32_t str_length32;
  if (!CheckAndCastStrLength(to_trim, &str_length32, error)) {
    return false;
  }
  unicode_set_ = absl::make_unique<icu::UnicodeSet>();
  has_explicit_replacement_char_ = false;
  int32_t offset = 0;
  while (offset < str_length32) {
    UChar32 character;
    U8_NEXT(to_trim.data(), offset, str_length32, character);
    if (character < 0) {
      return internal::UpdateError(error, kBadUtf8);
    } else {
      unicode_set_->add(character);
    }
    if (character == kUChar32ReplacementChar) {
      has_explicit_replacement_char_ = true;
    }
  }
  unicode_set_->freeze();
  return true;
}

// Implementation of trim left which takes a lambda to compute whether the
// Unicode character should be trimmed.
// Note: UChar32 means "Unicode chararacter 32-bit", and is signed.
static bool TrimLeftImpl(absl::string_view str,
                         const icu::UnicodeSet& unicode_set,
                         absl::string_view* out, absl::Status* error) {
  int32_t str_length32;
  if (!CheckAndCastStrLength(str, &str_length32, error)) {
    return false;
  }
  int32_t prefix_length =
      unicode_set.spanUTF8(str.data(), str_length32, USET_SPAN_CONTAINED);
  *out = str.substr(static_cast<size_t>(prefix_length),
                    static_cast<size_t>(str_length32 - prefix_length));
  return true;
}

static bool TrimRightImpl(absl::string_view str,
                          const icu::UnicodeSet& unicode_set,
                          absl::string_view* out, absl::Status* error) {
  int32_t str_length32;
  if (!CheckAndCastStrLength(str, &str_length32, error)) {
    return false;
  }
  int32_t suffix_start =
      unicode_set.spanBackUTF8(str.data(), str_length32, USET_SPAN_CONTAINED);
  *out = str.substr(0, static_cast<size_t>(suffix_start));
  return true;
}

bool Utf8Trimmer::TrimLeft(absl::string_view str, absl::string_view* out,
                           absl::Status* error) const {
  if (unicode_set_ == nullptr) {
    // Not initialized, no characters are trimmed. Return input.
    *out = str;
    return true;
  }
  if (has_explicit_replacement_char_ && !IsWellFormedUTF8(str)) {
    return internal::UpdateError(error, kBadUtf8);
  }

  return TrimLeftImpl(str, *unicode_set_, out, error);
}

bool Utf8Trimmer::TrimRight(absl::string_view str, absl::string_view* out,
                            absl::Status* error) const {
  if (unicode_set_ == nullptr) {
    // Not initialized, no characters are trimmed. Return input.
    *out = str;
    return true;
  }
  if (has_explicit_replacement_char_ && !IsWellFormedUTF8(str)) {
    return internal::UpdateError(error, kBadUtf8);
  }

  return TrimRightImpl(str, *unicode_set_, out, error);
}

bool Utf8Trimmer::Trim(absl::string_view str, absl::string_view* out,
                       absl::Status* error) const {
  absl::string_view intermediate;
  return TrimLeft(str, &intermediate, error) &&
         TrimRight(intermediate, out, error);
}

void BytesTrimmer::Initialize(absl::string_view to_trim) {
  memset(bytes_to_trim_, 0, sizeof(bytes_to_trim_));
  for (const char ch : to_trim) {
    uint8_t byte = static_cast<uint8_t>(ch);
    bytes_to_trim_[byte] = true;
  }
}

absl::string_view BytesTrimmer::TrimLeft(absl::string_view str) {
  for (absl::string_view::iterator it = str.begin(); it != str.end(); ++it) {
    uint8_t byte = static_cast<uint8_t>(*it);
    if (!bytes_to_trim_[byte]) {
      return absl::string_view(it, str.end() - it);
    }
  }
  // Everything got trimmed. Return an empty string.
  return "";
}

absl::string_view BytesTrimmer::TrimRight(absl::string_view str) {
  for (absl::string_view::reverse_iterator it = str.rbegin(); it != str.rend();
       ++it) {
    uint8_t byte = static_cast<uint8_t>(*it);
    if (!bytes_to_trim_[byte]) {
      return absl::string_view(str.data(), it.base() - str.begin());
    }
  }
  // Everything got trimmed. Return an empty string.
  return "";
}

absl::string_view BytesTrimmer::Trim(absl::string_view str) {
  return TrimLeft(TrimRight(str));
}

bool StrposUtf8(absl::string_view str, absl::string_view substr, int64_t* out,
                absl::Status* error) {
  absl::string_view::size_type pos = str.find(substr);
  if (pos == absl::string_view::npos) {
    *out = 0;
  } else {
    str = str.substr(0, pos);
    if (!LengthUtf8(str, out, error)) return false;
    (*out)++;
  }
  return true;
}

bool StrposBytes(absl::string_view str, absl::string_view substr, int64_t* out,
                 absl::Status* error) {
  absl::string_view::size_type pos = str.find(substr);
  if (pos == absl::string_view::npos) {
    *out = 0;
  } else {
    *out = pos + 1;
  }
  return true;
}

bool LengthUtf8(absl::string_view str, int64_t* out, absl::Status* error) {
  int32_t str_length32;
  if (!CheckAndCastStrLength(str, &str_length32, error)) {
    return false;
  }

  int utf8_length = 0;
  int32_t offset = 0;
  while (offset < str_length32) {
    UChar32 character;
    U8_NEXT(str.data(), offset, str_length32, character);
    if (character < 0) {
      return internal::UpdateError(error, kBadUtf8);
    }
    utf8_length++;
  }
  *out = utf8_length;
  return true;
}

bool LengthBytes(absl::string_view str, int64_t* out, absl::Status* error) {
  *out = str.length();
  return true;
}

bool StartsWithUtf8(absl::string_view str, absl::string_view substr, bool* out,
                    absl::Status* error) {
  *out = absl::StartsWith(str, substr);
  return true;
}

bool StartsWithBytes(absl::string_view str, absl::string_view substr, bool* out,
                     absl::Status* error) {
  *out = absl::StartsWith(str, substr);
  return true;
}

bool EndsWithUtf8(absl::string_view str, absl::string_view substr, bool* out,
                  absl::Status* error) {
  *out = absl::EndsWith(str, substr);
  return true;
}

bool EndsWithBytes(absl::string_view str, absl::string_view substr, bool* out,
                   absl::Status* error) {
  *out = absl::EndsWith(str, substr);
  return true;
}

bool TrimSpacesUtf8(absl::string_view str, absl::string_view* out,
                    absl::Status* error) {
  absl::string_view intermediate;
  return LeftTrimSpacesUtf8(str, &intermediate, error) &&
         RightTrimSpacesUtf8(intermediate, out, error);
}

bool LeftTrimSpacesUtf8(absl::string_view str, absl::string_view* out,
                        absl::Status* error) {
  icu::ErrorCode cannot_fail;
  const icu::UnicodeSet* whitespace_unicode_set = icu::UnicodeSet::fromUSet(
      u_getBinaryPropertySet(UCHAR_WHITE_SPACE, cannot_fail));
  return TrimLeftImpl(str, *whitespace_unicode_set, out, error);
}

bool RightTrimSpacesUtf8(absl::string_view str, absl::string_view* out,
                         absl::Status* error) {
  icu::ErrorCode cannot_fail;
  const icu::UnicodeSet* whitespace_unicode_set = icu::UnicodeSet::fromUSet(
      u_getBinaryPropertySet(UCHAR_WHITE_SPACE, cannot_fail));
  return TrimRightImpl(str, *whitespace_unicode_set, out, error);
}

bool TrimUtf8(absl::string_view str, absl::string_view chars,
              absl::string_view* out, absl::Status* error) {
  Utf8Trimmer trimmer;
  if (!trimmer.Initialize(chars, error)) return false;
  return trimmer.Trim(str, out, error);
}

bool LeftTrimUtf8(absl::string_view str, absl::string_view chars,
                  absl::string_view* out, absl::Status* error) {
  Utf8Trimmer trimmer;
  if (!trimmer.Initialize(chars, error)) return false;
  return trimmer.TrimLeft(str, out, error);
}

bool RightTrimUtf8(absl::string_view str, absl::string_view chars,
                   absl::string_view* out, absl::Status* error) {
  Utf8Trimmer trimmer;
  if (!trimmer.Initialize(chars, error)) return false;
  return trimmer.TrimRight(str, out, error);
}

bool TrimBytes(absl::string_view str, absl::string_view chars,
               absl::string_view* out, absl::Status* error) {
  BytesTrimmer trimmer;
  trimmer.Initialize(chars);
  *out = trimmer.TrimLeft(trimmer.TrimRight(str));
  return true;
}

bool LeftTrimBytes(absl::string_view str, absl::string_view chars,
                   absl::string_view* out, absl::Status* error) {
  BytesTrimmer trimmer;
  trimmer.Initialize(chars);
  *out = trimmer.TrimLeft(str);
  return true;
}

bool RightTrimBytes(absl::string_view str, absl::string_view chars,
                    absl::string_view* out, absl::Status* error) {
  BytesTrimmer trimmer;
  trimmer.Initialize(chars);
  *out = trimmer.TrimRight(str);
  return true;
}

bool LeftUtf8(absl::string_view str, int64_t length,
              absl::string_view* out, absl::Status* error) {
  if (length < 0) {
    return internal::UpdateError(
        error, "Second argument in LEFT() cannot be negative");
  }
  return SubstrWithLengthUtf8(str, 0, length, out, error);
}

bool LeftBytes(absl::string_view str, int64_t length, absl::string_view* out,
               absl::Status* error) {
  if (length < 0) {
    return internal::UpdateError(
        error, "Second argument in LEFT() cannot be negative");
  }
  return SubstrWithLengthBytes(str, 0, length, out, error);
}

bool RightUtf8(absl::string_view str, int64_t length,
              absl::string_view* out, absl::Status* error) {
  if (length < 0) {
    return internal::UpdateError(
        error, "Second argument in RIGHT() cannot be negative");
  }
  return SubstrWithLengthUtf8(str, -length, length, out, error);
}

bool RightBytes(absl::string_view str, int64_t length, absl::string_view* out,
               absl::Status* error) {
  if (length < 0) {
    return internal::UpdateError(
        error, "Second argument in RIGHT() cannot be negative");
  }
  return SubstrWithLengthBytes(str, -length, length, out, error);
}

bool SubstrUtf8(absl::string_view str, int64_t pos, absl::string_view* out,
                absl::Status* error) {
  return SubstrWithLengthUtf8(str, pos, std::numeric_limits<int64_t>::max(), out,
                              error);
}

// Move forward `num_code_points` in str, by updating `str_offset`.
// Similar to U8_FWD_N, but will detect bad utf codepoints.
static bool ForwardN(const char* str, int32_t str_length32, int64_t num_code_points,
                     int32_t* str_offset, absl::Status* error) {
  for (int64_t i = 0; i < num_code_points && *str_offset < str_length32; ++i) {
    UChar32 character;
    U8_NEXT(str, *str_offset, str_length32, character);
    if (character < 0) {
      return internal::UpdateError(error, kBadUtf8);
    }
  }
  return true;
}

// Similar to U8_BACK_N, but will detect bad utf codepoints.
static bool BackN(const char* str, int64_t num_code_points, int32_t* str_offset,
                  absl::Status* error) {
  for (int64_t i = 0; i<num_code_points&& * str_offset> 0; ++i) {
    UChar32 character;
    U8_PREV(str, 0, *str_offset, character);

    if (character < 0) {
      return internal::UpdateError(error, kBadUtf8);
    }
    if (*str_offset == 0) {
      // Hit the front of the string, give up.
      break;
    }
  }
  return true;
}

// This function handles SUBSTR(str, pos, length) where pos is negative.
// In this case pos identifies a character counting from the end of the string.
static bool SubstrSuffixUtf8(absl::string_view str, int64_t pos, int64_t length,
                             absl::string_view* out, absl::Status* error) {
  DCHECK_LT(pos, 0);
  int32_t str_length32;
  if (!CheckAndCastStrLength(str, &str_length32, error)) {
    return false;
  }

  // suffix_length is the number of characters that follow resulting
  // substring. We don't know the full length of str here, and
  // -pos can be greater than the length, so here we just estimate it
  // assuming that -pos <= length(str).
  int64_t suffix_length;
  if (pos + length >= 0) {
    suffix_length = 0;
    length = -pos;
  } else {
    suffix_length = -(pos + length);
  }
  const int32_t suffix_length32 = ClampToInt32Max(suffix_length);

  int32_t suffix_end_offset = str_length32;
  // Walk the string backwards from the end counting suffix_length characters
  // to find the end of the substring.
  if (!BackN(str.data(), suffix_length32, &suffix_end_offset, error)) {
    return false;
  }
  // At this point, suffix_end_offset is just a guess, we'll fix it later.
  int32_t suffix_start_offset = suffix_end_offset;

  while (length > 0 && suffix_start_offset > 0) {
    if (!BackN(str.data(), 1, &suffix_start_offset, error)) {
      return false;
    }
    length--;
  }

  if (length > 0) {
    const int32_t length32 = ClampToInt32Max(length);
    // Hit the start of the string, so we need to fix our estimated end point
    // by pushing it out the remaining length.
    if (!ForwardN(str.data(), str_length32, length32, &suffix_end_offset,
                  error)) {
      return false;
    }
  }
  // Cast guaranteed safe
  *out =
      str.substr(static_cast<size_t>(suffix_start_offset),
                 static_cast<size_t>(suffix_end_offset - suffix_start_offset));
  return true;
}

bool SubstrWithLengthUtf8(absl::string_view str, int64_t pos, int64_t length,
                          absl::string_view* out, absl::Status* error) {
  if (length < 0) {
    return internal::UpdateError(
        error, "Third argument in SUBSTR() cannot be negative");
  }
  if (pos < 0) {
    if (pos < std::numeric_limits<int32_t>::lowest() || -pos > str.length()) {
      // If position is more negative than the length of the string, then
      // we short circuit actual suffix computation and just start at 0.
      pos = 0;
    } else {
      return SubstrSuffixUtf8(str, pos, length, out, error);
    }
  } else if (pos > 0) {
    // pos is 1-based.
    pos -= 1;
  }
  int32_t str_length32;
  if (!CheckAndCastStrLength(str, &str_length32, error)) {
    return false;
  }

  const int32_t length32 = ClampToInt32Max(length);

  int32_t start_offset = 0;
  if (pos >= str_length32) {
    // Conceptually, U8_FWD_N should perform this check itself, but it
    // explicitly documents that pos must be < str_length32 as a precondition.
    *out = absl::string_view("", 0);
    return true;
  }
  if (!ForwardN(str.data(), str_length32, pos, &start_offset, error)) {
    return false;
  }
  // offset is now at the start we might need to truncate.
  if (start_offset >= str_length32) {
    *out = absl::string_view("", 0);
    return true;
  }

  // Cast of start_offset is safe; we've guaranteed start_offset < str_length32.
  if (str_length32 - static_cast<int32_t>(start_offset) < length) {
    // Shortcut: length is definitely longer than the substring after start.
    // Return the entire suffix.
    *out = str.substr(start_offset);
  } else {
    int32_t end_offset = start_offset;
    if (!ForwardN(str.data(), str_length32, length32, &end_offset, error)) {
      return false;
    }
    *out = str.substr(start_offset, end_offset - start_offset);
  }

  return true;
}

bool SubstrBytes(absl::string_view str, int64_t pos, absl::string_view* out,
                 absl::Status* error) {
  return SubstrWithLengthBytes(str, pos, std::numeric_limits<int64_t>::max(), out,
                               error);
}

bool SubstrWithLengthBytes(absl::string_view str, int64_t pos, int64_t length,
                           absl::string_view* out, absl::Status* error) {
  if (length < 0) {
    return internal::UpdateError(
        error, "Third argument in SUBSTR() cannot be negative");
  }
  int32_t str_length32;
  if (!CheckAndCastStrLength(str, &str_length32, error)) {
    return false;
  }
  if (pos < 0) {
    pos += str.length();  // cannot overflow (just barely).
    if (pos < 0) pos = 0;
  } else if (pos > 0) {
    pos -= 1;
    if (pos > static_cast<int64_t>(str.length())) {
      // Pos is beyond the end of the string. Return an empty string.
      *out = absl::string_view("", 0);
      return true;
    }
  }
  *out = absl::ClippedSubstr(str, pos, length);
  return true;
}

// UPPER(STRING) -> STRING
bool UpperUtf8(absl::string_view str, std::string* out, absl::Status* error) {
  int32_t str_length32;  // unused
  if (!CheckAndCastStrLength(str, &str_length32, error)) {
    return false;
  }
  out->clear();
  out->reserve(str.length());

  icu::ErrorCode status;
  icu::StringByteSink<std::string> icu_out(out);
  icu::CaseMap::utf8ToUpper("" /* root locale */, 0 /* default options */, str,
                            icu_out, nullptr /* edits - unused */, status);
  if (status.isFailure()) {
    error->Update(
        absl::Status(absl::StatusCode::kInternal,
                     absl::StrCat("icu::CaseMap::utf8ToUpper error: %s",
                                  status.errorName())));
    status.reset();
    return false;
  }
  return true;
}

// LOWER(STRING) -> STRING
bool LowerUtf8(absl::string_view str, std::string* out, absl::Status* error) {
  int32_t str_length32;  // unused
  if (!CheckAndCastStrLength(str, &str_length32, error)) {
    return false;
  }
  out->clear();
  out->reserve(str.length());

  icu::ErrorCode icu_status;
  icu::StringByteSink<std::string> icu_out(out);
  icu::CaseMap::utf8ToLower("" /* root locale */, 0 /* default options */, str,
                            icu_out, nullptr /* edits - unused */, icu_status);
  if (icu_status.isFailure()) {
    error->Update(
        absl::Status(absl::StatusCode::kInternal,
                     absl::StrCat("icu::CaseMap::utf8ToUpper error: %s",
                                  icu_status.errorName())));
    icu_status.reset();
    return false;
  }
  return true;
}

bool UpperBytes(absl::string_view str, std::string* out, absl::Status* error) {
  out->resize(str.size());
  for (int i = 0; i < str.size(); ++i) {
    (*out)[i] = absl::ascii_toupper(str[i]);
  }
  return true;
}

bool LowerBytes(absl::string_view str, std::string* out, absl::Status* error) {
  out->resize(str.size());
  for (int i = 0; i < str.size(); ++i) {
    (*out)[i] = absl::ascii_tolower(str[i]);
  }
  return true;
}

// REPLACE(STRING, STRING, STRING) -> STRING
bool ReplaceUtf8(absl::string_view str, absl::string_view oldsub,
                 absl::string_view newsub, std::string* out,
                 absl::Status* error) {
  out->clear();
  return GlobalStringReplace(str, oldsub, newsub, out, error);
}

// REPLACE(BYTES, BYTES, BYTES) -> BYTES
bool ReplaceBytes(absl::string_view str, absl::string_view oldsub,
                  absl::string_view newsub, std::string* out,
                  absl::Status* error) {
  out->clear();
  return GlobalStringReplace(str, oldsub, newsub, out, error);
}

template <class Container>
bool SplitUtf8Impl(absl::string_view str, absl::string_view delimiter,
                   Container* out, absl::Status* error) {
  if (delimiter.empty()) {
    out->clear();
    if (str.empty()) {
      out->push_back("");
      return true;
    }
    size_t offset = 0;
    const size_t length = str.length();

    while (offset < length) {
      const size_t prev = offset;
      UChar32 character;
      U8_NEXT(str.data(), offset, length, character);
      if (character < 0) {
        return internal::UpdateError(error, kBadUtf8);
      } else {
        out->emplace_back(str.substr(prev, offset - prev));
      }
    }

    return true;
  }

  if (!IsWellFormedUTF8(delimiter)) {
    return internal::UpdateError(
        error, "Delimiter in SPLIT function is not a valid UTF-8 string");
  }
  // If str is valid UTF8 string, then regular absl::StrSplit is guaranteed to
  // produce correct results, since no valid UTF8 sequence is ever a prefix of
  // another valid UTF8 sequence (one of fundamental UTF8 design points).
  // Special case empty StringView, because absl::StrSplit behaves differently
  // on StringView() vs. StringView("", 0)
  if (ABSL_PREDICT_FALSE(str.empty())) {
    str = absl::string_view("", 0);
  }
  *out = absl::StrSplit(str, delimiter);
  return true;
}

bool SplitUtf8(absl::string_view str, absl::string_view delimiter,
               std::vector<std::string>* out, absl::Status* error) {
  return SplitUtf8Impl(str, delimiter, out, error);
}

bool SplitUtf8(absl::string_view str, absl::string_view delimiter,
               std::vector<absl::string_view>* out, absl::Status* error) {
  return SplitUtf8Impl(str, delimiter, out, error);
}

bool SplitBytes(absl::string_view str, absl::string_view delimiter,
                std::vector<std::string>* out, absl::Status* error) {
  // Special case empty StringView, because absl::StrSplit behaves differently
  // on StringView() vs. StringView("", 0)
  if (ABSL_PREDICT_FALSE(str.empty())) {
    str = absl::string_view("", 0);
  }
  *out = absl::StrSplit(str, delimiter);
  return true;
}
bool SplitBytes(absl::string_view str, absl::string_view delimiter,
                std::vector<absl::string_view>* out, absl::Status* error) {
  // Special case empty StringView, because absl::StrSplit behaves differently
  // on StringView() vs. StringView("", 0)
  if (ABSL_PREDICT_FALSE(str.empty())) {
    str = absl::string_view("", 0);
  }
  *out = absl::StrSplit(str, delimiter);
  return true;
}

bool SafeConvertBytes(absl::string_view str, std::string* out,
                      absl::Status* error) {
  // Note, this implementation is _nearly_ identical to CoerceToWellFormedUTF8
  // (from utf_util), but it replaces _every byte_ with "\uFFFD" instead of each
  // almost-Unicode sequence.
  // If the 'str' contains only valid UTF-8 chars, simply copies it to 'out',
  // then returns. Otherwise, replaces the invalid UTF-8 chars with U+FFFD.
  const char* str_data = str.data();
  const size_t length = str.length();
  size_t prev = 0;
  for (size_t i = 0; i < length; /* U8_NEXT increments i */) {
    size_t start = i;
    UChar32 character;
    U8_NEXT(str_data, i, length, character);
    if (character < 0) {
      if (prev == 0) {
        // This was the first time we found a bad character, do some
        // initialization.
        // Determines the max size of the output after invalid char
        // replacements. In worst case, every char is invalid and replaced by
        // U+FFFD.
        size_t max_out_buf_size =
            prev + (length - prev) * kUtf8ReplacementChar.length();
        out->clear();
        out->reserve(max_out_buf_size);
      }
      if (prev < start) {
        // Append the well-formed span between the last ill-formed sequence
        // (or start of input), and the point just before the current one.
        out->append(str_data + prev, start - prev);
      }
      for (size_t j = start; j < i; ++j) {
        out->append(kUtf8ReplacementChar.data(), kUtf8ReplacementChar.size());
      }
      prev = i;
    }
  }
  if (prev == 0) {
    // str was actually just fine, use more efficient 'assign' call.
    out->assign(str.data(), str.size());
  } else if (prev < length) {
    // Append any remaining well formed span.
    out->append(str_data + prev, length - prev);
  }

  return true;
}

bool Normalize(absl::string_view str, NormalizeMode mode, bool is_casefold,
               std::string* out, absl::Status* error) {
  const icu::Normalizer2* normalizer = GetNormalizerByMode(mode, error);
  if (!error->ok()) {
    return false;
  }
  icu::ErrorCode icu_errorcode;
  icu::UnicodeString unicode_str;
  normalizer->normalize(icu::UnicodeString::fromUTF8(str), unicode_str,
                        icu_errorcode);
  if (icu_errorcode.isFailure()) {
    return internal::UpdateError(
        error, absl::StrCat("Failed to normalize string with error: ",
                            icu_errorcode.errorName()));
  }
  if (is_casefold) {
    unicode_str.foldCase();  // Uses default U_FOLD_CASE_DEFAULT option.
  }
  out->clear();
  unicode_str.toUTF8String(*out);
  return true;
}

bool ToBase64(absl::string_view str, std::string* out, absl::Status* error) {
  absl::Base64Escape(str, out);
  return true;
}

bool FromBase64(absl::string_view str, std::string* out, absl::Status* error) {
  if (!absl::Base64Unescape(str, out)) {
    return internal::UpdateError(error,
                                 "Failed to decode invalid base64 string");
  }
  return true;
}

bool ToHex(absl::string_view str, std::string* out, absl::Status* error) {
  *out = absl::BytesToHexString(str);
  return true;
}

bool FromHex(absl::string_view str, std::string* out, absl::Status* error) {
  if (str.empty()) {
    out->clear();
    return true;
  }
  int offset = 0;
  for (char c : str) {
    if (!absl::ascii_isxdigit(c)) {
      return internal::UpdateError(
          error, absl::Substitute("Failed to decode invalid hexadecimal "
                                  "string due to character at offset $0: $1",
                                  offset, str));
    }
    ++offset;
  }
  // Account for strings with an odd number of hex digits.
  if (str.size() % 2 == 1) {
    zetasql_base::STLStringResizeUninitialized(out, (str.size() + 1) / 2);
    char* string_ptr = &(*out)[0];
    *string_ptr = zetasql_base::hex_digit_to_int(str[0]);
    ++string_ptr;
    str.remove_prefix(1);
    std::string tmp = absl::HexStringToBytes(str);
    out->replace(1, tmp.size(), tmp);
  } else {
    *out = absl::HexStringToBytes(str);
  }
  return true;
}

bool StringToCodePoints(absl::string_view str, std::vector<int64_t>* out,
                        absl::Status* error) {
  int32_t str_length32;
  if (!CheckAndCastStrLength(str, &str_length32, error)) {
    return false;
  }

  out->clear();
  for (size_t i = 0; i < str_length32; /* U8_NEXT increments i */) {
    UChar32 character;
    U8_NEXT(str.data(), i, str_length32, character);
    if (character < 0) {
      return internal::UpdateError(
          error, absl::Substitute("Argument to TO_CODE_POINTS is not a "
                                  "structurally valid UTF-8 string: '$0'",
                                  str));
    }
    out->push_back(character);
  }
  return true;
}

bool BytesToCodePoints(absl::string_view str, std::vector<int64_t>* out,
                       absl::Status* error) {
  out->clear();
  for (char c : str) {
    // Represent ASCII values in the range [0, 255].
    const unsigned char ascii_value = absl::bit_cast<unsigned char>(c);
    out->push_back(ascii_value);
  }
  return true;
}

bool CodePointsToString(const std::vector<int64_t>& codepoints, std::string* out,
                        absl::Status* error) {
  out->clear();
  if (codepoints.empty()) {
    return true;
  }
  // This method will perform either 1 or 2 resizes. The first resize assumes
  // we can encode each codepoint as a single byte, with a little extra buffer
  // to simplify the later resize logic.
  // The 2nd resize will occur if the above assumption is wrong, and will just
  // jump straight to the most pessimistic allocation (that every remaining
  // codepoint is 4 bytes).
  //
  // Start by assuming minimal sized (1-byte) codepoints, plus 3 (because we
  // want to guarantee each call has minimum capacity for 4 bytes (the max
  // utf8 byte counts).
  zetasql_base::STLStringResizeUninitialized(out, codepoints.size() + U8_MAX_LENGTH - 1);
  size_t out_offset = 0;
  for (size_t i = 0; i < codepoints.size(); ++i) {
    // We use uint32_t instead of UChar32 because, for some reason, U8_APPEND
    // expects it (unlike every other `U8_` function).
    int64_t codepoint = codepoints[i];
    if (codepoint < 0 || codepoint > std::numeric_limits<uint32_t>::max()) {
      return internal::UpdateError(
          error, absl::Substitute("Invalid codepoint $0", codepoint));
    }
    uint32_t codepoint32 = static_cast<uint32_t>(codepoint);
    size_t remaining_capacity = out->size() - out_offset;
    if (remaining_capacity < U8_MAX_LENGTH) {
      // We want to guarantee we have enough headroom to avoid additional
      // allocations.
      out->resize(out_offset + (codepoints.size() - i) * U8_MAX_LENGTH);
    }
    bool is_error = false;
    U8_APPEND(out->data(), out_offset, out->size(), codepoint32, is_error);
    if (is_error) {
      out->clear();
      return internal::UpdateError(
          error, absl::Substitute("Invalid codepoint $0", codepoint));
    }
  }
  // out_offset will be exactly the size we need.
  out->resize(out_offset);
  return true;
}

bool CodePointsToBytes(const std::vector<int64_t>& codepoints, std::string* out,
                       absl::Status* error) {
  out->clear();
  for (int64_t codepoint : codepoints) {
    if (codepoint < 0 ||
        codepoint > std::numeric_limits<unsigned char>::max()) {
      return internal::UpdateError(
          error, absl::Substitute("Invalid ASCII value $0", codepoint));
    }
    out->push_back(codepoint);
  }
  return true;
}

// Helper function namespace.
namespace {

// Helper function for all padding functions.
bool VerifyPadInputs(absl::string_view input_str, absl::string_view pattern,
                     int64_t output_size, absl::Status* error) {
  if (output_size < 0) {
    return internal::UpdateError(
        error,
        "Second argument (output size) for LPAD/RPAD cannot be negative");
  } else if (output_size > kMaxOutputSize) {
    return internal::UpdateError(error, kExceededPadOutputSize);
  } else if (pattern.empty()) {
    return internal::UpdateError(
        error, "Third argument (pad pattern) for LPAD/RPAD cannot be empty");
  }
  return true;
}

// Helper function for RightPadBytes and LeftPadBytes
bool PadBytes(absl::string_view input_str, int64_t output_size_bytes,
              absl::string_view pattern, bool left_pad, std::string* out,
              absl::Status* error) {
  if (!VerifyPadInputs(input_str, pattern, output_size_bytes, error)) {
    return false;
  }
  if (output_size_bytes <= static_cast<int64_t>(input_str.length())) {
    out->assign(input_str.data(), output_size_bytes);
    return true;
  }

  out->clear();
  out->reserve(output_size_bytes);
  if (!left_pad) {
    // Fill input_str.
    absl::StrAppend(out, input_str);
  }

  // Fill pattern.
  const int64_t pad_size_bytes = output_size_bytes - input_str.length();
  if (pattern.length() == 1) {
    out->append(pad_size_bytes, pattern[0]);
  } else {
    auto div_result =
        std::div(pad_size_bytes, static_cast<int64_t>(pattern.length()));
    for (int64_t i = 0; i < div_result.quot; ++i) {
      absl::StrAppend(out, pattern);
    }
    absl::StrAppend(out, pattern.substr(0, div_result.rem));
  }

  if (left_pad) {
    // Fill input_str.
    absl::StrAppend(out, input_str);
  }
  return true;
}

// Helper function for RightPadUtf8 and LeftPadUtf8.
bool PadUtf8(absl::string_view input_str, int64_t output_size_chars,
             absl::string_view pattern, bool left_pad, std::string* out,
             absl::Status* error) {
  if (!VerifyPadInputs(input_str, pattern, output_size_chars, error)) {
    return false;
  }

  if (!IsWellFormedUTF8(input_str)) {
    return internal::UpdateError(error, kBadUtf8);
  }

  int64_t input_size_chars = 0;
  LengthUtf8(input_str, &input_size_chars, error);

  if (output_size_chars <= input_size_chars) {
    absl::string_view input_str_prefix;
    SubstrWithLengthUtf8(input_str, 0, output_size_chars, &input_str_prefix,
                         error);
    if (input_str_prefix.length() > kMaxOutputSize) {
      return internal::UpdateError(error, kExceededPadOutputSize);
    }
    out->assign(input_str_prefix.data(), input_str_prefix.length());
    return true;
  }

  if (!IsWellFormedUTF8(pattern)) {
    return internal::UpdateError(error, kBadUtf8);
  }

  int64_t pattern_size_chars = 1;
  if (pattern.length() > 1) {
    LengthUtf8(pattern, &pattern_size_chars, error);
  }
  auto padding_div =
      std::div(output_size_chars - input_size_chars, pattern_size_chars);
  absl::string_view pattern_prefix;

  SubstrWithLengthUtf8(pattern, 0, padding_div.rem, &pattern_prefix, error);
  // To compute output_size_bytes correctly in all cases we need
  // 'lg(kMaxOutputSize) +  3' bits. If kMaxOutputSize is changed ensure that
  // 'lg(kMaxOutputSize) + 3 <= 64'.
  const size_t output_size_bytes = (padding_div.quot * pattern.length()) +
                                   pattern_prefix.length() + input_str.length();
  if (output_size_bytes > kMaxOutputSize) {
    return internal::UpdateError(error, kExceededPadOutputSize);
  }

  out->clear();
  out->reserve(output_size_bytes);
  int64_t rep_count = padding_div.quot;

  if (!left_pad) {
    // Fill input_str.
    absl::StrAppend(out, input_str);
  }

  // Fill pattern.
  if (pattern.length() == 1) {
    out->append(rep_count, pattern[0]);
  } else {
    for (int64_t i = 0; i < rep_count; ++i) {
      absl::StrAppend(out, pattern);
    }
    absl::StrAppend(out, pattern_prefix);
  }

  if (left_pad) {
    // Fill input_str.
    absl::StrAppend(out, input_str);
  }

  return true;
}

}  // namespace

bool LeftPadBytes(absl::string_view input_str, int64_t output_size_bytes,
                  absl::string_view pattern, std::string* out,
                  absl::Status* error) {
  return PadBytes(input_str, output_size_bytes, pattern, true /* left_pad */,
                  out, error);
}

bool LeftPadBytesDefault(absl::string_view input_str, int64_t output_size_bytes,
                         std::string* out, absl::Status* error) {
  return PadBytes(input_str, output_size_bytes, " ", true /* left_pad */, out,
                  error);
}

bool LeftPadUtf8(absl::string_view input_str, int64_t output_size_chars,
                 absl::string_view pattern, std::string* out,
                 absl::Status* error) {
  return PadUtf8(input_str, output_size_chars, pattern, true /* left_pad */,
                 out, error);
}

bool LeftPadUtf8Default(absl::string_view input_str, int64_t output_size_chars,
                        std::string* out, absl::Status* error) {
  return PadUtf8(input_str, output_size_chars, " ", true /* left_pad */, out,
                 error);
}

bool RightPadBytes(absl::string_view input_str, int64_t output_size_bytes,
                   absl::string_view pattern, std::string* out,
                   absl::Status* error) {
  return PadBytes(input_str, output_size_bytes, pattern, false /* left_pad */,
                  out, error);
}

bool RightPadBytesDefault(absl::string_view input_str, int64_t output_size_bytes,
                          std::string* out, absl::Status* error) {
  return PadBytes(input_str, output_size_bytes, " ", false /* left_pad */, out,
                  error);
}

bool RightPadUtf8(absl::string_view input_str, int64_t output_size_chars,
                  absl::string_view pattern, std::string* out,
                  absl::Status* error) {
  return PadUtf8(input_str, output_size_chars, pattern, false /* left_pad */,
                 out, error);
}

bool RightPadUtf8Default(absl::string_view input_str, int64_t output_size_chars,
                         std::string* out, absl::Status* error) {
  return PadUtf8(input_str, output_size_chars, " ", false /* left_pad */, out,
                 error);
}

bool Repeat(absl::string_view input_str, int64_t repeat_count, std::string* out,
            absl::Status* error) {
  if (repeat_count < 0) {
    return internal::UpdateError(
        error, "Second argument (repeat count) for REPEAT cannot be negative");
  }

  out->clear();
  if (input_str.length() > 0) {
    size_t rep_mem_bytes = 0;
    if (input_str.length() > kMaxOutputSize || repeat_count > kMaxOutputSize ||
        (rep_mem_bytes = repeat_count * input_str.length()) > kMaxOutputSize) {
      // repeat_count * input_str.length() should not overflow as it needs
      // atmost 60 bits.
      return internal::UpdateError(error, kExceededRepeatOutputSize);
    }
    out->reserve(rep_mem_bytes);
    for (int64_t i = 0; i < repeat_count; ++i) {
      absl::StrAppend(out, input_str);
    }
  }
  return true;
}

bool ReverseBytes(absl::string_view input, std::string* out,
                  absl::Status* error) {
  out->assign(input.rbegin(), input.rend());
  return true;
}

bool ReverseUtf8(absl::string_view input, std::string* out,
                 absl::Status* error) {
  int32_t str_length32;
  if (!CheckAndCastStrLength(input, &str_length32, error)) {
    return false;
  }

  out->clear();
  out->reserve(input.size());

  int32_t offset = str_length32;  // start at the end
  while (offset > 0) {
    int32_t prev_offset = offset;
    UChar32 character;
    U8_PREV(input.data(), 0, offset, character);
    if (character < 0) {
      return internal::UpdateError(
          error, absl::Substitute("Argument to REVERSE is not a structurally "
                                  "valid UTF-8 string: '$0'",
                                  input));
    }
    out->append(input.begin() + offset, input.begin() + prev_offset);
  }
  return true;
}

LikeRewriteType GetRewriteForLikePattern(bool is_string,
                                         absl::string_view pattern,
                                         absl::string_view* substring) {
  // Don't attempt to rewrite LIKE where the pattern contains \ or _, the former
  // of which is an escape character and the latter of which is a wildcard. Also
  // don't attempt to rewrite LIKE where a % appears somewhere other than the
  // beginning or end.
  bool starts_with_percent = false;
  bool ends_with_percent = false;
  if (!is_string) {
    while (absl::ConsumePrefix(&pattern, "%")) {
      starts_with_percent = true;
    }
    while (absl::ConsumeSuffix(&pattern, "%")) {
      ends_with_percent = true;
    }

    for (char c : pattern) {
      switch (c) {
        case '\\':
        case '_':
        case '%':
          return LikeRewriteType::kNoRewrite;
        default:
          break;
      }
    }
  } else {
    // With UTF-8, some characters may be multiple bytes, so we instead need to
    // scan code points.
    if (!IsWellFormedUTF8(pattern)) {
      return LikeRewriteType::kNoRewrite;
    }
    while (!pattern.empty() && pattern[0] == '%' &&
           absl::ConsumePrefix(&pattern, "%")) {
      starts_with_percent = true;
    }

    absl::string_view::size_type pos = 0;
    while (pos < static_cast<int64_t>(pattern.size())) {
      absl::string_view::size_type next_pos = pos;
      // We've already tested this to be well formed; so we use the 'UNSAFE'
      // variant (it may have been modified by 'ConsumePrefix', but this is a
      // safe mutation).
      U8_FWD_1_UNSAFE(pattern.data(), next_pos);

      const absl::string_view::size_type character_length = next_pos - pos;
      if (character_length == 1) {
        switch (pattern[pos]) {
          case '\\':
          case '_':
            return LikeRewriteType::kNoRewrite;
          case '%': {
            // We allow any number of trailing '%' at the end. So make sure
            // that's all we have left here.
            for (absl::string_view::size_type j = pos + 1; j < pattern.length();
                 ++j) {
              if (pattern[j] != '%') {
                // If we see anything else, then we cannot rewrite this so
                // return.
                // Note, there is no guarantee that pattern[j] is actually
                // a one byte pattern, however, we only care that it is not an
                // ascii '%'.
                return LikeRewriteType::kNoRewrite;
              }
            }

            ends_with_percent = true;
            pattern.remove_suffix(pattern.size() - pos);
            break;
          }
          default:
            break;
        }
      }
      pos += character_length;
    }
  }

  // At this point, the pattern will have its leading and trailing '%'
  // stripped (if any) and will not contain any escape or wildcard characters.

  *substring = pattern;
  if ((starts_with_percent || ends_with_percent) && pattern.empty()) {
    // The pattern was '%' or '%%', both of which evaluate to true for any non-
    // null string input.
    return LikeRewriteType::kNotNull;
  }

  if (starts_with_percent && ends_with_percent) {
    // Rewrite "input LIKE '%pattern%'" to a containment check.
    return LikeRewriteType::kContains;
  } else if (starts_with_percent) {
    // Rewrite "input LIKE '%pattern'" to ENDS_WITH.
    return LikeRewriteType::kEndsWith;
  } else if (ends_with_percent) {
    // Rewrite "input LIKE 'pattern%'" to STARTS_WITH.
    return LikeRewriteType::kStartsWith;
  } else {
    // Rewrite "input LIKE 'pattern'" to an equality check.
    return LikeRewriteType::kEquals;
  }
}

}  // namespace functions
}  // namespace zetasql
