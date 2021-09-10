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

#include "zetasql/public/functions/string_with_collation.h"

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "zetasql/common/utf_util.h"
#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/string.h"
#include "zetasql/public/functions/util.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "unicode/coleitr.h"
#include "unicode/errorcode.h"
#include "unicode/schriter.h"
#include "unicode/stsearch.h"
#include "unicode/tblcoll.h"
#include "unicode/unistr.h"
#include "unicode/usearch.h"
#include "unicode/ustring.h"

namespace zetasql {
namespace functions {

namespace {

const size_t kMaxOutputSize = (1 << 20);  // 1MB
// Based on https://tools.ietf.org/html/rfc2045#section-6.8

const char kExceededReplaceOutputSize[] =
    "Output of REPLACE exceeds max allowed output size of 1MB";

constexpr absl::string_view kBadPosStringPos = "Position must be non-zero";

const int64_t int32max = std::numeric_limits<int32_t>::max();
const int64_t int32min = std::numeric_limits<int32_t>::min();

// Returns false if an error occurs. If the method succeeds, the length return
// argument is set to the byte length of the UTF-8 string corresponding to the
// UTF-16 substring. Requires the input string to be a well formed UTF-8 string.
bool GetUtf8Length(const icu::UnicodeString& unicode_str, int32_t start,
                      int32_t limit, int32_t* length, absl::Status* error) {
  icu::ErrorCode status;
  u_strToUTF8(/*dest=*/nullptr, /*destCapacity =*/0, length,
              unicode_str.getBuffer() + start, limit - start, status);
  // Ignore U_BUFFER_OVERFLOW_ERROR since the call above returns this error if
  // we pass dest == nullptr && destCapacity == 0.
  if (ABSL_PREDICT_FALSE(status.isFailure()) &&
      status != U_BUFFER_OVERFLOW_ERROR) {
    *error = absl::Status(
        absl::StatusCode::kOutOfRange,
        absl::StrCat("Internal error when computing UTF-8 offset: ",
                     status.errorName()));
    status.reset();
    return false;
  }
  status.reset();
  return true;
}

}  // anonymous namespace

// REPLACE(COLLATOR, STRING, STRING, STRING) -> STRING
bool ReplaceUtf8WithCollation(const ZetaSqlCollator& collator,
                              absl::string_view str, absl::string_view oldsub,
                              absl::string_view newsub, std::string* out,
                              absl::Status* error) {
  out->clear();
  // The StringSearch API requires both inputs to be non-empty valid UTF-8
  // strings so handle these cases individually.
  if (!IsWellFormedUTF8(str)) {
    return internal::UpdateError(
        error, "Value in REPLACE function is not a valid UTF-8 string");
  }
  if (!IsWellFormedUTF8(oldsub)) {
    return internal::UpdateError(error,
                                 "The substring to be replaced in REPLACE "
                                 "function is not a valid UTF-8 string");
  }
  if (!IsWellFormedUTF8(newsub)) {
    return internal::UpdateError(error,
                                 "The new replacement substring in REPLACE "
                                 "function is not a valid UTF-8 string");
  }
  // The StringSearch API requires both the haystack and the needle to be
  // non-empty strings. Handle these 2 cases manually.
  if (str.empty()) {
    return true;
  }
  if (oldsub.empty()) {
    if (str.length() > kMaxOutputSize) {
      return internal::UpdateError(error, kExceededReplaceOutputSize);
    }
    // If empty, return the input.
    out->append(str.data(), str.length());
    return true;
  }
  if (collator.IsBinaryComparison()) {
    // If the collator uses binary comparison then the use of ICU libraries is
    // not necessary. Use the non-collation version of REPLACE in this case.
    return ReplaceUtf8(str, oldsub, newsub, out, error);
  }
  icu::ErrorCode status;
  icu::UnicodeString old_sequence = icu::UnicodeString::fromUTF8(oldsub);
  icu::UnicodeString original = icu::UnicodeString::fromUTF8(str);
  // This cast is necessary because the StringSearch API requires a non-const
  // collator. The collator is not changed by the StringSearch methods used
  // below which makes the cast here okay.
  icu::RuleBasedCollator* icu_collator =
      const_cast<icu::RuleBasedCollator*>(collator.GetIcuCollator());
  icu::StringSearch stsearch(old_sequence, original, icu_collator, nullptr,
                             status);
  if (ABSL_PREDICT_FALSE(status.isFailure())) {
    *error = absl::Status(
        absl::StatusCode::kOutOfRange,
        absl::StrCat("Error initializing StringSearch: ", status.errorName()));
    status.reset();
    return false;
  }

  int32_t utf16_pos = 0;
  int32_t utf8_pos = 0;
  while (true) {
    int32_t utf16_match_index = stsearch.next(status);

    if (ABSL_PREDICT_FALSE(status.isFailure())) {
      *error = absl::Status(absl::StatusCode::kOutOfRange,
                            absl::StrCat("Error in StringSearch operation: ",
                                         status.errorName()));
      status.reset();
      return false;
    }
    if (utf16_match_index == USEARCH_DONE) {
      break;
    }
    int32_t unmatched_string_utf8_length;
    if (!GetUtf8Length(original, utf16_pos, utf16_match_index,
                      &unmatched_string_utf8_length, error)) {
      return false;
    }
    const size_t total_append_size =
        unmatched_string_utf8_length + newsub.length();
    if (out->size() + total_append_size > kMaxOutputSize) {
      return internal::UpdateError(error, kExceededReplaceOutputSize);
    }
    int32_t matched_string_utf8_length;
    if (!GetUtf8Length(original, utf16_match_index,
                      utf16_match_index + stsearch.getMatchedLength(),
                      &matched_string_utf8_length, error)) {
      return false;
    }

    out->append(str, utf8_pos, unmatched_string_utf8_length).append(newsub);
    utf16_pos = utf16_match_index + stsearch.getMatchedLength();
    utf8_pos += unmatched_string_utf8_length + matched_string_utf8_length;
  }
  out->append(str, utf8_pos);
  return true;
}

bool SplitUtf8WithCollationImpl(const ZetaSqlCollator& collator,
                                absl::string_view str,
                                absl::string_view delimiter,
                                std::vector<absl::string_view>* out,
                                absl::Status* error) {
  out->clear();
  if (collator.IsBinaryComparison()) {
    // If the collator uses binary comparison then the use of ICU libraries is
    // not necessary. Use the non-collation version of SPLIT in this case.
    return SplitUtf8(str, delimiter, out, error);
  }
  // The StringSearch API requires both inputs to be non-empty valid UTF-8
  // strings so handle these cases individually.
  if (!IsWellFormedUTF8(str)) {
    return internal::UpdateError(
        error, "Value in SPLIT function is not a valid UTF-8 string");
  }
  if (!IsWellFormedUTF8(delimiter)) {
    return internal::UpdateError(
        error, "Delimiter in SPLIT function is not a valid UTF-8 string");
  }
  if (str.empty()) {
    out->push_back("");
    return true;
  }
  icu::UnicodeString unicode_str = icu::UnicodeString::fromUTF8(str);
  // This cast is necessary because the StringSearch API requires a non-const
  // collator. The collator is not changed by the StringSearch methods used
  // below which makes the cast here okay.
  icu::RuleBasedCollator* icu_collator =
      const_cast<icu::RuleBasedCollator*>(collator.GetIcuCollator());
  icu::ErrorCode status;
  if (delimiter.empty()) {
    // Splitting on an empty delimiter produces an array of collation elements.
    std::unique_ptr<icu::CollationElementIterator> coll_iterator =
        absl::WrapUnique(
            icu_collator->createCollationElementIterator(unicode_str));
    int32_t utf16_start = 0;
    int32_t utf8_start = 0;
    while (coll_iterator->next(status) !=
           icu::CollationElementIterator::NULLORDER) {
      int32_t next_utf16_start = coll_iterator->getOffset();
      // Some characters (e.g. "ä") expand to multiple collation elements.
      // Without this check, there will be spurious empty strings in the result.
      if (utf16_start == next_utf16_start) {
        continue;
      }
      int32_t utf8_length;
      if (!GetUtf8Length(unicode_str, utf16_start, next_utf16_start,
                        &utf8_length, error)) {
        return false;
      }
      out->push_back(str.substr(utf8_start, utf8_length));
      utf16_start = next_utf16_start;
      utf8_start += utf8_length;
    }
    if (ABSL_PREDICT_FALSE(status.isFailure())) {
      *error = absl::Status(
          absl::StatusCode::kOutOfRange,
          absl::StrCat("Error when iterating through a value in SPLIT: ",
                       status.errorName()));
      status.reset();
      return false;
    }
    return true;
  }
  icu::UnicodeString unicode_delimiter =
      icu::UnicodeString::fromUTF8(delimiter);
  icu::StringSearch stsearch(unicode_delimiter, unicode_str, icu_collator,
                             /*breakiter=*/nullptr, status);
  if (ABSL_PREDICT_FALSE(status.isFailure())) {
    *error = absl::Status(
        absl::StatusCode::kOutOfRange,
        absl::StrCat("Error initializing StringSearch: ", status.errorName()));
    status.reset();
    return false;
  }
  int32_t utf16_pos = 0;
  int32_t utf8_pos = 0;
  while (true) {
    int32_t utf16_match_index = stsearch.next(status);
    if (ABSL_PREDICT_FALSE(status.isFailure())) {
      *error = absl::Status(absl::StatusCode::kOutOfRange,
                            absl::StrCat("Error in StringSearch operation: ",
                                         status.errorName()));
      status.reset();
      return false;
    }
    if (utf16_match_index == USEARCH_DONE) {
      break;
    }
    int32_t unmatched_string_utf8_length;
    if (!GetUtf8Length(unicode_str, utf16_pos, utf16_match_index,
                      &unmatched_string_utf8_length, error)) {
      return false;
    }
    int32_t matched_string_utf8_length;
    if (!GetUtf8Length(unicode_str, utf16_match_index,
                      utf16_match_index + stsearch.getMatchedLength(),
                      &matched_string_utf8_length, error)) {
      return false;
    }
    out->push_back(str.substr(utf8_pos, unmatched_string_utf8_length));
    utf16_pos = utf16_match_index + stsearch.getMatchedLength();
    utf8_pos += unmatched_string_utf8_length + matched_string_utf8_length;
  }
  out->push_back(str.substr(utf8_pos));
  return true;
}

bool SplitUtf8WithCollation(const ZetaSqlCollator& collator,
                            absl::string_view str, absl::string_view delimiter,
                            std::vector<absl::string_view>* out,
                            absl::Status* error) {
  return SplitUtf8WithCollationImpl(collator, str, delimiter, out, error);
}

// Returns an icu::StringSearch with overlapping search attribute enabled and
// offset set to the given value. If offset is out of bounds,
// offset_out_of_bounds is set to true. The returned StringSearch is required
// not to call methods that modify its collator.
absl::StatusOr<std::unique_ptr<icu::StringSearch>>
InitOverlappingStringSearchAtOffset(const ZetaSqlCollator& collator,
                                    icu::UnicodeString& unicode_str,
                                    icu::UnicodeString& unicode_substr,
                                    int32_t offset, bool* is_out_of_bounds) {
  // This cast is necessary because the StringSearch API requires a non-const
  // collator. This cast is okay as long as the methods called on the
  // StringSearch do not modify the collator.
  icu::RuleBasedCollator* icu_collator =
      const_cast<icu::RuleBasedCollator*>(collator.GetIcuCollator());
  icu::ErrorCode status;
  auto stsearch = absl::make_unique<icu::StringSearch>(
      unicode_substr, unicode_str, icu_collator,
      /*breakiter=*/nullptr, status);
  if (ABSL_PREDICT_FALSE(status.isFailure())) {
    absl::Status error = absl::Status(
        absl::StatusCode::kOutOfRange,
        absl::StrCat("Error initializing StringSearch: ", status.errorName()));
    status.reset();
    return error;
  }
  stsearch->setAttribute(USEARCH_OVERLAP, USEARCH_ON, status);
  if (ABSL_PREDICT_FALSE(status.isFailure())) {
    absl::Status error = absl::Status(
        absl::StatusCode::kOutOfRange,
        absl::StrCat("Error setting overlap attribute in StringSearch: ",
                     status.errorName()));
    status.reset();
    return error;
  }
  stsearch->setOffset(offset, status);
  if (ABSL_PREDICT_FALSE(status.isFailure())) {
    if (ABSL_PREDICT_FALSE(status != U_INDEX_OUTOFBOUNDS_ERROR)) {
      absl::Status error =
          absl::Status(absl::StatusCode::kOutOfRange,
                       absl::StrCat("Error setting offset in StringSearch: ",
                                    status.errorName()));
      status.reset();
      return error;
    }
    *is_out_of_bounds = true;
  } else {
    *is_out_of_bounds = false;
  }
  return stsearch;
}

bool GetNthPosMatchIndex(const ZetaSqlCollator& collator,
                         absl::string_view str, absl::string_view substr,
                         int32_t code_point_pos, int32_t occurrence,
                         int64_t* out, absl::Status* error) {
  if (ABSL_PREDICT_FALSE(code_point_pos <= 0 || occurrence <= 0)) {
    return internal::UpdateError(
        error,
        "Internal error when computing starting position of a substring.");
  }
  icu::UnicodeString unicode_substr = icu::UnicodeString::fromUTF8(substr);
  if (unicode_substr.isEmpty()) {
    *out = 0;
    return true;
  }
  icu::ErrorCode status;
  icu::UnicodeString unicode_str = icu::UnicodeString::fromUTF8(str);
  if (unicode_str.isEmpty()) {
    // substr is not empty so return no match.
    *out = 0;
    return true;
  }
  // Convert code_point_pos from code point offset to code unit offset which is
  // what icu::UnicodeString uses when setting offsets. code_point_pos is
  // 1-based.
  int32_t code_unit_search_start =
      unicode_str.moveIndex32(0, code_point_pos - 1);
  bool offset_out_of_bounds;
  absl::StatusOr<std::unique_ptr<icu::StringSearch>> stsearch =
      InitOverlappingStringSearchAtOffset(collator, unicode_str, unicode_substr,
                                          code_unit_search_start,
                                          &offset_out_of_bounds);
  if (!stsearch.ok()) {
    *error = stsearch.status();
    return false;
  }
  if (offset_out_of_bounds) {
    *out = 0;
    return true;
  }
  int32_t match_code_unit_pos;
  for (int64_t i = 0; i < occurrence; i++) {
    match_code_unit_pos = stsearch.value()->next(status);
    if (ABSL_PREDICT_FALSE(status.isFailure())) {
      *error = absl::Status(absl::StatusCode::kOutOfRange,
                            absl::StrCat("Error in StringSearch operation: ",
                                         status.errorName()));
      status.reset();
      return false;
    }
    if (match_code_unit_pos == USEARCH_DONE) {
      *out = 0;
      return true;
    }
  }
  // icu::StringSearch returns offsets in code units but we want to return the
  // code point offset.
  int32_t match_code_point_pos =
      unicode_str.countChar32(0, match_code_unit_pos);
  // Return value is 1-based.
  *out = match_code_point_pos + 1;
  return true;
}

bool GetNthNegMatchIndex(const ZetaSqlCollator& collator,
                         absl::string_view str, absl::string_view substr,
                         int32_t code_point_pos, int32_t occurrence,
                         int64_t* out, bool* is_ends_with,
                         absl::Status* error) {
  *is_ends_with = false;
  if (ABSL_PREDICT_FALSE(code_point_pos >= 0 || occurrence <= 0)) {
    return internal::UpdateError(
        error,
        "Internal error when computing starting position of a substring.");
  }
  icu::UnicodeString unicode_substr = icu::UnicodeString::fromUTF8(substr);
  if (unicode_substr.isEmpty()) {
    *out = 0;
    return true;
  }
  icu::UnicodeString unicode_str = icu::UnicodeString::fromUTF8(str);
  if (unicode_str.isEmpty()) {
    // substr is not empty so return no match.
    *out = 0;
    return true;
  }

  // code_point_pos is negative and 1-based. Compute whether to begin the search
  // at the end of the string or at a code unit offset.
  int32_t code_unit_search_start = unicode_str.length();
  int32_t substr_code_point_length = unicode_substr.countChar32();
  if (code_point_pos + substr_code_point_length < 0) {
    // Convert code_point_pos from code point offset to code unit offset which
    // is what icu::UnicodeString uses when setting offsets. code_point_pos is
    // 1-based.
    code_unit_search_start =
        unicode_str.moveIndex32(unicode_str.length(), code_point_pos + 1);
  }
  bool offset_out_of_bounds;
  absl::StatusOr<std::unique_ptr<icu::StringSearch>> stsearch =
      InitOverlappingStringSearchAtOffset(collator, unicode_str, unicode_substr,
                                          code_unit_search_start,
                                          &offset_out_of_bounds);
  if (!stsearch.ok()) {
    *error = stsearch.status();
    return false;
  }
  if (offset_out_of_bounds) {
    *out = 0;
    return true;
  }
  int32_t match_code_unit_pos;
  icu::ErrorCode status;
  for (int64_t i = 0; i < occurrence; i++) {
    match_code_unit_pos = stsearch.value()->previous(status);
    if (ABSL_PREDICT_FALSE(status.isFailure())) {
      *error = absl::Status(absl::StatusCode::kOutOfRange,
                            absl::StrCat("Error in StringSearch operation: ",
                                         status.errorName()));
      status.reset();
      return false;
    }
    if (match_code_unit_pos == USEARCH_DONE) {
      *out = 0;
      return true;
    }
  }
  *is_ends_with =
      match_code_unit_pos != USEARCH_DONE &&
      (match_code_unit_pos + stsearch.value()->getMatchedLength()) ==
          unicode_str.length();
  // Convert the code unit offset to code points.
  int32_t match_code_point_pos =
      unicode_str.countChar32(0, match_code_unit_pos);
  // Return value is 1-based.
  *out = match_code_point_pos + 1;
  return true;
}

bool StrPosOccurrenceUtf8WithCollation(const ZetaSqlCollator& collator,
                                       absl::string_view str,
                                       absl::string_view substr, int64_t pos,
                                       int64_t occurrence, int64_t* out,
                                       absl::Status* error) {
  if (collator.IsBinaryComparison()) {
    return StrPosOccurrenceUtf8(str, substr, pos, occurrence, out, error);
  }
  if (pos == 0) {
    return internal::UpdateError(error, kBadPosStringPos);
  }
  if (occurrence < 1) {
    return internal::UpdateError(error,
                                 "Occurrence in STRPOS cannot be less than 1");
  }
  if (str.length() > int32max || substr.length() > int32max) {
    return internal::UpdateError(
        error, "STRPOS can only operate on strings with length <= INT32_MAX.");
  }
  if (pos > int32max || pos < int32min || occurrence > int32max ||
      occurrence < int32min) {
    // It is impossible for the combination of pos and occurrence to be inside
    // of str so return no match.
    *out = 0;
    return true;
  }
  if (!IsWellFormedUTF8(str)) {
    return internal::UpdateError(
        error, "Value in INSTR function is not a valid UTF-8 string");
  }
  if (!IsWellFormedUTF8(substr)) {
    return internal::UpdateError(
        error, "Substring in INSTR function is not a valid UTF-8 string");
  }
  if (pos > 0) {
    return GetNthPosMatchIndex(collator, str, substr, static_cast<int32_t>(pos),
                               static_cast<int32_t>(occurrence), out, error);
  } else {
    bool is_ends_with;
    return GetNthNegMatchIndex(collator, str, substr, static_cast<int32_t>(pos),
                               static_cast<int32_t>(occurrence), out,
                               &is_ends_with, error);
  }
}

bool StartsWithUtf8WithCollation(const ZetaSqlCollator& collator,
                                 absl::string_view str,
                                 absl::string_view substr, bool* out,
                                 absl::Status* error) {
  if (collator.IsBinaryComparison()) {
    // If the collator uses binary comparison then the use of ICU libraries is
    // not necessary. Use the non-collation version of STARTS_WITH in this case.
    return StartsWithUtf8(str, substr, out, error);
  }
  if (!IsWellFormedUTF8(substr)) {
    return internal::UpdateError(
        error, "Substring in STARTS_WITH function is not a valid UTF-8 string");
  }
  if (!IsWellFormedUTF8(str)) {
    return internal::UpdateError(
        error, "Value in STARTS_WITH function is not a valid UTF-8 string");
  }
  int64_t match_index;
  if (!GetNthPosMatchIndex(collator, str, substr, /*code_point_pos=*/1,
                           /*occurrence=*/1, &match_index, error)) {
    *out = false;
    return false;
  }
  *out = match_index == 1;
  return true;
}

bool EndsWithUtf8WithCollation(const ZetaSqlCollator& collator,
                               absl::string_view str, absl::string_view substr,
                               bool* out, absl::Status* error) {
  if (collator.IsBinaryComparison()) {
    // If the collator uses binary comparison then the use of ICU libraries is
    // not necessary. Use the non-collation version of ENDS_WITH in this case.
    return EndsWithUtf8(str, substr, out, error);
  }
  if (!IsWellFormedUTF8(substr)) {
    return internal::UpdateError(
        error, "Substring in ENDS_WITH function is not a valid UTF-8 string");
  }
  if (!IsWellFormedUTF8(str)) {
    return internal::UpdateError(
        error, "Value in ENDS_WITH function is not a valid UTF-8 string");
  }
  int64_t match_index;
  if (!GetNthNegMatchIndex(collator, str, substr, /*code_point_pos=*/-1,
                           /*occurrence=*/1, &match_index, out, error)) {
    *out = false;
    return false;
  }
  return true;
}

}  // namespace functions
}  // namespace zetasql
