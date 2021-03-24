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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_REGEXP_H_
#define ZETASQL_PUBLIC_FUNCTIONS_REGEXP_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include <cstdint>
#include "absl/strings/string_view.h"
#include "re2/re2.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

// A helper class that implements regexp fuctions: REGEXP_MATCH,
// REGEXP_EXTRACT and REGEXP_REPLACE.
// Normally per every function call an implementation should call
// InitializePatternUtf8 (if parameters have type STRING) or
// InitializePatternBytes (BYTES parameters) and then call Match(), Extract()
// or Replace() depending on the function.
// Implementations may skip InitializePatter call if the corresponding
// function parameter is constant.
//
// E.g. code implementing REGEXP_MATCH may look as follows:
//
//   Regexp re;
//   bool out;
//   if (!re.InitializePatternUtf8(x[1], error) ||
//       !re.Match(x[0], out, error)) {
//     return error;
//   } else {
//     return Value::Bool(*out);
//   }
//
class RegExp {
 public:
  // TODO: extend this class to support memory limits on memory used
  // by the processed regexp.

  // The following two functions parse a regular expression assuming
  // UTF-8 (InitializePatternUtf8) or Latin1 (InitializePatternBytes) encoding.
  // If the regular expression is not correct *error is updated
  // and false is returned.
  bool InitializePatternUtf8(absl::string_view pattern, absl::Status* error);
  bool InitializePatternBytes(absl::string_view pattern, absl::Status* error);

  // REGEXP_CONTAINS (substring match)
  bool Contains(absl::string_view str, bool* out, absl::Status* error);

  // REGEXP_MATCH (full match)
  bool Match(absl::string_view str, bool* out, absl::Status* error);

  enum PositionUnit {
    kBytes,
    kUtf8Chars,
  };

  // REGEXP_EXTRACT
  // Extracts a match from `str` of type `position_unit` starting at `position`
  // and looks for the specified `occurrence_index`.
  // If `occurrence_index` is greater than the number of matches found returns
  // true with *is_null set to true.
  // If a match was extracted, returns true with *is_null set to false.
  // If a match was not extracted, returns true with *is_null set to true.
  // If extraction failed for some other reason, returns false with a non-OK
  // status in *error.
  // Note: Both `position` and `occurrence_index` are one-based indices rather
  // than zero-based indices.
  bool Extract(absl::string_view str, PositionUnit position_unit,
               int64_t position, int64_t occurrence_index,
               absl::string_view* out, bool* is_null, absl::Status* error);

  inline bool Extract(absl::string_view str, absl::string_view* out,
                      bool* is_null, absl::Status* error) {
    // Position unit doesn't matter here since both the `position` and
    // `occurrence_index` are 1 so we set a no-op value.
    return Extract(str, /*position_unit=*/PositionUnit::kBytes, /*position=*/1,
                   /*occurrence_index=*/1, out, is_null, error);
  }

  // REGEXP_EXTRACT_ALL
  // This ZetaSQL function returns an array of strings or bytes.
  // An implementation should first call ExtractAllReset and then repeatedly
  // call ExtractAllNext() to get every next element of the array until it
  // returns false. 'error' should be examined to distinguish error condition
  // from no more matches condition.
  //
  // absl::string_view input;
  // absl::string_view output;
  // ...
  // ExtractAllReset(input);
  // while (ExtractAllNext(&output, &error)) {
  //  ZETASQL_LOG(INFO) << output;
  // }
  // ZETASQL_RETURN_IF_ERROR(error);
  //
  // Note that on success, error will _not_ be explicitly set to OK, but rather
  // left unchanged.
  void ExtractAllReset(const absl::string_view str);
  bool ExtractAllNext(absl::string_view* out, absl::Status* error);

  enum ReturnPosition {
    // Returns the position of the start of the match
    kStartOfMatch,

    // Returns the position of the character immediately following the
    // match, or, if the match is at the end of the input string, 1 +
    // length(input_string).
    kEndOfMatch,
  };

  // InstrParams, parameters of Instr()
  struct InstrParams {
    // input_str can be a STRING or BYTES
    absl::string_view input_str;

    // position_unit can be kBytes or kUtf8Chars, which is the unit of input
    // position and output position.
    PositionUnit position_unit = kUtf8Chars;

    // Start position of the input str, one-based. It's in either characters
    // or bytes, depending on position_unit
    int64_t position = 1;

    // *out will be set to the position of the match at this index,
    // or 0 if this index exceeds the number of matches of the regular
    // expression against 'input_str'. One-based.
    int64_t occurrence_index = 1;

    // return_position can be kStartOfMatch or kEndOfMatch
    ReturnPosition return_position = kStartOfMatch;

    // The returned position, one based. In either characters or bytes,
    // depending on the position_unit. Users are required to set it.
    // Out will be set to 0 in case of error.
    int64_t* out = nullptr;
  };

  // REGEXP_INSTR
  // Returns the position (one-based) of the specified occurrence of the
  // regexp_value pattern starting at 'position' in str.
  // If position is greater than str length, 0 is returned.
  // If occurrence is greater than the number of matches found, 0 is returned.
  // If either position or occurrence is NULL, return true with NULL result
  //
  // If the regular expression regexp_value contains a capturing group, the
  // function returns the position of the substring matched by that capturing
  // group based on occurrence (default to 1). If the expression does not
  // contain a capturing group, the function returns the position for the
  // entire matching substring.
  //
  // If no match is found, or no match is found for the given position or
  // occurrence if specified, 0 is returned.
  // If the regex expression is empty, 0 will be returned.
  //
  // Return false with a non-ok status in error if :
  //   Either position or occurrence from input is not a positive integer.
  //   return_position_after_match is neither 0 nor 1.
  //   The regular expression is invalid
  //   The regular expression has more than one capturing group
  // Examples:
  // REGEX_INSTR("-2020-jack-class1", "-[^.-]*", 2, 1, 0) -> 6
  // REGEX_INSTR("-2020-jack-class1", "-[^.-]*", 2, 1, 1) -> 11
  bool Instr(const InstrParams& options, absl::Status* error);

  // REGEXP_REPLACE
  // Replaces all matching substrings in str with newsub and returns result
  // to *out.
  bool Replace(absl::string_view str, absl::string_view newsub,
               std::string* out, absl::Status* error);

  // Sets maximum length in bytes of an output string of any function
  // (e.g. Replace()). This limit is not strictly enforced, but it's
  // guaranteed that this size will not be exceeded by more than the length
  // of one of the input strings.
  void SetMaxOutSize(int32_t size);

  // Accessor to the initialized RE2 object. Must Initialize() first before
  // calling this.
  const RE2& re() const {
    ZETASQL_DCHECK(re_ != nullptr) << "Not initialized";
    return *re_;
  }

 private:
  // Appends the "rewrite" string, with backslash substitutions from "groups",
  // to string "out".
  // Similar to RE2::Rewrite but (1) returns a proper error message instead
  // of logging it, and (2) enforces output string limit set by
  // SetMaxOutSize().
  bool Rewrite(const absl::string_view rewrite,
               const std::vector<absl::string_view>& groups, std::string* out,
               absl::Status* error);

  // The compiled RE2 object. It is NULL if this has not been initialized yet.
  std::unique_ptr<RE2> re_;
  int32_t max_out_size_ = std::numeric_limits<int32_t>::max();

  // The following fields keep internal state of the matcher between calls of
  // ExtractAllReset() and ExtractAllNext().

  // REGEXP_EXTRACT_ALL input string.
  absl::string_view extract_all_input_;
  // Position of the next byte inside extract_all_input_ that will be matched by
  // ExtractAllNext().
  int extract_all_position_;
  // Position of the next byte after last match of capture group
  int capture_group_position_;
  // Keeps track whether match was the last one. It is needed to prevent
  // infinite loop when input is empty and regexp matches empty string.
  bool last_match_;
};

}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_REGEXP_H_
