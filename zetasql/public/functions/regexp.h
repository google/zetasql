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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_REGEXP_H_
#define ZETASQL_PUBLIC_FUNCTIONS_REGEXP_H_

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

  // REGEXP_EXTRACT
  // If a match was extracted, returns true with *is_null set to false.
  // If a match was not extracted, returns true with *is_null set to true.
  // If extraction failed for some other reason, returns false with a non-OK
  // status in *error.
  bool Extract(absl::string_view str, absl::string_view* out, bool* is_null,
               absl::Status* error);

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
  //  LOG(INFO) << output;
  // }
  // ZETASQL_RETURN_IF_ERROR(error);
  //
  // Note that on success, error will _not_ be explicitly set to OK, but rather
  // left unchanged.
  void ExtractAllReset(const absl::string_view str);
  bool ExtractAllNext(absl::string_view* out, absl::Status* error);

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
    DCHECK(re_ != nullptr) << "Not initialized";
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
  // Keeps track whether match was the last one. It is needed to prevent
  // infinite loop when input is empty and regexp matches empty string.
  bool last_match_;
};

}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_REGEXP_H_
