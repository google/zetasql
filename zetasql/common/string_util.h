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

#ifndef ZETASQL_COMMON_STRING_UTIL_H_
#define ZETASQL_COMMON_STRING_UTIL_H_

#include <cmath>

#include "absl/strings/str_cat.h"
#include "zetasql/base/string_numbers.h"

namespace zetasql {

// Converts a float into a string which, if passed to `strtof()`, will produce
// the original float to the same precision.
//
// Exception: for NaN values, `strtod(RoundTripFloatToString(NaN))`
// may produce any NaN value, not necessarily the original NaN value.
//
// No guarantees are made about 'compactness' of the returned string.
// `1.0` may print as `1.0000000000000` or as `1.0`
//
// If this round-trip property is not required, consider simply absl::StrCat(v).
inline std::string RoundTripFloatToString(float f) {
  if (!std::isnan(f)) {
    return zetasql_base::RoundTripFloatToString(f);
  }
  return "nan";
}

// Converts a double into a string which, if passed to `strtod()`, will produce
// the original float to the same precision.
//
// Exception: for NaN values, `strtod(RoundTripDoubleToString(NaN))`
// may produce any NaN value, not necessarily the original NaN value.
//
// No guarantees are made about 'compactness' of the returned string.
// `1.0` may print as `1.0000000000000` or as `1.0`
//
// If this round-trip property is not required, consider simply absl::StrCat(v).
inline std::string RoundTripDoubleToString(double d) {
  if (!std::isnan(d)) {
    return zetasql_base::RoundTripDoubleToString(d);
  }
  return "nan";
}

// Alias for `RoundTripDoubleToString` to allow calling from templated methods
// where the type is not known to be double or float.
inline std::string RoundTripFloatToString(double d) {
  return RoundTripDoubleToString(d);
}

// Replace the first instance of `oldsub` with `newsub` inside `s`.  If
// `oldsub` doesn't exist in `s`, just returns `s`.
inline std::string ReplaceFirst(absl::string_view s, absl::string_view oldsub,
                                absl::string_view newsub) {
  absl::string_view::size_type pos = s.find(oldsub);
  if (pos == absl::string_view::npos) {
    return std::string(s);
  }
  return absl::StrCat(s.substr(0, pos), newsub,
                      s.substr(pos + newsub.length()));
}
}  // namespace zetasql

#endif  // ZETASQL_COMMON_STRING_UTIL_H_
