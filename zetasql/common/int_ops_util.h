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

#ifndef ZETASQL_COMMON_INT_OPS_UTIL_H_
#define ZETASQL_COMMON_INT_OPS_UTIL_H_

#include <cmath>
#include <cstdint>

namespace zetasql {

// LosslessConvert casts a value from double to int64_t, detecting if any
// information is lost due to overflow, rounding, or changes in signedness. If
// successful, returns true and writes converted value to `*output`. Otherwise,
// returns false, and the contents of `*output` is undefined.
inline bool LossLessConvertDoubleToInt64(double input, int64_t* output) {
  if (std::isnan(input) || std::isinf(input)) {
    return false;
  }

  double lower_bound = std::numeric_limits<int64_t>::min();
  if (input < lower_bound) {
    return false;
  }

  if (input > 0) {
    // Set exp such that value == f * 2^exp for some f with |f| in [0.5, 1.0).
    // Note that this implies that the magnitude of value is strictly less than
    // 2^exp.
    int exp = 0;
    std::frexp(input, &exp);

    // Let N be the number of non-sign bits in the representation of int64_t.
    // If the magnitude of value is strictly less than 2^N, the truncated
    // version of input is representable as int64_t.
    if (exp > std::numeric_limits<int64_t>::digits) {
      return false;
    }
  }

  *output = static_cast<int64_t>(input);
  return input == static_cast<double>(*output);
}

}  // namespace zetasql

#endif  // ZETASQL_COMMON_INT_OPS_UTIL_H_
