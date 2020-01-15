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

#include "zetasql/public/functions/percentile.h"

#include <limits>
#include "zetasql/base/logging.h"
#include "zetasql/base/mathutil.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {

size_t PercentileEvaluator::ComputePercentileIndex(
    size_t max_index, long double* left_weight,
    long double* right_weight) const {
  // Some fast paths for common use cases.
  if (percentile_ == 0.5) {
    *right_weight = 0.5 * (max_index & 1);
    *left_weight = 1 - *right_weight;
    return max_index >> 1;
  }
  if (percentile_ == 0) {
    *left_weight = 1;
    *right_weight = 0;
    return 0;
  }
  if (percentile_ == 1) {
    *left_weight = 1;
    *right_weight = 0;
    return max_index;
  }
  using uint128 = unsigned __int128;
  // Use integer multiplication to preserve all bits. The compiler is smart
  // enough to optimize the multiplication to only one mulq instruction.
  const uint128 product =
      static_cast<uint128>(static_cast<uint64_t>(percentile_mantissa_)) *
      max_index;
  if (num_fractional_bits_ < 128) {
    const uint128 scale = static_cast<uint128>(1) << num_fractional_bits_;
    const uint128 fractional_part = product & (scale - 1);
    // To minimize precision loss, the smaller weight should be computed from
    // the integer; the bigger weight can be computed as 1 - smaller weight
    // without further precision loss.
    if (fractional_part > (scale >> 1)) {
      *left_weight = std::ldexp(
          static_cast<long double>(scale - fractional_part),
          percentile_exponent_);
      *right_weight = 1 - *left_weight;
    } else {
      *right_weight = std::ldexp(static_cast<long double>(fractional_part),
                                 percentile_exponent_);
      *left_weight = 1 - *right_weight;
    }
    const size_t index = static_cast<size_t>(product >> num_fractional_bits_);
    DCHECK_LE(index, max_index);
    DCHECK_GE(*left_weight, 0);
    DCHECK_LE(*left_weight, 1);
    DCHECK_GE(*right_weight, 0);
    DCHECK_LE(*right_weight, 1);
    return index;
  }
  *right_weight = std::ldexp(static_cast<long double>(product),
                             percentile_exponent_);
  *left_weight = 1 - *right_weight;
  return 0;
}

// static
zetasql_base::StatusOr<PercentileEvaluator> PercentileEvaluator::Create(
    double percentile) {
  ZETASQL_RET_CHECK_GE(percentile, 0);
  ZETASQL_RET_CHECK_LE(percentile, 1);
  const zetasql_base::MathUtil::DoubleParts parts = zetasql_base::MathUtil::Decompose(percentile);
  ZETASQL_RET_CHECK_GE(parts.mantissa, 0);
  return PercentileEvaluator(percentile, parts.mantissa, parts.exponent);
}

}  // namespace zetasql
