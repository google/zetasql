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

#ifndef ZETASQL_PUBLIC_NUMERIC_VALUE_TEST_UTILS_H_
#define ZETASQL_PUBLIC_NUMERIC_VALUE_TEST_UTILS_H_

#include "zetasql/public/numeric_value.h"
#include "absl/random/random.h"

namespace zetasql {

// Generates a random valid numeric value, intended to cover all precisions
// and scales when enough numbers are generated. Do not assume the result to
// follow any specific distribution.
template <typename T>
T MakeRandomNumericValue(absl::BitGen* random) {
  constexpr int kNumWords = sizeof(T) / sizeof(uint64_t);
  constexpr int kNumBits = sizeof(T) * 8;
  std::array<uint64_t, kNumWords> value;
  for (uint64_t& elem : value) {
    elem = absl::Uniform<uint64_t>(*random);
  }
  FixedInt<64, kNumWords> fixed_int(value);
  uint32_t shift_bits = absl::Uniform<uint32_t>(*random, 0, kNumBits);
  fixed_int >>= shift_bits;
  T result;
  if constexpr (std::is_same_v<T, NumericValue>) {
    __int128 v = std::max(
        std::min(static_cast<__int128>(FixedInt<64, 2>(fixed_int.number())),
                 NumericValue::MaxValue().as_packed_int()),
        NumericValue::MinValue().as_packed_int());
    result = NumericValue::FromPackedInt(v).value();
  } else {
    static_assert(std::is_same_v<T, BigNumericValue>,
                  "T must be NumericValue or BigNumericValue");
    result = BigNumericValue::FromPackedLittleEndianArray(fixed_int.number());
  }
  constexpr int scale = T::kMaxFractionalDigits;
  constexpr int precision = T::kMaxIntegerDigits + scale;
  int32_t digits_to_trunc = absl::Uniform<uint32_t>(
      *random, 0, (kNumBits - shift_bits) * precision / kNumBits);
  return result.Trunc(scale - digits_to_trunc);
}

// Generates a random vald numeric value using a static RNG.
// Prefer using the other variant of this function.
template <typename T>
T MakeRandomNumericValue() {
  thread_local absl::BitGen generator;
  return MakeRandomNumericValue<T>(&generator);
}

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_NUMERIC_VALUE_TEST_UTILS_H_
