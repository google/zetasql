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

#ifndef ZETASQL_PUBLIC_NUMERIC_VALUE_INL_H_
#define ZETASQL_PUBLIC_NUMERIC_VALUE_INL_H_



#include <cmath>
#include <limits>
#include <string>
#include <type_traits>

#include "zetasql/common/errors.h"
#include "absl/base/config.h"
#include <cstdint>
#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/canonical_errors.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

namespace internal {

// Helper functions to build min and max NUMERIC values at compile time. We need
// them because there is no way to build int128 literals. Note that these
// functions perform no overflow checks on NUMERIC values and can produce any
// int128 value, possibly outside of allowed NUMERIC range.
constexpr __int128 int128_square(__int128 x) { return x * x; }
constexpr __int128 int128_exp(__int128 x, int n) {
  return n == 0 ? 1
                : int128_square(int128_exp(x, n / 2)) * (n % 2 == 0 ? 1 : x);
}
constexpr __int128 kNumericMax = int128_exp(10, 38) - 1;
constexpr __int128 kNumericMin = -int128_exp(10, 38) + 1;

// Adds two __int128 values and returns true if there was an overflow; otherwise
// returns false.
inline bool int128_add_overflow(__int128 a, __int128 b, __int128* res) {
#if ABSL_HAVE_BUILTIN(__builtin_add_overflow)
  return __builtin_add_overflow(a, b, res);
#else
  *res = static_cast<__int128>(
      static_cast<unsigned __int128>(a) + static_cast<unsigned __int128>(b));
  return (a < 0 && b < 0 && *res > 0) || (a > 0 && b > 0 && *res < 0);
#endif
}

}  // namespace internal

inline NumericValue::NumericValue(uint64_t high_bits, uint64_t low_bits)
    : high_bits_(high_bits), low_bits_(low_bits) {}

inline constexpr NumericValue::NumericValue(__int128 value)
    : high_bits_(static_cast<__int128>(value) >> 64),
      low_bits_(value & std::numeric_limits<uint64_t>::max()) {}

inline constexpr NumericValue::NumericValue()
    : NumericValue(static_cast<__int128>(0)) {}

inline constexpr NumericValue::NumericValue(int value)
    : NumericValue(static_cast<__int128>(value) * kScalingFactor) {}

inline constexpr NumericValue::NumericValue(unsigned int value)
    : NumericValue(static_cast<__int128>(value) * kScalingFactor) {}

inline constexpr NumericValue::NumericValue(long value)  // NOLINT(runtime/int)
    : NumericValue(static_cast<__int128>(value) * kScalingFactor) {}

// NOLINTNEXTLINE(runtime/int)
inline constexpr NumericValue::NumericValue(unsigned long value)
    : NumericValue(static_cast<__int128>(value) * kScalingFactor) {}

// NOLINTNEXTLINE(runtime/int)
inline constexpr NumericValue::NumericValue(long long value)
    : NumericValue(static_cast<__int128>(value) * kScalingFactor) {}

// NOLINTNEXTLINE(runtime/int)
inline constexpr NumericValue::NumericValue(unsigned long long value)
    : NumericValue(static_cast<__int128>(value) * kScalingFactor) {}

inline constexpr NumericValue NumericValue::MaxValue() {
  return NumericValue(internal::kNumericMax);
}

inline constexpr NumericValue NumericValue::MinValue() {
  return NumericValue(internal::kNumericMin);
}

inline zetasql_base::StatusOr<NumericValue> NumericValue::FromPackedInt(
    __int128 value) {
  NumericValue ret(value);

  if (ABSL_PREDICT_FALSE(ret < MinValue() || ret > MaxValue())) {
    return MakeEvalError() << "numeric overflow: result out of range";
  }

  return ret;
}

inline zetasql_base::StatusOr<NumericValue> NumericValue::FromHighAndLowBits(
    uint64_t high_bits, uint64_t low_bits) {
  NumericValue ret(high_bits, low_bits);

  if (ABSL_PREDICT_FALSE(ret < MinValue() || ret > MaxValue())) {
    return MakeEvalError() << "numeric overflow: result out of range";
  }

  return ret;
}

inline zetasql_base::StatusOr<NumericValue> NumericValue::Add(NumericValue rh) const {
  __int128 res;
  if (ABSL_PREDICT_FALSE(internal::int128_add_overflow(
          as_packed_int(), rh.as_packed_int(), &res))) {
    return MakeEvalError() << "numeric overflow: " << ToString() << " + "
                           << rh.ToString();
  }
  auto numeric_value_status = FromPackedInt(res);
  if (!numeric_value_status.ok()) {
    return MakeEvalError() << "numeric overflow: " << ToString() << " + "
                           << rh.ToString();
  }
  return numeric_value_status.ValueOrDie();
}

inline zetasql_base::StatusOr<NumericValue> NumericValue::Subtract(
    NumericValue rh) const {
  __int128 res;
  if (ABSL_PREDICT_FALSE(internal::int128_add_overflow(
          as_packed_int(), -rh.as_packed_int(), &res))) {
    return MakeEvalError() << "numeric overflow: " << ToString() << " - "
                           << rh.ToString();
  }
  auto numeric_value_status = FromPackedInt(res);
  if (!numeric_value_status.ok()) {
    return MakeEvalError() << "numeric overflow: " << ToString() << " - "
                           << rh.ToString();
  }
  return numeric_value_status.ValueOrDie();
}

inline NumericValue NumericValue::UnaryMinus(NumericValue value) {
  // The result is expected to be within the valid range.
  return NumericValue(-value.as_packed_int());
}

inline bool NumericValue::operator==(NumericValue rh) const {
  return as_packed_int() == rh.as_packed_int();
}

inline bool NumericValue::operator!=(NumericValue rh) const {
  return as_packed_int() != rh.as_packed_int();
}

inline bool NumericValue::operator<(NumericValue rh) const {
  return as_packed_int() < rh.as_packed_int();
}

inline bool NumericValue::operator>(NumericValue rh) const {
  return as_packed_int() > rh.as_packed_int();
}

inline bool NumericValue::operator>=(NumericValue rh) const {
  return as_packed_int() >= rh.as_packed_int();
}

inline bool NumericValue::operator<=(NumericValue rh) const {
  return as_packed_int() <= rh.as_packed_int();
}

inline zetasql_base::StatusOr<NumericValue> NumericValue::FromDouble(double value) {
  if (ABSL_PREDICT_FALSE(!std::isfinite(value))) {
    // This error message should be kept consistent with the error message found
    // in .../public/functions/convert.h.
    return MakeEvalError() << "Illegal conversion of non-finite floating point "
                              "number to numeric: "
                           << value;
  }

  if (ABSL_PREDICT_FALSE((value <= -1e30) || (value >= 1e30))) {
    return MakeEvalError() << "numeric out of range: " << value;
  }

  double int_part = trunc(value);
  // Preserve an extra digit for rounding.
  double fract_part = trunc((value - int_part) * kScalingFactor * 10);
  __int128 rounded_fract_part =
      (static_cast<__int128>(fract_part) + (value > 0 ? 5 : -5)) / 10;
  zetasql_base::StatusOr<NumericValue> value_status = FromPackedInt(
      static_cast<__int128>(int_part) * kScalingFactor + rounded_fract_part);
  if (ABSL_PREDICT_FALSE(!value_status.ok())) {
    return MakeEvalError() << "numeric out of range: " << value;
  }
  return value_status.ValueOrDie();
}

template <typename H>
inline H AbslHashValue(H h, const NumericValue& v) {
  return H::combine(std::move(h), v.high_bits_, v.low_bits_);
}

template <typename T>
inline std::string TypeName();

template <>
inline std::string TypeName<int32_t>() {
  return "int32";
}

template <>
inline std::string TypeName<uint32_t>() {
  return "uint32";
}

template <>
inline std::string TypeName<int64_t>() {
  return "int64";
}

template <>
inline std::string TypeName<uint64_t>() {
  return "uint64";
}

template<class T>
inline zetasql_base::StatusOr<T> NumericValue::To() const {
  static_assert(std::is_same<T, int32_t>::value ||
                std::is_same<T, int64_t>::value ||
                std::is_same<T, uint32_t>::value ||
                std::is_same<T, uint64_t>::value,
                "In NumericValue::To<T>() T can only be one of "
                "int32, int64_t, uint32_t or uint64");

  __int128 value = as_packed_int();
  __int128 rounded_value = value > 0 ? value + kScalingFactor / 2 :
                                       value - kScalingFactor / 2;
  rounded_value /= kScalingFactor;
  if (ABSL_PREDICT_FALSE((rounded_value < std::numeric_limits<T>::min()) ||
                         (rounded_value > std::numeric_limits<T>::max()))) {
    return MakeEvalError() << TypeName<T>() << " out of range: " << ToString();
  }
  return static_cast<T>(rounded_value);
}

inline double NumericValue::ToDouble() const {
  __int128 value = as_packed_int();
  __int128 int_part = value / kScalingFactor;
  __int128 fract_part = value % kScalingFactor;
  return static_cast<double>(int_part) +
         static_cast<double>(fract_part) / kScalingFactor;
}

inline __int128 NumericValue::as_packed_int() const {
  return (static_cast<__int128>(high_bits_) << 64) + low_bits_;
}

inline uint64_t NumericValue::high_bits() const {
  return high_bits_;
}

inline uint64_t NumericValue::low_bits() const {
  return low_bits_;
}

inline bool NumericValue::has_fractional_part() const {
  return as_packed_int() % kScalingFactor != 0;
}

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_NUMERIC_VALUE_INL_H_
