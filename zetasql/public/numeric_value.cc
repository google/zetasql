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

#include "zetasql/public/numeric_value.h"

#include <ctype.h>
#include <stddef.h>
#include <string.h>
#include <sys/types.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/multiprecision_int.h"
#include "zetasql/public/numeric_parser.h"
#include "absl/base/optimization.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/optional.h"
#include "zetasql/base/endian.h"
#include "zetasql/base/stl_util.h"
#include "zetasql/base/mathutil.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {
using FormatFlag = NumericValue::FormatSpec::Flag;

constexpr uint8_t kGroupSize = 3;
constexpr char kGroupChar = ',';

// Returns -1, 0 or 1 if the given int128 number is negative, zero of positive
// respectively.
inline int int128_sign(__int128 x) {
  return (0 < x) - (x < 0);
}

inline unsigned __int128 int128_abs(__int128 x) {
  // Must cast to unsigned type before negation. Negation of signed integer
  // could overflow and has undefined behavior in C/C++.
  return (x >= 0) ? x : -static_cast<unsigned __int128>(x);
}

constexpr FixedUint<64, 1> NumericScalingFactorSquared() {
  return FixedUint<64, 1>(static_cast<uint64_t>(NumericValue::kScalingFactor) *
                          NumericValue::kScalingFactor);
}

constexpr FixedUint<64, 4> BigNumericScalingFactorSquared() {
  return FixedUint<64, 4>(std::array<uint64_t, 4>{0ULL, 0x7775a5f171951000ULL,
                                                  0x0764b4abe8652979ULL,
                                                  0x161bcca7119915b5ULL});
}

template <int n>
FixedInt<64, n * 2> GetScaledCovarianceNumerator(
    const FixedInt<64, n>& sum_x, const FixedInt<64, n>& sum_y,
    const FixedInt<64, n * 2 - 1>& sum_product, uint64_t count) {
  FixedInt<64, n * 2> numerator(sum_product);
  numerator *= count;
  numerator -= ExtendAndMultiply(sum_x, sum_y);
  return numerator;
}

template <int n, int m>
double Covariance(const FixedInt<64, n>& sum_x, const FixedInt<64, n>& sum_y,
                  const FixedInt<64, n * 2 - 1>& sum_product,
                  const FixedUint<64, m>& scaling_factor_square, uint64_t count,
                  uint64_t count_offset) {
  FixedInt<64, n* 2> numerator =
      GetScaledCovarianceNumerator(sum_x, sum_y, sum_product, count);
  FixedUint<64, m + 2> denominator(scaling_factor_square);
  denominator *= count;
  denominator *= (count - count_offset);
  return static_cast<double>(numerator) / static_cast<double>(denominator);
}

template <int n>
inline void SerializeFixedInt(std::string* dest, const FixedInt<64, n>& num) {
  num.SerializeToBytes(dest);
}

template <int n1, int... n>
inline void SerializeFixedInt(std::string* dest, const FixedInt<64, n1>& num1,
                              const FixedInt<64, n>&... num) {
  static_assert(sizeof(num1) < 128);
  size_t old_size = dest->size();
  dest->push_back('\0');  // add a place holder for size
  num1.SerializeToBytes(dest);
  ZETASQL_DCHECK_LE(dest->size() - old_size, 128);
  (*dest)[old_size] = static_cast<char>(dest->size() - old_size - 1);
  SerializeFixedInt(dest, num...);
}

template <int n>
bool DeserializeFixedInt(absl::string_view bytes, FixedInt<64, n>* num) {
  return num->DeserializeFromBytes(bytes);
}

template <int n1, int... n>
bool DeserializeFixedInt(absl::string_view bytes, FixedInt<64, n1>* num1,
                         FixedInt<64, n>*... num) {
  if (!bytes.empty()) {
    int len = bytes[0];
    return len < bytes.size() - 1 &&
           num1->DeserializeFromBytes(bytes.substr(1, len)) &&
           DeserializeFixedInt(bytes.substr(len + 1), num...);
  }
  return false;
}

// Helper method for appending a decimal value to a string. This function
// assumes the value is not zero, and the FixedInt string has already been
// appended to output. This function adds the decimal point and adjusts the
// leading and trailing zeros.
// Examples:
// (1, 9, 0, false, "-123") -> "-0.000000123"
// (1, 9, 0, false, "-123456789") -> "-0.123456789"
// (1, 9, 0, false, "-1234567890") -> "-1.23456789"
// (1, 9, 10, false, "-1234567890") -> "-1.2345678900"
// (1, 9, 0, false, "-1000000000") -> "-1"
// (1, 9, 0, true, "-1000000000") -> "-1."
// Returns the index of the decimal point (if decimal point was added) or
// output->size() otherwise.
size_t AddDecimalPointAndAdjustZeros(size_t first_digit_index, size_t scale,
                                     size_t min_num_fractional_digits,
                                     bool always_add_decimal_point,
                                     std::string* output) {
  size_t string_length = output->size();
  // Make a string_view that includes only the digits, so that find_last_not_of
  // does not search the substring before first_digit_index. This is for
  // performance instead of correctness. Note, std::string::find_last_not_of
  // doesn't have a signature that specifies the starting position.
  absl::string_view fixed_uint_str(*output);
  fixed_uint_str.remove_prefix(first_digit_index);
  size_t fixed_uint_length = fixed_uint_str.size();
  size_t zeros_to_truncate = 0;
  size_t num_fractional_digits = min_num_fractional_digits;
  if (min_num_fractional_digits < scale) {
    size_t num_trailing_zeros =
        fixed_uint_length - fixed_uint_str.find_last_not_of('0') - 1;
    zeros_to_truncate =
        std::min(num_trailing_zeros, scale - min_num_fractional_digits);
    output->resize(string_length - zeros_to_truncate);
    num_fractional_digits = scale - zeros_to_truncate;
  } else {
    output->append(min_num_fractional_digits - scale, '0');
  }

  size_t decimal_point_index = output->size();
  if (fixed_uint_length < scale + 1) {
    // Add zeros and decimal point if smaller than 1.
    output->insert(first_digit_index, scale + 2 - fixed_uint_length, '0');
    decimal_point_index = first_digit_index + 1;
    (*output)[decimal_point_index] = '.';
  } else if ((num_fractional_digits != 0) || always_add_decimal_point) {
    decimal_point_index -= num_fractional_digits;
    output->insert(output->begin() + decimal_point_index, '.');
  }
  return decimal_point_index;
}

// PowersAsc<Word, first_value, base, size>() returns a std::array<Word, size>
// {first_value, first_value * base, ..., first_value * pow(base, size - 1)}.
template <typename Word, Word first_value, Word base, int size, typename... T>
constexpr std::array<Word, size> PowersAsc(T... v) {
  if constexpr (sizeof...(T) < size) {
    return PowersAsc<Word, first_value, base, size>(first_value, v * base...);
  } else {
    return std::array<Word, size>{v...};
  }
}

// PowersDesc<Word, last_value, base, size>() returns a std::array<Word, size>
// {last_value * pow(base, size - 1), last_value * pow(base, size - 2), ...,
//  last_value}.
template <typename Word, Word last_value, Word base, int size, typename... T>
constexpr std::array<Word, size> PowersDesc(T... v) {
  if constexpr (sizeof...(T) < size) {
    return PowersDesc<Word, last_value, base, size>(v * base..., last_value);
  } else {
    return std::array<Word, size>{v...};
  }
}

// Computes static_cast<double>(value / kScalingFactor) with minimal precision
// loss.
double RemoveScaleAndConvertToDouble(__int128 value) {
  if (value == 0) {
    return 0;
  }
  using uint128 = unsigned __int128;
  uint128 abs_value = int128_abs(value);
  // binary_scaling_factor must be a power of 2, so that the division by it
  // never loses any precision.
  double binary_scaling_factor = 1;
  // Make sure abs_value has at least 96 significant bits, so that after
  // dividing by kScalingFactor, it has at least 64 significant bits
  // before conversion to double.
  if (abs_value < (uint128{1} << 96)) {
    if (abs_value >= (uint128{1} << 64)) {
      abs_value <<= 32;
      binary_scaling_factor = static_cast<double>(uint128{1} << 32);
    } else if (abs_value >= (uint128{1} << 32)) {
      abs_value <<= 64;
      binary_scaling_factor = static_cast<double>(uint128{1} << 64);
    } else {
      abs_value <<= 96;
      binary_scaling_factor = static_cast<double>(uint128{1} << 96);
    }
  }
  // FixedUint<64, 2> / std::integral_constant<uint32_t, *> is much faster than
  // uint128 / uint32_t.
  FixedUint<64, 2> tmp(abs_value);
  uint32_t remainder;
  tmp.DivMod(NumericValue::kScalingFactor, &tmp, &remainder);
  std::array<uint64_t, 2> n = tmp.number();
  // If the remainder is not 0, set the least significant bit to 1 so that the
  // round-to-even in static_cast<double>() will not treat the value as a tie
  // between 2 nearest double values.
  n[0] |= (remainder != 0);
  double result =
      static_cast<double>(FixedUint<64, 2>(n)) / binary_scaling_factor;
  return value >= 0 ? result : -result;
}

FixedUint<64, 4> UnsignedFloor(FixedUint<64, 4> value) {
  // Remove the decimal portion of the value by dividing by the
  // ScalingFactor(10^38) then multiplying correspondingly.
  // For efficiency, the division is split into 5^13, 5^13, 5^12 and 2^38,
  // and the multiplcation into 2^38, 5^19, 5^19.
  value /= std::integral_constant<uint32_t, internal::k5to13>();
  value /= std::integral_constant<uint32_t, internal::k5to13>();
  value /= std::integral_constant<uint32_t, internal::k5to12>();
  // Since dividing then multiplying by 2^38 is the same as shifting right
  // then left by 38, which just zeroes the low 38 bits we can do the equivalent
  // by directly masking the lower 38 bits.
  const std::array<uint64_t, 4>& a = value.number();
  value = FixedUint<64, 4>(
      std::array<uint64_t, 4>{a[0] & 0xFFFFFFC000000000, a[1], a[2], a[3]});
  value *= internal::k5to19;
  value *= internal::k5to19;
  return value;
}

FixedUint<64, 4> UnsignedCeiling(FixedUint<64, 4> value) {
  value += FixedUint<64, 4>(BigNumericValue::kScalingFactor - 1);
  return UnsignedFloor(value);
}

template <int n>
void ShiftRightAndRound(uint num_bits, FixedUint<64, n>* value) {
  ZETASQL_DCHECK_GT(num_bits, 0);
  constexpr uint kNumBits = n * 64;
  ZETASQL_DCHECK_LT(num_bits, kNumBits);
  uint bit_idx = num_bits - 1;
  uint64_t round_up = (value->number()[bit_idx / 64] >> (bit_idx % 64)) & 1;
  *value >>= num_bits;
  *value += round_up;
}

// SignedBinaryFraction and UnsignedBinaryFraction represent a fraction
// (x / pow(2, kFractionalBits)), where x is a FixedInt<64, kNumWords>
// or FixedUint<64, kNumWords>. These 2 classes are designed for advanced
// functions such as EXP and LN where there is 100% precise algorithm.
// Binary scale is used to make the operators much faster than decimal scale.
template <int kNumWords, int kFractionalBits>
class SignedBinaryFraction;

template <int kNumWords, int kFractionalBits>
class UnsignedBinaryFraction {
 public:
  using SignedType = SignedBinaryFraction<kNumWords, kFractionalBits>;
  static_assert(kNumWords * 64 > kFractionalBits);
  UnsignedBinaryFraction() {}
  explicit UnsignedBinaryFraction(uint64_t value) : value_(value) {
    value_ <<= kFractionalBits;
  }
  // Constructs an instance representing value * 2 ^ scale_bits.
  UnsignedBinaryFraction(uint64_t value, int scale_bits) : value_(value) {
    ZETASQL_DCHECK_GE(scale_bits, -kFractionalBits);
    ZETASQL_DCHECK_LE(kFractionalBits + scale_bits + 64, kNumWords * 64);
    value_ <<= (kFractionalBits + scale_bits);
  }
  static UnsignedBinaryFraction FromScaledValue(
      const FixedUint<64, kNumWords>& src) {
    UnsignedBinaryFraction result;
    result.value_ = src;
    return result;
  }
  bool To(bool is_negative, NumericValue* output) const {
    FixedUint<64, 2> result_abs;
    if (ABSL_PREDICT_TRUE(MulDivByScale(
            value_, FixedUint<64, 1>(NumericValue::kScalingFactor),
            &result_abs))) {
      unsigned __int128 v = static_cast<unsigned __int128>(result_abs);
      if (ABSL_PREDICT_TRUE(v <= internal::kNumericMax)) {
        __int128 packed = static_cast<__int128>(is_negative ? -v : v);
        *output = NumericValue::FromPackedInt(packed).value();
        return true;
      }
    }
    return false;
  }
  bool To(bool is_negative, BigNumericValue* output) const {
    FixedUint<64, 4> result_abs;
    FixedInt<64, 4> result;
    if (ABSL_PREDICT_TRUE(MulDivByScale(
            value_, FixedUint<64, 2>(BigNumericValue::kScalingFactor),
            &result_abs)) &&
        ABSL_PREDICT_TRUE(result.SetSignAndAbs(is_negative, result_abs))) {
      *output = BigNumericValue::FromPackedLittleEndianArray(result.number());
      return true;
    }
    return false;
  }

  // Similar to operator *=, but returns true iff no overflow.
  bool Multiply(const UnsignedBinaryFraction& rhs) {
    return this->MulDivByScale(value_, rhs.value_, &value_);
  }
  bool Inverse();
  template <int n>
  bool IntegerPower(FixedUint<64, n> exponent,
                    UnsignedBinaryFraction* output) const;
  bool FractionalPower(const SignedType& exponent,
                       UnsignedBinaryFraction* output) const;
  bool Ln(const UnsignedBinaryFraction& unit_of_last_precision,
          SignedType* output) const;
  bool Log10(const UnsignedBinaryFraction& unit_of_last_precision,
             SignedType* output) const;
  bool Log(const UnsignedBinaryFraction& base,
           const UnsignedBinaryFraction& unit_of_last_precision,
           SignedType* output) const;
  bool Sqrt(const UnsignedBinaryFraction& unit_of_last_precision,
            UnsignedBinaryFraction* output) const;

 private:
  friend SignedType;

  // Sets *output to lhs * rhs / pow(2, kFractionalBits), and returns
  // true iff there is no overflow.
  template <int n, int m>
  static bool MulDivByScale(const FixedUint<64, kNumWords>& lhs,
                            const FixedUint<64, n>& rhs,
                            FixedUint<64, m>* output);

  FixedUint<64, kNumWords> value_;
};

template <int kNumWords, int kFractionalBits>
template <int n, int m>
bool UnsignedBinaryFraction<kNumWords, kFractionalBits>::MulDivByScale(
    const FixedUint<64, kNumWords>& lhs, const FixedUint<64, n>& rhs,
    FixedUint<64, m>* output) {
  FixedUint<64, kNumWords + n> product = ExtendAndMultiply(lhs, rhs);
  ShiftRightAndRound(kFractionalBits, &product);
  for (int i = m; i < kNumWords + n - kFractionalBits / 64; ++i) {
    if (ABSL_PREDICT_FALSE(product.number()[i] != 0)) {
      return false;
    }
  }
  *output = FixedUint<64, m>(product);
  return true;
}

template <int kNumWords, int kFractionalBits>
template <int n>
bool UnsignedBinaryFraction<kNumWords, kFractionalBits>::IntegerPower(
    FixedUint<64, n> exponent, UnsignedBinaryFraction* output) const {
  UnsignedBinaryFraction power(*this);
  *output = UnsignedBinaryFraction(1);
  while (true) {
    if ((exponent.number()[0] & 1) != 0 &&
        ABSL_PREDICT_FALSE(!output->Multiply(power))) {
      return false;
    }
    exponent >>= 1;
    if (exponent.is_zero()) {
      return true;
    }
    if (ABSL_PREDICT_FALSE(!power.Multiply(power))) {
      return false;
    }
  }
}

template <int kNumWords, int kFractionalBits>
bool UnsignedBinaryFraction<kNumWords, kFractionalBits>::Sqrt(
    const UnsignedBinaryFraction& unit_of_last_precision,
    UnsignedBinaryFraction* output) const {
  if (value_.is_zero()) {
    *output = UnsignedBinaryFraction();
    return true;
  }
  if (value_ == UnsignedBinaryFraction(1).value_) {
    *output = UnsignedBinaryFraction(1);
    return true;
  }

  // For the algorithm to converge faster, here we calculate a initial estimate.
  // With a = r * 2^2t where 0.5 <= r < 2, the initial estimate of SQRT(a) is
  // (0.485 + 0.485 * r) * 2^t
  FixedUint<64, kNumWords> r = value_;
  uint msb_index = r.FindMSBSetNonZero();
  int p = msb_index - kFractionalBits;
  p = p % 2 == 0 ? p : p + 1;
  int t = p / 2;
  if (p < 0) {
    r <<= (-p);
  } else if (p > 0) {
    ShiftRightAndRound(p, &r);
  }
  // 0.485 is approximately equals to 31*2^(-6).
  UnsignedBinaryFraction factor(31, -6);
  FixedUint<64, kNumWords> estimated_sqrt;
  if (!ABSL_PREDICT_TRUE(
          this->MulDivByScale(factor.value_, r, &estimated_sqrt))) {
    return false;
  }
  estimated_sqrt += factor.value_;
  if (t < 0) {
    ShiftRightAndRound(-t, &estimated_sqrt);
  } else if (t > 0) {
    estimated_sqrt <<= t;
  }

  // The approximate value of SQRT is calculated by Babylonian's method:
  // y(n+1) = ( yn + x/yn ) / 2
  output->value_ = r;
  constexpr int n = kNumWords + (kFractionalBits + 63) / 64;
  FixedUint<64, n> scaled_value(value_);
  scaled_value <<= kFractionalBits;
  FixedUint<64, kNumWords>& y = output->value_;
  FixedInt<64, kNumWords> delta;
  do {
    delta = FixedInt<64, kNumWords>(y);
    FixedUint<64, n> ratio(scaled_value);
    // When x is large and t is large, with initial value close to (0.485 +
    // 0.485 * r) * 2^t, yn is expected to be larger than 2^(t-2) and less than
    // x/2, so that x/yn is less than x/(2^(t-2)). Thus yn + x/yn is always less
    // than x.
    ratio.DivAndRoundAwayFromZero(FixedUint<64, n>(y));
    y += FixedUint<64, kNumWords>(ratio);
    ShiftRightAndRound(1, &y);
    delta -= FixedInt<64, kNumWords>(y);
  } while (delta.abs() >= unit_of_last_precision.value_);
  return true;
}

template <int kNumWords, int kFractionalBits>
class SignedBinaryFraction {
 public:
  using UnsignedType = UnsignedBinaryFraction<kNumWords, kFractionalBits>;
  SignedBinaryFraction() {}
  explicit SignedBinaryFraction(const NumericValue& src) {
    FixedInt<64, 2> src_number(src.as_packed_int());
    constexpr int n = 2 + (kFractionalBits + 63) / 64;
    FixedUint<64, n> src_abs(src_number.abs());
    src_abs <<= kFractionalBits;
    src_abs.DivAndRoundAwayFromZero(NumericValue::kScalingFactor);
    static_assert(kNumWords * 64 - kFractionalBits >= 98);
    // max(src_abs) < 10^29 * 2^kFractionalBits
    // < 2^(97 + kFractionalBits)
    // <= 2^(kNumWords * 64 - 1)
    value_ = FixedInt<64, kNumWords>(src_abs);
    if (src_number.is_negative()) {
      value_ = -value_;
    }
  }
  explicit SignedBinaryFraction(const BigNumericValue& src) {
    FixedInt<64, 4> src_number(src.ToPackedLittleEndianArray());
    constexpr int n = 4 + (kFractionalBits + 63) / 64;
    FixedUint<64, n> src_abs(src_number.abs());
    src_abs <<= kFractionalBits;
    FixedInt<64, n - 1> result_abs(
        BigNumericValue::RemoveScalingFactor</* round = */ true>(src_abs));
    static_assert(kNumWords * 64 - kFractionalBits >= 130);
    // max(result_abs) = 2^(255 + kFractionalBits) / 10^38
    // < 2^(255 + kFractionalBits) / 2^126 = 2^(129 + kFractionalBits)
    // <= 2^(kNumWords * 64 - 1)
    value_ = FixedInt<64, kNumWords>(result_abs);
    if (src_number.is_negative()) {
      value_ = -value_;
    }
  }
  UnsignedType Abs() const {
    return UnsignedType::FromScaledValue(value_.abs());
  }
  bool Exp(UnsignedType* output) const;
  template <typename T>
  bool To(T* output) const {
    return Abs().To(value_.is_negative(), output);
  }
  bool Multiply(const SignedBinaryFraction& rhs) {
    FixedUint<64, kNumWords> result_abs;
    bool result_is_negative = value_.is_negative() != rhs.value_.is_negative();
    return UnsignedType::MulDivByScale(value_.abs(), rhs.value_.abs(),
                                       &result_abs) &&
           value_.SetSignAndAbs(result_is_negative, result_abs);
  }

 private:
  friend UnsignedType;

  FixedInt<64, kNumWords> value_;
};

template <int kNumWords, int kFractionalBits>
bool SignedBinaryFraction<kNumWords, kFractionalBits>::Exp(
    UnsignedType* output) const {
  *output = UnsignedType(1);
  if (value_.is_zero()) {
    return true;
  }
  // For faster convergence, here we are calculating:
  // e^x = e^(r*2^t) = (e^r)^(2^t), where r < 1/8 and t >= 0
  const bool r_is_negative = value_.is_negative();
  UnsignedType r_abs = Abs();
  uint msb_index = r_abs.value_.FindMSBSetNonZero();
  uint t = 0;
  if (msb_index > kFractionalBits - 4) {
    t = msb_index - (kFractionalBits - 4);
    ShiftRightAndRound(t, &r_abs.value_);
  }
  // e^r is calculating with Taylor Series:
  // e^r = 1 + r + (r^2)/2! + (r^3)/3! + (r^4)/4! + ...
  uint64_t n = 0;
  UnsignedType term(1);
  bool term_is_negative = false;
  while (true) {
    if (ABSL_PREDICT_FALSE(!term.Multiply(r_abs))) {
      return false;
    }
    n++;
    term.value_.DivAndRoundAwayFromZero(n);
    if (term.value_.is_zero()) {
      break;
    }
    term_is_negative ^= r_is_negative;
    if (term_is_negative) {
      output->value_ -= term.value_;
    } else {
      output->value_ += term.value_;
    }
  }

  for (uint i = 0; i < t; ++i) {
    if (ABSL_PREDICT_FALSE(!output->Multiply(*output))) {
      return false;
    }
  }
  return true;
}

// Returns x * ln(2) * pow(2, scale_bits). scale_bits cannot exceed 320.
FixedUint<64, 6> MultiplyByScaledLn2(uint64_t x, uint scale_bits) {
  // kScaledLn2 = ROUND(ln(2) * pow(2, 320)).
  static constexpr FixedUint<64, 5> kScaledLn2(std::array<uint64_t, 5>{
      0xe7b876206debac98, 0x8a0d175b8baafa2b, 0x40f343267298b62d,
      0xc9e3b39803f2f6af, 0xb17217f7d1cf79ab});
  ZETASQL_DCHECK_LE(scale_bits, 320);
  FixedUint<64, 6> result = ExtendAndMultiply(kScaledLn2, FixedUint<64, 1>(x));
  ShiftRightAndRound(320 - scale_bits, &result);
  return result;
}

template <int kNumWords, int kFractionalBits>
bool UnsignedBinaryFraction<kNumWords, kFractionalBits>::Ln(
    const UnsignedBinaryFraction& unit_of_last_precision,
    SignedType* output) const {
  if (value_ == UnsignedBinaryFraction(1).value_) {
    *output = SignedType();
    return true;
  }
  // For the algorithm to converge faster, here we calculate
  // ln(a) = ln(r * 2^t) = ln(r) + t*ln(2), where 1 <= r < 2.
  FixedUint<64, kNumWords> r = value_;
  uint msb_index = r.FindMSBSetNonZero();
  int t = msb_index - kFractionalBits;
  if (t < 0) {
    r <<= (-t);
  } else if (t > 0) {
    ShiftRightAndRound(t, &r);
  }

  // The approximate value of Ln is calculated by Halley's method:
  // y(n+1) = yn + 2 * ( r - exp(yn) )/( r + exp(yn) )
  // When r ~= 1, ln(r) ~= r - 1, so initialize y0 = r - 1;
  output->value_ = FixedInt<64, kNumWords>(r);
  output->value_ -= (FixedInt<64, kNumWords>(1) <<= kFractionalBits);
  // The Hally's method has cubic convergence, it is expected to converge within
  // 6 iterations. Thus, to be safe we allow at most 12 iterations here.
  // Because r < 2, exp(r) < 8, and the scaled value is far lower
  // than the max value of UnsignedBinaryFraction. No need to check overflow in
  // the operators in the loop.
  for (int i = 0; i < 12; i++) {
    UnsignedBinaryFraction exp_y;
    if (ABSL_PREDICT_FALSE(!output->Exp(&exp_y))) {
      return false;
    }
    if (r == exp_y.value_) {
      break;
    }
    FixedInt<64, kNumWords> r_minus_exp_y(r);
    r_minus_exp_y -= FixedInt<64, kNumWords>(exp_y.value_);
    bool r_less_than_exp_y = r_minus_exp_y.is_negative();
    FixedUint<64, kNumWords> r_minus_exp_y_abs_times_2 = r_minus_exp_y.abs();
    r_minus_exp_y_abs_times_2 <<= 1;
    FixedUint<64, kNumWords> r_plus_exp_y(r);
    r_plus_exp_y += exp_y.value_;
    constexpr int n = kNumWords + (kFractionalBits + 63) / 64;
    FixedUint<64, n> ratio(r_minus_exp_y_abs_times_2);
    ratio <<= kFractionalBits;
    ratio.DivAndRoundAwayFromZero(FixedUint<64, n>(r_plus_exp_y));
    FixedUint<64, kNumWords> delta_abs(ratio);
    if (r_less_than_exp_y) {
      output->value_ -= FixedInt<64, kNumWords>(delta_abs);
    } else {
      output->value_ += FixedInt<64, kNumWords>(delta_abs);
    }
    if (delta_abs < unit_of_last_precision.value_) {
      break;
    }
  }

  if (t != 0) {
    FixedInt<64, kNumWords> offset_abs(
        MultiplyByScaledLn2(std::abs(t), kFractionalBits));
    if (t > 0) {
      output->value_ += offset_abs;
    } else {
      output->value_ -= offset_abs;
    }
  }
  return true;
}

template <int kNumWords, int kFractionalBits>
bool UnsignedBinaryFraction<kNumWords, kFractionalBits>::Log10(
    const UnsignedBinaryFraction& unit_of_last_precision,
    SignedType* output) const {
  // Calculate log10(a) = ln(a)*(1/ln(10)) by changing base.
  SignedType ln;
  if (!ABSL_PREDICT_TRUE(Ln(unit_of_last_precision, &ln))) {
    return false;
  }
  bool result_is_negative = ln.value_.is_negative();
  // kInversedScaledLn2 = ROUND(1/ln(10) * pow(2, 320)).
  static constexpr FixedUint<64, 5> kScaledLn2(std::array<uint64_t, 5>{
      4224701343442500089ULL, 2098561575983469214ULL, 2265771312819785985ULL,
      11145799226051128857ULL, 8011319160293570762ULL});
  FixedUint<64, kNumWords + 5> result_abs =
      ExtendAndMultiply(kScaledLn2, ln.value_.abs());
  ShiftRightAndRound(320, &result_abs);
  // There is no need to check cast overflow as 1/ln10 ~= 0.4343
  return output->value_.SetSignAndAbs(result_is_negative,
                                      FixedUint<64, kNumWords>(result_abs));
}

template <int kNumWords, int kFractionalBits>
bool UnsignedBinaryFraction<kNumWords, kFractionalBits>::Log(
    const UnsignedBinaryFraction& base,
    const UnsignedBinaryFraction& unit_of_last_precision,
    SignedType* output) const {
  // Calculate log_b(a) = ln(a)/ln(b) by changing base.
  SignedType ln_a;
  SignedType ln_b;
  if (!ABSL_PREDICT_TRUE(Ln(unit_of_last_precision, &ln_a)) ||
      !ABSL_PREDICT_TRUE(base.Ln(unit_of_last_precision, &ln_b)) ||
      ABSL_PREDICT_FALSE(ln_b.value_.is_zero())) {
    return false;
  }
  bool result_is_negative =
      ln_a.value_.is_negative() != ln_b.value_.is_negative();
  constexpr int n = kNumWords + (kFractionalBits + 63) / 64;
  FixedUint<64, n> result_abs(ln_a.value_.abs());
  result_abs <<= kFractionalBits;
  result_abs.DivAndRoundAwayFromZero(FixedUint<64, n>(ln_b.value_.abs()));
  for (int i = kNumWords; i < n; ++i) {
    if (ABSL_PREDICT_FALSE(result_abs.number()[i] != 0)) {
      return false;
    }
  }
  return output->value_.SetSignAndAbs(result_is_negative,
                                      FixedUint<64, kNumWords>(result_abs));
}

template <int kNumWords, int kFractionalBits>
bool UnsignedBinaryFraction<kNumWords, kFractionalBits>::FractionalPower(
    const SignedType& exponent, UnsignedBinaryFraction* output) const {
  // max_error_bits will be used in LN iteration ending condition.
  // pow(2, 4 - kFractionalBits) is chosen here to provide enough
  // precision, and also provide a loose range to avoid precision lost in
  // exp(yn) affecting the convergence.
  UnsignedBinaryFraction unit_of_last_precision(1, 4 - kFractionalBits);
  SignedType ln;
  // Here pow(x,y) is calculated as x^y=exp(y*ln(x))
  return (ABSL_PREDICT_TRUE(Ln(unit_of_last_precision, &ln)) &&
          ABSL_PREDICT_TRUE(ln.Multiply(exponent)) &&
          ABSL_PREDICT_TRUE(ln.Exp(output)));
}

template <int kNumWords, int kFractionalBits>
bool UnsignedBinaryFraction<kNumWords, kFractionalBits>::Inverse() {
  if (value_.is_zero()) {
    return false;
  }
  constexpr int n = std::max(kNumWords, kFractionalBits / 32 + 1);
  FixedUint<64, n> result(uint64_t{1});
  result <<= (kFractionalBits * 2);
  result.DivAndRoundAwayFromZero(FixedUint<64, n>(value_));
  for (int i = kNumWords; i < n; ++i) {
    if (ABSL_PREDICT_FALSE(result.number()[i] != 0)) {
      return false;
    }
  }
  value_ = FixedUint<64, kNumWords>(result);
  return true;
}

template <int kNumWords, int kMaxIntegerWords, int kBinaryFractionWords,
          int kBinaryFractionalBits, typename T>
absl::StatusOr<T> PowerInternal(const T& base, const T& exp) {
  constexpr absl::string_view type_name =
      std::is_same_v<T, BigNumericValue> ? "BIGNUMERIC" : "numeric";
  // For the cases where POW(base, exy) is equivalent as multiplication or
  // division of at most 2 T values, avoid conversion to
  // SignedBinaryFraction.
  if (exp == T(2)) {  // most common use case
    auto status_or_result = base.Multiply(base);
    if (ABSL_PREDICT_TRUE(status_or_result.ok())) {
      return status_or_result;
    }
    return MakeEvalError() << type_name << " overflow";
  }
  if (exp == T()) {
    return T(1);
  }
  if (exp == T(1)) {
    return base;
  }

  if (base == T()) {
    // An attempt to raise zero to a negative power results in division by zero.
    if (exp.Sign() < 0) {
      return MakeEvalError() << "division by zero";
    }
    // Otherwise zero raised to any power is still zero.
    return T();
  }
  if (exp == T(-1)) {
    return T(1).Divide(base);
  }

  FixedUint<64, kNumWords> extended_abs_integer_exp;
  FixedUint<64, kNumWords> extended_abs_fract_exp;
  FixedInt<64, kNumWords>(exp.ToPackedLittleEndianArray())
      .abs()
      .DivMod(FixedUint<64, kNumWords>(T::kScalingFactor),
              &extended_abs_integer_exp, &extended_abs_fract_exp);
  FixedUint<64, kMaxIntegerWords> abs_integer_exp(extended_abs_integer_exp);
  __int128 scaled_fract_exp =
      static_cast<unsigned __int128>(extended_abs_fract_exp);
  if (exp.Sign() < 0) {
    scaled_fract_exp = -scaled_fract_exp;
  }
  bool result_is_negative = false;
  if (base.Sign() < 0) {
    if (scaled_fract_exp != 0) {
      return MakeEvalError() << "Negative " << absl::AsciiStrToUpper(type_name)
                             << " value cannot be raised to a fractional power";
    }
    result_is_negative = (abs_integer_exp.number()[0] & 1) != 0;
  }
  using UnsignedFraction =
      UnsignedBinaryFraction<kBinaryFractionWords, kBinaryFractionalBits>;
  using SignedFraction =
      SignedBinaryFraction<kBinaryFractionWords, kBinaryFractionalBits>;
  UnsignedFraction base_binary_frac = SignedFraction(base).Abs();
  UnsignedFraction result;
  if (!abs_integer_exp.is_zero()) {
    if (exp.Sign() >= 0) {
      if (!base_binary_frac.IntegerPower(abs_integer_exp, &result)) {
        return MakeEvalError() << type_name << " overflow";
      }
    } else {
      FixedUint<64, kNumWords> value_abs =
          FixedInt<64, kNumWords>(base.ToPackedLittleEndianArray()).abs();
      if (value_abs > FixedUint<64, kNumWords>(T::kScalingFactor)) {
        // If the exponent is negative and value_abs is > 1, then we compute
        // 1 / (value_abs ^ (-integer_exp)) for integer part.
        if (!base_binary_frac.IntegerPower(abs_integer_exp, &result)) {
          return T();
        }
        if (!result.Inverse()) {
          return zetasql_base::InternalErrorBuilder()
                 << "Inverse of a value greater than 1 should not fail.";
        }
      } else {
        // If the exponent is negative and value_abs is < 1, then we compute
        // (1 / value_abs) ^ (-integer_exp).
        // Instead of calling base.Inverse(), we combine the conversion to
        // binary scale and the inversion for better precision.
        FixedUint<64, kBinaryFractionWords> scaled_inverse(T::kScalingFactor);
        scaled_inverse <<= kBinaryFractionalBits;
        scaled_inverse.DivAndRoundAwayFromZero(
            FixedUint<64, kBinaryFractionWords>(value_abs));
        base_binary_frac = UnsignedFraction::FromScaledValue(scaled_inverse);
        if (!base_binary_frac.IntegerPower(abs_integer_exp, &result)) {
          return MakeEvalError() << type_name << " overflow";
        }
        scaled_fract_exp = -scaled_fract_exp;
      }
    }
  }
  if (scaled_fract_exp != 0) {
    UnsignedFraction frac_result;
    if (ABSL_PREDICT_FALSE(!base_binary_frac.FractionalPower(
            SignedFraction(T::FromScaledValue(scaled_fract_exp)),
            &frac_result))) {
      return zetasql_base::InternalErrorBuilder()
             << "Fractional Power should never "
                "overflow with exponent less than 1";
    }
    if (abs_integer_exp.is_zero()) {
      result = frac_result;
    } else if (ABSL_PREDICT_FALSE(!result.Multiply(frac_result))) {
      return MakeEvalError() << type_name << " overflow";
    }
  }

  T output;
  if (ABSL_PREDICT_TRUE(result.To(result_is_negative, &output))) {
    return output;
  }
  return MakeEvalError() << type_name << " overflow";
}

}  // namespace

absl::StatusOr<NumericValue> NumericValue::FromStringStrict(
    absl::string_view str) {
  return FromStringInternal</*is_strict=*/true>(str);
}

absl::StatusOr<NumericValue> NumericValue::FromString(absl::string_view str) {
  return FromStringInternal</*is_strict=*/false>(str);
}

size_t NumericValue::HashCode() const {
  return absl::Hash<NumericValue>()(*this);
}

void NumericValue::AppendToString(std::string* output) const {
  if (as_packed_int() == 0) {
    output->push_back('0');
    return;
  }
  size_t old_size = output->size();
  FixedInt<64, 2> value(as_packed_int());
  value.AppendToString(output);
  size_t first_digit_index = old_size + value.is_negative();
  AddDecimalPointAndAdjustZeros(first_digit_index, kMaxFractionalDigits, 0,
                                false, output);
}

// Parses a textual representation of a NUMERIC value. Returns an error if the
// given string cannot be parsed as a number or if the textual numeric value
// exceeds NUMERIC precision. If 'is_strict' is true then the function will
// return an error if there are more that 9 digits in the fractional part,
// otherwise the number will be rounded to contain no more than 9 fractional
// digits.
template <bool is_strict>
absl::StatusOr<NumericValue> NumericValue::FromStringInternal(
    absl::string_view str) {
  constexpr uint8_t word_count = 2;
  FixedPointRepresentation<word_count> parsed;
  absl::Status parse_status = ParseNumeric<is_strict>(str, parsed);
  if (ABSL_PREDICT_TRUE(parse_status.ok())) {
    auto number_or_status = FromFixedUint(parsed.output, parsed.is_negative);
    if (number_or_status.ok()) {
      return number_or_status;
    }
  }
  return MakeEvalError() << "Invalid NUMERIC value: " << str;
}

double NumericValue::ToDouble() const {
  return RemoveScaleAndConvertToDouble(as_packed_int());
}

inline unsigned __int128 ScaleMantissa(uint64_t mantissa, uint32_t scale) {
  return static_cast<unsigned __int128>(mantissa) * scale;
}

inline FixedUint<64, 4> ScaleMantissa(uint64_t mantissa,
                                      unsigned __int128 scale) {
  return ExtendAndMultiply(FixedUint<64, 2>(mantissa), FixedUint<64, 2>(scale));
}

template <typename S, typename T>
bool ScaleAndRoundAwayFromZero(S scale, double value, T* result) {
  if (value == 0) {
    *result = T();
    return true;
  }
  constexpr int kNumOutputBits = sizeof(T) * 8;
  zetasql_base::MathUtil::DoubleParts parts = zetasql_base::MathUtil::Decompose(value);
  ZETASQL_DCHECK_NE(parts.mantissa, 0) << value;
  if (parts.exponent <= -kNumOutputBits) {
    *result = T();
    return true;
  }
  // Because mantissa != 0, parts.exponent >= kNumOutputBits - 1 would mean that
  // (abs_mantissa * scale) << parts.exponent will exceed kNumOutputBits - 1
  // bits. Note, the most significant bit in abs_result cannot be set, or the
  // sign of *result will be wrong. We do not need to exempt the special case
  // of 1 << (kNumOutputBits - 1) which might keep the sign correct, because
  // <scale> is not a power of 2 and thus abs_result is never equal to
  // 1 << (kNumOutputBits - 1).
  if (ABSL_PREDICT_FALSE(parts.exponent >= kNumOutputBits - 1)) {
    return false;
  }
  bool negative = parts.mantissa < 0;
  uint64_t abs_mantissa =
      negative ? -static_cast<uint64_t>(parts.mantissa) : parts.mantissa;
  auto abs_result = ScaleMantissa(abs_mantissa, scale);
  static_assert(sizeof(abs_result) == sizeof(T));
  if (parts.exponent < 0) {
    abs_result >>= -1 - parts.exponent;
    abs_result += uint64_t{1};  // round away from zero
    abs_result >>= 1;
  } else if (parts.exponent > 0) {
    int msb_idx =
        FixedUint<64, kNumOutputBits / 64>(abs_result).FindMSBSetNonZero();
    if (ABSL_PREDICT_FALSE(msb_idx >= kNumOutputBits - 1 - parts.exponent)) {
      return false;
    }
    abs_result <<= parts.exponent;
  }
  static_assert(sizeof(T) > sizeof(S) + sizeof(uint64_t));
  // Because sizeof(T) is bigger than sizeof(S) + sizeof(uint64_t), the sign bit
  // of abs_result cannot be 1 when parts.exponent = 0. Same for the cases
  // where parts.exponent != 0. Therefore, we do not need to check overflow in
  // negation.
  T rv(abs_result);
  ZETASQL_DCHECK(rv >= T()) << value;
  *result = negative ? -rv : rv;
  return true;
}

absl::StatusOr<NumericValue> NumericValue::FromDouble(double value) {
  if (ABSL_PREDICT_FALSE(!std::isfinite(value))) {
    // This error message should be kept consistent with the error message found
    // in .../public/functions/convert.h.
    if (std::isnan(value)) {
      // Don't show the negative sign for -nan values.
      value = std::numeric_limits<double>::quiet_NaN();
    }
    return MakeEvalError() << "Illegal conversion of non-finite floating point "
                              "number to numeric: "
                           << value;
  }
  __int128 result = 0;
  if (ScaleAndRoundAwayFromZero(kScalingFactor, value, &result)) {
    absl::StatusOr<NumericValue> value_status = FromPackedInt(result);
    if (ABSL_PREDICT_TRUE(value_status.ok())) {
      return value_status;
    }
  }
  return MakeEvalError() << "numeric out of range: " << value;
}

absl::StatusOr<NumericValue> NumericValue::Multiply(NumericValue rh) const {
  const __int128 value = as_packed_int();
  const __int128 rh_value = rh.as_packed_int();
  bool negative = value < 0;
  bool rh_negative = rh_value < 0;
  FixedUint<64, 4> product =
      ExtendAndMultiply(FixedUint<64, 2>(int128_abs(value)),
                        FixedUint<64, 2>(int128_abs(rh_value)));

  // This value represents kNumericMax * kScalingFactor + kScalingFactor / 2.
  // At this value, <res> would be internal::kNumericMax + 1 and overflow.
  static constexpr FixedUint<64, 4> kOverflowThreshold(std::array<uint64_t, 4>{
      6450984253243169536ULL, 13015503840481697412ULL, 293873587ULL, 0ULL});
  if (ABSL_PREDICT_TRUE(product < kOverflowThreshold)) {
    // Now we need to adjust the scale of the result. With a 32-bit constant
    // divisor, the compiler is expected to emit no div instructions for the
    // code below. We care about div instructions because they are much more
    // expensive than multiplication (for example on Skylake throughput of a
    // 64-bit multiplication is 1 cycle, compared to ~80-95 cycles for a
    // division).
    product += kScalingFactor / 2;
    FixedUint<32, 5> res(product);
    res /= kScalingFactor;
    unsigned __int128 v = static_cast<unsigned __int128>(res);
    // We already checked the value range, so no need to call FromPackedInt.
    return NumericValue(
        static_cast<__int128>(negative == rh_negative ? v : -v));
  }
  return MakeEvalError() << "numeric overflow: " << ToString() << " * "
                         << rh.ToString();
}

NumericValue NumericValue::Abs() const {
  // The result is expected to be within the valid range.
  return NumericValue(static_cast<__int128>(int128_abs(as_packed_int())));
}

int NumericValue::Sign() const { return int128_sign(as_packed_int()); }

absl::StatusOr<NumericValue> NumericValue::Power(NumericValue exp) const {
  auto res_or_status = PowerInternal<2, 2, 3, 94>(*this, exp);
  if (res_or_status.ok()) {
    return res_or_status;
  }
  return zetasql_base::StatusBuilder(res_or_status.status()).SetAppend()
         << ": POW(" << ToString() << ", " << exp.ToString() << ")";
}

absl::StatusOr<NumericValue> NumericValue::Exp() const {
  SignedBinaryFraction<3, 94> base(*this);
  UnsignedBinaryFraction<3, 94> exp;
  NumericValue result;
  if (ABSL_PREDICT_TRUE(base.Exp(&exp)) &&
      ABSL_PREDICT_TRUE(exp.To(false, &result))) {
    return result;
  }
  return MakeEvalError() << "numeric overflow: EXP(" << ToString() << ")";
}

absl::StatusOr<NumericValue> NumericValue::Ln() const {
  if (as_packed_int() <= 0) {
    return MakeEvalError() << "LN is undefined for zero or negative value: LN("
                           << ToString() << ")";
  }
  UnsignedBinaryFraction<3, 94> exp = SignedBinaryFraction<3, 94>(*this).Abs();
  SignedBinaryFraction<3, 94> ln;
  // unit_of_last_precision is set to pow(2, -34) ~= 5.8e-11 here. In the
  // implementation of Ln with Halley's method, computation will stop when the
  // delta of the iteration is less than unit_of_last_precision. Thus, 5.8e-11
  // is set up here to provide enough precision for NumericValue and avoid
  // unnecessary computation.
  UnsignedBinaryFraction<3, 94> unit_of_last_precision(1, -34);
  NumericValue result;
  if (ABSL_PREDICT_TRUE(exp.Ln(unit_of_last_precision, &ln)) &&
      ABSL_PREDICT_TRUE(ln.To(&result))) {
    return result;
  }
  return zetasql_base::InternalErrorBuilder()
         << "LN should never overflow: LN(" << ToString() << ")";
}

absl::StatusOr<NumericValue> NumericValue::Log10() const {
  if (as_packed_int() <= 0) {
    return MakeEvalError()
           << "LOG10 is undefined for zero or negative value: LOG10("
           << ToString() << ")";
  }
  UnsignedBinaryFraction<3, 94> exp = SignedBinaryFraction<3, 94>(*this).Abs();
  SignedBinaryFraction<3, 94> log10;
  // unit_of_last_precision will be used in LN iteration ending condition.
  // pow(2, -34) ~= 5.8e-11 is chosen here to provide enough precision and avoid
  // unnecessary computation.
  UnsignedBinaryFraction<3, 94> unit_of_last_precision(1, -34);
  NumericValue result;
  if (ABSL_PREDICT_TRUE(exp.Log10(unit_of_last_precision, &log10)) &&
      ABSL_PREDICT_TRUE(log10.To(&result))) {
    return result;
  }
  return zetasql_base::InternalErrorBuilder()
         << "LOG10 should never overflow: LOG10(" << ToString() << ")";
}

absl::StatusOr<NumericValue> NumericValue::Log(NumericValue base) const {
  if (as_packed_int() <= 0 || base.as_packed_int() <= 0 ||
      base == NumericValue(1)) {
    return MakeEvalError() << "LOG is undefined for zero or negative value, or "
                              "when base equals 1: "

                              "LOG("
                           << ToString() << ", " << base.ToString() << ")";
  }
  UnsignedBinaryFraction<3, 94> abs_value =
      SignedBinaryFraction<3, 94>(*this).Abs();
  UnsignedBinaryFraction<3, 94> abs_base =
      SignedBinaryFraction<3, 94>(base).Abs();
  SignedBinaryFraction<3, 94> log;
  // unit_of_last_precision will be used in LN iteration ending condition.
  // pow(2, -90) is chosen here to provide enough precision, and also provide a
  // loose range to avoid precision lost in division.
  UnsignedBinaryFraction<3, 94> unit_of_last_precision(1, -90);
  NumericValue result;
  if (ABSL_PREDICT_TRUE(
          abs_value.Log(abs_base, unit_of_last_precision, &log)) &&
      ABSL_PREDICT_TRUE(log.To(&result))) {
    return result;
  }
  // A theoretical max is
  // ZETASQL_LOG(99999999999999999999999999999.999999999, 1.000000001) ~=
  // 66774967730.214808679 and theoretical min is
  // ZETASQL_LOG(99999999999999999999999999999.999999999, 0.999999999) ~=
  // -66774967663.439840983
  return zetasql_base::InternalErrorBuilder()
         << "LOG(NumericValue, NumericValue) should never overflow: "

            "LOG("
         << ToString() << ", " << base.ToString() << ")";
}

absl::StatusOr<NumericValue> NumericValue::Sqrt() const {
  if (as_packed_int() < 0) {
    return MakeEvalError() << "SQRT is undefined for negative value: SQRT("
                           << ToString() << ")";
  }
  UnsignedBinaryFraction<3, 94> value =
      SignedBinaryFraction<3, 94>(*this).Abs();
  UnsignedBinaryFraction<3, 94> sqrt;
  // unit_of_last_precision is set to pow(2, -34) ~= 5.8e-11 here. In the
  // implementation of Sqrt with Babylonian's method, computation will stop when
  // the delta of the iteration is less than unit_of_last_precision.
  // Thus, 5.8e-11 is set up here to provide enough precision for NumericValue
  // and avoid unnecessary computation.
  UnsignedBinaryFraction<3, 94> unit_of_last_precision(1, -34);
  NumericValue result;
  if (ABSL_PREDICT_TRUE(value.Sqrt(unit_of_last_precision, &sqrt)) &&
      ABSL_PREDICT_TRUE(sqrt.To(false, &result))) {
    return result;
  }
  return zetasql_base::InternalErrorBuilder()
         << "SQRT should never overflow: SQRT(" << ToString() << ")";
}

namespace {

template <uint32_t divisor, bool round_away_from_zero>
inline unsigned __int128 RoundOrTruncConst32(unsigned __int128 dividend) {
  if (round_away_from_zero) {
    dividend += divisor / 2;
  }
  uint32_t remainder;
  // This is much faster than "dividend % divisor".
  FixedUint<64, 2>(dividend).DivMod(std::integral_constant<uint32_t, divisor>(),
                                    nullptr, &remainder);
  return dividend - remainder;
}

// Rounds or truncates this NUMERIC value to the given number of decimal
// digits after the decimal point (or before the decimal point if 'digits' is
// negative), and returns the packed integer. If 'round_away_from_zero' is
// true, then rounds the result away from zero, and the result might be out of
// the range of valid NumericValue. If 'round_away_from_zero' is false, then
// the extra digits are discarded and the result is always in the valid range.
template <bool round_away_from_zero>
unsigned __int128 RoundOrTrunc(unsigned __int128 value, int64_t digits) {
  switch (digits) {
    // Fast paths for some common values of the second argument.
    case 0:
      return RoundOrTruncConst32<internal::k1e9, round_away_from_zero>(value);
    case 1:
      return RoundOrTruncConst32<100000000, round_away_from_zero>(value);
    case 2:
      return RoundOrTruncConst32<10000000, round_away_from_zero>(value);
    case 3:
      return RoundOrTruncConst32<1000000, round_away_from_zero>(value);
    case 4:  // Format("%e", x) for ABS(x) in [100.0, 1000.0)
      return RoundOrTruncConst32<100000, round_away_from_zero>(value);
    case 5:  // Format("%e", x) for ABS(x) in [10.0, 100.0)
      return RoundOrTruncConst32<10000, round_away_from_zero>(value);
    case 6:  // Format("%f", *) and Format("%e", x) for ABS(x) in [1.0, 10.0)
      return RoundOrTruncConst32<1000, round_away_from_zero>(value);
    default: {
      if (digits >= NumericValue::kMaxFractionalDigits) {
        // Rounding beyond the max number of supported fractional digits has no
        // effect.
        return value;
      }
      if (digits < -NumericValue::kMaxIntegerDigits) {
        // Rounding (kMaxIntegerDigits + 1) digits away results in zero.
        // Rounding kMaxIntegerDigits digits away might result in overflow
        // instead of zero.
        return 0;
      }
      constexpr uint kMaxDigits =
          NumericValue::kMaxFractionalDigits + NumericValue::kMaxIntegerDigits;
      static constexpr std::array<__int128, kMaxDigits> kTruncFactors =
          PowersDesc<__int128, 10, 10, kMaxDigits>();
      __int128 trunc_factor =
          kTruncFactors[digits + NumericValue::kMaxIntegerDigits];
      if (round_away_from_zero) {
        // The max result is < 1.5e38 < pow(2, 127); no need to check overflow.
        value += (trunc_factor >> 1);
      }
      value -= value % trunc_factor;
      return value;
    }
  }
}

inline void RoundInternal(FixedUint<64, 2>* input, int64_t digits) {
  *input = FixedUint<64, 2>(
      RoundOrTrunc<true>(static_cast<unsigned __int128>(*input), digits));
}
}  // namespace

absl::StatusOr<NumericValue> NumericValue::Round(int64_t digits) const {
  __int128 value = as_packed_int();
  if (value >= 0) {
    value = RoundOrTrunc</*round_away_from_zero*/ true>(value, digits);
    if (ABSL_PREDICT_TRUE(value <= internal::kNumericMax)) {
      return NumericValue(value);
    }
  } else {
    value = RoundOrTrunc</*round_away_from_zero*/ true>(-value, digits);
    if (ABSL_PREDICT_TRUE(value <= internal::kNumericMax)) {
      return NumericValue(-value);
    }
  }
  return MakeEvalError() << "numeric overflow: ROUND(" << ToString() << ", "
                         << digits << ")";
}

NumericValue NumericValue::Trunc(int64_t digits) const {
  __int128 value = as_packed_int();
  if (value >= 0) {
    // TRUNC never overflows.
    return NumericValue(static_cast<__int128>(
        RoundOrTrunc</*round_away_from_zero*/ false>(value, digits)));
  }
  return NumericValue(static_cast<__int128>(
      -RoundOrTrunc</*round_away_from_zero*/ false>(-value, digits)));
}

absl::StatusOr<NumericValue> NumericValue::Ceiling() const {
  __int128 value = as_packed_int();
  int64_t fract_part = GetFractionalPart();
  value -= fract_part > 0 ? fract_part - kScalingFactor : fract_part;
  auto res_status = NumericValue::FromPackedInt(value);
  if (res_status.ok()) {
    return res_status;
  }
  return MakeEvalError() << "numeric overflow: CEIL(" << ToString() << ")";
}

absl::StatusOr<NumericValue> NumericValue::Floor() const {
  __int128 value = as_packed_int();
  int64_t fract_part = GetFractionalPart();
  value -= fract_part < 0 ? fract_part + kScalingFactor : fract_part;
  auto res_status = NumericValue::FromPackedInt(value);
  if (res_status.ok()) {
    return res_status;
  }
  return MakeEvalError() << "numeric overflow: FLOOR(" << ToString() << ")";
}

absl::StatusOr<NumericValue> NumericValue::Divide(NumericValue rh) const {
  const __int128 value = as_packed_int();
  const __int128 rh_value = rh.as_packed_int();
  const bool is_negative = value < 0;
  const bool rh_is_negative = rh_value < 0;

  if (ABSL_PREDICT_TRUE(rh_value != 0)) {
    FixedUint<64, 3> dividend(int128_abs(value));
    unsigned __int128 divisor = int128_abs(rh_value);

    // To preserve the scale of the result we need to multiply the dividend by
    // the scaling factor first.
    dividend *= kScalingFactor;
    // Not using DivAndRoundAwayFromZero because the addition never overflows
    // and shifting unsigned __int128 is more efficient.
    dividend += FixedUint<64, 3>(divisor >> 1);
    dividend /= FixedUint<64, 3>(divisor);

    auto res_or_status =
        NumericValue::FromFixedUint(dividend, is_negative != rh_is_negative);
    if (ABSL_PREDICT_TRUE(res_or_status.ok())) {
      return res_or_status;
    }
    return zetasql_base::StatusBuilder(res_or_status.status()).SetAppend()
           << ": " << ToString() << " / " << rh.ToString();
  }
  return MakeEvalError() << "division by zero: " << ToString() << " / "
                         << rh.ToString();
}

absl::StatusOr<NumericValue> NumericValue::DivideToIntegralValue(
    NumericValue rh) const {
  __int128 rh_value = rh.as_packed_int();
  if (ABSL_PREDICT_TRUE(rh_value != 0)) {
    __int128 value = as_packed_int() / rh_value;
    if (ABSL_PREDICT_TRUE(value <= internal::kNumericMax / kScalingFactor) &&
        ABSL_PREDICT_TRUE(value >= internal::kNumericMin / kScalingFactor)) {
      return NumericValue(value * kScalingFactor);
    }
    return MakeEvalError() << "numeric overflow: DIV(" << ToString() << ", "
                           << rh.ToString() << ")";
  }
  return MakeEvalError() << "division by zero: DIV(" << ToString() << ", "
                         << rh.ToString() << ")";
}

absl::StatusOr<NumericValue> NumericValue::Mod(NumericValue rh) const {
  __int128 rh_value = rh.as_packed_int();
  if (ABSL_PREDICT_TRUE(rh_value != 0)) {
    return NumericValue(as_packed_int() % rh_value);
  }
  return MakeEvalError() << "division by zero: MOD(" << ToString() << ", "
                         << rh.ToString() << ")";
}

void NumericValue::SerializeAndAppendToProtoBytes(std::string* bytes) const {
  FixedInt<64, 2>(as_packed_int()).SerializeToBytes(bytes);
}

absl::StatusOr<NumericValue> NumericValue::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  FixedInt<64, 2> value;
  if (ABSL_PREDICT_TRUE(value.DeserializeFromBytes(bytes))) {
    return NumericValue::FromPackedInt(static_cast<__int128>(value));
  }
  return MakeEvalError() << "Invalid numeric encoding";
}

namespace {
void AppendZero(size_t fractional_size, bool always_print_decimal_point,
                std::string* output) {
  output->push_back('0');
  if (fractional_size > 0) {
    size_t decimal_point_pos = output->size();
    output->append(fractional_size + 1, '0');
    (*output)[decimal_point_pos] = '.';
  } else if (always_print_decimal_point) {
    output->push_back('.');
  }
}

void AppendExponent(int exponent, char e, std::string* output) {
  size_t size = output->size();
  zetasql_base::STLStringResizeUninitialized(output, size + 4);
  char exponent_sign = '+';
  if (exponent < 0) {
    exponent_sign = '-';
    exponent = -exponent;
  }
  ZETASQL_DCHECK_LE(exponent, 99);
  char* p = &(*output)[size];
  p[0] = e;
  p[1] = exponent_sign;
  p[2] = exponent / 10 + '0';
  p[3] = exponent % 10 + '0';
}

// Rounds to the Pow-th digit using 3 divisions of Factors,
// whose product must equal 5^Pow, where Pow < 64.
// If round_away_from_zero is false, the value is always rounded down.
template <int Pow, int Factor1, int Factor2, int Factor3,
          bool round_away_from_zero>
inline void RoundInternalFixedFactors(FixedUint<64, 4>* value) {
  *value /= std::integral_constant<uint32_t, Factor1>();
  *value /= std::integral_constant<uint32_t, Factor2>();
  *value /= std::integral_constant<uint32_t, Factor3>();
  if (round_away_from_zero && value->number()[0] & (1ULL << (Pow - 1))) {
    // Since the max value of value is 0x80... this
    // addition cannot overflow.
    *value += (uint64_t{1} << Pow);
  }
  // We need to divide by 2^Pow, but rather than do
  // value >>= Pow, value <<= Pow we instead zero the appropriate bits
  // using a mask with the high 64 - pow bits set and all others zeroed
  constexpr uint64_t mask = ~((uint64_t{1} << Pow) - 1);
  std::array<uint64_t, 4> array = value->number();
  array[0] &= mask;
  *value = FixedUint<64, 4>(array);
  // Following these multiplications, the max value could be is
  // pow(2, 255) + 5 * pow(10, 38) < pow(2, 256). The multiplications
  // never overflow, though the highest bit in the result might be 1.
  *value *= static_cast<uint64_t>(Factor1) * Factor2;
  *value *= Factor3;
}

// Rounds or truncates this BIGNUMERIC value to the given number of decimal
// digits after the decimal point (or before the decimal point if 'digits' is
// negative), setting result if overflow does not occur.
// If 'round_away_from_zero' is true, then the result rounds away from zero,
// and might be out of the range of valid BigNumericValues.
// If 'round_away_from_zero' is false, then the extra digits are discarded
// and the result is always in the valid range.
template <bool round_away_from_zero>
bool RoundOrTrunc(FixedUint<64, 4>* abs_value, int64_t digits) {
  switch (digits) {
    // Fast paths for some common values of the second argument.
    case 0:
      RoundInternalFixedFactors<38, internal::k5to13, internal::k5to13,
                                internal::k5to12, round_away_from_zero>(
          abs_value);
      break;
    case 1:
      RoundInternalFixedFactors<37, internal::k5to13, internal::k5to12,
                                internal::k5to12, round_away_from_zero>(
          abs_value);
      break;
    case 2:
      RoundInternalFixedFactors<36, internal::k5to12, internal::k5to12,
                                internal::k5to12, round_away_from_zero>(
          abs_value);
      break;
    case 3:
      RoundInternalFixedFactors<35, internal::k5to12, internal::k5to12,
                                internal::k5to11, round_away_from_zero>(
          abs_value);
      break;
    case 4:
      RoundInternalFixedFactors<34, internal::k5to12, internal::k5to11,
                                internal::k5to11, round_away_from_zero>(
          abs_value);
      break;
    case 5:
      RoundInternalFixedFactors<33, internal::k5to11, internal::k5to11,
                                internal::k5to11, round_away_from_zero>(
          abs_value);
      break;
    case 6:
      RoundInternalFixedFactors<32, internal::k5to11, internal::k5to11,
                                internal::k5to10, round_away_from_zero>(
          abs_value);
      break;
    default: {
      if (ABSL_PREDICT_FALSE(digits >= BigNumericValue::kMaxFractionalDigits)) {
        // Rounding beyond the max number of supported fractional digits has no
        // effect.
        return true;
      }

      if (ABSL_PREDICT_FALSE(digits < -BigNumericValue::kMaxIntegerDigits)) {
        // Rounding (kBigNumericMaxIntegerDigits + 1) digits away results in
        // zero. Rounding kBigNumericMaxIntegerDigits digits away might result
        // in overflow instead of zero.
        *abs_value = FixedUint<64, 4>();
        return true;
      }

      static constexpr std::array<unsigned __int128, 39> kPowers =
          PowersAsc<unsigned __int128, 5, 5, 39>();
      // Power of 10 to divide the abs_value by, this should correspond to
      // 38 - digits when digits is positive and abs(digits) when negative
      // since we do an initial division of 10^38.
      uint64_t pow;
      if (digits < 0) {
        pow = -digits;
        *abs_value = FixedUint<64, 4>(
            BigNumericValue::RemoveScalingFactor</* round = */ false>(
                *abs_value));
      } else {
        pow = 38 - digits;
      }

      *abs_value /= FixedUint<64, 4>(kPowers[pow - 1]);
      if (round_away_from_zero &&
          abs_value->number()[0] & (1ULL << (pow - 1))) {
        *abs_value += (uint64_t{1} << pow);
      }
      const uint64_t mask = ~((uint64_t{1} << pow) - 1);
      std::array<uint64_t, 4> array = abs_value->number();
      array[0] &= mask;
      *abs_value = FixedUint<64, 4>(array);
      *abs_value *= FixedUint<64, 4>(kPowers[pow - 1]);

      if (digits < 0) {
        *abs_value *= internal::k1e19;
        *abs_value *= internal::k1e19;
      }
    }
  }
  return !round_away_from_zero || !FixedInt<64, 4>(*abs_value).is_negative();
}

inline bool RoundInternal(FixedUint<64, 4>* input, int64_t digits) {
  return RoundOrTrunc<true>(input, digits);
}

// Helper function to add grouping chars to the integer portion of the numeric
// string.
static void AddGroupingChar(const size_t first_digit_index,
                            const size_t end_of_integer_index,
                            std::string* output) {
  size_t grouping_char_count =
      (end_of_integer_index - first_digit_index - 1) / kGroupSize;
  // Resize output to account for 'soon-to-be-added' grouping chars
  zetasql_base::STLStringResizeUninitialized(output,
                                    output->size() + grouping_char_count);
  // Step 1: Copy all the fractional digits and decimal point to the end of
  // string.
  size_t copy_to_index = output->size() - 1;
  size_t copy_from_index = copy_to_index - grouping_char_count;
  while (copy_from_index >= end_of_integer_index) {
    output->at(copy_to_index--) = output->at(copy_from_index--);
  }

  // Step 2: Copy integer digits, adding grouping character as needed)
  while (copy_from_index < copy_to_index) {
    for (uint8_t curr_group_size = 0; curr_group_size < kGroupSize;
         curr_group_size++) {
      output->at(copy_to_index--) = output->at(copy_from_index--);
    }
    output->at(copy_to_index--) = kGroupChar;
  }
}

template <int scale, int n>
void Format(NumericValue::FormatSpec spec, const FixedInt<64, n>& input,
            std::string* output) {
  const size_t old_size = output->size();
  FixedUint<64, n> abs = input.abs();
  // Determine the sign.
  if (input.is_negative()) {
    output->push_back('-');
  } else if (spec.format_flags & FormatFlag::ALWAYS_PRINT_SIGN) {
    output->push_back('+');
  } else if (spec.format_flags & FormatFlag::SIGN_SPACE) {
    output->push_back(' ');
  }
  const size_t first_digit_index = output->size();

  switch (spec.mode) {
    case NumericValue::FormatSpec::DEFAULT: {
      size_t fractional_size =
          (spec.format_flags &
           FormatFlag::REMOVE_TRAILING_ZEROS_AFTER_DECIMAL_POINT)
              ? 0
              : spec.precision;
      // Round to the expected fractional size. If precision is 0, the
      // string produced by ToString() will not have a decimal point.
      RoundInternal(&abs, spec.precision);
      if (abs.is_zero()) {
        AppendZero(fractional_size,
                   spec.format_flags & FormatFlag::ALWAYS_PRINT_DECIMAL_POINT,
                   output);
      } else {
        abs.AppendToString(output);
        size_t decimal_point_index = AddDecimalPointAndAdjustZeros(
            first_digit_index, scale, fractional_size,
            spec.format_flags & FormatFlag::ALWAYS_PRINT_DECIMAL_POINT, output);
        if (spec.format_flags & FormatFlag::USE_GROUPING_CHAR) {
          AddGroupingChar(first_digit_index, decimal_point_index, output);
        }
      }
    } break;
    case NumericValue::FormatSpec::E_NOTATION_LOWER_CASE:
    case NumericValue::FormatSpec::E_NOTATION_UPPER_CASE: {
      int exponent = 0;
      size_t fractional_size =
          (spec.format_flags &
           FormatFlag::REMOVE_TRAILING_ZEROS_AFTER_DECIMAL_POINT)
              ? 0
              : spec.precision;
      if (abs.is_zero()) {
        AppendZero(fractional_size,
                   spec.format_flags & FormatFlag::ALWAYS_PRINT_DECIMAL_POINT,
                   output);
      } else {
        int num_digits = abs.CountDecimalDigits();
        exponent = num_digits - 1 - scale;
        RoundInternal(&abs, static_cast<int64_t>(spec.precision) - exponent);
        const FixedUint<64, n>& next_power_of_10 =
            FixedUint<64, n>::PowerOf10(num_digits);
        // If abs is rounded up with one more digit, adjust exponent.
        // Example: to compute FORMAT("%.0e", 9.5e-8), 95 is rounded to 100;
        // "1e-7" should be returned instead of "10e-8".
        exponent += (abs >= next_power_of_10);
        abs.AppendToString(output);
        AddDecimalPointAndAdjustZeros(
            first_digit_index, scale + exponent, fractional_size,
            spec.format_flags & FormatFlag::ALWAYS_PRINT_DECIMAL_POINT, output);
      }
      AppendExponent(exponent, spec.mode, output);
    } break;
    case NumericValue::FormatSpec::GENERAL_FORMAT_LOWER_CASE:
    case NumericValue::FormatSpec::GENERAL_FORMAT_UPPER_CASE: {
      int64_t adjusted_precision = std::max<uint>(spec.precision, 1);
      int64_t fractional_size = adjusted_precision - 1;
      if (abs.is_zero()) {
        fractional_size =
            spec.format_flags &
                    FormatFlag::REMOVE_TRAILING_ZEROS_AFTER_DECIMAL_POINT
                ? 0
                : fractional_size;
        AppendZero(fractional_size,
                   spec.format_flags & FormatFlag::ALWAYS_PRINT_DECIMAL_POINT,
                   output);
      } else {
        int num_digits = abs.CountDecimalDigits();
        int exponent = num_digits - 1 - scale;
        RoundInternal(&abs, fractional_size - exponent);
        const FixedUint<64, n>& next_power_of_10 =
            FixedUint<64, n>::PowerOf10(num_digits);
        exponent += (abs >= next_power_of_10);
        abs.AppendToString(output);
        if (exponent >= -4 && exponent <= fractional_size) {
          // Use f-style.
          size_t end_integer_index = AddDecimalPointAndAdjustZeros(
              first_digit_index, scale,
              spec.format_flags &
                      FormatFlag::REMOVE_TRAILING_ZEROS_AFTER_DECIMAL_POINT
                  ? 0
                  : fractional_size - exponent,
              spec.format_flags & FormatFlag::ALWAYS_PRINT_DECIMAL_POINT,
              output);
          if (spec.format_flags & FormatFlag::USE_GROUPING_CHAR) {
            AddGroupingChar(first_digit_index, end_integer_index, output);
          }
        } else {
          // Use e-style.
          AddDecimalPointAndAdjustZeros(
              first_digit_index, scale + exponent,
              spec.format_flags &
                      FormatFlag::REMOVE_TRAILING_ZEROS_AFTER_DECIMAL_POINT
                  ? 0
                  : fractional_size,
              spec.format_flags & FormatFlag::ALWAYS_PRINT_DECIMAL_POINT,
              output);
          char mode = spec.mode - ('G' - 'E');
          AppendExponent(exponent, mode, output);
        }
      }
    }
  }

  const size_t inserted_size = output->size() - old_size;
  if (inserted_size < spec.minimum_size) {
    size_t padding_size = spec.minimum_size - inserted_size;
    // With space padding, the spaces come first. With zero-padding, the sign
    // comes first. Left justification takes precedence over the zero padding
    // flag.
    if (spec.format_flags & FormatFlag::LEFT_JUSTIFY) {
      output->append(padding_size, ' ');
    } else if (spec.format_flags & FormatFlag::ZERO_PAD) {
      output->insert(first_digit_index, padding_size, '0');
    } else {
      output->insert(old_size, padding_size, ' ');
    }
  }
}

absl::Status FromScaledValueOutOfRangeError(absl::string_view type_name,
                                            size_t input_len, int scale) {
  return MakeEvalError() << "Value is out of range after scaling to "
                         << type_name << " type; input length: " << input_len
                         << "; scale: " << scale;
}

absl::StatusOr<FixedInt<64, 4>> FixedIntFromScaledValue(
    absl::string_view little_endian_value, int scale, int max_integer_digits,
    int max_fractional_digits, bool allow_rounding,
    absl::string_view type_name) {
  const size_t original_input_len = little_endian_value.size();
  if (original_input_len == 0) {
    return FixedInt<64, 4>();
  }

  // Ignore trailing '\xff' or '\x00' bytes if they don't affect the value.
  // For example, '\xab\xff' is equivalent to '\xab'.
  const char* most_significant_byte = &little_endian_value.back();
  bool is_negative = (*most_significant_byte & '\x80') != 0;
  char extension_byte = is_negative ? '\xff' : '\x00';
  while (most_significant_byte > little_endian_value.data() &&
         *most_significant_byte == extension_byte &&
         ((most_significant_byte[-1] ^ extension_byte) & '\x80') == 0) {
    --most_significant_byte;
  }
  if (most_significant_byte == little_endian_value.data() &&
      *most_significant_byte == '\x0') {
    return FixedInt<64, 4>();
  }
  little_endian_value =
      absl::string_view(little_endian_value.data(),
                        most_significant_byte - little_endian_value.data() + 1);

  if (scale <= max_fractional_digits) {
    // Scale up the value.
    FixedInt<64, 4> value;
    if (scale <= -max_integer_digits ||
        !value.DeserializeFromBytes(little_endian_value)) {
      return FromScaledValueOutOfRangeError(type_name, original_input_len,
                                            scale);
    }
    int scale_up_digits = max_fractional_digits - scale;
    ZETASQL_DCHECK_GE(scale_up_digits, 0);
    ZETASQL_DCHECK_LT(scale_up_digits, max_fractional_digits + max_integer_digits);
    if (scale_up_digits > 0 &&
        value.MultiplyOverflow(FixedInt<64, 4>::PowerOf10(scale_up_digits))) {
      return FromScaledValueOutOfRangeError(type_name, original_input_len,
                                            scale);
    }
    return value;
  }

  // Scale down the value.
  std::vector<uint32_t> dividend(
      (little_endian_value.size() + sizeof(uint32_t) - 1) / sizeof(uint32_t));
  VarIntRef<32> var_int_ref(dividend);
  bool success = var_int_ref.DeserializeFromBytes(little_endian_value);
  ZETASQL_DCHECK(success);
  if (is_negative) {
    var_int_ref.Negate();
    if (dividend.back() == 0) {
      dividend.pop_back();
    }
  }

  int scale_down_digits = scale - max_fractional_digits;
  uint32_t remainder = 0;
  uint32_t divisor = internal::k1e9;
  // Compute dividend /= pow(10, scale_down_digits) by repeating
  // dividend /= uint32_t divisor. When scale_down_digits > 9, this loop is not
  // as efficient as fixed_int_internal::LongDiv, but this method is not
  // expected to be called in a performance-critical path.
  while (scale_down_digits > 0) {
    if (dividend.empty()) {
      return FixedInt<64, 4>();
    }
    remainder = 0;
    VarUintRef<32> var_int_ref(dividend);
    if (scale_down_digits >= 9) {
      remainder = var_int_ref.DivMod(
          std::integral_constant<uint32_t, internal::k1e9>());
    } else {
      divisor = FixedUint<32, 1>::PowerOf10(scale_down_digits).number()[0];
      remainder = var_int_ref.DivMod(divisor);
    }
    if (remainder != 0 && !allow_rounding) {
      return MakeEvalError()
             << "Value will lose precision after "
                "scaling down to "
             << type_name << " type; input length: " << original_input_len
             << "; scale: " << scale;
    }
    scale_down_digits -= 9;
    if (dividend.back() == 0) {
      dividend.pop_back();
    }
  }
  if (dividend.size() > 8) {
    return FromScaledValueOutOfRangeError(type_name, original_input_len, scale);
  }
  std::array<uint32_t, 8> src;
  auto itr = std::copy(dividend.begin(), dividend.end(), src.begin());
  std::fill(itr, src.end(), 0);
  FixedUint<64, 4> abs_value((FixedUint<32, 8>(src)));
  // Here half is rounded away from zero. divisor is always an even number.
  if (remainder >= (divisor >> 1) && abs_value.AddOverflow(uint64_t{1})) {
    return FromScaledValueOutOfRangeError(type_name, original_input_len, scale);
  }
  FixedInt<64, 4> value;
  if (!value.SetSignAndAbs(is_negative, abs_value)) {
    return FromScaledValueOutOfRangeError(type_name, original_input_len, scale);
  }
  return value;
}
}  // namespace

void NumericValue::FormatAndAppend(FormatSpec spec, std::string* output) const {
  Format<kMaxFractionalDigits>(spec, FixedInt<64, 2>(as_packed_int()), output);
}

std::ostream& operator<<(std::ostream& out, NumericValue value) {
  return out << value.ToString();
}

absl::StatusOr<NumericValue> NumericValue::Rescale(int scale,
                                                   bool allow_rounding) const {
  if (scale < 0 || scale > kMaxFractionalDigits) {
    return MakeEvalError() << absl::Substitute(
               "NUMERIC scale must be between 0 and $0 but got $1",
               kMaxFractionalDigits, scale);
  }

  const FixedInt<64, 2>& divisor =
      FixedInt<64, 2>::PowerOf10(kMaxFractionalDigits - scale);
  FixedInt<64, 2> scaled_value(as_packed_int());
  if (allow_rounding) {
    scaled_value.DivAndRoundAwayFromZero(divisor);
  } else {
    FixedInt<64, 2> remainder;
    scaled_value.DivMod(divisor, &scaled_value, &remainder);
    if (!remainder.is_zero()) {
      return MakeEvalError() << absl::Substitute(
                 "Value will lose precision after scaling down to a scale of "
                 "$0",
                 scale);
    }
  }
  return NumericValue(static_cast<__int128>(scaled_value));
}

absl::StatusOr<NumericValue> NumericValue::FromScaledLittleEndianValue(
    absl::string_view little_endian_value, int scale, bool allow_rounding) {
  FixedInt<64, 4> value;
  ZETASQL_ASSIGN_OR_RETURN(value, FixedIntFromScaledValue(
                              little_endian_value, scale, kMaxIntegerDigits,
                              kMaxFractionalDigits, allow_rounding, "NUMERIC"));
  auto res_status = NumericValue::FromFixedInt(value);
  if (res_status.ok()) {
    return res_status;
  }
  return FromScaledValueOutOfRangeError("NUMERIC", little_endian_value.size(),
                                        scale);
}

absl::StatusOr<NumericValue> NumericValue::SumAggregator::GetSum() const {
  auto res_status = NumericValue::FromFixedInt(sum_);
  if (res_status.ok()) {
    return res_status;
  }
  return MakeEvalError() << "numeric overflow: SUM";
}

absl::StatusOr<NumericValue> NumericValue::SumAggregator::GetAverage(
    uint64_t count) const {
  if (count == 0) {
    return MakeEvalError() << "division by zero: AVG";
  }

  FixedInt<64, 3> dividend = sum_;
  dividend.DivAndRoundAwayFromZero(count);

  auto res_status = NumericValue::FromFixedInt(dividend);
  if (res_status.ok()) {
    return res_status;
  }
  return MakeEvalError() << "numeric overflow: AVG";
}

void NumericValue::SumAggregator::SerializeAndAppendToProtoBytes(
    std::string* bytes) const {
  sum_.SerializeToBytes(bytes);
}

absl::StatusOr<NumericValue::SumAggregator>
NumericValue::SumAggregator::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  NumericValue::SumAggregator out;
  if (out.sum_.DeserializeFromBytes(bytes)) {
    return out;
  }
  return MakeEvalError() << "Invalid NumericValue::SumAggregator encoding";
}

void NumericValue::VarianceAggregator::Add(NumericValue value) {
  sum_ += FixedInt<64, 3>(value.as_packed_int());
  FixedInt<64, 2> v(value.as_packed_int());
  sum_square_ += FixedInt<64, 5>(ExtendAndMultiply(v, v));
}

void NumericValue::VarianceAggregator::Subtract(NumericValue value) {
  sum_ -= FixedInt<64, 3>(value.as_packed_int());
  FixedInt<64, 2> v(value.as_packed_int());
  sum_square_ -= FixedInt<64, 5>(ExtendAndMultiply(v, v));
}

absl::optional<double> NumericValue::VarianceAggregator::GetVariance(
    uint64_t count, bool is_sampling) const {
  uint64_t count_offset = is_sampling;
  if (count > count_offset) {
    return Covariance(sum_, sum_, sum_square_, NumericScalingFactorSquared(),
                      count, count_offset);
  }
  return absl::nullopt;
}

absl::optional<double> NumericValue::VarianceAggregator::GetStdDev(
    uint64_t count, bool is_sampling) const {
  uint64_t count_offset = is_sampling;
  if (count > count_offset) {
    return std::sqrt(Covariance(sum_, sum_, sum_square_,
                                NumericScalingFactorSquared(), count,
                                count_offset));
  }
  return absl::nullopt;
}

void NumericValue::VarianceAggregator::MergeWith(
    const VarianceAggregator& other) {
  sum_ += other.sum_;
  sum_square_ += other.sum_square_;
}

void NumericValue::VarianceAggregator::SerializeAndAppendToProtoBytes(
    std::string* bytes) const {
  SerializeFixedInt(bytes, sum_, sum_square_);
}

absl::StatusOr<NumericValue::VarianceAggregator>
NumericValue::VarianceAggregator::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  VarianceAggregator out;
  if (DeserializeFixedInt(bytes, &out.sum_, &out.sum_square_)) {
    return out;
  }
  return MakeEvalError() << "Invalid NumericValue::VarianceAggregator encoding";
}

void NumericValue::CovarianceAggregator::Add(NumericValue x,
                                             NumericValue y) {
  sum_x_ += FixedInt<64, 3>(x.as_packed_int());
  sum_y_ += FixedInt<64, 3>(y.as_packed_int());
  FixedInt<64, 2> x_num(x.as_packed_int());
  FixedInt<64, 2> y_num(y.as_packed_int());
  sum_product_ += FixedInt<64, 5>(ExtendAndMultiply(x_num, y_num));
}

void NumericValue::CovarianceAggregator::Subtract(NumericValue x,
                                                  NumericValue y) {
  sum_x_ -= FixedInt<64, 3>(x.as_packed_int());
  sum_y_ -= FixedInt<64, 3>(y.as_packed_int());
  FixedInt<64, 2> x_num(x.as_packed_int());
  FixedInt<64, 2> y_num(y.as_packed_int());
  sum_product_ -= FixedInt<64, 5>(ExtendAndMultiply(x_num, y_num));
}

absl::optional<double> NumericValue::CovarianceAggregator::GetCovariance(
    uint64_t count, bool is_sampling) const {
  uint64_t count_offset = is_sampling;
  if (count > count_offset) {
    return Covariance(sum_x_, sum_y_, sum_product_,
                      NumericScalingFactorSquared(), count, count_offset);
  }
  return absl::nullopt;
}

void NumericValue::CovarianceAggregator::MergeWith(
    const CovarianceAggregator& other) {
  sum_x_ += other.sum_x_;
  sum_y_ += other.sum_y_;
  sum_product_ += other.sum_product_;
}

void NumericValue::CovarianceAggregator::SerializeAndAppendToProtoBytes(
    std::string* bytes) const {
  SerializeFixedInt(bytes, sum_product_, sum_x_, sum_y_);
}

absl::StatusOr<NumericValue::CovarianceAggregator>
NumericValue::CovarianceAggregator::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  CovarianceAggregator out;
  if (DeserializeFixedInt(bytes, &out.sum_product_, &out.sum_x_, &out.sum_y_)) {
    return out;
  }
  return MakeEvalError()
         << "Invalid NumericValue::CovarianceAggregator encoding";
}

void NumericValue::CorrelationAggregator::Add(NumericValue x,
                                             NumericValue y) {
  cov_agg_.Add(x, y);
  FixedInt<64, 2> x_num(x.as_packed_int());
  FixedInt<64, 2> y_num(y.as_packed_int());
  sum_square_x_ += FixedInt<64, 5>(ExtendAndMultiply(x_num, x_num));
  sum_square_y_ += FixedInt<64, 5>(ExtendAndMultiply(y_num, y_num));
}

void NumericValue::CorrelationAggregator::Subtract(NumericValue x,
                                                  NumericValue y) {
  cov_agg_.Subtract(x, y);
  FixedInt<64, 2> x_num(x.as_packed_int());
  FixedInt<64, 2> y_num(y.as_packed_int());
  sum_square_x_ -= FixedInt<64, 5>(ExtendAndMultiply(x_num, x_num));
  sum_square_y_ -= FixedInt<64, 5>(ExtendAndMultiply(y_num, y_num));
}

absl::optional<double> NumericValue::CorrelationAggregator::GetCorrelation(
    uint64_t count) const {
  if (count > 1) {
    FixedInt<64, 6> numerator = GetScaledCovarianceNumerator(
        cov_agg_.sum_x_, cov_agg_.sum_y_, cov_agg_.sum_product_, count);
    FixedInt<64, 6> variance_numerator_x = GetScaledCovarianceNumerator(
        cov_agg_.sum_x_, cov_agg_.sum_x_, sum_square_x_, count);
    FixedInt<64, 6> variance_numerator_y = GetScaledCovarianceNumerator(
        cov_agg_.sum_y_, cov_agg_.sum_y_, sum_square_y_, count);
    FixedInt<64, 12> denominator_square =
        ExtendAndMultiply(variance_numerator_x, variance_numerator_y);
    return static_cast<double>(numerator) /
           std::sqrt(static_cast<double>(denominator_square));
  }
  return absl::nullopt;
}

void NumericValue::CorrelationAggregator::MergeWith(
    const CorrelationAggregator& other) {
  cov_agg_.MergeWith(other.cov_agg_);
  sum_square_x_ += other.sum_square_x_;
  sum_square_y_ += other.sum_square_y_;
}

void NumericValue::CorrelationAggregator::SerializeAndAppendToProtoBytes(
    std::string* bytes) const {
  SerializeFixedInt(bytes, cov_agg_.sum_product_, cov_agg_.sum_x_,
                    cov_agg_.sum_y_, sum_square_x_, sum_square_y_);
}

absl::StatusOr<NumericValue::CorrelationAggregator>
NumericValue::CorrelationAggregator::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  CorrelationAggregator out;
  if (DeserializeFixedInt(bytes, &out.cov_agg_.sum_product_,
                          &out.cov_agg_.sum_x_, &out.cov_agg_.sum_y_,
                          &out.sum_square_x_, &out.sum_square_y_)) {
    return out;
  }
  return MakeEvalError()
         << "Invalid NumericValue::CorrelationAggregator encoding";
}

inline absl::Status MakeInvalidBigNumericError(absl::string_view str) {
  return MakeEvalError() << "Invalid BIGNUMERIC value: " << str;
}

absl::StatusOr<BigNumericValue> BigNumericValue::Multiply(
    const BigNumericValue& rh) const {
  bool lh_negative = value_.is_negative();
  bool rh_negative = rh.value_.is_negative();
  FixedUint<64, 8> abs_result_64x8 =
      ExtendAndMultiply(value_.abs(), rh.value_.abs());
  if (ABSL_PREDICT_TRUE(abs_result_64x8.number()[6] == 0) &&
      ABSL_PREDICT_TRUE(abs_result_64x8.number()[7] == 0)) {
    FixedUint<64, 5> abs_result_64x5 = RemoveScalingFactor</* round = */ true>(
        FixedUint<64, 6>(abs_result_64x8));
    if (ABSL_PREDICT_TRUE(abs_result_64x5.number()[4] == 0)) {
      FixedInt<64, 4> result;
      FixedUint<64, 4> abs_result_64x4(abs_result_64x5);
      if (ABSL_PREDICT_TRUE(result.SetSignAndAbs(lh_negative != rh_negative,
                                                 abs_result_64x4))) {
        return BigNumericValue(result);
      }
    }
  }
  return MakeEvalError() << "BIGNUMERIC overflow: " << ToString() << " * "
                         << rh.ToString();
}

absl::StatusOr<BigNumericValue> BigNumericValue::Divide(
    const BigNumericValue& rh) const {
  bool lh_negative = value_.is_negative();
  bool rh_negative = rh.value_.is_negative();
  if (ABSL_PREDICT_TRUE(!rh.value_.is_zero())) {
    FixedUint<64, 4> abs_value = value_.abs();
    FixedUint<64, 6> rh_abs_value(rh.value_.abs());
    FixedUint<64, 6> scaled_abs_value =
        ExtendAndMultiply(abs_value, FixedUint<64, 2>(kScalingFactor));
    scaled_abs_value.DivAndRoundAwayFromZero(rh_abs_value);
    if (ABSL_PREDICT_TRUE(scaled_abs_value.number()[4] == 0 &&
                          scaled_abs_value.number()[5] == 0)) {
      FixedUint<64, 4> abs_result(scaled_abs_value);
      FixedInt<64, 4> result;
      if (ABSL_PREDICT_TRUE(
              result.SetSignAndAbs(lh_negative != rh_negative, abs_result))) {
        return BigNumericValue(result);
      }
    }
    return MakeEvalError() << "BIGNUMERIC overflow: " << ToString() << " / "
                           << rh.ToString();
  }
  return MakeEvalError() << "division by zero: " << ToString() << " / "
                         << rh.ToString();
}

absl::StatusOr<BigNumericValue> BigNumericValue::DivideToIntegralValue(
    const BigNumericValue& rh) const {
  if (ABSL_PREDICT_TRUE(!rh.value_.is_zero())) {
    bool lh_negative = value_.is_negative();
    bool rh_negative = rh.value_.is_negative();
    FixedUint<64, 4> abs_result = value_.abs();
    abs_result /= rh.value_.abs();
    bool overflow =
        abs_result.MultiplyOverflow(FixedUint<64, 4>(kScalingFactor));

    if (ABSL_PREDICT_TRUE(!overflow)) {
      FixedInt<64, 4> result;
      if (ABSL_PREDICT_TRUE(
              result.SetSignAndAbs(lh_negative != rh_negative, abs_result))) {
        return BigNumericValue(result);
      }
    }
    return MakeEvalError() << "BIGNUMERIC overflow: DIV(" << ToString() << ", "
                           << rh.ToString() << ")";
  }
  return MakeEvalError() << "division by zero: DIV(" << ToString() << ", "
                         << rh.ToString() << ")";
}

absl::StatusOr<BigNumericValue> BigNumericValue::Mod(
    const BigNumericValue& rh) const {
  if (ABSL_PREDICT_TRUE(!rh.value_.is_zero())) {
    FixedInt<64, 4> remainder = value_;
    remainder %= rh.value_;
    return BigNumericValue(remainder);
  }
  return MakeEvalError() << "division by zero: MOD(" << ToString() << ", "
                         << rh.ToString() << ")";
}

bool BigNumericValue::HasFractionalPart() const {
  FixedUint<64, 4> abs_value = value_.abs();
  // Check whether abs_value is a multiple of pow(2, 38).
  if ((abs_value.number()[0] & ((1ULL << 38) - 1)) != 0) return true;
  // Check whether abs_value is a multiple of pow(5, 38).
  uint32_t mod = 0;
  abs_value.DivMod(std::integral_constant<uint32_t, internal::k5to13>(),
                   &abs_value, &mod);
  if (mod != 0) return true;
  abs_value.DivMod(std::integral_constant<uint32_t, internal::k5to13>(),
                   &abs_value, &mod);
  if (mod != 0) return true;
  abs_value.DivMod(std::integral_constant<uint32_t, internal::k5to12>(),
                   &abs_value, &mod);
  return (mod != 0);
}

double BigNumericValue::RemoveScaleAndConvertToDouble(
    const FixedInt<64, 4>& value) {
  bool is_negative = value.is_negative();
  FixedUint<64, 4> abs_value = value.abs();
  int num_32bit_words = FixedUint<32, 8>(abs_value).NonZeroLength();
  static constexpr std::array<uint32_t, 14> kPowersOf5 =
      PowersAsc<uint32_t, 1, 5, 14>();
  double binary_scaling_factor = 1;
  // To ensure precision, the number should have more than 54 bits after scaled
  // down by the all factors as 5 in scaling factor (5^38, 89 bits). Since
  // dividing the double by 2 won't produce precision loss, the value can be
  // divided by 5 factors in the scaling factor for 3 times, and divided by all
  // 2 factors in the scaling factor and binary scaling factor after converted
  // to double.
  switch (num_32bit_words) {
    case 0:
      return 0;
    case 1:
      abs_value <<= 144;
      // std::exp2, std::pow and std::ldexp are not constexpr.
      // Use static_cast from integers to compute the value at compile time.
      binary_scaling_factor = static_cast<double>(__int128{1} << 100) *
                              static_cast<double>(__int128{1} << 82);
      break;
    case 2:
      abs_value <<= 112;
      binary_scaling_factor = static_cast<double>(__int128{1} << 100) *
                              static_cast<double>(__int128{1} << 50);
      break;
    case 3:
      abs_value <<= 80;
      binary_scaling_factor = static_cast<double>(__int128{1} << 118);
      break;
    case 4:
      abs_value <<= 48;
      binary_scaling_factor = static_cast<double>(__int128{1} << 86);
      break;
    case 5:
      abs_value <<= 16;
      binary_scaling_factor = static_cast<double>(__int128{1} << 54);
      break;
    default:
      // shifting bits <= 0
      binary_scaling_factor = static_cast<double>(__int128{1} << 38);
  }
  uint32_t remainder_bits;
  abs_value.DivMod(std::integral_constant<uint32_t, kPowersOf5[13]>(),
                   &abs_value, &remainder_bits);
  uint32_t remainder;
  abs_value.DivMod(std::integral_constant<uint32_t, kPowersOf5[13]>(),
                   &abs_value, &remainder);
  remainder_bits |= remainder;
  abs_value.DivMod(std::integral_constant<uint32_t, kPowersOf5[12]>(),
                   &abs_value, &remainder);
  remainder_bits |= remainder;
  std::array<uint64_t, 4> n = abs_value.number();
  n[0] |= (remainder_bits != 0);
  double result =
      static_cast<double>(FixedUint<64, 4>(n)) / binary_scaling_factor;
  return is_negative ? -result : result;
}

absl::StatusOr<BigNumericValue> BigNumericValue::Round(int64_t digits) const {
  FixedUint<64, 4> abs_value = value_.abs();
  if (ABSL_PREDICT_TRUE(RoundInternal(&abs_value, digits))) {
    FixedInt<64, 4> result(abs_value);
    return BigNumericValue(!value_.is_negative() ? result : -result);
  }
  return MakeEvalError() << "BIGNUMERIC overflow: ROUND(" << ToString() << ", "
                         << digits << ")";
}

BigNumericValue BigNumericValue::Trunc(int64_t digits) const {
  FixedUint<64, 4> abs_value = value_.abs();
  RoundOrTrunc</*round_away_from_zero*/ false>(&abs_value, digits);
  FixedInt<64, 4> result(abs_value);
  return BigNumericValue(!value_.is_negative() ? result : -result);
}

absl::StatusOr<BigNumericValue> BigNumericValue::Floor() const {
  if (!value_.is_negative()) {
    FixedInt<64, 4> floor_value(UnsignedFloor(value_.abs()));
    return BigNumericValue(floor_value);
  }
  // UnsignedCeiling cannot overflow, however it can return a FixedUint which
  // is out of range as the same FixedInt. Because the constructor simply
  // copies the underlying bits we can check the high bit, i.e. is_negative
  FixedInt<64, 4> ceiling_value(UnsignedCeiling(value_.abs()));
  if (ABSL_PREDICT_TRUE(!ceiling_value.is_negative())) {
    return BigNumericValue(-ceiling_value);
  }
  return MakeEvalError() << "BIGNUMERIC overflow: FLOOR(" << ToString() << ")";
}

absl::StatusOr<BigNumericValue> BigNumericValue::Ceiling() const {
  if (!value_.is_negative()) {
    FixedInt<64, 4> ceiling_value(UnsignedCeiling(value_.abs()));
    // UnsignedCeiling cannot overflow, however it can return a FixedUint which
    // is out of range as the same FixedInt. Because the constructor simply
    // copies the underlying bits we can check the high bit, i.e. is_negative
    if (ABSL_PREDICT_TRUE(!ceiling_value.is_negative())) {
      return BigNumericValue(ceiling_value);
    }
    return MakeEvalError() << "BIGNUMERIC overflow: CEIL(" << ToString() << ")";
  }
  FixedInt<64, 4> floor_value(UnsignedFloor(value_.abs()));
  return BigNumericValue(-floor_value);
}

absl::StatusOr<BigNumericValue> BigNumericValue::Power(
    const BigNumericValue& exp) const {
  auto res_or_status = PowerInternal<4, 3, 6, 254>(*this, exp);
  if (res_or_status.ok()) {
    return res_or_status;
  }
  return zetasql_base::StatusBuilder(res_or_status.status()).SetAppend()
         << ": POW(" << ToString() << ", " << exp.ToString() << ")";
}

absl::StatusOr<BigNumericValue> BigNumericValue::Exp() const {
  SignedBinaryFraction<6, 254> base(*this);
  UnsignedBinaryFraction<6, 254> exp;
  BigNumericValue result;
  if (ABSL_PREDICT_TRUE(base.Exp(&exp)) &&
      ABSL_PREDICT_TRUE(exp.To(false, &result))) {
    return result;
  }
  return MakeEvalError() << "BIGNUMERIC overflow: EXP(" << ToString() << ")";
}

absl::StatusOr<BigNumericValue> BigNumericValue::Ln() const {
  if (value_.is_negative() || value_.is_zero()) {
    return MakeEvalError() << "LN is undefined for zero or negative value: LN("
                           << ToString() << ")";
  }
  UnsignedBinaryFraction<6, 254> exp =
      SignedBinaryFraction<6, 254>(*this).Abs();
  SignedBinaryFraction<6, 254> ln;
  // unit_of_last_precision is set to pow(2, -144) ~= 4.5e-44 here. In the
  // implementation of Ln with Halley's mothod, computation will stop when the
  // delta of the iteration is less than unit_of_last_precision. Thus, 4.5e-44
  // is set up here to provide enough precision for BigNumericValue and avoid
  // unnecessary computation.
  UnsignedBinaryFraction<6, 254> unit_of_last_precision(1, -144);
  BigNumericValue result;
  if (ABSL_PREDICT_TRUE(exp.Ln(unit_of_last_precision, &ln)) &&
      ABSL_PREDICT_TRUE(ln.To(&result))) {
    return result;
  }
  return zetasql_base::InternalErrorBuilder()
         << "LN should never overflow: LN(" << ToString() << ")";
}

absl::StatusOr<BigNumericValue> BigNumericValue::Log10() const {
  if (value_.is_negative() || value_.is_zero()) {
    return MakeEvalError()
           << "LOG10 is undefined for zero or negative value: LOG10("
           << ToString() << ")";
  }
  UnsignedBinaryFraction<6, 254> exp =
      SignedBinaryFraction<6, 254>(*this).Abs();
  SignedBinaryFraction<6, 254> log10;
  // unit_of_last_precision will be used in LN iteration ending condition.
  // pow(2, -144) ~= 4.5e-44 is chosen here to provide enough precision and
  // avoid unnecessary computation.
  UnsignedBinaryFraction<6, 254> unit_of_last_precision(1, -144);
  BigNumericValue result;
  if (ABSL_PREDICT_TRUE(exp.Log10(unit_of_last_precision, &log10)) &&
      ABSL_PREDICT_TRUE(log10.To(&result))) {
    return result;
  }
  return zetasql_base::InternalErrorBuilder()
         << "LOG10 should never overflow: LOG10(" << ToString() << ")";
}

absl::StatusOr<BigNumericValue> BigNumericValue::Log(
    const BigNumericValue& base) const {
  if (value_.is_negative() || value_.is_zero() || base.value_.is_negative() ||
      base.value_.is_zero() || base == BigNumericValue(1)) {
    return MakeEvalError() << "LOG is undefined for zero or negative value, or "
                              "when base equals 1: "

                              "LOG("
                           << ToString() << ", " << base.ToString() << ")";
  }
  UnsignedBinaryFraction<6, 254> abs_value =
      SignedBinaryFraction<6, 254>(*this).Abs();
  UnsignedBinaryFraction<6, 254> abs_base =
      SignedBinaryFraction<6, 254>(base).Abs();
  SignedBinaryFraction<6, 254> log;
  // unit_of_last_precision will be used in LN iteration ending condition.
  // pow(2, -250) is chosen here to provide enough precision, and also provide a
  // loose range to avoid precision lost in division.
  UnsignedBinaryFraction<6, 254> unit_of_last_precision(1, -250);
  BigNumericValue result;
  if (ABSL_PREDICT_TRUE(
          abs_value.Log(abs_base, unit_of_last_precision, &log)) &&
      ABSL_PREDICT_TRUE(log.To(&result))) {
    return result;
  }
  return MakeEvalError() << "BIGNUMERIC overflow: "

                            "LOG("
                         << ToString() << ", " << base.ToString() << ")";
}

absl::StatusOr<BigNumericValue> BigNumericValue::Sqrt() const {
  if (value_.is_negative()) {
    return MakeEvalError() << "SQRT is undefined for negative value: SQRT("
                           << ToString() << ")";
  }

  UnsignedBinaryFraction<6, 254> value =
      SignedBinaryFraction<6, 254>(*this).Abs();
  UnsignedBinaryFraction<6, 254> sqrt;
  // unit_of_last_precision is set to pow(2, -144) ~= 4.5e-44 here. In the
  // implementation of Sqrt with Babylonian's mothod, computation will stop when
  // the delta of the iteration is less than unit_of_last_precision.
  // Thus, 4.5e-44 is set up here to provide enough precision for
  // BigNumericValue and avoid unnecessary computation.
  UnsignedBinaryFraction<6, 254> unit_of_last_precision(1, -144);
  BigNumericValue result;
  if (ABSL_PREDICT_TRUE(value.Sqrt(unit_of_last_precision, &sqrt)) &&
      ABSL_PREDICT_TRUE(sqrt.To(false, &result))) {
    return result;
  }
  return zetasql_base::InternalErrorBuilder()
         << "SQRT should never overflow: SQRT(" << ToString() << ")";
}

// Parses a textual representation of a BIGNUMERIC value. Returns an error if
// the given string cannot be parsed as a number or if the textual numeric value
// exceeds BIGNUMERIC range. If 'is_strict' is true then the function will
// return an error if there are more that 38 digits in the fractional part,
// otherwise the number will be rounded to contain no more than 38 fractional
// digits.
template <bool is_strict>
absl::StatusOr<BigNumericValue> BigNumericValue::FromStringInternal(
    absl::string_view str) {
  constexpr uint8_t word_count = 4;
  BigNumericValue result;
  FixedPointRepresentation<word_count> parsed;
  absl::Status parse_status = ParseBigNumeric<is_strict>(str, parsed);
  if (ABSL_PREDICT_TRUE(parse_status.ok()) &&
      ABSL_PREDICT_TRUE(
          result.value_.SetSignAndAbs(parsed.is_negative, parsed.output))) {
    return result;
  }
  return MakeInvalidBigNumericError(str);
}

absl::StatusOr<BigNumericValue> BigNumericValue::FromStringStrict(
    absl::string_view str) {
  return FromStringInternal</*is_strict=*/true>(str);
}

absl::StatusOr<BigNumericValue> BigNumericValue::FromString(
    absl::string_view str) {
  return FromStringInternal</*is_strict=*/false>(str);
}

size_t BigNumericValue::HashCode() const {
  return absl::Hash<BigNumericValue>()(*this);
}

absl::StatusOr<BigNumericValue> BigNumericValue::FromDouble(double value) {
  if (ABSL_PREDICT_FALSE(!std::isfinite(value))) {
    // This error message should be kept consistent with the error message found
    // in .../public/functions/convert.h.
    if (std::isnan(value)) {
      // Don't show the negative sign for -nan values.
      value = std::numeric_limits<double>::quiet_NaN();
    }
    return MakeEvalError() << "Illegal conversion of non-finite floating point "
                              "number to BIGNUMERIC: "
                           << value;
  }
  FixedInt<64, 4> result;
  if (ScaleAndRoundAwayFromZero(kScalingFactor, value, &result)) {
    return BigNumericValue(result);
  }
  return MakeEvalError() << "BIGNUMERIC out of range: " << value;
}

void BigNumericValue::AppendToString(std::string* output) const {
  if (value_.is_zero()) {
    output->push_back('0');
    return;
  }
  size_t old_size = output->size();
  value_.AppendToString(output);
  size_t first_digit_index = old_size + value_.is_negative();
  AddDecimalPointAndAdjustZeros(first_digit_index, kMaxFractionalDigits, 0,
                                false, output);
}

void BigNumericValue::SerializeAndAppendToProtoBytes(std::string* bytes) const {
  value_.SerializeToBytes(bytes);
}

absl::StatusOr<BigNumericValue> BigNumericValue::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  BigNumericValue out;
  if (out.value_.DeserializeFromBytes(bytes)) {
    return out;
  }
  return MakeEvalError() << "Invalid BIGNUMERIC encoding";
}

void BigNumericValue::FormatAndAppend(FormatSpec spec,
                                      std::string* output) const {
  Format<kMaxFractionalDigits>(spec, value_, output);
}

std::ostream& operator<<(std::ostream& out, const BigNumericValue& value) {
  return out << value.ToString();
}

absl::StatusOr<BigNumericValue> BigNumericValue::FromScaledLittleEndianValue(
    absl::string_view little_endian_value, int scale, bool allow_rounding) {
  FixedInt<64, 4> value;
  ZETASQL_ASSIGN_OR_RETURN(
      value, FixedIntFromScaledValue(little_endian_value, scale,
                                     kMaxIntegerDigits, kMaxFractionalDigits,
                                     allow_rounding, "BIGNUMERIC"));
  return BigNumericValue(value);
}

absl::StatusOr<BigNumericValue> BigNumericValue::Rescale(
    int scale, bool allow_rounding) const {
  if (scale < 0 || scale > kMaxFractionalDigits) {
    return MakeEvalError() << absl::Substitute(
               "BIGNUMERIC scale must be between 0 and $0 but got $1",
               kMaxFractionalDigits, scale);
  }

  const FixedInt<64, 4>& divisor =
      FixedInt<64, 4>::PowerOf10(kMaxFractionalDigits - scale);
  FixedInt<64, 4> scaled_value(value_);
  if (allow_rounding) {
    scaled_value.DivAndRoundAwayFromZero(divisor);
  } else {
    FixedInt<64, 4> remainder;
    scaled_value.DivMod(divisor, &scaled_value, &remainder);
    if (!remainder.is_zero()) {
      return MakeEvalError() << absl::Substitute(
                 "Value will lose precision after scaling down to a scale of "
                 "$0",
                 scale);
    }
  }
  return BigNumericValue(scaled_value);
}

absl::StatusOr<BigNumericValue> BigNumericValue::SumAggregator::GetSum() const {
  if (sum_.number()[4] ==
      static_cast<uint64_t>(static_cast<int64_t>(sum_.number()[3]) >> 63)) {
    FixedInt<64, 4> sum_trunc(sum_);
    return BigNumericValue(sum_trunc);
  }
  return MakeEvalError() << "BIGNUMERIC overflow: SUM";
}

absl::StatusOr<BigNumericValue> BigNumericValue::SumAggregator::GetAverage(
    uint64_t count) const {
  if (count == 0) {
    return MakeEvalError() << "division by zero: AVG";
  }

  FixedInt<64, 5> dividend = sum_;
  dividend.DivAndRoundAwayFromZero(count);
  if (ABSL_PREDICT_TRUE(
          dividend.number()[4] ==
          static_cast<uint64_t>(static_cast<int64_t>(dividend.number()[3]) >>
                                63))) {
    FixedInt<64, 4> dividend_trunc(dividend);
    return BigNumericValue(dividend_trunc);
  }

  return MakeEvalError() << "BIGNUMERIC overflow: AVG";
}

void BigNumericValue::SumAggregator::SerializeAndAppendToProtoBytes(
    std::string* bytes) const {
  sum_.SerializeToBytes(bytes);
}

absl::StatusOr<BigNumericValue::SumAggregator>
BigNumericValue::SumAggregator::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  BigNumericValue::SumAggregator out;
  if (out.sum_.DeserializeFromBytes(bytes)) {
    return out;
  }
  return MakeEvalError() << "Invalid BigNumericValue::SumAggregator encoding";
}

void BigNumericValue::VarianceAggregator::Add(BigNumericValue value) {
  const FixedInt<64, 4>& v = value.value_;
  sum_ += FixedInt<64, 5>(v);
  sum_square_ += FixedInt<64, 9>(ExtendAndMultiply(v, v));
}

void BigNumericValue::VarianceAggregator::Subtract(BigNumericValue value) {
  const FixedInt<64, 4>& v = value.value_;
  sum_ -= FixedInt<64, 5>(v);
  sum_square_ -= FixedInt<64, 9>(ExtendAndMultiply(v, v));
}

absl::optional<double> BigNumericValue::VarianceAggregator::GetVariance(
    uint64_t count, bool is_sampling) const {
  uint64_t count_offset = is_sampling;
  if (count > count_offset) {
    return Covariance(sum_, sum_, sum_square_, BigNumericScalingFactorSquared(),
                      count, count_offset);
  }
  return absl::nullopt;
}

absl::optional<double> BigNumericValue::VarianceAggregator::GetStdDev(
    uint64_t count, bool is_sampling) const {
  uint64_t count_offset = is_sampling;
  if (count > count_offset) {
    return std::sqrt(Covariance(sum_, sum_, sum_square_,
                                BigNumericScalingFactorSquared(), count,
                                count_offset));
  }
  return absl::nullopt;
}

void BigNumericValue::VarianceAggregator::MergeWith(
    const VarianceAggregator& other) {
  sum_ += other.sum_;
  sum_square_ += other.sum_square_;
}

void BigNumericValue::VarianceAggregator::SerializeAndAppendToProtoBytes(
    std::string* bytes) const {
  SerializeFixedInt(bytes, sum_, sum_square_);
}

absl::StatusOr<BigNumericValue::VarianceAggregator>
BigNumericValue::VarianceAggregator::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  VarianceAggregator out;
  if (DeserializeFixedInt(bytes, &out.sum_, &out.sum_square_)) {
    return out;
  }
  return MakeEvalError()
         << "Invalid BigNumericValue::VarianceAggregator encoding";
}

void BigNumericValue::CovarianceAggregator::Add(BigNumericValue x,
                                                BigNumericValue y) {
  sum_x_ += FixedInt<64, 5>(x.value_);
  sum_y_ += FixedInt<64, 5>(y.value_);
  sum_product_ += FixedInt<64, 9>(ExtendAndMultiply(x.value_, y.value_));
}

void BigNumericValue::CovarianceAggregator::Subtract(BigNumericValue x,
                                                     BigNumericValue y) {
  sum_x_ -= FixedInt<64, 5>(x.value_);
  sum_y_ -= FixedInt<64, 5>(y.value_);
  sum_product_ -= FixedInt<64, 9>(ExtendAndMultiply(x.value_, y.value_));
}

absl::optional<double> BigNumericValue::CovarianceAggregator::GetCovariance(
    uint64_t count, bool is_sampling) const {
  uint64_t count_offset = is_sampling;
  if (count > count_offset) {
    return Covariance(sum_x_, sum_y_, sum_product_,
                      BigNumericScalingFactorSquared(), count, count_offset);
  }
  return absl::nullopt;
}

void BigNumericValue::CovarianceAggregator::MergeWith(
    const CovarianceAggregator& other) {
  sum_x_ += other.sum_x_;
  sum_y_ += other.sum_y_;
  sum_product_ += other.sum_product_;
}

void BigNumericValue::CovarianceAggregator::SerializeAndAppendToProtoBytes(
    std::string* bytes) const {
  SerializeFixedInt(bytes, sum_product_, sum_x_, sum_y_);
}

absl::StatusOr<BigNumericValue::CovarianceAggregator>
BigNumericValue::CovarianceAggregator::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  CovarianceAggregator out;
  if (DeserializeFixedInt(bytes, &out.sum_product_, &out.sum_x_, &out.sum_y_)) {
    return out;
  }
  return MakeEvalError()
         << "Invalid BigNumericValue::CovarianceAggregator encoding";
}

void BigNumericValue::CorrelationAggregator::Add(BigNumericValue x,
                                                 BigNumericValue y) {
  cov_agg_.Add(x, y);
  sum_square_x_ += FixedInt<64, 9>(ExtendAndMultiply(x.value_, x.value_));
  sum_square_y_ += FixedInt<64, 9>(ExtendAndMultiply(y.value_, y.value_));
}

void BigNumericValue::CorrelationAggregator::Subtract(BigNumericValue x,
                                                      BigNumericValue y) {
  cov_agg_.Subtract(x, y);
  sum_square_x_ -= FixedInt<64, 9>(ExtendAndMultiply(x.value_, x.value_));
  sum_square_y_ -= FixedInt<64, 9>(ExtendAndMultiply(y.value_, y.value_));
}

absl::optional<double> BigNumericValue::CorrelationAggregator::GetCorrelation(
    uint64_t count) const {
  if (count > 1) {
    FixedInt<64, 10> numerator = GetScaledCovarianceNumerator(
        cov_agg_.sum_x_, cov_agg_.sum_y_, cov_agg_.sum_product_, count);
    FixedInt<64, 10> variance_numerator_x = GetScaledCovarianceNumerator(
        cov_agg_.sum_x_, cov_agg_.sum_x_, sum_square_x_, count);
    FixedInt<64, 10> variance_numerator_y = GetScaledCovarianceNumerator(
        cov_agg_.sum_y_, cov_agg_.sum_y_, sum_square_y_, count);
    FixedInt<64, 20> denominator_square =
        ExtendAndMultiply(variance_numerator_x, variance_numerator_y);
    // If the denominator is outside the range of valid double
    // conversion we'll remove 5 words from the denominator and 2.5 from the
    // numerator.
    // To avoid precision loss in the numerator we treat it as a double
    // and divide by 2^160.
    bool negate = denominator_square.is_negative() != numerator.is_negative();
    FixedUint<64, 20> denominator_square_abs = denominator_square.abs();
    FixedUint<64, 10> numerator_abs = numerator.abs();
    double converted_numerator = static_cast<double>(numerator_abs);
    if (denominator_square_abs.NonZeroLength() > 15) {
      denominator_square_abs >>= 320;
      converted_numerator = std::ldexp(converted_numerator, -160);
    }
    return (negate ? -1 : 1) * converted_numerator /
           std::sqrt(
               static_cast<double>(FixedInt<64, 15>(denominator_square_abs)));
  }
  return absl::nullopt;
}

void BigNumericValue::CorrelationAggregator::MergeWith(
    const CorrelationAggregator& other) {
  cov_agg_.MergeWith(other.cov_agg_);
  sum_square_x_ += other.sum_square_x_;
  sum_square_y_ += other.sum_square_y_;
}

void BigNumericValue::CorrelationAggregator::SerializeAndAppendToProtoBytes(
    std::string* bytes) const {
  SerializeFixedInt(bytes, cov_agg_.sum_product_, cov_agg_.sum_x_,
                    cov_agg_.sum_y_, sum_square_x_, sum_square_y_);
}

absl::StatusOr<BigNumericValue::CorrelationAggregator>
BigNumericValue::CorrelationAggregator::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  CorrelationAggregator out;
  if (DeserializeFixedInt(bytes, &out.cov_agg_.sum_product_,
                          &out.cov_agg_.sum_x_, &out.cov_agg_.sum_y_,
                          &out.sum_square_x_, &out.sum_square_y_)) {
    return out;
  }
  return MakeEvalError()
         << "Invalid BigNumericValue::CorrelationAggregator encoding";
}

std::ostream& operator<<(std::ostream& out, const VarNumericValue& value) {
  return out << value.ToString();
}

VarNumericValue VarNumericValue::FromScaledLittleEndianValue(
    absl::string_view little_endian_value, uint scale) {
  VarNumericValue result;
  result.scale_ = scale;
  if (!little_endian_value.empty()) {
    result.value_.resize((little_endian_value.size() + sizeof(uint32_t) - 1) /
                         sizeof(uint32_t));
    VarIntRef<32> var_int_ref(result.value_);
    bool success = var_int_ref.DeserializeFromBytes(little_endian_value);
    ZETASQL_DCHECK(success);
  }
  return result;
}

void VarNumericValue::AppendToString(std::string* output) const {
  ZETASQL_DCHECK(output != nullptr);
  size_t first_digit_index = output->size();
  ConstVarIntRef<32>(value_).AppendToString(output);
  if (output->size() == first_digit_index + 1 &&
      output->at(first_digit_index) == '0') {
    return;
  }
  first_digit_index += output->at(first_digit_index) == '-';
  AddDecimalPointAndAdjustZeros(first_digit_index, scale_, 0, false, output);
}

}  // namespace zetasql
