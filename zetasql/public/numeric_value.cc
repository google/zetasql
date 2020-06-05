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

#include "zetasql/public/numeric_value.h"

#include <ctype.h>
#include <stddef.h>
#include <string.h>
#include <sys/types.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <limits>
#include <string>
#include <type_traits>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/fixed_int.h"
#include "absl/base/optimization.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "zetasql/base/endian.h"
#include "zetasql/base/stl_util.h"
#include "zetasql/base/mathutil.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

using FormatFlag = NumericValue::FormatSpec::Flag;

namespace {

constexpr int kBitsPerByte = 8;
constexpr int kBitsPerUint64 = 64;
constexpr int kBitsPerInt64 = 64;
constexpr int kBitsPerInt128 = 128;
constexpr int kBytesPerInt64 = kBitsPerInt64 / kBitsPerByte;
constexpr int kBytesPerInt128 = kBitsPerInt128 / kBitsPerByte;
constexpr uint8_t kGroupSize = 3;
constexpr char kGroupChar = ',';

inline absl::Status MakeInvalidNumericError(absl::string_view str) {
  return MakeEvalError() << "Invalid NUMERIC value: " << str;
}

// Returns OK if the given character is a decimal ascii digit '0' to '9'.
// Returns an INVALID_ARGUMENT otherwise.
inline absl::Status ValidateAsciiDigit(char c, absl::string_view str) {
  if (ABSL_PREDICT_FALSE(!absl::ascii_isdigit(c))) {
    return MakeInvalidNumericError(str);
  }

  return absl::OkStatus();
}

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

FixedInt<64, 6> GetScaledCovarianceNumerator(const FixedInt<64, 3>& sum_x,
                                             const FixedInt<64, 3>& sum_y,
                                             const FixedInt<64, 5>& sum_product,
                                             uint64_t count) {
  FixedInt<64, 6> numerator(sum_product);
  numerator *= count;
  numerator -= ExtendAndMultiply(sum_x, sum_y);
  return numerator;
}

double GetCovariance(const FixedInt<64, 3>& sum_x,
                     const FixedInt<64, 3>& sum_y,
                     const FixedInt<64, 5>& sum_product,
                     uint64_t count,
                     uint64_t count_offset) {
  FixedInt<64, 6> numerator(
      GetScaledCovarianceNumerator(sum_x, sum_y, sum_product, count));
  FixedUint<64, 3> denominator(count);
  denominator *= (count - count_offset);
  denominator *= (static_cast<uint64_t>(NumericValue::kScalingFactor) *
                  NumericValue::kScalingFactor);
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
  DCHECK_LE(dest->size() - old_size, 128);
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

struct ENotationParts {
  bool negative = false;
  absl::string_view int_part;
  absl::string_view fract_part;
  absl::string_view exp_part;
};

#define RETURN_FALSE_IF(x) \
  if (ABSL_PREDICT_FALSE(x)) return false

bool SplitENotationParts(absl::string_view str, ENotationParts* parts) {
  const char* start = str.data();
  const char* end = str.data() + str.size();

  // Skip whitespace.
  for (; start < end && (absl::ascii_isspace(*start)); ++start) {
  }
  for (; start < end && absl::ascii_isspace(*(end - 1)); --end) {
  }

  // Empty or only spaces
  RETURN_FALSE_IF(start == end);
  *parts = ENotationParts();

  parts->negative = (*start == '-');
  start += (*start == '-' || *start == '+');
  for (const char* c = end; --c >= start;) {
    if (*c == 'e' || *c == 'E') {
      parts->exp_part = absl::string_view(c + 1, end - c - 1);
      RETURN_FALSE_IF(parts->exp_part.empty());
      end = c;
      break;
    }
  }
  for (const char* c = start; c < end; ++c) {
    if (*c == '.') {
      parts->fract_part = absl::string_view(c + 1, end - c - 1);
      end = c;
      break;
    }
  }
  parts->int_part = absl::string_view(start, end - start);
  return true;
}

// Parses <exp_part> and add <extra_scale> to the result.
// If <exp_part> represents an integer that is below int64min, the result is
// int64min.
bool ParseExponent(absl::string_view exp_part, uint extra_scale, int64_t* exp) {
  *exp = extra_scale;
  if (!exp_part.empty()) {
    FixedInt<64, 1> exp_fixed_int;
    if (ABSL_PREDICT_TRUE(exp_fixed_int.ParseFromStringStrict(exp_part))) {
      RETURN_FALSE_IF(exp_fixed_int.AddOverflow(*exp));
      *exp = exp_fixed_int.number()[0];
    } else if (exp_part.size() > 1 && exp_part[0] == '-') {
      // Still need to check whether exp_part contains only digits after '-'.
      exp_part.remove_prefix(1);
      for (char c : exp_part) {
        RETURN_FALSE_IF(!std::isdigit(c));
      }
      *exp = std::numeric_limits<int64_t>::min();
    } else {
      return false;
    }
  }
  return true;
}

// Parses <int_part>.<fract_part>E<exp> to FixedUint.
// If <strict> is true, treats the input as invalid if it does not represent
// a whole number. If <strict> is false, rounds the input away from zero to a
// whole number. Returns true iff the input is valid.
template <int n>
bool ParseNumber(absl::string_view int_part, absl::string_view fract_part,
                 int64_t exp, bool strict, FixedUint<64, n>* output) {
  *output = FixedUint<64, n>();
  bool round_up = false;
  if (exp >= 0) {
    // Promote up to exp fractional digits to the integer part.
    size_t num_promoted_fract_digits = fract_part.size();
    if (exp < fract_part.size()) {
      round_up = fract_part[exp] >= '5';
      num_promoted_fract_digits = exp;
    }
    absl::string_view promoted_fract_part(fract_part.data(),
                                          num_promoted_fract_digits);
    fract_part.remove_prefix(num_promoted_fract_digits);
    if (int_part.empty()) {
      RETURN_FALSE_IF(!output->ParseFromStringStrict(promoted_fract_part));
    } else {
      RETURN_FALSE_IF(
          !output->ParseFromStringSegments(int_part, {promoted_fract_part}));
      int_part = absl::string_view();
    }

    // If exp is greater than the number of promoted fractional digits,
    // scale the result up by pow(10, exp - num_promoted_fract_digits).
    size_t extra_exp = static_cast<size_t>(exp) - num_promoted_fract_digits;
    for (; extra_exp >= 19; extra_exp -= 19) {
      RETURN_FALSE_IF(output->MultiplyOverflow(internal::k1e19));
    }
    if (extra_exp != 0) {
      static constexpr std::array<uint64_t, 19> kPowers =
          PowersAsc<uint64_t, 1, 10, 19>();
      RETURN_FALSE_IF(output->MultiplyOverflow(kPowers[extra_exp]));
    }
  } else {  // exp < 0
    RETURN_FALSE_IF(int_part.size() + fract_part.size() == 0);
    // Demote up to -exp digits from int_part
    if (exp >= static_cast<int64_t>(-int_part.size())) {
      size_t int_digits = int_part.size() + exp;
      round_up = int_part[int_digits] >= '5';
      RETURN_FALSE_IF(int_digits != 0 &&
                      !output->ParseFromStringStrict(
                          absl::string_view(int_part.data(), int_digits)));
      int_part.remove_prefix(int_digits);
    }
  }
  // The remaining characters in int_part and fract_part have not been visited.
  // They represent the fractional digits to be discarded. In strict mode, they
  // must be zeros; otherwise they must be digits.
  if (strict) {
    for (char c : int_part) {
      RETURN_FALSE_IF(c != '0');
    }
    for (char c : fract_part) {
      RETURN_FALSE_IF(c != '0');
    }
  } else {
    for (char c : int_part) {
      RETURN_FALSE_IF(!std::isdigit(c));
    }
    for (char c : fract_part) {
      RETURN_FALSE_IF(!std::isdigit(c));
    }
  }
  RETURN_FALSE_IF(round_up && output->AddOverflow(uint64_t{1}));
  return true;
}
#undef RETURN_FALSE_IF

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
  value += FixedUint<64, 4>(BigNumericValue::ScalingFactor() - 1);
  return UnsignedFloor(value);
}

}  // namespace

zetasql_base::StatusOr<NumericValue> NumericValue::FromStringStrict(
    absl::string_view str) {
  return FromStringInternal(str, /*is_strict=*/true);
}

zetasql_base::StatusOr<NumericValue> NumericValue::FromString(absl::string_view str) {
  return FromStringInternal(str, /*is_strict=*/false);
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
zetasql_base::StatusOr<NumericValue> NumericValue::FromStringInternal(
    absl::string_view str, bool is_strict) {
  ENotationParts parts;
  int64_t exp;
  FixedUint<64, 2> abs;
  if (ABSL_PREDICT_TRUE(SplitENotationParts(str, &parts)) &&
      ABSL_PREDICT_TRUE(
          ParseExponent(parts.exp_part, kMaxFractionalDigits, &exp)) &&
      ABSL_PREDICT_TRUE(ParseNumber(parts.int_part, parts.fract_part, exp,
                                    is_strict, &abs))) {
    auto number_or_status = FromFixedUint(abs, parts.negative);
    if (number_or_status.ok()) {
      return number_or_status;
    }
  }
  return MakeInvalidNumericError(str);
}

double NumericValue::ToDouble() const {
  return RemoveScaleAndConvertToDouble(as_packed_int());
}

inline unsigned __int128 Scalemantissa(uint64_t mantissa, uint32_t scale) {
  return static_cast<unsigned __int128>(mantissa) * scale;
}

inline FixedUint<64, 4> Scalemantissa(uint64_t mantissa,
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
  DCHECK_NE(parts.mantissa, 0) << value;
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
  auto abs_result = Scalemantissa(abs_mantissa, scale);
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
  DCHECK(rv >= T()) << value;
  *result = negative ? -rv : rv;
  return true;
}

zetasql_base::StatusOr<NumericValue> NumericValue::FromDouble(double value) {
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
    zetasql_base::StatusOr<NumericValue> value_status = FromPackedInt(result);
    if (ABSL_PREDICT_TRUE(value_status.ok())) {
      return value_status;
    }
  }
  return MakeEvalError() << "numeric out of range: " << value;
}

zetasql_base::StatusOr<NumericValue> NumericValue::Multiply(NumericValue rh) const {
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

zetasql_base::StatusOr<NumericValue> NumericValue::Power(NumericValue exp) const {
  auto res_or_status = PowerInternal(exp);
  if (res_or_status.ok()) {
    return res_or_status;
  }
  return zetasql_base::StatusBuilder(res_or_status.status()).SetAppend()
         << ": POW(" << ToString() << ", " << exp.ToString() << ")";
}

namespace {
constexpr uint64_t kScalingFactorSquare =
    static_cast<uint64_t>(NumericValue::kScalingFactor) *
    static_cast<uint64_t>(NumericValue::kScalingFactor);
constexpr unsigned __int128 kScalingFactorCube =
    static_cast<unsigned __int128>(kScalingFactorSquare) *
    NumericValue::kScalingFactor;

// Divides input by kScalingFactor ^ 2 with rounding and store the result to
// output. Returns false if the result cannot fit into FixedUint<64, 3>.
template <int size>
inline bool RemoveDoubleScale(FixedUint<64, size>* input,
                              FixedUint<64, size - 1>* output) {
  if (ABSL_PREDICT_TRUE(!input->AddOverflow(kScalingFactorSquare / 2)) &&
      ABSL_PREDICT_TRUE(input->number()[size - 1] < kScalingFactorSquare)) {
    *input /= NumericValue::kScalingFactor;
    *input /= NumericValue::kScalingFactor;
    *output = FixedUint<64, size - 1>(*input);
    return true;
  }
  return false;
}

// Raises value to exp. *double_scaled_value (input and output) is scaled
// by kScalingFactorSquare. Extra scaling is used for preserving
// precision during computations.
// Returns false if the result is too big (not necessarily an error).
bool DoubleScaledPower(FixedUint<64, 3>* double_scaled_value,
                       FixedUint<64, 2> unscaled_exp) {
  FixedUint<64, 3> double_scaled_result(kScalingFactorSquare);
  FixedUint<64, 3> double_scaled_power(*double_scaled_value);
  unsigned __int128 exp = static_cast<unsigned __int128>(unscaled_exp);
  while (true) {
    if ((exp & 1) != 0) {
      FixedUint<64, 6> tmp_scaled_4x =
          ExtendAndMultiply(double_scaled_result, double_scaled_power);
      if (ABSL_PREDICT_FALSE(tmp_scaled_4x.number()[4] != 0) ||
          ABSL_PREDICT_FALSE(tmp_scaled_4x.number()[5] != 0)) {
        return false;
      }
      FixedUint<64, 4> truncated_tmp_scaled_4x(tmp_scaled_4x);
      if (ABSL_PREDICT_FALSE(!RemoveDoubleScale(&truncated_tmp_scaled_4x,
                                                &double_scaled_result))) {
        return false;
      }
    }
    if (exp <= 1) {
      *double_scaled_value = double_scaled_result;
      return true;
    }
    if (ABSL_PREDICT_FALSE((double_scaled_power.number()[2] != 0))) {
      return false;
    }
    FixedUint<64, 2> truncated_power(double_scaled_power);
    FixedUint<64, 4> tmp_scaled_4x =
        ExtendAndMultiply(truncated_power, truncated_power);
    if (ABSL_PREDICT_FALSE(
            !RemoveDoubleScale(&tmp_scaled_4x, &double_scaled_power))) {
      return false;
    }
    exp >>= 1;
  }
}

// *dest *= pow(abs_value / kScalingFactor, fract_exp / kScalingFactor) *
// kScalingFactor
absl::Status MultiplyByFractionalPower(unsigned __int128 abs_value,
                                       int64_t fract_exp,
                                       FixedUint<64, 3>* dest) {
  // We handle the fractional part of the exponent by raising the original value
  // to the fractional part of the exponent by converting them to doubles and
  // using the standard library's pow() function.
  // TODO Using std::pow() gives a result with reasonable precision
  // (comparable to MS SQL and MySQL), but we can probably do better here.
  // Explore a more accurate implementation in the future.
  double fract_pow = std::pow(RemoveScaleAndConvertToDouble(abs_value),
                              RemoveScaleAndConvertToDouble(fract_exp));
  ZETASQL_ASSIGN_OR_RETURN(NumericValue fract_term,
                   NumericValue::FromDouble(fract_pow));
  FixedUint<64, 5> ret = ExtendAndMultiply(
      *dest, FixedUint<64, 2>(
                 static_cast<unsigned __int128>(fract_term.as_packed_int())));
  if (ABSL_PREDICT_TRUE(ret.number()[3] == 0) &&
      ABSL_PREDICT_TRUE(ret.number()[4] == 0)) {
    *dest = FixedUint<64, 3>(ret);
    return absl::OkStatus();
  }
  return MakeEvalError() << "numeric overflow";
}
}  // namespace

zetasql_base::StatusOr<NumericValue> NumericValue::PowerInternal(
    NumericValue exp) const {
  // Any value raised to a zero power is always one.
  if (exp == NumericValue()) {
    return NumericValue(1);
  }

  const bool exp_is_negative = exp.as_packed_int() < 0;
  if (*this == NumericValue()) {
    // An attempt to raise zero to a negative power results in division by zero.
    if (exp_is_negative) {
      return MakeEvalError() << "division by zero";
    }
    // Otherwise zero raised to any power is still zero.
    return NumericValue();
  }
  FixedUint<64, 2> abs_integer_exp;
  uint32_t abs_fract_exp;
  FixedUint<64, 2>(int128_abs(exp.as_packed_int()))
      .DivMod(kScalingFactor, &abs_integer_exp, &abs_fract_exp);
  int64_t fract_exp = abs_fract_exp;
  if (exp.as_packed_int() < 0) {
    fract_exp = -fract_exp;
  }

  bool result_is_negative = false;
  unsigned __int128 abs_value = int128_abs(as_packed_int());
  if (as_packed_int() < 0) {
    if (fract_exp != 0) {
      return MakeEvalError()
             << "Negative NUMERIC value cannot be raised to a fractional power";
    }
    result_is_negative = (abs_integer_exp.number()[0] & 1) != 0;
  }

  FixedUint<64, 3> double_scaled_value;
  if (!exp_is_negative) {
    double_scaled_value = FixedUint<64, 3>(abs_value);
    double_scaled_value *= kScalingFactor;
  } else {
    // If the exponent is negative and abs_value is > 1, then we compute
    // compute 1 / (abs_value ^ (-integer_exp)). Note, computing
    // (1 / abs_value) ^ (-integer_exp) would lose precision in the division
    // because the input of DoubleScaledPower can have only 9 digits after the
    // decimal point.
    if (abs_value > kScalingFactor) {
      double_scaled_value = FixedUint<64, 3>(abs_value);
      double_scaled_value *= kScalingFactor;
      if (!DoubleScaledPower(&double_scaled_value, abs_integer_exp) ||
          double_scaled_value > FixedUint<64, 3>(kScalingFactorCube * 2)) {
        return NumericValue();
      }
      DCHECK(static_cast<unsigned __int128>(double_scaled_value) != 0);
      if (fract_exp == 0) {
        FixedUint<64, 3> numerator(kScalingFactorCube);  // triple-scaled
        numerator.DivAndRoundAwayFromZero(double_scaled_value);
        return NumericValue::FromFixedUint(numerator, result_is_negative);
      }
      FixedUint<64, 3> numerator(kScalingFactorSquare);
      // Because fract_exp < 0, the upper bound of pow(abs_value, fract_exp)
      // is pow(1e-9, -1) = 1e9 with scaled value = kScalingFactor ^ 2, which
      // means MultiplyByFractionalPower should not overflow.
      ZETASQL_RETURN_IF_ERROR(
          MultiplyByFractionalPower(abs_value, fract_exp, &numerator));
      // Now numerator is triple-scaled.
      numerator.DivAndRoundAwayFromZero(double_scaled_value);
      return NumericValue::FromFixedUint(numerator, result_is_negative);
    }
    // If the exponent is negative and abs_value is <= 1, then we compute
    // (1 / abs_value) ^ (-abs_integer_exp).
    double_scaled_value = FixedUint<64, 3>(kScalingFactorCube);
    FixedUint<64, 3> denominator(abs_value);
    double_scaled_value.DivAndRoundAwayFromZero(denominator);
  }

  if (!DoubleScaledPower(&double_scaled_value, abs_integer_exp)) {
    return MakeEvalError() << "numeric overflow";
  }

  if (fract_exp == 0) {
    // Divide double_scaled_value by kScalingFactor to make it single-scaled.
    double_scaled_value.DivAndRoundAwayFromZero(kScalingFactor);
    return NumericValue::FromFixedUint(double_scaled_value, result_is_negative);
  }

  ZETASQL_RETURN_IF_ERROR(
      MultiplyByFractionalPower(abs_value, fract_exp, &double_scaled_value));
  // After MultiplyByFractionalPower, tmp is triple-scaled. Divide it by
  // kScalingFactor ^ 2 to make it single-scaled.
  FixedUint<64, 2> ret;
  if (ABSL_PREDICT_FALSE(!RemoveDoubleScale(&double_scaled_value, &ret))) {
    return MakeEvalError() << "numeric overflow";
  }
  return NumericValue::FromFixedUint(ret, result_is_negative);
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

zetasql_base::StatusOr<NumericValue> NumericValue::Round(int64_t digits) const {
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

zetasql_base::StatusOr<NumericValue> NumericValue::Ceiling() const {
  __int128 value = as_packed_int();
  int64_t fract_part = GetFractionalPart();
  value -= fract_part > 0 ? fract_part - kScalingFactor : fract_part;
  auto res_status = NumericValue::FromPackedInt(value);
  if (res_status.ok()) {
    return res_status;
  }
  return MakeEvalError() << "numeric overflow: CEIL(" << ToString() << ")";
}

zetasql_base::StatusOr<NumericValue> NumericValue::Floor() const {
  __int128 value = as_packed_int();
  int64_t fract_part = GetFractionalPart();
  value -= fract_part < 0 ? fract_part + kScalingFactor : fract_part;
  auto res_status = NumericValue::FromPackedInt(value);
  if (res_status.ok()) {
    return res_status;
  }
  return MakeEvalError() << "numeric overflow: FLOOR(" << ToString() << ")";
}

zetasql_base::StatusOr<NumericValue> NumericValue::Divide(NumericValue rh) const {
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

zetasql_base::StatusOr<NumericValue> NumericValue::DivideToIntegralValue(
    NumericValue rh) const {
  __int128 rh_value = rh.as_packed_int();
  if (ABSL_PREDICT_TRUE(rh_value != 0)) {
    __int128 value = as_packed_int() / rh_value;
    if (ABSL_PREDICT_TRUE(value <= internal::kNumericMax / kScalingFactor) &&
        ABSL_PREDICT_TRUE(value >= internal::kNumericMin / kScalingFactor)) {
      return NumericValue(value * kScalingFactor);
    }
    return MakeEvalError() << "numeric overflow: " << ToString() << " / "
                           << rh.ToString();
  }
  return MakeEvalError() << "division by zero: " << ToString() << " / "
                         << rh.ToString();
}

zetasql_base::StatusOr<NumericValue> NumericValue::Mod(NumericValue rh) const {
  __int128 rh_value = rh.as_packed_int();
  if (ABSL_PREDICT_TRUE(rh_value != 0)) {
    return NumericValue(as_packed_int() % rh_value);
  }
  return MakeEvalError() << "division by zero: " << ToString() << " / "
                         << rh.ToString();
}

void NumericValue::SerializeAndAppendToProtoBytes(std::string* bytes) const {
  FixedInt<64, 2>(as_packed_int()).SerializeToBytes(bytes);
}

zetasql_base::StatusOr<NumericValue> NumericValue::DeserializeFromProtoBytes(
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
  DCHECK_LE(exponent, 99);
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
        *abs_value /= std::integral_constant<uint32_t, internal::k5to13>();
        *abs_value /= std::integral_constant<uint32_t, internal::k5to13>();
        *abs_value /= std::integral_constant<uint32_t, internal::k5to12>();
        *abs_value >>= 38;
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

}  // namespace

void NumericValue::FormatAndAppend(FormatSpec spec, std::string* output) const {
  Format<kMaxFractionalDigits>(spec, FixedInt<64, 2>(as_packed_int()), output);
}

std::ostream& operator<<(std::ostream& out, NumericValue value) {
  return out << value.ToString();
}

zetasql_base::StatusOr<NumericValue> NumericValue::SumAggregator::GetSum() const {
  auto res_status = NumericValue::FromFixedInt(sum_);
  if (res_status.ok()) {
    return res_status;
  }
  return MakeEvalError() << "numeric overflow: SUM";
}

zetasql_base::StatusOr<NumericValue> NumericValue::SumAggregator::GetAverage(
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

zetasql_base::StatusOr<NumericValue::SumAggregator>
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

absl::optional<double> NumericValue::VarianceAggregator::GetPopulationVariance(
    uint64_t count) const {
  if (count > 0) {
    return GetCovariance(sum_, sum_, sum_square_, count, 0);
  }
  return absl::nullopt;
}

absl::optional<double> NumericValue::VarianceAggregator::GetSamplingVariance(
    uint64_t count) const {
  if (count > 1) {
    return GetCovariance(sum_, sum_, sum_square_, count, 1);
  }
  return absl::nullopt;
}

absl::optional<double> NumericValue::VarianceAggregator::GetPopulationStdDev(
    uint64_t count) const {
  if (count > 0) {
    return std::sqrt(
        GetCovariance(sum_, sum_, sum_square_, count, 0));
  }
  return absl::nullopt;
}

absl::optional<double> NumericValue::VarianceAggregator::GetSamplingStdDev(
    uint64_t count) const {
  if (count > 1) {
    return std::sqrt(
        GetCovariance(sum_, sum_, sum_square_, count, 1));
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

zetasql_base::StatusOr<NumericValue::VarianceAggregator>
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

absl::optional<double>
NumericValue::CovarianceAggregator::GetPopulationCovariance(
    uint64_t count) const {
  if (count > 0) {
    return GetCovariance(sum_x_, sum_y_, sum_product_, count, 0);
  }
  return absl::nullopt;
}

absl::optional<double>
NumericValue::CovarianceAggregator::GetSamplingCovariance(uint64_t count) const {
  if (count > 1) {
    return GetCovariance(sum_x_, sum_y_, sum_product_, count, 1);
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

zetasql_base::StatusOr<NumericValue::CovarianceAggregator>
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

absl::optional<double>
NumericValue::CorrelationAggregator::GetCorrelation(uint64_t count) const {
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

zetasql_base::StatusOr<NumericValue::CorrelationAggregator>
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

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::Multiply(
    const BigNumericValue& rh) const {
  bool lh_negative = value_.is_negative();
  bool rh_negative = rh.value_.is_negative();
  FixedUint<64, 8> abs_result_64x8 =
      ExtendAndMultiply(value_.abs(), rh.value_.abs());
  if (ABSL_PREDICT_TRUE(abs_result_64x8.number()[6] == 0) &&
      ABSL_PREDICT_TRUE(abs_result_64x8.number()[7] == 0)) {
    FixedUint<64, 5> abs_result_64x5 =
        RemoveScalingFactor(FixedUint<64, 6>(abs_result_64x8));
    if (ABSL_PREDICT_TRUE(abs_result_64x5.number()[4] == 0)) {
      FixedInt<64, 4> result;
      FixedUint<64, 4> abs_result_64x4(abs_result_64x5);
      if (ABSL_PREDICT_TRUE(result.SetSignAndAbs(lh_negative != rh_negative,
                                                 abs_result_64x4))) {
        return BigNumericValue(result);
      }
    }
  }
  return MakeEvalError() << "BigNumeric overflow: " << ToString() << " * "
                         << rh.ToString();
}

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::Divide(
    const BigNumericValue& rh) const {
  bool lh_negative = value_.is_negative();
  bool rh_negative = rh.value_.is_negative();
  if (ABSL_PREDICT_TRUE(!rh.value_.is_zero())) {
    FixedUint<64, 4> abs_value = value_.abs();
    FixedUint<64, 6> rh_abs_value(rh.value_.abs());
    FixedUint<64, 6> scaled_abs_value =
        ExtendAndMultiply(abs_value, FixedUint<64, 2>(ScalingFactor()));
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
    return MakeEvalError() << "BigNumeric overflow: " << ToString() << " / "
                           << rh.ToString();
  }
  return MakeEvalError() << "division by zero: " << ToString() << " / "
                         << rh.ToString();
}

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::DivideToIntegralValue(
    const BigNumericValue& rh) const {
  if (ABSL_PREDICT_TRUE(!rh.value_.is_zero())) {
    bool lh_negative = value_.is_negative();
    bool rh_negative = rh.value_.is_negative();
    FixedUint<64, 4> abs_result = value_.abs();
    abs_result /= rh.value_.abs();
    bool overflow =
        abs_result.MultiplyOverflow(FixedUint<64, 4>(ScalingFactor()));

    if (ABSL_PREDICT_TRUE(!overflow)) {
      FixedInt<64, 4> result;
      if (ABSL_PREDICT_TRUE(
              result.SetSignAndAbs(lh_negative != rh_negative, abs_result))) {
        return BigNumericValue(result);
      }
    }
    return MakeEvalError() << "BigNumeric overflow: " << ToString() << " / "
                           << rh.ToString();
  }
  return MakeEvalError() << "division by zero: " << ToString() << " / "
                         << rh.ToString();
}

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::Mod(
    const BigNumericValue& rh) const {
  if (ABSL_PREDICT_TRUE(!rh.value_.is_zero())) {
    FixedInt<64, 4> remainder = value_;
    remainder %= rh.value_;
    return BigNumericValue(remainder);
  }
  return MakeEvalError() << "division by zero: Mod(" << ToString() << ", "
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
  abs_value.DivMod(std::integral_constant<uint32_t, kPowersOf5[13]>(), &abs_value,
                   &remainder_bits);
  uint32_t remainder;
  abs_value.DivMod(std::integral_constant<uint32_t, kPowersOf5[13]>(), &abs_value,
                   &remainder);
  remainder_bits |= remainder;
  abs_value.DivMod(std::integral_constant<uint32_t, kPowersOf5[12]>(), &abs_value,
                   &remainder);
  remainder_bits |= remainder;
  std::array<uint64_t, 4> n = abs_value.number();
  n[0] |= (remainder_bits != 0);
  double result =
      static_cast<double>(FixedUint<64, 4>(n)) / binary_scaling_factor;
  return is_negative ? -result : result;
}

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::Round(int64_t digits) const {
  FixedUint<64, 4> abs_value = value_.abs();
  if (ABSL_PREDICT_TRUE(RoundInternal(&abs_value, digits))) {
    FixedInt<64, 4> result(abs_value);
    return BigNumericValue(!value_.is_negative() ? result : -result);
  }
  return MakeEvalError() << "BigNumeric overflow: ROUND(" << ToString() << ", "
                         << digits << ")";
}

BigNumericValue BigNumericValue::Trunc(int64_t digits) const {
  FixedUint<64, 4> abs_value = value_.abs();
  RoundOrTrunc</*round_away_from_zero*/ false>(&abs_value, digits);
  FixedInt<64, 4> result(abs_value);
  return BigNumericValue(!value_.is_negative() ? result : -result);
}

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::Floor() const {
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
  return MakeEvalError() << "BigNumeric overflow: FLOOR(" << ToString() << ")";
}

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::Ceiling() const {
  if (!value_.is_negative()) {
    FixedInt<64, 4> ceiling_value(UnsignedCeiling(value_.abs()));
    // UnsignedCeiling cannot overflow, however it can return a FixedUint which
    // is out of range as the same FixedInt. Because the constructor simply
    // copies the underlying bits we can check the high bit, i.e. is_negative
    if (ABSL_PREDICT_TRUE(!ceiling_value.is_negative())) {
      return BigNumericValue(ceiling_value);
    }
    return MakeEvalError()
           << "BigNumeric overflow: CEIL(" << ToString() << ")";
  }
  FixedInt<64, 4> floor_value(UnsignedFloor(value_.abs()));
  return BigNumericValue(-floor_value);
}

// Parses a textual representation of a BIGNUMERIC value. Returns an error if
// the given string cannot be parsed as a number or if the textual numeric value
// exceeds BIGNUMERIC range. If 'is_strict' is true then the function will
// return an error if there are more that 38 digits in the fractional part,
// otherwise the number will be rounded to contain no more than 38 fractional
// digits.
zetasql_base::StatusOr<BigNumericValue> BigNumericValue::FromStringInternal(
    absl::string_view str, bool is_strict) {
  ENotationParts parts;
  int64_t exp;
  FixedUint<64, 4> abs;
  BigNumericValue result;
  if (ABSL_PREDICT_TRUE(SplitENotationParts(str, &parts)) &&
      ABSL_PREDICT_TRUE(
          ParseExponent(parts.exp_part, kMaxFractionalDigits, &exp)) &&
      ABSL_PREDICT_TRUE(ParseNumber(parts.int_part, parts.fract_part, exp,
                                    is_strict, &abs)) &&
      ABSL_PREDICT_TRUE(result.value_.SetSignAndAbs(parts.negative, abs))) {
    return result;
  }
  return MakeInvalidBigNumericError(str);
}

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::FromStringStrict(
    absl::string_view str) {
  return FromStringInternal(str, /*is_strict=*/true);
}

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::FromString(
    absl::string_view str) {
  return FromStringInternal(str, /*is_strict=*/false);
}

size_t BigNumericValue::HashCode() const {
  return absl::Hash<BigNumericValue>()(*this);
}

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::FromDouble(double value) {
  if (ABSL_PREDICT_FALSE(!std::isfinite(value))) {
    // This error message should be kept consistent with the error message found
    // in .../public/functions/convert.h.
    if (std::isnan(value)) {
      // Don't show the negative sign for -nan values.
      value = std::numeric_limits<double>::quiet_NaN();
    }
    return MakeEvalError() << "Illegal conversion of non-finite floating point "
                              "number to BigNumeric: "
                           << value;
  }
  FixedInt<64, 4> result;
  if (ScaleAndRoundAwayFromZero(ScalingFactor(), value, &result)) {
    return BigNumericValue(result);
  }
  return MakeEvalError() << "BigNumeric out of range: " << value;
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

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  BigNumericValue out;
  if (out.value_.DeserializeFromBytes(bytes)) {
    return out;
  }
  return MakeEvalError() << "Invalid BigNumeric encoding";
}

void BigNumericValue::FormatAndAppend(FormatSpec spec,
                                      std::string* output) const {
  Format<kMaxFractionalDigits>(spec, value_, output);
}

std::ostream& operator<<(std::ostream& out, const BigNumericValue& value) {
  return out << value.ToString();
}

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::SumAggregator::GetSum() const {
  if (sum_.number()[4] ==
      static_cast<uint64_t>(static_cast<int64_t>(sum_.number()[3]) >> 63)) {
    FixedInt<64, 4> sum_trunc(sum_);
    return BigNumericValue(sum_trunc);
  }
  return MakeEvalError() << "BigNumeric overflow: SUM";
}

zetasql_base::StatusOr<BigNumericValue> BigNumericValue::SumAggregator::GetAverage(
    uint64_t count) const {
  if (count == 0) {
    return MakeEvalError() << "division by zero: AVG";
  }

  FixedInt<64, 5> dividend = sum_;
  dividend.DivAndRoundAwayFromZero(count);
  if (ABSL_PREDICT_TRUE(dividend.number()[4] ==
                        static_cast<uint64_t>(
                            static_cast<int64_t>(dividend.number()[3]) >> 63))) {
    FixedInt<64, 4> dividend_trunc(dividend);
    return BigNumericValue(dividend_trunc);
  }

  return MakeEvalError() << "BigNumeric overflow: AVG";
}

void BigNumericValue::SumAggregator::SerializeAndAppendToProtoBytes(
    std::string* bytes) const {
  sum_.SerializeToBytes(bytes);
}

zetasql_base::StatusOr<BigNumericValue::SumAggregator>
BigNumericValue::SumAggregator::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  BigNumericValue::SumAggregator out;
  if (out.sum_.DeserializeFromBytes(bytes)) {
    return out;
  }
  return MakeEvalError() << "Invalid BigNumericValue::SumAggregator encoding";
}

}  // namespace zetasql
