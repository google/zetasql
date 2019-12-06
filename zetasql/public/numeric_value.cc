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

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <cmath>
#include <string>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/fixed_int.h"
#include <cstdint>
#include "absl/base/optimization.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/bits.h"
#include "zetasql/base/endian.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

namespace {

// Maximum number of decimal digits in the integer and the fractional part of a
// NUMERIC value.
const int kMaxIntegerDigits = 29;
const int kMaxFractionalDigits = 9;

const int kBitsPerByte = 8;
const int kBitsPerUint32 = 32;
const int kBitsPerUint64 = 64;
const int kBitsPerInt64 = 64;
const int kBitsPerInt128 = 128;
constexpr int kBytesPerInt64 = kBitsPerInt64 / kBitsPerByte;
constexpr int kBytesPerInt128 = kBitsPerInt128 / kBitsPerByte;

inline zetasql_base::Status MakeInvalidNumericError(absl::string_view str) {
  return MakeEvalError() << "Invalid NUMERIC value: " << str;
}

// Returns OK if the given character is a decimal ascii digit '0' to '9'.
// Returns an INVALID_ARGUMENT otherwise.
inline zetasql_base::Status ValidateAsciiDigit(char c, absl::string_view str) {
  if (ABSL_PREDICT_FALSE(!absl::ascii_isdigit(c))) {
    return MakeInvalidNumericError(str);
  }

  return zetasql_base::OkStatus();
}

// Returns -1, 0 or 1 if the given int128 number is negative, zero of positive
// respectively.
inline int int128_sign(__int128 x) {
  return (0 < x) - (x < 0);
}

inline unsigned __int128 int128_abs(__int128 x) { return (x >= 0) ? x : -x; }

}  // namespace

zetasql_base::StatusOr<NumericValue> NumericValue::FromStringStrict(
    absl::string_view str) {
  return FromStringInternal(str, /*is_strict=*/true);
}

zetasql_base::StatusOr<NumericValue> NumericValue::FromString(absl::string_view str) {
  return FromStringInternal(str, /*is_strict=*/false);
}

template <int kNumBitsPerWord, int kNumWords>
zetasql_base::StatusOr<NumericValue> NumericValue::FromFixedUint(
    const FixedUint<kNumBitsPerWord, kNumWords>& val, bool negate) {
  if (ABSL_PREDICT_TRUE(val.NonZeroLength() <= 128 / kNumBitsPerWord)) {
    unsigned __int128 v = static_cast<unsigned __int128>(val);
    if (ABSL_PREDICT_TRUE(v <= internal::kNumericMax)) {
      return NumericValue(static_cast<__int128>(negate ? -v : v));
    }
  }
  return MakeEvalError() << "numeric overflow";
}

template <int kNumBitsPerWord, int kNumWords>
zetasql_base::StatusOr<NumericValue> NumericValue::FromFixedInt(
    const FixedInt<kNumBitsPerWord, kNumWords>& val) {
  if (val < FixedInt<kNumBitsPerWord, kNumWords>(internal::kNumericMin) ||
      val > FixedInt<kNumBitsPerWord, kNumWords>(internal::kNumericMax)) {
    return MakeEvalError() << "numeric overflow";
  }
  return NumericValue(static_cast<__int128>(val));
}

size_t NumericValue::HashCode() const {
  return absl::Hash<NumericValue>()(*this);
}

std::string NumericValue::ToString() const {
  const __int128 value = as_packed_int();
  const __int128 abs_value = int128_abs(value);

  // First convert the integer part.
  std::string ret;
  for (__int128 int_part = abs_value / kScalingFactor;
       int_part != 0; int_part /= 10) {
    int digit = int_part % 10;
    ret.append(1, digit + '0');
  }
  if (ret.empty()) {
    ret = "0";
  }
  if (value < 0) {
    ret.append(1, '-');
  }
  std::reverse(ret.begin(), ret.end());

  // And now convert the fractional part.
  std::string fract_str;
  for (__int128 fract_part = abs_value % kScalingFactor;
       fract_part != 0; fract_part /= 10) {
    int digit = fract_part % 10;
    fract_str.append(1, digit + '0');
  }

  if (!fract_str.empty()) {
    // Potentially add zeroes that will end up immediately after the decimal
    // point.
    if (fract_str.size() < kMaxFractionalDigits) {
      fract_str.append(kMaxFractionalDigits - fract_str.size(), '0');
    }
    // Skip zeroes that will end up at the end of the fractional part.
    auto idx = fract_str.find_first_not_of('0');
    std::reverse(fract_str.begin() + idx, fract_str.end());
    ret.append(1, '.');
    ret.append(fract_str.substr(idx));
  }

  return ret;
}

// Parses a textual representation of a NUMERIC value. Returns an error if the
// given std::string cannot be parsed as a number or if the textual numeric value
// exceeds NUMERIC precision. If 'is_strict' is true then the function will
// return an error if there are more that 9 digits in the fractional part,
// otherwise the number will be rounded to contain no more than 9 fractional
// digits.
zetasql_base::StatusOr<NumericValue> NumericValue::FromStringInternal(
    absl::string_view str, bool is_strict) {
  constexpr __int128 kMaxIntegerPart = internal::kNumericMax / kScalingFactor;
  // Max allowed value of the exponent part.
  const int kMaxNumericExponent = 65536;

  if (str.empty()) {
    return MakeInvalidNumericError(str);
  }

  const char* start = str.data();
  const char* end = str.data() + str.size();

  // Skip whitespace.
  for (; start < end && absl::ascii_isspace(*start); ++start) {}
  for (; start < end && absl::ascii_isspace(*(end - 1)); --end) {}

  int sign = 1;
  if (*start == '+') {
    ++start;
  } else if (*start == '-') {
    sign = -1;
    ++start;
  }

  // Find position of the decimal dot and the exponent part in the std::string. The
  // numerical part will be validated for correctness, namely we are going to
  // check that it consists only of digits and potentially a single dot.
  const char* exp_start = nullptr;
  const char* dot_pos = nullptr;
  for (const char* c = start; c < end; ++c) {
    if (*c == '.') {
      if (dot_pos != nullptr) {
        return MakeInvalidNumericError(str);
      }
      dot_pos = c;
      continue;
    }
    if (*c == 'e' || *c == 'E') {
      exp_start = c + 1;
      break;
    }
    ZETASQL_RETURN_IF_ERROR(ValidateAsciiDigit(*c, str));
  }

  // If the exponent part is present, validate and parse it now.
  int64_t exponent = 0;
  if (exp_start != nullptr) {
    if (exp_start == end) {
      return MakeInvalidNumericError(str);
    }

    const char* end_expluding_exponent = exp_start - 1;

    int exp_sign = 1;
    if (*exp_start == '+') {
      ++exp_start;
    } else if (*exp_start == '-') {
      exp_sign = -1;
      ++exp_start;
    }

    int num_exp_digits = 0;
    for (; exp_start < end; ++exp_start, ++num_exp_digits) {
      ZETASQL_RETURN_IF_ERROR(ValidateAsciiDigit(*exp_start, str));
      if (ABSL_PREDICT_FALSE(exponent > kMaxNumericExponent)) {
        return MakeInvalidNumericError(str);
      }

      int digit = *exp_start - '0';
      exponent = exponent * 10 + digit;
    }

    if (num_exp_digits == 0) {
      return MakeInvalidNumericError(str);
    }

    exponent *= exp_sign;
    end = end_expluding_exponent;
  }

  int64_t num_integer_digits = dot_pos == nullptr ? end - start : dot_pos - start;
  int64_t num_fract_digits = dot_pos == nullptr ? 0 : end - dot_pos - 1;
  int64_t num_total_digits = num_integer_digits + num_fract_digits;

  // We adjust the position of the decimal point if the exponent is present.
  // That is done by changing the count of how many digits belong to the integer
  // and the fractional parts.
  num_integer_digits = std::max<int64_t>(0, num_integer_digits + exponent);
  num_fract_digits = std::max<int64_t>(0, num_fract_digits - exponent);

  // Parse the numerical part now.
  __int128 value = 0;
  __int128 fractional_part = 0;
  int64_t int_count = 0;
  int64_t fract_count = 0;

  if (num_fract_digits > num_total_digits) {
    // Account for the zeroes that come right after the point but before digits
    // in the fractional part.
    fract_count = num_fract_digits - num_total_digits;
  }

  for (const char* c = start; c < end; ++c) {
    if (*c == '.') {
      continue;
    }

    int digit = *c - '0';

    if (int_count < num_integer_digits) {
      ++int_count;
      value = value * 10 + digit;

      // After adding each digit, check if the integer part is outside the
      // permitted range. The multiplication itself can't overflow, since we're
      // operating on the lower bits prior to shifting by the scaling factor.
      if (ABSL_PREDICT_FALSE(value > kMaxIntegerPart)) {
        return MakeInvalidNumericError(str);
      }
    } else if (fract_count < num_fract_digits) {
      if (fract_count >= kMaxFractionalDigits && is_strict && digit != 0) {
        return MakeInvalidNumericError(str);
      } else if (fract_count == kMaxFractionalDigits && !is_strict) {
        // Non-strict parsing, we get an extra digit in the fractional part, so
        // perform a half away from zero rounding.
        fractional_part += (digit + 5) / 10;
      } else if (fract_count < kMaxFractionalDigits) {
        fractional_part = fractional_part * 10 + digit;
      }

      ++fract_count;
    }
  }

  if ((int_count == 0) && (fract_count == 0)) {
    return MakeInvalidNumericError(str);
  }

  // If the exponent was bigger than zero there might be extra zeroes that
  // should come after all digits in the std::string have been exhausted, account for
  // them now.
  for (; int_count < num_integer_digits; ++int_count) {
    value *= 10;

    // After shifting for each zero, check if the integer part is outside the
    // permitted range. The multiplication itself can't overflow, since we're
    // operating on the lower bits prior to shifting by the scaling factor.
    if (ABSL_PREDICT_FALSE(value > kMaxIntegerPart)) {
      return MakeInvalidNumericError(str);
    }
  }

  // Potentially add zeroes at the end of the fractional part.
  for (int64_t i = num_fract_digits; i < kMaxFractionalDigits; ++i) {
    fractional_part *= 10;
  }

  value *= kScalingFactor;
  value += fractional_part;

  auto number_or_status = FromPackedInt(value * sign);
  return number_or_status.ok() ?
      number_or_status : MakeInvalidNumericError(str);
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
    FixedUint<32, 5> res(product);
    res += kScalingFactor / 2;
    res /= kScalingFactor;
    unsigned __int128 v = static_cast<unsigned __int128>(res);
    // We already checked the value range, so no need to call FromPackedInt.
    return NumericValue(
        static_cast<__int128>(negative == rh_negative ? v : -v));
  }
  return MakeEvalError() << "numeric overflow: " << ToString() << " * "
                         << rh.ToString();
}

NumericValue NumericValue::Abs(NumericValue value) {
  // The result is expected to be within the valid range.
  return NumericValue(static_cast<__int128>(int128_abs(value.as_packed_int())));
}

NumericValue NumericValue::Sign(NumericValue value) {
  return NumericValue(
      static_cast<int64_t>(int128_sign(value.as_packed_int())));
}

zetasql_base::StatusOr<NumericValue> NumericValue::Power(NumericValue exp) const {
  auto res_or_status = PowerInternal(exp);
  if (res_or_status.ok()) {
    return res_or_status;
  }
  return zetasql_base::StatusBuilder(res_or_status.status()).SetAppend()
         << ": POW(" << ToString() << ", " << exp.ToString() << ")";
}

zetasql_base::StatusOr<NumericValue> NumericValue::PowerInternal(
    NumericValue exp) const {
  // Any value raised to a zero power is always one.
  if (exp == NumericValue()) {
    return NumericValue(1);
  }

  // An attempt to raise zero to a negative power results in division by zero.
  if (*this == NumericValue() && exp.as_packed_int() < 0) {
    return MakeEvalError() << "division by zero";
  }

  // Otherwise zero raised to any power is still zero.
  if (*this == NumericValue()) {
    return NumericValue();
  }

  int exp_sign = int128_sign(exp.as_packed_int());
  unsigned __int128 integer_exp =
      int128_abs(exp.as_packed_int()) / kScalingFactor;
  unsigned __int128 fract_exp =
      int128_abs(exp.as_packed_int()) % kScalingFactor;

  // Determine the sign of the result.
  bool result_is_negative = as_packed_int() < 0 && (integer_exp % 2) != 0;

  if (as_packed_int() < 0 && fract_exp != 0) {
    return MakeEvalError()
           << "Negative NUMERIC value cannot be raised to a fractional power";
  }

  NumericValue value = NumericValue::Abs(*this);
  if (exp_sign < 0) {
    // If the exponent is negative then we compute the result by raising the
    // reciprocal to the positive exponent, i.e. x^(-n) is computed as (1/x)^n.
    auto value_status = NumericValue(1).Divide(value);
    if (!value_status.ok()) {
      return MakeEvalError() << "numeric overflow";
    }
    value = value_status.ValueOrDie();
  }

  FixedUint<32, 7> ret(
      static_cast<unsigned __int128>(NumericValue(1).as_packed_int()));
  FixedUint<32, 7> squared(
      static_cast<unsigned __int128>(value.as_packed_int()));

  // We are performing computations with extra digits of precision added. This
  // allows to preserve precision when raising small numbers, like 1.001, to
  // huge powers. We also use extra precision to perform rounding.
  ret *= kScalingFactor;
  squared *= kScalingFactor;

  const FixedUint<32, 7> kScalingFactorSquareHalf(
      static_cast<uint64_t>(kScalingFactor) * kScalingFactor / 2);
  for (;; integer_exp /= 2) {
    if ((integer_exp % 2) != 0) {
      if (!ret.MultiplyAndCheckOverflow(squared)) {
        return MakeEvalError() << "numeric overflow";
      }
      // Restore the scale.
      ret += kScalingFactorSquareHalf;
      ret /= kScalingFactor;
      ret /= kScalingFactor;
    }
    if (integer_exp <= 1) {
      break;
    }
    if (!squared.MultiplyAndCheckOverflow(squared)) {
      return MakeEvalError() << "numeric overflow";
    }
    // Restore the scale.
    squared += kScalingFactorSquareHalf;
    squared /= kScalingFactor;
    squared /= kScalingFactor;
  }

  // Round away from zero.
  ret += (kScalingFactor / 2);
  // Remove extra digits of precision that were introduced in the beginning.
  ret /= kScalingFactor;

  ZETASQL_ASSIGN_OR_RETURN(NumericValue res,
                   NumericValue::FromFixedUint(ret, result_is_negative));

  // We handle the practional part of the exponent by raising the original value
  // to the fractional part of the exponent by converting them to doubles and
  // using the standard library's pow() function.
  if (fract_exp != 0) {
    // TODO Using std::pow() gives a result with reasonable precision
    // (comparable to MS SQL and MySQL), but we can probably do better here.
    // Explore a more accurate implementation in the future.
    double fract_pow = std::pow(
        value.ToDouble(),
        static_cast<double>(fract_exp) / kScalingFactor);
    ZETASQL_ASSIGN_OR_RETURN(NumericValue fract_term,
                     NumericValue::FromDouble(fract_pow));
    ZETASQL_ASSIGN_OR_RETURN(res, res.Multiply(fract_term));
  }

  return res;
}

zetasql_base::StatusOr<NumericValue> NumericValue::RoundInternal(
    int64_t digits, bool round_away_from_zero) const {
  if (digits >= kMaxFractionalDigits) {
    // Rounding beyond the max number of supported fractional digits has no
    // effect.
    return *this;
  }

  if (digits <= -kMaxIntegerDigits) {
    // Rounding all digits away results in zero.
    return NumericValue();
  }

  __int128 value = as_packed_int();
  __int128 trunc_factor = internal::int128_exp(
      10, static_cast<int>(kMaxFractionalDigits - digits));

  if (round_away_from_zero) {
    __int128 rounding_term =
        value < 0 ? -trunc_factor / 2 : trunc_factor / 2;
    if (internal::int128_add_overflow(value, rounding_term, &value)) {
      return MakeEvalError() << "numeric overflow: ROUND(" << ToString() << ", "
                             << digits << ")";
    }
  }

  value /= trunc_factor;
  value *= trunc_factor;
  auto res_status = NumericValue::FromPackedInt(value);
  if (!res_status.ok()) {
    return MakeEvalError() << "numeric overflow: ROUND(" << ToString() << ", "
                           << digits << ")";
  }
  return res_status.ValueOrDie();
}

zetasql_base::StatusOr<NumericValue> NumericValue::Round(int64_t digits) const {
  return RoundInternal(digits, /*round_away_from_zero*/ true);
}

NumericValue NumericValue::Trunc(int64_t digits) const {
  return RoundInternal(digits, /*round_away_from_zero*/ false).ValueOrDie();
}

zetasql_base::StatusOr<NumericValue> NumericValue::Ceiling() const {
  __int128 value = as_packed_int();
  if (value % kScalingFactor > 0) {
    if (ABSL_PREDICT_FALSE(internal::int128_add_overflow(
            value, kScalingFactor, &value))) {
      return MakeEvalError() << "numeric overflow: CEIL(" << ToString() << ")";
    }
  }
  value /= kScalingFactor;
  value *= kScalingFactor;
  auto res_status = NumericValue::FromPackedInt(value);
  if (!res_status.ok()) {
    return MakeEvalError() << "numeric overflow: CEIL(" << ToString() << ")";
  }
  return res_status.ValueOrDie();
}

zetasql_base::StatusOr<NumericValue> NumericValue::Floor() const {
  __int128 value = as_packed_int();
  if (value % kScalingFactor < 0) {
    if (ABSL_PREDICT_FALSE(internal::int128_add_overflow(
            value, -static_cast<__int128>(kScalingFactor), &value))) {
      return MakeEvalError() << "numeric overflow: FLOOR(" << ToString() << ")";
    }
  }
  value /= kScalingFactor;
  value *= kScalingFactor;
  auto res_status = NumericValue::FromPackedInt(value);
  if (!res_status.ok()) {
    return MakeEvalError() << "numeric overflow: FLOOR(" << ToString() << ")";
  }
  return res_status.ValueOrDie();
}

zetasql_base::StatusOr<NumericValue> NumericValue::Divide(NumericValue rh) const {
  const __int128 value = as_packed_int();
  const __int128 rh_value = rh.as_packed_int();
  const bool is_negative = value < 0;
  const bool rh_is_negative = rh_value < 0;

  if (ABSL_PREDICT_TRUE(rh_value != 0)) {
    FixedUint<32, 5> dividend(int128_abs(value));
    unsigned __int128 divisor = int128_abs(rh_value);

    // To preserve the scale of the result we need to multiply the dividend by
    // the scaling factor first.
    dividend *= kScalingFactor;
    dividend += FixedUint<32, 5>(divisor >> 1);
    dividend /= FixedUint<32, 5>(divisor);

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

zetasql_base::StatusOr<NumericValue> NumericValue::IntegerDivide(
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

std::string NumericValue::SerializeAsProtoBytes() const {
  __int128 value = as_packed_int();

  std::string ret;

  if (value == 0) {
    ret.push_back(0);
    return ret;
  }

  const unsigned __int128 abs_value = int128_abs(value);
  const uint64_t abs_value_hi = static_cast<uint64_t>(abs_value >> kBitsPerUint64);
  const uint64_t abs_value_lo = static_cast<uint64_t>(abs_value);

  int non_zero_bit_idx = 0;
  if (abs_value_hi != 0) {
    non_zero_bit_idx =
        zetasql_base::Bits::FindMSBSetNonZero64(abs_value_hi) + kBitsPerUint64;
  } else {
    non_zero_bit_idx = zetasql_base::Bits::FindMSBSetNonZero64(abs_value_lo);
  }

  int non_zero_byte_idx = non_zero_bit_idx / kBitsPerByte;
  const char non_zero_byte = static_cast<char>(
      abs_value >> (non_zero_byte_idx * kBitsPerByte));
  if ((non_zero_byte & 0x80) != 0) {
    ++non_zero_byte_idx;
  }

  ret.resize(non_zero_byte_idx + 1);
#ifdef IS_BIG_ENDIAN
  // Big endian platforms are not supported in production right now, so we do
  // not care about optimizations here.
  for (int i = 0; i < non_zero_byte_idx + 1; ++i, value >>= kBitsPerByte) {
    ret[i] = static_cast<char>(value);
  }
#else
  // TODO explore unrolling this call to memcpy.
  memcpy(&ret[0], reinterpret_cast<const void*>(&value),
         non_zero_byte_idx + 1);
#endif

  return ret;
}

zetasql_base::StatusOr<NumericValue> NumericValue::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  if (bytes.empty() || (bytes.size() > 16)) {
    return MakeEvalError() << "Invalid numeric encoding";
  }

  __int128 res = 0;

#ifdef IS_BIG_ENDIAN
  // Big endian platforms are not supported in production right now, so we do
  // not care about optimizations here.
  for (int i = bytes.size() - 1; i >= 0; --i) {
    res = (res << kBitsPerByte) | bytes[i];
  }
#else
  // TODO explore unrolling this call to memcpy.
  memcpy(&res, bytes.data(), bytes.size());
#endif

  // Extend sign if the stored number is negative.
  if ((bytes[bytes.size() - 1] & 0x80) != 0) {
    __int128 sign = 0;
    zetasql_base::Bits::SetBits(1, kBitsPerInt128 - 1, 1, &sign);
    sign >>= (kBytesPerInt128 - bytes.size()) * kBitsPerByte;
    res |= sign;
  }

  return NumericValue::FromPackedInt(res);
}

std::ostream& operator<<(std::ostream& out, NumericValue value) {
  return out << value.ToString();
}

void NumericValue::Aggregator::Add(NumericValue value) {
  const __int128 v = value.as_packed_int();
  if (ABSL_PREDICT_FALSE(internal::int128_add_overflow(
      sum_lower_, v, &sum_lower_))) {
    sum_upper_ += v < 0 ? -1 : 1;
  }
}

zetasql_base::StatusOr<NumericValue> NumericValue::Aggregator::GetSum() const {
  if (sum_upper_ != 0) {
    return MakeEvalError() << "numeric overflow: SUM";
  }

  auto res_status = NumericValue::FromPackedInt(sum_lower_);
  if (!res_status.ok()) {
    return MakeEvalError() << "numeric overflow: SUM";
  }
  return res_status.ValueOrDie();
}

zetasql_base::StatusOr<NumericValue>
NumericValue::Aggregator::GetAverage(uint64_t count) const {
  constexpr __int128 kInt128Min = static_cast<__int128>(
      static_cast<unsigned __int128>(1) << (kBitsPerInt128 - 1));

  if (count == 0) {
    return MakeEvalError() << "division by zero: AVG";
  }

  // The code below constructs an unsigned 196 bits of FixedUint<32, 7> from
  // sum_upper_ and sum_lower_. The following cases need to be considered:
  // 1) If sum_upper_ is zero, the entire value (including the sign) comes
  //    from sum_lower_. We need to get abs(sum_lower_) because the division
  //    works on unsigned values.
  // 2) If sum_upper_ is non-zero, the sign comes from it and sum_lower
  //    may have a different sign. For example, if sum_upper_ is 3 and
  //    sum_lower_ is -123, that means that the total value is
  //    3 * 2^128 - 123. Since we pass an unsigned value to the division
  //    method, the upper part needs to be adjusted by -1 in that case so that
  //    the dividend looks like 2 * 2^128 + some_very_large_128_bit_value
  //    where some_very_large_128_bit_value is -123 casted to an unsigned value.
  // 3) If sum_lower_ is kInt128Min we can't change its sign because there is
  //    no positive number that complements kInt128Min. We leave it as it is
  //    because kInt128Min (0x8000...0000) complements itself when converted to
  //    the unsigned 128 bit value.
  bool negate = false;
  __int128 lower;
  uint64_t upper_abs = std::abs(sum_upper_);

  if (ABSL_PREDICT_TRUE(upper_abs == 0)) {
    negate = sum_lower_ < 0;
    lower = (sum_lower_ != kInt128Min) ? int128_abs(sum_lower_) : sum_lower_;
  } else {
    negate = sum_upper_ < 0;
    lower = (negate && sum_lower_ != kInt128Min) ? -sum_lower_ : sum_lower_;
    if (lower < 0)
      upper_abs--;
  }

  // The reason we need 224 bits of precision is because the constructor
  // (uint64_t hi, unsigned __int128 low) needs 192 bits; on top of that we need
  // 32 bits to be able to normalize the numbers before performing long division
  // (see LongDivision()).
  FixedUint<32, 6> dividend(upper_abs, static_cast<unsigned __int128>(lower));
  dividend += FixedUint<32, 6>(count >> 1);
  dividend /= FixedUint<32, 6>(count);

  auto res_status = NumericValue::FromFixedUint(dividend, negate);
  if (res_status.ok()) {
    return res_status;
  }
  return MakeEvalError() << "numeric overflow: AVG";
}

void NumericValue::Aggregator::MergeWith(const Aggregator& other) {
  if (ABSL_PREDICT_FALSE(internal::int128_add_overflow(
      sum_lower_, other.sum_lower_, &sum_lower_))) {
    sum_upper_ += other.sum_lower_ < 0 ? -1 : 1;
  }

  sum_upper_ += other.sum_upper_;
}

std::string NumericValue::Aggregator::SerializeAsProtoBytes() const {
  std::string res;
  res.resize(kBytesPerInt128 + kBytesPerInt64);
  uint64_t sum_lower_lo = static_cast<uint64_t>(sum_lower_ & ~uint64_t{0});
  uint64_t sum_lower_hi = static_cast<uint64_t>(
      static_cast<unsigned __int128>(sum_lower_) >> kBitsPerUint64);
  zetasql_base::LittleEndian::Store64(&res[0], sum_lower_lo);
  zetasql_base::LittleEndian::Store64(&res[kBytesPerInt64], sum_lower_hi);
  zetasql_base::LittleEndian::Store64(&res[kBytesPerInt128], sum_upper_);
  return res;
}

zetasql_base::StatusOr<NumericValue::Aggregator>
NumericValue::Aggregator::DeserializeFromProtoBytes(absl::string_view bytes) {
  if (bytes.size() != kBytesPerInt128 + kBytesPerInt64) {
    return MakeEvalError() << "Invalid NumericValue::Aggregator encoding";
  }

  Aggregator res;
  uint64_t sum_lower_lo = zetasql_base::LittleEndian::Load64(bytes.data());
  uint64_t sum_lower_hi =
      zetasql_base::LittleEndian::Load64(bytes.substr(kBytesPerInt64).data());
  res.sum_lower_ = static_cast<__int128>(
      (static_cast<unsigned __int128>(sum_lower_hi) << 64) + sum_lower_lo);
  res.sum_upper_ = zetasql_base::LittleEndian::Load64(bytes.substr(kBytesPerInt128).data());
  return res;
}

void NumericValue::SumAggregator::Add(NumericValue value) {
  sum_ += FixedInt<64, 3>(value.as_packed_int());
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
  uint64_t half_count_for_rounding = count >> 1;
  if (dividend.is_negative()) {
    dividend -= half_count_for_rounding;
  } else {
    dividend += half_count_for_rounding;
  }
  dividend /= count;

  auto res_status = NumericValue::FromFixedInt(dividend);
  if (res_status.ok()) {
    return res_status;
  }
  return MakeEvalError() << "numeric overflow: AVG";
}

void NumericValue::SumAggregator::MergeWith(const SumAggregator& other) {
  sum_ += other.sum_;
}

std::string NumericValue::SumAggregator::SerializeAsProtoBytes() const {
  return sum_.SerializeToBytes();
}

zetasql_base::StatusOr<NumericValue::SumAggregator>
NumericValue::SumAggregator::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  NumericValue::SumAggregator out;
  if (out.sum_.DeserializeFromBytes(bytes)) {
    return out;
  }
  return MakeEvalError() << "Invalid NumericValue::Aggregator encoding";
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
    return GetVariance(count, 0);
  }
  return absl::nullopt;
}

absl::optional<double> NumericValue::VarianceAggregator::GetSamplingVariance(
    uint64_t count) const {
  if (count > 1) {
    return GetVariance(count, 1);
  }
  return absl::nullopt;
}

absl::optional<double> NumericValue::VarianceAggregator::GetPopulationStdDev(
    uint64_t count) const {
  if (count > 0) {
    return std::sqrt(GetVariance(count, 0));
  }
  return absl::nullopt;
}

absl::optional<double> NumericValue::VarianceAggregator::GetSamplingStdDev(
    uint64_t count) const {
  if (count > 1) {
    return std::sqrt(GetVariance(count, 1));
  }
  return absl::nullopt;
}

void NumericValue::VarianceAggregator::MergeWith(
    const VarianceAggregator& other) {
  sum_ += other.sum_;
  sum_square_ += other.sum_square_;
}

double NumericValue::VarianceAggregator::GetVariance(
    uint64_t count, uint64_t count_offset) const {
  FixedInt<64, 6> numerator(sum_square_);
  numerator *= count;
  numerator -= ExtendAndMultiply(sum_, sum_);
  FixedUint<64, 3> denominator(count);
  denominator *= (count - count_offset);
  denominator *= (static_cast<uint64_t>(kScalingFactor) * kScalingFactor);
  return static_cast<double>(numerator) / static_cast<double>(denominator);
}

std::string NumericValue::VarianceAggregator::SerializeAsProtoBytes() const {
  std::string sum_bytes = sum_.SerializeToBytes();
  std::string sum_square_bytes = sum_square_.SerializeToBytes();
  std::string out;
  out.reserve(1 + sum_bytes.size() + sum_square_bytes.size());
  static_assert(sizeof(sum_) < 128);
  DCHECK_LT(sum_bytes.size(), 128);
  out.push_back(static_cast<int8_t>(sum_bytes.size()));
  absl::StrAppend(&out, sum_bytes, sum_square_bytes);
  return out;
}

zetasql_base::StatusOr<NumericValue::VarianceAggregator>
NumericValue::VarianceAggregator::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  if (!bytes.empty()) {
    int sum_len = bytes[0];
    if (sum_len < bytes.size() - 1) {
      NumericValue::VarianceAggregator out;
      if (out.sum_.DeserializeFromBytes(bytes.substr(1, sum_len)) &&
          out.sum_square_.DeserializeFromBytes(bytes.substr(sum_len + 1))) {
        return out;
      }
    }
  }
  return MakeEvalError() << "Invalid NumericValue::VarianceAggregator encoding";
}

}  // namespace zetasql
