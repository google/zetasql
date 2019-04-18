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

#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdlib>
#include <limits>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "absl/base/optimization.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/bits.h"
#include "zetasql/base/endian.h"
#include "zetasql/base/canonical_errors.h"
#include "zetasql/base/status_macros.h"

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

inline __int128 int128_abs(__int128 x) { return (x >= 0) ? x : -x; }

// This class provides an implementation for a 224-bit unsigned integer and some
// operations on it. This implementation serves as an aid for implementation of
// the NUMERIC division operation and should not be used outside of this file.
//
// Internally the 224-bit number is stored as an array of 7 uint32_t words.
//
// The reason we need 224 bits of precision to implement the NUMERIC division is
// because the NUMERIC value itself is represented as an 128-bit integer, we
// need extra 64 bits to preserve the precision and to have an extra decimal
// digit for rounding (total 10 decimal digits). On top of that we need 32 bits
// to be able to normalize the numbers before performing long division (see
// LongDivision()).
class Uint224 final {
 public:
  Uint224() {
    number_.fill(0);
  }

  explicit Uint224(__int128 x) {
    number_.fill(0);
    number_[0] = static_cast<uint32_t>(x);
    number_[1] = static_cast<uint32_t>(x >> 32);
    number_[2] = static_cast<uint32_t>(x >> 64);
    number_[3] = static_cast<uint32_t>(x >> 96);
  }

  explicit Uint224(uint64_t x) {
    number_.fill(0);
    number_[0] = static_cast<uint32_t>(x);
    number_[1] = static_cast<uint32_t>(x >> 32);
  }

  Uint224(uint64_t hi, unsigned __int128 low) {
    number_.fill(0);
    number_[0] = static_cast<uint32_t>(low);
    number_[1] = static_cast<uint32_t>(low >> 32);
    number_[2] = static_cast<uint32_t>(low >> 64);
    number_[3] = static_cast<uint32_t>(low >> 96);
    number_[4] = static_cast<uint32_t>(hi);
    number_[5] = static_cast<uint32_t>(hi >> 32);
  }

  // Multiplies this 224-bit number by the given uint32_t value. This operation
  // does not check for overflow.
  void MultiplyByUint32(uint32_t x) {
    uint32_t carry = 0;
    for (int i = 0; i < number_.size(); ++i) {
      uint64_t tmp = static_cast<uint64_t>(number_[i]) * x + carry;
      number_[i] = static_cast<uint32_t>(tmp);
      carry = static_cast<uint32_t>(tmp >> 32);
    }
  }

  // Multiplies this 224-bit number by the given unsigned 224-bit value. May
  // return OUT_OF_RANGE error if the result of the multiplication overflows.
  zetasql_base::Status Multiply(const Uint224& rh) {
    Uint224 res;

    uint32_t carry = 0;
    for (int j = 0; j < rh.number_.size(); ++j) {
      for (int i = 0; i < number_.size(); ++i) {
        if (i + j >= number_.size()) {
          if (ABSL_PREDICT_FALSE(carry != 0)) {
            return MakeEvalError() << "numeric overflow";
          }
          continue;
        }
        uint64_t tmp = static_cast<uint64_t>(number_[i]) * rh.number_[j] +
            res.number_[i + j] + carry;
        res.number_[i + j] = static_cast<uint32_t>(tmp);
        carry = static_cast<uint32_t>(tmp >> 32);
      }
    }

    *this = res;
    return zetasql_base::OkStatus();
  }

  // Adds a uint32_t value to this 224-bit number. This operation does not check
  // for overflow.
  void AddUint32(uint32_t x) {
    uint32_t carry = x;
    for (int i = 0; i < number_.size(); ++i) {
      uint64_t tmp = static_cast<uint64_t>(number_[i]) + carry;
      number_[i] = static_cast<uint32_t>(tmp);
      carry = static_cast<uint32_t>(tmp >> 32);
      if (carry == 0) {
        break;
      }
    }
  }

  // Divides this 224-bit number by a single uint32_t value. The caller is
  // responsible for ensuring that the value is not zero.
  void DivideByUint32(uint32_t x) {
    Uint224 quotient;

    uint32_t carry = 0;
    for (int i = static_cast<int>(number_.size()) - 1; i >= 0; --i) {
      uint64_t tmp = (static_cast<uint64_t>(carry) << 32) | number_[i];
      quotient.number_[i] = static_cast<uint32_t>(tmp / x);
      carry = tmp % x;
    }

    *this = quotient;
  }

  // Divides by the given 224-bit value. The caller is responsible for ensuring
  // that the divisor is not zero. If 'round_away_from_zero' is set to true then
  // the result will be rounded away from zero.
  void Divide(const Uint224& x, bool round_away_from_zero) {
    if (round_away_from_zero) {
      // Rounding is achieved by multiplying the current value by 10 to preserve
      // an extra decimal digit for rounding, then performing the actual
      // division, then adding 5 to the result (to round away from zero), and
      // finally dividing back by 10.
      MultiplyByUint32(10);
    }

    if (x.NonZeroLength() > 1) {
      LongDivision(x);
    } else {
      DivideByUint32(x.number_[0]);
    }

    if (round_away_from_zero) {
      AddUint32(5);
      // TODO this line can be sped up by providing a templated (by a
      // constant) implementation. Take another look at this to see if the
      // division by a constant can be unified between here and multiplication.
      DivideByUint32(10);
    }
  }

  // Returns OUT_OF_RANGE if the stored number cannot fit into int128. This is
  // possible when, for example, diving a relatively large number by a number
  // that is less than 1.
  zetasql_base::StatusOr<__int128> to_int128() const;

 private:
  // Returns the length of the number in uint32_t words minus the non-significant
  // leading zeroes.
  int NonZeroLength() const;

  // Shifts the number left by the given number of bits. The number of bits must
  // not exceed 32.
  void ShiftLeft(int bits);

  // Treats the given 'rh' value as a number consisting of 'n' uint32_t words and
  // subtracts it from this number starting at the given 'prefix_idx' index.
  void PrefixSubtract(const Uint224& rh, int prefix_idx, int n);

  // Treats the given 'rh' value as a number consisting of 'n' uint32_t words and
  // compares it with this number starting at the given 'prefix_idx' index.
  // Returns true if the prefix of this number under comparison is less than
  // 'rh'.
  bool PrefixLessThan(const Uint224& rh, int prefix_idx, int n) const;

  // Performs a long division by the given 224-bit number.
  void LongDivision(const Uint224& x);

  // The 224-bit number stored as a sequence of 32-bit words. The number is
  // stored in the little-endian order with the least significant word being at
  // the index 0.
  std::array<uint32_t, 7> number_;
};

inline zetasql_base::StatusOr<__int128> Uint224::to_int128() const {
  if (NonZeroLength() > 4 || (number_[3] >> (kBitsPerUint32 - 1)) > 0) {
    // Callers are expected to return a more specific error message.
    return MakeEvalError() << "numeric overflow";
  }
  return (static_cast<__int128>(number_[3]) << 96) |
         (static_cast<__int128>(number_[2]) << 64) |
         (static_cast<__int128>(number_[1]) << 32) |
         number_[0];
}

inline int Uint224::NonZeroLength() const {
  for (int i = static_cast<int>(number_.size()) - 1; i >= 0; --i) {
    if (number_[i] != 0) {
      return i + 1;
    }
  }
  return 0;
}

inline void Uint224::ShiftLeft(int bits) {
  for (int i = static_cast<int>(number_.size()) - 1; i > 0; --i) {
    number_[i] = (number_[i] << bits) |
                 (number_[i - 1] >> (kBitsPerUint32 - bits));
  }
  number_[0] <<= bits;
}

inline void Uint224::PrefixSubtract(
    const Uint224& rh, int prefix_idx, int n) {
  int32_t borrow = 0;
  for (int i = 0; i <= n; ++i) {
    int64_t tmp = static_cast<int64_t>(number_[prefix_idx - n + i]) -
                rh.number_[i] + borrow;
    borrow = tmp < 0 ? -1 : 0;
    number_[prefix_idx - n + i] = static_cast<uint32_t>(tmp);
  }
}

inline bool Uint224::PrefixLessThan(
    const Uint224& rh, int prefix_idx, int n) const {
  int i = n;
  for (; i > 0; --i) {
    if (number_[prefix_idx - n + i] != rh.number_[i]) {
      break;
    }
  }
  return number_[prefix_idx - n + i] < rh.number_[i];
}

void Uint224::LongDivision(const Uint224& x) {
  Uint224 divisor = x;

  // Find the actual length of the dividend and the divisor.
  int n = divisor.NonZeroLength();
  int m = NonZeroLength();

  // First we need to normalize the divisor to make the most significant digit
  // of it larger than radix/2 (radix being 2^32 in our case). This is necessary
  // for accurate guesses of the quotent digits. See Knuth "The Art of Computer
  // Programming" Vol.2 for details.
  //
  // We perform normalization by finding how far we need to shift to make the
  // most significant bit of the divisor 1. And then we shift both the divident
  // and the divisor thus preserving the result of the division.
  int non_zero_bit_idx = zetasql_base::Bits::FindMSBSetNonZero(
      divisor.number_[n - 1]);
  int shift_amount = kBitsPerUint32 - non_zero_bit_idx - 1;

  if (shift_amount > 0) {
    ShiftLeft(shift_amount);
    divisor.ShiftLeft(shift_amount);
  }

  Uint224 quotient;

  for (int i = m - n; i >= 0; --i) {
    // Make the guess of the quotent digit. The guess we take here is:
    //
    //   qhat = min((divident[m] * b + divident[m-1]) / divisor[n], b-1)
    //
    // where b is the radix, which in our case is 2^32. In "The Art of Computer
    // Programming" Vol.2 Knuth proves that given the normalization above this
    // guess is often accurate and when not it is always larger than the actual
    // quotent digit and is no larger than 2 removed from it.
    uint64_t tmp = (static_cast<uint64_t>(number_[i + n]) << 32) |
                 number_[i + n - 1];
    uint64_t quotent_candidate = tmp / divisor.number_[n - 1];
    if (quotent_candidate > std::numeric_limits<uint32_t>::max()) {
      quotent_candidate = std::numeric_limits<uint32_t>::max();
    }

    Uint224 dq = divisor;
    dq.MultiplyByUint32(static_cast<uint32_t>(quotent_candidate));

    // If the guess was not accurate, adjust it. As stated above, at the worst,
    // the original guess qhat is q + 2, where q is the actual quotent digit, so
    // this loop will not be executed more than 2 iterations.
    for (int iter = 0; PrefixLessThan(dq, i + n, n); ++iter) {
      DCHECK_LT(iter, 2);
      --quotent_candidate;
      dq = divisor;
      dq.MultiplyByUint32(static_cast<uint32_t>(quotent_candidate));
    }

    PrefixSubtract(dq, i + n, n);
    quotient.number_[i] = static_cast<uint32_t>(quotent_candidate);
  }

  *this = quotient;
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
        fractional_part = (fractional_part * 10 + digit + 5) / 10;
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
  int sign_a = int128_sign(value);
  int sign_b = int128_sign(rh_value);
  __int128 a = int128_abs(value);
  __int128 b = int128_abs(rh_value);

  uint64_t a1 = static_cast<uint64_t>(a >> 64);
  uint64_t a0 = static_cast<uint64_t>(a);
  uint64_t b1 = static_cast<uint64_t>(b >> 64);
  uint64_t b0 = static_cast<uint64_t>(b);

  // Partial products.
  unsigned __int128 p0 = static_cast<unsigned __int128>(a0) * b0;
  unsigned __int128 p1 = static_cast<unsigned __int128>(a1) * b0 +
                         static_cast<unsigned __int128>(a0) * b1;
  unsigned __int128 p2 = static_cast<unsigned __int128>(a1) * b1;

  // Potentially round the result of the multiplication.
  p0 += kScalingFactor / 2;

  // Propagate carry overs.
  p1 += static_cast<uint64_t>(p0 >> 64);
  p2 += static_cast<uint64_t>(p1 >> 64);

  // This is the max spillover in excess of 128 bits that can happen without
  // resulting in overflow as the result of increased factional precision after
  // multiplication. The precision will be adjusted below.
  const int32_t kMaxSpillOver = 293873587;
  if (ABSL_PREDICT_FALSE(p2 > kMaxSpillOver)) {
    return MakeEvalError() << "numeric overflow: " << ToString() << " * "
                           << rh.ToString();
  }

  uint32_t p1_hi = static_cast<uint32_t>(p1 >> 32);
  uint32_t p1_lo = static_cast<uint32_t>(p1);
  uint32_t p0_hi = static_cast<uint32_t>(p0 >> 32);
  uint32_t p0_lo = static_cast<uint32_t>(p0);

  // Now we need to adjust the scale of the result. We implement the long
  // division algorith here for two reasons. First, there is no existing
  // operator that would allow as to divide a number wider than int128, and
  // second we want to capitalize on the compiler's optimization of turning a
  // division by a constant into a multiplication. The compiler is expected to
  // emit no div instructions for the code below. We care about div instructions
  // because they are much more expensive compared to multiplication (for
  // example on Skylake throughput of a 64bit multiplication is 1 cycle,
  // compared to ~80-95 cycles for a division).
  uint64_t t1_hi = (static_cast<uint64_t>(p2) << 32) | p1_hi;
  uint32_t r1_hi = static_cast<uint32_t>(t1_hi / kScalingFactor);
  uint64_t t1_lo = (static_cast<uint64_t>(t1_hi % kScalingFactor) << 32) | p1_lo;
  uint32_t r1_lo = static_cast<uint32_t>(t1_lo / kScalingFactor);
  uint64_t t0_hi = (static_cast<uint64_t>(t1_lo % kScalingFactor) << 32) | p0_hi;
  uint32_t r0_hi = static_cast<uint32_t>(t0_hi / kScalingFactor);
  uint64_t t0_lo = (static_cast<uint64_t>(t0_hi % kScalingFactor) << 32) | p0_lo;
  uint32_t r0_lo = static_cast<uint32_t>(t0_lo / kScalingFactor);

  __int128 res = (static_cast<__int128>(r1_hi) << 96) |
                 (static_cast<__int128>(r1_lo) << 64) |
                 (static_cast<__int128>(r0_hi) << 32) |
                 r0_lo;

  auto numeric_value_status = FromPackedInt((sign_a * sign_b) < 0 ? -res : res);
  if (!numeric_value_status.ok()) {
    return MakeEvalError() << "numeric overflow: " << ToString() << " * "
                           << rh.ToString();
  }
  return numeric_value_status.ValueOrDie();
}

NumericValue NumericValue::Abs(NumericValue value) {
  // The result is expected to be within the valid range.
  return NumericValue(int128_abs(value.as_packed_int()));
}

NumericValue NumericValue::Sign(NumericValue value) {
  return NumericValue(
      static_cast<int64_t>(int128_sign(value.as_packed_int())));
}

zetasql_base::StatusOr<NumericValue> NumericValue::Power(NumericValue exp) const {
  // Any value raised to a zero power is always one.
  if (exp == NumericValue()) {
    return NumericValue(1);
  }

  // An attempt to raise zero to a negative power results in division by zero.
  if (*this == NumericValue() && exp.as_packed_int() < 0) {
    return MakeEvalError() << "division by zero: POW(" << ToString() << ", "
                           << exp.ToString() << ")";
  }

  // Otherwise zero raised to any power is still zero.
  if (*this == NumericValue()) {
    return NumericValue();
  }

  int exp_sign = int128_sign(exp.as_packed_int());
  __int128 integer_exp = int128_abs(exp.as_packed_int()) / kScalingFactor;
  __int128 fract_exp = int128_abs(exp.as_packed_int()) % kScalingFactor;

  // Determine the sign of the result.
  int result_sign = 1;
  if (as_packed_int() < 0 && (integer_exp % 2) != 0) {
    result_sign = -1;
  }

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
      return MakeEvalError() << "numeric overflow: POW(" << ToString() << ", "
                             << exp.ToString() << ")";
    }
    value = value_status.ValueOrDie();
  }

  Uint224 ret(NumericValue(1).as_packed_int());
  Uint224 squared(value.as_packed_int());

  // We are performing computations with extra digits of precision added. This
  // allows to preserve precision when raising small numbers, like 1.001, to
  // huge powers. We also use extra precision to perform rounding.
  ret.MultiplyByUint32(kScalingFactor);
  squared.MultiplyByUint32(kScalingFactor);

  for (;; integer_exp /= 2) {
    if ((integer_exp % 2) != 0) {
      if (!ret.Multiply(squared).ok()) {
        return MakeEvalError() << "numeric overflow: POW(" << ToString() << ", "
                               << exp.ToString() << ")";
      }
      // Restore the scale.
      ret.DivideByUint32(kScalingFactor);
      ret.DivideByUint32(kScalingFactor);
    }
    if (integer_exp <= 1) {
      break;
    }
    if (!squared.Multiply(squared).ok()) {
      return MakeEvalError() << "numeric overflow: POW(" << ToString() << ", "
                             << exp.ToString() << ")";
    }
    // Restore the scale.
    squared.DivideByUint32(kScalingFactor);
    squared.DivideByUint32(kScalingFactor);
  }

  // Round away from zero.
  ret.AddUint32(kScalingFactor / 2);
  // Remove extra digits of precision that were introduced in the beginning.
  ret.DivideByUint32(kScalingFactor);

  ZETASQL_ASSIGN_OR_RETURN(__int128 int128_res, ret.to_int128());
  ZETASQL_ASSIGN_OR_RETURN(NumericValue res, NumericValue::FromPackedInt(int128_res));

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

  return (result_sign < 0) ? NumericValue::UnaryMinus(res) : res;
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
  return DivideInternal(rh, /* round_away_from_zero */ true);
}

zetasql_base::StatusOr<NumericValue> NumericValue::DivideInternal(
    NumericValue rh, bool round_away_from_zero) const {
  const __int128 value = as_packed_int();
  const __int128 rh_value = rh.as_packed_int();
  const int sign_value = int128_sign(value);
  const int sign_rh_value = int128_sign(rh_value);

  if (rh_value == 0) {
    return MakeEvalError() << "division by zero: " << ToString() << " / "
                           << rh.ToString();
  }

  Uint224 dividend(int128_abs(value));
  Uint224 divisor(int128_abs(rh_value));

  // To preserve the scale of the result we need to multiply the dividend by the
  // scaling factor first.
  dividend.MultiplyByUint32(kScalingFactor);
  dividend.Divide(divisor, round_away_from_zero);

  auto to_int128_status = dividend.to_int128();
  if (!to_int128_status.ok()) {
    return MakeEvalError() << "numeric overflow: " << ToString() << " / "
                           << rh.ToString();
  }
  const __int128 res = to_int128_status.ValueOrDie();
  auto numeric_value_status = NumericValue::FromPackedInt(
      (sign_value * sign_rh_value) < 0 ? -res : res);
  if (!numeric_value_status.ok()) {
    return MakeEvalError() << "numeric overflow: " << ToString() << " / "
                           << rh.ToString();
  }
  return numeric_value_status.ValueOrDie();
}

zetasql_base::StatusOr<NumericValue> NumericValue::IntegerDivide(
    NumericValue rh) const {
  ZETASQL_ASSIGN_OR_RETURN(NumericValue quotent,
                   DivideInternal(rh, /* round_away_from_zero */ false));
  return quotent.Trunc(0);
}

zetasql_base::StatusOr<NumericValue> NumericValue::Mod(NumericValue rh) const {
  if ((rh != NumericValue()) &&
      (int128_abs(rh.as_packed_int()) / kScalingFactor == 0)) {
    // If the absolute value of the divisor is less than 1 then the operation is
    // equivalent to a multiplication and the remainder is always zero.
    return NumericValue();
  }

  ZETASQL_ASSIGN_OR_RETURN(NumericValue q, IntegerDivide(rh));
  ZETASQL_ASSIGN_OR_RETURN(NumericValue tmp_product, q.Multiply(rh));
  return Subtract(tmp_product);
}

std::string NumericValue::SerializeAsProtoBytes() const {
  const __int128 value = as_packed_int();

  std::string ret;

  if (value == 0) {
    ret.push_back(0);
    return ret;
  }

  const __int128 abs_value = int128_abs(value);
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

  // The code below constructs an unsigned 196 bits of Uint224 from
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
  int sign;
  __int128 lower;
  uint64_t upper_abs = std::abs(sum_upper_);

  if (ABSL_PREDICT_TRUE(upper_abs == 0)) {
    sign = int128_sign(sum_lower_);
    lower = (sum_lower_ != kInt128Min) ? int128_abs(sum_lower_) : sum_lower_;
  } else {
    sign = sum_upper_ < 0 ? -1 : 1;
    lower = (sign < 0 && sum_lower_ != kInt128Min) ? -sum_lower_ : sum_lower_;
    if (lower < 0)
      upper_abs--;
  }

  Uint224 dividend(upper_abs, static_cast<unsigned __int128>(lower));
  dividend.Divide(Uint224(count), /* round_away_from_zero */ true);

  auto res_status = dividend.to_int128();
  if (!res_status.ok()) {
    return MakeEvalError() << "numeric overflow: AVG";
  }
  __int128 res = res_status.ValueOrDie();
  auto numeric_status = NumericValue::FromPackedInt(sign < 0 ? -res : res);
  if (!numeric_status.ok()) {
    return MakeEvalError() << "numeric overflow: AVG";
  }
  return numeric_status.ValueOrDie();
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

}  // namespace zetasql
