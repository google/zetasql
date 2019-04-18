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

#ifndef ZETASQL_PUBLIC_NUMERIC_VALUE_H_
#define ZETASQL_PUBLIC_NUMERIC_VALUE_H_

#include <cstddef>
#include <iosfwd>
#include <memory>
#include <string>

#include <cstdint>
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

// This class represents values of the ZetaSQL NUMERIC type. Such values are
// decimal numbers with maximum total precision of 38 decimal digits and fixed
// scale of 9 decimal digits.
//
// Internally NUMERIC values are stored as scaled 128 bit integers.
class NumericValue final {
 public:
  // Default constructor, constructs a zero value.
  constexpr NumericValue();

  // In order to allow simple constants e.g. NumericValue(0) it is necessary
  // to define all possible built-in types.
  explicit constexpr NumericValue(int value);
  explicit constexpr NumericValue(unsigned int value);
  explicit constexpr NumericValue(long value);           // NOLINT(runtime/int)
  explicit constexpr NumericValue(unsigned long value);  // NOLINT(runtime/int)
  explicit constexpr NumericValue(long long value);      // NOLINT(runtime/int)
  // NOLINTNEXTLINE(runtime/int)
  explicit constexpr NumericValue(unsigned long long value);

  // NUMERIC minimum and maximum limits.
  static constexpr NumericValue MaxValue();
  static constexpr NumericValue MinValue();

  // Constructs a Numeric object using its packed representation. May return
  // OUT_OF_RANGE error if the given value is outside the range of valid
  // NUMERIC values.
  static zetasql_base::StatusOr<NumericValue> FromPackedInt(__int128 value);

  // Constructs a Numeric object from the high and low bits of the packed
  // integer representation. May return OUT_OF_RANGE error if the combined 128
  // bit value is outside the range of valid NUMERIC values.
  static zetasql_base::StatusOr<NumericValue> FromHighAndLowBits(uint64_t high_bits,
                                                         uint64_t low_bits);

  // Parses a textual representation of a NUMERIC value. Returns an error if the
  // given std::string cannot be parsed as a number or if the textual numeric value
  // exceeds NUMERIC precision. This method will also return an error if the
  // textual representation has more than 9 digits after the decimal point.
  //
  // This method accepts the same number formats as ZetaSQL floating point
  // literals, namely:
  //   [+-]DIGITS[.[DIGITS]][e[+-]DIGITS]
  //   [+-][DIGITS].DIGITS[e[+-]DIGITS]
  static zetasql_base::StatusOr<NumericValue> FromStringStrict(absl::string_view str);

  // Like FromStringStrict() but accepts more than 9 digits after the point
  // rounding the number half away from zero.
  static zetasql_base::StatusOr<NumericValue> FromString(absl::string_view str);

  // Constructs a NumericValue from a double. This method might return an error
  // if the given value cannot be converted to a NUMERIC (e.g. NaN).
  static zetasql_base::StatusOr<NumericValue> FromDouble(double value);

  // Arithmetic operators. These operators can return OUT_OF_RANGE error on
  // overflow. Additionally the division returns OUT_OF_RANGE if the divisor is
  // zero.
  zetasql_base::StatusOr<NumericValue> Add(NumericValue rh) const;
  zetasql_base::StatusOr<NumericValue> Subtract(NumericValue rh) const;
  zetasql_base::StatusOr<NumericValue> Multiply(NumericValue rh) const;
  zetasql_base::StatusOr<NumericValue> Divide(NumericValue rh) const;

  // An integer division operation. Similar to general division followed by
  // truncating the result to the whole integer. May return OUT_OF_RANGE if an
  // overflow or division by zero happens. This operation is the same as the SQL
  // DIV function.
  zetasql_base::StatusOr<NumericValue> IntegerDivide(NumericValue rh) const;
  // Returns a remainder of division of this numeric value by the given divisor.
  // Returns an OUT_OF_RANGE error if the divisor is zero.
  zetasql_base::StatusOr<NumericValue> Mod(NumericValue rh) const;

  // Comparison operators.
  bool operator==(NumericValue rh) const;
  bool operator!=(NumericValue rh) const;
  bool operator<(NumericValue rh) const;
  bool operator>(NumericValue rh) const;
  bool operator>=(NumericValue rh) const;
  bool operator<=(NumericValue rh) const;

  // Math functions.
  static NumericValue UnaryMinus(NumericValue value);
  static NumericValue Abs(NumericValue value);
  static NumericValue Sign(NumericValue value);

  // Raises this numeric value to the given power and returns the result.
  // Returns OUT_OF_RANGE error on overflow.
  zetasql_base::StatusOr<NumericValue> Power(NumericValue exp) const;

  // Rounds this NUMERIC value to the given number of decimal digits after the
  // decimal point. 'digits' can be negative to cause rounding of the digits to
  // the left of the decimal point. Halfway cases are rounded away from zero.
  // Returns OUT_OF_RANGE if the rounding causes numerical overflow.
  zetasql_base::StatusOr<NumericValue> Round(int64_t digits) const;
  // Similar to the method above, but rounds towards zero, i.e. truncates the
  // number. Because this method truncates instead of rounding away from zero it
  // never causes an error.
  NumericValue Trunc(int64_t digits) const;

  // Rounds this NUMERIC value upwards, returning the integer least upper bound
  // of this value. Returns OUT_OF_RANGE error on overflow.
  zetasql_base::StatusOr<NumericValue> Ceiling() const;

  // Rounds this NUMERIC value downwards, returning the integer greatest lower
  // bound of this value. Returns OUT_OF_RANGE error on overflow.
  zetasql_base::StatusOr<NumericValue> Floor() const;

  // Returns hash code for the value.
  size_t HashCode() const;

  template <typename H>
  friend H AbslHashValue(H h, const NumericValue& v);

  // Converts the NUMERIC value into a value of another number type. T can be
  // one of int32_t, int64_t, uint32_t, uint64_t. Numeric values with fractional parts
  // will be rounded to a whole integer with a half away from zero rounding
  // semantics. This method will return OUT_OF_RANGE error if an overflow occurs
  // during conversion.
  template<class T> zetasql_base::StatusOr<T> To() const;

  // Converts the NUMERIC value to a floating point number.
  double ToDouble() const;

  // Converts the NUMERIC value into a std::string. String representation of NUMERICs
  // follow regular rules of textual numeric values representation. For example,
  // "1.34", "123", "0.23".
  std::string ToString() const;

  // Returns the packed NUMERIC value.
  __int128 as_packed_int() const;

  // Returns high 64 bits of the packed NUMERIC value.
  uint64_t high_bits() const;

  // Returns low 64 bits of the packed NUMERIC value.
  uint64_t low_bits() const;

  // Returns whether the NUMERIC value has a fractional part.
  bool has_fractional_part() const;

  // Serialization and deserialization methods for NUMERIC values that are
  // intended to be used to store them in protos. The encoding is variable in
  // length with max size of 16 bytes.
  //
  // Storage format:
  // NUMERIC values are written as integers in two's complement form. The
  // original numeric value is scaled by 10^9 to get the integer. The scaled
  // integer is written in little endian mode with the repeating most
  // significant bytes suppressed. The most significant bit of the most
  // significant byte in the written sequence of bytes represents the sign.
  std::string SerializeAsProtoBytes() const;
  static zetasql_base::StatusOr<NumericValue> DeserializeFromProtoBytes(
      absl::string_view bytes);

  // Aggregates multiple NUMERIC values and produces sum and average of all
  // values. This class handles a temporary overflow while adding values.
  // OUT_OF_RANGE error is generated only when retrieving the sum and only if
  // the final sum is outside of the valid NUMERIC range.
  class Aggregator final {
   public:
    // Adds a NUMERIC value to the input.
    void Add(NumericValue value);
    // Returns sum of all input values. Returns OUT_OF_RANGE error on overflow.
    zetasql_base::StatusOr<NumericValue> GetSum() const;
    // Returns sum of all input values divided by the specified divisor.
    // Returns OUT_OF_RANGE error on overflow of the division result.
    // Please note that the division result may be in the valid range even if
    // the sum exceeds the range.
    zetasql_base::StatusOr<NumericValue> GetAverage(uint64_t count) const;

    // Merges the state with other Aggregator instance's state.
    void MergeWith(const Aggregator& other);

    // Serialization and deserialization methods that are intended to be
    // used to store the state in protos.
    // sum_lower and sum_upper fields are written as integers in binary little
    // endian mode - 16 bytes of sum_lower followed by 8 bytes of sum_upper.
    std::string SerializeAsProtoBytes() const;
    static zetasql_base::StatusOr<Aggregator> DeserializeFromProtoBytes(
        absl::string_view bytes);

   private:
    // Higher 64 bits and lower 128 bits of the sum.
    // Both sum_lower and sum_upper are signed values. For smaller sums that
    // fit entirely in 128 bits, sum_lower provides both the value and the sign
    // of the sum. For larger values the sign of the sum is determined by
    // sum_upper sign and sum_lower may have a different sign.
    // The total sum can be expressed as sum_upper * 2^128 + sum_lower.
    __int128 sum_lower_ = 0;
    int64_t sum_upper_ = 0;
  };

 private:
  NumericValue(uint64_t high_bits, uint64_t low_bits);
  explicit constexpr NumericValue(__int128 value);

  static zetasql_base::StatusOr<NumericValue> FromStringInternal(
      absl::string_view str, bool is_strict);

  // Rounds this NUMERIC value to the given number of decimal digits after the
  // decimal point (or before the decimal point if 'digits' is negative).
  // Halfway cases are rounded away from zero if 'round_away_from_zero' is set
  // to true, towards zero otherwise. May return OUT_OF_RANGE error if rounding
  // causes overflow.
  zetasql_base::StatusOr<NumericValue> RoundInternal(
      int64_t digits, bool round_away_from_zero) const;

  // Perfoms the division operation. Will round the result if
  // 'round_away_from_zero' is set to true. May return OUT_OF_RANGE if an
  // overflow or division by zero occurs.
  zetasql_base::StatusOr<NumericValue> DivideInternal(
      NumericValue rh, bool round_away_from_zero) const;

  static constexpr uint32_t kScalingFactor = 1000000000;

  // A NUMERIC value is stored as a scaled integer, the original NUMERIC value
  // is multiplied by the scaling factor 10^9. The intended representation is
  // __int128, but since __int128 causes crashes for loads and stores that are
  // not 16-byte aligned, it is split into two 64-bit components here.
  uint64_t high_bits_;
  uint64_t low_bits_;
};

// Allow NUMERIC values to be logged.
std::ostream& operator<<(std::ostream& out, NumericValue value);

}  // namespace zetasql

// Include implementation of the inline methods. Out of line for clarity. It is
// not intended to be directly used.
#include "zetasql/public/numeric_value.inc"  

#endif  // ZETASQL_PUBLIC_NUMERIC_VALUE_H_
