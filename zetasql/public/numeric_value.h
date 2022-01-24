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

#ifndef ZETASQL_PUBLIC_NUMERIC_VALUE_H_
#define ZETASQL_PUBLIC_NUMERIC_VALUE_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/multiprecision_int.h"
#include "zetasql/public/numeric_constants.h"
#include "absl/base/attributes.h"
#include <cstdint>
#include "absl/base/optimization.h"
#include "absl/base/port.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "zetasql/base/status_builder.h"

namespace zetasql {

// This class represents values of the ZetaSQL NUMERIC type. Such values are
// decimal numbers with maximum total precision of 38 decimal digits and fixed
// scale of 9 decimal digits.
//
// Internally NUMERIC values are stored as scaled 128 bit integers.
class NumericValue final {
 public:
  // Must use integral_constant to utilize the compiler optimization for integer
  // divisions with constant 32-bit divisors.
  static constexpr std::integral_constant<uint32_t, internal::k1e9>
      kScalingFactor{};
  static constexpr int kMaxIntegerDigits = 29;
  static constexpr int kMaxFractionalDigits = 9;
  static constexpr int kMaxPrecision = kMaxIntegerDigits + kMaxFractionalDigits;
  // Default constructor, constructs a zero value.
  constexpr NumericValue();

  // In order to allow simple constants e.g. NumericValue(0) it is necessary
  // to define all possible built-in types.
  explicit constexpr NumericValue(int value);
  explicit constexpr NumericValue(unsigned int value);
  explicit constexpr NumericValue(long value);                // NOLINT
  explicit constexpr NumericValue(unsigned long value);       // NOLINT
  explicit constexpr NumericValue(long long value);           // NOLINT
  explicit constexpr NumericValue(unsigned long long value);  // NOLINT

  // NUMERIC minimum and maximum limits.
  static constexpr NumericValue MaxValue();
  static constexpr NumericValue MinValue();

  // Constructs a Numeric object using its packed representation. May return
  // OUT_OF_RANGE error if the given value is outside the range of valid
  // NUMERIC values.
  static absl::StatusOr<NumericValue> FromPackedInt(__int128 value);
  // Returns value / kScalingFactor.
  static constexpr NumericValue FromScaledValue(int64_t value);

  // Returns a value representing little_endian_value / pow(10, scale), where
  // little_endian_value represents an integer in serialized binary format
  // in little endian byte order, using 2's complement encoding for negative
  // values (the last byte's highest bit determines the sign). For example, if
  // little_endian_value = "\x00\xff" and scale = 5, then the result is
  // -256 / pow(10, 5). When the result has more than 9 digits in the
  // fractional part, the result will be rounded half away from zero if
  // allow_rounding is true, or an error will be returned if allow_rounding is
  // false. This method is not optimized for performance, and is much slower
  // than FromScaledValue(int64_t), FromPackedInt and FromHighAndLowBits.
  static absl::StatusOr<NumericValue> FromScaledLittleEndianValue(
      absl::string_view little_endian_value, int scale, bool allow_rounding);

  // Returns <*this> / pow(10, kMaxFractionalDigits - <scale>), or an error
  // if <scale> is not in the supported range [0, kMaxFractionalDigits].
  //
  // When there are more than <scale> significant fractional digits in the
  // result,
  // * If <allow_rounding> is true, the result will be rounded away from zero
  //   to <scale> fractional digits;
  // * If <allow_rounding> is false, an error is returned.
  absl::StatusOr<NumericValue> Rescale(int scale, bool allow_rounding) const;

  // Constructs a Numeric object from the high and low bits of the packed
  // integer representation. May return OUT_OF_RANGE error if the combined 128
  // bit value is outside the range of valid NUMERIC values.
  static absl::StatusOr<NumericValue> FromHighAndLowBits(uint64_t high_bits,
                                                         uint64_t low_bits);

  // Parses a textual representation of a NUMERIC value. Returns an error if the
  // given string cannot be parsed as a number or if the textual numeric value
  // exceeds NUMERIC precision. This method will also return an error if the
  // textual representation has more than 9 digits after the decimal point.
  //
  // This method accepts the same number formats as ZetaSQL floating point
  // literals, namely:
  //   [+-]DIGITS[.[DIGITS]][e[+-]DIGITS]
  //   [+-][DIGITS].DIGITS[e[+-]DIGITS]
  static absl::StatusOr<NumericValue> FromStringStrict(absl::string_view str);

  // Like FromStringStrict() but accepts more than 9 digits after the point
  // rounding the number half away from zero.
  static absl::StatusOr<NumericValue> FromString(absl::string_view str);

  // Constructs a NumericValue from a double. This method might return an error
  // if the given value cannot be converted to a NUMERIC (e.g. NaN).
  static absl::StatusOr<NumericValue> FromDouble(double value);

  // Arithmetic operators. These operators can return OUT_OF_RANGE error on
  // overflow. Additionally the division returns OUT_OF_RANGE if the divisor is
  // zero.
  absl::StatusOr<NumericValue> Add(NumericValue rh) const;
  absl::StatusOr<NumericValue> Subtract(NumericValue rh) const;
  absl::StatusOr<NumericValue> Multiply(NumericValue rh) const;
  absl::StatusOr<NumericValue> Divide(NumericValue rh) const;

  // An integer division operation. Similar to general division followed by
  // truncating the result to the whole integer. May return OUT_OF_RANGE if an
  // overflow or division by zero happens. This operation is the same as the SQL
  // DIV function.
  absl::StatusOr<NumericValue> DivideToIntegralValue(NumericValue rh) const;
  // Returns a remainder of division of this numeric value by the given divisor.
  // Returns an OUT_OF_RANGE error if the divisor is zero.
  absl::StatusOr<NumericValue> Mod(NumericValue rh) const;

  // Comparison operators.
  bool operator==(NumericValue rh) const;
  bool operator!=(NumericValue rh) const;
  bool operator<(NumericValue rh) const;
  bool operator>(NumericValue rh) const;
  bool operator>=(NumericValue rh) const;
  bool operator<=(NumericValue rh) const;

  // Math functions.
  NumericValue Negate() const;
  NumericValue Abs() const;
  int Sign() const;

  // Raises this numeric value to the given power and returns the result.
  // Returns OUT_OF_RANGE error on overflow.
  absl::StatusOr<NumericValue> Power(NumericValue exp) const;
  // Raise natural e to this numeric value and return the result.
  // Returns OUT_OF_RANGE error on overflow.
  absl::StatusOr<NumericValue> Exp() const;
  // Return natural logarithm of this numeric value.
  // Returns OUT_OF_RANGE error on non-positive value.
  absl::StatusOr<NumericValue> Ln() const;
  // Return base 10 logarithm of this numeric value.
  // Returns OUT_OF_RANGE error on non-positive value.
  absl::StatusOr<NumericValue> Log10() const;
  // Return logarithm of this numeric value on base.
  // Returns OUT_OF_RANGE error on non-positive value.
  absl::StatusOr<NumericValue> Log(NumericValue base) const;
  // Return square root of this numeric value.
  // Returns OUT_OF_RANGE error on negative value.
  absl::StatusOr<NumericValue> Sqrt() const;

  // Rounds this NUMERIC value to the given number of decimal digits after the
  // decimal point. 'digits' can be negative to cause rounding of the digits to
  // the left of the decimal point. Halfway cases are rounded away from zero.
  // Returns OUT_OF_RANGE if the rounding causes numerical overflow.
  absl::StatusOr<NumericValue> Round(int64_t digits) const;
  // Similar to the method above, but rounds towards zero, i.e. truncates the
  // number. Because this method truncates instead of rounding away from zero it
  // never causes an error.
  NumericValue Trunc(int64_t digits) const;

  // Rounds this NUMERIC value upwards, returning the integer least upper bound
  // of this value. Returns OUT_OF_RANGE error on overflow.
  absl::StatusOr<NumericValue> Ceiling() const;

  // Rounds this NUMERIC value downwards, returning the integer greatest lower
  // bound of this value. Returns OUT_OF_RANGE error on overflow.
  absl::StatusOr<NumericValue> Floor() const;

  // Returns hash code for the value.
  size_t HashCode() const;

  template <typename H>
  friend H AbslHashValue(H h, const NumericValue& v);

  // Converts the NUMERIC value into a value of another number type. T can be
  // one of int32_t, int64_t, uint32_t, uint64_t. Numeric values with fractional parts
  // will be rounded to a whole integer with a half away from zero rounding
  // semantics. This method will return OUT_OF_RANGE error if an overflow occurs
  // during conversion.
  template <class T>
  absl::StatusOr<T> To() const;

  // Converts the NUMERIC value to a floating point number.
  double ToDouble() const;

  // Converts the NUMERIC value into a string. String representation of NUMERICs
  // follows regular rules of textual numeric values representation. For
  // example, "1.34", "123", "0.23". AppendToString is typically more efficient
  // due to fewer memory allocations.
  std::string ToString() const;
  void AppendToString(std::string* output) const;

  struct FormatSpec {
    // Minimum total output size. If the formatted string is shorter than this
    // size, it will be padded with spaces or zeros, depending on zero_pad and
    // left_justify.
    uint32_t minimum_size = 0;
    // For DEFAULT, E_NOTATION_LOWER_CASE and E_NOTATION_UPPER_CASE modes,
    // this field means the number of digits after the decimal point (before
    // remove_trailing_zeros_after_decimal_point is applied) to print.
    // For GENERAL_FORMAT_LOWER_CASE and GENERAL_FORMAT_UPPER_CASE modes,
    // this field means the max number of significant digits to keep (before
    // remove_trailing_zeros_after_decimal_point is applied).
    // For all modes, the value is rounded if it has more digits than specified.
    uint32_t precision = 6;
    // See https://en.cppreference.com/w/c/io/fprintf for the documentation
    // of the modes.
    enum Mode : char {
      DEFAULT = 'f',                    // %f
      E_NOTATION_LOWER_CASE = 'e',      // %e
      E_NOTATION_UPPER_CASE = 'E',      // %E
      GENERAL_FORMAT_LOWER_CASE = 'g',  // %g
      GENERAL_FORMAT_UPPER_CASE = 'G',  // %G
    };
    Mode mode = DEFAULT;
    // Enum for the bit flags representing how to print the numeric value
    enum Flag : uint8_t {
      NO_FLAGS = 0x0,
      // If set, trailing zeros after the decimal point are removed.
      REMOVE_TRAILING_ZEROS_AFTER_DECIMAL_POINT = 0x1,
      // If set, print the decimal point even when there is no fractional
      // digit.
      ALWAYS_PRINT_DECIMAL_POINT = 0x2,
      // If set, print '+' sign for non-negative values.
      ALWAYS_PRINT_SIGN = 0x4,
      // If set, print a space in place of sign for non-negative values.
      // Ignored if ALWAYS_PRINT_SIGN is set.
      SIGN_SPACE = 0x8,
      // If set, pad the output with leading zeros after the sign to
      // minimum_size. Else, pad the output with leading spaces before
      // the sign to minimum_size. Ignored if LEFT_JUSTIFY is set.
      ZERO_PAD = 0x10,
      // If set, left-justify the output by appending padding after the numeric
      // value. Else, output is right-justified.
      LEFT_JUSTIFY = 0x20,
      // If set, the integer portion of the numeric value is formatted using
      // the ',' character (i.e. 23456789 is formatted as 23,456,789). Note that
      // if zero_pad if true, the leading zeros padded are NOT formatted with
      // the grouping character, even if use_grouping_char is true.
      USE_GROUPING_CHAR = 0x40,
    };
    // Flags controlling how to print the numeric value
    uint32_t format_flags = NO_FLAGS;
  };

  static_assert(sizeof(FormatSpec) <= 16, "Size of FormatSpec is too large");

  // Formats the NUMERIC value and appends the result to 'output'.
  // This method is much slower than AppendToString.
  void FormatAndAppend(FormatSpec spec, std::string* output) const;

  // Returns the packed NUMERIC value.
  constexpr __int128 as_packed_int() const;

  // Returns the packed uint64_t array in little endian order.
  std::array<uint64_t, 2> ToPackedLittleEndianArray() const {
    return {low_bits_, high_bits_};
  }

  // Returns high 64 bits of the packed NUMERIC value.
  constexpr uint64_t high_bits() const;

  // Returns low 64 bits of the packed NUMERIC value.
  constexpr uint64_t low_bits() const;

  // Returns whether the NUMERIC value has a fractional part.
  // Faster than computing Round/Trunc/Ceiling/Floor and comparing to *this.
  bool HasFractionalPart() const;

  // Serialization and deserialization methods for NUMERIC values that are
  // intended to be used to store them in protos. The encoding is variable in
  // length with max size of 16 bytes. SerializeAndAppendToProtoBytes is
  // typically more efficient due to fewer memory allocations.
  void SerializeAndAppendToProtoBytes(std::string* bytes) const;
  std::string SerializeAsProtoBytes() const {
    std::string result;
    SerializeAndAppendToProtoBytes(&result);
    return result;
  }
  static absl::StatusOr<NumericValue> DeserializeFromProtoBytes(
      absl::string_view bytes);

  // Aggregates multiple NUMERIC values and produces sum and average of all
  // values. This class handles a temporary overflow while adding values.
  // OUT_OF_RANGE error is generated only when retrieving the sum and only if
  // the final sum is outside of the valid NUMERIC range.
  class SumAggregator final {
   public:
    // Adds a NUMERIC value to the sum.
    void Add(NumericValue value);
    // Subtracts a NUMERIC value from the sum.
    void Subtract(NumericValue value);
    // Returns sum of all input values. Returns OUT_OF_RANGE error on overflow.
    absl::StatusOr<NumericValue> GetSum() const;
    // Returns sum of all input values divided by the specified divisor.
    // Returns OUT_OF_RANGE error on overflow of the division result.
    // Please note that the division result may be in the valid range even if
    // the sum exceeds the range.
    absl::StatusOr<NumericValue> GetAverage(uint64_t count) const;

    // Merges the state with other SumAggregator instance's state.
    void MergeWith(const SumAggregator& other);

    // SerializeAndAppendToProtoBytes is typically more efficient due to fewer
    // memory allocations.
    void SerializeAndAppendToProtoBytes(std::string* bytes) const;
    std::string SerializeAsProtoBytes() const {
      std::string result;
      SerializeAndAppendToProtoBytes(&result);
      return result;
    }
    static absl::StatusOr<SumAggregator> DeserializeFromProtoBytes(
        absl::string_view bytes);

    bool operator==(const SumAggregator& other) const {
      return sum_ == other.sum_;
    }

   private:
    FixedInt<64, 3> sum_;
  };

  // Aggregates the input of multiple NUMERIC values and provides functions for
  // the population/sample variance/standard deviation of the values in double
  // data type.
  class VarianceAggregator {
   public:
    // Adds a NUMERIC value to the input.
    void Add(NumericValue value);
    // Removes a previously added NUMERIC value from the input.
    // This method is provided for implementing analytic functions with
    // sliding windows. If the value has not been added to the input, or if it
    // has already been removed, then the result of this method is undefined.
    void Subtract(NumericValue value);
    // Returns the variance, or absl::nullopt if count is too low.
    absl::optional<double> GetVariance(uint64_t count, bool is_sampling) const;
    // Returns the population variance, or absl::nullopt if count is 0.
    absl::optional<double> GetPopulationVariance(uint64_t count) const {
      return GetVariance(count, /*is_sampling=*/false);
    }
    // Returns the sampling variance, or absl::nullopt if count < 2.
    absl::optional<double> GetSamplingVariance(uint64_t count) const {
      return GetVariance(count, /*is_sampling=*/true);
    }
    // Returns the standard deviation, or absl::nullopt if count is too low.
    absl::optional<double> GetStdDev(uint64_t count, bool is_sampling) const;
    // Returns the population standard deviation, or absl::nullopt if count is
    // 0.
    absl::optional<double> GetPopulationStdDev(uint64_t count) const {
      return GetStdDev(count, /*is_sampling=*/false);
    }
    // Returns the sampling standard deviation, or absl::nullopt if count < 2.
    absl::optional<double> GetSamplingStdDev(uint64_t count) const {
      return GetStdDev(count, /*is_sampling=*/true);
    }
    // Merges the state with other VarianceAggregator instance's state.
    void MergeWith(const VarianceAggregator& other);
    // Serialization and deserialization methods that are intended to be
    // used to store the state in protos.
    // sum_ is length prefixed and serialized, followed by sum_square_.
    // SerializeAndAppendToProtoBytes is typically more efficient due to fewer
    // memory allocations.
    void SerializeAndAppendToProtoBytes(std::string* bytes) const;
    std::string SerializeAsProtoBytes() const {
      std::string result;
      SerializeAndAppendToProtoBytes(&result);
      return result;
    }
    static absl::StatusOr<VarianceAggregator> DeserializeFromProtoBytes(
        absl::string_view bytes);

    bool operator==(const VarianceAggregator& other) const {
      return sum_ == other.sum_ && sum_square_ == other.sum_square_;
    }

   private:
    FixedInt<64, 3> sum_;
    FixedInt<64, 5> sum_square_;
  };

  class CorrelationAggregator;

  // Aggregates the input of multiple pairs of NUMERIC values and provides
  // functions for the population/sample covariance of the pairs in double data
  // type.
  class CovarianceAggregator {
   public:
    // Adds a pair of NUMERIC values to the input.
    void Add(NumericValue x, NumericValue y);
    // Removes a previously added pair of NUMERIC values from the input.
    // This method is provided for implementing analytic functions with
    // sliding windows. If the pair has not been added to the input, or if it
    // has already been removed, then the result of this method is undefined.
    void Subtract(NumericValue x, NumericValue y);
    // Returns the covariance, or absl::nullopt if count is too low.
    absl::optional<double> GetCovariance(uint64_t count,
                                         bool is_sampling) const;
    // Returns the population covariance of non-null pairs from input, or
    // absl::nullopt if count is 0.
    absl::optional<double> GetPopulationCovariance(uint64_t count) const {
      return GetCovariance(count, /*is_sampling=*/false);
    }
    // Returns the sample covariance of non-null pairs from input, or
    // absl::nullopt if count < 2.
    absl::optional<double> GetSamplingCovariance(uint64_t count) const {
      return GetCovariance(count, /*is_sampling=*/true);
    }
    // Merges the state with other CovarianceAggregator instance's state.
    void MergeWith(const CovarianceAggregator& other);
    // Serialization and deserialization methods that are intended to be
    // used to store the state in protos.
    // sum_product_ is length prefixed and serialized, sum_x_ is length prefixed
    // and serialized, followed by sum_y_.
    // SerializeAndAppendToProtoBytes is typically more efficient due to fewer
    // memory allocations.
    void SerializeAndAppendToProtoBytes(std::string* bytes) const;
    std::string SerializeAsProtoBytes() const {
      std::string result;
      SerializeAndAppendToProtoBytes(&result);
      return result;
    }
    static absl::StatusOr<CovarianceAggregator> DeserializeFromProtoBytes(
        absl::string_view bytes);

    bool operator==(const CovarianceAggregator& other) const {
      return sum_product_ == other.sum_product_ && sum_x_ == other.sum_x_ &&
             sum_y_ == other.sum_y_;
    }

   private:
    friend class CorrelationAggregator;
    FixedInt<64, 5> sum_product_;
    FixedInt<64, 3> sum_x_;
    FixedInt<64, 3> sum_y_;
  };

  // Aggregates the input of multiple pairs of NUMERIC values and provides
  // functions for the correlation of the pairs in double data type.
  class CorrelationAggregator {
   public:
    // Adds a pair of NUMERIC values to the input.
    void Add(NumericValue x, NumericValue y);
    // Removes a previously added pair of NUMERIC values from the input.
    // This method is provided for implementing analytic functions with
    // sliding windows. If the pair has not been added to the input, or if it
    // has already been removed, then the result of this method is undefined.
    void Subtract(NumericValue x, NumericValue y);
    // Returns the correlation coefficient for non-null pairs from input.
    absl::optional<double> GetCorrelation(uint64_t count) const;
    // Merges the state with other CorrelationAggregator instance's state.
    void MergeWith(const CorrelationAggregator& other);
    // Serialization and deserialization methods that are intended to be
    // used to store the state in protos.
    // Each of cov_agg_'s members are length prefixed and serialized, followed
    // by sum_square_x_ length prefixed and serialized and then sum_square_y_
    // serialized.
    // SerializeAndAppendToProtoBytes is typically more efficient due to fewer
    // memory allocations.
    void SerializeAndAppendToProtoBytes(std::string* bytes) const;
    std::string SerializeAsProtoBytes() const {
      std::string result;
      SerializeAndAppendToProtoBytes(&result);
      return result;
    }
    static absl::StatusOr<CorrelationAggregator> DeserializeFromProtoBytes(
        absl::string_view bytes);

    bool operator==(const CorrelationAggregator& other) const {
      return cov_agg_ == other.cov_agg_ &&
             sum_square_x_ == other.sum_square_x_ &&
             sum_square_y_ == other.sum_square_y_;
    }

   private:
    CovarianceAggregator cov_agg_;
    FixedInt<64, 5> sum_square_x_;
    FixedInt<64, 5> sum_square_y_;
  };

 private:
  friend class BigNumericValue;

  NumericValue(uint64_t high_bits, uint64_t low_bits);
  explicit constexpr NumericValue(__int128 value);

  template <bool is_strict>
  static absl::StatusOr<NumericValue> FromStringInternal(absl::string_view str);

  template <int kNumBitsPerWord, int kNumWords>
  static absl::StatusOr<NumericValue> FromFixedUint(
      const FixedUint<kNumBitsPerWord, kNumWords>& val, bool negate);
  template <int kNumBitsPerWord, int kNumWords>
  static absl::StatusOr<NumericValue> FromFixedInt(
      const FixedInt<kNumBitsPerWord, kNumWords>& val);

  // Returns the scaled fractional digits.
  int32_t GetFractionalPart() const;

  // A NUMERIC value is stored as a scaled integer, the original NUMERIC value
  // is multiplied by the scaling factor 10^9. The intended representation is
  // __int128, but since __int128 causes crashes for loads and stores that are
  // not 16-byte aligned, it is split into two 64-bit components here.
  uint64_t high_bits_;
  uint64_t low_bits_;
};

// This class represents values of the ZetaSQL BIGNUMERIC type. Supports 38
// full digits (and a partial 39th digit) before the decimal point and 38 digits
// after the decimal point. The support value range is -2^255 * 10^-38 to (2^255
// - 1) * 10^-38 (roughly 5.7896 * 10^38). The range covers all values of
// uint128.
// Internally BIGNUMERIC values are stored as scaled FixedInt<64, 4>.
class BigNumericValue final {
 public:
  static constexpr std::integral_constant<unsigned __int128, internal::k1e38>
      kScalingFactor{};
  static constexpr int kMaxIntegerDigits = 39;
  static constexpr int kMaxFractionalDigits = 38;
  static constexpr int kMaxPrecision = kMaxIntegerDigits + kMaxFractionalDigits;

  // Default constructor, constructs a zero value.
  constexpr BigNumericValue();

  explicit BigNumericValue(int value);
  explicit BigNumericValue(unsigned int value);
  explicit BigNumericValue(long value);                // NOLINT
  explicit BigNumericValue(unsigned long value);       // NOLINT
  explicit BigNumericValue(long long value);           // NOLINT
  explicit BigNumericValue(unsigned long long value);  // NOLINT
  explicit BigNumericValue(__int128 value);
  explicit BigNumericValue(unsigned __int128 value);
  explicit BigNumericValue(NumericValue value);

  // BIGNUMERIC minimum and maximum limits.
  static constexpr BigNumericValue MaxValue();
  static constexpr BigNumericValue MinValue();

  // Constructs a BigNumericValue object using its packed representation.
  static constexpr BigNumericValue FromPackedLittleEndianArray(
      const std::array<uint64_t, 4>& uint_array);
  // Returns value / kScalingFactor.
  static constexpr BigNumericValue FromScaledValue(__int128 value);

  // Returns a value representing little_endian_value / pow(10, scale), where
  // little_endian_value represents an integer in serialized binary format
  // in little endian byte order, using 2's complement encoding for negative
  // values (the last byte's highest bit determines the sign). For example, if
  // little_endian_value = "\x00\xff" and scale = 5, then the result is
  // -256 / pow(10, 5). When the result has more than 38 digits in the
  // fractional part, the result will be rounded half away from zero if
  // allow_rounding is true, or an error will be returned if allow_rounding is
  // false. This method is not optimized for performance, and is much slower
  // than FromScaledValue(__int128) and FromPackedLittleEndianArray.
  static absl::StatusOr<BigNumericValue> FromScaledLittleEndianValue(
      absl::string_view little_endian_value, int scale, bool allow_rounding);

  // Returns <*this> / pow(10, kMaxFractionalDigits - <scale>), or an error
  // if <scale> is not in the supported range [0, kMaxFractionalDigits].
  //
  // When there are more than <scale> significant fractional digits in the
  // result,
  // * If <allow_rounding> is true, the result will be rounded away from zero
  //   to <scale> fractional digits;
  // * If <allow_rounding> is false, an error is returned.
  absl::StatusOr<BigNumericValue> Rescale(int scale, bool allow_rounding) const;

  // Parses a textual representation of a BigNumericValue. Returns an error if
  // the given string cannot be parsed as a number or if the textual numeric
  // value exceeds BIGNUMERIC precision. This method will also return an error
  // if the textual representation has more than 38 digits after the decimal
  // point.
  //
  // This method accepts the same number formats as ZetaSQL floating point
  // literals, namely:
  //   [+-]DIGITS[.[DIGITS]][e[+-]DIGITS]
  //   [+-][DIGITS].DIGITS[e[+-]DIGITS]
  static absl::StatusOr<BigNumericValue> FromStringStrict(
      absl::string_view str);

  // Like FromStringStrict() but accepts more than 38 digits after the point
  // rounding the number to the nearest and ties away from zero.
  static absl::StatusOr<BigNumericValue> FromString(absl::string_view str);

  // Constructs a BigNumericValue from a double. This method might return an
  // error if the given value cannot be converted to a BIGNUMERIC (e.g. NaN).
  static absl::StatusOr<BigNumericValue> FromDouble(double value);

  // Arithmetic operators. These operators can return OUT_OF_RANGE error on
  // overflow. Additionally the division returns OUT_OF_RANGE if the divisor is
  // zero.
  absl::StatusOr<BigNumericValue> Add(const BigNumericValue& rh) const;
  absl::StatusOr<BigNumericValue> Subtract(const BigNumericValue& rh) const;
  absl::StatusOr<BigNumericValue> Multiply(const BigNumericValue& rh) const;
  absl::StatusOr<BigNumericValue> Divide(const BigNumericValue& rh) const;

  // An integer division operation. Similar to general division followed by
  // truncating the result to the whole integer. May return OUT_OF_RANGE if an
  // overflow or division by zero happens. This operation is the same as the SQL
  // DIV function.
  absl::StatusOr<BigNumericValue> DivideToIntegralValue(
      const BigNumericValue& rh) const;
  // Returns a remainder of division of this numeric value by the given divisor.
  // Returns an OUT_OF_RANGE error if the divisor is zero.
  absl::StatusOr<BigNumericValue> Mod(const BigNumericValue& rh) const;

  // Comparison operators.
  bool operator==(const BigNumericValue& rh) const;
  bool operator!=(const BigNumericValue& rh) const;
  bool operator<(const BigNumericValue& rh) const;
  bool operator>(const BigNumericValue& rh) const;
  bool operator>=(const BigNumericValue& rh) const;
  bool operator<=(const BigNumericValue& rh) const;

  // Math functions.
  absl::StatusOr<BigNumericValue> Negate() const;
  absl::StatusOr<BigNumericValue> Abs() const;
  int Sign() const;

  // Raises this BigNumericValue to the given power and returns the result.
  // Returns OUT_OF_RANGE error on overflow.
  absl::StatusOr<BigNumericValue> Power(const BigNumericValue& exp) const;
  // Raise natural e to this BigNumericValue and return the result.
  // Returns OUT_OF_RANGE error on overflow.
  absl::StatusOr<BigNumericValue> Exp() const;
  // Return natural logarithm of this BigNumericValue.
  // Returns OUT_OF_RANGE error on non-positive value.
  absl::StatusOr<BigNumericValue> Ln() const;
  // Return base 10 logarithm of this BigNumericValue.
  // Returns OUT_OF_RANGE error on non-positive value.
  absl::StatusOr<BigNumericValue> Log10() const;
  // Return logarithm of this BigNumericValue on base.
  // Returns OUT_OF_RANGE error on non-positive value or overflow.
  absl::StatusOr<BigNumericValue> Log(const BigNumericValue& base) const;
  // Return square root of this BigNumericValue.
  // Returns OUT_OF_RANGE error on negative value.
  absl::StatusOr<BigNumericValue> Sqrt() const;

  // Rounds this BigNumericValue to the given number of decimal digits after the
  // decimal point. 'digits' can be negative to cause rounding of the digits to
  // the left of the decimal point. Rounds the number to the nearest and ties
  // away from zero. Returns OUT_OF_RANGE if the rounding causes numerical
  // overflow.
  absl::StatusOr<BigNumericValue> Round(int64_t digits) const;

  // Similar to the method above, but rounds towards zero, i.e. truncates the
  // number. Because this method truncates instead of rounding away from zero it
  // never causes an error.
  BigNumericValue Trunc(int64_t digits) const;

  // Rounds this BigNumericValue upwards, returning the integer least upper
  // bound of this value. Returns OUT_OF_RANGE error on overflow.
  absl::StatusOr<BigNumericValue> Ceiling() const;

  // Rounds this BigNumericValue downwards, returning the integer greatest lower
  // bound of this value. Returns OUT_OF_RANGE error on overflow.
  absl::StatusOr<BigNumericValue> Floor() const;

  // Returns whether the BIGNUMERIC value has a fractional part.
  // Faster than computing Round/Trunc/Ceiling/Floor and comparing to *this.
  bool HasFractionalPart() const;

  // Returns hash code for the BigNumericValue.
  size_t HashCode() const;

  template <typename H>
  friend H AbslHashValue(H h, const BigNumericValue& v);

  // Converts the BigNumericValue into a value of another number type. T can be
  // one of int32_t, int64_t, uint32_t, uint64_t. Numeric values with fractional parts
  // will be rounded to a whole integer with a half away from zero rounding
  // semantics. This method will return OUT_OF_RANGE error if an overflow occurs
  // during conversion.
  template <class T>
  absl::StatusOr<T> To() const;

  // Converts the BigNumericValue to a NumericValue.
  absl::StatusOr<NumericValue> ToNumericValue() const;

  // Converts the BigNumericValue to a floating point number.
  double ToDouble() const;

  // Converts the BigNumericValue into a string. String representation of
  // BigNumericValue follows regular rules of textual numeric values
  // representation. For example, "1.34", "123", "0.23". AppendToString is
  // typically more efficient due to fewer memory allocations.
  std::string ToString() const;
  void AppendToString(std::string* output) const;

  using FormatSpec = NumericValue::FormatSpec;
  // Formats the BigNumericValue and appends the result to 'output'.
  // This method is much slower than AppendToString.
  void FormatAndAppend(FormatSpec spec, std::string* output) const;

  // Returns the packed uint64_t array in little endian order.
  constexpr const std::array<uint64_t, 4>& ToPackedLittleEndianArray() const;

  // Serialization and deserialization methods for BIGNUMERIC values that are
  // intended to be used to store them in protos. The encoding is variable in
  // length with max size of 32 bytes. SerializeAndAppendToProtoBytes is
  // typically more efficient due to fewer memory allocations.
  void SerializeAndAppendToProtoBytes(std::string* bytes) const;
  std::string SerializeAsProtoBytes() const  {
    std::string bytes;
    SerializeAndAppendToProtoBytes(&bytes);
    return bytes;
  }
  static absl::StatusOr<BigNumericValue> DeserializeFromProtoBytes(
      absl::string_view bytes);

  // Aggregates multiple BIGNUMERIC values and produces sum and average of all
  // values. This class handles a temporary overflow while adding values.
  // OUT_OF_RANGE error is generated only when retrieving the sum and only if
  // the final sum is outside of the valid BIGNUMERIC range.
  class SumAggregator final {
   public:
    // Adds a BIGNUMERIC value to the sum.
    void Add(const BigNumericValue& value);
    // Subtracts a BIGNUMERIC value from the sum.
    void Subtract(const BigNumericValue& value);
    // Returns sum of all input values. Returns OUT_OF_RANGE error on overflow.
    absl::StatusOr<BigNumericValue> GetSum() const;
    // Returns sum of all input values divided by the specified divisor.
    // Returns OUT_OF_RANGE error on overflow of the division result.
    // Please note that the division result may be in the valid range even if
    // the sum exceeds the range.
    absl::StatusOr<BigNumericValue> GetAverage(uint64_t count) const;

    // Merges the state with other SumAggregator instance's state.
    void MergeWith(const SumAggregator& other);

    // SerializeAndAppendToProtoBytes is typically more efficient due to fewer
    // memory allocations.
    void SerializeAndAppendToProtoBytes(std::string* bytes) const;
    std::string SerializeAsProtoBytes() const {
      std::string result;
      SerializeAndAppendToProtoBytes(&result);
      return result;
    }
    static absl::StatusOr<SumAggregator> DeserializeFromProtoBytes(
        absl::string_view bytes);

    bool operator==(const SumAggregator& other) const {
      return sum_ == other.sum_;
    }

   private:
    FixedInt<64, 5> sum_;
  };

  // Aggregates the input of multiple BIGNUMERIC values and provides functions
  // for the population/sample variance/standard deviation of the values in
  // double data type.
  class VarianceAggregator {
   public:
    // Adds a BIGNUMERIC value to the input.
    void Add(BigNumericValue value);
    // Removes a previously added BIGNUMERIC value from the input.
    // This method is provided for implementing analytic functions with
    // sliding windows. If the value has not been added to the input, or if it
    // has already been removed, then the result of this method is undefined.
    void Subtract(BigNumericValue value);
    // Returns the variance, or absl::nullopt if count is too low.
    absl::optional<double> GetVariance(uint64_t count, bool is_sampling) const;
    // Returns the population variance, or absl::nullopt if count is 0.
    absl::optional<double> GetPopulationVariance(uint64_t count) const {
      return GetVariance(count, /*is_sampling=*/false);
    }
    // Returns the sampling variance, or absl::nullopt if count < 2.
    absl::optional<double> GetSamplingVariance(uint64_t count) const {
      return GetVariance(count, /*is_sampling=*/true);
    }
    // Returns the standard deviation, or absl::nullopt if count is too low.
    absl::optional<double> GetStdDev(uint64_t count, bool is_sampling) const;
    // Returns the population standard deviation, or absl::nullopt if count is
    // 0.
    absl::optional<double> GetPopulationStdDev(uint64_t count) const {
      return GetStdDev(count, /*is_sampling=*/false);
    }
    // Returns the sampling standard deviation, or absl::nullopt if count < 2.
    absl::optional<double> GetSamplingStdDev(uint64_t count) const {
      return GetStdDev(count, /*is_sampling=*/true);
    }
    // Merges the state with other VarianceAggregator instance's state.
    void MergeWith(const VarianceAggregator& other);
    // Serialization and deserialization methods that are intended to be
    // used to store the state in protos.
    // sum_ is length prefixed and serialized, followed by sum_square_.
    // SerializeAndAppendToProtoBytes is typically more efficient due to fewer
    // memory allocations.
    void SerializeAndAppendToProtoBytes(std::string* bytes) const;
    std::string SerializeAsProtoBytes() const {
      std::string result;
      SerializeAndAppendToProtoBytes(&result);
      return result;
    }
    static absl::StatusOr<VarianceAggregator> DeserializeFromProtoBytes(
        absl::string_view bytes);

    bool operator==(const VarianceAggregator& other) const {
      return sum_ == other.sum_ && sum_square_ == other.sum_square_;
    }

   private:
    FixedInt<64, 5> sum_;
    FixedInt<64, 9> sum_square_;
  };

  class CorrelationAggregator;

  // Aggregates the input of multiple pairs of BIGNUMERIC values and provides
  // functions for the population/sample covariance of the pairs in double data
  // type.
  class CovarianceAggregator {
   public:
    // Adds a pair of BIGNUMERIC values to the input.
    void Add(BigNumericValue x, BigNumericValue y);
    // Removes a previously added pair of NUMERIC values from the input.
    // This method is provided for implementing analytic functions with
    // sliding windows. If the pair has not been added to the input, or if it
    // has already been removed, then the result of this method is undefined.
    void Subtract(BigNumericValue x, BigNumericValue y);
    // Returns the covariance, or absl::nullopt if count is too low.
    absl::optional<double> GetCovariance(uint64_t count,
                                         bool is_sampling) const;
    // Returns the population covariance of non-null pairs from input, or
    // absl::nullopt if count is 0.
    absl::optional<double> GetPopulationCovariance(uint64_t count) const {
      return GetCovariance(count, /*is_sampling=*/false);
    }
    // Returns the sample covariance of non-null pairs from input, or
    // absl::nullopt if count < 2.
    absl::optional<double> GetSamplingCovariance(uint64_t count) const {
      return GetCovariance(count, /*is_sampling=*/true);
    }
    // Merges the state with other CovarianceAggregator instance's state.
    void MergeWith(const CovarianceAggregator& other);
    // Serialization and deserialization methods that are intended to be
    // used to store the state in protos.
    // sum_product_ is length prefixed and serialized, sum_x_ is length prefixed
    // and serialized, followed by sum_y_.
    // SerializeAndAppendToProtoBytes is typically more efficient due to fewer
    // memory allocations.
    void SerializeAndAppendToProtoBytes(std::string* bytes) const;
    std::string SerializeAsProtoBytes() const {
      std::string result;
      SerializeAndAppendToProtoBytes(&result);
      return result;
    }
    static absl::StatusOr<CovarianceAggregator> DeserializeFromProtoBytes(
        absl::string_view bytes);

    bool operator==(const CovarianceAggregator& other) const {
      return sum_product_ == other.sum_product_ && sum_x_ == other.sum_x_ &&
             sum_y_ == other.sum_y_;
    }

   private:
    friend class CorrelationAggregator;
    FixedInt<64, 9> sum_product_;
    FixedInt<64, 5> sum_x_;
    FixedInt<64, 5> sum_y_;
  };

  // Aggregates the input of multiple pairs of BIGNUMERIC values and provides
  // functions for the correlation of the pairs in double data type.
  class CorrelationAggregator {
   public:
    // Adds a pair of BIGNUMERIC values to the input.
    void Add(BigNumericValue x, BigNumericValue y);
    // Removes a previously added pair of BIGNUMERIC values from the input.
    // This method is provided for implementing analytic functions with
    // sliding windows. If the pair has not been added to the input, or if it
    // has already been removed, then the result of this method is undefined.
    void Subtract(BigNumericValue x, BigNumericValue y);
    // Returns the correlation coefficient for non-null pairs from input.
    absl::optional<double> GetCorrelation(uint64_t count) const;
    // Merges the state with other CorrelationAggregator instance's state.
    void MergeWith(const CorrelationAggregator& other);
    // Serialization and deserialization methods that are intended to be
    // used to store the state in protos.
    // Each of cov_agg_'s members are length prefixed and serialized, followed
    // by sum_square_x_ length prefixed and serialized and then sum_square_y_
    // serialized.
    // SerializeAndAppendToProtoBytes is typically more efficient due to fewer
    // memory allocations.
    void SerializeAndAppendToProtoBytes(std::string* bytes) const;
    std::string SerializeAsProtoBytes() const {
      std::string result;
      SerializeAndAppendToProtoBytes(&result);
      return result;
    }
    static absl::StatusOr<CorrelationAggregator> DeserializeFromProtoBytes(
        absl::string_view bytes);

    bool operator==(const CorrelationAggregator& other) const {
      return cov_agg_ == other.cov_agg_ &&
             sum_square_x_ == other.sum_square_x_ &&
             sum_square_y_ == other.sum_square_y_;
    }

   private:
    CovarianceAggregator cov_agg_;
    FixedInt<64, 9> sum_square_x_;
    FixedInt<64, 9> sum_square_y_;
  };

  // Returns ROUND(value / 10^38) or TRUNC(value / 10^38), depending on <round>.
  template <bool round, int N>
  static FixedUint<64, N - 1> RemoveScalingFactor(FixedUint<64, N> value);

 private:
  explicit constexpr BigNumericValue(const FixedInt<64, 4>& value);
  explicit constexpr BigNumericValue(const std::array<uint64_t, 4>& uint_array);
  template <bool is_strict>
  static absl::StatusOr<BigNumericValue> FromStringInternal(
      absl::string_view str);
  static double RemoveScaleAndConvertToDouble(const FixedInt<64, 4>& value);

  FixedInt<64, 4> value_;
};

// Supports variable length and variable scale.
// This class has very limited functionalities, and its performance is much
// worse than NumericValue and BigNumericValue.
class VarNumericValue {
 public:
  VarNumericValue() {}
  // Returns a value representing little_endian_value / pow(10, scale), where
  // little_endian_value represents an integer in serialized binary format
  // in little endian byte order, using 2's complement encoding for negative
  // values (the last byte's highest bit determines the sign). For example, if
  // little_endian_value = "\x00\xff" and scale = 5, then the result is
  // -256 / pow(10, 5).
  static VarNumericValue FromScaledLittleEndianValue(
      absl::string_view little_endian_value, uint scale);

  // Converts the VarNumericValue into a string. String representation of
  // VarNumericValue follows regular rules of textual numeric values
  // representation. For example, "1.34", "123", "0.23". AppendToString is
  // typically more efficient due to fewer memory allocations.
  std::string ToString() const {
    std::string output;
    AppendToString(&output);
    return output;
  }
  void AppendToString(std::string* output) const;

 private:
  // NOTE: uint32_t is not the best for operations other than division in terms
  // of performance. When more methods are supported, this should change to
  // std::vector<uint64_t>. For this reason, FromPackedLittleEndianArray is not
  // defined yet.
  std::vector<uint32_t> value_;
  uint scale_ = 0;
};

// Allow NUMERIC values to be logged.
std::ostream& operator<<(std::ostream& out, NumericValue value);

// Allow BIGNUMERIC values to be logged.
std::ostream& operator<<(std::ostream& out, const BigNumericValue& value);

// Allow VarNumericValue values to be logged.
std::ostream& operator<<(std::ostream& out, const VarNumericValue& value);

// ---------------- Below are implementation details. -------------------

namespace internal {

constexpr __int128 kNumericMax = k1e38 - 1;
constexpr __int128 kNumericMin = -kNumericMax;
constexpr uint32_t k5to9 = k1e9 >> 9;
constexpr uint32_t k5to10 = k5to9 * 5;
constexpr uint32_t k5to11 = k5to10 * 5;
constexpr uint32_t k5to12 = k5to11 * 5;
constexpr uint32_t k5to13 = k5to12 * 5;
constexpr uint64_t k5to19 = static_cast<uint64_t>(k5to10) * k5to9;
constexpr std::integral_constant<int32_t, internal::k1e9>
    kSignedScalingFactor{};

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

inline constexpr NumericValue::NumericValue(long value)  // NOLINT
    : NumericValue(static_cast<__int128>(value) * kScalingFactor) {}

inline constexpr NumericValue::NumericValue(unsigned long value)  // NOLINT
    : NumericValue(static_cast<__int128>(value) * kScalingFactor) {}

inline constexpr NumericValue::NumericValue(long long value)  // NOLINT
    : NumericValue(static_cast<__int128>(value) * kScalingFactor) {}

inline constexpr NumericValue::NumericValue(unsigned long long value)  // NOLINT
    : NumericValue(static_cast<__int128>(value) * kScalingFactor) {}

inline constexpr NumericValue NumericValue::MaxValue() {
  return NumericValue(internal::kNumericMax);
}

inline constexpr NumericValue NumericValue::MinValue() {
  return NumericValue(internal::kNumericMin);
}

inline absl::StatusOr<NumericValue> NumericValue::FromPackedInt(
    __int128 value) {
  NumericValue ret(value);

  if (ABSL_PREDICT_FALSE(ret < MinValue() || ret > MaxValue())) {
    return MakeEvalError() << "numeric overflow: result out of range";
  }

  return ret;
}

inline constexpr NumericValue NumericValue::FromScaledValue(int64_t value) {
  return NumericValue(static_cast<__int128>(value));
}

template <int kNumBitsPerWord, int kNumWords>
inline absl::StatusOr<NumericValue> NumericValue::FromFixedInt(
    const FixedInt<kNumBitsPerWord, kNumWords>& val) {
  constexpr FixedInt<kNumBitsPerWord, kNumWords> kMin(internal::kNumericMin);
  constexpr FixedInt<kNumBitsPerWord, kNumWords> kMax(internal::kNumericMax);
  if (ABSL_PREDICT_TRUE(val >= kMin) && ABSL_PREDICT_TRUE(val <= kMax)) {
    return NumericValue(static_cast<__int128>(val));
  }
  return MakeEvalError() << "numeric overflow";
}

template <int kNumBitsPerWord, int kNumWords>
inline absl::StatusOr<NumericValue> NumericValue::FromFixedUint(
    const FixedUint<kNumBitsPerWord, kNumWords>& val, bool negate) {
  if (ABSL_PREDICT_TRUE(val.NonZeroLength() <= 128 / kNumBitsPerWord)) {
    unsigned __int128 v = static_cast<unsigned __int128>(val);
    if (ABSL_PREDICT_TRUE(v <= internal::kNumericMax)) {
      return NumericValue(static_cast<__int128>(negate ? -v : v));
    }
  }
  return MakeEvalError() << "numeric overflow";
}

inline absl::StatusOr<NumericValue> NumericValue::FromHighAndLowBits(
    uint64_t high_bits, uint64_t low_bits) {
  NumericValue ret(high_bits, low_bits);

  if (ABSL_PREDICT_FALSE(ret < MinValue() || ret > MaxValue())) {
    return MakeEvalError() << "numeric overflow: result out of range";
  }

  return ret;
}

inline absl::StatusOr<NumericValue> NumericValue::Add(NumericValue rh) const {
  FixedInt<64, 2> sum(as_packed_int());
  bool overflow = sum.AddOverflow(FixedInt<64, 2>(rh.as_packed_int()));
  if (ABSL_PREDICT_TRUE(!overflow)) {
    auto numeric_value_status = FromFixedInt(sum);
    if (ABSL_PREDICT_TRUE(numeric_value_status.ok())) {
      return numeric_value_status;
    }
  }
  return MakeEvalError() << "numeric overflow: " << ToString() << " + "
                         << rh.ToString();
}

inline absl::StatusOr<NumericValue> NumericValue::Subtract(
    NumericValue rh) const {
  FixedInt<64, 2> diff(as_packed_int());
  bool overflow = diff.SubtractOverflow(FixedInt<64, 2>(rh.as_packed_int()));
  if (ABSL_PREDICT_TRUE(!overflow)) {
    auto numeric_value_status = FromFixedInt(diff);
    if (ABSL_PREDICT_TRUE(numeric_value_status.ok())) {
      return numeric_value_status;
    }
  }
  return MakeEvalError() << "numeric overflow: " << ToString() << " - "
                         << rh.ToString();
}

inline NumericValue NumericValue::Negate() const {
  // The result is expected to be within the valid range.
  return NumericValue(-as_packed_int());
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

inline std::string NumericValue::ToString() const {
  std::string result;
  AppendToString(&result);
  return result;
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

template <class T>
inline absl::StatusOr<T> NumericValue::To() const {
  static_assert(
      std::is_same<T, int32_t>::value || std::is_same<T, int64_t>::value ||
          std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value,
      "In NumericValue::To<T>() T can only be one of "
      "int32, int64_t, uint32_t or uint64");

  __int128 rounded_value = static_cast<__int128>(
      FixedInt<64, 2>(as_packed_int())
          .DivAndRoundAwayFromZero(internal::kSignedScalingFactor));
  T result = static_cast<T>(rounded_value);
  if (rounded_value == result) {
    return result;
  }
  return MakeEvalError() << TypeName<T>() << " out of range: " << ToString();
}

inline constexpr __int128 NumericValue::as_packed_int() const {
  return (static_cast<__int128>(high_bits_) << 64) + low_bits_;
}

inline constexpr uint64_t NumericValue::high_bits() const { return high_bits_; }

inline constexpr uint64_t NumericValue::low_bits() const { return low_bits_; }

inline int32_t NumericValue::GetFractionalPart() const {
  int32_t remainder;
  FixedInt<64, 2>(as_packed_int())
      .DivMod(internal::kSignedScalingFactor, nullptr, &remainder);
  return remainder;
}

inline bool NumericValue::HasFractionalPart() const {
  return GetFractionalPart() != 0;
}

inline void NumericValue::SumAggregator::Add(NumericValue value) {
  sum_ += FixedInt<64, 3>(value.as_packed_int());
}

inline void NumericValue::SumAggregator::Subtract(NumericValue value) {
  sum_ -= FixedInt<64, 3>(value.as_packed_int());
}

inline void NumericValue::SumAggregator::MergeWith(const SumAggregator& other) {
  sum_ += other.sum_;
}

inline constexpr BigNumericValue::BigNumericValue(
    const std::array<uint64_t, 4>& uint_array)
    : value_(uint_array) {}

inline constexpr BigNumericValue::BigNumericValue(const FixedInt<64, 4>& value)
    : value_(value) {}

inline constexpr BigNumericValue::BigNumericValue() {}

inline BigNumericValue::BigNumericValue(int value)
    : BigNumericValue(static_cast<long long>(value)) {}  // NOLINT

inline BigNumericValue::BigNumericValue(unsigned int value)
    : BigNumericValue(static_cast<unsigned long long>(value)) {}  // NOLINT

inline BigNumericValue::BigNumericValue(long value)      // NOLINT
    : BigNumericValue(static_cast<long long>(value)) {}  // NOLINT

inline BigNumericValue::BigNumericValue(unsigned long value)      // NOLINT
    : BigNumericValue(static_cast<unsigned long long>(value)) {}  // NOLINT

inline BigNumericValue::BigNumericValue(long long value)  // NOLINT
    : value_(ExtendAndMultiply(FixedInt<64, 1>(static_cast<int64_t>(value)),
                               FixedInt<64, 2>(internal::k1e38))) {}

inline BigNumericValue::BigNumericValue(unsigned long long value)  // NOLINT
    : value_(ExtendAndMultiply(FixedUint<64, 1>(static_cast<uint64_t>(value)),
                               FixedUint<64, 2>(kScalingFactor))) {}

inline BigNumericValue::BigNumericValue(__int128 value)
    : value_(ExtendAndMultiply(FixedInt<64, 2>(value),
                               FixedInt<64, 2>(internal::k1e38))) {}

inline BigNumericValue::BigNumericValue(unsigned __int128 value)
    : value_(ExtendAndMultiply(FixedUint<64, 2>(value),
                               FixedUint<64, 2>(kScalingFactor))) {}

inline BigNumericValue::BigNumericValue(NumericValue value)
    : value_(ExtendAndMultiply(
          FixedInt<64, 2>(value.as_packed_int()),
          FixedInt<64, 2>(internal::k1e38 / NumericValue::kScalingFactor))) {}

inline constexpr BigNumericValue BigNumericValue::MaxValue() {
  return BigNumericValue(FixedInt<64, 4>::max());
}

inline constexpr BigNumericValue BigNumericValue::MinValue() {
  return BigNumericValue(FixedInt<64, 4>::min());
}

inline constexpr BigNumericValue BigNumericValue::FromPackedLittleEndianArray(
    const std::array<uint64_t, 4>& uint_array) {
  return BigNumericValue(uint_array);
}

inline constexpr BigNumericValue BigNumericValue::FromScaledValue(
    __int128 value) {
  return BigNumericValue(FixedInt<64, 4>(value));
}

inline constexpr const std::array<uint64_t, 4>&
BigNumericValue::ToPackedLittleEndianArray() const {
  return value_.number();
}

inline absl::StatusOr<BigNumericValue> BigNumericValue::Add(
    const BigNumericValue& rh) const {
  BigNumericValue res(this->value_);
  if (ABSL_PREDICT_FALSE(res.value_.AddOverflow(rh.value_))) {
    return MakeEvalError() << "BIGNUMERIC overflow: " << ToString() << " + "
                           << rh.ToString();
  }
  return res;
}

inline absl::StatusOr<BigNumericValue> BigNumericValue::Subtract(
    const BigNumericValue& rh) const {
  BigNumericValue res(this->value_);
  if (ABSL_PREDICT_FALSE(res.value_.SubtractOverflow(rh.value_))) {
    return MakeEvalError() << "BIGNUMERIC overflow: " << ToString() << " - "
                           << rh.ToString();
  }
  return res;
}

inline bool BigNumericValue::operator==(const BigNumericValue& rh) const {
  return value_ == rh.value_;
}

inline bool BigNumericValue::operator!=(const BigNumericValue& rh) const {
  return value_ != rh.value_;
}

inline bool BigNumericValue::operator<(const BigNumericValue& rh) const {
  return value_ < rh.value_;
}

inline bool BigNumericValue::operator>(const BigNumericValue& rh) const {
  return value_ > rh.value_;
}

inline bool BigNumericValue::operator>=(const BigNumericValue& rh) const {
  return value_ >= rh.value_;
}

inline bool BigNumericValue::operator<=(const BigNumericValue& rh) const {
  return value_ <= rh.value_;
}

inline absl::StatusOr<BigNumericValue> BigNumericValue::Negate() const {
  FixedInt<64, 4> result = value_;
  if (ABSL_PREDICT_TRUE(!result.NegateOverflow())) {
    return BigNumericValue(result);
  }
  return MakeEvalError() << "BIGNUMERIC overflow: -(" << ToString() << ")";
}

inline int BigNumericValue::Sign() const {
  return value_.is_negative() ? -1 : (value_.is_zero() ? 0 : 1);
}

inline absl::StatusOr<BigNumericValue> BigNumericValue::Abs() const {
  FixedInt<64, 4> result = value_;
  if (ABSL_PREDICT_TRUE(!result.is_negative()) ||
      ABSL_PREDICT_TRUE(!result.NegateOverflow())) {
    return BigNumericValue(result.number());
  }
  return MakeEvalError() << "BIGNUMERIC overflow: ABS(" << ToString() << ")";
}

inline double BigNumericValue::ToDouble() const {
  return BigNumericValue::RemoveScaleAndConvertToDouble(value_);
}

inline std::string BigNumericValue::ToString() const {
  std::string result;
  AppendToString(&result);
  return result;
}

template <bool round, int N>
    inline FixedUint<64, N - 1>
    BigNumericValue::RemoveScalingFactor(FixedUint<64, N> value) {
  // Suppose the theoretical value of value / 5^38 in binary format is (x).(y),
  // where x is the integer part, and y is the fractional part that can have
  // infinite number of bits. Then value / 10^38 =
  // (x >> 38).(lower 38 bits of x)(y), and thus
  // ROUND(value / 10^38) = (x >> 38) + (38th bit of x).
  // To compute x = FLOOR(value / 5^38), we use 3 divisions by 32-bit constants
  // for optimal performance.
  value /= std::integral_constant<uint32_t, internal::k5to13>();
  value /= std::integral_constant<uint32_t, internal::k5to13>();
  value /= std::integral_constant<uint32_t, internal::k5to12>();
  // 5^38 > 2^64, so the highest uint64_t must be 0, even after adding 2^38.
  ZETASQL_DCHECK_EQ(value.number()[N - 1], 0);
  FixedUint<64, N - 1> value_trunc(value);
  if (round &&
      (value_trunc.number()[0] & (1ULL << (kMaxFractionalDigits - 1)))) {
    value_trunc += (uint64_t{1} << kMaxFractionalDigits);
  }
  value_trunc >>= kMaxFractionalDigits;
  return value_trunc;
}

template <class T>
inline absl::StatusOr<T> BigNumericValue::To() const {
  static_assert(
      std::is_same<T, int32_t>::value || std::is_same<T, int64_t>::value ||
          std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value,
      "In BigNumericValue::To<T>() T can only be one of "
      "int32, int64_t, uint32_t or uint64");
  bool is_negative = value_.is_negative();
  FixedUint<64, 4> abs_value = value_.abs();
  if (abs_value.number()[3] == 0) {
    FixedUint<64, 2> rounded_value =
        RemoveScalingFactor</* round = */ true>(FixedUint<64, 3>(abs_value));
    if (rounded_value.number()[1] == 0) {
      unsigned __int128 abs_result = rounded_value.number()[0];
      __int128 result = is_negative ? -abs_result : abs_result;
      T truncated_result = static_cast<T>(result);
      if (result == truncated_result) {
        return truncated_result;
      }
    }
  }
  return MakeEvalError() << TypeName<T>() << " out of range: " << ToString();
}

inline absl::StatusOr<NumericValue> BigNumericValue::ToNumericValue() const {
  bool is_negative = value_.is_negative();
  FixedUint<64, 4> abs_value = value_.abs();
  // Divide by 10^29 (the difference in scaling factors),
  // using 5^29 = 5^10 * 5^10 * 5^9, then a shift by 29
  abs_value /= std::integral_constant<uint32_t, internal::k5to10>();
  abs_value /= std::integral_constant<uint32_t, internal::k5to10>();
  abs_value /= std::integral_constant<uint32_t, internal::k5to9>();
  ZETASQL_DCHECK_EQ(abs_value.number()[3], 0);
  FixedUint<64, 3> abs_value_trunc(abs_value);
  if (abs_value_trunc.number()[0] & (1ULL << 28)) {
    abs_value_trunc += (uint64_t{1} << 29);
  }
  abs_value_trunc >>= 29;
  if (abs_value_trunc.number()[2] == 0) {
    absl::StatusOr<NumericValue> result =
        NumericValue::FromFixedUint(abs_value_trunc, is_negative);
    if (result.ok()) {
      return *result;
    }
  }
  return MakeEvalError() << "numeric out of range: " << ToString();
}

inline void BigNumericValue::SumAggregator::Add(const BigNumericValue& value) {
  sum_ += FixedInt<64, 5>(value.value_);
}

inline void BigNumericValue::SumAggregator::Subtract(
    const BigNumericValue& value) {
  sum_ -= FixedInt<64, 5>(value.value_);
}

inline void BigNumericValue::SumAggregator::MergeWith(
    const SumAggregator& other) {
  sum_ += other.sum_;
}

template <typename H>
inline H AbslHashValue(H h, const BigNumericValue& v) {
  return H::combine(std::move(h), v.value_);
}

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_NUMERIC_VALUE_H_
