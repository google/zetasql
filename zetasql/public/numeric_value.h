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

#include "zetasql/common/fixed_int.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
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
  // Must use integral_constant to utilize the compiler optimization for integer
  // divisions with constant 32-bit divisors.
  static constexpr std::integral_constant<uint32_t, 1000000000> kScalingFactor{};
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
  static zetasql_base::StatusOr<NumericValue> FromPackedInt(__int128 value);

  // Constructs a Numeric object from the high and low bits of the packed
  // integer representation. May return OUT_OF_RANGE error if the combined 128
  // bit value is outside the range of valid NUMERIC values.
  static zetasql_base::StatusOr<NumericValue> FromHighAndLowBits(uint64_t high_bits,
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

  // Converts the NUMERIC value into a string. String representation of NUMERICs
  // follow regular rules of textual numeric values representation. For example,
  // "1.34", "123", "0.23". AppendToString is typically more efficient due to
  // fewer memory allocations.
  std::string ToString() const;
  void AppendToString(std::string* output) const;

  // Returns the packed NUMERIC value.
  constexpr __int128 as_packed_int() const;

  // Returns high 64 bits of the packed NUMERIC value.
  constexpr uint64_t high_bits() const;

  // Returns low 64 bits of the packed NUMERIC value.
  constexpr uint64_t low_bits() const;

  // Returns whether the NUMERIC value has a fractional part.
  bool has_fractional_part() const;

  // Serialization and deserialization methods for NUMERIC values that are
  // intended to be used to store them in protos. The encoding is variable in
  // length with max size of 16 bytes.
  // WARNING: currently, SerializeAsProtoBytes does not always produce the same
  // result as SerializeAndAppendToProtoBytes. When the value is negative,
  // SerializeAsProtoBytes might output one more byte than
  // SerializeAndAppendToProtoBytes. The results of both methods can be
  // deserialized with DeserializeFromProtoBytes.
  // TODO: Make SerializeAsProtoBytes consistent with
  // SerializeAndAppendToProtoBytes.
  void SerializeAndAppendToProtoBytes(std::string* bytes) const;
  std::string SerializeAsProtoBytes() const;
  static zetasql_base::StatusOr<NumericValue> DeserializeFromProtoBytes(
      absl::string_view bytes);

  // Aggregates multiple NUMERIC values and produces sum and average of all
  // values. This class handles a temporary overflow while adding values.
  // OUT_OF_RANGE error is generated only when retrieving the sum and only if
  // the final sum is outside of the valid NUMERIC range.
  // WARNING: This class is going to be replaced with SumAggregator. New code
  // should not depend on sizeof(Aggregator) or its SerializeAsProtoBytes()
  // implementation.
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

    bool operator==(const Aggregator& other) const {
      return sum_lower_ == other.sum_lower_ && sum_upper_ == other.sum_upper_;
    }

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

  // A temporary class. Will be renamed to Aggregator.
  // Aggregates multiple NUMERIC values and produces sum and average of all
  // values. This class handles a temporary overflow while adding values.
  // OUT_OF_RANGE error is generated only when retrieving the sum and only if
  // the final sum is outside of the valid NUMERIC range.
  class SumAggregator final {
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

    // Merges the state with other SumAggregator instance's state.
    void MergeWith(const SumAggregator& other);

    std::string SerializeAsProtoBytes() const;
    static zetasql_base::StatusOr<SumAggregator> DeserializeFromProtoBytes(
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
    // Returns the population variance, or absl::nullopt if count is 0.
    absl::optional<double> GetPopulationVariance(uint64_t count) const;
    // Returns the sampling variance, or absl::nullopt if count < 2.
    absl::optional<double> GetSamplingVariance(uint64_t count) const;
    // Returns the population standard deviation, or absl::nullopt if count is
    // 0.
    absl::optional<double> GetPopulationStdDev(uint64_t count) const;
    // Returns the sampling standard deviation, or absl::nullopt if count < 2.
    absl::optional<double> GetSamplingStdDev(uint64_t count) const;
    // Merges the state with other VarianceAggregator instance's state.
    void MergeWith(const VarianceAggregator& other);
    // Serialization and deserialization methods that are intended to be
    // used to store the state in protos.
    // sum_ is length prefixed and serialized, followed by sum_square_.
    std::string SerializeAsProtoBytes() const;
    static zetasql_base::StatusOr<VarianceAggregator> DeserializeFromProtoBytes(
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
    // Returns the population covariance of non-null pairs from input, or
    // absl::nullopt if count is 0.
    absl::optional<double> GetPopulationCovariance(uint64_t count) const;
    // Returns the sample covariance of non-null pairs from input, or
    // absl::nullopt if count < 2.
    absl::optional<double> GetSamplingCovariance(uint64_t count) const;
    // Merges the state with other CovarianceAggregator instance's state.
    void MergeWith(const CovarianceAggregator& other);
    // Serialization and deserialization methods that are intended to be
    // used to store the state in protos.
    // sum_product_ is length prefixed and serialized, sum_x_ is length prefixed
    // and serialized, followed by sum_y_.
    std::string SerializeAsProtoBytes() const;
    static zetasql_base::StatusOr<CovarianceAggregator> DeserializeFromProtoBytes(
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
    std::string SerializeAsProtoBytes() const;
    static zetasql_base::StatusOr<CorrelationAggregator> DeserializeFromProtoBytes(
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

  static zetasql_base::StatusOr<NumericValue> FromStringInternal(
      absl::string_view str, bool is_strict);

  template <int kNumBitsPerWord, int kNumWords>
  static zetasql_base::StatusOr<NumericValue> FromFixedUint(
      const FixedUint<kNumBitsPerWord, kNumWords>& val, bool negate);
  template <int kNumBitsPerWord, int kNumWords>
  static zetasql_base::StatusOr<NumericValue> FromFixedInt(
      const FixedInt<kNumBitsPerWord, kNumWords>& val);

  // Rounds this NUMERIC value to the given number of decimal digits after the
  // decimal point (or before the decimal point if 'digits' is negative).
  // Halfway cases are rounded away from zero if 'round_away_from_zero' is set
  // to true, towards zero otherwise. May return OUT_OF_RANGE error if rounding
  // causes overflow.
  zetasql_base::StatusOr<NumericValue> RoundInternal(
      int64_t digits, bool round_away_from_zero) const;

  // Raises this numeric value to the given power and returns the result.
  // The caller should annotate the error with the inputs.
  zetasql_base::StatusOr<NumericValue> PowerInternal(NumericValue exp) const;

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
// Internally NUMERIC values are stored as scaled FixedInt<64, 4>.
class BigNumericValue final {
 public:
  static constexpr unsigned __int128 ScalingFactor();

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
      std::array<uint64_t, 4> uint_array);

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
  static zetasql_base::StatusOr<BigNumericValue> FromStringStrict(
      absl::string_view str);

  // Like FromStringStrict() but accepts more than 38 digits after the point
  // rounding the number to the nearest and ties away from zero.
  static zetasql_base::StatusOr<BigNumericValue> FromString(absl::string_view str);

  // Constructs a BigNumericValue from a double. This method might return an
  // error if the given value cannot be converted to a BIGNUMERIC (e.g. NaN).
  static zetasql_base::StatusOr<BigNumericValue> FromDouble(double value);

  // Arithmetic operators. These operators can return OUT_OF_RANGE error on
  // overflow. Additionally the division returns OUT_OF_RANGE if the divisor is
  // zero.
  zetasql_base::StatusOr<BigNumericValue> Add(const BigNumericValue& rh) const;
  zetasql_base::StatusOr<BigNumericValue> Subtract(const BigNumericValue& rh) const;
  zetasql_base::StatusOr<BigNumericValue> Multiply(const BigNumericValue& rh) const;
  zetasql_base::StatusOr<BigNumericValue> Divide(const BigNumericValue& rh) const;

  // An integer division operation. Similar to general division followed by
  // truncating the result to the whole integer. May return OUT_OF_RANGE if an
  // overflow or division by zero happens. This operation is the same as the SQL
  // DIV function.
  zetasql_base::StatusOr<BigNumericValue> IntegerDivide(
      const BigNumericValue& rh) const;
  // Returns a remainder of division of this numeric value by the given divisor.
  // Returns an OUT_OF_RANGE error if the divisor is zero.
  zetasql_base::StatusOr<BigNumericValue> Mod(const BigNumericValue& rh) const;

  // Comparison operators.
  bool operator==(const BigNumericValue& rh) const;
  bool operator!=(const BigNumericValue& rh) const;
  bool operator<(const BigNumericValue& rh) const;
  bool operator>(const BigNumericValue& rh) const;
  bool operator>=(const BigNumericValue& rh) const;
  bool operator<=(const BigNumericValue& rh) const;

  // Math functions.
  static zetasql_base::StatusOr<BigNumericValue> UnaryMinus(
      const BigNumericValue& value);
  static zetasql_base::StatusOr<BigNumericValue> Abs(const BigNumericValue& value);
  static BigNumericValue Sign(const BigNumericValue& value);

  // Raises this BigNumericValue to the given power and returns the result.
  // Returns OUT_OF_RANGE error on overflow.
  zetasql_base::StatusOr<BigNumericValue> Power(const BigNumericValue& exp) const;

  // Rounds this BigNumericValue to the given number of decimal digits after the
  // decimal point. 'digits' can be negative to cause rounding of the digits to
  // the left of the decimal point. Rounds the number to the nearest and ties
  // away from zero. Returns OUT_OF_RANGE if the rounding causes numerical
  // overflow.
  zetasql_base::StatusOr<BigNumericValue> Round(int64_t digits) const;

  // Similar to the method above, but rounds towards zero, i.e. truncates the
  // number. Because this method truncates instead of rounding away from zero it
  // never causes an error.
  BigNumericValue Trunc(int64_t digits) const;

  // Rounds this BigNumericValue upwards, returning the integer least upper
  // bound of this value. Returns OUT_OF_RANGE error on overflow.
  zetasql_base::StatusOr<BigNumericValue> Ceiling() const;

  // Rounds this BigNumericValue downwards, returning the integer greatest lower
  // bound of this value. Returns OUT_OF_RANGE error on overflow.
  zetasql_base::StatusOr<BigNumericValue> Floor() const;

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
  zetasql_base::StatusOr<T> To() const;

  // Converts the BigNumericValue to a NumericValue.
  zetasql_base::StatusOr<NumericValue> ToNumericValue() const;

  // Converts the BigNumericValue to a floating point number.
  double ToDouble() const;

  // Converts the BigNumericValue into a string. String representation of
  // NUMERICs follow regular rules of textual numeric values representation. For
  // example, "1.34", "123", "0.23". AppendToString is typically more efficient
  // due to fewer memory allocations.
  std::string ToString() const;
  void AppendToString(std::string* output) const;

  // Returns the packed uint64_t array in little endian order.
  constexpr const std::array<uint64_t, 4>& ToPackedLittleEndianArray() const;

  void SerializeAndAppendToProtoBytes(std::string* bytes) const;
  std::string SerializeAsProtoBytes() const  {
    std::string bytes;
    SerializeAndAppendToProtoBytes(&bytes);
    return bytes;
  }
  static zetasql_base::StatusOr<BigNumericValue> DeserializeFromProtoBytes(
      absl::string_view bytes);

 private:
  explicit constexpr BigNumericValue(FixedInt<64, 4> value);
  explicit constexpr BigNumericValue(std::array<uint64_t, 4> uint_array);
  static zetasql_base::StatusOr<BigNumericValue> FromStringInternal(
      absl::string_view str, bool is_strict);
  template <int N>
  static FixedUint<64, N - 1> RemoveScalingFactor(FixedUint<64, N> value);
  static double RemoveScaleAndConvertToDouble(const FixedInt<64, 4>& value);

  FixedInt<64, 4> value_;
};

// Allow NUMERIC values to be logged.
std::ostream& operator<<(std::ostream& out, NumericValue value);

}  // namespace zetasql

// Include implementation of the inline methods. Out of line for clarity. It is
// not intended to be directly used.
#include "zetasql/public/numeric_value.inc"  

#endif  // ZETASQL_PUBLIC_NUMERIC_VALUE_H_
