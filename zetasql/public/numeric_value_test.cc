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

#include <stdlib.h>

#include <functional>
#include <limits>
#include <new>
#include <utility>
#include <vector>

#include "zetasql/common/fixed_int.h"
#include "zetasql/base/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/numeric/int128.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/endian.h"
#include "zetasql/base/canonical_errors.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

class NumericValueTest : public testing::Test {
 protected:
  NumericValueTest()
    : expected_max_string_(
          std::string(29, '9') + std::string(".") + std::string(9, '9')),
      expected_min_string_(std::string("-") + expected_max_string_) {
  }

  inline NumericValue MkNumeric(const std::string& str) {
    return NumericValue::FromStringStrict(str).ValueOrDie();
  }

  // Generates a random valid numeric value.
  NumericValue MakeRandomNumeric() {
    int32_t sign = absl::Uniform<int32_t>(random_, 0, 2);
    int32_t int_digits = absl::Uniform<int32_t>(random_, 0, 30);
    int32_t fract_digits = absl::Uniform<int32_t>(random_, 0, 10);

    std::string str(sign ? "-" : "");
    if (int_digits > 0) {
      for (int i = 0; i < int_digits; ++i) {
        str.push_back(static_cast<char>(absl::Uniform<int32_t>(random_, 0, 10)) +
                      '0');
      }
    } else {
      str.push_back('0');
    }

    if (fract_digits > 0) {
      str.push_back('.');
      for (int i = 0; i < fract_digits; ++i) {
        str.push_back(static_cast<char>(absl::Uniform<int32_t>(random_, 0, 10)) +
                      '0');
      }
    }

    return MkNumeric(str);
  }

  void VerifyVariance(absl::optional<double> expect_var,
                      absl::optional<double> actual_var,
                      absl::optional<double> actual_stddev) {
    ASSERT_EQ(expect_var.has_value(), actual_var.has_value());
    ASSERT_EQ(expect_var.has_value(), actual_stddev.has_value());
    if (expect_var.has_value()) {
      EXPECT_DOUBLE_EQ(expect_var.value(), actual_var.value());
      EXPECT_DOUBLE_EQ(std::sqrt(expect_var.value()), actual_stddev.value());
    }
  }
  void TestVarianceAggregator(const NumericValue::VarianceAggregator& agg,
                              absl::optional<double> expect_var_pop,
                              absl::optional<double> expect_var_samp,
                              uint64_t count) {
    VerifyVariance(expect_var_pop, agg.GetPopulationVariance(count),
                   agg.GetPopulationStdDev(count));
    VerifyVariance(expect_var_samp, agg.GetSamplingVariance(count),
                   agg.GetSamplingStdDev(count));
  }

  void TestSerialize(NumericValue value) {
    std::string bytes = value.SerializeAsProtoBytes();
    NumericValue deserialized = NumericValue::DeserializeFromProtoBytes(
        bytes).ValueOrDie();
    EXPECT_EQ(value, deserialized);
  }

  // Expected min and max numeric values. The max values consists of 29 '9's,
  // the decimal point and the following nine '9's after the point. The min
  // value is similar to the max value but with an unary minus in front.
  const std::string expected_max_string_;
  const std::string expected_min_string_;

  absl::BitGen random_;
};


TEST_F(NumericValueTest, FromInteger) {
  NumericValue from_int64(static_cast<int64_t>(123));
  NumericValue from_uint64(static_cast<uint64_t>(123));;
  EXPECT_EQ(from_int64.HashCode(), from_uint64.HashCode());
}

TEST_F(NumericValueTest, FromPackedInt) {
  __int128 largest_negative = static_cast<unsigned __int128>(1) << 127;
  __int128 largest_positive =
      ~static_cast<__int128>(0) & ~(static_cast<unsigned __int128>(1) << 127);
  __int128 min_one_less = NumericValue::MinValue().as_packed_int() - 1;
  __int128 max_one_more = NumericValue::MaxValue().as_packed_int() + 1;

  EXPECT_THAT(NumericValue::FromPackedInt(largest_negative),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: result out of range"));
  EXPECT_THAT(NumericValue::FromPackedInt(largest_positive),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: result out of range"));
  EXPECT_THAT(NumericValue::FromPackedInt(max_one_more),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: result out of range"));
  EXPECT_THAT(NumericValue::FromPackedInt(min_one_less),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: result out of range"));
  ZETASQL_EXPECT_OK(NumericValue::FromPackedInt(0));
  ZETASQL_EXPECT_OK(
      NumericValue::FromPackedInt(NumericValue::MaxValue().as_packed_int()));
  ZETASQL_EXPECT_OK(
      NumericValue::FromPackedInt(NumericValue::MinValue().as_packed_int()));
}

TEST_F(NumericValueTest, FromHighAndLowBits) {
  uint64_t max_high = NumericValue::MaxValue().high_bits();
  uint64_t max_low = NumericValue::MaxValue().low_bits();
  uint64_t min_high = NumericValue::MinValue().high_bits();
  uint64_t min_low = NumericValue::MinValue().low_bits();
  uint64_t max_int64 = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
  uint64_t min_int64 = static_cast<uint64_t>(std::numeric_limits<int64_t>::min());

  ZETASQL_EXPECT_OK(NumericValue::FromHighAndLowBits(0, 0));
  ZETASQL_EXPECT_OK(NumericValue::FromHighAndLowBits(max_high, max_low));
  ZETASQL_EXPECT_OK(NumericValue::FromHighAndLowBits(min_high, min_low));

  EXPECT_THAT(NumericValue::FromHighAndLowBits(max_high, max_low + 1),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: result out of range"));
  EXPECT_THAT(NumericValue::FromHighAndLowBits(min_high, min_low - 1),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: result out of range"));

  EXPECT_THAT(NumericValue::FromHighAndLowBits(max_int64, 0),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: result out of range"));
  EXPECT_THAT(NumericValue::FromHighAndLowBits(min_int64, 0),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: result out of range"));
}

TEST_F(NumericValueTest, ToString) {
  EXPECT_EQ("0", NumericValue().ToString());
  EXPECT_EQ(expected_max_string_, NumericValue::MaxValue().ToString());
  EXPECT_EQ(expected_min_string_, NumericValue::MinValue().ToString());
  EXPECT_EQ("0.000000001",
            NumericValue::FromPackedInt(1).ValueOrDie().ToString());
  EXPECT_EQ("0.00000001",
            NumericValue::FromPackedInt(10).ValueOrDie().ToString());
  EXPECT_EQ("0.123",
            NumericValue::FromPackedInt(123000000).ValueOrDie().ToString());
  EXPECT_EQ("-0.000000001",
            NumericValue::FromPackedInt(-1).ValueOrDie().ToString());
  EXPECT_EQ("-0.00000001",
            NumericValue::FromPackedInt(-10).ValueOrDie().ToString());
  EXPECT_EQ("-0.123",
            NumericValue::FromPackedInt(-123000000).ValueOrDie().ToString());
  EXPECT_EQ("1", NumericValue(1).ToString());
  EXPECT_EQ("10", NumericValue(10).ToString());
  EXPECT_EQ("123", NumericValue(123).ToString());
  EXPECT_EQ("1", NumericValue(1).ToString());
  EXPECT_EQ("10", NumericValue(10).ToString());
  EXPECT_EQ("123", NumericValue(123).ToString());
  EXPECT_EQ("-1", NumericValue(-1).ToString());
  EXPECT_EQ("-10", NumericValue(-10).ToString());
  EXPECT_EQ("-123", NumericValue(-123).ToString());
  EXPECT_EQ("-9223372036854775808",
            NumericValue(std::numeric_limits<int64_t>::min()).ToString());
  EXPECT_EQ("9223372036854775807",
            NumericValue(std::numeric_limits<int64_t>::max()).ToString());
  EXPECT_EQ("0",
            NumericValue(std::numeric_limits<uint64_t>::min()).ToString());
  EXPECT_EQ("18446744073709551615",
            NumericValue(std::numeric_limits<uint64_t>::max()).ToString());
}

TEST_F(NumericValueTest, FromString) {
  using FromStringFunc =
      std::function<zetasql_base::StatusOr<NumericValue>(absl::string_view)>;
  std::vector<FromStringFunc> functions = {
      &NumericValue::FromStringStrict, &NumericValue::FromString
  };

  const std::vector<std::pair<NumericValue, std::string>> kStringTests = {
      {NumericValue(), "0"},
      {NumericValue(), "00"},
      {NumericValue(), "0.0"},
      {NumericValue(), "00.000"},
      {NumericValue(123), "123"},
      {NumericValue(123), "+123"},
      {NumericValue(-123), "-123"},
      {NumericValue(123), "123.0"},
      {NumericValue(123), "123."},
      {NumericValue(123), "+123.0"},
      {NumericValue(-123), "-123.0"},
      {NumericValue::FromPackedInt(1).ValueOrDie(), "0.000000001"},
      {NumericValue::FromPackedInt(1).ValueOrDie(), "+0.000000001"},
      {NumericValue::FromPackedInt(123000000).ValueOrDie(), "0.123"},
      {NumericValue::FromPackedInt(123000000).ValueOrDie(), ".123"},
      {NumericValue::FromPackedInt(123000000).ValueOrDie(), "+.123"},
      {NumericValue::FromPackedInt(-123000000).ValueOrDie(), "-0.123"},
      {NumericValue::FromPackedInt(-123000000).ValueOrDie(), "-.123"},
      {NumericValue::FromPackedInt(123456000000).ValueOrDie(), "123.456"},
      {NumericValue::FromPackedInt(-123456000000).ValueOrDie(), "-123.456"},
      {NumericValue::MaxValue(), expected_max_string_},
      {NumericValue::MinValue(), expected_min_string_},
      {NumericValue(std::numeric_limits<int64_t>::max()), "9223372036854775807"},
      {NumericValue(std::numeric_limits<int64_t>::min()), "-9223372036854775808"},
      {NumericValue(std::numeric_limits<uint64_t>::max()),
       "18446744073709551615"},
      {MkNumeric("0.123"), ".123"},
      {MkNumeric("-0.123"), "-.123"},
      {MkNumeric("0.123"), "+.123"},
      {MkNumeric("123"), "123."},

      // The white space is ignored.
      {NumericValue(), " 0"},
      {NumericValue(), "0 "},
      {NumericValue(), " 0 "},

      // Non-essential zeroes are ignored.
      {NumericValue(), "00000000000000000000000000000000000000000000000000"},
      {NumericValue(), "-00000000000000000000000000000000000000000000000000"},
      {NumericValue(), "+00000000000000000000000000000000000000000000000000"},
      {NumericValue(), ".00000000000000000000000000000000000000000000000000"},
      {NumericValue(), "-.00000000000000000000000000000000000000000000000000"},
      {NumericValue(), "+.00000000000000000000000000000000000000000000000000"},
      {NumericValue(), "00000000000000000000000000000000.0000000000000000000"},
      {NumericValue(), "-00000000000000000000000000000000.0000000000000000000"},
      {NumericValue(), "+00000000000000000000000000000000.0000000000000000000"},
      {MkNumeric("123.34"),
       "0000000000000000000000000000000123.340000000000000000000000"},
      {MkNumeric("123.34"),
       "+0000000000000000000000000000000123.340000000000000000000000"},
      {MkNumeric("-123.34"),
       "-0000000000000000000000000000000123.340000000000000000000000"},

      // Exponent form.
      {MkNumeric("123000"), "123e3"},
      {MkNumeric("123000"), "123E3"},
      {MkNumeric("123000"), "123.e3"},
      {MkNumeric("123000"), "123e+3"},
      {MkNumeric("0.123"), "123e-3"},
      {MkNumeric("0.123"), "123E-3"},
      {MkNumeric("123000"), "0000000000000000000000000123e3"},
      {MkNumeric("123000"), "0000000000000000000000000123.000000000000000e3"},
      {MkNumeric("-123000"), "-0000000000000000000000000123e3"},
      {MkNumeric("-123000"), "-0000000000000000000000000123.000000000000000e3"},
      {MkNumeric("1230000000"), ".123e10"},
      {MkNumeric("11234567890"), "1.1234567890E10"},
      {MkNumeric("12345678901234567890123456789.012345678"),
       "0.12345678901234567890123456789012345678e+29"},
      {MkNumeric("-12345678901234567890123456789.012345678"),
       "-0.12345678901234567890123456789012345678e+29"},
      {MkNumeric("12345678901234567890123456789.012345678"),
       "12345678901234567890123456789012345678e-9"},
      {MkNumeric("-12345678901234567890123456789.012345678"),
       "-12345678901234567890123456789012345678e-9"},
      {MkNumeric("99999999999999999999999999999.999999999"),
       "0.99999999999999999999999999999999999999e+29"},
      {MkNumeric("-99999999999999999999999999999.999999999"),
       "-0.99999999999999999999999999999999999999e+29"},
      {MkNumeric("99999999999999999999999999999.999999999"),
       "9999999999999999999999999999999999999900000000000000000000e-29"},
      {MkNumeric("-99999999999999999999999999999.999999999"),
       "-9999999999999999999999999999999999999900000000000000000000e-29"},
  };

  // Common successful cases under strict and non-strict parsing.
  for (const std::pair<NumericValue, std::string>& test_case : kStringTests) {
    SCOPED_TRACE(absl::Substitute("Input: $0, expected output: $1",
                                  test_case.second,
                                  test_case.first.ToString()));
    for (const auto& from_string : functions) {
      EXPECT_THAT(from_string(test_case.second),
                  IsOkAndHolds(test_case.first));
    }
  }

  // Test common failures for the strict and non-strict parsing.
  for (const auto& from_string : functions) {
    // Check integer part overflow.
    EXPECT_THAT(
        from_string(std::string(30, '9')),
        StatusIs(zetasql_base::OUT_OF_RANGE,
                 "Invalid NUMERIC value: 999999999999999999999999999999"));
    EXPECT_THAT(
        from_string(std::string("-") + std::string(30, '9')),
        StatusIs(zetasql_base::OUT_OF_RANGE,
                 "Invalid NUMERIC value: -999999999999999999999999999999"));
    EXPECT_THAT(
        from_string("266666666666666666666666666666"),
        StatusIs(zetasql_base::OUT_OF_RANGE,
                 "Invalid NUMERIC value: 266666666666666666666666666666"));
    EXPECT_THAT(
        from_string("-266666666666666666666666666666"),
        StatusIs(zetasql_base::OUT_OF_RANGE,
                 "Invalid NUMERIC value: -266666666666666666666666666666"));
    EXPECT_THAT(
        from_string("266666666666666666666666666600"),
        StatusIs(zetasql_base::OUT_OF_RANGE,
                 "Invalid NUMERIC value: 266666666666666666666666666600"));
    EXPECT_THAT(
        from_string("-266666666666666666666666666600"),
        StatusIs(zetasql_base::OUT_OF_RANGE,
                 "Invalid NUMERIC value: -266666666666666666666666666600"));

    EXPECT_THAT(from_string("abcd"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: abcd"));
    EXPECT_THAT(from_string("- 123"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: - 123"));
    EXPECT_THAT(from_string("123abc"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: 123abc"));
    EXPECT_THAT(from_string("123..456"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: 123..456"));
    EXPECT_THAT(from_string("123.4.56"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: 123.4.56"));
    EXPECT_THAT(from_string("."),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: ."));
    EXPECT_THAT(from_string(""),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: "));
    // Null string_view is treated the same as an empty std::string.
    EXPECT_THAT(from_string(absl::string_view()),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: "));
    EXPECT_THAT(from_string("  "),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value:   "));
    EXPECT_THAT(from_string("+"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: +"));
    EXPECT_THAT(from_string("-"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: -"));
    EXPECT_THAT(from_string("123e5.6"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: 123e5.6"));
    EXPECT_THAT(from_string("345e+ 4"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: 345e+ 4"));
    EXPECT_THAT(from_string("123e"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: 123e"));
    EXPECT_THAT(from_string("e"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: e"));
    EXPECT_THAT(from_string("123e-"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: 123e-"));
    EXPECT_THAT(from_string("123e+"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: 123e+"));
    EXPECT_THAT(from_string(".e"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: .e"));
    EXPECT_THAT(from_string("123e-9999e"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: 123e-9999e"));
    EXPECT_THAT(from_string("123e9999e"),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: 123e9999e"));
    EXPECT_THAT(from_string("123e99999999"),  // exponent too big
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: 123e99999999"));

    std::string numeric_exp = "123e2";
    EXPECT_THAT(from_string(absl::string_view(numeric_exp.data(), 4)),
                StatusIs(zetasql_base::OUT_OF_RANGE,
                         "Invalid NUMERIC value: 123e"));
  }

  // Test the case where there is more than 9 fractional digits.
  EXPECT_THAT(NumericValue::FromStringStrict("0.1234567891"),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "Invalid NUMERIC value: 0.1234567891"));
  EXPECT_THAT(NumericValue::FromStringStrict("-0.1234567891"),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "Invalid NUMERIC value: -0.1234567891"));

  // Now check rounding.
  EXPECT_EQ(NumericValue::FromPackedInt(123456789).ValueOrDie(),
            NumericValue::FromString("0.1234567891").ValueOrDie());
  EXPECT_EQ(NumericValue::FromPackedInt(123456789).ValueOrDie(),
            NumericValue::FromString("0.123456789123456789").ValueOrDie());
  EXPECT_EQ(NumericValue::FromPackedInt(123456789).ValueOrDie(),
            NumericValue::FromString("0.1234567894").ValueOrDie());
  EXPECT_EQ(NumericValue::FromPackedInt(123456789).ValueOrDie(),
            NumericValue::FromString("0.1234567885").ValueOrDie());
  EXPECT_EQ(NumericValue::FromPackedInt(123456789).ValueOrDie(),
            NumericValue::FromString("0.123456788555").ValueOrDie());
  EXPECT_EQ(NumericValue::FromPackedInt(123456789).ValueOrDie(),
            NumericValue::FromString("0.1234567889").ValueOrDie());
  EXPECT_EQ(NumericValue::FromPackedInt(-123456789).ValueOrDie(),
            NumericValue::FromString("-0.1234567891").ValueOrDie());
  EXPECT_EQ(NumericValue::FromPackedInt(-123456789).ValueOrDie(),
            NumericValue::FromString("-0.123456789123456789").ValueOrDie());
  EXPECT_EQ(NumericValue::FromPackedInt(-123456789).ValueOrDie(),
            NumericValue::FromString("-0.1234567894").ValueOrDie());
  EXPECT_EQ(NumericValue::FromPackedInt(-123456789).ValueOrDie(),
            NumericValue::FromString("-0.1234567885").ValueOrDie());
  EXPECT_EQ(NumericValue::FromPackedInt(-123456789).ValueOrDie(),
            NumericValue::FromString("-0.123456788555").ValueOrDie());
  EXPECT_EQ(NumericValue::FromPackedInt(-123456789).ValueOrDie(),
            NumericValue::FromString("-0.1234567889").ValueOrDie());
  EXPECT_THAT(
      NumericValue::FromString(
          "99999999999999999999999999999.9999999995"),
      StatusIs(
          zetasql_base::OUT_OF_RANGE,
          "Invalid NUMERIC value: 99999999999999999999999999999.9999999995"));
  EXPECT_THAT(
      NumericValue::FromString(
          "-99999999999999999999999999999.9999999995"),
      StatusIs(
          zetasql_base::OUT_OF_RANGE,
          "Invalid NUMERIC value: -99999999999999999999999999999.9999999995"));
  EXPECT_EQ(NumericValue::MaxValue(),
            NumericValue::FromString("99999999999999999999999999999.9999999991")
                .ValueOrDie());
  EXPECT_EQ(
      NumericValue::MinValue(),
      NumericValue::FromString("-99999999999999999999999999999.9999999991")
          .ValueOrDie());

  // More tests for the exponential form.
  EXPECT_THAT(NumericValue::FromString(std::string(1000, '1') + "e-999"),
              IsOkAndHolds(MkNumeric("1.111111111")));
  EXPECT_THAT(NumericValue::FromString(".123e-10"),
              IsOkAndHolds(NumericValue()));
  EXPECT_THAT(NumericValue::FromStringStrict(".123e-10"),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "Invalid NUMERIC value: .123e-10"));
  EXPECT_THAT(NumericValue::FromString(".123e-9999"),
              IsOkAndHolds(NumericValue()));
  EXPECT_THAT(NumericValue::FromStringStrict(".123e-9999"),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "Invalid NUMERIC value: .123e-9999"));
  EXPECT_THAT(
      NumericValue::FromString("-12345678901234567890123456789012345678e-11"),
      IsOkAndHolds(MkNumeric("-123456789012345678901234567.890123457")));
  EXPECT_THAT(
      NumericValue::FromStringStrict(
          "-12345678901234567890123456789012345678e-11"),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "Invalid NUMERIC value: "
               "-12345678901234567890123456789012345678e-11"));
}

TEST_F(NumericValueTest, Add) {
  // The result is too large to fit into int128.
  EXPECT_THAT(
      NumericValue::MaxValue().Add(NumericValue::MaxValue()),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: 99999999999999999999999999999.999999999 + "
               "99999999999999999999999999999.999999999"));
  EXPECT_THAT(
      NumericValue::MinValue().Add(NumericValue::MinValue()),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: -99999999999999999999999999999.999999999 + "
               "-99999999999999999999999999999.999999999"));
  // The result fits into int128 but exceeds the NUMERIC range.
  EXPECT_THAT(
      NumericValue::MaxValue().Add(NumericValue::FromPackedInt(1).ValueOrDie()),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: 99999999999999999999999999999.999999999 + "
               "0.000000001"));
  EXPECT_THAT(
      NumericValue::MinValue().Add(
          NumericValue::FromPackedInt(-1).ValueOrDie()),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: -99999999999999999999999999999.999999999 + "
               "-0.000000001"));

  EXPECT_EQ(NumericValue::MinValue(),
            NumericValue::MinValue().Add(NumericValue()).ValueOrDie());
  EXPECT_EQ(NumericValue::MaxValue(),
            NumericValue::MaxValue().Add(NumericValue()).ValueOrDie());
  EXPECT_EQ(
      NumericValue(),
      NumericValue::MinValue().Add(NumericValue::MaxValue()).ValueOrDie());
  EXPECT_EQ(NumericValue(3),
            NumericValue(1).Add(NumericValue(2)).ValueOrDie());
  EXPECT_EQ(NumericValue(-3),
            NumericValue(-1).Add(NumericValue(-2)).ValueOrDie());
  EXPECT_EQ(NumericValue::FromPackedInt(1000000001).ValueOrDie(),
            NumericValue(1).Add(
                NumericValue::FromPackedInt(1).ValueOrDie()).ValueOrDie());
}

TEST_F(NumericValueTest, Subtract) {
  // The result is too large to fit into int128.
  EXPECT_THAT(
      NumericValue::MaxValue().Subtract(NumericValue::MinValue()),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: 99999999999999999999999999999.999999999 - "
               "-99999999999999999999999999999.999999999"));
  EXPECT_THAT(
      NumericValue::MinValue().Subtract(NumericValue::MaxValue()),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: -99999999999999999999999999999.999999999 - "
               "99999999999999999999999999999.999999999"));
  // The result fits into int128 but exceeds the NUMERIC range.
  EXPECT_THAT(
      NumericValue::MinValue().Subtract(
          NumericValue::FromPackedInt(1).ValueOrDie()),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: -99999999999999999999999999999.999999999 - "
               "0.000000001"));
  EXPECT_THAT(
      NumericValue::MaxValue().Subtract(
          NumericValue::FromPackedInt(-1).ValueOrDie()),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: 99999999999999999999999999999.999999999 - "
               "-0.000000001"));

  EXPECT_EQ(NumericValue::MinValue(),
            NumericValue::MinValue().Subtract(NumericValue()).ValueOrDie());
  EXPECT_EQ(NumericValue::MaxValue(),
            NumericValue::MaxValue().Subtract(NumericValue()).ValueOrDie());
  EXPECT_EQ(NumericValue(),
            NumericValue(1).Subtract(NumericValue(1)).ValueOrDie());
  EXPECT_EQ(NumericValue(2),
            NumericValue(3).Subtract(NumericValue(1)).ValueOrDie());
  EXPECT_EQ(NumericValue(-2),
            NumericValue(3).Subtract(NumericValue(5)).ValueOrDie());
  EXPECT_EQ(NumericValue(-3),
            NumericValue(-1).Subtract(NumericValue(2)).ValueOrDie());
}

TEST_F(NumericValueTest, Multiply) {
  EXPECT_EQ(NumericValue(),
            NumericValue().Multiply(NumericValue(2)).ValueOrDie());
  EXPECT_EQ(NumericValue::MaxValue(),
            NumericValue::MaxValue().Multiply(NumericValue(1)).ValueOrDie());
  EXPECT_EQ(NumericValue::MinValue(),
            NumericValue::MinValue().Multiply(NumericValue(1)).ValueOrDie());

  // 6 * 0.5
  EXPECT_EQ(NumericValue(3),
            NumericValue(6).Multiply(
                NumericValue::FromPackedInt(
                    500000000).ValueOrDie()).ValueOrDie());
  EXPECT_EQ(NumericValue(-3),
            NumericValue(-6).Multiply(
                NumericValue::FromPackedInt(
                    500000000).ValueOrDie()).ValueOrDie());
  EXPECT_EQ(NumericValue(-3),
            NumericValue(6).Multiply(
                NumericValue::FromPackedInt(
                    -500000000).ValueOrDie()).ValueOrDie());
  EXPECT_EQ(NumericValue(3),
            NumericValue(-6).Multiply(
                NumericValue::FromPackedInt(
                    -500000000).ValueOrDie()).ValueOrDie());

  // Max uint32_t * max uint32_t.
  EXPECT_EQ(MkNumeric("18446744073709551616"),
            NumericValue(4294967296).Multiply(
                NumericValue(4294967296)).ValueOrDie());
  EXPECT_EQ(MkNumeric("-18446744073709551616"),
            NumericValue(-4294967296).Multiply(
                NumericValue(4294967296)).ValueOrDie());
  EXPECT_EQ(MkNumeric("-18446744073709551616"),
            NumericValue(4294967296).Multiply(
                NumericValue(-4294967296)).ValueOrDie());
  EXPECT_EQ(MkNumeric("18446744073709551616"),
            NumericValue(-4294967296).Multiply(
                NumericValue(-4294967296)).ValueOrDie());

  // Rounding.
  // 2.4999999975 -> 2.499999998
  EXPECT_EQ(MkNumeric("2.499999998"), MkNumeric("0.999999999").Multiply(
      MkNumeric("2.5")).ValueOrDie());
  EXPECT_EQ(MkNumeric("-2.499999998"), MkNumeric("-0.999999999").Multiply(
      MkNumeric("2.5")).ValueOrDie());
  EXPECT_EQ(MkNumeric("-2.499999998"), MkNumeric("0.999999999").Multiply(
      MkNumeric("-2.5")).ValueOrDie());
  EXPECT_EQ(MkNumeric("2.499999998"), MkNumeric("-0.999999999").Multiply(
      MkNumeric("-2.5")).ValueOrDie());
  // 2.6999999973 -> 2.699999997
  EXPECT_EQ(MkNumeric("2.699999997"), MkNumeric("0.999999999").Multiply(
      MkNumeric("2.7")).ValueOrDie());

  EXPECT_EQ(NumericValue::MaxValue(),
            MkNumeric("33333333333333333333333333333.333333333")
                .Multiply(NumericValue(3))
                .ValueOrDie());
  EXPECT_EQ(NumericValue::MinValue(),
            MkNumeric("-33333333333333333333333333333.333333333")
                .Multiply(NumericValue(3))
                .ValueOrDie());

  EXPECT_THAT(
      NumericValue::MaxValue().Multiply(NumericValue(2)),
      StatusIs(
          zetasql_base::OUT_OF_RANGE,
          "numeric overflow: 99999999999999999999999999999.999999999 * 2"));
  EXPECT_THAT(
      NumericValue::MinValue().Multiply(NumericValue(2)),
      StatusIs(
          zetasql_base::OUT_OF_RANGE,
          "numeric overflow: -99999999999999999999999999999.999999999 * 2"));
  EXPECT_THAT(
      NumericValue::MaxValue().Multiply(MkNumeric("1.000000001")),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: 99999999999999999999999999999.999999999 * "
               "1.000000001"));
  EXPECT_THAT(
      NumericValue::MinValue().Multiply(MkNumeric("1.000000001")),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: -99999999999999999999999999999.999999999 * "
               "1.000000001"));
  EXPECT_THAT(
      NumericValue(500000000000000LL).Multiply(NumericValue(200000000000000LL)),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: 500000000000000 * 200000000000000"));
  EXPECT_THAT(NumericValue(-500000000000000LL)
                  .Multiply(NumericValue(200000000000000LL)),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: -500000000000000 * 200000000000000"));
  EXPECT_THAT(NumericValue(500000000000000LL)
                  .Multiply(NumericValue(-200000000000000LL)),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: 500000000000000 * -200000000000000"));
  EXPECT_THAT(
      NumericValue(-500000000000000LL)
          .Multiply(NumericValue(-200000000000000LL)),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: -500000000000000 * -200000000000000"));
}

TEST_F(NumericValueTest, Multiply_PowersOfTen) {
  std::string expected_str = "1";
  NumericValue positive(1);
  NumericValue negative(-1);
  for (int i = 0; i < 28; ++i) {
    expected_str.append("0");
    positive = positive.Multiply(NumericValue(10)).ValueOrDie();
    negative = negative.Multiply(NumericValue(10)).ValueOrDie();
    EXPECT_EQ(expected_str, positive.ToString());
    EXPECT_EQ(std::string("-") + expected_str, negative.ToString());
  }

  // Next multiplication will add 30th number and cause overflow.
  EXPECT_THAT(positive.Multiply(NumericValue(10)),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: 10000000000000000000000000000 * 10"));
  EXPECT_THAT(
      negative.Multiply(NumericValue(10)),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: -10000000000000000000000000000 * 10"));

  positive = MkNumeric("0.1");
  negative = MkNumeric("-0.1");
  for (int i = 1; i < 9; ++i) {
    expected_str = std::string("0.") + std::string(i, '0') + "1";
    positive = positive.Multiply(MkNumeric("0.1")).ValueOrDie();
    negative = negative.Multiply(MkNumeric("0.1")).ValueOrDie();
    EXPECT_EQ(expected_str, positive.ToString());
    EXPECT_EQ(std::string("-") + expected_str, negative.ToString());
  }

  // With next multiplication the number becomes too small and gets rounded to
  // zero.
  EXPECT_EQ(NumericValue(), positive.Multiply(MkNumeric("0.1")).ValueOrDie());
  EXPECT_EQ(NumericValue(), negative.Multiply(MkNumeric("0.1")).ValueOrDie());
}

TEST_F(NumericValueTest, Multiply_AllPrecisionCombinations) {
  NumericValue n1 = MkNumeric("0.000000005");
  for (int i = -9; i < 28; ++i) {
    NumericValue n2 = MkNumeric("0.000000003");
    for (int j = -9; j < 28; ++j) {
      int res_prec = i + j;

      if (res_prec < -10) {
        // The result is too small to fit into 9 fractional digits, the result
        // is 0.
        EXPECT_EQ(NumericValue(), n1.Multiply(n2).ValueOrDie());
      } else if (res_prec == -10) {
        // The trailing "15" was rounded.
        EXPECT_EQ("0.000000002",
                  n1.Multiply(n2).ValueOrDie().ToString());
      } else if (res_prec > 27) {
        EXPECT_THAT(n1.Multiply(n2),
                    StatusIs(zetasql_base::OUT_OF_RANGE,
                             absl::StrCat("numeric overflow: ", n1.ToString(),
                                          " * ", n2.ToString())));
      } else {
        NumericValue res = n1.Multiply(n2).ValueOrDie();

        std::string expected_str = "15";
        if (res_prec < -1) {
          expected_str = absl::StrCat(
              "0.", std::string(-res_prec - 2, '0'), "15");
        } else if (res_prec == -1) {
          expected_str = "1.5";
        } else if (res_prec > 0) {
          expected_str += std::string(res_prec, '0');
        }

        EXPECT_EQ(expected_str, res.ToString());
      }

      n2 = n2.Multiply(NumericValue(10)).ValueOrDie();
    }

    n1 = n1.Multiply(NumericValue(10)).ValueOrDie();
  }
}

TEST_F(NumericValueTest, UnaryMinus) {
  EXPECT_EQ(NumericValue(0), NumericValue::UnaryMinus(NumericValue(0)));
  EXPECT_EQ(NumericValue(-1), NumericValue::UnaryMinus(NumericValue(1)));
  EXPECT_EQ(NumericValue(2), NumericValue::UnaryMinus(NumericValue(-2)));
  EXPECT_EQ(NumericValue::MinValue(),
            NumericValue::UnaryMinus(NumericValue::MaxValue()));
  EXPECT_EQ(NumericValue::MaxValue(),
            NumericValue::UnaryMinus(NumericValue::MinValue()));
}

TEST_F(NumericValueTest, Abs) {
  EXPECT_EQ(NumericValue(0), NumericValue::Abs(NumericValue(0)));
  EXPECT_EQ(NumericValue(1), NumericValue::Abs(NumericValue(1)));
  EXPECT_EQ(NumericValue(2), NumericValue::Abs(NumericValue(-2)));
  EXPECT_EQ(NumericValue::MaxValue(),
            NumericValue::Abs(NumericValue::MaxValue()));
  EXPECT_EQ(NumericValue::MaxValue(),
            NumericValue::Abs(NumericValue::MinValue()));
}

TEST_F(NumericValueTest, Sign) {
  EXPECT_EQ(NumericValue(0), NumericValue::Sign(NumericValue(0)));
  EXPECT_EQ(NumericValue(1), NumericValue::Sign(NumericValue(1)));
  EXPECT_EQ(NumericValue(1), NumericValue::Sign(NumericValue(123)));
  EXPECT_EQ(NumericValue(1), NumericValue::Sign(NumericValue::MaxValue()));
  EXPECT_EQ(NumericValue(-1), NumericValue::Sign(NumericValue(-456)));
  EXPECT_EQ(NumericValue(-1), NumericValue::Sign(NumericValue::MinValue()));
}

TEST_F(NumericValueTest, UnalignedReadWrite) {
  std::unique_ptr<char[]> buffer(new char[100]);
  char* buffer_ptr = buffer.get() + 3;
  NumericValue* value = new (buffer_ptr) NumericValue;
  buffer_ptr += 20;
  NumericValue* max_value =
      new (buffer_ptr) NumericValue(NumericValue::MaxValue());

  EXPECT_EQ(NumericValue(static_cast<int64_t>(0)), *value);
  EXPECT_EQ(NumericValue(NumericValue::MaxValue()), *max_value);

  *value = NumericValue(static_cast<int64_t>(10));
  EXPECT_EQ(NumericValue(static_cast<int64_t>(10)), *value);
}

TEST_F(NumericValueTest, IsTriviallyDestructible) {
  // Verify that NumericValue has a trivial destructor and hence can have static
  // storage duration per (broken link).
  constexpr NumericValue kOne(static_cast<int64_t>(1));
  constexpr NumericValue kTwo(static_cast<uint64_t>(2));
  EXPECT_EQ(kOne, kOne);
  EXPECT_LT(kOne, kTwo);
}

TEST_F(NumericValueTest, SerializeSize) {
  std::string bytes = NumericValue(1).SerializeAsProtoBytes();
  EXPECT_EQ(4, bytes.size());
  bytes = NumericValue(-1).SerializeAsProtoBytes();
  EXPECT_EQ(4, bytes.size());
  bytes = NumericValue::MaxValue().SerializeAsProtoBytes();
  EXPECT_EQ(16, bytes.size());
  bytes = NumericValue::MinValue().SerializeAsProtoBytes();
  EXPECT_EQ(16, bytes.size());
  bytes = NumericValue().SerializeAsProtoBytes();
  EXPECT_EQ(1, bytes.size());
}

TEST_F(NumericValueTest, SerializeDeserializeProtoBytes) {
  TestSerialize(NumericValue());
  TestSerialize(NumericValue(1));
  TestSerialize(NumericValue(-1));
  TestSerialize(MkNumeric("123.01"));
  TestSerialize(MkNumeric("-123.01"));
  TestSerialize(NumericValue::MaxValue());
  TestSerialize(NumericValue::MinValue());
  TestSerialize(MkNumeric("0.000000001"));
  TestSerialize(MkNumeric("-0.000000001"));
  TestSerialize(MkNumeric("0.999999999"));
  TestSerialize(MkNumeric("-0.999999999"));

  const int kTestIterations = 500;
  for (int i = 0; i < kTestIterations; ++i) {
    NumericValue value = MakeRandomNumeric();
    NumericValue neg_value = NumericValue::FromPackedInt(
        -value.as_packed_int()).ValueOrDie();
    TestSerialize(value);
    TestSerialize(neg_value);
  }
}

TEST_F(NumericValueTest, DeserializeProtoBytesFailures) {
  std::string bytes;

  EXPECT_THAT(NumericValue::DeserializeFromProtoBytes(bytes),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "Invalid numeric encoding"));
  bytes.resize(17);
  EXPECT_THAT(NumericValue::DeserializeFromProtoBytes(bytes),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "Invalid numeric encoding"));

  bytes.resize(16);
  bytes[15] = 0x7f;
  for (int i = 0; i < 15; ++i) {
    bytes[i] = 0xff;
  }
  EXPECT_THAT(NumericValue::DeserializeFromProtoBytes(bytes),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       ::testing::HasSubstr("numeric overflow: ")));
}

TEST_F(NumericValueTest, ToDouble) {
  EXPECT_DOUBLE_EQ(1.5, MkNumeric("1.5").ToDouble());
  EXPECT_DOUBLE_EQ(-1.5, MkNumeric("-1.5").ToDouble());
  EXPECT_DOUBLE_EQ(0, NumericValue().ToDouble());
  EXPECT_DOUBLE_EQ(0.999999999, MkNumeric("0.999999999").ToDouble());
  EXPECT_DOUBLE_EQ(-0.999999999, MkNumeric("-0.999999999").ToDouble());
  EXPECT_DOUBLE_EQ(1e29, NumericValue::MaxValue().ToDouble());
  EXPECT_DOUBLE_EQ(-1e29, NumericValue::MinValue().ToDouble());
  EXPECT_DOUBLE_EQ(123.0, MkNumeric("123").ToDouble());
  EXPECT_DOUBLE_EQ(-123.0, MkNumeric("-123").ToDouble());

  // Large number that can still be exactly represented as a double.
  EXPECT_DOUBLE_EQ(4503599627370496.0,
                   MkNumeric("4503599627370496").ToDouble());
  EXPECT_DOUBLE_EQ(-4503599627370496.0,
                   MkNumeric("-4503599627370496").ToDouble());
}

TEST_F(NumericValueTest, ToInt32) {
  EXPECT_EQ(0, NumericValue().To<int32_t>().ValueOrDie());
  EXPECT_EQ(123, MkNumeric("123").To<int32_t>().ValueOrDie());
  EXPECT_EQ(-123, MkNumeric("-123").To<int32_t>().ValueOrDie());
  EXPECT_EQ(124, MkNumeric("123.56").To<int32_t>().ValueOrDie());
  EXPECT_EQ(-124, MkNumeric("-123.56").To<int32_t>().ValueOrDie());
  EXPECT_EQ(124, MkNumeric("123.5").To<int32_t>().ValueOrDie());
  EXPECT_EQ(-124, MkNumeric("-123.5").To<int32_t>().ValueOrDie());
  EXPECT_EQ(123, MkNumeric("123.16").To<int32_t>().ValueOrDie());
  EXPECT_EQ(-123, MkNumeric("-123.16").To<int32_t>().ValueOrDie());
  EXPECT_EQ(2147483647, MkNumeric("2147483647").To<int32_t>().ValueOrDie());
  EXPECT_EQ(-2147483648,
            MkNumeric("-2147483648").To<int32_t>().ValueOrDie());

  EXPECT_THAT(MkNumeric("2147483648").To<int32_t>(),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "int32 out of range: 2147483648"));
  EXPECT_THAT(MkNumeric("-2147483649").To<int32_t>(),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "int32 out of range: -2147483649"));
}

TEST_F(NumericValueTest, ToInt64) {
  EXPECT_EQ(0, NumericValue().To<int64_t>().ValueOrDie());
  EXPECT_EQ(123, MkNumeric("123").To<int64_t>().ValueOrDie());
  EXPECT_EQ(-123, MkNumeric("-123").To<int64_t>().ValueOrDie());
  EXPECT_EQ(124, MkNumeric("123.56").To<int64_t>().ValueOrDie());
  EXPECT_EQ(-124, MkNumeric("-123.56").To<int64_t>().ValueOrDie());
  EXPECT_EQ(124, MkNumeric("123.5").To<int64_t>().ValueOrDie());
  EXPECT_EQ(-124, MkNumeric("-123.5").To<int64_t>().ValueOrDie());
  EXPECT_EQ(123, MkNumeric("123.16").To<int64_t>().ValueOrDie());
  EXPECT_EQ(-123, MkNumeric("-123.16").To<int64_t>().ValueOrDie());
  EXPECT_EQ(9223372036854775807ll,
            MkNumeric("9223372036854775807").To<int64_t>().ValueOrDie());
  EXPECT_EQ(-9223372036854775807LL-1,
            MkNumeric("-9223372036854775808").To<int64_t>().ValueOrDie());

  EXPECT_THAT(MkNumeric("9223372036854775808").To<int64_t>(),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "int64 out of range: 9223372036854775808"));
  EXPECT_THAT(MkNumeric("-9223372036854775809").To<int64_t>(),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "int64 out of range: -9223372036854775809"));
}

TEST_F(NumericValueTest, ToUint32) {
  EXPECT_EQ(0, NumericValue().To<uint32_t>().ValueOrDie());
  EXPECT_EQ(123, MkNumeric("123").To<uint32_t>().ValueOrDie());
  EXPECT_EQ(124, MkNumeric("123.56").To<uint32_t>().ValueOrDie());
  EXPECT_EQ(124, MkNumeric("123.5").To<uint32_t>().ValueOrDie());
  EXPECT_EQ(123, MkNumeric("123.16").To<uint32_t>().ValueOrDie());
  EXPECT_EQ(4294967295ul, MkNumeric("4294967295").To<uint32_t>().ValueOrDie());

  EXPECT_THAT(MkNumeric("-1").To<uint32_t>(),
              StatusIs(zetasql_base::OUT_OF_RANGE, "uint32 out of range: -1"));
  EXPECT_THAT(
      MkNumeric("4294967296").To<uint32_t>(),
      StatusIs(zetasql_base::OUT_OF_RANGE, "uint32 out of range: 4294967296"));
}

TEST_F(NumericValueTest, ToUint64) {
  EXPECT_EQ(0, NumericValue().To<uint64_t>().ValueOrDie());
  EXPECT_EQ(123, MkNumeric("123").To<uint64_t>().ValueOrDie());
  EXPECT_EQ(124, MkNumeric("123.56").To<uint64_t>().ValueOrDie());
  EXPECT_EQ(124, MkNumeric("123.5").To<uint64_t>().ValueOrDie());
  EXPECT_EQ(123, MkNumeric("123.16").To<uint64_t>().ValueOrDie());
  EXPECT_EQ(18446744073709551615ull,
            MkNumeric("18446744073709551615").To<uint64_t>().ValueOrDie());

  EXPECT_THAT(MkNumeric("-1").To<uint64_t>(),
              StatusIs(zetasql_base::OUT_OF_RANGE, "uint64 out of range: -1"));
  EXPECT_THAT(MkNumeric("18446744073709551616").To<uint64_t>(),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "uint64 out of range: 18446744073709551616"));
}

TEST_F(NumericValueTest, FromDouble) {
  EXPECT_EQ(NumericValue(), NumericValue::FromDouble(0.0).ValueOrDie());
  EXPECT_EQ(MkNumeric("1.5"), NumericValue::FromDouble(1.5).ValueOrDie());
  EXPECT_EQ(MkNumeric("-1.5"), NumericValue::FromDouble(-1.5).ValueOrDie());
  EXPECT_EQ(MkNumeric("123"), NumericValue::FromDouble(123.0).ValueOrDie());
  EXPECT_EQ(MkNumeric("-123"), NumericValue::FromDouble(-123.0).ValueOrDie());
  EXPECT_EQ(MkNumeric("0.999999999"),
            NumericValue::FromDouble(0.999999999).ValueOrDie());
  EXPECT_EQ(MkNumeric("-0.999999999"),
            NumericValue::FromDouble(-0.999999999).ValueOrDie());
  EXPECT_EQ(MkNumeric("1500000000000000000"),
            NumericValue::FromDouble(1.5e18).ValueOrDie());
  EXPECT_EQ(MkNumeric("-1500000000000000000"),
            NumericValue::FromDouble(-1.5e18).ValueOrDie());
  EXPECT_EQ(MkNumeric("99999999999999991433150857216"),
            NumericValue::FromDouble(
                99999999999999999999999999999.999999999).ValueOrDie());
  EXPECT_EQ(MkNumeric("-99999999999999991433150857216"),
            NumericValue::FromDouble(
                -99999999999999999999999999999.999999999).ValueOrDie());

  // Check rounding.
  EXPECT_EQ(MkNumeric("3.141592653"),
            NumericValue::FromDouble(3.141592653).ValueOrDie());
  EXPECT_EQ(MkNumeric("-3.141592653"),
            NumericValue::FromDouble(-3.141592653).ValueOrDie());
  EXPECT_EQ(MkNumeric("3.141592654"),
            NumericValue::FromDouble(3.141592653589).ValueOrDie());
  EXPECT_EQ(MkNumeric("-3.141592654"),
            NumericValue::FromDouble(-3.141592653589).ValueOrDie());
  EXPECT_EQ(MkNumeric("3.141592653"),
            NumericValue::FromDouble(3.1415926532).ValueOrDie());
  EXPECT_EQ(MkNumeric("-3.141592653"),
            NumericValue::FromDouble(-3.1415926532).ValueOrDie());
  EXPECT_EQ(MkNumeric("3.141592654"),
            NumericValue::FromDouble(3.1415926539).ValueOrDie());
  EXPECT_EQ(MkNumeric("-3.141592654"),
            NumericValue::FromDouble(-3.1415926539).ValueOrDie());
  EXPECT_EQ(MkNumeric("0.555555556"),
            NumericValue::FromDouble(0.5555555555).ValueOrDie());
  EXPECT_EQ(MkNumeric("-0.555555556"),
            NumericValue::FromDouble(-0.5555555555).ValueOrDie());

  // Values that are too small to fit in 9 digits after the point.
  EXPECT_EQ(NumericValue(),
            NumericValue::FromDouble(0.0000000001).ValueOrDie());
  EXPECT_EQ(NumericValue(),
            NumericValue::FromDouble(-0.0000000001).ValueOrDie());

  EXPECT_THAT(
      NumericValue::FromDouble(1e30),
      StatusIs(zetasql_base::OUT_OF_RANGE, "numeric out of range: 1e+30"));
  EXPECT_THAT(
      NumericValue::FromDouble(-1e30),
      StatusIs(zetasql_base::OUT_OF_RANGE, "numeric out of range: -1e+30"));
  EXPECT_THAT(
      NumericValue::FromDouble(std::numeric_limits<double>::quiet_NaN()),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "Illegal conversion of non-finite "
               "floating point number to numeric: nan"));
  EXPECT_THAT(NumericValue::FromDouble(std::numeric_limits<double>::infinity()),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "Illegal conversion of non-finite "
                       "floating point number to numeric: inf"));
  EXPECT_THAT(
      NumericValue::FromDouble(-std::numeric_limits<double>::infinity()),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "Illegal conversion of non-finite "
               "floating point number to numeric: -inf"));
}

TEST_F(NumericValueTest, Divide) {
#define NUM_DIVIDE(x, y) x.Divide(y).ValueOrDie()
#define NUM_MULTIPLY(x, y) x.Multiply(y).ValueOrDie()

  EXPECT_EQ(NumericValue(3),
            NUM_DIVIDE(NumericValue(6), NumericValue(2)));
  EXPECT_EQ(NumericValue(-3),
            NUM_DIVIDE(NumericValue(-6), NumericValue(2)));
  EXPECT_EQ(NumericValue(-3),
            NUM_DIVIDE(NumericValue(6), NumericValue(-2)));
  EXPECT_EQ(NumericValue(3),
            NUM_DIVIDE(NumericValue(-6), NumericValue(-2)));
  EXPECT_EQ(MkNumeric("33333333333333333333333333333.333333333"),
            NUM_DIVIDE(NumericValue::MaxValue(), NumericValue(3)));
  EXPECT_EQ(MkNumeric("-33333333333333333333333333333.333333333"),
            NUM_DIVIDE(NumericValue::MinValue(), NumericValue(3)));
  EXPECT_EQ(MkNumeric("0.333333333"),
            NUM_DIVIDE(NumericValue(1), NumericValue(3)));
  EXPECT_EQ(NumericValue(1),
            NUM_DIVIDE(NumericValue::MaxValue(), NumericValue::MaxValue()));
  EXPECT_EQ(NumericValue(1),
            NUM_DIVIDE(NumericValue::MinValue(), NumericValue::MinValue()));
  EXPECT_EQ(NumericValue(-1),
            NUM_DIVIDE(NumericValue::MaxValue(), NumericValue::MinValue()));
  EXPECT_EQ(MkNumeric("3"),
            NUM_DIVIDE(NumericValue::MaxValue(),
                       MkNumeric("33333333333333333333333333333.333333333")));
  EXPECT_EQ(MkNumeric("-3"),
            NUM_DIVIDE(NumericValue::MinValue(),
                       MkNumeric("33333333333333333333333333333.333333333")));
  EXPECT_EQ(MkNumeric("2.5"),
            NUM_DIVIDE(NumericValue(5), NumericValue(2)));
  EXPECT_EQ(NumericValue(4294967296),
            NUM_DIVIDE(MkNumeric("18446744073709551616"),
                       NumericValue(4294967296)));
  EXPECT_EQ(NumericValue(10),
            NUM_DIVIDE(NumericValue(5), MkNumeric("0.5")));
  EXPECT_EQ(MkNumeric("0.000000003"),
            NUM_DIVIDE(MkNumeric("100000000000000000000"),
                       MkNumeric("33333333333333333333333333333")));

  // Roundtrip division/multiplication.
  EXPECT_EQ(MkNumeric("18446744073709551616"),
            NUM_MULTIPLY(NUM_DIVIDE(MkNumeric("18446744073709551616"),
                                    NumericValue(4294967296)),
                         NumericValue(4294967296)));
  EXPECT_EQ(NumericValue::MaxValue(),
            NUM_MULTIPLY(
                NUM_DIVIDE(
                    NumericValue::MaxValue(),
                    MkNumeric("33333333333333333333333333333.333333333")),
                MkNumeric("33333333333333333333333333333.333333333")));

  // Rounding.
  EXPECT_EQ(MkNumeric("0.666666667"),
            NUM_DIVIDE(NumericValue(2), NumericValue(3)));
  EXPECT_EQ(MkNumeric("-0.666666667"),
            NUM_DIVIDE(NumericValue(-2), NumericValue(3)));
  EXPECT_EQ(MkNumeric("-0.666666667"),
            NUM_DIVIDE(NumericValue(2), NumericValue(-3)));

  // Specific test cases that exercise rarely executed branches in the division
  // algorithm.
  EXPECT_EQ(MkNumeric("2367730588486448005.037486330"),
            NUM_DIVIDE(MkNumeric("72532070012368178591038012.422501607"),
                       MkNumeric("30633582.37")));
  EXPECT_EQ(MkNumeric("-1356135723410310462967487.051135118"),
            NUM_DIVIDE(MkNumeric("75968009597863048104202226663"),
                       MkNumeric("-56017.999")));

  EXPECT_THAT(NumericValue(1).Divide(NumericValue()),
              StatusIs(zetasql_base::OUT_OF_RANGE, "division by zero: 1 / 0"));
  EXPECT_THAT(
      NumericValue::MaxValue().Divide(MkNumeric("0.3")),
      StatusIs(
          zetasql_base::OUT_OF_RANGE,
          "numeric overflow: 99999999999999999999999999999.999999999 / 0.3"));
  EXPECT_THAT(
      NumericValue::MinValue().Divide(MkNumeric("0.3")),
      StatusIs(
          zetasql_base::OUT_OF_RANGE,
          "numeric overflow: -99999999999999999999999999999.999999999 / 0.3"));
  EXPECT_THAT(
      NumericValue::MaxValue().Divide(MkNumeric("0.999999999")),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: 99999999999999999999999999999.999999999 / "
               "0.999999999"));
  EXPECT_THAT(
      NumericValue::MaxValue().Divide(MkNumeric("0.000000001")),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: 99999999999999999999999999999.999999999 / "
               "0.000000001"));

#undef NUM_MULTIPLY
#undef NUM_DIVIDE
}

TEST_F(NumericValueTest, Power) {
#define NUM_POW(x, exp) x.Power(exp).ValueOrDie()

  EXPECT_EQ(NumericValue(1),
            NUM_POW(NumericValue::MaxValue(), NumericValue()));
  EXPECT_EQ(NumericValue(1),
            NUM_POW(NumericValue::MinValue(), NumericValue()));
  EXPECT_EQ(NumericValue(), NUM_POW(NumericValue(), MkNumeric("10")));
  EXPECT_EQ(NumericValue(3), NUM_POW(NumericValue(3), NumericValue(1)));
  EXPECT_EQ(NumericValue(-3), NUM_POW(NumericValue(-3), NumericValue(1)));
  EXPECT_EQ(NumericValue(9), NUM_POW(NumericValue(3), NumericValue(2)));
  EXPECT_EQ(NumericValue(9), NUM_POW(NumericValue(-3), NumericValue(2)));
  EXPECT_EQ(NumericValue(32768),
            NUM_POW(NumericValue(2), NumericValue(15)));
  EXPECT_EQ(NumericValue(-32768),
            NUM_POW(NumericValue(-2), NumericValue(15)));
  EXPECT_EQ(NumericValue::MaxValue(),
            NUM_POW(NumericValue::MaxValue(), NumericValue(1)));
  EXPECT_EQ(NumericValue::MinValue(),
            NUM_POW(NumericValue::MinValue(), NumericValue(1)));
  EXPECT_EQ(MkNumeric("0.01"),
            NUM_POW(MkNumeric("0.1"), MkNumeric("2")));
  EXPECT_EQ(MkNumeric("0.001"),
            NUM_POW(MkNumeric("0.1"), MkNumeric("3")));
  EXPECT_EQ(MkNumeric("0.0001"),
            NUM_POW(MkNumeric("0.1"), MkNumeric("4")));

  EXPECT_EQ(MkNumeric("1.000100005"),
            NUM_POW(MkNumeric("1.00001"), MkNumeric("10")));
  EXPECT_EQ(MkNumeric("86.497558594"),
            NUM_POW(MkNumeric("1.5"), MkNumeric("11")));
  EXPECT_EQ(MkNumeric("-86.497558594"),
            NUM_POW(MkNumeric("-1.5"), MkNumeric("11")));
  EXPECT_EQ(MkNumeric("21916.681339078"),
            NUM_POW(MkNumeric("1.001"), MkNumeric("10000")));

  EXPECT_THAT(
      NumericValue().Power(MkNumeric("-2")),
      StatusIs(zetasql_base::OUT_OF_RANGE, "division by zero: POW(0, -2)"));

  // Negative exponent.
  EXPECT_EQ(MkNumeric("0.2"),
            NUM_POW(MkNumeric("5"), MkNumeric("-1")));
  EXPECT_EQ(MkNumeric("-0.2"),
            NUM_POW(MkNumeric("-5"), MkNumeric("-1")));
  EXPECT_EQ(MkNumeric("1"),
            NUM_POW(MkNumeric("1"), MkNumeric("-10")));
  EXPECT_EQ(MkNumeric("1"),
            NUM_POW(MkNumeric("-1"), MkNumeric("-10")));
  EXPECT_EQ(MkNumeric("-1"),
            NUM_POW(MkNumeric("-1"), MkNumeric("-11")));
  EXPECT_EQ(MkNumeric("10000000000"),
            NUM_POW(MkNumeric("0.1"), MkNumeric("-10")));
  EXPECT_EQ(MkNumeric("10000000000"),
            NUM_POW(MkNumeric("-0.1"), MkNumeric("-10")));
  EXPECT_EQ(MkNumeric("-100000000000"),
            NUM_POW(MkNumeric("-0.1"), MkNumeric("-11")));
  EXPECT_EQ(MkNumeric("10000000000000000000000000000"),
            NUM_POW(MkNumeric("0.1"), MkNumeric("-28")));

  // Fractional exponent.
  EXPECT_EQ(MkNumeric("2"),
            NUM_POW(NumericValue(4), MkNumeric("0.5")));
  EXPECT_EQ(MkNumeric("0.5"),
            NUM_POW(NumericValue(4), MkNumeric("-0.5")));
  EXPECT_EQ(MkNumeric("8"),
            NUM_POW(NumericValue(4), MkNumeric("1.5")));
  EXPECT_EQ(MkNumeric("0.125"),
            NUM_POW(NumericValue(4), MkNumeric("-1.5")));
  EXPECT_EQ(MkNumeric("32"),
            NUM_POW(NumericValue(4), MkNumeric("2.5")));
  EXPECT_EQ(MkNumeric("0.03125"),
            NUM_POW(NumericValue(4), MkNumeric("-2.5")));
  EXPECT_EQ(MkNumeric("30000000000"),
            NUM_POW(MkNumeric("900000000000000000000"), MkNumeric("0.5")));
  EXPECT_EQ(MkNumeric("316227766016837.9375"),
            NUM_POW(NumericValue::MaxValue(), MkNumeric("0.5")));
  EXPECT_EQ(MkNumeric("12345678912345"),
            NUM_POW(MkNumeric("152415787806720022193399025"),
                    MkNumeric("0.5")));

  // POW(1.5, 140): a case with inexact result
  FixedInt<64, 4> expected_packed(int64_t{1000000000});
  const int kExp = 140;
  for (int i = 0; i < kExp; ++i) {
    expected_packed *= 3;
  }
  // Divide expected_packed by pow(2, 140);
  // skip rounding (the resulting error is at most 1e-9).
  expected_packed >>= kExp;
  NumericValue expected =
      NumericValue::FromPackedInt(static_cast<__int128>(expected_packed))
          .ValueOrDie();
  NumericValue actual = NUM_POW(MkNumeric("1.5"), NumericValue(kExp));
  NumericValue error =
      NumericValue::Abs(actual.Subtract(expected).ValueOrDie());
  EXPECT_LT(error, NumericValue(20));

  EXPECT_THAT(MkNumeric("-123").Power(MkNumeric("2.1")),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "Negative NUMERIC value cannot be raised to "
                       "a fractional power: POW(-123, 2.1)"));

  // Underflow.
  EXPECT_EQ(NumericValue(),
            NUM_POW(MkNumeric("0.1"), MkNumeric("10")));
  EXPECT_EQ(NumericValue(),
            NUM_POW(MkNumeric("10000000"), MkNumeric("-2")));
  EXPECT_EQ(NumericValue(),
            NUM_POW(MkNumeric("5.123"), MkNumeric("-40")));
  EXPECT_EQ(NumericValue(),
            NUM_POW(MkNumeric("-5.123"), MkNumeric("-40")));

  // Overflow.
  EXPECT_THAT(
      NumericValue::MaxValue().Power(MkNumeric("2")),
      StatusIs(
          zetasql_base::OUT_OF_RANGE,
          "numeric overflow: POW(99999999999999999999999999999.999999999, 2)"));
  EXPECT_THAT(NumericValue::MinValue().Power(MkNumeric("2")),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: "
                       "POW(-99999999999999999999999999999.999999999, 2)"));
  EXPECT_THAT(
      MkNumeric("5.123").Power(MkNumeric("50")),
      StatusIs(zetasql_base::OUT_OF_RANGE, "numeric overflow: POW(5.123, 50)"));
  EXPECT_THAT(
      MkNumeric("-5.123").Power(MkNumeric("50")),
      StatusIs(zetasql_base::OUT_OF_RANGE, "numeric overflow: POW(-5.123, 50)"));

#undef NUM_POW
}

TEST_F(NumericValueTest, Power_PowersOfTwo) {
  uint64_t oracle = 1;
  for (int64_t exp = 0; exp < 63; ++exp, oracle <<= 1) {
    EXPECT_EQ(NumericValue(oracle),
              NumericValue(2).Power(NumericValue(exp)).ValueOrDie());
    NumericValue neg_oracle(oracle);
    if ((exp % 2) != 0) {
      neg_oracle = NumericValue::UnaryMinus(neg_oracle);
    }
    EXPECT_EQ(neg_oracle,
              NumericValue(-2).Power(NumericValue(exp)).ValueOrDie());
  }
}

template <typename T>
class NumericValueByTypeTest : public NumericValueTest {};

template <typename T>
class AggregatorSerializationByTypeTest : public NumericValueByTypeTest<T> {
 protected:
  void TestSerializeAggregator(const std::vector<NumericValue>& values) {
    T aggregator;
    for (auto value : values) {
      aggregator.Add(value);
    }

    std::string bytes = aggregator.SerializeAsProtoBytes();
    ZETASQL_ASSERT_OK_AND_ASSIGN(T deserialized_aggregator,
                         T::DeserializeFromProtoBytes(bytes));
    EXPECT_EQ(aggregator, deserialized_aggregator);
  }
};

using SumAggregatorTypes =
    ::testing::Types<NumericValue::Aggregator, NumericValue::SumAggregator>;
using AllAggregatorTypes =
    ::testing::Types<NumericValue::Aggregator, NumericValue::SumAggregator,
                     NumericValue::VarianceAggregator>;

TYPED_TEST_SUITE(NumericValueByTypeTest, SumAggregatorTypes);

TYPED_TEST_SUITE(AggregatorSerializationByTypeTest, AllAggregatorTypes);

TYPED_TEST(NumericValueByTypeTest, AggregatorOneValue) {
  TypeParam a1;
  a1.Add(NumericValue(0));
  ZETASQL_ASSERT_OK_AND_ASSIGN(NumericValue sum, a1.GetSum());
  EXPECT_EQ(NumericValue(0), sum);

  TypeParam a2;
  a2.Add(NumericValue::MaxValue());
  ZETASQL_ASSERT_OK_AND_ASSIGN(sum, a2.GetSum());
  EXPECT_EQ(NumericValue::MaxValue(), sum);

  TypeParam a3;
  a3.Add(NumericValue::MinValue());
  ZETASQL_ASSERT_OK_AND_ASSIGN(sum, a3.GetSum());
  EXPECT_EQ(NumericValue::MinValue(), sum);
}

TYPED_TEST(NumericValueByTypeTest, AggregatorMultipleValues) {
  const int64_t kCount = 10000;

  TypeParam a1;
  TypeParam a2;
  TypeParam a3;

  for (int64_t i = 1; i <= kCount; i++) {
    a1.Add(NumericValue(i));
    a2.Add(NumericValue(-i));
    a3.Add(NumericValue(i % 2 ? i : -i));
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(NumericValue sum, a1.GetSum());
  EXPECT_EQ(NumericValue((1 + kCount) * kCount / 2), sum);

  ZETASQL_ASSERT_OK_AND_ASSIGN(sum, a2.GetSum());
  EXPECT_EQ(NumericValue((-1LL - kCount) * kCount / 2), sum);
  ZETASQL_ASSERT_OK_AND_ASSIGN(sum, a3.GetSum());
  EXPECT_EQ(NumericValue(-kCount / 2), sum);

  ZETASQL_ASSERT_OK_AND_ASSIGN(NumericValue expected,
                       NumericValue::FromDouble((1 + kCount) / 2.0));
  ZETASQL_ASSERT_OK_AND_ASSIGN(NumericValue avg, a1.GetAverage(kCount));
  EXPECT_EQ(expected, avg);
  ZETASQL_ASSERT_OK_AND_ASSIGN(expected,
                       NumericValue::FromDouble((-1LL - kCount) / 2.0));
  ZETASQL_ASSERT_OK_AND_ASSIGN(sum, a2.GetSum());
  ZETASQL_ASSERT_OK_AND_ASSIGN(avg, a2.GetAverage(kCount));
  EXPECT_EQ(expected, avg);
  ZETASQL_ASSERT_OK_AND_ASSIGN(expected, NumericValue::FromDouble(-0.5));
  ZETASQL_ASSERT_OK_AND_ASSIGN(sum, a3.GetSum());
  ZETASQL_ASSERT_OK_AND_ASSIGN(avg, a3.GetAverage(kCount));
  EXPECT_EQ(expected, avg);
}

TYPED_TEST(NumericValueByTypeTest, AggregatorAverageRounding) {
  // 1/3 - rounding down.
  TypeParam a1;
  a1.Add(NumericValue(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(NumericValue avg, a1.GetAverage(3));
  EXPECT_EQ("0.333333333", avg.ToString());

  // 2/3 - rounding up.
  a1.Add(NumericValue(1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(avg, a1.GetAverage(3));
  EXPECT_EQ("0.666666667", avg.ToString());

  // 5/11- rounding up.
  a1.Add(NumericValue(3));
  ZETASQL_ASSERT_OK_AND_ASSIGN(avg, a1.GetAverage(11));
  EXPECT_EQ("0.454545455", avg.ToString());

  // -1/3 - rounding down.
  TypeParam a2;
  a2.Add(NumericValue(-1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(avg, a2.GetAverage(3));
  EXPECT_EQ("-0.333333333", avg.ToString());

  // -4/6 - rounding up.
  a2.Add(NumericValue(-1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(avg, a2.GetAverage(3));
  EXPECT_EQ("-0.666666667", avg.ToString());

  // -5/11 - - rounding up.
  a2.Add(NumericValue(-3));
  ZETASQL_ASSERT_OK_AND_ASSIGN(avg, a2.GetAverage(11));
  EXPECT_EQ("-0.454545455", avg.ToString());
}

TYPED_TEST(NumericValueByTypeTest, AggregatorOverflow) {
  TypeParam a1;

  EXPECT_THAT(a1.GetAverage(0),
              StatusIs(zetasql_base::OUT_OF_RANGE, "division by zero: AVG"));

  a1.Add(NumericValue::MaxValue());
  a1.Add(NumericValue(1));
  EXPECT_THAT(a1.GetSum(),
              StatusIs(zetasql_base::OUT_OF_RANGE, "numeric overflow: SUM"));
  EXPECT_THAT(a1.GetAverage(1),
              StatusIs(zetasql_base::OUT_OF_RANGE, "numeric overflow: AVG"));
  a1.Add(NumericValue(-1));
  // The sum no longer overflows.
  ZETASQL_ASSERT_OK_AND_ASSIGN(NumericValue sum, a1.GetSum());
  EXPECT_EQ(NumericValue::MaxValue(), sum);
  ZETASQL_ASSERT_OK_AND_ASSIGN(NumericValue avg, a1.GetAverage(1));
  EXPECT_EQ(NumericValue::MaxValue(), avg);

  // Advance to 2 * MaxValue
  a1.Add(NumericValue::MaxValue());
  EXPECT_THAT(a1.GetSum(),
              StatusIs(zetasql_base::OUT_OF_RANGE, "numeric overflow: SUM"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(avg, a1.GetAverage(2));
  EXPECT_EQ(NumericValue::MaxValue(), avg);

  // Advance back to MaxValue
  a1.Add(NumericValue::MinValue());
  // The sum no longer overflows.
  ZETASQL_ASSERT_OK_AND_ASSIGN(sum, a1.GetSum());
  EXPECT_EQ(NumericValue::MaxValue(), sum);

  // Advance the sum to 2 * MinValue
  a1.Add(NumericValue::MinValue());
  a1.Add(NumericValue::MinValue());
  a1.Add(NumericValue::MinValue());
  // The sum should overflow again.
  EXPECT_THAT(a1.GetSum(),
              StatusIs(zetasql_base::OUT_OF_RANGE, "numeric overflow: SUM"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(avg, a1.GetAverage(6));
  EXPECT_EQ("-33333333333333333333333333333.333333333", avg.ToString());

  // Advance to 2 * MinValue - 1 so dividing by 2 overflows
  a1.Add(NumericValue(-1));
  EXPECT_THAT(a1.GetAverage(2),
              StatusIs(zetasql_base::OUT_OF_RANGE, "numeric overflow: AVG"));

  // Special case: adding 4 values which internal representation is exactly
  // -2^125. That makes the sum exactly -2^127 which is the minimum
  // possible __int128 value.
  NumericValue large_negative =
      TestFixture::MkNumeric("-42535295865117307932921825928.971026432");
  TypeParam a2;
  for (int i = 1; i <= 4; i++) {
    a2.Add(large_negative);
    ZETASQL_ASSERT_OK_AND_ASSIGN(avg, a2.GetAverage(i));
    EXPECT_EQ(large_negative, avg);
  }

  EXPECT_THAT(a2.GetSum(),
              StatusIs(zetasql_base::OUT_OF_RANGE, "numeric overflow: SUM"));

  // Add 4 more of large_negative values - the end result should be the same.
  for (int i = 1; i <= 4; i++) {
    a2.Add(large_negative);
    EXPECT_THAT(a2.GetSum(),
                StatusIs(zetasql_base::OUT_OF_RANGE, "numeric overflow: SUM"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(avg, a2.GetAverage(4 + i));
    EXPECT_EQ(large_negative, avg);
  }

  EXPECT_THAT(a2.GetAverage(2),
              StatusIs(zetasql_base::OUT_OF_RANGE, "numeric overflow: AVG"));

  // Add 10,000 max values, then 20,000 min values, then 10,000 max values.
  const int kCount = 10000;
  TypeParam a3;

  for (int i = 0; i < kCount; i++) {
    a3.Add(NumericValue::MaxValue());
  }
  for (int i = 0; i < kCount * 2; i++) {
    a3.Add(NumericValue::MinValue());
  }
  for (int i = 0; i < kCount; i++) {
    a3.Add(NumericValue::MaxValue());
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(sum, a3.GetSum());
  EXPECT_EQ(NumericValue(0), sum);
  ZETASQL_ASSERT_OK_AND_ASSIGN(avg, a3.GetAverage(kCount * 4));
  EXPECT_EQ(NumericValue(0), avg);
}

TYPED_TEST(NumericValueByTypeTest, AggregatorMergeWith) {
  std::vector<NumericValue> values = {
    NumericValue(0),
    NumericValue(1),
    NumericValue::MaxValue(),
    TestFixture::MkNumeric("-123.01"),
    NumericValue::MinValue(),
    NumericValue::MinValue(),
    NumericValue::MinValue(),
    NumericValue::MaxValue(),
    NumericValue::MaxValue(),
    NumericValue::MaxValue(),
    TestFixture::MkNumeric("56.999999999")
  };

  // Single aggregator used as a control for the MergeWith operation.
  TypeParam control;

  // Tests different total number of aggregated values.
  for (int num_values = 0; num_values <= values.size(); num_values++) {
    if (num_values > 0) {
      control.Add(values[num_values - 1]);
    }

    bool control_sum_overflow = !control.GetSum().ok();
    NumericValue control_sum = control_sum_overflow
        ? NumericValue() : control.GetSum().ValueOrDie();
    NumericValue control_average = num_values > 0
        ? control.GetAverage(num_values).ValueOrDie() : NumericValue();

    // Break input values between two aggregators and test that merging them
    // doesn't affect the sum or the average of values against the control
    // aggregator.
    for (int num_in_first_aggregator = 0; num_in_first_aggregator <= num_values;
         num_in_first_aggregator++) {
      TypeParam a1;
      TypeParam a2;
      for (int i = 0; i < num_in_first_aggregator; i++) {
        a1.Add(values[i]);
      }
      for (int i = num_in_first_aggregator; i < num_values; i++) {
        a2.Add(values[i]);
      }

      TypeParam test;
      test.MergeWith(a1);
      test.MergeWith(a2);

      if (control_sum_overflow) {
        EXPECT_THAT(test.GetSum(), StatusIs(zetasql_base::OUT_OF_RANGE,
                                            "numeric overflow: SUM"));
      } else {
        ZETASQL_ASSERT_OK_AND_ASSIGN(NumericValue sum, test.GetSum());
        EXPECT_EQ(sum, control_sum);
      }

      if (num_values > 0) {
        ZETASQL_ASSERT_OK_AND_ASSIGN(NumericValue avg, test.GetAverage(num_values));
        EXPECT_EQ(avg, control_average);
      }
    }
  }
}

TYPED_TEST(AggregatorSerializationByTypeTest, AggregatorSerialization) {
  TypeParam a1;
  std::string bytes = a1.SerializeAsProtoBytes();
  if (std::is_same_v<TypeParam, NumericValue::Aggregator>) {
    EXPECT_EQ(24, bytes.size());
  }

  this->TestSerializeAggregator({NumericValue()});
  this->TestSerializeAggregator({NumericValue(1)});
  this->TestSerializeAggregator({NumericValue(-1)});
  this->TestSerializeAggregator({this->MkNumeric("123.01")});
  this->TestSerializeAggregator({this->MkNumeric("-123.01")});
  this->TestSerializeAggregator({NumericValue::MinValue()});
  this->TestSerializeAggregator({NumericValue::MaxValue()});
}

TYPED_TEST(AggregatorSerializationByTypeTest,
           AggregatorLargeValueSerialization) {
  std::vector<NumericValue> min_values;
  std::vector<NumericValue> max_values;
  for (int i = 0; i < 100; i++) {
    min_values.push_back(NumericValue::MinValue());
    max_values.push_back(NumericValue::MaxValue());
    this->TestSerializeAggregator(min_values);
    this->TestSerializeAggregator(max_values);
  }
}

TEST_F(NumericValueTest, VarianceAggregatorNoValue) {
  NumericValue::VarianceAggregator agg;
  TestVarianceAggregator(agg, absl::nullopt, absl::nullopt, 0);
}

TEST_F(NumericValueTest, VarianceAggregatorSingleValue) {
  NumericValue::VarianceAggregator agg1;
  agg1.Add(NumericValue(0));
  TestVarianceAggregator(agg1, 0.0, absl::nullopt, 1);

  NumericValue::VarianceAggregator agg2;
  agg2.Add(NumericValue::MaxValue());
  TestVarianceAggregator(agg2, 0.0, absl::nullopt, 1);

  NumericValue::VarianceAggregator agg3;
  agg3.Add(NumericValue::MinValue());
  TestVarianceAggregator(agg3, 0.0, absl::nullopt, 1);
}

TEST_F(NumericValueTest, VarianceAggregatorMultipleValues) {
  const int64_t kMoreCount = 10000;
  const int64_t kLessCount = 5;

  NumericValue::VarianceAggregator agg1;
  NumericValue::VarianceAggregator agg2;
  NumericValue::VarianceAggregator agg3;
  NumericValue::VarianceAggregator agg4;
  NumericValue::VarianceAggregator agg5;
  NumericValue::VarianceAggregator agg6;

  for (int64_t i = 1; i <= kMoreCount; i++) {
    agg1.Add(NumericValue(i));
    agg2.Add(NumericValue(-i));
    agg3.Add(NumericValue(i % 2 ? i : -i));
    agg4.Add(NumericValue(kMoreCount));
  }

  agg5.Add(NumericValue(2));
  agg5.Add(NumericValue(2));
  agg5.Add(NumericValue(-3));
  agg5.Add(NumericValue(2));
  agg5.Add(NumericValue(2));

  agg6.Add(NumericValue(1));
  agg6.Add(NumericValue(-1));
  agg6.Add(NumericValue(1));
  agg6.Add(NumericValue(1));
  agg6.Add(NumericValue(0));

  double expect_pvar1 = 8333333.25;
  double expect_svar1 = 8334166.666666667;
  TestVarianceAggregator(agg1, expect_pvar1, expect_svar1, kMoreCount);
  double expect_pvar2 = 8333333.25;
  double expect_svar2 = 8334166.666666667;
  TestVarianceAggregator(agg2, expect_pvar2, expect_svar2, kMoreCount);
  double expect_pvar3 = 33338333.25;
  double expect_svar3 = 33341667.41674167;
  TestVarianceAggregator(agg3, expect_pvar3, expect_svar3, kMoreCount);
  double expect_pvar4 = 0;
  double expect_svar4 = 0;
  TestVarianceAggregator(agg4, expect_pvar4, expect_svar4, kMoreCount);
  double expect_pvar5 = 4;
  double expect_svar5 = 5;
  TestVarianceAggregator(agg5, expect_pvar5, expect_svar5, kLessCount);
  double expect_pvar6 = 0.64;
  double expect_svar6 = 0.8;
  TestVarianceAggregator(agg6, expect_pvar6, expect_svar6, kLessCount);
}

TEST_F(NumericValueTest, VarianceAggregatorSubtractedValues) {
  const int64_t kLessCount = 5;

  NumericValue::VarianceAggregator agg1;
  NumericValue::VarianceAggregator agg2;

  agg1.Add(NumericValue(2));
  agg1.Add(NumericValue(4));
  agg1.Add(NumericValue(2));
  agg1.Add(NumericValue(5));
  agg1.Add(NumericValue(-3));
  agg1.Add(NumericValue(2));
  agg1.Add(NumericValue(2));
  agg1.Subtract(NumericValue(5));
  agg1.Subtract(NumericValue(4));

  agg2.Add(NumericValue(1));
  agg2.Add(NumericValue(-100));
  agg2.Add(NumericValue(-1));
  agg2.Add(NumericValue(200));
  agg2.Add(NumericValue(1));
  agg2.Subtract(NumericValue(-100));
  agg2.Add(NumericValue(1));
  agg2.Subtract(NumericValue(200));
  agg2.Add(NumericValue(0));

  double expect_pvar1 = 4;
  double expect_svar1 = 5;
  TestVarianceAggregator(agg1, expect_pvar1, expect_svar1, kLessCount);
  double expect_pvar2 = 0.64;
  double expect_svar2 = 0.8;
  TestVarianceAggregator(agg2, expect_pvar2, expect_svar2, kLessCount);
}

TEST_F(NumericValueTest, VarianceAggregatorMergeWith) {
  std::vector<NumericValue> values = {
    NumericValue(0),
    NumericValue(1),
    NumericValue(-1),
    NumericValue::MaxValue(),
    MkNumeric("-123.01"),
    NumericValue::MinValue(),
    NumericValue::MinValue(),
    NumericValue::MinValue(),
    NumericValue::MaxValue(),
    NumericValue::MaxValue(),
    NumericValue::MaxValue(),
    MkNumeric("56.999999999")
  };

  // Single aggregator used as a control for the MergeWith operation.
  NumericValue::VarianceAggregator control;

  // Tests different total number of aggregated values.
  for (int num_values = 0; num_values <= values.size(); ++num_values) {
    if (num_values > 0) {
      control.Add(values[num_values - 1]);
    }

    absl::optional<double> control_population_variance =
        control.GetPopulationVariance(num_values);
    absl::optional<double> control_sampling_variance =
        control.GetSamplingVariance(num_values);

    for (int num_in_first_aggregator = 0; num_in_first_aggregator <= num_values;
         num_in_first_aggregator++) {
      NumericValue::VarianceAggregator a1;
      NumericValue::VarianceAggregator a2;
      for (int i = 0; i < num_in_first_aggregator; i++) {
        a1.Add(values[i]);
      }
      for (int i = num_in_first_aggregator; i < num_values; i++) {
        a2.Add(values[i]);
      }

      NumericValue::VarianceAggregator test;
      test.MergeWith(a2);
      test.MergeWith(a1);

      EXPECT_EQ(control_population_variance,
                test.GetPopulationVariance(num_values));
      EXPECT_EQ(control_sampling_variance,
                test.GetSamplingVariance(num_values));
    }
  }
}

TEST_F(NumericValueTest, HasFractionalPart) {
  const std::vector<std::pair<NumericValue, /*has_fractional_part*/ bool>>
      test_cases = {
          {MkNumeric("0.1"), true},
          {MkNumeric("0.01"), true},
          {MkNumeric("0.001"), true},
          {MkNumeric("0.000000001"), true},
          {MkNumeric("0.987654321"), true},
          {MkNumeric("0.999999999"), true},
          {MkNumeric("-0.1"), true},
          {MkNumeric("-0.01"), true},
          {MkNumeric("-0.001"), true},
          {MkNumeric("-0.000001"), true},
          {MkNumeric("-0.000000001"), true},
          {MkNumeric("1"), false},
          {MkNumeric("10"), false},
          {MkNumeric("9999"), false},
          {MkNumeric("0.000001"), true},
          {NumericValue::MaxValue(), true},
          {NumericValue::MinValue(), true},
          {MkNumeric("99999999999999999999999999999"), false},
          {MkNumeric("99999999999999999999999999999.000000001"), true},
          {MkNumeric("-99999999999999999999999999999"), false},
          {MkNumeric("-99999999999999999999999999999.000000001"), true},
      };

  for (const auto& test_case : test_cases) {
    SCOPED_TRACE(test_case.first.ToString());
    EXPECT_EQ(test_case.first.has_fractional_part(), test_case.second);
  }
}

TEST_F(NumericValueTest, Trunc) {
  EXPECT_EQ(MkNumeric("999"), MkNumeric("999.123").Trunc(0));
  EXPECT_EQ(MkNumeric("-999"), MkNumeric("-999.123").Trunc(0));
  EXPECT_EQ(MkNumeric("999.999999999"), MkNumeric("999.999999999").Trunc(9));
  EXPECT_EQ(MkNumeric("999.999999999"), MkNumeric("999.999999999").Trunc(100));
  EXPECT_EQ(MkNumeric("-999.999999999"), MkNumeric("-999.999999999").Trunc(9));
  EXPECT_EQ(MkNumeric("-999.999999999"),
            MkNumeric("-999.999999999").Trunc(100));
  EXPECT_EQ(NumericValue(), NumericValue::MaxValue().Trunc(-29));
  EXPECT_EQ(NumericValue(), NumericValue::MaxValue().Trunc(-100));
  EXPECT_EQ(NumericValue(), NumericValue::MinValue().Trunc(-29));
  EXPECT_EQ(NumericValue(), NumericValue::MinValue().Trunc(-100));
  EXPECT_EQ(MkNumeric("99999999999999999999999999999.99"),
            NumericValue::MaxValue().Trunc(2));
  EXPECT_EQ(MkNumeric("-99999999999999999999999999999.99"),
            NumericValue::MinValue().Trunc(2));
  EXPECT_EQ(MkNumeric("99999999999999999999999999999.999999999"),
            NumericValue::MaxValue().Trunc(10));
  EXPECT_EQ(MkNumeric("-99999999999999999999999999999.999999999"),
            NumericValue::MinValue().Trunc(10));
  EXPECT_EQ(MkNumeric("999.99999999"), MkNumeric("999.999999999").Trunc(8));
  EXPECT_EQ(MkNumeric("999.9999999"), MkNumeric("999.999999999").Trunc(7));
  EXPECT_EQ(MkNumeric("999.999999"), MkNumeric("999.999999999").Trunc(6));
  EXPECT_EQ(MkNumeric("999.99999"), MkNumeric("999.999999999").Trunc(5));
  EXPECT_EQ(MkNumeric("999.9999"), MkNumeric("999.999999999").Trunc(4));
  EXPECT_EQ(MkNumeric("999.999"), MkNumeric("999.999999999").Trunc(3));
  EXPECT_EQ(MkNumeric("999.99"), MkNumeric("999.999999999").Trunc(2));
  EXPECT_EQ(MkNumeric("999.9"), MkNumeric("999.999999999").Trunc(1));
  EXPECT_EQ(MkNumeric("-999.9"), MkNumeric("-999.999999999").Trunc(1));
  EXPECT_EQ(MkNumeric("99999999999999999999999999990"),
            NumericValue::MaxValue().Trunc(-1));
  EXPECT_EQ(MkNumeric("99999999999999999999999900000"),
            NumericValue::MaxValue().Trunc(-5));
  EXPECT_EQ(MkNumeric("99999999999999999990000000000"),
            NumericValue::MaxValue().Trunc(-10));
  EXPECT_EQ(MkNumeric("99999999900000000000000000000"),
            NumericValue::MaxValue().Trunc(-20));
  EXPECT_EQ(MkNumeric("90000000000000000000000000000"),
            NumericValue::MaxValue().Trunc(-28));
  EXPECT_EQ(MkNumeric("-90000000000000000000000000000"),
            NumericValue::MinValue().Trunc(-28));
  EXPECT_EQ(NumericValue::MaxValue(),
            NumericValue::MaxValue().Trunc(std::numeric_limits<int64_t>::max()));
  EXPECT_EQ(NumericValue::MinValue(),
            NumericValue::MinValue().Trunc(std::numeric_limits<int64_t>::max()));
  EXPECT_EQ(NumericValue(),
            NumericValue::MaxValue().Trunc(std::numeric_limits<int64_t>::min()));
  EXPECT_EQ(NumericValue(),
            NumericValue::MinValue().Trunc(std::numeric_limits<int64_t>::min()));
}

TEST_F(NumericValueTest, Round) {
#define NUM_ROUND(x, digits) x.Round(digits).ValueOrDie()

  EXPECT_EQ(MkNumeric("999"), NUM_ROUND(MkNumeric("999.1"), 0));
  EXPECT_EQ(MkNumeric("1000"), NUM_ROUND(MkNumeric("999.9"), 0));
  EXPECT_EQ(MkNumeric("-999"), NUM_ROUND(MkNumeric("-999.1"), 0));
  EXPECT_EQ(MkNumeric("-1000"), NUM_ROUND(MkNumeric("-999.9"), 0));
  EXPECT_EQ(MkNumeric("999.11"), NUM_ROUND(MkNumeric("999.111"), 2));
  EXPECT_EQ(MkNumeric("999.11"), NUM_ROUND(MkNumeric("999.114"), 2));
  EXPECT_EQ(MkNumeric("999.12"), NUM_ROUND(MkNumeric("999.115"), 2));
  EXPECT_EQ(MkNumeric("999.12"), NUM_ROUND(MkNumeric("999.119"), 2));
  EXPECT_EQ(MkNumeric("999.119"), NUM_ROUND(MkNumeric("999.119"), 3));
  EXPECT_EQ(MkNumeric("999.1"), NUM_ROUND(MkNumeric("999.119"), 1));
  EXPECT_EQ(MkNumeric("1000"), NUM_ROUND(MkNumeric("999.119"), -2));
  EXPECT_EQ(MkNumeric("900"), NUM_ROUND(MkNumeric("919.119"), -2));
  EXPECT_EQ(MkNumeric("1000"), NUM_ROUND(MkNumeric("999.119"), -3));
  EXPECT_EQ(MkNumeric("0"), NUM_ROUND(MkNumeric("999.119"), -4));
  EXPECT_EQ(MkNumeric("-999.11"), NUM_ROUND(MkNumeric("-999.111"), 2));
  EXPECT_EQ(MkNumeric("-999.11"), NUM_ROUND(MkNumeric("-999.114"), 2));
  EXPECT_EQ(MkNumeric("-999.12"), NUM_ROUND(MkNumeric("-999.115"), 2));
  EXPECT_EQ(MkNumeric("-999.12"), NUM_ROUND(MkNumeric("-999.119"), 2));
  EXPECT_EQ(MkNumeric("-999.119"), NUM_ROUND(MkNumeric("-999.119"), 3));
  EXPECT_EQ(MkNumeric("-999.1"), NUM_ROUND(MkNumeric("-999.119"), 1));
  EXPECT_EQ(MkNumeric("-1000"), NUM_ROUND(MkNumeric("-999.119"), -2));
  EXPECT_EQ(MkNumeric("-900"), NUM_ROUND(MkNumeric("-919.119"), -2));
  EXPECT_EQ(MkNumeric("-1000"), NUM_ROUND(MkNumeric("-999.119"), -3));
  EXPECT_EQ(MkNumeric("0"), NUM_ROUND(MkNumeric("-999.119"), -4));
  EXPECT_EQ(MkNumeric("11111111111111111111111111110"),
            NUM_ROUND(MkNumeric("11111111111111111111111111111.111111111"),
                      -1));
  EXPECT_EQ(MkNumeric("-11111111111111111111111111110"),
            NUM_ROUND(MkNumeric("-11111111111111111111111111111.111111111"),
                      -1));
  EXPECT_EQ(MkNumeric("11111111111111111111111111120"),
            NUM_ROUND(MkNumeric("11111111111111111111111111119.111111111"),
                      -1));
  EXPECT_EQ(MkNumeric("-11111111111111111111111111120"),
            NUM_ROUND(MkNumeric("-11111111111111111111111111119.111111111"),
                      -1));
  EXPECT_EQ(NumericValue::MaxValue(),
            NUM_ROUND(NumericValue::MaxValue(),
                      std::numeric_limits<int64_t>::max()));
  EXPECT_EQ(NumericValue::MinValue(),
            NUM_ROUND(NumericValue::MinValue(),
                      std::numeric_limits<int64_t>::max()));
  EXPECT_EQ(NumericValue(),
            NUM_ROUND(NumericValue::MaxValue(),
                      std::numeric_limits<int64_t>::min()));
  EXPECT_EQ(NumericValue(),
            NUM_ROUND(NumericValue::MinValue(),
                      std::numeric_limits<int64_t>::min()));

  EXPECT_THAT(NumericValue::MaxValue().Round(1),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: "
                       "ROUND(99999999999999999999999999999.999999999, 1)"));
  EXPECT_THAT(NumericValue::MaxValue().Round(0),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: "
                       "ROUND(99999999999999999999999999999.999999999, 0)"));
  EXPECT_THAT(NumericValue::MaxValue().Round(-1),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: "
                       "ROUND(99999999999999999999999999999.999999999, -1)"));
  EXPECT_THAT(NumericValue::MinValue().Round(1),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: "
                       "ROUND(-99999999999999999999999999999.999999999, 1)"));
  EXPECT_THAT(NumericValue::MinValue().Round(0),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: "
                       "ROUND(-99999999999999999999999999999.999999999, 0)"));
  EXPECT_THAT(NumericValue::MinValue().Round(-1),
              StatusIs(zetasql_base::OUT_OF_RANGE,
                       "numeric overflow: "
                       "ROUND(-99999999999999999999999999999.999999999, -1)"));

#undef NUM_ROUND
}

TEST_F(NumericValueTest, IntegerDivide) {
#define NUM_DIVIDE(x, y) x.IntegerDivide(y).ValueOrDie()

  EXPECT_EQ(NumericValue(3),
            NUM_DIVIDE(NumericValue(6), NumericValue(2)));
  EXPECT_EQ(NumericValue(-3),
            NUM_DIVIDE(NumericValue(-6), NumericValue(2)));
  EXPECT_EQ(NumericValue(-3),
            NUM_DIVIDE(NumericValue(6), NumericValue(-2)));
  EXPECT_EQ(NumericValue(3),
            NUM_DIVIDE(NumericValue(-6), NumericValue(-2)));
  EXPECT_EQ(MkNumeric("0"),
            NUM_DIVIDE(NumericValue(1), NumericValue(3)));
  EXPECT_EQ(MkNumeric("33333333333333333333333333333"),
            NUM_DIVIDE(NumericValue::MaxValue(), NumericValue(3)));
  EXPECT_EQ(MkNumeric("-33333333333333333333333333333"),
            NUM_DIVIDE(NumericValue::MinValue(), NumericValue(3)));
  EXPECT_EQ(MkNumeric("49999999999999999999999999999"),
            NUM_DIVIDE(NumericValue::MaxValue(), NumericValue(2)));
  EXPECT_EQ(MkNumeric("-49999999999999999999999999999"),
            NUM_DIVIDE(NumericValue::MinValue(), NumericValue(2)));
  EXPECT_EQ(MkNumeric("99999999999999999999999999999"),
            NUM_DIVIDE(NumericValue::MaxValue(), NumericValue(1)));
  EXPECT_EQ(MkNumeric("-99999999999999999999999999999"),
            NUM_DIVIDE(NumericValue::MaxValue(), NumericValue(-1)));
  EXPECT_EQ(MkNumeric("-99999999999999999999999999999"),
            NUM_DIVIDE(NumericValue::MinValue(), NumericValue(1)));
  EXPECT_EQ(MkNumeric("99999999999999999999999999999"),
            NUM_DIVIDE(NumericValue::MinValue(), NumericValue(-1)));
  EXPECT_EQ(NumericValue(2),
            NUM_DIVIDE(NumericValue(5), MkNumeric("2.3")));
  EXPECT_EQ(NumericValue(-2),
            NUM_DIVIDE(NumericValue(-5), MkNumeric("2.3")));
  EXPECT_EQ(NumericValue(-2),
            NUM_DIVIDE(NumericValue(5), MkNumeric("-2.3")));
  EXPECT_EQ(NumericValue(2),
            NUM_DIVIDE(NumericValue(-5), MkNumeric("-2.3")));
  EXPECT_EQ(NumericValue(2),
            NUM_DIVIDE(MkNumeric("5.2"), NumericValue(2)));

  EXPECT_THAT(NumericValue(1).IntegerDivide(NumericValue()),
              StatusIs(zetasql_base::OUT_OF_RANGE, "division by zero: 1 / 0"));
  EXPECT_THAT(
      NumericValue::MaxValue().IntegerDivide(MkNumeric("0.3")),
      StatusIs(
          zetasql_base::OUT_OF_RANGE,
          "numeric overflow: 99999999999999999999999999999.999999999 / 0.3"));
  EXPECT_THAT(
      NumericValue::MinValue().IntegerDivide(MkNumeric("0.3")),
      StatusIs(
          zetasql_base::OUT_OF_RANGE,
          "numeric overflow: -99999999999999999999999999999.999999999 / 0.3"));
  EXPECT_THAT(
      MkNumeric("1e20").IntegerDivide(MkNumeric("1e-9")),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: 100000000000000000000 / 0.000000001"));
  EXPECT_THAT(
      MkNumeric("1e20").IntegerDivide(MkNumeric("-1e-9")),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: 100000000000000000000 / -0.000000001"));
  EXPECT_THAT(
      MkNumeric("-1e20").IntegerDivide(MkNumeric("1e-9")),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: -100000000000000000000 / 0.000000001"));
  EXPECT_THAT(
      MkNumeric("-1e20").IntegerDivide(MkNumeric("-1e-9")),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: -100000000000000000000 / -0.000000001"));

#undef NUM_DIVIDE
}

TEST_F(NumericValueTest, Mod) {
#define NUM_MOD(x, y) x.Mod(y).ValueOrDie()

  EXPECT_EQ(NumericValue(1),
            NUM_MOD(NumericValue(5), NumericValue(2)));
  EXPECT_EQ(NumericValue(-1),
            NUM_MOD(NumericValue(-5), NumericValue(2)));
  EXPECT_EQ(NumericValue(1),
            NUM_MOD(NumericValue(5), NumericValue(-2)));
  EXPECT_EQ(NumericValue(-1),
            NUM_MOD(NumericValue(-5), NumericValue(-2)));
  EXPECT_EQ(NumericValue(),
            NUM_MOD(NumericValue(5), MkNumeric("0.001")));
  EXPECT_EQ(NumericValue(),
            NUM_MOD(NumericValue(-5), MkNumeric("0.001")));
  EXPECT_EQ(MkNumeric("0.999999999"),
            NUM_MOD(NumericValue::MaxValue(), NumericValue(3)));
  EXPECT_EQ(MkNumeric("-0.999999999"),
            NUM_MOD(NumericValue::MinValue(), NumericValue(3)));
  EXPECT_EQ(MkNumeric("1.999999999"),
            NUM_MOD(NumericValue::MaxValue(), NumericValue(2)));
  EXPECT_EQ(MkNumeric("-1.999999999"),
            NUM_MOD(NumericValue::MinValue(), NumericValue(2)));
  EXPECT_EQ(MkNumeric("0.4"),
            NUM_MOD(NumericValue(5), MkNumeric("2.3")));
  EXPECT_EQ(MkNumeric("-0.4"),
            NUM_MOD(NumericValue(-5), MkNumeric("2.3")));
  EXPECT_EQ(MkNumeric("0.4"),
            NUM_MOD(NumericValue(5), MkNumeric("-2.3")));
  EXPECT_EQ(MkNumeric("-0.4"),
            NUM_MOD(NumericValue(-5), MkNumeric("-2.3")));
  EXPECT_EQ(MkNumeric("0.2"),
            NUM_MOD(NumericValue(5), MkNumeric("0.3")));
  EXPECT_EQ(MkNumeric("-0.2"),
            NUM_MOD(NumericValue(-5), MkNumeric("0.3")));
  EXPECT_EQ(MkNumeric("0.2"),
            NUM_MOD(NumericValue(5), MkNumeric("-0.3")));
  EXPECT_EQ(MkNumeric("-0.2"),
            NUM_MOD(NumericValue(-5), MkNumeric("-0.3")));
  EXPECT_EQ(MkNumeric("1.2"),
            NUM_MOD(MkNumeric("5.2"), NumericValue(2)));

  EXPECT_THAT(NumericValue(1).Mod(NumericValue()),
              StatusIs(zetasql_base::OUT_OF_RANGE, "division by zero: 1 / 0"));

#undef NUM_MOD
}

TEST_F(NumericValueTest, Ceiling) {
#define NUM_CEIL(x) x.Ceiling().ValueOrDie()

  EXPECT_EQ(MkNumeric("999"), NUM_CEIL(MkNumeric("999")));
  EXPECT_EQ(MkNumeric("1000"), NUM_CEIL(MkNumeric("999.1")));
  EXPECT_EQ(MkNumeric("1000"), NUM_CEIL(MkNumeric("999.9")));
  EXPECT_EQ(MkNumeric("-999"), NUM_CEIL(MkNumeric("-999")));
  EXPECT_EQ(MkNumeric("-999"), NUM_CEIL(MkNumeric("-999.1")));
  EXPECT_EQ(MkNumeric("-999"), NUM_CEIL(MkNumeric("-999.9")));
  EXPECT_EQ(MkNumeric("1"), NUM_CEIL(MkNumeric("0.999999999")));
  EXPECT_EQ(MkNumeric("0"), NUM_CEIL(MkNumeric("-0.999999999")));
  EXPECT_EQ(NumericValue(), NUM_CEIL(NumericValue()));
  EXPECT_EQ(MkNumeric("-99999999999999999999999999999"),
            NUM_CEIL(NumericValue::MinValue()));

  EXPECT_THAT(
      NumericValue::MaxValue().Ceiling(),
      StatusIs(
          zetasql_base::OUT_OF_RANGE,
          "numeric overflow: CEIL(99999999999999999999999999999.999999999)"));
  EXPECT_THAT(
      MkNumeric("99999999999999999999999999999.1").Ceiling(),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: CEIL(99999999999999999999999999999.1)"));

#undef NUM_CEIL
}

TEST_F(NumericValueTest, Floor) {
#define NUM_FLOOR(x) x.Floor().ValueOrDie()

  EXPECT_EQ(MkNumeric("999"), NUM_FLOOR(MkNumeric("999")));
  EXPECT_EQ(MkNumeric("999"), NUM_FLOOR(MkNumeric("999.1")));
  EXPECT_EQ(MkNumeric("999"), NUM_FLOOR(MkNumeric("999.9")));
  EXPECT_EQ(MkNumeric("-999"), NUM_FLOOR(MkNumeric("-999")));
  EXPECT_EQ(MkNumeric("-1000"), NUM_FLOOR(MkNumeric("-999.1")));
  EXPECT_EQ(MkNumeric("-1000"), NUM_FLOOR(MkNumeric("-999.9")));
  EXPECT_EQ(MkNumeric("0"), NUM_FLOOR(MkNumeric("0.999999999")));
  EXPECT_EQ(MkNumeric("-1"), NUM_FLOOR(MkNumeric("-0.999999999")));
  EXPECT_EQ(NumericValue(), NUM_FLOOR(NumericValue()));
  EXPECT_EQ(MkNumeric("99999999999999999999999999999"),
            NUM_FLOOR(NumericValue::MaxValue()));

  EXPECT_THAT(
      NumericValue::MinValue().Floor(),
      StatusIs(
          zetasql_base::OUT_OF_RANGE,
          "numeric overflow: FLOOR(-99999999999999999999999999999.999999999)"));
  EXPECT_THAT(
      MkNumeric("-99999999999999999999999999999.1").Floor(),
      StatusIs(zetasql_base::OUT_OF_RANGE,
               "numeric overflow: FLOOR(-99999999999999999999999999999.1)"));

#undef NUM_FLOOR
}

// A regression test for b/77498186.
TEST_F(NumericValueTest, FromString_UnexpectedInvalidValue) {
  const char buf[] = "9997.";
  ZETASQL_ASSERT_OK_AND_ASSIGN(NumericValue nv,
                       NumericValue::FromString(absl::string_view(buf, 4)));
  EXPECT_EQ(NumericValue(9997), nv);
}

}  // namespace zetasql
