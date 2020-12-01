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

#include "zetasql/public/interval_value.h"

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/interval_value_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"

namespace zetasql {
namespace {

using zetasql_base::testing::StatusIs;

using interval_testing::Months;
using interval_testing::Days;
using interval_testing::Micros;
using interval_testing::Nanos;
using interval_testing::MonthsDaysMicros;
using interval_testing::MonthsDaysNanos;

TEST(IntervalValueTest, Months) {
  IntervalValue interval;
  std::vector<int64_t> values{IntervalValue::kMinMonths,
                            IntervalValue::kMaxMonths,
                            0,
                            -1,
                            12,
                            -55,
                            7654};
  for (int64_t value : values) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(interval, IntervalValue::FromMonths(value));
    EXPECT_EQ(value, interval.get_months());
  }
  EXPECT_THAT(IntervalValue::FromMonths(IntervalValue::kMaxMonths + 1),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(IntervalValue::FromMonths(IntervalValue::kMinMonths - 1),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalValueTest, Days) {
  IntervalValue interval;
  std::vector<int64_t> values{
      IntervalValue::kMinDays, IntervalValue::kMaxDays, 0, 30, -365, 12345};
  for (int64_t value : values) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(interval, IntervalValue::FromDays(value));
    EXPECT_EQ(value, interval.get_days());
  }
  EXPECT_THAT(IntervalValue::FromDays(IntervalValue::kMaxDays + 1),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(IntervalValue::FromDays(IntervalValue::kMinDays - 1),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalValueTest, Micros) {
  IntervalValue interval;
  std::vector<int64_t> values{IntervalValue::kMinMicros,
                            IntervalValue::kMaxMicros,
                            IntervalValue::kMinMicros + 1,
                            IntervalValue::kMaxMicros - 1,
                            0,
                            1,
                            -1000,
                            1000000,
                            -123456789};
  for (int64_t value : values) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(interval, IntervalValue::FromMicros(value));
    EXPECT_EQ(value, interval.get_micros());
    EXPECT_EQ(value, interval.GetAsMicros());
    EXPECT_EQ(0, interval.get_nano_fractions());
  }
  EXPECT_THAT(IntervalValue::FromMicros(IntervalValue::kMaxMicros + 1),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(IntervalValue::FromMicros(IntervalValue::kMinMicros - 1),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalValueTest, Nanos) {
  IntervalValue interval;
  std::vector<__int128> values{IntervalValue::kMinNanos,
                               IntervalValue::kMaxNanos,
                               IntervalValue::kMinNanos + 1,
                               IntervalValue::kMaxNanos - 1,
                               0,
                               1,
                               -1,
                               999,
                               -999,
                               1000,
                               -1000,
                               1001,
                               -1001,
                               123456789,
                               -987654321012345};
  for (int64_t value : values) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(interval, IntervalValue::FromNanos(value));
    EXPECT_EQ(value, interval.get_nanos());
    EXPECT_EQ(value, interval.GetAsNanos());
    EXPECT_GE(interval.get_nano_fractions(), 0);
  }
  EXPECT_THAT(IntervalValue::FromMicros(IntervalValue::kMaxMicros + 1),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(IntervalValue::FromMicros(IntervalValue::kMinMicros - 1),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalValueTest, MonthsDaysMicros) {
  IntervalValue interval;
  absl::BitGen random_;

  for (int i = 0; i < 10000; i++) {
    int64_t month = absl::Uniform<int64_t>(random_, IntervalValue::kMinMonths,
                                       IntervalValue::kMaxMonths);
    int64_t day = absl::Uniform<int64_t>(random_, IntervalValue::kMinDays,
                                     IntervalValue::kMaxDays);
    int64_t micros = absl::Uniform<int64_t>(random_, IntervalValue::kMinMicros,
                                        IntervalValue::kMaxMicros);
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        interval, IntervalValue::FromMonthsDaysMicros(month, day, micros));
    EXPECT_EQ(month, interval.get_months());
    EXPECT_EQ(day, interval.get_days());
    EXPECT_EQ(micros, interval.get_micros());

    std::string serialized = interval.SerializeAsBytes();
    ZETASQL_ASSERT_OK_AND_ASSIGN(IntervalValue interval_deserialized,
                         IntervalValue::DeserializeFromBytes(serialized));
    EXPECT_EQ(month, interval_deserialized.get_months());
    EXPECT_EQ(day, interval_deserialized.get_days());
    EXPECT_EQ(micros, interval_deserialized.get_micros());
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      interval, IntervalValue::FromMonthsDaysMicros(IntervalValue::kMaxMonths,
                                                    IntervalValue::kMaxDays,
                                                    IntervalValue::kMaxMicros));
  EXPECT_EQ(943488000000000000, interval.GetAsMicros());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      interval, IntervalValue::FromMonthsDaysMicros(IntervalValue::kMinMonths,
                                                    IntervalValue::kMinDays,
                                                    IntervalValue::kMinMicros));
  EXPECT_EQ(-943488000000000000, interval.GetAsMicros());
}

TEST(IntervalValueTest, MonthsDaysNanos) {
  IntervalValue interval;
  absl::BitGen random_;

  for (int i = 0; i < 10000; i++) {
    int64_t month = absl::Uniform<int64_t>(random_, IntervalValue::kMinMonths,
                                       IntervalValue::kMaxMonths);
    int64_t day = absl::Uniform<int64_t>(random_, IntervalValue::kMinDays,
                                     IntervalValue::kMaxDays);
    int64_t micros = absl::Uniform<int64_t>(random_, IntervalValue::kMinMicros,
                                        IntervalValue::kMaxMicros);
    int64_t nano_fractions = absl::Uniform<int64_t>(
        random_, -IntervalValue::kNanosInMicro, IntervalValue::kNanosInMicro);
    __int128 nanos = static_cast<__int128>(micros) * 1000 + nano_fractions;
    ZETASQL_ASSERT_OK_AND_ASSIGN(interval,
                         IntervalValue::FromMonthsDaysNanos(month, day, nanos));
    EXPECT_EQ(month, interval.get_months());
    EXPECT_EQ(day, interval.get_days());
    EXPECT_EQ(nanos, interval.get_nanos());
    EXPECT_GE(interval.get_nano_fractions(), 0);

    std::string serialized = interval.SerializeAsBytes();
    ZETASQL_ASSERT_OK_AND_ASSIGN(IntervalValue interval_deserialized,
                         IntervalValue::DeserializeFromBytes(serialized));
    EXPECT_EQ(month, interval_deserialized.get_months());
    EXPECT_EQ(day, interval_deserialized.get_days());
    EXPECT_EQ(nanos, interval_deserialized.get_nanos());
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      interval, IntervalValue::FromMonthsDaysNanos(IntervalValue::kMaxMonths,
                                                   IntervalValue::kMaxDays,
                                                   IntervalValue::kMaxNanos));
  EXPECT_EQ(static_cast<__int128>(943488000000000000) * 1000,
            interval.GetAsNanos());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      interval, IntervalValue::FromMonthsDaysNanos(IntervalValue::kMinMonths,
                                                   IntervalValue::kMinDays,
                                                   IntervalValue::kMinNanos));
  EXPECT_EQ(static_cast<__int128>(-943488000000000000) * 1000,
            interval.GetAsNanos());
}

TEST(IntervalValueTest, Deserialize) {
  {
    // Empty bytes translate to interval value 0
    ZETASQL_ASSERT_OK_AND_ASSIGN(IntervalValue v, IntervalValue::DeserializeFromBytes(
                                              absl::string_view()));
    EXPECT_EQ(0, v.get_days());
    EXPECT_EQ(0, v.get_months());
    EXPECT_EQ(0, v.get_micros());
    EXPECT_EQ(0, v.get_nanos());
    EXPECT_EQ(0, v.get_nano_fractions());
  }

  {
    // All zeros in serialized form should map to all interval fields being 0.
    const char bytes[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    ZETASQL_ASSERT_OK_AND_ASSIGN(IntervalValue v, IntervalValue::DeserializeFromBytes(
                                              absl::string_view(bytes, 16)));
    EXPECT_EQ(0, v.get_days());
    EXPECT_EQ(0, v.get_months());
    EXPECT_EQ(0, v.get_micros());
    EXPECT_EQ(0, v.get_nanos());
    EXPECT_EQ(0, v.get_nano_fractions());
  }

  {
    const char bytes[] = {0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};
    ZETASQL_ASSERT_OK_AND_ASSIGN(IntervalValue v, IntervalValue::DeserializeFromBytes(
                                              absl::string_view(bytes, 16)));
    EXPECT_EQ(1, v.get_days());
    EXPECT_EQ(0, v.get_months());
    EXPECT_EQ(0, v.get_micros());
    EXPECT_EQ(0, v.get_nanos());
    EXPECT_EQ(0, v.get_nano_fractions());
  }

  {
    const char bytes[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0};
    ZETASQL_ASSERT_OK_AND_ASSIGN(IntervalValue v, IntervalValue::DeserializeFromBytes(
                                              absl::string_view(bytes, 16)));
    EXPECT_EQ(0, v.get_days());
    EXPECT_EQ(1, v.get_months());
    EXPECT_EQ(0, v.get_micros());
    EXPECT_EQ(0, v.get_nanos());
    EXPECT_EQ(0, v.get_nano_fractions());
  }

  {
    const char bytes[] = {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    ZETASQL_ASSERT_OK_AND_ASSIGN(IntervalValue v, IntervalValue::DeserializeFromBytes(
                                              absl::string_view(bytes, 16)));
    EXPECT_EQ(0, v.get_days());
    EXPECT_EQ(0, v.get_months());
    EXPECT_EQ(1, v.get_micros());
    EXPECT_EQ(1000, v.get_nanos());
    EXPECT_EQ(0, v.get_nano_fractions());
  }

  {
    const char bytes[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0};
    ZETASQL_ASSERT_OK_AND_ASSIGN(IntervalValue v, IntervalValue::DeserializeFromBytes(
                                              absl::string_view(bytes, 16)));
    EXPECT_EQ(0, v.get_days());
    EXPECT_EQ(0, v.get_months());
    EXPECT_EQ(0, v.get_micros());
    EXPECT_EQ(1, v.get_nanos());
    EXPECT_EQ(1, v.get_nano_fractions());
  }

  // Error cases
  {
    // Not enough bytes
    const char bytes[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    EXPECT_THAT(
        IntervalValue::DeserializeFromBytes(absl::string_view(bytes, 15)),
        StatusIs(absl::StatusCode::kOutOfRange));
  }
  {
    // Invalid fields
    const char bytes[] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    EXPECT_THAT(
        IntervalValue::DeserializeFromBytes(absl::string_view(bytes, 16)),
        StatusIs(absl::StatusCode::kOutOfRange));
  }
}

void ExpectEqual(IntervalValue v1, IntervalValue v2) {
  EXPECT_EQ(v1, v2);
  EXPECT_EQ(v2, v1);
  EXPECT_FALSE(v1 != v2);
  EXPECT_FALSE(v2 != v1);
  EXPECT_LE(v1, v2);
  EXPECT_LE(v2, v1);
  EXPECT_GE(v1, v2);
  EXPECT_GE(v2, v1);
  EXPECT_FALSE(v1 < v2);
  EXPECT_FALSE(v2 < v1);
  EXPECT_FALSE(v1 > v2);
  EXPECT_FALSE(v2 > v1);
}

void ExpectLess(IntervalValue v1, IntervalValue v2) {
  EXPECT_LT(v1, v2);
  EXPECT_LE(v1, v2);
  EXPECT_GT(v2, v1);
  EXPECT_GE(v2, v1);
  EXPECT_NE(v1, v2);
  EXPECT_NE(v2, v1);
  EXPECT_FALSE(v1 == v2);
  EXPECT_FALSE(v2 == v1);
}

TEST(IntervalValueTest, Comparisons) {
  ExpectEqual(Months(0), Days(0));
  ExpectEqual(Months(0), Micros(0));
  ExpectEqual(Months(0), Nanos(0));

  ExpectEqual(Months(10), Months(10));
  ExpectEqual(Days(-3), Days(-3));
  ExpectEqual(Micros(12345), Micros(12345));
  ExpectEqual(Nanos(-9876), Nanos(-9876));

  ExpectLess(Months(-1), Days(0));
  ExpectLess(Days(1), Months(1));
  ExpectLess(Days(-1), Micros(1));

  ExpectEqual(Days(30), Months(1));
  ExpectEqual(Months(-2), Days(-60));
  ExpectEqual(Days(360), Months(12));
  ExpectLess(Days(1), Months(1));
  ExpectLess(Days(29), Months(1));
  ExpectLess(Days(-31), Months(-1));
  ExpectLess(Months(1), Days(31));
  ExpectLess(Months(-1), Days(25));
  ExpectLess(Months(12), Days(365));

  ExpectEqual(Micros(IntervalValue::kMicrosInDay), Days(1));
  ExpectLess(Micros(IntervalValue::kMicrosInDay - 1), Days(1));
  ExpectLess(Days(1), Micros(IntervalValue::kMicrosInDay + 1));
  ExpectEqual(Nanos(-IntervalValue::kNanosInDay), Days(-1));
  ExpectLess(Nanos(-IntervalValue::kNanosInDay - 1), Days(-1));
  ExpectEqual(Micros(IntervalValue::kMicrosInMonth), Months(1));
  ExpectEqual(Nanos(-IntervalValue::kNanosInMonth), Months(-1));

  ExpectEqual(Micros(-1), Nanos(-1000));
  ExpectEqual(Micros(7), Nanos(7000));
  ExpectLess(Micros(1), Nanos(1001));
  ExpectLess(Micros(-1), Nanos(900));
  ExpectLess(Nanos(999), Micros(1));
  ExpectLess(Nanos(-1001), Micros(-1));
  ExpectLess(Nanos(1), Micros(1));
  ExpectLess(Micros(-1), Nanos(1));
  ExpectLess(Nanos(1), Micros(1));

  ExpectEqual(MonthsDaysMicros(1, 1, 0), Days(31));
  ExpectEqual(MonthsDaysMicros(1, -1, 0), Days(29));
  ExpectEqual(MonthsDaysMicros(-1, 1, 0), Days(-29));
  ExpectEqual(MonthsDaysMicros(-1, -1, 0), Days(-31));
  ExpectEqual(MonthsDaysMicros(0, 1, 10),
              Micros(IntervalValue::kMicrosInDay + 10));
  ExpectEqual(MonthsDaysMicros(-1, 30, 1), Micros(1));
  ExpectEqual(MonthsDaysMicros(2, -61, 0), Days(-1));
  ExpectEqual(MonthsDaysMicros(-3, 92, -10), MonthsDaysMicros(0, 2, -10));

  ExpectEqual(MonthsDaysMicros(1, 2, 3), MonthsDaysNanos(1, 2, 3000));
  ExpectEqual(MonthsDaysMicros(1, 2, -3), MonthsDaysNanos(1, 2, -3000));
  ExpectEqual(MonthsDaysMicros(10, -301, 9), MonthsDaysNanos(0, -1, 9000));

  ExpectLess(MonthsDaysNanos(2, -30, 0), MonthsDaysNanos(1, 0, 1));
  ExpectLess(MonthsDaysNanos(2, 0, -1), MonthsDaysNanos(0, 60, 1));
  ExpectLess(MonthsDaysNanos(5, 15, 9999999), MonthsDaysNanos(5, 15, 10000000));
}

TEST(IntervalValueTest, ToString) {
  EXPECT_EQ("0-0 0 0:0:0", Months(0).ToString());
  EXPECT_EQ("0-1 0 0:0:0", Months(1).ToString());
  EXPECT_EQ("-0-1 0 0:0:0", Months(-1).ToString());
  EXPECT_EQ("0-11 0 0:0:0", Months(11).ToString());
  EXPECT_EQ("-0-11 0 0:0:0", Months(-11).ToString());
  EXPECT_EQ("1-0 0 0:0:0", Months(12).ToString());
  EXPECT_EQ("-1-0 0 0:0:0", Months(-12).ToString());
  EXPECT_EQ("1-1 0 0:0:0", Months(13).ToString());
  EXPECT_EQ("-1-1 0 0:0:0", Months(-13).ToString());
  EXPECT_EQ("1-8 0 0:0:0", Months(20).ToString());
  EXPECT_EQ("-1-8 0 0:0:0", Months(-20).ToString());
  EXPECT_EQ("200-0 0 0:0:0", Months(2400).ToString());
  EXPECT_EQ("-200-0 0 0:0:0", Months(-2400).ToString());
  EXPECT_EQ("200-11 0 0:0:0", Months(2411).ToString());
  EXPECT_EQ("-200-11 0 0:0:0", Months(-2411).ToString());
  EXPECT_EQ("1028-9 0 0:0:0", Months(12345).ToString());
  EXPECT_EQ("-1028-9 0 0:0:0", Months(-12345).ToString());

  EXPECT_EQ("0-0 0 0:0:0", Days(0).ToString());
  EXPECT_EQ("0-0 1 0:0:0", Days(1).ToString());
  EXPECT_EQ("0-0 -1 0:0:0", Days(-1).ToString());
  EXPECT_EQ("0-0 30 0:0:0", Days(30).ToString());
  EXPECT_EQ("0-0 -30 0:0:0", Days(-30).ToString());
  EXPECT_EQ("0-0 100000 0:0:0", Days(100000).ToString());
  EXPECT_EQ("0-0 -100000 0:0:0", Days(-100000).ToString());

  EXPECT_EQ("0-0 0 0:0:0", Nanos(0).ToString());
  EXPECT_EQ("0-0 0 0:0:0.000000001", Nanos(1).ToString());
  EXPECT_EQ("0-0 0 -0:0:0.000000001", Nanos(-1).ToString());
  EXPECT_EQ("0-0 0 0:0:0.000000020", Nanos(20).ToString());
  EXPECT_EQ("0-0 0 -0:0:0.000000020", Nanos(-20).ToString());
  EXPECT_EQ("0-0 0 0:0:0.000000021", Nanos(21).ToString());
  EXPECT_EQ("0-0 0 -0:0:0.000000021", Nanos(-21).ToString());
  EXPECT_EQ("0-0 0 0:0:0.000000321", Nanos(321).ToString());
  EXPECT_EQ("0-0 0 -0:0:0.000000321", Nanos(-321).ToString());
  EXPECT_EQ("0-0 0 0:0:0.000000999", Nanos(999).ToString());
  EXPECT_EQ("0-0 0 -0:0:0.000000999", Nanos(-999).ToString());
  EXPECT_EQ("0-0 0 0:0:0.000001", Nanos(1000).ToString());
  EXPECT_EQ("0-0 0 -0:0:0.000001", Nanos(-1000).ToString());
  EXPECT_EQ("0-0 0 0:0:0.000001001", Nanos(1001).ToString());
  EXPECT_EQ("0-0 0 -0:0:0.000001001", Nanos(-1001).ToString());

  EXPECT_EQ("0-0 0 0:0:0.000001", Micros(1).ToString());
  EXPECT_EQ("0-0 0 -0:0:0.000001", Micros(-1).ToString());
  EXPECT_EQ("0-0 0 0:0:0.000100", Micros(100).ToString());
  EXPECT_EQ("0-0 0 -0:0:0.000100", Micros(-100).ToString());
  EXPECT_EQ("0-0 0 0:0:0.001", Micros(1000).ToString());
  EXPECT_EQ("0-0 0 -0:0:0.001", Micros(-1000).ToString());
  EXPECT_EQ("0-0 0 0:0:1", Micros(IntervalValue::kMicrosInSecond).ToString());
  EXPECT_EQ("0-0 0 -0:0:1", Micros(-IntervalValue::kMicrosInSecond).ToString());
  EXPECT_EQ("0-0 0 0:1:0", Micros(IntervalValue::kMicrosInMinute).ToString());
  EXPECT_EQ("0-0 0 -0:1:0", Micros(-IntervalValue::kMicrosInMinute).ToString());
  int64_t micros_12 =
      IntervalValue::kMicrosInMinute + 2 * IntervalValue::kMicrosInSecond;
  EXPECT_EQ("0-0 0 0:1:2", Micros(micros_12).ToString());
  EXPECT_EQ("0-0 0 -0:1:2", Micros(-micros_12).ToString());
  EXPECT_EQ("0-0 0 1:0:0", Micros(IntervalValue::kMicrosInHour).ToString());
  EXPECT_EQ("0-0 0 -1:0:0", Micros(-IntervalValue::kMicrosInHour).ToString());
  int64_t micros_123 = IntervalValue::kMicrosInHour +
                     2 * IntervalValue::kMicrosInMinute +
                     3 * IntervalValue::kMicrosInSecond;
  EXPECT_EQ("0-0 0 1:2:3", Micros(micros_123).ToString());
  EXPECT_EQ("0-0 0 -1:2:3", Micros(-micros_123).ToString());
  int64_t micros_123456 =
      IntervalValue::kMicrosInHour + 2 * IntervalValue::kMicrosInMinute +
      3 * IntervalValue::kMicrosInSecond + 456 * IntervalValue::kMicrosInMilli;
  EXPECT_EQ("0-0 0 1:2:3.456", Micros(micros_123456).ToString());
  EXPECT_EQ("0-0 0 -1:2:3.456", Micros(-micros_123456).ToString());
  EXPECT_EQ("0-0 0 100:0:0",
            Micros(100 * IntervalValue::kMicrosInHour).ToString());
  EXPECT_EQ("0-0 0 -100:0:0",
            Micros(-100 * IntervalValue::kMicrosInHour).ToString());
  EXPECT_EQ("0-0 0 10:1:0",
            Micros(601 * IntervalValue::kMicrosInMinute).ToString());
  EXPECT_EQ("0-0 0 -10:1:0",
            Micros(-601 * IntervalValue::kMicrosInMinute).ToString());

  __int128 v_nanos = 89 * IntervalValue::kNanosInHour +
                     12 * IntervalValue::kNanosInMinute +
                     34 * IntervalValue::kNanosInSecond + 56789;
  EXPECT_EQ("10-3 4567 89:12:34.000056789",
            MonthsDaysNanos(123, 4567, v_nanos).ToString());
  EXPECT_EQ("-10-3 -4567 -89:12:34.000056789",
            MonthsDaysNanos(-123, -4567, -v_nanos).ToString());
  EXPECT_EQ("10-3 -4567 89:12:34.000056789",
            MonthsDaysNanos(123, -4567, v_nanos).ToString());
  EXPECT_EQ("-10-3 4567 -89:12:34.000056789",
            MonthsDaysNanos(-123, 4567, -v_nanos).ToString());
}

std::string ParseToString(absl::string_view input,
                          functions::DateTimestampPart part) {
  return IntervalValue::ParseFromString(input, part).ValueOrDie().ToString();
}

void ExpectParseError(absl::string_view input,
                      functions::DateTimestampPart part) {
  EXPECT_THAT(IntervalValue::ParseFromString(input, part),
              StatusIs(absl::StatusCode::kOutOfRange));
}

using functions::DAY;
using functions::HOUR;
using functions::MINUTE;
using functions::MONTH;
using functions::QUARTER;
using functions::SECOND;
using functions::WEEK;
using functions::YEAR;

TEST(IntervalValueTest, ParseFromString) {
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0", YEAR));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("-0", YEAR));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("+0", YEAR));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("000", YEAR));
  EXPECT_EQ("9-0 0 0:0:0", ParseToString("009", YEAR));
  EXPECT_EQ("-9-0 0 0:0:0", ParseToString("-009", YEAR));
  EXPECT_EQ("123-0 0 0:0:0", ParseToString("123", YEAR));
  EXPECT_EQ("-123-0 0 0:0:0", ParseToString("-123", YEAR));
  EXPECT_EQ("123-0 0 0:0:0", ParseToString("+123", YEAR));
  EXPECT_EQ("10000-0 0 0:0:0", ParseToString("10000", YEAR));
  EXPECT_EQ("-10000-0 0 0:0:0", ParseToString("-10000", YEAR));

  // reject spaces
  ExpectParseError("", YEAR);
  ExpectParseError(" 1", YEAR);
  ExpectParseError("-1 ", YEAR);
  ExpectParseError("- 1", YEAR);
  ExpectParseError("\t1", YEAR);
  ExpectParseError("1\t", YEAR);
  ExpectParseError("\n1", YEAR);
  ExpectParseError("1\n", YEAR);
  // invalid formatting
  ExpectParseError("--1", YEAR);
  ExpectParseError("1.0", YEAR);
  ExpectParseError("123 0", YEAR);
  // exceeds max number of months
  ExpectParseError("10001", YEAR);
  ExpectParseError("-10001", YEAR);
  // overflow during multiplication
  ExpectParseError("9223372036854775807", YEAR);
  ExpectParseError("-9223372036854775808", YEAR);
  // overflow fitting into int64_t at SimpleAtoi
  ExpectParseError("9223372036854775808", YEAR);
  ExpectParseError("-9223372036854775809", YEAR);

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0", QUARTER));
  EXPECT_EQ("0-9 0 0:0:0", ParseToString("3", QUARTER));
  EXPECT_EQ("-0-9 0 0:0:0", ParseToString("-3", QUARTER));
  EXPECT_EQ("2-6 0 0:0:0", ParseToString("10", QUARTER));
  EXPECT_EQ("-2-6 0 0:0:0", ParseToString("-10", QUARTER));
  EXPECT_EQ("10000-0 0 0:0:0", ParseToString("40000", QUARTER));
  EXPECT_EQ("-10000-0 0 0:0:0", ParseToString("-40000", QUARTER));

  // exceeds max number of months
  ExpectParseError("40001", QUARTER);
  ExpectParseError("-40001", QUARTER);
  // overflow during multiplication
  ExpectParseError("9223372036854775807", QUARTER);
  ExpectParseError("-9223372036854775808", QUARTER);
  // overflow fitting into int64_t at SimpleAtoi
  ExpectParseError("9223372036854775808", QUARTER);
  ExpectParseError("-9223372036854775809", QUARTER);

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0", MONTH));
  EXPECT_EQ("0-6 0 0:0:0", ParseToString("6", MONTH));
  EXPECT_EQ("-0-6 0 0:0:0", ParseToString("-6", MONTH));
  EXPECT_EQ("40-5 0 0:0:0", ParseToString("485", MONTH));
  EXPECT_EQ("-40-5 0 0:0:0", ParseToString("-485", MONTH));
  EXPECT_EQ("10000-0 0 0:0:0", ParseToString("120000", MONTH));
  EXPECT_EQ("-10000-0 0 0:0:0", ParseToString("-120000", MONTH));

  // exceeds max number of months
  ExpectParseError("120001", MONTH);
  ExpectParseError("-120001", MONTH);
  // overflow fitting into int64_t at SimpleAtoi
  ExpectParseError("9223372036854775808", MONTH);
  ExpectParseError("-9223372036854775809", MONTH);

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0", WEEK));
  EXPECT_EQ("0-0 7 0:0:0", ParseToString("1", WEEK));
  EXPECT_EQ("0-0 -7 0:0:0", ParseToString("-1", WEEK));
  EXPECT_EQ("0-0 140 0:0:0", ParseToString("20", WEEK));
  EXPECT_EQ("0-0 -140 0:0:0", ParseToString("-20", WEEK));
  EXPECT_EQ("0-0 3659999 0:0:0", ParseToString("522857", WEEK));
  EXPECT_EQ("0-0 -3659999 0:0:0", ParseToString("-522857", WEEK));

  // exceeds max number of days
  ExpectParseError("522858", WEEK);
  ExpectParseError("-522858", WEEK);
  // overflow during multiplication
  ExpectParseError("9223372036854775807", WEEK);
  ExpectParseError("-9223372036854775808", WEEK);
  // overflow fitting into int64_t at SimpleAtoi
  ExpectParseError("9223372036854775808", WEEK);
  ExpectParseError("-9223372036854775809", WEEK);

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0", DAY));
  EXPECT_EQ("0-0 371 0:0:0", ParseToString("371", DAY));
  EXPECT_EQ("0-0 -371 0:0:0", ParseToString("-371", DAY));
  EXPECT_EQ("0-0 3660000 0:0:0", ParseToString("3660000", DAY));
  EXPECT_EQ("0-0 -3660000 0:0:0", ParseToString("-3660000", DAY));

  // exceeds max number of days
  ExpectParseError("3660001", DAY);
  ExpectParseError("-3660001", DAY);

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0", HOUR));
  EXPECT_EQ("0-0 0 25:0:0", ParseToString("25", HOUR));
  EXPECT_EQ("0-0 0 -25:0:0", ParseToString("-25", HOUR));
  EXPECT_EQ("0-0 0 87840000:0:0", ParseToString("87840000", HOUR));
  EXPECT_EQ("0-0 0 -87840000:0:0", ParseToString("-87840000", HOUR));

  // exceeds max number of micros
  ExpectParseError("87840001", HOUR);
  ExpectParseError("-87840001", HOUR);

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0", MINUTE));
  EXPECT_EQ("0-0 0 0:3:0", ParseToString("3", MINUTE));
  EXPECT_EQ("0-0 0 -0:3:0", ParseToString("-3", MINUTE));
  EXPECT_EQ("0-0 0 1:12:0", ParseToString("72", MINUTE));
  EXPECT_EQ("0-0 0 -1:12:0", ParseToString("-72", MINUTE));
  EXPECT_EQ("0-0 0 87840000:0:0", ParseToString("5270400000", MINUTE));
  EXPECT_EQ("0-0 0 -87840000:0:0", ParseToString("-5270400000", MINUTE));

  // exceeds max number of micros
  ExpectParseError("5270400001", MINUTE);
  ExpectParseError("-5270400001", MINUTE);

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0", SECOND));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("-0", SECOND));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("+0", SECOND));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0.0", SECOND));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("-0.0", SECOND));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("+0.0", SECOND));
  EXPECT_EQ("0-0 0 0:0:1", ParseToString("1", SECOND));
  EXPECT_EQ("0-0 0 -0:0:1", ParseToString("-1", SECOND));
  EXPECT_EQ("0-0 0 0:0:0.100", ParseToString("0.1", SECOND));
  EXPECT_EQ("0-0 0 -0:0:0.100", ParseToString("-0.1", SECOND));
  EXPECT_EQ("0-0 0 0:0:0.120", ParseToString("+.12", SECOND));
  EXPECT_EQ("0-0 0 0:0:0.120", ParseToString(".12", SECOND));
  EXPECT_EQ("0-0 0 -0:0:0.120", ParseToString("-.12", SECOND));
  EXPECT_EQ("0-0 0 0:0:0.100", ParseToString("+0.1", SECOND));
  EXPECT_EQ("0-0 0 0:0:1.200", ParseToString("1.2", SECOND));
  EXPECT_EQ("0-0 0 -0:0:1.200", ParseToString("-1.2", SECOND));
  EXPECT_EQ("0-0 0 0:0:1.230", ParseToString("1.23", SECOND));
  EXPECT_EQ("0-0 0 -0:0:1.230", ParseToString("-1.23", SECOND));
  EXPECT_EQ("0-0 0 0:0:1.234", ParseToString("1.23400", SECOND));
  EXPECT_EQ("0-0 0 -0:0:1.234", ParseToString("-1.23400", SECOND));
  EXPECT_EQ("0-0 0 0:0:1.234560", ParseToString("1.23456", SECOND));
  EXPECT_EQ("0-0 0 -0:0:1.234560", ParseToString("-1.23456", SECOND));
  EXPECT_EQ("0-0 0 0:0:0.123456789", ParseToString("0.123456789", SECOND));
  EXPECT_EQ("0-0 0 -0:0:0.123456789", ParseToString("-0.123456789", SECOND));
  EXPECT_EQ("0-0 0 27777777:46:39", ParseToString("99999999999", SECOND));
  EXPECT_EQ("0-0 0 -27777777:46:39", ParseToString("-99999999999", SECOND));
  EXPECT_EQ("0-0 0 87840000:0:0", ParseToString("316224000000", SECOND));
  EXPECT_EQ("0-0 0 -87840000:0:0", ParseToString("-316224000000", SECOND));

  ExpectParseError("", SECOND);
  ExpectParseError(" 1", SECOND);
  ExpectParseError("1 ", SECOND);
  ExpectParseError(" 1.1", SECOND);
  ExpectParseError("1.1 ", SECOND);
  ExpectParseError(".", SECOND);
  ExpectParseError("1. 2", SECOND);
  ExpectParseError("1.", SECOND);
  ExpectParseError("-1.", SECOND);
  ExpectParseError("+1.", SECOND);
  ExpectParseError("\t1.1", SECOND);
  ExpectParseError("1.1\t", SECOND);
  ExpectParseError("\n1.1", SECOND);
  ExpectParseError("1.1\n", SECOND);
  // more than 9 fractional digits
  ExpectParseError("0.1234567890", SECOND);
  // exceeds max number of seconds
  ExpectParseError("316224000000.000001", SECOND);
  ExpectParseError("-316224000000.000001", SECOND);
  // overflow fitting into int64_t at SimpleAtoi
  ExpectParseError("9223372036854775808", SECOND);
  ExpectParseError("-9223372036854775809", SECOND);

  // Unsupported dateparts
  ExpectParseError("0", functions::DAYOFWEEK);
  ExpectParseError("0", functions::DAYOFYEAR);
  ExpectParseError("0", functions::MILLISECOND);
  ExpectParseError("0", functions::MICROSECOND);
  ExpectParseError("0", functions::NANOSECOND);
  ExpectParseError("0", functions::DATE);
  ExpectParseError("0", functions::DATETIME);
  ExpectParseError("0", functions::TIME);
  ExpectParseError("0", functions::ISOYEAR);
  ExpectParseError("0", functions::ISOWEEK);
  ExpectParseError("0", functions::WEEK_MONDAY);
  ExpectParseError("0", functions::WEEK_TUESDAY);
  ExpectParseError("0", functions::WEEK_WEDNESDAY);
  ExpectParseError("0", functions::WEEK_THURSDAY);
  ExpectParseError("0", functions::WEEK_FRIDAY);
  ExpectParseError("0", functions::WEEK_SATURDAY);
}

}  // namespace
}  // namespace zetasql
