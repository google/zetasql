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

#include <cstdint>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/interval_value_test_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/hash/hash_testing.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"

namespace zetasql {
namespace {

using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

using interval_testing::Days;
using interval_testing::Hours;
using interval_testing::Micros;
using interval_testing::Minutes;
using interval_testing::Months;
using interval_testing::MonthsDaysMicros;
using interval_testing::MonthsDaysNanos;
using interval_testing::Nanos;
using interval_testing::Seconds;
using interval_testing::Years;
using interval_testing::YMDHMS;

using functions::DAY;
using functions::HOUR;
using functions::MICROSECOND;
using functions::MILLISECOND;
using functions::MINUTE;
using functions::MONTH;
using functions::NANOSECOND;
using functions::QUARTER;
using functions::SECOND;
using functions::WEEK;
using functions::YEAR;

IntervalValue Interval(absl::string_view str) {
  return *IntervalValue::Parse(str);
}

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
  for (__int128 value : values) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(interval, IntervalValue::FromNanos(value));
    EXPECT_EQ(value, interval.get_nanos());
    EXPECT_EQ(value, interval.GetAsNanos());
    EXPECT_GE(interval.get_nano_fractions(), 0);
  }
  EXPECT_THAT(IntervalValue::FromNanos(IntervalValue::kMaxNanos + 1),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(IntervalValue::FromNanos(IntervalValue::kMinNanos - 1),
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

TEST(IntervalValueTest, FromYMDHMS) {
  EXPECT_EQ("0-0 0 0:0:0", YMDHMS(0, 0, 0, 0, 0, 0).ToString());
  EXPECT_EQ("-1-0 0 0:0:0", YMDHMS(-1, 0, 0, 0, 0, 0).ToString());
  EXPECT_EQ("-0-2 0 0:0:0", YMDHMS(0, -2, 0, 0, 0, 0).ToString());
  EXPECT_EQ("0-0 0 0:0:0", YMDHMS(1, -12, 0, 0, 0, 0).ToString());
  EXPECT_EQ("2-0 0 0:0:0", YMDHMS(3, -12, 0, 0, 0, 0).ToString());
  EXPECT_EQ("0-0 100 0:0:0", YMDHMS(0, 0, 100, 0, 0, 0).ToString());
  EXPECT_EQ("0-0 -5 0:0:0", YMDHMS(0, 0, -5, 0, 0, 0).ToString());
  EXPECT_EQ("0-0 0 24:0:0", YMDHMS(0, 0, 0, 24, 0, 0).ToString());
  EXPECT_EQ("0-0 0 22:0:0", YMDHMS(0, 0, 0, 24, -120, 0).ToString());
  EXPECT_EQ("0-0 0 -2:0:0", YMDHMS(0, 0, 0, 0, -120, 0).ToString());
  EXPECT_EQ("0-0 0 0:59:0", YMDHMS(0, 0, 0, 0, 59, 0).ToString());
  EXPECT_EQ("0-0 0 1:0:0", YMDHMS(0, 0, 0, 0, 59, 60).ToString());
  EXPECT_EQ("0-0 0 -10:20:30", YMDHMS(0, 0, 0, -10, -20, -30).ToString());
  EXPECT_EQ("0-0 0 -9:40:25", YMDHMS(0, 0, 0, -10, 20, -25).ToString());
  EXPECT_EQ("0-0 0 10:19:35", YMDHMS(0, 0, 0, 10, 20, -25).ToString());
  EXPECT_EQ("1-2 3 4:5:6", YMDHMS(1, 2, 3, 4, 5, 6).ToString());

  EXPECT_THAT(IntervalValue::FromYMDHMS(std::numeric_limits<int64_t>::max(), 0,
                                        0, 0, 0, 0),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      IntervalValue::FromYMDHMS(IntervalValue::kMaxYears, 1, 0, 0, 0, 0),
      StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      IntervalValue::FromYMDHMS(-1, IntervalValue::kMinMonths, 0, 0, 0, 0),
      StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      IntervalValue::FromYMDHMS(0, 0, IntervalValue::kMaxDays + 1, 0, 0, 0),
      StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      IntervalValue::FromYMDHMS(0, 0, 0, IntervalValue::kMaxHours, 0, 1),
      StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      IntervalValue::FromYMDHMS(0, 0, 0, 0, IntervalValue::kMaxMinutes, 1),
      StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      IntervalValue::FromYMDHMS(0, 0, 0, 0, 0, IntervalValue::kMaxSeconds + 1),
      StatusIs(absl::StatusCode::kOutOfRange));
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
    // Too many bytes
    const char bytes[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    EXPECT_THAT(
        IntervalValue::DeserializeFromBytes(absl::string_view(bytes, 17)),
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

TEST(IntervalValueTest, UnaryMinus) {
  EXPECT_EQ(Days(0), -Days(0));
  EXPECT_EQ(Nanos(IntervalValue::kMaxNanos), -Nanos(-IntervalValue::kMaxNanos));
  EXPECT_EQ(Micros(-123456789), -Micros(123456789));
  EXPECT_EQ(Days(1), -Days(-1));
  EXPECT_EQ(-Years(10000), Years(-10000));
  EXPECT_EQ(YMDHMS(1, -2, 3, -4, 5, -6), -YMDHMS(-1, 2, -3, 4, -5, 6));

  absl::BitGen gen;
  for (int i = 0; i < 10000; i++) {
    int64_t months = absl::Uniform(gen, IntervalValue::kMinMonths,
                                   IntervalValue::kMaxMonths);
    int64_t days =
        absl::Uniform(gen, IntervalValue::kMinDays, IntervalValue::kMaxDays);
    int64_t micros = absl::Uniform(gen, IntervalValue::kMinMicros,
                                   IntervalValue::kMaxMicros);
    int64_t nano_fractions = absl::Uniform(gen, -999, 999);
    __int128 nanos = static_cast<__int128>(micros) * 1000 + nano_fractions;

    IntervalValue interval1 = MonthsDaysNanos(months, days, nanos);
    IntervalValue interval2 = MonthsDaysNanos(-months, -days, -nanos);
    EXPECT_EQ(interval1, -interval2);
    EXPECT_EQ(-interval1, interval2);
    EXPECT_EQ(-(-interval1), interval1);
  }
}

void ExpectPlus(const IntervalValue& op1, const IntervalValue& op2,
                const IntervalValue& result) {
  EXPECT_EQ(result, *(op1 + op2));
  EXPECT_EQ(result, *(op2 + op1));
  EXPECT_EQ(-result, *(-op1 - op2));
  EXPECT_EQ(-result, *(-op2 - op1));
  EXPECT_EQ(op1, *(result - op2));
  EXPECT_EQ(op2, *(result - op1));
  EXPECT_EQ(result, *(op2 - (-op1)));
  EXPECT_EQ(result, *(op1 - (-op2)));
}

void ExpectPlusFail(const IntervalValue& op1, const IntervalValue& op2) {
  EXPECT_THAT(op1 + op2, StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(op2 + op1, StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(op1 - (-op2), StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(op2 - (-op1), StatusIs(absl::StatusCode::kOutOfRange));
}

template <typename T>
void ExpectEqStatusOr(const absl::StatusOr<T>& expected,
                      const absl::StatusOr<T>& actual) {
  EXPECT_EQ(expected.status(), actual.status());
  if (expected.ok() && actual.ok()) {
    EXPECT_EQ(*expected, *actual);
  }
}

TEST(IntervalValueTest, BinaryPlusMinus) {
  ExpectPlus(Years(1), Years(2), Years(3));
  ExpectPlus(Months(1), Months(2), Months(3));
  ExpectPlus(Days(1), Days(2), Days(3));
  ExpectPlus(Hours(1), Hours(2), Hours(3));
  ExpectPlus(Minutes(1), Minutes(2), Minutes(3));
  ExpectPlus(Seconds(1), Seconds(2), Seconds(3));
  ExpectPlus(Micros(1), Micros(2), Micros(3));
  ExpectPlus(Nanos(1), Nanos(2), Nanos(3));
  ExpectPlus(Years(1), Months(2), Interval("1-2 0 0:0:0"));
  ExpectPlus(Years(1), Days(2), Interval("1-0 2 0:0:0"));
  ExpectPlus(Years(1), Hours(2), Interval("1-0 0 2:0:0"));
  ExpectPlus(Years(1), Minutes(2), Interval("1-0 0 0:2:0"));
  ExpectPlus(Years(1), Seconds(2), Interval("1-0 0 0:0:2"));
  ExpectPlus(Years(1), Micros(2), Interval("1-0 0 0:0:0.000002"));
  ExpectPlus(Years(1), Nanos(2), Interval("1-0 0 0:0:0.000000002"));

  ExpectPlus(Years(10000), Days(3660000), Interval("10000-0 3660000 0:0:0"));
  ExpectPlus(Years(10000), Hours(87840000), Interval("10000-0 0 87840000:0:0"));
  ExpectPlus(Days(3660000), Hours(87840000),
             Interval("0-0 3660000 87840000:0:0"));
  ExpectPlus(YMDHMS(1, 2, 3, 4, 5, 6), YMDHMS(1, 1, 1, 1, 1, 1),
             YMDHMS(2, 3, 4, 5, 6, 7));
  ExpectPlus(YMDHMS(1, -2, 3, -4, 5, -6), YMDHMS(-1, 2, -3, 4, -5, 6), Days(0));

  ExpectPlusFail(Years(10000), Months(1));
  ExpectPlusFail(Years(-10000), -Months(1));
  ExpectPlusFail(Days(3660000), Days(1));
  ExpectPlusFail(Days(-1), -Days(3660000));
  ExpectPlusFail(Hours(87840000), Micros(1));
  ExpectPlusFail(Hours(87840000), Nanos(1));
  ExpectPlusFail(-Hours(87840000), Micros(-1));
  ExpectPlusFail(-Hours(87840000), Nanos(-1));

  absl::BitGen gen;
  for (int i = 0; i < 10000; i++) {
    IntervalValue interval1 = interval_testing::GenerateRandomInterval(&gen);
    IntervalValue interval2 = interval_testing::GenerateRandomInterval(&gen);

    // Note that the result of + and - is StatusOr<IntervalValue>, and therefore
    // we verify that equivalent expressions either both fail with same error,
    // or both succeed and give same result.
    ExpectEqStatusOr(interval1 + interval2, interval2 + interval1);
    ExpectEqStatusOr(interval1 + interval2, interval1 - (-interval2));
    ExpectEqStatusOr(interval2 - interval1, -interval1 + interval2);
  }
}

TEST(IntervalValueTest, Multiply) {
  for (int64_t v : {0, 1, -1, 2, -2, 10, -10, 1000, -1000}) {
    EXPECT_EQ(Years(v), *(Years(1) * v)) << v;
    EXPECT_EQ(Months(v), *(Months(1) * v)) << v;
    EXPECT_EQ(Days(v), *(Days(1) * v)) << v;
    EXPECT_EQ(Hours(v), *(Hours(1) * v)) << v;
    EXPECT_EQ(Minutes(v), *(Minutes(1) * v)) << v;
    EXPECT_EQ(Seconds(v), *(Seconds(1) * v)) << v;
    EXPECT_EQ(Micros(v), *(Micros(1) * v)) << v;
    EXPECT_EQ(Nanos(v), *(Nanos(1) * v)) << v;
    EXPECT_EQ(YMDHMS(0, v, v, v, v, v), *(YMDHMS(0, 1, 1, 1, 1, 1) * v)) << v;

    // -interval is the same as interval * (-1)
    EXPECT_EQ(-Years(v), *(Years(v) * (-1))) << v;
    EXPECT_EQ(-Months(v), *(Months(v) * (-1))) << v;
    EXPECT_EQ(-Days(v), *(Days(v) * (-1))) << v;
    EXPECT_EQ(-Hours(v), *(Hours(v) * (-1))) << v;
    EXPECT_EQ(-Minutes(v), *(Minutes(v) * (-1))) << v;
    EXPECT_EQ(-Seconds(v), *(Seconds(v) * (-1))) << v;
    EXPECT_EQ(-Micros(v), *(Micros(v) * (-1))) << v;
    EXPECT_EQ(-Nanos(v), *(Nanos(v) * (-1))) << v;
  }

  EXPECT_THAT(Years(2) * 10000, StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(Days(3660000) * (-2), StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(Days(999999) * 99999999999,
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalValueTest, Divide) {
  EXPECT_EQ(Months(6), *(Years(1) / 2));
  EXPECT_EQ(Months(-6), *(Years(1) / (-2)));
  EXPECT_EQ(Days(15), *(Months(1) / 2));
  EXPECT_EQ(Days(-15), *(Months(1) / (-2)));
  EXPECT_EQ(Hours(12), *(Days(1) / 2));
  EXPECT_EQ(Hours(-12), *(Days(1) / (-2)));
  EXPECT_EQ(Minutes(30), *(Hours(1) / 2));
  EXPECT_EQ(Minutes(-30), *(Hours(1) / (-2)));
  EXPECT_EQ(Seconds(30), *(Minutes(1) / 2));
  EXPECT_EQ(Seconds(-30), *(Minutes(1) / (-2)));
  EXPECT_EQ(Micros(500000), *(Seconds(1) / 2));
  EXPECT_EQ(Micros(-500000), *(Seconds(1) / (-2)));
  EXPECT_EQ(Nanos(500), *(Micros(1) / 2));
  EXPECT_EQ(Nanos(-500), *(Micros(1) / (-2)));
  EXPECT_EQ(Years(0), *(Nanos(1) / 2));
  EXPECT_EQ(Years(0), *(Nanos(1) / (-2)));

  EXPECT_THAT(Years(0) / 0, StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(Micros(1) / 0, StatusIs(absl::StatusCode::kOutOfRange));

  // (interval / number) * number = interval +/- (1 nanosecond * number)
  absl::BitGen gen;
  for (int i = 0; i < 1000; i++) {
    // We divide random interval by 100 to prevent accidental out of range
    // errors during multiplication for dateparts which had spillover during
    // division.
    IntervalValue interval =
        *(interval_testing::GenerateRandomInterval(&gen) / 100);
    int64_t number = absl::Uniform<int64_t>(gen, 1, 99);
    IntervalValue roundtrip_interval = *(*(interval / number) * number);
    IntervalValue diff = *(interval - roundtrip_interval);
    EXPECT_TRUE(diff < Nanos(100) && diff > Nanos(-100))
        << "original: " << interval << " multiplier: " << number
        << " roundtrip: " << roundtrip_interval << " diff: " << diff;
  }
}

TEST(IntervalValueTest, Extract) {
  EXPECT_EQ(0, *Years(0).Extract(YEAR));
  EXPECT_EQ(0, *Days(10000).Extract(YEAR));
  EXPECT_EQ(0, *Hours(1000000).Extract(YEAR));
  EXPECT_EQ(0, *Nanos(1).Extract(YEAR));
  EXPECT_EQ(5, *Years(5).Extract(YEAR));
  EXPECT_EQ(-5, *Years(-5).Extract(YEAR));
  EXPECT_EQ(0, *Months(11).Extract(YEAR));
  EXPECT_EQ(0, *Months(-11).Extract(YEAR));
  EXPECT_EQ(1, *Months(12).Extract(YEAR));
  EXPECT_EQ(-1, *Months(-12).Extract(YEAR));
  EXPECT_EQ(1, *Months(13).Extract(YEAR));
  EXPECT_EQ(-1, *Months(-13).Extract(YEAR));
  EXPECT_EQ(10000, *Years(10000).Extract(YEAR));
  EXPECT_EQ(-10000, *Years(-10000).Extract(YEAR));
  EXPECT_EQ(9, *YMDHMS(10, -1, 5000, 1000, 20, 30).Extract(YEAR));
  EXPECT_EQ(-9, *YMDHMS(-10, 1, -5000, -1000, -20, -30).Extract(YEAR));

  EXPECT_EQ(0, *Hours(0).Extract(HOUR));
  EXPECT_EQ(1, *Hours(1).Extract(HOUR));
  EXPECT_EQ(-1, *Hours(-1).Extract(HOUR));
  EXPECT_EQ(25, *Hours(25).Extract(HOUR));
  EXPECT_EQ(-25, *Hours(-25).Extract(HOUR));
  EXPECT_EQ(0, *Minutes(59).Extract(HOUR));
  EXPECT_EQ(0, *Minutes(-59).Extract(HOUR));
  EXPECT_EQ(1, *Minutes(60).Extract(HOUR));
  EXPECT_EQ(-1, *Minutes(-60).Extract(HOUR));
  EXPECT_EQ(1, *Minutes(61).Extract(HOUR));
  EXPECT_EQ(-1, *Minutes(-61).Extract(HOUR));
  EXPECT_EQ(87840000, *Hours(87840000).Extract(HOUR));
  EXPECT_EQ(-87840000, *Hours(-87840000).Extract(HOUR));
  EXPECT_EQ(1000, *YMDHMS(10, -1, 5000, 1000, 20, 30).Extract(HOUR));
  EXPECT_EQ(-1000, *YMDHMS(-10, 1, -5000, -1000, -20, -30).Extract(HOUR));

  EXPECT_EQ(0, *Minutes(0).Extract(MINUTE));
  EXPECT_EQ(1, *Minutes(1).Extract(MINUTE));
  EXPECT_EQ(-1, *Minutes(-1).Extract(MINUTE));
  EXPECT_EQ(0, *Seconds(59).Extract(MINUTE));
  EXPECT_EQ(0, *Seconds(-59).Extract(MINUTE));
  EXPECT_EQ(1, *Seconds(60).Extract(MINUTE));
  EXPECT_EQ(-1, *Seconds(-60).Extract(MINUTE));
  EXPECT_EQ(1, *Seconds(61).Extract(MINUTE));
  EXPECT_EQ(-1, *Seconds(-61).Extract(MINUTE));
  EXPECT_EQ(0, *Micros(59999999).Extract(MINUTE));
  EXPECT_EQ(0, *Micros(-59999999).Extract(MINUTE));
  EXPECT_EQ(1, *Micros(60000001).Extract(MINUTE));
  EXPECT_EQ(-1, *Micros(-60000001).Extract(MINUTE));
  EXPECT_EQ(0, *Nanos(59999999999).Extract(MINUTE));
  EXPECT_EQ(0, *Nanos(-59999999999).Extract(MINUTE));
  EXPECT_EQ(1, *Nanos(60000000001).Extract(MINUTE));
  EXPECT_EQ(-1, *Nanos(-60000000001).Extract(MINUTE));
  EXPECT_EQ(58, *Seconds(3539).Extract(MINUTE));
  EXPECT_EQ(-58, *Seconds(-3539).Extract(MINUTE));
  EXPECT_EQ(59, *Seconds(3599).Extract(MINUTE));
  EXPECT_EQ(-59, *Seconds(-3599).Extract(MINUTE));
  EXPECT_EQ(0, *Seconds(3600).Extract(MINUTE));
  EXPECT_EQ(0, *Seconds(-3600).Extract(MINUTE));
  EXPECT_EQ(0, *Seconds(3601).Extract(MINUTE));
  EXPECT_EQ(0, *Seconds(-3601).Extract(MINUTE));
  EXPECT_EQ(1, *Seconds(3660).Extract(MINUTE));
  EXPECT_EQ(-1, *Seconds(-3660).Extract(MINUTE));
  EXPECT_EQ(1, *Seconds(3661).Extract(MINUTE));
  EXPECT_EQ(-1, *Seconds(-3661).Extract(MINUTE));
  EXPECT_EQ(0, *Minutes(5270400000).Extract(MINUTE));
  EXPECT_EQ(0, *Minutes(-5270400000).Extract(MINUTE));
  EXPECT_EQ(20, *YMDHMS(10, -1, 5000, 1000, 20, 30).Extract(MINUTE));
  EXPECT_EQ(-20, *YMDHMS(-10, 1, -5000, -1000, -20, -30).Extract(MINUTE));

  EXPECT_EQ(0, *Seconds(0).Extract(SECOND));
  EXPECT_EQ(1, *Seconds(1).Extract(SECOND));
  EXPECT_EQ(-1, *Seconds(-1).Extract(SECOND));
  EXPECT_EQ(0, *Micros(999999).Extract(SECOND));
  EXPECT_EQ(0, *Micros(-999999).Extract(SECOND));
  EXPECT_EQ(1, *Micros(1000000).Extract(SECOND));
  EXPECT_EQ(-1, *Micros(-1000000).Extract(SECOND));
  EXPECT_EQ(1, *Micros(1000001).Extract(SECOND));
  EXPECT_EQ(-1, *Micros(-1000001).Extract(SECOND));
  EXPECT_EQ(0, *Nanos(999999999).Extract(SECOND));
  EXPECT_EQ(0, *Nanos(-999999999).Extract(SECOND));
  EXPECT_EQ(1, *Nanos(1000000000).Extract(SECOND));
  EXPECT_EQ(-1, *Nanos(-1000000000).Extract(SECOND));
  EXPECT_EQ(1, *Nanos(1000000001).Extract(SECOND));
  EXPECT_EQ(-1, *Nanos(-1000000001).Extract(SECOND));

  EXPECT_EQ(0, *Seconds(111).Extract(MILLISECOND));
  EXPECT_EQ(0, *Micros(-999).Extract(MILLISECOND));
  EXPECT_EQ(0, *Micros(999).Extract(MILLISECOND));
  EXPECT_EQ(0, *Micros(-999).Extract(MILLISECOND));
  EXPECT_EQ(1, *Micros(1000).Extract(MILLISECOND));
  EXPECT_EQ(-1, *Micros(-1000).Extract(MILLISECOND));
  EXPECT_EQ(1, *Micros(1001).Extract(MILLISECOND));
  EXPECT_EQ(-1, *Micros(-1001).Extract(MILLISECOND));
  EXPECT_EQ(1, *Micros(1001).Extract(MILLISECOND));
  EXPECT_EQ(-1, *Micros(-1001).Extract(MILLISECOND));
  EXPECT_EQ(2, *Micros(1002003).Extract(MILLISECOND));
  EXPECT_EQ(-2, *Micros(-1002003).Extract(MILLISECOND));
  EXPECT_EQ(999, *Nanos(999999999).Extract(MILLISECOND));
  EXPECT_EQ(-999, *Nanos(-999999999).Extract(MILLISECOND));

  EXPECT_EQ(0, *Seconds(5).Extract(MICROSECOND));
  EXPECT_EQ(0, *Seconds(-3).Extract(MICROSECOND));
  EXPECT_EQ(0, *Nanos(999).Extract(MICROSECOND));
  EXPECT_EQ(0, *Nanos(-999).Extract(MICROSECOND));
  EXPECT_EQ(1, *Micros(1).Extract(MICROSECOND));
  EXPECT_EQ(-1, *Micros(-1).Extract(MICROSECOND));
  EXPECT_EQ(1000, *Micros(1000).Extract(MICROSECOND));
  EXPECT_EQ(-1000, *Micros(-1000).Extract(MICROSECOND));
  EXPECT_EQ(1001, *Micros(1001).Extract(MICROSECOND));
  EXPECT_EQ(-1001, *Micros(-1001).Extract(MICROSECOND));
  EXPECT_EQ(999999, *Nanos(999999999).Extract(MICROSECOND));
  EXPECT_EQ(-999999, *Nanos(-999999999).Extract(MICROSECOND));

  EXPECT_EQ(0, *Seconds(123).Extract(NANOSECOND));
  EXPECT_EQ(0, *Seconds(-123).Extract(NANOSECOND));
  EXPECT_EQ(1, *Nanos(1).Extract(NANOSECOND));
  EXPECT_EQ(-1, *Nanos(-1).Extract(NANOSECOND));
  EXPECT_EQ(999, *Nanos(999).Extract(NANOSECOND));
  EXPECT_EQ(-999, *Nanos(-999).Extract(NANOSECOND));
  EXPECT_EQ(1000, *Micros(1).Extract(NANOSECOND));
  EXPECT_EQ(-1000, *Micros(-1).Extract(NANOSECOND));
  EXPECT_EQ(1001, *Nanos(1001).Extract(NANOSECOND));
  EXPECT_EQ(-1001, *Nanos(-1001).Extract(NANOSECOND));
  EXPECT_EQ(999999999, *Nanos(999999999).Extract(NANOSECOND));
  EXPECT_EQ(-999999999, *Nanos(-999999999).Extract(NANOSECOND));

  EXPECT_EQ(1, *Interval("1-2 3 4:5:6.123456789").Extract(YEAR));
  EXPECT_EQ(2, *Interval("1-2 3 4:5:6.123456789").Extract(MONTH));
  EXPECT_EQ(3, *Interval("1-2 3 4:5:6.123456789").Extract(DAY));
  EXPECT_EQ(4, *Interval("1-2 3 4:5:6.123456789").Extract(HOUR));
  EXPECT_EQ(5, *Interval("1-2 3 4:5:6.123456789").Extract(MINUTE));
  EXPECT_EQ(6, *Interval("1-2 3 4:5:6.123456789").Extract(SECOND));
  EXPECT_EQ(123, *Interval("1-2 3 4:5:6.123456789").Extract(MILLISECOND));
  EXPECT_EQ(123456, *Interval("1-2 3 4:5:6.123456789").Extract(MICROSECOND));
  EXPECT_EQ(123456789, *Interval("1-2 3 4:5:6.123456789").Extract(NANOSECOND));

  EXPECT_EQ(-1, *Interval("-1-2 -3 -4:5:6.123456789").Extract(YEAR));
  EXPECT_EQ(-2, *Interval("-1-2 -3 -4:5:6.123456789").Extract(MONTH));
  EXPECT_EQ(-3, *Interval("-1-2 -3 -4:5:6.123456789").Extract(DAY));
  EXPECT_EQ(-4, *Interval("-1-2 -3 -4:5:6.123456789").Extract(HOUR));
  EXPECT_EQ(-5, *Interval("-1-2 -3 -4:5:6.123456789").Extract(MINUTE));
  EXPECT_EQ(-6, *Interval("-1-2 -3 -4:5:6.123456789").Extract(SECOND));
  EXPECT_EQ(-123, *Interval("-1-2 -3 -4:5:6.123456789").Extract(MILLISECOND));
  EXPECT_EQ(-123456,
            *Interval("-1-2 -3 -4:5:6.123456789").Extract(MICROSECOND));
  EXPECT_EQ(-123456789,
            *Interval("-1-2 -3 -4:5:6.123456789").Extract(NANOSECOND));

  EXPECT_THAT(Years(0).Extract(WEEK), StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(Years(0).Extract(QUARTER),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalValueTest, SumAggregatorSum) {
  IntervalValue::SumAggregator agg;
  // Max years
  agg.Add(Years(10000));
  // Adding other date parts doesn't cause overflow
  agg.Add(Days(100));
  agg.Add(Minutes(10));
  ZETASQL_ASSERT_OK(agg.GetSum());
  EXPECT_EQ(YMDHMS(10000, 0, 100, 0, 10, 0), *agg.GetSum());
  // But adding months causes overflow
  agg.Add(Months(1));
  EXPECT_THAT(agg.GetSum(), StatusIs(absl::StatusCode::kOutOfRange));
  // But aggregator is still valid, and allows additional Adds
  agg.Add(-Months(2));
  ZETASQL_ASSERT_OK(agg.GetSum());
  EXPECT_EQ(YMDHMS(9999, 11, 100, 0, 10, 0), *agg.GetSum());

  agg.Subtract(YMDHMS(9999, 11, 100, 0, 10, 0));
  EXPECT_EQ(Micros(0), *agg.GetSum());
}

TEST(IntervalValueTest, SumAggregatorAverage) {
  IntervalValue::SumAggregator agg;
  // Total number of years is 30,000 - more than maximum of 10,000
  agg.Add(Years(10000));
  agg.Add(Years(10000));
  agg.Add(Years(10000));
  // Sum, and Average of less than 3 elements overflow
  EXPECT_THAT(agg.GetSum(), StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(agg.GetAverage(1), StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(agg.GetAverage(2), StatusIs(absl::StatusCode::kOutOfRange));
  // But Average of 3 or more elements works
  EXPECT_EQ(Years(10000), *agg.GetAverage(3));
  EXPECT_EQ(Years(7500), *agg.GetAverage(4));
  EXPECT_EQ(Years(2500), *agg.GetAverage(12));
  EXPECT_EQ(Years(30), *agg.GetAverage(1000));
  EXPECT_EQ(Years(3), *agg.GetAverage(10000));
  EXPECT_EQ(Years(1), *agg.GetAverage(30000));
  // Spillover into other parts
  EXPECT_EQ(Interval("83-4"), *agg.GetAverage(360));
  EXPECT_EQ(Interval("8-4"), *agg.GetAverage(3600));
  EXPECT_EQ(Interval("0-10"), *agg.GetAverage(36000));
  EXPECT_EQ(Days(1), *agg.GetAverage(10800000));
  EXPECT_EQ(Hours(1), *agg.GetAverage(259200000));
  EXPECT_EQ(Minutes(1), *agg.GetAverage(15552000000));
  EXPECT_EQ(Interval("125-6 8 6:49:42.426778242"), *agg.GetAverage(239));
  EXPECT_EQ(Interval("2-5 4 20:21:17.278250303"), *agg.GetAverage(12345));
  EXPECT_EQ(Interval("2:5:58.272068780"), *agg.GetAverage(123456789));

  // Fractions of nanos are rounded
  IntervalValue::SumAggregator agg2;
  agg2.Add(Nanos(5));
  EXPECT_EQ(Nanos(5), *agg2.GetAverage(1));
  EXPECT_EQ(Nanos(2), *agg2.GetAverage(2));
  EXPECT_EQ(Nanos(1), *agg2.GetAverage(3));
  EXPECT_EQ(Nanos(1), *agg2.GetAverage(4));
  EXPECT_EQ(Nanos(1), *agg2.GetAverage(5));
  EXPECT_EQ(Nanos(0), *agg2.GetAverage(6));
  agg2.Add(Nanos(-10));
  EXPECT_EQ(Nanos(-5), *agg2.GetAverage(1));
  EXPECT_EQ(Nanos(-2), *agg2.GetAverage(2));
  EXPECT_EQ(Nanos(-1), *agg2.GetAverage(3));
  EXPECT_EQ(Nanos(-1), *agg2.GetAverage(4));
  EXPECT_EQ(Nanos(-1), *agg2.GetAverage(5));
  EXPECT_EQ(Nanos(0), *agg2.GetAverage(6));
}

TEST(IntervalValueTest, SumAggregatorDeserializeEdgeCases) {
  // Empty string produces empty aggregator
  EXPECT_THAT(IntervalValue::SumAggregator::DeserializeFromProtoBytes(""),
              IsOkAndHolds(::testing::Property(
                  &IntervalValue::SumAggregator::DebugString,
                  "IntervalValue::SumAggregator (months=0, days=0, nanos=0)")));

  // Input string too small to deserialize month
  EXPECT_THAT(IntervalValue::SumAggregator::DeserializeFromProtoBytes("abc"),
              StatusIs(absl::StatusCode::kOutOfRange));

  // Input string too small to deserialize month and day
  EXPECT_THAT(IntervalValue::SumAggregator::DeserializeFromProtoBytes(
                  "1234567890123456abc"),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      IntervalValue::SumAggregator::DeserializeFromProtoBytes("1234567890"),
      StatusIs(absl::StatusCode::kOutOfRange));

  // 32-byte input string, just barely long enough for month and day.
  EXPECT_THAT(IntervalValue::SumAggregator::DeserializeFromProtoBytes(
                  "123456789012345678901234567890123456789012"),
              IsOkAndHolds(::testing::Property(
                  &IntervalValue::SumAggregator::DebugString,
                  "IntervalValue::SumAggregator (months=7.21E+37, "
                  "days=6.67E+37, nanos=237025689473491305575475)")));
  // 33-byte input string. Holds month and day, plus one extra byte for nanos.
  EXPECT_THAT(IntervalValue::SumAggregator::DeserializeFromProtoBytes(
                  "1234567890123456789012345678901234567890123"),
              IsOkAndHolds(::testing::Property(
                  &IntervalValue::SumAggregator::DebugString,
                  "IntervalValue::SumAggregator (months=7.21E+37, "
                  "days=6.67E+37, nanos=61892242489819579215590451)")));

  // 1K input string, holds room for month, day, and nanos, with other stuff
  // at the end.
  EXPECT_THAT(IntervalValue::SumAggregator::DeserializeFromProtoBytes(
                  std::string(1024, 'a')),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalValueTest, SumAggregatorSerializeDeserialize) {
  // Positive intervals
  IntervalValue::SumAggregator agg1;
  agg1.Add(Years(1));
  agg1.Add(Months(2));
  agg1.Add(Days(3));
  agg1.Add(Hours(4));
  agg1.Add(Minutes(5));
  agg1.Add(Seconds(6));
  agg1.Add(Nanos(7));

  // Negative intervals
  IntervalValue::SumAggregator agg2;
  agg1.Add(Years(-1));
  agg1.Add(Months(-2));
  agg1.Add(Days(-3));
  agg1.Add(Hours(-4));
  agg1.Add(Minutes(-5));
  agg1.Add(Seconds(-6));
  agg1.Add(Nanos(-7));

  // Intervals whose cumulative sum exceeds 10000 years
  IntervalValue::SumAggregator agg3;
  agg3.Add(Years(5000));
  agg3.Add(Years(5000));
  agg3.Add(Years(5000));

  for (const IntervalValue::SumAggregator* agg : {&agg1, &agg2, &agg3}) {
    SCOPED_TRACE(agg->DebugString());
    // Make sure serialization result is the same when appending
    std::string serialized = agg->SerializeAsProtoBytes();
    std::string serialized_via_append("test");
    agg->SerializeAndAppendToProtoBytes(&serialized_via_append);
    EXPECT_EQ(absl::StrCat("test", serialized), serialized_via_append);

    // Deserialize the serialized bytes and make sure the deserialized
    // aggregator holds the same state as the original.
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        IntervalValue::SumAggregator deserialized,
        IntervalValue::SumAggregator::DeserializeFromProtoBytes(serialized));

    EXPECT_EQ(deserialized.DebugString(), agg->DebugString());
  }
}

TEST(IntervalValueTest, SumAggregatorMerge) {
  IntervalValue::SumAggregator agg1;
  agg1.Add(Interval("1-2 3 4:5:6.123456789"));
  agg1.Add(Days(15));

  IntervalValue::SumAggregator agg2;
  agg2.Add(Days(5));
  agg2.Add(Seconds(30));

  agg1.MergeWith(agg2);
  EXPECT_THAT(agg1.GetSum(), IsOkAndHolds(Interval("1-2 23 4:5:36.123456789")));
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
  return IntervalValue::ParseFromString(input, part).value().ToString();
}

void ExpectParseError(absl::string_view input,
                      functions::DateTimestampPart part) {
  EXPECT_THAT(IntervalValue::ParseFromString(input, part),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalValueTest, ParseFromString1) {
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

std::string ParseToString(absl::string_view input,
                          functions::DateTimestampPart from,
                          functions::DateTimestampPart to) {
  return IntervalValue::ParseFromString(input, from, to).value().ToString();
}

void ExpectParseError(absl::string_view input,
                      functions::DateTimestampPart from,
                      functions::DateTimestampPart to) {
  EXPECT_THAT(IntervalValue::ParseFromString(input, from, to),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalValueTest, ParseFromString2) {
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0-0", YEAR, MONTH));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("-0-0", YEAR, MONTH));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("+0-0", YEAR, MONTH));
  EXPECT_EQ("1-0 0 0:0:0", ParseToString("1-0", YEAR, MONTH));
  EXPECT_EQ("1-0 0 0:0:0", ParseToString("+1-0", YEAR, MONTH));
  EXPECT_EQ("-1-0 0 0:0:0", ParseToString("-1-0", YEAR, MONTH));
  EXPECT_EQ("0-1 0 0:0:0", ParseToString("0-1", YEAR, MONTH));
  EXPECT_EQ("0-1 0 0:0:0", ParseToString("+0-1", YEAR, MONTH));
  EXPECT_EQ("-0-1 0 0:0:0", ParseToString("-0-1", YEAR, MONTH));
  EXPECT_EQ("1-0 0 0:0:0", ParseToString("0-12", YEAR, MONTH));
  EXPECT_EQ("-1-0 0 0:0:0", ParseToString("-0-12", YEAR, MONTH));
  EXPECT_EQ("1-8 0 0:0:0", ParseToString("0-20", YEAR, MONTH));
  EXPECT_EQ("-1-8 0 0:0:0", ParseToString("-0-20", YEAR, MONTH));
  EXPECT_EQ("10000-0 0 0:0:0", ParseToString("9999-12", YEAR, MONTH));
  EXPECT_EQ("-10000-0 0 0:0:0", ParseToString("-9999-12", YEAR, MONTH));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0-0 0", YEAR, DAY));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0-0 -0", YEAR, DAY));
  EXPECT_EQ("0-0 7 0:0:0", ParseToString("0-0 7", YEAR, DAY));
  EXPECT_EQ("0-0 -7 0:0:0", ParseToString("0-0 -7", YEAR, DAY));
  EXPECT_EQ("0-0 7 0:0:0", ParseToString("-0-0 +7", YEAR, DAY));
  EXPECT_EQ("11-8 30 0:0:0", ParseToString("10-20 30", YEAR, DAY));
  EXPECT_EQ("11-8 -30 0:0:0", ParseToString("10-20 -30", YEAR, DAY));
  EXPECT_EQ("-11-8 -30 0:0:0", ParseToString("-10-20 -30", YEAR, DAY));
  EXPECT_EQ("0-0 3660000 0:0:0", ParseToString("0-0 3660000", YEAR, DAY));
  EXPECT_EQ("0-0 -3660000 0:0:0", ParseToString("0-0 -3660000", YEAR, DAY));
  EXPECT_EQ("10000-0 3660000 0:0:0",
            ParseToString("10000-0 3660000", YEAR, DAY));
  EXPECT_EQ("-10000-0 -3660000 0:0:0",
            ParseToString("-10000-0 -3660000", YEAR, DAY));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0-0 0 0", YEAR, HOUR));
  EXPECT_EQ("0-0 0 24:0:0", ParseToString("0-0 0 24", YEAR, HOUR));
  EXPECT_EQ("0-0 0 -24:0:0", ParseToString("0-0 0 -24", YEAR, HOUR));
  EXPECT_EQ("0-0 0 24:0:0", ParseToString("0-0 0 +24", YEAR, HOUR));
  EXPECT_EQ("1-2 3 4:0:0", ParseToString("1-2 3 4", YEAR, HOUR));
  EXPECT_EQ("-1-2 -3 -4:0:0", ParseToString("-1-2 -3 -4", YEAR, HOUR));
  EXPECT_EQ("0-0 0 87840000:0:0", ParseToString("0-0 0 87840000", YEAR, HOUR));
  EXPECT_EQ("0-0 0 -87840000:0:0",
            ParseToString("0-0 0 -87840000", YEAR, HOUR));
  EXPECT_EQ("10000-0 3660000 87840000:0:0",
            ParseToString("10000-0 3660000 87840000", YEAR, HOUR));
  EXPECT_EQ("-10000-0 -3660000 -87840000:0:0",
            ParseToString("-10000-0 -3660000 -87840000", YEAR, HOUR));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0-0 0 0:0", YEAR, MINUTE));
  EXPECT_EQ("0-0 0 12:34:0", ParseToString("0-0 0 12:34", YEAR, MINUTE));
  EXPECT_EQ("0-0 0 -12:34:0", ParseToString("0-0 0 -12:34", YEAR, MINUTE));
  EXPECT_EQ("0-0 0 12:34:0", ParseToString("0-0 0 +12:34", YEAR, MINUTE));
  EXPECT_EQ("0-0 0 101:40:0", ParseToString("0-0 0 100:100", YEAR, MINUTE));
  EXPECT_EQ("0-0 0 -101:40:0", ParseToString("0-0 0 -100:100", YEAR, MINUTE));
  EXPECT_EQ("10-2 30 43:21:0", ParseToString("10-2 30 43:21", YEAR, MINUTE));
  EXPECT_EQ("10-2 30 -43:21:0", ParseToString("10-2 30 -43:21", YEAR, MINUTE));
  EXPECT_EQ("0-0 0 87840000:0:0",
            ParseToString("0-0 0 0:5270400000", YEAR, MINUTE));
  EXPECT_EQ("0-0 0 -87840000:0:0",
            ParseToString("0-0 0 -0:5270400000", YEAR, MINUTE));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0-0 0 0:0:0", YEAR, SECOND));
  EXPECT_EQ("0-0 0 0:0:9", ParseToString("0-0 0 0:0:9", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -0:0:9", ParseToString("0-0 0 -0:0:9", YEAR, SECOND));
  EXPECT_EQ("0-0 0 0:0:9", ParseToString("0-0 0 0:0:09", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -0:0:9", ParseToString("0-0 0 -0:0:09", YEAR, SECOND));
  EXPECT_EQ("0-0 0 0:0:59", ParseToString("0-0 0 0:0:59", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -0:0:59", ParseToString("0-0 0 -0:0:59", YEAR, SECOND));
  EXPECT_EQ("0-0 0 0:2:3", ParseToString("0-0 0 0:0:123", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -0:2:3", ParseToString("0-0 0 -0:0:123", YEAR, SECOND));
  EXPECT_EQ("0-0 0 1:2:3", ParseToString("0-0 0 1:2:3", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -1:2:3", ParseToString("0-0 0 -1:2:3", YEAR, SECOND));
  EXPECT_EQ("0-0 0 1:2:3", ParseToString("0-0 0 01:02:03", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -1:2:3", ParseToString("0-0 0 -01:02:03", YEAR, SECOND));
  EXPECT_EQ("0-0 0 12:34:56", ParseToString("0-0 0 12:34:56", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -12:34:56", ParseToString("0-0 0 -12:34:56", YEAR, SECOND));
  EXPECT_EQ("0-0 0 12:34:56", ParseToString("0-0 0 +12:34:56", YEAR, SECOND));
  EXPECT_EQ("0-0 0 101:41:40",
            ParseToString("0-0 0 100:100:100", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -101:41:40",
            ParseToString("0-0 0 -100:100:100", YEAR, SECOND));
  EXPECT_EQ("10-2 30 4:56:7", ParseToString("10-2 30 4:56:7", YEAR, SECOND));
  EXPECT_EQ("10-2 30 -4:56:7", ParseToString("10-2 30 -4:56:7", YEAR, SECOND));
  EXPECT_EQ("0-0 0 87840000:0:0",
            ParseToString("0-0 0 0:0:316224000000", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -87840000:0:0",
            ParseToString("0-0 0 -0:0:316224000000", YEAR, SECOND));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0-0 0 0:0:0.0", YEAR, SECOND));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0-0 0 -0:0:0.0000", YEAR, SECOND));
  EXPECT_EQ("0-0 0 0:0:0.100", ParseToString("0-0 0 0:0:0.1", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -0:0:0.100", ParseToString("0-0 0 -0:0:0.1", YEAR, SECOND));
  EXPECT_EQ("0-0 0 0:0:1.234500",
            ParseToString("0-0 0 0:0:1.2345", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -0:0:1.234500",
            ParseToString("0-0 0 -0:0:1.2345", YEAR, SECOND));
  EXPECT_EQ("0-0 0 0:1:2.345678",
            ParseToString("0-0 0 0:1:2.345678", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -0:1:2.345678",
            ParseToString("0-0 0 -0:1:2.345678", YEAR, SECOND));
  EXPECT_EQ("0-0 0 1:2:3.000456789",
            ParseToString("0-0 0 1:2:3.000456789", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -1:2:3.000456789",
            ParseToString("0-0 0 -1:2:3.000456789", YEAR, SECOND));
  EXPECT_EQ("10-2 30 4:56:7.891234500",
            ParseToString("10-2 30 4:56:7.8912345", YEAR, SECOND));
  EXPECT_EQ("10-2 30 -4:56:7.891234500",
            ParseToString("10-2 30 -4:56:7.8912345", YEAR, SECOND));
  EXPECT_EQ("0-0 0 87839999:59:1.999999999",
            ParseToString("0-0 0 0:0:316223999941.999999999", YEAR, SECOND));
  EXPECT_EQ("0-0 0 -87839999:59:1.999999999",
            ParseToString("0-0 0 -0:0:316223999941.999999999", YEAR, SECOND));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0 0", MONTH, DAY));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0 -0", MONTH, DAY));
  EXPECT_EQ("0-0 7 0:0:0", ParseToString("0 7", MONTH, DAY));
  EXPECT_EQ("0-0 -7 0:0:0", ParseToString("0 -7", MONTH, DAY));
  EXPECT_EQ("0-0 7 0:0:0", ParseToString("-0 +7", MONTH, DAY));
  EXPECT_EQ("11-8 30 0:0:0", ParseToString("140 30", MONTH, DAY));
  EXPECT_EQ("11-8 -30 0:0:0", ParseToString("140 -30", MONTH, DAY));
  EXPECT_EQ("-11-8 -30 0:0:0", ParseToString("-140 -30", MONTH, DAY));
  EXPECT_EQ("0-0 3660000 0:0:0", ParseToString("0 3660000", MONTH, DAY));
  EXPECT_EQ("0-0 -3660000 0:0:0", ParseToString("0 -3660000", MONTH, DAY));
  EXPECT_EQ("10000-0 3660000 0:0:0",
            ParseToString("120000 3660000", MONTH, DAY));
  EXPECT_EQ("-10000-0 -3660000 0:0:0",
            ParseToString("-120000 -3660000", MONTH, DAY));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0 0 0", MONTH, HOUR));
  EXPECT_EQ("0-0 0 24:0:0", ParseToString("0 0 24", MONTH, HOUR));
  EXPECT_EQ("0-0 0 -24:0:0", ParseToString("0 0 -24", MONTH, HOUR));
  EXPECT_EQ("0-0 0 24:0:0", ParseToString("0 0 +24", MONTH, HOUR));
  EXPECT_EQ("1-0 3 4:0:0", ParseToString("12 3 4", MONTH, HOUR));
  EXPECT_EQ("-1-0 -3 -4:0:0", ParseToString("-12 -3 -4", MONTH, HOUR));
  EXPECT_EQ("0-0 0 87840000:0:0", ParseToString("0 0 87840000", MONTH, HOUR));
  EXPECT_EQ("0-0 0 -87840000:0:0", ParseToString("0 0 -87840000", MONTH, HOUR));
  EXPECT_EQ("10000-0 3660000 87840000:0:0",
            ParseToString("120000 3660000 87840000", MONTH, HOUR));
  EXPECT_EQ("-10000-0 -3660000 -87840000:0:0",
            ParseToString("-120000 -3660000 -87840000", MONTH, HOUR));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0 0 0:0", MONTH, MINUTE));
  EXPECT_EQ("0-0 0 12:34:0", ParseToString("0 0 12:34", MONTH, MINUTE));
  EXPECT_EQ("0-0 0 -12:34:0", ParseToString("0 0 -12:34", MONTH, MINUTE));
  EXPECT_EQ("0-0 0 12:34:0", ParseToString("0 0 +12:34", MONTH, MINUTE));
  EXPECT_EQ("0-0 0 101:40:0", ParseToString("0 0 100:100", MONTH, MINUTE));
  EXPECT_EQ("0-0 0 -101:40:0", ParseToString("0 0 -100:100", MONTH, MINUTE));
  EXPECT_EQ("10-2 30 43:21:0", ParseToString("122 30 43:21", MONTH, MINUTE));
  EXPECT_EQ("10-2 30 -43:21:0", ParseToString("122 30 -43:21", MONTH, MINUTE));
  EXPECT_EQ("0-0 0 87840000:0:0",
            ParseToString("0 0 0:5270400000", MONTH, MINUTE));
  EXPECT_EQ("0-0 0 -87840000:0:0",
            ParseToString("0 0 -0:5270400000", MONTH, MINUTE));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0 0 0:0:0", MONTH, SECOND));
  EXPECT_EQ("0-0 0 0:0:9", ParseToString("0 0 0:0:9", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -0:0:9", ParseToString("0 0 -0:0:9", MONTH, SECOND));
  EXPECT_EQ("0-0 0 0:0:9", ParseToString("0 0 0:0:09", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -0:0:9", ParseToString("0 0 -0:0:09", MONTH, SECOND));
  EXPECT_EQ("0-0 0 0:0:59", ParseToString("0 0 0:0:59", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -0:0:59", ParseToString("0 0 -0:0:59", MONTH, SECOND));
  EXPECT_EQ("0-0 0 0:2:3", ParseToString("0 0 0:0:123", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -0:2:3", ParseToString("0 0 -0:0:123", MONTH, SECOND));
  EXPECT_EQ("0-0 0 1:2:3", ParseToString("0 0 1:2:3", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -1:2:3", ParseToString("0 0 -1:2:3", MONTH, SECOND));
  EXPECT_EQ("0-0 0 1:2:3", ParseToString("0 0 01:02:03", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -1:2:3", ParseToString("0 0 -01:02:03", MONTH, SECOND));
  EXPECT_EQ("0-0 0 12:34:56", ParseToString("0 0 12:34:56", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -12:34:56", ParseToString("0 0 -12:34:56", MONTH, SECOND));
  EXPECT_EQ("0-0 0 12:34:56", ParseToString("0 0 +12:34:56", MONTH, SECOND));
  EXPECT_EQ("0-0 0 101:41:40", ParseToString("0 0 100:100:100", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -101:41:40",
            ParseToString("0 0 -100:100:100", MONTH, SECOND));
  EXPECT_EQ("1-8 30 4:56:7", ParseToString("20 30 4:56:7", MONTH, SECOND));
  EXPECT_EQ("1-8 30 -4:56:7", ParseToString("20 30 -4:56:7", MONTH, SECOND));
  EXPECT_EQ("0-0 0 87840000:0:0",
            ParseToString("0 0 0:0:316224000000", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -87840000:0:0",
            ParseToString("0 0 -0:0:316224000000", MONTH, SECOND));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0 0 0:0:0.0", MONTH, SECOND));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0 0 -0:0:0.0000", MONTH, SECOND));
  EXPECT_EQ("0-0 0 0:0:0.100", ParseToString("0 0 0:0:0.1", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -0:0:0.100", ParseToString("0 0 -0:0:0.1", MONTH, SECOND));
  EXPECT_EQ("0-0 0 0:0:1.234500",
            ParseToString("0 0 0:0:1.2345", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -0:0:1.234500",
            ParseToString("0 0 -0:0:1.2345", MONTH, SECOND));
  EXPECT_EQ("0-0 0 0:1:2.345678",
            ParseToString("0 0 0:1:2.345678", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -0:1:2.345678",
            ParseToString("0 0 -0:1:2.345678", MONTH, SECOND));
  EXPECT_EQ("0-0 0 1:2:3.000456789",
            ParseToString("0 0 1:2:3.000456789", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -1:2:3.000456789",
            ParseToString("0 0 -1:2:3.000456789", MONTH, SECOND));
  EXPECT_EQ("1-8 30 4:56:7.891234500",
            ParseToString("20 30 4:56:7.8912345", MONTH, SECOND));
  EXPECT_EQ("1-8 30 -4:56:7.891234500",
            ParseToString("20 30 -4:56:7.8912345", MONTH, SECOND));
  EXPECT_EQ("0-0 0 87839999:59:1.999999999",
            ParseToString("0 0 0:0:316223999941.999999999", MONTH, SECOND));
  EXPECT_EQ("0-0 0 -87839999:59:1.999999999",
            ParseToString("0 0 -0:0:316223999941.999999999", MONTH, SECOND));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0 0", DAY, HOUR));
  EXPECT_EQ("0-0 0 24:0:0", ParseToString("0 24", DAY, HOUR));
  EXPECT_EQ("0-0 0 -24:0:0", ParseToString("0 -24", DAY, HOUR));
  EXPECT_EQ("0-0 0 24:0:0", ParseToString("0 +24", DAY, HOUR));
  EXPECT_EQ("0-0 3 4:0:0", ParseToString("3 4", DAY, HOUR));
  EXPECT_EQ("0-0 -3 -4:0:0", ParseToString("-3 -4", DAY, HOUR));
  EXPECT_EQ("0-0 0 87840000:0:0", ParseToString("0 87840000", DAY, HOUR));
  EXPECT_EQ("0-0 0 -87840000:0:0", ParseToString("0 -87840000", DAY, HOUR));
  EXPECT_EQ("0-0 3660000 87840000:0:0",
            ParseToString("3660000 87840000", DAY, HOUR));
  EXPECT_EQ("0-0 -3660000 -87840000:0:0",
            ParseToString("-3660000 -87840000", DAY, HOUR));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0 0:0", DAY, MINUTE));
  EXPECT_EQ("0-0 0 12:34:0", ParseToString("0 12:34", DAY, MINUTE));
  EXPECT_EQ("0-0 0 -12:34:0", ParseToString("0 -12:34", DAY, MINUTE));
  EXPECT_EQ("0-0 0 12:34:0", ParseToString("0 +12:34", DAY, MINUTE));
  EXPECT_EQ("0-0 0 101:40:0", ParseToString("0 100:100", DAY, MINUTE));
  EXPECT_EQ("0-0 0 -101:40:0", ParseToString("0 -100:100", DAY, MINUTE));
  EXPECT_EQ("0-0 30 43:21:0", ParseToString("30 43:21", DAY, MINUTE));
  EXPECT_EQ("0-0 30 -43:21:0", ParseToString("30 -43:21", DAY, MINUTE));
  EXPECT_EQ("0-0 0 87840000:0:0", ParseToString("0 0:5270400000", DAY, MINUTE));
  EXPECT_EQ("0-0 0 -87840000:0:0",
            ParseToString("0 -0:5270400000", DAY, MINUTE));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0 0:0:0", DAY, SECOND));
  EXPECT_EQ("0-0 0 0:0:9", ParseToString("0 0:0:9", DAY, SECOND));
  EXPECT_EQ("0-0 0 -0:0:9", ParseToString("0 -0:0:9", DAY, SECOND));
  EXPECT_EQ("0-0 0 0:0:9", ParseToString("0 0:0:09", DAY, SECOND));
  EXPECT_EQ("0-0 0 -0:0:9", ParseToString("0 -0:0:09", DAY, SECOND));
  EXPECT_EQ("0-0 0 0:0:59", ParseToString("0 0:0:59", DAY, SECOND));
  EXPECT_EQ("0-0 0 -0:0:59", ParseToString("0 -0:0:59", DAY, SECOND));
  EXPECT_EQ("0-0 0 0:2:3", ParseToString("0 0:0:123", DAY, SECOND));
  EXPECT_EQ("0-0 0 -0:2:3", ParseToString("0 -0:0:123", DAY, SECOND));
  EXPECT_EQ("0-0 0 1:2:3", ParseToString("0 1:2:3", DAY, SECOND));
  EXPECT_EQ("0-0 0 -1:2:3", ParseToString("0 -1:2:3", DAY, SECOND));
  EXPECT_EQ("0-0 0 1:2:3", ParseToString("0 01:02:03", DAY, SECOND));
  EXPECT_EQ("0-0 0 -1:2:3", ParseToString("0 -01:02:03", DAY, SECOND));
  EXPECT_EQ("0-0 0 12:34:56", ParseToString("0 12:34:56", DAY, SECOND));
  EXPECT_EQ("0-0 0 -12:34:56", ParseToString("0 -12:34:56", DAY, SECOND));
  EXPECT_EQ("0-0 0 12:34:56", ParseToString("0 +12:34:56", DAY, SECOND));
  EXPECT_EQ("0-0 0 101:41:40", ParseToString("0 100:100:100", DAY, SECOND));
  EXPECT_EQ("0-0 0 -101:41:40", ParseToString("0 -100:100:100", DAY, SECOND));
  EXPECT_EQ("0-0 30 4:56:7", ParseToString("30 4:56:7", DAY, SECOND));
  EXPECT_EQ("0-0 30 -4:56:7", ParseToString("30 -4:56:7", DAY, SECOND));
  EXPECT_EQ("0-0 0 87840000:0:0",
            ParseToString("0 0:0:316224000000", DAY, SECOND));
  EXPECT_EQ("0-0 0 -87840000:0:0",
            ParseToString("0 -0:0:316224000000", DAY, SECOND));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0 0:0:0.0", DAY, SECOND));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0 -0:0:0.0000", DAY, SECOND));
  EXPECT_EQ("0-0 0 0:0:0.100", ParseToString("0 0:0:0.1", DAY, SECOND));
  EXPECT_EQ("0-0 0 -0:0:0.100", ParseToString("0 -0:0:0.1", DAY, SECOND));
  EXPECT_EQ("0-0 0 0:0:1.234500", ParseToString("0 0:0:1.2345", DAY, SECOND));
  EXPECT_EQ("0-0 0 -0:0:1.234500", ParseToString("0 -0:0:1.2345", DAY, SECOND));
  EXPECT_EQ("0-0 0 0:1:2.345678", ParseToString("0 0:1:2.345678", DAY, SECOND));
  EXPECT_EQ("0-0 0 -0:1:2.345678",
            ParseToString("0 -0:1:2.345678", DAY, SECOND));
  EXPECT_EQ("0-0 0 1:2:3.000456789",
            ParseToString("0 1:2:3.000456789", DAY, SECOND));
  EXPECT_EQ("0-0 0 -1:2:3.000456789",
            ParseToString("0 -1:2:3.000456789", DAY, SECOND));
  EXPECT_EQ("0-0 30 4:56:7.891234500",
            ParseToString("30 4:56:7.8912345", DAY, SECOND));
  EXPECT_EQ("0-0 30 -4:56:7.891234500",
            ParseToString("30 -4:56:7.8912345", DAY, SECOND));
  EXPECT_EQ("0-0 0 87839999:59:1.999999999",
            ParseToString("0 0:0:316223999941.999999999", DAY, SECOND));
  EXPECT_EQ("0-0 0 -87839999:59:1.999999999",
            ParseToString("0 -0:0:316223999941.999999999", DAY, SECOND));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0:0", HOUR, MINUTE));
  EXPECT_EQ("0-0 0 12:34:0", ParseToString("12:34", HOUR, MINUTE));
  EXPECT_EQ("0-0 0 -12:34:0", ParseToString("-12:34", HOUR, MINUTE));
  EXPECT_EQ("0-0 0 12:34:0", ParseToString("+12:34", HOUR, MINUTE));
  EXPECT_EQ("0-0 0 101:40:0", ParseToString("100:100", HOUR, MINUTE));
  EXPECT_EQ("0-0 0 -101:40:0", ParseToString("-100:100", HOUR, MINUTE));
  EXPECT_EQ("0-0 0 87840000:0:0", ParseToString("0:5270400000", HOUR, MINUTE));
  EXPECT_EQ("0-0 0 -87840000:0:0",
            ParseToString("-0:5270400000", HOUR, MINUTE));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0:0:0", HOUR, SECOND));
  EXPECT_EQ("0-0 0 0:0:9", ParseToString("0:0:9", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -0:0:9", ParseToString("-0:0:9", HOUR, SECOND));
  EXPECT_EQ("0-0 0 0:0:9", ParseToString("0:0:09", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -0:0:9", ParseToString("-0:0:09", HOUR, SECOND));
  EXPECT_EQ("0-0 0 0:0:59", ParseToString("0:0:59", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -0:0:59", ParseToString("-0:0:59", HOUR, SECOND));
  EXPECT_EQ("0-0 0 0:2:3", ParseToString("0:0:123", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -0:2:3", ParseToString("-0:0:123", HOUR, SECOND));
  EXPECT_EQ("0-0 0 1:2:3", ParseToString("1:2:3", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -1:2:3", ParseToString("-1:2:3", HOUR, SECOND));
  EXPECT_EQ("0-0 0 1:2:3", ParseToString("01:02:03", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -1:2:3", ParseToString("-01:02:03", HOUR, SECOND));
  EXPECT_EQ("0-0 0 12:34:56", ParseToString("12:34:56", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -12:34:56", ParseToString("-12:34:56", HOUR, SECOND));
  EXPECT_EQ("0-0 0 12:34:56", ParseToString("+12:34:56", HOUR, SECOND));
  EXPECT_EQ("0-0 0 101:41:40", ParseToString("100:100:100", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -101:41:40", ParseToString("-100:100:100", HOUR, SECOND));
  EXPECT_EQ("0-0 0 87840000:0:0",
            ParseToString("0:0:316224000000", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -87840000:0:0",
            ParseToString("-0:0:316224000000", HOUR, SECOND));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0:0:0.0", HOUR, SECOND));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("-0:0:0.0000", HOUR, SECOND));
  EXPECT_EQ("0-0 0 0:0:0.100", ParseToString("0:0:0.1", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -0:0:0.100", ParseToString("-0:0:0.1", HOUR, SECOND));
  EXPECT_EQ("0-0 0 0:0:1.234500", ParseToString("0:0:1.2345", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -0:0:1.234500", ParseToString("-0:0:1.2345", HOUR, SECOND));
  EXPECT_EQ("0-0 0 0:1:2.345678", ParseToString("0:1:2.345678", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -0:1:2.345678",
            ParseToString("-0:1:2.345678", HOUR, SECOND));
  EXPECT_EQ("0-0 0 1:2:3.000456789",
            ParseToString("1:2:3.000456789", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -1:2:3.000456789",
            ParseToString("-1:2:3.000456789", HOUR, SECOND));
  EXPECT_EQ("0-0 0 87839999:59:1.999999999",
            ParseToString("0:0:316223999941.999999999", HOUR, SECOND));
  EXPECT_EQ("0-0 0 -87839999:59:1.999999999",
            ParseToString("-0:0:316223999941.999999999", HOUR, SECOND));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0:0", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 0:0:9", ParseToString("0:9", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -0:0:9", ParseToString("-0:9", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 0:0:9", ParseToString("0:09", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -0:0:9", ParseToString("-0:09", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 0:0:59", ParseToString("0:59", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -0:0:59", ParseToString("-0:59", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 0:2:3", ParseToString("0:123", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -0:2:3", ParseToString("-0:123", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 0:2:3", ParseToString("2:3", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -0:2:3", ParseToString("-2:3", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 0:2:3", ParseToString("02:03", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -0:2:3", ParseToString("-02:03", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 20:34:56", ParseToString("1234:56", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -20:34:56", ParseToString("-1234:56", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 20:34:56", ParseToString("+1234:56", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 87840000:0:0",
            ParseToString("0:316224000000", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -87840000:0:0",
            ParseToString("-0:316224000000", MINUTE, SECOND));

  EXPECT_EQ("0-0 0 0:0:0", ParseToString("0:0.0", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 0:0:0", ParseToString("-0:0.0000", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 0:0:0.100", ParseToString("0:0.1", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -0:0:0.100", ParseToString("-0:0.1", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 0:0:1.234500", ParseToString("0:1.2345", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -0:0:1.234500", ParseToString("-0:1.2345", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 0:1:2.345678", ParseToString("1:2.345678", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -0:1:2.345678",
            ParseToString("-1:2.345678", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 2:0:3.000456789",
            ParseToString("120:3.000456789", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -2:0:3.000456789",
            ParseToString("-120:3.000456789", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 87839999:59:1.999999999",
            ParseToString("0:316223999941.999999999", MINUTE, SECOND));
  EXPECT_EQ("0-0 0 -87839999:59:1.999999999",
            ParseToString("-0:316223999941.999999999", MINUTE, SECOND));

  std::array<functions::DateTimestampPart, 6> parts = {YEAR, MONTH,  DAY,
                                                       HOUR, MINUTE, SECOND};
  for (int i = 0; i < parts.size(); i++) {
    for (int j = i + 1; j < parts.size(); j++) {
      ExpectParseError("", parts[i], parts[j]);
      ExpectParseError(" ", parts[i], parts[j]);
      ExpectParseError("0", parts[i], parts[j]);
      ExpectParseError(".", parts[i], parts[j]);
    }
  }

  // Whitespace padding
  ExpectParseError(" 0-0", YEAR, MONTH);
  ExpectParseError("0-0 ", YEAR, MONTH);
  ExpectParseError("\t0-0", YEAR, MONTH);
  ExpectParseError("0-0\t", YEAR, MONTH);
  ExpectParseError("0- 0", YEAR, MONTH);
  ExpectParseError("- 0-0", YEAR, MONTH);

  // Exceeds maximum allowed value
  ExpectParseError("10001-0", YEAR, MONTH);
  ExpectParseError("-10001-0", YEAR, MONTH);
  ExpectParseError("0-120001", YEAR, MONTH);
  ExpectParseError("-0-120001", YEAR, MONTH);
  ExpectParseError("10000-1", YEAR, MONTH);
  ExpectParseError("-10000-1", YEAR, MONTH);
  ExpectParseError("0 3660001", MONTH, DAY);
  ExpectParseError("0 -3660001", MONTH, DAY);
  ExpectParseError("0 87840001:0:0", DAY, SECOND);
  ExpectParseError("0 -87840001:0:0", DAY, SECOND);
  ExpectParseError("0 0:5270400001:0", DAY, SECOND);
  ExpectParseError("0 -0:5270400001:0", DAY, SECOND);
  ExpectParseError("0 0:0:316224000001", DAY, SECOND);
  ExpectParseError("0 -0:0:316224000001", DAY, SECOND);
  ExpectParseError("0 0:0:316224000000.000000001", DAY, SECOND);
  ExpectParseError("0 -0:0:316224000000.000000001", DAY, SECOND);
  ExpectParseError("0 87840000:0:0.000000001", DAY, SECOND);
  ExpectParseError("0 -87840000:0:0.000000001", DAY, SECOND);

  // Numbers too large to fit into int64_t
  ExpectParseError("9223372036854775808-0", YEAR, MONTH);
  ExpectParseError("-9223372036854775808-0", YEAR, MONTH);
  ExpectParseError("0-9223372036854775808", YEAR, MONTH);
  ExpectParseError("-0-9223372036854775808", YEAR, MONTH);
  ExpectParseError("0 9223372036854775808", MONTH, DAY);
  ExpectParseError("0 -9223372036854775808", MONTH, DAY);
  ExpectParseError("0 9223372036854775808:0:0", DAY, SECOND);
  ExpectParseError("0 -9223372036854775808:0:0", DAY, SECOND);
  ExpectParseError("0 0:9223372036854775808:0", DAY, SECOND);
  ExpectParseError("0 -0:9223372036854775808:0", DAY, SECOND);
  ExpectParseError("0 0:0:9223372036854775808", DAY, SECOND);
  ExpectParseError("0 -0:0:9223372036854775808", DAY, SECOND);

  // Too many fractional digits
  ExpectParseError("0-0 0 0:0:0.0000000000", YEAR, SECOND);
  ExpectParseError("0 0 0:0:0.0000000000", MONTH, SECOND);
  ExpectParseError("0 0:0:0.0000000000", DAY, SECOND);
  ExpectParseError("0:0:0.0000000000", HOUR, SECOND);
  ExpectParseError("0:0.0000000000", MINUTE, SECOND);

  // Trailing dot
  ExpectParseError("0-0 0 0:0:0.", YEAR, SECOND);
  ExpectParseError("0 0 0:0:0.", MONTH, SECOND);
  ExpectParseError("0 0:0:0.", DAY, SECOND);
  ExpectParseError("0:0:0.", HOUR, SECOND);
  ExpectParseError("0:0.", MINUTE, SECOND);

  // Unsupported combinations of dateparts
  for (int i = 0; i < parts.size(); i++) {
    // same part cannot be used twice
    ExpectParseError("0", parts[i], parts[i]);
    for (int j = i + 1; j < parts.size(); j++) {
      // reverse order of parts
      ExpectParseError("0", parts[j], parts[i]);
    }
  }
}

void TestRoundtrip(const IntervalValue& interval) {
  std::string interval_text = interval.ToString();
  ZETASQL_ASSERT_OK_AND_ASSIGN(IntervalValue interval_reparsed,
                       IntervalValue::ParseFromString(
                           interval_text, functions::YEAR, functions::SECOND));
  EXPECT_EQ(interval, interval_reparsed);
  EXPECT_EQ(interval_text, interval_reparsed.ToString());
}

// Generate random interval, convert to string, parse back and verify that
// the result is the same as original interval.
TEST(IntervalValueTest, ParseFromStringYearToSecond) {
  absl::BitGen gen;

  int64_t months;
  int64_t days;
  int64_t seconds;
  int64_t micros;
  int64_t nano_fractions;
  __int128 nanos;

  for (int i = 0; i < 10000; i++) {
    months = absl::Uniform(gen, IntervalValue::kMinMonths,
                           IntervalValue::kMaxMonths);
    days = absl::Uniform(gen, IntervalValue::kMinDays, IntervalValue::kMaxDays);
    seconds = absl::Uniform(
        gen, IntervalValue::kMinMicros / IntervalValue::kMicrosInSecond,
        IntervalValue::kMaxMicros / IntervalValue::kMicrosInSecond);
    micros = absl::Uniform(gen, IntervalValue::kMinMicros,
                           IntervalValue::kMaxMicros);
    nano_fractions = absl::Uniform(gen, -999, 999);
    IntervalValue interval;
    // Seconds granularity
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        interval, IntervalValue::FromMonthsDaysMicros(
                      months, days, seconds * IntervalValue::kMicrosInSecond));
    TestRoundtrip(interval);
    // Microseconds granularity
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        interval, IntervalValue::FromMonthsDaysMicros(months, days, micros));
    TestRoundtrip(interval);
    // Nanoseconds granularity
    nanos = IntervalValue::kNanosInMicro128 * micros + nano_fractions;
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        interval, IntervalValue::FromMonthsDaysNanos(months, days, nanos));
    TestRoundtrip(interval);
  }
}

std::string ParseToString(absl::string_view input) {
  return IntervalValue::ParseFromString(input).value().ToString();
}

void ExpectParseFromString(absl::string_view expected,
                           absl::string_view input) {
  EXPECT_EQ(expected, (*IntervalValue::ParseFromString(input)).ToString());
  EXPECT_EQ(expected, (*IntervalValue::Parse(input)).ToString());
}

void ExpectParseError(absl::string_view input) {
  EXPECT_THAT(IntervalValue::ParseFromString(input),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(IntervalValue::Parse(input),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalValueTest, ParseFromString) {
  ExpectParseFromString("1-2 3 4:5:6", "1-2 3 4:5:6");
  ExpectParseFromString("-1-2 3 4:5:6.700", "-1-2 3 4:5:6.7");
  ExpectParseFromString("1-2 3 -4:5:6.780", "1-2 3 -4:5:6.78");
  ExpectParseFromString("-1-2 -3 4:5:6.789", "-1-2 -3 +4:5:6.78900");
  ExpectParseFromString("1-2 3 4:5:0", "1-2 3 4:5");
  ExpectParseFromString("1-2 3 -4:5:0", "1-2 3 -4:5");
  ExpectParseFromString("1-2 3 4:0:0", "1-2 3 4");
  ExpectParseFromString("1-2 3 -4:0:0", "1-2 3 -4");
  ExpectParseFromString("1-2 3 0:0:0", "1-2 3");
  ExpectParseFromString("-1-2 -3 0:0:0", "-1-2 -3");
  ExpectParseFromString("1-2 0 0:0:0", "1-2");
  ExpectParseFromString("1-2 0 0:0:0", "+1-2");
  ExpectParseFromString("-1-2 0 0:0:0", "-1-2");
  ExpectParseFromString("0-1 2 3:0:0", "1 2 3");
  ExpectParseFromString("0-1 -2 3:0:0", "1 -2 3");
  ExpectParseFromString("-0-1 -2 -3:0:0", "-1 -2 -3");
  ExpectParseFromString("0-1 2 3:4:0", "1 2 3:4");
  ExpectParseFromString("0-1 2 -3:4:0", "1 2 -3:4");
  ExpectParseFromString("0-1 2 3:4:5", "1 2 3:4:5");
  ExpectParseFromString("0-1 2 -3:4:5.600", "1 2 -3:4:5.6");
  ExpectParseFromString("0-0 1 2:3:0", "1 2:3");
  ExpectParseFromString("0-0 -1 -2:3:0", "-1 -2:3");
  ExpectParseFromString("0-0 1 2:3:4", "1 2:3:4");
  ExpectParseFromString("0-0 -1 2:3:4.567800", "-1 2:3:4.5678");
  ExpectParseFromString("0-0 0 1:2:3", "1:2:3");
  ExpectParseFromString("0-0 0 -1:2:3", "-1:2:3");
  ExpectParseFromString("0-0 0 -1:2:3.456", "-1:2:3.456");
  ExpectParseFromString("0-0 0 -1:2:3.456789100", "-1:2:3.4567891");

  // Ambiguous: Could be MONTH TO DAY or DAY TO HOUR
  ExpectParseError("1 2");
  // Ambiguous: Could be HOUR TO MINUTE or MINUTE TO SECOND
  ExpectParseError("1:2");
  // Good number of spaces/colons/dashes, but wrong format
  ExpectParseError("1:2:3 2");
  ExpectParseError("1 2-3");
  ExpectParseError("1-2  1:2:3");
  // Unexpected number of spaces/colons/dashes
  ExpectParseError("1-2 1:2:3");
  ExpectParseError("1-2-3");
}

void ExpectToISO8601(absl::string_view expected, IntervalValue interval) {
  EXPECT_EQ(expected, interval.ToISO8601());
  // Check roundtriping by parsing the expected string back and comparing with
  // original interval.
  EXPECT_EQ(interval, *IntervalValue::ParseFromISO8601(expected));
}

TEST(IntervalValueTest, ToISO8601) {
  ExpectToISO8601("P0Y", Years(0));
  ExpectToISO8601("P0Y", Days(0));
  ExpectToISO8601("P0Y", Nanos(0));

  ExpectToISO8601("P1Y", Years(1));
  ExpectToISO8601("P-1Y", Years(-1));
  ExpectToISO8601("P999Y", Years(999));
  ExpectToISO8601("P-999Y", Years(-999));
  ExpectToISO8601("P10000Y", Years(10000));
  ExpectToISO8601("P-10000Y", Years(-10000));
  ExpectToISO8601("P11M", Months(11));
  ExpectToISO8601("P-11M", Months(-11));
  ExpectToISO8601("P1D", Days(1));
  ExpectToISO8601("P-1D", Days(-1));
  ExpectToISO8601("P3660000D", Days(3660000));
  ExpectToISO8601("P-3660000D", Days(-3660000));
  ExpectToISO8601("PT1H", Hours(1));
  ExpectToISO8601("PT-1H", Hours(-1));
  ExpectToISO8601("PT87840000H", Hours(87840000));
  ExpectToISO8601("PT-87840000H", Hours(-87840000));
  ExpectToISO8601("PT1M", Minutes(1));
  ExpectToISO8601("PT-1M", Minutes(-1));
  ExpectToISO8601("PT59M", Minutes(59));
  ExpectToISO8601("PT-59M", Minutes(-59));
  ExpectToISO8601("PT1S", Seconds(1));
  ExpectToISO8601("PT-1S", Seconds(-1));
  ExpectToISO8601("PT59S", Seconds(59));
  ExpectToISO8601("PT-59S", Seconds(-59));

  ExpectToISO8601("PT0.1S", Nanos(100000000));
  ExpectToISO8601("PT0.01S", Nanos(10000000));
  ExpectToISO8601("PT0.001S", Nanos(1000000));
  ExpectToISO8601("PT0.0001S", Nanos(100000));
  ExpectToISO8601("PT0.00001S", Nanos(10000));
  ExpectToISO8601("PT0.000001S", Nanos(1000));
  ExpectToISO8601("PT0.0000001S", Nanos(100));
  ExpectToISO8601("PT0.00000001S", Nanos(10));
  ExpectToISO8601("PT0.000000001S", Nanos(1));

  ExpectToISO8601("PT-0.1S", Nanos(-100000000));
  ExpectToISO8601("PT-0.01S", Nanos(-10000000));
  ExpectToISO8601("PT-0.001S", Nanos(-1000000));
  ExpectToISO8601("PT-0.0001S", Nanos(-100000));
  ExpectToISO8601("PT-0.00001S", Nanos(-10000));
  ExpectToISO8601("PT-0.000001S", Nanos(-1000));
  ExpectToISO8601("PT-0.0000001S", Nanos(-100));
  ExpectToISO8601("PT-0.00000001S", Nanos(-10));
  ExpectToISO8601("PT-0.000000001S", Nanos(-1));

  ExpectToISO8601("PT0.000000001S", Nanos(1));
  ExpectToISO8601("PT0.000000021S", Nanos(21));
  ExpectToISO8601("PT0.000000321S", Nanos(321));
  ExpectToISO8601("PT0.000004321S", Nanos(4321));
  ExpectToISO8601("PT0.000054321S", Nanos(54321));
  ExpectToISO8601("PT0.000654321S", Nanos(654321));
  ExpectToISO8601("PT0.007654321S", Nanos(7654321));
  ExpectToISO8601("PT0.087654321S", Nanos(87654321));
  ExpectToISO8601("PT0.987654321S", Nanos(987654321));

  ExpectToISO8601("PT-0.000000001S", Nanos(-1));
  ExpectToISO8601("PT-0.000000021S", Nanos(-21));
  ExpectToISO8601("PT-0.000000321S", Nanos(-321));
  ExpectToISO8601("PT-0.000004321S", Nanos(-4321));
  ExpectToISO8601("PT-0.000054321S", Nanos(-54321));
  ExpectToISO8601("PT-0.000654321S", Nanos(-654321));
  ExpectToISO8601("PT-0.007654321S", Nanos(-7654321));
  ExpectToISO8601("PT-0.087654321S", Nanos(-87654321));
  ExpectToISO8601("PT-0.987654321S", Nanos(-987654321));

  ExpectToISO8601("P1Y2M3DT4H5M6S", Interval("1-2 3 4:5:6"));
  ExpectToISO8601("P1Y2M3DT4H5M6.7S", Interval("1-2 3 4:5:6.7"));
  ExpectToISO8601("P1Y2M3DT4H5M6.789S", Interval("1-2 3 4:5:6.789"));
  ExpectToISO8601("P1Y2M3DT4H5M6.78912S", Interval("1-2 3 4:5:6.78912"));
  ExpectToISO8601("P1Y2M3DT4H5M6.789123S", Interval("1-2 3 4:5:6.789123"));
  ExpectToISO8601("P1Y2M3DT4H5M6.7891234S", Interval("1-2 3 4:5:6.7891234"));
  ExpectToISO8601("P1Y2M3DT4H5M6.789123456S",
                  Interval("1-2 3 4:5:6.789123456"));

  ExpectToISO8601("P-1Y-2M-3DT-4H-5M-6S", Interval("-1-2 -3 -4:5:6"));
  ExpectToISO8601("P-1Y-2M-3DT-4H-5M-6.7S", Interval("-1-2 -3 -4:5:6.7"));
  ExpectToISO8601("P-1Y-2M-3DT-4H-5M-6.789S", Interval("-1-2 -3 -4:5:6.789"));
  ExpectToISO8601("P-1Y-2M-3DT-4H-5M-6.78912S",
                  Interval("-1-2 -3 -4:5:6.78912"));
  ExpectToISO8601("P-1Y-2M-3DT-4H-5M-6.789123S",
                  Interval("-1-2 -3 -4:5:6.789123"));
  ExpectToISO8601("P-1Y-2M-3DT-4H-5M-6.7891234S",
                  Interval("-1-2 -3 -4:5:6.7891234"));
  ExpectToISO8601("P-1Y-2M-3DT-4H-5M-6.789123456S",
                  Interval("-1-2 -3 -4:5:6.789123456"));
}

void ExpectFromISO8601(absl::string_view expected, absl::string_view input) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(IntervalValue interval,
                       IntervalValue::ParseFromISO8601(input));
  EXPECT_EQ(expected, interval.ToString());

  ZETASQL_ASSERT_OK_AND_ASSIGN(interval, IntervalValue::Parse(input));
  EXPECT_EQ(expected, interval.ToString());
}

void ExpectFromISO8601Error(absl::string_view input,
                            const std::string& error_text) {
  EXPECT_THAT(
      IntervalValue::ParseFromISO8601(input),
      StatusIs(absl::StatusCode::kOutOfRange, testing::HasSubstr(error_text)))
      << input;

  EXPECT_THAT(IntervalValue::Parse(input),
              StatusIs(absl::StatusCode::kOutOfRange))
      << input;
}

TEST(IntervalValueTest, ParseFromISO8601) {
  ExpectFromISO8601("0-0 0 0:0:0", "PT");
  ExpectFromISO8601("0-0 0 0:0:0", "P0Y");
  ExpectFromISO8601("0-0 0 0:0:0", "P-1Y2Y-1Y");
  ExpectFromISO8601("1-0 0 0:0:0", "P1Y");
  ExpectFromISO8601("-1-0 0 0:0:0", "P-1Y");
  ExpectFromISO8601("1-0 0 0:0:0", "P100Y1M-1M-99Y");
  ExpectFromISO8601("0-2 0 0:0:0", "P2M");
  ExpectFromISO8601("-0-2 0 0:0:0", "P2M-4M");
  ExpectFromISO8601("2-1 0 0:0:0", "P25M");
  ExpectFromISO8601("0-11 0 0:0:0", "P1Y-1M");
  ExpectFromISO8601("0-0 70 0:0:0", "P10W");
  ExpectFromISO8601("0-0 -70 0:0:0", "P-10W");
  ExpectFromISO8601("0-0 3 0:0:0", "P3D");
  ExpectFromISO8601("0-0 -3 0:0:0", "P-1D-1D-1D");
  ExpectFromISO8601("0-0 123456 0:0:0", "P123456D");
  ExpectFromISO8601("0-0 0 -4:0:0", "PT-4H");
  ExpectFromISO8601("0-0 0 4:0:0", "PT3H60M");
  ExpectFromISO8601("0-0 0 100000:0:0", "PT100000H");
  ExpectFromISO8601("0-0 0 0:5:0", "PT5M");
  ExpectFromISO8601("0-0 0 -0:5:0", "PT5M-10M");
  ExpectFromISO8601("0-0 0 0:0:6", "PT6S");
  ExpectFromISO8601("0-0 0 -0:0:6", "PT-6S");
  ExpectFromISO8601("0-0 0 0:0:0.100", "PT0.1S");
  ExpectFromISO8601("0-0 0 -0:0:0.100", "PT-0,1S");
  ExpectFromISO8601("0-0 0 0:0:1.234", "PT1.234S");
  ExpectFromISO8601("0-0 0 0:0:10.987654321", "PT10,987654321S");
  ExpectFromISO8601("1-2 3 4:5:6.789", "P3D2M1YT6,789S5M4H");
  ExpectFromISO8601("-1-2 -3 -4:5:6.789", "P-3D-2M-1YT-6.789S-5M-4H");
  // Handle intermediate overflows
  ExpectFromISO8601("9900-0 0 0:0:0", "P10000Y100Y-200Y");
  ExpectFromISO8601("0-0 -3659999 0:0:0", "P-3660000D-1D2D");

  // Input can be of unbounded length because same datetime part can appear
  // multiple times. Test for scalability/robustness in big inputs.
  for (int n : {10, 1000, 10000, 1000000}) {
    std::string input("PT1H");
    for (int i = 0; i < n; i++) {
      absl::StrAppend(&input, i, "S");
    }
    for (int i = 0; i < n; i++) {
      absl::StrAppend(&input, "-", i, "S");
    }
    ExpectFromISO8601("0-0 0 1:0:0", input);
  }

  // Errors
  ExpectFromISO8601Error("", "Interval must start with 'P'");
  ExpectFromISO8601Error("1Y", "Interval must start with 'P'");
  ExpectFromISO8601Error("T", "Interval must start with 'P'");
  ExpectFromISO8601Error("P", "At least one datetime part must be defined");
  ExpectFromISO8601Error("P--1Y", "Expected number");
  ExpectFromISO8601Error("P-", "Expected number");
  ExpectFromISO8601Error("P1", "Unexpected end of input in the date portion");
  ExpectFromISO8601Error("PTT", "Unexpected duplicate time separator");
  ExpectFromISO8601Error("PT1HT1M", "Unexpected duplicate time separator");
  ExpectFromISO8601Error("P1YM", "Unexpected 'M'");
  ExpectFromISO8601Error("P1YT2MS", "Unexpected 'S'");
  ExpectFromISO8601Error("P1M ", "Unexpected ' '");
  ExpectFromISO8601Error("PT.1S", "Unexpected '.'");
  ExpectFromISO8601Error("PT99999999999999999999999999999H", "Cannot convert");
  ExpectFromISO8601Error("PT-99999999999999999999999999999H", "Cannot convert");
  ExpectFromISO8601Error("P-1S", "Unexpected 'S' in the date portion");
  ExpectFromISO8601Error("P1H2", "Unexpected 'H' in the date portion");
  ExpectFromISO8601Error("PT1D", "Unexpected 'D' in the time portion");
  ExpectFromISO8601Error("P123", "Unexpected end of input in the date portion");
  ExpectFromISO8601Error("P9223372036854775807D1D", "int64 overflow");
  ExpectFromISO8601Error("PT0.1234567890S", "Invalid interval");
  ExpectFromISO8601Error("PT1.S", "Invalid interval");
  ExpectFromISO8601Error("P1.Y", "Fractional values are only allowed");
  ExpectFromISO8601Error("PT1.M", "Fractional values are only allowed");
  ExpectFromISO8601Error("P9223372036854775807Y", "int64 overflow");
  ExpectFromISO8601Error("P9223372036854775807W", "int64 overflow");
  ExpectFromISO8601Error("P9223372036854775807D1W", "int64 overflow");
  ExpectFromISO8601Error("P-10001Y", "is out of range");
  ExpectFromISO8601Error("P-9999999M", "is out of range");
  ExpectFromISO8601Error("P-987654321D", "is out of range");
  ExpectFromISO8601Error("PT-99999999999H", "is out of range");
}

std::vector<IntervalValue>* kInterestingIntervals =
    new std::vector<IntervalValue>{
        Years(0),
        Years(1),
        Years(-2),
        Months(3),
        Months(-4),
        Days(5),
        Days(-6),
        Hours(7),
        Hours(-8),
        Minutes(9),
        Minutes(-10),
        Seconds(11),
        Seconds(-12),
        Micros(13),
        Micros(-14),
        Nanos(15),
        Nanos(-16),
        MonthsDaysMicros(1, -2, 3),
        MonthsDaysMicros(-4, 5, -6),
        MonthsDaysNanos(7, -8, 9),
        MonthsDaysNanos(10, -11, 12),
        YMDHMS(1, -2, 3, -4, 5, -6),
        YMDHMS(-7, 8, -9, 10, -11, 12),
        Months(IntervalValue::kMaxMonths),
        Months(IntervalValue::kMinMonths),
        Days(IntervalValue::kMaxDays),
        Days(IntervalValue::kMinDays),
        Micros(IntervalValue::kMaxMicros),
        Micros(IntervalValue::kMinMicros),
        Nanos(IntervalValue::kMaxNanos),
        Nanos(IntervalValue::kMinNanos),
        MonthsDaysMicros(IntervalValue::kMaxMonths, IntervalValue::kMaxDays,
                         IntervalValue::kMaxMicros),
        MonthsDaysMicros(IntervalValue::kMinMonths, IntervalValue::kMinDays,
                         IntervalValue::kMinMicros),
        MonthsDaysNanos(IntervalValue::kMaxMonths, IntervalValue::kMaxDays,
                        IntervalValue::kMaxNanos),
        MonthsDaysNanos(IntervalValue::kMinMonths, IntervalValue::kMinDays,
                        IntervalValue::kMinNanos),
    };

TEST(IntervalValueTest, HashCode) {
  EXPECT_TRUE(
      absl::VerifyTypeImplementsAbslHashCorrectly(*kInterestingIntervals));

  // Values which compare equal even though have different binary
  // representations, check that they have same hash code.
  std::vector<IntervalValue> values{
      Years(1),  Months(IntervalValue::kMonthsInYear),
      Months(1), Days(IntervalValue::kDaysInMonth),
      Days(1),   Hours(IntervalValue::kHoursInDay),
      Hours(1),  Micros(IntervalValue::kMicrosInHour),
  };
  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(values));
}

TEST(IntervalValueTest, ToStringParseRoundtrip) {
  for (IntervalValue value : *kInterestingIntervals) {
    std::string str = value.ToString();
    ZETASQL_ASSERT_OK_AND_ASSIGN(IntervalValue roundtrip_value,
                         IntervalValue::ParseFromString(str));
    EXPECT_EQ(value, roundtrip_value);
  }
}

std::string FromIntegerToString(int64_t value,
                                functions::DateTimestampPart part) {
  return IntervalValue::FromInteger(value, part).value().ToString();
}

void ExpectFromIntegerError(int64_t value, functions::DateTimestampPart part) {
  EXPECT_THAT(IntervalValue::FromInteger(value, part),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalValueTest, FromInteger) {
  EXPECT_EQ("1-0 0 0:0:0", FromIntegerToString(1, YEAR));
  EXPECT_EQ("-10000-0 0 0:0:0", FromIntegerToString(-10000, YEAR));
  EXPECT_EQ("0-6 0 0:0:0", FromIntegerToString(2, QUARTER));
  EXPECT_EQ("10000-0 0 0:0:0", FromIntegerToString(40000, QUARTER));
  EXPECT_EQ("-0-2 0 0:0:0", FromIntegerToString(-2, MONTH));
  EXPECT_EQ("10000-0 0 0:0:0", FromIntegerToString(120000, MONTH));
  EXPECT_EQ("0-0 28 0:0:0", FromIntegerToString(4, WEEK));
  EXPECT_EQ("0-0 -3659999 0:0:0", FromIntegerToString(-522857, WEEK));
  EXPECT_EQ("0-0 -5 0:0:0", FromIntegerToString(-5, DAY));
  EXPECT_EQ("0-0 3660000 0:0:0", FromIntegerToString(3660000, DAY));
  EXPECT_EQ("0-0 0 6:0:0", FromIntegerToString(6, HOUR));
  EXPECT_EQ("0-0 0 -87840000:0:0", FromIntegerToString(-87840000, HOUR));
  EXPECT_EQ("0-0 0 -0:7:0", FromIntegerToString(-7, MINUTE));
  EXPECT_EQ("0-0 0 87840000:0:0", FromIntegerToString(5270400000, MINUTE));
  EXPECT_EQ("0-0 0 0:0:8", FromIntegerToString(8, SECOND));
  EXPECT_EQ("0-0 0 -87840000:0:0", FromIntegerToString(-316224000000, SECOND));

  EXPECT_EQ("0-0 0 0:0:0", FromIntegerToString(0, YEAR));
  EXPECT_EQ("0-0 0 0:0:0", FromIntegerToString(0, QUARTER));
  EXPECT_EQ("0-0 0 0:0:0", FromIntegerToString(0, MONTH));
  EXPECT_EQ("0-0 0 0:0:0", FromIntegerToString(0, WEEK));
  EXPECT_EQ("0-0 0 0:0:0", FromIntegerToString(0, DAY));

  // Exceeds maximum allowed value
  ExpectFromIntegerError(10001, YEAR);
  ExpectFromIntegerError(-40001, QUARTER);
  ExpectFromIntegerError(120001, MONTH);
  ExpectFromIntegerError(522858, WEEK);
  ExpectFromIntegerError(-3660001, DAY);
  ExpectFromIntegerError(87840001, HOUR);
  ExpectFromIntegerError(-5270400001, MINUTE);
  ExpectFromIntegerError(316224000001, SECOND);

  // Overflow in multiplication
  ExpectFromIntegerError(9223372036854775807, QUARTER);
  ExpectFromIntegerError(-9223372036854775807, QUARTER);
  ExpectFromIntegerError(9223372036854775807, WEEK);
  ExpectFromIntegerError(-9223372036854775807, WEEK);

  // Invalid datetime part fields
  ExpectFromIntegerError(0, functions::DAYOFWEEK);
  ExpectFromIntegerError(0, functions::DAYOFYEAR);
  ExpectFromIntegerError(0, functions::MILLISECOND);
  ExpectFromIntegerError(0, functions::MICROSECOND);
  ExpectFromIntegerError(0, functions::NANOSECOND);
  ExpectFromIntegerError(0, functions::DATE);
  ExpectFromIntegerError(0, functions::DATETIME);
  ExpectFromIntegerError(0, functions::TIME);
  ExpectFromIntegerError(0, functions::ISOYEAR);
  ExpectFromIntegerError(0, functions::ISOWEEK);
  ExpectFromIntegerError(0, functions::WEEK_MONDAY);
  ExpectFromIntegerError(0, functions::WEEK_TUESDAY);
  ExpectFromIntegerError(0, functions::WEEK_WEDNESDAY);
  ExpectFromIntegerError(0, functions::WEEK_THURSDAY);
  ExpectFromIntegerError(0, functions::WEEK_FRIDAY);
  ExpectFromIntegerError(0, functions::WEEK_SATURDAY);
}

TEST(IntervalTest, JustifyHours) {
  EXPECT_EQ(*JustifyHours(Interval("0 12:0:0")), Interval("0 12:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("0 -12:0:0")), Interval("0 -12:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("0 24:0:0")), Interval("1 0:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("0 -24:0:0")), Interval("-1 0:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("0 24:0:0.1")), Interval("1 0:0:0.1"));
  EXPECT_EQ(*JustifyHours(Interval("0 -24:0:0.1")), Interval("-1 -0:0:0.1"));
  EXPECT_EQ(*JustifyHours(Interval("0 25:0:0")), Interval("1 1:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("0 -25:0:0")), Interval("-1 -1:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("0 240:0:0")), Interval("10 0:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("0 -240:0:0")), Interval("-10 -0:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("1 25:0:0")), Interval("2 1:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("-1 -25:0:0")), Interval("-2 -1:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("1 -1:0:0")), Interval("0 23:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("-1 1:0:0")), Interval("0 -23:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("3660000 -0:0:0.000001")),
            Interval("3659999 23:59:59.999999"));
  EXPECT_EQ(*JustifyHours(Interval("-3660000 0:0:0.000001")),
            Interval("-3659999 -23:59:59.999999"));
  EXPECT_EQ(*JustifyHours(Interval("1 0 -1:0:0")), Interval("1 0 -1:0:0"));
  EXPECT_EQ(*JustifyHours(Interval("-1 0 1:0:0")), Interval("-1 0 1:0:0"));

  EXPECT_THAT(JustifyHours(Interval("0 3660000 24:0:0")),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(JustifyHours(Interval("0 -3660000 -24:0:0")),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalTest, JustifyDays) {
  EXPECT_EQ(*JustifyDays(Interval("0-0 29")), Interval("0-0 29"));
  EXPECT_EQ(*JustifyDays(Interval("0-0 -29")), Interval("0-0 -29"));
  EXPECT_EQ(*JustifyDays(Interval("0-0 30")), Interval("0-1 0"));
  EXPECT_EQ(*JustifyDays(Interval("0-0 -30")), Interval("-0-1 0"));
  EXPECT_EQ(*JustifyDays(Interval("0-0 31")), Interval("0-1 1"));
  EXPECT_EQ(*JustifyDays(Interval("0-0 -31")), Interval("-0-1 -1"));
  EXPECT_EQ(*JustifyDays(Interval("0-1 30")), Interval("0-2 0"));
  EXPECT_EQ(*JustifyDays(Interval("-0-1 -30")), Interval("-0-2 0"));
  EXPECT_EQ(*JustifyDays(Interval("1-11 30")), Interval("2-0 0"));
  EXPECT_EQ(*JustifyDays(Interval("-1-11 -30")), Interval("-2-0 0"));
  EXPECT_EQ(*JustifyDays(Interval("0-1 -1")), Interval("0-0 29"));
  EXPECT_EQ(*JustifyDays(Interval("-0-1 1")), Interval("0-0 -29"));
  EXPECT_EQ(*JustifyDays(Interval("10000-0 -1")), Interval("9999-11 29"));
  EXPECT_EQ(*JustifyDays(Interval("-10000-0 1")), Interval("-9999-11 -29"));
  EXPECT_EQ(*JustifyDays(Interval("0-0 3600000")), Interval("10000-0 0"));
  EXPECT_EQ(*JustifyDays(Interval("0-0 -3600000")), Interval("-10000-0 0"));
  EXPECT_EQ(*JustifyDays(Interval("0-0 3600010")), Interval("10000-0 10"));
  EXPECT_EQ(*JustifyDays(Interval("0-0 -3600010")), Interval("-10000-0 -10"));
  EXPECT_EQ(*JustifyDays(Interval("29 240:0:0")), Interval("29 240:0:0"));
  EXPECT_EQ(*JustifyDays(Interval("-29 -240:0:0")), Interval("-29 -240:0:0"));

  EXPECT_THAT(JustifyDays(Interval("10000-0 30")),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(JustifyDays(Interval("-10000-0 -30")),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(JustifyDays(Interval("0-0 3660000")),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(JustifyDays(Interval("0-0 -3660000")),
              StatusIs(absl::StatusCode::kOutOfRange));
}

TEST(IntervalTest, JustifyInterval) {
  EXPECT_EQ(*JustifyInterval(Interval("0 0 23")), Interval("0 0 23"));
  EXPECT_EQ(*JustifyInterval(Interval("0 0 -23")), Interval("0 0 -23"));
  EXPECT_EQ(*JustifyInterval(Interval("0 0 24")), Interval("0 1 0"));
  EXPECT_EQ(*JustifyInterval(Interval("0 0 -24")), Interval("0 -1 0"));
  EXPECT_EQ(*JustifyInterval(Interval("0 29 24")), Interval("1 0 0"));
  EXPECT_EQ(*JustifyInterval(Interval("0 -29 -24")), Interval("-1 0 0"));
  EXPECT_EQ(*JustifyInterval(Interval("1 0 -1")), Interval("0 29 23"));
  EXPECT_EQ(*JustifyInterval(Interval("-1 0 1")), Interval("0 -29 -23"));
  EXPECT_EQ(*JustifyInterval(Interval("1 -1 1")), Interval("0 29 1"));
  EXPECT_EQ(*JustifyInterval(Interval("-1 1 -1")), Interval("0 -29 -1"));
  EXPECT_EQ(*JustifyInterval(Interval("1 -1 -1")), Interval("0 28 23"));
  EXPECT_EQ(*JustifyInterval(Interval("-1 1 1")), Interval("0 -28 -23"));
  EXPECT_EQ(*JustifyInterval(Interval("0 3600000 241")),
            Interval("10000-0 10 1"));
  EXPECT_EQ(*JustifyInterval(Interval("0 -3600000 -241")),
            Interval("-10000-0 -10 -1"));

  EXPECT_THAT(JustifyInterval(Interval("10000-0 29 24:0:0")),
              StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(JustifyInterval(Interval("-10000-0 -29 -24:0:0")),
              StatusIs(absl::StatusCode::kOutOfRange));
}

}  // namespace
}  // namespace zetasql
