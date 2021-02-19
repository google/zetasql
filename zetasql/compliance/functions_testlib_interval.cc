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

#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/parse_date_time.h"
#include "zetasql/public/interval_value_test_util.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/using_test_value.cc"  // NOLINT (build/include)
#include "absl/time/time.h"

namespace zetasql {

constexpr absl::StatusCode OUT_OF_RANGE = absl::StatusCode::kOutOfRange;

namespace {

std::vector<QueryParamsWithResult> WrapFeatureIntervalType(
    const std::vector<QueryParamsWithResult>& params) {
  std::vector<QueryParamsWithResult> wrapped_params;
  wrapped_params.reserve(params.size());
  for (const auto& param : params) {
    wrapped_params.emplace_back(param.WrapWithFeature(FEATURE_INTERVAL_TYPE));
  }
  return wrapped_params;
}

std::vector<QueryParamsWithResult> WrapFeaturesIntervalAndCivilTimeTypes(
    const std::vector<QueryParamsWithResult>& params) {
  std::vector<QueryParamsWithResult> wrapped_params;
  wrapped_params.reserve(params.size());
  for (const auto& param : params) {
    wrapped_params.emplace_back(param.WrapWithFeatureSet(
        {FEATURE_V_1_2_CIVIL_TIME, FEATURE_INTERVAL_TYPE}));
  }
  return wrapped_params;
}

std::vector<FunctionTestCall> WrapFeatureIntervalType(
    const std::vector<FunctionTestCall>& tests) {
  std::vector<FunctionTestCall> wrapped_tests;
  wrapped_tests.reserve(tests.size());
  for (auto call : tests) {
    call.params = call.params.WrapWithFeature(FEATURE_INTERVAL_TYPE);
    wrapped_tests.emplace_back(call);
  }
  return wrapped_tests;
}

Value Years(int64_t value) {
  return Value::Interval(interval_testing::Years(value));
}
Value Months(int64_t value) {
  return Value::Interval(interval_testing::Months(value));
}
Value Days(int64_t value) {
  return Value::Interval(interval_testing::Days(value));
}
Value Hours(int64_t value) {
  return Value::Interval(interval_testing::Hours(value));
}
Value Minutes(int64_t value) {
  return Value::Interval(interval_testing::Minutes(value));
}
Value Seconds(int64_t value) {
  return Value::Interval(interval_testing::Seconds(value));
}
Value Micros(int64_t value) {
  return Value::Interval(interval_testing::Micros(value));
}
Value Nanos(__int128 value) {
  return Value::Interval(interval_testing::Nanos(value));
}
Value YMDHMS(int64_t year, int64_t month, int64_t day, int64_t hour, int64_t minute,
             int64_t second) {
  return Value::Interval(
      interval_testing::YMDHMS(year, month, day, hour, minute, second));
}

}  // namespace

std::vector<FunctionTestCall> GetFunctionTestsIntervalConstructor() {
  std::vector<FunctionTestCall> tests = {
      {"", {NullInt64(), "YEAR"}, NullInterval()},
      {"", {Int64(0), "YEAR"}, Years(0)},
      {"", {Int64(1), "YEAR"}, Years(1)},
      {"", {Int64(-10000), "YEAR"}, Years(-10000)},
      {"", {Int64(0), "QUARTER"}, Months(0)},
      {"", {Int64(40000), "QUARTER"}, Years(10000)},
      {"", {Int64(-3), "MONTH"}, Months(-3)},
      {"", {Int64(120000), "MONTH"}, Months(120000)},
      {"", {Int64(4), "WEEK"}, Days(28)},
      {"", {Int64(-522857), "WEEK"}, Days(-3659999)},
      {"", {Int64(-5), "DAY"}, Days(-5)},
      {"", {Int64(3660000), "DAY"}, Days(3660000)},
      {"", {Int64(6), "HOUR"}, Hours(6)},
      {"", {Int64(-87840000), "HOUR"}, Hours(-87840000)},
      {"", {Int64(-7), "MINUTE"}, Minutes(-7)},
      {"", {Int64(5270400000), "MINUTE"}, Minutes(5270400000)},
      {"", {Int64(8), "SECOND"}, Seconds(8)},
      {"", {Int64(-316224000000), "SECOND"}, Seconds(-316224000000)},

      // Exceeds maximum allowed value
      {"", {Int64(10001), "YEAR"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(-40001), "QUARTER"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(120001), "MONTH"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(522858), "WEEK"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(-3660001), "DAY"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(87840001), "HOUR"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(-5270400001), "MINUTE"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(316224000001), "SECOND"}, NullInterval(), OUT_OF_RANGE},

      // Overflow in multiplication
      {"",
       {Int64(9223372036854775807), "QUARTER"},
       NullInterval(),
       OUT_OF_RANGE},
      {"", {Int64(9223372036854775807), "WEEK"}, NullInterval(), OUT_OF_RANGE},

      // Invalid datetime part fields
      {"", {Int64(0), "DAYOFWEEK"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(0), "DAYOFYEAR"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(0), "MILLISECOND"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(0), "MICROSECOND"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(0), "NANOSECOND"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(0), "DATE"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(0), "DATETIME"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(0), "TIME"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(0), "ISOYEAR"}, NullInterval(), OUT_OF_RANGE},
      {"", {Int64(0), "ISOWEEK"}, NullInterval(), OUT_OF_RANGE},
  };
  return WrapFeatureIntervalType(tests);
}

std::vector<FunctionTestCall> GetFunctionTestsIntervalComparisons() {
  // We only add tests for "=" and "<", because the test driver automatically
  // generates all comparison functions for every test case.
  std::vector<FunctionTestCall> tests = {
      {"=", {NullInterval(), Years(0)}, NullBool()},
      {"=", {Years(0), NullInterval()}, NullBool()},
      {"=", {NullInterval(), NullInterval()}, NullBool()},

      {"<", {NullInterval(), Years(0)}, NullBool()},
      {"<", {Years(0), NullInterval()}, NullBool()},
      {"<", {NullInterval(), NullInterval()}, NullBool()},

      {"=", {Years(0), Years(0)}, Bool(true)},
      {"=", {Months(0), Days(0)}, Bool(true)},
      {"=", {Months(0), Micros(0)}, Bool(true)},
      {"=", {Months(0), Nanos(0)}, Bool(true)},

      {"=", {Months(10), Months(10)}, Bool(true)},
      {"=", {Days(-3), Days(-3)}, Bool(true)},
      {"=", {Micros(12345), Micros(12345)}, Bool(true)},
      {"=", {Nanos(-9876), Nanos(-9876)}, Bool(true)},

      {"<", {Nanos(999), Micros(1)}, Bool(true)},
      {"<", {Micros(999999), Seconds(1)}, Bool(true)},
      {"<", {Seconds(59), Minutes(1)}, Bool(true)},
      {"<", {Minutes(59), Hours(1)}, Bool(true)},
      {"<", {Hours(23), Days(1)}, Bool(true)},
      {"<", {Days(29), Months(1)}, Bool(true)},
      {"<", {Months(11), Years(1)}, Bool(true)},
      {"<", {Years(1), YMDHMS(1, 0, 0, 0, 0, 1)}, Bool(true)},

      {"<", {Months(-1), Days(0)}, Bool(true)},
      {"<", {Days(1), Months(1)}, Bool(true)},
      {"<", {Days(-1), Micros(1)}, Bool(true)},

      {"=", {Days(30), Months(1)}, Bool(true)},
      {"=", {Months(-2), Days(-60)}, Bool(true)},
      {"=", {Days(360), Months(12)}, Bool(true)},
      {"<", {Days(1), Months(1)}, Bool(true)},
      {"<", {Days(29), Months(1)}, Bool(true)},
      {"<", {Days(-31), Months(-1)}, Bool(true)},
      {"<", {Months(1), Days(31)}, Bool(true)},
      {"<", {Months(-1), Days(25)}, Bool(true)},
      {"<", {Months(12), Days(365)}, Bool(true)},

      {"=", {Micros(IntervalValue::kMicrosInDay), Days(1)}, Bool(true)},
      {"<", {Micros(IntervalValue::kMicrosInDay - 1), Days(1)}, Bool(true)},
      {"<", {Days(1), Micros(IntervalValue::kMicrosInDay + 1)}, Bool(true)},
      {"=", {Nanos(-IntervalValue::kNanosInDay), Days(-1)}, Bool(true)},
      {"<", {Nanos(-IntervalValue::kNanosInDay - 1), Days(-1)}, Bool(true)},
      {"=", {Micros(IntervalValue::kMicrosInMonth), Months(1)}, Bool(true)},
      {"=", {Nanos(-IntervalValue::kNanosInMonth), Months(-1)}, Bool(true)},

      {"=", {Micros(-1), Nanos(-1000)}, Bool(true)},
      {"=", {Micros(7), Nanos(7000)}, Bool(true)},
      {"<", {Micros(1), Nanos(1001)}, Bool(true)},
      {"<", {Micros(-1), Nanos(900)}, Bool(true)},
      {"<", {Nanos(999), Micros(1)}, Bool(true)},
      {"<", {Nanos(-1001), Micros(-1)}, Bool(true)},
      {"<", {Nanos(1), Micros(1)}, Bool(true)},
      {"<", {Micros(-1), Nanos(1)}, Bool(true)},
      {"<", {Nanos(1), Micros(1)}, Bool(true)},

      {"=", {YMDHMS(1, 0, 1, 0, 0, 0), Days(361)}, Bool(true)},
      {"=", {YMDHMS(-1, 0, 360, 0, 2, 0), Minutes(2)}, Bool(true)},
      {"=", {YMDHMS(0, 1, 1, 0, 0, 0), Days(31)}, Bool(true)},
      {"=", {YMDHMS(0, 1, -1, 0, 0, 0), Days(29)}, Bool(true)},
      {"=", {YMDHMS(0, -1, 1, 0, 0, 0), Days(-29)}, Bool(true)},
      {"=", {YMDHMS(0, -1, -1, 0, 0, 0), Days(-31)}, Bool(true)},
      {"=", {YMDHMS(0, 2, -61, 0, 0, 0), Hours(-24)}, Bool(true)},
      {"=", {YMDHMS(0, -1, 30, 0, 0, 1), Seconds(1)}, Bool(true)},

      {"<", {YMDHMS(0, 1, 1, -1, 0, 0), Days(31)}, Bool(true)},
      {"<", {YMDHMS(0, 0, 0, 23, 59, 59), Days(1)}, Bool(true)},
      {"<", {YMDHMS(0, -1, 30, 0, 0, 1), Seconds(2)}, Bool(true)},
  };
  return WrapFeatureIntervalType(tests);
}

std::vector<QueryParamsWithResult> GetFunctionTestsIntervalUnaryMinus() {
  std::vector<QueryParamsWithResult> tests = {
      {{NullInterval()}, NullInterval()},
      {{Days(0)}, Years(0)},
      {{Nanos(IntervalValue::kMaxNanos)}, Nanos(-IntervalValue::kMaxNanos)},
      {{Micros(-987654)}, Micros(987654)},
      {{Seconds(1000)}, Seconds(-1000)},
      {{Minutes(-1)}, Minutes(1)},
      {{Hours(12345)}, Hours(-12345)},
      {{Days(-321)}, Days(321)},
      {{Months(2)}, Months(-2)},
      {{Years(-10000)}, Years(10000)},
      {{YMDHMS(0, 0, 0, 100, 200, 300)}, YMDHMS(0, 0, 0, -100, -200, -300)},
      {{YMDHMS(-123, 456, -789, 0, 0, 0)}, YMDHMS(123, -456, 789, 0, 0, 0)},
      {{YMDHMS(1, -2, 3, -4, 5, -6)}, YMDHMS(-1, 2, -3, 4, -5, 6)},
  };
  return WrapFeatureIntervalType(tests);
}

Value Date(absl::string_view str) {
  int32_t date;
  ZETASQL_CHECK_OK(functions::ConvertStringToDate(str, &date));
  return Value::Date(date);
}

Value Timestamp(absl::string_view str) {
  int64_t ts;
  ZETASQL_CHECK_OK(functions::ConvertStringToTimestamp(str, absl::UTCTimeZone(),
                                                functions::kMicroseconds, &ts));
  return Value::TimestampFromUnixMicros(ts);
}

Value Datetime(absl::string_view str) {
  DatetimeValue datetime;
  ZETASQL_CHECK_OK(functions::ConvertStringToDatetime(str, functions::kMicroseconds,
                                               &datetime));
  return Value::Datetime(datetime);
}

Value Time(absl::string_view str) {
  TimeValue time;
  ZETASQL_CHECK_OK(
      functions::ConvertStringToTime(str, functions::kMicroseconds, &time));
  return Value::Time(time);
}

std::vector<QueryParamsWithResult> GetDateTimestampIntervalSubtractions() {
  // The result is of type STRING, not INTERVAL, because we want to make sure
  // that correct date parts are populated. Intervals with different date parts
  // can still compare equal, so we will cast result to string and compare it.
  std::vector<QueryParamsWithResult> tests = {
      {{NullDate(), NullDate()}, NullString()},
      {{Date("1970-01-01"), NullDate()}, NullString()},
      {{NullDate(), Date("1970-01-01")}, NullString()},
      {{Date("1990-01-23"), Date("1990-01-23")}, "0-0 0 0:0:0"},
      {{Date("2000-01-01"), Date("1900-01-01")}, "0-0 36524 0:0:0"},
      {{Date("1910-01-01"), Date("1900-01-01")}, "0-0 3652 0:0:0"},
      {{Date("1901-01-01"), Date("1900-01-01")}, "0-0 365 0:0:0"},
      {{Date("1900-12-31"), Date("1900-01-01")}, "0-0 364 0:0:0"},
      {{Date("1900-12-01"), Date("1900-01-01")}, "0-0 334 0:0:0"},
      {{Date("1900-02-01"), Date("1900-01-01")}, "0-0 31 0:0:0"},
      {{Date("1900-01-31"), Date("1900-01-01")}, "0-0 30 0:0:0"},
      {{Date("1900-01-02"), Date("1900-01-01")}, "0-0 1 0:0:0"},
      {{Date("1900-01-01"), Date("1900-01-01")}, "0-0 0 0:0:0"},
      {{Date("0001-01-01"), Date("9999-12-31")}, "0-0 -3652058 0:0:0"},

      {{NullTimestamp(), NullTimestamp()}, NullString()},
      {{Timestamp("1970-01-01 10:20:30"), NullTimestamp()}, NullString()},
      {{NullTimestamp(), Timestamp("1970-01-01 00:00:01")}, NullString()},
      {{Timestamp("1955-12-25 11:22:33"), Timestamp("1955-12-25 11:22:33")},
       "0-0 0 0:0:0"},
      {{Timestamp("2000-01-01 00:00:00"), Timestamp("1900-01-01 00:00:00")},
       "0-0 0 876576:0:0"},
      {{Timestamp("1910-01-01 00:00:00"), Timestamp("1900-01-01 00:00:00")},
       "0-0 0 87648:0:0"},
      {{Timestamp("1901-01-01 00:00:00"), Timestamp("1900-01-01 00:00:00")},
       "0-0 0 8760:0:0"},
      {{Timestamp("1900-12-31 00:00:00"), Timestamp("1900-01-01 00:00:00")},
       "0-0 0 8736:0:0"},
      {{Timestamp("1900-02-01 00:00:00"), Timestamp("1900-01-01 00:00:00")},
       "0-0 0 744:0:0"},
      {{Timestamp("1900-01-02 00:00:00"), Timestamp("1900-01-01 00:00:00")},
       "0-0 0 24:0:0"},
      {{Timestamp("1900-01-02 00:00:00"),
        Timestamp("1900-01-01 00:00:00.000001")},
       "0-0 0 23:59:59.999999"},
      {{Timestamp("1900-01-02 00:00:00"), Timestamp("1900-01-01 00:00:00.001")},
       "0-0 0 23:59:59.999"},
      {{Timestamp("1900-01-02 00:00:00"), Timestamp("1900-01-01 00:00:01")},
       "0-0 0 23:59:59"},
      {{Timestamp("1900-01-02 00:00:00"), Timestamp("1900-01-01 00:01:00")},
       "0-0 0 23:59:0"},
      {{Timestamp("1900-01-02 00:00:00"), Timestamp("1900-01-01 01:00:00")},
       "0-0 0 23:0:0"},
      {{Timestamp("1900-01-02 00:00:00"), Timestamp("1900-01-01 23:59:59")},
       "0-0 0 0:0:1"},
      {{Timestamp("1900-01-02 00:00:00"), Timestamp("1900-01-01 23:59:59.999")},
       "0-0 0 0:0:0.001"},
      {{Timestamp("1900-01-02 00:00:00"),
        Timestamp("1900-01-01 23:59:59.999999")},
       "0-0 0 0:0:0.000001"},
      {{Timestamp("1900-01-02 00:00:00"), Timestamp("1900-01-02 00:00:00")},
       "0-0 0 0:0:0"},
      {{Timestamp("0001-01-01 00:00:00"),
        Timestamp("9999-12-31 23:59:59.999999")},
       "0-0 0 -87649415:59:59.999999"},
  };
  return WrapFeatureIntervalType(tests);
}

std::vector<QueryParamsWithResult> GetDatetimeTimeIntervalSubtractions() {
  // The result is of type STRING, not INTERVAL, because we want to make sure
  // that correct date parts are populated. Intervals with different date parts
  // can still compare equal, so we will cast result to string and compare it.
  std::vector<QueryParamsWithResult> tests = {
      {{NullDatetime(), NullDatetime()}, NullString()},
      {{Datetime("1970-01-01 01:02:03"), NullDatetime()}, NullString()},
      {{NullDatetime(), Datetime("1970-01-01 22:22:22")}, NullString()},
      {{Datetime("2015-05-06 01:02:03"), Datetime("2015-05-06 01:02:03")},
       "0-0 0 0:0:0"},
      {{Datetime("2000-01-01 00:00:00"), Datetime("1900-01-01 00:00:00")},
       "0-0 36524 0:0:0"},
      {{Datetime("1910-01-01 00:00:00"), Datetime("1900-01-01 00:00:00")},
       "0-0 3652 0:0:0"},
      {{Datetime("1901-01-01 00:00:00"), Datetime("1900-01-01 00:00:00")},
       "0-0 365 0:0:0"},
      {{Datetime("1900-12-31 00:00:00"), Datetime("1900-01-01 00:00:00")},
       "0-0 364 0:0:0"},
      {{Datetime("1900-02-01 00:00:00"), Datetime("1900-01-01 00:00:00")},
       "0-0 31 0:0:0"},
      {{Datetime("1900-01-02 00:00:00"), Datetime("1900-01-01 00:00:00")},
       "0-0 1 0:0:0"},
      {{Datetime("1900-01-02 00:00:00"),
        Datetime("1900-01-01 00:00:00.000001")},
       "0-0 0 23:59:59.999999"},
      {{Datetime("1900-01-02 00:00:00"), Datetime("1900-01-01 00:00:00.001")},
       "0-0 0 23:59:59.999"},
      {{Datetime("1900-01-02 00:00:00"), Datetime("1900-01-01 00:00:01")},
       "0-0 0 23:59:59"},
      {{Datetime("1900-01-02 00:00:00"), Datetime("1900-01-01 00:01:00")},
       "0-0 0 23:59:0"},
      {{Datetime("1900-01-02 00:00:00"), Datetime("1900-01-01 01:00:00")},
       "0-0 0 23:0:0"},
      {{Datetime("1900-01-02 00:00:00"), Datetime("1900-01-01 23:59:59")},
       "0-0 0 0:0:1"},
      {{Datetime("1900-01-02 00:00:00"), Datetime("1900-01-01 23:59:59.999")},
       "0-0 0 0:0:0.001"},
      {{Datetime("1900-01-02 00:00:00"),
        Datetime("1900-01-01 23:59:59.999999")},
       "0-0 0 0:0:0.000001"},
      {{Datetime("1900-01-02 00:00:00"), Datetime("1900-01-02 00:00:00")},
       "0-0 0 0:0:0"},
      {{Datetime("0001-01-01 00:00:00"),
        Datetime("9999-12-31 23:59:59.999999")},
       "0-0 -3652058 -23:59:59.999999"},

      {{NullTime(), NullTime()}, NullString()},
      {{Time("01:02:03"), NullTime()}, NullString()},
      {{NullTime(), Time("20:30:40")}, NullString()},
      {{Time("12:34:56.789"), Time("12:34:56.789")}, "0-0 0 0:0:0"},
      {{Time("00:00:00"), Time("23:59:59.999999")}, "0-0 0 -23:59:59.999999"},
      {{Time("00:00:00"), Time("23:59:59.999")}, "0-0 0 -23:59:59.999"},
      {{Time("00:00:00"), Time("23:59:59.1")}, "0-0 0 -23:59:59.100"},
      {{Time("00:00:00"), Time("23:59:59")}, "0-0 0 -23:59:59"},
      {{Time("00:00:00"), Time("23:59:00")}, "0-0 0 -23:59:0"},
      {{Time("00:00:00"), Time("23:00:00")}, "0-0 0 -23:0:0"},
      {{Time("00:00:00"), Time("01:00:00")}, "0-0 0 -1:0:0"},
      {{Time("00:00:00"), Time("00:01:00")}, "0-0 0 -0:1:0"},
      {{Time("00:00:00"), Time("00:00:01")}, "0-0 0 -0:0:1"},
      {{Time("00:00:00"), Time("00:00:00.1")}, "0-0 0 -0:0:0.100"},
      {{Time("00:00:00"), Time("00:00:00.001")}, "0-0 0 -0:0:0.001"},
      {{Time("00:00:00"), Time("00:00:00.000001")}, "0-0 0 -0:0:0.000001"},
      {{Time("10:20:30.456789"), Time("01:02:03.987654")},
       "0-0 0 9:18:26.469135"},
  };
  return WrapFeaturesIntervalAndCivilTimeTypes(tests);
}

}  // namespace zetasql
