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

#include "zetasql/public/interval_value_test_util.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/using_test_value.cc"  // NOLINT (build/include)

namespace zetasql {

constexpr absl::StatusCode OUT_OF_RANGE = absl::StatusCode::kOutOfRange;

namespace {

std::vector<FunctionTestCall> WrapFeatureIntervalType(
    const std::vector<FunctionTestCall>& tests) {
  std::vector<FunctionTestCall> wrapped_tests;
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
Value Nanos(int64_t value) {
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

}  // namespace zetasql
