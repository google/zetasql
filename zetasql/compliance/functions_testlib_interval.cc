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

}  // namespace zetasql
