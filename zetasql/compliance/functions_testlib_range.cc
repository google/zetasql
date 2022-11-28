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

#include <cstdint>
#include <string>
#include <vector>

#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/using_test_value.cc"  // NOLINT (build/include)
#include "absl/time/time.h"

namespace zetasql {

using zetasql::Value;
using zetasql::test_values::Range;

namespace {

bool HasDatetimeElementType(Value range) {
  return range.type()->AsRange()->element_type()->kind() ==
         TypeKind::TYPE_DATETIME;
}

std::vector<FunctionTestCall> WrapFeatures(
    const std::vector<FunctionTestCall>& tests) {
  std::vector<FunctionTestCall> wrapped_tests;
  wrapped_tests.reserve(tests.size());
  for (auto call : tests) {
    call.params = call.params.WrapWithFeature(FEATURE_RANGE_TYPE);
    if (HasDatetimeElementType(call.params.params().front()) ||
        HasDatetimeElementType(call.params.params().back())) {
      call.params = call.params.WrapWithFeature(FEATURE_V_1_2_CIVIL_TIME);
    }
    wrapped_tests.emplace_back(call);
  }
  return wrapped_tests;
}

std::vector<FunctionTestCall> EqualityTests(const Value& t1, const Value& t2,
                                            const Value& t3,
                                            const Value& unbounded,
                                            const Value& nullRange) {
  ZETASQL_CHECK(t1.LessThan(t2));
  ZETASQL_CHECK(t2.LessThan(t3));
  return {
      // Same ranges
      {"=", {Range(t1, t2), Range(t1, t2)}, Bool(true)},
      // Different ends
      {"=", {Range(t1, t3), Range(t1, t2)}, Bool(false)},
      // Different starts
      {"=", {Range(t1, t3), Range(t1, t2)}, Bool(false)},
      // Different starts and ends
      {"=", {Range(t1, t2), Range(t2, t3)}, Bool(false)},
      // Same ranges with unbounded start
      {"=", {Range(unbounded, t2), Range(unbounded, t2)}, Bool(true)},
      // Different ranges, one with unbounded start, same end date
      {"=", {Range(unbounded, t2), Range(t1, t2)}, Bool(false)},
      // Different ranges, both with unbounded start, different end date
      {"=", {Range(unbounded, t2), Range(unbounded, t3)}, Bool(false)},
      // Same ranges with unobunded end
      {"=", {Range(t1, unbounded), Range(t1, unbounded)}, Bool(true)},
      // Different ranges, same start date, one with unbounded end
      {"=", {Range(t1, unbounded), Range(t1, t2)}, Bool(false)},
      // Different ranges, different start date, both with unbounded end
      {"=", {Range(t1, unbounded), Range(t2, unbounded)}, Bool(false)},
      // Same ranges, unbounded start and end
      {"=",
       {Range(unbounded, unbounded), Range(unbounded, unbounded)},
       Bool(true)},
      // Null compared to non-null
      {"=", {nullRange, Range(t1, t2)}, NullBool()},
  };
}

std::vector<FunctionTestCall> ComparisonTests(const Value& t1, const Value& t2,
                                              const Value& t3, const Value& t4,
                                              const Value& unbounded,
                                              const Value& nullRange) {
  // Verify provided values are of the same type
  // All values must be of the same type, coercison is not allowed
  ZETASQL_CHECK(t1.type_kind() == t2.type_kind());
  ZETASQL_CHECK(t2.type_kind() == t3.type_kind());
  ZETASQL_CHECK(t3.type_kind() == t4.type_kind());
  ZETASQL_CHECK(t4.type_kind() == unbounded.type_kind());
  ZETASQL_CHECK(nullRange.type_kind() == TypeKind::TYPE_RANGE);
  ZETASQL_CHECK(unbounded.type_kind() ==
        nullRange.type()->AsRange()->element_type()->kind());
  // Verify t1 < t2 < t3 < t4
  ZETASQL_CHECK(t1.LessThan(t2));
  ZETASQL_CHECK(t2.LessThan(t3));
  ZETASQL_CHECK(t3.LessThan(t4));
  // Check nulls are nulls indeed
  ZETASQL_CHECK(unbounded.is_null());
  ZETASQL_CHECK(nullRange.is_null());
  // We only add tests for "=" and "<", because the test driver automatically
  // generates all comparison functions for every test case.
  return {
      // Regular ranges
      // [___)........
      // ........[___)
      {"<", {Range(t1, t2), Range(t3, t4)}, Bool(true)},
      // [___)........
      // ....[_______)
      {"<", {Range(t1, t2), Range(t2, t4)}, Bool(true)},
      // [_______)....
      // ....[_______)
      {"<", {Range(t1, t3), Range(t2, t4)}, Bool(true)},
      // [_______)....
      // ....[___)....
      {"<", {Range(t1, t3), Range(t2, t3)}, Bool(true)},
      // [___________)
      // ....[___)....
      {"<", {Range(t1, t4), Range(t2, t3)}, Bool(true)},
      // [___________)
      // ....[___)....
      {"<", {Range(t1, t4), Range(t2, t3)}, Bool(true)},
      // ....[_______)
      // ....[___)....
      {"<", {Range(t2, t4), Range(t2, t3)}, Bool(false)},
      // ....[___)....
      // ....[___)....
      {"<", {Range(t2, t3), Range(t2, t3)}, Bool(false)},
      // ....[_______)
      // [_______)....
      {"<", {Range(t2, t4), Range(t1, t3)}, Bool(false)},
      // ........[___)
      // [_______)....
      {"<", {Range(t3, t4), Range(t1, t3)}, Bool(false)},
      // ........[___)
      // [___)........
      {"<", {Range(t3, t4), Range(t1, t2)}, Bool(false)},
      // Ranges with unbounded start
      // ____)........
      // ........[___)
      {"<", {Range(unbounded, t2), Range(t3, t4)}, Bool(true)},
      // ____)........
      // ....[___)....
      {"<", {Range(unbounded, t2), Range(t2, t3)}, Bool(true)},
      // ________)....
      // ....[_______)
      {"<", {Range(unbounded, t3), Range(t2, t4)}, Bool(true)},
      // ________)....
      // ....[___)....
      {"<", {Range(unbounded, t3), Range(t2, t3)}, Bool(true)},
      // ____________)
      // ....[___)....
      {"<", {Range(unbounded, t4), Range(t2, t3)}, Bool(true)},
      // Ranges with unbounded end
      // [____________
      // ....[___)....
      {"<", {Range(t1, unbounded), Range(t2, t3)}, Bool(true)},
      // ....[________
      // ....[___)....
      {"<", {Range(t2, unbounded), Range(t2, t3)}, Bool(false)},
      // ........[____
      // ....[_______)
      {"<", {Range(t3, unbounded), Range(t2, t4)}, Bool(false)},
      // ........[____
      // ....[___)....
      {"<", {Range(t3, unbounded), Range(t2, t3)}, Bool(false)},
      // ........[____
      // [___)........
      {"<", {Range(t3, unbounded), Range(t1, t2)}, Bool(false)},
      // Ranges with unbounded start and end
      // _____________
      // ....[___)....
      {"<", {Range(unbounded, unbounded), Range(t2, t3)}, Bool(true)},
      // _____________
      // ....[________
      {"<", {Range(unbounded, unbounded), Range(t2, unbounded)}, Bool(true)},
      // _____________
      // ___).........
      {"<", {Range(unbounded, unbounded), Range(unbounded, t2)}, Bool(false)},
      // _____________
      // _____________
      {"<",
       {Range(unbounded, unbounded), Range(unbounded, unbounded)},
       Bool(false)},
      // Null range
      {"<", {nullRange, Range(t2, t3)}, NullBool()},
  };
}

}  // namespace

std::vector<FunctionTestCall> GetFunctionTestsRangeComparisons() {
  const Value d1 = Value::Date(1);
  const Value d2 = Value::Date(2);
  const Value d3 = Value::Date(3);
  const Value d4 = Value::Date(4);
  const Value nullDate = Value::NullDate();
  const Value nullDateRange = Value::Null(types::DateRangeType());

  const Value dt1 =
      Value::Datetime(DatetimeValue::FromYMDHMSAndNanos(1, 1, 1, 1, 1, 1, 1));
  const Value dt2 =
      Value::Datetime(DatetimeValue::FromYMDHMSAndNanos(1, 1, 2, 1, 1, 1, 1));
  const Value dt3 =
      Value::Datetime(DatetimeValue::FromYMDHMSAndNanos(1, 1, 3, 1, 1, 1, 1));
  const Value dt4 =
      Value::Datetime(DatetimeValue::FromYMDHMSAndNanos(1, 1, 4, 1, 1, 1, 1));
  const Value nullDatetime = Value::NullDatetime();
  const Value nullDatetimeRange = Value::Null(types::DatetimeRangeType());

  const absl::TimeZone utc = absl::UTCTimeZone();
  const Value ts1 = Value::Timestamp(
      absl::FromCivil(absl::CivilSecond(1111, 01, 01, 01, 01, 01), utc));
  const Value ts2 = Value::Timestamp(
      absl::FromCivil(absl::CivilSecond(1111, 01, 02, 01, 01, 01), utc));
  const Value ts3 = Value::Timestamp(
      absl::FromCivil(absl::CivilSecond(1111, 01, 03, 01, 01, 01), utc));
  const Value ts4 = Value::Timestamp(
      absl::FromCivil(absl::CivilSecond(1111, 01, 04, 01, 01, 01), utc));
  const Value nullTimestamp = Value::NullTimestamp();
  const Value nullTimestampRange = Value::Null(types::TimestampRangeType());

  std::vector<FunctionTestCall> tests;
  std::vector<FunctionTestCall> date_eq_tests =
      EqualityTests(d1, d2, d3, nullDate, nullDateRange);
  tests.insert(tests.end(), date_eq_tests.begin(), date_eq_tests.end());
  std::vector<FunctionTestCall> datetime_eq_tests =
      EqualityTests(dt1, dt2, dt3, nullDatetime, nullDatetimeRange);
  tests.insert(tests.end(), datetime_eq_tests.begin(), datetime_eq_tests.end());
  std::vector<FunctionTestCall> ts_eq_tests =
      EqualityTests(ts1, ts2, ts3, nullTimestamp, nullTimestampRange);
  tests.insert(tests.end(), ts_eq_tests.begin(), ts_eq_tests.end());

  std::vector<FunctionTestCall> date_cmp_tests =
      ComparisonTests(d1, d2, d3, d4, nullDate, nullDateRange);
  tests.insert(tests.end(), date_cmp_tests.begin(), date_cmp_tests.end());
  std::vector<FunctionTestCall> datetime_cmp_tests =
      ComparisonTests(dt1, dt2, dt3, dt4, nullDatetime, nullDatetimeRange);
  tests.insert(tests.end(), datetime_cmp_tests.begin(),
               datetime_cmp_tests.end());
  std::vector<FunctionTestCall> ts_cmp_tests =
      ComparisonTests(ts1, ts2, ts3, ts4, nullTimestamp, nullTimestampRange);
  tests.insert(tests.end(), ts_cmp_tests.begin(), ts_cmp_tests.end());
  return WrapFeatures(tests);
}

}  // namespace zetasql
