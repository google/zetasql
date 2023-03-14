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

void CommonInitialCheck(const Value& t1, const Value& t2, const Value& t3,
                        const Value& t4, const Value& unbounded,
                        const Value& null_range) {
  // Verify provided values are of the same type
  // All values must be of the same type, coercion is not allowed
  ZETASQL_CHECK(t1.type_kind() == t2.type_kind());
  ZETASQL_CHECK(t2.type_kind() == t3.type_kind());
  ZETASQL_CHECK(t3.type_kind() == t4.type_kind());
  ZETASQL_CHECK(t4.type_kind() == unbounded.type_kind());
  ZETASQL_CHECK(null_range.type_kind() == TypeKind::TYPE_RANGE);
  ZETASQL_CHECK(unbounded.type_kind() ==
        null_range.type()->AsRange()->element_type()->kind());
  // Verify t1 < t2 < t3 < t4
  ZETASQL_CHECK(t1.LessThan(t2));
  ZETASQL_CHECK(t2.LessThan(t3));
  ZETASQL_CHECK(t3.LessThan(t4));
  // Check nulls are nulls indeed
  ZETASQL_CHECK(unbounded.is_null());
  ZETASQL_CHECK(null_range.is_null());
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
                                            const Value& null_range) {
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
      {"=", {null_range, Range(t1, t2)}, NullBool()},
  };
}

std::vector<FunctionTestCall> ComparisonTests(const Value& t1, const Value& t2,
                                              const Value& t3, const Value& t4,
                                              const Value& unbounded,
                                              const Value& null_range) {
  CommonInitialCheck(t1, t2, t3, t4, unbounded, null_range);
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
      {"<", {null_range, Range(t2, t3)}, NullBool()},
  };
}

std::vector<FunctionTestCall> RangeOverlapsTests(
    const Value& t1, const Value& t2, const Value& t3, const Value& t4,
    const Value& unbounded, const Value& null_range) {
  // Verify t1 < t2 < t3 < t4 and provided values are of the same type
  CommonInitialCheck(t1, t2, t3, t4, unbounded, null_range);

  return {
      // Regular ranges
      // [___)........
      // ........[___)
      {"range_overlaps", {Range(t1, t2), Range(t3, t4)}, Bool(false)},
      // [___)........
      // ....[_______)
      {"range_overlaps", {Range(t1, t2), Range(t2, t4)}, Bool(false)},
      // [_______)....
      // ....[_______)
      {"range_overlaps", {Range(t1, t3), Range(t2, t4)}, Bool(true)},
      // [_______)....
      // ....[___)....
      {"range_overlaps", {Range(t1, t3), Range(t2, t3)}, Bool(true)},
      // [___________)
      // ....[___)....
      {"range_overlaps", {Range(t1, t4), Range(t2, t3)}, Bool(true)},
      // ....[_______)
      // ....[___)....
      {"range_overlaps", {Range(t2, t4), Range(t2, t3)}, Bool(true)},
      // ....[___)....
      // ....[___)....
      {"range_overlaps", {Range(t2, t3), Range(t2, t3)}, Bool(true)},
      // ....[_______)
      // [_______)....
      {"range_overlaps", {Range(t2, t4), Range(t1, t3)}, Bool(true)},
      // ........[___)
      // [_______)....
      {"range_overlaps", {Range(t3, t4), Range(t1, t3)}, Bool(false)},
      // ........[___)
      // [___)........
      {"range_overlaps", {Range(t3, t4), Range(t1, t2)}, Bool(false)},
      // First Range with unbounded start
      // ___)........
      // ........[___)
      {"range_overlaps", {Range(unbounded, t2), Range(t3, t4)}, Bool(false)},
      // ____)........
      // ....[_______)
      {"range_overlaps", {Range(unbounded, t2), Range(t2, t4)}, Bool(false)},
      // _______)....
      // ....[_______)
      {"range_overlaps", {Range(unbounded, t3), Range(t2, t4)}, Bool(true)},
      // ________)....
      // ....[___)....
      {"range_overlaps", {Range(unbounded, t3), Range(t2, t3)}, Bool(true)},
      // ___________)
      // ....[___)....
      {"range_overlaps", {Range(unbounded, t4), Range(t2, t3)}, Bool(true)},
      // ____________)
      // [_______)....
      {"range_overlaps", {Range(unbounded, t4), Range(t1, t3)}, Bool(true)},
      // ____________)
      // [___)........
      {"range_overlaps", {Range(unbounded, t4), Range(t1, t2)}, Bool(true)},
      // First Range with unbounded end
      // [___________
      // ........[___)
      {"range_overlaps", {Range(t1, unbounded), Range(t3, t4)}, Bool(true)},
      // ....[________
      // ....[___)....
      {"range_overlaps", {Range(t2, unbounded), Range(t2, t3)}, Bool(true)},
      // ....[________
      // [_______)....
      {"range_overlaps", {Range(t2, unbounded), Range(t1, t3)}, Bool(true)},
      // ........[____
      // [_______)....
      {"range_overlaps", {Range(t3, unbounded), Range(t1, t3)}, Bool(false)},
      // ........[____
      // [___)........
      {"range_overlaps", {Range(t3, unbounded), Range(t1, t2)}, Bool(false)},
      // Second Range with unbounded start
      // [___)........
      // ____________)
      {"range_overlaps", {Range(t1, t2), Range(unbounded, t4)}, Bool(true)},
      // [_______)....
      //  _______)....
      {"range_overlaps", {Range(t1, t3), Range(unbounded, t3)}, Bool(true)},
      // [___________)
      //  ________)...
      {"range_overlaps", {Range(t1, t4), Range(unbounded, t3)}, Bool(true)},
      // ....[_______)
      // ________)....
      {"range_overlaps", {Range(t2, t4), Range(unbounded, t3)}, Bool(true)},
      // ........[___)
      // ________)....
      {"range_overlaps", {Range(t3, t4), Range(unbounded, t3)}, Bool(false)},
      // ........[___)
      // ____)........
      {"range_overlaps", {Range(t3, t4), Range(unbounded, t2)}, Bool(false)},
      // Second Range with unbounded end
      // [___)........
      // ........[____
      {"range_overlaps", {Range(t1, t2), Range(t3, unbounded)}, Bool(false)},
      // [___)........
      // ....[________
      {"range_overlaps", {Range(t1, t2), Range(t2, unbounded)}, Bool(false)},
      // [_______)....
      // ....[________
      {"range_overlaps", {Range(t1, t3), Range(t2, unbounded)}, Bool(true)},
      // ....[_______)
      // ....[_______
      {"range_overlaps", {Range(t2, t4), Range(t2, unbounded)}, Bool(true)},
      // ....[_______)
      // [___________
      {"range_overlaps", {Range(t2, t4), Range(t1, unbounded)}, Bool(true)},
      // First Range with unbounded start and Second Range with unbounded start
      // ___)........
      // ___________)
      {"range_overlaps",
       {Range(unbounded, t2), Range(unbounded, t4)},
       Bool(true)},
      // ________)....
      // ________)....
      {"range_overlaps",
       {Range(unbounded, t3), Range(unbounded, t3)},
       Bool(true)},
      // ____________)
      // _______)....
      {"range_overlaps",
       {Range(unbounded, t4), Range(unbounded, t3)},
       Bool(true)},
      // First Range with unbounded start and Second Range with unbounded end
      // ____)........
      // ........[____
      {"range_overlaps",
       {Range(unbounded, t2), Range(t3, unbounded)},
       Bool(false)},
      // ____)........
      // ....[________
      {"range_overlaps",
       {Range(unbounded, t2), Range(t2, unbounded)},
       Bool(false)},
      // ________)....
      // ....[________
      {"range_overlaps",
       {Range(unbounded, t3), Range(t2, unbounded)},
       Bool(true)},
      // ____________)
      // [___________
      {"range_overlaps",
       {Range(unbounded, t4), Range(t1, unbounded)},
       Bool(true)},
      // First Range with unbounded end and Second Range with unbounded start
      // [___________
      //  ___________)
      {"range_overlaps",
       {Range(t1, unbounded), Range(unbounded, t4)},
       Bool(true)},
      // ....[________
      // _______)....
      {"range_overlaps",
       {Range(t2, unbounded), Range(unbounded, t3)},
       Bool(true)},
      // ........[____
      // ________)....
      {"range_overlaps",
       {Range(t3, unbounded), Range(unbounded, t3)},
       Bool(false)},
      // ........[____
      // ____)........
      {"range_overlaps",
       {Range(t3, unbounded), Range(unbounded, t2)},
       Bool(false)},
      // First Range with unbounded end and Second Range with unbounded end
      // [___________
      // ........[___
      {"range_overlaps",
       {Range(t1, unbounded), Range(t3, unbounded)},
       Bool(true)},
      // ....[________
      // ....[________
      {"range_overlaps",
       {Range(t2, unbounded), Range(t2, unbounded)},
       Bool(true)},
      // ....[________
      // [____________
      {"range_overlaps",
       {Range(t2, unbounded), Range(t1, unbounded)},
       Bool(true)},
      // First Range with unbounded start and end
      // _____________
      // ....[___)....
      {"range_overlaps",
       {Range(unbounded, unbounded), Range(t2, t3)},
       Bool(true)},
      // _____________
      // ....[________
      {"range_overlaps",
       {Range(unbounded, unbounded), Range(t2, unbounded)},
       Bool(true)},
      // _____________
      // ___).........
      {"range_overlaps",
       {Range(unbounded, unbounded), Range(unbounded, t2)},
       Bool(true)},
      // Second Range with unbounded start and end
      // ....[___)....
      // _____________
      {"range_overlaps",
       {Range(t2, t3), Range(unbounded, unbounded)},
       Bool(true)},
      // ....[________
      // _____________
      {"range_overlaps",
       {Range(t2, unbounded), Range(unbounded, unbounded)},
       Bool(true)},
      // ___).........
      // _____________
      {"range_overlaps",
       {Range(unbounded, t2), Range(unbounded, unbounded)},
       Bool(true)},
      // Both Range have unbounded start and end
      // _____________
      // _____________
      {"range_overlaps",
       {Range(unbounded, unbounded), Range(unbounded, unbounded)},
       Bool(true)},
      // Null Ranges
      {"range_overlaps", {null_range, Range(t2, t3)}, NullBool()},
      {"range_overlaps", {null_range, Range(unbounded, t3)}, NullBool()},
      {"range_overlaps", {null_range, Range(t2, unbounded)}, NullBool()},
      {"range_overlaps", {null_range, Range(unbounded, unbounded)}, NullBool()},
      {"range_overlaps", {Range(t2, t3), null_range}, NullBool()},
      {"range_overlaps", {Range(unbounded, t3), null_range}, NullBool()},
      {"range_overlaps", {Range(t2, unbounded), null_range}, NullBool()},
      {"range_overlaps", {Range(unbounded, unbounded), null_range}, NullBool()},
      {"range_overlaps", {null_range, null_range}, NullBool()},
  };
}

std::vector<FunctionTestCall> RangeIntersectTests(
    const Value& t1, const Value& t2, const Value& t3, const Value& t4,
    const Value& unbounded, const Value& null_range) {
  // Verify t1 < t2 < t3 < t4 and provided values are of the same type
  CommonInitialCheck(t1, t2, t3, t4, unbounded, null_range);

  return {
      // Regular ranges
      // [_______)....
      // ....[_______)
      {"range_intersect", {Range(t1, t3), Range(t2, t4)}, Range(t2, t3)},
      // [_______)....
      // ....[___)....
      {"range_intersect", {Range(t1, t3), Range(t2, t3)}, Range(t2, t3)},
      // [___________)
      // ....[___)....
      {"range_intersect", {Range(t1, t4), Range(t2, t3)}, Range(t2, t3)},
      // ....[_______)
      // ....[___)....
      {"range_intersect", {Range(t2, t4), Range(t2, t3)}, Range(t2, t3)},
      // ....[___)....
      // ....[___)....
      {"range_intersect", {Range(t2, t3), Range(t2, t3)}, Range(t2, t3)},
      // ....[_______)
      // [_______)....
      {"range_intersect", {Range(t2, t4), Range(t1, t3)}, Range(t2, t3)},
      // First Range with unbounded start
      // _______)....
      // ....[_______)
      {"range_intersect", {Range(unbounded, t3), Range(t2, t4)}, Range(t2, t3)},
      // ________)....
      // ....[___)....
      {"range_intersect", {Range(unbounded, t3), Range(t2, t3)}, Range(t2, t3)},
      // ___________)
      // ....[___)....
      {"range_intersect", {Range(unbounded, t4), Range(t2, t3)}, Range(t2, t3)},
      // ____________)
      // [_______)....
      {"range_intersect", {Range(unbounded, t4), Range(t1, t3)}, Range(t1, t3)},
      // ____________)
      // [___)........
      {"range_intersect", {Range(unbounded, t4), Range(t1, t2)}, Range(t1, t2)},
      // First Range with unbounded end
      // [___________
      // ........[___)
      {"range_intersect", {Range(t1, unbounded), Range(t3, t4)}, Range(t3, t4)},
      // ....[________
      // ....[___)....
      {"range_intersect", {Range(t2, unbounded), Range(t2, t3)}, Range(t2, t3)},
      // ....[________
      // [_______)....
      {"range_intersect", {Range(t2, unbounded), Range(t1, t3)}, Range(t2, t3)},
      // Second Range with unbounded start
      // [___)........
      // ____________)
      {"range_intersect", {Range(t1, t2), Range(unbounded, t4)}, Range(t1, t2)},
      // [_______)....
      //  _______)....
      {"range_intersect", {Range(t1, t3), Range(unbounded, t3)}, Range(t1, t3)},
      // [___________)
      //  ________)...
      {"range_intersect", {Range(t1, t4), Range(unbounded, t3)}, Range(t1, t3)},
      // ....[_______)
      // ________)....
      {"range_intersect", {Range(t2, t4), Range(unbounded, t3)}, Range(t2, t3)},
      // Second Range with unbounded end
      // [_______)....
      // ....[________
      {"range_intersect", {Range(t1, t3), Range(t2, unbounded)}, Range(t2, t3)},
      // ....[_______)
      // ....[_______
      {"range_intersect", {Range(t2, t4), Range(t2, unbounded)}, Range(t2, t4)},
      // ....[_______)
      // [___________
      {"range_intersect", {Range(t2, t4), Range(t1, unbounded)}, Range(t2, t4)},
      // First Range with unbounded start and Second Range with unbounded start
      // ___)........
      // ___________)
      {"range_intersect",
       {Range(unbounded, t2), Range(unbounded, t4)},
       Range(unbounded, t2)},
      // ________)....
      // ________)....
      {"range_intersect",
       {Range(unbounded, t3), Range(unbounded, t3)},
       Range(unbounded, t3)},
      // ____________)
      // _______)....
      {"range_intersect",
       {Range(unbounded, t4), Range(unbounded, t3)},
       Range(unbounded, t3)},
      // First Range with unbounded start and Second Range with unbounded end
      // ________)....
      // ....[________
      {"range_intersect",
       {Range(unbounded, t3), Range(t2, unbounded)},
       Range(t2, t3)},
      // ____________)
      // [___________
      {"range_intersect",
       {Range(unbounded, t4), Range(t1, unbounded)},
       Range(t1, t4)},
      // First Range with unbounded end and Second Range with unbounded start
      // [___________
      //  ___________)
      {"range_intersect",
       {Range(t1, unbounded), Range(unbounded, t4)},
       Range(t1, t4)},
      // ....[________
      // _______)....
      {"range_intersect",
       {Range(t2, unbounded), Range(unbounded, t3)},
       Range(t2, t3)},
      // First Range with unbounded end and Second Range with unbounded end
      // [___________
      // ........[___
      {"range_intersect",
       {Range(t1, unbounded), Range(t3, unbounded)},
       Range(t3, unbounded)},
      // ....[________
      // ....[________
      {"range_intersect",
       {Range(t2, unbounded), Range(t2, unbounded)},
       Range(t2, unbounded)},
      // ....[________
      // [____________
      {"range_intersect",
       {Range(t2, unbounded), Range(t1, unbounded)},
       Range(t2, unbounded)},
      // First Range with unbounded start and end
      // _____________
      // ....[___)....
      {"range_intersect",
       {Range(unbounded, unbounded), Range(t2, t3)},
       Range(t2, t3)},
      // _____________
      // ....[________
      {"range_intersect",
       {Range(unbounded, unbounded), Range(t2, unbounded)},
       Range(t2, unbounded)},
      // _____________
      // ___).........
      {"range_intersect",
       {Range(unbounded, unbounded), Range(unbounded, t2)},
       Range(unbounded, t2)},
      // Second Range with unbounded start and end
      // ....[___)....
      // _____________
      {"range_intersect",
       {Range(t2, t3), Range(unbounded, unbounded)},
       Range(t2, t3)},
      // ....[________
      // _____________
      {"range_intersect",
       {Range(t2, unbounded), Range(unbounded, unbounded)},
       Range(t2, unbounded)},
      // ___).........
      // _____________
      {"range_intersect",
       {Range(unbounded, t2), Range(unbounded, unbounded)},
       Range(unbounded, t2)},
      // Both Ranges have unbounded start and end
      // _____________
      // _____________
      {"range_intersect",
       {Range(unbounded, unbounded), Range(unbounded, unbounded)},
       Range(unbounded, unbounded)},
      // Null Ranges
      {"range_intersect", {null_range, Range(t2, t3)}, null_range},
      {"range_intersect", {null_range, Range(unbounded, t3)}, null_range},
      {"range_intersect", {null_range, Range(t2, unbounded)}, null_range},
      {"range_intersect",
       {null_range, Range(unbounded, unbounded)},
       null_range},
      {"range_intersect", {Range(t2, t3), null_range}, null_range},
      {"range_intersect", {Range(unbounded, t3), null_range}, null_range},
      {"range_intersect", {Range(t2, unbounded), null_range}, null_range},
      {"range_intersect",
       {Range(unbounded, unbounded), null_range},
       null_range},
      {"range_intersect", {null_range, null_range}, null_range},
  };
}

}  // namespace

// Helper factory methods for RANGE tests
std::vector<Value> GetIncreasingDateValues() {
  return {Value::Date(1), Value::Date(2), Value::Date(3), Value::Date(4)};
}

std::vector<Value> GetIncreasingDatetimeValues() {
  return {
      Value::Datetime(DatetimeValue::FromYMDHMSAndNanos(1, 1, 1, 1, 1, 1, 1)),
      Value::Datetime(DatetimeValue::FromYMDHMSAndNanos(1, 1, 2, 1, 1, 1, 1)),
      Value::Datetime(DatetimeValue::FromYMDHMSAndNanos(1, 1, 3, 1, 1, 1, 1)),
      Value::Datetime(DatetimeValue::FromYMDHMSAndNanos(1, 1, 4, 1, 1, 1, 1))};
}

std::vector<Value> GetIncreasingTimestampValues() {
  const absl::TimeZone utc = absl::UTCTimeZone();
  return {Value::Timestamp(absl::FromCivil(
              absl::CivilSecond(1111, 01, 01, 01, 01, 01), utc)),
          Value::Timestamp(absl::FromCivil(
              absl::CivilSecond(1111, 01, 02, 01, 01, 01), utc)),
          Value::Timestamp(absl::FromCivil(
              absl::CivilSecond(1111, 01, 03, 01, 01, 01), utc)),
          Value::Timestamp(absl::FromCivil(
              absl::CivilSecond(1111, 01, 04, 01, 01, 01), utc))};
}

std::vector<FunctionTestCall> GetFunctionTestsRangeComparisons() {
  const std::vector<Value>& dates = GetIncreasingDateValues();
  const Value null_date = Value::NullDate();
  const Value null_date_range = Value::Null(types::DateRangeType());

  const std::vector<Value>& datetimes = GetIncreasingDatetimeValues();
  const Value null_datetime = Value::NullDatetime();
  const Value null_datetime_range = Value::Null(types::DatetimeRangeType());

  const std::vector<Value>& timestamps = GetIncreasingTimestampValues();
  const Value null_timestamp = Value::NullTimestamp();
  const Value null_timestamp_range = Value::Null(types::TimestampRangeType());

  std::vector<FunctionTestCall> tests;
  // equality tests
  std::vector<FunctionTestCall> date_eq_tests =
      EqualityTests(dates[0], dates[1], dates[2], null_date, null_date_range);
  tests.insert(tests.end(), date_eq_tests.begin(), date_eq_tests.end());
  std::vector<FunctionTestCall> datetime_eq_tests =
      EqualityTests(datetimes[0], datetimes[1], datetimes[2], null_datetime,
                    null_datetime_range);
  tests.insert(tests.end(), datetime_eq_tests.begin(), datetime_eq_tests.end());
  std::vector<FunctionTestCall> ts_eq_tests =
      EqualityTests(timestamps[0], timestamps[1], timestamps[2], null_timestamp,
                    null_timestamp_range);
  tests.insert(tests.end(), ts_eq_tests.begin(), ts_eq_tests.end());

  // comparison tests
  std::vector<FunctionTestCall> date_cmp_tests = ComparisonTests(
      dates[0], dates[1], dates[2], dates[3], null_date, null_date_range);
  tests.insert(tests.end(), date_cmp_tests.begin(), date_cmp_tests.end());
  std::vector<FunctionTestCall> datetime_cmp_tests =
      ComparisonTests(datetimes[0], datetimes[1], datetimes[2], datetimes[3],
                      null_datetime, null_datetime_range);
  tests.insert(tests.end(), datetime_cmp_tests.begin(),
               datetime_cmp_tests.end());
  std::vector<FunctionTestCall> ts_cmp_tests =
      ComparisonTests(timestamps[0], timestamps[1], timestamps[2],
                      timestamps[3], null_timestamp, null_timestamp_range);
  tests.insert(tests.end(), ts_cmp_tests.begin(), ts_cmp_tests.end());

  return WrapFeatures(tests);
}

std::vector<FunctionTestCall> GetFunctionTestsRangeOverlaps() {
  const std::vector<Value>& dates = GetIncreasingDateValues();
  const Value null_date = Value::NullDate();
  const Value null_date_range = Value::Null(types::DateRangeType());

  const std::vector<Value>& datetimes = GetIncreasingDatetimeValues();
  const Value null_datetime = Value::NullDatetime();
  const Value null_datetime_range = Value::Null(types::DatetimeRangeType());

  const std::vector<Value>& timestamps = GetIncreasingTimestampValues();
  const Value null_timestamp = Value::NullTimestamp();
  const Value null_timestamp_range = Value::Null(types::TimestampRangeType());

  // range_overlaps tests
  std::vector<FunctionTestCall> tests;
  std::vector<FunctionTestCall> date_overlaps_tests = RangeOverlapsTests(
      dates[0], dates[1], dates[2], dates[3], null_date, null_date_range);
  tests.insert(tests.end(), date_overlaps_tests.begin(),
               date_overlaps_tests.end());

  std::vector<FunctionTestCall> datetime_overlaps_tests =
      RangeOverlapsTests(datetimes[0], datetimes[1], datetimes[2], datetimes[3],
                         null_datetime, null_datetime_range);
  tests.insert(tests.end(), datetime_overlaps_tests.begin(),
               datetime_overlaps_tests.end());

  std::vector<FunctionTestCall> ts_overlaps_tests =
      RangeOverlapsTests(timestamps[0], timestamps[1], timestamps[2],
                         timestamps[3], null_timestamp, null_timestamp_range);
  tests.insert(tests.end(), ts_overlaps_tests.begin(), ts_overlaps_tests.end());

  return WrapFeatures(tests);
}

std::vector<FunctionTestCall> GetFunctionTestsRangeIntersect() {
  const std::vector<Value>& dates = GetIncreasingDateValues();
  const Value null_date = Value::NullDate();
  const Value null_date_range = Value::Null(types::DateRangeType());

  const std::vector<Value>& datetimes = GetIncreasingDatetimeValues();
  const Value null_datetime = Value::NullDatetime();
  const Value null_datetime_range = Value::Null(types::DatetimeRangeType());

  const std::vector<Value>& timestamps = GetIncreasingTimestampValues();
  const Value null_timestamp = Value::NullTimestamp();
  const Value null_timestamp_range = Value::Null(types::TimestampRangeType());

  // range_intersect tests
  std::vector<FunctionTestCall> tests;
  std::vector<FunctionTestCall> date_intersect_tests = RangeIntersectTests(
      dates[0], dates[1], dates[2], dates[3], null_date, null_date_range);
  tests.insert(tests.end(), date_intersect_tests.begin(),
               date_intersect_tests.end());

  std::vector<FunctionTestCall> datetime_intersect_tests =
      RangeIntersectTests(datetimes[0], datetimes[1], datetimes[2],
                          datetimes[3], null_datetime, null_datetime_range);
  tests.insert(tests.end(), datetime_intersect_tests.begin(),
               datetime_intersect_tests.end());

  std::vector<FunctionTestCall> ts_intersect_tests =
      RangeIntersectTests(timestamps[0], timestamps[1], timestamps[2],
                          timestamps[3], null_timestamp, null_timestamp_range);
  tests.insert(tests.end(), ts_intersect_tests.begin(),
               ts_intersect_tests.end());

  return WrapFeatures(tests);
}

}  // namespace zetasql
