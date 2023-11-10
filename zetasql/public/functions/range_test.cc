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

#include "zetasql/public/functions/range.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"  
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/interval_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/test_value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

class GetBoundariesTest : public ::testing::Test {
 public:
  void TestGetBoundaries(const absl::string_view input,
                         const std::optional<std::string>& expected_start,
                         const std::optional<std::string>& expected_end) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(const auto boundaries, ParseRangeBoundaries(input));

    EXPECT_EQ(expected_start, boundaries.start);
    EXPECT_EQ(expected_end, boundaries.end);
  }

  void TestGetBoundariesError(
      const absl::string_view input,
      const absl::string_view expected_error_message,
      absl::StatusCode expected_status_code = absl::StatusCode::kOutOfRange) {
    EXPECT_THAT(
        ParseRangeBoundaries(input),
        zetasql_base::testing::StatusIs(expected_status_code,
                                  testing::HasSubstr(expected_error_message)))
        << input;
  }
};

TEST_F(GetBoundariesTest, ValidRangeLiterals) {
  TestGetBoundaries("[2022-01-01, 2022-02-02)", "2022-01-01", "2022-02-02");
  TestGetBoundaries(
      "[2022-09-13 16:36:11.000000001, 2022-09-13 16:37:11.000000001)",
      "2022-09-13 16:36:11.000000001", "2022-09-13 16:37:11.000000001");
  TestGetBoundaries(
      "[0001-01-01 00:00:00+02, 9999-12-31 23:59:59.999999999+02)",
      "0001-01-01 00:00:00+02", "9999-12-31 23:59:59.999999999+02");
}

TEST_F(GetBoundariesTest, RangeLiteralsWithUnbounded) {
  TestGetBoundaries("[UNBOUNDED, 2022-02-02)", std::nullopt, "2022-02-02");
  TestGetBoundaries("[2022-09-13 16:36:11.000000001, unbounded)",
                    "2022-09-13 16:36:11.000000001", std::nullopt);
  TestGetBoundaries("[Unbounded, Unbounded)", std::nullopt, std::nullopt);
  TestGetBoundaries("[unbounded, UNBOUNDED)", std::nullopt, std::nullopt);
  TestGetBoundaries("[NULL, null)", std::nullopt, std::nullopt);
  TestGetBoundaries("[UnBoUnDeD, NuLl)", std::nullopt, std::nullopt);
}

TEST_F(GetBoundariesTest, RangeLiteralsWithNull) {
  // Test ranges with NULL. NULL could be used instead of UNBOUNDED
  TestGetBoundaries("[NULL, 2022-02-02)", std::nullopt, "2022-02-02");
  TestGetBoundaries("[2022-09-13 16:36:11.000000001, null)",
                    "2022-09-13 16:36:11.000000001", std::nullopt);
  TestGetBoundaries("[Null, Null)", std::nullopt, std::nullopt);
}

TEST_F(GetBoundariesTest, RangeLiteralsValueWithIncorrectNumberOfParts) {
  TestGetBoundariesError("[2022-01-01, 2022-02-02, 2022-03-02)",
                         R"(with two parts, START and END, divided with ", ")");
  TestGetBoundariesError("[)",
                         R"(with two parts, START and END, divided with ", ")");
  TestGetBoundariesError("[2022-01-01)",
                         R"(with two parts, START and END, divided with ", ")");
}

TEST_F(GetBoundariesTest,
       SemanticallyIncorrectButSyntacticallyCorrectRangeLiterals) {
  TestGetBoundaries("[01/01/2022, 02/02/2022)", "01/01/2022", "02/02/2022");
  TestGetBoundaries("[, )", "", "");
  TestGetBoundaries("[2022-01-01 00:00:00.0, )", "2022-01-01 00:00:00.0", "");
  TestGetBoundaries("[, 2022-01-01)", "", "2022-01-01");
  TestGetBoundaries("[0000-01-01, 0000-01-02)", "0000-01-01", "0000-01-02");
}

TEST_F(GetBoundariesTest, RangeLiteralsWithIncorrectBrackets) {
  TestGetBoundariesError("[2022-01-01, 2022-02-02]",
                         "formatted exactly as [START, END)");
  TestGetBoundariesError("(2022-01-01, 2022-02-02]",
                         "formatted exactly as [START, END)");
  TestGetBoundariesError("(2022-01-01, 2022-02-02)",
                         "formatted exactly as [START, END)");
  TestGetBoundariesError("2022-01-01, 2022-02-02)",
                         "formatted exactly as [START, END)");
  TestGetBoundariesError("[2022-01-01, 2022-02-02",
                         "formatted exactly as [START, END)");
}

TEST_F(GetBoundariesTest, GetBoundariesStrictFormattingSeparator) {
  // Range literal must have exactly one space after , and no other spaces
  TestGetBoundariesError("[2022-01-01,2022-02-02)",
                         R"(with two parts, START and END, divided with ", ")");

  // Unless it's overridden with the argument
  ZETASQL_ASSERT_OK_AND_ASSIGN(const auto boundaries,
                       ParseRangeBoundaries("[2022-01-01,2022-02-02)",
                                            /*strict_formatting=*/false));
  EXPECT_EQ("2022-01-01", boundaries.start);
  EXPECT_EQ("2022-02-02", boundaries.end);
}

TEST_F(GetBoundariesTest, GetBoundariesStrictFormattingLeadingOrTrailingSpace) {
  // Range literal start and end must not have any leading or trailing spaces
  TestGetBoundariesError("[ 2022-01-01, 2022-02-02)",
                         "START having no leading or trailing spaces");
  TestGetBoundariesError("[2022-01-01  , 2022-02-02)",
                         "START having no leading or trailing spaces");
  TestGetBoundariesError("[2022-01-01,  2022-02-02)",
                         "END having no leading or trailing spaces");
  TestGetBoundariesError("[2022-01-01, 2022-02-02 )",
                         "END having no leading or trailing spaces");

  // Unless it's overridden with the argument
  ZETASQL_ASSERT_OK_AND_ASSIGN(const auto boundaries,
                       ParseRangeBoundaries("[ 2022-01-01  ,  2022-02-02 )",
                                            /*strict_formatting=*/false));
  EXPECT_EQ("2022-01-01", boundaries.start);
  EXPECT_EQ("2022-02-02", boundaries.end);
}

template <typename T>
struct SerializeDeserializeRangeTestCase {
  std::optional<T> start;
  std::optional<T> end;
  size_t serialized_size;
};

// Used for printing test case results on a test failure.
template <typename T>
std::ostream& operator<<(std::ostream& os,
                         const SerializeDeserializeRangeTestCase<T>& param) {
  std::string start_str =
      param.start ? absl::StrCat(*param.start) : "UNBOUNDED";
  std::string end_str = param.end ? absl::StrCat(*param.end) : "UNBOUNDED";
  os << "[" << start_str << ", " << end_str
     << "), serialized_size = " << param.serialized_size;
  return os;
}

template <typename T>
void TestSerializeDeserializeRange(
    const SerializeDeserializeRangeTestCase<T>& test_case) {
  RangeBoundaries<T> range_boundaries = {test_case.start, test_case.end};
  std::string buffer;
  SerializeRangeAndAppendToBytes(range_boundaries, &buffer);
  size_t bytes_read;
  ZETASQL_ASSERT_OK_AND_ASSIGN(RangeBoundaries<T> decoded_range_boundaries,
                       DeserializeRangeFromBytes<T>(buffer, &bytes_read));
  EXPECT_EQ(bytes_read, test_case.serialized_size);
  ZETASQL_ASSERT_OK_AND_ASSIGN(size_t calculated_encoded_range_size,
                       GetEncodedRangeSize<T>(buffer));
  EXPECT_EQ(calculated_encoded_range_size, test_case.serialized_size);
  EXPECT_EQ(buffer.size(), test_case.serialized_size);

  EXPECT_EQ(range_boundaries.start, decoded_range_boundaries.start);
  EXPECT_EQ(range_boundaries.end, decoded_range_boundaries.end);

  // Now do the same but without bytes_read pointer.
  ZETASQL_ASSERT_OK_AND_ASSIGN(decoded_range_boundaries,
                       DeserializeRangeFromBytes<T>(buffer));
  EXPECT_EQ(range_boundaries.start, decoded_range_boundaries.start);
  EXPECT_EQ(range_boundaries.end, decoded_range_boundaries.end);
}

template <typename T>
void TestDeserializeRangeTooFewBytes(
    const SerializeDeserializeRangeTestCase<T>& test_case) {
  if (test_case.serialized_size == 1) return;

  RangeBoundaries<T> range_boundaries = {test_case.start, test_case.end};
  std::string serialized_range;
  SerializeRangeAndAppendToBytes(range_boundaries, &serialized_range);
  std::string serialized_range_header = serialized_range.substr(0, 1);
  EXPECT_THAT(DeserializeRangeFromBytes<T>(serialized_range_header),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  testing::HasSubstr(absl::StrFormat(
                      "Too few bytes to read RANGE content (needed %d; got 1)",
                      test_case.serialized_size))));
}

class SerializeDeserializeRangeInt32Test
    : public ::testing::TestWithParam<
          SerializeDeserializeRangeTestCase<int32_t>> {};

std::vector<SerializeDeserializeRangeTestCase<int32_t>>
GetSerializeDeserializeRangeInt32TestCases() {
  return {
      // Regular range.
      {
          .start = {1},
          .end = {2},
          .serialized_size = 9,
      },
      // Unbounded at end.
      {
          .start = {1},
          .end = {},
          .serialized_size = 5,
      },
      // Unbounded at start.
      {
          .start = {},
          .end = {2},
          .serialized_size = 5,
      },
      // Unbounded at start and end.
      {
          .start = {},
          .end = {},
          .serialized_size = 1,
      },
      // Very small values.
      {
          .start = {std::numeric_limits<int32_t>::min()},
          .end = {std::numeric_limits<int32_t>::min() + 1},
          .serialized_size = 9,
      },
      // Very large values.
      {
          .start = {std::numeric_limits<int32_t>::max() - 1},
          .end = {std::numeric_limits<int32_t>::max()},
          .serialized_size = 9,
      },
      // Very small start and very large end.
      {
          .start = {std::numeric_limits<int32_t>::min()},
          .end = {std::numeric_limits<int32_t>::max()},
          .serialized_size = 9,
      },
      // Mix of 0 and 1 bits.
      {
          .start = {-1431655766},
          .end = {1431655765},
          .serialized_size = 9,
      },
  };
}

INSTANTIATE_TEST_SUITE_P(
    SerializeDeserializeRangeInt32Tests, SerializeDeserializeRangeInt32Test,
    ::testing::ValuesIn(GetSerializeDeserializeRangeInt32TestCases()));

TEST_P(SerializeDeserializeRangeInt32Test, SerializeDeserializeSucceeds) {
  TestSerializeDeserializeRange(GetParam());
}

TEST_P(SerializeDeserializeRangeInt32Test, TooFewBytes) {
  TestDeserializeRangeTooFewBytes(GetParam());
}

TEST(SerializeDeserializeRangeInt32Test, DeserializeEmptyFails) {
  EXPECT_THAT(
      DeserializeRangeFromBytes<int32_t>(""),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::HasSubstr(
              "Too few bytes to read RANGE content (needed at least 1)")));
}

class SerializeDeserializeRangeInt64Test
    : public ::testing::TestWithParam<
          SerializeDeserializeRangeTestCase<int64_t>> {};

std::vector<SerializeDeserializeRangeTestCase<int64_t>>
GetSerializeDeserializeRangeInt64TestCases() {
  return {
      // Regular range.
      {
          .start = {1},
          .end = {2},
          .serialized_size = 17,
      },
      // Unbounded at end.
      {
          .start = {1},
          .end = {},
          .serialized_size = 9,
      },
      // Unbounded at start.
      {
          .start = {},
          .end = {2},
          .serialized_size = 9,
      },
      // Unbounded at start and end.
      {
          .start = {},
          .end = {},
          .serialized_size = 1,
      },
      // Very small values.
      {
          .start = {std::numeric_limits<int64_t>::min()},
          .end = {std::numeric_limits<int64_t>::min() + 1},
          .serialized_size = 17,
      },
      // Very large values.
      {
          .start = {std::numeric_limits<int64_t>::max() - 1},
          .end = {std::numeric_limits<int64_t>::max()},
          .serialized_size = 17,
      },
      // Very small start and very large end.
      {
          .start = {std::numeric_limits<int64_t>::min()},
          .end = {std::numeric_limits<int64_t>::max()},
          .serialized_size = 17,
      },
      // Mix of 0 and 1 bits.
      {
          .start = {-6148914691236517206},
          .end = {6148914691236517205},
          .serialized_size = 17,
      },
  };
}

INSTANTIATE_TEST_SUITE_P(
    SerializeDeserializeRangeInt64Tests, SerializeDeserializeRangeInt64Test,
    ::testing::ValuesIn(GetSerializeDeserializeRangeInt64TestCases()));

TEST_P(SerializeDeserializeRangeInt64Test, SerializeDeserializeSucceeds) {
  TestSerializeDeserializeRange(GetParam());
}

TEST_P(SerializeDeserializeRangeInt64Test, TooFewBytes) {
  TestDeserializeRangeTooFewBytes(GetParam());
}

TEST(SerializeDeserializeRangeInt64Test, DeserializeEmptyFails) {
  EXPECT_THAT(
      DeserializeRangeFromBytes<int64_t>(""),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::HasSubstr(
              "Too few bytes to read RANGE content (needed at least 1)")));
}

}  // namespace
}  // namespace zetasql

namespace zetasql {
namespace functions {
namespace {

class TimestampRangeArrayGeneratorTest
    : public ::testing::TestWithParam<FunctionTestCall> {};

TEST_P(TimestampRangeArrayGeneratorTest, TestCreateAndGenerate) {
  const FunctionTestCall& test = GetParam();
  ASSERT_EQ(test.function_name, "generate_range_array");
  ASSERT_EQ(test.params.num_params(), 3);
  ASSERT_FALSE(test.params.param(0).is_null());
  ASSERT_FALSE(test.params.param(1).is_null());
  ASSERT_FALSE(test.params.param(2).is_null());

  Value input_range_value = test.params.param(0);
  Value start_value = input_range_value.start();
  Value end_value = input_range_value.end();
  IntervalValue step = test.params.param(1).interval_value();
  bool last_partial_range = test.params.param(2).bool_value();

  TimestampScale scale =
      zetasql_base::ContainsKey(test.params.required_features(), FEATURE_TIMESTAMP_NANOS)
          ? kNanoseconds
          : kMicroseconds;

  std::optional<absl::Time> range_start =
      start_value.is_null() ? std::nullopt
                            : std::make_optional(start_value.ToTime());
  std::optional<absl::Time> range_end =
      end_value.is_null() ? std::nullopt
                          : std::make_optional(end_value.ToTime());

  absl::StatusOr<TimestampRangeArrayGenerator> generator =
      TimestampRangeArrayGenerator::Create(step, last_partial_range, scale);
  std::vector<Value> results;
  const auto& emitter = [&results](absl::Time start,
                                   absl::Time end) -> absl::Status {
    Value start_value = Value::Timestamp(start);
    Value end_value = Value::Timestamp(end);
    ZETASQL_ASSIGN_OR_RETURN(Value range_value,
                     Value::MakeRange(start_value, end_value));
    results.push_back(range_value);
    return absl::OkStatus();
  };

  absl::Status status;
  if (generator.ok()) {
    status.Update(generator->Generate(range_start, range_end, emitter));
  } else {
    status.Update(generator.status());
  }

  if (test.params.status().ok()) {
    // status OK case
    ZETASQL_EXPECT_OK(status);
    Value result_array =
        Value::MakeArray(
            test_values::MakeArrayType(types::TimestampRangeType()), results)
            .value();
    EXPECT_EQ(test.params.result(), result_array);
  } else {
    // error case
    EXPECT_EQ(test.params.status(), status);
  }
}

INSTANTIATE_TEST_SUITE_P(
    TimestampRangeArrayGeneratorTests, TimestampRangeArrayGeneratorTest,
    ::testing::ValuesIn(GetFunctionTestsGenerateTimestampRangeArray()));

TEST(TimestampRangeArrayGeneratorCreateTest, InvalidTimestampScale) {
  EXPECT_THAT(TimestampRangeArrayGenerator::Create(
                  IntervalValue::FromMicros(10).value(),
                  /*last_partial_range=*/false, kSeconds),
              ::zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(TimestampRangeArrayGenerator::Create(
                  IntervalValue::FromMicros(10).value(),
                  /*last_partial_range=*/false, kMilliseconds),
              ::zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST(TimestampRangeArrayGeneratorGenerateTest, EmitterReturnsError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(TimestampRangeArrayGenerator generator,
                       TimestampRangeArrayGenerator::Create(
                           IntervalValue::FromMicros(10).value(),
                           /*last_partial_range=*/false, kMicroseconds));

  int num_emitter_calls = 0;
  auto error_emitter = [&num_emitter_calls](absl::Time start,
                                            absl::Time end) -> absl::Status {
    num_emitter_calls++;
    return absl::InvalidArgumentError("test error");
  };

  std::optional<absl::Time> range_start = absl::FromUnixMicros(10);
  std::optional<absl::Time> range_end = absl::FromUnixMicros(100);

  EXPECT_THAT(generator.Generate(range_start, range_end, error_emitter),
              ::zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument,
                                          "test error"));
  // Verify that returning an error from the emitter immediately terminated
  // the process.
  EXPECT_EQ(num_emitter_calls, 1);
}

class DateRangeArrayGeneratorTest
    : public ::testing::TestWithParam<FunctionTestCall> {};

TEST_P(DateRangeArrayGeneratorTest, TestCreateAndGenerate) {
  const FunctionTestCall& test = GetParam();
  ASSERT_EQ(test.function_name, "generate_range_array");
  ASSERT_EQ(test.params.num_params(), 3);
  ASSERT_FALSE(test.params.param(0).is_null());
  ASSERT_FALSE(test.params.param(1).is_null());
  ASSERT_FALSE(test.params.param(2).is_null());

  Value input_range_value = test.params.param(0);
  Value start_value = input_range_value.start();
  Value end_value = input_range_value.end();
  IntervalValue step = test.params.param(1).interval_value();
  bool last_partial_range = test.params.param(2).bool_value();

  std::optional<int32_t> range_start =
      start_value.is_null() ? std::nullopt
                            : std::make_optional(start_value.date_value());
  std::optional<int32_t> range_end =
      end_value.is_null() ? std::nullopt
                          : std::make_optional(end_value.date_value());

  absl::StatusOr<DateRangeArrayGenerator> generator =
      DateRangeArrayGenerator::Create(step, last_partial_range);
  std::vector<Value> results;
  const auto& emitter = [&results](int32_t start, int32_t end) -> absl::Status {
    Value start_value = Value::Date(start);
    Value end_value = Value::Date(end);
    ZETASQL_ASSIGN_OR_RETURN(Value range_value,
                     Value::MakeRange(start_value, end_value));
    results.push_back(range_value);
    return absl::OkStatus();
  };

  absl::Status status;
  if (generator.ok()) {
    status.Update(generator->Generate(range_start, range_end, emitter));
  } else {
    status.Update(generator.status());
  }

  if (test.params.status().ok()) {
    // status OK case
    ZETASQL_EXPECT_OK(status);
    Value result_array =
        Value::MakeArray(test_values::MakeArrayType(types::DateRangeType()),
                         results)
            .value();
    EXPECT_EQ(test.params.result(), result_array);
  } else {
    // error case
    EXPECT_EQ(test.params.status(), status);
  }
}

INSTANTIATE_TEST_SUITE_P(
    DateRangeArrayGeneratorTests, DateRangeArrayGeneratorTest,
    ::testing::ValuesIn(GetFunctionTestsGenerateDateRangeArray()));

TEST(DateRangeArrayGeneratorGenerateTest, EmitterReturnsError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      DateRangeArrayGenerator generator,
      DateRangeArrayGenerator::Create(IntervalValue::FromDays(10).value(),
                                      /*last_partial_range=*/false));

  int num_emitter_calls = 0;
  auto error_emitter = [&num_emitter_calls](int32_t start,
                                            int32_t end) -> absl::Status {
    num_emitter_calls++;
    return absl::InvalidArgumentError("test error");
  };

  std::optional<int32_t> range_start = std::optional<int32_t>(10);
  std::optional<int32_t> range_end = std::optional<int32_t>(100);

  EXPECT_THAT(generator.Generate(range_start, range_end, error_emitter),
              ::zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument,
                                          "test error"));
  // Verify that returning an error from the emitter immediately terminated
  // the process.
  EXPECT_EQ(num_emitter_calls, 1);
}

class DatetimeRangeArrayGeneratorTest
    : public ::testing::TestWithParam<FunctionTestCall> {};

TEST_P(DatetimeRangeArrayGeneratorTest, TestCreateAndGenerate) {
  const FunctionTestCall& test = GetParam();
  ASSERT_EQ(test.function_name, "generate_range_array");
  ASSERT_EQ(test.params.num_params(), 3);
  ASSERT_FALSE(test.params.param(0).is_null());
  ASSERT_FALSE(test.params.param(1).is_null());
  ASSERT_FALSE(test.params.param(2).is_null());

  Value input_range_value = test.params.param(0);
  Value start_value = input_range_value.start();
  Value end_value = input_range_value.end();
  IntervalValue step = test.params.param(1).interval_value();
  bool last_partial_range = test.params.param(2).bool_value();

  TimestampScale scale =
      zetasql_base::ContainsKey(test.params.required_features(), FEATURE_TIMESTAMP_NANOS)
          ? kNanoseconds
          : kMicroseconds;

  std::optional<DatetimeValue> range_start =
      start_value.is_null() ? std::nullopt
                            : std::make_optional(start_value.datetime_value());
  std::optional<DatetimeValue> range_end =
      end_value.is_null() ? std::nullopt
                          : std::make_optional(end_value.datetime_value());

  absl::StatusOr<DatetimeRangeArrayGenerator> generator =
      DatetimeRangeArrayGenerator::Create(step, last_partial_range, scale);
  std::vector<Value> results;
  const auto& emitter = [&results](DatetimeValue start,
                                   DatetimeValue end) -> absl::Status {
    Value start_value = Value::Datetime(start);
    Value end_value = Value::Datetime(end);
    ZETASQL_ASSIGN_OR_RETURN(Value range_value,
                     Value::MakeRange(start_value, end_value));
    results.push_back(range_value);
    return absl::OkStatus();
  };

  absl::Status status;
  if (generator.ok()) {
    status.Update(generator->Generate(range_start, range_end, emitter));
  } else {
    status.Update(generator.status());
  }

  if (test.params.status().ok()) {
    // status OK case
    ZETASQL_EXPECT_OK(status);
    Value result_array =
        Value::MakeArray(test_values::MakeArrayType(types::DatetimeRangeType()),
                         results)
            .value();
    EXPECT_EQ(test.params.result(), result_array);
  } else {
    // error case
    EXPECT_EQ(test.params.status(), status);
  }
}

INSTANTIATE_TEST_SUITE_P(
    DatetimeRangeArrayGeneratorTests, DatetimeRangeArrayGeneratorTest,
    ::testing::ValuesIn(GetFunctionTestsGenerateDatetimeRangeArray()));

TEST(DatetimeRangeArrayGeneratorCreateTest, InvalidTimestampScale) {
  EXPECT_THAT(DatetimeRangeArrayGenerator::Create(
                  IntervalValue::FromDays(10).value(),
                  /*last_partial_range=*/false, kSeconds),
              ::zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(DatetimeRangeArrayGenerator::Create(
                  IntervalValue::FromDays(10).value(),
                  /*last_partial_range=*/false, kMilliseconds),
              ::zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST(DatetimeRangeArrayGeneratorGenerateTest, EmitterReturnsError) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(DatetimeRangeArrayGenerator generator,
                       DatetimeRangeArrayGenerator::Create(
                           IntervalValue::FromDays(10).value(),
                           /*last_partial_range=*/false, kMicroseconds));

  int num_emitter_calls = 0;
  auto error_emitter = [&num_emitter_calls](DatetimeValue start,
                                            DatetimeValue end) -> absl::Status {
    num_emitter_calls++;
    return absl::InvalidArgumentError("test error");
  };

  std::optional<DatetimeValue> range_start = std::optional<DatetimeValue>(
      DatetimeValue::FromYMDHMSAndMicros(2023, 5, 1, 1, 1, 1, 1));
  std::optional<DatetimeValue> range_end = std::optional<DatetimeValue>(
      DatetimeValue::FromYMDHMSAndMicros(2023, 6, 1, 1, 1, 1, 1));

  EXPECT_THAT(generator.Generate(range_start, range_end, error_emitter),
              ::zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument,
                                          "test error"));
  // Verify that returning an error from the emitter immediately terminated
  // the process.
  EXPECT_EQ(num_emitter_calls, 1);
}

}  // namespace
}  // namespace functions
}  // namespace zetasql
