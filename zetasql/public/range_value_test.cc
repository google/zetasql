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

#include "zetasql/public/range_value.h"

#include <cstdint>
#include <string>
#include <vector>

#include "zetasql/common/testing/proto_matchers.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/timestamp_util.h"
#include "zetasql/public/types/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/str_format.h"

namespace zetasql {

using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

template <typename Elem>
struct RangeValueTestCase {
  std::optional<Elem> start;
  std::optional<Elem> end;
  size_t encoded_size;
  std::string str;
};

template <typename T>
void TestEncodeDecodeRange(const RangeValue<T>& range_value,
                           const size_t expected_encoded_str_size,
                           const TypeKind& element_type_kind) {
  std::string buffer;
  ZETASQL_ASSERT_OK(range_value.SerializeAndAppendToBytes(&buffer));
  ZETASQL_ASSERT_OK_AND_ASSIGN(RangeValue<T> decoded_range_value,
                       RangeValue<T>::DeserializeFromBytes(buffer));
  ZETASQL_ASSERT_OK_AND_ASSIGN(size_t calculated_encoded_range_size,
                       GetEncodedRangeSize(buffer, element_type_kind));
  EXPECT_EQ(calculated_encoded_range_size, expected_encoded_str_size);
  ZETASQL_ASSERT_OK_AND_ASSIGN(calculated_encoded_range_size,
                       GetEncodedRangeSize<T>(buffer));
  EXPECT_EQ(calculated_encoded_range_size, expected_encoded_str_size);
  EXPECT_EQ(buffer.size(), expected_encoded_str_size);
  EXPECT_EQ(range_value.DebugString(), decoded_range_value.DebugString());
}

class RangeValueDatesTest
    : public ::testing::TestWithParam<RangeValueTestCase<int32_t>> {};

std::vector<RangeValueTestCase<int32_t>> GetRangeValueDatesTestCases() {
  return {// Regular range.
          {.start = {1},
           .end = {2},
           .encoded_size = 9,
           .str = "[1970-01-02, 1970-01-03)"},
          // Unbounded at end.
          {.start = {1},
           .end = {},
           .encoded_size = 5,
           .str = "[1970-01-02, UNBOUNDED)"},
          // Unbounded at start.
          {.start = {},
           .end = {2},
           .encoded_size = 5,
           .str = "[UNBOUNDED, 1970-01-03)"},
          // Unbounded at start and end.
          {.start = {},
           .end = {},
           .encoded_size = 1,
           .str = "[UNBOUNDED, UNBOUNDED)"}};
}

INSTANTIATE_TEST_SUITE_P(Range, RangeValueDatesTest,
                         ::testing::ValuesIn(GetRangeValueDatesTestCases()));

TEST_P(RangeValueDatesTest, MakeRangeDatesSucceeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(RangeValue<int32_t> range_value,
                       RangeValueFromDates(GetParam().start, GetParam().end));

  EXPECT_EQ(GetParam().start, range_value.start());
  EXPECT_EQ(GetParam().end, range_value.end());
}

TEST_P(RangeValueDatesTest, ParseRangeValueFromDateStringsSucceeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const auto boundaries,
                       GetRangeBoundaries(GetParam().str));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      RangeValue<int32_t> range_value,
      ParseRangeValueFromDateStrings(boundaries.first, boundaries.second));
  EXPECT_EQ(GetParam().start, range_value.start());
  EXPECT_EQ(GetParam().end, range_value.end());
}

TEST(InvalidRangeValueTest, MakeRangeValueDateFails) {
  EXPECT_THAT(
      RangeValueFromDates(std::optional<int32_t>{types::kDateMax + 1}, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid start date")));
  EXPECT_THAT(
      RangeValueFromDates(std::optional<int32_t>{}, {types::kDateMax + 1}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid end date")));
  EXPECT_THAT(
      RangeValueFromDates(std::optional<int32_t>{types::kDateMin - 1}, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid start date")));
  EXPECT_THAT(
      RangeValueFromDates(std::optional<int32_t>{}, {types::kDateMin - 1}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid end date")));
  EXPECT_THAT(
      RangeValueFromDates(std::optional<int32_t>{2}, {1}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "Range start element must be smaller than range end element")));
}

TEST_P(RangeValueDatesTest, EncodeDecodeRangeValueDatesSucceeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(RangeValue<int32_t> range_value,
                       RangeValueFromDates(GetParam().start, GetParam().end));
  TestEncodeDecodeRange(range_value, GetParam().encoded_size,
                        TypeKind::TYPE_DATE);
}

TEST_P(RangeValueDatesTest, DecodeTooShortEncodedRange) {
  if (GetParam().encoded_size == 1) return;

  ZETASQL_ASSERT_OK_AND_ASSIGN(RangeValue<int32_t> range_value,
                       RangeValueFromDates(GetParam().start, GetParam().end));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::string encoded_range,
                       range_value.SerializeAsBytes());
  std::string encoded_range_header = encoded_range.substr(0, 1);
  EXPECT_THAT(
      RangeValue<int32_t>::DeserializeFromBytes(encoded_range_header),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr(absl::StrFormat(
                   "Too few bytes to read RANGE content (needed %d; got 1)",
                   GetParam().encoded_size))));
}

TEST_P(RangeValueDatesTest, FormatAs) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(RangeValue<int32_t> range_value,
                       RangeValueFromDates(GetParam().start, GetParam().end));
  EXPECT_THAT(RangeValueDatesToString(range_value),
              IsOkAndHolds(GetParam().str));
}

class RangeValueDatetimesTest
    : public ::testing::TestWithParam<RangeValueTestCase<DatetimeValue>> {};

std::vector<RangeValueTestCase<zetasql::DatetimeValue>>
GetRangeValueDatetimesTestCases() {
  return {// Regular range.
          {.start = {zetasql::DatetimeValue::FromYMDHMSAndMicros(
               2022, 12, 05, 16, 44, 0, 7)},
           .end = {zetasql::DatetimeValue::FromYMDHMSAndMicros(2022, 12, 05,
                                                                 16, 45, 0, 7)},
           .encoded_size = 17,
           .str = "[2022-12-05 16:44:00.000007, 2022-12-05 16:45:00.000007)"},
          // Unbounded at end.
          {.start = {zetasql::DatetimeValue::FromYMDHMSAndMicros(
               2022, 12, 05, 16, 44, 0, 7)},
           .end = {},
           .encoded_size = 9,
           .str = "[2022-12-05 16:44:00.000007, UNBOUNDED)"},
          // Unbounded at start.
          {.start = {},
           .end = {zetasql::DatetimeValue::FromYMDHMSAndMicros(2022, 12, 05,
                                                                 16, 45, 0, 7)},
           .encoded_size = 9,
           .str = "[UNBOUNDED, 2022-12-05 16:45:00.000007)"},
          // Unbounded at start and end.
          {.start = {},
           .end = {},
           .encoded_size = 1,
           .str = "[UNBOUNDED, UNBOUNDED)"}};
}

INSTANTIATE_TEST_SUITE_P(
    Range, RangeValueDatetimesTest,
    ::testing::ValuesIn(GetRangeValueDatetimesTestCases()));

TEST_P(RangeValueDatetimesTest, MakeRangeValueDatetimeSucceeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      RangeValue<int64_t> range_value,
      RangeValueFromDatetimeMicros(GetParam().start, GetParam().end));

  if (GetParam().start.has_value()) {
    EXPECT_EQ(GetParam().start->Packed64DatetimeMicros(),
              range_value.start().value());
  }
  if (GetParam().end.has_value()) {
    EXPECT_EQ(GetParam().end->Packed64DatetimeMicros(),
              range_value.end().value());
  }
}

TEST_P(RangeValueDatetimesTest,
       ParseRangeValueDatetimeMicrosFromStringsSucceeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const auto boundaries,
                       GetRangeBoundaries(GetParam().str));
  ZETASQL_ASSERT_OK_AND_ASSIGN(RangeValue<int64_t> range_value,
                       ParseRangeValueDatetimeMicrosFromStrings(
                           boundaries.first, boundaries.second));
  EXPECT_EQ(GetParam().start.has_value(), range_value.start().has_value());
  if (GetParam().start.has_value()) {
    EXPECT_EQ(GetParam().start->Packed64DatetimeMicros(), range_value.start());
  }
  EXPECT_EQ(GetParam().end.has_value(), range_value.end().has_value());
  if (GetParam().end.has_value()) {
    EXPECT_EQ(GetParam().end->Packed64DatetimeMicros(), range_value.end());
  }
}

TEST(InvalidRangeValueTest, MakeRangeValueDatetimeFails) {
  EXPECT_THAT(RangeValueFromDatetimeMicros(
                  std::optional<DatetimeValue>{
                      zetasql::DatetimeValue::FromPacked64Micros(1)},
                  {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid start datetime")));
  EXPECT_THAT(RangeValueFromDatetimeMicros(
                  std::optional<DatetimeValue>{},
                  {zetasql::DatetimeValue::FromPacked64Micros(1)}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid end datetime")));
  EXPECT_THAT(
      RangeValueFromDatetimeMicros(
          std::optional<DatetimeValue>{
              zetasql::DatetimeValue::FromYMDHMSAndMicros(2022, 12, 05, 16,
                                                            44, 0, 7)},
          {zetasql::DatetimeValue::FromYMDHMSAndMicros(2021, 11, 04, 15, 43,
                                                         0, 6)}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "Range start element must be smaller than range end element")));
}

TEST_P(RangeValueDatetimesTest, EncodeDecodeRangeValueDatetimesSucceeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      RangeValue<int64_t> range_value,
      RangeValueFromDatetimeMicros(GetParam().start, GetParam().end));
  TestEncodeDecodeRange(range_value, GetParam().encoded_size,
                        TypeKind::TYPE_DATETIME);
}

TEST_P(RangeValueDatetimesTest, DecodeTooShortEncodedRange) {
  if (GetParam().encoded_size == 1) return;

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      RangeValue<int64_t> range_value,
      RangeValueFromDatetimeMicros(GetParam().start, GetParam().end));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::string encoded_range,
                       range_value.SerializeAsBytes());
  std::string encoded_range_header = encoded_range.substr(0, 1);
  EXPECT_THAT(
      RangeValue<int64_t>::DeserializeFromBytes(encoded_range_header),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr(absl::StrFormat(
                   "Too few bytes to read RANGE content (needed %d; got 1)",
                   GetParam().encoded_size))));
}

TEST_P(RangeValueDatetimesTest, FormatAs) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      RangeValue<int64_t> range_value,
      RangeValueFromDatetimeMicros(GetParam().start, GetParam().end));
  EXPECT_THAT(RangeValueDatetimeMicrosToString(range_value),
              IsOkAndHolds(GetParam().str));
}

class RangeValueTimestampsTest
    : public ::testing::TestWithParam<RangeValueTestCase<int64_t>> {};

std::vector<RangeValueTestCase<int64_t>> GetRangeValueTimestampsTestCases() {
  return {
      // Regular range.
      {.start = {1},
       .end = {2},
       .encoded_size = 17,
       .str = "[1970-01-01 00:00:00.000001+00, 1970-01-01 00:00:00.000002+00)"},
      // Unbounded at end.
      {.start = {1},
       .end = {},
       .encoded_size = 9,
       .str = "[1970-01-01 00:00:00.000001+00, UNBOUNDED)"},
      // Unbounded at start.
      {.start = {},
       .end = {2},
       .encoded_size = 9,
       .str = "[UNBOUNDED, 1970-01-01 00:00:00.000002+00)"},
      // Unbounded at start and end.
      {.start = {},
       .end = {},
       .encoded_size = 1,
       .str = "[UNBOUNDED, UNBOUNDED)"}};
}

INSTANTIATE_TEST_SUITE_P(
    Range, RangeValueTimestampsTest,
    ::testing::ValuesIn(GetRangeValueTimestampsTestCases()));

TEST_P(RangeValueTimestampsTest, MakeRangeValueTimestampSucceeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      RangeValue<int64_t> range_value,
      RangeValueFromTimestampMicros(GetParam().start, GetParam().end));

  EXPECT_EQ(GetParam().start, range_value.start());
  EXPECT_EQ(GetParam().end, range_value.end());
}

TEST_P(RangeValueTimestampsTest,
       ParseRangeValueTimestampMicrosFromTimestampStringsSucceeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const auto boundaries,
                       GetRangeBoundaries(GetParam().str));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      RangeValue<int64_t> range_value,
      ParseRangeValueTimestampMicrosFromTimestampStrings(
          boundaries.first, boundaries.second, absl::UTCTimeZone()));
  EXPECT_EQ(GetParam().start, range_value.start());
  EXPECT_EQ(GetParam().end, range_value.end());
}

TEST_P(RangeValueTimestampsTest, EncodeDecodeRangeValueTimestampsSucceeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      RangeValue<int64_t> range_value,
      RangeValueFromTimestampMicros(GetParam().start, GetParam().end));
  TestEncodeDecodeRange(range_value, GetParam().encoded_size,
                        TypeKind::TYPE_TIMESTAMP);
}

TEST_P(RangeValueTimestampsTest, DecodeTooShortEncodedRange) {
  if (GetParam().encoded_size == 1) return;

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      RangeValue<int64_t> range_value,
      RangeValueFromTimestampMicros(GetParam().start, GetParam().end));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::string encoded_range,
                       range_value.SerializeAsBytes());
  std::string encoded_range_header = encoded_range.substr(0, 1);
  EXPECT_THAT(
      RangeValue<int64_t>::DeserializeFromBytes(encoded_range_header),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr(absl::StrFormat(
                   "Too few bytes to read RANGE content (needed %d; got 1)",
                   GetParam().encoded_size))));
}

TEST(InvalidRangeValueTest, MakeRangeValueTimestampsFails) {
  EXPECT_THAT(RangeValueFromTimestampMicros(
                  std::optional<int64_t>{types::kTimestampMax + 1}, {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid start timestamp")));
  EXPECT_THAT(RangeValueFromTimestampMicros(std::optional<int64_t>{},
                                            {types::kTimestampMax + 1}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid end timestamp")));
  EXPECT_THAT(RangeValueFromTimestampMicros(
                  std::optional<int64_t>{types::kTimestampMin - 1}, {}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid start timestamp")));
  EXPECT_THAT(RangeValueFromTimestampMicros(std::optional<int64_t>{},
                                            {types::kTimestampMin - 1}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid end timestamp")));
  EXPECT_THAT(
      RangeValueFromTimestampMicros(std::optional<int64_t>{2}, {1}),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "Range start element must be smaller than range end element")));
}

TEST_P(RangeValueTimestampsTest, ToString) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      RangeValue<int64_t> range_value,
      RangeValueFromTimestampMicros(GetParam().start, GetParam().end));
  EXPECT_THAT(RangeValueTimestampMicrosToString(range_value),
              IsOkAndHolds(GetParam().str));
}

TEST(InvalidRangeValueTest, DecodeInvalidEncodedRange) {
  EXPECT_THAT(
      GetEncodedRangeSize("", TypeKind::TYPE_DATE),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Too few bytes to determine encoded RANGE size")));
  EXPECT_THAT(
      GetEncodedRangeSize("", TypeKind::TYPE_DATETIME),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Too few bytes to determine encoded RANGE size")));
  EXPECT_THAT(
      GetEncodedRangeSize("", TypeKind::TYPE_TIMESTAMP),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Too few bytes to determine encoded RANGE size")));
}

}  // namespace zetasql
