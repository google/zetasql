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

#include "zetasql/public/timestamp_pico_value.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/time/time.h"

namespace {

class TimestampPicoTest : public testing::Test {};

TEST_F(TimestampPicoTest, FromAndToTime) {
  std::vector<int64_t> picos = {
      1704164645123456789,
      -2208891354876543211,
  };

  for (int64_t pico : picos) {
    absl::Time time = absl::FromUnixNanos(pico);
    zetasql::TimestampPicoValue value(time);
    EXPECT_EQ(value.ToTime(), time);
  }
}

TEST_F(TimestampPicoTest, MinAndMax) {
  EXPECT_EQ(zetasql::TimestampPicoValue::MinValue().ToString(),
            "0001-01-01 00:00:00+00");

  EXPECT_EQ(zetasql::TimestampPicoValue::MaxValue().ToString(),
            "9999-12-31 23:59:59.999999999+00");
}

TEST_F(TimestampPicoTest, SerializeAndDeserialize) {
  std::vector<zetasql::TimestampPicoValue> picos = {
      zetasql::TimestampPicoValue(),
      zetasql::TimestampPicoValue(absl::FromUnixSeconds(1234567890)),
      zetasql::TimestampPicoValue::MinValue(),
      zetasql::TimestampPicoValue::MaxValue(),
  };

  for (const zetasql::TimestampPicoValue& t1 : picos) {
    std::string serialized = t1.SerializeAsProtoBytes();

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        zetasql::TimestampPicoValue t2,
        zetasql::TimestampPicoValue::DeserializeFromProtoBytes(serialized));
    EXPECT_EQ(t1.ToTime(), t2.ToTime());
    EXPECT_EQ(t1, t2);
  }

  // Invalid input.
  EXPECT_THAT(
      zetasql::TimestampPicoValue::DeserializeFromProtoBytes("invalid"),
      zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

constexpr int64_t kAscendingNanoseconds[] = {
    -1000000001, -1000000000, 0, 1000000000, 1000000001, 1234567890,
};

TEST_F(TimestampPicoTest, ComparisonOps) {
  size_t n = sizeof(kAscendingNanoseconds) / sizeof(kAscendingNanoseconds[0]);
  std::vector<zetasql::TimestampPicoValue> values;
  values.resize(n);
  for (size_t i = 0; i < n; ++i) {
    values[i] = zetasql::TimestampPicoValue(
        absl::FromUnixNanos(kAscendingNanoseconds[i]));
  }
  for (size_t i = 0; i < n; ++i) {
    for (size_t j = 0; j < n; ++j) {
      EXPECT_EQ(values[i] == values[j], i == j);
      EXPECT_EQ(values[i] != values[j], i != j);
      EXPECT_EQ(values[i] < values[j], i < j);
      EXPECT_EQ(values[i] > values[j], i > j);
      EXPECT_EQ(values[i] <= values[j], i <= j);
      EXPECT_EQ(values[i] >= values[j], i >= j);
    }
  }
}

TEST_F(TimestampPicoTest, HashCode) {
  EXPECT_NE(zetasql::TimestampPicoValue(absl::FromUnixSeconds(1)).HashCode(),
            zetasql::TimestampPicoValue(absl::FromUnixSeconds(2)).HashCode());
  EXPECT_NE(zetasql::TimestampPicoValue(absl::FromUnixNanos(1)).HashCode(),
            zetasql::TimestampPicoValue(absl::FromUnixNanos(2)).HashCode());
  EXPECT_EQ(zetasql::TimestampPicoValue(absl::FromUnixSeconds(1)).HashCode(),
            zetasql::TimestampPicoValue(absl::FromUnixNanos(1000000000))
                .HashCode());
}

// TODO: Add random value tests with round trip between FromString
// and ToString.
TEST_F(TimestampPicoTest, FromString) {
  absl::TimeZone pst;
  ASSERT_TRUE(absl::LoadTimeZone("America/Los_Angeles", &pst));

  EXPECT_EQ(zetasql::TimestampPicoValue::FromString(
                "2024-08-19 12:34:56.123456789", absl::UTCTimeZone(),
                /*allow_tz_in_str=*/true)
                .value()
                .ToString(),
            "2024-08-19 12:34:56.123456789+00");
  EXPECT_EQ(zetasql::TimestampPicoValue::FromString(
                "2024-08-19 12:34:56.123456789", pst, /*allow_tz_in_str=*/true)
                .value()
                .ToString(),
            "2024-08-19 19:34:56.123456789+00");
  EXPECT_EQ(
      zetasql::TimestampPicoValue::FromString(
          "2024-08-19 12:34:56.123456789+00", pst, /*allow_tz_in_str=*/true)
          .value()
          .ToString(),
      "2024-08-19 12:34:56.123456789+00");
}

}  // namespace
