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
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"

namespace zetasql {
namespace {

using zetasql_base::testing::StatusIs;

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
                            0,
                            1,
                            -1000,
                            1000000,
                            -123456789};
  for (int64_t value : values) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(interval, IntervalValue::FromMicros(value));
    EXPECT_EQ(value, interval.get_micros());
    EXPECT_EQ(value, interval.GetAsMicros());
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

}  // namespace
}  // namespace zetasql
