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

#include "zetasql/public/uuid_value.h"

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/functions/uuid.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/hash/hash_testing.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace zetasql {
namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class UuidValueTest : public testing::Test {
 protected:
  absl::StatusOr<UuidValue> MakeRandomUuidValue() {
    std::string uuid_str = functions::GenerateUuid(random_);
    return UuidValue::FromString(uuid_str);
  }

 private:
  absl::BitGen random_;
};

constexpr absl::string_view kAscendingUuidStrings[] = {
    "9d3da3234c20360fbd9bec54feec54f0",
    "9D4DA3234C20360FBD9BEC54FEEC54F0",
    "9d5da323-4c20-360f-bd9b-ec54feec54f0",
    "9D6DA323-4c20-360f-bd9b-ec54feec54f0",
    "9D7DA3234C20360fbd9bec54feec54f0",
    "9d8da323-4c20360fbd9bec54feec54f0",
    "9d9da3234c20360f-bd9b-ec54feec54f0",
};

struct UuidStringTestData {
  absl::string_view uuid_str;
  uint64_t high_bits;
  uint64_t low_bits;
};
constexpr UuidStringTestData kUuidToStringTestData[] = {
    {"9d3da323-4c20-360f-bd9b-ec54feec54f0", 0x9d3da3234c20360f,
     0xbd9bec54feec54f0},
    {"9d4da323-4c20-360f-bd9b-ec54feec54f0", 0x9d4da3234c20360f,
     0xbd9bec54feec54f0},
};
constexpr UuidStringTestData kUuidFromStringTestData[] = {
    {"9d3da323-4c20-360f-bd9b-ec54feec54f0", 0x9d3da3234c20360f,
     0xbd9bec54feec54f0},
    {"9d4da323-4c20-360f-bd9b-ec54feec54f0", 0x9d4da3234c20360f,
     0xbd9bec54feec54f0},
    {"9-d-5-d-a-3-2-3-4-c-2-0-3-6-0-f-b-d-9-b-e-c-5-4-f-e-e-c-5-4-f-0",
     0x9d5da3234c20360f, 0xbd9bec54feec54f0}};

constexpr absl::string_view kInvalidUuidStrings[] = {
    "",
    "9d",
    "uuid",
    "----------------------",
    "9g6da3234c20360fbd9bec54",
    "9d5da323--4c20360f-bd9b-ec54feec54f0",
    "9g6da3234c20360fbd9bec54feec54f0",
    "199d7da3234c20360fbd9bec54feec54f0",
    "19909d8da3234c20360fbd9bec54feec54f0",
    "19909d8da3234c20360fbd9bec54feec54f00",
    "-9d5da323-4c20-360f-bd9b-ec54feec54f0",
    "{-9d5da323-4c20-360f-bd9b-ec54feec54f0}",
    "{9d5da323-4c20-360f-bd9b-ec54feec54f0",
    "{9d5da323-4c20-360f-bd9b-ec54feec54f0/",
    "{9d5da323-4c20-360f-bd9b-ec54feec54f0}}",
    "{{9d5da323-4c20-360f-bd9b-ec54feec54f0}",
};

void TestComparisonOperators(absl::Span<const absl::string_view> uuid_strings) {
  std::vector<UuidValue> values;
  size_t n = uuid_strings.size();
  values.resize(n);
  for (size_t i = 0; i < n; ++i) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(values[i], UuidValue::FromString(uuid_strings[i]));
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

void TestHashCode(absl::Span<const absl::string_view> uuid_strings) {
  std::vector<UuidValue> values;
  size_t n = uuid_strings.size();
  values.resize(n);
  for (size_t i = 0; i < n; ++i) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(values[i], UuidValue::FromString(uuid_strings[i]));
  }
  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly(values));
}

void TestSerializeDeserializeRoundtrip(const UuidValue& value) {
  std::string bytes = value.SerializeAsBytes();
  EXPECT_THAT(UuidValue::DeserializeFromBytes(bytes), IsOkAndHolds(value));

  absl::string_view kExistingValue = "existing_value";
  bytes = kExistingValue;
  value.SerializeAndAppendToBytes(&bytes);
  absl::string_view bytes_view = bytes;
  ASSERT_TRUE(absl::StartsWith(bytes_view, kExistingValue)) << bytes_view;
  bytes_view.remove_prefix(kExistingValue.size());
  EXPECT_THAT(UuidValue::DeserializeFromBytes(bytes_view), IsOkAndHolds(value));
}

TEST_F(UuidValueTest, ToString) {
  for (const UuidStringTestData& pair : kUuidToStringTestData) {
    UuidValue value = UuidValue::FromPackedInt(
        (static_cast<__int128>(pair.high_bits) << 64) + pair.low_bits);
    EXPECT_EQ(pair.uuid_str, value.ToString());
  }
}

TEST_F(UuidValueTest, FromString) {
  for (const UuidStringTestData& pair : kUuidFromStringTestData) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(UuidValue value, UuidValue::FromString(pair.uuid_str));
    uint64_t high_bits_ = value.as_packed_int() >> 64;
    uint64_t low_bits_ =
        value.as_packed_int() & std::numeric_limits<uint64_t>::max();
    EXPECT_EQ(pair.high_bits, high_bits_);
    EXPECT_EQ(pair.low_bits, low_bits_);
  }
}

TEST_F(UuidValueTest, FromInvalidString) {
  for (absl::string_view uuid_str : kInvalidUuidStrings) {
    EXPECT_THAT(
        UuidValue::FromString(uuid_str),
        StatusIs(absl::StatusCode::kOutOfRange, HasSubstr("Invalid input")));
  }
}

TEST_F(UuidValueTest, InvalidUuidStringValue) {
  for (absl::string_view uuid_str : kInvalidUuidStrings) {
    absl::StatusOr<UuidValue> value = UuidValue::FromString(uuid_str);
    EXPECT_THAT(
        UuidValue::FromString(uuid_str),
        StatusIs(absl::StatusCode::kOutOfRange, HasSubstr("Invalid input")));
  }
}

TEST_F(UuidValueTest, OperatorsTest) {
  TestComparisonOperators(kAscendingUuidStrings);
}

TEST_F(UuidValueTest, HashCode) { TestHashCode(kAscendingUuidStrings); }

TEST_F(UuidValueTest, SerializeDeserializeProtoBytes) {
  static constexpr absl::string_view kTestValues[] = {
      "00000000-0000-4000-8000-000000000000",
      "03020100-0504-4706-8908-0f0e0d0c0b0a",
      "ffffffff-ffff-4fff-bfff-ffffffffffff"};
  for (const absl::string_view test_value : kTestValues) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(UuidValue value, UuidValue::FromString(test_value));
    TestSerializeDeserializeRoundtrip(value);
  }

  constexpr int kTestIterations = 100;
  for (int i = 0; i < kTestIterations; ++i) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(UuidValue value, MakeRandomUuidValue());
    TestSerializeDeserializeRoundtrip(value);
  }
}

TEST_F(UuidValueTest, DeserializeProtoBytesFailures) {
  std::string bytes;

  EXPECT_THAT(UuidValue::DeserializeFromBytes(bytes),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Invalid serialized UUID size")));
  bytes.resize(17);
  EXPECT_THAT(UuidValue::DeserializeFromBytes(bytes),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Invalid serialized UUID size")));
}

}  // namespace
}  // namespace zetasql
