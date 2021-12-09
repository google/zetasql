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

#include "zetasql/public/json_value.h"

#include <math.h>
#include <stddef.h>
#include <string.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>


#include "zetasql/base/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <cstdint>  
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"

namespace {

using ::zetasql::JSONParsingOptions;
using ::zetasql::JSONValue;
using ::zetasql::JSONValueConstRef;
using ::zetasql::JSONValueRef;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::zetasql_base::testing::StatusIs;

constexpr char kJSONStr[] = R"(
  {
    "pi": 3.141,
    "happy": true,
    "name": "Niels",
    "nothing": null,
    "answer": {
      "everything": 42
    },
    "list": [1, 0, 2],
    "object": {
      "currency": "USD",
      "value": 42.99
    }
  }
)";

TEST(JSONValueTest, NullValue) {
  JSONValue value;
  JSONValueRef ref = value.GetRef();

  EXPECT_TRUE(ref.IsNull());
  EXPECT_FALSE(ref.IsObject());
  EXPECT_FALSE(ref.IsBoolean());
  EXPECT_FALSE(ref.IsArray());
  EXPECT_FALSE(ref.IsString());
  EXPECT_FALSE(ref.IsNumber());

  JSONValueConstRef const_ref = value.GetConstRef();

  EXPECT_TRUE(const_ref.IsNull());
  EXPECT_FALSE(const_ref.IsObject());
  EXPECT_FALSE(const_ref.IsBoolean());
  EXPECT_FALSE(const_ref.IsArray());
  EXPECT_FALSE(const_ref.IsString());
  EXPECT_FALSE(const_ref.IsNumber());

  EXPECT_DEATH(ref.GetInt64(), "type must be number, but is null");
}

TEST(JSONValueTest, Int64Value) {
  constexpr int64_t kInt64Value = 1;
  {
    JSONValue value(kInt64Value);
    ASSERT_TRUE(value.GetConstRef().IsInt64());
    EXPECT_EQ(kInt64Value, value.GetConstRef().GetInt64());
  }

  {
    JSONValue value;
    JSONValueRef ref = value.GetRef();

    ref.SetInt64(kInt64Value);

    EXPECT_FALSE(ref.IsNull());
    EXPECT_FALSE(ref.IsObject());
    EXPECT_FALSE(ref.IsBoolean());
    EXPECT_FALSE(ref.IsArray());
    EXPECT_FALSE(ref.IsString());
    ASSERT_TRUE(ref.IsNumber());
    ASSERT_TRUE(ref.IsInt64());

    EXPECT_EQ(kInt64Value, ref.GetInt64());
    EXPECT_DEATH(ref.GetString(), "type must be string, but is number");
  }
}

TEST(JSONValueTest, UInt64Value) {
  constexpr uint64_t kUInt64Value = 1;

  {
    JSONValue value(kUInt64Value);
    ASSERT_TRUE(value.GetConstRef().IsUInt64());
    EXPECT_EQ(kUInt64Value, value.GetConstRef().GetUInt64());
  }

  {
    JSONValue value;
    JSONValueRef ref = value.GetRef();

    ref.SetUInt64(kUInt64Value);

    EXPECT_FALSE(ref.IsNull());
    EXPECT_FALSE(ref.IsObject());
    EXPECT_FALSE(ref.IsBoolean());
    EXPECT_FALSE(ref.IsArray());
    EXPECT_FALSE(ref.IsString());
    ASSERT_TRUE(ref.IsNumber());
    ASSERT_TRUE(ref.IsUInt64());

    EXPECT_EQ(kUInt64Value, ref.GetUInt64());
    EXPECT_DEATH(ref.GetString(), "type must be string, but is number");
  }
}

TEST(JSONValueTest, DoubleValue) {
  constexpr double kDoubleValue = 1;

  {
    JSONValue value(kDoubleValue);
    ASSERT_TRUE(value.GetConstRef().IsDouble());
    EXPECT_EQ(kDoubleValue, value.GetConstRef().GetDouble());
  }

  {
    JSONValue value;
    JSONValueRef ref = value.GetRef();

    ref.SetDouble(kDoubleValue);

    EXPECT_FALSE(ref.IsNull());
    EXPECT_FALSE(ref.IsObject());
    EXPECT_FALSE(ref.IsBoolean());
    EXPECT_FALSE(ref.IsArray());
    EXPECT_FALSE(ref.IsString());
    ASSERT_TRUE(ref.IsNumber());
    ASSERT_TRUE(ref.IsDouble());

    EXPECT_EQ(kDoubleValue, ref.GetDouble());
    EXPECT_DEATH(ref.GetString(), "type must be string, but is number");
  }
}

TEST(JSONValueTest, StringValue) {
  const std::string kStringValue = "test";

  {
    JSONValue value(kStringValue);
    ASSERT_TRUE(value.GetConstRef().IsString());
    EXPECT_EQ(kStringValue, value.GetConstRef().GetString());
  }

  {
    JSONValue value;
    JSONValueRef ref = value.GetRef();

    ref.SetString(kStringValue);

    EXPECT_FALSE(ref.IsNull());
    EXPECT_FALSE(ref.IsObject());
    EXPECT_FALSE(ref.IsBoolean());
    EXPECT_FALSE(ref.IsArray());
    ASSERT_TRUE(ref.IsString());
    EXPECT_FALSE(ref.IsNumber());

    EXPECT_EQ(kStringValue, ref.GetString());
    EXPECT_DEATH(ref.GetBoolean(), "type must be boolean, but is string");
  }
}

TEST(JSONValueTest, BooleanValue) {
  constexpr bool kBooleanValue = true;

  {
    JSONValue value(kBooleanValue);
    ASSERT_TRUE(value.GetConstRef().IsBoolean());
    EXPECT_EQ(kBooleanValue, value.GetConstRef().GetBoolean());
  }

  {
    JSONValue value;
    JSONValueRef ref = value.GetRef();

    ref.SetBoolean(kBooleanValue);

    EXPECT_FALSE(ref.IsNull());
    EXPECT_FALSE(ref.IsObject());
    ASSERT_TRUE(ref.IsBoolean());
    EXPECT_FALSE(ref.IsArray());
    EXPECT_FALSE(ref.IsString());
    EXPECT_FALSE(ref.IsNumber());

    EXPECT_EQ(kBooleanValue, ref.GetBoolean());
    EXPECT_DEATH(ref.GetString(), "type must be string, but is boolean");
  }
}

TEST(JSONValueTest, UpdateToDifferentPrimitiveTypes) {
  constexpr bool kBooleanValue = true;
  const std::string kStringValue = "test";
  constexpr int64_t kInt64Value = 1;

  JSONValue value;
  JSONValueRef ref = value.GetRef();

  ref.SetBoolean(kBooleanValue);
  ASSERT_TRUE(ref.IsBoolean());
  EXPECT_EQ(kBooleanValue, ref.GetBoolean());

  ref.SetString(kStringValue);
  ASSERT_TRUE(ref.IsString());
  EXPECT_EQ(kStringValue, ref.GetString());

  ref.SetInt64(kInt64Value);
  ASSERT_TRUE(ref.IsInt64());
  EXPECT_EQ(kInt64Value, ref.GetInt64());
}

TEST(JSONValueTest, UpdatePrimitiveTypeToObjectType) {
  constexpr bool kBooleanValue = true;

  JSONValue value;
  JSONValueRef ref = value.GetRef();

  ref.SetBoolean(kBooleanValue);
  EXPECT_DEATH(ref.GetMember("key"), "with a string argument with boolean");
}

TEST(JSONValueTest, UpdatePrimitiveTypeToArrayType) {
  constexpr bool kBooleanValue = true;

  JSONValue value;
  JSONValueRef ref = value.GetRef();

  ref.SetBoolean(kBooleanValue);
  EXPECT_DEATH(ref.GetArrayElement(0), "with a numeric argument with boolean");
}

TEST(JSONValueTest, ObjectValue) {
  constexpr int64_t kIntegerValue = 1;
  constexpr char kKey[] = "key";
  constexpr char kWrongKey[] = "wrong key";
  JSONValue value;

  JSONValueRef ref = value.GetRef();
  JSONValueRef member_ref = ref.GetMember(kKey);

  EXPECT_FALSE(ref.IsNull());
  ASSERT_TRUE(ref.IsObject());
  EXPECT_FALSE(ref.IsBoolean());
  EXPECT_FALSE(ref.IsArray());
  EXPECT_FALSE(ref.IsString());
  EXPECT_FALSE(ref.IsNumber());

  EXPECT_EQ(ref.GetObjectSize(), 1);
  EXPECT_TRUE(ref.HasMember(kKey));
  EXPECT_FALSE(ref.HasMember(kWrongKey));

  EXPECT_TRUE(member_ref.IsNull());

  member_ref.SetInt64(kIntegerValue);

  EXPECT_FALSE(ref.GetMember(kKey).IsNull());
  EXPECT_EQ(kIntegerValue, ref.GetMember(kKey).GetInt64());

  std::vector<std::pair<absl::string_view, JSONValueRef>> members =
      ref.GetMembers();

  EXPECT_EQ(1, members.size());
  EXPECT_EQ(kKey, members[0].first);
  EXPECT_EQ(kIntegerValue, members[0].second.GetInt64());

  absl::optional<JSONValueConstRef> optional_member_const_ref =
      ref.GetMemberIfExists(kKey);
  ASSERT_TRUE(optional_member_const_ref.has_value());
  EXPECT_TRUE(optional_member_const_ref->IsInt64());
  EXPECT_EQ(1, optional_member_const_ref->GetInt64());

  EXPECT_FALSE(ref.GetMemberIfExists(kWrongKey).has_value());
}

TEST(JSONValueTest, EmptyObjectValue) {
  constexpr int64_t kIntegerValue = 1;
  constexpr char kKey[] = "key";
  constexpr char kWrongKey[] = "wrong key";
  JSONValue value;

  JSONValueRef ref = value.GetRef();
  ref.SetToEmptyObject();

  EXPECT_FALSE(ref.IsNull());
  ASSERT_TRUE(ref.IsObject());
  EXPECT_FALSE(ref.IsBoolean());
  EXPECT_FALSE(ref.IsArray());
  EXPECT_FALSE(ref.IsString());
  EXPECT_FALSE(ref.IsNumber());

  EXPECT_EQ(ref.GetObjectSize(), 0);
  EXPECT_THAT(ref.GetMembers(), IsEmpty());

  JSONValueRef member_ref = ref.GetMember(kKey);

  EXPECT_EQ(ref.GetObjectSize(), 1);
  EXPECT_TRUE(ref.HasMember(kKey));
  EXPECT_FALSE(ref.HasMember(kWrongKey));
  EXPECT_TRUE(member_ref.IsNull());

  member_ref.SetInt64(kIntegerValue);

  EXPECT_FALSE(ref.GetMember(kKey).IsNull());
  EXPECT_EQ(ref.GetMember(kKey).GetInt64(), kIntegerValue);

  std::vector<std::pair<absl::string_view, JSONValueRef>> members =
      ref.GetMembers();

  ASSERT_EQ(members.size(), 1);
  EXPECT_EQ(members[0].first, kKey);
  EXPECT_EQ(members[0].second.GetInt64(), kIntegerValue);

  absl::optional<JSONValueConstRef> optional_member_const_ref =
      ref.GetMemberIfExists(kKey);
  ASSERT_TRUE(optional_member_const_ref.has_value());
  ASSERT_TRUE(optional_member_const_ref->IsInt64());
  EXPECT_EQ(optional_member_const_ref->GetInt64(), 1);

  EXPECT_FALSE(ref.GetMemberIfExists(kWrongKey).has_value());
}

TEST(JSONValueTest, ArrayValue) {
  constexpr int64_t kIntegerValue = 1;
  constexpr size_t kIndex = 5;
  JSONValue value;
  JSONValueRef ref = value.GetRef();

  JSONValueRef element_ref = ref.GetArrayElement(kIndex);

  EXPECT_FALSE(ref.IsNull());
  EXPECT_FALSE(ref.IsObject());
  EXPECT_FALSE(ref.IsBoolean());
  ASSERT_TRUE(ref.IsArray());
  EXPECT_FALSE(ref.IsString());
  EXPECT_FALSE(ref.IsNumber());

  EXPECT_EQ(kIndex + 1, ref.GetArraySize());

  EXPECT_TRUE(element_ref.IsNull());

  element_ref.SetInt64(kIntegerValue);

  EXPECT_FALSE(ref.GetArrayElement(kIndex).IsNull());
  EXPECT_EQ(kIntegerValue, ref.GetArrayElement(kIndex).GetInt64());

  std::vector<JSONValueRef> elements = ref.GetArrayElements();
  EXPECT_EQ(kIndex + 1, elements.size());
  EXPECT_TRUE(elements[0].IsNull());
  EXPECT_EQ(kIntegerValue, elements[kIndex].GetInt64());
}

TEST(JSONValueTest, EmptyArrayValue) {
  constexpr int64_t kIntegerValue = 1;
  constexpr size_t kIndex = 5;
  JSONValue value;
  JSONValueRef ref = value.GetRef();
  ref.SetToEmptyArray();

  EXPECT_FALSE(ref.IsNull());
  EXPECT_FALSE(ref.IsObject());
  EXPECT_FALSE(ref.IsBoolean());
  ASSERT_TRUE(ref.IsArray());
  EXPECT_FALSE(ref.IsString());
  EXPECT_FALSE(ref.IsNumber());

  EXPECT_EQ(ref.GetArraySize(), 0);

  JSONValueRef element_ref = ref.GetArrayElement(kIndex);

  EXPECT_EQ(ref.GetArraySize(), kIndex + 1);
  EXPECT_TRUE(element_ref.IsNull());
  for (JSONValueConstRef element_ref : ref.GetArrayElements()) {
    EXPECT_TRUE(element_ref.IsNull());
  }

  element_ref.SetInt64(kIntegerValue);
  EXPECT_FALSE(ref.GetArrayElement(kIndex).IsNull());
  EXPECT_EQ(ref.GetArrayElement(kIndex).GetInt64(), kIntegerValue);

  std::vector<JSONValueRef> elements = ref.GetArrayElements();
  EXPECT_EQ(elements.size(), kIndex + 1);
  EXPECT_TRUE(elements[0].IsNull());
  EXPECT_EQ(elements[kIndex].GetInt64(), kIntegerValue);
}

TEST(JSONValueTest, CopyFrom) {
  constexpr int64_t kInt64Value1 = 1;
  constexpr int64_t kInt64Value2 = 5;

  JSONValue value(kInt64Value1);
  JSONValueRef ref = value.GetRef();

  JSONValue copy = JSONValue::CopyFrom(ref);
  JSONValueConstRef copy_ref = copy.GetConstRef();

  ASSERT_TRUE(copy_ref.IsInt64());
  EXPECT_EQ(kInt64Value1, copy_ref.GetInt64());
  EXPECT_TRUE(ref.NormalizedEquals(copy_ref));

  ref.SetInt64(kInt64Value2);

  ASSERT_TRUE(copy_ref.IsInt64());
  EXPECT_EQ(kInt64Value1, copy_ref.GetInt64());
  EXPECT_FALSE(ref.NormalizedEquals(copy_ref));
}

class JSONParserTest : public ::testing::TestWithParam<JSONParsingOptions> {};

TEST_P(JSONParserTest, ParseString) {
  JSONValue value = JSONValue::ParseJSONString("\"str\"", GetParam()).value();
  ASSERT_TRUE(value.GetConstRef().IsString());
  EXPECT_EQ("str", value.GetConstRef().GetString());
}

TEST_P(JSONParserTest, ParseInteger) {
  JSONValue value = JSONValue::ParseJSONString("1", GetParam()).value();
  EXPECT_TRUE(value.GetConstRef().IsInt64());
  ASSERT_TRUE(value.GetConstRef().IsUInt64());
  EXPECT_EQ(1, value.GetConstRef().GetInt64());
  EXPECT_EQ(1, value.GetConstRef().GetUInt64());

  value = JSONValue::ParseJSONString("-1", GetParam()).value();
  ASSERT_TRUE(value.GetConstRef().IsInt64());
  ASSERT_FALSE(value.GetConstRef().IsUInt64());
  EXPECT_EQ(-1, value.GetConstRef().GetInt64());

  uint64_t uint64_value =
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1;
  value = JSONValue::ParseJSONString(std::to_string(uint64_value), GetParam())
              .value();
  EXPECT_FALSE(value.GetConstRef().IsInt64());
  ASSERT_TRUE(value.GetConstRef().IsUInt64());
  EXPECT_EQ(uint64_value, value.GetConstRef().GetUInt64());
}

TEST_P(JSONParserTest, ParseDouble) {
  JSONValue value = JSONValue::ParseJSONString("1.5", GetParam()).value();
  ASSERT_TRUE(value.GetConstRef().IsDouble());
  EXPECT_EQ(1.5, value.GetConstRef().GetDouble());
}

TEST_P(JSONParserTest, ParseLargeNumbers) {
  JSONValue value =
      JSONValue::ParseJSONString("11111111111111111111", GetParam()).value();
  EXPECT_FALSE(value.GetConstRef().IsInt64());
  ASSERT_TRUE(value.GetConstRef().IsUInt64());
  EXPECT_EQ(11111111111111111111ULL, value.GetConstRef().GetUInt64());

  if (GetParam().strict_number_parsing) {
    EXPECT_THAT(
        JSONValue::ParseJSONString("123456789012345678901234567890", GetParam())
            .status()
            .message(),
        ::testing::HasSubstr("cannot round-trip"));
  } else {
    value =
        JSONValue::ParseJSONString("123456789012345678901234567890", GetParam())
            .value();
    EXPECT_FALSE(value.GetConstRef().IsInt64());
    ASSERT_TRUE(value.GetConstRef().IsDouble());
    EXPECT_THAT(value.GetConstRef().GetDouble(),
                testing::DoubleEq(1.23456789012345678901234567890e+29));
  }

  // Legacy parser parses out of range doubles as inf while standard parser
  // fails the parse.
  auto result = JSONValue::ParseJSONString("3.14e314", GetParam());
  if (GetParam().legacy_mode) {
    auto const_ref = result.value().GetConstRef();
    ASSERT_TRUE(const_ref.IsDouble());
    EXPECT_TRUE(std::isinf(const_ref.GetDouble()));
  } else {
    EXPECT_FALSE(result.ok());
    EXPECT_THAT(result.status().message(),
                ::testing::HasSubstr("number overflow parsing '3.14e314'"));
  }
}

TEST_P(JSONParserTest, ParseBoolean) {
  JSONValue value = JSONValue::ParseJSONString("true", GetParam()).value();
  ASSERT_TRUE(value.GetConstRef().IsBoolean());
  EXPECT_TRUE(value.GetConstRef().GetBoolean());
}

TEST_P(JSONParserTest, ParseNull) {
  JSONValue value = JSONValue::ParseJSONString("null", GetParam()).value();
  ASSERT_TRUE(value.GetConstRef().IsNull());
}

TEST_P(JSONParserTest, ParseArray) {
  JSONValue value =
      JSONValue::ParseJSONString("[1, \n2, \t3]", GetParam()).value();
  ASSERT_TRUE(value.GetConstRef().IsArray());
  EXPECT_EQ(3, value.GetConstRef().GetArraySize());
}

TEST_P(JSONParserTest, ParseObject) {
  JSONValue value = JSONValue::ParseJSONString(kJSONStr, GetParam()).value();
  EXPECT_EQ(value.GetConstRef().GetObjectSize(), 7);
  ASSERT_TRUE(value.GetConstRef().IsObject());
  ASSERT_TRUE(value.GetConstRef().GetMember("pi").IsDouble());
  EXPECT_EQ(3.141, value.GetConstRef().GetMember("pi").GetDouble());
  ASSERT_TRUE(value.GetConstRef().GetMember("happy").IsBoolean());
  EXPECT_TRUE(value.GetConstRef().GetMember("happy").GetBoolean());
  ASSERT_TRUE(value.GetConstRef().GetMember("name").IsString());
  EXPECT_EQ("Niels", value.GetConstRef().GetMember("name").GetString());
  ASSERT_TRUE(value.GetConstRef().GetMember("nothing").IsNull());
  ASSERT_TRUE(value.GetConstRef().GetMember("list").IsArray());
  EXPECT_EQ(3, value.GetConstRef().GetMember("list").GetArraySize());
  ASSERT_TRUE(value.GetConstRef().GetMember("object").IsObject());
}

TEST_P(JSONParserTest, ParseDuplicateKeys) {
  absl::flat_hash_map<std::string, std::string> input_to_expected_output;
  input_to_expected_output.emplace(R"(
    {
      "a":{"a":1},
      "a":{"b":2}
    }
  )",
  R"({"a":{"a":1}})");
  input_to_expected_output.emplace(R"(
    {
      "f":1,
      "f":{"a":1, "b":[1, 2, 3]},
      "g":1,
      "f":[{"d":{"a":1}}]
    }
  )",
  R"({"f":1,"g":1})");
  input_to_expected_output.emplace(R"(
    {
      "a":[{"a":1,"b":3,"a":4,"b":1}],
      "a":{"a":1, "b":[1, 2, 3]},
      "b":1,
      "f":[{"d":{"a":1}}],
      "f":[1,2,3]
    }
  )",
  R"({"a":[{"a":1,"b":3}],"b":1,"f":[{"d":{"a":1}}]})");
  input_to_expected_output.emplace(R"(
    {
      "f":1,
      "f":{"a":1, "b":[1, {"a":1, "b":[2], "a":{"a":1}}]},
      "g":1,
      "f":[{"a":{"a":1, "a":[{"a":1}]}}, 2]
    }
  )",
  R"({"f":1,"g":1})");
  input_to_expected_output.emplace(R"(
    {
      "a":
      [{
        "a":1,
        "a":2,
        "c":
        {
          "a":1,
          "b":1,
          "a":3,
          "c":["b","d","e"],
          "b":{}
        }
      }],
      "b":
      {
        "a":1,
        "b":[1, 2, 3],
        "c":[],
        "c":[{"a":1,"b":2}],
        "a":{}
      },
      "c":
      [
        {"a":1},
        {"a":1,"b":2,"a":2,"b":[{"c":1,"c":[2,3,4]}]},
        {"c":{}}
      ],
      "b":
      [
        {"d":{"a":1}}
      ],
      "a":1
    }
  )",
  absl::StrCat(R"({"a":[{"a":1,"c":{"a":1,"b":1,"c":["b","d","e"]}}],)",
               R"("b":{"a":1,"b":[1,2,3],"c":[]},"c":[{"a":1},{"a":1,"b":2},)",
               R"({"c":{}}]})"));
  input_to_expected_output.emplace(R"(
    {
      "a":1,
      "b":
      [
        {"d":{"a":1}}
      ],
      "b":
      {
        "a":1,
        "b":[1, 2, 3],
        "c":[],
        "c":[{"a":1,"b":2}],
        "a":{}
      },
      "a":[
        {},
        {"a":1,"b":5},
        {"a":[1,2,3,4,5],"b":{"a":2}},
        {"a":1}
      ],
      "b":
      {
        "b":{"b":2,"b":["b","b","b"],"c":{"b":2}},
        "a":{"b":2,"b":"d","a":["b","b","b"]},
        "b":{"b":[{},{"b":["b"],"b":{"b":{"b":2,"b":[1,2]},"b":{"b":5}}}]}
      },
      "c":
      [
        {"a":1},
        {"a":1,"b":2,"a":2,"b":[{"c":1,"c":[2,3,4]}]},
        {"c":{}}
      ],
      "a":
      [{
        "a":1,
        "a":2,
        "c":
        {
          "a":1,
          "b":1,
          "a":3,
          "c":["b","d","e"],
          "b":{}
        }
      }]
    }
  )",
  absl::StrCat(R"({"a":1,"b":[{"d":{"a":1}}],)",
               R"("c":[{"a":1},{"a":1,"b":2},)",
               R"({"c":{}}]})"));
  input_to_expected_output.emplace(R"(
    {
      "a":0,
      "a":1,
      "b":2,
      "c":3,
      "d":
      {
        "a":
        {
          "a":[{"a":4,"a":5}],
          "a":6
        },
        "b":7,
        "a":8,
        "b":9,
        "c":10,
        "b":11
      },
      "a":12,
      "b":13,
      "a":14,
      "c":15,
      "e":16,
      "f":[{"a":17},{"b":18,"a":19,"b":20},{"c":21,"d":22,"b":23,"c":24}],
      "f":25,
      "g":[{"a":17},{"b":18,"a":19,"b":20},{"c":21,"d":22,"b":23,"c":24}]
    }
  )",
  absl::StrCat(R"({"a":0,"b":2,"c":3,"d":{"a":{"a":[{"a":4}]},"b":7,)",
  R"("c":10},"e":16,"f":[{"a":17},{"a":19,"b":18},{"b":)",
  R"(23,"c":21,"d":22}],"g":[{"a":17},{"a":19,"b":18},)",
  R"({"b":23,"c":21,"d":22}]})"));

  for (const auto& pair : input_to_expected_output) {
    JSONValue json = JSONValue::ParseJSONString(pair.first, GetParam()).value();
    EXPECT_EQ(json.GetConstRef().ToString(), pair.second);
  }
}

INSTANTIATE_TEST_SUITE_P(
    CommonJSONParserTests, JSONParserTest,
    ::testing::Values(JSONParsingOptions{.legacy_mode = true,
                                         .strict_number_parsing = false},
                      JSONParsingOptions{.legacy_mode = false,
                                         .strict_number_parsing = false},
                      JSONParsingOptions{.legacy_mode = false,
                                         .strict_number_parsing = true}));

TEST(JSONStrictNumberParsingTest, NumberParsingSuccess) {
  JSONParsingOptions options{.legacy_mode = false,
                             .strict_number_parsing = true};
  absl::flat_hash_map<absl::string_view, absl::string_view> test_cases;
  test_cases.try_emplace("1", "1");
  test_cases.try_emplace("1e0", "1.0");
  test_cases.try_emplace("1.35235e2", "135.235");
  test_cases.try_emplace("1.35235e-2", "0.0135235");
  test_cases.try_emplace("1.0035023500e2", "100.350235");
  test_cases.try_emplace("-1.003502000000000000000000000", "-1.003502");
  test_cases.try_emplace("0.0000000000000001", "1e-16");
  test_cases.try_emplace("-0.0000000000000000000000000000000000000000000001",
                         "-1e-46");
  test_cases.try_emplace("1e-300", "1e-300");
  test_cases.try_emplace("10000000000000000000000000000", "1e+28");
  test_cases.try_emplace("234567.12345678", "234567.12345678");
  test_cases.try_emplace("-234567.12345678", "-234567.12345678");
  test_cases.try_emplace("234567.12345678e15", "2.3456712345678e+20");
  test_cases.try_emplace("-234567.12345678e-25", "-2.3456712345678e-20");
  test_cases.try_emplace("17.9769313486231e307", "1.79769313486231e+308");
  test_cases.try_emplace("-17.9769313486231e307", "-1.79769313486231e+308");
  test_cases.try_emplace("17.9769313486231e-309", "1.79769313486231e-308");
  test_cases.try_emplace("-17.9769313486231e-309", "-1.79769313486231e-308");

  for (const auto& pair : test_cases) {
    JSONValue value = JSONValue::ParseJSONString(pair.first, options).value();
    EXPECT_EQ(value.GetConstRef().ToString(), pair.second);
  }
}

TEST(JSONStrictNumberParsingTest, NumberParsingFailure) {
  constexpr char overflow_err[] = "number overflow parsing";
  constexpr char failed_to_parse_err[] = "Failed to parse";
  constexpr char roundtrip_err[] = "cannot round-trip through string";
  JSONParsingOptions options{.legacy_mode = false,
                             .strict_number_parsing = true};
  absl::flat_hash_map<std::string, std::string> test_cases;
  // Number overflow failure test cases
  test_cases.try_emplace("1e1000", overflow_err);
  test_cases.try_emplace("1.79769313486232e308", overflow_err);
  test_cases.try_emplace("-1.79769313486232e308", overflow_err);
  // Parsing failure - input string too long
  test_cases.try_emplace(absl::StrCat("1.0", std::string(1500, '0'), "1"),
                         "is too long");
  // Parsing failure - too many significant fractional digits
  test_cases.try_emplace(absl::StrCat("1.0", std::string(1074, '0'), "1"),
                         failed_to_parse_err);
  // Round-tripping test failures
  test_cases.try_emplace("1523.3523546364323253e2", roundtrip_err);
  test_cases.try_emplace("-1523.3523546364323253e-2", roundtrip_err);
  test_cases.try_emplace("-1.003502000000000000000000001", roundtrip_err);
  test_cases.try_emplace("1e-500", roundtrip_err);
  for (const auto& pair : test_cases) {
    EXPECT_THAT(
        JSONValue::ParseJSONString(pair.first, options).status().message(),
        ::testing::HasSubstr(pair.second))
        << "Input: " << pair.first;
  }
}

TEST(JSONLegacyParserTest, ParseSingleQuotes) {
  JSONValue value =
      JSONValue::ParseJSONString(
          "'abc'", JSONParsingOptions{.legacy_mode = true,
                                      .strict_number_parsing = false})
          .value();
  ASSERT_TRUE(value.GetConstRef().IsString());
  EXPECT_EQ("abc", value.GetConstRef().GetString());

  constexpr char json_str[] = R"(
    {
      "pi": 3.141,
      "happy": true,
      "name": "Niels",
      "nothing": null,
      'answer': {
        "everything": 42
      },
      "list": [1, 0, 2],
      "object": {
        "currency": 'USD',
        "value": 42.99
      }
    }
  )";
  value = JSONValue::ParseJSONString(
              json_str, JSONParsingOptions{.legacy_mode = true,
                                           .strict_number_parsing = false})
              .value();
  ASSERT_TRUE(value.GetConstRef().IsObject());
  EXPECT_EQ(value.GetConstRef().GetObjectSize(), 7);
  ASSERT_TRUE(value.GetConstRef().GetMember("pi").IsDouble());
  EXPECT_EQ(3.141, value.GetConstRef().GetMember("pi").GetDouble());
  ASSERT_TRUE(value.GetConstRef().GetMember("happy").IsBoolean());
  EXPECT_TRUE(value.GetConstRef().GetMember("happy").GetBoolean());
  ASSERT_TRUE(value.GetConstRef().GetMember("name").IsString());
  EXPECT_EQ("Niels", value.GetConstRef().GetMember("name").GetString());
  ASSERT_TRUE(value.GetConstRef().GetMember("nothing").IsNull());
  ASSERT_TRUE(value.GetConstRef().GetMember("list").IsArray());
  EXPECT_EQ(3, value.GetConstRef().GetMember("list").GetArraySize());
  ASSERT_TRUE(value.GetConstRef().GetMember("object").IsObject());
}

TEST(JSONStandardParserTest, ParseErrorStandard) {
  auto result = JSONValue::ParseJSONString("[[[");
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(
      result.status().message(),
      ::testing::HasSubstr(
          "syntax error while parsing value - unexpected end of input"));

  result = JSONValue::ParseJSONString("t");
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr(
                  "syntax error while parsing value - invalid literal"));

  result = JSONValue::ParseJSONString("[1, a]");
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr(
                  "syntax error while parsing value - invalid literal"));

  result = JSONValue::ParseJSONString("{a: b}");
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr(
                  "syntax error while parsing object key - invalid literal"));

  result = JSONValue::ParseJSONString("+");
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr(
                  "syntax error while parsing value - invalid literal"));
}

TEST(JSONLegacyParserTest, ParseErrorLegacy) {
  auto result = JSONValue::ParseJSONString(
      "[[[",
      JSONParsingOptions{.legacy_mode = true, .strict_number_parsing = false});
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr("Unexpected end of string"));

  result = JSONValue::ParseJSONString(
      "t",
      JSONParsingOptions{.legacy_mode = true, .strict_number_parsing = false});
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr("Unexpected token"));

  result = JSONValue::ParseJSONString(
      "[1, a]",
      JSONParsingOptions{.legacy_mode = true, .strict_number_parsing = false});
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr("Unexpected token"));

  result = JSONValue::ParseJSONString(
      "{a: b}",
      JSONParsingOptions{.legacy_mode = true, .strict_number_parsing = false});
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr("Non-string key encountered"));

  result = JSONValue::ParseJSONString(
      "+",
      JSONParsingOptions{.legacy_mode = true, .strict_number_parsing = false});
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr("Unknown token type"));
}

TEST(JSONValueTest, SerializePrimitiveValueToString) {
  JSONValue value;
  JSONValueRef ref = value.GetRef();

  constexpr char kStringValue[] = "value";
  ref.SetString(kStringValue);
  EXPECT_EQ(absl::Substitute("\"$0\"", kStringValue), ref.ToString());

  constexpr int64_t kIntValue = 1;
  ref.SetInt64(kIntValue);
  EXPECT_EQ(absl::Substitute("$0", kIntValue), ref.ToString());
}

TEST(JSONValueTest, SerializeObjectValueToString) {
  JSONValue value;
  JSONValueRef ref = value.GetRef();

  constexpr char kIntValKey[] = "key_int";
  constexpr int64_t kIntValue = 1;
  constexpr char kStringValKey[] = "key_str";
  constexpr char kStringValue[] = "value";

  ref.GetMember(kIntValKey).SetInt64(kIntValue);
  ref.GetMember(kStringValKey).SetString(kStringValue);
  EXPECT_EQ(absl::Substitute("{\"$0\":$1,\"$2\":\"$3\"}", kIntValKey, kIntValue,
                             kStringValKey, kStringValue),
            ref.ToString());
}

TEST(JSONValueTest, SerializeArrayValueToString) {
  JSONValue value;
  JSONValueRef ref = value.GetRef();

  constexpr bool kBooleanValue = true;
  constexpr uint64_t kIntValue = 1;
  ref.GetArrayElement(0).SetBoolean(kBooleanValue);
  ref.GetArrayElement(2).SetUInt64(kIntValue);

  EXPECT_EQ(absl::Substitute("[$0,null,$1]", kBooleanValue, kIntValue),
            ref.ToString());
}

TEST(JSONValueTest, Format) {
  // String value.
  EXPECT_EQ(
      JSONValue::ParseJSONString(R"("value")").value().GetConstRef().Format(),
      R"("value")");

  // Integer value.
  EXPECT_EQ(JSONValue::ParseJSONString("123").value().GetConstRef().Format(),
            "123");

  // Object value.
  EXPECT_EQ(
      JSONValue::ParseJSONString(R"({"key_int": 123, "key_str": "value"})")
          .value()
          .GetConstRef()
          .Format(),
      R"({
  "key_int": 123,
  "key_str": "value"
})");

  // Array value.
  EXPECT_EQ(JSONValue::ParseJSONString(R"([true, null, 123])")
                .value()
                .GetConstRef()
                .Format(),
            R"([
  true,
  null,
  123
])");

  // Nested value.
  EXPECT_EQ(JSONValue::ParseJSONString(R"({"key_arr": [true, null, 123], )"
                                       R"("key_int": 321, "key_str": "value"})")
                .value()
                .GetConstRef()
                .Format(),
            R"({
  "key_arr": [
    true,
    null,
    123
  ],
  "key_int": 321,
  "key_str": "value"
})");
}

TEST(JSONValueTest, ProtoBytesSerialization) {
  std::string test_json_strs[] = {"1", "null", "\"str\"", "[true, null, 5.0]",
                                  kJSONStr};

  for (const std::string& test_json_str : test_json_strs) {
    JSONValue value = JSONValue::ParseJSONString(test_json_str).value();
    std::string encoded_bytes;
    value.GetConstRef().SerializeAndAppendToProtoBytes(&encoded_bytes);
    JSONValue decoded_value =
        JSONValue::DeserializeFromProtoBytes(encoded_bytes).value();
    EXPECT_TRUE(
        decoded_value.GetConstRef().NormalizedEquals(value.GetConstRef()))
        << "Encoding test failed for '" << test_json_str << "'";
  }
}

TEST(JSONValueTest, DeserializeFromProtoBytesError) {
  auto result = JSONValue::DeserializeFromProtoBytes("invalid bytes");
  ASSERT_FALSE(result.status().ok());
  EXPECT_THAT(result.status().ToString(),
              testing::HasSubstr("syntax error while parsing UBJSON value"));
}

TEST(JSONValueTest, DeserializeFromProtoBytesMaxNestingLevel) {
  // Nesting level: 3
  JSONValue value =
      JSONValue::ParseJSONString(R"({"foo": [{"bar": 10}, 20]})").value();

  std::string encoded_bytes;
  value.GetConstRef().SerializeAndAppendToProtoBytes(&encoded_bytes);

  EXPECT_THAT(JSONValue::DeserializeFromProtoBytes(encoded_bytes,
                                                   /*max_nesting_level=*/2)
                  .status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Max nesting of 2 has been exceeded while "
                                 "parsing JSON document")));

  EXPECT_THAT(JSONValue::DeserializeFromProtoBytes(encoded_bytes,
                                                   /*max_nesting_level=*/0)
                  .status(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Max nesting of 0 has been exceeded while "
                                 "parsing JSON document")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(JSONValue decoded_value,
                       JSONValue::DeserializeFromProtoBytes(
                           encoded_bytes, /*max_nesting_level=*/3));
  EXPECT_TRUE(
      decoded_value.GetConstRef().NormalizedEquals(value.GetConstRef()));

  // Default value for 'max_nesting_level' is absl::nullopt which means no limit
  // is enforced.
  ZETASQL_ASSERT_OK_AND_ASSIGN(JSONValue decoded_value2,
                       JSONValue::DeserializeFromProtoBytes(encoded_bytes));
  EXPECT_TRUE(
      decoded_value2.GetConstRef().NormalizedEquals(value.GetConstRef()));
}

TEST(JSONValueTest, NestingLevelExceedsMaxTest) {
  std::vector<std::tuple<std::string, int, bool>> test_cases = {
      {"1", -1, false},
      {"1", 0, false},
      {"1", 1, false},
      {R"("abc")", 0, false},
      {R"("abc")", 1, false},
      {R"({})", -1, true},
      {R"({})", 0, true},
      {R"({})", 1, false},
      {R"([])", 0, true},
      {R"([])", 1, false},
      {R"([[]])", 0, true},
      {R"([[]])", 1, true},
      {R"([[]])", 2, false},
      {R"({"a":1})", 0, true},
      {R"({"a":1})", 1, false},
      {R"({"a":[1,2]})", 1, true},
      {R"({"a":[1,2]})", 2, false},
      {R"({"a":{"b":{"c":null}}})", 2, true},
      {R"({"a":{"b":{"c":null}}})", 3, false},
      {R"({"a":[{"1":1},{"2":2}],"b":[{"3":3},{"4":4}]})", 2, true},
      {R"({"a":[{"1":1},{"2":2}],"b":[{"3":3},{"4":4}]})", 3, false},
      {R"([{"a":1},{"a":2}])", 1, true},
      {R"([{"a":1},{"a":2}])", 2, false},
      {R"([{"a":1},{"a":2}])", 3, false},
  };
  for (const auto& [json, max_nesting, is_excess] : test_cases) {
    JSONValue value = JSONValue::ParseJSONString(json).value();
    EXPECT_EQ(value.GetConstRef().NestingLevelExceedsMax(max_nesting),
              is_excess);
  }
}

TEST(JSONValueTest, NormalizedEqualsNull) {
  constexpr int64_t kInt64Value = 1;
  JSONValue value;
  JSONValueRef ref = value.GetRef();
  JSONValue other;
  JSONValueRef other_ref = other.GetRef();

  EXPECT_TRUE(ref.NormalizedEquals(other_ref));

  ref.SetInt64(kInt64Value);
  EXPECT_FALSE(ref.NormalizedEquals(other_ref));
}

TEST(JSONValueTest, NormalizedEqualsInt64) {
  constexpr int64_t kInt64Value = 1;
  constexpr int64_t kInt64Value2 = 5;
  constexpr uint64_t kUInt64Value = 1;
  constexpr double kDoubleValue = 1;

  JSONValue value(kInt64Value);
  JSONValueRef ref = value.GetRef();

  JSONValue other;
  JSONValueRef other_ref = other.GetRef();

  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.SetInt64(kInt64Value2);
  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.SetUInt64(kUInt64Value);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));

  other_ref.SetDouble(kDoubleValue);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));

  other_ref.SetInt64(kInt64Value);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));
}

TEST(JSONValueTest, NormalizedEqualsUInt64) {
  constexpr uint64_t kUInt64Value = 1;
  constexpr uint64_t kUInt64Value2 = 5;
  constexpr int64_t kInt64Value = 1;
  constexpr double kDoubleValue = 1;

  JSONValue value(kUInt64Value);
  JSONValueRef ref = value.GetRef();

  JSONValue other;
  JSONValueRef other_ref = other.GetRef();

  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.SetUInt64(kUInt64Value2);
  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.SetInt64(kInt64Value);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));

  other_ref.SetDouble(kDoubleValue);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));

  other_ref.SetUInt64(kUInt64Value);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));
}

TEST(JSONValueTest, NormalizedEqualsDouble) {
  constexpr double kDoubleValue = 1;
  constexpr double kDoubleValue2 = 5.1;
  constexpr int64_t kInt64Value = 1;
  constexpr uint64_t kUInt64Value = 1;

  JSONValue value(kDoubleValue);
  JSONValueRef ref = value.GetRef();

  JSONValue other;
  JSONValueRef other_ref = other.GetRef();

  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.SetDouble(kDoubleValue2);
  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.SetInt64(kInt64Value);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));

  other_ref.SetUInt64(kUInt64Value);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));

  other_ref.SetDouble(kDoubleValue);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));
}

TEST(JSONValueTest, NormalizedEqualsString) {
  const std::string kStringValue = "test";
  const std::string kStringValue2 = "Test";

  JSONValue value(kStringValue);
  JSONValueRef ref = value.GetRef();

  JSONValue other;
  JSONValueRef other_ref = other.GetRef();

  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.SetString(kStringValue2);
  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.SetString(kStringValue);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));
}

TEST(JSONValueTest, NormalizedEqualsBoolean) {
  constexpr bool kBooleanValue = true;
  const std::string kStringValue = "true";
  constexpr int64_t kInt64Value = 1;

  JSONValue value(kBooleanValue);
  JSONValueRef ref = value.GetRef();

  JSONValue other;
  JSONValueRef other_ref = other.GetRef();

  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.SetString(kStringValue);
  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.SetInt64(kInt64Value);
  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.SetBoolean(kBooleanValue);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));
}

TEST(JSONValueTest, NormalizedEqualsObject) {
  constexpr int64_t kInt64Value = 1;
  constexpr int64_t kInt64Value2 = 3;
  const std::string kKey = "key";
  const std::string kKey2 = "key2";

  JSONValue value;
  JSONValueRef ref = value.GetRef();
  ref.GetMember(kKey).SetInt64(kInt64Value);

  JSONValue other;
  JSONValueRef other_ref = other.GetRef();

  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.GetMember(kKey).SetInt64(kInt64Value2);
  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  ref.GetMember(kKey2).SetInt64(kInt64Value2);
  other_ref.GetMember(kKey).SetInt64(kInt64Value);
  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.GetMember(kKey2).SetInt64(kInt64Value2);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));
}

TEST(JSONValueTest, NormalizedEqualsArray) {
  const std::string kStringValue = "test";
  constexpr int64_t kInt64Value = 1;

  JSONValue value;
  JSONValueRef ref = value.GetRef();
  ref.GetArrayElement(0).SetInt64(kInt64Value);

  JSONValue other;
  JSONValueRef other_ref = other.GetRef();

  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.GetArrayElement(1).SetInt64(kInt64Value);
  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  ref.GetArrayElement(1).SetString(kStringValue);
  other_ref.GetArrayElement(0).SetString(kStringValue);
  EXPECT_FALSE(ref.NormalizedEquals(other_ref));

  other_ref.GetArrayElement(0).SetInt64(kInt64Value);
  other_ref.GetArrayElement(1).SetString(kStringValue);
  EXPECT_TRUE(ref.NormalizedEquals(other_ref));

  other_ref.GetArrayElement(2);
  EXPECT_FALSE(ref.NormalizedEquals(other_ref));
}

TEST(JSONValueTest, ParseWithNestingLimit) {
  JSONParsingOptions options{.legacy_mode = false,
                             .strict_number_parsing = false,
                             .max_nesting = absl::nullopt};
  auto result = JSONValue::ParseJSONString("[10, 20]", options);
  ASSERT_TRUE(result.ok());
  EXPECT_THAT(result->GetConstRef().ToString(), "[10,20]");

  result = JSONValue::ParseJSONString(R"({"a": [10, 20]})", options);
  ASSERT_TRUE(result.ok());
  EXPECT_THAT(result->GetConstRef().ToString(), R"({"a":[10,20]})");

  options.max_nesting = -1;
  result = JSONValue::ParseJSONString("10", options);
  ASSERT_TRUE(result.ok());
  EXPECT_THAT(result->GetConstRef().ToString(), "10");

  result = JSONValue::ParseJSONString("[10, 20]", options);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(
      result.status().message(),
      HasSubstr(
          "Max nesting of 0 has been exceeded while parsing JSON document"));

  options.max_nesting = 0;
  result = JSONValue::ParseJSONString(R"({"a": 10})", options);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(
      result.status().message(),
      HasSubstr(
          "Max nesting of 0 has been exceeded while parsing JSON document"));

  options.max_nesting = 1;
  result = JSONValue::ParseJSONString("[10, 20]", options);
  ASSERT_TRUE(result.ok());
  EXPECT_THAT(result->GetConstRef().ToString(), "[10,20]");

  result = JSONValue::ParseJSONString(R"({"a": 10})", options);
  ASSERT_TRUE(result.ok());
  EXPECT_THAT(result->GetConstRef().ToString(), R"({"a":10})");

  result = JSONValue::ParseJSONString(R"({"a": [10, 20]})", options);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(
      result.status().message(),
      HasSubstr(
          "Max nesting of 1 has been exceeded while parsing JSON document"));

  options.max_nesting = 3;
  result = JSONValue::ParseJSONString(
      R"({"a": [{"b": "foo"}, {"b": "bar"}], "c": 10})", options);
  ASSERT_TRUE(result.ok());
  EXPECT_THAT(result->GetConstRef().ToString(),
              R"({"a":[{"b":"foo"},{"b":"bar"}],"c":10})");

  result = JSONValue::ParseJSONString(
      R"({"a": [{"b": "foo"}, {"b": {"c": 10}}], "d": 10})", options);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(
      result.status().message(),
      HasSubstr(
          "Max nesting of 3 has been exceeded while parsing JSON document"));
}

}  // namespace
