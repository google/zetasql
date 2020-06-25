//
// Copyright 2019 ZetaSQL Authors
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

#include <stddef.h>
#include <string.h>

#include <limits>
#include <string>
#include <utility>
#include <vector>


#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <cstdint>  
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/statusor.h"

namespace {

using ::zetasql::JSONValue;
using ::zetasql::JSONValueConstRef;
using ::zetasql::JSONValueRef;
using ::testing::HasSubstr;

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
  constexpr char key[] = "key";
  JSONValue value;

  JSONValueRef ref = value.GetRef();
  JSONValueRef member_ref = ref.GetMember(key);

  EXPECT_FALSE(ref.IsNull());
  ASSERT_TRUE(ref.IsObject());
  EXPECT_FALSE(ref.IsBoolean());
  EXPECT_FALSE(ref.IsArray());
  EXPECT_FALSE(ref.IsString());
  EXPECT_FALSE(ref.IsNumber());

  EXPECT_TRUE(member_ref.IsNull());

  member_ref.SetInt64(kIntegerValue);

  EXPECT_FALSE(ref.GetMember(key).IsNull());
  EXPECT_EQ(kIntegerValue, ref.GetMember(key).GetInt64());

  std::vector<std::pair<absl::string_view, JSONValueRef>> members =
      ref.GetMembers();

  EXPECT_EQ(1, members.size());
  EXPECT_EQ(key, members[0].first);
  EXPECT_EQ(kIntegerValue, members[0].second.GetInt64());
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

class JSONParserTest : public ::testing::TestWithParam<bool> {};

TEST_P(JSONParserTest, ParseString) {
  JSONValue value = JSONValue::ParseJSONString("\"str\"", GetParam()).value();
  ASSERT_TRUE(value.GetConstRef().IsString());
  EXPECT_EQ("str", value.GetConstRef().GetString());
}

TEST_P(JSONParserTest, ParseInt) {
  JSONValue value = JSONValue::ParseJSONString("1", GetParam()).value();
  ASSERT_TRUE(value.GetConstRef().IsInt64());
  EXPECT_EQ(1, value.GetConstRef().GetInt64());
}

TEST_P(JSONParserTest, ParseUInt) {
  uint64_t uint64_value =
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1;
  JSONValue value =
      JSONValue::ParseJSONString(absl::Substitute("$0", uint64_value), true)
          .value();
  ASSERT_TRUE(value.GetConstRef().IsInt64());
  EXPECT_EQ(uint64_value, value.GetConstRef().GetInt64());
}

TEST_P(JSONParserTest, ParseDouble) {
  JSONValue value = JSONValue::ParseJSONString("1.5", GetParam()).value();
  ASSERT_TRUE(value.GetConstRef().IsDouble());
  EXPECT_EQ(1.5, value.GetConstRef().GetDouble());
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
  constexpr char json_str[] = R"(
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
  JSONValue value = JSONValue::ParseJSONString(json_str, GetParam()).value();
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

INSTANTIATE_TEST_SUITE_P(CommonJSONParserTests, JSONParserTest,
                         ::testing::Values(true, false));

TEST(JSONLegacyParserTest, ParseSingleQuotes) {
  JSONValue value = JSONValue::ParseJSONString("'abc'", true).value();
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
  value = JSONValue::ParseJSONString(json_str, true).value();
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

TEST(JSONLegacyParserTest, ParseErrorStandard) {
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
  auto result = JSONValue::ParseJSONString("[[[", true);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr("Unexpected end of string"));

  result = JSONValue::ParseJSONString("t", true);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr("Unexpected token"));

  result = JSONValue::ParseJSONString("[1, a]", true);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr("Unexpected token"));

  result = JSONValue::ParseJSONString("{a: b}", true);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr("Non-string key encountered"));

  result = JSONValue::ParseJSONString("+", true);
  EXPECT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              ::testing::HasSubstr("Unknown token type"));
}

TEST(JSONValueTest, SerializePrimitiveValueToString) {
  JSONValue value;
  JSONValueRef ref = value.GetRef();

  constexpr char kStringValue[] = "value";
  ref.SetString(kStringValue);
  EXPECT_EQ(absl::Substitute("\"$0\"", kStringValue), ref.SerializeToString());

  constexpr int64_t kIntValue = 1;
  ref.SetInt64(kIntValue);
  EXPECT_EQ(absl::Substitute("$0", kIntValue), ref.SerializeToString());
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
            ref.SerializeToString());
}

TEST(JSONValueTest, SerializeArrayValueToString) {
  JSONValue value;
  JSONValueRef ref = value.GetRef();

  constexpr bool kBooleanValue = true;
  constexpr uint64_t kIntValue = 1;
  ref.GetArrayElement(0).SetBoolean(true);
  ref.GetArrayElement(2).SetUInt64(kIntValue);

  EXPECT_EQ(absl::Substitute("[$0,null,$1]", kBooleanValue, kIntValue),
            ref.SerializeToString());
}

}  // namespace
