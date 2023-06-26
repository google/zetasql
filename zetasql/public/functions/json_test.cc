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

// Tests for the ZetaSQL JSON functions.

#include "zetasql/public/functions/json.h"

#include <stddef.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/testing/proto_matchers.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/functions/json_internal.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace functions {
namespace {

using ::testing::ElementsAreArray;
using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

MATCHER_P(JsonEq, expected, expected.ToString()) {
  *result_listener << arg.ToString();
  return arg.NormalizedEquals(expected);
}

// Note that the compliance tests below are more exhaustive.
TEST(JsonTest, StringJsonExtract) {
  const std::string json =
      R"({"a": {"b": [ { "c" : "foo" } ], "d": {"b\"ar": "q\"w"} } })";
  const std::vector<std::pair<std::string, std::string>> inputs_and_outputs = {
      // This output contains an unescaped key and value because escaping is
      // disabled.
      {"$.a", R"({"b":[{"c":"foo"}],"d":{"b"ar":"q"w"}})"},
      {"$.a.b", R"([{"c":"foo"}])"},
      {"$.a.b[0]", R"({"c":"foo"})"},
      {"$.a.b[0].c", R"("foo")"}};
  for (const auto& input_and_output : inputs_and_outputs) {
    SCOPED_TRACE(absl::Substitute("JSON_EXTRACT('$0', '$1')", json,
                                  input_and_output.first));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            input_and_output.first,
            /*sql_standard_mode=*/false,
            /*enable_special_character_escaping_in_values=*/false,
            /*enable_special_character_escaping_in_keys=*/false));
    std::string value;
    bool is_null;
    bool is_warning_called = false;
    ZETASQL_ASSERT_OK(evaluator->Extract(
        json, &value, &is_null,
        [&](absl::Status status) { is_warning_called = true; }));
    EXPECT_FALSE(is_null);
    EXPECT_FALSE(is_warning_called);
    EXPECT_EQ(input_and_output.second, value);
  }
}

TEST(JsonTest, StringJsonExtractKeyEscapingDisabled) {
  const std::string json =
      R"({"foo": {"b\"ar": "q\"w"}, "foo_array": [{"b\"ar": "q\"w"}] })";
  {
    // Only enable value escaping and not key escaping. This should result in
    // unescaped keys.
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            "$.foo",
            /*sql_standard_mode=*/true,
            /*enable_special_character_escaping_in_values=*/true,
            /*enable_special_character_escaping_in_keys=*/false));
    std::string value;
    bool is_null;
    absl::Status result_status = absl::OkStatus();
    ZETASQL_ASSERT_OK(evaluator->Extract(
        json, &value, &is_null,
        [&](absl::Status status) { result_status = status; }));
    EXPECT_FALSE(is_null);
    EXPECT_TRUE(!result_status.ok());
    EXPECT_EQ(R"({"b"ar":"q\"w"})", value);
  }
  {
    // Only enable value escaping and not key escaping. This should result in
    // unescaped keys.
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            "$.foo_array",
            /*sql_standard_mode=*/true,
            /*enable_special_character_escaping_in_values=*/true,
            /*enable_special_character_escaping_in_keys=*/false));
    std::vector<std::string> result;
    bool is_null;
    absl::Status result_status = absl::OkStatus();
    ZETASQL_ASSERT_OK(evaluator->ExtractArray(
        json, &result, &is_null,
        [&](absl::Status status) { result_status = status; }));
    EXPECT_FALSE(is_null);
    EXPECT_TRUE(!result_status.ok());
    EXPECT_THAT(result, ::testing::ElementsAre(R"({"b"ar":"q\"w"})"));
  }
}

TEST(JsonTest, StringJsonExtractKeyEscapingEnabledValueDisabled) {
  const std::string json =
      R"({"foo": {"b\"ar": "q\"w"}, "foo_array": [{"b\"ar": "q\"w"}] })";
  {
    // Only enable key escaping. Because value escaping is not enabled there
    // should be no escaping of keys or values.
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            "$.foo",
            /*sql_standard_mode=*/true,
            /*enable_special_character_escaping_in_values=*/false,
            /*enable_special_character_escaping_in_keys=*/true));
    std::string value;
    bool is_null;
    bool is_warning_called = false;
    ZETASQL_ASSERT_OK(evaluator->Extract(
        json, &value, &is_null,
        [&](absl::Status status) { is_warning_called = true; }));
    EXPECT_FALSE(is_null);
    EXPECT_FALSE(is_warning_called);
    ZETASQL_ASSERT_OK(evaluator->Extract(json, &value, &is_null,
                                 [&is_warning_called](absl::Status status) {
                                   is_warning_called = true;
                                 }));
    EXPECT_FALSE(is_null);
    EXPECT_FALSE(is_warning_called);
    EXPECT_EQ(value, R"({"b"ar":"q"w"})");
  }
  {
    // Only enable key escaping. Because value escaping is not enabled there
    // should be no escaping of keys or values.
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            "$.foo_array",
            /*sql_standard_mode=*/true,
            /*enable_special_character_escaping_in_values=*/false,
            /*enable_special_character_escaping_in_keys=*/true));
    std::vector<std::string> result;
    bool is_null;
    bool is_warning_called = false;
    ZETASQL_ASSERT_OK(evaluator->ExtractArray(
        json, &result, &is_null, [&is_warning_called](absl::Status status) {
          is_warning_called = true;
        }));
    EXPECT_FALSE(is_null);
    EXPECT_FALSE(is_warning_called);
    EXPECT_THAT(result, ::testing::ElementsAre(R"({"b"ar":"q"w"})"));
  }
}

TEST(JsonTest, StringJsonExtractKeyAndValueEscapingEnabled) {
  const std::string json =
      R"({"foo": {"b\"ar": "q\"w"}, "foo_array": [{"b\"ar": "q\"w"}] })";
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            "$.foo",
            /*sql_standard_mode=*/true,
            /*enable_special_character_escaping_in_values=*/true,
            /*enable_special_character_escaping_in_keys=*/true));
    std::string value;
    bool is_null;
    bool is_warning_called = false;
    ZETASQL_ASSERT_OK(evaluator->Extract(json, &value, &is_null,
                                 [&is_warning_called](absl::Status status) {
                                   is_warning_called = true;
                                 }));
    EXPECT_FALSE(is_null);
    EXPECT_FALSE(is_warning_called);
    EXPECT_EQ(R"({"b\"ar":"q\"w"})", value);
  }
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            "$.foo_array",
            /*sql_standard_mode=*/true,
            /*enable_special_character_escaping_in_values=*/true,
            /*enable_special_character_escaping_in_keys=*/true));
    std::vector<std::string> result;
    bool is_null;
    bool is_warning_called = false;
    ZETASQL_ASSERT_OK(evaluator->ExtractArray(
        json, &result, &is_null, [&is_warning_called](absl::Status status) {
          is_warning_called = true;
        }));
    EXPECT_FALSE(is_null);
    EXPECT_FALSE(is_warning_called);
    EXPECT_THAT(result, ::testing::ElementsAre(R"({"b\"ar":"q\"w"})"));
  }
}

class MockEscapingNeededCallback {
 public:
  MOCK_METHOD(void, Call, (absl::string_view));
};

TEST(JsonTest, JsonEscapingNeededCallback) {
  const std::string json = R"({"a": {"b": [ { "c" : "\t" } ] } })";
  const std::string input = "$.a.b[0].c";
  const std::string output = "\"\t\"";

  SCOPED_TRACE(absl::Substitute("JSON_EXTRACT('$0', '$1')", json, input));
  MockEscapingNeededCallback callback;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<JsonPathEvaluator> evaluator,
      JsonPathEvaluator::Create(
          input,
          /*sql_standard_mode=*/false,
          /*enable_special_character_escaping_in_values=*/false,
          /*enable_special_character_escaping_in_keys=*/false));
  evaluator->set_escaping_needed_callback(
      [&](absl::string_view str, bool is_key) {
        callback.Call(str);
        EXPECT_FALSE(is_key);
      });
  EXPECT_CALL(callback, Call("\t"));
  std::string value;
  bool is_null;
  ZETASQL_ASSERT_OK(evaluator->Extract(json, &value, &is_null));
  EXPECT_EQ(output, value);
  EXPECT_FALSE(is_null);
}

TEST(JsonTest, JsonKeyEscapingNeededCallback) {
  // This json contains an unescaped key.
  const std::string json = R"({"a": {"b": [ { "c\"ar" : "t" } ] } })";
  const std::string input = "$.a.b[0]";
  // b/265948860: When escaping special characters in keys, this output
  // should be: R"({"c\"ar":"t"})".
  const std::string output = R"({"c"ar":"t"})";

  SCOPED_TRACE(absl::Substitute("JSON_EXTRACT('$0', '$1')", json, input));
  MockEscapingNeededCallback callback;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<JsonPathEvaluator> evaluator,
      JsonPathEvaluator::Create(
          input,
          /*sql_standard_mode=*/false,
          /*enable_special_character_escaping_in_values=*/false,
          /*enable_special_character_escaping_in_keys=*/false));
  evaluator->set_escaping_needed_callback(
      [&](absl::string_view str, bool is_key) {
        callback.Call(str);
        EXPECT_TRUE(is_key);
      });
  EXPECT_CALL(callback, Call("c\"ar"));
  std::string value;
  bool is_null;
  ZETASQL_ASSERT_OK(evaluator->Extract(json, &value, &is_null));
  EXPECT_EQ(output, value);
  EXPECT_FALSE(is_null);
}

TEST(JsonTest, NativeJsonExtract) {
  const JSONValue json =
      JSONValue::ParseJSONString(R"({"a": {"b": [ { "c" : "foo" } ] } })")
          .value();
  JSONValueConstRef json_ref = json.GetConstRef();
  const std::vector<std::pair<std::string, std::string>> inputs_and_outputs = {
      {"$", R"({"a":{"b":[{"c":"foo"}]}})"},
      {"$.a", R"({"b":[{"c":"foo"}]})"},
      {"$.a.b", R"([{"c":"foo"}])"},
      {"$.a.b[0]", R"({"c":"foo"})"},
      {"$.a.b[0].c", R"("foo")"}};
  for (const auto& [input, output] : inputs_and_outputs) {
    SCOPED_TRACE(absl::Substitute("JSON_EXTRACT('$0', '$1')",
                                  json_ref.ToString(), input));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            input,
            /*sql_standard_mode=*/false,
            /*enable_special_character_escaping_in_values=*/false,
            /*enable_special_character_escaping_in_keys=*/false));

    std::optional<JSONValueConstRef> result = evaluator->Extract(json_ref);
    EXPECT_TRUE(result.has_value());
    if (result.has_value()) {
      EXPECT_THAT(
          result.value(),
          JsonEq(JSONValue::ParseJSONString(output).value().GetConstRef()));
    }
  }
}

TEST(JsonTest, StringJsonExtractScalar) {
  const std::string json = R"({"a": {"b": [ { "c" : "foo" } ] } })";
  const std::vector<std::pair<std::string, std::string>> inputs_and_outputs = {
      {"$", ""},
      {"$.a", ""},
      {"$.a.b", ""},
      {"$.a.b[0]", ""},
      {"$.a.b[0].c", "foo"}};
  for (const auto& input_and_output : inputs_and_outputs) {
    SCOPED_TRACE(absl::Substitute("JSON_EXTRACT_SCALAR('$0', '$1')", json,
                                  input_and_output.first));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            input_and_output.first,
            /*sql_standard_mode=*/false,
            /*enable_special_character_escaping_in_values=*/false,
            /*enable_special_character_escaping_in_keys=*/false));
    std::string value;
    bool is_null;
    ZETASQL_ASSERT_OK(evaluator->ExtractScalar(json, &value, &is_null));
    if (!input_and_output.second.empty()) {
      EXPECT_EQ(input_and_output.second, value);
      EXPECT_FALSE(is_null);
    } else {
      EXPECT_TRUE(is_null);
    }
  }
}

TEST(JsonTest, NativeJsonExtractScalar) {
  const JSONValue json =
      JSONValue::ParseJSONString(
          R"({"a": {"b": [ { "c" : "foo" } ], "d": 1, "e": -5, )"
          R"("f": true, "g": 4.2 } })")
          .value();
  JSONValueConstRef json_ref = json.GetConstRef();
  const std::vector<std::pair<std::string, std::string>> inputs_and_outputs = {
      {"$", ""},       {"$.a", ""},       {"$.a.d", "1"},
      {"$.a.e", "-5"}, {"$.a.f", "true"}, {"$.a.g", "4.2"},
      {"$.a.b", ""},   {"$.a.b[0]", ""},  {"$.a.b[0].c", "foo"}};
  for (const auto& [input, output] : inputs_and_outputs) {
    SCOPED_TRACE(absl::Substitute("JSON_EXTRACT_SCALAR('$0', '$1')",
                                  json_ref.ToString(), input));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            input,
            /*sql_standard_mode=*/false,
            /*enable_special_character_escaping_in_values=*/false,
            /*enable_special_character_escaping_in_keys=*/false));

    std::optional<std::string> result = evaluator->ExtractScalar(json_ref);
    if (!output.empty()) {
      ASSERT_TRUE(result.has_value());
      EXPECT_EQ(output, result.value());
    } else {
      EXPECT_FALSE(result.has_value());
    }
  }
}

TEST(JsonTest, NativeJsonExtractJsonArray) {
  auto json_value = JSONValue::ParseJSONString(
      R"({"a": {"b": [ { "c" : "foo" }, 15, null, "bar", )"
      R"([ 20, { "a": "baz" } ] ] } })");
  ZETASQL_ASSERT_OK(json_value.status());
  JSONValueConstRef json_ref = json_value->GetConstRef();

  const std::vector<
      std::pair<std::string, std::optional<std::vector<std::string>>>>
      inputs_and_outputs = {{"$", std::nullopt},
                            {"$.a", std::nullopt},
                            {"$.a.b",
                             {{R"({"c":"foo"})", "15", "null", "\"bar\"",
                               R"([20,{"a":"baz"}])"}}},
                            {"$.a.b[0]", std::nullopt},
                            {"$.a.b[4]", {{"20", R"({"a":"baz"})"}}}};
  for (const auto& [input, output] : inputs_and_outputs) {
    SCOPED_TRACE(absl::Substitute("JSON_EXTRACT_ARRAY('$0', '$1')",
                                  json_ref.ToString(), input));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            input,
            /*sql_standard_mode=*/false,
            /*enable_special_character_escaping_in_values=*/false,
            /*enable_special_character_escaping_in_keys=*/false));

    std::optional<std::vector<JSONValueConstRef>> result =
        evaluator->ExtractArray(json_ref);
    if (output.has_value()) {
      ASSERT_TRUE(result.has_value());

      std::vector<JSONValue> json_value_store;
      std::vector<::testing::Matcher<JSONValueConstRef>> expected_result;
      json_value_store.reserve(output->size());
      expected_result.reserve(output->size());
      for (const std::string& string_value : *output) {
        json_value_store.emplace_back();
        ZETASQL_ASSERT_OK_AND_ASSIGN(json_value_store.back(),
                             JSONValue::ParseJSONString(string_value));
        expected_result.push_back(
            JsonEq(json_value_store.back().GetConstRef()));
      }

      EXPECT_THAT(*result, ElementsAreArray(expected_result));
    } else {
      EXPECT_FALSE(result.has_value());
    }
  }
}

TEST(JsonTest, NativeJsonExtractStringArray) {
  auto json_value = JSONValue::ParseJSONString(
      R"({"a": {"b": [ { "c" : "foo" }, 15, null, "bar", )"
      R"([ 20, "a", true ] ] } })");
  ZETASQL_ASSERT_OK(json_value.status());
  JSONValueConstRef json_ref = json_value->GetConstRef();
  const std::vector<
      std::pair<std::string, std::optional<std::vector<std::string>>>>
      inputs_and_outputs = {{"$", std::nullopt},
                            {"$.a", std::nullopt},
                            {"$.a.b", std::nullopt},
                            {"$.a.b[0]", std::nullopt},
                            {"$.a.b[4]", {{"20", "a", "true"}}}};
  for (const auto& [input, output] : inputs_and_outputs) {
    SCOPED_TRACE(absl::Substitute("JSON_EXTRACT_STRING_ARRAY('$0', '$1')",
                                  json_ref.ToString(), input));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            input,
            /*sql_standard_mode=*/false,
            /*enable_special_character_escaping_in_values=*/false,
            /*enable_special_character_escaping_in_keys=*/false));
    std::optional<std::vector<std::optional<std::string>>> result =
        evaluator->ExtractStringArray(json_ref);
    if (output.has_value()) {
      ASSERT_TRUE(result.has_value());
      EXPECT_THAT(*result, ::testing::Pointwise(::testing::Eq(), *output));
    } else {
      EXPECT_FALSE(result.has_value());
    }
  }
}

void ExpectExtractScalar(absl::string_view json, absl::string_view path,
                         absl::string_view expected) {
  SCOPED_TRACE(absl::Substitute("JSON_EXTRACT_SCALAR('$0', '$1')", json, path));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<JsonPathEvaluator> evaluator,
      JsonPathEvaluator::Create(
          path, /*sql_standard_mode=*/true,
          /*enable_special_character_escaping_in_values=*/false,
          /*enable_special_character_escaping_in_keys=*/false));
  std::string value;
  bool is_null;
  ZETASQL_ASSERT_OK(evaluator->ExtractScalar(json, &value, &is_null));
  if (!expected.empty()) {
    EXPECT_EQ(expected, value);
    EXPECT_FALSE(is_null);
  } else {
    EXPECT_TRUE(is_null);
  }
}

TEST(JsonTest, StringJsonExtractScalarBadBehavior) {
  // This is almost certainly an unintentional bug in the implementation. The
  // root cause is that, in general, parsing stops once the scalar is found.
  // Thus what the parser sees is for example '"{"a": 0"<etc>'.  So all manner
  // of terrible stuff can be beyond the parsed string.

  // It is not clear if this is desired behavior, for now, this simply records
  // that this is the _current_ behavior.
  ExpectExtractScalar(R"({"a": 0001})", "$.a", "0");
  ExpectExtractScalar(R"({"a": 123abc})", "$.a", "123");
  ExpectExtractScalar(R"({"a": 1ab\\unicorn\0{{{{{{)", "$.a", "1");
}

TEST(JsonTest, StringJsonExtractScalarExpectVeryLongIntegersPassthrough) {
  std::string long_integer_str(500, '1');
  ZETASQL_CHECK_EQ(long_integer_str.size(), 500);
  ExpectExtractScalar(absl::StrFormat(R"({"a": %s})", long_integer_str), "$.a",
                      long_integer_str);
}

TEST(JsonTest, StringJsonCompliance) {
  std::vector<std::vector<FunctionTestCall>> all_tests = {
      GetFunctionTestsStringJsonQuery(), GetFunctionTestsStringJsonExtract(),
      GetFunctionTestsStringJsonValue(),
      GetFunctionTestsStringJsonExtractScalar()};
  for (const std::vector<FunctionTestCall>& tests : all_tests) {
    for (const FunctionTestCall& test : tests) {
      if (test.params.params()[0].is_null() ||
          (test.params.params().size() > 1 &&
           test.params.params()[1].is_null())) {
        continue;
      }

      const std::string json = test.params.param(0).string_value();
      const std::string json_path = (test.params.params().size() > 1)
                                        ? test.params.param(1).string_value()
                                        : "$";
      SCOPED_TRACE(absl::Substitute("$0('$1', '$2')", test.function_name, json,
                                    json_path));

      std::string value;
      bool is_null = false;
      absl::Status status;
      bool sql_standard_mode = test.function_name == "json_query" ||
                               test.function_name == "json_value";
      auto evaluator_status = JsonPathEvaluator::Create(
          json_path, sql_standard_mode,
          /*enable_special_character_escaping_in_values=*/true,
          /*enable_special_character_escaping_in_keys=*/true);
      if (evaluator_status.ok()) {
        const std::unique_ptr<JsonPathEvaluator>& evaluator =
            evaluator_status.value();
        if (test.function_name == "json_extract" ||
            test.function_name == "json_query") {
          bool is_warning_called = false;
          status =
              evaluator->Extract(json, &value, &is_null,
                                 [&is_warning_called](absl::Status status) {
                                   is_warning_called = true;
                                 });
          // Because key_escaping is enabled a warning should never be
          // triggered.
          EXPECT_FALSE(is_warning_called);
        } else {
          status = evaluator->ExtractScalar(json, &value, &is_null);
        }
      } else {
        status = evaluator_status.status();
      }
      if (!status.ok() || !test.params.status().ok()) {
        EXPECT_EQ(test.params.status().code(), status.code()) << status;
      } else {
        EXPECT_EQ(test.params.result().is_null(), is_null);
        if (!test.params.result().is_null() && !is_null) {
          EXPECT_EQ(test.params.result().string_value(), value);
        }
      }
    }
  }
}

TEST(JsonTest, NativeJsonCompliance) {
  std::vector<std::vector<FunctionTestCall>> all_tests = {
      GetFunctionTestsNativeJsonQuery(), GetFunctionTestsNativeJsonExtract(),
      GetFunctionTestsNativeJsonValue(),
      GetFunctionTestsNativeJsonExtractScalar()};
  for (const std::vector<FunctionTestCall>& tests : all_tests) {
    for (const FunctionTestCall& test : tests) {
      if (test.params.params()[0].is_null() ||
          (test.params.params().size() > 1 &&
           test.params.params()[1].is_null())) {
        continue;
      }
      if (test.params.param(0).is_unparsed_json()) {
        // Unvalidated JSON will be tested in compliance testing, not in unit
        // tests.
        continue;
      }
      const JSONValueConstRef json = test.params.param(0).json_value();
      const std::string json_path = (test.params.params().size() > 1)
                                        ? test.params.param(1).string_value()
                                        : "$";
      SCOPED_TRACE(absl::Substitute("$0('$1', '$2')", test.function_name,
                                    json.ToString(), json_path));

      absl::Status status;
      bool sql_standard_mode = test.function_name == "json_query" ||
                               test.function_name == "json_value";
      auto evaluator_status = JsonPathEvaluator::Create(
          json_path, sql_standard_mode,
          /*enable_special_character_escaping_in_values=*/false,
          /*enable_special_character_escaping_in_keys=*/false);
      if (evaluator_status.ok()) {
        const std::unique_ptr<JsonPathEvaluator>& evaluator =
            evaluator_status.value();
        if (test.function_name == "json_extract" ||
            test.function_name == "json_query") {
          std::optional<JSONValueConstRef> result_or = evaluator->Extract(json);
          EXPECT_EQ(test.params.result().is_null(), !result_or.has_value());
          if (!test.params.result().is_null() && result_or.has_value()) {
            EXPECT_THAT(result_or.value(),
                        JsonEq(test.params.result().json_value()));
          }
        } else {
          std::optional<std::string> result_or = evaluator->ExtractScalar(json);
          EXPECT_EQ(test.params.result().is_null(), !result_or.has_value());
          if (!test.params.result().is_null() && result_or.has_value()) {
            EXPECT_EQ(result_or.value(), test.params.result().string_value());
          }
        }
      } else {
        status = evaluator_status.status();
      }
      if (!status.ok() || !test.params.status().ok()) {
        EXPECT_EQ(test.params.status().code(), status.code()) << status;
      }
    }
  }
}

TEST(JsonTest, NativeJsonArrayCompliance) {
  const std::vector<std::vector<FunctionTestCall>> all_tests = {
      GetFunctionTestsNativeJsonQueryArray(),
      GetFunctionTestsNativeJsonExtractArray(),
      GetFunctionTestsNativeJsonValueArray(),
      GetFunctionTestsNativeJsonExtractStringArray()};

  for (const std::vector<FunctionTestCall>& tests : all_tests) {
    for (const FunctionTestCall& test : tests) {
      if (test.params.params()[0].is_null() ||
          test.params.params()[1].is_null()) {
        continue;
      }
      if (!test.params.param(0).is_validated_json()) {
        // Unvalidated JSON will be tested in compliance testing, not in unit
        // tests.
        continue;
      }
      const JSONValueConstRef json = test.params.param(0).json_value();
      const std::string json_path = test.params.param(1).string_value();
      SCOPED_TRACE(absl::Substitute("$0('$1', '$2')", test.function_name,
                                    json.ToString(), json_path));

      absl::Status status;
      bool sql_standard_mode = test.function_name == "json_query_array" ||
                               test.function_name == "json_value_array";
      auto evaluator_status = JsonPathEvaluator::Create(
          json_path, sql_standard_mode,
          /*enable_special_character_escaping_in_values=*/false,
          /*enable_special_character_escaping_in_keys=*/false);
      if (evaluator_status.ok()) {
        std::unique_ptr<JsonPathEvaluator> evaluator =
            std::move(evaluator_status).value();
        if (test.function_name == "json_extract_array" ||
            test.function_name == "json_query_array") {
          std::optional<std::vector<JSONValueConstRef>> result =
              evaluator->ExtractArray(json);

          EXPECT_EQ(test.params.result().is_null(), !result.has_value());
          if (!test.params.result().is_null() && result.has_value()) {
            std::vector<::testing::Matcher<JSONValueConstRef>> expected_result;
            expected_result.reserve(test.params.result().num_elements());
            for (const Value& value : test.params.result().elements()) {
              expected_result.push_back(JsonEq(value.json_value()));
            }

            EXPECT_THAT(*result, ElementsAreArray(expected_result));
          }
        } else {
          std::optional<std::vector<std::optional<std::string>>> result =
              evaluator->ExtractStringArray(json);
          EXPECT_EQ(test.params.result().is_null(), !result.has_value());
          if (!test.params.result().is_null() && result.has_value()) {
            std::vector<Value> string_array_result;
            string_array_result.reserve(result->size());
            for (const auto& element : *result) {
              string_array_result.push_back(element.has_value()
                                                ? values::String(*element)
                                                : values::NullString());
            }
            EXPECT_EQ(values::UnsafeArray(types::StringArrayType(),
                                          std::move(string_array_result)),
                      test.params.result());
          }
        }
      } else {
        status = evaluator_status.status();
      }
      if (!status.ok() || !test.params.status().ok()) {
        EXPECT_EQ(test.params.status().code(), status.code()) << status;
      }
    }
  }
}

TEST(JsonPathTest, JsonPathEndedWithDotNonStandardMode) {
  const std::string json = R"({"a": {"b": [ { "c" : "foo" } ] } })";
  const std::vector<std::pair<std::string, std::string>> inputs_and_outputs = {
      {"$.", R"({"a":{"b":[{"c":"foo"}]}})"},
      {"$.a.", R"({"b":[{"c":"foo"}]})"},
      {"$.a.b.", R"([{"c":"foo"}])"},
      {"$.a.b[0].", R"({"c":"foo"})"},
      {"$.a.b[0].c.", R"("foo")"}};
  for (const auto& input_and_output : inputs_and_outputs) {
    SCOPED_TRACE(absl::Substitute("JSON_EXTRACT('$0', '$1')", json,
                                  input_and_output.first));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(
            input_and_output.first,
            /*sql_standard_mode=*/false,
            /*enable_special_character_escaping_in_values=*/false,
            /*enable_special_character_escaping_in_keys=*/false));
    std::string value;
    bool is_null;
    ZETASQL_ASSERT_OK(evaluator->Extract(json, &value, &is_null));
    EXPECT_EQ(input_and_output.second, value);
    EXPECT_FALSE(is_null);
  }
}

TEST(JsonPathTest, JsonPathEndedWithDotStandardMode) {
  const std::string json = R"({"a": {"b": [ { "c" : "foo" } ] } })";
  const std::vector<std::pair<std::string, std::string>> inputs_and_outputs = {
      {"$.", R"({"a":{"b":[{"c":"foo"}]}})"},
      {"$.a.", R"({"b":[{"c":"foo"}]})"},
      {"$.a.b.", R"([{"c":"foo"}])"},
      {"$.a.b[0].", R"({"c":"foo"})"},
      {"$.a.b[0].c.", R"("foo")"}};
  for (const auto& input_and_output : inputs_and_outputs) {
    SCOPED_TRACE(absl::Substitute("JSON_QUERY('$0', '$1')", json,
                                  input_and_output.first));

    EXPECT_THAT(JsonPathEvaluator::Create(
                    input_and_output.first,
                    /*sql_standard_mode=*/true,
                    /*enable_special_character_escaping_in_values=*/false,
                    /*enable_special_character_escaping_in_keys=*/false),
                StatusIs(absl::StatusCode::kOutOfRange,
                         HasSubstr("Invalid token in JSONPath at:")));
  }
}

TEST(JsonTest, ConvertJSONPathToSqlStandardMode) {
  const std::string kInvalidJSONPath = "Invalid JSONPath input";

  std::vector<std::pair<std::string, std::string>> json_paths = {
      {"$", "$"},
      {"$['']", "$.\"\""},
      {"$.a", "$.a"},
      {"$[a]", "$.a"},
      {"$['a']", "$.a"},
      {"$[10]", "$.10"},
      {"$.a_b", "$.a_b"},
      {"$.a:b", "$.a:b"},
      {"$.a  \tb", "$.a  \tb"},
      {"$.a['b.c'].d[0].e", R"($.a."b.c".d.0.e)"},
      {"$['b.c'][d].e['f.g'][3]", R"($."b.c".d.e."f.g".3)"},
      // In non-standard mode, it is allowed for JSONPath to have a trailing "."
      {"$.", "$"},
      {"$.a.", "$.a"},
      {"$.a['b,c'].", R"($.a."b,c")"},
      // Special characters
      {R"($['a\''])", R"($."a'")"},
      {R"($['a,b'])", R"($."a,b")"},
      {R"($['a]'])", R"($."a]")"},
      {R"($['a[\'b\']'])", R"($."a['b']")"},
      {R"($['a"'])", R"($."a\"")"},
      {R"($['\\'])", R"($."\\")"},
      {R"($['a"\''].b['$#9"[\'s""]'])", R"($."a\"'".b."$#9\"['s\"\"]")"},
      // Invalid non-standard JSONPath.
      {R"($."a.b")", kInvalidJSONPath},
      // TODO: Single backslashes are not supported in JSONPath.
      {R"($['\'])", kInvalidJSONPath},
  };

  for (const auto& [non_standard_json_path, standard_json_path] : json_paths) {
    SCOPED_TRACE(absl::Substitute("ConvertJSONPathToSqlStandardMode($0)",
                                  non_standard_json_path));
    if (json_internal::IsValidJSONPath(non_standard_json_path,
                                       /*sql_standard_mode=*/false)
            .ok()) {
      EXPECT_THAT(ConvertJSONPathToSqlStandardMode(non_standard_json_path),
                  IsOkAndHolds(standard_json_path));
      ZETASQL_EXPECT_OK(json_internal::IsValidJSONPath(standard_json_path,
                                               /*sql_standard_mode=*/true));
    } else {
      EXPECT_THAT(ConvertJSONPathToSqlStandardMode(non_standard_json_path),
                  StatusIs(absl::StatusCode::kOutOfRange));
      EXPECT_EQ(standard_json_path, kInvalidJSONPath);
    }
  }
}

TEST(JsonTest, ConvertJSONPathTokenToSqlStandardMode) {
  const std::string kInvalidJSONPath = "Invalid JSONPath input";

  std::vector<std::pair<std::string, std::string>> json_path_tokens = {
      {"a", "a"},
      {"10", "10"},
      {"a_b", "a_b"},
      {"a:b", "a:b"},
      {"a  \tb", "a  \tb"},
      // Special characters
      {"a'", R"("a'")"},
      {"a.b", R"("a.b")"},
      {"a,b", R"("a,b")"},
      {"a]", R"("a]")"},
      {"a['b']", R"("a['b']")"},
      {R"(a")", R"("a\"")"},
      {R"(\\)", R"("\\")"},
  };

  for (const auto& [token, standard_json_path_token] : json_path_tokens) {
    SCOPED_TRACE(
        absl::Substitute("ConvertJSONPathTokenToSqlStandardMode($0)", token));
    EXPECT_EQ(ConvertJSONPathTokenToSqlStandardMode(token),
              standard_json_path_token);
    ZETASQL_EXPECT_OK(json_internal::IsValidJSONPath(
        absl::StrCat("$.", standard_json_path_token),
        /*sql_standard_mode=*/true));
  }
}

TEST(JsonTest, MergeJSONPathsIntoSqlStandardMode) {
  const std::string kInvalidJSONPath = "Invalid JSONPath input";

  std::vector<std::pair<std::vector<std::string>, std::string>>
      json_paths_test_cases = {
          {{"$"}, "$"},
          {{"$.a"}, "$.a"},
          {{"$['a']"}, "$.a"},
          {{"$['a']", "$.b"}, "$.a.b"},
          {{"$['a']", "$.b", R"($.c[1]."d.e")"}, R"($.a.b.c[1]."d.e")"},
          {{"$['a']", "$.b", "$.c[1]['d.e']"}, R"($.a.b.c.1."d.e")"},
          {{R"($['a\''])", R"($.b['c[\'d\']'])"}, R"($."a'".b."c['d']")"},
          // In non-standard mode, it is allowed for JSONPath to have a trailing
          // "."
          {{"$.", "$"}, "$"},
          {{"$.a.", "$[0]", "$['b,c']."}, R"($.a[0]."b,c")"},
          {{R"($."a\b")", "$.", "$", "$.a['b,c']."}, R"($."a\b".a."b,c")"},
          // Invalid inputs
          {{}, kInvalidJSONPath},
          {{"$", ".a"}, kInvalidJSONPath},
          {{"$", "$.a'"}, kInvalidJSONPath},
          // Standard mode cannot have a trailing "."
          {{R"($."a,b".)", "$.d"}, kInvalidJSONPath},
      };

  for (const auto& [json_paths, merged_json_path] : json_paths_test_cases) {
    SCOPED_TRACE(absl::Substitute("MergeJSONPathsIntoSqlStandardMode($0)",
                                  absl::StrJoin(json_paths, ", ")));
    if (merged_json_path == kInvalidJSONPath) {
      EXPECT_THAT(MergeJSONPathsIntoSqlStandardMode(json_paths),
                  StatusIs(absl::StatusCode::kOutOfRange));
    } else {
      EXPECT_THAT(MergeJSONPathsIntoSqlStandardMode(json_paths),
                  IsOkAndHolds(merged_json_path));
      ZETASQL_EXPECT_OK(json_internal::IsValidJSONPath(merged_json_path,
                                               /*sql_standard_mode=*/true));
    }
  }
}

}  // namespace

namespace json_internal {
namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

// Unit tests for the JSONPathExtractor and ValidJSONPathIterator.
static std::string Normalize(const std::string& in) {
  std::string output;
  std::string::const_iterator in_itr = in.begin();
  for (; in_itr != in.end(); ++in_itr) {
    if (!std::isspace(*in_itr)) {
      output.push_back(*in_itr);
    }
  }
  return output;
}

TEST(JsonPathExtractorTest, ScanTester) {
  std::unique_ptr<ValidJSONPathIterator> iptr;
  {
    std::string non_persisting_path = "$.a.b.c.d";
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        iptr, ValidJSONPathIterator::Create(non_persisting_path,
                                            /*sql_standard_mode=*/true));
    iptr->Scan();
  }
  ValidJSONPathIterator& itr = *iptr;
  ASSERT_TRUE(itr.End());
  itr.Rewind();
  ASSERT_TRUE(!itr.End());

  const std::vector<ValidJSONPathIterator::Token> gold = {"", "a", "b", "c",
                                                          "d"};
  std::vector<ValidJSONPathIterator::Token> tokens;
  for (; !itr.End(); ++itr) {
    tokens.push_back(*itr);
  }
  EXPECT_THAT(tokens, ElementsAreArray(gold));
}

TEST(JsonPathExtractorTest, SimpleValidPath) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValidJSONPathIterator> iptr,
      ValidJSONPathIterator::Create("$.a.b", /*sql_standard_mode=*/true));
  ValidJSONPathIterator& itr = *(iptr);

  ASSERT_TRUE(!itr.End());

  const std::vector<ValidJSONPathIterator::Token> gold = {"", "a", "b"};
  std::vector<ValidJSONPathIterator::Token> tokens;
  for (; !itr.End(); ++itr) {
    tokens.push_back(*itr);
  }
  EXPECT_THAT(tokens, ElementsAreArray(gold));
}

TEST(JsonPathExtractorTest, BackAndForthIteration) {
  const char* const input = "$.a.b";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValidJSONPathIterator> iptr,
      ValidJSONPathIterator::Create(input, /*sql_standard_mode=*/true));
  ValidJSONPathIterator& itr = *(iptr);

  ++itr;
  EXPECT_EQ(*itr, "a");
  --itr;
  EXPECT_EQ(*itr, "");
  --itr;
  EXPECT_TRUE(itr.End());
  ++itr;
  EXPECT_EQ(*itr, "");
  ++itr;
  EXPECT_EQ(*itr, "a");
  ++itr;
  EXPECT_EQ(*itr, "b");
}

TEST(JsonPathExtractorTest, EscapedPathTokens) {
  std::string esc_text("$.a['\\'\\'\\s '].g[1]");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValidJSONPathIterator> iptr,
      ValidJSONPathIterator::Create(esc_text, /*sql_standard_mode=*/false));
  ValidJSONPathIterator& itr = *(iptr);
  const std::vector<ValidJSONPathIterator::Token> gold = {"", "a", "''\\s ",
                                                          "g", "1"};

  std::vector<ValidJSONPathIterator::Token> tokens;
  for (; !itr.End(); ++itr) {
    tokens.push_back(*itr);
  }

  EXPECT_THAT(tokens, ElementsAreArray(gold));
}

TEST(JsonPathExtractorTest, EscapedPathTokensStandard) {
  std::string esc_text("$.a.\"\\\"\\\"\\s \".g[1]");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValidJSONPathIterator> iptr,
      ValidJSONPathIterator::Create(esc_text, /*sql_standard_mode=*/true));
  ValidJSONPathIterator& itr = *(iptr);
  const std::vector<ValidJSONPathIterator::Token> gold = {"", "a", "\"\"\\s ",
                                                          "g", "1"};

  std::vector<ValidJSONPathIterator::Token> tokens;
  for (; !itr.End(); ++itr) {
    tokens.push_back(*itr);
  }

  EXPECT_THAT(tokens, ElementsAreArray(gold));
}

TEST(JsonPathExtractorTest, EmptyPathTokens) {
  std::string esc_text("$.a[''].g[1]");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValidJSONPathIterator> iter,
      ValidJSONPathIterator::Create(esc_text, /*sql_standard_mode=*/false));
  const std::vector<ValidJSONPathIterator::Token> gold = {"", "a", "", "g",
                                                          "1"};

  std::vector<ValidJSONPathIterator::Token> tokens;
  for (; !iter->End(); ++(*iter)) {
    tokens.push_back(**iter);
  }

  EXPECT_THAT(tokens, ElementsAreArray(gold));
}

TEST(JsonPathExtractorTest, EmptyPathTokensStandard) {
  std::string esc_text("$.a.\"\".g[1]");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValidJSONPathIterator> iter,
      ValidJSONPathIterator::Create(esc_text, /*sql_standard_mode=*/true));
  const std::vector<ValidJSONPathIterator::Token> gold = {"", "a", "", "g",
                                                          "1"};

  std::vector<ValidJSONPathIterator::Token> tokens;
  for (; !iter->End(); ++(*iter)) {
    tokens.push_back(**iter);
  }

  EXPECT_THAT(tokens, ElementsAreArray(gold));
}

TEST(JsonPathExtractorTest, MixedPathTokens) {
  const char* const input_path =
      "$.a.b[423490].c['d::d'].e['abc\\\\\\'\\'     ']";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValidJSONPathIterator> iptr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  ValidJSONPathIterator& itr = *(iptr);
  const std::vector<ValidJSONPathIterator::Token> gold = {
      "", "a", "b", "423490", "c", "d::d", "e", "abc\\\\''     "};

  std::vector<ValidJSONPathIterator::Token> tokens;
  size_t n = gold.size();
  for (; !itr.End(); ++itr) {
    tokens.push_back(*itr);
  }

  EXPECT_THAT(tokens, ElementsAreArray(gold));

  tokens.clear();

  // Test along the decrement of the iterator.
  --itr;
  EXPECT_FALSE(itr.End());
  for (; !itr.End(); --itr) {
    tokens.push_back(*itr);
  }
  EXPECT_EQ(tokens.size(), n);

  for (size_t i = 0; i < tokens.size(); i++) {
    EXPECT_EQ(gold[(n - 1) - i], tokens[i]);
  }

  // Test along the increment of the iterator.
  tokens.clear();
  EXPECT_TRUE(itr.End());
  ++itr;
  EXPECT_FALSE(itr.End());
  for (; !itr.End(); ++itr) {
    tokens.push_back(*itr);
  }

  EXPECT_THAT(tokens, ElementsAreArray(gold));
}

TEST(RemoveBackSlashFollowedByChar, BasicTests) {
  std::string token = "'abc\\'\\'h'";
  std::string expected_token = "'abc''h'";
  RemoveBackSlashFollowedByChar(&token, '\'');
  EXPECT_EQ(token, expected_token);

  token = "";
  expected_token = "";
  RemoveBackSlashFollowedByChar(&token, '\'');
  EXPECT_EQ(token, expected_token);

  token = "\\'";
  expected_token = "'";
  RemoveBackSlashFollowedByChar(&token, '\'');
  EXPECT_EQ(token, expected_token);

  token = "\\'\\'\\\\'\\'\\'\\f ";
  expected_token = "''\\'''\\f ";
  RemoveBackSlashFollowedByChar(&token, '\'');
  EXPECT_EQ(token, expected_token);
}

TEST(IsValidJSONPathTest, BasicTests) {
  ZETASQL_EXPECT_OK(IsValidJSONPath("$", /*sql_standard_mode=*/true));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$.a", /*sql_standard_mode=*/true));

  // Escaped a
  EXPECT_THAT(IsValidJSONPath("$['a']", /*sql_standard_mode=*/true),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Invalid token in JSONPath at:")));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$['a']", /*sql_standard_mode=*/false));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$.\"a\"", /*sql_standard_mode=*/true));

  // Escaped efgh
  EXPECT_THAT(IsValidJSONPath("$.a.b.c['efgh'].e", /*sql_standard_mode=*/true),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Invalid token in JSONPath at:")));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$.a.b.c['efgh'].e", /*sql_standard_mode=*/false));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$.a.b.c.\"efgh\".e", /*sql_standard_mode=*/true));

  // Escaped b.c.d
  EXPECT_THAT(IsValidJSONPath("$.a['b.c.d'].e", /*sql_standard_mode=*/true),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Invalid token in JSONPath at:")));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$.a['b.c.d'].e", /*sql_standard_mode=*/false));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$.a.\"b.c.d\".e", /*sql_standard_mode=*/true));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$.\"b.c.d\".e", /*sql_standard_mode=*/true));

  EXPECT_THAT(
      IsValidJSONPath("$['a']['b']['c']['efgh']", /*sql_standard_mode=*/true),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Invalid token in JSONPath at:")));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$['a']['b']['c']['efgh']",
                            /*sql_standard_mode=*/false));

  ZETASQL_EXPECT_OK(IsValidJSONPath("$.a.b.c[0].e.f", /*sql_standard_mode=*/true));

  EXPECT_THAT(IsValidJSONPath("$['a']['b']['c'][0]['e']['f']",
                              /*sql_standard_mode=*/true),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Invalid token in JSONPath at:")));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$['a']['b']['c'][0]['e']['f']",
                            /*sql_standard_mode=*/false));

  EXPECT_THAT(IsValidJSONPath("$['a']['b\\'\\c\\\\d          ef']",
                              /*sql_standard_mode=*/true),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Invalid token in JSONPath at:")));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$['a']['b\\'\\c\\\\d          ef']",
                            /*sql_standard_mode=*/false));

  EXPECT_THAT(IsValidJSONPath("$['a;;;;;\\\\']['b\\'\\c\\\\d          ef']",
                              /*sql_standard_mode=*/true),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Invalid token in JSONPath at:")));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$['a;;;;;\\\\']['b\\'\\c\\\\d          ef']",
                            /*sql_standard_mode=*/false));

  EXPECT_THAT(IsValidJSONPath("$.a['\\'\\'\\'\\'\\'\\\\f '].g[1]",
                              /*sql_standard_mode=*/true),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Invalid token in JSONPath at:")));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$.a['\\'\\'\\'\\'\\'\\\\f '].g[1]",
                            /*sql_standard_mode=*/false));

  EXPECT_THAT(IsValidJSONPath("$.a.b.c[efgh]", /*sql_standard_mode=*/true),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Invalid token in JSONPath at:")));
  ZETASQL_EXPECT_OK(IsValidJSONPath("$.a.b.c[efgh]", /*sql_standard_mode=*/false));

  // unsupported @ in the path.
  EXPECT_THAT(
      IsValidJSONPath("$.a.;;;;;;;c[0];;;.@.f", /*sql_standard_mode=*/true),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Unsupported operator in JSONPath: @")));
  EXPECT_THAT(
      IsValidJSONPath("$.a.;;;;;;;.c[0].@.f", /*sql_standard_mode=*/true),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Unsupported operator in JSONPath: @")));
  EXPECT_THAT(IsValidJSONPath("$..", /*sql_standard_mode=*/true),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Unsupported operator in JSONPath: ..")));
  EXPECT_THAT(
      IsValidJSONPath("$.a.b.c[f.g.h.i].m.f", /*sql_standard_mode=*/false),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Invalid token in JSONPath at: [f.g.h.i]")));
  EXPECT_THAT(IsValidJSONPath("$.a.b.c['f.g.h.i'].[acdm].f",
                              /*sql_standard_mode=*/false),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Invalid token in JSONPath at: .[acdm]")));
}

TEST(IsValidJSONPathTest, StrictBasicTests) {
  ZETASQL_EXPECT_OK(IsValidJSONPathStrict("$"));
  ZETASQL_EXPECT_OK(IsValidJSONPathStrict("$.a"));
  ZETASQL_EXPECT_OK(IsValidJSONPathStrict("$[ 0 ]"));
  ZETASQL_EXPECT_OK(IsValidJSONPathStrict(R"($."a")"));
  ZETASQL_EXPECT_OK(IsValidJSONPathStrict(R"($.a.b.c."efgh".e)"));
  ZETASQL_EXPECT_OK(IsValidJSONPathStrict(R"($.a."b.c.d".e)"));
  ZETASQL_EXPECT_OK(IsValidJSONPathStrict(R"($."b.c.d".e)"));
  ZETASQL_EXPECT_OK(IsValidJSONPathStrict("$.a.b.c[0].e.f"));
  ZETASQL_EXPECT_OK(IsValidJSONPathStrict("$.\"a\tb\"[1]"));
}

TEST(IsValidJSONPathTest, InvalidPathStrictTests) {
  // Invalid cases.
  std::vector<std::string> invalid_paths = {"$[0-]",
                                            "$[0_]",
                                            "$[-1]"
                                            "[0]"
                                            "$[a]",
                                            "$['a']",
                                            "$.a.b.c['efgh'].e",
                                            "$.",
                                            ".a",
                                            "$[9223372036854775807990]"};

  for (const std::string& invalid_path : invalid_paths) {
    EXPECT_THAT(IsValidJSONPathStrict(invalid_path),
                StatusIs(absl::StatusCode::kOutOfRange));
    EXPECT_FALSE(StrictJSONPathIterator::Create(invalid_path).ok());
  }
}

TEST(JsonPathTest, StrictPathTests) {
  // Test all functions for iterating through a JSON path using
  // StrictJSONPathIterator.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<StrictJSONPathIterator> path_itr,
                         StrictJSONPathIterator::Create("$"));
    EXPECT_EQ((**path_itr).MaybeGetArrayIndex(), nullptr);
    EXPECT_EQ((**path_itr).MaybeGetObjectKey(), nullptr);
    EXPECT_FALSE(++(*path_itr));
    EXPECT_TRUE(path_itr->End());
    path_itr->Rewind();
    EXPECT_EQ((**path_itr).MaybeGetArrayIndex(), nullptr);
    EXPECT_EQ((**path_itr).MaybeGetObjectKey(), nullptr);
    EXPECT_TRUE(path_itr->NoSuffixToken());
    EXPECT_FALSE(path_itr->End());
  }
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<StrictJSONPathIterator> path_itr,
                         StrictJSONPathIterator::Create("$.1 "));
    // Skip first token.
    EXPECT_TRUE(++(*path_itr));
    EXPECT_EQ(*(**path_itr).MaybeGetObjectKey(), "1 ");
    EXPECT_TRUE(path_itr->NoSuffixToken());
    EXPECT_FALSE(++(*path_itr));
    EXPECT_TRUE(path_itr->End());
  }
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<StrictJSONPathIterator> path_itr,
                         StrictJSONPathIterator::Create("$[ 0 ]"));
    // Skip first token.
    EXPECT_TRUE(++(*path_itr));
    EXPECT_EQ(*(**path_itr).MaybeGetArrayIndex(), 0);
    EXPECT_TRUE(path_itr->NoSuffixToken());
    EXPECT_FALSE(++(*path_itr));
    EXPECT_TRUE(path_itr->End());
  }
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<StrictJSONPathIterator> path_itr,
                         StrictJSONPathIterator::Create(R"($."a")"));
    // Skip first token.
    EXPECT_TRUE(++(*path_itr));
    EXPECT_EQ(*(**path_itr).MaybeGetObjectKey(), "a");
    EXPECT_TRUE(path_itr->NoSuffixToken());
    EXPECT_FALSE(++(*path_itr));
    EXPECT_TRUE(path_itr->End());
  }
  {
    // Path escaping.
    ZETASQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<StrictJSONPathIterator> path_itr,
                         StrictJSONPathIterator::Create(R"($."a\"b")"));
    EXPECT_TRUE(++(*path_itr));
    EXPECT_EQ(*(**path_itr).MaybeGetObjectKey(), R"(a\"b)");
    EXPECT_TRUE(path_itr->NoSuffixToken());
    EXPECT_FALSE(++(*path_itr));
    EXPECT_TRUE(path_itr->End());
  }
  {
    // Test iterating and rewind.
    ZETASQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<StrictJSONPathIterator> path_itr,
                         StrictJSONPathIterator::Create("$.\"b.c.d\"[1].e"));
    // Skip first token.
    EXPECT_TRUE(++(*path_itr));
    EXPECT_EQ(*(**path_itr).MaybeGetObjectKey(), "b.c.d");
    EXPECT_TRUE(++(*path_itr));
    EXPECT_EQ(*(**path_itr).MaybeGetArrayIndex(), 1);
    EXPECT_TRUE(++(*path_itr));
    EXPECT_EQ(*(**path_itr).MaybeGetObjectKey(), "e");
    EXPECT_TRUE(path_itr->NoSuffixToken());
    EXPECT_FALSE(++(*path_itr));
    EXPECT_TRUE(path_itr->End());
    // No-op as we've already reached the end of the path.
    EXPECT_FALSE(++(*path_itr));
    // Rewind a single token.
    EXPECT_TRUE(--(*path_itr));
    EXPECT_FALSE(path_itr->End());
    EXPECT_EQ(*(**path_itr).MaybeGetObjectKey(), "e");
    // Rewind to the beginning and validate both tokens and type tokens.
    path_itr->Rewind();
    ++(*path_itr);
    EXPECT_EQ(*(**path_itr).MaybeGetObjectKey(), "b.c.d");
    ++(*path_itr);
    EXPECT_EQ(*(**path_itr).MaybeGetArrayIndex(), 1);
  }
}

TEST(JSONPathExtractorTest, BasicParsing) {
  std::string input =
      "{ \"l00\" : { \"l01\" : \"a10\", \"l11\" : \"test\" }, \"l10\" : { "
      "\"l01\" : null }, \"l20\" : \"a5\" }";
  absl::string_view input_str(input);
  absl::string_view input_path("$");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractor parser(input_str, path_itr.get());

  std::string result;
  bool is_null;

  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_EQ(result, Normalize(input));
  EXPECT_FALSE(is_null);
}

TEST(JSONPathExtractorTest, MatchingMultipleSuffixes) {
  std::string input =
      "{ \"a\" : { \"b\" : \"a10\", \"l11\" : \"test\" }, \"a\" : { "
      "\"c\" : null }, \"a\" : \"a5\", \"a\" : \"a6\" }";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a.c");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractor parser(input_str, path_itr.get());

  std::string result;
  bool is_null;
  std::string gold = "null";

  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_TRUE(parser.StoppedOnFirstMatch());
  EXPECT_EQ(result, gold);
  EXPECT_TRUE(is_null);
}

TEST(JSONPathExtractorTest, PartiallyMatchingSuffixes) {
  std::string input =
      "{ \"a\" : { \"b\" : \"a10\", \"l11\" : \"test\" }, \"a\" : { "
      "\"c\" : null }, \"a\" : \"a5\", \"a\" : \"a6\" }";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a.c.d");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractor parser(input_str, path_itr.get());

  std::string result;
  bool is_null;
  std::string gold = "";

  // Parsing of JSON was successful however no match.
  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_FALSE(parser.StoppedOnFirstMatch());
  EXPECT_TRUE(is_null);
  EXPECT_EQ(result, gold);
}

TEST(JSONPathExtractorTest, MatchedEmptyStringValue) {
  std::string input =
      "{ \"a\" : { \"b\" : \"a10\", \"l11\" : \"test\" }, \"a\" : { "
      "\"c\" : {\"d\" : \"\" } }, \"a\" : \"a5\", \"a\" : \"a6\" }";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a.c.d");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractor parser(input_str, path_itr.get());

  // Parsing of JSON was successful and the value
  // itself is "" so we can use StoppedOnFirstMatch() to
  // distinguish between a matched value which is empty and
  // the case where there is no match. We can also rely on
  // the return value of \"\" however this is more elegant.
  std::string result;
  bool is_null;
  std::string gold = "\"\"";

  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_TRUE(parser.StoppedOnFirstMatch());
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);
}

TEST(JSONPathExtractScalar, ValidateScalarResult) {
  std::string input =
      "{ \"a\" : { \"b\" : \"a10\", \"l11\" : \"tes\\\"t\" }, \"a\" : { "
      "\"c\" : {\"d\" : 1.9834 } , \"d\" : [ {\"a\" : \"a5\"}, {\"a\" : "
      "\"a6\"}] , \"quoted_null\" : \"null\" } , \"e\" : null , \"f\" : null}";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a.c.d");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));

  JSONPathExtractScalar parser(input_str, path_itr.get());
  std::string scalar_result;
  bool is_null;

  EXPECT_TRUE(parser.Extract(&scalar_result, &is_null));
  EXPECT_TRUE(parser.StoppedOnFirstMatch());
  std::string gold = "1.9834";
  EXPECT_FALSE(is_null);
  EXPECT_EQ(scalar_result, gold);

  input_path = "$.a.l11";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr1,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractScalar parser1(input_str, path_itr1.get());

  EXPECT_TRUE(parser1.Extract(&scalar_result, &is_null));
  gold = "tes\"t";
  EXPECT_FALSE(is_null);
  EXPECT_EQ(scalar_result, gold);

  input_path = "$.a.c";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr2,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractScalar parser2(input_str, path_itr2.get());

  EXPECT_TRUE(parser2.Extract(&scalar_result, &is_null));
  EXPECT_TRUE(is_null);

  input_path = "$.a.d";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr3,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractScalar parser3(input_str, path_itr3.get());

  EXPECT_TRUE(parser3.Extract(&scalar_result, &is_null));
  EXPECT_TRUE(is_null);

  input_path = "$.e";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr4,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractScalar parser4(input_str, path_itr4.get());

  EXPECT_TRUE(parser4.Extract(&scalar_result, &is_null));
  EXPECT_TRUE(is_null);

  input_path = "$.a.c.d.e";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr5,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractScalar parser5(input_str, path_itr5.get());

  EXPECT_TRUE(parser5.Extract(&scalar_result, &is_null));
  EXPECT_FALSE(parser5.StoppedOnFirstMatch());
  EXPECT_TRUE(is_null);

  input_path = "$.a.quoted_null";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr6,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractScalar parser6(input_str, path_itr6.get());

  EXPECT_TRUE(parser6.Extract(&scalar_result, &is_null));
  EXPECT_FALSE(is_null);
  gold = "null";
  EXPECT_EQ(scalar_result, gold);

  input_path = "$.a.b.c";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr7,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractScalar parser7(input_str, path_itr7.get());

  EXPECT_TRUE(parser7.Extract(&scalar_result, &is_null));
  EXPECT_TRUE(is_null);
  EXPECT_FALSE(parser7.StoppedOnFirstMatch());
}

TEST(JSONPathExtractorTest, ReturnJSONObject) {
  std::string input =
      "{ \"e\" : { \"b\" : \"a10\", \"l11\" : \"test\" }, \"a\" : { "
      "\"c\" : null, \"f\" : { \"g\" : \"h\", \"g\" : [ \"i\", { \"x\" : "
      "\"j\"} ] } }, "
      "\"a\" : \"a5\", \"a\" : \"a6\" }";

  absl::string_view input_str(input);
  absl::string_view input_path("$.a.f");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractor parser(input_str, path_itr.get());

  std::string result;
  bool is_null;
  std::string gold = "{ \"g\" : \"h\", \"g\" : [ \"i\", { \"x\" : \"j\" } ] }";

  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_TRUE(parser.StoppedOnFirstMatch());
  EXPECT_EQ(result, Normalize(gold));
}

TEST(JSONPathExtractorTest, StopParserOnFirstMatch) {
  std::string input =
      "{ \"a\" : { \"b\" : { \"c\" : { \"d\" : \"l1\" } } } ,"
      " \"a\" : { \"b\" :  { \"c\" : { \"e\" : \"l2\" } } } ,"
      " \"a\" : { \"b\" : { \"c\" : { \"e\" : \"l3\"} }}}";

  std::string result;
  bool is_null;

  {
    absl::string_view input_str(input);
    absl::string_view input_path("$.a.b.c");

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<ValidJSONPathIterator> path_itr,
        ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
    JSONPathExtractor parser(input_str, path_itr.get());

    std::string gold = "{ \"d\" : \"l1\" }";

    EXPECT_TRUE(parser.Extract(&result, &is_null));
    EXPECT_FALSE(is_null);
    EXPECT_TRUE(parser.StoppedOnFirstMatch());
    EXPECT_EQ(result, Normalize(gold));
  }

  {
    absl::string_view input_str(input);
    absl::string_view input_path("$.a.b.c");

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<ValidJSONPathIterator> path_itr,
        ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
    JSONPathExtractor parser(input_str, path_itr.get());

    std::string gold = "{ \"d\" : \"l1\" }";
    EXPECT_TRUE(parser.Extract(&result, &is_null));
    EXPECT_FALSE(is_null);
    EXPECT_TRUE(parser.StoppedOnFirstMatch());
    EXPECT_EQ(result, Normalize(gold));
  }
}

TEST(JSONPathExtractorTest, BasicArrayAccess) {
  std::string input =
      "{ \"e\" : { \"b\" : \"a10\", \"l11\" : \"test\" }, \"a\" : { "
      "\"c\" : null, \"f\" : { \"g\" : \"h\", \"g\" : [ \"i\", \"j\" ] } }, "
      "\"a\" : \"a5\", \"a\" : \"a6\" }";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a.f.g[1]");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractor parser(input_str, path_itr.get());

  std::string result;
  bool is_null;
  std::string gold = "\"j\"";

  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);
}

TEST(JSONPathExtractorTest, ArrayAccessObjectMultipleSuffixes) {
  std::string input =
      "{ \"e\" : { \"b\" : \"a10\", \"l11\" : \"test\" },"
      " \"a\" : { \"f\" : null, "
      "\"f\" : { \"g\" : \"h\", "
      "\"g\" : [ \"i\", \"j\" ] } }, "
      "\"a\" : \"a5\", \"a\" : \"a6\" }";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a.f.g[1]");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractor parser(input_str, path_itr.get());

  std::string gold = "\"j\"";
  std::string result;
  bool is_null;

  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);
}

TEST(JSONPathExtractorTest, EscapedAccessTestStandard) {
  // There are two escapings happening as follows:
  // a. C++ Compiler
  // b. JSON Parser
  //
  // So '4k' (k > = 1) backslashes translate to 'k' backslashes at runtime
  // "\\\\" = "\" at runtime. So "\\\\\\\\s" === "\\s"
  std::string input =
      "{ \"e\" : { \"b\" : \"a10\", \"l11\" : \"test\" },"
      " \"a\" : { \"b\" : null, "
      "\"''\\\\\\\\s \" : { \"g\" : \"h\", "
      "\"g\" : [ \"i\", \"j\" ] } }, "
      "\"a\" : \"a5\", \"a\" : \"a6\" }";
  absl::string_view input_str(input);
  std::string input_path("$.a['\\'\\'\\\\s '].g[1]");
  absl::string_view esc_input_path(input_path);

  ZETASQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<ValidJSONPathIterator> path_itr,
                       ValidJSONPathIterator::Create(
                           esc_input_path, /*sql_standard_mode=*/false));
  JSONPathExtractor parser(input_str, path_itr.get());

  std::string result;
  bool is_null;
  std::string gold = "\"j\"";

  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);
}

TEST(JSONPathExtractorTest, EscapedAccessTest) {
  std::string input = R"({"a\"b": 1 })";
  absl::string_view input_str(input);
  std::string input_path(R"($."a\"b")");
  absl::string_view esc_input_path(input_path);

  ZETASQL_LOG(INFO) << input;

  ZETASQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<ValidJSONPathIterator> path_itr,
                       ValidJSONPathIterator::Create(
                           esc_input_path, /*sql_standard_mode=*/true));
  JSONPathExtractor parser(input_str, path_itr.get());

  std::string result;
  bool is_null;
  std::string gold = "1";

  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);
}

TEST(JSONPathExtractorTest, NestedArrayAccess) {
  std::string input =
      "[0 , [ [],  [ [ 1, 4, 8, [2, 1, 0, {\"a\" : \"3\"}, 4 ], 11, 13] ] , "
      "[], \"a\" ], 2, [] ]";
  absl::string_view input_str(input);
  absl::string_view input_path("$[1][1][0][3][3]");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractor parser(input_str, path_itr.get());

  std::string result;
  bool is_null;
  std::string gold = "{ \"a\" : \"3\" }";
  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_EQ(result, Normalize(gold));
  EXPECT_FALSE(is_null);
}

TEST(JSONPathExtractorTest, NegativeNestedArrayAccess) {
  std::string input =
      "[0 , [ [],  [ [ 1, 4, 8, [2, 1, 0, {\"a\" : \"3\"}, 4 ], 11, 13] ] , "
      "[], \"a\" ], 2, [] ]";
  absl::string_view input_str(input);
  absl::string_view input_path("$[1][1]['-0'][3][3]");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathExtractor parser(input_str, path_itr.get());

  std::string result;
  bool is_null;

  std::string gold = "{ \"a\" : \"3\" }";
  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, Normalize(gold));

  absl::string_view input_path1("$[1][1]['-5'][3][3]");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr1,
      ValidJSONPathIterator::Create(input_path1, /*sql_standard_mode=*/false));
  JSONPathExtractor parser1(input_str, path_itr1.get());

  EXPECT_TRUE(parser1.Extract(&result, &is_null));
  EXPECT_TRUE(is_null);
  EXPECT_FALSE(parser1.StoppedOnFirstMatch());
  EXPECT_EQ(result, "");
}

TEST(JSONPathExtractorTest, MixedNestedArrayAccess) {
  std::string input =
      "{ \"a\" : [0 , [ [],  { \"b\" : [ 7, [ 1, 4, 8, [2, 1, 0, {\"a\" : { "
      "\"b\" : \"3\"}, \"c\" : \"d\" }, 4 ], 11, 13] ] }, "
      "[], \"a\" ], 2, [] ] }";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a[1][1].b[1][3][3].c");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/true));
  JSONPathExtractor parser(input_str, path_itr.get());
  std::string result;
  bool is_null;
  std::string gold = "\"d\"";

  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);
}

TEST(JSONPathExtractorTest, QuotedArrayIndex) {
  std::string input =
      "[0 , [ [],  [ [ 1, 4, 8, [2, 1, 0, {\"a\" : \"3\"}, 4 ], 11, 13] ] , "
      "[], \"a\" ], 2, [] ]";
  absl::string_view input_str(input);
  absl::string_view input_path("$['1'][1][0]['3']['3']");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathExtractor parser(input_str, path_itr.get());

  std::string result;
  bool is_null;
  std::string gold = "{ \"a\" : \"3\" }";

  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_EQ(result, Normalize(gold));
  EXPECT_FALSE(is_null);
}

TEST(JSONPathExtractorTest, TestReuseOfPathIterator) {
  std::string input =
      "[0 , [ [],  [ [ 1, 4, 8, [2, 1, 0, {\"a\" : \"3\"}, 4 ], 11, 13] ] , "
      "[], \"a\" ], 2, [] ]";
  std::string path = "$[1][1][0][3][3]";
  absl::string_view input_str(input);
  std::string gold = "{ \"a\" : \"3\" }";
  std::string result;
  bool is_null;

  // Default with local path_iterator object.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(path, /*sql_standard_mode=*/true));
  JSONPathExtractor parser(input_str, path_itr.get());

  EXPECT_TRUE(parser.Extract(&result, &is_null));
  EXPECT_EQ(result, Normalize(gold));
  EXPECT_FALSE(is_null);

  for (size_t i = 0; i < 10; i++) {
    // Reusable token iterator.
    absl::string_view input_str(input);
    JSONPathExtractor parser(input_str, path_itr.get());

    EXPECT_TRUE(parser.Extract(&result, &is_null));
    EXPECT_EQ(result, Normalize(gold));
    EXPECT_FALSE(is_null);
  }
}

TEST(JSONPathArrayExtractorTest, BasicParsing) {
  std::string input =
      "[ {\"l00\" : { \"l01\" : \"a10\", \"l11\" : \"test\" }}, {\"l10\" : { "
      "\"l01\" : null }}, {\"l20\" : \"a5\"} ]";
  absl::string_view input_str(input);
  absl::string_view input_path("$");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::string> result;
  std::vector<std::string> gold(
      {Normalize("{\"l00\": { \"l01\" : \"a10\", \"l11\" : \"test\" }}"),
       Normalize("{\"l10\" : { \"l01\" : null }}"),
       Normalize("{\"l20\" : \"a5\"}")});
  bool is_null;

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_EQ(result, gold);
  EXPECT_FALSE(is_null);
}

TEST(JSONPathArrayExtractorTest, MatchingMultipleSuffixes) {
  std::string input =
      R"({"a":{"b":"a10","l11":"test"}, "a":{"c":null}, "a":"a5", "a":"a6"})";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a.c");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::string> result;
  bool is_null;
  // Matching the leaf while it is not an array
  std::vector<std::string> gold;
  std::vector<std::optional<std::string>> scalar_gold;

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_TRUE(parser.StoppedOnFirstMatch());
  EXPECT_EQ(result, gold);
  EXPECT_TRUE(is_null);

  std::vector<std::optional<std::string>> scalar_result;
  JSONPathStringArrayExtractor scalar_parser(input_str, path_itr.get());
  EXPECT_TRUE(scalar_parser.ExtractStringArray(&scalar_result, &is_null));
  EXPECT_TRUE(scalar_parser.StoppedOnFirstMatch());
  EXPECT_EQ(scalar_result, scalar_gold);
  EXPECT_TRUE(is_null);
}

TEST(JSONPathArrayExtractorTest, MatchedEmptyArray) {
  std::string input =
      R"({"a":{"b":"a10", "l11":"test"}, "a":{"c":{"d":[]}}, "a":"a5",
      "a":"a6"})";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a.c.d");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::string> result;
  bool is_null;
  std::vector<std::string> gold;
  std::vector<std::optional<std::string>> scalar_gold;

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_TRUE(parser.StoppedOnFirstMatch());
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);

  std::vector<std::optional<std::string>> scalar_result;
  JSONPathStringArrayExtractor scalar_parser(input_str, path_itr.get());
  EXPECT_TRUE(scalar_parser.ExtractStringArray(&scalar_result, &is_null));
  EXPECT_TRUE(scalar_parser.StoppedOnFirstMatch());
  EXPECT_FALSE(is_null);
  EXPECT_EQ(scalar_result, scalar_gold);
}

TEST(JSONPathArrayExtractorTest, PartiallyMatchingSuffixes) {
  std::string input =
      R"({"a":{"b":"a10","l11":"test"}, "a":{"c":null}, "a":"a5", "a":"a6"})";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a.c.d");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::string> result;
  bool is_null;
  std::vector<std::string> gold;
  std::vector<std::optional<std::string>> scalar_gold;

  // Parsing of JSON was successful however no match.
  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_FALSE(parser.StoppedOnFirstMatch());
  EXPECT_TRUE(is_null);
  EXPECT_EQ(result, gold);

  std::vector<std::optional<std::string>> scalar_result;
  JSONPathStringArrayExtractor scalar_parser(input_str, path_itr.get());
  EXPECT_TRUE(scalar_parser.ExtractStringArray(&scalar_result, &is_null));
  EXPECT_FALSE(scalar_parser.StoppedOnFirstMatch());
  EXPECT_TRUE(is_null);
  EXPECT_EQ(scalar_result, scalar_gold);
}

TEST(JSONPathArrayExtractorTest, ReturnJSONObjectArray) {
  std::string input =
      R"({"e":{"b":"a10", "l11":"test"}, "a":{"c":null, "f":[{"g":"h"},
      {"g":["i", {"x":"j"}]}]}, "a":"a5", "a":"a6"})";

  absl::string_view input_str(input);
  absl::string_view input_path("$.a.f");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::string> result;
  bool is_null;
  std::vector<std::string> gold(
      {Normalize("{ \"g\" : \"h\"}"),
       Normalize("{\"g\" : [ \"i\", { \"x\" : \"j\" } ] }")});

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_TRUE(parser.StoppedOnFirstMatch());
  EXPECT_EQ(result, gold);
}

TEST(JSONPathArrayExtractorTest, StopParserOnFirstMatch) {
  std::string input =
      R"({"a":{"b":{"c":{"d":["l1"]}}}, "a":{"b":{"c":{"e":"l2"}}},
      "a":{"b":{"c":{"d":"l3"}}}})";

  absl::string_view input_str(input);
  absl::string_view input_path("$.a.b.c.d");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::string> result;
  bool is_null;
  std::vector<std::string> gold({"\"l1\""});

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_TRUE(parser.StoppedOnFirstMatch());
  EXPECT_EQ(result, gold);

  std::vector<std::optional<std::string>> scalar_result;
  std::vector<std::optional<std::string>> scalar_gold = {"l1"};
  JSONPathStringArrayExtractor scalar_parser(input_str, path_itr.get());
  EXPECT_TRUE(scalar_parser.ExtractStringArray(&scalar_result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_TRUE(scalar_parser.StoppedOnFirstMatch());
  EXPECT_EQ(scalar_result, scalar_gold);
}

TEST(JSONPathArrayExtractorTest, BasicArrayAccess) {
  std::string input =
      R"({"e":{"b":"a10", "l11":"test"},
      "a":{"c":null, "f":{"g":"h", "g":[["i"], ["j", "k"]]}},
      "a":"a5", "a":"a6"})";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a.f.g[1]");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::string> result;
  bool is_null;
  std::vector<std::string> gold({"\"j\"", "\"k\""});

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);

  std::vector<std::optional<std::string>> scalar_result;
  std::vector<std::optional<std::string>> scalar_gold = {"j", "k"};
  JSONPathStringArrayExtractor scalar_parser(input_str, path_itr.get());
  EXPECT_TRUE(scalar_parser.ExtractStringArray(&scalar_result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(scalar_result, scalar_gold);
}

TEST(JSONPathArrayExtractorTest, AccessObjectInArrayMultipleSuffixes) {
  std::string input =
      R"({"e":{"b" : "a10", "l11":"test"},
      "a":{"f":null, "f":{"g":"h", "g":[["i"], ["j", "k"]]}},
      "a":"a5", "a":"a6"})";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a.f.g[1]");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::string> result;
  bool is_null;
  std::vector<std::string> gold({"\"j\"", "\"k\""});

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);

  std::vector<std::optional<std::string>> scalar_result;
  std::vector<std::optional<std::string>> scalar_gold = {"j", "k"};
  JSONPathStringArrayExtractor scalar_parser(input_str, path_itr.get());
  EXPECT_TRUE(scalar_parser.ExtractStringArray(&scalar_result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(scalar_result, scalar_gold);
}

TEST(JSONPathArrayExtractorTest, EscapedAccessTestNonSqlStandard) {
  // There are two escapings happening as follows:
  // a. C++ Compiler
  // b. JSON Parser
  //
  // So '4k' (k > = 1) backslashes translate to 'k' backslashes at runtime
  // "\\\\" = "\" at runtime. So "\\\\\\\\s" === "\\s"
  std::string input =
      R"({"e":{"b":"a10", "l11":"test"},
      "a":{"b":null, "''\\\\s ":{"g":"h", "g":["i", ["j", "k"]]}},
      "a":"a5", "a":"a6"})";
  absl::string_view input_str(input);
  std::string input_path("$.a['\\'\\'\\\\s '].g[ 1]");
  absl::string_view esc_input_path(input_path);

  ZETASQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<ValidJSONPathIterator> path_itr,
                       ValidJSONPathIterator::Create(
                           esc_input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::string> result;
  bool is_null;
  std::vector<std::string> gold({"\"j\"", "\"k\""});

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);

  std::vector<std::optional<std::string>> scalar_result;
  std::vector<std::optional<std::string>> scalar_gold = {"j", "k"};
  JSONPathStringArrayExtractor scalar_parser(input_str, path_itr.get());
  EXPECT_TRUE(scalar_parser.ExtractStringArray(&scalar_result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(scalar_result, scalar_gold);
}

TEST(JSONPathArrayExtractorTest,
     EscapedAccessTestNonSqlStandardInvalidJsonPath) {
  std::string input =
      R"({"e":{"b":"a10", "l11":"test"},
      "a":{"b":null, "''\\\\s ":{"g":"h", "g":["i", ["j", "k"]]}},
      "a":"a5", "a":"a6"})";
  std::string input_path("$.a.\"\'\'\\\\s \".g[ 1]");
  absl::string_view esc_input_path(input_path);

  absl::Status status =
      ValidJSONPathIterator::Create(esc_input_path, /*sql_standard_mode=*/false)
          .status();
  EXPECT_THAT(
      status,
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr(R"(Invalid token in JSONPath at: ."''\\s ".g[ 1])")));
}

TEST(JSONPathArrayExtractorTest, NestedArrayAccess) {
  std::string input =
      R"([0 ,[[], [[1, 4, 8, [2, 1, 0, ["3", "4"], 4], 11, 13]], [], "a"], 2,
      []])";
  absl::string_view input_str(input);
  absl::string_view input_path("$[1][1][0][3][3]");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::string> result;
  bool is_null;
  std::vector<std::string> gold({"\"3\"", "\"4\""});

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_EQ(result, gold);
  EXPECT_FALSE(is_null);

  std::vector<std::optional<std::string>> scalar_result;
  std::vector<std::optional<std::string>> scalar_gold({"3", "4"});
  JSONPathStringArrayExtractor scalar_parser(input_str, path_itr.get());
  EXPECT_TRUE(scalar_parser.ExtractStringArray(&scalar_result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(scalar_result, scalar_gold);
}

TEST(JSONPathArrayExtractorTest, NegativeNestedArrayAccess) {
  std::string input =
      R"([0 ,[[], [[1, 4, 8, [2, 1, 0, ["3", "4"], 4], 11, 13]], [], "a"], 2,
      []])";
  absl::string_view input_str(input);
  absl::string_view input_path("$[1][1]['-0'][3][3]");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::string> result;
  bool is_null;
  std::vector<std::string> gold({"\"3\"", "\"4\""});

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);

  std::vector<std::optional<std::string>> scalar_result;
  std::vector<std::optional<std::string>> scalar_gold = {"3", "4"};
  JSONPathStringArrayExtractor scalar_parser(input_str, path_itr.get());
  EXPECT_TRUE(scalar_parser.ExtractStringArray(&scalar_result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(scalar_result, scalar_gold);

  absl::string_view input_path1("$[1][1]['-5'][3][3]");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr1,
      ValidJSONPathIterator::Create(input_path1, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser1(input_str, path_itr1.get());

  std::vector<std::string> gold1({});

  EXPECT_TRUE(parser1.ExtractArray(&result, &is_null));
  EXPECT_TRUE(is_null);
  EXPECT_FALSE(parser1.StoppedOnFirstMatch());
  EXPECT_EQ(result, gold1);

  scalar_result.clear();
  std::vector<std::optional<std::string>> scalar_gold1;
  JSONPathStringArrayExtractor scalar_parser1(input_str, path_itr1.get());
  EXPECT_TRUE(scalar_parser1.ExtractStringArray(&scalar_result, &is_null));
  EXPECT_TRUE(is_null);
  EXPECT_FALSE(scalar_parser1.StoppedOnFirstMatch());
  EXPECT_EQ(scalar_result, scalar_gold1);
}

TEST(JSONPathArrayExtractorTest, MixedNestedArrayAccess) {
  std::string input =
      R"({"a":[0, [[], {"b":[7, [1, 4, 8, [2, 1, 0, {"a":{"b":"3"},
      "c":[1, 2, 3]}, 4], 11, 13]]}, [], "a"], 2,[]]})";
  absl::string_view input_str(input);
  absl::string_view input_path("$.a[1][1].b[1][3][3].c");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());
  std::vector<std::string> result;
  bool is_null;
  std::vector<std::string> gold({"1", "2", "3"});

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);

  std::vector<std::optional<std::string>> scalar_result;
  std::vector<std::optional<std::string>> scalar_gold = {"1", "2", "3"};
  JSONPathStringArrayExtractor scalar_parser(input_str, path_itr.get());
  EXPECT_TRUE(scalar_parser.ExtractStringArray(&scalar_result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_EQ(scalar_result, scalar_gold);
}

TEST(JSONPathArrayExtractorTest, QuotedArrayIndex) {
  std::string input =
      R"([0, [[], [[1, 4, 8, [2, 1, 0, [{"a":"3"}, {"a":"4"}], 4], 11, 13]], [],
      "a"], 2, []])";
  absl::string_view input_str(input);
  absl::string_view input_path("$['1'][1][0]['3']['3']");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::string> result;
  bool is_null;
  std::vector<std::string> gold(
      {Normalize(R"({"a":"3"})"), Normalize(R"({"a":"4"})")});

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_EQ(result, gold);
  EXPECT_FALSE(is_null);
}

TEST(JSONPathArrayStringExtractorTest, BasicParsing) {
  std::string input = R"(["a", 1, "2"])";
  absl::string_view input_str(input);
  absl::string_view input_path("$");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathStringArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::optional<std::string>> result;
  std::vector<std::optional<std::string>> gold = {"a", "1", "2"};
  bool is_null;

  EXPECT_TRUE(parser.ExtractStringArray(&result, &is_null));
  EXPECT_EQ(result, gold);
  EXPECT_FALSE(is_null);
}

TEST(JSONPathArrayExtractorTest, ValidateScalarResult) {
  std::string input =
      R"({"a":[{"a1":"a11"}, "a2" ],
      "b":["b1", ["b21", "b22"]],
      "c":[[],"c2"],
      "d":["d1", "tes\"t", 1.9834, null, 123]})";

  absl::string_view input_str(input);
  absl::string_view input_path("$.a");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathStringArrayExtractor parser(input_str, path_itr.get());

  std::vector<std::optional<std::string>> result;
  bool is_null;
  EXPECT_TRUE(parser.ExtractStringArray(&result, &is_null));
  EXPECT_TRUE(is_null);

  input_path = "$.b";
  result.clear();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr1,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathStringArrayExtractor parser1(input_str, path_itr1.get());
  EXPECT_TRUE(parser1.ExtractStringArray(&result, &is_null));
  EXPECT_TRUE(is_null);

  input_path = "$.c";
  result.clear();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr2,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathStringArrayExtractor parser2(input_str, path_itr2.get());
  EXPECT_TRUE(parser2.ExtractStringArray(&result, &is_null));
  EXPECT_TRUE(is_null);

  input_path = "$.d";
  result.clear();
  std::vector<std::optional<std::string>> gold = {"d1", "tes\"t", "1.9834",
                                                  std::nullopt, "123"};
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const std::unique_ptr<ValidJSONPathIterator> path_itr3,
      ValidJSONPathIterator::Create(input_path, /*sql_standard_mode=*/false));
  JSONPathStringArrayExtractor parser3(input_str, path_itr3.get());
  EXPECT_TRUE(parser3.ExtractStringArray(&result, &is_null));
  EXPECT_FALSE(is_null);
  EXPECT_TRUE(parser.StoppedOnFirstMatch());
  EXPECT_EQ(result, gold);
}

TEST(ValidJSONPathIterator, BasicTest) {
  std::string path = "$[1][1][0][3][3]";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValidJSONPathIterator> iptr,
      ValidJSONPathIterator::Create(path, /*sql_standard_mode=*/true));
  ValidJSONPathIterator& itr = *(iptr);
  itr.Rewind();
  EXPECT_EQ(*itr, "");
  ++itr;
  EXPECT_EQ(*itr, "1");
  ++itr;
  EXPECT_EQ(*itr, "1");
  ++itr;
  EXPECT_EQ(*itr, "0");
  ++itr;
  EXPECT_EQ(*itr, "3");
  ++itr;
  EXPECT_EQ(*itr, "3");
  ++itr;
  EXPECT_TRUE(itr.End());

  // reverse.
  --itr;
  EXPECT_EQ(*itr, "3");
  --itr;
  EXPECT_EQ(*itr, "3");
  --itr;
  EXPECT_EQ(*itr, "0");
  --itr;
  EXPECT_EQ(*itr, "1");
  --itr;
  EXPECT_EQ(*itr, "1");
  --itr;
  EXPECT_EQ(*itr, "");
  --itr;
  EXPECT_TRUE(itr.End());

  ++itr;
  EXPECT_EQ(*itr, "");
  ++itr;
  EXPECT_EQ(*itr, "1");
}

TEST(ValidJSONPathIterator, DegenerateCases) {
  std::string path = "$";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValidJSONPathIterator> iptr,
      ValidJSONPathIterator::Create(path, /*sql_standard_mode=*/true));
  ValidJSONPathIterator& itr = *(iptr);

  EXPECT_FALSE(itr.End());
  EXPECT_EQ(*itr, "");

  path = "$";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValidJSONPathIterator> iptr1,
      ValidJSONPathIterator::Create(path, /*sql_standard_mode=*/true));
  ValidJSONPathIterator& itr1 = *(iptr1);

  EXPECT_FALSE(itr1.End());
  EXPECT_EQ(*itr1, "");
}

TEST(ValidJSONPathIterator, InvalidEmptyJSONPathCreation) {
  std::string path = "$.a.*.b.c";
  absl::Status status =
      ValidJSONPathIterator::Create(path, /*sql_standard_mode=*/true).status();
  EXPECT_THAT(status,
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Unsupported operator in JSONPath: *")));

  path = "$.@";
  status =
      ValidJSONPathIterator::Create(path, /*sql_standard_mode=*/true).status();
  EXPECT_THAT(status,
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Unsupported operator in JSONPath: @")));

  path = "$abc";
  status =
      ValidJSONPathIterator::Create(path, /*sql_standard_mode=*/true).status();
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kOutOfRange,
                               HasSubstr("Invalid token in JSONPath at: abc")));

  path = "";
  status =
      ValidJSONPathIterator::Create(path, /*sql_standard_mode=*/true).status();
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kOutOfRange,
                               HasSubstr("JSONPath must start with '$'")));
}

void ExtractArrayOrStringArray(
    JSONPathArrayExtractor* parser,
    std::vector<std::optional<std::string>>* output, bool* is_null,
    std::optional<std::function<void(absl::Status)>> issue_warning) {
  parser->set_special_character_escaping(true);
  parser->set_special_character_key_escaping(true);
  std::vector<std::string> result;
  parser->ExtractArray(&result, is_null, issue_warning);
  output->assign(result.begin(), result.end());
}

void ExtractArrayOrStringArray(
    JSONPathStringArrayExtractor* parser,
    std::vector<std::optional<std::string>>* output, bool* is_null,
    std::optional<std::function<void(absl::Status)>> ignored) {
  parser->ExtractStringArray(output, is_null);
}

template <class ParserClass>
void ComplianceJSONExtractArrayTest(const std::vector<FunctionTestCall>& tests,
                                    bool sql_standard_mode) {
  for (const FunctionTestCall& test : tests) {
    if (test.params.params()[0].is_null() ||
        test.params.params()[1].is_null()) {
      continue;
    }
    const std::string json = test.params.param(0).string_value();
    const std::string json_path = test.params.param(1).string_value();
    const Value& expected_result = test.params.result();

    std::vector<std::optional<std::string>> output;
    std::vector<Value> result_array;
    absl::Status status;
    bool is_null = true;
    auto evaluator_status =
        ValidJSONPathIterator::Create(json_path, sql_standard_mode);
    if (evaluator_status.ok()) {
      bool is_warning_called = false;
      const std::unique_ptr<ValidJSONPathIterator>& path_itr =
          evaluator_status.value();
      ParserClass parser(json, path_itr.get());
      // Because key_escaping is enabled a warning should never be
      // triggered.
      ExtractArrayOrStringArray(&parser, &output, &is_null,
                                [&is_warning_called](absl::Status status) {
                                  is_warning_called = true;
                                });
      EXPECT_FALSE(is_warning_called);
    } else {
      status = evaluator_status.status();
    }

    if (!status.ok() || !test.params.status().ok()) {
      EXPECT_EQ(test.params.status().code(), status.code()) << status;
    } else {
      for (const auto& element : output) {
        result_array.push_back(element.has_value() ? values::String(*element)
                                                   : values::NullString());
      }
      Value result = values::UnsafeArray(types::StringArrayType(),
                                         std::move(result_array));
      EXPECT_EQ(is_null, expected_result.is_null());
      if (!expected_result.is_null()) {
        EXPECT_EQ(result, expected_result);
      }
    }
  }
}

// Compliance Tests on JSON_QUERY_ARRAY
TEST(JSONPathExtractor, ComplianceJSONQueryArray) {
  const std::vector<FunctionTestCall> tests =
      GetFunctionTestsStringJsonQueryArray();
  ComplianceJSONExtractArrayTest<JSONPathArrayExtractor>(
      tests, /*sql_standard_mode=*/true);
}

// Compliance Tests on JSON_EXTRACT_ARRAY
TEST(JSONPathExtractor, ComplianceJSONExtractArray) {
  const std::vector<FunctionTestCall> tests =
      GetFunctionTestsStringJsonExtractArray();
  ComplianceJSONExtractArrayTest<JSONPathArrayExtractor>(
      tests, /*sql_standard_mode=*/false);
}

// Compliance Tests on JSON_VALUE_ARRAY
TEST(JSONPathExtractor, ComplianceJSONValueArray) {
  const std::vector<FunctionTestCall> tests =
      GetFunctionTestsStringJsonValueArray();
  ComplianceJSONExtractArrayTest<JSONPathStringArrayExtractor>(
      tests, /*sql_standard_mode=*/true);
}

// Compliance Tests on JSON_EXTRACT_STRING_ARRAY
TEST(JSONPathExtractor, ComplianceJSONExtractStringArray) {
  const std::vector<FunctionTestCall> tests =
      GetFunctionTestsStringJsonExtractStringArray();
  ComplianceJSONExtractArrayTest<JSONPathStringArrayExtractor>(
      tests, /*sql_standard_mode=*/false);
}

TEST(JsonPathEvaluatorTest, ExtractingArrayCloseToLimitSucceeds) {
  const int kNestingDepth = JSONPathExtractor::kMaxParsingDepth;
  const std::string nested_array_json(kNestingDepth, '[');
  std::string value;
  std::vector<std::string> array_value;
  std::vector<std::optional<std::string>> scalar_array_value;
  absl::Status status;
  bool is_null = true;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JsonPathEvaluator> path_evaluator,
      JsonPathEvaluator::Create(
          "$", /*sql_standard_mode=*/true,
          /*enable_special_character_escaping_in_values=*/false,
          /*enable_special_character_escaping_in_keys=*/false));
  // Extracting should succeed, but the result is null since the arrays are not
  // closed.
  ZETASQL_EXPECT_OK(path_evaluator->Extract(nested_array_json, &value, &is_null));
  EXPECT_TRUE(is_null);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      path_evaluator, JsonPathEvaluator::Create(
                          "$", /*sql_standard_mode=*/true,
                          /*enable_special_character_escaping_in_values=*/false,
                          /*enable_special_character_escaping_in_keys=*/false));
  // Extracting should succeed, but the result is null since the arrays are not
  // closed.
  ZETASQL_EXPECT_OK(path_evaluator->ExtractScalar(nested_array_json, &value, &is_null));
  EXPECT_TRUE(is_null);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      path_evaluator, JsonPathEvaluator::Create(
                          "$", /*sql_standard_mode=*/false,
                          /*enable_special_character_escaping_in_values=*/false,
                          /*enable_special_character_escaping_in_keys=*/false));
  // Extracting should succeed, but the result is null since the arrays are not
  // closed.
  ZETASQL_EXPECT_OK(
      path_evaluator->ExtractArray(nested_array_json, &array_value, &is_null));
  EXPECT_TRUE(is_null);
  ZETASQL_EXPECT_OK(path_evaluator->ExtractStringArray(nested_array_json,
                                               &scalar_array_value, &is_null));
  EXPECT_TRUE(is_null);
}

TEST(JsonPathEvaluatorTest, DeeplyNestedArrayCausesFailure) {
  const int kNestingDepth = JSONPathExtractor::kMaxParsingDepth + 1;
  const std::string nested_array_json(kNestingDepth, '[');
  std::string json_path = "$";
  for (int i = 0; i < kNestingDepth; ++i) {
    absl::StrAppend(&json_path, "[0]");
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JsonPathEvaluator> path_evaluator,
      JsonPathEvaluator::Create(
          json_path, /*sql_standard_mode=*/true,
          /*enable_special_character_escaping_in_values=*/false,
          /*enable_special_character_escaping_in_keys=*/false));
  std::string value;
  std::vector<std::string> array_value;
  std::vector<std::optional<std::string>> scalar_array_value;
  bool is_null = true;
  EXPECT_THAT(path_evaluator->Extract(nested_array_json, &value, &is_null),
              StatusIs(absl::StatusCode::kOutOfRange,
                       "JSON parsing failed due to deeply nested array/struct. "
                       "Maximum nesting depth is 1000"));
  EXPECT_TRUE(is_null);
  EXPECT_THAT(
      path_evaluator->ExtractScalar(nested_array_json, &value, &is_null),
      StatusIs(absl::StatusCode::kOutOfRange,
               "JSON parsing failed due to deeply nested array/struct. "
               "Maximum nesting depth is 1000"));
  EXPECT_TRUE(is_null);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      path_evaluator, JsonPathEvaluator::Create(
                          json_path, /*sql_standard_mode=*/false,
                          /*enable_special_character_escaping_in_values=*/false,
                          /*enable_special_character_escaping_in_keys=*/false));
  EXPECT_THAT(
      path_evaluator->ExtractArray(nested_array_json, &array_value, &is_null),
      StatusIs(absl::StatusCode::kOutOfRange,
               "JSON parsing failed due to deeply nested array/struct. "
               "Maximum nesting depth is 1000"));
  EXPECT_THAT(path_evaluator->ExtractStringArray(nested_array_json,
                                                 &scalar_array_value, &is_null),
              StatusIs(absl::StatusCode::kOutOfRange,
                       "JSON parsing failed due to deeply nested array/struct. "
                       "Maximum nesting depth is 1000"));
  EXPECT_TRUE(is_null);
}

TEST(JsonPathEvaluatorTest, ExtractingObjectCloseToLimitSucceeds) {
  const int kNestingDepth = JSONPathExtractor::kMaxParsingDepth;
  std::string nested_object_json;
  for (int i = 0; i < kNestingDepth; ++i) {
    absl::StrAppend(&nested_object_json, "{\"x\":");
  }
  std::string value;
  std::vector<std::string> array_value;
  std::vector<std::optional<std::string>> scalar_array_value;
  absl::Status status;
  bool is_null = true;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JsonPathEvaluator> path_evaluator,
      JsonPathEvaluator::Create(
          "$", /*sql_standard_mode=*/true,
          /*enable_special_character_escaping_in_values=*/false,
          /*enable_special_character_escaping_in_keys=*/false));
  // Extracting should succeed, but the result is null since the objects are not
  // closed.
  ZETASQL_EXPECT_OK(path_evaluator->Extract(nested_object_json, &value, &is_null));
  EXPECT_TRUE(is_null);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      path_evaluator, JsonPathEvaluator::Create(
                          "$", /*sql_standard_mode=*/true,
                          /*enable_special_character_escaping_in_values=*/false,
                          /*enable_special_character_escaping_in_keys=*/false));
  // Extracting should succeed, but the result is null since the objects are not
  // closed.
  ZETASQL_EXPECT_OK(
      path_evaluator->ExtractScalar(nested_object_json, &value, &is_null));
  EXPECT_TRUE(is_null);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      path_evaluator, JsonPathEvaluator::Create(
                          "$", /*sql_standard_mode=*/false,
                          /*enable_special_character_escaping_in_values=*/false,
                          /*enable_special_character_escaping_in_keys=*/false));
  // Extracting should succeed, but the result is null since the objects are not
  // closed.
  ZETASQL_EXPECT_OK(
      path_evaluator->ExtractArray(nested_object_json, &array_value, &is_null));
  EXPECT_TRUE(is_null);
  ZETASQL_EXPECT_OK(path_evaluator->ExtractStringArray(nested_object_json,
                                               &scalar_array_value, &is_null));
  EXPECT_TRUE(is_null);
}

TEST(JsonPathEvaluatorTest, DeeplyNestedObjectCausesFailure) {
  const int kNestingDepth = JSONPathExtractor::kMaxParsingDepth + 1;
  std::string nested_object_json;
  for (int i = 0; i < kNestingDepth; ++i) {
    absl::StrAppend(&nested_object_json, "{\"x\":");
  }
  std::string json_path = "$";
  for (int i = 0; i < kNestingDepth; ++i) {
    absl::StrAppend(&json_path, ".x");
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JsonPathEvaluator> path_evaluator,
      JsonPathEvaluator::Create(
          json_path, /*sql_standard_mode=*/true,
          /*enable_special_character_escaping_in_values=*/false,
          /*enable_special_character_escaping_in_keys=*/false));

  std::string value;
  std::vector<std::string> array_value;
  std::vector<std::optional<std::string>> scalar_array_value;
  bool is_null = true;
  EXPECT_THAT(path_evaluator->Extract(nested_object_json, &value, &is_null),
              StatusIs(absl::StatusCode::kOutOfRange,
                       "JSON parsing failed due to deeply nested array/struct. "
                       "Maximum nesting depth is 1000"));
  EXPECT_TRUE(is_null);
  EXPECT_THAT(
      path_evaluator->ExtractScalar(nested_object_json, &value, &is_null),
      StatusIs(absl::StatusCode::kOutOfRange,
               "JSON parsing failed due to deeply nested array/struct. "
               "Maximum nesting depth is 1000"));
  EXPECT_TRUE(is_null);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      path_evaluator, JsonPathEvaluator::Create(
                          json_path, /*sql_standard_mode=*/false,
                          /*enable_special_character_escaping_in_values=*/false,
                          /*enable_special_character_escaping_in_keys=*/false));
  EXPECT_THAT(
      path_evaluator->ExtractArray(nested_object_json, &array_value, &is_null),
      StatusIs(absl::StatusCode::kOutOfRange,
               "JSON parsing failed due to deeply nested array/struct. "
               "Maximum nesting depth is 1000"));
  EXPECT_TRUE(is_null);
  EXPECT_THAT(path_evaluator->ExtractStringArray(nested_object_json,
                                                 &scalar_array_value, &is_null),
              StatusIs(absl::StatusCode::kOutOfRange,
                       "JSON parsing failed due to deeply nested array/struct. "
                       "Maximum nesting depth is 1000"));
  EXPECT_TRUE(is_null);
}

TEST(JsonConversionTest, ConvertJsonToInt64) {
  std::vector<std::pair<JSONValue, std::optional<int64_t>>>
      inputs_and_expected_outputs;
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{1}), 1);
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{-1}), -1);
  inputs_and_expected_outputs.emplace_back(JSONValue(10.0), 10);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::numeric_limits<int64_t>::min()),
      std::numeric_limits<int64_t>::min());
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::numeric_limits<int64_t>::max()),
      std::numeric_limits<int64_t>::max());
  // Other types should return an error
  inputs_and_expected_outputs.emplace_back(JSONValue(1e100), std::nullopt);
  inputs_and_expected_outputs.emplace_back(JSONValue(1.5), std::nullopt);
  inputs_and_expected_outputs.emplace_back(JSONValue(true), std::nullopt);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"10"}),
                                           std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"({"a": 1})").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"([10, 20])").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("null").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::numeric_limits<uint64_t>::max()), std::nullopt);

  for (const auto& [input, expected_output] : inputs_and_expected_outputs) {
    SCOPED_TRACE(
        absl::Substitute("INT64('$0')", input.GetConstRef().ToString()));

    absl::StatusOr<int64_t> output = ConvertJsonToInt64(input.GetConstRef());
    EXPECT_EQ(output.ok(), expected_output.has_value());
    if (output.ok() && expected_output.has_value()) {
      EXPECT_EQ(*output, *expected_output);
    }
  }
}

TEST(JsonConversionTest, ConvertJsonToBool) {
  std::vector<std::pair<JSONValue, std::optional<bool>>>
      inputs_and_expected_outputs;
  inputs_and_expected_outputs.emplace_back(JSONValue(false), false);
  inputs_and_expected_outputs.emplace_back(JSONValue(true), true);
  // Other types should return an error
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{1}), std::nullopt);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"10"}),
                                           std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"({"a": 1})").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"([10, 20])").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("null").value(), std::nullopt);
  for (const auto& [input, expected_output] : inputs_and_expected_outputs) {
    SCOPED_TRACE(
        absl::Substitute("BOOL('$0')", input.GetConstRef().ToString()));

    absl::StatusOr<bool> output = ConvertJsonToBool(input.GetConstRef());
    EXPECT_EQ(output.ok(), expected_output.has_value());
    if (output.ok() && expected_output.has_value()) {
      EXPECT_EQ(*output, *expected_output);
    }
  }
}

TEST(JsonConversionTest, ConvertJsonToString) {
  std::vector<std::pair<JSONValue, std::optional<std::string>>>
      inputs_and_expected_outputs;
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"test"}),
                                           "test");
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"abc123"}),
                                           "abc123");
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"TesT"}),
                                           "TesT");
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"1"}), "1");
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{""}), "");
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"12¿©?Æ"}),
                                           "12¿©?Æ");
  // Other types should return an error
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{1}), std::nullopt);
  inputs_and_expected_outputs.emplace_back(JSONValue(true), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"({"a": 1})").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"([10, 20])").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("null").value(), std::nullopt);
  for (const auto& [input, expected_output] : inputs_and_expected_outputs) {
    SCOPED_TRACE(
        absl::Substitute("STRING('$0')", input.GetConstRef().ToString()));

    absl::StatusOr<std::string> output =
        ConvertJsonToString(input.GetConstRef());
    EXPECT_EQ(output.ok(), expected_output.has_value());
    if (output.ok() && expected_output.has_value()) {
      EXPECT_EQ(*output, *expected_output);
    }
  }
}

TEST(JsonConversionTest, ConvertJsonToDouble) {
  std::vector<std::pair<JSONValue, std::optional<double>>>
      inputs_and_expected_outputs;
  // Behavior the same when wide_number_mode is "round" and "exact"
  inputs_and_expected_outputs.emplace_back(JSONValue(1.0), 1.0);
  inputs_and_expected_outputs.emplace_back(JSONValue(-1.0), -1.0);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::numeric_limits<double>::min()),
      std::numeric_limits<double>::min());
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::numeric_limits<double>::max()),
      std::numeric_limits<double>::max());
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{1}), double{1});
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{-1}), double{-1});
  inputs_and_expected_outputs.emplace_back(JSONValue(uint64_t{1}), double{1});
  inputs_and_expected_outputs.emplace_back(
      JSONValue(int64_t{-9007199254740992}), double{-9007199254740992});
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{9007199254740992}),
                                           double{9007199254740992});
  // Other types should return an error
  inputs_and_expected_outputs.emplace_back(JSONValue(true), std::nullopt);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"10"}),
                                           std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"({"a": 1})").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"([10, 20])").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("null").value(), std::nullopt);

  for (const auto& [input, expected_output] : inputs_and_expected_outputs) {
    SCOPED_TRACE(absl::Substitute("DOUBLE('$0', 'exact')",
                                  input.GetConstRef().ToString()));
    absl::StatusOr<double> output = ConvertJsonToDouble(
        input.GetConstRef(), WideNumberMode::kExact, PRODUCT_INTERNAL);
    EXPECT_EQ(output.ok(), expected_output.has_value());
    if (output.ok() && expected_output.has_value()) {
      EXPECT_EQ(*output, *expected_output);
    }
    SCOPED_TRACE(absl::Substitute("DOUBLE('$0', 'round')",
                                  input.GetConstRef().ToString()));
    output = ConvertJsonToDouble(input.GetConstRef(), WideNumberMode::kRound,
                                 PRODUCT_INTERNAL);
    EXPECT_EQ(output.ok(), expected_output.has_value());
    if (output.ok() && expected_output.has_value()) {
      EXPECT_EQ(*output, *expected_output);
    }
  }
}

TEST(JsonConversionTest, ConvertJsonToDoubleFailInExactOnly) {
  std::vector<std::pair<JSONValue, double>> inputs_and_expected_outputs;
  // Number too large to round trip
  inputs_and_expected_outputs.emplace_back(
      JSONValue(uint64_t{18446744073709551615u}),
      double{1.8446744073709552e+19});
  // Number too small to round trip
  inputs_and_expected_outputs.emplace_back(
      JSONValue(int64_t{-9007199254740993}), double{-9007199254740992});

  for (const auto& [input, expected_output] : inputs_and_expected_outputs) {
    SCOPED_TRACE(absl::Substitute("DOUBLE('$0', 'round')",
                                  input.GetConstRef().ToString()));
    absl::StatusOr<double> output = ConvertJsonToDouble(
        input.GetConstRef(), WideNumberMode::kRound, PRODUCT_INTERNAL);

    EXPECT_TRUE(output.ok());
    EXPECT_EQ(*output, expected_output);
    SCOPED_TRACE(absl::Substitute("DOUBLE('$0', 'exact')",
                                  input.GetConstRef().ToString()));
    output = ConvertJsonToDouble(input.GetConstRef(), WideNumberMode::kExact,
                                 PRODUCT_INTERNAL);
    EXPECT_FALSE(output.ok());
  }
}

TEST(JsonConversionTest, ConvertJsonToDoubleErrorMessage) {
  JSONValue input = JSONValue(uint64_t{18446744073709551615u});
  // Internal mode uses DOUBLE in error message
  SCOPED_TRACE(absl::Substitute("DOUBLE('$0', 'exact')",
                                input.GetConstRef().ToString()));
  absl::StatusOr<double> output = ConvertJsonToDouble(
      input.GetConstRef(), WideNumberMode::kExact, PRODUCT_INTERNAL);
  EXPECT_FALSE(output.ok());
  EXPECT_EQ(output.status().message(),
            "JSON number: 18446744073709551615 cannot be converted to DOUBLE "
            "without loss of precision");
  // External mode uses FLOAT64 in error message
  SCOPED_TRACE(absl::Substitute("FLOAT64('$0', 'exact')",
                                input.GetConstRef().ToString()));
  output = ConvertJsonToDouble(input.GetConstRef(), WideNumberMode::kExact,
                               PRODUCT_EXTERNAL);
  EXPECT_FALSE(output.ok());
  EXPECT_EQ(output.status().message(),
            "JSON number: 18446744073709551615 cannot be converted to FLOAT64 "
            "without loss of precision");
}

TEST(JsonConversionTest, GetJsonType) {
  std::vector<std::pair<JSONValue, std::optional<std::string>>>
      inputs_and_expected_outputs;
  inputs_and_expected_outputs.emplace_back(JSONValue(2.0), "number");
  inputs_and_expected_outputs.emplace_back(JSONValue(-1.0), "number");
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{1}), "number");
  inputs_and_expected_outputs.emplace_back(JSONValue(true), "boolean");
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"10"}),
                                           "string");
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"({"a": 1})").value(), "object");
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"([10, 20])").value(), "array");
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("null").value(), "null");
  for (const auto& [input, expected_output] : inputs_and_expected_outputs) {
    SCOPED_TRACE(
        absl::Substitute("TYPE('$0')", input.GetConstRef().ToString()));
    absl::StatusOr<std::string> output = GetJsonType(input.GetConstRef());
    EXPECT_EQ(output.ok(), expected_output.has_value());
    if (output.ok() && expected_output.has_value()) {
      EXPECT_EQ(*output, *expected_output);
    }
  }
}

TEST(JsonLaxConversionTest, Bool) {
  std::vector<std::pair<JSONValue, std::optional<bool>>>
      inputs_and_expected_outputs;
  // Bools
  inputs_and_expected_outputs.emplace_back(JSONValue(true), true);
  inputs_and_expected_outputs.emplace_back(JSONValue(false), false);
  // Strings
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"true"}),
                                           true);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"false"}),
                                           false);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"TRue"}),
                                           true);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"FaLse"}),
                                           false);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"foo"}),
                                           std::nullopt);
  // Numbers. Note that -inf, inf, and NaN are not valid JSON numeric values.
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{0}), false);
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{10}), true);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(int64_t{std::numeric_limits<int64_t>::min()}), true);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(uint64_t{std::numeric_limits<uint64_t>::max()}), true);
  inputs_and_expected_outputs.emplace_back(JSONValue(double{0.0}), false);
  inputs_and_expected_outputs.emplace_back(JSONValue(double{1.1}), true);
  inputs_and_expected_outputs.emplace_back(JSONValue(double{-1.1}), true);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{std::numeric_limits<double>::min()}), true);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{std::numeric_limits<double>::lowest()}), true);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{std::numeric_limits<double>::max()}), true);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("-0").value(), false);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("-0.0").value(), false);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("-0.0e2").value(), false);
  // Object/Array/Null
  inputs_and_expected_outputs.emplace_back(JSONValue(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"({"a": 1})").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("[true]").value(), std::nullopt);
  for (const auto& [input, expected_output] : inputs_and_expected_outputs) {
    SCOPED_TRACE(
        absl::Substitute("LAX_BOOL($0)", input.GetConstRef().ToString()));
    absl::StatusOr<std::optional<bool>> result =
        LaxConvertJsonToBool(input.GetConstRef());
    ZETASQL_ASSERT_OK(result);
    EXPECT_EQ(*result, expected_output);
  }
}

TEST(JsonLaxConversionTest, Int64) {
  std::vector<std::pair<JSONValue, std::optional<int64_t>>>
      inputs_and_expected_outputs;
  // Bools
  inputs_and_expected_outputs.emplace_back(JSONValue(true), 1);
  inputs_and_expected_outputs.emplace_back(JSONValue(false), 0);
  // Strings
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"10"}), 10);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"1.1"}), 1);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"1.1e2"}),
                                           110);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"+1.5"}), 2);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::string{"123456789012345678.0"}), 123456789012345678);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"1e100"}),
                                           std::nullopt);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"foo"}),
                                           std::nullopt);
  // Numbers. Note that -inf, inf, and NaN are not valid JSON numeric values.
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{10}), 10);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(int64_t{std::numeric_limits<int64_t>::min()}),
      std::numeric_limits<int64_t>::min());
  inputs_and_expected_outputs.emplace_back(
      JSONValue(int64_t{std::numeric_limits<int64_t>::max()}),
      std::numeric_limits<int64_t>::max());
  inputs_and_expected_outputs.emplace_back(
      JSONValue(uint64_t{std::numeric_limits<uint64_t>::max()}), std::nullopt);
  inputs_and_expected_outputs.emplace_back(JSONValue(double{1.1}), 1);
  inputs_and_expected_outputs.emplace_back(JSONValue(double{3.5}), 4);
  inputs_and_expected_outputs.emplace_back(JSONValue(double{1.1e2}), 110);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{123456789012345678.0}), 123456789012345680);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{std::numeric_limits<double>::min()}), 0);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{std::numeric_limits<double>::lowest()}), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{std::numeric_limits<double>::max()}), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("1e100").value(), std::nullopt);
  // Object/Array/Null
  inputs_and_expected_outputs.emplace_back(JSONValue(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"({"a": 1})").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("[1]").value(), std::nullopt);
  for (const auto& [input, expected_output] : inputs_and_expected_outputs) {
    SCOPED_TRACE(
        absl::Substitute("LAX_INT64('$0')", input.GetConstRef().ToString()));
    absl::StatusOr<std::optional<int64_t>> result =
        LaxConvertJsonToInt64(input.GetConstRef());
    ZETASQL_ASSERT_OK(result);
    EXPECT_EQ(*result, expected_output);
  }
}

TEST(JsonLaxConversionTest, Float) {
  std::vector<std::pair<JSONValue, std::optional<double>>>
      inputs_and_expected_outputs;
  // Bools
  inputs_and_expected_outputs.emplace_back(JSONValue(true), std::nullopt);
  inputs_and_expected_outputs.emplace_back(JSONValue(false), std::nullopt);
  // Strings
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"10"}), 10.0);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"-10"}),
                                           -10.0);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"1.1"}), 1.1);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"1.1e2"}),
                                           110.0);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::string{"9007199254740993"}), 9007199254740992.0);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"+1.5"}), 1.5);
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"foo"}),
                                           std::nullopt);
  // Numbers. Note that -inf, inf, and NaN are not valid JSON numeric values.
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{-10}), -10);
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{9007199254740993}),
                                           9007199254740992);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(int64_t{std::numeric_limits<int64_t>::min()}),
      static_cast<double>(std::numeric_limits<int64_t>::min()));
  inputs_and_expected_outputs.emplace_back(
      JSONValue(int64_t{std::numeric_limits<int64_t>::max()}),
      static_cast<double>(std::numeric_limits<int64_t>::max()));
  inputs_and_expected_outputs.emplace_back(
      JSONValue(uint64_t{std::numeric_limits<uint64_t>::max()}),
      static_cast<double>((std::numeric_limits<uint64_t>::max())));
  inputs_and_expected_outputs.emplace_back(JSONValue(double{1.1}), 1.1);
  inputs_and_expected_outputs.emplace_back(JSONValue(double{3.5}), 3.5);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("1.1e2").value(), 110);
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{std::numeric_limits<double>::min()}),
      std::numeric_limits<double>::min());
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{std::numeric_limits<double>::lowest()}),
      std::numeric_limits<double>::lowest());
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{std::numeric_limits<double>::max()}),
      std::numeric_limits<double>::max());
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("1e100").value(), 1e+100);
  // Object/Array/Null
  inputs_and_expected_outputs.emplace_back(JSONValue(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"({"a": 1})").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("[1]").value(), std::nullopt);
  for (const auto& [input, expected_output] : inputs_and_expected_outputs) {
    SCOPED_TRACE(
        absl::Substitute("LAX_FLOAT64('$0')", input.GetConstRef().ToString()));
    absl::StatusOr<std::optional<double>> result =
        LaxConvertJsonToFloat64(input.GetConstRef());
    ZETASQL_ASSERT_OK(result);
    EXPECT_EQ(*result, expected_output);
  }

  // Special cases.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::optional<double> result,
      LaxConvertJsonToFloat64(JSONValue(std::string{"NaN"}).GetConstRef()));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(std::isnan(*result));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      result,
      LaxConvertJsonToFloat64(JSONValue(std::string{"Inf"}).GetConstRef()));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(std::isinf(*result));
  ZETASQL_ASSERT_OK_AND_ASSIGN(result,
                       LaxConvertJsonToFloat64(
                           JSONValue(std::string{"-InfiNiTY"}).GetConstRef()));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(std::isinf(*result));
}

TEST(JsonLaxConversionTest, String) {
  std::vector<std::pair<JSONValue, std::optional<std::string>>>
      inputs_and_expected_outputs;
  // Bools
  inputs_and_expected_outputs.emplace_back(JSONValue(true), "true");
  inputs_and_expected_outputs.emplace_back(JSONValue(false), "false");
  // Strings
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"foo"}),
                                           "foo");
  inputs_and_expected_outputs.emplace_back(JSONValue(std::string{"10"}), "10");
  // Numbers. Note that -inf, inf, and NaN are not valid JSON numeric values.
  inputs_and_expected_outputs.emplace_back(JSONValue(int64_t{-10}), "-10");
  inputs_and_expected_outputs.emplace_back(
      JSONValue(int64_t{std::numeric_limits<int64_t>::min()}),
      absl::StrCat(std::numeric_limits<std::int64_t>::min()));
  inputs_and_expected_outputs.emplace_back(
      JSONValue(int64_t{std::numeric_limits<int64_t>::max()}),
      absl::StrCat(std::numeric_limits<int64_t>::max()));
  inputs_and_expected_outputs.emplace_back(JSONValue(uint64_t{10}), "10");
  inputs_and_expected_outputs.emplace_back(
      JSONValue(uint64_t{std::numeric_limits<uint64_t>::max()}),
      absl::StrCat(std::numeric_limits<std::uint64_t>::max()));
  inputs_and_expected_outputs.emplace_back(JSONValue(double{1.1}), "1.1");
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{std::numeric_limits<double>::min()}),
      "2.2250738585072014e-308");
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{std::numeric_limits<double>::lowest()}),
      "-1.7976931348623157e+308");
  inputs_and_expected_outputs.emplace_back(
      JSONValue(double{std::numeric_limits<double>::max()}),
      "1.7976931348623157e+308");
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("1e100").value(), "1e+100");
  // Object/Array/Null
  inputs_and_expected_outputs.emplace_back(JSONValue(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString(R"({"a": 1})").value(), std::nullopt);
  inputs_and_expected_outputs.emplace_back(
      JSONValue::ParseJSONString("[1]").value(), std::nullopt);
  for (const auto& [input, expected_output] : inputs_and_expected_outputs) {
    SCOPED_TRACE(
        absl::Substitute("LAX_STRING('$0')", input.GetConstRef().ToString()));
    absl::StatusOr<std::optional<std::string>> result =
        LaxConvertJsonToString(input.GetConstRef());
    ZETASQL_ASSERT_OK(result);
    EXPECT_EQ(*result, expected_output);
  }
}

TEST(JsonArrayTest, Compliance) {
  const std::vector<FunctionTestCall> tests = GetFunctionTestsJsonArray();

  for (const FunctionTestCall& test : tests) {
    SCOPED_TRACE(absl::Substitute(
        "JSON_ARRAY($0)",
        absl::StrJoin(test.params.params(), ",",
                      [](std::string* out, const Value& value) {
                        absl::StrAppend(out, value.ShortDebugString());
                      })));
    zetasql::LanguageOptions language_options;
    for (const auto& feature : test.params.required_features()) {
      language_options.EnableLanguageFeature(feature);
    }

    absl::StatusOr<JSONValue> output =
        JsonArray(test.params.params(), language_options,
                  /*canonicalize_zero=*/true);

    const Value expected_value = test.params.result();
    const absl::Status expected_status = test.params.status();

    if (expected_status.ok()) {
      ZETASQL_ASSERT_OK(output);
      EXPECT_EQ(expected_value, values::Json(*std::move(output)));
    } else {
      EXPECT_EQ(output.status().code(), expected_status.code());
    }
  }
}

std::string JsonObjectDebugString(const FunctionTestCall& test) {
  return absl::Substitute(
      "JSON_OBJECT($0)",
      absl::StrJoin(test.params.params(), ",",
                    [](std::string* out, const Value& value) {
                      absl::StrAppend(out, value.ShortDebugString());
                    }));
}

zetasql::LanguageOptions GetLanguageOptionsFromTest(
    const FunctionTestCall& test) {
  zetasql::LanguageOptions language_options;
  for (const auto& feature : test.params.required_features()) {
    language_options.EnableLanguageFeature(feature);
  }
  return language_options;
}

TEST(JsonObjectTest, VariadicArgs) {
  // NULL keys are not supported by the library function. They will be handled
  // by the SQL function implementation.
  const std::vector<FunctionTestCall> tests =
      GetFunctionTestsJsonObject(/*include_null_key_tests=*/false);

  for (const FunctionTestCall& test : tests) {
    SCOPED_TRACE(JsonObjectDebugString(test));

    JsonObjectBuilder builder(GetLanguageOptionsFromTest(test),
                              /*canonicalize_zero=*/true);

    std::vector<absl::string_view> keys;
    std::vector<const Value*> values;

    // Signature: JSON_OBJECT(STRING key, ANY value, ...)
    keys.reserve((test.params.num_params() + 1) / 2);
    values.reserve(test.params.num_params() / 2);
    for (int i = 0; i < test.params.num_params(); i += 2) {
      const Value& key = test.params.param(i);
      ASSERT_TRUE(key.type()->IsString());
      ASSERT_FALSE(key.is_null());
      keys.push_back(key.string_value());
    }
    for (int i = 1; i < test.params.num_params(); i += 2) {
      values.push_back(&test.params.param(i));
    }
    absl::StatusOr<JSONValue> output =
        JsonObject(keys, absl::MakeSpan(values), builder);

    const Value expected_value = test.params.result();
    const absl::Status expected_status = test.params.status();

    if (expected_status.ok()) {
      ZETASQL_ASSERT_OK(output);
      EXPECT_EQ(expected_value, values::Json(*std::move(output)));
    } else {
      EXPECT_EQ(output.status().code(), expected_status.code());
    }
  }
}

TEST(JsonObjectTest, TwoArrayArgs) {
  // NULL keys are not supported by the library function. They will be handled
  // by the SQL function implementation.
  const std::vector<FunctionTestCall> tests =
      GetFunctionTestsJsonObjectArrays(/*include_null_key_tests=*/false);

  for (const FunctionTestCall& test : tests) {
    SCOPED_TRACE(JsonObjectDebugString(test));

    JsonObjectBuilder builder(GetLanguageOptionsFromTest(test),
                              /*canonicalize_zero=*/true);

    // Signature: JSON_OBJECT(ARRAY<STRING> keys, ARRAY<ANY> values)
    ASSERT_EQ(test.params.num_params(), 2);
    ASSERT_TRUE(test.params.param(0).type()->IsArray());
    ASSERT_TRUE(test.params.param(1).type()->IsArray());

    std::vector<absl::string_view> keys;
    std::vector<const Value*> values;
    keys.reserve(test.params.param(0).num_elements());
    for (const Value& key : test.params.param(0).elements()) {
      ASSERT_TRUE(key.type()->IsString());
      ASSERT_FALSE(key.is_null());
      keys.push_back(key.string_value());
    }

    for (const Value& value : test.params.param(1).elements()) {
      values.push_back(&value);
    }

    absl::StatusOr<JSONValue> output =
        JsonObject(keys, absl::MakeSpan(values), builder);

    const Value expected_value = test.params.result();
    const absl::Status expected_status = test.params.status();

    if (expected_status.ok()) {
      ZETASQL_ASSERT_OK(output);
      EXPECT_EQ(expected_value, values::Json(*std::move(output)));
    } else {
      EXPECT_EQ(output.status().code(), expected_status.code());
    }
  }
}

TEST(JsonObjectTest, ReuseBuilder) {
  JsonObjectBuilder builder(LanguageOptions(), /*canonicalize_zero=*/true);

  {
    std::vector<absl::string_view> keys = {"a", "b"};
    std::vector<Value> values_storage = {values::Int64(10),
                                         values::String("foo")};
    std::vector<const Value*> values;
    for (const auto& value : values_storage) {
      values.push_back(&value);
    }
    absl::StatusOr<JSONValue> output =
        JsonObject(keys, absl::MakeSpan(values), builder);
    ZETASQL_ASSERT_OK(output);
    EXPECT_THAT(output->GetConstRef(),
                JsonEq(JSONValue::ParseJSONString(R"({"a":10,"b":"foo"})")
                           ->GetConstRef()));
  }
  {
    std::vector<absl::string_view> keys = {"a", "c"};
    std::vector<Value> values_storage = {values::Int64(15), values::Bool(true)};
    std::vector<const Value*> values;
    for (const auto& value : values_storage) {
      values.push_back(&value);
    }
    absl::StatusOr<JSONValue> output =
        JsonObject(keys, absl::MakeSpan(values), builder);
    ZETASQL_ASSERT_OK(output);
    EXPECT_THAT(
        output->GetConstRef(),
        JsonEq(
            JSONValue::ParseJSONString(R"({"a":15,"c":true})")->GetConstRef()));
  }
}

TEST(JsonObjectBuilderTest, NoCallsToAdd) {
  JsonObjectBuilder builder(LanguageOptions(), /*canonicalize_zero=*/true);
  EXPECT_THAT(builder.Build().GetConstRef(),
              JsonEq(JSONValue::ParseJSONString("{}")->GetConstRef()));
}

TEST(JsonObjectBuilderTest, AddNoDuplicate) {
  JsonObjectBuilder builder(LanguageOptions(), /*canonicalize_zero=*/true);
  {
    auto inserted = builder.Add("field", values::Int64(10));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_TRUE(*inserted);
  }
  {
    auto inserted = builder.Add("field2", values::String("foo"));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_TRUE(*inserted);
  }
  EXPECT_THAT(
      builder.Build().GetConstRef(),
      JsonEq(JSONValue::ParseJSONString(R"({"field":10,"field2":"foo"})")
                 ->GetConstRef()));
}

TEST(JsonObjectBuilderTest, AddDuplicate) {
  JsonObjectBuilder builder(LanguageOptions(), /*canonicalize_zero=*/true);
  {
    auto inserted = builder.Add("field", values::Int64(10));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_TRUE(*inserted);
  }
  {
    auto inserted = builder.Add("field", values::String("foo"));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_FALSE(*inserted);
  }
  {
    auto inserted = builder.Add("field", values::Int64(20));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_FALSE(*inserted);
  }
  {
    auto inserted = builder.Add("field2", values::Int64(20));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_TRUE(*inserted);
  }
  EXPECT_THAT(builder.Build().GetConstRef(),
              JsonEq(JSONValue::ParseJSONString(R"({"field":10,"field2":20})")
                         ->GetConstRef()));
}

TEST(JsonObjectBuilderTest, ResetBuilder) {
  JsonObjectBuilder builder(LanguageOptions(), /*canonicalize_zero=*/true);
  {
    auto inserted = builder.Add("field", values::Int64(10));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_TRUE(*inserted);
  }
  builder.Reset();

  {
    // Same field to test that 'keys_set_' has been reset.
    auto inserted = builder.Add("field", values::String("foo"));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_TRUE(*inserted);
  }
  {
    auto inserted = builder.Add("field2", values::Int64(20));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_TRUE(*inserted);
  }
  EXPECT_THAT(
      builder.Build().GetConstRef(),
      JsonEq(JSONValue::ParseJSONString(R"({"field":"foo","field2":20})")
                 ->GetConstRef()));
}

TEST(JsonObjectBuilderTest, ReuseBuilder) {
  JsonObjectBuilder builder(LanguageOptions(), /*canonicalize_zero=*/true);
  {
    auto inserted = builder.Add("field", values::Int64(10));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_TRUE(*inserted);
  }
  EXPECT_THAT(
      builder.Build().GetConstRef(),
      JsonEq(JSONValue::ParseJSONString(R"({"field":10})")->GetConstRef()));

  {
    // Same field to test that 'keys_set_' has been reset.
    auto inserted = builder.Add("field", values::String("foo"));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_TRUE(*inserted);
  }
  EXPECT_THAT(
      builder.Build().GetConstRef(),
      JsonEq(JSONValue::ParseJSONString(R"({"field":"foo"})")->GetConstRef()));
}

TEST(JsonObjectBuilderTest, CanonicalizeZero) {
  {
    JsonObjectBuilder builder(LanguageOptions(), /*canonicalize_zero=*/true);
    auto inserted = builder.Add("field", values::Double(-0.0));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_TRUE(*inserted);

    EXPECT_THAT(
        builder.Build().GetConstRef(),
        JsonEq(JSONValue::ParseJSONString(R"({"field":0.0})")->GetConstRef()));
  }
  {
    JsonObjectBuilder builder(LanguageOptions(), /*canonicalize_zero=*/false);
    auto inserted = builder.Add("field", values::Double(-0.0));
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_TRUE(*inserted);

    EXPECT_THAT(
        builder.Build().GetConstRef(),
        JsonEq(JSONValue::ParseJSONString(R"({"field":-0.0})")->GetConstRef()));
  }
}

TEST(JsonObjectBuilderTest, StrictNumberParsing) {
  Value value = values::BigNumeric(
      BigNumericValue::FromString("1.111111111111111111").value());

  {
    LanguageOptions options;
    options.EnableLanguageFeature(FEATURE_JSON_STRICT_NUMBER_PARSING);
    JsonObjectBuilder builder(options, /*canonicalize_zero=*/true);
    auto inserted = builder.Add("field", value);
    EXPECT_THAT(inserted, StatusIs(absl::StatusCode::kOutOfRange));
    builder.Reset();
  }
  {
    JsonObjectBuilder builder(LanguageOptions(), /*canonicalize_zero=*/false);
    auto inserted = builder.Add("field", value);
    ZETASQL_ASSERT_OK(inserted);
    EXPECT_TRUE(*inserted);

    EXPECT_THAT(
        builder.Build().GetConstRef(),
        JsonEq(JSONValue::ParseJSONString(R"({"field":1.1111111111111112})")
                   ->GetConstRef()));
  }
}

std::unique_ptr<StrictJSONPathIterator> ParseJSONPath(absl::string_view path) {
  return StrictJSONPathIterator::Create(path).value();
}

TEST(JsonRemoveTest, InvalidJSONPath) {
  JSONValue value;
  JSONValueRef ref = value.GetRef();
  auto path_iter = ParseJSONPath("$");
  EXPECT_THAT(JsonRemove(ref, *path_iter),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("The JSONPath cannot be '$'")));
}

TEST(JsonRemoveTest, NoOp) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true, "b": [1.1, false, []]}, 10])";

  std::vector<absl::string_view> paths = {
      // $ is not an object
      "$.a",
      // $[0] is not an array
      "$[0][0]",
      // Array index out of bound
      "$[10]",
      // Key doesn't exist
      "$[2].c",
      // $[2] doesn't have the 'c' 'key'
      "$[2].c[1]",
      // Array index out of bound in array at $[2].b
      "$[2].b[4].a",
  };

  for (auto path : paths) {
    SCOPED_TRACE(
        absl::Substitute("JsonRemove('$0', '$1')", kInitialValue, path));
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    auto path_iter = ParseJSONPath(path);
    ZETASQL_ASSERT_OK_AND_ASSIGN(bool success, JsonRemove(ref, *path_iter));
    EXPECT_FALSE(success);
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  }
}

TEST(JsonRemoveTest, ValidRemove) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true, "b": [1.1, false, []]}, 10])";

  // Pair of: JSONPath to remove, expected output.
  std::vector<std::pair<absl::string_view, absl::string_view>>
      paths_and_outputs = {
          {"$[0]", R"([null,{"a":true,"b":[1.1,false,[]]},10])"},
          {"$[1]", R"(["foo",{"a":true,"b":[1.1,false,[]]},10])"},
          {"$[2]", R"(["foo",null,10])"},
          {"$[2].a", R"(["foo",null,{"b":[1.1,false,[]]},10])"},
          {"$[2].b", R"(["foo",null,{"a":true},10])"},
          {"$[2].b[2]", R"(["foo",null,{"a":true,"b":[1.1,false]},10])"},
      };

  for (auto [path, output] : paths_and_outputs) {
    SCOPED_TRACE(
        absl::Substitute("JsonRemove('$0', '$1')", kInitialValue, path));
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    auto path_iter = ParseJSONPath(path);
    ZETASQL_ASSERT_OK_AND_ASSIGN(bool success, JsonRemove(ref, *path_iter));
    EXPECT_TRUE(success);
    EXPECT_THAT(ref, JsonEq(JSONValue::ParseJSONString(output)->GetConstRef()));
  }
}

TEST(JsonRemoveTest, EmptyObjectAndArrayAreNotCleaned) {
  {
    // JsonRemove results in empty object which remains in the JSON value.
    JSONValue value = JSONValue::ParseJSONString(R"([10, {"a": 10}])").value();
    JSONValueRef ref = value.GetRef();
    auto path_iter = ParseJSONPath("$[1].a");
    ZETASQL_ASSERT_OK_AND_ASSIGN(bool success, JsonRemove(ref, *path_iter));
    EXPECT_TRUE(success);
    EXPECT_THAT(ref,
                JsonEq(JSONValue::ParseJSONString("[10, {}]")->GetConstRef()));
  }
  {
    // JsonRemove results in empty array which remains in the JSON value.
    JSONValue value = JSONValue::ParseJSONString(R"({"a": [10]})").value();
    JSONValueRef ref = value.GetRef();
    auto path_iter = ParseJSONPath("$.a[0]");
    ZETASQL_ASSERT_OK_AND_ASSIGN(bool success, JsonRemove(ref, *path_iter));
    EXPECT_TRUE(success);
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(R"({"a": []})")->GetConstRef()));
  }
}

TEST(JsonInsertArrayTest, NoOp) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true, "b": [1.1, false, []]}, 10])";

  auto test_fn = [&kInitialValue](absl::string_view path) {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    auto path_iter = ParseJSONPath(path);
    ZETASQL_EXPECT_OK(JsonInsertArrayElement(ref, *path_iter, Value::Int64(10),
                                     LanguageOptions(),
                                     /*canonicalize_zero=*/true));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  };

  // Last token is not an array index
  test_fn("$.a");
  // Path doesn't exist
  test_fn("$[1][1].a");
  // Path doesn't exist
  test_fn("$.a[0]");
  // Path doesn't exist
  test_fn("$[2].c[0]");
  // Path doesn't point to an array ($[2] is not an array)
  test_fn("$[2][0]");
  // Path doesn't point to an array ($[2] is not an array)
  test_fn("$[2][0][0]");
  // With strict JSONPath, .0 is a member access and cannot be an array
  // index.
  test_fn("$.a.0");
  // Doesn't recursively create array on null.
  test_fn("$[1][2][0]");
}

TEST(JsonInsertArrayTest, ValidInserts) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true, "b": [1.1, false, []]}, 10])";

  auto test_fn = [&kInitialValue](absl::string_view path, const Value& value,
                                  bool insert_each_element,
                                  absl::string_view expected_output) {
    JSONValue json = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = json.GetRef();
    auto path_iter = ParseJSONPath(path);
    ZETASQL_ASSERT_OK(JsonInsertArrayElement(ref, *path_iter, value, LanguageOptions(),
                                     /*canonicalize_zero=*/true,
                                     insert_each_element));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(expected_output)->GetConstRef()));
  };

  // Inserts into top level array.
  test_fn("$[0]", Value::Int64(1), false,
          R"([1,"foo",null,{"a":true,"b":[1.1,false,[]]},10])");
  // Inserts past the array size.
  test_fn("$[4]", Value::Int64(1), false,
          R"(["foo",null,{"a":true,"b":[1.1,false,[]]},10,1])");
  // Inserts past the array size.
  test_fn("$[7]", Value::Int64(1), false,
          R"(["foo",null,{"a":true,"b":[1.1,false,[]]},10,null,null,null,1])");
  // Inserts into nested array.
  test_fn("$[2].b[1]", Value::String("bar"), false,
          R"(["foo",null,{"a":true,"b":[1.1,"bar",false,[]]},10])");
  // Path points to an array. Does not insert into the array. Inserts
  // before the array.
  test_fn("$[2].b[2]", Value::String("bar"), false,
          R"(["foo",null,{"a":true,"b":[1.1,false,"bar",[]]},10])");
  // Inserts into nested array.
  test_fn("$[2].b[2][0]", Value::String("bar"), false,
          R"(["foo",null,{"a":true,"b":[1.1,false,["bar"]]},10])");
  // Inserts into nested array, past the array size.
  test_fn("$[2].b[2][2]", Value::String("bar"), false,
          R"(["foo",null,{"a":true,"b":[1.1,false,[null,null,"bar"]]},10])");
  // Inserts an array as a single element.
  test_fn("$[2].b[1]", values::StringArray({"a", "b"}), false,
          R"(["foo",null,{"a":true,"b":[1.1,["a","b"],false,[]]},10])");
  // Inserts an array as multiple elements.
  test_fn("$[2].b[1]", values::StringArray({"a", "b"}), true,
          R"(["foo",null,{"a":true,"b":[1.1,"a","b",false,[]]},10])");
  // Inserts an array as multiple elements past the array size.
  test_fn("$[2].b[5]", values::StringArray({"a", "b"}), true,
          R"(["foo",null,{"a":true,"b":[1.1,false,[],null,null,"a","b"]},10])");
  // Inserts into top level array.
  test_fn("$[1]", Value::String("a"), true,
          R"(["foo","a",null,{"a":true,"b":[1.1,false,[]]},10])");
  // Inserts into null.
  test_fn("$[1][0]", Value::String("a"), true,
          R"(["foo",["a"],{"a":true,"b":[1.1,false,[]]},10])");
  // Inserts into null and expands array.
  test_fn("$[1][2]", Value::String("a"), true,
          R"(["foo",[null,null,"a"],{"a":true,"b":[1.1,false,[]]},10])");
  // Inserts 0 element into null. Creates an array.
  test_fn("$[1][2]", values::StringArray(std::vector<std::string>()), true,
          R"(["foo",[null,null,null],{"a":true,"b":[1.1,false,[]]},10])");
  // Inserts multiple elements into null.
  test_fn("$[1][0]", values::Int64Array({1, 5}), true,
          R"(["foo",[1,5],{"a":true,"b":[1.1,false,[]]},10])");
  // Inserts multiple elements into null.
  test_fn("$[1][2]", values::Int64Array({1, 5}), true,
          R"(["foo",[null,null,1,5],{"a":true,"b":[1.1,false,[]]},10])");
}

TEST(JsonInsertArrayTest, FailedConversionComesFirst) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true}, 10])";

  Value big_value = values::BigNumeric(
      BigNumericValue::FromString("1.111111111111111111").value());

  {
    // An error is returned even if the path doesn't exist.
    auto path_iter = ParseJSONPath("$.a");
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    LanguageOptions options;
    options.EnableLanguageFeature(FEATURE_JSON_STRICT_NUMBER_PARSING);

    ASSERT_THAT(JsonInsertArrayElement(ref, *path_iter, big_value, options,
                                       /*canonicalize_zero=*/true),
                StatusIs(absl::StatusCode::kOutOfRange));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  }
  {
    // Insertion in null would have created an array but failed conversion comes
    // first.
    auto path_iter = ParseJSONPath("$[1][1]");
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    LanguageOptions options;
    options.EnableLanguageFeature(FEATURE_JSON_STRICT_NUMBER_PARSING);

    ASSERT_THAT(JsonInsertArrayElement(ref, *path_iter, big_value, options,
                                       /*canonicalize_zero=*/true),
                StatusIs(absl::StatusCode::kOutOfRange));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  }
}

TEST(JsonInsertArrayTest, CanonicalizeZero) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true}, 10])";
  auto path_iter = ParseJSONPath("$[1]");
  {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    ZETASQL_ASSERT_OK(JsonInsertArrayElement(ref, *path_iter, Value::Double(-0.0),
                                     LanguageOptions(),
                                     /*canonicalize_zero=*/true));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(R"(["foo",0.0,null,{"a":true},10])")
                   ->GetConstRef()));
  }

  {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    ZETASQL_ASSERT_OK(JsonInsertArrayElement(ref, *path_iter, Value::Double(-0.0),
                                     LanguageOptions(),
                                     /*canonicalize_zero=*/false));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(R"(["foo",-0.0,null,{"a":true},10])")
                   ->GetConstRef()));
  }
}

TEST(JsonInsertArrayTest, StrictNumberParsing) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true}, 10])";

  Value big_value = values::BigNumeric(
      BigNumericValue::FromString("1.111111111111111111").value());

  auto path_iter = ParseJSONPath("$[1]");
  {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    LanguageOptions options;
    options.EnableLanguageFeature(FEATURE_JSON_STRICT_NUMBER_PARSING);

    ASSERT_THAT(JsonInsertArrayElement(ref, *path_iter, big_value, options,
                                       /*canonicalize_zero=*/true),
                StatusIs(absl::StatusCode::kOutOfRange));
  }

  {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    ZETASQL_ASSERT_OK(JsonInsertArrayElement(ref, *path_iter, big_value,
                                     LanguageOptions(),
                                     /*canonicalize_zero=*/true));
    EXPECT_THAT(ref,
                JsonEq(JSONValue::ParseJSONString(
                           R"(["foo",1.111111111111111111,null,{"a":true},10])")
                           ->GetConstRef()));
  }
}

TEST(JsonInsertArrayTest, MaxArraySizeExceeded) {
  constexpr absl::string_view kInitialValue = R"({"a": null, "b": [10]})";

  {
    auto path_iter =
        ParseJSONPath(absl::Substitute("$$.a[$0]", kJSONMaxArraySize));
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    EXPECT_THAT(JsonInsertArrayElement(ref, *path_iter, Value::String("foo"),
                                       LanguageOptions(),
                                       /*canonicalize_zero=*/true),
                StatusIs(absl::StatusCode::kOutOfRange,
                         HasSubstr("Exceeded maximum array size")));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  }

  {
    auto path_iter =
        ParseJSONPath(absl::Substitute("$$.b[$0]", kJSONMaxArraySize));
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    EXPECT_THAT(JsonInsertArrayElement(ref, *path_iter, Value::String("foo"),
                                       LanguageOptions(),
                                       /*canonicalize_zero=*/true),
                StatusIs(absl::StatusCode::kOutOfRange,
                         HasSubstr("Exceeded maximum array size")));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  }
}

TEST(JsonAppendArrayTest, NoOp) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true, "b": [1.1, false, []]}, 10])";

  auto test_fn = [&kInitialValue](absl::string_view path) {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    auto path_iter = ParseJSONPath(path);
    ZETASQL_EXPECT_OK(JsonAppendArrayElement(ref, *path_iter, Value::Int64(10),
                                     LanguageOptions(),
                                     /*canonicalize_zero=*/true));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  };

  // Path doesn't exist
  test_fn("$.a");
  // Path doesn't exist
  test_fn("$[1][1].a");
  // Path doesn't exist
  test_fn("$[2].c");
  // Path doesn't exist
  test_fn("$[4]");
  // Path doesn't exist
  test_fn("$[2].b[2][0]");
  // Path doesn't point to an array
  test_fn("$[2]");
  // With strict JSONPath); .2 is a member access and cannot be an array
  // index.
  test_fn("$[2].b.2");
  // Doesn't recursively create array on null.
  test_fn("$[1][1]");
}

TEST(JsonAppendArrayTest, ValidInserts) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true, "b": [1.1, false, []]}, 10])";

  auto test_fn = [&kInitialValue](absl::string_view path, const Value& value,
                                  bool insert_each_element,
                                  absl::string_view expected_output) {
    JSONValue json = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = json.GetRef();
    auto path_iter = ParseJSONPath(path);
    ZETASQL_ASSERT_OK(JsonAppendArrayElement(ref, *path_iter, value, LanguageOptions(),
                                     /*canonicalize_zero=*/true,
                                     insert_each_element));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(expected_output)->GetConstRef()));
  };

  // Appends into top level array.
  test_fn("$", Value::Int64(1), false,
          R"(["foo",null,{"a":true,"b":[1.1,false,[]]},10,1])");
  // Appends into nested array.
  test_fn("$[2].b", Value::String("bar"), false,
          R"(["foo",null,{"a":true,"b":[1.1,false,[],"bar"]},10])");
  // Appends into nested array.
  test_fn("$[2].b[2]", Value::String("bar"), false,
          R"(["foo",null,{"a":true,"b":[1.1,false,["bar"]]},10])");
  // Appends an array as a single element.
  test_fn("$[2].b", values::StringArray({"a", "b"}), false,
          R"(["foo",null,{"a":true,"b":[1.1,false,[],["a","b"]]},10])");
  // Appends an array as multiple elements.
  test_fn("$[2].b", values::StringArray({"a", "b"}), true,
          R"(["foo",null,{"a":true,"b":[1.1,false,[],"a","b"]},10])");
  // Appends into null.
  test_fn("$[1]", Value::Int64(1), false,
          R"(["foo",[1],{"a":true,"b":[1.1,false,[]]},10])");
  // Appends 0 element into null. Creates an array.
  test_fn("$[1]", values::Int64Array({}), true,
          R"(["foo",[],{"a":true,"b":[1.1,false,[]]},10])");
  // Appends multiple elements into null.
  test_fn("$[1]", values::Int64Array({1, 2}), true,
          R"(["foo",[1,2],{"a":true,"b":[1.1,false,[]]},10])");
}

TEST(JsonAppendArrayTest, CanonicalizeZero) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true}, 10])";
  auto path_iter = ParseJSONPath("$");
  {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    ZETASQL_ASSERT_OK(JsonAppendArrayElement(ref, *path_iter, Value::Double(-0.0),
                                     LanguageOptions(),
                                     /*canonicalize_zero=*/true));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(R"(["foo",null,{"a":true},10,0.0])")
                   ->GetConstRef()));
  }

  {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    ZETASQL_ASSERT_OK(JsonAppendArrayElement(ref, *path_iter, Value::Double(-0.0),
                                     LanguageOptions(),
                                     /*canonicalize_zero=*/false));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(R"(["foo",null,{"a":true},10,-0.0])")
                   ->GetConstRef()));
  }
}

TEST(JsonAppendArrayTest, StrictNumberParsing) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true}, 10])";
  auto path_iter = ParseJSONPath("$");

  Value big_value = values::BigNumeric(
      BigNumericValue::FromString("1.111111111111111111").value());

  {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    LanguageOptions options;
    options.EnableLanguageFeature(FEATURE_JSON_STRICT_NUMBER_PARSING);

    ASSERT_THAT(JsonAppendArrayElement(ref, *path_iter, big_value, options,
                                       /*canonicalize_zero=*/true),
                StatusIs(absl::StatusCode::kOutOfRange));
  }

  {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    LanguageOptions options;
    ZETASQL_ASSERT_OK(JsonAppendArrayElement(ref, *path_iter, big_value, options,
                                     /*canonicalize_zero=*/true));
    EXPECT_THAT(ref,
                JsonEq(JSONValue::ParseJSONString(
                           R"(["foo",null,{"a":true},10,1.111111111111111111])")
                           ->GetConstRef()));
  }
}

TEST(JsonAppendArrayTest, FailedConversionComesFirst) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true}, 10])";

  Value big_value = values::BigNumeric(
      BigNumericValue::FromString("1.111111111111111111").value());

  {
    // An error is returned even if the path doesn't exist.
    auto path_iter = ParseJSONPath("$.a[1]");
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    LanguageOptions options;
    options.EnableLanguageFeature(FEATURE_JSON_STRICT_NUMBER_PARSING);

    ASSERT_THAT(JsonAppendArrayElement(ref, *path_iter, big_value, options,
                                       /*canonicalize_zero=*/true),
                StatusIs(absl::StatusCode::kOutOfRange));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  }
  {
    // Insertion in null would have created an array but failed conversion comes
    // first.
    auto path_iter = ParseJSONPath("$[1]");
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    LanguageOptions options;
    options.EnableLanguageFeature(FEATURE_JSON_STRICT_NUMBER_PARSING);

    ASSERT_THAT(JsonAppendArrayElement(ref, *path_iter, big_value, options,
                                       /*canonicalize_zero=*/true),
                StatusIs(absl::StatusCode::kOutOfRange));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  }
}

TEST(JsonAppendArrayTest, MaxArraySizeExceeded) {
  constexpr absl::string_view kInitialValue = R"({"a": null, "b": [10]})";

  JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
  JSONValueRef ref = value.GetRef();
  ref.GetMember("b").GetArrayElement(kJSONMaxArraySize - 1);
  auto path_iter = ParseJSONPath("$.b");

  EXPECT_THAT(JsonAppendArrayElement(ref, *path_iter, Value::String("foo"),
                                     LanguageOptions(),
                                     /*canonicalize_zero=*/true),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Exceeded maximum array size")));
  EXPECT_EQ(ref.GetMember("b").GetArraySize(), kJSONMaxArraySize);
}

TEST(JsonSetTest, NoOpTopLevelObject) {
  constexpr absl::string_view kInitialObjectValue =
      R"({"a":null, "b":{}, "c":[], "d":{"e":1}, "f":[2,[],{},[3,4]]})";

  auto test_fn = [&kInitialObjectValue](absl::string_view path) {
    JSONValue value = JSONValue::ParseJSONString(kInitialObjectValue).value();
    JSONValueRef ref = value.GetRef();
    std::unique_ptr<StrictJSONPathIterator> path_iterator = ParseJSONPath(path);
    ZETASQL_EXPECT_OK(JsonSet(ref, *path_iterator, Value::Int64(10), LanguageOptions(),
                      /*canonicalize_zero=*/true));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(kInitialObjectValue)->GetConstRef()));
  };

  // Type mismatch. Path prefix "$" is an object but expected array.
  test_fn("$[0]");
  // Type mismatch. Path prefix "$.b" is a object but expected array.
  test_fn("$.b[0]");
  // Type mismatch. Path prefix "$.c" is an array but expected object.
  test_fn("$.c.d");
  // Type mismatch. Path prefix "$.d.e" is a scalar but expected object.
  test_fn("$.d.e.f");
}

TEST(JsonSetTest, NoOpTopLevelObject2) {
  // Tests when there are suffix tokens left to process and there is a type
  // mismatch.
  constexpr absl::string_view kInitialObjectValue =
      R"({"a":[1],"b":{"c":2},"d":3})";

  auto test_fn = [&kInitialObjectValue](absl::string_view path) {
    std::unique_ptr<StrictJSONPathIterator> path_iterator = ParseJSONPath(path);
    JSONValue json = JSONValue::ParseJSONString(kInitialObjectValue).value();
    JSONValueRef ref = json.GetRef();
    ZETASQL_ASSERT_OK(JsonSet(ref, *path_iterator, Value::Int64(1), LanguageOptions(),
                      /*canonicalize_zero=*/true));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(kInitialObjectValue)->GetConstRef()));
  };

  test_fn("$.a.b.c");
  test_fn("$.b[1]");
  test_fn("$.d.e");
}

TEST(JsonSetTest, NoOpTopLevelArray) {
  constexpr absl::string_view kInitialArrayValue =
      R"(["foo", null, {"a": true, "b": [1.1, [], [2]], "c": {}},
         {"d": {"e": "f"}}])";

  auto test_fn = [&kInitialArrayValue](absl::string_view path) {
    JSONValue value = JSONValue::ParseJSONString(kInitialArrayValue).value();
    JSONValueRef ref = value.GetRef();
    std::unique_ptr<StrictJSONPathIterator> path_iterator = ParseJSONPath(path);
    ZETASQL_EXPECT_OK(JsonSet(ref, *path_iterator, Value::Int64(10), LanguageOptions(),
                      /*canonicalize_zero=*/true));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(kInitialArrayValue)->GetConstRef()));
  };

  // Type mismatch. Path prefix "$" is an array but expected object.
  test_fn("$.a");
  // Type mismatch. Path prefix "$[0]" is a scalar but expected array.
  test_fn("$[0][0]");
  // Type mismatch. Path prefix "$[2].a" is a scalar but expected object.
  test_fn("$[2].a.b");
  // Type mismatch. Path prefix "$[2].b" is an array but expected object.
  test_fn("$[2].b.a");
}

TEST(JsonSetTest, SingleJsonScalarCases) {
  // If output is std::nullopt there should be no change in input. Set
  // operation is a no-op.

  const Value value = Value::Int64(999);
  auto test_fn = [&](absl::string_view path,
                     std::optional<absl::string_view> expected_output) {
    // Test different scalar JSON representations as inputs.
    for (absl::string_view json_string : {"1", "1.1", "false", R"("foo")"}) {
      SCOPED_TRACE(
          absl::Substitute("JsonSet('$0', '$1', 999)", json_string, path));
      JSONValue json = JSONValue::ParseJSONString(json_string).value();
      JSONValueRef ref = json.GetRef();
      std::unique_ptr<StrictJSONPathIterator> path_iterator =
          ParseJSONPath(path);
      ZETASQL_ASSERT_OK(JsonSet(ref, *path_iterator, value, LanguageOptions(),
                        /*canonicalize_zero=*/true));
      if (expected_output.has_value()) {
        EXPECT_THAT(
            ref,
            JsonEq(
                JSONValue::ParseJSONString(*expected_output)->GetConstRef()));
      } else {
        // This is an expected no-op.
        EXPECT_THAT(
            ref,
            JsonEq(
                JSONValue::ParseJSONString(json_string).value().GetConstRef()));
      }
    }
  };

  test_fn(/*path=*/"$", "999");
  test_fn(/*path=*/"$.a", std::nullopt);
  test_fn(/*path=*/"$[0]", std::nullopt);
}

TEST(JsonSetTest, ValidTopLevelEmptyObject) {
  const Value value = Value::Int64(999);
  auto test_fn = [&](absl::string_view path,
                     absl::string_view expected_output) {
    JSONValue json;
    JSONValueRef ref = json.GetRef();
    ref.SetToEmptyObject();
    std::unique_ptr<StrictJSONPathIterator> path_iterator = ParseJSONPath(path);
    ZETASQL_ASSERT_OK(JsonSet(ref, *path_iterator, value, LanguageOptions(),
                      /*canonicalize_zero=*/true));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(expected_output)->GetConstRef()));
  };

  // Empty path.
  test_fn(/*path=*/"$", /*expected_output=*/"999");
  // Insert single key.
  test_fn(/*path=*/"$.a", /*expected_output=*/R"({"a":999})");
  // Recursive creation of multiple object keys.
  test_fn(/*path=*/"$.a.b", /*expected_output=*/R"({"a":{"b":999}})");
  // Recursive creation of multiple object keys and array index.
  test_fn(/*path=*/"$.a.b[2]",
          /*expected_output=*/R"({"a":{"b":[null, null, 999]}})");
  // Recursive creation of multiple object keys and array index.
  test_fn(/*path=*/"$.a.b[2].c",
          /*expected_output=*/R"({"a":{"b":[null, null, {"c":999}]}})");
}

TEST(JsonSetTest, ValidTopLevelEmptyArray) {
  const Value value = Value::Int64(999);
  auto test_fn = [&](absl::string_view path,
                     absl::string_view expected_output) {
    JSONValue json;
    JSONValueRef ref = json.GetRef();
    ref.SetToEmptyArray();
    std::unique_ptr<StrictJSONPathIterator> path_iterator = ParseJSONPath(path);
    ZETASQL_ASSERT_OK(JsonSet(ref, *path_iterator, value, LanguageOptions(),
                      /*canonicalize_zero=*/true));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(expected_output)->GetConstRef()));
  };

  // Empty path.
  test_fn(/*path=*/"$", /*expected_output=*/"999");
  // Set element into first position.
  test_fn(/*path=*/"$[0]", /*expected_output=*/"[999]");
  // Set past the end of the array.
  test_fn(/*path=*/"$[2]", /*expected_output=*/"[null, null, 999]");
  // Recursive creation of nested arrays.
  test_fn(/*path=*/"$[2][1]", /*expected_output=*/"[null, null, [null, 999]]");
  // Recursive creation of nested arrays and objects.
  test_fn(/*path=*/"$[2][1].a",
          /*expected_output=*/R"([null, null, [null, {"a":999}]])");
}

TEST(JsonSetTest, ComplexTests) {
  constexpr absl::string_view kInitialObjectValue =
      R"({"a":null, "b":{}, "c":[], "d":{"e":1}, "f":[2, [], {}, [3, 4]]})";
  const Value value = Value::Int64(999);

  auto test_fn = [&](absl::string_view path,
                     absl::string_view expected_output) {
    std::unique_ptr<StrictJSONPathIterator> path_iterator = ParseJSONPath(path);
    JSONValue json = JSONValue::ParseJSONString(kInitialObjectValue).value();
    JSONValueRef ref = json.GetRef();
    ZETASQL_ASSERT_OK(JsonSet(ref, *path_iterator, value, LanguageOptions(),
                      /*canonicalize_zero=*/true));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(expected_output)->GetConstRef()));
  };

  // Replace the entire value.
  test_fn(/*path=*/"$", /*expected_output=*/"999");
  // Insert object into null.
  test_fn(
      /*path=*/"$.a.b.c",
      /*expected_output=*/R"({"a":{"b":{"c":999}}, "b":{}, "c":[], "d":{"e":1},
                          "f":[2, [], {}, [3, 4]]})");
  // Insert array into null.
  test_fn(
      /*path=*/"$.a[1][2]",
      /*expected_output=*/R"({"a":[null, [null, null, 999]], "b":{}, "c":[],
                          "d":{"e":1}, "f":[2,[],{},[3,4]]})");
  // Set operation ignored. Type mismatch.
  test_fn(/*path=*/"$.d[1]", /*expected_output=*/kInitialObjectValue);
  // Replace an object key.
  test_fn(/*path=*/"$.f", /*expected_output=*/
          R"({"a":null, "b":{}, "c":[], "d":{"e":1}, "f":999})");
  // Insert key into top level object.
  test_fn(
      /*path=*/"$.g",
      /*expected_output=*/R"({"a":null, "b":{}, "c":[], "d":{"e":1},
                          "f":[2,[],{},[3,4]], "g":999})");
  // Inserts key in nested object.
  test_fn(
      /*path=*/"$.b.c",
      /*expected_output=*/R"({"a":null, "b":{"c":999}, "c":[], "d":{"e":1},
                          "f":[2,[],{},[3,4]]})");
  // Inserts key in empty object with basic recursive creation.
  test_fn(
      /*path=*/"$.b.c.d",
      /*expected_output=*/R"({"a":null, "b":{"c":{"d":999}}, "c":[],
                "d":{"e":1}, "f":[2,[],{},[3,4]]})");
  // Inserts into empty array.
  test_fn(
      /*path=*/"$.c[0]",
      /*expected_output=*/R"({"a":null, "b":{}, "c":[999], "d":{"e":1},
               "f":[2,[],{},[3,4]]})");
  // Inserts into empty array with basic recursive creation.
  test_fn(
      /*path=*/"$.c[0][1]",
      /*expected_output=*/R"({"a":null, "b":{}, "c":[[null, 999]],
               "d":{"e":1}, "f":[2,[],{},[3,4]]})");
  // Inserts into empty array with recursive creation of nested arrays
  // and objects.
  test_fn(
      /*path=*/"$.c[0][1].y",
      /*expected_output=*/R"({"a":null, "b":{}, "c":[[null, {"y":999}]],
               "d":{"e":1}, "f":[2,[],{},[3,4]]})");
  // Replaces specific element in an array.
  test_fn(
      /*path=*/"$.f[1]",
      /*expected_output=*/R"({"a":null, "b":{}, "c":[], "d":{"e":1},
               "f":[2, 999, {}, [3, 4]]})");
  // Inserts past end of an array with recursive creation.
  test_fn(
      /*path=*/"$.f[4].x.y[1]",
      /*expected_output=*/
      R"({"a":null, "b":{}, "c":[], "d":{"e":1}, "f":[2, [], {}, [3, 4],
              {"x":{"y":[null, 999]}}]})");
}

TEST(JsonSetTest, CanonicalizeZero) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true}, 10])";
  std::unique_ptr<StrictJSONPathIterator> path_iterator =
      ParseJSONPath(/*path*/ "$[2]");
  {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    ZETASQL_ASSERT_OK(JsonSet(ref, *path_iterator, Value::Double(-0.0),
                      LanguageOptions(),
                      /*canonicalize_zero=*/true));
    EXPECT_THAT(ref,
                JsonEq(JSONValue::ParseJSONString(R"(["foo", null, 0.0, 10])")
                           ->GetConstRef()));
  }
  {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    ZETASQL_ASSERT_OK(JsonSet(ref, *path_iterator, Value::Double(-0.0),
                      LanguageOptions(),
                      /*canonicalize_zero=*/false));
    EXPECT_THAT(ref,
                JsonEq(JSONValue::ParseJSONString(R"(["foo", null, -0.0, 10])")
                           ->GetConstRef()));
  }
}

TEST(JsonSetTest, StrictNumberParsing) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a":true}, 10])";
  std::unique_ptr<StrictJSONPathIterator> path_iterator =
      ParseJSONPath(/*path*/ "$[2]");
  Value big_value = values::BigNumeric(
      BigNumericValue::FromString("1.111111111111111111").value());
  {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    LanguageOptions options;
    options.EnableLanguageFeature(FEATURE_JSON_STRICT_NUMBER_PARSING);

    ASSERT_THAT(JsonSet(ref, *path_iterator, big_value, options,
                        /*canonicalize_zero=*/true),
                StatusIs(absl::StatusCode::kOutOfRange));
  }
  {
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    ZETASQL_ASSERT_OK(JsonSet(ref, *path_iterator, big_value, LanguageOptions(),
                      /*canonicalize_zero=*/true));
    EXPECT_THAT(ref, JsonEq(JSONValue::ParseJSONString(
                                R"(["foo", null, 1.111111111111111111, 10])")
                                ->GetConstRef()));
  }
}

TEST(JsonSetTest, FailedConversionComesFirst) {
  constexpr absl::string_view kInitialValue =
      R"(["foo", null, {"a": true}, 10])";

  Value big_value = values::BigNumeric(
      BigNumericValue::FromString("1.111111111111111111").value());

  {
    // An error is returned even if the path doesn't exist.
    auto path_iter = ParseJSONPath("$.a[1]");
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    LanguageOptions options;
    options.EnableLanguageFeature(FEATURE_JSON_STRICT_NUMBER_PARSING);

    ASSERT_THAT(JsonSet(ref, *path_iter, big_value, options,
                        /*canonicalize_zero=*/true),
                StatusIs(absl::StatusCode::kOutOfRange));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  }
  {
    // Auto-creation only happens when the conversion succeeds.
    auto path_iter = ParseJSONPath("$[2].b[0]");
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    LanguageOptions options;
    options.EnableLanguageFeature(FEATURE_JSON_STRICT_NUMBER_PARSING);

    ASSERT_THAT(JsonAppendArrayElement(ref, *path_iter, big_value, options,
                                       /*canonicalize_zero=*/true),
                StatusIs(absl::StatusCode::kOutOfRange));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  }
}

TEST(JsonSetTest, MaxArraySizeExceeded) {
  constexpr absl::string_view kInitialValue = R"({"a": [10]})";

  {
    auto path_iter =
        ParseJSONPath(absl::Substitute("$$.a[$0]", kJSONMaxArraySize));
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    EXPECT_THAT(
        JsonSet(ref, *path_iter, Value::String("foo"), LanguageOptions(),
                /*canonicalize_zero=*/true),
        StatusIs(absl::StatusCode::kOutOfRange,
                 HasSubstr("Exceeded maximum array size")));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  }
  {
    auto path_iter =
        ParseJSONPath(absl::Substitute("$$.b[$0]", kJSONMaxArraySize));
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();

    EXPECT_THAT(
        JsonSet(ref, *path_iter, Value::String("foo"), LanguageOptions(),
                /*canonicalize_zero=*/true),
        StatusIs(absl::StatusCode::kOutOfRange,
                 HasSubstr("Exceeded maximum array size")));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  }
}

TEST(JsonStripNullsTest, NoopPathNonexistent) {
  constexpr absl::string_view kInitialValue =
      R"({"a":1, "b":[null, {"c":null}], "d":{"e":[null], "f":[null]}})";

  auto test_fn = [&kInitialValue](absl::string_view path) {
    std::unique_ptr<StrictJSONPathIterator> path_iterator = ParseJSONPath(path);
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    ZETASQL_ASSERT_OK(JsonStripNulls(ref, *path_iterator,
                             /*include_arrays=*/true,
                             /*remove_empty=*/true));
    ZETASQL_ASSERT_OK(JsonStripNulls(ref, *path_iterator, /*include_arrays=*/true,
                             /*remove_empty=*/true));
    EXPECT_THAT(
        ref, JsonEq(JSONValue::ParseJSONString(kInitialValue)->GetConstRef()));
  };

  // Path suffix ".b" doesn't exist.
  test_fn("$.a.b");
  // Path suffix "[2]" is larger than existing array.
  test_fn("$.b[2]");
  // Type mismatch. Path prefix "$.b" is an array but expected object.
  test_fn("$.b.c");
  // Type mismatch. Path prefix "$.d" is an object but expected array.
  test_fn("$.d[1]");
  // Object "$.d" doesn't contain key "$.z".
  test_fn("$.d.z");
}

TEST(JsonStripNullsTest, SimpleObject) {
  constexpr absl::string_view kInitialValue =
      R"({"a":null, "b":1, "c":[null, true], "d":{}, "e":[null], "f":[]})";

  auto test_fn = [&kInitialValue](absl::string_view path, bool include_arrays,
                                  bool remove_empty,
                                  absl::string_view expected_output) {
    std::unique_ptr<StrictJSONPathIterator> path_iterator = ParseJSONPath(path);
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    ZETASQL_ASSERT_OK(JsonStripNulls(ref, *path_iterator,
                             /*include_arrays=*/include_arrays,
                             /*remove_empty=*/remove_empty));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(expected_output)->GetConstRef()));
  };

  test_fn("$", /*include_arrays=*/false, /*remove_empty=*/false,
          R"({"b":1, "c":[null, true], "d":{}, "e":[null], "f":[]})");
  test_fn("$", /*include_arrays=*/true, /*remove_empty=*/false,
          R"({"b":1, "c":[true], "d":{}, "e":[], "f":[]})");
  test_fn("$", /*include_arrays=*/false, /*remove_empty=*/true,
          R"({"b":1, "c":[null, true], "e":[null], "f":[]})");
  test_fn("$", /*include_arrays=*/true, /*remove_empty=*/true,
          R"({"b":1, "c":[true]})");
  // Subpath points to a simple type. Does nothing.
  test_fn("$.a", /*include_arrays=*/true, /*remove_empty=*/true, kInitialValue);
  test_fn("$.c", /*include_arrays=*/true, /*remove_empty=*/true,
          R"({"a":null, "b":1, "c":[true], "d":{}, "e":[null],
               "f":[]})");
}

TEST(JsonStripNullsTest, SimpleArray) {
  constexpr absl::string_view kInitialValue =
      R"(["a", null, 1.1, [], [null], [1, null], {}, {"a":null},
         {"b":1, "c":null}])";

  auto test_fn = [&kInitialValue](absl::string_view path, bool include_arrays,
                                  bool remove_empty,
                                  absl::string_view expected_output) {
    std::unique_ptr<StrictJSONPathIterator> path_iterator = ParseJSONPath(path);
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    ZETASQL_ASSERT_OK(JsonStripNulls(ref, *path_iterator,
                             /*include_arrays=*/include_arrays,
                             /*remove_empty=*/remove_empty));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(expected_output)->GetConstRef()));
  };

  test_fn("$", /*include_arrays=*/false, /*remove_empty=*/false,
          R"(["a", null, 1.1, [], [null], [1, null], {}, {},
                      {"b":1}])");
  test_fn("$", /*include_arrays=*/true, /*remove_empty=*/false,
          R"(["a", 1.1, [], [], [1], {}, {}, {"b":1}])");
  // Because parent of empty OBJECTs is an ARRAY, empty OBJECTs are not
  // removed.
  test_fn("$", /*include_arrays=*/false, /*remove_empty=*/true,
          R"(["a", null, 1.1, [], [null], [1, null], {}, {}, {"b":1}])");
  test_fn("$", /*include_arrays=*/true, /*remove_empty=*/true,
          R"(["a", 1.1, [1], {"b":1}])");
  // Subpath points to an array that is replaced with JSON 'null'.
  test_fn("$[4]", /*include_arrays=*/true, /*remove_empty=*/true,
          R"(["a", null, 1.1, [], null, [1, null], {}, {"a":null},
              {"b":1, "c":null}])");
  // Subpath points to an OBJECT that is replaced with JSON 'null'.
  test_fn("$[7]", /*include_arrays=*/true, /*remove_empty=*/true,
          R"(["a", null, 1.1, [], [null], [1, null], {}, null,
              {"b":1, "c":null}])");
}

TEST(JsonStripNullsTest, AllNullsOrEmptyObject) {
  constexpr absl::string_view kInitialValue =
      R"({"a": {"b":null, "c":null, "d":[[null], null]}, "e":null})";

  auto test_fn = [&kInitialValue](absl::string_view path, bool include_arrays,
                                  bool remove_empty,
                                  absl::string_view expected_output) {
    std::unique_ptr<StrictJSONPathIterator> path_iterator = ParseJSONPath(path);
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    ZETASQL_ASSERT_OK(JsonStripNulls(ref, *path_iterator,
                             /*include_arrays=*/include_arrays,
                             /*remove_empty=*/remove_empty));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(expected_output)->GetConstRef()));
  };

  test_fn("$", /*include_arrays=*/true, /*remove_empty=*/true, "null");
  // No change. Subpath already JSON 'null'.
  test_fn("$.e", /*include_arrays=*/true, /*remove_empty=*/true, kInitialValue);
  // No change. Subpath points to a nested ARRAY.
  test_fn("$.a.d", /*include_arrays=*/false, /*remove_empty=*/true,
          kInitialValue);
  // Subpath is nested ARRAY and removes JSON 'null's.
  test_fn("$.a.d", /*include_arrays=*/true, /*remove_empty=*/false,
          R"({"a": {"b":null, "c":null, "d":[[]]}, "e":null})");
  // Subpath is nested ARRAY replaced by JSON 'null'.
  test_fn("$.a.d", /*include_arrays=*/true, /*remove_empty=*/true,
          R"({"a": {"b":null, "c":null, "d":null}, "e":null})");
  // Subpath is OBJECT replaced by JSON 'null'.
  test_fn("$.a", /*include_arrays=*/true, /*remove_empty=*/true,
          R"({"a":null, "e":null})");
}

TEST(JsonStripNullsTest, AllNullsOrEmptyArray) {
  constexpr absl::string_view kInitialValue =
      R"([null, {"b":null, "c":null, "d":[[null], null]}, [null, null],
      []])";

  auto test_fn = [&kInitialValue](absl::string_view path, bool include_arrays,
                                  bool remove_empty,
                                  absl::string_view expected_output) {
    std::unique_ptr<StrictJSONPathIterator> path_iterator = ParseJSONPath(path);
    JSONValue value = JSONValue::ParseJSONString(kInitialValue).value();
    JSONValueRef ref = value.GetRef();
    ZETASQL_ASSERT_OK(JsonStripNulls(ref, *path_iterator,
                             /*include_arrays=*/include_arrays,
                             /*remove_empty=*/remove_empty));
    EXPECT_THAT(
        ref,
        JsonEq(JSONValue::ParseJSONString(expected_output)->GetConstRef()));
  };

  test_fn("$", /*include_arrays=*/true, /*remove_empty=*/true, "null");
  // Cleanup nested arrays to JSON 'null'.
  test_fn("$[1]", /*include_arrays=*/true, /*remove_empty=*/true,
          "[null, null, [null, null],[]]");
  // Cleanup nested arrays to JSON 'null' but no array cleanup.
  test_fn("$[1]", /*include_arrays=*/false, /*remove_empty=*/true,
          R"([null, {"d":[[null], null]}, [null, null], []])");
  // No change. Subpath points to a nested ARRAY.
  test_fn("$[1].d", /*include_arrays=*/false, /*remove_empty=*/true,
          kInitialValue);
}

}  // namespace

}  // namespace json_internal
}  // namespace functions
}  // namespace zetasql
