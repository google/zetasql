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

#include <cctype>
#include <cmath>
#include <cstdint>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/testing/proto_matchers.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/functions/json_internal.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
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
  const std::string json = R"({"a": {"b": [ { "c" : "foo" } ] } })";
  const std::vector<std::pair<std::string, std::string>> inputs_and_outputs = {
      {"$", R"({"a":{"b":[{"c":"foo"}]}})"},
      {"$.a", R"({"b":[{"c":"foo"}]})"},
      {"$.a.b", R"([{"c":"foo"}])"},
      {"$.a.b[0]", R"({"c":"foo"})"},
      {"$.a.b[0].c", R"("foo")"}};
  for (const auto& input_and_output : inputs_and_outputs) {
    SCOPED_TRACE(absl::Substitute("JSON_EXTRACT('$0', '$1')", json,
                                  input_and_output.first));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(input_and_output.first,
                                  /*sql_standard_mode=*/false));
    std::string value;
    bool is_null;
    ZETASQL_ASSERT_OK(evaluator->Extract(json, &value, &is_null));
    EXPECT_EQ(input_and_output.second, value);
    EXPECT_FALSE(is_null);
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(const std::unique_ptr<JsonPathEvaluator> evaluator,
                       JsonPathEvaluator::Create(input,
                                                 /*sql_standard_mode=*/false));
  evaluator->set_escaping_needed_callback(
      [&](absl::string_view str) { callback.Call(str); });
  EXPECT_CALL(callback, Call("\t"));
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
        JsonPathEvaluator::Create(input,
                                  /*sql_standard_mode=*/false));

    absl::optional<JSONValueConstRef> result = evaluator->Extract(json_ref);
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
        JsonPathEvaluator::Create(input_and_output.first,
                                  /*sql_standard_mode=*/false));
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
      {"$", ""},
      {"$.a", ""},
      {"$.a.d", "1"},
      {"$.a.e", "-5"},
      {"$.a.f", "true"},
      {"$.a.g", "4.2"},
      {"$.a.b", ""},
      {"$.a.b[0]", ""},
      {"$.a.b[0].c", "foo"}};
  for (const auto& [input, output] : inputs_and_outputs) {
    SCOPED_TRACE(absl::Substitute("JSON_EXTRACT_SCALAR('$0', '$1')",
                                  json_ref.ToString(), input));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(input,
                                  /*sql_standard_mode=*/false));

    absl::optional<std::string> result = evaluator->ExtractScalar(json_ref);
    if (!output.empty()) {
      ASSERT_TRUE(result.has_value());
      EXPECT_EQ(output, result.value());
    } else {
      EXPECT_FALSE(result.has_value());
    }
  }
}

TEST(JsonTest, NativeJsonExtractJsonArray) {
  auto json_value =
      JSONValue::ParseJSONString(
          R"({"a": {"b": [ { "c" : "foo" }, 15, null, "bar", )"
          R"([ 20, { "a": "baz" } ] ] } })");
  ZETASQL_ASSERT_OK(json_value.status());
  JSONValueConstRef json_ref = json_value->GetConstRef();

  const std::vector<
      std::pair<std::string, absl::optional<std::vector<std::string>>>>
      inputs_and_outputs = {{"$", absl::nullopt},
                            {"$.a", absl::nullopt},
                            {"$.a.b",
                             {{R"({"c":"foo"})", "15", "null", "\"bar\"",
                               R"([20,{"a":"baz"}])"}}},
                            {"$.a.b[0]", absl::nullopt},
                            {"$.a.b[4]", {{"20", R"({"a":"baz"})"}}}};
  for (const auto& [input, output] : inputs_and_outputs) {
    SCOPED_TRACE(absl::Substitute("JSON_EXTRACT_ARRAY('$0', '$1')",
                                  json_ref.ToString(), input));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(input,
                                  /*sql_standard_mode=*/false));

    absl::optional<std::vector<JSONValueConstRef>> result =
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
  auto json_value =
      JSONValue::ParseJSONString(
          R"({"a": {"b": [ { "c" : "foo" }, 15, null, "bar", )"
          R"([ 20, "a", true ] ] } })");
  ZETASQL_ASSERT_OK(json_value.status());
  JSONValueConstRef json_ref = json_value->GetConstRef();
  const std::vector<
      std::pair<std::string, absl::optional<std::vector<std::string>>>>
      inputs_and_outputs = {{"$", absl::nullopt},
                            {"$.a", absl::nullopt},
                            {"$.a.b", absl::nullopt},
                            {"$.a.b[0]", absl::nullopt},
                            {"$.a.b[4]", {{"20", "a", "true"}}}};
  for (const auto& [input, output] : inputs_and_outputs) {
    SCOPED_TRACE(absl::Substitute("JSON_EXTRACT_STRING_ARRAY('$0', '$1')",
                                  json_ref.ToString(), input));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const std::unique_ptr<JsonPathEvaluator> evaluator,
        JsonPathEvaluator::Create(input,
                                  /*sql_standard_mode=*/false));
    absl::optional<std::vector<absl::optional<std::string>>> result =
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
      JsonPathEvaluator::Create(path, /*sql_standard_mode=*/true));
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
      auto evaluator_status =
          JsonPathEvaluator::Create(json_path, sql_standard_mode);
      if (evaluator_status.ok()) {
        const std::unique_ptr<JsonPathEvaluator>& evaluator =
            evaluator_status.value();
        evaluator->enable_special_character_escaping();
        if (test.function_name == "json_extract" ||
            test.function_name == "json_query") {
          status = evaluator->Extract(json, &value, &is_null);
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
      auto evaluator_status =
          JsonPathEvaluator::Create(json_path, sql_standard_mode);
      if (evaluator_status.ok()) {
        const std::unique_ptr<JsonPathEvaluator>& evaluator =
            evaluator_status.value();
        if (test.function_name == "json_extract" ||
            test.function_name == "json_query") {
          absl::optional<JSONValueConstRef> result_or =
              evaluator->Extract(json);
          EXPECT_EQ(test.params.result().is_null(), !result_or.has_value());
          if (!test.params.result().is_null() && result_or.has_value()) {
            EXPECT_THAT(result_or.value(),
                        JsonEq(test.params.result().json_value()));
          }
        } else {
          absl::optional<std::string> result_or =
              evaluator->ExtractScalar(json);
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
      const JSONValueConstRef json =
          test.params.param(0).json_value();
      const std::string json_path = test.params.param(1).string_value();
      SCOPED_TRACE(absl::Substitute("$0('$1', '$2')", test.function_name,
                                    json.ToString(), json_path));

      absl::Status status;
      bool sql_standard_mode = test.function_name == "json_query_array" ||
                               test.function_name == "json_value_array";
      auto evaluator_status =
          JsonPathEvaluator::Create(json_path, sql_standard_mode);
      if (evaluator_status.ok()) {
        std::unique_ptr<JsonPathEvaluator> evaluator =
            std::move(evaluator_status).value();
        if (test.function_name == "json_extract_array" ||
            test.function_name == "json_query_array")  {
          absl::optional<std::vector<JSONValueConstRef>> result =
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
          absl::optional<std::vector<absl::optional<std::string>>> result =
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
        JsonPathEvaluator::Create(input_and_output.first,
                                  /*sql_standard_mode=*/false));
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

    EXPECT_THAT(JsonPathEvaluator::Create(input_and_output.first,
                                          /*sql_standard_mode=*/true),
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
  ValidJSONPathIterator& itr = *(iptr.get());

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
  ValidJSONPathIterator& itr = *(iptr.get());

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
  ValidJSONPathIterator& itr = *(iptr.get());
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
  ValidJSONPathIterator& itr = *(iptr.get());
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
  ValidJSONPathIterator& itr = *(iptr.get());
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
  std::vector<absl::optional<std::string>> scalar_gold;

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_TRUE(parser.StoppedOnFirstMatch());
  EXPECT_EQ(result, gold);
  EXPECT_TRUE(is_null);

  std::vector<absl::optional<std::string>> scalar_result;
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
  std::vector<absl::optional<std::string>> scalar_gold;

  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_TRUE(parser.StoppedOnFirstMatch());
  EXPECT_FALSE(is_null);
  EXPECT_EQ(result, gold);

  std::vector<absl::optional<std::string>> scalar_result;
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
  std::vector<absl::optional<std::string>> scalar_gold;

  // Parsing of JSON was successful however no match.
  EXPECT_TRUE(parser.ExtractArray(&result, &is_null));
  EXPECT_FALSE(parser.StoppedOnFirstMatch());
  EXPECT_TRUE(is_null);
  EXPECT_EQ(result, gold);

  std::vector<absl::optional<std::string>> scalar_result;
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

  std::vector<absl::optional<std::string>> scalar_result;
  std::vector<absl::optional<std::string>> scalar_gold = {"l1"};
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

  std::vector<absl::optional<std::string>> scalar_result;
  std::vector<absl::optional<std::string>> scalar_gold = {"j", "k"};
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

  std::vector<absl::optional<std::string>> scalar_result;
  std::vector<absl::optional<std::string>> scalar_gold = {"j", "k"};
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

  std::vector<absl::optional<std::string>> scalar_result;
  std::vector<absl::optional<std::string>> scalar_gold = {"j", "k"};
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

  std::vector<absl::optional<std::string>> scalar_result;
  std::vector<absl::optional<std::string>> scalar_gold({"3", "4"});
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

  std::vector<absl::optional<std::string>> scalar_result;
  std::vector<absl::optional<std::string>> scalar_gold = {"3", "4"};
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
  std::vector<absl::optional<std::string>> scalar_gold1;
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

  std::vector<absl::optional<std::string>> scalar_result;
  std::vector<absl::optional<std::string>> scalar_gold = {"1", "2", "3"};
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

  std::vector<absl::optional<std::string>> result;
  std::vector<absl::optional<std::string>> gold = {"a", "1", "2"};
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

  std::vector<absl::optional<std::string>> result;
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
  std::vector<absl::optional<std::string>> gold = {"d1", "tes\"t", "1.9834",
                                                   absl::nullopt, "123"};
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
  ValidJSONPathIterator& itr = *(iptr.get());
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
  ValidJSONPathIterator& itr = *(iptr.get());

  EXPECT_FALSE(itr.End());
  EXPECT_EQ(*itr, "");

  path = "$";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ValidJSONPathIterator> iptr1,
      ValidJSONPathIterator::Create(path, /*sql_standard_mode=*/true));
  ValidJSONPathIterator& itr1 = *(iptr1.get());

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

void ExtractArrayOrStringArray(JSONPathArrayExtractor* parser,
                               std::vector<absl::optional<std::string>>* output,
                               bool* is_null) {
  parser->set_special_character_escaping(true);
  std::vector<std::string> result;
  parser->ExtractArray(&result, is_null);
  output->assign(result.begin(), result.end());
}

void ExtractArrayOrStringArray(JSONPathStringArrayExtractor* parser,
                               std::vector<absl::optional<std::string>>* output,
                               bool* is_null) {
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
    const Value& expected_result = test.params.results().begin()->second.result;

    std::vector<absl::optional<std::string>> output;
    std::vector<Value> result_array;
    absl::Status status;
    bool is_null = true;
    auto evaluator_status =
        ValidJSONPathIterator::Create(json_path, sql_standard_mode);
    if (evaluator_status.ok()) {
      const std::unique_ptr<ValidJSONPathIterator>& path_itr =
          evaluator_status.value();
      ParserClass parser(json, path_itr.get());
      ExtractArrayOrStringArray(&parser, &output, &is_null);
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
  std::vector<absl::optional<std::string>> scalar_array_value;
  absl::Status status;
  bool is_null = true;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JsonPathEvaluator> path_evaluator,
      JsonPathEvaluator::Create("$", /*sql_standard_mode=*/true));
  // Extracting should succeed, but the result is null since the arrays are not
  // closed.
  ZETASQL_EXPECT_OK(path_evaluator->Extract(nested_array_json, &value, &is_null));
  EXPECT_TRUE(is_null);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      path_evaluator,
      JsonPathEvaluator::Create("$", /*sql_standard_mode=*/true));
  // Extracting should succeed, but the result is null since the arrays are not
  // closed.
  ZETASQL_EXPECT_OK(path_evaluator->ExtractScalar(nested_array_json, &value, &is_null));
  EXPECT_TRUE(is_null);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      path_evaluator,
      JsonPathEvaluator::Create("$", /*sql_standard_mode=*/false));
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
      JsonPathEvaluator::Create(json_path, /*sql_standard_mode=*/true));
  std::string value;
  std::vector<std::string> array_value;
  std::vector<absl::optional<std::string>> scalar_array_value;
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
      path_evaluator,
      JsonPathEvaluator::Create(json_path, /*sql_standard_mode=*/false));
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
  std::vector<absl::optional<std::string>> scalar_array_value;
  absl::Status status;
  bool is_null = true;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JsonPathEvaluator> path_evaluator,
      JsonPathEvaluator::Create("$", /*sql_standard_mode=*/true));
  // Extracting should succeed, but the result is null since the objects are not
  // closed.
  ZETASQL_EXPECT_OK(path_evaluator->Extract(nested_object_json, &value, &is_null));
  EXPECT_TRUE(is_null);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      path_evaluator,
      JsonPathEvaluator::Create("$", /*sql_standard_mode=*/true));
  // Extracting should succeed, but the result is null since the objects are not
  // closed.
  ZETASQL_EXPECT_OK(
      path_evaluator->ExtractScalar(nested_object_json, &value, &is_null));
  EXPECT_TRUE(is_null);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      path_evaluator,
      JsonPathEvaluator::Create("$", /*sql_standard_mode=*/false));
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
      JsonPathEvaluator::Create(json_path, /*sql_standard_mode=*/true));

  std::string value;
  std::vector<std::string> array_value;
  std::vector<absl::optional<std::string>> scalar_array_value;
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
      path_evaluator,
      JsonPathEvaluator::Create(json_path, /*sql_standard_mode=*/false));
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
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::string{"test"}), "test");
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::string{"abc123"}), "abc123");
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::string{"TesT"}), "TesT");
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::string{"1"}), "1");
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::string{""}), "");
  inputs_and_expected_outputs.emplace_back(
      JSONValue(std::string{"12¿©?Æ"}), "12¿©?Æ");
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

    absl::StatusOr<std::string> output = ConvertJsonToString(
        input.GetConstRef());
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

  for (const auto& [input, expected_output] :
       inputs_and_expected_outputs) {
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

}  // namespace

}  // namespace json_internal
}  // namespace functions
}  // namespace zetasql
