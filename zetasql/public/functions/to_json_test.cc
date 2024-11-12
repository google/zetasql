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

// Tests for TO_JSON functions.
#include "zetasql/public/functions/to_json.h"

#include <algorithm>
#include <map>
#include <utility>
#include <vector>

#include "zetasql/common/float_margin.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/functions/unsupported_fields.pb.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/types/value_equality_check_options.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/test_value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/map_util.h"

namespace zetasql {
namespace functions {
namespace {

constexpr UnsupportedFieldsEnum::UnsupportedFields kUnsupportFieldsDefault =
    UnsupportedFieldsEnum::FAIL;
constexpr absl::StatusCode kUnimplemented = absl::StatusCode::kUnimplemented;

TEST(ToJsonTest, Compliance) {
  const std::vector<FunctionTestCall> tests = GetFunctionTestsToJson();
  const QueryParamsWithResult::FeatureSet default_feature_set = {
      FEATURE_V_1_2_CIVIL_TIME, FEATURE_TIMESTAMP_NANOS};

  for (const FunctionTestCall& test : tests) {
    if (std::any_of(test.params.params().begin(), test.params.params().end(),
                    [](const Value& param) { return param.is_null(); })) {
      continue;
    }
    const Value& input_value = test.params.param(0);
    const bool stringify_wide_numbers = test.params.params().size() == 2
                                            ? test.params.param(1).bool_value()
                                            : false;
    SCOPED_TRACE(absl::Substitute("$0('$1', '$2')", test.function_name,
                                  input_value.ShortDebugString(),
                                  stringify_wide_numbers));
    zetasql::LanguageOptions language_options;
    if (test.params.results().size() == 1 &&
        zetasql_base::ContainsKey(test.params.results().begin()->first,
                         FEATURE_JSON_STRICT_NUMBER_PARSING)) {
      language_options.EnableLanguageFeature(
          FEATURE_JSON_STRICT_NUMBER_PARSING);
    }
    absl::StatusOr<JSONValue> output =
        ToJson(input_value, stringify_wide_numbers, language_options,
               /*canonicalize_zero=*/true);

    // If the test is conditioned on civil time with nanos, use that result.
    // Otherwise just use the default result.
    const QueryParamsWithResult::Result* result =
        zetasql_base::FindOrNull(test.params.results(), default_feature_set);
    const Value expected_result_value =
        result == nullptr ? test.params.results().begin()->second.result
                          : result->result;
    const absl::Status expected_status =
        result == nullptr ? test.params.results().begin()->second.status
                          : result->status;
    FloatMargin margin =
        result == nullptr ? test.params.results().begin()->second.float_margin
                          : result->float_margin;

    if (expected_status.ok()) {
      if (margin.IsExactEquality()) {
        EXPECT_EQ(expected_result_value,
                  values::Json(std::move(output.value())));
      } else {
        // For float and double, allow margin comparison.
        InternalValue::Equals(
            expected_result_value, values::Json(std::move(output.value())),
            ValueEqualityCheckOptions{.float_margin = margin});
      }
    } else {
      EXPECT_EQ(output.status().code(), expected_status.code());
    }
  }
}

TEST(ToJsonTest, LegacyCanonicalizeZeroDouble) {
  zetasql::LanguageOptions language_options;

  absl::StatusOr<JSONValue> output = ToJson(
      Value::Double(-0.0), /*stringify_wide_numbers=*/false, language_options,
      /*canonicalize_zero=*/false, kUnsupportFieldsDefault);
  ZETASQL_ASSERT_OK(output);
  EXPECT_EQ(values::Json(std::move(output.value())),
            values::Json(JSONValue(-0.0)));
}

TEST(ToJsonTest, LegacyCanonicalizeZeroFloat) {
  zetasql::LanguageOptions language_options;
  absl::StatusOr<JSONValue> output = ToJson(
      Value::Float(-0.0f), /*stringify_wide_numbers=*/false, language_options,
      /*canonicalize_zero=*/false, kUnsupportFieldsDefault);
  ZETASQL_ASSERT_OK(output);
  EXPECT_EQ(values::Json(std::move(output.value())),
            values::Json(JSONValue(-0.0)));
}

TEST(ToJsonTest, GraphNode) {
  zetasql::LanguageOptions language_options;
  const Value p0_value = Value::String("v0");
  const Value p1_value = Value::Int32(1);
  const Value node = test_values::GraphNode(
      {"graph_name"}, "id", {{"P0", p0_value}, {"p1", p1_value}},
      {"label_2", "label_1"}, "ElementTable");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      JSONValue output,
      ToJson(node, /*stringify_wide_numbers=*/false, language_options,
             /*canonicalize_zero=*/false, kUnsupportFieldsDefault));

  ZETASQL_ASSERT_OK_AND_ASSIGN(JSONValue expectation,
                       JSONValue::ParseJSONString(absl::Substitute(
                           R"json({
    "identifier": "$0",
    "kind": "node",
    "labels": ["label_1", "label_2"],
    "properties": {
      "P0": "v0",
      "p1": 1
    }
  })json",
                           absl::Base64Escape("id"))));
  EXPECT_EQ(values::Json(std::move(output)),
            values::Json(std::move(expectation)));
}

TEST(ToJsonTest, GraphEdge) {
  LanguageOptions language_options;
  const Value p0_value = Value::String("v0");
  const Value p1_value = Value::Int32(1);
  const Value edge = test_values::GraphEdge(
      {"graph_name"}, "id", {{"P0", p0_value}, {"p1", p1_value}},
      {"label_2", "label_1"}, "ElementTable", "src_node_id", "dst_node_id");
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      JSONValue output,
      ToJson(edge, /*stringify_wide_numbers=*/false, language_options,
             /*canonicalize_zero=*/false, kUnsupportFieldsDefault));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      JSONValue expectation,
      JSONValue::ParseJSONString(absl::Substitute(
          R"json({
    "identifier": "$0",
    "kind": "edge",
    "labels": ["label_1", "label_2"],
    "properties": {
      "P0": "v0",
      "p1": 1
    },
    "source_node_identifier": "$1",
    "destination_node_identifier": "$2"
  })json",
          absl::Base64Escape("id"), absl::Base64Escape("src_node_id"),
          absl::Base64Escape("dst_node_id"))));
  EXPECT_EQ(values::Json(std::move(output)),
            values::Json(std::move(expectation)));
}

TEST(ToJsonTest, GraphPath) {
  LanguageOptions language_options;
  const Value p0_value = Value::String("v0");
  const Value p1_value = Value::Int32(1);
  const Value node1 = test_values::GraphNode(
      {"graph_name"}, "src_node_id", {{"P0", p0_value}, {"p1", p1_value}},
      {"label_2", "label_1"}, "ElementTable");
  const Value edge = test_values::GraphEdge(
      {"graph_name"}, "id", {{"P0", p0_value}, {"p1", p1_value}},
      {"label_2", "label_1"}, "ElementTable", "src_node_id", "dst_node_id");
  const Value node2 = test_values::GraphNode(
      {"graph_name"}, "dst_node_id", {{"P0", p0_value}, {"p1", p1_value}},
      {"label_2", "label_1"}, "ElementTable");

  ZETASQL_ASSERT_OK_AND_ASSIGN(const Value path,
                       Value::MakeGraphPath(test_values::MakeGraphPathType(
                                                node1.type()->AsGraphElement(),
                                                edge.type()->AsGraphElement()),
                                            {node1, edge, node2}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      JSONValue output,
      ToJson(path, /*stringify_wide_numbers=*/false, language_options,
             /*canonicalize_zero=*/false, kUnsupportFieldsDefault));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      JSONValue expectation,
      JSONValue::ParseJSONString(absl::Substitute(
          R"json([
    {
      "identifier": "$0",
      "kind": "node",
      "labels": ["label_1", "label_2"],
      "properties": {
        "P0": "v0",
        "p1": 1
      }
    },
    {
      "identifier": "$1",
      "kind": "edge",
      "labels": ["label_1", "label_2"],
      "properties": {
        "P0": "v0",
        "p1": 1
      },
      "source_node_identifier": "$2",
      "destination_node_identifier": "$3"
    },
    {
      "identifier": "$4",
      "kind": "node",
      "labels": ["label_1", "label_2"],
      "properties": {
        "P0": "v0",
        "p1": 1
      }
    }
  ])json",
          absl::Base64Escape("src_node_id"), absl::Base64Escape("id"),
          absl::Base64Escape("src_node_id"), absl::Base64Escape("dst_node_id"),
          absl::Base64Escape("dst_node_id"))));

  EXPECT_EQ(values::Json(std::move(output)),
            values::Json(std::move(expectation)));
}

}  // namespace
}  // namespace functions
}  // namespace zetasql
