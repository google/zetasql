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

#include "zetasql/public/functions/regexp.h"

#include <cstdint>
#include <utility>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {
namespace {

std::string StringValue(const Value& value) {
  if (value.type_kind() == TYPE_STRING) {
    return value.string_value();
  } else {
    return value.bytes_value();
  }
}

typedef testing::TestWithParam<FunctionTestCall> RegexpTemplateTest;
TEST_P(RegexpTemplateTest, TestLib) {
  const FunctionTestCall& param = GetParam();
  const std::string& function = param.function_name;
  const std::vector<Value>& args = param.params.params();
  const bool expected_ok = param.params.status().ok();
  for (const Value& arg : args) {
    // Ignore tests with null arguments.
    if (arg.is_null()) return;
  }
  absl::StatusOr<std::unique_ptr<const RegExp>> re;
  RegExp::PositionUnit position_unit;
  if (args[1].type_kind() == TYPE_STRING) {
    re = MakeRegExpUtf8(args[1].string_value());
    position_unit = RegExp::kUtf8Chars;
  } else {
    re = MakeRegExpBytes(args[1].bytes_value());
    position_unit = RegExp::kBytes;
  }
  if (!re.ok()) {
    ASSERT_FALSE(expected_ok);
    ASSERT_EQ(param.params.status().code(), re.status().code());
    return;
  }
  absl::Status status;
  if (function == "regexp_contains") {
    bool out;
    bool ok = (*re)->Contains(StringValue(args[0]), &out, &status);
    ASSERT_EQ(expected_ok, ok);
    ASSERT_EQ(param.params.status().code(), status.code());
    if (ok) {
      EXPECT_EQ(param.params.result().bool_value(), out);
    }
  } else if (function == "regexp_match") {
    bool out;
    bool ok = (*re)->Match(StringValue(args[0]), &out, &status);
    ASSERT_EQ(expected_ok, ok);
    ASSERT_EQ(param.params.status().code(), status.code());
    if (ok) {
      EXPECT_EQ(param.params.result().bool_value(), out);
    }
  } else if (function == "regexp_extract" || function == "regexp_substr") {
    absl::string_view out;
    bool is_null;
    std::string in = StringValue(args[0]);
    int64_t position = 1;
    int64_t occurrence = 1;
    if (args.size() >= 3) {
      position = args[2].int64_value();
      if (args.size() == 4) {
        occurrence = args[3].int64_value();
      }
    }
    bool ok = (*re)->Extract(in, position_unit, position, occurrence, &out,
                             &is_null, &status);
    ASSERT_EQ(expected_ok, ok);
    ASSERT_EQ(param.params.status().code(), status.code());
    if (ok) {
      if (param.params.result().is_null()) {
        EXPECT_TRUE(is_null);
      } else {
        EXPECT_FALSE(is_null);
        EXPECT_NE(nullptr, out.data());
        EXPECT_EQ(StringValue(param.params.result()), out);
      }
    }
  } else if (function == "regexp_replace") {
    std::string out;
    bool ok = (*re)->Replace(StringValue(args[0]), StringValue(args[2]), &out,
                             &status);
    ASSERT_EQ(expected_ok, ok);
    ASSERT_EQ(param.params.status().code(), status.code());
    if (ok) {
      EXPECT_EQ(StringValue(param.params.result()), out);
    }
  } else if (function == "regexp_extract_all") {
    std::string in_str = StringValue(args[0]);
    absl::string_view in = in_str;
    RegExp::ExtractAllIterator iter = (*re)->CreateExtractAllIterator(in);
    std::vector<Value> values;
    while (true) {
      absl::string_view out;
      if (!iter.Next(&out, &status)) {
        ASSERT_EQ(expected_ok, status.ok());
        if (!expected_ok) {
          ASSERT_EQ(param.params.status().code(), status.code());
        } else {
          EXPECT_EQ(param.params.result(),
                    Value::Array(
                        types::ArrayTypeFromSimpleTypeKind(args[0].type_kind()),
                        values));
        }
        break;
      } else {
        if (args[0].type_kind() == TYPE_STRING) {
          values.push_back(Value::String(out));
        } else {
          values.push_back(Value::Bytes(out));
        }
      }
    }
  } else {
    FAIL() << "Unrecognized regexp function: " << function;
  }
}

INSTANTIATE_TEST_SUITE_P(
    Regexp2, RegexpTemplateTest,
    testing::ValuesIn(GetFunctionTestsRegexp2(/*include_feature_set=*/false)));

INSTANTIATE_TEST_SUITE_P(Regexp, RegexpTemplateTest,
                         testing::ValuesIn(GetFunctionTestsRegexp()));

typedef testing::TestWithParam<FunctionTestCall> RegexpInstrTest;
TEST_P(RegexpInstrTest, TestLib) {
  const FunctionTestCall& param = GetParam();
  const std::vector<Value>& args = param.params.params();
  bool expected_ok = param.params.status().ok();
  for (const Value& arg : args) {
    // Ignore tests with null arguments.
    if (arg.is_null()) return;
  }
  absl::StatusOr<std::unique_ptr<const RegExp>> re;
  RegExp::InstrParams options;
  if (args[1].type_kind() == TYPE_STRING) {
    re = MakeRegExpUtf8(args[1].string_value());
    options.input_str = args[0].string_value();
    options.position_unit = RegExp::kUtf8Chars;
  } else {
    re = MakeRegExpBytes(args[1].bytes_value());
    options.input_str = args[0].bytes_value();
    options.position_unit = RegExp::kBytes;
  }
  if (!re.status().ok()) {
    ASSERT_FALSE(expected_ok);
    ASSERT_EQ(param.params.status().code(), re.status().code());
    return;
  }

  if (args.size() >= 3) {
    options.position = args[2].int64_value();
    if (args.size() >= 4) {
      options.occurrence_index = args[3].int64_value();
      if (args.size() == 5) {
        if (args[4].int64_value() == 1) {
          options.return_position = RegExp::kEndOfMatch;
        } else if (args[4].int64_value() == 0) {
          options.return_position = RegExp::kStartOfMatch;
        } else {
          return;
        }
      }
    }
  }
  absl::Status status;
  int64_t out;
  options.out = &out;
  bool ok = (*re)->Instr(options, &status);
  ASSERT_EQ(expected_ok, ok);
  ASSERT_EQ(param.params.status().code(), status.code());
  if (ok) {
    EXPECT_EQ(param.params.result().int64_value(), out);
  }
}

INSTANTIATE_TEST_SUITE_P(RegexpInstrTests, RegexpInstrTest,
                         testing::ValuesIn(GetFunctionTestsRegexpInstr()));

TEST(RegexpExtract, NullStringView) {
  // Tests for b/25378427.
  absl::string_view null_string;
  absl::string_view empty_string("", 0);
  const std::vector<std::pair<absl::string_view, absl::string_view>>
      patterns_and_inputs = {{null_string, null_string},
                             {null_string, empty_string},
                             {empty_string, null_string},
                             {empty_string, empty_string}};

  for (const auto& pattern_and_input : patterns_and_inputs) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto regexp, MakeRegExpUtf8(pattern_and_input.first));
    absl::Status status;
    absl::string_view out;
    bool is_null;
    ASSERT_TRUE(
        regexp->Extract(pattern_and_input.second, &out, &is_null, &status))
        << status;
    EXPECT_NE(nullptr, out.data());
    EXPECT_TRUE(out.empty());
    EXPECT_FALSE(is_null);
  }
}

TEST(RegexpReplace, MemLimit) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExpUtf8("A"));

  std::string in(64 * 1024, 'A');
  std::string out;
  absl::Status error;
  EXPECT_TRUE(re->Replace(in, "BB", &out, &error));

  EXPECT_TRUE(re->Replace(in, "B", 64 * 1024, &out, &error));
  EXPECT_FALSE(re->Replace(in, "BB", 64 * 1024, &out, &error));
}

TEST(RegexpReplace, NullStringView) {
  // Tests for b/160737744.
  absl::string_view null_string;
  absl::string_view empty_string("", 0);
  const std::vector<std::pair<absl::string_view, absl::string_view>>
      patterns_and_inputs = {{null_string, null_string},
                             {null_string, empty_string},
                             {empty_string, null_string},
                             {empty_string, empty_string}};

  for (const auto& pattern_and_input : patterns_and_inputs) {
    absl::Status status;
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto regexp, MakeRegExpUtf8(pattern_and_input.first));

    std::string out;
    ASSERT_TRUE(regexp->Replace(pattern_and_input.second, "foo", &out, &status))
        << status;
    EXPECT_EQ("foo", out);
  }
}

TEST(InitializeWithOptions, CaseInsensitive) {
  RE2::Options options;
  options.set_case_sensitive(false);
  absl::Status status;
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto reg_exp, MakeRegExpWithOptions("abc", options));
  EXPECT_FALSE(reg_exp->re().options().case_sensitive());

  bool out;
  ASSERT_TRUE(reg_exp->Match("abc", &out, &status));
  EXPECT_TRUE(out);
  ASSERT_TRUE(reg_exp->Match("AbC", &out, &status));
  EXPECT_TRUE(out);

  ASSERT_TRUE(reg_exp->Match("defgh", &out, &status));
  EXPECT_FALSE(out);
}

}  // anonymous namespace
}  // namespace functions
}  // namespace zetasql
