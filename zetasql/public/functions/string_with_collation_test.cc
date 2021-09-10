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

#include "zetasql/public/functions/string_with_collation.h"

#include <sys/types.h>

#include <string>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/testing/test_function.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

namespace {

template <typename OutType, typename FunctionType, class... Args>
void TestStringFunctionWithCollation(FunctionType function,
                                     const QueryParamsWithResult& param,
                                     absl::string_view collation_spec,
                                     Args... args) {
  OutType out;
  absl::Status status;
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const ZetaSqlCollator> collator,
                       MakeSqlCollator(collation_spec));
  EXPECT_EQ(function(*collator, args..., &out, &status), param.status().ok());
  if (param.status().ok()) {
    EXPECT_EQ(absl::OkStatus(), status);
    EXPECT_TRUE(param.result().Equals(Value::String(out)))
        << "Expected: " << param.result() << "\n"
        << "Actual: '" << out << "'\n";
  } else if (!param.status().message().empty()) {
    // If an error message is provided, it means we want to test the exact
    // message instead of a binary success/failure.
    EXPECT_EQ(param.status(), status);
  } else {
    EXPECT_NE(absl::OkStatus(), status) << "Unexpected value: " << out;
  }
}

template <typename FunctionType, class... Args>
void TestStringArrayFunctionWithCollation(FunctionType function,
                                          const QueryParamsWithResult& param,
                                          absl::string_view collation_spec,
                                          Args... args) {
  std::vector<absl::string_view> out;
  absl::Status status;
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const ZetaSqlCollator> collator,
                       MakeSqlCollator(collation_spec));
  EXPECT_EQ(function(*collator, args..., &out, &status), param.status().ok());
  if (param.status().ok()) {
    std::vector<Value> out_values;
    for (auto element : out) {
      out_values.push_back(Value::String(element));
    }
    auto out_array = Value::Array(
        zetasql::types::StringArrayType()->AsArray(), out_values);
    EXPECT_EQ(absl::OkStatus(), status);
    EXPECT_TRUE(param.result().Equals(out_array))
        << "Expected: " << param.result() << "\n"
        << "Actual: '" << out_array << "'\n";
  } else if (!param.status().message().empty()) {
    // If an error message is provided, it means we want to test the exact
    // message instead of a binary success/failure.
    EXPECT_EQ(param.status(), status);
  } else {
    EXPECT_NE(absl::OkStatus(), status) << "Unexpected value: " << out.size();
  }
}

template <typename OutType, typename FunctionType, class... Args>
void TestFunctionWithCollation(FunctionType function,
                               const QueryParamsWithResult& param,
                               absl::string_view collation_spec, Args... args) {
  OutType out;
  absl::Status status;
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const ZetaSqlCollator> collator,
                       MakeSqlCollator(collation_spec));
  EXPECT_EQ(function(*collator, args..., &out, &status), param.status().ok());
  if (param.status().ok()) {
    EXPECT_EQ(absl::OkStatus(), status);
    EXPECT_TRUE(param.result().Equals(Value::Make<OutType>(out)))
        << "Expected: " << param.result() << "\n"
        << "Actual: '" << out << "'\n";
  } else if (!param.status().message().empty()) {
    // If an error message is provided, it means we want to test the exact
    // message instead of a binary success/failure.
    EXPECT_EQ(param.status(), status);
  } else {
    EXPECT_NE(absl::OkStatus(), status) << "Unexpected value: " << out;
  }
}

typedef testing::TestWithParam<FunctionTestCall> StringWithCollatorTemplateTest;
TEST_P(StringWithCollatorTemplateTest, Testlib) {
  const FunctionTestCall& param = GetParam();
  const std::string& function = param.function_name;
  const std::vector<Value>& args = param.params.params();
  for (const Value& arg : args) {
    // Ignore tests with null and bytes arguments.
    if (arg.is_null() || arg.type_kind() == TypeKind::TYPE_BYTES) return;
  }
  if (function == "replace_with_collator") {
    TestStringFunctionWithCollation<std::string>(
        &ReplaceUtf8WithCollation, param.params, args[0].string_value(),
        args[1].string_value(), args[2].string_value(), args[3].string_value());
  } else if (function == "split_with_collator") {
    TestStringArrayFunctionWithCollation(
        &SplitUtf8WithCollation, param.params, args[0].string_value(),
        args[1].string_value(), args[2].string_value());
  } else if (function == "instr") {
    // Use test cases for regular INSTR for both unicode:cs and unicode:ci.
    int64_t pos = 1;
    int64_t occurrence = 1;
    if (args.size() >= 3) pos = args[2].int64_value();
    if (args.size() == 4) occurrence = args[3].int64_value();
    TestFunctionWithCollation<int64_t>(
        &StrPosOccurrenceUtf8WithCollation, param.params, "unicode:ci",
        args[0].string_value(), args[1].string_value(), pos, occurrence);
    // Verify that unicode:cs uses the non-collation version.
    TestFunctionWithCollation<int64_t>(
        &StrPosOccurrenceUtf8WithCollation, param.params, "unicode:cs",
        args[0].string_value(), args[1].string_value(), pos, occurrence);
  } else if (function == "instr_with_collator") {
    int64_t pos = 1;
    int64_t occurrence = 1;
    if (args.size() >= 4) pos = args[3].int64_value();
    if (args.size() == 5) occurrence = args[4].int64_value();
    TestFunctionWithCollation<int64_t>(&StrPosOccurrenceUtf8WithCollation,
                                       param.params, args[0].string_value(),
                                       args[1].string_value(),
                                       args[2].string_value(), pos, occurrence);
  } else if (function == "strpos_with_collator") {
    TestFunctionWithCollation<int64_t>(
        &StrposUtf8WithCollation, param.params, args[0].string_value(),
        args[1].string_value(), args[2].string_value());
  } else if (function == "starts_with_collator") {
    TestFunctionWithCollation<bool>(
        &StartsWithUtf8WithCollation, param.params, args[0].string_value(),
        args[1].string_value(), args[2].string_value());
  } else if (function == "ends_with_collator") {
    TestFunctionWithCollation<bool>(
        &EndsWithUtf8WithCollation, param.params, args[0].string_value(),
        args[1].string_value(), args[2].string_value());
  } else {
    EXPECT_FALSE(true) << "undefined method name: " << function;
  }
}

INSTANTIATE_TEST_SUITE_P(
    String, StringWithCollatorTemplateTest,
    testing::ValuesIn(GetFunctionTestsStringWithCollator()));

INSTANTIATE_TEST_SUITE_P(String1, StringWithCollatorTemplateTest,
                         testing::ValuesIn(GetFunctionTestsInstr1()));

INSTANTIATE_TEST_SUITE_P(String2, StringWithCollatorTemplateTest,
                         testing::ValuesIn(GetFunctionTestsInstr2()));

INSTANTIATE_TEST_SUITE_P(String3, StringWithCollatorTemplateTest,
                         testing::ValuesIn(GetFunctionTestsInstr3()));

TEST(ReplaceWithCollator, HandleExplodingStringLength) {
  absl::Status error;
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const ZetaSqlCollator> collator,
                       MakeSqlCollator("unicode:ci"));
  std::string generation0 = "22222222";
  // Generation 1: 8 -> 8^2 (64)
  std::string generation1;
  EXPECT_TRUE(ReplaceUtf8WithCollation(*collator, generation0, "2", generation0,
                                       &generation1, &error));
  // Generation 2: 64 -> 64^2 (4k)
  std::string generation2;
  EXPECT_TRUE(ReplaceUtf8WithCollation(*collator, generation1, "2", generation1,
                                       &generation2, &error));
  // Generation 2: 4k -> 4k^2 (16m) TOO BIG
  std::string generation3;
  EXPECT_FALSE(ReplaceUtf8WithCollation(*collator, generation2, "2",
                                        generation2, &generation3, &error));
}

}  // anonymous namespace
}  // namespace functions
}  // namespace zetasql
