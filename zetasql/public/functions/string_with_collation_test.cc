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
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/testing/test_function.h"
#include "gtest/gtest.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

namespace {

struct LikeMatchTestParams {
  const char* pattern;
  const char* input;
  TypeKind type;
  bool expected_outcome;
};

std::vector<LikeMatchTestParams> LikeMatchTestCases() {
  return {
      // '_' matches single character...
      {"_", "", TYPE_STRING, false},
      {"_", "a", TYPE_STRING, true},
      {"_", "ab", TYPE_STRING, false},
      // ... any Unicode character actually.
      {"_", "ф", TYPE_STRING, true},

      // Escaped '_' matches itself
      {"\\_", "_", TYPE_STRING, true},
      {"\\_", "a", TYPE_STRING, false},

      // '_' matches CR and LF
      {"_", "\n", TYPE_STRING, true},
      {"_", "\r", TYPE_STRING, true},

      // Now the string itself is not valid UTF-8.
      {"_", "\xC2", TYPE_STRING, false},

      // '%' should match any string
      {"%", "", TYPE_STRING, true},
      {"%", "abc", TYPE_STRING, true},
      {"%", "фюы", TYPE_STRING, true},

      // A few more more complex expressions
      {"a(%)b", "a()b", TYPE_STRING, true},
      {"a(%)b", "a(z)b", TYPE_STRING, true},
      {"a(_%)b", "a()b", TYPE_STRING, false},
      {"a(_%)b", "a(z)b", TYPE_STRING, true},
      {"a(_\\%)b", "a(z)b", TYPE_STRING, false},
      {"\\a\\(_%\\)\\b", "a(z)b", TYPE_STRING, true},
      {"a%b%c", "abc", TYPE_STRING, true},
      {"a%b%c", "axyzbxyzc", TYPE_STRING, true},
      {"a%xyz%c", "abxybyzbc", TYPE_STRING, false},
      {"a%xyz%c", "abxybyzbxyzbc", TYPE_STRING, true},

      {"foo", "foo", TYPE_STRING, true},
      {"foo", "bar", TYPE_STRING, false},
      {"%bar", "foobar", TYPE_STRING, true},
      {"foo%", "foobar", TYPE_STRING, true},
      {"foo", "foob", TYPE_STRING, false},
      {"%bar%", "foobarfoo", TYPE_STRING, true},
      {"foo%bar", "foobar", TYPE_STRING, true},
      {"foo%foo", "foobarfoobarfoo", TYPE_STRING, true},
      {"foo%bar", "foobarfoobarfoo", TYPE_STRING, false},
      {"%foo%foo%", "foobarbarbaz", TYPE_STRING, false},
      {"aba%aba", "ababa", TYPE_STRING, false},
      {"abababa", "ababa", TYPE_STRING, false},
      {"ababa", "abababa", TYPE_STRING, false},
      {"\\%barfoo%", "%barfoobar", TYPE_STRING, true},
      {"\\%foo%", "barfoobar", TYPE_STRING, false},
      {"bar\\%foobar", "barfoobar", TYPE_STRING, false},
      {"\\_barfoo%", "_barfoobar", TYPE_STRING, true},
      {"\\_foo%", "barfoobar", TYPE_STRING, false},
      {"bar\\_foobar", "barfoobar", TYPE_STRING, false},
      {"%%%%%", "", TYPE_STRING, true},
      {"%%%%%", "barfoobar", TYPE_STRING, true},
      {"", "barfoobar", TYPE_STRING, false},
  };
}

std::vector<LikeMatchTestParams> LikeWithCollationMatchTestCases() {
  return {
      // Ignorable characters.
      {"\u0001", "", TYPE_STRING, true},
      {"\u0001%%", "", TYPE_STRING, true},
      {"%\u0001%", "", TYPE_STRING, true},
      {"%%\u0001", "", TYPE_STRING, true},
      {"foo\u0001", "foo", TYPE_STRING, true},
      {"\u0001foo", "foo", TYPE_STRING, true},
      {"foo%\u0001%bar", "foobar", TYPE_STRING, true},
      {"foo%\u0001%foo", "foobar", TYPE_STRING, false},
      {"foo%\u0001%foo%bar", "foobar", TYPE_STRING, false},
      {"foo\u0001bar", "foobar", TYPE_STRING, true},
      {"foo%\u0001bar", "foobar", TYPE_STRING, true},
      {"foo%\u0001bar%baz", "foobar", TYPE_STRING, false},
      {"foo%\u0001bar%baz", "foobarbaz", TYPE_STRING, true},
      {"foo%bar\u0001", "foobar", TYPE_STRING, true},
      {"aba%\u0001%aba", "ababa", TYPE_STRING, false},
      {"aba\u0001ba", "ababa", TYPE_STRING, true},
      {"", "\u0001", TYPE_STRING, true},
      {"foobar", "\u0001foobar", TYPE_STRING, true},
      {"foobar", "foo\u0001bar", TYPE_STRING, true},
      {"foobar", "foobar\u0001", TYPE_STRING, true},
      {"foobar%", "foobar\u0001", TYPE_STRING, true},
      {"%foobar", "\u0001foobar", TYPE_STRING, true},
      {"foo%bar", "foo\u0001bar", TYPE_STRING, true},
      {"aba%aba", "ab\u0001aba", TYPE_STRING, false},

      // Below cases could have different results for different collations.
      // Using und:ci as an example.
      {"FOO", "foo", TYPE_STRING, true},
      {"CamelCase", "camelcase", TYPE_STRING, true},
      {"CamelCase", "CAMELCASE", TYPE_STRING, true},
      {"camelcase", "CamelCase", TYPE_STRING, true},
      {"CAMELCASE", "CamelCase", TYPE_STRING, true},
      {"FOOBAR", "foo\u0001bar", TYPE_STRING, true},
      {"%FOO%BAR%", "foobar\u0001", TYPE_STRING, true},
      {"%%%%%FOOBAR", "foobar", TYPE_STRING, true},
      {"FOO%%%%%BAR", "foobar", TYPE_STRING, true},
      {"FOOBAR%%%%%", "foobar", TYPE_STRING, true},
      {"\\%BarFoo%", "%barfoobar", TYPE_STRING, true},
      {"\\%Foo%", "barfoobar", TYPE_STRING, false},
      {"Bar\\%FooBar", "barfoobar", TYPE_STRING, false},
      {"\\_BarFoo%", "_barfoobar", TYPE_STRING, true},
      {"\\_Foo%", "barfoobar", TYPE_STRING, false},
      {"bar\\_Foobar", "barfoobar", TYPE_STRING, false},
      {"foo\\%", "foo\\%", TYPE_STRING, false},
      {"foo\\%", "foo%", TYPE_STRING, true},
      {"Foo%Foo", "foobarfoobarfoo", TYPE_STRING, true},
      {"Foo%Bar", "foobarfoobarfoo", TYPE_STRING, false},
      {"%Foo%Foo%", "foobarbarbaz", TYPE_STRING, false},
      {"aBa%AbA", "ababa", TYPE_STRING, false},
      {"AbAbAbA", "ababa", TYPE_STRING, false},
      {"aBaBa", "abababa", TYPE_STRING, false},
      // 'ß' and 'ẞ'
      {"\u00DF", "\u1E9E", TYPE_STRING, true},
      // 'Å' and 'A''◌̊'
      {"\u00C5", "\u0041\u030A", TYPE_STRING, true},
      // 'ự' and 'u''◌̣''◌̛'
      {"\u1EF1", "\u0075\u031B\u0323", TYPE_STRING, true},
      {"\u0075%\u0323", "\u1EF1", TYPE_STRING, false},
      {"u%", "\u1EF1", TYPE_STRING, false},
      {"あ", "ア", TYPE_STRING, true},
      // "MW" and '㎿'
      {"MW", "\u33BF", TYPE_STRING, true},
      {"MM%", "M\u33BF", TYPE_STRING, false},
      {"A", "Å", TYPE_STRING, false},
      // 'ß' and "SS"
      {"\u00DF", "SS", TYPE_STRING, false},
      // 's' and 'ſ'
      {"s", "\u017F", TYPE_STRING, false},
      // "1/2" and '½'
      {"1/2", "\u00BD", TYPE_STRING, false},
      // Strings are not normalized.
      {"\u0078\u031B\u0323", "\u0078\u0323\u031B", TYPE_STRING, false},
      // 'Å' and 'a''◌̊'
      {"\u00C5", "\u0061\u030A", TYPE_STRING, true},
      // 'ự' and 'U''◌̣''◌̛'
      {"\u1EF1", "\u0055\u031B\u0323", TYPE_STRING, true},
      // "mw" and '㎿'
      {"mw", "\u33BF", TYPE_STRING, true},
  };
}

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

TEST(LikeWithCollationMatchTest, MatchTest) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const ZetaSqlCollator> collator,
                       MakeSqlCollator("und:ci"));
  for (const LikeMatchTestParams& params : LikeMatchTestCases()) {
    if (params.type == TYPE_STRING &&
        !absl::StrContains(absl::string_view(params.pattern), '_')) {
      SCOPED_TRACE(
          absl::Substitute("Matching pattern \"$0\" with string \"$1\"",
                           params.pattern, params.input));
      ZETASQL_ASSERT_OK_AND_ASSIGN(
          bool result,
          LikeWithUtf8WithCollation(params.input, params.pattern, *collator));
      EXPECT_EQ(params.expected_outcome, result)
          << params.input << " LIKE " << params.pattern;
      if (!absl::StrContains(absl::string_view(params.pattern), '%')) {
        absl::Status error;
        EXPECT_EQ(
            params.expected_outcome,
            collator->CompareUtf8(params.input, params.pattern, &error) == 0)
            << params.input << "==" << params.pattern;
      }
    }
  }
  for (const LikeMatchTestParams& params : LikeWithCollationMatchTestCases()) {
    SCOPED_TRACE(absl::Substitute("Matching pattern \"$0\" with string \"$1\"",
                                  params.pattern, params.input));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        bool result,
        LikeWithUtf8WithCollation(params.input, params.pattern, *collator));
    EXPECT_EQ(params.expected_outcome, result)
        << params.input << " LIKE " << params.pattern;
    if (!absl::StrContains(absl::string_view(params.pattern), '%')) {
      absl::Status error;
      EXPECT_EQ(
          params.expected_outcome,
          collator->CompareUtf8(params.input, params.pattern, &error) == 0)
          << params.input << "==" << params.pattern;
    }
  }
}

TEST(LikeWithCollationMatchTest, BinaryMatchTest) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const ZetaSqlCollator> collator,
                       MakeSqlCollator("binary"));
  std::vector<QueryParamsWithResult> params_list = GetFunctionTestsLike();
  for (const QueryParamsWithResult params : params_list) {
    Value text = params.param(0);
    Value pattern = params.param(1);
    if (text.type_kind() == TYPE_STRING && pattern.type_kind() == TYPE_STRING &&
        !text.is_null() && !pattern.is_null()) {
      SCOPED_TRACE(
          absl::Substitute("Matching pattern \"$0\" with string \"$1\"",
                           pattern.string_value(), text.string_value()));
      if (!params.status().ok()) {
        EXPECT_THAT(LikeWithUtf8WithCollation(
                        text.string_value(), pattern.string_value(), *collator),
                    zetasql_base::testing::StatusIs(params.status().code()));
      } else {
        ZETASQL_ASSERT_OK_AND_ASSIGN(
            bool result,
            LikeWithUtf8WithCollation(text.string_value(),
                                      pattern.string_value(), *collator));
        EXPECT_EQ(params.result().bool_value(), result)
            << text.string_value() << " LIKE " << pattern.string_value();
      }
    }
  }
}

TEST(LikeWithCollationMatchTest, BadPatternUTF8) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const ZetaSqlCollator> collator,
                       MakeSqlCollator("und:ci"));
  EXPECT_THAT(LikeWithUtf8WithCollation("", "\xC2", *collator),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kOutOfRange,
                  testing::HasSubstr("The second operand of LIKE operator is "
                                     "not a valid UTF-8 string")));
}

TEST(LikeWithCollationMatchTest, BadPatternEscape) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const ZetaSqlCollator> collator,
                       MakeSqlCollator("und:ci"));
  EXPECT_THAT(LikeWithUtf8WithCollation("", "\\", *collator),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kOutOfRange,
                  testing::HasSubstr("LIKE pattern ends with a backslash")));
}

TEST(LikeWithCollationMatchTest, BadPatternSpecifier) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const ZetaSqlCollator> collator,
                       MakeSqlCollator("und:ci"));
  EXPECT_THAT(
      LikeWithUtf8WithCollation(" ", "_", *collator),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kOutOfRange,
          testing::HasSubstr("LIKE pattern has '_' which is not "
                             "allowed when its operands have collation")));
}

}  // anonymous namespace
}  // namespace functions
}  // namespace zetasql
