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
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "re2/re2.h"

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

TEST(RegexpExtractGroupsResultStructTest, ExplicitTests) {
  TypeFactory type_factory;
  LanguageOptions language_options;
  const bool kDeriveFieldTypes = true;

  // Simple pattern with named and unnamed groups
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExpUtf8("(?P<first>[a-z]+)-([0-9]+)"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const Type* result_type,
        re->ExtractGroupsResultStruct(&type_factory, language_options,
                                      kDeriveFieldTypes));
    ASSERT_TRUE(result_type->IsStruct());
    const StructType* struct_type = result_type->AsStruct();
    ASSERT_EQ(struct_type->num_fields(), 2);
    EXPECT_EQ(struct_type->field(0).name, "first");
    EXPECT_TRUE(struct_type->field(0).type->IsString());
    EXPECT_EQ(struct_type->field(1).name, "");
    EXPECT_TRUE(struct_type->field(1).type->IsString());
  }

  // Perl syntax for capture groups (?<name>)
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExpUtf8("(?<first>[a-z]+)-([0-9]+)"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const Type* result_type,
        re->ExtractGroupsResultStruct(&type_factory, language_options,
                                      kDeriveFieldTypes));
    ASSERT_TRUE(result_type->IsStruct());
    const StructType* struct_type = result_type->AsStruct();
    ASSERT_EQ(struct_type->num_fields(), 2);
    EXPECT_EQ(struct_type->field(0).name, "first");
    EXPECT_TRUE(struct_type->field(0).type->IsString());
    EXPECT_EQ(struct_type->field(1).name, "");
    EXPECT_TRUE(struct_type->field(1).type->IsString());
  }

  // Pattern with duplicate group names (should fail)
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re,
                         MakeRegExpUtf8("(?P<name>[a-z]+)-(?P<name>[0-9]+)"));
    EXPECT_THAT(re->ExtractGroupsResultStruct(&type_factory, language_options,
                                              kDeriveFieldTypes),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kInvalidArgument,
                    testing::HasSubstr("Regular expression contains duplicate "
                                       "capturing group name: name")));
  }
  // Case-insensitive duplicate group names should also fail.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re,
                         MakeRegExpUtf8("(?P<name>[a-z]+)-(?P<nAmE>[0-9]+)"));
    EXPECT_THAT(re->ExtractGroupsResultStruct(&type_factory, language_options,
                                              kDeriveFieldTypes),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kInvalidArgument,
                    testing::HasSubstr("Regular expression contains duplicate "
                                       "capturing group name: nAmE")));
  }
  // Duplicate field names after stripping suffix (should fail).
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        auto re,
        MakeRegExpUtf8("(?P<my_val__INT64>[0-9]+)-(?P<my_val>[a-z]+)"));
    EXPECT_THAT(re->ExtractGroupsResultStruct(&type_factory, language_options,
                                              !kDeriveFieldTypes),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kInvalidArgument,
                    testing::HasSubstr("duplicate capturing group name")));
  }

  // Pattern with no capture groups (should fail)
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExpUtf8("[a-z]+"));
    EXPECT_THAT(
        re->ExtractGroupsResultStruct(&type_factory, language_options,
                                      !kDeriveFieldTypes),
        zetasql_base::testing::StatusIs(
            absl::StatusCode::kInvalidArgument,
            testing::HasSubstr(
                "Regular expression does not contain any capturing groups")));
  }

  // Bytes pattern
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re,
                         MakeRegExpBytes("(?P<first>[a-z]+)-([0-9]+)"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const Type* result_type,
        re->ExtractGroupsResultStruct(&type_factory, language_options,
                                      !kDeriveFieldTypes));
    ASSERT_TRUE(result_type->IsStruct());
    const StructType* struct_type = result_type->AsStruct();
    ASSERT_EQ(struct_type->num_fields(), 2);
    EXPECT_EQ(struct_type->field(0).name, "first");
    EXPECT_TRUE(struct_type->field(0).type->IsBytes());
    EXPECT_EQ(struct_type->field(1).name, "");
    EXPECT_TRUE(struct_type->field(1).type->IsBytes());
  }

  // Pattern with type suffixes for STRING.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExpUtf8("(?P<name__STRING>[a-z]+) "
                                                 "(?P<age__INT64>[0-9]+) "
                                                 "(?P<checked__BOOL>true)"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const Type* result_type,
        re->ExtractGroupsResultStruct(&type_factory, language_options,
                                      kDeriveFieldTypes));
    ASSERT_TRUE(result_type->IsStruct());
    const StructType* struct_type = result_type->AsStruct();
    ASSERT_EQ(struct_type->num_fields(), 3);
    EXPECT_EQ(struct_type->field(0).name, "name");
    EXPECT_TRUE(struct_type->field(0).type->IsString());
    EXPECT_EQ(struct_type->field(1).name, "age");
    EXPECT_TRUE(struct_type->field(1).type->IsInt64());
    EXPECT_EQ(struct_type->field(2).name, "checked");
    EXPECT_TRUE(struct_type->field(2).type->IsBool());
  }

  // Field type is STRING if kDeriveFieldTypes is false.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExpUtf8("(?P<age__INT64>[0-9]+) "
                                                 "(?P<checked__BOOL>)"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const Type* result_type,
        re->ExtractGroupsResultStruct(&type_factory, language_options,
                                      !kDeriveFieldTypes));
    ASSERT_TRUE(result_type->IsStruct());
    const StructType* struct_type = result_type->AsStruct();
    ASSERT_EQ(struct_type->num_fields(), 2);
    EXPECT_EQ(struct_type->field(0).name, "age");
    EXPECT_TRUE(struct_type->field(0).type->IsString());
    EXPECT_EQ(struct_type->field(1).name, "checked");
    EXPECT_TRUE(struct_type->field(1).type->IsString());
  }

  // Type name should be case insensitive.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExpUtf8("(?P<age__iNt64>[0-9]+) "
                                                 "(?P<checked__bOoL>)"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const Type* result_type,
        re->ExtractGroupsResultStruct(&type_factory, language_options,
                                      kDeriveFieldTypes));
    ASSERT_TRUE(result_type->IsStruct());
    const StructType* struct_type = result_type->AsStruct();
    ASSERT_EQ(struct_type->num_fields(), 2);
    EXPECT_EQ(struct_type->field(0).name, "age");
    EXPECT_TRUE(struct_type->field(0).type->IsInt64());
    EXPECT_EQ(struct_type->field(1).name, "checked");
    EXPECT_TRUE(struct_type->field(1).type->IsBool());
  }

  // Invalid type suffix.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExpUtf8("(?P<name__INVALID>[a-z]+)"));
    EXPECT_THAT(re->ExtractGroupsResultStruct(&type_factory, language_options,
                                              kDeriveFieldTypes),
                zetasql_base::testing::StatusIs(
                    absl::StatusCode::kInvalidArgument,
                    testing::HasSubstr("Expected a type name as the suffix")));
  }

  // Empty field name after stripping suffix.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExpUtf8("(?P<__INT64>[0-9]+)"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const Type* result_type,
        re->ExtractGroupsResultStruct(&type_factory, language_options,
                                      kDeriveFieldTypes));
    ASSERT_TRUE(result_type->IsStruct());
    const StructType* struct_type = result_type->AsStruct();
    ASSERT_EQ(struct_type->num_fields(), 1);
    EXPECT_EQ(struct_type->field(0).name, "");
    EXPECT_TRUE(struct_type->field(0).type->IsInt64());
  }

  // Group name with multiple __.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExpUtf8("(?P<a__b__INT64>[0-9]+)"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const Type* result_type,
        re->ExtractGroupsResultStruct(&type_factory, language_options,
                                      kDeriveFieldTypes));
    ASSERT_TRUE(result_type->IsStruct());
    const StructType* struct_type = result_type->AsStruct();
    ASSERT_EQ(struct_type->num_fields(), 1);
    EXPECT_EQ(struct_type->field(0).name, "a__b");
    EXPECT_TRUE(struct_type->field(0).type->IsInt64());
  }

  // Group name with __ but no type suffix.
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExpUtf8("(?P<a__>[0-9]+)"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const Type* result_type,
        re->ExtractGroupsResultStruct(&type_factory, language_options,
                                      kDeriveFieldTypes));
    ASSERT_TRUE(result_type->IsStruct());
    const StructType* struct_type = result_type->AsStruct();
    ASSERT_EQ(struct_type->num_fields(), 1);
    EXPECT_EQ(struct_type->field(0).name, "a");
    EXPECT_TRUE(struct_type->field(0).type->IsString());
  }

  // Pattern with only unnamed groups
  {
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExpUtf8("([a-z]+)-([0-9]+)"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        const Type* result_type,
        re->ExtractGroupsResultStruct(&type_factory, language_options,
                                      kDeriveFieldTypes));
    ASSERT_TRUE(result_type->IsStruct());
    const StructType* struct_type = result_type->AsStruct();
    ASSERT_EQ(struct_type->num_fields(), 2);
    EXPECT_EQ(struct_type->field(0).name, "");
    EXPECT_TRUE(struct_type->field(0).type->IsString());
    EXPECT_EQ(struct_type->field(1).name, "");
    EXPECT_TRUE(struct_type->field(1).type->IsString());
  }
}

typedef testing::TestWithParam<FunctionTestCall>
    RegexpExtractGroupsResultStructTest;
TEST_P(RegexpExtractGroupsResultStructTest, Test) {
  const FunctionTestCall& param = GetParam();
  const std::vector<Value>& args = param.params.params();
  bool expected_ok = param.params.status().ok();

  TypeFactory type_factory;
  LanguageOptions language_options;
  const bool kDeriveFieldTypes = true;

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const RegExp> re,
                       args[0].type_kind() == TYPE_STRING
                           ? MakeRegExpUtf8(args[1].string_value())
                           : MakeRegExpBytes(args[1].bytes_value()));
  // We always set derive_field_types = true here, and check that the types of
  // the result struct fields match the test cases.
  absl::StatusOr<const Type*> result_type = re->ExtractGroupsResultStruct(
      &type_factory, language_options, kDeriveFieldTypes);
  ASSERT_EQ(result_type.status().ok(), expected_ok);
  if (!result_type.ok()) {
    EXPECT_THAT(result_type.status(),
                zetasql_base::testing::StatusIs(
                    param.params.status().code(),
                    testing::HasSubstr(param.params.status().message())));
    return;
  }
  ASSERT_TRUE((*result_type)->IsStruct());
  const StructType* struct_type = (*result_type)->AsStruct();
  const StructType* expected_struct_type =
      param.params.result().type()->AsStruct();
  ASSERT_EQ(struct_type->num_fields(), expected_struct_type->num_fields());
  for (int i = 0; i < struct_type->num_fields(); ++i) {
    EXPECT_EQ(struct_type->field(i).name, expected_struct_type->field(i).name);
    EXPECT_EQ(struct_type->field(i).type, expected_struct_type->field(i).type)
        << struct_type->field(i).type->DebugString() << " vs "
        << expected_struct_type->field(i).type->DebugString();
  }
}

INSTANTIATE_TEST_SUITE_P(
    RegexpExtractGroupsResultStructTest, RegexpExtractGroupsResultStructTest,
    testing::ValuesIn(GetFunctionTestsRegexpExtractGroups()));

class RegexpExtractGroupsTest
    : public testing::TestWithParam<std::tuple<FunctionTestCall>> {
 public:
  std::optional<absl::string_view> MatchString() const {
    const Value& match_string = std::get<0>(GetParam()).params.param(0);
    if (match_string.is_null()) {
      return std::nullopt;
    }
    return match_string.type()->IsString() ? match_string.string_value()
                                           : match_string.bytes_value();
  }
  absl::StatusOr<std::unique_ptr<const RegExp>> MakeRegExp() const {
    const Value& regexp_value = std::get<0>(GetParam()).params.param(1);
    return regexp_value.type()->IsString()
               ? MakeRegExpUtf8(regexp_value.string_value())
               : MakeRegExpBytes(regexp_value.bytes_value());
  }
  Value ResultValue() const { return std::get<0>(GetParam()).params.result(); }
};

INSTANTIATE_TEST_SUITE_P(
    RegexpExtractGroupsTest, RegexpExtractGroupsTest,
    testing::Combine(testing::ValuesIn(
        GetFunctionTestsRegexpExtractGroupsWithoutAutoCasting())));

TEST_P(RegexpExtractGroupsTest, Test) {
  const FunctionTestCall& param = std::get<0>(GetParam());
  TypeFactory type_factory;
  const LanguageOptions language_options;
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto re, MakeRegExp());
  absl::StatusOr<const Type*> result_type =
      re->ExtractGroupsResultStruct(&type_factory, language_options,
                                    /*derive_field_types=*/false);
  ASSERT_EQ(result_type.status().ok(), param.params.status().ok());
  if (!result_type.ok()) {
    EXPECT_THAT(result_type.status(),
                zetasql_base::testing::StatusIs(
                    param.params.status().code(),
                    testing::HasSubstr(param.params.status().message())));
    return;
  }

  std::optional<absl::string_view> match_string = MatchString();
  if (!match_string.has_value()) {
    EXPECT_TRUE(ResultValue().is_null());
    return;
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Value out, re->ExtractGroups(match_string.value(), result_type.value()));
  EXPECT_TRUE(out.Equals(ResultValue()))
      << out.DebugString() << " vs " << ResultValue().DebugString();

  // Test the version that computes the result type internally.
  ZETASQL_ASSERT_OK_AND_ASSIGN(Value out2,
                       re->ExtractGroups(match_string.value(), &type_factory));
  EXPECT_TRUE(out2.Equals(ResultValue()))
      << out.DebugString() << " vs " << ResultValue().DebugString();
}

// TODO: b/328210654 - Move these tests to testlib with prohibited feature:
// FEATURE_LEGACY_REGEXP_POSITION_BEHAVIOR. This is a temporary test set to
// ensure that the offset is correctly forwarded. This behavior is not yet used
// by any engine.
TEST(RegexpExtract, ForwardsOffset) {
  absl::StatusOr<std::unique_ptr<const RegExp>> re;
  absl::Status status;
  absl::string_view out;
  bool is_null;
  const std::vector<std::tuple<absl::string_view, absl::string_view, int, int,
                               absl::string_view>>
      tests = {{"xyzabc", "^abc$", 4, 1, ""},
               {"xyzabc", "abc$", 4, 1, "abc"},
               {"xyzabc-abc", "^abc.", 4, 1, ""},
               {"xyzabc-abc", "abc.", 4, 1, "abc-"}};
  for (const auto& [in, regex, position, occurrence, expected_out] : tests) {
    re = MakeRegExpBytes(regex);
    (*re)->Extract(in, RegExp::kBytes, position, occurrence,
                   /*use_legacy_position_behavior=*/false, &out, &is_null,
                   &status);
    ZETASQL_ASSERT_OK(status);
    EXPECT_EQ(out, expected_out);
  }
}

TEST(RegexpInstr, ForwardsOffset) {
  absl::StatusOr<std::unique_ptr<const RegExp>> re;
  absl::Status status;
  RegExp::InstrParams options;
  int64_t out;
  const std::vector<
      std::tuple<absl::string_view, absl::string_view, int, int, int>>
      tests = {{"xyzabc", "^abc$", 4, 1, 0},
               {"xyzabc", "abc$", 4, 1, 4},
               {"xyzabc-abc", "^abc.", 4, 1, 0},
               {"xyzabc-abc", "abc.", 4, 1, 4}};
  for (const auto& [in, regex, position, occurrence, expected_out] : tests) {
    re = MakeRegExpBytes(regex);
    options = {
        .input_str = in,
        .position_unit = RegExp::kBytes,
        .position = position,
        .occurrence_index = occurrence,
        .return_position = RegExp::kStartOfMatch,
        .out = &out,
    };
    (*re)->Instr(options, /*use_legacy_position_behavior=*/false, &status);
    ZETASQL_ASSERT_OK(status);
    EXPECT_EQ(out, expected_out);
  }
}

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
