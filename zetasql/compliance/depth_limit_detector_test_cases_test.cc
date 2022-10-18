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

#include "zetasql/compliance/depth_limit_detector_test_cases.h"

#include <cmath>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/depth_limit_detector_internal.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_tokens.h"
#include "zetasql/reference_impl/reference_driver.h"
#include "zetasql/testing/type_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"

ABSL_DECLARE_FLAG(std::size_t, depth_limit_detector_max_sql_bytes);

namespace zetasql {
namespace depth_limit_detector_internal {
namespace {

// Simple class to set and restore a flag for testing.
template <typename Flag>
class FlagSetInScope {
 public:
  FlagSetInScope(const FlagSetInScope&) = delete;
  FlagSetInScope& operator=(const FlagSetInScope&) = delete;

  template <typename Value>
  FlagSetInScope(Flag& flag, const Value& value)
      : flag_(flag), previous_value_(absl::GetFlag(flag_)) {
    absl::SetFlag(&flag, value);
  }
  ~FlagSetInScope() { absl::SetFlag(&flag_, previous_value_); }

 private:
  Flag& flag_;
  decltype(absl::GetFlag(flag_)) previous_value_;
};

template <typename Flag, typename Value>
FlagSetInScope(Flag& flag, const Value& value) -> FlagSetInScope<Flag>;

TEST(DepthLimitDetectorTest, DepthLimitDetectorSqrtTest) {
  // As the testcase just generates a number, 8 bytes corresponds
  // to 8 digits.
  FlagSetInScope restore(FLAGS_depth_limit_detector_max_sql_bytes, 8);
  DepthLimitDetectorTestCase test_case = DepthLimitDetectorTestCase{
      .depth_limit_test_case_name = "sqrt",
      .depth_limit_template =
          DepthLimitDetectorTemplate({DepthLimitDetectorDepthNumber()}),
  };
  auto results =
      RunDepthLimitDetectorTestCase(test_case, [](std::string_view number) {
        int64_t i;
        ZETASQL_CHECK(absl::SimpleAtoi(number, &i));
        return absl::ResourceExhaustedError(
            absl::StrCat(static_cast<int64_t>(sqrt(i))));
      });

  // Prove that there are not two return conditions next to each other with
  // the same return status as they should be merged.
  ASSERT_GT(results.depth_limit_detector_return_conditions.size(), 2);
  absl::Status last_status = absl::UnknownError("DepthLimitDetectorTest");
  int64_t last_ending_depth = 0;
  for (const auto& condition : results.depth_limit_detector_return_conditions) {
    EXPECT_NE(last_status, condition.return_status)
        << "Condition at starting depth " << condition.starting_depth
        << " is the same status as the previous one";
    EXPECT_EQ(last_ending_depth, condition.starting_depth - 1)
        << condition.return_status;
    last_status = condition.return_status;
    last_ending_depth = condition.ending_depth;
  }
  EXPECT_GT(last_ending_depth, 9999999);

  // Prove the return conditions are on the correct boundaries.
  int64_t s = 1;
  for (const auto& condition : results.depth_limit_detector_return_conditions) {
    EXPECT_EQ(condition.starting_depth, s * s) << condition.return_status;
    EXPECT_THAT(
        condition.return_status,
        ::zetasql_base::testing::StatusIs(absl::StatusCode::kResourceExhausted))
        << condition.return_status;
    s++;
  }
}

TEST(DepthLimitDetectorTest, DepthLimitDetectorPayloadTest) {
  // As the testcase just generates a number, 3 bytes corresponds
  // to 3 digits.
  FlagSetInScope restore(FLAGS_depth_limit_detector_max_sql_bytes, 3);
  int64_t call_count = 0;
  DepthLimitDetectorTestCase test_case = DepthLimitDetectorTestCase{
      .depth_limit_test_case_name = "payload",
      .depth_limit_template =
          DepthLimitDetectorTemplate({DepthLimitDetectorDepthNumber()}),
  };
  auto results =
      RunDepthLimitDetectorTestCase(test_case, [&](std::string_view number) {
        absl::Status ret = absl::ResourceExhaustedError("payload_test");
        ret.SetPayload("test_payload_url",
                       absl::Cord(absl::StrCat("payload", call_count++)));
        return ret;
      });
  // Prove we were called multiple times
  ASSERT_GT(call_count, 1);
  // Prove that all the statuses are merged correctly
  ASSERT_EQ(results.depth_limit_detector_return_conditions.size(), 1);
  // Prove that only the first payload is remembered
  EXPECT_EQ(*results.depth_limit_detector_return_conditions[0]
                 .return_status.GetPayload("test_payload_url"),
            absl::Cord("payload0"));
}

class DepthLimitDetectorTemplateTest
    : public ::testing::TestWithParam<
          std::reference_wrapper<const DepthLimitDetectorTestCase>> {};
}  // namespace

// Return a different error for each depth and check that is in the output.
TEST_P(DepthLimitDetectorTemplateTest, DepthLimitDetectorDisectTest) {
  const DepthLimitDetectorTestCase& test_case = GetParam();
  auto results =
      RunDepthLimitDetectorTestCase(test_case, [](std::string_view str) {
        return absl::InvalidArgumentError(absl::StrCat(str.size()));
      });
  ASSERT_GE(results.depth_limit_detector_return_conditions.size(), 1);
  EXPECT_EQ(results.depth_limit_detector_return_conditions[0].starting_depth,
            1);
  EXPECT_EQ(results.depth_limit_test_case_name,
            test_case.depth_limit_test_case_name);

  int last_ending_depth = 0;
  for (const auto& condition : results.depth_limit_detector_return_conditions) {
    EXPECT_EQ(condition.starting_depth, last_ending_depth + 1);
    EXPECT_EQ(condition.ending_depth, condition.starting_depth);
    EXPECT_THAT(
        condition.return_status,
        ::zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
    last_ending_depth = condition.ending_depth;
  }
}

TEST_P(DepthLimitDetectorTemplateTest, ParserAcceptsInstantiation) {
  const DepthLimitDetectorTestCase& test_case = GetParam();
  std::unique_ptr<zetasql::ParserOutput> output;
  zetasql::ParserOptions parser_options;
  parser_options.set_language_options(
      DepthLimitDetectorTestCaseLanguageOptions(test_case));

  std::string level1 = DepthLimitDetectorTemplateToString(test_case, 1);
  ZETASQL_EXPECT_OK(zetasql::ParseStatement(level1, parser_options, &output))
      << level1;
  std::string level2 = DepthLimitDetectorTemplateToString(test_case, 2);
  ZETASQL_EXPECT_OK(zetasql::ParseStatement(level2, parser_options, &output))
      << level2;
  std::string level10 = DepthLimitDetectorTemplateToString(test_case, 10);
  ZETASQL_EXPECT_OK(zetasql::ParseStatement(level10, parser_options, &output))
      << level10;
}

TEST_P(DepthLimitDetectorTemplateTest, ReferenceImplAcceptsInstantiation) {
  const DepthLimitDetectorTestCase& test_case = GetParam();
  zetasql::TypeFactory type_factory;
  ReferenceDriver driver(DepthLimitDetectorTestCaseLanguageOptions(test_case));
  TestDatabase test_db;
  for (const std::string& proto_file : testing::ZetaSqlTestProtoFilepaths()) {
    test_db.proto_files.insert(proto_file);
  }
  for (const std::string& proto_name : testing::ZetaSqlTestProtoNames()) {
    test_db.proto_names.insert(proto_name);
  }
  for (const std::string& enum_name : testing::ZetaSqlTestEnumNames()) {
    test_db.enum_names.insert(enum_name);
  }
  ZETASQL_ASSERT_OK(driver.CreateDatabase(test_db));

  for (int i = 1; i <= 5; ++i) {
    std::string sql = DepthLimitDetectorTemplateToString(test_case, i);
    ZETASQL_ASSERT_OK(driver.ExecuteStatement(sql, {}, &type_factory))
        << "Reference driver failed at depth " << i << "\n"
        << sql;
  }

  FlagSetInScope restore(FLAGS_depth_limit_detector_max_sql_bytes,
                         2000
  );
  DepthLimitDetectorTestResult results;

  ZETASQL_LOG(INFO) << "Disecting reference implementation " << test_case << " up to "
            << absl::GetFlag(FLAGS_depth_limit_detector_max_sql_bytes)
            << " bytes";

    results =
        RunDepthLimitDetectorTestCase(test_case, [&](std::string_view sql) {
          return driver.ExecuteStatement(std::string(sql), {}, &type_factory)
              .status();
        });

  ZETASQL_LOG(INFO) << "Disection finished " << results;
  EXPECT_THAT(results.depth_limit_detector_return_conditions[0].return_status,
              ::zetasql_base::testing::IsOk())
      << results;

  std::string last_successful_sql = DepthLimitDetectorTemplateToString(
      test_case,
      results.depth_limit_detector_return_conditions[0].ending_depth);
  ParseResumeLocation resume_location =
      ParseResumeLocation::FromString(last_successful_sql);
  ParseTokenOptions options;
  options.language_options =
      DepthLimitDetectorTestCaseLanguageOptions(test_case);
  std::vector<ParseToken> parse_tokens;
  ZETASQL_ASSERT_OK(GetParseTokens(options, &resume_location, &parse_tokens));

  RecordProperty("max_successful_tokens", absl::StrCat(parse_tokens.size()));
  RecordProperty("max_successful_bytes",
                 absl::StrCat(last_successful_sql.size()));

  for (int64_t i = 1; i < results.depth_limit_detector_return_conditions.size();
       ++i) {
    EXPECT_TRUE(absl::IsResourceExhausted(
        results.depth_limit_detector_return_conditions[i].return_status))
        << results.depth_limit_detector_return_conditions[i];
  }
}

INSTANTIATE_TEST_CASE_P(DepthLimitDetectorTest, DepthLimitDetectorTemplateTest,
                        ::testing::ValuesIn(AllDepthLimitDetectorTestCases()));

// Validate repeated template output.
TEST(DepthLimitDetectorTest, SimpleTestCaseToString) {
  std::string output = DepthLimitDetectorTemplateToString(
      DepthLimitDetectorTemplate(
          {"SELECT ", DepthLimitDetectorRepeatedTemplate({"+1"}), ";"}),
      2);
  EXPECT_EQ("SELECT +1+1;", output);
}

// Validate numbered template output.
TEST(DepthLimitDetectorTest, NumberedTestCaseToString) {
  std::string output = DepthLimitDetectorTemplateToString(
      DepthLimitDetectorTemplate({"SELECT ",
                                  DepthLimitDetectorRepeatedTemplate(
                                      {"+", DepthLimitDetectorDepthNumber()}),
                                  ";"}),
      4);
  EXPECT_EQ("SELECT +1+2+3+4;", output);
}
}  // namespace depth_limit_detector_internal
}  // namespace zetasql
