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
#include "zetasql/common/thread_stack.h"
#include "zetasql/compliance/depth_limit_detector_internal.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_tokens.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/reference_impl/reference_driver.h"
#include "zetasql/testing/type_util.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

ABSL_FLAG(
    absl::Duration, depth_limit_detector_reference_max_probing_duration,
    absl::Seconds(30),
    "Amount of time to search for failing reference implementation cases");

namespace zetasql {
namespace depth_limit_detector_internal {
namespace {

class ReferenceDriverStatementRunner {
  ReferenceDriver driver_;
  TestDatabase test_db_;

 public:
  explicit ReferenceDriverStatementRunner(const LanguageOptions& options)
      : driver_(options) {
    for (const std::string& proto_file :
         testing::ZetaSqlTestProtoFilepaths()) {
      test_db_.proto_files.insert(proto_file);
    }
    for (const std::string& proto_name : testing::ZetaSqlTestProtoNames()) {
      test_db_.proto_names.insert(proto_name);
    }
    for (const std::string& enum_name : testing::ZetaSqlTestEnumNames()) {
      test_db_.enum_names.insert(enum_name);
    }
    ZETASQL_EXPECT_OK(driver_.CreateDatabase(test_db_));
  }

  absl::Status operator()(std::string_view sql) {
    zetasql::TypeFactory type_factory;
    return driver_.ExecuteStatement(std::string(sql), {}, &type_factory)
        .status();
  }
};

TEST(DepthLimitDetectorTest, DepthLimitDetectorSqrtTest) {
  // As the testcase just generates a number, 8 bytes corresponds
  // to 8 digits.
  DepthLimitDetectorRuntimeControl runtime_control;
  runtime_control.max_sql_bytes = 8;
  DepthLimitDetectorTestCase test_case = DepthLimitDetectorTestCase{
      .depth_limit_test_case_name = "sqrt",
      .depth_limit_template =
          DepthLimitDetectorTemplate({DepthLimitDetectorDepthNumber()}),
  };
  auto results = RunDepthLimitDetectorTestCase(
      test_case,
      [](std::string_view number) {
        int64_t i;
        ABSL_CHECK(absl::SimpleAtoi(number, &i));
        return absl::ResourceExhaustedError(
            absl::StrCat(static_cast<int64_t>(sqrt(i))));
      },
      runtime_control);

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

TEST(DepthLimitDetectorTest, DepthLimitDetectorSleepTest) {
  DepthLimitDetectorRuntimeControl control;
  control.max_sql_bytes = 6;
  DepthLimitDetectorTestCase test_case = DepthLimitDetectorTestCase{
      .depth_limit_test_case_name = "just_number",
      .depth_limit_template =
          DepthLimitDetectorTemplate({DepthLimitDetectorDepthNumber()}),
  };

  // Prove that we run the test many times if we don't have a time limit.
  auto results = RunDepthLimitDetectorTestCase(
      test_case, [](std::string_view number) { return absl::OkStatus(); },
      control);
  EXPECT_EQ(results.depth_limit_detector_return_conditions.size(), 1);
  EXPECT_GT(results.depth_limit_detector_return_conditions[0].ending_depth,
            100000);
  EXPECT_LT(results.depth_limit_detector_return_conditions[0].ending_depth,
            1000000);

  // Prove we stop the test if we have a time limit.
  control.max_probing_duration = absl::Seconds(3);
  auto sleep_results = RunDepthLimitDetectorTestCase(
      test_case,
      [](std::string_view number) {
        int64_t i;
        ABSL_CHECK(absl::SimpleAtoi(number, &i));
        absl::SleepFor(absl::Seconds(i));
        return absl::OkStatus();
      },
      control);
  EXPECT_EQ(sleep_results.depth_limit_detector_return_conditions.size(), 1);
  EXPECT_LT(
      sleep_results.depth_limit_detector_return_conditions[0].ending_depth, 10);
}

TEST(DepthLimitDetectorTest, DepthLimitDetectorPayloadTest) {
  // As the testcase just generates a number, 3 bytes corresponds
  // to 3 digits.
  DepthLimitDetectorRuntimeControl runtime_control;
  runtime_control.max_sql_bytes = 3;
  int64_t call_count = 0;
  DepthLimitDetectorTestCase test_case = DepthLimitDetectorTestCase{
      .depth_limit_test_case_name = "payload",
      .depth_limit_template =
          DepthLimitDetectorTemplate({DepthLimitDetectorDepthNumber()}),
  };
  auto results = RunDepthLimitDetectorTestCase(
      test_case,
      [&](std::string_view number) {
        absl::Status ret = absl::ResourceExhaustedError("payload_test");
        ret.SetPayload("test_payload_url",
                       absl::Cord(absl::StrCat("payload", call_count++)));
        return ret;
      },
      runtime_control);
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

  // Very big SQL query that is too big to print out still should have no parser
  // issues, but we shouldn't try to print it out.
  std::string level1000 = DepthLimitDetectorTemplateToString(test_case, 1000);
  ZETASQL_EXPECT_OK(zetasql::ParseStatement(level1000, parser_options, &output));
}

TEST_P(DepthLimitDetectorTemplateTest, ReferenceImplAcceptsInstantiation) {
  const DepthLimitDetectorTestCase& test_case = GetParam();
  ReferenceDriverStatementRunner driver(
      DepthLimitDetectorTestCaseLanguageOptions(test_case));

  for (int i = 1; i <= 5; ++i) {
    std::string sql = DepthLimitDetectorTemplateToString(test_case, i);
    ZETASQL_ASSERT_OK(driver(sql)) << "Reference driver failed at depth " << i << "\n"
                           << sql;
  }

  DepthLimitDetectorTestResult results;

  ABSL_LOG(INFO) << "Disecting reference implementation " << test_case;

  DepthLimitDetectorRuntimeControl runtime_control;
  runtime_control.max_probing_duration =
      absl::GetFlag(FLAGS_depth_limit_detector_reference_max_probing_duration);

    results = RunDepthLimitDetectorTestCase(
        test_case, [&](std::string_view sql) { return driver(sql); },
        runtime_control);

  ABSL_LOG(INFO) << "Disection finished " << results;
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

  RecordProperty("max_successful_tokens", parse_tokens.size());
  RecordProperty("max_successful_bytes", last_successful_sql.size());

  for (int64_t i = 1; i < results.depth_limit_detector_return_conditions.size();
       ++i) {
    EXPECT_TRUE(absl::IsResourceExhausted(
        results.depth_limit_detector_return_conditions[i].return_status))
        << results.depth_limit_detector_return_conditions[i];
  }
}

TEST_P(DepthLimitDetectorTemplateTest, LanguageOptionsAllNeeded) {
  const DepthLimitDetectorTestCase& test_case = GetParam();
  const LanguageOptions options =
      DepthLimitDetectorTestCaseLanguageOptions(test_case);
  std::string sql = DepthLimitDetectorTemplateToString(test_case, 5);
  for (LanguageFeature f : options.GetEnabledLanguageFeatures()) {
    LanguageOptions without = options;
    without.DisableLanguageFeature(f);
    ReferenceDriverStatementRunner driver(without);
    absl::Status status = driver(sql);
    EXPECT_FALSE(status.ok())
        << "Language option " << LanguageFeature_Name(f)
        << " is not needed to run " << test_case << " example " << sql;
  }
}

INSTANTIATE_TEST_CASE_P(DepthLimitDetectorTest, DepthLimitDetectorTemplateTest,
                        ::testing::ValuesIn(AllDepthLimitDetectorTestCases()),
                        ::testing::PrintToStringParamName());

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
