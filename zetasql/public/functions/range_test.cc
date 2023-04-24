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

#include "zetasql/public/functions/range.h"

#include <optional>
#include <string>

#include "zetasql/base/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {

class GetBoundariesTest : public ::testing::Test {
 public:
  void TestGetBoundaries(const absl::string_view input,
                         const std::optional<std::string>& expected_start,
                         const std::optional<std::string>& expected_end) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(const auto boundaries, ParseRangeBoundaries(input));

    EXPECT_EQ(expected_start, boundaries.start);
    EXPECT_EQ(expected_end, boundaries.end);
  }

  void TestGetBoundariesError(const absl::string_view input,
                              const absl::string_view expected_error_message,
                              absl::StatusCode expected_status_code =
                                  absl::StatusCode::kInvalidArgument) {
    EXPECT_THAT(
        ParseRangeBoundaries(input),
        zetasql_base::testing::StatusIs(expected_status_code,
                                  testing::HasSubstr(expected_error_message)))
        << input;
  }
};

TEST_F(GetBoundariesTest, ValidRangeLiterals) {
  TestGetBoundaries("[2022-01-01, 2022-02-02)", "2022-01-01", "2022-02-02");
  TestGetBoundaries(
      "[2022-09-13 16:36:11.000000001, 2022-09-13 16:37:11.000000001)",
      "2022-09-13 16:36:11.000000001", "2022-09-13 16:37:11.000000001");
  TestGetBoundaries(
      "[0001-01-01 00:00:00+02, 9999-12-31 23:59:59.999999999+02)",
      "0001-01-01 00:00:00+02", "9999-12-31 23:59:59.999999999+02");
}

TEST_F(GetBoundariesTest, RangeLiteralsWithUnbounded) {
  TestGetBoundaries("[UNBOUNDED, 2022-02-02)", std::nullopt, "2022-02-02");
  TestGetBoundaries("[2022-09-13 16:36:11.000000001, unbounded)",
                    "2022-09-13 16:36:11.000000001", std::nullopt);
  TestGetBoundaries("[Unbounded, Unbounded)", std::nullopt, std::nullopt);
  TestGetBoundaries("[unbounded, UNBOUNDED)", std::nullopt, std::nullopt);
  TestGetBoundaries("[NULL, null)", std::nullopt, std::nullopt);
  TestGetBoundaries("[UnBoUnDeD, NuLl)", std::nullopt, std::nullopt);
}

TEST_F(GetBoundariesTest, RangeLiteralsWithNull) {
  // Test ranges with NULL. NULL could be used instead of UNBOUNDED
  TestGetBoundaries("[NULL, 2022-02-02)", std::nullopt, "2022-02-02");
  TestGetBoundaries("[2022-09-13 16:36:11.000000001, null)",
                    "2022-09-13 16:36:11.000000001", std::nullopt);
  TestGetBoundaries("[Null, Null)", std::nullopt, std::nullopt);
}

TEST_F(GetBoundariesTest, RangeLiteralsValueWithIncorrectNumberOfParts) {
  TestGetBoundariesError("[2022-01-01, 2022-02-02, 2022-03-02)",
                         R"(with two parts, START and END, divided with ", ")");
  TestGetBoundariesError("[)",
                         R"(with two parts, START and END, divided with ", ")");
  TestGetBoundariesError("[2022-01-01)",
                         R"(with two parts, START and END, divided with ", ")");
}

TEST_F(GetBoundariesTest,
       SemanticallyIncorrectButSyntacticallyCorrectRangeLiterals) {
  TestGetBoundaries("[01/01/2022, 02/02/2022)", "01/01/2022", "02/02/2022");
  TestGetBoundaries("[, )", "", "");
  TestGetBoundaries("[2022-01-01 00:00:00.0, )", "2022-01-01 00:00:00.0", "");
  TestGetBoundaries("[, 2022-01-01)", "", "2022-01-01");
  TestGetBoundaries("[0000-01-01, 0000-01-02)", "0000-01-01", "0000-01-02");
}

TEST_F(GetBoundariesTest, RangeLiteralsWithIncorrectBrackets) {
  TestGetBoundariesError("[2022-01-01, 2022-02-02]",
                         "formatted exactly as [START, END)");
  TestGetBoundariesError("(2022-01-01, 2022-02-02]",
                         "formatted exactly as [START, END)");
  TestGetBoundariesError("(2022-01-01, 2022-02-02)",
                         "formatted exactly as [START, END)");
  TestGetBoundariesError("2022-01-01, 2022-02-02)",
                         "formatted exactly as [START, END)");
  TestGetBoundariesError("[2022-01-01, 2022-02-02",
                         "formatted exactly as [START, END)");
}

TEST_F(GetBoundariesTest, GetBoundariesStrictFormattingSeparator) {
  // Range literal must have exactly one space after , and no other spaces
  TestGetBoundariesError("[2022-01-01,2022-02-02)",
                         R"(with two parts, START and END, divided with ", ")");

  // Unless it's overridden with the argument
  ZETASQL_ASSERT_OK_AND_ASSIGN(const auto boundaries,
                       ParseRangeBoundaries("[2022-01-01,2022-02-02)",
                                            /*strict_formatting=*/false));
  EXPECT_EQ("2022-01-01", boundaries.start);
  EXPECT_EQ("2022-02-02", boundaries.end);
}

TEST_F(GetBoundariesTest, GetBoundariesStrictFormattingLeadingOrTrailingSpace) {
  // Range literal start and end must not have any leading or trailing spaces
  TestGetBoundariesError("[ 2022-01-01, 2022-02-02)",
                         "START having no leading or trailing spaces");
  TestGetBoundariesError("[2022-01-01  , 2022-02-02)",
                         "START having no leading or trailing spaces");
  TestGetBoundariesError("[2022-01-01,  2022-02-02)",
                         "END having no leading or trailing spaces");
  TestGetBoundariesError("[2022-01-01, 2022-02-02 )",
                         "END having no leading or trailing spaces");

  // Unless it's overridden with the argument
  ZETASQL_ASSERT_OK_AND_ASSIGN(const auto boundaries,
                       ParseRangeBoundaries("[ 2022-01-01  ,  2022-02-02 )",
                                            /*strict_formatting=*/false));
  EXPECT_EQ("2022-01-01", boundaries.start);
  EXPECT_EQ("2022-02-02", boundaries.end);
}

}  // namespace zetasql
