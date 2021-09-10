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

#include "zetasql/public/parse_helpers.h"

#include <vector>

#include "zetasql/common/status_payload_utils.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/match.h"
#include "zetasql/base/status.h"

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

namespace zetasql {

// Test cases that are valid for both IsValidStatementSyntax() and
// IsValidNextStatementSyntax().
struct ValidTestCase {
  // The SQL string to test
  std::string sql;

  // The following are only relevant for IsValidNextStatementSyntax() calls,
  // and indicate the expected return values for <at_end_of_input> and
  // <parse_resume_location>.byte_position().
  bool at_end_of_input;
  int byte_position;

  // Only relevant for GetNextStatementKind() calls.
  ResolvedNodeKind statement_kind;
};

std::vector<ValidTestCase> GetValidTestCases() {
  return {
      {"SELECT * FROM T", true, 15, RESOLVED_QUERY_STMT},
      {"SELECT * FROM T;", true, 16, RESOLVED_QUERY_STMT},

      // whitespace after the ';'
      {"SELECT * FROM T;   ", true, 19, RESOLVED_QUERY_STMT},

      // comments after the ';'
      {"SELECT * FROM T;  -- comments ", true, 30, RESOLVED_QUERY_STMT},
      {"SELECT * FROM T;  -- comments ;", true, 31, RESOLVED_QUERY_STMT},

      {"WITH T AS (SELECT 1) SELECT * FROM T", true, 36, RESOLVED_QUERY_STMT},
      {"CREATE TABLE FOO AS SELECT * FROM T", true, 35,
       RESOLVED_CREATE_TABLE_AS_SELECT_STMT},

      // Tests with newlines
      {"SELECT\n*\nFROM\nT\n -- comment;   ", true, 31, RESOLVED_QUERY_STMT},
      {"SELECT\n*\nFROM\nT\nblah", true, 20, RESOLVED_QUERY_STMT},

      // Test with comments and newline
      {"-- comment\nSELECT * FROM T; --comment\n--comment\n--comment", true, 57,
       RESOLVED_QUERY_STMT},
  };
}

// Test cases that are invalid for both IsValidStatementSyntax() and
// IsValidNextStatementSyntax().
struct ErrorTestCase {
  std::string sql;
  std::string expected_error_substring;
  ResolvedNodeKind statement_kind;
};

std::vector<ErrorTestCase> GetInvalidSyntaxTestCases() {
  return {
      // Normal Syntax errors.
      {"", "Syntax error: Unexpected end of statement", RESOLVED_LITERAL},
      {";", "Syntax error", RESOLVED_LITERAL},
      {"-- bah bah bah COMMENTS!", "Syntax error", RESOLVED_LITERAL},
      {"-- bah bah bah COMMENTS!;", "Syntax error", RESOLVED_LITERAL},
      {"blah", "Syntax error", RESOLVED_LITERAL},
      {"SELECT * FROM T ORDER BY bar with crap on the end", "Syntax error",
       RESOLVED_QUERY_STMT},
      {"SELECT\n*\nFROM\nT\nORDER\nBY\nT.foo\nblah", "Syntax error",
       RESOLVED_QUERY_STMT},
      // An error produced by the parser that is not a 'syntax error'.
      {"SELECT x FROM UNNEST(SELECT y+z) AS k WITH OFFSET pos",
       "The argument to UNNEST is an expression", RESOLVED_QUERY_STMT},
      // parse_helpers APIs do not support script statements.
      {"IF TRUE THEN select 3; end if", "Syntax error", RESOLVED_LITERAL},
  };
}

// Test cases that valid for IsValidNextStatementSyntax(), but invalid
// for IsValidStatementSyntax().  The difference is that
// IsValidStatementSyntax() produces an error if there is trailing garbage
// after the semicolon that is not a comment or whitespace.  In contrast,
// IsValidNextStatementSyntax does not produce an error, but rather returns
// that the next statement has valid syntax and resets the ParseResumeLocation
// to point at the trailing garbage.
//
// For simplicity, all of these test cases are "Syntax error" test cases
// for IsValidStatementSyntax(), and all of these test cases return
// <at_end_of_input> false for IsValidNextStatementSyntax().
struct OtherTestCase {
  // The SQL string to test
  std::string sql;

  // The <byte_position> is only relevant for IsValidNextStatementSyntax()
  // calls, and indicates the expected return value for
  // <parse_resume_location>.byte_position().
  int byte_position;

  // The type of the first statement in the query string.  Only relevant
  // for tests which call GetNextStatementKind().
  ResolvedNodeKind statement_kind;
};

std::vector<OtherTestCase> GetOtherTestCases() {
  return {
      {"SELECT * FROM T;  -- comments \n;", 16, RESOLVED_QUERY_STMT},
      {"SELECT * FROM T;  -- comments\n SELECT * FROM T;", 16,
       RESOLVED_QUERY_STMT},
      {"SELECT * FROM T;  SELECT * FROM T", 16, RESOLVED_QUERY_STMT},
      {"SELECT * FROM T;  SELECT * FROM T;", 16, RESOLVED_QUERY_STMT},
      {"SELECT * FROM T;  WITH crap on the end ", 16, RESOLVED_QUERY_STMT},
      {"SELECT * FROM T;  more crap ", 16, RESOLVED_QUERY_STMT},

      // Test with comments and newline
      {"--- comment\nSELECT * FROM T; blah", 28, RESOLVED_QUERY_STMT},
      {"--- comment\nSELECT * FROM T; --comment\nblah", 28,
       RESOLVED_QUERY_STMT},
  };
}

TEST(IsValidStatementSyntaxTest, BasicStatements) {
  for (const ValidTestCase& valid_test_case : GetValidTestCases()) {
    ZETASQL_EXPECT_OK(IsValidStatementSyntax(valid_test_case.sql,
                                     ERROR_MESSAGE_WITH_PAYLOAD))
        << valid_test_case.sql;
  }
  for (const ErrorTestCase& invalid_test_case : GetInvalidSyntaxTestCases()) {
    const absl::Status status =
        IsValidStatementSyntax(invalid_test_case.sql,
                               ERROR_MESSAGE_WITH_PAYLOAD);
    EXPECT_THAT(
        status,
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr(invalid_test_case.expected_error_substring)));
  }
  for (const OtherTestCase& other_test_case : GetOtherTestCases()) {
    const absl::Status status =
        IsValidStatementSyntax(other_test_case.sql,
                               ERROR_MESSAGE_WITH_PAYLOAD);
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                                 HasSubstr("Syntax error")));
  }

  // Test that we get an error location payload from the error Status.
  absl::Status status =
      IsValidStatementSyntax("SELECT * FROM oops I did it again",
                             ERROR_MESSAGE_WITH_PAYLOAD);
  // The syntax error location is at the 'did', since 'oops' is interpreted
  // as a table name and 'I' is interpreted as the alias.
  EXPECT_EQ(
      internal::StatusToString(status),
      "generic::invalid_argument: Syntax error: Expected end of input but "
      "got identifier "
      "\"did\" [zetasql.ErrorLocation] { line: 1 column: 22 }");

  status = IsValidStatementSyntax("SELECT * FROM oops I did it again",
                                  ERROR_MESSAGE_ONE_LINE);
  EXPECT_EQ(
      internal::StatusToString(status),
      "generic::invalid_argument: Syntax error: Expected end of input but "
      "got identifier "
      "\"did\" [at 1:22]");

  status = IsValidStatementSyntax("SELECT * FROM oops I did it again",
                                  ERROR_MESSAGE_MULTI_LINE_WITH_CARET);
  EXPECT_EQ(
      internal::StatusToString(status),
      "generic::invalid_argument: Syntax error: Expected end of input but "
      "got identifier "
      "\"did\" [at 1:22]\n"
      "SELECT * FROM oops I did it again\n"
      "                     ^");
}

TEST(IsValidNextStatementSyntaxTest, BasicStatements) {
  for (const ValidTestCase& valid_test_case : GetValidTestCases()) {
    ParseResumeLocation parse_resume_location =
        ParseResumeLocation::FromString(valid_test_case.sql);
    bool at_end_of_input;
    ZETASQL_EXPECT_OK(IsValidNextStatementSyntax(
        &parse_resume_location, ERROR_MESSAGE_WITH_PAYLOAD, &at_end_of_input))
        << valid_test_case.sql;
    // Validate that the updated ParseResumeLocation byte location is what
    // we expect.
    EXPECT_EQ(parse_resume_location.byte_position(),
              valid_test_case.byte_position) << valid_test_case.sql;
    EXPECT_EQ(at_end_of_input, valid_test_case.at_end_of_input)
         << valid_test_case.sql;
  }
  for (const ErrorTestCase& invalid_test_case : GetInvalidSyntaxTestCases()) {
    ParseResumeLocation parse_resume_location =
        ParseResumeLocation::FromString(invalid_test_case.sql);
    bool at_end_of_input;
    const absl::Status status = IsValidNextStatementSyntax(
        &parse_resume_location, ERROR_MESSAGE_WITH_PAYLOAD, &at_end_of_input);
    EXPECT_THAT(status,
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr(invalid_test_case.expected_error_substring)))
        << invalid_test_case.sql;
  }
  for (const OtherTestCase& other_test_case : GetOtherTestCases()) {
    ParseResumeLocation parse_resume_location =
        ParseResumeLocation::FromString(other_test_case.sql);
    bool at_end_of_input;
    ZETASQL_EXPECT_OK(IsValidNextStatementSyntax(
        &parse_resume_location, ERROR_MESSAGE_WITH_PAYLOAD, &at_end_of_input))
        << other_test_case.sql;
    // Validate that the updated ParseResumeLocation byte location is what
    // we expect.
    EXPECT_EQ(parse_resume_location.byte_position(),
              other_test_case.byte_position) << other_test_case.sql;
    EXPECT_FALSE(at_end_of_input) << other_test_case.sql;
  }

  // Test that we get an error location payload from the error Status.
  ParseResumeLocation parse_resume_location =
      ParseResumeLocation::FromString("SELECT * FROM oops I did it again");
  bool at_end_of_input;
  absl::Status status =
      IsValidNextStatementSyntax(&parse_resume_location,
                                 ERROR_MESSAGE_WITH_PAYLOAD, &at_end_of_input);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("Syntax error")));
  EXPECT_THAT(internal::StatusToString(status),
              HasSubstr("[zetasql.ErrorLocation] { line: 1 column: 22 }"));

  // Test where the ParseResumeLocation is in the middle of a string.
  parse_resume_location = ParseResumeLocation::FromString(
      "some invalid stuff... SELECT * FROM T;  some more stuff");
  parse_resume_location.set_byte_position(22);
  status = IsValidNextStatementSyntax(
      &parse_resume_location, ERROR_MESSAGE_WITH_PAYLOAD, &at_end_of_input);
  ZETASQL_EXPECT_OK(status);
  EXPECT_EQ(parse_resume_location.byte_position(), 38);
  EXPECT_FALSE(at_end_of_input);
}

TEST(IsValidNextStatementSyntaxTest, MultiStatementsTest) {
  // Test that loops through all the statements in a multi-statement string
  // to check syntax.
  ParseResumeLocation parse_resume_location = ParseResumeLocation::FromString(
      "SELECT * FROM T; SELECT * FROM U; SELECT * FROM V; CREATE TABLE T AS \n"
      "SELECT * FROM T1;  SELECT * FROM T");
  bool at_end_of_input = false;
  int statement_count = 0;
  while (!at_end_of_input) {
    ZETASQL_EXPECT_OK(IsValidNextStatementSyntax(
        &parse_resume_location, ERROR_MESSAGE_WITH_PAYLOAD, &at_end_of_input));
    statement_count++;
  }
  EXPECT_EQ(statement_count, 5);

  // Similar to the previous test, but the third statement has a syntax error.
  parse_resume_location = ParseResumeLocation::FromString(
      "SELECT * FROM T; SELECT * FROM U; SELECTS FROMM V; CREATE TABLE T AS \n"
      "SELECT * FROM T1;  SELECT * FROM T");
  at_end_of_input = false;
  statement_count = 0;
  absl::Status status;
  while (status.ok() && !at_end_of_input) {
    status = IsValidNextStatementSyntax(
        &parse_resume_location, ERROR_MESSAGE_MULTI_LINE_WITH_CARET,
        &at_end_of_input);
    statement_count++;
  }
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(internal::StatusToString(status), HasSubstr("[at 1:35]\nSELECT"))
      << status;
  EXPECT_EQ(statement_count, 3);
}

TEST(GetNextStatementKindAndPropertiesTest, BasicStatements) {
  for (const ValidTestCase& valid_test_case : GetValidTestCases()) {
    ParseResumeLocation parse_resume_location =
        ParseResumeLocation::FromString(valid_test_case.sql);
    ResolvedNodeKind kind = GetNextStatementKind(parse_resume_location);
    EXPECT_EQ(ResolvedNodeKind_Name(kind),
              ResolvedNodeKind_Name(valid_test_case.statement_kind))
        << valid_test_case.sql;
    StatementProperties statement_properties;
    ZETASQL_ASSERT_OK(GetNextStatementProperties(parse_resume_location,
                                         LanguageOptions(),
                                         &statement_properties))
        << valid_test_case.sql;
    ASSERT_EQ(ResolvedNodeKind_Name(kind),
              ResolvedNodeKind_Name(statement_properties.node_kind))
        << valid_test_case.sql;
  }
  for (const ErrorTestCase& invalid_test_case : GetInvalidSyntaxTestCases()) {
    ParseResumeLocation parse_resume_location =
        ParseResumeLocation::FromString(invalid_test_case.sql);
    ResolvedNodeKind kind = GetNextStatementKind(parse_resume_location);
    EXPECT_EQ(ResolvedNodeKind_Name(kind),
              ResolvedNodeKind_Name(invalid_test_case.statement_kind))
        << invalid_test_case.sql;
    StatementProperties statement_properties;
    ZETASQL_ASSERT_OK(GetNextStatementProperties(parse_resume_location,
                                         LanguageOptions(),
                                         &statement_properties))
        << invalid_test_case.sql;
    ASSERT_EQ(ResolvedNodeKind_Name(kind),
              ResolvedNodeKind_Name(statement_properties.node_kind))
        << invalid_test_case.sql;
  }
  for (const OtherTestCase& other_test_case : GetOtherTestCases()) {
    ParseResumeLocation parse_resume_location =
        ParseResumeLocation::FromString(other_test_case.sql);
    ResolvedNodeKind kind = GetNextStatementKind(parse_resume_location);
    EXPECT_EQ(ResolvedNodeKind_Name(kind),
              ResolvedNodeKind_Name(other_test_case.statement_kind))
        << other_test_case.sql;
    StatementProperties statement_properties;
    ZETASQL_ASSERT_OK(GetNextStatementProperties(parse_resume_location,
                                         LanguageOptions(),
                                         &statement_properties))
        << other_test_case.sql;
    ASSERT_EQ(ResolvedNodeKind_Name(kind),
              ResolvedNodeKind_Name(statement_properties.node_kind))
        << other_test_case.sql;
  }
}

struct StatementPropertiesTestCase {
  // The SQL string to test
  std::string sql;

  // The expected properties of <sql>.
  ResolvedNodeKind statement_kind;    // The statement's kind
  StatementProperties::StatementCategory statement_category;  // DDL, DML, etc.
  bool is_create_temp_object;         // CREATE TEMP TABLE, etc.
  std::map<std::string, std::string> hint_map;  // Statement level hints.
};

std::vector<StatementPropertiesTestCase> GetStatementPropertiesTestCases() {
  return {
      {
          "SELECT * FROM T",
          RESOLVED_QUERY_STMT,
          StatementProperties::SELECT,
          false,
          {},
      },
      {
          "CREATE TABLE T AS SELECT 1",
          RESOLVED_CREATE_TABLE_AS_SELECT_STMT,
          StatementProperties::DDL,
          false,
          {},
      },
      {
          "CREATE TEMP TABLE T (A INT64);",
          RESOLVED_CREATE_TABLE_STMT,
          StatementProperties::DDL,
          true,
          {},
      },
      {
          "INSERT INTO FOO VALUES (1,2,3)",
          RESOLVED_INSERT_STMT,
          StatementProperties::DML,
          false,
          {},
      },
      {
          "EXPORT DATA OPTIONS",
          RESOLVED_EXPORT_DATA_STMT,
          StatementProperties::OTHER,
          false,
          {},
      },
      {
          "@{a = 4, b = 1 +2} SELECT * FROM T",
          RESOLVED_QUERY_STMT,
          StatementProperties::SELECT,
          false,
          {{"a", "4"}, {"b", "1 +2"}},
      },
      {
          "@{b = 5, a = 1 +2} SELECT * FROM T",
          RESOLVED_QUERY_STMT,
          StatementProperties::SELECT,
          false,
          {{"a", "1 +2"}, {"b", "5"}},
      },
      {
          "@2 SELECT * FROM T",
          RESOLVED_QUERY_STMT,
          StatementProperties::SELECT,
          false,
          {},
      },
      {
          "@{b = 9} CREATE TABLE T AS SELECT 1",
          RESOLVED_CREATE_TABLE_AS_SELECT_STMT,
          StatementProperties::DDL,
          false,
          {{"b", "9"}},
      },
      {
          "  /**/ @{b = 9} /**/ CREATE TABLE T AS SELECT 1",
          RESOLVED_CREATE_TABLE_AS_SELECT_STMT,
          StatementProperties::DDL,
          false,
          {{"b", "9"}},
      },
      {"RETURN 'foo'", RESOLVED_LITERAL, StatementProperties::OTHER, false, {}},
      {R"(IF TRUE THEN
            SELECT 5;
          END IF;)",
       RESOLVED_LITERAL,
       StatementProperties::OTHER,
       false,
       {}},
      {R"(LOOP
          END LOOP;)",
       RESOLVED_LITERAL,
       StatementProperties::OTHER,
       false,
       {}},
      {R"(SET (a, b) = (1, 2))",
       RESOLVED_LITERAL,
       StatementProperties::OTHER,
       false,
       {}},
      {R"(L1: REPEAT UNTIL TRUE
          END REPEAT;)",
       RESOLVED_LITERAL,
       StatementProperties::OTHER,
       false,
       {}},
  };
}

TEST(GetNextStatementPropertiesTest, BasicStatements) {
  for (const StatementPropertiesTestCase& test_case
           : GetStatementPropertiesTestCases()) {
    ParseResumeLocation parse_resume_location =
        ParseResumeLocation::FromString(test_case.sql);

    StatementProperties statement_properties;
    ZETASQL_ASSERT_OK(GetNextStatementProperties(parse_resume_location,
                                         LanguageOptions(),
                                         &statement_properties))
        << test_case.sql;

    EXPECT_EQ(ResolvedNodeKind_Name(test_case.statement_kind),
              ResolvedNodeKind_Name(statement_properties.node_kind))
        << test_case.sql;
    EXPECT_EQ(test_case.statement_category,
              statement_properties.statement_category)
        << test_case.sql;
    EXPECT_EQ(test_case.is_create_temp_object,
              statement_properties.is_create_temporary_object)
        << test_case.sql;

    // Create an ordered map for comparison.
    std::map<std::string, std::string> statement_properties_hint_map(
        statement_properties.statement_level_hints.begin(),
        statement_properties.statement_level_hints.end());

    EXPECT_EQ(test_case.hint_map.size(), statement_properties_hint_map.size())
        << test_case.sql;

    std::string expected_hints;
    for (const auto& map_entry : test_case.hint_map) {
      absl::StrAppend(&expected_hints, map_entry.first, "=", map_entry.second,
                      ";");
    }
    std::string fetched_hints;
    for (const auto& map_entry : statement_properties_hint_map) {
      absl::StrAppend(&fetched_hints, map_entry.first, "=", map_entry.second,
                    ";");
    }
    EXPECT_EQ(expected_hints, fetched_hints) << test_case.sql;

    // Ensure that GetNextStatementKind returns the same kind.
    ResolvedNodeKind kind = GetNextStatementKind(parse_resume_location);
    EXPECT_EQ(ResolvedNodeKind_Name(kind),
              ResolvedNodeKind_Name(statement_properties.node_kind))
        << test_case.sql;
  }
}

}  // namespace zetasql
