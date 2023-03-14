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

#include "zetasql/parser/unparser.h"

#include <memory>
#include <string>
#include <string_view>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/parser.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"

namespace zetasql {

using testing::NotNull;

static void CompareParseTrees(const ASTNode* expected_tree,
                              const ASTNode* unparsed_tree,
                              absl::string_view expected_string,
                              absl::string_view unparsed_string) {
  std::string expected = expected_tree->DebugString();
  std::string from_unparsed = unparsed_tree->DebugString();
  EXPECT_EQ(expected, from_unparsed)
      << "Different trees:\n"
      << "\nfor unparsed vs. original tree.\nOriginal query:\n"
      << expected_string << "\nUnparsed query:\n"
      << unparsed_string << "\nExpected Tree:\n"
      << expected << "\nTree for unparsed sql:\n"
      << from_unparsed;
}

class UnparserQueryTest : public testing::TestWithParam<std::string_view> {};

INSTANTIATE_TEST_SUITE_P(
    UnparserQueryTestSuite, UnparserQueryTest,
    testing::Values(
        // These testcases must as be formatted as from the unparser
        "SELECT\n"
        "  *\n"
        "FROM\n"
        "  foo\n",
        "SELECT\n"
        "  (0).`0`\n"));

TEST_P(UnparserQueryTest, QueryStatementParsesThenUnparses) {
  std::string_view query_string = GetParam();
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_EXPECT_OK(ParseStatement(query_string, ParserOptions(), &parser_output));
  ASSERT_THAT(parser_output->statement(), NotNull());
  std::string unparsed_string = Unparse(parser_output->node());

  std::unique_ptr<ParserOutput> unparsed_query_parser_output;
  ZETASQL_EXPECT_OK(ParseStatement(unparsed_string, ParserOptions(),
                           &unparsed_query_parser_output));
  // This checks that the input and output are effectively the same, or least
  // all tokens have the exact same source locations.
  CompareParseTrees(parser_output->node(), unparsed_query_parser_output->node(),
                    query_string, unparsed_string);
}

TEST(TestUnparser, ExpressionTest) {
  std::string expression_string(
      "CASE\n"
      "  WHEN a = 5 THEN true\n"
      "END\n");
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_EXPECT_OK(
      ParseExpression(expression_string, ParserOptions(), &parser_output));
  ASSERT_THAT(parser_output.get(), NotNull());
  ASSERT_THAT(parser_output->expression(), NotNull());
  std::string unparsed_expression_string = Unparse(parser_output->expression());
  // Cannot generally do string equality because of capitalization and white
  // space issues, so we will reparse and also compare the parse trees.
  EXPECT_EQ(expression_string, unparsed_expression_string);
  std::unique_ptr<ParserOutput> unparsed_expression_parser_output;
  ZETASQL_EXPECT_OK(ParseExpression(unparsed_expression_string, ParserOptions(),
                            &unparsed_expression_parser_output));
  CompareParseTrees(parser_output->expression(),
                    unparsed_expression_parser_output->expression(),
                    expression_string, unparsed_expression_string);
}

}  // namespace zetasql
