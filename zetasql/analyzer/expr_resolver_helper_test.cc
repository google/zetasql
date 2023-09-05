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

#include "zetasql/analyzer/expr_resolver_helper.h"

#include <memory>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/templated_sql_tvf.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"

namespace zetasql {

using ::zetasql_base::testing::StatusIs;

TEST(ResolvedTVFArgTest, GetScan) {
  ResolvedTVFArg arg;
  EXPECT_FALSE(arg.IsScan());
  EXPECT_THAT(arg.GetScan(), StatusIs(absl::StatusCode::kInternal));
}

TEST(GetAliasForExpression, ASTIdentifier) {
  ASTIdentifier identifier_node;
  IdString identifier = IdString::MakeGlobal("foo");
  identifier_node.SetIdentifier(identifier);
  EXPECT_EQ(GetAliasForExpression(&identifier_node).ToString(), "foo");
}

TEST(GetAliasForExpression, ASTPathExpression) {
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_ASSERT_OK(
      ParseExpression("a.b", ParserOptions{LanguageOptions{}}, &parser_output));
  const ASTPathExpression* path =
      parser_output->expression()->GetAsOrDie<ASTPathExpression>();
  EXPECT_EQ(GetAliasForExpression(path).ToString(), "b");
}

TEST(GetAliasForExpression, ASTDotIdentifier) {
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_ASSERT_OK(ParseExpression("foo[3].array", ParserOptions{LanguageOptions{}},
                            &parser_output));
  const ASTDotIdentifier* dot_identifier =
      parser_output->expression()->GetAsOrDie<ASTDotIdentifier>();
  EXPECT_EQ(GetAliasForExpression(dot_identifier).ToString(), "array");
}

// `GetAliasForExpression` does not know how to assign aliases, so an empty
// IdString will be returned.
TEST(GetAliasForExpression, OtherASTNodeTypes) {
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_ASSERT_OK(ParseExpression("(SELECT 1)", ParserOptions{LanguageOptions{}},
                            &parser_output));
  EXPECT_EQ(GetAliasForExpression(parser_output->expression()).ToString(), "");
}

}  // namespace zetasql
