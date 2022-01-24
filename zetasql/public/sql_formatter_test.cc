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

#include "zetasql/public/sql_formatter.h"

#include "zetasql/base/testing/status_matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {

using testing::_;
using testing::HasSubstr;
using zetasql_base::testing::StatusIs;

namespace {

TEST(SqlFormatterTest, ValidSingleStatement) {
  std::string formatted_sql;

  // Without semicolon.
  ZETASQL_ASSERT_OK(FormatSql("select a", &formatted_sql));
  EXPECT_EQ("SELECT\n"
            "  a;",
            formatted_sql);

  // With semicolon and trailing whitespaces.
  ZETASQL_ASSERT_OK(FormatSql(" select a ; \t ", &formatted_sql));
  EXPECT_EQ("SELECT\n"
            "  a;",
            formatted_sql);

  // With semicolon and trailing comment.
  ZETASQL_ASSERT_OK(FormatSql(" select a ; # foo", &formatted_sql));
  EXPECT_EQ("SELECT\n"
            "  a;",
            formatted_sql);
}

TEST(SqlFormatterTest, InvalidSingleStatement) {
  // SQLs in this test are returning errors, but we still expect that the
  // formatted_sql is always filled in.
  std::string formatted_sql;

  // Without semicolon.
  EXPECT_THAT(FormatSql("select f1 as a from T having a > 5 having a > 5",
                        &formatted_sql),
              StatusIs(_, HasSubstr("Syntax error: Expected end of input but "
                                    "got keyword HAVING [at 1:36]")));
  EXPECT_EQ("select f1 as a from T having a > 5 having a > 5;",
            formatted_sql);

  // With semicolon as the last char.
  EXPECT_THAT(FormatSql("select f1 as a from T having a > 5 having a > 5;",
                        &formatted_sql),
              StatusIs(_, HasSubstr("Syntax error: Expected end of input but "
                                    "got keyword HAVING [at 1:36]")));
  EXPECT_EQ("select f1 as a from T having a > 5 having a > 5;",
            formatted_sql);

  // With semicolon and trailing whitespaces.
  EXPECT_THAT(FormatSql("select f1 as a from T having a > 5 having a > 5;    ",
                        &formatted_sql),
              StatusIs(_, HasSubstr("Syntax error: Expected end of input but "
                                    "got keyword HAVING [at 1:36]")));
  EXPECT_EQ("select f1 as a from T having a > 5 having a > 5;",
            formatted_sql);

  // With semicolon and trailing comment.
  EXPECT_THAT(
      FormatSql("select f1 as a from T having a > 5 having a > 5; # foo",
                &formatted_sql),
      StatusIs(_,
               HasSubstr(
                   "Syntax error: Expected end of input but got keyword HAVING "
                   "[at 1:36]\n"
                   "select f1 as a from T having a > 5 having a > 5; # foo\n"
                   "                                   ^\n"
                   "Syntax error: Unexpected end of statement [at 1:55]\n"
                   "select f1 as a from T having a > 5 having a > 5; # foo\n"
                   "                                                      ^")));
  EXPECT_EQ("select f1 as a from T having a > 5 having a > 5;",
            formatted_sql);

  // Empty statement.
  EXPECT_THAT(
      FormatSql(";", &formatted_sql),
      StatusIs(_, HasSubstr("Syntax error: Unexpected \";\" [at 1:1]")));
  EXPECT_EQ(";", formatted_sql);

  // Semicolon in string.
  EXPECT_THAT(FormatSql("select ' ; ' as a as b;", &formatted_sql),
              StatusIs(_, HasSubstr("Syntax error: Expected end of input but "
                                    "got keyword AS [at 1:19]")));
  EXPECT_EQ("select ' ; ' as a as b;", formatted_sql);

  EXPECT_THAT(
      FormatSql("select a group by 1 where a < 'xxx;yyy';", &formatted_sql),
      StatusIs(_, HasSubstr("Syntax error: Expected end of input but got "
                            "keyword WHERE [at 1:21]")));
  EXPECT_EQ("select a group by 1 where a < 'xxx;yyy';", formatted_sql);
}

TEST(SqlFormatterTest, ValidMultipleStatements) {
  std::string formatted_sql;

  ZETASQL_ASSERT_OK(FormatSql(" define table t1 (a=1,b=\"a\",c=1.4,d=true) ; "
                      "select a from t1; ", &formatted_sql));
  EXPECT_EQ("DEFINE TABLE t1(a = 1, b = \"a\", c = 1.4, d = true);\n"
            "SELECT\n"
            "  a\n"
            "FROM\n"
            "  t1;",
            formatted_sql);

  ZETASQL_ASSERT_OK(FormatSql("select 1;\n"
                      "select 2", &formatted_sql));
  EXPECT_EQ("SELECT\n"
            "  1;\n"
            "SELECT\n"
            "  2;",
            formatted_sql);
}

TEST(SqlFormatterTest, InvalidMultipleStatements) {
  // SQLs in this test are returning errors, but we still expect that the
  // formatted_sql is always filled in.
  std::string formatted_sql;

  // The second and last statements are formatted, but the other two are passed
  // through as-is since they do not parse successfully.
  EXPECT_THAT(
      FormatSql(
          " drop foo.bar;  define table t1 (a=1,b=\"a\",c=1.4,d=true) ;\n"
          " select sum(f1) as a from T having a > 5 having a > 5;select 1",
          &formatted_sql),
      StatusIs(
          _,
          HasSubstr(
              "foo is not a supported object type [at 1:7]\n"
              " drop foo.bar;  define table t1 (a=1,b=\"a\",c=1.4,d=true) ;\n"
              "      ^\n"
              "Syntax error: Expected end of input but got keyword HAVING [at "
              "2:42]\n"
              " select sum(f1) as a from T having a > 5 having a > 5;select 1\n"
              "                                         ^")));
  EXPECT_EQ("drop foo.bar;\n"
            "DEFINE TABLE t1(a = 1, b = \"a\", c = 1.4, d = true);\n"
            "select sum(f1) as a from T having a > 5 having a > 5;\n"
            "SELECT\n"
            "  1;",
            formatted_sql);

  // The second statement is an invalid empty statement.
  EXPECT_THAT(
      FormatSql("select 1;  ;", &formatted_sql),
      StatusIs(_, HasSubstr("Syntax error: Unexpected \";\" [at 1:12]")));
  EXPECT_EQ("SELECT\n"
            "  1;\n"
            ";",
            formatted_sql);

  // The second statement contains invalid input character '$', which makes
  // GetParseTokens fail. Original sql is returned in this case even if the
  // first statement can be formatted.
  EXPECT_THAT(
      FormatSql("select 1;  select $d ;", &formatted_sql),
      StatusIs(
          _,
          HasSubstr("Syntax error: Illegal input character \"$\" [at 1:19]")));
  EXPECT_EQ("select 1;  select $d ;", formatted_sql);
}

}  // namespace
}  // namespace zetasql
