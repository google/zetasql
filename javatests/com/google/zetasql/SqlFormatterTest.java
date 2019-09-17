/*
 * Copyright 2019 ZetaSQL Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.google.zetasql;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;



import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)

public class SqlFormatterTest {

  @Test
  public void testFormatSql_Query() throws Exception {
    String formatted =
        new SqlFormatter().formatSql("SELECT foo, bar from some_table where something limit 1;");
    assertThat(formatted)
        .isEqualTo(
            ""
                + "SELECT\n"
                + "  foo,\n"
                + "  bar\n"
                + "FROM\n"
                + "  some_table\n"
                + "WHERE\n"
                + "  something\n"
                + "LIMIT 1;");
  }

  @Test
  public void testFormatSql_CreateFunction() throws Exception {
    String formatted =
        new SqlFormatter().formatSql("CREATE FUNCTION foo(INT64 a) RETURNS INT64 AS ( a );");
    assertThat(formatted).isEqualTo("CREATE FUNCTION foo(INT64 a)\nRETURNS INT64 AS (\n  a\n);");
  }

  @Test
  public void testFormatSql_InvalidInput_Throws() throws Exception {
    try {
      new SqlFormatter().formatSql("SEL 1;");
      fail("Formatter should've thrown");
    } catch (SqlException e) {
      assertThat(e).hasMessageThat().contains("Syntax error");
    }
  }
}
