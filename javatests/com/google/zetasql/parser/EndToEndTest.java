/*
 * Copyright 2019 Google LLC
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

package com.google.zetasql.parser;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.truth.Truth.assertThat;
import static com.google.zetasql.ZetaSQLOptions.LanguageFeature.FEATURE_V_1_3_ALLOW_DASHES_IN_TABLE_NAME;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.Parser;
import com.google.zetasql.SqlException;
import com.google.zetasql.parser.ASTBinaryExpressionEnums.Op;
import com.google.zetasql.parser.ASTJoinEnums.JoinType;
import com.google.zetasql.parser.ASTNodes.ASTAlias;
import com.google.zetasql.parser.ASTNodes.ASTAndExpr;
import com.google.zetasql.parser.ASTNodes.ASTBinaryExpression;
import com.google.zetasql.parser.ASTNodes.ASTExpression;
import com.google.zetasql.parser.ASTNodes.ASTFromClause;
import com.google.zetasql.parser.ASTNodes.ASTGroupBy;
import com.google.zetasql.parser.ASTNodes.ASTGroupingItem;
import com.google.zetasql.parser.ASTNodes.ASTIntLiteral;
import com.google.zetasql.parser.ASTNodes.ASTJoin;
import com.google.zetasql.parser.ASTNodes.ASTLimitOffset;
import com.google.zetasql.parser.ASTNodes.ASTNullLiteral;
import com.google.zetasql.parser.ASTNodes.ASTOnClause;
import com.google.zetasql.parser.ASTNodes.ASTOrderBy;
import com.google.zetasql.parser.ASTNodes.ASTOrderingExpression;
import com.google.zetasql.parser.ASTNodes.ASTPathExpression;
import com.google.zetasql.parser.ASTNodes.ASTQuery;
import com.google.zetasql.parser.ASTNodes.ASTQueryExpression;
import com.google.zetasql.parser.ASTNodes.ASTQueryStatement;
import com.google.zetasql.parser.ASTNodes.ASTRollup;
import com.google.zetasql.parser.ASTNodes.ASTSelect;
import com.google.zetasql.parser.ASTNodes.ASTSelectColumn;
import com.google.zetasql.parser.ASTNodes.ASTSelectList;
import com.google.zetasql.parser.ASTNodes.ASTStatement;
import com.google.zetasql.parser.ASTNodes.ASTStringLiteral;
import com.google.zetasql.parser.ASTNodes.ASTTableExpression;
import com.google.zetasql.parser.ASTNodes.ASTTablePathExpression;
import com.google.zetasql.parser.ASTNodes.ASTWhereClause;
import com.google.zetasql.parser.ASTOrderingExpressionEnums.OrderingSpec;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)

public class EndToEndTest {

  private final LanguageOptions languageOptions = new LanguageOptions();

  @Before
  public void setup() {
    languageOptions.disableAllLanguageFeatures();
  }

  @Test
  public void testQueryStatement1() {
    String queryString = "select 123";

    ASTStatement statement = Parser.parseStatement(queryString, languageOptions);

    ASTQueryStatement queryStatement = assertAndCast(ASTQueryStatement.class, statement);
    ASTQuery query = queryStatement.getQuery();
    ASTQueryExpression queryExpression = query.getQueryExpr();
    ASTSelect select = assertAndCast(ASTSelect.class, queryExpression);
    ASTSelectColumn selectColumn = select.getSelectList().getColumns().get(0);
    ASTExpression expression = selectColumn.getExpression();
    ASTIntLiteral intLiteral = assertAndCast(ASTIntLiteral.class, expression);
    assertThat(intLiteral.getImage()).isEqualTo("123");
  }

  @Test
  public void testQueryStatementInvalid() {
    String queryString = "invalid query string";
    assertThrows(SqlException.class, () -> Parser.parseStatement(queryString, languageOptions));
  }

  @Test
  public void testQueryStatementLanguageOptions1() {
    String queryString = "select 1 from dashed-table-name";
    // FEATURE_V_1_3_ALLOW_DASHES_IN_TABLE_NAME disabled
    assertThrows(SqlException.class, () -> Parser.parseStatement(queryString, languageOptions));
  }

  @Test
  public void testQueryStatementLanguageOptions2() {
    String queryString = "select 1 from dashed-table-name";
    languageOptions.enableLanguageFeature(FEATURE_V_1_3_ALLOW_DASHES_IN_TABLE_NAME);
    try {
      Parser.parseStatement(queryString, languageOptions);
    } catch (SqlException e) {
      throw new AssertionError("LanguageOptions was not respected.", e);
    }
  }

  @Test
  public void testQueryStatement2() {
    String queryString = "select t2.col1 as t2_col1,\n"
        + " t1.col2 as t1_col2,\n"
        + " t1.col3 as t1_col3\n"
        + " from table1 t1 join table2 t2 on t1.col1 = t2.col1\n"
        + " where t1.col2='foo' and t2.col1=5 and t1.col3 is not null\n"
        + " group by rollup(t2_col1, t1_col2, 3)"
        + " order by 1 asc, 2 desc, t1_col3 "
        + " limit 100";

    ASTStatement statement = Parser.parseStatement(queryString, languageOptions);

    ASTQueryStatement queryStatement = assertAndCast(ASTQueryStatement.class, statement);
    ASTQuery query = queryStatement.getQuery();
    ASTQueryExpression queryExpression = query.getQueryExpr();
    ASTSelect select = assertAndCast(ASTSelect.class, queryExpression);

    // Validate selected columns
    ASTSelectList selectList = select.getSelectList();
    ImmutableList<ASTSelectColumn> columns = selectList.getColumns();
    assertThat(columns).hasSize(3);
    // t2.col1 as t2_col1
    ASTSelectColumn selectColumn0 = columns.get(0);
    ASTExpression expression0 = selectColumn0.getExpression();
    ASTPathExpression pathExpression0 = assertAndCast(ASTPathExpression.class, expression0);
    validatePathexpression(pathExpression0, ImmutableList.of("t2", "col1"));
    assertThat(pathExpression0.getParenthesized()).isFalse();
    validateAlias(selectColumn0.getAlias(), "t2_col1");
    // t1.col2 as t1_col2
    ASTSelectColumn selectColumn1 = columns.get(1);
    ASTExpression expression1 = selectColumn1.getExpression();
    ASTPathExpression pathExpression1 = assertAndCast(ASTPathExpression.class, expression1);
    validatePathexpression(pathExpression1, ImmutableList.of("t1", "col2"));
    validateAlias(selectColumn1.getAlias(), "t1_col2");
    // t1.col3 as t1_col3
    ASTSelectColumn selectColumn2 = columns.get(2);
    ASTExpression expression2 = selectColumn2.getExpression();
    ASTPathExpression pathExpression2 = assertAndCast(ASTPathExpression.class, expression2);
    validatePathexpression(pathExpression2, ImmutableList.of("t1", "col3"));
    validateAlias(selectColumn2.getAlias(), "t1_col3");

    // Validate from clause
    ASTFromClause fromClause = select.getFromClause();
    ASTTableExpression tableExpression = fromClause.getTableExpression();
    ASTJoin astJoin = assertAndCast(ASTJoin.class, tableExpression);
    assertThat(astJoin.getJoinType()).isEqualTo(JoinType.DEFAULT_JOIN_TYPE);
    ASTTablePathExpression lhs = assertAndCast(ASTTablePathExpression.class, astJoin.getLhs());
    validatePathexpression(lhs.getPathExpr(), ImmutableList.of("table1"));
    validateAlias(lhs.getAlias(), "t1");

    ASTTablePathExpression rhs = assertAndCast(ASTTablePathExpression.class, astJoin.getRhs());
    validatePathexpression(rhs.getPathExpr(), ImmutableList.of("table2"));
    validateAlias(rhs.getAlias(), "t2");

    // Validate on clause
    ASTOnClause onClause = astJoin.getOnClause();
    ASTBinaryExpression binaryExpression = assertAndCast(ASTBinaryExpression.class,
        onClause.getExpression());
    assertThat(binaryExpression.getOp()).isEqualTo(Op.EQ);
    assertThat(binaryExpression.getIsNot()).isFalse();
    assertThat(binaryExpression.getParenthesized()).isFalse();

    ASTPathExpression onLhs = assertAndCast(ASTPathExpression.class, binaryExpression.getLhs());
    validatePathexpression(onLhs, ImmutableList.of("t1", "col1"));
    ASTPathExpression onRhs = assertAndCast(ASTPathExpression.class, binaryExpression.getRhs());
    validatePathexpression(onRhs, ImmutableList.of("t2", "col1"));

    // Validate where clause
    ASTWhereClause whereClause = select.getWhereClause();
    ASTAndExpr andExpr = assertAndCast(ASTAndExpr.class, whereClause.getExpression());
    ImmutableList<ASTExpression> conjuncts = andExpr.getConjuncts();
    assertThat(conjuncts).hasSize(3);
    // First conjunct: t1.col2='foo'
    ASTBinaryExpression conjunct0 = assertAndCast(ASTBinaryExpression.class, conjuncts.get(0));
    ASTPathExpression conjunct0Lhs = assertAndCast(ASTPathExpression.class, conjunct0.getLhs());
    validatePathexpression(conjunct0Lhs, ImmutableList.of("t1", "col2"));
    ASTStringLiteral conjunct0Rhs = assertAndCast(ASTStringLiteral.class, conjunct0.getRhs());
    assertThat(conjunct0Rhs.getStringValue()).isEqualTo("foo");
    assertThat(conjunct0.getOp()).isEqualTo(Op.EQ);
    assertThat(conjunct0.getIsNot()).isFalse();
    // Second conjunct: t2.col1=5
    ASTBinaryExpression conjunct1 = assertAndCast(ASTBinaryExpression.class, conjuncts.get(1));
    ASTPathExpression conjunct1Lhs = assertAndCast(ASTPathExpression.class, conjunct1.getLhs());
    validatePathexpression(conjunct1Lhs, ImmutableList.of("t2", "col1"));
    ASTIntLiteral conjunct1Rhs = assertAndCast(ASTIntLiteral.class, conjunct1.getRhs());
    assertThat(conjunct1Rhs.getImage()).isEqualTo("5");
    assertThat(conjunct1.getOp()).isEqualTo(Op.EQ);
    assertThat(conjunct1.getIsNot()).isFalse();
    // Third conjunct: t1.col3 is not null
    ASTBinaryExpression conjunct2 = assertAndCast(ASTBinaryExpression.class, conjuncts.get(2));
    ASTPathExpression conjunct2Lhs = assertAndCast(ASTPathExpression.class, conjunct2.getLhs());
    validatePathexpression(conjunct2Lhs, ImmutableList.of("t1", "col3"));
    ASTNullLiteral conjunct2Rhs = assertAndCast(ASTNullLiteral.class, conjunct2.getRhs());
    assertThat(conjunct2Rhs.getImage()).isEqualTo("null");
    assertThat(conjunct2.getOp()).isEqualTo(Op.IS);
    assertThat(conjunct2.getIsNot()).isTrue();

    // Validate group by
    ASTGroupBy groupBy = select.getGroupBy();
    ImmutableList<ASTGroupingItem> groupingItems = groupBy.getGroupingItems();
    assertThat(groupingItems).hasSize(1);
    ASTGroupingItem groupingItem = groupingItems.get(0);
    ASTRollup rollup = groupingItem.getRollup();
    ImmutableList<ASTExpression> rollupexpressions = rollup.getExpressions();
    assertThat(rollupexpressions).hasSize(3);
    ASTPathExpression rollupExpression0 =
        assertAndCast(ASTPathExpression.class, rollupexpressions.get(0));
    validatePathexpression(rollupExpression0, ImmutableList.of("t2_col1"));
    ASTPathExpression rollupExpression1 =
        assertAndCast(ASTPathExpression.class, rollupexpressions.get(1));
    validatePathexpression(rollupExpression1, ImmutableList.of("t1_col2"));
    ASTIntLiteral rollupExpression2 =
        assertAndCast(ASTIntLiteral.class, rollupexpressions.get(2));
    assertThat(rollupExpression2.getImage()).isEqualTo("3");

    // Validate order by
    ASTOrderBy orderBy = query.getOrderBy();
    ImmutableList<ASTOrderingExpression> orderingExpressions = orderBy.getOrderingExpressions();
    assertThat(orderingExpressions).hasSize(3);

    ASTOrderingExpression orderingExpression0 = orderingExpressions.get(0);
    assertThat(assertAndCast(ASTIntLiteral.class, orderingExpression0.getExpression()).getImage())
        .isEqualTo("1");
    assertThat(orderingExpression0.getOrderingSpec()).isEqualTo(OrderingSpec.ASC);

    ASTOrderingExpression orderingExpression1 = orderingExpressions.get(1);
    assertThat(assertAndCast(ASTIntLiteral.class, orderingExpression1.getExpression()).getImage())
        .isEqualTo("2");
    assertThat(orderingExpression1.getOrderingSpec()).isEqualTo(OrderingSpec.DESC);

    ASTOrderingExpression orderingExpression2 = orderingExpressions.get(2);
    validatePathexpression(assertAndCast(ASTPathExpression.class,
        orderingExpression2.getExpression()), ImmutableList.of("t1_col3"));
    assertThat(orderingExpression2.getOrderingSpec()).isEqualTo(OrderingSpec.UNSPECIFIED);

    // Validate limit
    ASTLimitOffset limitOffset = query.getLimitOffset();
    assertThat(assertAndCast(ASTIntLiteral.class, limitOffset.getLimit()).getImage())
        .isEqualTo("100");
  }

  private static <T> T assertAndCast(Class<T> clazz, Object obj) {
    assertNotNull(obj);
    assertThat(obj).isInstanceOf(clazz);
    return clazz.cast(obj);
  }

  private static void validateAlias(ASTAlias alias, String expected) {
    assertNotNull(alias);
    assertThat(alias.getIdentifier().getIdString()).isEqualTo(expected);
  }

private static void validatePathexpression(ASTPathExpression pathExpression,
      ImmutableList<String> expectedParts) {
    assertNotNull(pathExpression);
    assertThat(
            pathExpression.getNames().stream()
                .map(id -> id.getIdString())
                .collect(toImmutableList()))
        .isEqualTo(expectedParts);
  }
}
