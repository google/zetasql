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
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.zetasql.ZetaSQLFunctions.SignatureArgumentKind;
import com.google.zetasql.ZetaSQLOptions.LanguageFeature;
import com.google.zetasql.ZetaSQLResolvedNodeKind.ResolvedNodeKind;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.TableValuedFunction.ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedExpr;
import com.google.zetasql.resolvedast.ResolvedNodes.ResolvedStatement;
import com.google.zetasqltest.TestSchemaProto.KitchenSinkPB;


import java.util.Arrays;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)

public class AnalyzerTest {

  @Test
  public void testAnalyzeStatementEmptyCatalog() {
    SimpleCatalog catalog = new SimpleCatalog("foo");
    AnalyzerOptions options = new AnalyzerOptions();
    assertThat(new Analyzer(options, catalog).analyzeStatement("select 1;")).isNotNull();
  }

  @Test
  public void testAnalyzeNextStatementWithCatalog() {
    SimpleCatalog catalog = new SimpleCatalog("foo");
    catalog.addSimpleTable(
        SimpleTable.tableFromProto(catalog.getTypeFactory().createProtoType(KitchenSinkPB.class)));

    AnalyzerOptions options = new AnalyzerOptions();

    ParseResumeLocation aParseResumeLocation =
        new ParseResumeLocation(
            "select nullable_int from KitchenSinkPB;select key_value from KitchenSinkPB;");
    assertThat(Analyzer.analyzeNextStatement(aParseResumeLocation, options, catalog)).isNotNull();
    assertThat(aParseResumeLocation.getBytePosition()).isEqualTo(39);
    assertThat(Analyzer.analyzeNextStatement(aParseResumeLocation, options, catalog)).isNotNull();
    assertThat(aParseResumeLocation.getBytePosition()).isEqualTo(75);

    // Try registering the catalog.
    catalog.register();
    ParseResumeLocation parseResumeLocation0 =
        new ParseResumeLocation(
            "select nullable_int from KitchenSinkPB;select key_value from KitchenSinkPB;");
    assertThat(Analyzer.analyzeNextStatement(parseResumeLocation0, options, catalog)).isNotNull();
    assertThat(parseResumeLocation0.getBytePosition()).isEqualTo(39);
    assertThat(Analyzer.analyzeNextStatement(parseResumeLocation0, options, catalog)).isNotNull();
    assertThat(parseResumeLocation0.getBytePosition()).isEqualTo(75);

    ParseResumeLocation parseResumeLocation1 =
        new ParseResumeLocation(
            "select nullable_int from KitchenSinkPB;select key_value from KitchenSinkPB;");
    assertThat(Analyzer.analyzeNextStatement(parseResumeLocation1, options, catalog)).isNotNull();
    assertThat(parseResumeLocation1.getBytePosition()).isEqualTo(39);
    assertThat(Analyzer.analyzeNextStatement(parseResumeLocation1, options, catalog)).isNotNull();
    assertThat(parseResumeLocation1.getBytePosition()).isEqualTo(75);

    // Try unregistering the catalog.
    catalog.unregister();
    aParseResumeLocation =
        new ParseResumeLocation(
            "select nullable_int from KitchenSinkPB;select key_value from KitchenSinkPB;");
    assertThat(Analyzer.analyzeNextStatement(aParseResumeLocation, options, catalog)).isNotNull();
    assertThat(aParseResumeLocation.getBytePosition()).isEqualTo(39);
    assertThat(Analyzer.analyzeNextStatement(aParseResumeLocation, options, catalog)).isNotNull();
    assertThat(aParseResumeLocation.getBytePosition()).isEqualTo(75);
  }

  @Test
  public void testAnalyzeStatementWithCatalog() {
    SimpleCatalog catalog = new SimpleCatalog("foo");
    catalog.addSimpleTable(
        SimpleTable.tableFromProto(catalog.getTypeFactory().createProtoType(KitchenSinkPB.class)));

    AnalyzerOptions options = new AnalyzerOptions();
    String sql = "select nullable_int from KitchenSinkPB;";
    assertThat(Analyzer.analyzeStatement(sql, options, catalog)).isNotNull();

    // Try registering the catalog.
    catalog.register();
    assertThat(Analyzer.analyzeStatement(sql, options, catalog)).isNotNull();
    assertThat(Analyzer.analyzeStatement(sql, options, catalog)).isNotNull();

    // Try unregistering the catalog.
    catalog.unregister();
    assertThat(Analyzer.analyzeStatement(sql, options, catalog)).isNotNull();
  }

  @Test
  public void testAnalyzeExpressionWithCatalog() {
    SimpleCatalog catalog = new SimpleCatalog("foo");
    catalog.addZetaSQLFunctions(new ZetaSQLBuiltinFunctionOptions());

    AnalyzerOptions options = new AnalyzerOptions();
    options.addExpressionColumn("foo", TypeFactory.createSimpleType(TypeKind.TYPE_INT32));

    String expression = "foo < 123";
    assertThat(Analyzer.analyzeExpression(expression, options, catalog)).isNotNull();

    // Try registering the catalog.
    catalog.register();
    assertThat(Analyzer.analyzeExpression(expression, options, catalog)).isNotNull();

    // Try unregistering the catalog.
    catalog.unregister();
    assertThat(Analyzer.analyzeExpression(expression, options, catalog)).isNotNull();
  }

  @Test
  public void buildExpressionRoundTrip() {
    SimpleCatalog catalog = new SimpleCatalog("foo");
    catalog.addZetaSQLFunctions(new ZetaSQLBuiltinFunctionOptions());

    AnalyzerOptions options = new AnalyzerOptions();
    options.addExpressionColumn("foo", TypeFactory.createSimpleType(TypeKind.TYPE_INT32));

    String expression = "foo < 123";
    ResolvedExpr resolvedExpr = Analyzer.analyzeExpression(expression, options, catalog);

    String expectedResponse = "foo < (CAST(123 AS INT32))"; // Sql builder normalizes expression.
    String response = Analyzer.buildExpression(resolvedExpr, catalog);
    assertEquals(expectedResponse, response);
  }

  @Test
  public void buildExpressionWithDatePartsRoundTrip() {
    SimpleCatalog catalog = new SimpleCatalog("foo");
    catalog.addZetaSQLFunctions(new ZetaSQLBuiltinFunctionOptions());

    AnalyzerOptions options = new AnalyzerOptions();

    String expression = "DATE_TRUNC(DATE \"2008-12-25\", MONTH)";
    ResolvedExpr resolvedExpr = Analyzer.analyzeExpression(expression, options, catalog);

    String expectedResponse = "DATE_TRUNC(DATE \"2008-12-25\", MONTH)";
    String response = Analyzer.buildExpression(resolvedExpr, catalog);
    assertEquals(expectedResponse, response);
  }

  @Test
  @Ignore("hack in place to get DateParts working, doesn't work with register; see b/146434918")
  public void buildExpressionWithDatePartsAndRegisteredCatalogRoundTrip() {
    SimpleCatalog catalog = new SimpleCatalog("foo");
    catalog.register();
    catalog.addZetaSQLFunctions(new ZetaSQLBuiltinFunctionOptions());

    AnalyzerOptions options = new AnalyzerOptions();

    String expression = "DATE_TRUNC(DATE \"2008-12-25\", MONTH)";
    ResolvedExpr resolvedExpr = Analyzer.analyzeExpression(expression, options, catalog);

    String expectedResponse = "DATE_TRUNC(DATE \"2008-12-25\", MONTH)";
    String response = Analyzer.buildExpression(resolvedExpr, catalog);
    assertEquals(expectedResponse, response);
  }

  @Test
  public void buildStatementRoundTrip() {
    SimpleCatalog catalog = new SimpleCatalog("foo");
    catalog.addZetaSQLFunctions(new ZetaSQLBuiltinFunctionOptions());

    SimpleColumn column =
        new SimpleColumn("t", "bar", TypeFactory.createSimpleType(TypeKind.TYPE_INT32));
    SimpleTable table = new SimpleTable("t", ImmutableList.of(column));
    catalog.addSimpleTable(table);

    AnalyzerOptions options = new AnalyzerOptions();
    String sql = "select t.bar from t";

    ResolvedStatement resolvedStatement = Analyzer.analyzeStatement(sql, options, catalog);
    assertThat(resolvedStatement).isNotNull();

    // Sql builder normalizes expression.
    String expectedResponse = "SELECT t_2.a_1 AS bar FROM (SELECT t.bar AS a_1 FROM t) AS t_2";
    String response = Analyzer.buildStatement(resolvedStatement, catalog);
    assertEquals(expectedResponse, response);
  }

  @Test
  public void testExtractTableNamesFromStatement() {
    List<List<String>> tableNames =
        Analyzer.extractTableNamesFromStatement("select count(1) from foo.bar");

    assertThat(tableNames).containsExactly(Arrays.asList("foo", "bar"));
  }

  @Test
  public void testAnalyzeNextStatementWithParseResumeLocation() {
    SimpleCatalog catalog = new SimpleCatalog("foo");
    catalog.addSimpleTable(
        SimpleTable.tableFromProto(catalog.getTypeFactory().createProtoType(KitchenSinkPB.class)));

    AnalyzerOptions options = new AnalyzerOptions();

    ParseResumeLocation aParseResumeLocation =
        new ParseResumeLocation(
            "select nullable_int from KitchenSinkPB;select key_value from KitchenSinkPB;");

    // Try registering the parse resume location.
    aParseResumeLocation.register();
    assertThat(Analyzer.analyzeNextStatement(aParseResumeLocation, options, catalog)).isNotNull();
    assertThat(aParseResumeLocation.getBytePosition()).isEqualTo(39);
    assertThat(Analyzer.analyzeNextStatement(aParseResumeLocation, options, catalog)).isNotNull();
    assertThat(aParseResumeLocation.getBytePosition()).isEqualTo(75);

    // Mutating after register.
    aParseResumeLocation.setBytePosition(39);
    assertThat(aParseResumeLocation.getBytePosition()).isEqualTo(39);
    assertThat(Analyzer.analyzeNextStatement(aParseResumeLocation, options, catalog)).isNotNull();
    assertThat(aParseResumeLocation.getBytePosition()).isEqualTo(75);

    // Try unregistering the parse resume location.
    aParseResumeLocation.unregister();
    aParseResumeLocation.setBytePosition(0);
    assertThat(Analyzer.analyzeNextStatement(aParseResumeLocation, options, catalog)).isNotNull();
    assertThat(aParseResumeLocation.getBytePosition()).isEqualTo(39);
    assertThat(Analyzer.analyzeNextStatement(aParseResumeLocation, options, catalog)).isNotNull();
    assertThat(aParseResumeLocation.getBytePosition()).isEqualTo(75);
  }

  @Test
  public void testSelectColumnsFromForwardInputSchemaToOutputSchemaWithAppendedColumnTVF() {
    FunctionArgumentType tableType =
        new FunctionArgumentType(SignatureArgumentKind.ARG_TYPE_RELATION);
    TableValuedFunction tvf =
        new ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
            ImmutableList.of("test_tvf"),
            new FunctionSignature(tableType, ImmutableList.of(tableType), /* contextId= */ -1),
            ImmutableList.of(
                TVFRelation.Column.create(
                    "append_col_1", TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
                TVFRelation.Column.create(
                    "append_col_2", TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP))));

    SimpleCatalog catalog = new SimpleCatalog("catalog1");
    catalog.addTableValuedFunction(tvf);
    // Test if tvf is added to catalog.
    assertThat(catalog.getTVFByName("test_tvf")).isEqualTo(tvf);
    AnalyzerOptions analyzerOptions = new AnalyzerOptions();
    analyzerOptions
        .getLanguageOptions()
        .setEnabledLanguageFeatures(
            ImmutableSet.of(LanguageFeature.FEATURE_TABLE_VALUED_FUNCTIONS));
    Analyzer analyzer = new Analyzer(analyzerOptions, catalog);
    ResolvedStatement resolvedStatement =
        analyzer.analyzeStatement(
            "select append_col_1, append_col_2 from test_tvf((select cast (12 as int64)))");
    // Test if select extra columns query is analyzed correctly.
    assertThat(resolvedStatement).isNotNull();
  }

  @Test
  public void testExtractTableNamesFromNextStatement() {
    AnalyzerOptions analyzerOptions = new AnalyzerOptions();
    analyzerOptions
        .getLanguageOptions()
        .setSupportedStatementKinds(
            ImmutableSet.of(
                ResolvedNodeKind.RESOLVED_QUERY_STMT,
                ResolvedNodeKind.RESOLVED_CREATE_FUNCTION_STMT));

    ParseResumeLocation aParseResumeLocation =
        new ParseResumeLocation(
            "CREATE FUNCTION plusOne(x INT64) AS (x + 1);select bar from bar_dataset.bar_table"
                + " union all select * from baz_table;");

    assertThat(Analyzer.extractTableNamesFromNextStatement(aParseResumeLocation, analyzerOptions))
        .isEmpty();
    assertThat(aParseResumeLocation.getBytePosition()).isEqualTo(44);
    assertThat(Analyzer.extractTableNamesFromNextStatement(aParseResumeLocation, analyzerOptions))
        .containsExactly(
            ImmutableList.of("bar_dataset", "bar_table"), ImmutableList.of("baz_table"));
    assertThat(aParseResumeLocation.getBytePosition()).isEqualTo(116);
  }
}
