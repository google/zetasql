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

import com.google.zetasqltest.TestSchemaProto.KitchenSinkPB;
import java.util.Arrays;
import java.util.List;
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
}
