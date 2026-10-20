//
// Copyright 2026 Google LLC
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
package com.google.googlesqlmavensmoke;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.googlesql.AnalyzerOptions;
import com.google.googlesql.GoogleSQLType.TypeKind;
import com.google.googlesql.PreparedExpression;
import com.google.googlesql.TypeFactory;
import com.google.googlesql.Value;
import org.junit.Test;

/** Smoke test for testing GoogleSQL Maven LOCAL-SNAPSHOT artifacts. */
public class GoogleSQLAppTest {
  @Test
  public void preparedExpressionWithParameters() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    AnalyzerOptions options = new AnalyzerOptions();
    options.addQueryParameter("p1", factory.createSimpleType(TypeKind.TYPE_INT32));
    String sql = "@p1 * 10";
    try (PreparedExpression expr = new PreparedExpression(sql)) {
      expr.prepare(options);
      Value result =
          expr.execute(
              ImmutableMap.<String, Value>of(), ImmutableMap.of("p1", Value.createInt32Value(5)));

      assertEquals(50L, result.getInt64Value());
    }
  }
}
