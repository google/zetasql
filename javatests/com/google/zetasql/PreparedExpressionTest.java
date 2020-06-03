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
import static com.google.common.truth.Truth.assertWithMessage;
import static com.google.zetasql.TypeTestBase.getDescriptorPoolWithTypeProtoAndTypeKind;
import static org.junit.Assert.fail;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.zetasql.ZetaSQLOptions.ErrorMessageMode;
import com.google.zetasql.ZetaSQLOptions.ProductMode;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.ZetaSQLType.TypeProto;


import java.util.Collections;
import java.util.HashMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)

public class PreparedExpressionTest {

  @Test
  public void testExecuteWithLiteral() {
    try (PreparedExpression exp = new PreparedExpression("42");
        PreparedExpression exp2 = new PreparedExpression("41")) {
      Value value = exp.execute();
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_INT64);
      assertThat(value.getInt64Value()).isEqualTo(42);
      Value value2 = exp2.execute();
      assertThat(value2.getType().getKind()).isEqualTo(TypeKind.TYPE_INT64);
      assertThat(value2.getInt64Value()).isEqualTo(41);
    }

    try (PreparedExpression exp = new PreparedExpression("\"hello\"")) {
      Value value = exp.execute();
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_STRING);
      assertThat(value.getStringValue()).isEqualTo("hello");
    }

    try (PreparedExpression exp = new PreparedExpression("b\"world\"")) {
      Value value = exp.execute();
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_BYTES);
      assertThat(value.getBytesValue().toStringUtf8()).isEqualTo("world");
    }

    try (PreparedExpression exp = new PreparedExpression("true")) {
      Value value = exp.execute();
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_BOOL);
      assertThat(value.getBoolValue()).isEqualTo(true);
    }

    try (PreparedExpression exp = new PreparedExpression("1.25")) {
      Value value = exp.execute();
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_DOUBLE);
      assertThat(value.getDoubleValue()).isEqualTo(1.25);
    }
  }

  @Test
  public void testExecuteWithBuiltinFunction() {
    try (PreparedExpression exp = new PreparedExpression("41+1")) {
      Value value = exp.execute();
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_INT64);
      assertThat(value.getInt64Value()).isEqualTo(42);
    }
  }

  @Test
  public void testExecuteWithColumns() {
    try (PreparedExpression exp = new PreparedExpression("a+b")) {
      HashMap<String, Value> columns = new HashMap<>();
      HashMap<String, Value> parameters = new HashMap<>();
      columns.put("a", Value.createInt64Value(40));
      columns.put("b", Value.createInt64Value(2));
      Value value = exp.execute(columns, parameters);
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_INT64);
      assertThat(value.getInt64Value()).isEqualTo(42);

      columns.put("a", Value.createInt64Value(41));
      columns.put("b", Value.createInt64Value(1));
      value = exp.execute(columns, parameters);
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_INT64);
      assertThat(value.getInt64Value()).isEqualTo(42);

      columns.put("a", Value.createInt64Value(42));
      columns.put("b", Value.createInt64Value(0));
      value = exp.execute(columns, parameters);
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_INT64);
      assertThat(value.getInt64Value()).isEqualTo(42);

      columns.clear();
      columns.put("a", Value.createInt64Value(40));
      try {
        value = exp.execute(columns, parameters);
        fail();
      } catch (SqlException expected) {
        checkSqlExceptionErrorSubstr(expected, "Incomplete column parameters");
      }
    }
  }

  @Test
  public void testExecuteWithParameters() {
    HashMap<String, Value> columns = new HashMap<>();
    HashMap<String, Value> parameters = new HashMap<>();
    try (PreparedExpression exp = new PreparedExpression("@a+@b");
        PreparedExpression exp2 = new PreparedExpression("@b+@a")) {
      parameters.put("a", Value.createInt32Value(39));
      parameters.put("b", Value.createInt32Value(3));
      Value value = exp.execute(columns, parameters);
      Value value2 = exp2.execute(columns, parameters);
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_INT64);
      assertThat(value.getInt64Value()).isEqualTo(42);
      assertThat(value2.getType().getKind()).isEqualTo(TypeKind.TYPE_INT64);
      assertThat(value2.getInt64Value()).isEqualTo(42);

      parameters.put("a", Value.createInt32Value(38));
      parameters.put("b", Value.createInt32Value(4));
      value = exp.execute(columns, parameters);
      value2 = exp2.execute(columns, parameters);
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_INT64);
      assertThat(value.getInt64Value()).isEqualTo(42);
      assertThat(value2.getType().getKind()).isEqualTo(TypeKind.TYPE_INT64);
      assertThat(value2.getInt64Value()).isEqualTo(42);
    }
  }

  @Test
  public void testExecuteWithEnumColumn() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    EnumType typeKind = factory.createEnumType(TypeKind.class);

    HashMap<String, Value> columns = new HashMap<>();
    columns.put("a", Value.createEnumValue(typeKind, TypeKind.TYPE_DATE_VALUE));
    HashMap<String, Value> parameters = new HashMap<>();

    try (PreparedExpression exp = new PreparedExpression("a")) {
      Value value = exp.execute(columns, parameters);
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_ENUM);
      assertThat(value.getEnumValue()).isEqualTo(TypeKind.TYPE_DATE_VALUE);
      assertThat(value.getEnumName()).isEqualTo(TypeKind.TYPE_DATE.name());
    }

    try (PreparedExpression exp = new PreparedExpression("cast(a AS STRING)")) {
      Value value = exp.execute(columns, parameters);
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_STRING);
      assertThat(value.getStringValue()).isEqualTo(TypeKind.TYPE_DATE.name());
    }

    try (PreparedExpression exp = new PreparedExpression("cast(a AS INT32)")) {
      Value value = exp.execute(columns, parameters);
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_INT32);
      assertThat(value.getInt32Value()).isEqualTo(TypeKind.TYPE_DATE_VALUE);
    }
  }

  @Test
  public void testExecuteWithProtoColumn() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ProtoType typeProto = factory.createProtoType(TypeProto.class);

    HashMap<String, Value> columns = new HashMap<>();
    columns.put(
        "a",
        Value.createProtoValue(
            typeProto,
            TypeProto.newBuilder().setTypeKind(TypeKind.TYPE_DATE).build().toByteString()));
    HashMap<String, Value> parameters = new HashMap<>();

    try (PreparedExpression exp = new PreparedExpression("a")) {
      Value value = exp.execute(columns, parameters);
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_PROTO);
      assertThat(TypeProto.parseFrom(value.getProtoValue()).getTypeKind())
          .isEqualTo(TypeKind.TYPE_DATE);
    } catch (InvalidProtocolBufferException e) {
      fail(e.toString());
    }

    try (PreparedExpression exp = new PreparedExpression("a.type_kind")) {
      Value value = exp.execute(columns, parameters);
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_ENUM);
      assertThat(value.getEnumValue()).isEqualTo(TypeKind.TYPE_DATE_VALUE);
      assertThat(value.getEnumName()).isEqualTo(TypeKind.TYPE_DATE.name());
    }
  }

  @Test
  public void testExecuteWithProtoColumnAndEnumParamFromDifferentPools() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ProtoType typeProto = factory.createProtoType(TypeProto.class);
    EnumType enumProto =
        factory.createEnumType(
            getDescriptorPoolWithTypeProtoAndTypeKind().findEnumTypeByName("zetasql.TypeKind"));

    HashMap<String, Value> columns = new HashMap<>();
    columns.put(
        "c",
        Value.createProtoValue(
            typeProto,
            TypeProto.newBuilder().setTypeKind(TypeKind.TYPE_DATE).build().toByteString()));

    HashMap<String, Value> parameters = new HashMap<>();
    parameters.put("e", Value.createEnumValue(enumProto, TypeKind.TYPE_DATE_VALUE));

    try (PreparedExpression exp = new PreparedExpression("c.type_kind = @e")) {
      Value value = exp.execute(columns, parameters);
      assertThat(value.getType().getKind()).isEqualTo(TypeKind.TYPE_BOOL);
      assertThat(value.getBoolValue()).isTrue();
    }
  }

  @Test
  public void testPrepareDefaultOptions() {
    try (PreparedExpression exp = new PreparedExpression("42")) {
      exp.prepare(new AnalyzerOptions());
      assertThat(exp.getOutputType().isInt64()).isTrue();
    }
  }

  @Test
  public void testPrepareAndExecuteWithInscopeColumn() {
    try (PreparedExpression exp =
        new PreparedExpression(
            "IF(type_kind = 16, array_type.element_type.type_kind, type_kind)")) {
      AnalyzerOptions options = new AnalyzerOptions();
      TypeFactory factory = TypeFactory.nonUniqueNames();
      ProtoType type = factory.createProtoType(TypeProto.class);
      EnumType typeKind = factory.createEnumType(TypeKind.class);
      options.setInScopeExpressionColumn("value", type);
      exp.prepare(options);
      assertThat(exp.getOutputType().isEnum()).isTrue();
      assertThat(exp.getOutputType()).isEqualTo(typeKind);
      HashMap<String, Value> columns = new HashMap<>();
      columns.put(
          "value",
          Value.createProtoValue(
              type, TypeProto.newBuilder().setTypeKind(TypeKind.TYPE_DATE).build().toByteString()));
      HashMap<String, Value> params = new HashMap<>();
      assertThat(exp.execute(columns, params).getEnumValue()).isEqualTo(TypeKind.TYPE_DATE_VALUE);
    }
  }

  @Test
  public void testPrepareWithColumns() {
    try (PreparedExpression exp = new PreparedExpression("IF(true, a, b.type_kind)")) {
      AnalyzerOptions options = new AnalyzerOptions();
      TypeFactory factory = TypeFactory.nonUniqueNames();
      ProtoType type = factory.createProtoType(TypeProto.class);
      EnumType typeKind = factory.createEnumType(TypeKind.class);
      options.addExpressionColumn("a", typeKind);
      options.addExpressionColumn("b", type);
      exp.prepare(options);
      assertThat(exp.getOutputType().isEnum()).isTrue();
      assertThat(exp.getOutputType()).isEqualTo(typeKind);
    }
  }

  @Test
  public void testPrepareWithParameters() {
    try (PreparedExpression exp = new PreparedExpression("IF(true, @a, @b.type_kind)")) {
      AnalyzerOptions options = new AnalyzerOptions();
      TypeFactory factory = TypeFactory.nonUniqueNames();
      ProtoType type = factory.createProtoType(TypeProto.class);
      EnumType typeKind = factory.createEnumType(TypeKind.class);
      options.addQueryParameter("a", typeKind);
      options.addQueryParameter("b", type);
      exp.prepare(options);
      assertThat(exp.getOutputType().isEnum()).isTrue();
      assertThat(exp.getOutputType()).isEqualTo(typeKind);
    }
  }

  @Test
  public void testPrepareWithColumnAndParameter() {
    try (PreparedExpression exp = new PreparedExpression("IF(true, a, @b.type_kind)")) {
      AnalyzerOptions options = new AnalyzerOptions();
      TypeFactory factory = TypeFactory.nonUniqueNames();
      ProtoType type = factory.createProtoType(TypeProto.class);
      EnumType typeKind = factory.createEnumType(TypeKind.class);
      options.addExpressionColumn("a", typeKind);
      options.addQueryParameter("b", type);
      exp.prepare(options);
      assertThat(exp.getOutputType().isEnum()).isTrue();
      assertThat(exp.getOutputType()).isEqualTo(typeKind);
    }
  }

  @Test
  public void testPrepareWithParameterAndColumn() {
    try (PreparedExpression exp = new PreparedExpression("IF(true, @a, b.type_kind)")) {
      AnalyzerOptions options = new AnalyzerOptions();
      TypeFactory factory = TypeFactory.nonUniqueNames();
      ProtoType type = factory.createProtoType(TypeProto.class);
      EnumType typeKind = factory.createEnumType(TypeKind.class);
      options.addQueryParameter("a", typeKind);
      options.addExpressionColumn("b", type);
      exp.prepare(options);
      assertThat(exp.getOutputType().isEnum()).isTrue();
      assertThat(exp.getOutputType()).isEqualTo(typeKind);
    }
  }

  @Test
  public void testPrepareErrorMessageModes() {
    String expr = "1 +\n2 + BadCol +\n3";
    try (PreparedExpression exp = new PreparedExpression(expr)) {
      AnalyzerOptions options = new AnalyzerOptions();
      exp.prepare(options);
      fail();
    } catch (SqlException e) {
      checkSqlExceptionErrorSubstr(e, "Unrecognized name: BadCol [at 2:5]");
    }

    try (PreparedExpression exp = new PreparedExpression(expr)) {
      AnalyzerOptions options = new AnalyzerOptions();
      options.setErrorMessageMode(ErrorMessageMode.ERROR_MESSAGE_ONE_LINE);
      exp.prepare(options);
      fail();
    } catch (SqlException e) {
      checkSqlExceptionErrorSubstr(e, "Unrecognized name: BadCol [at 2:5]");
    }

    try (PreparedExpression exp = new PreparedExpression(expr)) {
      AnalyzerOptions options = new AnalyzerOptions();
      options.setErrorMessageMode(ErrorMessageMode.ERROR_MESSAGE_WITH_PAYLOAD);
      exp.prepare(options);
      fail();
    } catch (SqlException e) {
      checkSqlExceptionErrorSubstr(e, "Unrecognized name: BadCol");
    }

    try (PreparedExpression exp = new PreparedExpression(expr)) {
      AnalyzerOptions options = new AnalyzerOptions();
      options.setErrorMessageMode(ErrorMessageMode.ERROR_MESSAGE_MULTI_LINE_WITH_CARET);
      exp.prepare(options);
      fail();
    } catch (SqlException e) {
      checkSqlExceptionErrorSubstr(e, "Unrecognized name: BadCol [at 2:5]\n2 + BadCol +\n    ^");
    }
  }

  @Test
  public void testPrepareDefaultTimezoneInCastOperator() {
    String expr = "cast(cast('2015-04-01' as timestamp) as string)";
    try (PreparedExpression exp = new PreparedExpression(expr)) {
      AnalyzerOptions options = new AnalyzerOptions();
      exp.prepare(options);
      Value value = exp.execute();
      assertThat(value.getStringValue()).isEqualTo("2015-04-01 00:00:00-07");
    }

    try (PreparedExpression exp = new PreparedExpression(expr)) {
      AnalyzerOptions options = new AnalyzerOptions();
      options.setDefaultTimezone("Asia/Shanghai");
      exp.prepare(options);
      Value value = exp.execute();
      assertThat(value.getStringValue()).isEqualTo("2015-04-01 00:00:00+08");
    }
  }

  @Test
  public void testPrepareDefaultTimezoneInTimestampFunction() {
    String expr = "cast(timestamp(date '2015-04-01') as string)";
    try (PreparedExpression exp = new PreparedExpression(expr)) {
      AnalyzerOptions options = new AnalyzerOptions();
      options.setDefaultTimezone("UTC");
      exp.prepare(options);
      Value value = exp.execute();
      assertThat(value.getStringValue()).isEqualTo("2015-04-01 00:00:00+00");
    }

    try (PreparedExpression exp = new PreparedExpression(expr)) {
      AnalyzerOptions options = new AnalyzerOptions();
      options.setDefaultTimezone("Asia/Shanghai");
      exp.prepare(options);
      Value value = exp.execute();
      assertThat(value.getStringValue()).isEqualTo("2015-04-01 00:00:00+08");
    }
  }

  @Test
  public void testPrepareProductModes() {
    String expr = "cast(1 as uint64)";
    try (PreparedExpression exp = new PreparedExpression(expr)) {
      AnalyzerOptions options = new AnalyzerOptions();
      LanguageOptions languageOptions = new LanguageOptions();
      languageOptions.setProductMode(ProductMode.PRODUCT_EXTERNAL);
      options.setLanguageOptions(languageOptions);
      exp.prepare(options);
      fail();
    } catch (SqlException expected) {
    }

    try (PreparedExpression exp = new PreparedExpression(expr)) {
      AnalyzerOptions options = new AnalyzerOptions();
      exp.prepare(options);
    }
  }

  @Test
  public void testPrepareAndExecute() {
    try (PreparedExpression exp = new PreparedExpression("a")) {
      AnalyzerOptions options = new AnalyzerOptions();
      options.addExpressionColumn("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32));
      exp.prepare(options);
      assertThat(exp.getOutputType().isInt32()).isTrue();
      HashMap<String, Value> columns = new HashMap<>();
      HashMap<String, Value> parameters = new HashMap<>();
      columns.put("a", Value.createInt32Value(1));
      Value result = exp.execute(columns, parameters);
      assertThat(result.getType().isInt32()).isTrue();
      assertThat(result.getInt32Value()).isEqualTo(1);
    }
  }

  @Test
  public void testPrepareAndExecuteTwice() {
    try (PreparedExpression exp = new PreparedExpression("a")) {
      AnalyzerOptions options = new AnalyzerOptions();
      options.addExpressionColumn("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32));
      exp.prepare(options);
      assertThat(exp.getOutputType().isInt32()).isTrue();
      HashMap<String, Value> columns = new HashMap<>();
      HashMap<String, Value> parameters = new HashMap<>();
      columns.put("a", Value.createInt32Value(1));
      Value result = exp.execute(columns, parameters);
      assertThat(result.getType().isInt32()).isTrue();
      assertThat(result.getInt32Value()).isEqualTo(1);
      columns.clear();
      columns.put("a", Value.createInt32Value(2));
      result = exp.execute(columns, parameters);
      assertThat(result.getType().isInt32()).isTrue();
      assertThat(result.getInt32Value()).isEqualTo(2);
    }
  }

  @Test
  public void testPrepareTwice() {
    try (PreparedExpression exp = new PreparedExpression("a")) {
      AnalyzerOptions options = new AnalyzerOptions();
      options.addExpressionColumn("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32));
      exp.prepare(options);
      try {
        exp.prepare(options);
        fail();
      } catch (IllegalStateException expected) {
      }
    }
  }

  @Test
  public void testPrepareAndClose() {
    PreparedExpression exp = new PreparedExpression("a");
    AnalyzerOptions options = new AnalyzerOptions();
    options.addExpressionColumn("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32));
    exp.prepare(options);
    exp.close();
  }

  @Test
  public void testPrepareAndCloseTwice() {
    PreparedExpression exp = new PreparedExpression("a");
    AnalyzerOptions options = new AnalyzerOptions();
    options.addExpressionColumn("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32));
    exp.prepare(options);
    exp.close();
    exp.close();
  }

  @Test
  public void testUseAfterClose() {
    PreparedExpression exp = new PreparedExpression("1");
    AnalyzerOptions options = new AnalyzerOptions();
    exp.prepare(options);
    exp.close();

    try {
      exp.getOutputType();
      fail();
    } catch (IllegalStateException expected) {
    }

    try {
      exp.prepare(options);
      fail();
    } catch (IllegalStateException expected) {
    }

    try {
      exp.execute();
      fail();
    } catch (IllegalStateException expected) {
    }

    try {
      exp.execute(Collections.<String, Value>emptyMap(), Collections.<String, Value>emptyMap());
      fail();
    } catch (IllegalStateException expected) {
    }
  }

  @Test
  public void testCloseBeforePrepare() {
    PreparedExpression exp = new PreparedExpression("a");
    exp.close();
  }

  @Test
  public void testOutOfOrderClose() {
    PreparedExpression exp = new PreparedExpression("a");
    PreparedExpression exp2 = new PreparedExpression("@b");
    PreparedExpression exp3 = new PreparedExpression("@a");
    HashMap<String, Value> columns = new HashMap<>();
    HashMap<String, Value> parameters = new HashMap<>();
    columns.put("a", Value.createInt32Value(-1));
    parameters.put("a", Value.createInt32Value(-1));
    parameters.put("b", Value.createUint32Value(1));
    exp2.execute(columns, parameters);
    exp2.close();
    exp.execute(columns, parameters);
    exp3.execute(columns, parameters);
    exp.close();
    exp3.close();
  }

  @Test
  public void testPrepareAndExecuteWrongColumnType() {
    try (PreparedExpression exp = new PreparedExpression("a")) {
      AnalyzerOptions options = new AnalyzerOptions();
      options.addExpressionColumn("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32));
      exp.prepare(options);
      assertThat(exp.getOutputType().isInt32()).isTrue();
      HashMap<String, Value> columns = new HashMap<>();
      HashMap<String, Value> parameters = new HashMap<>();
      columns.put("a", Value.createInt64Value(1));
      exp.execute(columns, parameters);
      fail();
    } catch (SqlException expected) {
      checkSqlExceptionErrorSubstr(expected, "Expected column parameter 'a' to be of type INT32");
    }
  }

  @Test
  public void testPrepareAndExecuteWrongParameterType() {
    try (PreparedExpression exp = new PreparedExpression("@a")) {
      AnalyzerOptions options = new AnalyzerOptions();
      options.addQueryParameter("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32));
      exp.prepare(options);
      assertThat(exp.getOutputType().isInt32()).isTrue();
      HashMap<String, Value> columns = new HashMap<>();
      HashMap<String, Value> parameters = new HashMap<>();
      parameters.put("a", Value.createInt64Value(1));
      exp.execute(columns, parameters);
      fail();
    } catch (SqlException expected) {
      checkSqlExceptionErrorSubstr(expected, "Expected query parameter 'a' to be of type INT32");
    }
  }

  @Test
  public void testPrepareAndExecuteUnknownQueryParameter() {
    try (PreparedExpression exp = new PreparedExpression("@a")) {
      AnalyzerOptions options = new AnalyzerOptions();
      options.addQueryParameter("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32));
      exp.prepare(options);
      assertThat(exp.getOutputType().isInt32()).isTrue();
      HashMap<String, Value> columns = new HashMap<>();
      HashMap<String, Value> parameters = new HashMap<>();
      parameters.put("b", Value.createInt64Value(1));
      exp.execute(columns, parameters);
      fail();
    } catch (SqlException expected) {
      checkSqlExceptionErrorSubstr(expected, "Unexpected query parameter 'b'");
    }
  }

  private static void checkSqlExceptionErrorSubstr(SqlException exception, String error) {
    assertWithMessage(exception.getMessage()).that(exception.getMessage().contains(error)).isTrue();
  }
}
