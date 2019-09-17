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

import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.ZetaSQLType.TypeProto;

import java.util.ArrayList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)

public class TypeTest {

  @Test
  public void testIsInt32() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isInt32())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isInt32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isInt32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isInt32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isInt32()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isInt32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isInt32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isInt32()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isInt32()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isInt32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isInt32()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isInt32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isInt32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isInt32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isInt32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY).isInt32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isInt32()).isFalse();
  }

  @Test
  public void testIsInt64() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isInt64())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isInt64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isInt64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isInt64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isInt64()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isInt64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isInt64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isInt64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isInt64()).isTrue();
    assertThat(factory.createProtoType(TypeProto.class).isInt64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isInt64()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isInt64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isInt64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isInt64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isInt64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY).isInt64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isInt64()).isFalse();
  }

  @Test
  public void testIsUint32() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isUint32())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isUint32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isUint32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isUint32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isUint32()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isUint32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isUint32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isUint32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isUint32()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isUint32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isUint32()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isUint32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isUint32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isUint32()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isUint32()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isUint32()).isFalse();
  }

  @Test
  public void testIsUint64() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isUint64())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isUint64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isUint64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isUint64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isUint64()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isUint64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isUint64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isUint64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isUint64()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isUint64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isUint64()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isUint64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isUint64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isUint64()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isUint64()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isUint64()).isFalse();
  }

  @Test
  public void testIsBool() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32)).isBool())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isBool()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isBool()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isBool()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isBool()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isBool()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isBool()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isBool()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isBool()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isBool()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isBool()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isBool()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isBool()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isBool()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isBool()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY).isBool()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isBool()).isFalse();
  }

  @Test
  public void testIsFloat() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isFloat())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isFloat()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isFloat()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isFloat()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isFloat()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isFloat()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isFloat()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isFloat()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isFloat()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isFloat()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isFloat()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isFloat()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isFloat()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isFloat()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isFloat()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY).isFloat()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isFloat()).isFalse();
  }

  @Test
  public void testIsDouble() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isDouble())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isDouble()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isDouble()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isDouble()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isDouble()).isTrue();
    assertThat(factory.createEnumType(TypeKind.class).isDouble()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isDouble()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isDouble()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isDouble()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isDouble()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isDouble()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isDouble()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isDouble()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isDouble()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isDouble()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY).isDouble()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isDouble()).isFalse();
  }

  @Test
  public void testIsString() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isString())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isString()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isString()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isString()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isString()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isString()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isString()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isString()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isString()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isString()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isString()).isTrue();
    assertThat(TypeFactory.createStructType(fields1).isString()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isString()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isString()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isString()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY).isString()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isString()).isFalse();
  }

  @Test
  public void testIsBytes() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isBytes())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isBytes()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isBytes()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isBytes()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isBytes()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isBytes()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isBytes()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isBytes()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isBytes()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isBytes()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isBytes()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isBytes()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isBytes()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isBytes()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isBytes()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY).isBytes()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isBytes()).isFalse();
  }

  @Test
  public void testIsDate() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isBytes())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isDate()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isDate()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isDate()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isDate()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isDate()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isDate()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isDate()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isDate()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isDate()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isDate()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isDate()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isDate()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isDate()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isDate()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY).isDate()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isDate()).isFalse();
  }

  @Test
  public void testIsTimestamp() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isTimestamp())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isTimestamp()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isTimestamp()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isTimestamp()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isTimestamp()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isTimestamp()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isTimestamp()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isTimestamp()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isTimestamp()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isTimestamp()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isTimestamp()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isTimestamp()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isTimestamp()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isTimestamp()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isTimestamp()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY).isTimestamp()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isTimestamp()).isFalse();
  }

  @Test
  public void testIsGeography() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isGeography())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isGeography()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isGeography()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isGeography()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isGeography()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isGeography()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isGeography()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isGeography()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isGeography()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isGeography()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isGeography()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isGeography()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isGeography()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isGeography()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isGeography()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY).isGeography()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isGeography()).isFalse();
  }

  @Test
  public void testIsEnum() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32)).isEnum())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isEnum()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isEnum()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isEnum()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isEnum()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isEnum()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isEnum()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isEnum()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isEnum()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isEnum()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isEnum()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isEnum()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isEnum()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isEnum()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isEnum()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isEnum()).isFalse();
  }

  @Test
  public void testIsArray() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isArray())
        .isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isArray()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isArray()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isArray()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isArray()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isArray()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isArray()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isArray()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isArray()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isArray()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isArray()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isArray()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isArray()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isArray()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isArray()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isArray()).isFalse();
  }

  @Test
  public void testIsStruct() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isStruct())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isStruct()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isStruct()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isStruct()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isStruct()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isStruct()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isStruct()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isStruct()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isStruct()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isStruct()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isStruct()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isStruct()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isStruct()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isStruct()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isStruct()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isStruct()).isFalse();
  }

  @Test
  public void testIsProto() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isProto())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isProto()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isProto()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isProto()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isProto()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isProto()).isFalse();
  }

  @Test
  public void testIsStructOrProto() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isStructOrProto())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isStructOrProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isStructOrProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isStructOrProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isStructOrProto()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isStructOrProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isStructOrProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isStructOrProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isStructOrProto()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isStructOrProto()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isStructOrProto()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isStructOrProto()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isStructOrProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isStructOrProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isStructOrProto()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isStructOrProto()).isFalse();
  }

  @Test
  public void testIsFloatingPoint() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isFloatingPoint())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isFloatingPoint()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isFloatingPoint()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isFloatingPoint()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isFloatingPoint()).isTrue();
    assertThat(factory.createEnumType(TypeKind.class).isFloatingPoint()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isFloatingPoint()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isFloatingPoint()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isFloatingPoint()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isFloatingPoint()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isFloatingPoint()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isFloatingPoint()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isFloatingPoint()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isFloatingPoint()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isFloatingPoint()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isFloatingPoint()).isFalse();
  }

  @Test
  public void testIsNumerical() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isNumerical())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isNumerical()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isNumerical()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isNumerical()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isNumerical()).isTrue();
    assertThat(factory.createEnumType(TypeKind.class).isNumerical()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isNumerical()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isNumerical()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isNumerical()).isTrue();
    assertThat(factory.createProtoType(TypeProto.class).isNumerical()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isNumerical()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isNumerical()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isNumerical()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isNumerical()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isNumerical()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isNumerical()).isTrue();
  }

  @Test
  public void testIsInteger() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isInteger())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isInteger()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isInteger()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isInteger()).isTrue();
    assertThat(factory.createProtoType(TypeProto.class).isInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isInteger()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isInteger()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isInteger()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isInteger()).isFalse();
  }

  @Test
  public void testIsSignedInteger() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isSignedInteger())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isSignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isSignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isSignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isSignedInteger()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isSignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isSignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isSignedInteger()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isSignedInteger()).isTrue();
    assertThat(factory.createProtoType(TypeProto.class).isSignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isSignedInteger()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isSignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isSignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isSignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isSignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isSignedInteger()).isFalse();
  }

  @Test
  public void testIsUnsignedInteger() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayList<StructType.StructField> fields1 = new ArrayList<>();
    fields1.add(new StructType.StructField("", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)));
    fields1.add(new StructType.StructField("a", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));

    assertThat(
            TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32))
                .isUnsignedInteger())
        .isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BOOL).isUnsignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_BYTES).isUnsignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DATE).isUnsignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE).isUnsignedInteger()).isFalse();
    assertThat(factory.createEnumType(TypeKind.class).isUnsignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT).isUnsignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT32).isUnsignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_INT64).isUnsignedInteger()).isFalse();
    assertThat(factory.createProtoType(TypeProto.class).isUnsignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_STRING).isUnsignedInteger()).isFalse();
    assertThat(TypeFactory.createStructType(fields1).isUnsignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP).isUnsignedInteger()).isFalse();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT32).isUnsignedInteger()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_UINT64).isUnsignedInteger()).isTrue();
    assertThat(TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC).isUnsignedInteger()).isFalse();
  }

  @Test
  public void testClassAndProtoSize() {
    assertWithMessage(
            "The number of fields of TypeProto has changed, "
                + "please also update the serialization code accordingly.")
        .that(TypeProto.getDescriptor().getFields())
        .hasSize(6);
    assertWithMessage(
            "The number of fields in Type class has changed, "
                + "please also update the proto and serialization code accordingly.")
        .that(TestUtil.getNonStaticFieldCount(Type.class))
        .isEqualTo(1);
  }
}
