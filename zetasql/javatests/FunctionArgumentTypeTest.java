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

import com.google.zetasql.FunctionProtos.FunctionArgumentTypeProto;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.ArgumentCardinality;
import com.google.zetasql.ZetaSQLFunctions.SignatureArgumentKind;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.ZetaSQLType.TypeProto;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FunctionArgumentTypeTest {

  @Test
  public void testFixedType() {
    FunctionArgumentType fixedTypeInt32 =
        new FunctionArgumentType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32));
    assertThat(fixedTypeInt32.isConcrete()).isFalse();
    assertThat(fixedTypeInt32.getType()).isNotNull();
    assertThat(fixedTypeInt32.isRepeated()).isFalse();
    assertThat(fixedTypeInt32.isOptional()).isFalse();
    assertThat(fixedTypeInt32.isRequired()).isTrue();
    assertThat(fixedTypeInt32.getNumOccurrences()).isEqualTo(-1);
    assertThat(fixedTypeInt32.debugString()).isEqualTo("INT32");

    FunctionArgumentType concreteFixedType =
        new FunctionArgumentType(
            TypeFactory.createSimpleType(TypeKind.TYPE_INT32), ArgumentCardinality.REQUIRED, 0);
    assertThat(concreteFixedType.isConcrete()).isTrue();
    assertThat(concreteFixedType.getType()).isNotNull();
    assertThat(concreteFixedType.isRepeated()).isFalse();
    assertThat(concreteFixedType.isOptional()).isFalse();
    assertThat(concreteFixedType.isRequired()).isTrue();
    assertThat(concreteFixedType.getNumOccurrences()).isEqualTo(0);
    assertThat(concreteFixedType.debugString()).isEqualTo("INT32");

    FunctionArgumentType repeatedFixedType =
        new FunctionArgumentType(
            TypeFactory.createSimpleType(TypeKind.TYPE_INT32), ArgumentCardinality.REPEATED, 1);
    assertThat(repeatedFixedType.isConcrete()).isTrue();
    assertThat(repeatedFixedType.getType()).isNotNull();
    assertThat(repeatedFixedType.isRepeated()).isTrue();
    assertThat(repeatedFixedType.isOptional()).isFalse();
    assertThat(repeatedFixedType.isRequired()).isFalse();
    assertThat(repeatedFixedType.getNumOccurrences()).isEqualTo(1);
    assertThat(repeatedFixedType.debugString()).isEqualTo("repeated(1) INT32");

    FunctionArgumentType optionalFixedType =
        new FunctionArgumentType(
            TypeFactory.createSimpleType(TypeKind.TYPE_INT32), ArgumentCardinality.OPTIONAL, 1);
    assertThat(optionalFixedType.isConcrete()).isTrue();
    assertThat(optionalFixedType.getType()).isNotNull();
    assertThat(optionalFixedType.isRepeated()).isFalse();
    assertThat(optionalFixedType.isOptional()).isTrue();
    assertThat(optionalFixedType.isRequired()).isFalse();
    assertThat(optionalFixedType.getNumOccurrences()).isEqualTo(1);
    assertThat(optionalFixedType.debugString()).isEqualTo("optional(1) INT32");

    FunctionArgumentType typeNull =
        new FunctionArgumentType((Type) null, ArgumentCardinality.REPEATED, -1);
    assertThat(typeNull.isConcrete()).isFalse();
    assertThat(typeNull.getType()).isNull();
    assertThat(typeNull.isRepeated()).isTrue();
    assertThat(typeNull.isOptional()).isFalse();
    assertThat(typeNull.isRequired()).isFalse();
    assertThat(typeNull.getNumOccurrences()).isEqualTo(-1);
    assertThat(typeNull.debugString()).isEqualTo("repeated FIXED");
  }

  @Test
  public void testNotFixedType() {
    FunctionArgumentType arrayTypeAny1 =
        new FunctionArgumentType(
            SignatureArgumentKind.ARG_ARRAY_TYPE_ANY_1, ArgumentCardinality.REPEATED, 0);
    assertThat(arrayTypeAny1.isConcrete()).isFalse();
    assertThat(arrayTypeAny1.getType()).isNull();
    assertThat(arrayTypeAny1.isRepeated()).isTrue();
    assertThat(arrayTypeAny1.debugString()).isEqualTo("repeated <array<T1>>");

    FunctionArgumentType arrayTypeAny2 =
        new FunctionArgumentType(
            SignatureArgumentKind.ARG_ARRAY_TYPE_ANY_2, ArgumentCardinality.REQUIRED, 1);
    assertThat(arrayTypeAny2.isConcrete()).isFalse();
    assertThat(arrayTypeAny2.getType()).isNull();
    assertThat(arrayTypeAny2.isRepeated()).isFalse();
    assertThat(arrayTypeAny2.debugString()).isEqualTo("<array<T2>>");

    FunctionArgumentType enumAny =
        new FunctionArgumentType(
            SignatureArgumentKind.ARG_ENUM_ANY, ArgumentCardinality.OPTIONAL, -1);
    assertThat(enumAny.isConcrete()).isFalse();
    assertThat(enumAny.getType()).isNull();
    assertThat(enumAny.isRepeated()).isFalse();
    assertThat(enumAny.debugString()).isEqualTo("optional <enum>");

    FunctionArgumentType protoAny =
        new FunctionArgumentType(
            SignatureArgumentKind.ARG_PROTO_ANY, ArgumentCardinality.OPTIONAL, 3);
    assertThat(protoAny.isConcrete()).isFalse();
    assertThat(protoAny.getType()).isNull();
    assertThat(protoAny.isRepeated()).isFalse();
    assertThat(protoAny.debugString()).isEqualTo("optional <proto>");

    FunctionArgumentType structAny =
        new FunctionArgumentType(
            SignatureArgumentKind.ARG_STRUCT_ANY, ArgumentCardinality.OPTIONAL, 0);
    assertThat(structAny.isConcrete()).isFalse();
    assertThat(structAny.getType()).isNull();
    assertThat(structAny.isRepeated()).isFalse();
    assertThat(structAny.debugString()).isEqualTo("optional <struct>");

    FunctionArgumentType typeAny1 =
        new FunctionArgumentType(
            SignatureArgumentKind.ARG_TYPE_ANY_1, ArgumentCardinality.REQUIRED, 0);
    assertThat(typeAny1.isConcrete()).isFalse();
    assertThat(typeAny1.getType()).isNull();
    assertThat(typeAny1.isRepeated()).isFalse();
    assertThat(typeAny1.debugString()).isEqualTo("<T1>");

    FunctionArgumentType typeAny2 =
        new FunctionArgumentType(
            SignatureArgumentKind.ARG_TYPE_ANY_2, ArgumentCardinality.REQUIRED, 2);
    assertThat(typeAny2.isConcrete()).isFalse();
    assertThat(typeAny2.getType()).isNull();
    assertThat(typeAny2.isRepeated()).isFalse();
    assertThat(typeAny2.debugString()).isEqualTo("<T2>");

    FunctionArgumentType typeArbitrary =
        new FunctionArgumentType(
            SignatureArgumentKind.ARG_TYPE_ARBITRARY, ArgumentCardinality.REQUIRED, -1);
    assertThat(typeArbitrary.isConcrete()).isFalse();
    assertThat(typeArbitrary.getType()).isNull();
    assertThat(typeArbitrary.isRepeated()).isFalse();
    assertThat(typeArbitrary.debugString()).isEqualTo("ANY TYPE");

    FunctionArgumentType typeUnknown =
        new FunctionArgumentType(
            SignatureArgumentKind.__SignatureArgumentKind__switch_must_have_a_default__,
            ArgumentCardinality.REPEATED,
            1);
    assertThat(typeUnknown.isConcrete()).isFalse();
    assertThat(typeUnknown.getType()).isNull();
    assertThat(typeUnknown.isRepeated()).isTrue();
    assertThat(typeUnknown.debugString()).isEqualTo("repeated UNKNOWN_ARG_KIND");
  }

  @Test
  public void testSerializationAndDeserialization() {
    FunctionArgumentType arrayTypeAny1 =
        new FunctionArgumentType(
            SignatureArgumentKind.ARG_ARRAY_TYPE_ANY_1, ArgumentCardinality.REPEATED, 0);
    FunctionArgumentType typeFixed1 =
        new FunctionArgumentType(
            TypeFactory.createSimpleType(TypeKind.TYPE_INT32), ArgumentCardinality.REPEATED, 0);
    TypeFactory factory = TypeFactory.nonUniqueNames();
    FunctionArgumentType typeFixed2 =
        new FunctionArgumentType(
            factory.createProtoType(TypeProto.class), ArgumentCardinality.REPEATED, 0);

    checkSerializeAndDeserialize(arrayTypeAny1);
    checkSerializeAndDeserialize(typeFixed1);
    checkSerializeAndDeserialize(typeFixed2);
  }

  private static void checkSerializeAndDeserialize(FunctionArgumentType functionArgumentType) {
    FileDescriptorSetsBuilder fileDescriptorSetsBuilder = new FileDescriptorSetsBuilder();
    checkEquals(
        functionArgumentType,
        FunctionArgumentType.deserialize(
            functionArgumentType.serialize(fileDescriptorSetsBuilder),
            fileDescriptorSetsBuilder.getDescriptorPools()));
    assertThat(functionArgumentType.serialize(fileDescriptorSetsBuilder))
        .isEqualTo(
            FunctionArgumentType.deserialize(
                    functionArgumentType.serialize(fileDescriptorSetsBuilder),
                    fileDescriptorSetsBuilder.getDescriptorPools())
                .serialize(fileDescriptorSetsBuilder));
  }

  static void checkEquals(FunctionArgumentType type1, FunctionArgumentType type2) {
    assertThat(type2.getNumOccurrences()).isEqualTo(type1.getNumOccurrences());
    assertThat(type2.getCardinality()).isEqualTo(type1.getCardinality());
    assertThat(type2.getKind()).isEqualTo(type1.getKind());
    if (type1.isConcrete()) {
      assertThat(type1.getType().equals(type2.getType())).isTrue();
    }
  }

  @Test
  public void testClassAndProtoSize() {
    assertWithMessage(
            "The number of fields of FunctionArgumentTypeProto has changed, "
                + "please also update the serialization code accordingly.")
        .that(FunctionArgumentTypeProto.getDescriptor().getFields())
        .hasSize(4);
    assertWithMessage(
            "The number of fields in FunctionArgumentType class has changed, "
                + "please also update the proto and serialization code accordingly.")
        .that(TestUtil.getNonStaticFieldCount(FunctionArgumentType.class))
        .isEqualTo(4);
  }
}
