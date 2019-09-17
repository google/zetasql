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
import static com.google.zetasql.TypeTestBase.getDescriptorPoolWithTypeProtoAndTypeKind;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.testing.SerializableTester;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;
import com.google.zetasql.ZetaSQLOptions.ProductMode;
import com.google.zetasql.ZetaSQLType.EnumTypeProto;
import com.google.zetasql.ZetaSQLType.ProtoTypeProto.Builder;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.ZetaSQLType.TypeProto;

import java.util.ArrayList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)

public class TypeFactoryTest {

  @Test
  public void testIsSimpleType() {
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_ARRAY)).isFalse();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_BOOL)).isTrue();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_BYTES)).isTrue();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_DATE)).isTrue();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_DOUBLE)).isTrue();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_ENUM)).isFalse();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_FLOAT)).isTrue();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_INT32)).isTrue();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_INT64)).isTrue();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_PROTO)).isFalse();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_STRING)).isTrue();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_STRUCT)).isFalse();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_TIMESTAMP)).isTrue();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_UINT32)).isTrue();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_UINT64)).isTrue();
    assertThat(TypeFactory.isSimpleType(TypeKind.TYPE_UNKNOWN)).isFalse();
  }

  @Test
  public void testIsSimpleTypeName() {
    assertThat(TypeFactory.isSimpleTypeName("Array", ProductMode.PRODUCT_EXTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("BOOL", ProductMode.PRODUCT_EXTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("Boolean", ProductMode.PRODUCT_EXTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("BYTES", ProductMode.PRODUCT_EXTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("DATE", ProductMode.PRODUCT_EXTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("double", ProductMode.PRODUCT_EXTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("float", ProductMode.PRODUCT_EXTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("float32", ProductMode.PRODUCT_EXTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("float64", ProductMode.PRODUCT_EXTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("ENUM", ProductMode.PRODUCT_EXTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("FLOAT", ProductMode.PRODUCT_EXTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("INT32", ProductMode.PRODUCT_EXTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("INT64", ProductMode.PRODUCT_EXTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("PROTO", ProductMode.PRODUCT_EXTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("STRING", ProductMode.PRODUCT_EXTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("STRUCT", ProductMode.PRODUCT_EXTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("TIMESTAMP", ProductMode.PRODUCT_EXTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("UINT32", ProductMode.PRODUCT_EXTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("UINT64", ProductMode.PRODUCT_EXTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("UNKNOWN", ProductMode.PRODUCT_EXTERNAL)).isFalse();

    assertThat(TypeFactory.isSimpleTypeName("Array", ProductMode.PRODUCT_INTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("BOOL", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("Boolean", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("BYTES", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("DATE", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("double", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("ENUM", ProductMode.PRODUCT_INTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("FLOAT", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("FLOAT32", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("FLOAT64", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("INT32", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("INT64", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("PROTO", ProductMode.PRODUCT_INTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("STRING", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("STRUCT", ProductMode.PRODUCT_INTERNAL)).isFalse();
    assertThat(TypeFactory.isSimpleTypeName("TIMESTAMP", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("UINT32", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("UINT64", ProductMode.PRODUCT_INTERNAL)).isTrue();
    assertThat(TypeFactory.isSimpleTypeName("UNKNOWN", ProductMode.PRODUCT_INTERNAL)).isFalse();
  }

  @Test
  public void testCreateSimpleType() {
    for (TypeKind kind : TypeKind.values()) {
      if (TypeFactory.isSimpleType(kind)) {
        SimpleType type = TypeFactory.createSimpleType(kind);
        assertThat(type.getKind()).isEqualTo(kind);
        assertThat(type.isSimpleType()).isTrue();
      } else {
        assertThat(TypeFactory.createSimpleType(kind)).isNull();
      }
    }
  }

  @Test
  public void testCreateEnumType() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    EnumType type = factory.createEnumType(TypeKind.class);
    assertThat(type.getKind()).isEqualTo(TypeKind.TYPE_ENUM);
    assertThat(type.getDescriptor()).isEqualTo(TypeKind.getDescriptor());
  }

  @Test
  public void testCreateProtoType() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ProtoType type = factory.createProtoType(TypeProto.class);
    assertThat(type.getKind()).isEqualTo(TypeKind.TYPE_PROTO);
    assertThat(type.getDescriptor()).isEqualTo(TypeProto.getDescriptor());
  }

  @Test
  public void testCreateArrayType() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    EnumType type = factory.createEnumType(TypeKind.class);
    ArrayType array = TypeFactory.createArrayType(type);
    assertThat(array.getKind()).isEqualTo(TypeKind.TYPE_ARRAY);
    assertThat(array.getElementType()).isEqualTo(type);
  }

  @Test
  public void testCreateStructType() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    EnumType enumType = factory.createEnumType(TypeKind.class);
    ProtoType protoType = factory.createProtoType(TypeProto.class);

    ArrayList<StructType.StructField> fields = new ArrayList<>();
    fields.add(new StructType.StructField("enum", enumType));
    fields.add(new StructType.StructField("proto", protoType));

    StructType struct = TypeFactory.createStructType(fields);
    assertThat(struct.getKind()).isEqualTo(TypeKind.TYPE_STRUCT);
    assertThat(struct.getFieldCount()).isEqualTo(2);
    assertThat(struct.getField(0).getName()).isEqualTo("enum");
    assertThat(struct.getField(1).getName()).isEqualTo("proto");
    assertThat(struct.getField(0).getType()).isEqualTo(enumType);
    assertThat(struct.getField(1).getType()).isEqualTo(protoType);
  }

  @Test
  public void testDedupByName() {
    TypeFactory factory = TypeFactory.uniqueNames();
    EnumType enumType = factory.createEnumType(TypeKind.class);
    ProtoType protoType = factory.createProtoType(TypeProto.class);
    EnumType enumType2 = factory.createEnumType(TypeKind.class);
    ProtoType protoType2 = factory.createProtoType(TypeProto.class);
    assertThat(enumType).isSameInstanceAs(enumType2);
    assertThat(protoType).isSameInstanceAs(protoType2);
    try {
      EnumType enumType3 = factory.deserialize(enumType2.serialize()).asEnum();
      ProtoType protoType3 = factory.deserialize(protoType2.serialize()).asProto();
      assertThat(enumType3).isSameInstanceAs(enumType2);
      assertThat(protoType3).isSameInstanceAs(protoType2);
    } catch (IllegalArgumentException e) {
      fail(e.toString());
    }
  }

  @Test
  public void testNoDedupByName() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    EnumType enumType = factory.createEnumType(TypeKind.class);
    ProtoType protoType = factory.createProtoType(TypeProto.class);
    EnumType enumType2 = factory.createEnumType(TypeKind.class);
    ProtoType protoType2 = factory.createProtoType(TypeProto.class);
    assertThat(enumType).isNotSameInstanceAs(enumType2);
    assertThat(protoType).isNotSameInstanceAs(protoType2);
    assertThat(enumType.equivalent(enumType2)).isTrue();
    assertThat(protoType.equivalent(protoType2)).isTrue();
    try {
      EnumType enumType3 = factory.deserialize(enumType2.serialize()).asEnum();
      ProtoType protoType3 = factory.deserialize(protoType2.serialize()).asProto();
      assertThat(enumType3).isNotSameInstanceAs(enumType2);
      assertThat(protoType3).isNotSameInstanceAs(protoType2);
      assertThat(enumType3.equivalent(enumType2)).isTrue();
      assertThat(protoType3.equivalent(protoType2)).isTrue();
    } catch (IllegalArgumentException e) {
      fail(e.toString());
    }
  }

  @Test
  public void testDeserializeInvalidTypeProto() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    TypeProto.Builder builder = TypeProto.newBuilder();
    try {
      factory.deserialize(builder.build());
      fail("Should throw when deserializing empty TypeProto.");
    } catch (IllegalArgumentException expected) {
    }

    builder.setTypeKind(TypeKind.TYPE_UNKNOWN);
    try {
      factory.deserialize(builder.build());
      fail("Should throw when deserializing unknown type.");
    } catch (IllegalArgumentException expected) {
    }

    builder.setTypeKind(TypeKind.TYPE_ARRAY);
    try {
      factory.deserialize(builder.build());
      fail("Should throw when deserializing array without element.");
    } catch (IllegalArgumentException expected) {
    }

    TypeProto.Builder elementBuilder = builder.getArrayTypeBuilder().getElementTypeBuilder();
    elementBuilder.setTypeKind(TypeKind.TYPE_ARRAY);
    elementBuilder.getArrayTypeBuilder().getElementTypeBuilder().setTypeKind(TypeKind.TYPE_BOOL);
    try {
      factory.deserialize(builder.build());
      fail("Should throw when deserializing array of array.");
    } catch (IllegalArgumentException expected) {
    }

    builder.clearArrayType();
    builder.setTypeKind(TypeKind.TYPE_ENUM);
    try {
      factory.deserialize(builder.build());
      fail("Should throw when deserializing empty enum.");
    } catch (IllegalArgumentException expected) {
    }

    EnumTypeProto.Builder enumBuilder = builder.getEnumTypeBuilder();
    try {
      factory.deserialize(builder.build());
      fail("Should throw when deserializing enum without name.");
    } catch (IllegalArgumentException expected) {
    }

    enumBuilder.setEnumName("zetasql.TypeKind");
    try {
      factory.deserialize(builder.build());
      fail("Should throw when deserializing enum without DescriptorPools.");
    } catch (IllegalArgumentException expected) {
    }

    ZetaSQLDescriptorPool pool = getDescriptorPoolWithTypeProtoAndTypeKind();
    enumBuilder.setEnumFileName("zetasql/public/type.proto");
    try {
      factory.deserialize(builder.build(), Lists.newArrayList(pool));
    } catch (IllegalArgumentException e) {
      fail(e.toString());
    }

    enumBuilder.setEnumName("zetasql.UnknownMessage");
    try {
      factory.deserialize(builder.build(), Lists.newArrayList(pool));
      fail("Should throw when deserializing enum with wrong type name.");
    } catch (NullPointerException expected) {
    }

    enumBuilder.setEnumName("zetasql.TypeKind");
    enumBuilder.setFileDescriptorSetIndex(-1);
    try {
      factory.deserialize(builder.build(), Lists.newArrayList(pool));
      fail("Should throw when deserializing enum with wrong FileDescriptorSetIndex.");
    } catch (IllegalArgumentException expected) {
    }

    enumBuilder.setFileDescriptorSetIndex(1);
    try {
      factory.deserialize(builder.build(), Lists.newArrayList(pool));
      fail("Should throw when deserializing enum with wrong FileDescriptorSetIndex.");
    } catch (IllegalArgumentException expected) {
    }

    try {
      factory.deserialize(builder.build(), Lists.newArrayList(pool, new ZetaSQLDescriptorPool()));
      fail("Should throw when deserializing enum with wrong FileDescriptorSet.");
    } catch (NullPointerException expected) {
    }

    enumBuilder.setFileDescriptorSetIndex(0);
    enumBuilder.setEnumFileName("zetasql/public/wrong.proto");
    try {
      factory.deserialize(builder.build(), Lists.newArrayList(pool));
      fail("Should throw when deserializing enum with wrong filename.");
    } catch (IllegalArgumentException expected) {
    }

    builder.clearEnumType();
    builder.setTypeKind(TypeKind.TYPE_PROTO);
    Builder protoBuilder = builder.getProtoTypeBuilder();
    try {
      factory.deserialize(builder.build());
      fail("Should throw when deserializing proto without name.");
    } catch (IllegalArgumentException expected) {
    }

    protoBuilder.setProtoName("zetasql.TypeProto");
    try {
      factory.deserialize(builder.build());
      fail("Should throw when deserializing proto without DescriptorPools.");
    } catch (IllegalArgumentException expected) {
    }

    protoBuilder.setProtoFileName("zetasql/public/type.proto");
    try {
      factory.deserialize(builder.build(), Lists.newArrayList(pool));
    } catch (IllegalArgumentException e) {
      fail(e.toString());
    }

    protoBuilder.setProtoName("zetasql.UnknownMessage");
    try {
      factory.deserialize(builder.build(), Lists.newArrayList(pool));
      fail("Should throw when deserializing proto with wrong type name.");
    } catch (NullPointerException expected) {
    }

    protoBuilder.setProtoName("zetasql.TypeProto");
    protoBuilder.setFileDescriptorSetIndex(-1);
    try {
      factory.deserialize(builder.build(), Lists.newArrayList(pool));
      fail("Should throw when deserializing proto with wrong FileDescriptorSetIndex.");
    } catch (IllegalArgumentException expected) {
    }

    protoBuilder.setFileDescriptorSetIndex(1);
    try {
      factory.deserialize(builder.build(), Lists.newArrayList(pool));
      fail("Should throw when deserializing proto with wrong FileDescriptorSetIndex.");
    } catch (IllegalArgumentException expected) {
    }

    try {
      factory.deserialize(builder.build(), Lists.newArrayList(pool, new ZetaSQLDescriptorPool()));
      fail("Should throw when deserializing proto with wrong FileDescriptorSet.");
    } catch (NullPointerException expected) {
    }

    protoBuilder.setFileDescriptorSetIndex(0);
    protoBuilder.setProtoFileName("zetasql/public/wrong.proto");
    try {
      factory.deserialize(builder.build(), Lists.newArrayList(pool));
      fail("Should throw when deserializing proto with wrong filename.");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testNonGeneratedClass() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    try {
      factory.createProtoType(BadGeneratedProto.class);
      fail("Should throw when trying to create proto type from non-generated proto class.");
    } catch (IllegalArgumentException expected) {
    }

    try {
      factory.createEnumType(BadGeneratedEnum.class);
      fail("Should throw when trying to create proto type from non-generated enum class.");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testSerializable() {
    TypeFactory factory = TypeFactory.nonUniqueNames();
    ProtoType protoType = factory.createProtoType(TypeProto.class);
    EnumType enumType = factory.createEnumType(TypeKind.class);
    ArrayType arrayType = TypeFactory.createArrayType(protoType);
    ArrayList<StructType.StructField> fields = new ArrayList<>();
    fields.add(new StructType.StructField("enum", enumType));
    fields.add(new StructType.StructField("proto", protoType));
    StructType structType = TypeFactory.createStructType(fields);

    TypeFactory factory2 = SerializableTester.reserialize(factory);
    ProtoType protoType2 = factory2.createProtoType(TypeProto.class);
    EnumType enumType2 = factory2.createEnumType(TypeKind.class);
    ArrayType arrayType2 = TypeFactory.createArrayType(protoType2);
    fields = new ArrayList<>();
    fields.add(new StructType.StructField("enum", enumType));
    fields.add(new StructType.StructField("proto", protoType));
    StructType structType2 = TypeFactory.createStructType(fields);

    // The descriptors used in these types will be different references due to the fact that proto
    // descriptors aren't serializable, but the types should otherwise be equivalent.
    assertThat(protoType.equivalent(protoType2)).isTrue();
    assertThat(enumType.equivalent(enumType2)).isTrue();
    assertThat(arrayType.equivalent(arrayType2)).isTrue();
    assertThat(structType.equivalent(structType2)).isTrue();
  }

  private static final class BadGeneratedProto extends GeneratedMessage {
    @Override
    public com.google.protobuf.Message.Builder newBuilderForType() {
      return null;
    }

    @Override
    public com.google.protobuf.Message.Builder toBuilder() {
      return null;
    }

    @Override
    public Message getDefaultInstanceForType() {
      return null;
    }

    @Override
    protected FieldAccessorTable internalGetFieldAccessorTable() {
      return null;
    }

    @Override
    protected com.google.protobuf.Message.Builder newBuilderForType(BuilderParent parent) {
      return null;
    }
  }

  private static final class BadGeneratedEnum implements ProtocolMessageEnum {
    @Override
    public int getNumber() {
      return 0;
    }

    @Override
    public EnumValueDescriptor getValueDescriptor() {
      return null;
    }

    @Override
    public EnumDescriptor getDescriptorForType() {
      return null;
    }
  }
}
