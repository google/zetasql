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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Ascii;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.ProtocolMessageEnum;
import com.google.zetasql.ZetaSQLDescriptorPool.ZetaSQLDescriptor;
import com.google.zetasql.ZetaSQLDescriptorPool.ZetaSQLEnumDescriptor;
import com.google.zetasql.ZetaSQLOptions.ProductMode;
import com.google.zetasql.ZetaSQLType.ArrayTypeProto;
import com.google.zetasql.ZetaSQLType.EnumTypeProto;
import com.google.zetasql.ZetaSQLType.ProtoTypeProto;
import com.google.zetasql.ZetaSQLType.StructFieldProto;
import com.google.zetasql.ZetaSQLType.StructTypeProto;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.ZetaSQLType.TypeProto;
import com.google.zetasql.StructType.StructField;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A factory for {@link Type} objects.
 *
 * A {@code TypeFactory} can be obtained via {@link #uniqueNames()} or {@link #nonUniqueNames()}
 * static factory methods, depending if you want to guarantee uniqueness of descriptor full names
 * for non-simple types or not, respectively.
 */
public abstract class TypeFactory implements Serializable {

  private static final ImmutableMap<String, TypeKind> SIMPLE_TYPE_KIND_NAMES =
      new ImmutableMap.Builder<String, TypeKind>()
          .put("int32", TypeKind.TYPE_INT32)
          .put("int64", TypeKind.TYPE_INT64) // external
          .put("uint32", TypeKind.TYPE_UINT32)
          .put("uint64", TypeKind.TYPE_UINT64)
          .put("bool", TypeKind.TYPE_BOOL) // external
          .put("boolean", TypeKind.TYPE_BOOL) // external
          .put("float", TypeKind.TYPE_FLOAT)
          .put("float32", TypeKind.TYPE_FLOAT)
          .put("double", TypeKind.TYPE_DOUBLE)
          .put("float64", TypeKind.TYPE_DOUBLE) // external
          .put("string", TypeKind.TYPE_STRING) // external
          .put("bytes", TypeKind.TYPE_BYTES) // external
          .put("date", TypeKind.TYPE_DATE) // external
          .put("timestamp", TypeKind.TYPE_TIMESTAMP) // external
          .put("time", TypeKind.TYPE_TIME) // external
          .put("datetime", TypeKind.TYPE_DATETIME) // external
          .put("geography", TypeKind.TYPE_GEOGRAPHY) // external
          .put("numeric", TypeKind.TYPE_NUMERIC) // external
          .build();

  //See (broken link) for approved list of externally visible types.
  private static final ImmutableSet<String> EXTERNAL_MODE_SIMPLE_TYPE_KIND_NAMES =
      ImmutableSet.of(
          "int64",
          "bool",
          "boolean",
          "float64",
          "string",
          "bytes",
          "date",
          "timestamp",
          "time",
          "datetime",
          "geography",
          "numeric");

  private static final ImmutableSet<TypeKind> SIMPLE_TYPE_KINDS =
      ImmutableSet.copyOf(SIMPLE_TYPE_KIND_NAMES.values());

  private static final ImmutableMap<TypeKind, SimpleType> SIMPLE_TYPES =
      Maps.toMap(
          SIMPLE_TYPE_KINDS,
          new Function<TypeKind, SimpleType>() {
            @Override
            public SimpleType apply(TypeKind typeKind) {
              return new SimpleType(typeKind);
            }
          });

  /**
   * Returns whether the given type kind is a simple type.
   *
   * <p>Simple types are those that can be represented with just a TypeKind, with no parameters.
   */
  public static boolean isSimpleType(TypeKind kind) {
    return SIMPLE_TYPE_KINDS.contains(kind);
  }

  /**
   * Returns whether {@code typeName} identifies a simple type.
   *
   * <p>Simple types are those that can be represented with just a TypeKind, with no parameters.
   */
  public static boolean isSimpleTypeName(String typeName, ProductMode prodMode) {
    if (prodMode == ProductMode.PRODUCT_EXTERNAL) {
      return EXTERNAL_MODE_SIMPLE_TYPE_KIND_NAMES.contains(Ascii.toLowerCase(typeName));
    }
    return SIMPLE_TYPE_KIND_NAMES.containsKey(Ascii.toLowerCase(typeName));
  }

  /**
   * Returns a TypeFactory which does *not* enforce uniquely named types.
   *
   * <p>The returned TypeFactory allows the creation of {@link EnumType EnumTypes} and {@link
   * ProtoType ProtoTypes} with different descriptors, even if they share the same full name.
   */
  public static TypeFactory nonUniqueNames() {
    return NonUniqueNamesTypeFactory.getInstance();
  }

  /**
   * Returns a TypeFactory which enforces uniquely named types.
   *
   * <p>For {@link EnumType EnumTypes} and {@link ProtoType ProtoTypes} created through the returned
   * factory, it is true that:
   *
   * <pre>
   * For all a, b,
   * if a.getDescriptor().getFullName().equals(b.getDescriptor().getFullName()),
   * then a == b.
   * </pre>
   */
  public static TypeFactory uniqueNames() {
    return new UniqueNamesTypeFactory();
  }

  /** Returns a SimpleType of given {@code kind}. */
  public static SimpleType createSimpleType(TypeKind kind) {
    return SIMPLE_TYPES.get(kind);
  }

  /** Returns a new ArrayType with the given {@code elementType}. */
  public static ArrayType createArrayType(Type elementType) {
    return new ArrayType(elementType);
  }

  /** Returns a StructType that contains the given {@code fields}. */
  public static StructType createStructType(Collection<StructType.StructField> fields) {
    return new StructType(fields);
  }

  /**
   * Returns a ProtoType with a proto message descriptor that is loaded from FileDescriptorSet with
   * {@link ZetaSQLDescriptorPool}.
   *
   * @param descriptor A ZetaSQLDescriptor that defines the ProtoType. A ZetaSQLDescriptor can
   *     be retrieved from a ZetaSQLDescriptorPool, which in turn can be created by loading a
   *     FileDescriptorSet protobuf.
   */
  public abstract ProtoType createProtoType(ZetaSQLDescriptor descriptor);

  /**
   * Returns a ProtoType with a generated (i.e. one that is compiled into the Java program)
   * descriptor.
   *
   * @param generatedDescriptorClass Class of a generated protocol message.
   */
  public abstract ProtoType createProtoType(Class<? extends Message> descriptorClass);

  /**
   * Returns a EnumType with a enum descriptor that is loaded from FileDescriptorSet with {@link
   * ZetaSQLDescriptorPool}.
   *
   * @param descriptor ZetaSQLEnumDescriptor that defines the EnumType. A ZetaSQLDescriptor can
   *     be retrieved from a ZetaSQLDescriptorPool, which in turn can be created by loading a
   *     FileDescriptorSet protobuf.
   */
  public abstract EnumType createEnumType(ZetaSQLEnumDescriptor descriptor);

  /**
   * Returns a EnumType with a generated (i.e. one that is compiled into the Java program) enum
   * descriptor.
   *
   * @param generatedEnumClass Class that is generated from a protobuf enum.
   */
  public abstract EnumType createEnumType(Class<? extends ProtocolMessageEnum> generatedEnumClass);

  /** Deserialize a self-contained {@link TypeProto} into a {@link Type}. */
  public abstract Type deserialize(TypeProto proto);

  /**
   * Deserialize a {@link TypeProto} into a {@link Type} using the given {@link
   * ZetaSQLDescriptorPool ZetaSQLDescriptorPools}.
   */
  public abstract Type deserialize(TypeProto proto, List<ZetaSQLDescriptorPool> pools);

  /**
   * A {@link TypeFactory} which implements methods based on {@link #createProtoType(Descriptor,
   * ZetaSQLDescriptorPool)} and {@link #createEnumType(EnumDescriptor, ZetaSQLDescriptorPool)}.
   */
  private abstract static class AbstractTypeFactory extends TypeFactory {

    /**
     * Creates a {@link ProtoType} using the given {@link Descriptor}.
     *
     * <p>The {@code Descriptor} together with the {@link ZetaSQLDescriptorPool} fully describes
     * the {@code ProtoType}.
     */
    protected abstract ProtoType createProtoType(
        Descriptor descriptor, ZetaSQLDescriptorPool pool);

    @Override
    public final ProtoType createProtoType(Class<? extends Message> messageClass) {
      Descriptor descriptor = getDescriptor(messageClass);
      ZetaSQLDescriptorPool.importIntoGeneratedPool(descriptor);
      ZetaSQLDescriptorPool pool = ZetaSQLDescriptorPool.getGeneratedPool();
      return createProtoType(descriptor, pool);
    }

    @Override
    public final ProtoType createProtoType(ZetaSQLDescriptor descriptor) {
      return createProtoType(descriptor.getDescriptor(), descriptor.getZetaSQLDescriptorPool());
    }

    /**
     * Creates a {@link EnumType} using the given {@link EnumDescriptor}.
     *
     * <p>The {@code EnumDescriptor} together with the {@link ZetaSQLDescriptorPool} fully
     * describes the {@code EnumType}.
     */
    protected abstract EnumType createEnumType(
        EnumDescriptor descriptor, ZetaSQLDescriptorPool pool);

    @Override
    public final EnumType createEnumType(ZetaSQLEnumDescriptor descriptor) {
      return createEnumType(descriptor.getDescriptor(), descriptor.getZetaSQLDescriptorPool());
    }

    @Override
    public final EnumType createEnumType(Class<? extends ProtocolMessageEnum> generatedEnumClass) {
      EnumDescriptor descriptor = getEnumDescriptor(generatedEnumClass);
      ZetaSQLDescriptorPool.importIntoGeneratedPool(descriptor);
      ZetaSQLDescriptorPool pool = ZetaSQLDescriptorPool.getGeneratedPool();
      return createEnumType(descriptor, pool);
    }

    @Override
    public final Type deserialize(TypeProto proto) {
      ImmutableList.Builder<ZetaSQLDescriptorPool> pools = ImmutableList.builder();
      for (FileDescriptorSet fileDescriptorSet : proto.getFileDescriptorSetList()) {
        ZetaSQLDescriptorPool pool = new ZetaSQLDescriptorPool();
        pool.importFileDescriptorSet(fileDescriptorSet);
        pools.add(pool);
      }
      return deserialize(proto, pools.build());
    }

    @Override
    public final Type deserialize(TypeProto proto, List<ZetaSQLDescriptorPool> pools) {
      if (TypeFactory.isSimpleType(proto.getTypeKind())) {
        return deserializeSimpleType(proto);
      }
      switch (proto.getTypeKind()) {
        case TYPE_ENUM:
          return deserializeEnumType(proto, pools);

        case TYPE_ARRAY:
          return deserializeArrayType(proto, pools);

        case TYPE_STRUCT:
          return deserializeStructType(proto, pools);

        case TYPE_PROTO:
          return deserializeProtoType(proto, pools);

        default:
          throw new IllegalArgumentException(
              String.format("proto.type_kind: %s", proto.getTypeKind()));
      }
    }

    private static Type deserializeSimpleType(TypeProto proto) {
      return createSimpleType(proto.getTypeKind());
    }

    private EnumType deserializeEnumType(TypeProto proto, List<ZetaSQLDescriptorPool> pools) {
      EnumTypeProto enumType = proto.getEnumType();

      String name = enumType.getEnumName();
      checkArgument(!name.isEmpty(), "Names missing from EnumTypeProto %s", enumType);

      int index = enumType.getFileDescriptorSetIndex();
      checkArgument(
          index >= 0 && index < pools.size(), "FileDescriptorSetIndex out of bound: %s", enumType);

      ZetaSQLDescriptorPool pool = pools.get(index);

      ZetaSQLEnumDescriptor descriptor = pool.findEnumTypeByName(name);
      checkNotNull(descriptor, "Enum descriptor not found: %s", enumType);

      String filename = descriptor.getDescriptor().getFile().getName();
      checkArgument(
          filename.equals(enumType.getEnumFileName()),
          "Enum %s found in wrong file: %s",
          enumType,
          filename);

      return createEnumType(descriptor.getDescriptor(), pool);
    }

    private ArrayType deserializeArrayType(TypeProto proto, List<ZetaSQLDescriptorPool> pools) {
      ArrayTypeProto arrayType = proto.getArrayType();
      return createArrayType(deserialize(arrayType.getElementType(), pools));
    }

    private StructType deserializeStructType(TypeProto proto, List<ZetaSQLDescriptorPool> pools) {
      StructTypeProto structType = proto.getStructType();
      ImmutableList.Builder<StructField> fields = ImmutableList.builder();
      for (StructFieldProto field : structType.getFieldList()) {
        fields.add(new StructField(field.getFieldName(), deserialize(field.getFieldType(), pools)));
      }
      return createStructType(fields.build());
    }

    private ProtoType deserializeProtoType(TypeProto proto, List<ZetaSQLDescriptorPool> pools) {
      ProtoTypeProto protoType = proto.getProtoType();

      String name = protoType.getProtoName();
      checkArgument(!name.isEmpty(), "Name missing from ProtoTypeProto %s", protoType);

      int index = protoType.getFileDescriptorSetIndex();
      checkArgument(
          index >= 0 && index < pools.size(), "FileDescriptorSetIndex out of bound: %s", protoType);

      ZetaSQLDescriptorPool pool = pools.get(index);

      ZetaSQLDescriptor descriptor = pool.findMessageTypeByName(name);
      checkNotNull(descriptor, "Proto descriptor not found: ", protoType);

      String filename = descriptor.getDescriptor().getFile().getName();
      checkArgument(
          filename.equals(protoType.getProtoFileName()),
          "Proto %s found in wrong file: %s",
          protoType,
          filename);

      return createProtoType(descriptor.getDescriptor(), pool);
    }
  }

  private static Descriptor getDescriptor(Class<? extends MessageOrBuilder> type) {
    try {
      return (Descriptor) type.getMethod("getDescriptor").invoke(null);
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static EnumDescriptor getEnumDescriptor(Class<? extends ProtocolMessageEnum> type) {
    try {
      return (EnumDescriptor) type.getMethod("getDescriptor").invoke(null);
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /** A {@link TypeFactory} which does *not* ensure uniquely named types. */
  private static final class NonUniqueNamesTypeFactory extends AbstractTypeFactory {
    private static final NonUniqueNamesTypeFactory INSTANCE = new NonUniqueNamesTypeFactory();

    static NonUniqueNamesTypeFactory getInstance() {
      return INSTANCE;
    }

    private NonUniqueNamesTypeFactory() {
      super();
    }

    @Override
    protected ProtoType createProtoType(Descriptor descriptor, ZetaSQLDescriptorPool pool) {
      return new ProtoType(descriptor, pool);
    }

    @Override
    protected EnumType createEnumType(EnumDescriptor descriptor, ZetaSQLDescriptorPool pool) {
      return new EnumType(descriptor, pool);
    }
  }

  /** A {@link TypeFactory} which ensures uniquely named types. */
  private static final class UniqueNamesTypeFactory extends AbstractTypeFactory {
    private Map<String, EnumType> enumTypesByName;
    private Map<String, ProtoType> protoTypesByName;

    UniqueNamesTypeFactory() {
      super();
      enumTypesByName = new HashMap<>();
      protoTypesByName = new HashMap<>();
    }

    @Override
    public ProtoType createProtoType(Descriptor descriptor, ZetaSQLDescriptorPool pool) {
      if (protoTypesByName.containsKey(descriptor.getFullName())) {
        return protoTypesByName.get(descriptor.getFullName());
      }
      ProtoType protoType = new ProtoType(descriptor, pool);
      protoTypesByName.put(descriptor.getFullName(), protoType);
      return protoType;
    }

    @Override
    public EnumType createEnumType(EnumDescriptor descriptor, ZetaSQLDescriptorPool pool) {
      if (enumTypesByName.containsKey(descriptor.getFullName())) {
        return enumTypesByName.get(descriptor.getFullName());
      }
      EnumType enumType = new EnumType(descriptor, pool);
      enumTypesByName.put(descriptor.getFullName(), enumType);
      return enumType;
    }
  }
}
