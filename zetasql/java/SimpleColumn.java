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

import com.google.common.collect.ImmutableList;
import com.google.zetasql.ZetaSQLType.TypeProto;
import com.google.zetasql.SimpleTableProtos.SimpleColumnProto;
import java.io.Serializable;

/** SimpleColumn is a concrete implementation of the Column interface. */
public final class SimpleColumn implements Column, Serializable {

  private final String name;
  private final String fullName;
  private Type type;
  private boolean isPseudoColumn;
  private boolean isWritableColumn;

  SimpleColumn(
      String tableName, String name, Type type, boolean isPseudoColumn, boolean isWritableColumn) {
    this.name = name == null ? "" : name;
    this.fullName = String.format("%s.%s", tableName, name);
    this.type = type;
    this.isPseudoColumn = isPseudoColumn;
    this.isWritableColumn = isWritableColumn;
  }

  SimpleColumn(String tableName, String name, Type type) {
    this(tableName, name, type, /* isPseudoColumn = */ false, /* isWritableColumn = */ true);
  }

  /**
   * Serialize this column into protobuf, with
   * FileDescriptors emitted to the builder as needed.
   */
  public SimpleColumnProto serialize(FileDescriptorSetsBuilder fileDescriptorSetsBuilder) {
    SimpleColumnProto.Builder builder = SimpleColumnProto.newBuilder();
    builder.setName(name);
    TypeProto.Builder typeProtoBuilder = TypeProto.newBuilder();
    type.serialize(typeProtoBuilder, fileDescriptorSetsBuilder);
    builder.setType(typeProtoBuilder.build());
    builder.setIsPseudoColumn(isPseudoColumn);
    builder.setIsWritableColumn(isWritableColumn);
    return builder.build();
  }

  /**
   * Deserialize a proto into a new column with existing Descriptor pools.
   * Types will be deserialized using the given TypeFactory and Descriptors
   * from the given pools. The DescriptorPools should have been created by
   * type serialization, and all proto types are treated as references into
   * these pools.
   *
   * @param proto
   * @param tableName Callers should give name of table the column belongs to.
   * @param pools contains all proto type
   * @param factory for type creating
   * @return a new SimpleColumn
   * @throws IllegalArgumentException if the proto is inconsistent.
   */
  public static SimpleColumn deserialize(
      SimpleColumnProto proto,
      String tableName,
      ImmutableList<ZetaSQLDescriptorPool> pools,
      TypeFactory factory) {
    Type type = factory.deserialize(proto.getType(), pools);
    return new SimpleColumn(
        tableName, proto.getName(), type, proto.getIsPseudoColumn(), proto.getIsWritableColumn());
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getFullName() {
    return fullName;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public boolean isPseudoColumn() {
    return isPseudoColumn;
  }

  @Override
  public boolean isWritableColumn() {
    return isWritableColumn;
  }
}
