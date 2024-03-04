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

package com.google.zetasql;

import com.google.zetasql.ZetaSQLOptions.ProductMode;
import com.google.zetasql.ZetaSQLType.MapTypeProto;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.ZetaSQLType.TypeProto;
import java.util.Objects;

/** Represents a MAP<K, V> type, where K is the key and V is the value. */
public class MapType extends Type {
  static boolean equalsImpl(MapType type1, MapType type2, boolean equivalent) {
    return type1.keyType.equalsInternal(type2.keyType, equivalent)
        && type1.valueType.equalsInternal(type2.valueType, equivalent);
  }

  private final Type keyType;
  private final Type valueType;

  /** Private constructor, instances must be created with {@link TypeFactory} */
  MapType(Type keyType, Type valueType) {
    super(TypeKind.TYPE_MAP);
    this.keyType = keyType;
    this.valueType = valueType;
  }

  public Type getKeyType() {
    return keyType;
  }

  public Type getValueType() {
    return valueType;
  }

  @Override
  public void serialize(
      TypeProto.Builder typeProtoBuilder, FileDescriptorSetsBuilder fileDescriptorSetsBuilder) {
    typeProtoBuilder.setTypeKind(getKind());
    MapTypeProto.Builder map = typeProtoBuilder.getMapTypeBuilder();
    keyType.serialize(map.getKeyTypeBuilder(), fileDescriptorSetsBuilder);
    valueType.serialize(map.getValueTypeBuilder(), fileDescriptorSetsBuilder);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyType, valueType, getKind());
  }

  @Override
  public String typeName(ProductMode productMode) {
    return String.format(
        "MAP<%s, %s>", keyType.typeName(productMode), valueType.typeName(productMode));
  }

  @Override
  public String debugString(boolean details) {
    return String.format(
        "MAP<%s, %s>", keyType.debugString(details), valueType.debugString(details));
  }

  @Override
  public MapType asMap() {
    return this;
  }
}
