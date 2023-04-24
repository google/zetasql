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

import com.google.zetasql.ZetaSQLAnnotation.AnnotationMapProto;
import java.util.Optional;

/**
 * An {@link AnnotationMap} that represents a {@link ArrayType}. In addition to the annotation on
 * the whole type, this class also keeps an {@link AnnotationMap} for each field of the element type
 * of {@link ArrayType}.
 */
public final class ArrayAnnotationMap extends AnnotationMap {

  /**
   * AnnotationMap on array element. The element can be null, which indicates that the AnnotationMap
   * for the element (and all its children if applicable) is empty.
   */
  private AnnotationMap element;

  /** Creates an {@link ArrayAnnotationMap} with the given type. */
  static ArrayAnnotationMap create(ArrayType type) {
    return new ArrayAnnotationMap(type);
  }

  /** Creates an empty {@link ArrayAnnotationMap }. */
  static ArrayAnnotationMap create() {
    return new ArrayAnnotationMap();
  }

  /**
   * Gets the array element of {@link ArrayAnnotationMap}.
   *
   * <p>The returned element can be null if the element is null.
   */
  public AnnotationMap getElement() {
    return element;
  }

  @Override
  public boolean isArrayMap() {
    return true;
  }

  @Override
  public ArrayAnnotationMap asArrayMap() {
    return this;
  }

  @Override
  protected String debugStringInternal(Optional<Integer> annotationSpecId) {
    StringBuilder builder = new StringBuilder(super.debugStringInternal(annotationSpecId));
    builder.append("[");

    String elementDebugString =
        element == null ? "_" : element.debugStringInternal(annotationSpecId);
    if (elementDebugString.isEmpty() && !annotationSpecId.isPresent()) {
      elementDebugString = "{}";
    }
    builder.append(elementDebugString);
    builder.append("]");
    return builder.toString();
  }

  @Override
  public AnnotationMapProto serialize() {
    AnnotationMapProto proto = super.serialize();
    AnnotationMapProto.Builder builder = proto.toBuilder();
    if (element == null) {
      builder.setArrayElement(AnnotationMapProto.newBuilder().setIsNull(true).build());
    } else {
      builder.setArrayElement(element.serialize());
    }
    return builder.build();
  }

  final void setElement(AnnotationMap element) {
    this.element = element;
  }

  private ArrayAnnotationMap(ArrayType type) {
    element = AnnotationMap.create(type.getElementType());
  }

  private ArrayAnnotationMap() {
    element = null;
  }
}
