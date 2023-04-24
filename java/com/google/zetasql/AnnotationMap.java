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

import static java.util.stream.Collectors.joining;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.zetasql.ZetaSQLAnnotation.AnnotationMapProto;
import com.google.zetasql.ZetaSQLAnnotation.AnnotationProto;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

// TODO: Implement AnnotationSpec class.
/** A map from the AnnotationSpec ID to the {@link SimpleValue}. */
public class AnnotationMap {

  /**
   * A map from AnnotationSpec ID to {@link SimpleValue} to represent annotation values of
   * annotation ids.
   */
  private final Map<Integer, SimpleValue> annotations;

  protected AnnotationMap() {
    annotations = new HashMap<>();
  }

  /**
   * Creates an instance of AnnotationMap.
   *
   * <p>Returns a {@link StructAnnotationMap} instance if <type> is a {@link StructType}. Returns an
   * {@link ArrayAnnotationMap} if <type> is an {@link ArrayType}.
   */
  public static AnnotationMap create(Type type) {
    if (type.isStruct()) {
      return StructAnnotationMap.create(type.asStruct());
    } else if (type.isArray()) {
      return ArrayAnnotationMap.create(type.asArray());
    } else {
      return new AnnotationMap();
    }
  }

  /**
   * Sets annotation value for given AnnotationSpec ID, overwriting existing value if it exists.
   *
   * <p>Returns a self reference for caller to be able to chain SetAnnotation calls.
   */
  @CanIgnoreReturnValue
  public AnnotationMap setAnnotation(int id, SimpleValue value) {
    if (!value.isValid()) {
      throw new IllegalArgumentException("The value is invalid, value: " + value.debugString());
    }
    annotations.put(id, value);
    return this;
  }

  /** Clears annotation value for the given AnnotationSpec ID if it exists. */
  public void unsetAnnotation(int id) {
    annotations.remove(id);
  }

  /**
   * Returns annotation value for given AnnotationSpec ID. Returns null if the ID is not in the map.
   */
  public SimpleValue getAnnotation(int id) {
    return annotations.getOrDefault(id, null);
  }

  /** Returns whether the annotation map is a {@link StructAnnotationMap}. */
  public boolean isStructMap() {
    return false;
  }

  /** Returns whether the annotation map is a {@link ArrayAnnotationMap}. */
  public boolean isArrayMap() {
    return false;
  }

  /** Returns a {@link StructAnnotationMap} if it's a struct map, otherwise returns a null. */
  public StructAnnotationMap asStructMap() {
    return null;
  }

  /** Returns a {@link ArrayAnnotationMap} if it's a array map, otherwise returns a null. */
  public ArrayAnnotationMap asArrayMap() {
    return null;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof AnnotationMap) {
      AnnotationMap other = (AnnotationMap) object;
      return equalsInternal(this, other, /* annotationSpecId= */ Optional.empty());
    }
    return false;
  }

  /**
   * Returns true if {@code lhs} and {@code rhs} are equal, false otherwise.
   *
   * <p>The comparison accepts null and treats null to be equal to an empty {@link AnnotationMap}
   * (both for <lhs> and <rhs> as well as for any nested maps).
   */
  public static boolean equals(AnnotationMap lhs, AnnotationMap rhs) {
    return equalsInternal(lhs, rhs, /* annotationSpecId= */ Optional.empty());
  }

  @Override
  public int hashCode() {
    if (isStructMap()) {
      return Objects.hash(annotations, asStructMap().getFields());
    } else if (isArrayMap()) {
      return Objects.hash(annotations, asArrayMap().getElement());
    }
    return Objects.hashCode(annotations);
  }

  /** Returns true if this {@link AnnotationMap} and all nested {@link AnnotationMap} is empty. */
  public boolean isEmpty() {
    return isEmptyInternal(/* annotationSpecId= */ Optional.empty());
  }

  // TODO: Add an AnnotationSpec templated has method
  /**
   * Returns if the {@link AnnotationMap} or any of the nested {@link AnnotationMap} has an
   * annotation for the given annotation spec id.
   */
  public boolean has(int annotationSpecId) {
    return !isEmptyInternal(Optional.of(annotationSpecId));
  }

  /**
   * Returns whether two {@link AnnotationMap} instances have equal annotation values.
   *
   * <p>This comparison will be conducted recursively on all nested levels for the specified
   * annotation spec id (all other annotations are ignored for this comparison).
   */
  public boolean hasEqualAnnotations(AnnotationMap that, int annotationSpecId) {
    return equalsInternal(this, that, Optional.of(annotationSpecId));
  }

  /**
   * Returns whether two AnnotationMap instances have equal annotation values with given specified
   * annotation spc ID.
   *
   * <p>This method accepts null and treats null to be equal to a non-null {@link AnnotationMap}
   * that does not contain the specified annotation spec id.
   */
  public static boolean hasEqualAnnotationsBetween(
      AnnotationMap lhs, AnnotationMap rhs, int annotationSpecId) {
    return equalsInternal(lhs, rhs, Optional.of(annotationSpecId));
  }

  /**
   * Returns true if this {@code AnnotationMap} has compatible nested structure with {@code type}.
   *
   * <p>The structures are compatible when they meet one of the conditions below:
   *
   * <ul>
   *   <li>This instance and {@code type} both are non {@link StructType} and non {@link ArrayType}.
   *   <li>This instance is a {@link StructAnnotationMap} and {@code type} is a {@link StructType}
   *       (and the number of fields matches), and its fields are either null or are compatible by
   *       recursively following these rules.
   *   <li>This instance is an {@link ArrayAnnotationMap} and {@code type} is an {@link ArrayType},
   *       and its element is null or is compatible by recursively following these rules.
   * </ul>
   *
   * <p>When an annotation map is null, it indicates that the annotation map is empty on all the
   * nested levels, and therefore such maps are compatible with any types (including {@link
   * StructType} and {@link ArrayType}).
   */
  public boolean hasCompatibleStructure(Type type) {
    if (isStructMap()) {
      if (!type.isStruct() || asStructMap().getFieldCount() != type.asStruct().getFieldCount()) {
        return false;
      }
      for (int i = 0; i < asStructMap().getFieldCount(); i++) {
        AnnotationMap fieldAnnotationMap = asStructMap().getField(i);
        if (fieldAnnotationMap != null
            && !fieldAnnotationMap.hasCompatibleStructure(type.asStruct().getField(i).getType())) {
          return false;
        }
      }
      return true;
    } else if (isArrayMap()) {
      return type.isArray()
          && (asArrayMap().getElement() == null
              || asArrayMap().getElement().hasCompatibleStructure(type.asArray().getElementType()));
    }
    return !type.isStruct() && !type.isArray();
  }

  /** Serializes this instance to an {@link AnnotationMapProto} protobuf. */
  public AnnotationMapProto serialize() {
    AnnotationMapProto.Builder builder = AnnotationMapProto.newBuilder();
    annotations.forEach(
        (id, annotation) ->
            builder.addAnnotations(
                AnnotationProto.newBuilder().setId(id).setValue(annotation.serialize()).build()));
    return builder.build();
  }

  /** Deserializes the {@link AnnotationMapProto} protobuf to an {@link AnnotationMap} instance. */
  public static AnnotationMap deserialize(AnnotationMapProto proto) {
    if (proto.getIsNull()) {
      throw new IllegalArgumentException(
          "is_null could only be true for struct field or array element");
    }
    AnnotationMap annotationMap;
    //  Recursively handle struct fields and array element.
    if (proto.getStructFieldsCount() > 0) {
      annotationMap = StructAnnotationMap.create();
      for (int i = 0; i < proto.getStructFieldsCount(); i++) {
        AnnotationMap fieldAnnotationMap = null;
        if (!proto.getStructFields(i).getIsNull()) {
          fieldAnnotationMap = deserialize(proto.getStructFields(i));
        }
        annotationMap.asStructMap().addField(fieldAnnotationMap);
      }
    } else if (proto.hasArrayElement()) {
      annotationMap = ArrayAnnotationMap.create();
      // The default element of ArrayAnnotationMap is null, so we don't need to re-assign a null to
      // the element when the proto array element is null.
      if (!proto.getArrayElement().getIsNull()) {
        annotationMap.asArrayMap().setElement(deserialize(proto.getArrayElement()));
      }
    } else {
      annotationMap = new AnnotationMap();
    }

    // Deserialize annotation map.
    for (AnnotationProto annotation : proto.getAnnotationsList()) {
      SimpleValue value = SimpleValue.deserialize(annotation.getValue());
      annotationMap.setAnnotation((int) annotation.getId(), value);
    }
    return annotationMap;
  }

  /** Returns the debug string of this {@link AnnotationMap}. */
  public String debugString() {
    return debugStringInternal(/* annotationSpecId= */ Optional.empty());
  }

  /**
   * Returns the debug string of this {@link AnntationMap} with the given {@code annotationSpecId}.
   */
  public String debugString(int annotationSpecId) {
    return debugStringInternal(Optional.of(annotationSpecId));
  }

  /**
   * Returns the debug string of this {@link AnntationMap} with the optional {@code
   * annotationSpecId}.
   */
  protected String debugStringInternal(Optional<Integer> annotationSpecId) {
    if (annotations.isEmpty()) {
      return "";
    }
    if (annotationSpecId.isPresent()) {
      SimpleValue annotation = getAnnotation(annotationSpecId.get());
      if (annotation != null) {
        return annotation.debugString();
      }
      return "";
    }

    // outputs a debug string for all annotation spec ids.
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    builder.append(
        annotations.entrySet().stream()
            .map(
                entry -> {
                  String annotationId;
                  if (entry.getKey() <= AnnotationKind.MAX_BUILTIN_ANNOTATION_KIND.getValue()) {
                    annotationId = getAnnotationKindName(AnnotationKind.fromId(entry.getKey()));
                  } else {
                    annotationId = String.valueOf(entry.getKey());
                  }
                  return annotationId + ":" + entry.getValue().debugString();
                })
            .collect(joining(", ")));
    builder.append("}");
    return builder.toString();
  }

  /**
   * Normalizes a {@link AnnotationMap} by replacing empty annotation maps with NULL.
   *
   * <p>After normalization, on all the nested levels:
   *
   * <ul/>
   *   <li/>For a {@link StructAnnotationMap}, each one of its fields is either null or non-empty.
   *   <li/>For an {@link ArrayAnnotationMap}, its element is either null or non-empty.
   * </ul>
   */
  public void normalize() {
    normalizeInternal();
  }

  /** Returns the kind's name. */
  public static String getAnnotationKindName(AnnotationKind kind) {
    switch (kind) {
      case COLLATION:
        return "Collation";
      case SAMPLE_ANNOTATION:
        return "SampleAnnotation";
      case MAX_BUILTIN_ANNOTATION_KIND:
        return "MaxBuiltinAnnotationKind";
    }
    throw new IllegalArgumentException("Unexpected AnnotationKind: " + kind);
  }

  /** Built-in annotation IDs. */
  public static enum AnnotationKind {
    /** Annotation ID for CollationAnnotation. */
    COLLATION(1),

    /** Annotation ID for the SampleAnnotation, which is used for testing purposes only. */
    SAMPLE_ANNOTATION(2),

    /** Annotation ID up to kMaxBuiltinAnnotationKind are reserved for built-in annotations. */
    MAX_BUILTIN_ANNOTATION_KIND(10000);

    private final int id;

    AnnotationKind(int id) {
      this.id = id;
    }

    public int getValue() {
      return id;
    }

    public static AnnotationKind fromId(int id) {
      for (AnnotationKind kind : AnnotationKind.values()) {
        if (kind.getValue() == id) {
          return kind;
        }
      }
      throw new IllegalArgumentException("Unexpected AnnotationKind id: " + id);
    }
  }

  /**
   * Returns true if this instance is in the simplest form described in {@link #normalize()}
   * comments.
   *
   * <p>This function is mainly for testing purpose.
   */
  boolean isNormailized() {
    return isNormailizedInternal(/* checkEmpty= */ false);
  }

  /**
   * Normailizes the {@link AnnotationMap} and returns true if the annotation map is empty on all
   * nested levels.
   */
  @CanIgnoreReturnValue
  boolean normalizeInternal() {
    boolean empty = annotations.isEmpty();
    if (isStructMap()) {
      StructAnnotationMap structMap = asStructMap();
      for (int i = 0; i < structMap.getFieldCount(); i++) {
        AnnotationMap field = structMap.getField(i);
        if (field == null) {
          continue;
        }
        // If the field is empty after normalization, then replace the field to null.
        if (field.normalizeInternal()) {
          structMap.setField(i, null);
        } else {
          empty = false;
        }
      }
    } else if (isArrayMap()) {
      ArrayAnnotationMap arrayMap = asArrayMap();
      if (arrayMap.getElement() != null) {
        // if the element is empty after normalization, then replace the element to null
        if (arrayMap.getElement().normalizeInternal()) {
          arrayMap.setElement(null);
        } else {
          empty = false;
        }
      }
    }
    return empty;
  }

  /**
   * Returns whether the annotation map is empty or contains the given {@code annotationSpecId}.
   *
   * <ul/>
   *   <li/>If the {@code annotationSpecId} is not present, the method checks whether the map and
   *       all nested maps are empty.
   *   <li/>If {@code annotationSpecId} is provided, then return {@code false} if the id is found in
   *       the map or nested map, or {@code true} if it's not found anywhere.
   * </ul>
   */
  private boolean isEmptyInternal(Optional<Integer> annotationSpecId) {
    if (annotationSpecId.isPresent()) {
      if (getAnnotation(annotationSpecId.get()) != null) {
        return false;
      }
    } else if (!annotations.isEmpty()) {
      return false;
    }
    if (isStructMap()) {
      for (int i = 0; i < asStructMap().getFieldCount(); i++) {
        if (asStructMap().getField(i) != null
            && !asStructMap().getField(i).isEmptyInternal(annotationSpecId)) {
          return false;
        }
      }
    } else if (isArrayMap()) {
      if (asArrayMap().getElement() != null
          && !asArrayMap().getElement().isEmptyInternal(annotationSpecId)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns whether the annotation map is normalized.
   *
   * <p>A normalized annotation map means:
   *
   * <ul/>
   *   <li/>All children annotation maps are null or normalized, AND
   *   <li/>If all children annotation maps are empty, then the current annotations must be non
   *       empty.
   * </ul>
   *
   * Note that the top-level annotation map can have empty annotations no matter whether any of
   * child annotation map is empty. But all nested annotation maps must have non-empty annotations
   * if all children annotation maps are empty.
   *
   * <ul/>
   *   <li/>If {@code checkEmpty} is false, returns true if all nested annotation maps are
   *       normalized. It doesn't check if the current annotations are empty are not, which is used
   *       as the top-level annotation map check.
   *   <li/>If {@code checkEmpty} is true, returns true if all nested annotation map are normalized,
   *       AND there is non-empty children annataion maps, or the current annotation is empty as
   *       well if all children annotation map are empty. This is to check the nested children
   *       annotation maps.
   * </ul>
   */
  private boolean isNormailizedInternal(boolean checkEmpty) {
    boolean hasNonEmptyChild = false;
    if (isStructMap()) {
      for (int i = 0; i < asStructMap().getFieldCount(); i++) {
        AnnotationMap field = asStructMap().getField(i);
        if (field != null && !field.isNormailizedInternal(/* checkEmpty= */ true)) {
          return false;
        }
        hasNonEmptyChild |= field != null;
      }
    } else if (isArrayMap()) {
      AnnotationMap element = asArrayMap().getElement();
      if (element != null && !element.isNormailizedInternal(/* checkEmpty= */ true)) {
        return false;
      }
      hasNonEmptyChild = element != null;
    }
    if (!checkEmpty) {
      return true;
    }
    // if any of child annotation map is not empty, we don't need to check the current annotation
    // map, otherwise we have to make sure the current annotation isn't empty.
    return hasNonEmptyChild || !annotations.isEmpty();
  }

  /**
   * Returns whether two annotation maps are equals with the given {@code annotationSpecId}.
   *
   * <p>When {@code annotationSpecId} is present, we only check annotations on the given {@code
   * annotationSpecId}, and ignore other annotation specs.
   *
   * <p>When {@code annotationSpecId} doesn't have a value, we check all annotations that each
   * annotation map has.
   */
  private static boolean equalsInternal(
      AnnotationMap lhs, AnnotationMap rhs, Optional<Integer> annotationSpecId) {
    if (lhs == null) {
      return rhs == null || rhs.isEmptyInternal(annotationSpecId);
    }
    if (rhs == null) {
      return lhs.isEmptyInternal(annotationSpecId);
    }
    // lhs and rhs are guaranteed to be non-null.
    if (annotationSpecId.isPresent()) {
      if (!simpleValueEquals(
          lhs.getAnnotation(annotationSpecId.get()), rhs.getAnnotation(annotationSpecId.get()))) {
        return false;
      }
    } else if (!lhs.annotations.equals(rhs.annotations)) {
      return false;
    }
    if (lhs.isStructMap()) {
      if (!rhs.isStructMap()
          || lhs.asStructMap().getFieldCount() != rhs.asStructMap().getFieldCount()) {
        return false;
      }
      for (int i = 0; i < lhs.asStructMap().getFieldCount(); i++) {
        if (!equalsInternal(
            lhs.asStructMap().getField(i), rhs.asStructMap().getField(i), annotationSpecId)) {
          return false;
        }
      }
      return true;
    } else if (lhs.isArrayMap()) {
      return rhs.isArrayMap()
          && equalsInternal(
              lhs.asArrayMap().getElement(), rhs.asArrayMap().getElement(), annotationSpecId);
    }
    return !rhs.isStructMap() && !rhs.isArrayMap();
  }

  private static boolean simpleValueEquals(SimpleValue lhs, SimpleValue rhs) {
    return (lhs == null && rhs == null) || (lhs != null && rhs != null && lhs.equals(rhs));
  }
}
