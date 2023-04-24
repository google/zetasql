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

import static com.google.common.truth.Truth.assertThat;
import static com.google.zetasql.AnnotationMapTestConstants.NESTED_ARRAY_TYPE;
import static com.google.zetasql.AnnotationMapTestConstants.SIMPLE_ARRAY_TYPE;
import static com.google.zetasql.AnnotationMapTestConstants.STRING_TYPE;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ArrayAnnotationMapTest {

  @Test
  public void create_emptyArrayAnnotationMap_hasNullElement() {
    ArrayAnnotationMap annotationMap = ArrayAnnotationMap.create();

    assertThat(annotationMap.isArrayMap()).isTrue();
    assertThat(annotationMap.getElement()).isNull();
  }

  @Test
  public void isArrayMap_simpleArrayAnnotationMap_returnsTrue() {
    AnnotationMap annotationMap = AnnotationMap.create(SIMPLE_ARRAY_TYPE);

    assertThat(annotationMap.isArrayMap()).isTrue();
  }

  @Test
  public void isArrayMap_nestedArrayAnnotationMap() {
    // AnnotationMap for ARRAY<STRUCT<a STRING, b ARRAY<INT64>>>
    AnnotationMap annotationMap = AnnotationMap.create(NESTED_ARRAY_TYPE);
    assertThat(annotationMap.isArrayMap()).isTrue();

    // AnnotationMap for STRUCT<a STRING, b ARRAY<INT64>>
    AnnotationMap elementAnnotationMap = annotationMap.asArrayMap().getElement();
    assertThat(elementAnnotationMap).isNotNull();
    assertThat(elementAnnotationMap.isArrayMap()).isFalse();

    // AnnotationMap for STRING
    AnnotationMap structField0AnnotationMap = elementAnnotationMap.asStructMap().getField(0);
    // AnnotationMap for ARRAY<INT64>
    AnnotationMap structField1AnnotationMap = elementAnnotationMap.asStructMap().getField(1);
    assertThat(structField0AnnotationMap).isNotNull();
    assertThat(structField0AnnotationMap.isArrayMap()).isFalse();
    assertThat(structField1AnnotationMap).isNotNull();
    assertThat(structField1AnnotationMap.isArrayMap()).isTrue();
  }

  @Test
  public void asArrayMap_simpleArrayAnnotationMap() {
    AnnotationMap annotationMap = AnnotationMap.create(SIMPLE_ARRAY_TYPE);

    assertThat(annotationMap.isArrayMap()).isTrue();
    ArrayAnnotationMap arrayAnnotationMap = annotationMap.asArrayMap();

    assertThat(arrayAnnotationMap).isNotNull();
  }

  @Test
  public void asArrayMap_nestedArrayAnnotationMap() {
    // AnnotationMap for ARRAY<STRUCT<a STRING, b ARRAY<INT64>>>
    AnnotationMap annotationMap = AnnotationMap.create(NESTED_ARRAY_TYPE);
    assertThat(annotationMap.asArrayMap()).isNotNull();

    // AnnotationMap for STRUCT<a STRING, b ARRAY<INT64>>
    AnnotationMap elementAnnotationMap = annotationMap.asArrayMap().getElement();
    assertThat(elementAnnotationMap).isNotNull();
    assertThat(elementAnnotationMap.asArrayMap()).isNull();
    assertThat(elementAnnotationMap.asStructMap()).isNotNull();

    // AnnotationMap for STRING
    AnnotationMap structField0AnnotationMap = elementAnnotationMap.asStructMap().getField(0);
    // AnnotationMap for ARRAY<INT64>
    AnnotationMap structField1AnnotationMap = elementAnnotationMap.asStructMap().getField(1);

    assertThat(structField0AnnotationMap.asArrayMap()).isNull();
    assertThat(structField1AnnotationMap.asArrayMap()).isNotNull();
  }

  @Test
  public void setElement_nullElement_setSuccessfully() {
    ArrayAnnotationMap annotationMap = ArrayAnnotationMap.create();

    annotationMap.setElement(null);

    assertThat(annotationMap.getElement()).isNull();
  }

  @Test
  public void setElement_nonNullElement_setSuccessfully() {
    ArrayAnnotationMap annotationMap = ArrayAnnotationMap.create();
    AnnotationMap elementAnnotationMap = AnnotationMap.create(STRING_TYPE);

    annotationMap.setElement(elementAnnotationMap);

    assertThat(annotationMap.getElement()).isEqualTo(elementAnnotationMap);
  }
}
