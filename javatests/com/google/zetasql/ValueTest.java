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

import com.google.common.testing.EqualsTester;
import com.google.common.testing.SerializableTester;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import com.google.protobuf.Timestamp;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.ZetaSQLType.TypeProto;
import com.google.zetasql.ZetaSQLValue.ValueProto;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)

public class ValueTest {

  @Test
  public void testInt32Value() {
    Value zero = Value.createInt32Value(0);
    Value one = Value.createInt32Value(1);
    Value fortyTwo = Value.createInt32Value(42);
    Value anotherFortyTwo = Value.createInt32Value(42);
    Value minusOne = Value.createInt32Value(-1);

    assertThat(zero.isNull()).isFalse();
    assertThat(one.isNull()).isFalse();
    assertThat(fortyTwo.isNull()).isFalse();
    assertThat(anotherFortyTwo.isNull()).isFalse();
    assertThat(minusOne.isNull()).isFalse();

    assertThat(zero.isValid()).isTrue();
    assertThat(one.isValid()).isTrue();
    assertThat(fortyTwo.isValid()).isTrue();
    assertThat(anotherFortyTwo.isValid()).isTrue();
    assertThat(minusOne.isValid()).isTrue();

    assertThat(zero.getInt32Value()).isEqualTo(0);
    assertThat(one.getInt32Value()).isEqualTo(1);
    assertThat(fortyTwo.getInt32Value()).isEqualTo(42);
    assertThat(anotherFortyTwo.getInt32Value()).isEqualTo(42);
    assertThat(minusOne.getInt32Value()).isEqualTo(-1);

    assertThat(fortyTwo).isEqualTo(anotherFortyTwo);
    assertThat(anotherFortyTwo).isEqualTo(fortyTwo);

    assertThat(anotherFortyTwo.hashCode()).isEqualTo(fortyTwo.hashCode());

    assertThat(zero.equals(one)).isFalse();
    assertThat(zero.equals(null)).isFalse();
    assertThat(one.equals(minusOne)).isFalse();

    checkSerializeAndDeserialize(fortyTwo, anotherFortyTwo);
    checkSerializeAndDeserialize(zero);
    checkSerializeAndDeserialize(one);
    checkSerializeAndDeserialize(fortyTwo);
    checkSerializeAndDeserialize(anotherFortyTwo);
    checkSerializeAndDeserialize(minusOne);

    assertThat(zero.toInt64()).isEqualTo(0L);
    assertThat(one.toInt64()).isEqualTo(1L);
    assertThat(fortyTwo.toInt64()).isEqualTo(42L);
    assertThat(anotherFortyTwo.toInt64()).isEqualTo(42L);
    assertThat(minusOne.toInt64()).isEqualTo(-1L);

    try {
      zero.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_INT32 to uint64");
    }
    try {
      one.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_INT32 to uint64");
    }
    try {
      fortyTwo.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_INT32 to uint64");
    }
    try {
      minusOne.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_INT32 to uint64");
    }

    assertThat(zero.toDouble()).isEqualTo(0.0);
    assertThat(one.toDouble()).isEqualTo(1.0);
    assertThat(fortyTwo.toDouble()).isEqualTo(42.0);
    assertThat(anotherFortyTwo.toDouble()).isEqualTo(42.0);
    assertThat(minusOne.toDouble()).isEqualTo(-1.0);
  }

  @Test
  public void testUint32Value() {
    Value zero = Value.createUint32Value(0);
    Value one = Value.createUint32Value(1);
    Value fortyTwo = Value.createUint32Value(42);
    Value anotherFortyTwo = Value.createUint32Value(42);

    assertThat(zero.isNull()).isFalse();
    assertThat(one.isNull()).isFalse();
    assertThat(fortyTwo.isNull()).isFalse();
    assertThat(anotherFortyTwo.isNull()).isFalse();

    assertThat(zero.isValid()).isTrue();
    assertThat(one.isValid()).isTrue();
    assertThat(fortyTwo.isValid()).isTrue();
    assertThat(anotherFortyTwo.isValid()).isTrue();

    assertThat(zero.getUint32Value()).isEqualTo(0);
    assertThat(one.getUint32Value()).isEqualTo(1);
    assertThat(fortyTwo.getUint32Value()).isEqualTo(42);
    assertThat(anotherFortyTwo.getUint32Value()).isEqualTo(42);

    assertThat(fortyTwo).isEqualTo(anotherFortyTwo);
    assertThat(anotherFortyTwo).isEqualTo(fortyTwo);

    assertThat(anotherFortyTwo.hashCode()).isEqualTo(fortyTwo.hashCode());

    assertThat(zero.equals(one)).isFalse();

    checkSerializeAndDeserialize(fortyTwo, anotherFortyTwo);
    checkSerializeAndDeserialize(zero);
    checkSerializeAndDeserialize(one);
    checkSerializeAndDeserialize(fortyTwo);
    checkSerializeAndDeserialize(anotherFortyTwo);

    assertThat(zero.toInt64()).isEqualTo(0L);
    assertThat(one.toInt64()).isEqualTo(1L);
    assertThat(fortyTwo.toInt64()).isEqualTo(42L);
    assertThat(anotherFortyTwo.toInt64()).isEqualTo(42L);

    assertThat(zero.toUint64()).isEqualTo(0L);
    assertThat(one.toUint64()).isEqualTo(1L);
    assertThat(fortyTwo.toUint64()).isEqualTo(42L);
    assertThat(anotherFortyTwo.toUint64()).isEqualTo(42L);

    assertThat(zero.toDouble()).isEqualTo(0.0);
    assertThat(one.toDouble()).isEqualTo(1.0);
    assertThat(fortyTwo.toDouble()).isEqualTo(42.0);
    assertThat(anotherFortyTwo.toDouble()).isEqualTo(42.0);
  }

  @Test
  public void testInt64Value() {
    Value zero = Value.createInt64Value(0);
    Value one = Value.createInt64Value(1);
    Value fortyTwo = Value.createInt64Value(42);
    Value anotherFortyTwo = Value.createInt64Value(42);
    Value minusOne = Value.createInt64Value(-1);

    assertThat(zero.isNull()).isFalse();
    assertThat(one.isNull()).isFalse();
    assertThat(fortyTwo.isNull()).isFalse();
    assertThat(anotherFortyTwo.isNull()).isFalse();
    assertThat(minusOne.isNull()).isFalse();

    assertThat(zero.isValid()).isTrue();
    assertThat(one.isValid()).isTrue();
    assertThat(fortyTwo.isValid()).isTrue();
    assertThat(anotherFortyTwo.isValid()).isTrue();
    assertThat(minusOne.isValid()).isTrue();

    assertThat(zero.getInt64Value()).isEqualTo(0);
    assertThat(one.getInt64Value()).isEqualTo(1);
    assertThat(fortyTwo.getInt64Value()).isEqualTo(42);
    assertThat(anotherFortyTwo.getInt64Value()).isEqualTo(42);
    assertThat(minusOne.getInt64Value()).isEqualTo(-1);

    assertThat(fortyTwo).isEqualTo(anotherFortyTwo);
    assertThat(anotherFortyTwo).isEqualTo(fortyTwo);

    assertThat(anotherFortyTwo.hashCode()).isEqualTo(fortyTwo.hashCode());

    assertThat(zero.equals(one)).isFalse();
    assertThat(one.equals(minusOne)).isFalse();

    checkSerializeAndDeserialize(fortyTwo, anotherFortyTwo);
    checkSerializeAndDeserialize(zero);
    checkSerializeAndDeserialize(one);
    checkSerializeAndDeserialize(fortyTwo);
    checkSerializeAndDeserialize(anotherFortyTwo);
    checkSerializeAndDeserialize(minusOne);

    assertThat(zero.toInt64()).isEqualTo(0L);
    assertThat(one.toInt64()).isEqualTo(1L);
    assertThat(fortyTwo.toInt64()).isEqualTo(42L);
    assertThat(anotherFortyTwo.toInt64()).isEqualTo(42L);
    assertThat(minusOne.toInt64()).isEqualTo(-1L);

    try {
      zero.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_INT64 to uint64");
    }
    try {
      one.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_INT64 to uint64");
    }
    try {
      fortyTwo.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_INT64 to uint64");
    }
    try {
      minusOne.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_INT64 to uint64");
    }

    assertThat(zero.toDouble()).isEqualTo(0.0);
    assertThat(one.toDouble()).isEqualTo(1.0);
    assertThat(fortyTwo.toDouble()).isEqualTo(42.0);
    assertThat(anotherFortyTwo.toDouble()).isEqualTo(42.0);
    assertThat(minusOne.toDouble()).isEqualTo(-1.0);
  }

  @Test
  public void testCreateAndGetUint64Value() {
    Value zero = Value.createUint64Value(0);
    Value one = Value.createUint64Value(1);
    Value fortyTwo = Value.createUint64Value(42);
    Value anotherFortyTwo = Value.createUint64Value(42);

    assertThat(zero.isNull()).isFalse();
    assertThat(one.isNull()).isFalse();
    assertThat(fortyTwo.isNull()).isFalse();
    assertThat(anotherFortyTwo.isNull()).isFalse();

    assertThat(zero.isValid()).isTrue();
    assertThat(one.isValid()).isTrue();
    assertThat(fortyTwo.isValid()).isTrue();
    assertThat(anotherFortyTwo.isValid()).isTrue();

    assertThat(zero.getUint64Value()).isEqualTo(0);
    assertThat(one.getUint64Value()).isEqualTo(1);
    assertThat(fortyTwo.getUint64Value()).isEqualTo(42);
    assertThat(anotherFortyTwo.getUint64Value()).isEqualTo(42);

    assertThat(fortyTwo).isEqualTo(anotherFortyTwo);
    assertThat(anotherFortyTwo).isEqualTo(fortyTwo);

    assertThat(anotherFortyTwo.hashCode()).isEqualTo(fortyTwo.hashCode());

    assertThat(zero.equals(one)).isFalse();

    checkSerializeAndDeserialize(fortyTwo, anotherFortyTwo);
    checkSerializeAndDeserialize(zero);
    checkSerializeAndDeserialize(one);
    checkSerializeAndDeserialize(fortyTwo);
    checkSerializeAndDeserialize(anotherFortyTwo);

    assertThat(zero.toUint64()).isEqualTo(0L);
    assertThat(one.toUint64()).isEqualTo(1L);
    assertThat(fortyTwo.toUint64()).isEqualTo(42L);
    assertThat(anotherFortyTwo.toUint64()).isEqualTo(42L);

    try {
      zero.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_UINT64 to int64");
    }
    try {
      one.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_UINT64 to int64");
    }
    try {
      fortyTwo.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_UINT64 to int64");
    }

    assertThat(zero.toDouble()).isEqualTo(0.0);
    assertThat(one.toDouble()).isEqualTo(1.0);
    assertThat(fortyTwo.toDouble()).isEqualTo(42.0);
    assertThat(anotherFortyTwo.toDouble()).isEqualTo(42.0);
  }

  @Test
  public void testBoolValue() {
    Value yes = Value.createBoolValue(true);
    Value no = Value.createBoolValue(false);
    Value negative = Value.createBoolValue(false);
    Value affirmative = Value.createBoolValue(true);

    assertThat(yes.isNull()).isFalse();
    assertThat(no.isNull()).isFalse();
    assertThat(negative.isNull()).isFalse();
    assertThat(affirmative.isNull()).isFalse();

    assertThat(yes.isValid()).isTrue();
    assertThat(no.isValid()).isTrue();
    assertThat(negative.isValid()).isTrue();
    assertThat(affirmative.isValid()).isTrue();

    assertThat(affirmative.hashCode()).isEqualTo(yes.hashCode());
    assertThat(negative.hashCode()).isEqualTo(no.hashCode());

    assertThat(yes).isEqualTo(affirmative);
    assertThat(yes.equals(no)).isFalse();
    assertThat(yes.equals(negative)).isFalse();
    assertThat(no).isEqualTo(negative);
    assertThat(no.equals(yes)).isFalse();
    assertThat(no.equals(affirmative)).isFalse();
    assertThat(affirmative).isEqualTo(yes);
    assertThat(affirmative.equals(no)).isFalse();
    assertThat(affirmative.equals(negative)).isFalse();
    assertThat(negative).isEqualTo(no);
    assertThat(negative.equals(yes)).isFalse();
    assertThat(negative.equals(affirmative)).isFalse();

    checkSerializeAndDeserialize(yes, affirmative);
    checkSerializeAndDeserialize(negative, no);
    checkSerializeAndDeserialize(yes);
    checkSerializeAndDeserialize(no);
    checkSerializeAndDeserialize(negative);
    checkSerializeAndDeserialize(affirmative);

    assertThat(yes.toInt64()).isEqualTo(1);
    assertThat(no.toInt64()).isEqualTo(0);
    assertThat(negative.toInt64()).isEqualTo(0);
    assertThat(affirmative.toInt64()).isEqualTo(1);

    assertThat(yes.toUint64()).isEqualTo(1);
    assertThat(no.toUint64()).isEqualTo(0);
    assertThat(negative.toUint64()).isEqualTo(0);
    assertThat(affirmative.toUint64()).isEqualTo(1);

    assertThat(yes.toDouble()).isEqualTo(1.0);
    assertThat(no.toDouble()).isEqualTo(0.0);
    assertThat(negative.toDouble()).isEqualTo(0.0);
    assertThat(affirmative.toDouble()).isEqualTo(1.0);
  }

  @Test
  public void testFloatValue() {
    Value zero = Value.createFloatValue(0);
    Value one = Value.createFloatValue(1);
    Value onePointTwo = Value.createFloatValue(1.2f);
    Value minusTwoPointOne = Value.createFloatValue(-2.1f);
    Value anotherOnePointTwo = Value.createFloatValue(1.2f);
    Value nan = Value.createFloatValue(Float.NaN);
    Value inf = Value.createFloatValue(Float.POSITIVE_INFINITY);
    Value negativeInf = Value.createFloatValue(Float.NEGATIVE_INFINITY);

    assertThat(zero.isNull()).isFalse();
    assertThat(one.isNull()).isFalse();
    assertThat(onePointTwo.isNull()).isFalse();
    assertThat(minusTwoPointOne.isNull()).isFalse();
    assertThat(anotherOnePointTwo.isNull()).isFalse();
    assertThat(nan.isNull()).isFalse();

    assertThat(zero.isValid()).isTrue();
    assertThat(one.isValid()).isTrue();
    assertThat(onePointTwo.isValid()).isTrue();
    assertThat(minusTwoPointOne.isValid()).isTrue();
    assertThat(anotherOnePointTwo.isValid()).isTrue();
    assertThat(nan.isValid()).isTrue();

    assertThat(0.0 == zero.getFloatValue()).isTrue();
    assertThat(1 == one.getFloatValue()).isTrue();
    assertThat(1.2f == onePointTwo.getFloatValue()).isTrue();
    assertThat(-2.1f == minusTwoPointOne.getFloatValue()).isTrue();
    assertThat(1.2f == anotherOnePointTwo.getFloatValue()).isTrue();
    assertThat(Float.isNaN(nan.getFloatValue())).isTrue();
    assertThat(Float.POSITIVE_INFINITY == inf.getFloatValue()).isTrue();
    assertThat(Float.NEGATIVE_INFINITY == negativeInf.getFloatValue()).isTrue();

    assertThat(anotherOnePointTwo.hashCode()).isEqualTo(onePointTwo.hashCode());

    assertThat(onePointTwo).isEqualTo(anotherOnePointTwo);
    assertThat(zero.equals(one)).isFalse();
    assertThat(nan.equals(inf)).isFalse();
    assertThat(inf.equals(negativeInf)).isFalse();
    assertThat(negativeInf.equals(nan)).isFalse();

    checkSerializeAndDeserialize(onePointTwo, anotherOnePointTwo);
    checkSerializeAndDeserialize(zero);
    checkSerializeAndDeserialize(one);
    checkSerializeAndDeserialize(onePointTwo);
    checkSerializeAndDeserialize(minusTwoPointOne);
    checkSerializeAndDeserialize(anotherOnePointTwo);
    checkSerializeAndDeserialize(nan);
    checkSerializeAndDeserialize(inf);
    checkSerializeAndDeserialize(negativeInf);

    try {
      zero.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to int64");
    }
    try {
      one.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to int64");
    }
    try {
      onePointTwo.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to int64");
    }
    try {
      minusTwoPointOne.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to int64");
    }
    try {
      nan.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to int64");
    }
    try {
      inf.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to int64");
    }
    try {
      negativeInf.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to int64");
    }

    try {
      zero.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to uint64");
    }
    try {
      one.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to uint64");
    }
    try {
      onePointTwo.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to uint64");
    }
    try {
      minusTwoPointOne.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to uint64");
    }
    try {
      nan.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to uint64");
    }
    try {
      inf.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to uint64");
    }
    try {
      negativeInf.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_FLOAT to uint64");
    }

    assertThat(0.0 == zero.toDouble()).isTrue();
    assertThat(1.0 == one.toDouble()).isTrue();
    assertThat(onePointTwo.toDouble()).isEqualTo(Double.valueOf(1.2f));
    assertThat((double) -2.1f == minusTwoPointOne.toDouble()).isTrue();
    assertThat((double) 1.2f == anotherOnePointTwo.toDouble()).isTrue();
    assertThat(Double.isNaN(nan.toDouble())).isTrue();
    assertThat(Double.POSITIVE_INFINITY == inf.toDouble()).isTrue();
    assertThat(Double.NEGATIVE_INFINITY == negativeInf.toDouble()).isTrue();
  }

  @Test
  public void testDoubleValue() {
    Value zero = Value.createDoubleValue(0);
    Value one = Value.createDoubleValue(1);
    Value onePointTwo = Value.createDoubleValue(1.2);
    Value minusTwoPointOne = Value.createDoubleValue(-2.1);
    Value anotherOnePointTwo = Value.createDoubleValue(1.2);
    Value nan = Value.createDoubleValue(Double.NaN);
    Value inf = Value.createDoubleValue(Double.POSITIVE_INFINITY);
    Value negativeInf = Value.createDoubleValue(Double.NEGATIVE_INFINITY);

    assertThat(zero.isNull()).isFalse();
    assertThat(one.isNull()).isFalse();
    assertThat(onePointTwo.isNull()).isFalse();
    assertThat(minusTwoPointOne.isNull()).isFalse();
    assertThat(anotherOnePointTwo.isNull()).isFalse();
    assertThat(Double.isNaN(nan.getDoubleValue())).isTrue();
    assertThat(Double.POSITIVE_INFINITY == inf.getDoubleValue()).isTrue();
    assertThat(Double.NEGATIVE_INFINITY == negativeInf.getDoubleValue()).isTrue();

    assertThat(zero.isValid()).isTrue();
    assertThat(one.isValid()).isTrue();
    assertThat(onePointTwo.isValid()).isTrue();
    assertThat(minusTwoPointOne.isValid()).isTrue();
    assertThat(anotherOnePointTwo.isValid()).isTrue();
    assertThat(Double.isNaN(nan.getDoubleValue())).isTrue();
    assertThat(Double.POSITIVE_INFINITY == inf.getDoubleValue()).isTrue();
    assertThat(Double.NEGATIVE_INFINITY == negativeInf.getDoubleValue()).isTrue();

    assertThat(0 == zero.getDoubleValue()).isTrue();
    assertThat(1 == one.getDoubleValue()).isTrue();
    assertThat(1.2 == onePointTwo.getDoubleValue()).isTrue();
    assertThat(-2.1 == minusTwoPointOne.getDoubleValue()).isTrue();
    assertThat(1.2 == anotherOnePointTwo.getDoubleValue()).isTrue();

    assertThat(anotherOnePointTwo.hashCode()).isEqualTo(onePointTwo.hashCode());

    assertThat(onePointTwo).isEqualTo(anotherOnePointTwo);
    assertThat(zero.equals(one)).isFalse();
    assertThat(nan.equals(inf)).isFalse();
    assertThat(inf.equals(negativeInf)).isFalse();
    assertThat(negativeInf.equals(nan)).isFalse();

    checkSerializeAndDeserialize(onePointTwo, anotherOnePointTwo);
    checkSerializeAndDeserialize(zero);
    checkSerializeAndDeserialize(one);
    checkSerializeAndDeserialize(onePointTwo);
    checkSerializeAndDeserialize(minusTwoPointOne);
    checkSerializeAndDeserialize(anotherOnePointTwo);
    checkSerializeAndDeserialize(nan);
    checkSerializeAndDeserialize(inf);
    checkSerializeAndDeserialize(negativeInf);

    try {
      zero.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to int64");
    }
    try {
      one.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to int64");
    }
    try {
      onePointTwo.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to int64");
    }
    try {
      minusTwoPointOne.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to int64");
    }
    try {
      nan.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to int64");
    }
    try {
      inf.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to int64");
    }
    try {
      negativeInf.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to int64");
    }

    try {
      zero.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to uint64");
    }
    try {
      one.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to uint64");
    }
    try {
      onePointTwo.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to uint64");
    }
    try {
      minusTwoPointOne.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to uint64");
    }
    try {
      nan.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to uint64");
    }
    try {
      inf.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to uint64");
    }
    try {
      negativeInf.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DOUBLE to uint64");
    }

    assertThat(0.0 == zero.toDouble()).isTrue();
    assertThat(1.0 == one.toDouble()).isTrue();
    assertThat(1.2 == onePointTwo.toDouble()).isTrue();
    assertThat(-2.1 == minusTwoPointOne.toDouble()).isTrue();
    assertThat(1.2 == anotherOnePointTwo.toDouble()).isTrue();
    assertThat(Double.isNaN(nan.toDouble())).isTrue();
    assertThat(Double.POSITIVE_INFINITY == inf.toDouble()).isTrue();
    assertThat(Double.NEGATIVE_INFINITY == negativeInf.toDouble()).isTrue();
  }

  @Test
  public void testNumericValue() {
    Value zero = Value.createNumericValue(BigDecimal.ZERO);
    Value onePointTwo = Value.createNumericValue(new BigDecimal("1.2"));
    Value anotherOnePointTwo = Value.createNumericValue(new BigDecimal("1.2"));
    Value minusOnePointTwo = Value.createNumericValue(new BigDecimal("-1.2"));
    Value maxNumeric = Value.createNumericValue(
        new BigDecimal("99999999999999999999999999999.999999999"));
    Value minNumeric = Value.createNumericValue(
        new BigDecimal("-99999999999999999999999999999.999999999"));

    assertThat(zero.getType().getKind()).isEqualTo(TypeKind.TYPE_NUMERIC);

    assertThat(zero.isNull()).isFalse();
    assertThat(onePointTwo.isNull()).isFalse();
    assertThat(minusOnePointTwo.isNull()).isFalse();
    assertThat(maxNumeric.isNull()).isFalse();
    assertThat(minNumeric.isNull()).isFalse();

    assertThat(zero.isValid()).isTrue();
    assertThat(onePointTwo.isValid()).isTrue();
    assertThat(minusOnePointTwo.isValid()).isTrue();
    assertThat(maxNumeric.isValid()).isTrue();
    assertThat(minNumeric.isValid()).isTrue();

    assertThat(zero.getNumericValue().signum()).isEqualTo(0);
    assertThat(onePointTwo.getNumericValue().compareTo(
        new BigDecimal("1.2"))).isEqualTo(0);
    assertThat(minusOnePointTwo.getNumericValue().compareTo(
        new BigDecimal("-1.2"))).isEqualTo(0);
    assertThat(maxNumeric.getNumericValue().compareTo(
        new BigDecimal("99999999999999999999999999999.999999999"))).isEqualTo(0);
    assertThat(minNumeric.getNumericValue().compareTo(
        new BigDecimal("-99999999999999999999999999999.999999999"))).isEqualTo(0);

    assertThat(anotherOnePointTwo.hashCode()).isEqualTo(onePointTwo.hashCode());

    assertThat(onePointTwo).isEqualTo(anotherOnePointTwo);
    assertThat(zero.equals(onePointTwo)).isFalse();

    checkSerializeAndDeserialize(onePointTwo, anotherOnePointTwo);
    checkSerializeAndDeserialize(zero);
    checkSerializeAndDeserialize(onePointTwo);
    checkSerializeAndDeserialize(minusOnePointTwo);
    checkSerializeAndDeserialize(maxNumeric);
    checkSerializeAndDeserialize(minNumeric);

    try {
      zero.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_NUMERIC to int64");
    }

    try {
      zero.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_NUMERIC to uint64");
    }

    assertThat(0.0 == zero.toDouble()).isTrue();
    assertThat(1.2 == onePointTwo.toDouble()).isTrue();
    assertThat(-1.2 == minusOnePointTwo.toDouble()).isTrue();

    BigDecimal tooLarge = maxNumeric.getNumericValue().add(BigDecimal.ONE);
    BigDecimal tooSmall = minNumeric.getNumericValue().subtract(BigDecimal.ONE);
    BigDecimal wrongScale = new BigDecimal("0.000000000001");

    try {
      Value.createNumericValue(tooLarge);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessageThat().isEqualTo(
          "Numeric overflow: 100000000000000000000000000000.999999999");
    }
    try {
      Value.createNumericValue(tooSmall);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessageThat().isEqualTo(
          "Numeric overflow: -100000000000000000000000000000.999999999");
    }
    try {
      Value.createNumericValue(wrongScale);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e).hasMessageThat().isEqualTo("Numeric scale cannot exceed 9: 0.000000000001");
    }
  }

  @Test
  public void testNumericValueDeserialize() {
    ValueProto zeroProto = valueProtoFromText("numeric_value: '\\000'");
    ValueProto onePointTwoProto = valueProtoFromText(
        "numeric_value: '\\000\\214\\206G'");
    ValueProto minusOnePointTwoProto = valueProtoFromText(
        "numeric_value: '\\000ty\\270'");
    ValueProto maxNumericProto = valueProtoFromText(
        "numeric_value: '\\377\\377\\377\\377?\"\\212\\tz\\304\\206Z\\250L;K'");
    ValueProto minNumericProto = valueProtoFromText(
        "numeric_value: '\\001\\000\\000\\000\\300\\335u\\366\\205;y\\245W\\263\\304\\264'");

    Value zero = Value.deserialize(
        TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC), zeroProto);
    assertThat(zero.getNumericValue().compareTo(BigDecimal.ZERO)).isEqualTo(0);

    Value onePointTwo = Value.deserialize(
        TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC), onePointTwoProto);
    assertThat(onePointTwo.getNumericValue().compareTo(new BigDecimal("1.2"))).isEqualTo(0);

    Value minusOnePointTwo = Value.deserialize(
        TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC), minusOnePointTwoProto);
    assertThat(minusOnePointTwo.getNumericValue().compareTo(new BigDecimal("-1.2"))).isEqualTo(0);

    Value maxNumeric = Value.deserialize(
        TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC), maxNumericProto);
    assertThat(maxNumeric.getNumericValue().compareTo(
        new BigDecimal("99999999999999999999999999999.999999999"))).isEqualTo(0);

    Value minNumeric = Value.deserialize(
        TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC), minNumericProto);
    assertThat(minNumeric.getNumericValue().compareTo(
        new BigDecimal("-99999999999999999999999999999.999999999"))).isEqualTo(0);
  }

  @Test
  public void testStringValue() {
    Value hello = Value.createStringValue("hello");
    Value world = Value.createStringValue("world");
    Value greeting = Value.createStringValue("hello");
    Value empty = Value.createStringValue("");
    Value invalidUtf8 =
        Value.deserialize(
            TypeFactory.createSimpleType(TypeKind.TYPE_STRING),
            ValueProto.newBuilder()
                .setStringValueBytes(ByteString.copyFrom(new byte[] {(byte) 0xc2}))
                .build());

    assertThat(hello.isNull()).isFalse();
    assertThat(world.isNull()).isFalse();
    assertThat(greeting.isNull()).isFalse();
    assertThat(empty.isNull()).isFalse();
    assertThat(invalidUtf8.isNull()).isFalse();

    assertThat(hello.isValid()).isTrue();
    assertThat(world.isValid()).isTrue();
    assertThat(greeting.isValid()).isTrue();
    assertThat(empty.isValid()).isTrue();
    assertThat(invalidUtf8.isValid()).isTrue();

    assertThat(hello.getStringValue()).isEqualTo("hello");
    assertThat(world.getStringValue()).isEqualTo("world");
    assertThat(greeting.getStringValue()).isEqualTo("hello");
    assertThat(empty.getStringValue()).isEqualTo("");
    assertThat(invalidUtf8.getStringValue()).isEqualTo("�");

    assertThat(hello.getStringValueBytes()).isEqualTo(ByteString.copyFromUtf8("hello"));
    assertThat(world.getStringValueBytes()).isEqualTo(ByteString.copyFromUtf8("world"));
    assertThat(greeting.getStringValueBytes()).isEqualTo(ByteString.copyFromUtf8("hello"));
    assertThat(empty.getStringValueBytes()).isEqualTo(ByteString.copyFromUtf8(""));
    assertThat(invalidUtf8.getStringValueBytes())
        .isEqualTo(ByteString.copyFrom(new byte[] {(byte) 0xc2}));

    assertThat(hello.getStringValueBytes().isValidUtf8()).isTrue();
    assertThat(world.getStringValueBytes().isValidUtf8()).isTrue();
    assertThat(greeting.getStringValueBytes().isValidUtf8()).isTrue();
    assertThat(empty.getStringValueBytes().isValidUtf8()).isTrue();
    assertThat(invalidUtf8.getStringValueBytes().isValidUtf8()).isFalse();

    assertThat(hello.hashCode()).isEqualTo(greeting.hashCode());
    assertThat(hello).isEqualTo(greeting);

    checkSerializeAndDeserialize(hello, greeting);
    checkSerializeAndDeserialize(hello);
    checkSerializeAndDeserialize(world);
    checkSerializeAndDeserialize(greeting);
    checkSerializeAndDeserialize(empty);
    checkSerializeAndDeserialize(invalidUtf8);

    try {
      hello.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_STRING to int64");
    }

    try {
      hello.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_STRING to uint64");
    }

    try {
      hello.toDouble();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_STRING to double");
    }
  }

  @Test
  public void testBytesValue() {
    Value hello = Value.createBytesValue(ByteString.copyFromUtf8("hello"));
    byte[] buffer = {1, 2, 3};
    Value bytes = Value.createBytesValue(ByteString.copyFrom(buffer));
    Value greeting = Value.createBytesValue(ByteString.copyFromUtf8("hello"));
    Value empty = Value.createBytesValue(ByteString.EMPTY);

    assertThat(hello.isNull()).isFalse();
    assertThat(bytes.isNull()).isFalse();
    assertThat(greeting.isNull()).isFalse();
    assertThat(empty.isNull()).isFalse();

    assertThat(hello.isValid()).isTrue();
    assertThat(bytes.isValid()).isTrue();
    assertThat(greeting.isValid()).isTrue();
    assertThat(empty.isValid()).isTrue();

    assertThat(hello.getBytesValue()).isEqualTo(ByteString.copyFromUtf8("hello"));
    assertThat(bytes.getBytesValue()).isEqualTo(ByteString.copyFrom(buffer));
    assertThat(greeting.getBytesValue()).isEqualTo(ByteString.copyFromUtf8("hello"));
    assertThat(empty.getBytesValue()).isEqualTo(ByteString.EMPTY);

    assertThat(hello.hashCode()).isEqualTo(greeting.hashCode());
    assertThat(hello).isEqualTo(greeting);

    checkSerializeAndDeserialize(hello, greeting);
    checkSerializeAndDeserialize(hello);
    checkSerializeAndDeserialize(bytes);
    checkSerializeAndDeserialize(greeting);
    checkSerializeAndDeserialize(empty);

    try {
      hello.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_BYTES to int64");
    }

    try {
      hello.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_BYTES to uint64");
    }

    try {
      hello.toDouble();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_BYTES to double");
    }
  }

  @Test
  public void testDateValue() {
    Value y1m1d1 = Value.createDateValue(-719162);
    Value y9999m12d31 = Value.createDateValue(2932896);
    Value zero = Value.createDateValue(0);
    Value o = Value.createDateValue(0);

    assertThat(y1m1d1.isNull()).isFalse();
    assertThat(y9999m12d31.isNull()).isFalse();
    assertThat(zero.isNull()).isFalse();
    assertThat(o.isNull()).isFalse();

    assertThat(y1m1d1.isValid()).isTrue();
    assertThat(y9999m12d31.isValid()).isTrue();
    assertThat(zero.isValid()).isTrue();
    assertThat(o.isValid()).isTrue();

    assertThat(y1m1d1.getDateValue()).isEqualTo(-719162);
    assertThat(y9999m12d31.getDateValue()).isEqualTo(2932896);
    assertThat(zero.getDateValue()).isEqualTo(0);
    assertThat(o.getDateValue()).isEqualTo(0);

    assertThat(o.hashCode()).isEqualTo(zero.hashCode());
    assertThat(zero).isEqualTo(o);

    try {
      Value.createDateValue(-719163);
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.createDateValue(2932897);
      fail();
    } catch (IllegalArgumentException expected) {
    }

    checkSerializeAndDeserialize(zero, o);
    checkSerializeAndDeserialize(y1m1d1);
    checkSerializeAndDeserialize(y9999m12d31);
    checkSerializeAndDeserialize(zero);
    checkSerializeAndDeserialize(o);

    assertThat(zero.toInt64()).isEqualTo(0L);
    assertThat(y1m1d1.toInt64()).isEqualTo(-719162L);
    assertThat(y9999m12d31.toInt64()).isEqualTo(2932896L);

    try {
      zero.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DATE to uint64");
    }
    try {
      y1m1d1.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DATE to uint64");
    }
    try {
      y9999m12d31.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DATE to uint64");
    }

    assertThat(zero.toDouble()).isEqualTo(0.0);
    assertThat(y1m1d1.toDouble()).isEqualTo(-719162.0);
    assertThat(y9999m12d31.toDouble()).isEqualTo(2932896.0);
  }

  @Test
  public void testTimestampValue() {
    Value y1m1d1 = Value.createTimestampValueFromUnixMicros(-62135596800L * 1000000);
    Value y9999m12d31 = Value.createTimestampValueFromUnixMicros(253402300800L * 1000000 - 1);
    Value zero = Value.createTimestampValueFromUnixMicros(0);
    Value o = Value.createTimestampValueFromUnixMicros(0);

    assertThat(y1m1d1.isNull()).isFalse();
    assertThat(y9999m12d31.isNull()).isFalse();
    assertThat(zero.isNull()).isFalse();
    assertThat(o.isNull()).isFalse();

    assertThat(y1m1d1.isValid()).isTrue();
    assertThat(y9999m12d31.isValid()).isTrue();
    assertThat(zero.isValid()).isTrue();
    assertThat(o.isValid()).isTrue();

    assertThat(y1m1d1.getTimestampUnixMicros()).isEqualTo(-62135596800000000L);
    assertThat(y9999m12d31.getTimestampUnixMicros()).isEqualTo(253402300799999999L);
    assertThat(zero.getTimestampUnixMicros()).isEqualTo(0);
    assertThat(o.getTimestampUnixMicros()).isEqualTo(0);

    assertThat(o.hashCode()).isEqualTo(zero.hashCode());
    assertThat(zero).isEqualTo(o);

    try {
      Value.createTimestampValueFromUnixMicros(-62135596800000001L);
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.createTimestampValueFromUnixMicros(253402300800000000L);
      fail();
    } catch (IllegalArgumentException expected) {
    }

    checkSerializeAndDeserialize(zero, o);
    checkSerializeAndDeserialize(y1m1d1);
    checkSerializeAndDeserialize(y9999m12d31);
    checkSerializeAndDeserialize(zero);
    checkSerializeAndDeserialize(o);

    assertThat(zero.getTimestampUnixMicros()).isEqualTo(0L);
    assertThat(y1m1d1.getTimestampUnixMicros()).isEqualTo(-62135596800000000L);
    assertThat(y9999m12d31.getTimestampUnixMicros()).isEqualTo(253402300799999999L);

    try {
      zero.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_TIMESTAMP to int64");
    }
    try {
      zero.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_TIMESTAMP to uint64");
    }
    try {
      zero.toDouble();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_TIMESTAMP to double");
    }

    // Check deserialization of nanos.
    ValueProto nanoTimestampMax =
        ValueProto.newBuilder()
            .setTimestampValue(
                Timestamp.newBuilder().setSeconds(253402300800L - 1).setNanos(999999999))
            .build();
    try {
      Value timestampMax =
          Value.deserialize(
              TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP), nanoTimestampMax);
      checkSerializeAndDeserialize(timestampMax);
    } catch (IllegalArgumentException e) {
      fail(e.toString());
    }
  }

  @Test
  public void testTimeValue() {
    Value midnight = Value.createTimeValue(0); // 00:00:00
    Value beforeMidnightMicros = Value.createTimeValue(0x5FBEFB9AC618L); // 23:59:59.999999
    Value beforeMidnightNanos = Value.createTimeValue(0x5FBEFB9AC9FFL); // 23:59:59.999999999
    Value someTimeMicros = Value.createTimeValue(0x322E27002568L); // 12:34:56.654321
    Value someTimeNanos = Value.createTimeValue(0x21083ADE68B1L); // 08:16:32.987654321
    Value zero = Value.createTimeValue(0); // 00:00:00

    assertThat(midnight.isNull()).isFalse();
    assertThat(beforeMidnightMicros.isNull()).isFalse();
    assertThat(beforeMidnightNanos.isNull()).isFalse();
    assertThat(someTimeMicros.isNull()).isFalse();
    assertThat(someTimeNanos.isNull()).isFalse();

    assertThat(midnight.isValid()).isTrue();
    assertThat(beforeMidnightMicros.isValid()).isTrue();
    assertThat(beforeMidnightNanos.isValid()).isTrue();
    assertThat(someTimeMicros.isValid()).isTrue();
    assertThat(someTimeNanos.isValid()).isTrue();

    assertThat(midnight.hashCode()).isEqualTo(zero.hashCode());
    assertThat(zero).isEqualTo(midnight);

    checkSerializeAndDeserialize(midnight, zero);
    checkSerializeAndDeserialize(midnight);
    checkSerializeAndDeserialize(beforeMidnightMicros);
    checkSerializeAndDeserialize(beforeMidnightNanos);
    checkSerializeAndDeserialize(someTimeMicros);
    checkSerializeAndDeserialize(someTimeNanos);
    checkSerializeAndDeserialize(zero);

    assertThat(midnight.toInt64()).isEqualTo(0L);
    assertThat(beforeMidnightMicros.toInt64()).isEqualTo(0x5FBEFB9AC618L);
    assertThat(beforeMidnightNanos.toInt64()).isEqualTo(0x5FBEFB9AC9FFL);
    assertThat(someTimeMicros.toInt64()).isEqualTo(0x322E27002568L);
    assertThat(someTimeNanos.toInt64()).isEqualTo(0x21083ADE68B1L);
    assertThat(zero.toInt64()).isEqualTo(0L);

    try {
      midnight.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_TIME to uint64");
    }

    try {
      midnight.toDouble();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_TIME to double");
    }

    List<Long> invalidTimeValues =
        java.util.Arrays.asList(
            0x600000000000L, // 24:00:00
            0x3C000000000L, // 00:60:00
            0xF00000000L, // 00:00:60
            0x3B9ACA00L, // 00:00:00 with 1000000000 nanos
            // The lower bits actually decode to 12:34:56.654321, but there something on the higher,
            // unused bits, making it invalid.
            0x1000322E27002568L,
            -1L // Any negative encoded value is inherently invalid.
            );
    for (long invalidTimeValue : invalidTimeValues) {
      try {
        Value.createTimeValue(invalidTimeValue);
        fail();
      } catch (IllegalArgumentException expected) {
      }
    }
  }

  @Test
  public void testDatetimeValue() {
    Value epoch = Value.createDatetimeValue(0x1EC8420000L, 0); // 1970-01-01 00:00:00
    Value beginningOfDatetime = Value.createDatetimeValue(0x4420000L, 0); // 0001-01-01 00:00:00
    // 9999-12-31 23:59:59.999999
    Value endOfDatetimeMicros = Value.createDatetimeValue(0x9C3F3F7EFBL, 999999000);
    // 9999-12-31 23:59:59.999999999
    Value endOfDatetimeNanos = Value.createDatetimeValue(0x9C3F3F7EFBL, 999999999);
    // 2016-07-28 12:34:56.654321
    Value someDatetimeMicros = Value.createDatetimeValue(0x1F81F8C8B8L, 654321000);
    // 2006-01-02 03:04:05.123456789
    Value someDatetimeNanos = Value.createDatetimeValue(0x1F58443105L, 123456789);
    Value year1970 = Value.createDatetimeValue(132208787456L, 0); // 1970-01-01 00:00:00

    assertThat(epoch.isNull()).isFalse();
    assertThat(beginningOfDatetime.isNull()).isFalse();
    assertThat(endOfDatetimeMicros.isNull()).isFalse();
    assertThat(endOfDatetimeNanos.isNull()).isFalse();
    assertThat(someDatetimeMicros.isNull()).isFalse();
    assertThat(someDatetimeNanos.isNull()).isFalse();
    assertThat(year1970.isNull()).isFalse();

    assertThat(epoch.isValid()).isTrue();
    assertThat(beginningOfDatetime.isValid()).isTrue();
    assertThat(endOfDatetimeMicros.isValid()).isTrue();
    assertThat(endOfDatetimeNanos.isValid()).isTrue();
    assertThat(someDatetimeMicros.isValid()).isTrue();
    assertThat(someDatetimeNanos.isValid()).isTrue();
    assertThat(year1970.isValid()).isTrue();

    assertThat(year1970.hashCode()).isEqualTo(epoch.hashCode());
    assertThat(epoch).isEqualTo(year1970);

    checkSerializeAndDeserialize(epoch, year1970);
    checkSerializeAndDeserialize(epoch);
    checkSerializeAndDeserialize(beginningOfDatetime);
    checkSerializeAndDeserialize(endOfDatetimeMicros);
    checkSerializeAndDeserialize(endOfDatetimeNanos);
    checkSerializeAndDeserialize(someDatetimeMicros);
    checkSerializeAndDeserialize(someDatetimeNanos);
    checkSerializeAndDeserialize(year1970);

    try {
      epoch.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DATETIME to int64");
    }

    try {
      epoch.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DATETIME to uint64");
    }

    try {
      epoch.toDouble();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_DATETIME to double");
    }

    List<Long> invalidDatetimeBitFieldSecondValues =
        java.util.Arrays.asList(
            0L, //  0000-01-01 00:00:00
            0x9C40420000L, // 10000-01-01 00:00:00
            0x1ECB420000L, //  1970-13-01 00:00:00
            //  1970-01-00 00:00:00 or 1970-01-32 00:00:00 which overflows the day part
            0x1EC8400000L,
            0x1EC8438000L, //  1970-01-01 24:00:00
            0x1EC8420F00L, //  1970-01-01 00:60:00
            0x1EC842003CL, //  1970-01-01 00:00:60
            // The lower bits actually decode to 2006-01-02 03:04:05.123456789, but there something
            // on the higher, unused bits, making it invalid.
            0x1001F58443105L,
            -1L // Any negative encoded value is inherently invalid.
            );
    for (long invalidDatetimeBitFieldSecond : invalidDatetimeBitFieldSecondValues) {
      try {
        Value.createDatetimeValue(invalidDatetimeBitFieldSecond, 0);
        fail();
      } catch (IllegalArgumentException expected) {
      }
    }
    List<Integer> invalidNanosValues = java.util.Arrays.asList(-1, 1000000000);
    for (int invalidNanos : invalidNanosValues) {
      try {
        Value.createDatetimeValue(0x1EC8420000L /* 1970-01-01 00:00:00 */, invalidNanos);
        fail();
      } catch (IllegalArgumentException expected) {
      }
    }
  }

  @Test
  public void testEnumValue() {
    Value typeInt32 = Value.createEnumValue(typeKindEnum, TypeKind.TYPE_INT32_VALUE);
    Value typeDouble = Value.createEnumValue(typeKindEnum, TypeKind.TYPE_DOUBLE_VALUE);
    Value anotherInt32 = Value.createEnumValue(typeKindEnum, 1);

    assertThat(typeInt32.isNull()).isFalse();
    assertThat(typeDouble.isNull()).isFalse();
    assertThat(anotherInt32.isNull()).isFalse();

    assertThat(typeInt32.isValid()).isTrue();
    assertThat(typeDouble.isValid()).isTrue();
    assertThat(anotherInt32.isValid()).isTrue();

    assertThat(typeInt32.getEnumValue()).isEqualTo(1);
    assertThat(typeInt32.getEnumName()).isEqualTo("TYPE_INT32");
    assertThat(typeDouble.getEnumValue()).isEqualTo(7);
    assertThat(typeDouble.getEnumName()).isEqualTo("TYPE_DOUBLE");
    assertThat(anotherInt32.getEnumValue()).isEqualTo(1);
    assertThat(anotherInt32.getEnumName()).isEqualTo("TYPE_INT32");

    assertThat(typeInt32).isEqualTo(anotherInt32);
    assertThat(anotherInt32.hashCode()).isEqualTo(typeInt32.hashCode());

    try {
      Value.createEnumValue(typeKindEnum, -10);
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.createEnumValue(typeKindEnum, 10000);
      fail();
    } catch (IllegalArgumentException expected) {
    }

    checkSerializeAndDeserialize(typeInt32, anotherInt32);
    checkSerializeAndDeserialize(typeInt32);
    checkSerializeAndDeserialize(typeDouble);
    checkSerializeAndDeserialize(anotherInt32);

    assertThat(typeInt32.toInt64()).isEqualTo(1L);
    assertThat(typeDouble.toInt64()).isEqualTo(7L);
    assertThat(anotherInt32.toInt64()).isEqualTo(1L);

    try {
      typeInt32.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_ENUM to uint64");
    }
    try {
      typeDouble.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_ENUM to uint64");
    }
    try {
      anotherInt32.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_ENUM to uint64");
    }

    assertThat(typeInt32.toDouble()).isEqualTo(1.0);
    assertThat(typeDouble.toDouble()).isEqualTo(7.0);
    assertThat(anotherInt32.toDouble()).isEqualTo(1.0);
  }

  @Test
  public void testProtoValue() throws InvalidProtocolBufferException {
    Value empty = Value.createProtoValue(typeProto, ByteString.EMPTY);
    SimpleType int32 = TypeFactory.createSimpleType(TypeKind.TYPE_INT32);
    Value int32Value = Value.createProtoValue(typeProto, int32.serialize().toByteString());
    Value int32Value2 =
        Value.createProtoValue(
            typeProto, typeProtoFromText("type_kind: TYPE_INT32").toByteString());

    TypeFactory factory = TypeFactory.nonUniqueNames();
    ProtoType typeProto2 =
        factory.createProtoType(
            getDescriptorPoolWithTypeProtoAndTypeKind()
                .findMessageTypeByName("zetasql.TypeProto"));
    assertThat(typeProto.equivalent(typeProto2)).isTrue();
    assertThat(typeProto.equals(typeProto2)).isFalse();
    Value int32Value3 = Value.createProtoValue(typeProto2, int32.serialize().toByteString());

    assertThat(empty.isNull()).isFalse();
    assertThat(int32Value.isNull()).isFalse();
    assertThat(int32Value2.isNull()).isFalse();
    assertThat(int32Value3.isNull()).isFalse();

    assertThat(empty.isValid()).isTrue();
    assertThat(int32Value.isValid()).isTrue();
    assertThat(int32Value2.isValid()).isTrue();
    assertThat(int32Value3.isValid()).isTrue();

    assertThat(empty.getProtoValue()).isEqualTo(ByteString.EMPTY);
    assertThat(int32Value).isEqualTo(int32Value2);
    assertThat(int32Value2.hashCode()).isEqualTo(int32Value.hashCode());
    assertThat(int32Value).isEqualTo(int32Value3);
    assertThat(int32Value3.hashCode()).isEqualTo(int32Value.hashCode());
    assertThat(int32Value2).isEqualTo(int32Value3);
    assertThat(int32Value3.hashCode()).isEqualTo(int32Value2.hashCode());

    checkSerializeAndDeserialize(int32Value, int32Value2);
    checkSerializeAndDeserialize(empty);
    checkSerializeAndDeserialize(int32Value);
    checkSerializeAndDeserialize(int32Value2);
    checkSerializeAndDeserialize(int32Value3);

    Message m = TypeProto.newBuilder().setTypeKind(TypeKind.TYPE_INT32).build();
    assertThat(int32Value.toMessage()).isEqualTo(m);
    assertThat(int32Value2.toMessage()).isEqualTo(m);
    assertThat(int32Value3.toMessage().toString()).isEqualTo(m.toString());
    assertThat(empty.toMessage()).isEqualTo(TypeProto.newBuilder().getDefaultInstanceForType());

    try {
      int32Value.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_PROTO to int64");
    }
    try {
      int32Value.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_PROTO to uint64");
    }
    try {
      int32Value.toDouble();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_PROTO to double");
    }
  }

  @Test
  public void testArrayValue() {
    ArrayType testArrayType =
        TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32));

    List<Value> elements1 = new ArrayList<>();
    elements1.add(Value.createInt32Value(1));
    elements1.add(Value.createInt32Value(2));
    elements1.add(Value.createInt32Value(3));

    List<Value> elements2 = new ArrayList<>();
    elements2.add(Value.createInt32Value(1));
    elements2.add(Value.createInt32Value(2));

    List<Value> elements3 = new ArrayList<>();
    elements3.add(Value.createInt32Value(1));
    elements3.add(Value.createInt32Value(2));
    elements3.add(Value.createInt32Value(4));

    Value array1 = Value.createArrayValue(testArrayType, elements1);
    Value array2 = Value.createArrayValue(testArrayType, elements2);
    Value array3 = Value.createArrayValue(testArrayType, elements1);
    Value array4 = Value.createArrayValue(testArrayType, new ArrayList<Value>());
    Value array5 = Value.createEmptyArrayValue(testArrayType);
    Value array6 = Value.createArrayValue(testArrayType, elements3);

    assertThat(array1.isNull()).isFalse();
    assertThat(array2.isNull()).isFalse();
    assertThat(array3.isNull()).isFalse();
    assertThat(array4.isNull()).isFalse();
    assertThat(array5.isNull()).isFalse();
    assertThat(array6.isNull()).isFalse();

    assertThat(array1.isValid()).isTrue();
    assertThat(array2.isValid()).isTrue();
    assertThat(array3.isValid()).isTrue();
    assertThat(array4.isValid()).isTrue();
    assertThat(array5.isValid()).isTrue();
    assertThat(array6.isValid()).isTrue();

    assertThat(array1.isEmptyArray()).isFalse();
    assertThat(array2.isEmptyArray()).isFalse();
    assertThat(array3.isEmptyArray()).isFalse();
    assertThat(array4.isEmptyArray()).isTrue();
    assertThat(array5.isEmptyArray()).isTrue();
    assertThat(array6.isEmptyArray()).isFalse();

    assertThat(array1.getElementCount()).isEqualTo(elements1.size());
    assertThat(array2.getElementCount()).isEqualTo(elements2.size());
    assertThat(array3.getElementCount()).isEqualTo(elements1.size());
    assertThat(array4.getElementCount()).isEqualTo(0);
    assertThat(array5.getElementCount()).isEqualTo(0);
    assertThat(array6.getElementCount()).isEqualTo(elements3.size());

    for (int i = 0; i < array1.getElementCount(); ++i) {
      assertThat(elements1.get(i).equals(array1.getElement(i))).isTrue();
    }

    int i = 0;
    for (Value element : array2.getElementList()) {
      assertThat(elements2.get(i++)).isEqualTo(element);
    }

    assertThat(array1).isEqualTo(array3);
    assertThat(array4).isEqualTo(array5);
    assertThat(array1.equals(array2)).isFalse();
    assertThat(array3.equals(array5)).isFalse();
    assertThat(array1.equals(array6)).isFalse();

    assertThat(array3.hashCode()).isEqualTo(array1.hashCode());
    assertThat(array5.hashCode()).isEqualTo(array4.hashCode());

    try {
      List<Value> values = new ArrayList<>();
      values.add(Value.createInt64Value(0));
      Value.createArrayValue(testArrayType, values);
      fail();
    } catch (IllegalArgumentException expected) {
    }

    checkSerializeAndDeserialize(array1, array3);
    checkSerializeAndDeserialize(array4, array5);
    checkSerializeAndDeserialize(array1);
    checkSerializeAndDeserialize(array2);
    checkSerializeAndDeserialize(array3);
    checkSerializeAndDeserialize(array4);
    checkSerializeAndDeserialize(array5);
    checkSerializeAndDeserialize(array6);

    try {
      array1.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_ARRAY to int64");
    }
    try {
      array1.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_ARRAY to uint64");
    }
    try {
      array1.toDouble();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_ARRAY to double");
    }
  }

  @Test
  public void testStructValue() {
    List<StructType.StructField> fields = new ArrayList<>();
    fields.add(new StructType.StructField("enum", typeKindEnum));
    fields.add(
        new StructType.StructField("int32", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));
    StructType type1 = TypeFactory.createStructType(fields);

    // type1 and type2 have fields with the same type but different names.
    List<StructType.StructField> fields2 = new ArrayList<>();
    fields2.add(new StructType.StructField("int32", typeKindEnum));
    fields2.add(
        new StructType.StructField("enum", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));
    StructType type2 = TypeFactory.createStructType(fields2);

    StructType emptyStruct = TypeFactory.createStructType(new ArrayList<StructType.StructField>());

    List<Value> values1 = new ArrayList<>();
    values1.add(Value.createEnumValue(typeKindEnum, 1));
    values1.add(Value.createInt32Value(2));

    List<Value> values2 = new ArrayList<>();
    values2.add(Value.createEnumValue(typeKindEnum, 1));
    values2.add(Value.createInt32Value(2));

    List<Value> values3 = new ArrayList<>();
    values3.add(Value.createEnumValue(typeKindEnum, 1));
    values3.add(Value.createInt32Value(3));

    Value struct1 = Value.createStructValue(type1, values1);
    Value struct2 = Value.createStructValue(type2, values2);
    Value struct3 = Value.createStructValue(emptyStruct, new ArrayList<Value>());
    Value struct4 = Value.createStructValue(type1, values3);
    Value struct5 = Value.createStructValue(emptyStruct, new ArrayList<Value>());

    assertThat(struct1.isNull()).isFalse();
    assertThat(struct2.isNull()).isFalse();
    assertThat(struct3.isNull()).isFalse();
    assertThat(struct4.isNull()).isFalse();
    assertThat(struct1.getFieldCount()).isEqualTo(values1.size());
    assertThat(struct2.getFieldCount()).isEqualTo(values2.size());
    assertThat(struct3.getFieldCount()).isEqualTo(0);
    assertThat(struct4.getFieldCount()).isEqualTo(values3.size());
    assertThat(struct5.getFieldCount()).isEqualTo(0);

    assertThat(struct1.isValid()).isTrue();
    assertThat(struct2.isValid()).isTrue();
    assertThat(struct3.isValid()).isTrue();
    assertThat(struct4.isValid()).isTrue();

    for (int i = 0; i < struct1.getFieldCount(); ++i) {
      assertThat(values1.get(i).equals(struct1.getField(i))).isTrue();
    }

    int i = 0;
    for (Value field : struct2.getFieldList()) {
      assertThat(values2.get(i++)).isEqualTo(field);
    }

    assertThat(struct1.findFieldByName("enum").getEnumValue()).isEqualTo(1);
    assertThat(struct1.findFieldByName("int32").getInt32Value()).isEqualTo(2);
    assertThat(struct1.findFieldByName("int64")).isNull();

    assertThat(struct2.hashCode()).isEqualTo(struct1.hashCode());
    assertThat(struct5.hashCode()).isEqualTo(struct3.hashCode());
    assertThat(struct1.getType().equals(struct2.getType())).isFalse();
    assertThat(struct1.getType().equivalent(struct2.getType())).isFalse();
    assertThat(struct1).isEqualTo(struct2);
    assertThat(struct1.equals(struct3)).isFalse();
    assertThat(struct1.equals(struct4)).isFalse();
    assertThat(struct3).isEqualTo(struct5);

    try {
      Value.createStructValue(type1, new ArrayList<Value>());
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.createStructValue(emptyStruct, values1);
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      List<Value> values4 = new ArrayList<>();
      values4.add(Value.createEnumValue(typeKindEnum, 1));
      values4.add(Value.createEnumValue(typeKindEnum, 1));
      Value.createStructValue(type1, values4);
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      struct1.toInt64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_STRUCT to int64");
    }
    try {
      struct1.toUint64();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_STRUCT to uint64");
    }
    try {
      struct1.toDouble();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot coerce TYPE_STRUCT to double");
    }
  }

  @Test
  public void testCreateNullValue() {
    SimpleType int32 = TypeFactory.createSimpleType(TypeKind.TYPE_INT32);
    Value int32Null = Value.createNullValue(int32);
    assertThat(int32Null.isNull()).isTrue();
    assertThat(int32Null.getType()).isEqualTo(int32);
    assertThat(valueProtoFromText("")).isEqualTo(int32Null.getProto());

    Value enumNull = Value.createNullValue(typeKindEnum);
    assertThat(enumNull.isNull()).isTrue();
    assertThat(enumNull.getType()).isEqualTo(typeKindEnum);
    assertThat(valueProtoFromText("")).isEqualTo(enumNull.getProto());

    Value protoNull = Value.createNullValue(typeProto);
    assertThat(protoNull.isNull()).isTrue();
    assertThat(protoNull.getType()).isEqualTo(typeProto);
    assertThat(valueProtoFromText("")).isEqualTo(protoNull.getProto());

    TypeFactory factory = TypeFactory.nonUniqueNames();
    ArrayType arrayType = TypeFactory.createArrayType(int32);
    Value arrayNull = Value.createNullValue(arrayType);
    assertThat(arrayNull.isNull()).isTrue();
    assertThat(arrayNull.getType()).isEqualTo(arrayType);
    assertThat(valueProtoFromText("")).isEqualTo(arrayNull.getProto());

    ProtoType valueProto = factory.createProtoType(ValueProto.class);
    Value protoNull2 = Value.createNullValue(valueProto);
    assertThat(protoNull2.isNull()).isTrue();
    assertThat(protoNull2.getType()).isEqualTo(valueProto);
    assertThat(valueProtoFromText("")).isEqualTo(protoNull2.getProto());

    assertThat(protoNull).isEqualTo(protoNull2);

    List<StructType.StructField> fields = new ArrayList<>();
    fields.add(new StructType.StructField("enum", typeKindEnum));
    fields.add(
        new StructType.StructField("int32", TypeFactory.createSimpleType(TypeKind.TYPE_INT32)));
    StructType structType = TypeFactory.createStructType(fields);
    Value structNull = Value.createNullValue(structType);
    assertThat(structNull.isNull()).isTrue();
    assertThat(structNull.getType()).isEqualTo(structType);
    assertThat(valueProtoFromText("")).isEqualTo(structNull.getProto());

    checkSerializeAndDeserialize(int32Null, Value.createNullValue(int32));
    checkSerializeAndDeserialize(enumNull, Value.createNullValue(typeKindEnum));
    checkSerializeAndDeserialize(protoNull, Value.createNullValue(typeProto));

    checkSerializeAndDeserialize(int32Null);
    checkSerializeAndDeserialize(enumNull);
    checkSerializeAndDeserialize(protoNull);
    checkSerializeAndDeserialize(protoNull2);
    checkSerializeAndDeserialize(arrayNull);
  }

  @Test
  public void testCreateSimpleNullValue() {
    for (TypeKind kind : TypeKind.values()) {
      if (TypeFactory.isSimpleType(kind)) {
        if (Value.isSupportedTypeKind(TypeFactory.createSimpleType(kind))) {
          Value value = Value.createSimpleNullValue(kind);
          assertThat(value.isNull()).isTrue();
          assertThat(value.getType().getKind()).isEqualTo(kind);
          assertThat(valueProtoFromText("")).isEqualTo(value.getProto());
          checkSerializeAndDeserialize(value);
        } else {
          try {
            Value.createSimpleNullValue(kind);
            fail("Should not be able to create unsupported simple type value.");
          } catch (IllegalArgumentException expected) {
          }
        }
      }
    }
  }

  public static void checkSerializeAndDeserialize(Value v1, Value v2) {
    assertThat(v1).isEqualTo(v2);
    assertThat(v2.hashCode()).isEqualTo(v1.hashCode());
    assertThat(v1.serialize()).isEqualTo(v2.serialize());
    try {
      Value newV1 = Value.deserialize(v1.getType(), v1.serialize());
      Value newV2 = Value.deserialize(v2.getType(), v2.serialize());
      assertThat(newV1).isEqualTo(v1);
      assertThat(newV2).isEqualTo(v2);
      assertThat(newV1).isEqualTo(newV2);
    } catch (IllegalArgumentException e) {
      fail(e.toString());
    }
  }

  public static void checkSerializeAndDeserialize(Value value) {
    try {
      Value value2 = Value.deserialize(value.getType(), value.serialize());
      assertThat(value).isEqualTo(value2);
      assertThat(value2).isEqualTo(value);
      assertThat(value2.hashCode()).isEqualTo(value.hashCode());
      assertThat(value.serialize()).isEqualTo(value2.serialize());
      assertThat(SerializableTester.reserialize(value)).isEqualTo(value);
    } catch (IllegalArgumentException e) {
      fail(e.toString());
    }
  }

  @Test
  public void testDeserialize() {
    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_INT64), valueProtoFromText("int64_value: 1"));
    } catch (IllegalArgumentException e) {
      fail(e.toString());
    }

    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_INT32), valueProtoFromText("int64_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_INT64), valueProtoFromText("int32_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_UINT32), valueProtoFromText("int64_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_UINT64), valueProtoFromText("int32_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_BOOL), valueProtoFromText("int64_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT), valueProtoFromText("int32_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE), valueProtoFromText("int64_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_STRING), valueProtoFromText("int32_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_BYTES), valueProtoFromText("int32_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_DATE), valueProtoFromText("int32_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_DATE),
          valueProtoFromText("date_value: 2932897"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP),
          valueProtoFromText("int32_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value v =
          Value.deserialize(
              TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP),
              valueProtoFromText("timestamp_value: {seconds: 253402300800}"));
      fail(v.toString());
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(typeKindEnum, valueProtoFromText("int32_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(typeKindEnum, valueProtoFromText("enum_value: -10"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(typeKindEnum, valueProtoFromText("enum_value: 1"));
    } catch (IllegalArgumentException e) {
      fail(e.toString());
    }

    ArrayType testArrayType =
        TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_INT32));
    try {
      Value.deserialize(testArrayType, valueProtoFromText("int32_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          testArrayType,
          valueProtoFromText("array_value {" + "  element {" + "    int32_value: 1" + "  }" + "}"));
    } catch (IllegalArgumentException e) {
      fail(e.toString());
    }

    try {
      Value.deserialize(
          testArrayType,
          valueProtoFromText(
              "array_value {"
                  + "  element {"
                  + "    int32_value: 1"
                  + "  }"
                  + "  element {"
                  + "    int32_value: 2"
                  + "  }"
                  + "}"));
    } catch (IllegalArgumentException e) {
      fail(e.toString());
    }

    try {
      Value.deserialize(testArrayType, valueProtoFromText("array_value {" + "}"));
    } catch (IllegalArgumentException e) {
      fail(e.toString());
    }

    try {
      Value.deserialize(
          testArrayType,
          valueProtoFromText("array_value {" + "  element {" + "    int64_value: 1" + "  }" + "}"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(
          testArrayType,
          valueProtoFromText(
              "array_value {"
                  + "  element {"
                  + "    int32_value: 1"
                  + "  }"
                  + "  element {"
                  + "    int64_value: 1"
                  + "  }"
                  + "}"));
      fail();
    } catch (IllegalArgumentException expected) {
    }

    try {
      Value.deserialize(typeProto, valueProtoFromText("int32_value: 1"));
      fail();
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testNotEquals() {
    Value int32Zero = Value.createInt32Value(0);
    Value int64Zero = Value.createInt64Value(0);
    Value enumZero = Value.createEnumValue(typeKindEnum, 0);
    Value int32Null = Value.createSimpleNullValue(TypeKind.TYPE_INT32);
    assertThat(int32Zero.equals(int64Zero)).isFalse();
    assertThat(enumZero.equals(int64Zero)).isFalse();
    assertThat(enumZero.equals(int32Zero)).isFalse();
    assertThat(int32Null.equals(int32Zero)).isFalse();
    assertThat(int32Zero.equals(int32Null)).isFalse();
  }

  @Test
  public void testClassAndProtoSize() {
    assertWithMessage(
            "The number of fields of ValueProto has changed, "
                + "please also update the serialization code accordingly.")
        .that(ValueProto.getDescriptor().getFields())
        .hasSize(20);
    assertWithMessage(
            "The number of fields of ValueProto::Array has changed, "
                + "please also update the serialization code accordingly.")
        .that(ValueProto.Array.getDescriptor().getFields())
        .hasSize(1);
    assertWithMessage(
            "The number of fields of ValueProto::Struct has changed, "
                + "please also update the serialization code accordingly.")
        .that(ValueProto.Struct.getDescriptor().getFields())
        .hasSize(1);
    assertWithMessage(
            "The number of fields in Value class has changed, "
                + "please also update the proto and serialization code accordingly.")
        .that(TestUtil.getNonStaticFieldCount(Value.class))
        .isEqualTo(6);
  }

  private static TypeProto typeProtoFromText(String textProto) {
    TypeProto.Builder builder = TypeProto.newBuilder();
    try {
      TextFormat.merge(textProto, builder);
    } catch (ParseException e) {
      fail(e.toString());
    }
    return builder.build();
  }

  private static ValueProto valueProtoFromText(String textProto) {
    ValueProto.Builder builder = ValueProto.newBuilder();
    try {
      TextFormat.merge(textProto, builder);
    } catch (ParseException e) {
      fail(e.toString());
    }
    return builder.build();
  }

  private static TypeFactory testFactory = TypeFactory.nonUniqueNames();
  private static EnumType typeKindEnum = testFactory.createEnumType(TypeKind.class);
  private static ProtoType typeProto = testFactory.createProtoType(TypeProto.class);
}
