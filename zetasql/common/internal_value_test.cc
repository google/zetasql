//
// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "zetasql/common/internal_value.h"

#include "zetasql/public/value.h"
#include "zetasql/testing/test_value.h"
#include "gtest/gtest.h"

namespace zetasql {

using types::Int64Type;

static Value Int64(int64_t v) { return Value::Int64(v); }

TEST(InternalValueTest, SimpleArraysUncertainOrders) {
  const ArrayType* type = test_values::MakeArrayType(Int64Type());
  Value array_null = Value::Null(type);
  Value array_empty =
      InternalValue::ArrayChecked(type, InternalValue::kIgnoresOrder,
                                  /*values=*/{});
  Value array_len_one_ordered = InternalValue::ArrayChecked(
      type, InternalValue::kPreservesOrder, {Int64(1)});
  Value array_len_one_unordered = InternalValue::ArrayChecked(
      type, InternalValue::kIgnoresOrder, {Int64(1)});
  Value array_len_two_ordered = InternalValue::ArrayChecked(
      type, InternalValue::kPreservesOrder, {Int64(1), Int64(2)});
  Value array_len_two_unordered = InternalValue::ArrayChecked(
      type, InternalValue::kIgnoresOrder, {Int64(1), Int64(2)});
  Value array_len_three_ordered = InternalValue::ArrayChecked(
      type, InternalValue::kPreservesOrder,
      {Int64(1), Int64(2), Value::Null(Int64Type())});
  Value array_len_three_unordered = InternalValue::ArrayChecked(
      type, InternalValue::kIgnoresOrder,
      {Int64(1), Int64(2), Value::Null(Int64Type())});

  EXPECT_FALSE(InternalValue::ContainsArrayWithUncertainOrder(array_null));
  EXPECT_FALSE(InternalValue::ContainsArrayWithUncertainOrder(array_empty));
  EXPECT_FALSE(
      InternalValue::ContainsArrayWithUncertainOrder(array_len_one_ordered));
  EXPECT_FALSE(
      InternalValue::ContainsArrayWithUncertainOrder(array_len_one_unordered));
  EXPECT_FALSE(
      InternalValue::ContainsArrayWithUncertainOrder(array_len_two_ordered));
  EXPECT_TRUE(
      InternalValue::ContainsArrayWithUncertainOrder(array_len_two_unordered));
  EXPECT_FALSE(
      InternalValue::ContainsArrayWithUncertainOrder(array_len_three_ordered));
  EXPECT_TRUE(InternalValue::ContainsArrayWithUncertainOrder(
      array_len_three_unordered));
}

TEST(InternalValueTest, NestedValues) {
  const ArrayType* array_int = test_values::MakeArrayType(Int64Type());
  const StructType* struct_type =
      test_values::MakeStructType({{"a", array_int}, {"b", array_int}});
  const ArrayType* struct_arr = test_values::MakeArrayType(struct_type);

  Value array_null = Value::Null(array_int);
  Value array_empty =
      InternalValue::ArrayChecked(array_int, InternalValue::kIgnoresOrder,
                                  /*values=*/{});
  Value int_array_len_three_ordered = InternalValue::ArrayChecked(
      array_int, InternalValue::kPreservesOrder,
      {Int64(1), Int64(2), Value::Null(Int64Type())});
  Value int_array_len_three_unordered = InternalValue::ArrayChecked(
      array_int, InternalValue::kIgnoresOrder,
      {Int64(1), Int64(2), Value::Null(Int64Type())});

  Value struct_null_empty =
      *Value::MakeStruct(struct_type, {array_null, array_empty});
  Value struct_ordered_unordered = *Value::MakeStruct(
      struct_type,
      {int_array_len_three_ordered, int_array_len_three_unordered});

  Value array_of_struct_ordered = InternalValue::ArrayChecked(
      struct_arr, InternalValue::kPreservesOrder,
      {struct_null_empty, struct_ordered_unordered});

  EXPECT_FALSE(
      InternalValue::ContainsArrayWithUncertainOrder(struct_null_empty));
  EXPECT_TRUE(
      InternalValue::ContainsArrayWithUncertainOrder(struct_ordered_unordered));
  EXPECT_TRUE(
      InternalValue::ContainsArrayWithUncertainOrder(array_of_struct_ordered));
}

}  // namespace zetasql
