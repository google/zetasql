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

#include "zetasql/reference_impl/function.h"

#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "zetasql/common/evaluator_registration_utils.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/interval_value.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/operator.h"
#include "zetasql/reference_impl/tuple.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {

TEST(SafeInvokeUnary, DoesNotLeakStatus) {
  ArithmeticFunction unary_minus_fn(FunctionKind::kSafeNegate,
                                    types::Int64Type());

  std::vector<const TupleData*> params;
  EvaluationContext context{/*options=*/{}};
  Value result;
  absl::Status status;
  EXPECT_TRUE(unary_minus_fn.Eval(
      /*params=*/{},
      /*args=*/{Value::Int64(std::numeric_limits<int64_t>::lowest())}, &context,
      &result, &status));

  EXPECT_TRUE(result.is_null());
  ZETASQL_EXPECT_OK(status);
}

TEST(SafeInvokeBinary, DoesNotLeakStatus) {
  ArithmeticFunction safe_divide_fn(FunctionKind::kSafeDivide,
                                    types::DoubleType());

  std::vector<const TupleData*> params;
  EvaluationContext context{/*options=*/{}};
  Value result;
  absl::Status status;
  EXPECT_TRUE(
      safe_divide_fn.Eval(/*params=*/{},
                          /*args=*/{Value::Double(1.0), Value::Double(0.0)},
                          &context, &result, &status));
  EXPECT_TRUE(result.is_null());
  ZETASQL_EXPECT_OK(status);
}

TEST(NonDeterministicEvaluationContextTest, ArrayFilterTransformFunctionTest) {
  TypeFactory factory;
  const ArrayType* array_type;
  const Type* element_type = factory.get_int64();
  ZETASQL_EXPECT_OK(factory.MakeArrayType(element_type, &array_type));

  std::unique_ptr<ConstExpr> lambda_body =
      ConstExpr::Create(Value::Int64(3)).value();
  std::vector<VariableId> lambda_arg_vars = {VariableId("e")};
  std::unique_ptr<InlineLambdaExpr> lambda_algebra =
      InlineLambdaExpr::Create(lambda_arg_vars, std::move(lambda_body));

  ArrayTransformFunction trans_fn(FunctionKind::kArrayTransform, array_type,
                                  lambda_algebra.get());

  EvaluationContext context{/*options=*/{}};
  EXPECT_TRUE(context.IsDeterministicOutput());
  ZETASQL_EXPECT_OK(
      trans_fn
          .Eval(/*params=*/{},
                /*args=*/
                {Value::Array(array_type, {Value::Int64(1), Value::Int64(2)})},
                &context)
          .status());
  EXPECT_TRUE(context.IsDeterministicOutput());

  ZETASQL_EXPECT_OK(trans_fn
                .Eval(/*params=*/{},
                      /*args=*/
                      {InternalValue::Array(array_type,
                                            {Value::Int64(1), Value::Int64(2)},
                                            InternalValue::kIgnoresOrder)},
                      &context)
                .status());
  EXPECT_FALSE(context.IsDeterministicOutput());
}

TEST(NonDeterministicEvaluationContextTest,
     ArrayMinMaxDistinguishableTiesStringTest) {
  // This setup overwrites the CollatorRegistration::CreateFromCollationNameFn
  // for current process, so that case insensitive collation name can be used.
  internal::EnableFullEvaluatorFeatures();

  // String with collation introduces non-determinism against distinguishable
  // ties.
  TypeFactory factory;
  const ArrayType* array_type;
  const Type* element_type = factory.get_string();
  ZETASQL_EXPECT_OK(factory.MakeArrayType(element_type, &array_type));

  std::vector<ResolvedCollation> collation_list;
  collation_list.push_back(ResolvedCollation::MakeScalar("unicode:ci"));

  // ARRAY_MIN
  ZETASQL_ASSERT_OK_AND_ASSIGN(CollatorList collator_list_min,
                       MakeCollatorList(collation_list));

  ArrayMinMaxFunction arr_min_fn(FunctionKind::kArrayMin, element_type,
                                 std::move(collator_list_min));
  EvaluationContext context_min{/*options=*/{}};
  EXPECT_TRUE(context_min.IsDeterministicOutput());
  ZETASQL_EXPECT_OK(arr_min_fn
                .Eval(/*params=*/{},
                      /*args=*/
                      {InternalValue::Array(
                          array_type, {Value::String("a"), Value::String("A")},
                          InternalValue::kIgnoresOrder)},
                      &context_min)
                .status());
  EXPECT_FALSE(context_min.IsDeterministicOutput());

  // ARRAY_MAX
  ZETASQL_ASSERT_OK_AND_ASSIGN(CollatorList collator_list_max,
                       MakeCollatorList(collation_list));

  ArrayMinMaxFunction arr_max_fn(FunctionKind::kArrayMax, element_type,
                                 std::move(collator_list_max));
  EvaluationContext context_max{/*options=*/{}};
  EXPECT_TRUE(context_max.IsDeterministicOutput());
  ZETASQL_EXPECT_OK(arr_max_fn
                .Eval(/*params=*/{},
                      /*args=*/
                      {InternalValue::Array(
                          array_type, {Value::String("a"), Value::String("A")},
                          InternalValue::kIgnoresOrder)},
                      &context_max)
                .status());
  EXPECT_FALSE(context_max.IsDeterministicOutput());
}

TEST(NonDeterministicEvaluationContextTest,
     ArrayMinMaxDistinguishableTiesIntervalTest) {
  // Interval with distinguishable ties could also trigger the deterministic
  // switch.
  TypeFactory factory;
  const ArrayType* array_type;
  const Type* element_type = factory.get_interval();
  ZETASQL_EXPECT_OK(factory.MakeArrayType(element_type, &array_type));

  // ARRAY_MIN
  ArrayMinMaxFunction arr_min_fn(FunctionKind::kArrayMin, element_type);
  EvaluationContext context_min{/*options=*/{}};
  EXPECT_TRUE(context_min.IsDeterministicOutput());
  ZETASQL_EXPECT_OK(
      arr_min_fn
          .Eval(/*params=*/{},
                /*args=*/
                {InternalValue::Array(
                    array_type,
                    {Value::Interval(IntervalValue::FromDays(30).value()),
                     Value::Interval(IntervalValue::FromMonths(1).value())},
                    InternalValue::kIgnoresOrder)},
                &context_min)
          .status());
  EXPECT_FALSE(context_min.IsDeterministicOutput());

  // ARRAY_MAX
  ArrayMinMaxFunction arr_max_fn(FunctionKind::kArrayMax, element_type);
  EvaluationContext context_max{/*options=*/{}};
  EXPECT_TRUE(context_max.IsDeterministicOutput());
  ZETASQL_EXPECT_OK(
      arr_max_fn
          .Eval(/*params=*/{},
                /*args=*/
                {InternalValue::Array(
                    array_type,
                    {Value::Interval(IntervalValue::FromDays(30).value()),
                     Value::Interval(IntervalValue::FromMonths(1).value())},
                    InternalValue::kIgnoresOrder)},
                &context_max)
          .status());
  EXPECT_FALSE(context_max.IsDeterministicOutput());
}

TEST(NonDeterministicEvaluationContextTest, ArraySumAvgFloatingPointTypeTest) {
  // Array input with floating-point type element introduces indeterminism for
  // ARRAY_SUM and ARRAY_AVG because floating point addition is not associative.
  TypeFactory factory;
  const ArrayType* array_type;
  const Type* element_type = factory.get_double();
  ZETASQL_EXPECT_OK(factory.MakeArrayType(element_type, &array_type));

  // ARRAY_SUM
  ArraySumAvgFunction arr_sum_fn(FunctionKind::kArraySum, factory.get_double());
  EvaluationContext context_sum{/*options=*/{}};
  EXPECT_TRUE(context_sum.IsDeterministicOutput());
  ZETASQL_EXPECT_OK(arr_sum_fn
                .Eval(/*params=*/{},
                      /*args=*/
                      {InternalValue::Array(
                          array_type, {Value::Double(4.1), Value::Double(-3.5)},
                          InternalValue::kIgnoresOrder)},
                      &context_sum)
                .status());
  EXPECT_FALSE(context_sum.IsDeterministicOutput());

  // ARRAY_AVG
  element_type = factory.get_float();
  ZETASQL_EXPECT_OK(factory.MakeArrayType(element_type, &array_type));
  ArraySumAvgFunction arr_avg_fn(FunctionKind::kArrayAvg, factory.get_double());
  EvaluationContext context_avg{/*options=*/{}};
  EXPECT_TRUE(context_avg.IsDeterministicOutput());
  ZETASQL_EXPECT_OK(arr_avg_fn
                .Eval(/*params=*/{},
                      /*args=*/
                      {InternalValue::Array(
                          array_type, {Value::Float(4.1), Value::Float(-3.5)},
                          InternalValue::kIgnoresOrder)},
                      &context_avg)
                .status());
  EXPECT_FALSE(context_avg.IsDeterministicOutput());
}

TEST(NonDeterministicEvaluationContextTest, ArraySumAvgUnsignedIntTypeTest) {
  // Array input with signed or unsigned integer type element introduces
  // indeterminism for ARRAY_AVG but not for ARRAY_SUM.
  TypeFactory factory;
  const ArrayType* array_type;
  const Type* element_type = factory.get_uint64();
  ZETASQL_EXPECT_OK(factory.MakeArrayType(element_type, &array_type));

  // ARRAY_SUM
  ArraySumAvgFunction arr_sum_fn(FunctionKind::kArraySum, factory.get_uint64());
  EvaluationContext context_sum{/*options=*/{}};
  EXPECT_TRUE(context_sum.IsDeterministicOutput());
  ZETASQL_EXPECT_OK(arr_sum_fn
                .Eval(/*params=*/{},
                      /*args=*/
                      {InternalValue::Array(
                          array_type, {Value::Uint64(40), Value::Uint64(20)},
                          InternalValue::kIgnoresOrder)},
                      &context_sum)
                .status());
  EXPECT_TRUE(context_sum.IsDeterministicOutput());

  // ARRAY_AVG
  ArraySumAvgFunction arr_avg_fn(FunctionKind::kArrayAvg, factory.get_double());
  EvaluationContext context_avg{/*options=*/{}};
  EXPECT_TRUE(context_avg.IsDeterministicOutput());
  ZETASQL_EXPECT_OK(arr_avg_fn
                .Eval(/*params=*/{},
                      /*args=*/
                      {InternalValue::Array(
                          array_type, {Value::Uint64(40), Value::Uint64(20)},
                          InternalValue::kIgnoresOrder)},
                      &context_avg)
                .status());
  EXPECT_FALSE(context_avg.IsDeterministicOutput());
}

TEST(NonDeterministicEvaluationContextTest,
     ArrayOffsetDistinguishableStringTest) {
  // Array input with collated STRING type element introduces indeterminism for
  // ARRAY_OFFSET(array<T>, T [, mode]) -> INT64
  // but not for
  // ARRAY_OFFSETS(array<T>, T) -> ARRAY<INT64>.

  // This setup overwrites the CollatorRegistration::CreateFromCollationNameFn
  // for current process, so that case insensitive collation name can be used.
  internal::EnableFullEvaluatorFeatures();

  TypeFactory factory;
  const ArrayType* input_type;
  const Type* element_type = factory.get_string();
  ZETASQL_EXPECT_OK(factory.MakeArrayType(element_type, &input_type));
  const ArrayType* int64_array_type;
  ZETASQL_EXPECT_OK(factory.MakeArrayType(factory.get_int64(), &int64_array_type));

  std::vector<ResolvedCollation> collation_list = {
      ResolvedCollation::MakeScalar("unicode:ci")};
  const Value array_find_mode_first =
      Value::Enum(types::ArrayFindModeEnumType(), 1);

  // ARRAY_OFFSET
  ZETASQL_ASSERT_OK_AND_ASSIGN(CollatorList collator_list_offset,
                       MakeCollatorList(collation_list));

  ArrayFindFunctions offset_fn(FunctionKind::kArrayOffset, factory.get_int64(),
                               std::move(collator_list_offset));
  {
    EvaluationContext context_offset{/*options=*/{}};
    EXPECT_TRUE(context_offset.IsDeterministicOutput());
    ZETASQL_EXPECT_OK(
        offset_fn
            .Eval(/*params=*/{},
                  /*args=*/
                  {InternalValue::Array(
                       input_type, {Value::String("a"), Value::String("A")},
                       InternalValue::kIgnoresOrder),
                   Value::String("a"), array_find_mode_first},
                  &context_offset)
            .status());
    EXPECT_FALSE(context_offset.IsDeterministicOutput());
  }

  // If no matching element is found, the result is deterministic.
  {
    EvaluationContext context_offset{/*options=*/{}};
    EXPECT_TRUE(context_offset.IsDeterministicOutput());
    ZETASQL_EXPECT_OK(
        offset_fn
            .Eval(/*params=*/{},
                  /*args=*/
                  {InternalValue::Array(
                       input_type, {Value::String("a"), Value::String("A")},
                       InternalValue::kIgnoresOrder),
                   Value::String("b"), array_find_mode_first},
                  &context_offset)
            .status());
    EXPECT_TRUE(context_offset.IsDeterministicOutput());
  }

  // If input array has length smaller or equal to 1, the result is
  // deterministic.
  {
    EvaluationContext context_offset{/*options=*/{}};
    EXPECT_TRUE(context_offset.IsDeterministicOutput());
    ZETASQL_EXPECT_OK(offset_fn
                  .Eval(/*params=*/{},
                        /*args=*/
                        {InternalValue::Array(input_type, {Value::String("a")},
                                              InternalValue::kIgnoresOrder),
                         Value::String("A"), array_find_mode_first},
                        &context_offset)
                  .status());
    EXPECT_TRUE(context_offset.IsDeterministicOutput());
  }

  // ARRAY_OFFSETS
  ZETASQL_ASSERT_OK_AND_ASSIGN(CollatorList collator_list_offsets,
                       MakeCollatorList(collation_list));

  ArrayFindFunctions offsets_fn(FunctionKind::kArrayOffsets, int64_array_type,
                                std::move(collator_list_offsets));
  {
    EvaluationContext context_offsets{/*options=*/{}};
    EXPECT_TRUE(context_offsets.IsDeterministicOutput());
    ZETASQL_EXPECT_OK(
        offsets_fn
            .Eval(/*params=*/{},
                  /*args=*/
                  {InternalValue::Array(
                       input_type, {Value::String("a"), Value::String("A")},
                       InternalValue::kIgnoresOrder),
                   Value::String("a")},
                  &context_offsets)
            .status());
    EXPECT_FALSE(context_offsets.IsDeterministicOutput());
  }

  // However, if no matching element is found, the result is deterministic.
  {
    EvaluationContext context_offsets{/*options=*/{}};
    EXPECT_TRUE(context_offsets.IsDeterministicOutput());
    ZETASQL_EXPECT_OK(
        offsets_fn
            .Eval(/*params=*/{},
                  /*args=*/
                  {InternalValue::Array(
                       input_type, {Value::String("a"), Value::String("A")},
                       InternalValue::kIgnoresOrder),
                   Value::String("b")},
                  &context_offsets)
            .status());
    EXPECT_TRUE(context_offsets.IsDeterministicOutput());
  }

  // If input array has length smaller or equal to 1, the result is
  // deterministic.
  {
    EvaluationContext context_offsets{/*options=*/{}};
    EXPECT_TRUE(context_offsets.IsDeterministicOutput());
    ZETASQL_EXPECT_OK(offsets_fn
                  .Eval(/*params=*/{},
                        /*args=*/
                        {InternalValue::Array(input_type, {Value::String("a")},
                                              InternalValue::kIgnoresOrder),
                         Value::String("A")},
                        &context_offsets)
                  .status());
    EXPECT_TRUE(context_offsets.IsDeterministicOutput());
  }
}

TEST(NonDeterministicEvaluationContextTest,
     ArrayFindDistinguishableTiesStringTest) {
  // Array input with collated STRING type element introduces indeterminism for
  // ARRAY_FIND(array<T>, T [, mode]) -> T.
  // Only when the number of ties is larger than 1, will the indeterministic
  // mark be set.

  // This setup overwrites the CollatorRegistration::CreateFromCollationNameFn
  // for current process, so that case insensitive collation name can be used.
  internal::EnableFullEvaluatorFeatures();

  TypeFactory factory;
  const ArrayType* input_type;
  const Type* element_type = factory.get_string();
  ZETASQL_EXPECT_OK(factory.MakeArrayType(element_type, &input_type));

  std::vector<ResolvedCollation> collation_list = {
      ResolvedCollation::MakeScalar("unicode:ci")};
  const Value array_find_mode_first =
      Value::Enum(types::ArrayFindModeEnumType(), 1);
  ZETASQL_ASSERT_OK_AND_ASSIGN(CollatorList collator_list,
                       MakeCollatorList(collation_list));
  ArrayFindFunctions find_fn_with_collation(
      FunctionKind::kArrayFind, factory.get_string(), std::move(collator_list));

  // ARRAY_FIND with one found element
  {
    EvaluationContext context1{/*options=*/{}};
    EXPECT_TRUE(context1.IsDeterministicOutput());
    ZETASQL_EXPECT_OK(
        find_fn_with_collation
            .Eval(/*params=*/{},
                  /*args=*/
                  {InternalValue::Array(
                       input_type, {Value::String("b"), Value::String("A")},
                       InternalValue::kIgnoresOrder),
                   Value::String("a"), array_find_mode_first},
                  &context1)
            .status());
    EXPECT_TRUE(context1.IsDeterministicOutput());
  }

  // ARRAY_FIND with two found elements that are distinguishable ties
  {
    EvaluationContext context2{/*options=*/{}};
    EXPECT_TRUE(context2.IsDeterministicOutput());
    ZETASQL_EXPECT_OK(
        find_fn_with_collation
            .Eval(/*params=*/{},
                  /*args=*/
                  {InternalValue::Array(input_type,
                                        {Value::String("b"), Value::String("a"),
                                         Value::String("A")},
                                        InternalValue::kIgnoresOrder),
                   Value::String("a"), array_find_mode_first},
                  &context2)
            .status());
    EXPECT_FALSE(context2.IsDeterministicOutput());
  }

  // ARRAY_FIND with lambda argument on case sensitive inputs.
  {
    std::unique_ptr<ConstExpr> lambda_body =
        ConstExpr::Create(Value::Bool(true)).value();
    std::vector<VariableId> lambda_arg_vars = {VariableId("e")};
    std::unique_ptr<InlineLambdaExpr> lambda_algebra =
        InlineLambdaExpr::Create(lambda_arg_vars, std::move(lambda_body));

    ArrayFindFunctions find_fn_with_lambda(
        FunctionKind::kArrayFind, factory.get_string(),
        /*collator_list=*/{}, lambda_algebra.get());
    EvaluationContext context{/*options=*/{}};
    EXPECT_TRUE(context.IsDeterministicOutput());
    ZETASQL_EXPECT_OK(
        find_fn_with_lambda
            .Eval(/*params=*/{},
                  /*args=*/
                  {InternalValue::Array(input_type,
                                        {Value::String("b"), Value::String("a"),
                                         Value::String("A")},
                                        InternalValue::kIgnoresOrder),
                   array_find_mode_first},
                  &context)
            .status());
    EXPECT_FALSE(context.IsDeterministicOutput());
  }
}

}  // namespace zetasql
