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

#include "zetasql/public/functions/arithmetics.h"

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <cstdint>
#include "absl/status/status.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

using zetasql_base::testing::StatusIs;

template <typename T>
bool IsNanImpl(T val) {
  return std::isnan(val);
}

template <>
bool IsNanImpl<NumericValue>(NumericValue val) {
  return false;
}

template <>
bool IsNanImpl<BigNumericValue>(BigNumericValue val) {
  return false;
}

template <typename ArgType, typename ResultType = ArgType>
void TestBinaryFunction(const QueryParamsWithResult& param,
                        bool (*function)(ArgType, ArgType, ResultType*,
                                         absl::Status* error)) {
  ZETASQL_CHECK_EQ(2, param.num_params());
  const Value& input1 = param.param(0);
  const Value& input2 = param.param(1);
  if (!input1.type()->Equals(input2.type())) {
    // These tests require the types are equal.  The compliance tests cover
    // cases where the expression requires coercion.
    return;
  }
  const Value& expected = param.results().begin()->second.result;
  const absl::Status& expected_status =
      param.results().begin()->second.status;
  if (input1.is_null() || input2.is_null()) {
    // The function libraries do not support null values, those cases
    // are handled in the compliance tests.
    return;
  }

  ResultType out = ResultType();
  absl::Status status;  // actual status
  function(input1.Get<ArgType>(), input2.Get<ArgType>(), &out, &status);
  if (expected_status.ok()) {
    EXPECT_EQ(absl::OkStatus(), status) << "op1: " << input1.DebugString()
                                        << ", op2: " << input2.DebugString();
    ASSERT_EQ(expected.type_kind(), Value::MakeNull<ResultType>().type_kind())
        << "expected result type: "
        << Type::TypeKindToString(expected.type_kind(), PRODUCT_INTERNAL)
        << ", actual result type: "
        << Type::TypeKindToString(Value::MakeNull<ResultType>().type_kind(),
                                  PRODUCT_INTERNAL);
    if (std::numeric_limits<ResultType>::is_integer) {
      EXPECT_EQ(expected.Get<ResultType>(), out);
    } else if (IsNanImpl(expected.Get<ResultType>())) {
      EXPECT_TRUE(IsNanImpl(out)) << out;
    } else {
      EXPECT_EQ(expected.Get<ResultType>(), out);
    }
  } else {
    EXPECT_THAT(
        status,
        StatusIs(absl::StatusCode::kOutOfRange,
                 ::testing::AnyOf(::testing::HasSubstr(" overflow: "),
                                  ::testing::HasSubstr("division by zero: "))))
        << "Unexpected value: " << out;
  }
}

typedef testing::TestWithParam<QueryParamsWithResult> AddTemplateTest;
TEST_P(AddTemplateTest, Testlib) {
  const QueryParamsWithResult& param = GetParam();

  switch (param.param(0).type_kind()) {
    case TYPE_INT64:
      TestBinaryFunction(param, &Add<int64_t>);
      break;
    case TYPE_UINT64:
      TestBinaryFunction(param, &Add<uint64_t>);
      break;
    case TYPE_DOUBLE:
      TestBinaryFunction(param, &Add<double>);
      break;
    case TYPE_NUMERIC:
      TestBinaryFunction(param, &Add<NumericValue>);
      break;
    case TYPE_BIGNUMERIC:
      TestBinaryFunction(param, &Add<BigNumericValue>);
      break;
    default:
      ZETASQL_LOG(FATAL) << "This op is not tested here: " << param;
  }
}

INSTANTIATE_TEST_SUITE_P(Add, AddTemplateTest,
                         testing::ValuesIn(GetFunctionTestsAdd()));

typedef testing::TestWithParam<QueryParamsWithResult> SubtractTemplateTest;
TEST_P(SubtractTemplateTest, Testlib) {
  const QueryParamsWithResult& param = GetParam();
  switch (param.param(0).type_kind()) {
    case TYPE_INT64:
      TestBinaryFunction(param, &Subtract<int64_t>);
      break;
    case TYPE_UINT64:
      TestBinaryFunction(param, &Subtract<uint64_t, int64_t>);
      break;
    case TYPE_DOUBLE:
      TestBinaryFunction(param, &Subtract<double>);
      break;
    case TYPE_NUMERIC:
      TestBinaryFunction(param, &Subtract<NumericValue>);
      break;
    case TYPE_BIGNUMERIC:
      TestBinaryFunction(param, &Subtract<BigNumericValue>);
      break;
    default:
      ZETASQL_LOG(FATAL) << "This op is not tested here: " << param;
  }
}

INSTANTIATE_TEST_SUITE_P(Subtract, SubtractTemplateTest,
                         testing::ValuesIn(GetFunctionTestsSubtract()));

typedef testing::TestWithParam<QueryParamsWithResult> MultiplyTemplateTest;
TEST_P(MultiplyTemplateTest, Testlib) {
  const QueryParamsWithResult& param = GetParam();
  switch (param.param(0).type_kind()) {
    case TYPE_INT64:
      TestBinaryFunction(param, &Multiply<int64_t>);
      break;
    case TYPE_UINT64:
      TestBinaryFunction(param, &Multiply<uint64_t>);
      break;
    case TYPE_DOUBLE:
      TestBinaryFunction(param, &Multiply<double>);
      break;
    case TYPE_NUMERIC:
      TestBinaryFunction(param, &Multiply<NumericValue>);
      break;
    case TYPE_BIGNUMERIC:
      TestBinaryFunction(param, &Multiply<BigNumericValue>);
      break;
    default:
      ZETASQL_LOG(FATAL) << "This op is not tested here: " << param;
  }
}

INSTANTIATE_TEST_SUITE_P(Multiply, MultiplyTemplateTest,
                         testing::ValuesIn(GetFunctionTestsMultiply()));

typedef testing::TestWithParam<QueryParamsWithResult> DivideTemplateTest;
TEST_P(DivideTemplateTest, Testlib) {
  const QueryParamsWithResult& param = GetParam();
  switch (param.param(0).type_kind()) {
    case TYPE_DOUBLE:
      TestBinaryFunction(param, &Divide<double>);
      break;
    case TYPE_NUMERIC:
      TestBinaryFunction(param, &Divide<NumericValue>);
      break;
    case TYPE_BIGNUMERIC:
      TestBinaryFunction(param, &Divide<BigNumericValue>);
      break;
    default:
      ZETASQL_LOG(FATAL) << "This op is not tested here: " << param;
  }
}

INSTANTIATE_TEST_SUITE_P(Divide, DivideTemplateTest,
                         testing::ValuesIn(GetFunctionTestsDivide()));

typedef testing::TestWithParam<QueryParamsWithResult> DivTemplateTest;
TEST_P(DivTemplateTest, Testlib) {
  const QueryParamsWithResult& param = GetParam();
  switch (param.param(0).type_kind()) {
    case TYPE_INT64:
      TestBinaryFunction(param, &Divide<int64_t>);
      break;
    case TYPE_UINT64:
      TestBinaryFunction(param, &Divide<uint64_t>);
      break;
    case TYPE_NUMERIC:
      TestBinaryFunction(param, &DivideToIntegralValue<NumericValue>);
      break;
    case TYPE_BIGNUMERIC:
      TestBinaryFunction(param, &DivideToIntegralValue<BigNumericValue>);
      break;
    default:
      ZETASQL_LOG(FATAL) << "This op is not tested here: " << param;
  }
}

INSTANTIATE_TEST_SUITE_P(Modulo, DivTemplateTest,
                         testing::ValuesIn(GetFunctionTestsDiv()));

typedef testing::TestWithParam<QueryParamsWithResult> ModuloTemplateTest;
TEST_P(ModuloTemplateTest, Testlib) {
  const QueryParamsWithResult& param = GetParam();
  switch (param.param(0).type_kind()) {
    case TYPE_INT64:
      TestBinaryFunction(param, &Modulo<int64_t>);
      break;
    case TYPE_UINT64:
      TestBinaryFunction(param, &Modulo<uint64_t>);
      break;
    case TYPE_NUMERIC:
      TestBinaryFunction(param, &Modulo<NumericValue>);
      break;
    case TYPE_BIGNUMERIC:
      TestBinaryFunction(param, &Modulo<BigNumericValue>);
      break;
    default:
      ZETASQL_LOG(FATAL) << "This op is not tested here: " << param;
  }
}

INSTANTIATE_TEST_SUITE_P(Modulo, ModuloTemplateTest,
                         testing::ValuesIn(GetFunctionTestsModulo()));

template <typename InType, typename OutType>
void TestUnaryMinus(const QueryParamsWithResult& param) {
  ZETASQL_CHECK_EQ(1, param.num_params());
  const Value& input1 = param.param(0);
  const Value& expected = param.results().begin()->second.result;
  const absl::Status& expected_status =
      param.results().begin()->second.status;
  if (input1.is_null()) {
    return;
  }

  OutType out = OutType();
  absl::Status status;  // actual status
  UnaryMinus<InType, OutType>(input1.Get<InType>(), &out, &status);
  if (expected_status.ok()) {
    EXPECT_EQ(absl::OkStatus(), status);
    EXPECT_EQ(expected, Value::Make(out));
  } else {
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kOutOfRange,
                                 ::testing::HasSubstr(" overflow: ")))
        << "Unexpected value: " << out;
  }
}

typedef testing::TestWithParam<QueryParamsWithResult> UnaryMinusTemplateTest;
TEST_P(UnaryMinusTemplateTest, Testlib) {
  const QueryParamsWithResult& param = GetParam();
  switch (param.param(0).type_kind()) {
    case TYPE_INT32:
      TestUnaryMinus<int32_t, int32_t>(param);
      break;
    case TYPE_INT64:
      TestUnaryMinus<int64_t, int64_t>(param);
      break;
    case TYPE_FLOAT:
      TestUnaryMinus<float, float>(param);
      break;
    case TYPE_DOUBLE:
      TestUnaryMinus<double, double>(param);
      break;
    case TYPE_NUMERIC:
      TestUnaryMinus<NumericValue, NumericValue>(param);
      break;
    case TYPE_BIGNUMERIC:
      TestUnaryMinus<BigNumericValue, BigNumericValue>(param);
      break;
    default:
      ZETASQL_LOG(FATAL) << "This op is not tested here: " << param;
  }
}

INSTANTIATE_TEST_SUITE_P(UnaryMinus, UnaryMinusTemplateTest,
                         testing::ValuesIn(GetFunctionTestsUnaryMinus()));

// Tests functions over "long double". The template test functions
// TestBinaryFunction() and TestUnaryMinus() cannot be used because
// "long double" is not a valid Value type. Since the functions are exact
// replicas of their counterparts using "double", their correctness should be
// straigtforward to see. Here we add some trivial test cases with the goal
// to exercise the code, not to be thorough in testing all possible values.
TEST(LongDouble, Simple) {
  const long double a = 1.1, b = 2.2, zero = 0.0, sum = 3.3, diff = -1.1,
                    product = 2.42, ratio = 0.5, neg = -1.1;
  long double result = 0;
  absl::Status status;

  status = absl::OkStatus();
  Add(a, b, &result, &status);
  ZETASQL_EXPECT_OK(status);
  EXPECT_DOUBLE_EQ(result, sum);

  status = absl::OkStatus();
  Subtract(a, b, &result, &status);
  ZETASQL_EXPECT_OK(status);
  EXPECT_DOUBLE_EQ(result, diff);

  status = absl::OkStatus();
  Multiply(a, b, &result, &status);
  ZETASQL_EXPECT_OK(status);
  EXPECT_DOUBLE_EQ(result, product);

  status = absl::OkStatus();
  Divide(a, b, &result, &status);
  ZETASQL_EXPECT_OK(status);
  EXPECT_DOUBLE_EQ(result, ratio);

  status = absl::OkStatus();
  Divide(a, zero, &result, &status);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kOutOfRange,
                               ::testing::HasSubstr("division by zero: ")));

  status = absl::OkStatus();
  UnaryMinus(a, &result, &status);
  ZETASQL_EXPECT_OK(status);
  EXPECT_DOUBLE_EQ(result, neg);
}

}  // namespace functions
}  // namespace zetasql
