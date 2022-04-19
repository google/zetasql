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

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/reference_impl/tuple.h"
#include "gtest/gtest.h"

namespace zetasql {
namespace {

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

}  // namespace
}  // namespace zetasql
