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

#include "zetasql/public/functions/numeric.h"

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/testing/test_function.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"

namespace zetasql {
namespace functions {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

namespace {

// Tests a function that returns NumericValue or BigNumericValue.
template <typename OutType, typename FunctionType, class... Args>
void TestNumericFunction(FunctionType function,
                         const QueryParamsWithResult& param, Args... args) {
  OutType out;
  absl::Status status;
  bool success = function(args..., &out, &status);
  const QueryParamsWithResult::Result& expected =
      param.results().begin()->second;
  if (expected.status.ok()) {
    EXPECT_EQ(success, true);
    EXPECT_EQ(absl::OkStatus(), status);
    ASSERT_EQ(expected.result.Get<OutType>(), out)
        << "Expected: " << expected.result.Get<OutType>() << "\n"
        << "Actual: " << out << "\n";
  } else {
    EXPECT_EQ(success, false);
    EXPECT_THAT(status,
                StatusIs(expected.status.code(),
                         HasSubstr(std::string(expected.status.message()))));
  }
}

typedef testing::TestWithParam<FunctionTestCall> ParseNumericTemplateTest;
TEST_P(ParseNumericTemplateTest, Testlib) {
  const FunctionTestCall& param = GetParam();
  const std::string& function = param.function_name;
  const std::vector<Value>& args = param.params.params();

  for (const Value& arg : args) {
    // Ignore tests with null arguments.
    if (arg.is_null()) return;
  }

  if (function == "parse_numeric") {
    TestNumericFunction<NumericValue>(&ParseNumeric, param.params,
                                      args[0].string_value());
  } else if (function == "parse_bignumeric") {
    TestNumericFunction<BigNumericValue>(&ParseBigNumeric, param.params,
                                         args[0].string_value());
  }
}

INSTANTIATE_TEST_SUITE_P(Numeric, ParseNumericTemplateTest,
                         testing::ValuesIn(GetFunctionTestsParseNumeric()));

}  // namespace
}  // namespace functions
}  // namespace zetasql
