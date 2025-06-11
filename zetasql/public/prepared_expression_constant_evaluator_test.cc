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

#include "zetasql/public/prepared_expression_constant_evaluator.h"

#include <memory>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/statusor.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

using ::zetasql_base::testing::IsOkAndHolds;

TEST(PreparedExpressionConstantEvaluator, Test) {
  AnalyzerOptions options;
  TypeFactory type_factory;
  SimpleCatalog catalog("catalog");
  catalog.AddBuiltinFunctions(
      zetasql::ZetaSQLBuiltinFunctionOptions::AllReleasedFunctions());
  FunctionCallBuilder fn_builder(options, catalog, type_factory);
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ResolvedExpr> is_null,
                       fn_builder.IsNull(MakeResolvedLiteral(
                           types::StringType(), Value::String("bar"),
                           /*has_explicit_type=*/true)));

  PreparedExpressionConstantEvaluator evaluator(/*options=*/{});
  EXPECT_THAT(evaluator.Evaluate(*is_null), IsOkAndHolds(Value::Bool(false)));
}

}  // namespace
}  // namespace zetasql
