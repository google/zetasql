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

#include "zetasql/common/constant_utils.h"

#include <memory>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/testdata/sample_catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace zetasql {

class IsConstantTest : public ::testing::Test {
 protected:
  IsConstantTest() {
    options_.mutable_language()->EnableMaximumLanguageFeaturesForDevelopment();
  }

  absl::Status Analyze(absl::string_view sql) {
    return AnalyzeExpression(sql, options_, catalog_.catalog(), &type_factory_,
                             &output_);
  }

  const ResolvedExpr* result() { return output_->resolved_expr(); }

  std::unique_ptr<const AnalyzerOutput> output_;
  TypeFactory type_factory_;
  SampleCatalog catalog_;
  AnalyzerOptions options_;
};

TEST_F(IsConstantTest, IsAnalysisConst) {
  ZETASQL_ASSERT_OK(Analyze("10000"));
  EXPECT_TRUE(IsAnalysisConstant(result()));

  ZETASQL_ASSERT_OK(Analyze("ARRAY<STRING>['a', 'b']"));
  EXPECT_TRUE(IsAnalysisConstant(result()));

  ZETASQL_ASSERT_OK(Analyze("10000 + 2 * 10"));
  EXPECT_FALSE(IsAnalysisConstant(result()));

  ZETASQL_ASSERT_OK(Analyze("STRUCT(1 AS key, 2 AS value)"));
  EXPECT_TRUE(IsAnalysisConstant(result()));

  ZETASQL_ASSERT_OK(
      Analyze("NEW `zetasql_test__.KeyValueStruct`('1' as key, 1 as value)"));
  EXPECT_TRUE(IsAnalysisConstant(result()));

  ZETASQL_ASSERT_OK(Analyze(
      "STRUCT(NEW `zetasql_test__.KeyValueStruct`('1' as key, 1 as value))"));
  EXPECT_TRUE(IsAnalysisConstant(result()));

  ZETASQL_ASSERT_OK(
      Analyze("STRUCT('1' as key, ARRAY_CONCAT([1, 2], [3, 4]) as value)"));
  EXPECT_FALSE(IsAnalysisConstant(result()));
}

}  // namespace zetasql
