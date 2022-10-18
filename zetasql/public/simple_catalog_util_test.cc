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

#include "zetasql/public/simple_catalog_util.h"

#include <memory>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/simple_catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::Not;
using ::zetasql_base::testing::IsOk;

namespace zetasql {

TEST(SimpleCatalogUtilTest, AddFunctionFromCreateFunctionTest) {
  SimpleCatalog simple("simple");
  AnalyzerOptions analyzer_options;
  std::unique_ptr<const AnalyzerOutput> analyzer_output;

  // Invalid analyzer options
  EXPECT_THAT(
      AddFunctionFromCreateFunction("CREATE TEMP FUNCTION Basic() AS (1)",
                                    analyzer_options, analyzer_output, simple),
      Not(IsOk()));

  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_FUNCTION_STMT);
  ZETASQL_EXPECT_OK(AddFunctionFromCreateFunction("CREATE TEMP FUNCTION Basic() AS (1)",
                                          analyzer_options, analyzer_output,
                                          simple));

  // Duplicate
  EXPECT_THAT(
      AddFunctionFromCreateFunction("CREATE TEMP FUNCTION Basic() AS (1)",
                                    analyzer_options, analyzer_output, simple),
      Not(IsOk()));

  // Invalid persistent function.
  EXPECT_THAT(
      AddFunctionFromCreateFunction("CREATE FUNCTION Persistent() AS (1)",
                                    analyzer_options, analyzer_output, simple),
      Not(IsOk()));
  ZETASQL_EXPECT_OK(AddFunctionFromCreateFunction("CREATE FUNCTION Persistent() AS (1)",
                                          analyzer_options, analyzer_output,
                                          simple,
                                          /*allow_persistent_function=*/true));

  // Analysis failure
  EXPECT_THAT(AddFunctionFromCreateFunction(
                  "CREATE TEMP FUNCTION Template(arg ANY TYPE) AS (arg)",
                  analyzer_options, analyzer_output, simple),
              Not(IsOk()));
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_TEMPLATE_FUNCTIONS);
  ZETASQL_EXPECT_OK(AddFunctionFromCreateFunction(
      "CREATE TEMP FUNCTION Template(arg ANY TYPE) AS (arg)", analyzer_options,
      analyzer_output, simple));
}

}  // namespace zetasql
