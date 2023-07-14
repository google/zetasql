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
#include <optional>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
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
  EXPECT_THAT(AddFunctionFromCreateFunction(
                  "CREATE TEMP FUNCTION Basic() AS (1)", analyzer_options,
                  /*allow_persistent_function=*/false,
                  /*function_options=*/std::nullopt, analyzer_output, simple),
              Not(IsOk()));

  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_FUNCTION_STMT);
  ZETASQL_EXPECT_OK(AddFunctionFromCreateFunction(
      "CREATE TEMP FUNCTION Basic() AS (1)", analyzer_options,
      /*allow_persistent_function=*/false, /*function_options=*/std::nullopt,
      analyzer_output, simple));

  // Duplicate
  EXPECT_THAT(AddFunctionFromCreateFunction(
                  "CREATE TEMP FUNCTION Basic() AS (1)", analyzer_options,
                  /*allow_persistent_function=*/false,
                  /*function_options=*/std::nullopt, analyzer_output, simple),
              Not(IsOk()));

  // Invalid persistent function.
  EXPECT_THAT(AddFunctionFromCreateFunction(
                  "CREATE FUNCTION Persistent() AS (1)", analyzer_options,
                  /*allow_persistent_function=*/false,
                  /*function_options=*/std::nullopt, analyzer_output, simple),
              Not(IsOk()));
  ZETASQL_EXPECT_OK(AddFunctionFromCreateFunction(
      "CREATE FUNCTION Persistent() AS (1)", analyzer_options,
      /*allow_persistent_function=*/true, /*function_options=*/std::nullopt,
      analyzer_output, simple));

  // Analysis failure
  EXPECT_THAT(AddFunctionFromCreateFunction(
                  "CREATE TEMP FUNCTION Template(arg ANY TYPE) AS (arg)",
                  analyzer_options, /*allow_persistent_function=*/false,
                  /*function_options=*/std::nullopt, analyzer_output, simple),
              Not(IsOk()));
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_TEMPLATE_FUNCTIONS);
  ZETASQL_EXPECT_OK(AddFunctionFromCreateFunction(
      "CREATE TEMP FUNCTION Template(arg ANY TYPE) AS (arg)", analyzer_options,
      /*allow_persistent_function=*/false, /*function_options=*/std::nullopt,
      analyzer_output, simple));
}

TEST(SimpleCatalogUtilTest, MakeFunctionFromCreateFunctionBasic) {
  SimpleCatalog catalog("simple");
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_FUNCTION_STMT);

  TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_EXPECT_OK(AnalyzeStatement("CREATE TEMP FUNCTION Basic() AS (1)",
                             analyzer_options, &catalog, &type_factory,
                             &analyzer_output));

  ASSERT_TRUE(
      analyzer_output->resolved_statement()->Is<ResolvedCreateFunctionStmt>());

  const ResolvedCreateFunctionStmt* create_function_stmt =
      analyzer_output->resolved_statement()
          ->GetAs<ResolvedCreateFunctionStmt>();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Function> function,
      MakeFunctionFromCreateFunction(
          *create_function_stmt, FunctionOptions().set_is_deprecated(true)));
  EXPECT_EQ(function->Name(), "Basic");
  EXPECT_TRUE(function->IsDeprecated());
  EXPECT_EQ(function->mode(), Function::SCALAR);
  EXPECT_EQ(function->NumSignatures(), 1);

  // Use string comparison as a proxy for signature equality.
  EXPECT_EQ(FunctionSignature::SignaturesToString(function->signatures()),
            FunctionSignature::SignaturesToString(
                {create_function_stmt->signature()}));
}

TEST(SimpleCatalogUtilTest, MakeFunctionFromCreateFunctionAgg) {
  SimpleCatalog catalog("simple");
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->AddSupportedStatementKind(
      RESOLVED_CREATE_FUNCTION_STMT);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_CREATE_AGGREGATE_FUNCTION);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_TEMPLATE_FUNCTIONS);

  TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_EXPECT_OK(AnalyzeStatement(
      "CREATE AGGREGATE FUNCTION Path.to.F(x any type) AS (sum(x))",
      analyzer_options, &catalog, &type_factory, &analyzer_output));

  ASSERT_TRUE(
      analyzer_output->resolved_statement()->Is<ResolvedCreateFunctionStmt>());

  const ResolvedCreateFunctionStmt* create_function_stmt =
      analyzer_output->resolved_statement()
          ->GetAs<ResolvedCreateFunctionStmt>();

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Function> function,
                       MakeFunctionFromCreateFunction(*create_function_stmt));
  EXPECT_EQ(function->Name(), "F");
  EXPECT_THAT(function->FunctionNamePath(),
              testing::ElementsAre("Path", "to", "F"));
  EXPECT_EQ(function->mode(), Function::AGGREGATE);
  EXPECT_EQ(function->NumSignatures(), 1);

  // Use string comparison as a proxy for signature equality.
  EXPECT_EQ(FunctionSignature::SignaturesToString(function->signatures()),
            FunctionSignature::SignaturesToString(
                {create_function_stmt->signature()}));
}

}  // namespace zetasql
