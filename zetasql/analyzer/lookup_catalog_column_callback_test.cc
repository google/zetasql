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

#include <memory>
#include <string>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOk;
using ::zetasql_base::testing::StatusIs;

class LookupCatalogColumnCallbackTest : public ::testing::Test {
 public:
  void SetUp() override {
    catalog_.AddTable(&table_);
    catalog_.AddBuiltinFunctions(
        BuiltinFunctionOptions::AllReleasedFunctions());
  }

  absl::Status AnalyzeExpression(absl::string_view sql) {
    return AnalyzeExpressionForAssignmentToType(
        sql, options_, &catalog_, &type_factory_, types::Int64Type(), &output_);
  }

  absl::Status AnalyzeStatement(absl::string_view sql) {
    return zetasql::AnalyzeStatement(sql, options_, &catalog_, &type_factory_,
                                       &output_);
  }

  std::unique_ptr<AnnotationMap> MakeAnnotation() {
    std::unique_ptr<AnnotationMap> m =
        AnnotationMap::Create(types::Int64Type());
    m->SetAnnotation(1234, SimpleValue::String("myannotation"));
    return m;
  }

  TypeFactory type_factory_;
  std::unique_ptr<AnnotationMap> column_annotation_ = MakeAnnotation();
  SimpleColumn column_{
      "mytable", "mycolumn", {types::Int64Type(), column_annotation_.get()}};
  SimpleTable table_{"mytable", {&column_}};
  SimpleCatalog catalog_{"mycatalog", &type_factory_};

  AnalyzerOptions options_;
  std::unique_ptr<const AnalyzerOutput> output_;
};

TEST_F(LookupCatalogColumnCallbackTest, BaselineErrorWhenNoColumnDefined) {
  EXPECT_THAT(AnalyzeExpression("mycolumn + 1"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Unrecognized name: mycolumn")));

  EXPECT_THAT(AnalyzeStatement("SELECT mycolumn + 1"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Unrecognized name: mycolumn")));
}

TEST_F(LookupCatalogColumnCallbackTest,
       SameErrorAsBaselineWhenCatalogColumnCallbackReturnsNullptr) {
  options_.SetLookupCatalogColumnCallback(
      [](absl::string_view column) -> absl::StatusOr<const Column*> {
        return nullptr;
      });

  EXPECT_THAT(AnalyzeExpression("mycolumn + 1"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Unrecognized name: mycolumn")));

  EXPECT_THAT(AnalyzeStatement("SELECT mycolumn + 1"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Unrecognized name: mycolumn")));
}

TEST_F(LookupCatalogColumnCallbackTest,
       ErrorWhenLookupCatalogColumnReturnsError) {
  options_.SetLookupCatalogColumnCallback(
      [](absl::string_view column) -> absl::StatusOr<const Column*> {
        return absl::NotFoundError("error column-not-found: mycolumn");
      });

  EXPECT_THAT(AnalyzeExpression("mycolumn + 1"),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr("error column-not-found: mycolumn")));

  EXPECT_THAT(AnalyzeStatement("SELECT mycolumn + 1"),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr("error column-not-found: mycolumn")));
}

TEST_F(LookupCatalogColumnCallbackTest, SuccessfulLookupTest) {
  options_.SetLookupCatalogColumnCallback(
      [&](absl::string_view column) -> absl::StatusOr<const Column*> {
        EXPECT_THAT(column, testing::StrCaseEq("mycolumn"));
        return &column_;
      });

  EXPECT_THAT(AnalyzeExpression("MycOluMn + 1"), IsOk());

  const ResolvedCatalogColumnRef& resolved_expression =
      *output_->resolved_expr()
           ->GetAs<ResolvedFunctionCallBase>()
           ->argument_list(0)
           ->GetAs<ResolvedCatalogColumnRef>();

  // Case of the column name will match the column returned by the callback.
  EXPECT_EQ(resolved_expression.column()->Name(), "mycolumn");
  // Types will match.
  EXPECT_TRUE(
      resolved_expression.column()->GetType()->Equals(types::Int64Type()))
      << resolved_expression.column()->GetType()->DebugString();
  // Column annotation will be propagated.
  EXPECT_TRUE(resolved_expression.column()->GetTypeAnnotationMap()->Equals(
      *column_annotation_))
      << resolved_expression.column()->GetTypeAnnotationMap()->DebugString();

  EXPECT_THAT(AnalyzeStatement("SELECT MycOluMn + 1"), IsOk());

  const ResolvedCatalogColumnRef& resolved_statement =
      *output_->resolved_statement()
           ->GetAs<ResolvedQueryStmt>()
           ->query()
           ->GetAs<ResolvedProjectScan>()
           ->expr_list(0)
           ->GetAs<ResolvedComputedColumn>()
           ->expr()
           ->GetAs<ResolvedFunctionCallBase>()
           ->argument_list(0)
           ->GetAs<ResolvedCatalogColumnRef>();

  // Case of the column name will match the column returned by the callback.
  EXPECT_EQ(resolved_statement.column()->Name(), "mycolumn");
  // Types will match.
  EXPECT_TRUE(
      resolved_statement.column()->GetType()->Equals(types::Int64Type()))
      << resolved_statement.column()->GetType()->DebugString();
  // Column annotation will be propagated.
  EXPECT_TRUE(resolved_statement.column()->GetTypeAnnotationMap()->Equals(
      *column_annotation_))
      << resolved_statement.column()->GetTypeAnnotationMap()->DebugString();
}

}  // namespace
}  // namespace zetasql
