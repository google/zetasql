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

#include "zetasql/public/measure_expression.h"

#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
using ::absl_testing::StatusIs;
using ::testing::HasSubstr;
using ExpressionKind =
    ::zetasql::Column::ExpressionAttributes::ExpressionKind;

class MeasureExpressionTest : public ::testing::Test {
 public:
  MeasureExpressionTest()
      : type_factory_(), catalog_("measure_catalog", &type_factory_) {
    catalog_.AddBuiltinFunctions(
        BuiltinFunctionOptions::AllReleasedFunctions());
  }

  absl::StatusOr<std::unique_ptr<SimpleTable>> CreateTableWithMeasureColumn(
      absl::string_view table_name,
      std::vector<std::pair<std::string, const Type*>> columns,
      absl::string_view measure_name, absl::string_view measure_expr) {
    auto table = std::make_unique<SimpleTable>(table_name, columns);
    AnalyzerOptions analyzer_options;
    analyzer_options.mutable_language()->EnableLanguageFeature(
        FEATURE_V_1_4_MULTILEVEL_AGGREGATION);
    ZETASQL_ASSIGN_OR_RETURN(
        std::vector<std::unique_ptr<const AnalyzerOutput>> analyzer_outputs,
        AddMeasureColumnsToTable(*table, {{measure_name, measure_expr}},
                                 type_factory_, catalog_, analyzer_options));
    analyzer_outputs_.insert(analyzer_outputs_.end(),
                             std::make_move_iterator(analyzer_outputs.begin()),
                             std::make_move_iterator(analyzer_outputs.end()));
    return std::move(table);
  }

 protected:
  TypeFactory type_factory_;
  SimpleCatalog catalog_;
  std::vector<std::unique_ptr<const AnalyzerOutput>> analyzer_outputs_;
};

TEST_F(MeasureExpressionTest, MeasureExpression) {
  const std::string measure_name = "total_value";
  const std::string measure_expr = "SUM(value)";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SimpleTable> table,
      CreateTableWithMeasureColumn("table",
                                   {{"key", type_factory_.get_int64()},
                                    {"value", type_factory_.get_int64()}},
                                   measure_name, measure_expr));
  const Column* measure_column = table->FindColumnByName(measure_name);
  ASSERT_NE(measure_column, nullptr);
  EXPECT_TRUE(measure_column->GetType()->IsMeasureType());
  EXPECT_TRUE(measure_column->HasMeasureExpression());
  EXPECT_EQ(measure_column->GetExpression()->GetExpressionKind(),
            ExpressionKind::MEASURE_EXPRESSION);
  EXPECT_EQ(measure_column->GetExpression()->GetExpressionString(),
            measure_expr);
  EXPECT_EQ(
      measure_column->GetExpression()->GetResolvedExpression()->node_kind(),
      ResolvedAggregateFunctionCall::TYPE);
}

TEST_F(MeasureExpressionTest, MeasureExpressionWithGrainLock) {
  const std::string measure_name = "total_value";
  const std::string measure_expr = "SUM(ANY_VALUE(value) GROUP BY key)";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SimpleTable> table,
      CreateTableWithMeasureColumn("table",
                                   {{"key", type_factory_.get_int64()},
                                    {"value", type_factory_.get_int64()}},
                                   measure_name, measure_expr));
  const Column* measure_column = table->FindColumnByName(measure_name);
  ASSERT_NE(measure_column, nullptr);
  EXPECT_TRUE(measure_column->GetType()->IsMeasureType());
  EXPECT_TRUE(measure_column->HasMeasureExpression());
  EXPECT_EQ(measure_column->GetExpression()->GetExpressionKind(),
            ExpressionKind::MEASURE_EXPRESSION);
  EXPECT_EQ(measure_column->GetExpression()->GetExpressionString(),
            measure_expr);
  EXPECT_EQ(
      measure_column->GetExpression()->GetResolvedExpression()->node_kind(),
      ResolvedAggregateFunctionCall::TYPE);
}

// TODO: support measure with grain-lock on repeated field.
TEST_F(MeasureExpressionTest, MeasureExpressionOnRepeatedField) {
  const std::string measure_name = "total_value";
  std::string measure_expr = R"sql(SUM(v) WITH GROUP ROWS(
                                    SELECT DISTINCT v
                                    FROM GROUP_ROWS(), UNNEST(values) AS v
                             ))sql";
  const Type* array_type = nullptr;
  ZETASQL_ASSERT_OK(
      type_factory_.MakeArrayType(type_factory_.get_int64(), &array_type));
  EXPECT_THAT(
      CreateTableWithMeasureColumn(
          "table", {{"key", type_factory_.get_int64()}, {"values", array_type}},
          measure_name, measure_expr),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("WITH GROUP ROWS is not supported")));
}

TEST_F(MeasureExpressionTest, InvalidScalarExpr) {
  EXPECT_THAT(
      CreateTableWithMeasureColumn("table",
                                   {{"key", type_factory_.get_int64()},
                                    {"value", type_factory_.get_int64()}},
                                   "total_value", "value + 1"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Measure expression must be an aggregate function call")));
}

TEST_F(MeasureExpressionTest, InvalidConstLiteral) {
  EXPECT_THAT(
      CreateTableWithMeasureColumn("table",
                                   {{"key", type_factory_.get_int64()},
                                    {"value", type_factory_.get_int64()}},
                                   "constant_value", "1"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Measure expression must be an aggregate function call")));
}

TEST_F(MeasureExpressionTest, CompositeMeasure) {
  std::string measure_name = "average_value";
  std::string measure_expr = "SUM(value) / COUNT(1) * 100";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SimpleTable> table,
      CreateTableWithMeasureColumn("table",
                                   {{"key", type_factory_.get_int64()},
                                    {"value", type_factory_.get_int64()}},
                                   measure_name, measure_expr));
  const Column* measure_column = table->FindColumnByName(measure_name);
  ASSERT_NE(measure_column, nullptr);
  EXPECT_TRUE(measure_column->GetType()->IsMeasureType());
  EXPECT_TRUE(measure_column->HasMeasureExpression());
  EXPECT_EQ(measure_column->GetExpression()->GetExpressionKind(),
            ExpressionKind::MEASURE_EXPRESSION);
  EXPECT_EQ(measure_column->GetExpression()->GetExpressionString(),
            measure_expr);
}

TEST_F(MeasureExpressionTest, InvalidCompositeMeasureWithColumnReference) {
  EXPECT_THAT(
      CreateTableWithMeasureColumn("table",
                                   {{"key", type_factory_.get_int64()},
                                    {"value", type_factory_.get_int64()}},
                                   "invalid", "value + SUM(value)"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Measure expression must be an aggregate function call")));
}

TEST_F(MeasureExpressionTest, InvalidMeasureReferencingMeasure) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SimpleTable> table,
      CreateTableWithMeasureColumn("table",
                                   {{"key", type_factory_.get_int64()},
                                    {"value", type_factory_.get_int64()}},
                                   "measure_1", "SUM(value)"));
  AnalyzerOptions analyzer_options;
  EXPECT_THAT(
      AddMeasureColumnsToTable(*table, {{"measure_2", "SUM(measure_1)"}},
                               type_factory_, catalog_, analyzer_options),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Measure cannot reference another measure-type column")));
}

TEST_F(MeasureExpressionTest, InvalidMeasureReferencingNonExistingColumn) {
  std::vector<std::pair<std::string, const Type*>> columns = {
      {"key", type_factory_.get_int64()}, {"value", type_factory_.get_int64()}};
  auto table = std::make_unique<SimpleTable>("MeasureTable", columns);
  AnalyzerOptions analyzer_options;
  EXPECT_THAT(
      AddMeasureColumnsToTable(*table, {{"measure_col", "SUM(doesnt_exist)"}},
                               type_factory_, catalog_, analyzer_options),
      StatusIs(absl::StatusCode::kNotFound, HasSubstr("Column not found")));
}

}  // namespace zetasql
