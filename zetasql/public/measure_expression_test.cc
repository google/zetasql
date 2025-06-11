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

#include <algorithm>
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
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/templated_sql_tvf.h"
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

namespace {

class TablePermittingDuplicateColumnNames : public Table {
 public:
  explicit TablePermittingDuplicateColumnNames(std::string name)
      : name_(name) {};

  std::string Name() const override { return name_; }

  std::string FullName() const override { return Name(); }

  int NumColumns() const override { return static_cast<int>(columns_.size()); }

  const Column* GetColumn(int i) const override {
    if (i < 0 || i >= columns_.size()) {
      return nullptr;
    }
    return columns_[i].get();
  }

  const Column* FindColumnByName(const std::string& name) const override {
    auto column_matcher = [&name](const std::unique_ptr<const Column>& column) {
      return column->Name() == name;
    };
    auto it = std::find_if(columns_.cbegin(), columns_.cend(), column_matcher);
    if (it == columns_.cend()) {
      return nullptr;
    }
    const Column* column = it->get();
    // Check for duplicate column names.
    it = std::find_if(it + 1, columns_.cend(), column_matcher);
    if (it != columns_.cend()) {
      return nullptr;
    }
    return column;
  }

  void AddColumn(std::unique_ptr<const Column> column) {
    columns_.push_back(std::move(column));
  }

 private:
  std::string name_;
  std::vector<std::unique_ptr<const Column>> columns_;
};

}  // namespace

TEST(DuplicateColumnNamesTest,
     ErrorWhenAnalyzingMeasureOverTableWithDuplicateColumnNames) {
  TypeFactory type_factory;
  SimpleCatalog catalog("placeholder_catalog", &type_factory);
  auto table = std::make_unique<TablePermittingDuplicateColumnNames>(
      "placeholder_table");
  table->AddColumn(std::make_unique<SimpleColumn>(
      "placeholder_table", "key", type_factory.get_int64(), /*is_owned=*/true));
  table->AddColumn(std::make_unique<SimpleColumn>(
      "placeholder_table", "key", type_factory.get_int64(), /*is_owned=*/true));
  AnalyzerOptions analyzer_options;
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  EXPECT_THAT(
      AnalyzeMeasureExpression("SUM(key)", *table, catalog, type_factory,
                               analyzer_options, analyzer_output),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Measures cannot be defined on tables with duplicate "
                         "column names")));
}

class MeasureExpressionTest : public ::testing::Test {
 public:
  MeasureExpressionTest()
      : type_factory_(), catalog_("measure_catalog", &type_factory_) {
    catalog_.AddBuiltinFunctions(
        BuiltinFunctionOptions::AllReleasedFunctions());
    LoadUserDefinedEntities();
  }

  void LoadUserDefinedEntities() {
    // Non templated UDF
    catalog_.AddOwnedFunction(new Function(
        "udf", "MyGroup", Function::SCALAR,
        {{types::Int64Type(), {types::Int64Type()}, /*context_id=*/-1}},
        FunctionOptions()));
    // Templated UDF
    catalog_.AddOwnedFunction(new TemplatedSQLFunction(
        {"udf_templated"},
        FunctionSignature(FunctionArgumentType(ARG_TYPE_ANY_1),
                          {FunctionArgumentType(
                              ARG_TYPE_ANY_1, FunctionArgumentType::REQUIRED)},
                          /*context_id=*/-1),
        /*argument_names=*/{"x"}, ParseResumeLocation::FromString("x + 42")));

    // Non templated UDA
    catalog_.AddOwnedFunction(new Function(
        "uda", "MyGroup", Function::AGGREGATE,
        {{types::Int64Type(), {types::Int64Type()}, /*context_id=*/-1}},
        FunctionOptions()));
    // Templated UDA
    catalog_.AddOwnedFunction(new TemplatedSQLFunction(
        {"uda_templated"},
        FunctionSignature(FunctionArgumentType(ARG_TYPE_ANY_1),
                          {FunctionArgumentType(
                              ARG_TYPE_ANY_1, FunctionArgumentType::REQUIRED)},
                          /*context_id=*/-1),
        /*argument_names=*/{"x"}, ParseResumeLocation::FromString("sum(x)"),
        Function::AGGREGATE));

    // Non templated TVF
    TVFRelation::ColumnList columns;
    columns.emplace_back("key", types::Int64Type());
    columns.emplace_back("value", types::Int64Type());
    TVFRelation output_schema(columns);
    FunctionSignature tvf_sig(
        FunctionArgumentType::RelationWithSchema(
            output_schema,
            /*extra_relation_input_columns_allowed=*/false),
        {FunctionArgumentType::RelationWithSchema(
            output_schema, /*extra_relation_input_columns_allowed=*/false)},
        /*context_id=*/-1);
    catalog_.AddOwnedTableValuedFunction(
        new FixedOutputSchemaTVF({"tvf"}, tvf_sig, output_schema));
    // Templated TVF
    ParseResumeLocation sql_body =
        ParseResumeLocation::FromStringView("SELECT 1 as output");
    FunctionSignature function_signature(FunctionArgumentType::AnyRelation(),
                                         {FunctionArgumentType::AnyRelation()},
                                         /*context_id=*/1);
    catalog_.AddOwnedTableValuedFunction(
        new TemplatedSQLTVF({"tvf_templated"}, function_signature,
                            /*arg_name_list=*/{"x"}, sql_body));
  }

  absl::StatusOr<const ResolvedExpr*> AnalyzeMeasureExpressionForTable(
      absl::string_view table_name,
      std::vector<std::pair<std::string, const Type*>> columns,
      absl::string_view measure_name, absl::string_view measure_expr) {
    auto table = std::make_unique<SimpleTable>(table_name, columns);
    AnalyzerOptions analyzer_options;
    analyzer_options.mutable_language()->EnableLanguageFeature(
        FEATURE_MULTILEVEL_AGGREGATION);
    analyzer_options.mutable_language()->EnableLanguageFeature(
        FEATURE_ORDER_BY_IN_AGGREGATE);
    analyzer_options.mutable_language()->EnableLanguageFeature(
        FEATURE_HAVING_IN_AGGREGATE);
    analyzer_options.mutable_language()->EnableLanguageFeature(
        FEATURE_TABLE_VALUED_FUNCTIONS);
    std::unique_ptr<const AnalyzerOutput> analyzer_output;
    ZETASQL_ASSIGN_OR_RETURN(
        const ResolvedExpr* resolved_measure_expr,
        AnalyzeMeasureExpression(measure_expr, *table, catalog_, type_factory_,
                                 analyzer_options, analyzer_output));
    analyzer_outputs_.push_back(std::move(analyzer_output));
    return resolved_measure_expr;
  }

 protected:
  TypeFactory type_factory_;
  SimpleCatalog catalog_;
  std::vector<std::unique_ptr<const AnalyzerOutput>> analyzer_outputs_;
};

static void TableContainsMeasure(const SimpleTable& table,
                                 const std::string& measure_name,
                                 const std::string& measure_expr,
                                 ResolvedNodeKind measure_node_kind) {
  const Column* measure_column = table.FindColumnByName(measure_name);
  ASSERT_NE(measure_column, nullptr);
  EXPECT_TRUE(measure_column->GetType()->IsMeasureType());
  EXPECT_TRUE(measure_column->HasMeasureExpression() &&
              measure_column->GetExpression()->HasResolvedExpression());
  EXPECT_EQ(measure_column->GetExpression()->GetExpressionKind(),
            ExpressionKind::MEASURE_EXPRESSION);
  EXPECT_EQ(measure_column->GetExpression()->GetExpressionString(),
            measure_expr);
  EXPECT_EQ(
      measure_column->GetExpression()->GetResolvedExpression()->node_kind(),
      measure_node_kind);
}

TEST_F(MeasureExpressionTest, MeasureExpression) {
  const std::string measure_name = "total_value";
  const std::string measure_expr = "SUM(value)";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedExpr* resolved_measure_expr,
      AnalyzeMeasureExpressionForTable("table",
                                       {{"key", type_factory_.get_int64()},
                                        {"value", type_factory_.get_int64()}},
                                       measure_name, measure_expr));
  EXPECT_EQ(resolved_measure_expr->node_kind(),
            ResolvedAggregateFunctionCall::TYPE);
}

TEST_F(MeasureExpressionTest, MeasureExpressionWithGrainLock) {
  const std::string measure_name = "total_value";
  const std::string measure_expr = "SUM(ANY_VALUE(value) GROUP BY key)";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedExpr* resolved_measure_expr,
      AnalyzeMeasureExpressionForTable("table",
                                       {{"key", type_factory_.get_int64()},
                                        {"value", type_factory_.get_int64()}},
                                       measure_name, measure_expr));
  EXPECT_EQ(resolved_measure_expr->node_kind(),
            ResolvedAggregateFunctionCall::TYPE);
}

TEST_F(MeasureExpressionTest, MeasureExpressionWithSubquery) {
  const std::string measure_name = "total_value";
  const std::string measure_expr =
      "SUM((SELECT key)) + (SELECT SUM(1) FROM UNNEST([1]))";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedExpr* resolved_measure_expr,
      AnalyzeMeasureExpressionForTable("table",
                                       {{"key", type_factory_.get_int64()},
                                        {"value", type_factory_.get_int64()}},
                                       measure_name, measure_expr));
  EXPECT_EQ(resolved_measure_expr->node_kind(), ResolvedFunctionCall::TYPE);
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
      AnalyzeMeasureExpressionForTable(
          "table", {{"key", type_factory_.get_int64()}, {"values", array_type}},
          measure_name, measure_expr),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("WITH GROUP ROWS is not supported")));
}

TEST_F(MeasureExpressionTest, InvalidScalarExpr) {
  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable("table",
                                       {{"key", type_factory_.get_int64()},
                                        {"value", type_factory_.get_int64()}},
                                       "total_value", "value + 1"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Measure expression must contain at least one aggregate "
                    "function call that is not nested within a subquery")));
}

TEST_F(MeasureExpressionTest, InvalidConstLiteral) {
  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable("table",
                                       {{"key", type_factory_.get_int64()},
                                        {"value", type_factory_.get_int64()}},
                                       "constant_value", "1"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Measure expression must contain at least one aggregate "
                    "function call that is not nested within a subquery")));
}

TEST_F(MeasureExpressionTest, InvalidNoTopLevelAggregateFunctionCall) {
  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable("table",
                                       {{"key", type_factory_.get_int64()},
                                        {"value", type_factory_.get_int64()}},
                                       "total_value",
                                       "(SELECT SUM(value) FROM UNNEST([1]))"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Measure expression must contain at least one aggregate "
                    "function call that is not nested within a subquery")));
}

TEST_F(MeasureExpressionTest, InvalidGroupingConstant) {
  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable("table",
                                       {{"key", type_factory_.get_int64()},
                                        {"value", type_factory_.get_int64()}},
                                       "total_value",
                                       "SUM(value GROUP BY key)"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Expression column value cannot be an argument to a "
                         "multi-level aggregate function")));
}

TEST_F(MeasureExpressionTest, CompositeMeasure) {
  std::string measure_name = "average_value";
  std::string measure_expr = "SUM(value) / COUNT(1) * 100";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const ResolvedExpr* resolved_measure_expr,
      AnalyzeMeasureExpressionForTable("table",
                                       {{"key", type_factory_.get_int64()},
                                        {"value", type_factory_.get_int64()}},
                                       measure_name, measure_expr));
  EXPECT_EQ(resolved_measure_expr->node_kind(), ResolvedFunctionCall::TYPE);
}

TEST_F(MeasureExpressionTest, InvalidCompositeMeasureWithColumnReference) {
  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable("table",
                                       {{"key", type_factory_.get_int64()},
                                        {"value", type_factory_.get_int64()}},
                                       "invalid", "value + SUM(value)"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Expression columns in a measure expression can only "
                         "be referenced within an aggregate function call")));
}
TEST_F(MeasureExpressionTest, InvalidCompositeMeasureWithSubquery) {
  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable(
          "table",
          {{"key", type_factory_.get_int64()},
           {"value", type_factory_.get_int64()}},
          "invalid", "SUM(value) + (SELECT SUM(value) FROM UNNEST([1]))"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Expression columns in a measure expression can only "
                         "be referenced within an aggregate function call: "
                         "SUM(value) + (SELECT SUM(value) FROM UNNEST([1]))")));
}

TEST_F(MeasureExpressionTest, InvalidMeasureReferencingMeasure) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Type* measure_type,
      type_factory_.MakeMeasureType(type_factory_.get_int64()));
  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable("table",
                                       {{"key", type_factory_.get_int64()},
                                        {"value", type_factory_.get_int64()},
                                        {"measure_col", measure_type}},
                                       "measure_1", "ANY_VALUE(measure_col)"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Unrecognized name: measure_col")));
}

TEST_F(MeasureExpressionTest, InvalidMeasureReferencingNonExistingColumn) {
  std::vector<std::pair<std::string, const Type*>> columns = {
      {"key", type_factory_.get_int64()}, {"value", type_factory_.get_int64()}};
  EXPECT_THAT(AnalyzeMeasureExpressionForTable("table", columns, "measure_col",
                                               "SUM(doesnt_exist)"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Unrecognized name: doesnt_exist")));
}

// TODO: b/350555383 - Remove this test once we support measure expressions with
// the ORDER BY modifier on aggregate functions.
TEST_F(MeasureExpressionTest, InvalidMeasureWithOrderByClause) {
  std::vector<std::pair<std::string, const Type*>> columns = {
      {"key", type_factory_.get_int64()}, {"value", type_factory_.get_int64()}};
  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable("table", columns, "measure_col",
                                       "ARRAY_AGG(value ORDER BY key)"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Standalone expression resolution does not support "
                         "aggregate function calls with the ORDER BY clause")));
}

TEST_F(MeasureExpressionTest, InvalidMeasureWithHavingMinMaxClause) {
  std::vector<std::pair<std::string, const Type*>> columns = {
      {"key", type_factory_.get_int64()}, {"value", type_factory_.get_int64()}};
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_HAVING_IN_AGGREGATE);
  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable("table", columns, "measure_col",
                                       "SUM(value HAVING MAX key)"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Measure expression must not contain an aggregate "
                         "function with a HAVING MIN/MAX clause")));
}

TEST_F(MeasureExpressionTest, InvalidMeasureReferencingUserDefinedEntities) {
  std::vector<std::pair<std::string, const Type*>> columns = {
      {"key", type_factory_.get_int64()}, {"value", type_factory_.get_int64()}};
  catalog_.AddOwnedTable(new SimpleTable("TestTable", columns));
  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable("table", columns, "measure_col",
                                       "SUM(value) + udf(1)"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Measure expression must not reference user defined "
                         "entities; found: udf")));

  EXPECT_THAT(AnalyzeMeasureExpressionForTable("table", columns, "measure_col",
                                               "SUM(value) + udf_templated(1)"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Measure expression must not reference user "
                                 "defined entities; found: udf_templated")));

  EXPECT_THAT(AnalyzeMeasureExpressionForTable("table", columns, "measure_col",
                                               "SUM(value) + uda(value)"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Measure expression must not reference user "
                                 "defined entities; found: uda")));

  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable("table", columns, "measure_col",
                                       "SUM(value) + uda_templated(value)"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Measure expression must not reference user "
                         "defined entities; found: uda_templated")));

  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable(
          "table", columns, "measure_col",
          "SUM(value) + countif(exists(select 1 from tvf(TABLE TestTable)))"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Measure expression must not reference user "
                         "defined entities; found: tvf")));

  EXPECT_THAT(
      AnalyzeMeasureExpressionForTable("table", columns, "measure_col",
                                       "SUM(value) + countif(exists(select 1 "
                                       "from tvf_templated(TABLE TestTable)))"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Measure expression must not reference user "
                         "defined entities; found: tvf_templated")));
}

}  // namespace zetasql
