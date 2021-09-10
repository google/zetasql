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

#include "zetasql/public/evaluator.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "zetasql/common/evaluator_test_table.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/evaluator_base.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/evaluation.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/sql_builder.h"
#include "zetasql/testdata/populate_sample_tables.h"
#include "zetasql/testdata/sample_catalog.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "zetasql/testing/test_value.h"
#include "zetasql/testing/using_test_value.cc"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <cstdint>
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "zetasql/base/stl_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/clock.h"

extern absl::Flag<int64_t>
    FLAGS_zetasql_simple_iterator_call_time_now_rows_period;

namespace zetasql {
namespace {

using ::testing::_;
using ::testing::AllOf;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;
using ExpressionOptions = ::zetasql::PreparedExpression::ExpressionOptions;
using QueryOptions = ::zetasql::PreparedQuery::QueryOptions;

class UDFEvalTest : public ::testing::Test {
 public:
  const int kFunctionId = 5000;

  void SetUp() override {
    catalog_ = absl::make_unique<SimpleCatalog>("udf_catalog");
    catalog_->AddZetaSQLFunctions();
    ZETASQL_ASSERT_OK(analyzer_options_.AddQueryParameter(
        "param", types::StringType()));
  }

  SimpleCatalog* catalog() const {
    return catalog_.get();
  }

  std::unique_ptr<SimpleCatalog> catalog_;
  AnalyzerOptions analyzer_options_;
  FunctionOptions function_options_;
};

TEST(EvaluatorTest, SimpleExpression) {
  PreparedExpression expr("1 + 2");
  EXPECT_EQ(Int64(3), expr.Execute().value());
  EXPECT_EQ(Int64(3), expr.Execute().value());
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
}

TEST(EvaluatorTest, ColumnExpression) {
  PreparedExpression expr("a + b");
  EXPECT_EQ(Int64(3), expr.Execute({{"a", Int64(1)}, {"b", Int64(2)}}).value());
  EXPECT_EQ(Int64(5),
            expr.Execute({{"a", Int64(-1)}, {"b", Int64(6)}}).value());
  EXPECT_THAT(
      expr.Execute({{"a", Int64(-1)}, {"b", Double(6)}}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Expected column parameter 'b' to be of type INT64")));
  // If we call Execute more than once, we must pass in the exact same
  // set of columns.
  EXPECT_THAT(expr.Execute({{"a", Int64(-1)}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Incomplete column parameters")));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
}

TEST(EvaluatorTest, NoRecoveryFromError) {
  PreparedExpression expr("a * 2");
  EXPECT_THAT(expr.Execute({{"a", Value::String("foo")}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature")));
  EXPECT_THAT(expr.Execute({{"a", Value::Int64(1)}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid prepared expression")));
  EXPECT_DEATH(expr.output_type(), "Invalid prepared expression");
}

TEST(EvaluatorTest, BooleanExpression) {
  PreparedExpression expr("(a > 1 AND a < 5) OR b IS NOT NULL");
  EXPECT_EQ(True(), expr.Execute({{"a", Int64(0)}, {"b", Int64(0)}}).value());
  EXPECT_EQ(True(),
            expr.Execute({{"a", Int64(3)}, {"b", NullInt64()}}).value());
  EXPECT_EQ(False(),
            expr.Execute({{"a", Int64(0)}, {"b", NullInt64()}}).value());
  EXPECT_TRUE(types::BoolType()->Equals(expr.output_type()));
}

TEST(EvaluatorTest, ExpressionWithSubquery) {
  PreparedExpression expr("1 + (SELECT a)");
  EXPECT_EQ(Int64(3), expr.Execute({{"a", Int64(2)}}).value());
}

TEST(EvaluatorTest, SubqueryExpression) {
  PreparedExpression expr("(SELECT 1 + a)");
  EXPECT_EQ(Int64(3), expr.Execute({{"a", Int64(2)}}).value());
}

TEST(EvaluatorTest, WithClauseSubquerySimple) {
  const std::string query("(WITH t AS (SELECT 2 as b) SELECT b FROM t)");

  // By default, the AnalyzerOptions used for Prepare/Execute do not enable
  // the LanguageFeature to support WITH clause inside subqueries.
  PreparedExpression expr(query);
  EXPECT_THAT(expr.Execute(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("WITH is not supported on subqueries in this "
                                 "language version")));

  // Prepare the expression with appropriate AnalyzerOptions.
  PreparedExpression expr2(query);
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
  AnalyzerOptions analyzer_options(language_options);
  ZETASQL_ASSERT_OK(expr2.Prepare(analyzer_options));
  const absl::StatusOr<Value> result = expr2.Execute();
  EXPECT_THAT(result, IsOkAndHolds(Int64(2)));
}

TEST(EvaluatorTest, WithClauseSubquery) {
  PreparedExpression expr("(WITH t AS (SELECT 2 + @a as b) SELECT b FROM t)");
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
  AnalyzerOptions analyzer_options(language_options);
  ZETASQL_ASSERT_OK(analyzer_options.AddQueryParameter("a", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  const absl::StatusOr<Value> result = expr.Execute({}, {{"a", Int64(2)}});
  EXPECT_THAT(result, IsOkAndHolds(Int64(4)));
}

TEST(EvaluatorTest, WithClauseSubqueryWithLimit) {
  // When a WITH entry is referenced only once, only rows which are needed for
  // the main query are evaluated. This allows the following query to succeed,
  // while the evaluation of all rows in the subquery would trigger a divide-by-
  // zero error.
  const std::string query = R"(ARRAY(
  WITH t AS (SELECT 8 / (5 - n) AS n FROM UNNEST([1, 2, 3, 4, 5]) AS n)
  SELECT n FROM t LIMIT 3
  ))";
  PreparedExpression expr(query);
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
  AnalyzerOptions analyzer_options(language_options);
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  const absl::StatusOr<Value> result = expr.Execute();
  EXPECT_THAT(result, IsOkAndHolds(Array({2.0, 8.0 / 3, 4.0})));
}

TEST(EvaluatorTest, InnerWithClauseSubqueryAndOuterLimit) {
  // Here, we have an extra subquery later so that only the first three rows
  // of t are referenced in the main query, even though the body of the inner
  // with references all of the rows.
  const std::string query = R"(ARRAY(
    WITH u AS (
      WITH t AS (SELECT 8 / (5 - n) AS n FROM UNNEST([1, 2, 3, 4, 5]) AS n)
      SELECT n FROM t
    ) SELECT * FROM u LIMIT 3
  ))";
  PreparedExpression expr(query);
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
  AnalyzerOptions analyzer_options(language_options);
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  const absl::StatusOr<Value> result = expr.Execute();
  EXPECT_THAT(result, IsOkAndHolds(Array({2.0, 8.0 / 3, 4.0})));
}

TEST(EvaluatorTest, WithClauseSubqueryWithLimitAndMultipleRefs) {
  // When a WITH entry is referenced multiple times, the whole thing is
  // evaluated up front, triggering a divide-by-zero error.
  const std::string query = R"(ARRAY(
  WITH
    t1 AS (SELECT 8 / (5 - n) AS n FROM UNNEST([1, 2, 3, 4, 5]) AS n),
    t2 AS (SELECT * FROM t1),
    t3 AS (SELECT * FROM t2)
  SELECT t2.n FROM t2 CROSS JOIN t3 LIMIT 3
  ))";
  PreparedExpression expr(query);
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
  AnalyzerOptions analyzer_options(language_options);
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  const absl::StatusOr<Value> result = expr.Execute();
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kOutOfRange,
                               HasSubstr("division by zero")));
}

TEST(EvaluatorTest, WithClauseSubqueryWithLimitAndRefHasMultipleRefs) {
  // Here, t1 is referenced only from t2, but t2 has multiple references, t3
  // and t4, which are both referenced in the main query. This requires full
  // evaluation of t1, which will trigger divide-by-zero.
  const std::string query = R"(ARRAY(
  WITH
    t1 AS (SELECT 8 / (5 - n) AS n FROM UNNEST([1, 2, 3, 4, 5]) AS n),
    t2 AS (SELECT * FROM t1),
    t3 AS (SELECT * FROM t2),
    t4 AS (SELECT * FROM t2)
  SELECT t3.n FROM t3 CROSS JOIN t4 LIMIT 3
  ))";
  PreparedExpression expr(query);
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
  AnalyzerOptions analyzer_options(language_options);
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  const absl::StatusOr<Value> result = expr.Execute();
  EXPECT_THAT(result, StatusIs(absl::StatusCode::kOutOfRange,
                               HasSubstr("division by zero")));
}

TEST(EvaluatorTest, WithClauseSubqueryWithLimitAndIndirectRef) {
  // Here, t1 is referenced indirectly through t2, but it's still just once
  // reference, so only the first three rows of t1 are ever evaluated.
  const std::string query = R"(ARRAY(
  WITH
    t1 AS (SELECT 8 / (5 - n) AS n FROM UNNEST([1, 2, 3, 4, 5]) AS n),
    t2 AS (SELECT * FROM t1)
  SELECT n FROM t2 LIMIT 3
  ))";
  PreparedExpression expr(query);
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
  AnalyzerOptions analyzer_options(language_options);
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  const absl::StatusOr<Value> result = expr.Execute();
  EXPECT_THAT(result, IsOkAndHolds(Array({2.0, 8.0 / 3, 4.0})));
}

TEST(EvaluatorTest, WithClauseSubqueryWithLimitAndUnreferencedRef) {
  // Here, t1 is referenced by both the main query and t2, but since t2, itself
  // is unreferenced in the main query (the reference from t3 doesn't count
  // since t3 is unreferenced), t1 is considered to have only one reference. So,
  // only the first three rows of t1 are evaluated.
  const std::string query = R"(ARRAY(
  WITH
    t1 AS (SELECT 8 / (5 - n) AS n FROM UNNEST([1, 2, 3, 4, 5]) AS n),
    t2 AS (SELECT * FROM t1),
    t3 AS (SELECT * FROM t2)
  SELECT n FROM t1 LIMIT 3
  ))";
  PreparedExpression expr(query);
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
  AnalyzerOptions analyzer_options(language_options);
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  const absl::StatusOr<Value> result = expr.Execute();
  EXPECT_THAT(result, IsOkAndHolds(Array({2.0, 8.0 / 3, 4.0})));
}

TEST(EvaluatorTest, WithRecursiveTerminatesDueToLimit) {
  // The recursive query, by itself, does not terminate, but the main query
  // referencing it uses a LIMIT clause, which terminates the recursion when
  // the LIMIT value is reached.
  //
  // As the behavior of LIMIT without ORDER BY is engine-dependent, this is not
  // a compliance test. However, the reference implementation provides
  // deterministic behavior to make the insertion of LIMIT useful for debugging
  // recursive queries. Within the reference implementation we guarantee that:
  // 1) Rows from each iteration come before rows from subsequent iterations
  // 2) The recursion halts when the number of rows in the outer LIMIT clause
  //      is reached (this guarantee only holds when the 'inlined_with_entries'
  //      optimization is enabled, which is the case in the evaluator api).
  for (const char* mode : {"ALL", "DISTINCT"}) {
    SCOPED_TRACE(absl::StrCat("UNION ", mode));
    const std::string query = absl::StrFormat(R"(ARRAY(
    WITH RECURSIVE
      Fib AS (
        SELECT 1 AS a, 1 AS b
        UNION %s
        SELECT b, a + b FROM Fib
    ) SELECT a FROM fib LIMIT 10
    ))",
                                              mode);
    PreparedExpression expr(query);
    LanguageOptions language_options;
    language_options.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
    language_options.EnableLanguageFeature(FEATURE_V_1_3_WITH_RECURSIVE);
    AnalyzerOptions analyzer_options(language_options);
    ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
    const absl::StatusOr<Value> result = expr.Execute();
    EXPECT_THAT(result,
                IsOkAndHolds(Array({Int64(1), Int64(1), Int64(2), Int64(3),
                                    Int64(5), Int64(8), Int64(13), Int64(21),
                                    Int64(34), Int64(55)})));
  }
}

TEST(EvaluatorTest, WithRecursiveNotTerminating) {
  // A non-terminating recursive query should fail cleanly when the rows
  // produced exceed memory limits.
  for (const char* mode : {"ALL", "DISTINCT"}) {
    SCOPED_TRACE(absl::StrCat("UNION ", mode));
    const std::string query = absl::StrFormat(R"(ARRAY(
      WITH RECURSIVE
        t AS (SELECT 1 AS n UNION %s SELECT n + 1 FROM t)
      SELECT * FROM t
    ))",
                                              mode);
    PreparedExpression expr(query);
    LanguageOptions language_options;
    language_options.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
    language_options.EnableLanguageFeature(FEATURE_V_1_3_WITH_RECURSIVE);
    AnalyzerOptions analyzer_options(language_options);
    ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
    const absl::StatusOr<Value> result = expr.Execute();
    EXPECT_THAT(
        result,
        StatusIs(_, HasSubstr("Cannot construct array Value larger than")));
  }
}

TEST(EvaluatorTest, WithRecursiveMemoryExhaustedByHashSet) {
  // This query does not produce any rows. But, the internal hash set of
  // accumulated rows (which is needed to filter duplicates) keeps growing.
  // Make sure that the query is properly aborted when the internal hash set
  // causes the EvaluatorOptions' memory limit to be exceeded.
  const std::string query = R"(ARRAY(
    WITH RECURSIVE
      t AS (SELECT 1 AS n UNION DISTINCT SELECT n + 1 FROM t)
    SELECT * FROM t WHERE n < 0
  ))";
  EvaluatorOptions options;
  options.max_intermediate_byte_size = 1000;
  PreparedExpression expr(query, options);
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
  language_options.EnableLanguageFeature(FEATURE_V_1_3_WITH_RECURSIVE);
  AnalyzerOptions analyzer_options(language_options);
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  const absl::StatusOr<Value> result = expr.Execute();
  EXPECT_THAT(result, StatusIs(_, HasSubstr("Out of memory: requested")));
}

TEST(EvaluatorTest, WithClauseSubquery_b119901615) {
  PreparedExpression expr(
      "(WITH a AS (SELECT true as b, 15 as c) SELECT IF(b, c, -1) FROM a)");
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_1_WITH_ON_SUBQUERY);
  AnalyzerOptions analyzer_options(language_options);
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  absl::StatusOr<Value> result = expr.Execute();
  EXPECT_THAT(result, IsOkAndHolds(Int64(15)));
}

TEST(EvaluatorTest, QueryAsExpression) {
  PreparedExpression expr("SELECT 1 + a");
  EXPECT_THAT(expr.Execute({{"a", Int64(2)}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Unexpected keyword SELECT")));
}

TEST(EvaluatorTest, QueryAsArray) {
  PreparedExpression query("ARRAY(SELECT a UNION ALL SELECT b)");
  Value result =
      query.Execute({{"a", Double(3.0)}, {"b", Double(5.0)}}).value();
  EXPECT_EQ(Array({3.0, 5.0}), result);
}

TEST(EvaluatorTest, ExpressionFromArray) {
  PreparedExpression expr("(SELECT t.y FROM UNNEST(arr) t WHERE t.x = 2)");
  Value arr1 = StructArray({"x", "y"}, {{1ll, 3.0}, {2ll, 5.0}});
  EXPECT_EQ(Double(5.0), expr.Execute({{"arr", arr1}}).value());
  Value arr2 = StructArray({"x", "y"}, {{1ll, 3.0}, {200ll, 5.0}});
  EXPECT_EQ(NullDouble(), expr.Execute({{"arr", arr2}}).value());
  Value arr3 = StructArray({"x", "y"}, {{2ll, 3.0}, {2ll, 5.0}});
  EXPECT_THAT(expr.Execute({{"arr", arr3}}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("More than one element")));
}

TEST(EvaluatorTest, ArrayExpressionOnHeap) {
  PreparedExpression* expr =
      new PreparedExpression(
          "ARRAY(SELECT AS STRUCT 1,2 UNION ALL SELECT AS STRUCT 3,4)");
  EXPECT_EQ(StructArray({"", ""}, {{1ll, 2ll}, {3ll, 4ll}}),
            expr->Execute().value());
  delete expr;
}

TEST(EvaluatorTest, ExpressionWithSystemVariables) {
  PreparedExpression expr("(@@sysvar1 + @@sysvar2.foo) * col + @@sysvar1");

  SystemVariableValuesMap system_variables;
  system_variables[{"sysvar1"}] = Value::Int64(3);
  system_variables[{"sysvar2", "foo"}] = Value::Int64(8);

  Value result =
      expr.Execute({{"col", Value::Int64(5)}}, {}, system_variables).value();
  EXPECT_EQ(Value::Int64(58), result);
}

TEST(EvaluatorTest, ExpressionWithUnusedSystemVariables) {
  PreparedExpression expr("(@@row_count)");

  SystemVariableValuesMap system_variables;
  system_variables[{"row_count"}] = Value::Int64(3);
  system_variables[{"error", "message"}] = Value::Int64(8);

  Value result =
      expr.Execute({{"col", Value::Int64(5)}}, {}, system_variables).value();
  EXPECT_EQ(Value::Int64(3), result);
}

TEST(EvaluatorTest, ExpressionWithUnusedSystemVariables2) {
  PreparedExpression expr("(@@sysvar1 + @@sysvar2.foo) * col");

  SystemVariableValuesMap system_variables;
  system_variables[{"sysvar1"}] = Value::Int64(3);
  system_variables[{"sysvar2", "foo"}] = Value::Int64(8);
  system_variables[{"sysvar_unused"}] = Value::Int64(22);

  Value result =
      expr.Execute({{"col", Value::Int64(5)}}, {}, system_variables).value();
  EXPECT_EQ(Value::Int64(55), result);
}

TEST(EvaluatorTest, ExpressionWithSystemVariablesAndQueryParameters) {
  PreparedExpression expr("(@@sysvar1 + @@sysvar2.foo + @param1) * col");

  SystemVariableValuesMap system_variables;
  system_variables[{"sysvar1"}] = Value::Int64(3);
  system_variables[{"sysvar2", "foo"}] = Value::Int64(8);

  Value result = expr.Execute({{"col", Value::Int64(5)}},
                              {{"param1", Value::Int64(1)}}, system_variables)
                     .value();
  EXPECT_EQ(Value::Int64(60), result);
}

TEST(EvaluatorTest, ExpressionWithSystemVariablesAndPositionalQueryParameters) {
  PreparedExpression expr("(@@sysvar1 + @@sysvar2.foo + ? + ?) * col");

  SystemVariableValuesMap system_variables;
  system_variables[{"sysvar1"}] = Value::Int64(3);
  system_variables[{"sysvar2", "foo"}] = Value::Int64(8);

  Value result = expr.ExecuteWithPositionalParams(
                         {{"col", Value::Int64(5)}},
                         std::vector<Value>{Value::Int64(1), Value::Int64(2)},
                         system_variables)
                     .value();
  EXPECT_EQ(Value::Int64(70), result);
}

TEST(EvaluatorTest, ExpressionWithQueryParameters) {
  PreparedExpression expr("(@param1 + @param2) * col");
  Value result =
      expr.Execute({{"col", Value::Int64(5)}},
                   {{"param1", Value::Int64(1)}, {"param2", Value::Int64(2)}})
          .value();
  EXPECT_EQ(Value::Int64(15), result);
}

TEST(EvaluatorTest, ExpressionWithPositionalQueryParameters) {
  PreparedExpression expr("(? + ?) * col");
  Value result = expr.ExecuteWithPositionalParams(
                         {{"col", Value::Int64(5)}},
                         std::vector<Value>{Value::Int64(1), Value::Int64(2)})
                     .value();
  EXPECT_EQ(Value::Int64(15), result);
}

TEST(EvaluatorTest, Prepare) {
  PreparedExpression expr("(@param1 + @param2) * col");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param1", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param2", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
  Value result =
      expr.Execute({{"col", Value::Int64(5)}},
                   {{"param1", Value::Int64(1)}, {"param2", Value::Int64(2)}})
          .value();
  EXPECT_EQ(Value::Int64(15), result);
}

TEST(EvaluatorTest, PrepareWithSystemVariables) {
  PreparedExpression expr(
      "(@@sysvar1 + @@sysvar2 + @@sysvar1) * col - @param1");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param1", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param2", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddSystemVariable({"sysvar1"}, types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddSystemVariable({"sysvar2"}, types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));

  SystemVariableValuesMap sysvar_values;
  sysvar_values[{"sysvar1"}] = Value::Int64(12);
  sysvar_values[{"sysvar2"}] = Value::Int64(10);

  Value result =
      expr.Execute({{"col", Value::Int64(5)}},
                   {{"param1", Value::Int64(1)}, {"param2", Value::Int64(2)}},
                   sysvar_values)
          .value();
  EXPECT_EQ(Value::Int64(169), result);
}

TEST(EvaluatorTest, PrepareWithPositionalParameters) {
  PreparedExpression expr("(? + ?) * col");
  AnalyzerOptions options;
  options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));

  Value result =
      expr.ExecuteWithPositionalParams({{"col", Value::Int64(5)}},
                                       {Value::Int64(1), Value::Int64(2)})
          .value();
  EXPECT_EQ(Value::Int64(15), result);
}

TEST(EvaluatorTest, PrepareWithPositionalParametersAndSystemVariables) {
  PreparedExpression expr("(? + ?) * col + @@sysvar1 + @@sysvar2");
  AnalyzerOptions options;
  options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddSystemVariable({"sysvar1"}, types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddSystemVariable({"sysvar2"}, types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));

  SystemVariableValuesMap sysvar_values;
  sysvar_values[{"sysvar1"}] = Value::Int64(12);
  sysvar_values[{"sysvar2"}] = Value::Int64(10);

  Value result = expr.ExecuteWithPositionalParams(
                         {{"col", Value::Int64(5)}},
                         {Value::Int64(1), Value::Int64(2)}, sysvar_values)
                     .value();
  EXPECT_EQ(Value::Int64(37), result);
}

TEST(EvaluatorTest, ExecuteAfterPrepare) {
  PreparedExpression expr("(@param1 + @param2) * col + @@sysvar1");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param1", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param2", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddSystemVariable({"sysvar1"}, types::Int64Type()));
  // Check that ExecuteAfterPrepare gives an error if Prepare has not yet been
  // called.
  EXPECT_THAT(expr.ExecuteAfterPrepare(
                  {{"col", Value::Int64(5)}},
                  {{"param1", Value::Int64(1)}, {"param2", Value::Int64(2)}},
                  {{{"sysvar1"}, Value::Int64(10)}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid prepared expression")));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));

  // Call ExecuteAfterPrepare against a const object: the point of having this
  // method is so it can be const, and this protects against regressions should
  // it be changed in future.
  const PreparedExpression* const_expr = &expr;
  Value result = const_expr
                     ->ExecuteAfterPrepare({{"col", Value::Int64(5)}},
                                           {{"param1", Value::Int64(1)},
                                            {"param2", Value::Int64(2)}},
                                           {{{"sysvar1"}, Value::Int64(10)}})
                     .value();
  EXPECT_EQ(Value::Int64(25), result);

  // Check that ExecuteAfterPrepare gives an error if Prepare has been called,
  // but failed.
  PreparedExpression expr2("(@param1 + @param2) * col");
  AnalyzerOptions options2;
  ZETASQL_ASSERT_OK(options2.AddQueryParameter("param1", types::Int64Type()));
  ZETASQL_ASSERT_OK(options2.AddQueryParameter("param2", types::Int64Type()));
  ZETASQL_ASSERT_OK(options2.AddExpressionColumn("col", types::StringType()));
  EXPECT_THAT(expr2.Prepare(options2),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature for operator *")));
  EXPECT_THAT(expr2.ExecuteAfterPrepare(
                  {{"col", Value::Int64(5)}},
                  {{"param1", Value::Int64(1)}, {"param2", Value::Int64(2)}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid prepared expression")));
}

TEST(EvaluatorTest, ExecuteAfterPrepareWithPositionalParameters) {
  PreparedExpression expr("(? + ?) * col");
  AnalyzerOptions options;
  options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  // Check that ExecuteAfterPrepare gives an error if Prepare has not yet been
  // called.
  EXPECT_THAT(
      expr.ExecuteAfterPrepareWithPositionalParams(
          {{"col", Value::Int64(5)}}, {Value::Int64(1), Value::Int64(2)}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid prepared expression")));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));

  // Call ExecuteAfterPrepare against a const object: the point of having this
  // method is so it can be const, and this protects against regressions should
  // it be changed in future.
  const PreparedExpression* const_expr = &expr;
  Value result =
      const_expr
          ->ExecuteAfterPrepareWithPositionalParams(
              {{"col", Value::Int64(5)}}, {Value::Int64(1), Value::Int64(2)})
          .value();
  EXPECT_EQ(Value::Int64(15), result);

  // Check that ExecuteAfterPrepare gives an error if Prepare has been called,
  // but failed.
  PreparedExpression expr2("(? + ?) * col");
  AnalyzerOptions options2;
  options2.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_ASSERT_OK(options2.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options2.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options2.AddExpressionColumn("col", types::StringType()));
  EXPECT_THAT(expr2.Prepare(options2),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature for operator *")));
  EXPECT_THAT(
      expr2.ExecuteAfterPrepareWithPositionalParams(
          {{"col", Value::Int64(5)}}, {Value::Int64(1), Value::Int64(2)}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid prepared expression")));
}

TEST(EvaluatorTest, PrepareFailOnAnalysis) {
  PreparedExpression expr("@param + col");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::StringType()));
  EXPECT_THAT(expr.Prepare(options),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature")));
}

TEST(EvaluatorTest, PrepareExecuteExtraQueryParameter) {
  PreparedExpression expr("@param + col");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param2", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
  Value result =
      expr.Execute({{"col", Value::Int64(5)}},
                   {{"param", Value::Int64(6)}, {"param2", Value::Int64(8)}})
          .value();
  EXPECT_EQ(result, Value::Int64(11));
}

TEST(EvaluatorTest, PrepareExecuteMissingQueryParameter) {
  PreparedExpression expr("@param + col");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  EXPECT_THAT(expr.Prepare(options),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Query parameter 'param' not found")));
}

TEST(EvaluatorTest, PrepareExecuteAllowUndeclaredQueryParameters) {
  PreparedExpression expr("@param + col");
  AnalyzerOptions analyzer_options;
  analyzer_options.set_allow_undeclared_parameters(true);
  ZETASQL_ASSERT_OK(analyzer_options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<std::string> params,
                       expr.GetReferencedParameters());
  EXPECT_THAT(params, testing::ElementsAre("param"));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Value result,
      expr.ExecuteAfterPrepare(
          {{"col", Value::Int64(5)}}, {{"param", Value::Int64(6)}}));
  EXPECT_EQ(result, Value::Int64(11));
}

TEST(EvaluatorTest, PrepareExecuteAllowUndeclaredQueryParametersResolvedExpr) {
  AnalyzerOptions analyzer_options;
  analyzer_options.set_allow_undeclared_parameters(true);
  ZETASQL_ASSERT_OK(analyzer_options.AddExpressionColumn("col", types::Int64Type()));

  auto catalog = absl::make_unique<SimpleCatalog>("foo");
  catalog->AddZetaSQLFunctions();
  TypeFactory type_factory;

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_ASSERT_OK(AnalyzeExpression(
      "@param + col", analyzer_options,
      catalog.get(), &type_factory, &analyzer_output));

  EvaluatorOptions evaluator_options;
  PreparedExpression expr(analyzer_output->resolved_expr(),
                          evaluator_options);
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<std::string> params,
                       expr.GetReferencedParameters());
  EXPECT_THAT(params, testing::ElementsAre("param"));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
  EXPECT_THAT(
      expr.ExecuteAfterPrepare(
          {{"col", Value::Int64(5)}}, {{"param", Value::Int64(6)}}),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr("Expected type not found for variable param")));
}

TEST(EvaluatorTest, PrepareExecuteWrongQueryParameterType) {
  PreparedExpression expr("@param + col");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
  absl::Status status = expr.Execute(
      {{"col", Value::Int64(5)}}, {{"param", Value::String("foo")}}).status();
  EXPECT_THAT(
      status,
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Expected query parameter 'param' to be of type INT64")));
}

TEST(EvaluatorTest, PrepareExecuteMissingPositionalQueryParameter) {
  PreparedExpression expr("? + col");
  AnalyzerOptions options;
  options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  EXPECT_THAT(expr.Prepare(options),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Query parameter number 1 is not defined")));
}

TEST(EvaluatorTest, PrepareExecuteAllowUndeclaredPositionalQueryParameters) {
  PreparedExpression expr("CONCAT(CAST((? + col) AS STRING), ?)");
  AnalyzerOptions analyzer_options;
  analyzer_options.set_allow_undeclared_parameters(true);
  analyzer_options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_ASSERT_OK(analyzer_options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  EXPECT_TRUE(types::StringType()->Equals(expr.output_type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(int count, expr.GetPositionalParameterCount());
  EXPECT_EQ(2, count);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Value result,
      expr.ExecuteAfterPrepareWithPositionalParams(
          {{"col", Value::Int64(5)}}, {Value::Int64(6),
                                        Value::StringValue("foo")}));
  EXPECT_EQ(result, Value::StringValue("11foo"));
}

TEST(EvaluatorTest,
     PrepareExecuteAllowUndeclaredPositionalQueryParametersResolvedExpr) {
  AnalyzerOptions analyzer_options;
  analyzer_options.set_allow_undeclared_parameters(true);
  analyzer_options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_ASSERT_OK(analyzer_options.AddExpressionColumn("col", types::Int64Type()));

  auto catalog = absl::make_unique<SimpleCatalog>("foo");
  catalog->AddZetaSQLFunctions();
  TypeFactory type_factory;

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_ASSERT_OK(AnalyzeExpression(
      "CONCAT(CAST((? + col) AS STRING), ?)", analyzer_options,
      catalog.get(), &type_factory, &analyzer_output));

  EvaluatorOptions evaluator_options;
  PreparedExpression expr(analyzer_output->resolved_expr(), evaluator_options);
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  EXPECT_TRUE(types::StringType()->Equals(expr.output_type()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(int count, expr.GetPositionalParameterCount());
  EXPECT_EQ(2, count);
  EXPECT_THAT(
      expr.ExecuteAfterPrepareWithPositionalParams(
          {{"col", Value::Int64(5)}}, {Value::Int64(6),
                                        Value::StringValue("foo")}),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("Mismatch in number of analyzer parameters versus "
                         "algebrizer parameters")));
}

TEST(EvaluatorTest, PrepareExecuteWrongPositionalQueryParameterType) {
  PreparedExpression expr("? + col");
  AnalyzerOptions options;
  options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
  absl::Status status =
      expr.ExecuteWithPositionalParams({{"col", Value::Int64(5)}},
                                       {Value::String("foo")})
          .status();
  EXPECT_THAT(
      status,
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Expected positional parameter 1 to be of type INT64")));
}

TEST(EvaluatorTest, PrepareExecuteWrongSystemVariableType) {
  PreparedExpression expr("@@sysvar + col");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddSystemVariable({"sysvar"}, types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
  absl::Status status = expr.Execute({{"col", Value::Int64(5)}}, {},
                                     {{{"sysvar"}, Value::String("foo")}})
                            .status();
  EXPECT_THAT(
      status,
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Expected system variable 'sysvar' to be of type INT64")));
}

TEST(EvaluatorTest, PrepareExecuteSystemVariableMissingValue) {
  PreparedExpression expr("@@sysvar + col");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddSystemVariable({"sysvar"}, types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
  absl::Status status = expr.Execute({{"col", Value::Int64(5)}}, {}).status();
  EXPECT_THAT(
      status,
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("No value provided for system variable sysvar")));
}

TEST(EvaluatorTest, PrepareExecuteSystemMissingInAnalyzerOptions) {
  // @@sysvar2 has a value, but isn't in the analyzer options.
  // Even though @@sysvar2 is not used in the query, this is not allowed.
  PreparedExpression expr("@@sysvar1 + col");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddSystemVariable({"sysvar1"}, types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
  absl::Status status = expr.Execute({{"col", Value::Int64(5)}}, {},
                                     {{{"sysvar1"}, Value::Int64(5)},
                                      {{"sysvar2"}, Value::Int64(6)}})
                            .status();
  EXPECT_THAT(status,
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Value provided for system variable sysvar2, "
                                 "which is not in the AnalyzerOptions")));
}

TEST(EvaluatorTest, PrepareExecuteMismatchedPositionalQueryParameterCount) {
  {
    // Too few positional parameters to Prepare.
    PreparedExpression expr("? + ? + col");
    AnalyzerOptions options;
    options.set_parameter_mode(PARAMETER_POSITIONAL);
    ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
    ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
    EXPECT_THAT(
        expr.Prepare(options),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr("Query parameter number 2 is not defined (1 provided)")));
  }

  {
    // Too many positional parameters to Prepare (not an error; the analyzer
    // doesn't raise an error for extra parameters).
    PreparedExpression expr("? + col");
    AnalyzerOptions options;
    options.set_parameter_mode(PARAMETER_POSITIONAL);
    ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
    ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
    ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
    ZETASQL_EXPECT_OK(expr.Prepare(options));
  }

  {
    // Too few positional parameters to Execute.
    PreparedExpression expr("? + ? + 1");
    EXPECT_THAT(
        expr.ExecuteWithPositionalParams({}, {Value::Int64(3)}).status(),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr("Query parameter number 2 is not defined (1 provided)")));
  }

  {
    // More positional parameters than needed; should be allowed.
    PreparedExpression expr("? + ? + 1");
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        Value result,
        expr.ExecuteWithPositionalParams(
            {}, {Value::Int64(3), Value::Int64(4), Value::Int64(5)}));
    EXPECT_EQ(Value::Int64(8), result);
  }
}

TEST(EvaluatorTest, PrepareExecuteSubexpressionsWithPositionalQueryParameters) {
  const ParameterValueList positional_parameters =
      {Value::Int64(1), Value::Int64(2), Value::Int64(4), Value::Int64(8)};

  AnalyzerOptions analyzer_options;
  analyzer_options.set_parameter_mode(PARAMETER_POSITIONAL);
  for (const Value& param : positional_parameters) {
    ZETASQL_ASSERT_OK(analyzer_options.AddPositionalQueryParameter(param.type()));
  }
  auto catalog = absl::make_unique<SimpleCatalog>("foo");
  catalog->AddZetaSQLFunctions();
  TypeFactory type_factory;

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_ASSERT_OK(AnalyzeExpression("(? + ?) + (? + ?)", analyzer_options,
                              catalog.get(), &type_factory, &analyzer_output));
  const ResolvedExpr* resolved_expr = analyzer_output->resolved_expr();
  ASSERT_TRUE(resolved_expr != nullptr);
  ASSERT_EQ(RESOLVED_FUNCTION_CALL, resolved_expr->node_kind());
  const auto* resolved_function_call =
      resolved_expr->GetAs<ResolvedFunctionCall>();
  ASSERT_EQ(2, resolved_function_call->argument_list_size());

  EvaluatorOptions options;
  options.type_factory = &type_factory;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Value result1,
      PreparedExpression(resolved_function_call->argument_list(0), options)
          .ExecuteWithPositionalParams({}, positional_parameters));
  EXPECT_EQ(Value::Int64(3), result1);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Value result2,
      PreparedExpression(resolved_function_call->argument_list(1), options)
          .ExecuteWithPositionalParams({}, positional_parameters));
  EXPECT_EQ(Value::Int64(12), result2);
}

TEST(EvaluatorTest, PrepareExecuteSubexpressionsWithNamedQueryParameters) {
  const ParameterValueMap parameters =
      {{"param1", Value::Int64(1)}, {"param2", Value::Int64(2)},
       {"param4", Value::Int64(4)}, {"param8", Value::Int64(8)}};

  AnalyzerOptions analyzer_options;
  analyzer_options.set_parameter_mode(PARAMETER_NAMED);
  for (const auto& entry : parameters) {
    ZETASQL_ASSERT_OK(analyzer_options.AddQueryParameter(
        entry.first, entry.second.type()));
  }
  auto catalog = absl::make_unique<SimpleCatalog>("foo");
  catalog->AddZetaSQLFunctions();
  TypeFactory type_factory;

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_ASSERT_OK(AnalyzeExpression("(@param1 + @param2) + (@param4 + @param8)",
                              analyzer_options, catalog.get(), &type_factory,
                              &analyzer_output));
  const ResolvedExpr* resolved_expr = analyzer_output->resolved_expr();
  ASSERT_TRUE(resolved_expr != nullptr);
  ASSERT_EQ(RESOLVED_FUNCTION_CALL, resolved_expr->node_kind());
  const auto* resolved_function_call =
      resolved_expr->GetAs<ResolvedFunctionCall>();
  ASSERT_EQ(2, resolved_function_call->argument_list_size());

  EvaluatorOptions options;
  options.type_factory = &type_factory;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Value result1,
      PreparedExpression(resolved_function_call->argument_list(0), options)
          .Execute({}, parameters));
  EXPECT_EQ(Value::Int64(3), result1);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Value result2,
      PreparedExpression(resolved_function_call->argument_list(1), options)
          .Execute({}, parameters));
  EXPECT_EQ(Value::Int64(12), result2);
}

TEST(EvaluatorTest, PrepareExecuteWrongColumnParameterType) {
  PreparedExpression expr("@param + col");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
  absl::Status status = expr.Execute(
      {{"col", Value::String("foo")}}, {{"param", Value::Int64(1)}}).status();
  EXPECT_THAT(
      status,
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Expected column parameter 'col' to be of type INT64")));
}

TEST(EvaluatorTest, PrepareExecuteWithTable) {
  SimpleTable test_table(
      "TestTable", {{"a", types::Int64Type()}, {"b", types::StringType()}});
  test_table.SetContents({{Int64(10), String("foo")},
                          {Int64(20), String("bar")},
                          {Int64(30), String("baz")}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);

  PreparedExpression expr("(SELECT a FROM TestTable ORDER BY a LIMIT 1)");
  ZETASQL_ASSERT_OK(expr.Prepare(AnalyzerOptions(), &catalog));
  EXPECT_THAT(expr.Execute(), IsOkAndHolds(Value::Int64(10)));
}

TEST(EvaluatorTest, PrepareTwice) {
  PreparedExpression expr("@param + col");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));
  EXPECT_THAT(expr.Prepare(options),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Prepare called twice")));
}

TEST(EvaluatorTest, ExecuteAfterPrepareWithOrderedParamsWithQueryParameters) {
  PreparedExpression expr("(@param1 + @param2) * col");

  AnalyzerOptions options;
  options.set_parameter_mode(PARAMETER_NAMED);
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param1", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param2", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));

  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_THAT(expr.GetReferencedColumns(), IsOkAndHolds(ElementsAre("col")));
  EXPECT_THAT(expr.GetReferencedParameters(),
              IsOkAndHolds(UnorderedElementsAre("param2", "param1")));
  EXPECT_THAT(expr.ExecuteAfterPrepareWithOrderedParams(
                  {Value::Int64(5)}, {Value::Int64(1), Value::Int64(2)}),
              IsOkAndHolds(Value::Int64(15)));
}

TEST(EvaluatorTest,
     ExecuteAfterPrepareWithOrderedParamsWithPositionalQueryParameters) {
  PreparedExpression expr("(? + ?) * col");

  AnalyzerOptions options;
  options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));

  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_THAT(expr.GetReferencedColumns(), IsOkAndHolds(ElementsAre("col")));
  EXPECT_THAT(expr.GetPositionalParameterCount(), IsOkAndHolds(2));
  EXPECT_THAT(expr.ExecuteAfterPrepareWithOrderedParams(
                  {Value::Int64(5)}, {Value::Int64(1), Value::Int64(2)}),
              IsOkAndHolds(Value::Int64(15)));
}

TEST(EvaluatorTest, ExplainAfterPrepareWithoutPrepare) {
  PreparedExpression expr("@param + col");
  EXPECT_THAT(expr.ExplainAfterPrepare(),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Prepare must be called first")));
}

TEST(EvaluatorTest, ExplainAfterPrepare) {
  PreparedExpression expr("@param + col");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_THAT(expr.ExplainAfterPrepare(),
              IsOkAndHolds("RootExpr(Add($param, $col))"));
}

TEST(EvaluatorTest, GetReferencedParametersAsProperSubset) {
  PreparedExpression expr("@param1 + @param2");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param1", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param2", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param3", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));

  auto status_or_parameters = expr.GetReferencedParameters();
  ZETASQL_ASSERT_OK(status_or_parameters);
  EXPECT_THAT(std::move(status_or_parameters).value(),
              UnorderedElementsAre("param1", "param2"));

  ZETASQL_ASSERT_OK(expr.Execute({}, {{"param1", values::Int64(1)},
        {"param2", values::Int64(2)}}));
}

TEST(EvaluatorTest, GetPositionalParameterCount) {
  PreparedExpression expr("? + ? + ?");
  AnalyzerOptions options;
  options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));

  EXPECT_THAT(expr.GetPositionalParameterCount(),
              ::zetasql_base::testing::IsOkAndHolds(::testing::Eq(3)));

  EXPECT_THAT(expr.ExecuteWithPositionalParams(
                  {}, {values::Int64(1), values::Int64(2), values::Int64(3)}),
              ::zetasql_base::testing::IsOkAndHolds(::testing::Eq(values::Int64(6))));
}

TEST(EvaluatorTest, GetReferencedParametersWithMixedCases) {
  PreparedExpression expr("@PARAM");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddQueryParameter("pArAm", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));

  auto status_or_parameters = expr.GetReferencedParameters();
  ZETASQL_ASSERT_OK(status_or_parameters);
  EXPECT_THAT(std::move(status_or_parameters).value(),
              UnorderedElementsAre("param"));
  ZETASQL_ASSERT_OK(expr.Execute({}, {{"PaRaM", values::Int64(1)}}));
}

TEST(EvaluatorTest, PrepareMismatchColumns) {
  PreparedExpression expr("col0 + col1");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col0", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col2", types::Int64Type()));
  // Set of columns involved in expression must be a subset of those
  // visible to Prepare().
  EXPECT_THAT(expr.Prepare(options),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Unrecognized name: col1")));
}

TEST(EvaluatorTest, GetReferencedColumnsAsProperSubset) {
  PreparedExpression expr("col0 + col1");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col0", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col1", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col2", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));

  auto status_or_columns = expr.GetReferencedColumns();
  ZETASQL_ASSERT_OK(status_or_columns);
  EXPECT_THAT(std::move(status_or_columns).value(),
              UnorderedElementsAre("col0", "col1"));

  auto status_or_value = expr.Execute({{"col0", values::Int64(1)},
    {"col1", values::Int64(2)}});
  ZETASQL_ASSERT_OK(status_or_value);
}

TEST(EvaluatorTest, GetReferencedColumnsInMixedCases) {
  PreparedExpression expr("cOl");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("COL", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));

  auto status_or_columns = expr.GetReferencedColumns();
  ZETASQL_ASSERT_OK(status_or_columns);
  EXPECT_THAT(std::move(status_or_columns).value(),
              UnorderedElementsAre("col"));
  ZETASQL_ASSERT_OK(expr.Execute({{"CoL", values::Int64(1)}}));
}

TEST(EvaluatorTest, GetReferencedColumnsUsingCallback) {
  PreparedExpression expr("col");
  AnalyzerOptions options;
  options.SetLookupExpressionColumnCallback(
      [](const std::string& column_name, const Type** column_type) {
        if (column_name == "col") {
          *column_type = types::Int64Type();
        }
        return absl::OkStatus();
      });
  ZETASQL_ASSERT_OK(expr.Prepare(options));

  auto status_or_columns = expr.GetReferencedColumns();
  ZETASQL_ASSERT_OK(status_or_columns);
  EXPECT_THAT(std::move(status_or_columns).value(),
              UnorderedElementsAre("col"));
  Value result = expr.Execute({{"col", values::Int64(1)}}).value();
  EXPECT_EQ(Value::Int64(1), result);
}

TEST(EvaluatorTest, GetReferencedColumnsUsingCallbackInMixedCases) {
  PreparedExpression expr("cOl");
  AnalyzerOptions options;
  options.SetLookupExpressionColumnCallback(
      [](const std::string& column_name, const Type** column_type) {
        if (column_name == "col") {
          *column_type = types::Int64Type();
        }
        return absl::OkStatus();
      });
  ZETASQL_ASSERT_OK(expr.Prepare(options));

  auto status_or_columns = expr.GetReferencedColumns();
  ZETASQL_ASSERT_OK(status_or_columns);
  EXPECT_THAT(std::move(status_or_columns).value(),
              UnorderedElementsAre("col"));
  Value result = expr.Execute({{"CoL", values::Int64(1)}}).value();
  EXPECT_EQ(Value::Int64(1), result);
}

TEST(EvaluatorTest, GetReferencedColumnsThatAreNotAdded) {
  PreparedExpression expr("col0 + col1");
  ZETASQL_ASSERT_OK(expr.Execute({{"col0", Int64(1)}, {"col1", Int64(2)}}));
  auto status_or_columns = expr.GetReferencedColumns();
  ZETASQL_ASSERT_OK(status_or_columns);
  EXPECT_THAT(std::move(status_or_columns).value(),
              UnorderedElementsAre("col0", "col1"));
}

TEST(EvaluatorTest, GetReferencedColumnsFromExpressionsWithoutColumn) {
  PreparedExpression expr("@param = 2");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));

  auto status_or_columns = expr.GetReferencedColumns();
  ZETASQL_ASSERT_OK(status_or_columns);
  EXPECT_TRUE(std::move(status_or_columns).value().empty());
}

TEST(EvaluatorTest, GetReferencedInScopeColumns) {
  TypeFactory type_factory;
  const ProtoType* proto_type;
  ZETASQL_ASSERT_OK(type_factory.MakeProtoType(
      zetasql_test__::KitchenSinkPB::descriptor(), &proto_type));

  PreparedExpression expr("col0.int64_key_1 + int64_key_2");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.SetInScopeExpressionColumn("col0", proto_type));
  ZETASQL_ASSERT_OK(expr.Prepare(options));

  auto status_or_columns = expr.GetReferencedColumns();
  ZETASQL_ASSERT_OK(status_or_columns);
  EXPECT_THAT(std::move(status_or_columns).value(),
              UnorderedElementsAre("col0"));
}

TEST(EvaluatorTest, GetReferencedColumnsWithUnreferencedInScopeColumns) {
  TypeFactory type_factory;
  const ProtoType* proto_type;
  ZETASQL_ASSERT_OK(type_factory.MakeProtoType(
      zetasql_test__::KitchenSinkPB::descriptor(), &proto_type));

  PreparedExpression expr("col1 = 1");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.SetInScopeExpressionColumn("col0", proto_type));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col1", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));

  auto status_or_columns = expr.GetReferencedColumns();
  ZETASQL_ASSERT_OK(status_or_columns);
  EXPECT_THAT(std::move(status_or_columns).value(),
              UnorderedElementsAre("col1"));
}

TEST(EvaluatorTest, GetReferencedColumnsInLowerCase) {
  PreparedExpression expr("col0 + COL1 + cOL2");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("col0", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("COL1", types::Int64Type()));
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("cOL2", types::Int64Type()));
  ZETASQL_ASSERT_OK(expr.Prepare(options));

  auto status_or_columns = expr.GetReferencedColumns();
  ZETASQL_ASSERT_OK(status_or_columns);
  EXPECT_THAT(std::move(status_or_columns).value(),
              UnorderedElementsAre("col0", "col1", "col2"));
}

TEST(EvaluatorTest, GetReferencedColumnsWithoutPrepare) {
  PreparedExpression expr("@param + col");
  EXPECT_THAT(expr.GetReferencedColumns(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Expression/Query has not been prepared")));
}

TEST(EvaluatorTest, GetReferencedColumnsAfterPrepareFailure) {
  PreparedExpression expr("col0 col1");
  AnalyzerOptions options;  // Empty options
  EXPECT_FALSE(expr.Prepare(options).ok());
  EXPECT_THAT(expr.GetReferencedColumns(),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid prepared expression/query")));
}

TEST(EvaluatorTest, GetReferencedColumnsFromConstantExpression) {
  PreparedExpression expr("1 + 1");
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(expr.Prepare(options));

  auto status_or_columns = expr.GetReferencedColumns();
  ZETASQL_ASSERT_OK(status_or_columns);
  EXPECT_TRUE(std::move(status_or_columns).value().empty());
}

TEST(EvaluatorTest, GetReferencedColumnsAfterExecute) {
  PreparedExpression expr("col1 + col2");
  ZETASQL_ASSERT_OK(expr.Execute(
      {{"col1", values::Int64(1)}, {"col2", values::Int64(2)}}));

  auto status_or_columns = expr.GetReferencedColumns();
  ZETASQL_ASSERT_OK(status_or_columns);
  std::vector<std::string> columns = std::move(status_or_columns).value();
  EXPECT_THAT(columns, UnorderedElementsAre("col1", "col2"));
  // Execute again with the obtained subset.
  ParameterValueMap column_value_map;
  for (const auto& column : columns) {
    column_value_map[column] = values::Int64(1);
  }
  ZETASQL_ASSERT_OK(expr.Execute(column_value_map));
}

TEST(EvaluatorTest, ExpressionValueColumn) {
  TypeFactory type_factory;
  const ProtoType* proto_type;
  ZETASQL_ASSERT_OK(type_factory.MakeProtoType(
      zetasql_test__::KitchenSinkPB::descriptor(), &proto_type));

  zetasql_test__::KitchenSinkPB input_value;
  input_value.set_int64_key_1(5);
  input_value.set_int64_key_2(10);

  // Test using a named in-scope expression column (and a regular expression
  // column).
  {
    PreparedExpression expr(
        "int64_key_1 + value.int64_key_2 + if(has_date, 100, 0) + int_value");

    AnalyzerOptions options;
    ZETASQL_ASSERT_OK(options.AddExpressionColumn("int_value", types::Int32Type()));
    ZETASQL_ASSERT_OK(options.SetInScopeExpressionColumn("value", proto_type));
    ZETASQL_ASSERT_OK(expr.Prepare(options));
    EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));

    Value result =
        expr.Execute({{"int_value", values::Int32(1000)},
                      {"value", values::Proto(proto_type, input_value)}})
            .value();
    EXPECT_EQ(Value::Int64(1015), result);
  }

  // Test using an anonymous in-scope expression column.
  {
    PreparedExpression expr(
        "int64_key_1 + int64_key_2 + if(has_date, 0, 100)");

    AnalyzerOptions options;
    ZETASQL_ASSERT_OK(options.SetInScopeExpressionColumn("", proto_type));
    ZETASQL_ASSERT_OK(expr.Prepare(options));
    EXPECT_TRUE(types::Int64Type()->Equals(expr.output_type()));

    Value result =
        expr.Execute({{"", values::Proto(proto_type, input_value)}}).value();
    EXPECT_EQ(Value::Int64(115), result);
  }

  // Test using anonymous in-scope expression column with implicit Prepare.
  {
    PreparedExpression expr(
        "int64_key_1 + int64_key_2 + if(has_date, 0, 100) + int_value");

    Value result = expr.Execute({{"", values::Proto(proto_type, input_value)},
                                 {"int_value", values::Int32(1000)}})
                       .value();
    EXPECT_EQ(Value::Int64(1115), result);
  }
}

TEST(EvaluatorTest, UnpreparedUnexecutedOutputType) {
  PreparedExpression expr("1 + 2");
  EXPECT_DEATH(expr.output_type(), "Prepare or Execute must be called first");
}

// This tests that if we provide an external TypeFactory, the returned
// value will still be valid after the PreparedExpression is deleted.
TEST(EvaluatorTest, ValueLifetime) {
  TypeFactory type_factory;
  Value value;
  {
    PreparedExpression expr("(1,2)", &type_factory);
    value = expr.Execute().value();
  }
  EXPECT_EQ("STRUCT<INT64, INT64>", value.type()->DebugString());
  EXPECT_EQ("{1, 2}", value.DebugString());
}

TEST(EvaluatorTest, LanguageOptions) {
  // This is meant to test that LanguageOptions make it through to the
  // appropriate place and affect what syntax is accepted and how things
  // behave.  Currently, we don't have anything where the behavior differs, so
  // we just try calling an analytic function, and get a different error
  // depending whether analytic functions are enabled.
  AnalyzerOptions options;

  {
    PreparedExpression expr("1 + RANK()");
    EXPECT_THAT(expr.Prepare(options),
                StatusIs(_, HasSubstr("Function not found: RANK")));
  }

  options.mutable_language()->EnableLanguageFeature(FEATURE_ANALYTIC_FUNCTIONS);
  {
    PreparedExpression expr("1 + RANK()");
    EXPECT_THAT(expr.Prepare(options),
                StatusIs(_, HasSubstr("Analytic function RANK cannot be "
                                      "called without an OVER clause")));
  }
}

TEST(EvaluatorTest, CurrentTimestamp) {
  const absl::Time test_time = absl::FromUnixMicros(1479885478000LL);
  zetasql_base::SimulatedClock clock(test_time);
  EvaluatorOptions evaluator_options;
  evaluator_options.clock = &clock;

  PreparedExpression expr("CURRENT_TIMESTAMP()", evaluator_options);
  Value value = expr.Execute().value();

  EXPECT_EQ(test_time, value.ToTime());
}

absl::Time GetTestTime() {
  absl::TimeZone gst;
  ZETASQL_CHECK(absl::LoadTimeZone("America/Los_Angeles", &gst));
  return absl::FromCivil(absl::CivilSecond(2016, 11, 22, 1, 2, 3), gst);
}

TEST(EvaluatorTest, CurrentDate) {
  zetasql_base::SimulatedClock clock(GetTestTime());
  EvaluatorOptions evaluator_options;
  evaluator_options.clock = &clock;

  PreparedExpression expr("CURRENT_DATE()", evaluator_options);
  Value value = expr.Execute().value();

  int32_t expected;
  ZETASQL_ASSERT_OK(functions::ConstructDate(2016, 11, 22, &expected));
  EXPECT_EQ(expected, value.date_value());
}

TEST(EvaluatorTest, CurrentDateTime) {
  zetasql_base::SimulatedClock clock(GetTestTime());
  EvaluatorOptions evaluator_options;
  evaluator_options.clock = &clock;
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(
      zetasql::LanguageFeature::FEATURE_V_1_2_CIVIL_TIME);

  PreparedExpression expr("CURRENT_DATETIME()", evaluator_options);
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  Value value = expr.Execute().value();

  DatetimeValue expected;
  ZETASQL_ASSERT_OK(functions::ConstructDatetime(2016, 11, 22, 1, 2, 3, &expected));
  EXPECT_EQ(expected.DebugString(), value.datetime_value().DebugString());
}

TEST(EvaluatorTest, CurrentTime) {
  zetasql_base::SimulatedClock clock(GetTestTime());
  EvaluatorOptions evaluator_options;
  evaluator_options.clock = &clock;
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(
      zetasql::LanguageFeature::FEATURE_V_1_2_CIVIL_TIME);

  PreparedExpression expr("CURRENT_TIME()", evaluator_options);
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options));
  Value value = expr.Execute().value();

  TimeValue expected;
  ZETASQL_ASSERT_OK(functions::ConstructTime(1, 2, 3, &expected));
  EXPECT_EQ(expected.DebugString(), value.time_value().DebugString());
}

TEST(EvaluatorTest, DeadlineExceeded) {
  zetasql_base::SimulatedClock clock(GetTestTime());
  EvaluatorOptions evaluator_options;
  evaluator_options.clock = &clock;

  PreparedExpression expr("1 + 2", evaluator_options);
  ZETASQL_ASSERT_OK(expr.Prepare(AnalyzerOptions()));
  // Deadline has passed.
  const absl::Time deadline = GetTestTime() - absl::Seconds(1);

  ExpressionOptions options;
  options.deadline = deadline;
  EXPECT_THAT(expr.Execute(options),
              StatusIs(absl::StatusCode::kResourceExhausted,
                       AllOf(HasSubstr("The statement has been aborted "
                                       "because the statement deadline"),
                             HasSubstr("was exceeded."))));
}

TEST(EvaluatorTest, DeadlineNotExceeded) {
  zetasql_base::SimulatedClock clock(GetTestTime());
  EvaluatorOptions evaluator_options;
  evaluator_options.clock = &clock;

  PreparedExpression expr("1 + 2", evaluator_options);
  ZETASQL_ASSERT_OK(expr.Prepare(AnalyzerOptions()));
  // Deadline will not pass.
  const absl::Time deadline = GetTestTime() + absl::Minutes(1);

  ExpressionOptions options;
  options.deadline = deadline;
  EXPECT_THAT(expr.Execute(options), IsOkAndHolds(Value::Int64(3)));
}

TEST(EvaluatorTest, NoColumnsGiven) {
  PreparedExpression expr("1 + 2", EvaluatorOptions());
  ZETASQL_ASSERT_OK(expr.Prepare(AnalyzerOptions()));
  EXPECT_THAT(expr.Execute(), IsOkAndHolds(Value::Int64(3)));
}

TEST(EvaluatorTest, NamedAndPositionalColumnsGiven) {
  PreparedExpression expr("1 + 2", EvaluatorOptions());
  ZETASQL_ASSERT_OK(expr.Prepare(AnalyzerOptions()));
  ExpressionOptions options;
  options.columns = ParameterValueMap();
  options.ordered_columns = ParameterValueList();
  EXPECT_THAT(
      expr.Execute(options),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr("One of the columns fields has to be set, but not both")));
}

TEST(EvaluatorTest, ParameterizedWithNoParameterMap) {
  auto expr_ptr = MakeResolvedParameter(StringType(), "string_parameter");

  PreparedExpression expr(expr_ptr.get(), EvaluatorOptions());
  EXPECT_THAT(
      expr.Execute({}, {}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Incomplete query parameters string_parameter")));
}

TEST(EvaluatorTest, ForSystemTimeAsOfWithUnsupportedTable) {
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_V_1_1_FOR_SYSTEM_TIME_AS_OF);

  std::unique_ptr<const AnalyzerOutput> analyzer_output;

  const std::string query(
      "(SELECT COUNT(*) FROM KeyValue FOR SYSTEM TIME AS OF '2017-01-01')");
  TypeFactory type_factory;
  SampleCatalog catalog;
  ZETASQL_ASSERT_OK(AnalyzeExpression(query, analyzer_options, catalog.catalog(),
                              &type_factory, &analyzer_output));
  ZETASQL_ASSERT_OK(PopulateSampleTables(&type_factory, &catalog));

  PreparedExpression expr(analyzer_output->resolved_expr(), EvaluatorOptions());
  EXPECT_THAT(
      expr.Execute(),
      StatusIs(
          absl::StatusCode::kUnimplemented,
          HasSubstr("EvaluatorTableIterator::SetReadTime() not implemented")));
}

TEST(EvaluatorTest, ForSystemTimeAsOfWithSupportedTable) {
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_V_1_1_FOR_SYSTEM_TIME_AS_OF);

  std::unique_ptr<const AnalyzerOutput> analyzer_output;

  const std::string query(
      "(SELECT COUNT(*) FROM KeyValueReadTimeIgnored FOR SYSTEM TIME AS OF "
      "'2017-01-01')");
  TypeFactory type_factory;
  SampleCatalog catalog;
  ZETASQL_ASSERT_OK(AnalyzeExpression(query, analyzer_options, catalog.catalog(),
                              &type_factory, &analyzer_output));
  ZETASQL_ASSERT_OK(PopulateSampleTables(&type_factory, &catalog));

  PreparedExpression expr(analyzer_output->resolved_expr(), EvaluatorOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(Value result, expr.Execute());
  ASSERT_EQ(result, Value::Int64(4));
}

TEST(EvaluatorTest, QueryParamInForSystemTimeAsOfExpr) {
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_V_1_1_FOR_SYSTEM_TIME_AS_OF);
  ZETASQL_ASSERT_OK(analyzer_options.AddQueryParameter("query_param",
                                               types::TimestampType()));

  std::unique_ptr<const AnalyzerOutput> analyzer_output;

  const std::string query(
      "(SELECT COUNT(*) FROM KeyValueReadTimeIgnored FOR SYSTEM TIME AS OF "
      "@query_param)");
  TypeFactory type_factory;
  SampleCatalog catalog;
  ZETASQL_ASSERT_OK(AnalyzeExpression(query, analyzer_options, catalog.catalog(),
                              &type_factory, &analyzer_output));
  ZETASQL_ASSERT_OK(PopulateSampleTables(&type_factory, &catalog));

  ParameterValueMap param_values;
  Value param_value = values::Timestamp(
      absl::FromCivil(absl::CivilSecond(2018, 1, 1), absl::UTCTimeZone()));
  zetasql_base::InsertOrDie(&param_values, "query_param", param_value);

  PreparedExpression expr(analyzer_output->resolved_expr(), EvaluatorOptions());
  ZETASQL_ASSERT_OK_AND_ASSIGN(Value result, expr.Execute({}, param_values));
  ASSERT_EQ(result, Value::Int64(4));
}

// Returns a PreparedExpression for SQL. Unlike the other tests in this file,
// this expression is constructed from an AST, rather than by passing SQL
// into the expression.
struct PreparedExpressionFromAST {
  std::unique_ptr<SimpleCatalog> catalog;
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  std::unique_ptr<PreparedExpression> expression;
};
PreparedExpressionFromAST ParseToASTAndPrepareOrDie(
    const std::string& sql, const AnalyzerOptions& analyzer_options,
    TypeFactory* type_factory) {
  PreparedExpressionFromAST prepared_from_ast;
  prepared_from_ast.catalog = absl::make_unique<SimpleCatalog>("foo");
  prepared_from_ast.catalog->AddZetaSQLFunctions();

  ZETASQL_CHECK_OK(AnalyzeExpression(sql, analyzer_options,
                             prepared_from_ast.catalog.get(), type_factory,
                             &prepared_from_ast.analyzer_output));

  EvaluatorOptions evaluator_options;
  evaluator_options.type_factory = type_factory;
  prepared_from_ast.expression = absl::make_unique<PreparedExpression>(
      prepared_from_ast.analyzer_output->resolved_expr(), evaluator_options);
  ZETASQL_CHECK_OK(prepared_from_ast.expression->Prepare(analyzer_options));
  return prepared_from_ast;
}

TEST(EvaluatorTest, PreparedFromAST_Execute) {
  TypeFactory type_factory;
  const ProtoType* proto_type;
  ZETASQL_ASSERT_OK(type_factory.MakeProtoType(
      zetasql_test__::KitchenSinkPB::descriptor(), &proto_type));

  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("int_value", types::Int32Type()));
  ZETASQL_ASSERT_OK(options.SetInScopeExpressionColumn("value", proto_type));

  PreparedExpressionFromAST prepared = ParseToASTAndPrepareOrDie(
      "int64_key_1 + int64_key_2 + int_value", options, &type_factory);

  EXPECT_THAT(prepared.expression->GetReferencedColumns().value(),
              UnorderedElementsAre("value", "int_value"));
  EXPECT_TRUE(types::Int64Type()->Equals(prepared.expression->output_type()))
      << prepared.expression->output_type()->DebugString();

  zetasql_test__::KitchenSinkPB input_value;
  input_value.set_int64_key_1(5);
  input_value.set_int64_key_2(10);
  Value result = prepared.expression
                     ->ExecuteAfterPrepare(
                         {{"int_value", values::Int32(1000)},
                          {"value", values::Proto(proto_type, input_value)}})
                     .value();
  EXPECT_EQ(Value::Int64(1015), result);
}

TEST(EvaluatorTest, PreparedFromAST_ExecuteWithWrongColumn) {
  TypeFactory type_factory;
  AnalyzerOptions options;
  ZETASQL_ASSERT_OK(options.AddExpressionColumn("int_value", types::Int32Type()));

  PreparedExpressionFromAST prepared =
      ParseToASTAndPrepareOrDie("int_value + 2", options, &type_factory);

  EXPECT_THAT(
      prepared.expression->ExecuteAfterPrepare(
          {{"wrong_value", values::Int32(1000)}}),
      zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument,
                                "Incomplete column parameters int_value"));
}

TEST(EvaluatorTest, PreparedFromAST_Parameters) {
  TypeFactory type_factory;
  AnalyzerOptions options;
  options.set_allow_undeclared_parameters(true);

  PreparedExpressionFromAST prepared =
      ParseToASTAndPrepareOrDie("@int_value + 2", options, &type_factory);

  EXPECT_THAT(prepared.expression->GetReferencedParameters().value(),
              UnorderedElementsAre("int_value"));
}

TEST(EvaluatorTest, PreparedFromAST_PositionalParameters) {
  TypeFactory type_factory;
  AnalyzerOptions options;
  options.set_allow_undeclared_parameters(true);
  options.set_parameter_mode(PARAMETER_POSITIONAL);

  PreparedExpressionFromAST prepared =
      ParseToASTAndPrepareOrDie("? + 2 = 5 AND ?", options, &type_factory);

  EXPECT_THAT(prepared.expression->GetPositionalParameterCount(),
              ::zetasql_base::testing::IsOkAndHolds(::testing::Eq(2)));
}

TEST(EvaluatorTest, PreparedFromAST_IllegalDeref) {
  ResolvedColumn fake_column(1, IdString::MakeGlobal("fake_table"),
                             IdString::MakeGlobal("fake_column"),
                             types::Int64Type());
  auto col_ref = MakeResolvedColumnRef(types::Int64Type(), fake_column, false);

  PreparedExpression expression(col_ref.get(), EvaluatorOptions());
  EXPECT_THAT(expression.Prepare(AnalyzerOptions()),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST(EvaluatorTest, ExecuteWithOrderedColumns) {
  PreparedExpression expr("1 + 2", EvaluatorOptions());
  ExpressionOptions options;
  options.ordered_columns = {Value()};
  EXPECT_THAT(
      expr.Execute(options),
      StatusIs(absl::StatusCode::kInternal,
               HasSubstr("`ordered_columns` cannot be set for Execute(). Did "
                         "you mean to call ExecuteAfterPrepare()?")));
}

TEST(EvaluatorTest, ExecuteAfterPrepareOnlyNamedParameters) {
  PreparedExpression expr("@param1");

  AnalyzerOptions options;
  options.set_parameter_mode(PARAMETER_NAMED);
  ZETASQL_ASSERT_OK(options.AddQueryParameter("param1", types::Int64Type()));

  ZETASQL_ASSERT_OK(expr.Prepare(options));
  EXPECT_THAT(expr.GetReferencedParameters(),
              IsOkAndHolds(UnorderedElementsAre("param1")));
  ExpressionOptions expr_options;
  expr_options.parameters = {{"param1", Value::Int64(1)}};

  EXPECT_THAT(expr.ExecuteAfterPrepare(expr_options),
              IsOkAndHolds(Value::Int64(1)));
}

TEST(EvaluatorTest, ExecuteAfterPreparePositonalColumnsNamedParameters) {
  PreparedExpression expr("1 + 2", EvaluatorOptions());
  ZETASQL_ASSERT_OK(expr.Prepare(AnalyzerOptions()));
  ExpressionOptions options;
  options.ordered_columns = ParameterValueList();
  options.parameters = ParameterValueMap();
  EXPECT_THAT(expr.ExecuteAfterPrepare(options),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Expected positional parameters since the "
                                 "columns are positional")));
}

TEST(EvaluatorTest, ResolvedExprValidatedWithCorrectLanguageOptions) {
  // When a resolved tree is passed to the PreparedQuery constructor, it
  // validates the tree. Make sure the correct language options are being
  // plumbed through to the validator.
  //
  // We choose a WITH RECURSIVE query for this, since the validator will give an
  // error if it sees a ResolvedRecursiveScan without the corresopnding language
  // option enabled.
  std::string query = R"(
    ARRAY(WITH RECURSIVE t AS (
      SELECT 1 AS x UNION ALL SELECT x + 1 FROM t WHERE x < 5)
    SELECT * FROM t))";

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_V_1_3_WITH_RECURSIVE);
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_V_1_1_WITH_ON_SUBQUERY);

  SimpleCatalog catalog("TestCatalog");
  catalog.AddZetaSQLFunctions();

  TypeFactory type_factory;

  ZETASQL_ASSERT_OK(AnalyzeExpression(query, analyzer_options, &catalog, &type_factory,
                              &analyzer_output));
  const ResolvedExpr* resolved_expr = analyzer_output->resolved_expr();

  EvaluatorOptions evaluator_options;

  // Try to prepare the query with feature enabled in analyzer options
  PreparedExpression prepared_expr(resolved_expr, evaluator_options);
  ZETASQL_EXPECT_OK(prepared_expr.Prepare(analyzer_options));

  // Now, try to prepare the query again with the same resolved tree, but
  // feature disabled.
  PreparedExpression prepared_expr2(resolved_expr, evaluator_options);
  EXPECT_THAT(prepared_expr2.Prepare(AnalyzerOptions()),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Found recursive scan, but WITH RECURSIVE is "
                                 "disabled in language features")));
}

TEST_F(UDFEvalTest, OkUDFEvaluator) {
  function_options_.set_evaluator([](const absl::Span<const Value> args) {
    // Returns string length as int64_t.
    return Value::Int64(args[0].string_value().size());
  });
  catalog()->AddOwnedFunction(new Function(
      "MyUdf", "udf", Function::SCALAR,
      {{types::Int64Type(), {types::StringType()}, kFunctionId}},
      function_options_));
  PreparedExpression expr("1 + myudf(@param)");
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options_, catalog()));
  Value result = expr.Execute({}, {{"param", Value::String("foo")}}).value();
  EXPECT_EQ(Value::Int64(4), result);
}

TEST_F(UDFEvalTest, OkPolymorphicUDFEvaluator) {
  static FunctionEvaluator evaluator_int64 =
      [](const absl::Span<const Value> args) {
        return Value::Int64(args[0].int64_value() * 2);
      };
  static FunctionEvaluator evaluator_string =
      [](const absl::Span<const Value> args) {
        return Value::Int64(args[0].string_value().size());
      };
  function_options_.set_evaluator_factory(
      [](const FunctionSignature& signature)
          -> absl::StatusOr<FunctionEvaluator> {
        switch (signature.ConcreteArgumentType(0)->kind()) {
          case TYPE_INT64:
            return evaluator_int64;
          case TYPE_STRING:
            return evaluator_string;
          case TYPE_BOOL:
            return FunctionEvaluator();
          default:
            return absl::Status(absl::StatusCode::kInternal,
                                "Beg your pardon: " + signature.DebugString());
        }
      });
  catalog()->AddOwnedFunction(new Function(
      "MyUdf", "udf", Function::SCALAR,
      {{types::Int64Type(), {ARG_TYPE_ANY_1}, kFunctionId}},
      function_options_));
  PreparedExpression expr("1 + myudf(myudf(@param))");
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options_, catalog()));
  Value result = expr.Execute({}, {{"param", Value::String("foo")}}).value();
  EXPECT_EQ(Value::Int64(7), result);  // 7 = 1 + length("foo") * 2

  PreparedExpression expr_double("1 + myudf(1.5)");
  absl::Status status = expr_double.Prepare(analyzer_options_, catalog());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                               HasSubstr("Beg your pardon")));

  PreparedExpression expr_bool("1 + myudf(false)");
  status = expr_bool.Prepare(analyzer_options_, catalog());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                               HasSubstr("NULL evaluator")));
}

TEST_F(UDFEvalTest, UndefinedUDF) {
  PreparedExpression expr("1 + myudf(@param)");
  absl::Status status = expr.Prepare(analyzer_options_);
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("Function not found")));
}

TEST_F(UDFEvalTest, NoUDFEvaluator) {
  catalog()->AddOwnedFunction(new Function(
      "MyUdf", "udf", Function::SCALAR,
      {{types::Int64Type(), {types::StringType()}, kFunctionId}}));
  PreparedExpression expr("1 + myudf(@param)");
  absl::Status status = expr.Prepare(analyzer_options_, catalog());
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInvalidArgument,
                               HasSubstr("has no evaluator")));
}

TEST_F(UDFEvalTest, UDFEvaluatorRuntimeErrors) {
  function_options_.set_evaluator(
      [](const absl::Span<const Value> args) -> absl::StatusOr<Value> {
        std::string arg = args[0].string_value();
        if (arg == "not found") {
          return absl::Status(absl::StatusCode::kNotFound, "Wrong number");
        } else if (arg == "wrong return type") {
          // Returns Value::Int32 instead of Value::Int64.
          return Value::Int32(arg.size());
        } else if (arg == "invalid") {
          return Value::Invalid();
        }
        return Value::Int64(arg.size());
      });
  catalog()->AddOwnedFunction(new Function(
      "MyUdf", "udf", Function::SCALAR,
      {{types::Int64Type(), {types::StringType()}, kFunctionId}},
      function_options_));
  PreparedExpression expr("1 + myudf(@param)");
  ZETASQL_ASSERT_OK(expr.Prepare(analyzer_options_, catalog()));

  // Error-free call.
  Value result = expr.Execute({}, {{"param", Value::String("foo")}}).value();
  EXPECT_EQ(Value::Int64(4), result);

  // Runtime errors.
  absl::Status status;
  status = expr.Execute(
      {}, {{"param", Value::String("not found")}}).status();
  EXPECT_THAT(status,
              StatusIs(absl::StatusCode::kNotFound, HasSubstr("Wrong number")));

  status = expr.Execute(
      {}, {{"param", Value::String("invalid")}}).status();
  EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                               HasSubstr("Uninitialized value")));

  if (ZETASQL_DEBUG_MODE) {
    status = expr.Execute(
        {}, {{"param", Value::String("wrong return type")}}).status();
    EXPECT_THAT(status, StatusIs(absl::StatusCode::kInternal,
                                 HasSubstr("Expected value of type: INT64")));
  } else {
    EXPECT_DEATH(
        expr.Execute({}, {{"param", Value::String("wrong return type")}})
            .IgnoreError(),
        "Not an int64");
  }
}

TEST(PreparedQuery, ExpressionQuery) {
  PreparedQuery query("select 1 a, 2 b, 'abc'", EvaluatorOptions());
  ZETASQL_EXPECT_OK(query.Prepare(AnalyzerOptions()));

  EXPECT_EQ(3, query.num_columns());
  EXPECT_EQ("a", query.column_name(0));
  EXPECT_EQ("b", query.column_name(1));
  EXPECT_EQ("", query.column_name(2));
  EXPECT_EQ("INT64", query.column_type(0)->DebugString());
  EXPECT_EQ("INT64", query.column_type(1)->DebugString());
  EXPECT_EQ("STRING", query.column_type(2)->DebugString());

  std::vector<PreparedQuery::NameAndType> columns = query.GetColumns();
  EXPECT_EQ(3, columns.size());
  EXPECT_EQ("a", columns[0].first);
  EXPECT_EQ("b", columns[1].first);
  // Anonymous columns get empty names.
  EXPECT_EQ("", columns[2].first);
  EXPECT_EQ("INT64", columns[0].second->DebugString());
  EXPECT_EQ("INT64", columns[1].second->DebugString());
  EXPECT_EQ("STRING", columns[2].second->DebugString());

  EXPECT_FALSE(query.is_value_table());
  EXPECT_EQ(3, query.resolved_query_stmt()->output_column_list_size());

  EXPECT_THAT(query.GetReferencedParameters(), IsOkAndHolds(IsEmpty()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.Execute());

  EXPECT_EQ(3, iter->NumColumns());
  EXPECT_EQ("a", iter->GetColumnName(0));
  EXPECT_EQ("b", iter->GetColumnName(1));
  // Anonymous columns get empty names.
  EXPECT_EQ("", iter->GetColumnName(2));
  EXPECT_EQ("INT64", iter->GetColumnType(0)->DebugString());
  EXPECT_EQ("INT64", iter->GetColumnType(1)->DebugString());
  EXPECT_EQ("STRING", iter->GetColumnType(2)->DebugString());

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(1), iter->GetValue(0));
  EXPECT_EQ(Int64(2), iter->GetValue(1));
  EXPECT_EQ(String("abc"), iter->GetValue(2));

  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST(PreparedQuery, NontrivialOutputColumnNames) {
  // Query adapted from b/123093575.
  const std::string query_str =
      R"(SELECT
  A.x AS y,
  B.x AS z,
  A.x,
  B.x
FROM
  (
    SELECT
      *
    FROM
      ((
        SELECT
          1 AS x
        ) UNION ALL(
        SELECT
          2 AS x
        )
      )
  ) AS A,
  (
    SELECT
      *
    FROM
      ((
        SELECT
          1 AS x
        ) UNION ALL(
        SELECT
          2 AS x
        )
      )
  ) AS B)";

  PreparedQuery query(query_str, EvaluatorOptions());
  ZETASQL_EXPECT_OK(query.Prepare(AnalyzerOptions()));

  EXPECT_EQ(4, query.num_columns());
  EXPECT_EQ("y", query.column_name(0));
  EXPECT_EQ("z", query.column_name(1));
  EXPECT_EQ("x", query.column_name(2));
  EXPECT_EQ("x", query.column_name(3));
  EXPECT_EQ("INT64", query.column_type(0)->DebugString());
  EXPECT_EQ("INT64", query.column_type(1)->DebugString());
  EXPECT_EQ("INT64", query.column_type(2)->DebugString());
  EXPECT_EQ("INT64", query.column_type(3)->DebugString());

  std::vector<PreparedQuery::NameAndType> columns = query.GetColumns();
  EXPECT_EQ(4, columns.size());
  EXPECT_EQ("y", columns[0].first);
  EXPECT_EQ("z", columns[1].first);
  EXPECT_EQ("INT64", columns[0].second->DebugString());
  EXPECT_EQ("INT64", columns[1].second->DebugString());
  EXPECT_EQ("INT64", columns[2].second->DebugString());
  EXPECT_EQ("INT64", columns[3].second->DebugString());

  EXPECT_FALSE(query.is_value_table());
  EXPECT_EQ(4, query.resolved_query_stmt()->output_column_list_size());

  EXPECT_THAT(query.GetReferencedParameters(), IsOkAndHolds(IsEmpty()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.Execute());

  EXPECT_EQ(4, iter->NumColumns());
  EXPECT_EQ("y", iter->GetColumnName(0));
  EXPECT_EQ("z", iter->GetColumnName(1));
  EXPECT_EQ("x", iter->GetColumnName(2));
  EXPECT_EQ("x", iter->GetColumnName(3));
  EXPECT_EQ("INT64", iter->GetColumnType(0)->DebugString());
  EXPECT_EQ("INT64", iter->GetColumnType(1)->DebugString());
  EXPECT_EQ("INT64", iter->GetColumnType(2)->DebugString());
  EXPECT_EQ("INT64", iter->GetColumnType(3)->DebugString());

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(1), iter->GetValue(0));
  EXPECT_EQ(Int64(1), iter->GetValue(1));
  EXPECT_EQ(Int64(1), iter->GetValue(2));
  EXPECT_EQ(Int64(1), iter->GetValue(3));

  EXPECT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(1), iter->GetValue(0));
  EXPECT_EQ(Int64(2), iter->GetValue(1));
  EXPECT_EQ(Int64(1), iter->GetValue(2));
  EXPECT_EQ(Int64(2), iter->GetValue(3));

  EXPECT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(2), iter->GetValue(0));
  EXPECT_EQ(Int64(1), iter->GetValue(1));
  EXPECT_EQ(Int64(2), iter->GetValue(2));
  EXPECT_EQ(Int64(1), iter->GetValue(3));

  EXPECT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(2), iter->GetValue(0));
  EXPECT_EQ(Int64(2), iter->GetValue(1));
  EXPECT_EQ(Int64(2), iter->GetValue(2));
  EXPECT_EQ(Int64(2), iter->GetValue(3));

  ZETASQL_EXPECT_OK(iter->Status());
}

TEST(PreparedQuery, FromTable) {
  SimpleTable test_table(
      "TestTable", {{"a", types::Int64Type()}, {"b", types::StringType()}});
  test_table.SetContents({{Int64(10), String("foo")},
                          {Int64(20), String("bar")},
                          {Int64(30), String("baz")}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);

  PreparedQuery query("select a, b from TestTable", EvaluatorOptions());
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), &catalog));

  EXPECT_EQ(2, query.num_columns());
  EXPECT_EQ("a", query.column_name(0));
  EXPECT_EQ("b", query.column_name(1));
  EXPECT_EQ("INT64", query.column_type(0)->DebugString());
  EXPECT_EQ("STRING", query.column_type(1)->DebugString());

  EXPECT_FALSE(query.is_value_table());
  EXPECT_EQ(2, query.resolved_query_stmt()->output_column_list_size());

  EXPECT_THAT(query.GetReferencedParameters(), IsOkAndHolds(IsEmpty()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.Execute());
  EXPECT_EQ(2, iter->NumColumns());
  EXPECT_EQ("a", iter->GetColumnName(0));
  EXPECT_EQ("b", iter->GetColumnName(1));
  EXPECT_EQ("INT64", iter->GetColumnType(0)->DebugString());
  EXPECT_EQ("STRING", iter->GetColumnType(1)->DebugString());

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(10), iter->GetValue(0));
  EXPECT_EQ(String("foo"), iter->GetValue(1));

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(20), iter->GetValue(0));
  EXPECT_EQ(String("bar"), iter->GetValue(1));

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(30), iter->GetValue(0));
  EXPECT_EQ(String("baz"), iter->GetValue(1));

  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST(PreparedQuery, PrepareExecuteMissingQueryParameter) {
  SimpleTable test_table(
      "TestTable", {{"col", types::Int64Type()}});
  test_table.SetContents({{Int64(5)}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);
  catalog.AddZetaSQLFunctions();

  EvaluatorOptions evaluator_options;
  PreparedQuery query("SELECT @param + col FROM TestTable", evaluator_options);
  EXPECT_THAT(query.Prepare(AnalyzerOptions(), &catalog),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Query parameter 'param' not found")));
}

TEST(PreparedQuery, PrepareExecuteAllowUndeclaredQueryParameters) {
  SimpleTable test_table(
      "TestTable", {{"col", types::Int64Type()}});
  test_table.SetContents({{Int64(5)}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);
  catalog.AddZetaSQLFunctions();

  EvaluatorOptions evaluator_options;
  PreparedQuery query("SELECT @param + col FROM TestTable", evaluator_options);

  AnalyzerOptions analyzer_options;
  analyzer_options.set_allow_undeclared_parameters(true);
  ZETASQL_ASSERT_OK(query.Prepare(analyzer_options, &catalog));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<std::string> params,
                       query.GetReferencedParameters());
  EXPECT_THAT(params, testing::ElementsAre("param"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.Execute({{"param", Value::Int64(6)}}));
  EXPECT_EQ(1, iter->NumColumns());
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(11), iter->GetValue(0));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST(PreparedQuery, PrepareExecuteAllowUndeclaredQueryParametersResolvedStmt) {
  SimpleTable test_table(
      "TestTable", {{"col", types::Int64Type()}});
  test_table.SetContents({{Int64(5)}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);
  catalog.AddZetaSQLFunctions();

  AnalyzerOptions analyzer_options;
  analyzer_options.set_allow_undeclared_parameters(true);

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  TypeFactory type_factory;
  ZETASQL_ASSERT_OK(AnalyzeStatement("SELECT @param + col FROM TestTable",
                             analyzer_options, &catalog, &type_factory,
                             &analyzer_output));
  ASSERT_TRUE(analyzer_output->resolved_statement()->Is<ResolvedQueryStmt>());

  EvaluatorOptions evaluator_options;
  PreparedQuery query(
      analyzer_output->resolved_statement()->GetAs<ResolvedQueryStmt>(),
      evaluator_options);

  ZETASQL_ASSERT_OK(query.Prepare(analyzer_options, &catalog));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<std::string> params,
                       query.GetReferencedParameters());
  EXPECT_THAT(params, testing::ElementsAre("param"));
  EXPECT_THAT(query.Execute({{"param", Value::Int64(6)}}),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr("Expected type not found for variable param")));
}

TEST(PreparedQuery, PrepareExecuteMissingPositionalQueryParameter) {
  SimpleTable test_table(
      "TestTable", {{"col", types::Int64Type()}});
  test_table.SetContents({{Int64(5)}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);
  catalog.AddZetaSQLFunctions();

  EvaluatorOptions evaluator_options;
  PreparedQuery query("SELECT ? + col FROM TestTable", evaluator_options);
  AnalyzerOptions options;
  options.set_parameter_mode(PARAMETER_POSITIONAL);
  EXPECT_THAT(query.Prepare(options, &catalog),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Query parameter number 1 is not defined")));
}

TEST(PreparedQuery, PrepareExecuteAllowUndeclaredPositionalQueryParameters) {
  SimpleTable test_table(
      "TestTable", {{"col", types::Int64Type()}});
  test_table.SetContents({{Int64(5)}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);
  catalog.AddZetaSQLFunctions();

  EvaluatorOptions evaluator_options;
  PreparedQuery query(
      "SELECT CONCAT(CAST((? + col) AS STRING), ?) FROM TestTable",
      evaluator_options);
  AnalyzerOptions analyzer_options;
  analyzer_options.set_allow_undeclared_parameters(true);
  analyzer_options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_ASSERT_OK(query.Prepare(analyzer_options, &catalog));
  ZETASQL_ASSERT_OK_AND_ASSIGN(int count, query.GetPositionalParameterCount());
  EXPECT_EQ(2, count);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EvaluatorTableIterator> iter,
      query.ExecuteWithPositionalParams({Value::Int64(6),
        Value::StringValue("foo")}));
  EXPECT_EQ(1, iter->NumColumns());
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Value::StringValue("11foo"), iter->GetValue(0));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST(PreparedQuery,
     PrepareExecuteAllowUndeclaredPositionalQueryParametersResolvedStmt) {
  SimpleTable test_table(
      "TestTable", {{"col", types::Int64Type()}});
  test_table.SetContents({{Int64(5)}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);
  catalog.AddZetaSQLFunctions();

  AnalyzerOptions analyzer_options;
  analyzer_options.set_allow_undeclared_parameters(true);
  analyzer_options.set_parameter_mode(PARAMETER_POSITIONAL);

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  TypeFactory type_factory;
  ZETASQL_ASSERT_OK(AnalyzeStatement(
      "SELECT CONCAT(CAST((? + col) AS STRING), ?) FROM TestTable",
      analyzer_options, &catalog, &type_factory,
      &analyzer_output));
  ASSERT_TRUE(analyzer_output->resolved_statement()->Is<ResolvedQueryStmt>());

  EvaluatorOptions evaluator_options;
  PreparedQuery query(
      analyzer_output->resolved_statement()->GetAs<ResolvedQueryStmt>(),
      evaluator_options);
  ZETASQL_ASSERT_OK(query.Prepare(analyzer_options, &catalog));
  ZETASQL_ASSERT_OK_AND_ASSIGN(int count, query.GetPositionalParameterCount());
  EXPECT_EQ(2, count);
  EXPECT_THAT(query.ExecuteWithPositionalParams({Value::Int64(6),
    Value::StringValue("foo")}),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr("Mismatch in number of analyzer parameters versus "
                    "algebrizer parameters")));
}

TEST(PreparedQuery, ResolvedQueryValidatedWithCorrectLanguageOptions) {
  // When a resolved tree is passed to the PreparedQuery constructor, it
  // validates the tree. Make sure the correct language options are being
  // plumbed through to the validator.
  //
  // We choose a WITH RECURSIVE query for this, since the validator will give an
  // error if it sees a ResolvedRecursiveScan without the corresopnding language
  // option enabled.
  std::string query = R"(
    WITH RECURSIVE t AS (
      SELECT 1 AS x UNION ALL SELECT x + 1 FROM t WHERE x < 5)
    SELECT * FROM t;)";

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  AnalyzerOptions analyzer_options;
  analyzer_options.mutable_language()->EnableLanguageFeature(
      FEATURE_V_1_3_WITH_RECURSIVE);

  SimpleCatalog catalog("TestCatalog");
  catalog.AddZetaSQLFunctions();

  TypeFactory type_factory;

  ZETASQL_ASSERT_OK(AnalyzeStatement(query, analyzer_options, &catalog, &type_factory,
                             &analyzer_output));
  const ResolvedQueryStmt* query_stmt =
      analyzer_output->resolved_statement()->GetAs<ResolvedQueryStmt>();

  EvaluatorOptions evaluator_options;

  // Try to prepare the query with feature enabled in analyzer options
  PreparedQuery prepared_query(query_stmt, evaluator_options);
  ZETASQL_EXPECT_OK(prepared_query.Prepare(analyzer_options));

  // Now, try to prepare the query again with the same resolved tree, but
  // feature disabled.
  PreparedQuery prepared_query2(query_stmt, evaluator_options);
  EXPECT_THAT(prepared_query2.Prepare(AnalyzerOptions()),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Found recursive scan, but WITH RECURSIVE is "
                                 "disabled in language features")));
}

class PreparedModifyTest : public ::testing::Test {
 public:
  void SetUp() override {
    catalog_.AddZetaSQLFunctions();
    AddNonValueTable();
    AddValueTableInt64RowType();

    analyzer_options_.mutable_language()->SetSupportsAllStatementKinds();
  }

  Catalog* catalog() { return &catalog_; }
  const AnalyzerOptions& analyzer_options() { return analyzer_options_; }

 protected:
  void AddNonValueTable() {
    auto test_table = absl::make_unique<SimpleTable>(
        kTestTable,
        std::vector<SimpleTable::NameAndType>{
            {"int_val", types::Int64Type()}, {"str_val", types::StringType()}});
    test_table->SetContents({{Int64(1), String("one")},
                            {Int64(2), String("two")},
                            {Int64(4), String("four")}});
    ZETASQL_ASSERT_OK(test_table->SetPrimaryKey({0}));
    catalog_.AddOwnedTable(std::move(test_table));
  }

  void AddValueTableInt64RowType() {
    auto test_value_table = absl::make_unique<SimpleTable>(
        kTestValueTable,
        std::vector<SimpleTable::NameAndType>{{"int_val", types::Int64Type()}});
    test_value_table->SetContents({{Int64(1)}, {Int64(2)}, {Int64(4)}});
    ZETASQL_ASSERT_OK(test_value_table->SetPrimaryKey({0}));
    test_value_table->set_is_value_table(true);
    catalog_.AddOwnedTable(std::move(test_value_table));
  }

  SimpleCatalog catalog_{"test_catalog"};
  AnalyzerOptions analyzer_options_;

  // Table names
  static constexpr char kTestTable[] = "test_table";
  static constexpr char kTestValueTable[] = "test_value_table";

  const StructType* test_value_table_struct_row_type_;
};

TEST_F(PreparedModifyTest, ExecutesInsert) {
  PreparedModify modify(
      "insert test_table(int_val, str_val) values(3, 'three')",
      EvaluatorOptions());
  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options(), catalog()));
  ASSERT_EQ(modify.resolved_statement()->node_kind(), RESOLVED_INSERT_STMT);
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableModifyIterator> iter,
                       modify.Execute());
  const Table* table;
  ZETASQL_ASSERT_OK(catalog()->FindTable({"test_table"}, &table));

  EXPECT_EQ(iter->table(), table);
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(3));
  EXPECT_EQ(iter->GetColumnValue(1), String("three"));
  EXPECT_FALSE(iter->GetOriginalKeyValue(0).is_valid());
  EXPECT_EQ(iter->GetOperation(),
            EvaluatorTableModifyIterator::Operation::kInsert);

  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST_F(PreparedModifyTest, ExecutesInsertToValueTable) {
  PreparedModify modify("insert test_value_table(int_val) values(3)",
                        EvaluatorOptions());
  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options(), catalog()));
  ASSERT_EQ(modify.resolved_statement()->node_kind(), RESOLVED_INSERT_STMT);
  ASSERT_THAT(
      modify.Execute(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "PreparedModify api does not support modifying value tables"));
}

TEST_F(PreparedModifyTest, ExecutesDelete) {
  PreparedModify modify("delete test_table where int_val in (2, 4)",
                      EvaluatorOptions());
  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options(), catalog()));
  ASSERT_EQ(modify.resolved_statement()->node_kind(), RESOLVED_DELETE_STMT);
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableModifyIterator> iter,
                       modify.Execute());
  const Table* table;
  ZETASQL_ASSERT_OK(catalog()->FindTable({"test_table"}, &table));

  EXPECT_EQ(iter->table(), table);

  ASSERT_TRUE(iter->NextRow());
  EXPECT_FALSE(iter->GetColumnValue(0).is_valid());
  EXPECT_FALSE(iter->GetColumnValue(1).is_valid());
  EXPECT_EQ(iter->GetOriginalKeyValue(0), Int64(2));
  EXPECT_EQ(iter->GetOperation(),
            EvaluatorTableModifyIterator::Operation::kDelete);

  ASSERT_TRUE(iter->NextRow());
  EXPECT_FALSE(iter->GetColumnValue(0).is_valid());
  EXPECT_FALSE(iter->GetColumnValue(1).is_valid());
  EXPECT_EQ(iter->GetOriginalKeyValue(0), Int64(4));
  EXPECT_EQ(iter->GetOperation(),
            EvaluatorTableModifyIterator::Operation::kDelete);

  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST_F(PreparedModifyTest, ExecutesDeleteFromValueTable) {
  PreparedModify modify(
      "delete test_value_table where test_value_table in (2, 4)",
      EvaluatorOptions());
  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options(), catalog()));
  ASSERT_EQ(modify.resolved_statement()->node_kind(), RESOLVED_DELETE_STMT);
  ASSERT_THAT(
      modify.Execute(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "PreparedModify api does not support modifying value tables"));
}

TEST_F(PreparedModifyTest, ExecutesUpdateOnValueTable) {
  PreparedModify modify(
      "update test_value_table set test_value_table = 2 WHERE TRUE",
      EvaluatorOptions());
  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options(), catalog()));
  ASSERT_EQ(modify.resolved_statement()->node_kind(), RESOLVED_UPDATE_STMT);
  ASSERT_THAT(
      modify.Execute(),
      StatusIs(absl::StatusCode::kInvalidArgument,
               "PreparedModify api does not support modifying value tables"));
}

TEST_F(PreparedModifyTest, ExecutesUpdate) {
  PreparedModify modify(
      "update test_table set str_val = 'foo' where int_val > 1",
      EvaluatorOptions());
  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options(), catalog()));
  ASSERT_EQ(modify.resolved_statement()->node_kind(), RESOLVED_UPDATE_STMT);
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableModifyIterator> iter,
                       modify.Execute());
  const Table* table;
  ZETASQL_ASSERT_OK(catalog()->FindTable({"test_table"}, &table));

  EXPECT_EQ(iter->table(), table);

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(2));
  EXPECT_EQ(iter->GetColumnValue(1), String("foo"));
  EXPECT_EQ(iter->GetOriginalKeyValue(0), Int64(2));
  EXPECT_EQ(iter->GetOperation(),
            EvaluatorTableModifyIterator::Operation::kUpdate);

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(4));
  EXPECT_EQ(iter->GetColumnValue(1), String("foo"));
  EXPECT_EQ(iter->GetOriginalKeyValue(0), Int64(4));
  EXPECT_EQ(iter->GetOperation(),
            EvaluatorTableModifyIterator::Operation::kUpdate);

  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST_F(PreparedModifyTest, IteratorStillLiveOnDestruction) {
  auto query = absl::make_unique<PreparedModify>(
      "delete from test_table where true", EvaluatorOptions());
  ZETASQL_ASSERT_OK(query->Prepare(analyzer_options(), catalog()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableModifyIterator> iter,
                       query->Execute());
  EXPECT_DEATH(query.reset(), "cannot outlive the PreparedQuery object");
}

TEST_F(PreparedModifyTest, InvalidStatementKind) {
  PreparedModify modify("select * from test_table", EvaluatorOptions());
  EXPECT_THAT(modify.Prepare(analyzer_options(), catalog()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("not correspond to a DML statement")));
}

TEST_F(PreparedModifyTest, Parameter) {
  PreparedModify modify("insert test_table(int_val, str_val) values(@p1, @p2)",
                        EvaluatorOptions());

  AnalyzerOptions analyzer_options = PreparedModifyTest::analyzer_options();
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p1", types::Int64Type()));
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p2", types::StringType()));
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p3", types::DoubleType()));

  ZETASQL_EXPECT_OK(modify.Prepare(analyzer_options, catalog()));

  EXPECT_THAT(modify.GetReferencedParameters(),
              IsOkAndHolds(UnorderedElementsAre("p1", "p2")));
  EXPECT_THAT(modify.GetPositionalParameterCount(), IsOkAndHolds(0));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EvaluatorTableModifyIterator> iter,
      modify.Execute({{"p2", String("three")}, {"p1", Int64(3)}}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(3));
  EXPECT_EQ(iter->GetColumnValue(1), String("three"));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  ZETASQL_ASSERT_OK_AND_ASSIGN(iter, modify.Execute({{"p2", NullString()},
                                             {"p1", Int64(0)},
                                             {"p3", NullDouble()},
                                             {"p4", NullBytes()}}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(0));
  EXPECT_EQ(iter->GetColumnValue(1), NullString());
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  EXPECT_THAT(modify.Execute({{"p2", values::NullString()}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Incomplete query parameters p1")));
}

TEST_F(PreparedModifyTest, NamedParameter) {
  PreparedModify modify(
      "insert test_table(int_val, str_val) values(@param1, @param2)",
      EvaluatorOptions());

  AnalyzerOptions analyzer_options = PreparedModifyTest::analyzer_options();
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("param1", types::Int64Type()));
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("param2", types::StringType()));

  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options, catalog()));

  EXPECT_THAT(modify.GetReferencedParameters(),
              IsOkAndHolds(ElementsAre("param1", "param2")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EvaluatorTableModifyIterator> iter,
      modify.Execute({{"param1", Int64(3)}, {"param2", String("three")}}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(3));
  EXPECT_EQ(iter->GetColumnValue(1), String("three"));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST_F(PreparedModifyTest, UndeclaredNamedParameter) {
  PreparedModify modify(
      "insert test_table(int_val, str_val) values(@param1, @param2)",
      EvaluatorOptions());

  AnalyzerOptions analyzer_options = PreparedModifyTest::analyzer_options();
  analyzer_options.set_allow_undeclared_parameters(true);

  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options, catalog()));

  EXPECT_THAT(modify.GetReferencedParameters(),
              IsOkAndHolds(ElementsAre("param1", "param2")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EvaluatorTableModifyIterator> iter,
      modify.Execute({{"param1", Int64(3)}, {"param2", String("three")}}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(3));
  EXPECT_EQ(iter->GetColumnValue(1), String("three"));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST_F(PreparedModifyTest, UndeclaredNamedParameterResolvedStmt) {
  AnalyzerOptions analyzer_options = PreparedModifyTest::analyzer_options();
  analyzer_options.set_allow_undeclared_parameters(true);

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  TypeFactory type_factory;
  ZETASQL_ASSERT_OK(AnalyzeStatement(
      "insert test_table(int_val, str_val) values(@param1, @param2)",
      analyzer_options, catalog(), &type_factory,
      &analyzer_output));

  PreparedModify modify(analyzer_output->resolved_statement(),
                        EvaluatorOptions());
  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options, catalog()));

  EXPECT_THAT(modify.GetReferencedParameters(),
              IsOkAndHolds(ElementsAre("param1", "param2")));

  EXPECT_THAT(
      modify.Execute({{"param1", Int64(3)}, {"param2", String("three")}}),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr("Expected type not found for variable param1")));
}

TEST_F(PreparedModifyTest, PositionalParameter) {
  PreparedModify modify("insert test_table(int_val, str_val) values(?, ?)",
                        EvaluatorOptions());

  AnalyzerOptions analyzer_options = PreparedModifyTest::analyzer_options();
  analyzer_options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_EXPECT_OK(analyzer_options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_EXPECT_OK(analyzer_options.AddPositionalQueryParameter(types::StringType()));
  ZETASQL_EXPECT_OK(analyzer_options.AddPositionalQueryParameter(types::DoubleType()));

  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options, catalog()));

  EXPECT_THAT(modify.GetReferencedParameters(), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(modify.GetPositionalParameterCount(), IsOkAndHolds(2));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EvaluatorTableModifyIterator> iter,
      modify.ExecuteWithPositionalParams({Int64(3), String("three")}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(3));
  EXPECT_EQ(iter->GetColumnValue(1), String("three"));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, modify.ExecuteWithPositionalParams(
                {Int64(0), NullString(), NullDouble(), NullBytes()}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(0));
  EXPECT_EQ(iter->GetColumnValue(1), NullString());
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  EXPECT_THAT(modify.ExecuteWithPositionalParams({Int64(100)}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Incorrect number of positional parameters")));
}

TEST_F(PreparedModifyTest, UndeclaredPositionalParameter) {
  PreparedModify modify("insert test_table(int_val, str_val) values(?, ?)",
                        EvaluatorOptions());

  AnalyzerOptions analyzer_options = PreparedModifyTest::analyzer_options();
  analyzer_options.set_allow_undeclared_parameters(true);
  analyzer_options.set_parameter_mode(PARAMETER_POSITIONAL);

  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options, catalog()));

  EXPECT_THAT(modify.GetReferencedParameters(), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(modify.GetPositionalParameterCount(), IsOkAndHolds(2));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EvaluatorTableModifyIterator> iter,
      modify.ExecuteWithPositionalParams({Int64(3), String("three")}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(3));
  EXPECT_EQ(iter->GetColumnValue(1), String("three"));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST_F(PreparedModifyTest, UndeclaredPositionalParameterResolvedStmt) {
  AnalyzerOptions analyzer_options = PreparedModifyTest::analyzer_options();
  analyzer_options.set_allow_undeclared_parameters(true);
  analyzer_options.set_parameter_mode(PARAMETER_POSITIONAL);

  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  TypeFactory type_factory;
  ZETASQL_ASSERT_OK(AnalyzeStatement("insert test_table(int_val, str_val) values(?, ?)",
                             analyzer_options, catalog(), &type_factory,
                             &analyzer_output));

  PreparedModify modify(analyzer_output->resolved_statement(),
                        EvaluatorOptions());
  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options, catalog()));

  EXPECT_THAT(modify.GetReferencedParameters(), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(modify.GetPositionalParameterCount(), IsOkAndHolds(2));

  EXPECT_THAT(
      modify.ExecuteWithPositionalParams({Int64(3), String("three")}),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr("Mismatch in number of analyzer parameters versus "
                    "algebrizer parameters")));
}

TEST_F(PreparedModifyTest, ExecuteAfterPrepareWithOrderedParamsWithParameter) {
  PreparedModify modify("insert test_table(int_val, str_val) values(@p1, @p2)",
                        EvaluatorOptions());

  AnalyzerOptions analyzer_options = PreparedModifyTest::analyzer_options();
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p1", types::Int64Type()));
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p2", types::StringType()));
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p3", types::DoubleType()));

  ZETASQL_EXPECT_OK(modify.Prepare(analyzer_options, catalog()));

  EXPECT_THAT(modify.GetReferencedParameters(),
              IsOkAndHolds(UnorderedElementsAre("p1", "p2")));
  EXPECT_THAT(modify.GetPositionalParameterCount(), IsOkAndHolds(0));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EvaluatorTableModifyIterator> iter,
      modify.Execute({{"p2", String("three")}, {"p1", Int64(3)}}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(3));
  EXPECT_EQ(iter->GetColumnValue(1), String("three"));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  ZETASQL_ASSERT_OK_AND_ASSIGN(iter, modify.Execute({{"p2", NullString()},
                                             {"p1", Int64(0)},
                                             {"p3", NullDouble()},
                                             {"p4", NullBytes()}}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(0));
  EXPECT_EQ(iter->GetColumnValue(1), NullString());
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  EXPECT_THAT(modify.Execute({{"p2", values::NullString()}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Incomplete query parameters p1")));
}

TEST_F(PreparedModifyTest,
     ExecuteAfterPreparedWithOrderedParamsWithPositionalParameter) {
  PreparedModify modify("insert test_table(int_val, str_val) values(?, ?)",
                        EvaluatorOptions());

  AnalyzerOptions analyzer_options = PreparedModifyTest::analyzer_options();
  analyzer_options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_EXPECT_OK(analyzer_options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_EXPECT_OK(analyzer_options.AddPositionalQueryParameter(types::StringType()));
  ZETASQL_EXPECT_OK(analyzer_options.AddPositionalQueryParameter(types::DoubleType()));

  ZETASQL_ASSERT_OK(modify.Prepare(analyzer_options, catalog()));

  EXPECT_THAT(modify.GetReferencedParameters(), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(modify.GetPositionalParameterCount(), IsOkAndHolds(2));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EvaluatorTableModifyIterator> iter,
      modify.ExecuteWithPositionalParams({Int64(3), String("three")}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(3));
  EXPECT_EQ(iter->GetColumnValue(1), String("three"));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  // Calling the ExecuteAfterPrepare variant.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, modify.ExecuteAfterPrepareWithOrderedParams(
                {Int64(0), NullString(), NullDouble(), NullBytes()}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetColumnValue(0), Int64(0));
  EXPECT_EQ(iter->GetColumnValue(1), NullString());
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  EXPECT_THAT(modify.ExecuteWithPositionalParams({Int64(100)}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Incorrect number of positional parameters")));
}

TEST_F(PreparedModifyTest, ExplainAfterPrepareWithoutPrepare) {
  PreparedModify modify("insert test_table(int_val, str_val) values(0, null)",
                        EvaluatorOptions());
  EXPECT_THAT(modify.ExplainAfterPrepare(),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Prepare must be called first")));
}

TEST(PreparedQuery, FromTableOnlySecondColumn) {
  SimpleTable test_table(
      "TestTable", {{"a", types::Int64Type()}, {"b", types::StringType()}});
  test_table.SetContents({{Int64(10), String("foo")},
                          {Int64(20), String("bar")},
                          {Int64(30), String("baz")}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);

  PreparedQuery query("select b from TestTable", EvaluatorOptions());
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), &catalog));

  EXPECT_EQ(1, query.num_columns());
  EXPECT_EQ("b", query.column_name(0));
  EXPECT_EQ("STRING", query.column_type(0)->DebugString());

  EXPECT_FALSE(query.is_value_table());
  EXPECT_EQ(1, query.resolved_query_stmt()->output_column_list_size());

  EXPECT_THAT(query.GetReferencedParameters(), IsOkAndHolds(IsEmpty()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.Execute());
  EXPECT_EQ(1, iter->NumColumns());
  EXPECT_EQ("b", iter->GetColumnName(0));
  EXPECT_EQ("STRING", iter->GetColumnType(0)->DebugString());

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(String("foo"), iter->GetValue(0));

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(String("bar"), iter->GetValue(0));

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(String("baz"), iter->GetValue(0));

  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST(PreparedQuery, FromTableFailure) {
  const std::string error = "Failed to read row from TestTable";
  const absl::Status failure = zetasql_base::OutOfRangeErrorBuilder() << error;

  EvaluatorTestTable test_table("TestTable", {{"a", types::Int64Type()}},
                                {{Int64(10)}, {Int64(20)}, {Int64(30)}},
                                failure);

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);

  PreparedQuery query("select a from TestTable", EvaluatorOptions());
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), &catalog));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.Execute());

  ASSERT_EQ(1, iter->NumColumns());
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(10), iter->GetValue(0));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(20), iter->GetValue(0));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(30), iter->GetValue(0));
  EXPECT_FALSE(iter->NextRow());
  EXPECT_THAT(iter->Status(), StatusIs(absl::StatusCode::kOutOfRange, error));
}

TEST(PreparedQuery, FromTableCancellation) {
  SimpleTable test_table("TestTable", {{"a", types::Int64Type()}});
  test_table.SetContents({{Int64(10)}, {Int64(20)}, {Int64(30)}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);

  PreparedQuery query("select a from TestTable", EvaluatorOptions());
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), &catalog));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.Execute());

  ASSERT_EQ(1, iter->NumColumns());
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(10), iter->GetValue(0));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(20), iter->GetValue(0));

  ZETASQL_EXPECT_OK(iter->Cancel());
  EXPECT_FALSE(iter->NextRow());
  EXPECT_THAT(iter->Status(), StatusIs(absl::StatusCode::kCancelled, _));
}

TEST(PreparedQuery, FromTableDeadlineExceeded) {
  absl::SetFlag(&FLAGS_zetasql_simple_iterator_call_time_now_rows_period, 1);

  zetasql_base::SimulatedClock clock(absl::UnixEpoch());

  EvaluatorTestTable test_table(
      "TestTable", {{"a", types::Int64Type()}},
      {{Int64(10)}, {Int64(20)}, {Int64(30)}},
      /*end_status=*/absl::OkStatus(),
      /*column_filter_idxs=*/{},
      /*cancel_cb=*/[]() {},
      /*set_deadline_cb=*/[](absl::Time /*deadline*/) {}, &clock);

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);

  EvaluatorOptions options;
  options.clock = &clock;

  PreparedQuery query("select a from TestTable", options);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), &catalog));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.Execute());
  iter->SetDeadline(clock.TimeNow() + absl::Seconds(10));

  ASSERT_EQ(1, iter->NumColumns());
  ASSERT_TRUE(iter->NextRow()) << iter->Status();
  EXPECT_EQ(Int64(10), iter->GetValue(0));

  clock.AdvanceTime(absl::Seconds(15));
  EXPECT_FALSE(iter->NextRow());
  EXPECT_THAT(iter->Status(), StatusIs(absl::StatusCode::kDeadlineExceeded, _));
}

TEST(PreparedQuery, OutputIsValueTable) {
  PreparedQuery query("select as value 1 a", EvaluatorOptions());
  ZETASQL_EXPECT_OK(query.Prepare(AnalyzerOptions()));

  EXPECT_EQ(1, query.num_columns());
  EXPECT_EQ("", query.column_name(0));
  EXPECT_EQ("INT64", query.column_type(0)->DebugString());

  std::vector<PreparedQuery::NameAndType> columns = query.GetColumns();
  EXPECT_EQ(1, columns.size());
  EXPECT_EQ("", columns[0].first);
  EXPECT_EQ("INT64", columns[0].second->DebugString());

  EXPECT_TRUE(query.is_value_table());

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.Execute());

  EXPECT_EQ(1, iter->NumColumns());
  EXPECT_EQ("", iter->GetColumnName(0));
  EXPECT_EQ("INT64", iter->GetColumnType(0)->DebugString());

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(1), iter->GetValue(0));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST(PreparedQuery, TwoIteratorsAtTheSameTime) {
  SimpleTable test_table("TestTable", {{"a", types::Int64Type()}});
  test_table.SetContents({{Int64(10)}, {Int64(20)}, {Int64(30)}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);
  catalog.AddZetaSQLFunctions();

  zetasql_base::SimulatedClock clock;
  EvaluatorOptions options;
  options.clock = &clock;

  PreparedQuery query(
      "select a, unix_seconds(current_timestamp()) from TestTable", options);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), &catalog));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter1,
                       query.Execute());

  ASSERT_TRUE(iter1->NextRow());
  EXPECT_EQ(Int64(10), iter1->GetValue(0));
  EXPECT_EQ(Int64(0), iter1->GetValue(1));

  clock.AdvanceTime(absl::Seconds(1));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter2,
                       query.Execute());

  ASSERT_TRUE(iter1->NextRow());
  EXPECT_EQ(Int64(20), iter1->GetValue(0));
  EXPECT_EQ(Int64(0), iter1->GetValue(1));

  ASSERT_TRUE(iter2->NextRow());
  EXPECT_EQ(Int64(10), iter2->GetValue(0));
  EXPECT_EQ(Int64(1), iter2->GetValue(1));

  clock.AdvanceTime(absl::Seconds(1));

  ASSERT_TRUE(iter1->NextRow());
  EXPECT_EQ(Int64(30), iter1->GetValue(0));
  EXPECT_EQ(Int64(0), iter1->GetValue(1));

  ASSERT_TRUE(iter2->NextRow());
  EXPECT_EQ(Int64(20), iter2->GetValue(0));
  EXPECT_EQ(Int64(1), iter2->GetValue(1));

  clock.AdvanceTime(absl::Seconds(1));

  EXPECT_FALSE(iter1->NextRow());

  ASSERT_TRUE(iter2->NextRow());
  EXPECT_EQ(Int64(30), iter2->GetValue(0));
  EXPECT_EQ(Int64(1), iter2->GetValue(1));
  EXPECT_FALSE(iter2->NextRow());
}

TEST(PreparedQuery, Error) {
  PreparedQuery query("select 1 + 'abc'", EvaluatorOptions());

  EXPECT_THAT(query.Prepare(AnalyzerOptions()),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature")));
}

TEST(PreparedQueryDeathTest, IteratorStillLiveOnDestruction) {
  SimpleTable test_table("TestTable", {{"a", types::Int64Type()}});
  test_table.SetContents({{Int64(10)}, {Int64(20)}, {Int64(30)}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);

  auto query = absl::make_unique<PreparedQuery>("select a from TestTable",
                                                EvaluatorOptions());
  ZETASQL_ASSERT_OK(query->Prepare(AnalyzerOptions(), &catalog));
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query->Execute());
  EXPECT_DEATH(query.reset(), "cannot outlive the PreparedQuery object");
}

TEST(PreparedQuery, InvalidStatementKind) {
  LanguageOptions language_options;
  language_options.SetSupportedStatementKinds({RESOLVED_DELETE_STMT});
  AnalyzerOptions analyzer_options(language_options);

  SimpleTable table("TestTable");
  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(table.Name(), &table);

  PreparedQuery query("delete from TestTable where true", EvaluatorOptions());
  EXPECT_THAT(query.Prepare(analyzer_options, &catalog),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("not correspond to a query")));
}

TEST(PreparedQuery, Parameter) {
  PreparedQuery query("select @p1, @p2", EvaluatorOptions());

  AnalyzerOptions analyzer_options;
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p1", types::Int64Type()));
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p2", types::StringType()));
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p3", types::DoubleType()));

  ZETASQL_EXPECT_OK(query.Prepare(analyzer_options));

  EXPECT_THAT(query.GetReferencedParameters(),
              IsOkAndHolds(UnorderedElementsAre("p1", "p2")));
  EXPECT_THAT(query.GetPositionalParameterCount(), IsOkAndHolds(0));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.Execute({{"p2", values::String("abc")},
                                      {"p1", values::Int64(123)}}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(123), iter->GetValue(0));
  EXPECT_EQ(String("abc"), iter->GetValue(1));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  ZETASQL_ASSERT_OK_AND_ASSIGN(iter, query.Execute({{"p2", values::NullString()},
                                            {"p1", values::Int64(111)},
                                            {"p3", values::NullDouble()},
                                            {"p4", values::NullBytes()}}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(111), iter->GetValue(0));
  EXPECT_EQ(NullString(), iter->GetValue(1));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  EXPECT_THAT(query.Execute({{"p2", values::NullString()}}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Incomplete query parameters p1")));
}

TEST(PreparedQuery, PositionalParameter) {
  PreparedQuery query("select ?, ?", EvaluatorOptions());

  AnalyzerOptions analyzer_options;
  analyzer_options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_EXPECT_OK(analyzer_options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_EXPECT_OK(analyzer_options.AddPositionalQueryParameter(types::StringType()));
  ZETASQL_EXPECT_OK(analyzer_options.AddPositionalQueryParameter(types::DoubleType()));

  ZETASQL_ASSERT_OK(query.Prepare(analyzer_options));

  EXPECT_THAT(query.GetReferencedParameters(), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(query.GetPositionalParameterCount(), IsOkAndHolds(2));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EvaluatorTableIterator> iter,
      query.ExecuteWithPositionalParams({Int64(123), String("abc")}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(123), iter->GetValue(0));
  EXPECT_EQ(String("abc"), iter->GetValue(1));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, query.ExecuteWithPositionalParams(
                {Int64(111), NullString(), NullDouble(), NullBytes()}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(111), iter->GetValue(0));
  EXPECT_EQ(NullString(), iter->GetValue(1));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  EXPECT_THAT(query.ExecuteWithPositionalParams({Int64(100)}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Incorrect number of positional parameters")));
}

TEST(PreparedQuery, ExecuteAfterPrepareWithOrderedParamsWithParameter) {
  PreparedQuery query("select @p1, @p2", EvaluatorOptions());

  AnalyzerOptions analyzer_options;
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p1", types::Int64Type()));
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p2", types::StringType()));
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p3", types::DoubleType()));

  ZETASQL_EXPECT_OK(query.Prepare(analyzer_options));

  EXPECT_THAT(query.GetReferencedParameters(),
              IsOkAndHolds(ElementsAre("p1", "p2")));
  EXPECT_THAT(query.GetPositionalParameterCount(), IsOkAndHolds(0));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare(
                           {values::Int64(123), values::String("abc")}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(123), iter->GetValue(0));
  EXPECT_EQ(String("abc"), iter->GetValue(1));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  ZETASQL_ASSERT_OK_AND_ASSIGN(iter, query.ExecuteAfterPrepare(
                                 {values::Int64(111), values::NullString()}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(111), iter->GetValue(0));
  EXPECT_EQ(NullString(), iter->GetValue(1));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  EXPECT_THAT(
      query.ExecuteAfterPrepare({values::NullString()}),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Incorrect number of named parameters")));
}

TEST(PreparedQuery,
     ExecuteAfterPreparedWithOrderedParamsWithPositionalParameter) {
  PreparedQuery query("select ?, ?", EvaluatorOptions());

  AnalyzerOptions analyzer_options;
  analyzer_options.set_parameter_mode(PARAMETER_POSITIONAL);
  ZETASQL_EXPECT_OK(analyzer_options.AddPositionalQueryParameter(types::Int64Type()));
  ZETASQL_EXPECT_OK(analyzer_options.AddPositionalQueryParameter(types::StringType()));
  ZETASQL_EXPECT_OK(analyzer_options.AddPositionalQueryParameter(types::DoubleType()));

  ZETASQL_ASSERT_OK(query.Prepare(analyzer_options));

  EXPECT_THAT(query.GetReferencedParameters(), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(query.GetPositionalParameterCount(), IsOkAndHolds(2));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EvaluatorTableIterator> iter,
      query.ExecuteAfterPrepare({Int64(123), String("abc")}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(123), iter->GetValue(0));
  EXPECT_EQ(String("abc"), iter->GetValue(1));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      iter, query.ExecuteAfterPrepare(
                {Int64(111), NullString(), NullDouble(), NullBytes()}));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(111), iter->GetValue(0));
  EXPECT_EQ(NullString(), iter->GetValue(1));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  iter.reset();
  EXPECT_THAT(query.ExecuteAfterPrepare({Int64(100)}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Incorrect number of positional parameters")));
}

TEST(PreparedQuery, ExplainAfterPrepareWithoutPrepare) {
  PreparedQuery query("select a, b from TestTable", EvaluatorOptions());
  EXPECT_THAT(query.ExplainAfterPrepare(),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Prepare must be called first")));
}

TEST(PreparedQuery, ExecuteAfterPrepareOnlyNamedParams) {
  PreparedQuery query("select @p1", EvaluatorOptions());

  AnalyzerOptions analyzer_options;
  ZETASQL_EXPECT_OK(analyzer_options.AddQueryParameter("p1", types::Int64Type()));

  ZETASQL_EXPECT_OK(query.Prepare(analyzer_options));

  EXPECT_THAT(query.GetReferencedParameters(),
              IsOkAndHolds(UnorderedElementsAre("p1")));
  EXPECT_THAT(query.GetPositionalParameterCount(), IsOkAndHolds(0));

  QueryOptions options;
  options.parameters = {{"p1", values::Int64(123)}};
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare(options));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(Int64(123), iter->GetValue(0));
  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());
}

TEST(PreparedQuery, ExecuteAfterPrepareBuiltinFunction) {
  PreparedQuery query("select 1 + 2 as x", EvaluatorOptions());

  AnalyzerOptions analyzer_options;
  ZETASQL_EXPECT_OK(query.Prepare(analyzer_options));
  SQLBuilder sql_builder;
  ZETASQL_ASSERT_OK(query.resolved_query_stmt()->Accept(&sql_builder));
  ASSERT_EQ(sql_builder.sql(), "SELECT 1 + 2 AS x");
}

}  // namespace

class PreparedQueryTest : public ::testing::Test {
 protected:
  void SetupContextCallback(PreparedQuery* query) {
    query->SetCreateEvaluationCallbackTestOnly([this](EvaluationContext* cb) {
      // There should only be one EvaluationContext in each of these tests.
      ZETASQL_CHECK(context_ == nullptr);
      context_ = cb;
    });
  }

  EvaluationContext* context_ = nullptr;
};

namespace {

TEST_F(PreparedQueryTest, TopNAccumulator) {
  SimpleTable test_table("TestTable", {{"a", types::Int64Type()}});
  test_table.SetContents({{Int64(30)}, {Int64(20)}, {Int64(10)}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);
  catalog.AddZetaSQLFunctions();

  PreparedQuery query(
      "select array_agg(a order by a limit 2) agg from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);

  AnalyzerOptions analyzer_options;
  LanguageOptions* language_options = analyzer_options.mutable_language();
  language_options->EnableLanguageFeature(FEATURE_V_1_1_ORDER_BY_IN_AGGREGATE);
  language_options->EnableLanguageFeature(FEATURE_V_1_1_LIMIT_IN_AGGREGATE);

  ZETASQL_ASSERT_OK(query.Prepare(analyzer_options, &catalog));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());

  ASSERT_EQ(iter->NumColumns(), 1);
  ASSERT_EQ(iter->GetColumnName(0), "agg");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), values::Int64Array({10, 20}));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_TRUE(context_->used_top_n_accumulator());
}

TEST_F(PreparedQueryTest, ReadZeroColumnsWithPruningUnusedColumnsEnabled) {
  SimpleTable test_table("TestTable", {{"a", types::Int64Type()}});
  test_table.SetContents({{Int64(30)}, {Int64(20)}, {Int64(10)}});

  SimpleCatalog catalog("TestCatalog");
  catalog.AddTable(test_table.Name(), &test_table);
  catalog.AddZetaSQLFunctions();

  PreparedQuery query("select 1 from TestTable", EvaluatorOptions());

  AnalyzerOptions options;
  options.set_prune_unused_columns(true);
  ZETASQL_ASSERT_OK(query.Prepare(options, &catalog));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.Execute());

  int64_t num_rows = 0;
  while (iter->NextRow()) {
    ++num_rows;
  }
  ZETASQL_ASSERT_OK(iter->Status());

  EXPECT_THAT(num_rows, Eq(3));
}

// Test fixture for end-to-end tests of reading all fields from a proto in one
// shot.
class PreparedQueryProtoTest : public PreparedQueryTest {
 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(
        zetasql_test__::KitchenSinkPB::descriptor(), &proto_type_));

    table_ =
        absl::WrapUnique(new SimpleTable("TestTable", {{"col", proto_type_}}));
    table_->SetContents({{GetProtoValue(1)}});

    ZETASQL_ASSERT_OK(type_factory_.MakeStructType({{"kitchen_sink", proto_type_}},
                                           &struct_with_just_proto_type_));
    ZETASQL_ASSERT_OK(type_factory_.MakeStructType(
        {{"kitchen_sink", proto_type_}, {"s", struct_with_just_proto_type_}},
        &struct_with_proto_type_));

    table2_ = absl::WrapUnique(new SimpleTable(
        "TestTable2",
        {{"kitchen_sink", proto_type_}, {"s", struct_with_proto_type_}}));
    table2_->SetContents(
        {{GetProtoValue(1),
          values::Struct(
              struct_with_proto_type_,
              {GetProtoValue(10), values::Struct(struct_with_just_proto_type_,
                                                 {GetProtoValue(100)})})}});

    catalog_ = absl::make_unique<SimpleCatalog>("TestCatalog");
    catalog_->AddTable(table_->Name(), table_.get());
    catalog_->AddTable(table2_->Name(), table2_.get());
    catalog_->AddZetaSQLFunctions();

    catalog_->AddType("zetasql_test__.KitchenSinkPB", proto_type_);

    ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(
        zetasql_test__::TestOptionalFields::descriptor(), &optional_type_));
    catalog_->AddType("zetasql_test__.TestOptionalFields", optional_type_);
  }

  Value GetProtoValue(int key) const {
    zetasql_test__::KitchenSinkPB proto;
    proto.set_int64_key_1(key);
    proto.set_int64_key_2(key + 1);

    PopulateNestedProto(key, proto.mutable_nested_value());

    zetasql_test__::RewrappedNullableInt* rewrapped_nullable_int =
        proto.mutable_rewrapped_nullable_int();
    rewrapped_nullable_int->mutable_value()->set_value(key * 1000);

    proto.add_nested_repeated_value()->set_nested_int64(2 * key * 10);
    proto.add_nested_repeated_value()->set_nested_int64(3 * key * 10);

    zetasql_test__::KitchenSinkPB_OptionalGroup* group =
        proto.mutable_optionalgroup();
    group->set_int64_val(500);
    group->add_optionalgroupnested()->set_int64_val(key * 1000 + 1);
    group->add_optionalgroupnested()->set_int64_val(key * 1000 + 2);

    return Value::Proto(proto_type_, SerializeToCord(proto));
  }

  static void PopulateNestedProto(
      int key, zetasql_test__::KitchenSinkPB_Nested* nested) {
    nested->set_nested_int64(key * 10);
    nested->add_nested_repeated_int64(key * 100);
    nested->add_nested_repeated_int64(key * 100 + 1);
  }

  int GetNumProtoDeserializations() const {
    return context_->num_proto_deserializations();
  }

  TypeFactory type_factory_;
  std::unique_ptr<SimpleTable> table_;
  std::unique_ptr<SimpleTable> table2_;
  std::unique_ptr<SimpleCatalog> catalog_;
  const ProtoType* proto_type_ = nullptr;
  const StructType* struct_with_just_proto_type_;
  const StructType* struct_with_proto_type_;
  const ProtoType* optional_type_ = nullptr;
};

TEST_F(PreparedQueryProtoTest, SelectProtoFromTable) {
  PreparedQuery query("select col from TestTable", EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 1);
  ASSERT_EQ(iter->GetColumnName(0), "col");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), GetProtoValue(/*key=*/1));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_EQ(GetNumProtoDeserializations(), 0);
}

TEST_F(PreparedQueryProtoTest, SelectFieldFromProto) {
  PreparedQuery query("select col.int64_key_1 from TestTable",
                      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 1);
  ASSERT_EQ(iter->GetColumnName(0), "int64_key_1");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Int64(1));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest, SelectTwoFieldsFromProto) {
  PreparedQuery query("select col.int64_key_1, col.int64_key_2 from TestTable",
                      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "int64_key_1");
  ASSERT_EQ(iter->GetColumnName(1), "int64_key_2");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Int64(1));
  EXPECT_EQ(iter->GetValue(1), Int64(2));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest, SelectTwoFieldsFromProtoInTwoRows) {
  auto new_table =
      absl::WrapUnique(new SimpleTable("NewTestTable", {{"col", proto_type_}}));
  new_table->SetContents({{GetProtoValue(1)}, {GetProtoValue(3)}});
  catalog_->AddTable(new_table->Name(), new_table.get());

  PreparedQuery query(
      "select col.int64_key_1, col.int64_key_2 from NewTestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "int64_key_1");
  ASSERT_EQ(iter->GetColumnName(1), "int64_key_2");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Int64(1));
  EXPECT_EQ(iter->GetValue(1), Int64(2));

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Int64(3));
  EXPECT_EQ(iter->GetValue(1), Int64(4));

  EXPECT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // One deserialization per row.
  EXPECT_EQ(GetNumProtoDeserializations(), 2);
}

TEST_F(PreparedQueryProtoTest, SelectSameFieldTwice) {
  PreparedQuery query("select col.int64_key_1, col.int64_key_1 from TestTable",
                      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "int64_key_1");
  ASSERT_EQ(iter->GetColumnName(1), "int64_key_1");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Int64(1));
  EXPECT_EQ(iter->GetValue(1), Int64(1));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest, SameFieldWithNoHasBitAndHasBit) {
  PreparedQuery query(
      "select col.int64_key_1, col.has_int64_key_1 from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "int64_key_1");
  ASSERT_EQ(iter->GetColumnName(1), "has_int64_key_1");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Int64(1));
  EXPECT_EQ(iter->GetValue(1), Bool(true));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest, SameFieldWithHasBitAndNoHasBit) {
  PreparedQuery query(
      "select col.has_int64_key_1, col.int64_key_1 from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "has_int64_key_1");
  ASSERT_EQ(iter->GetColumnName(1), "int64_key_1");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Bool(true));
  EXPECT_EQ(iter->GetValue(1), Int64(1));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest, SameProtoFieldWithNoHasBitAndHasBit) {
  PreparedQuery query(
      "select col.nested_value, col.has_nested_value from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "nested_value");
  ASSERT_EQ(iter->GetColumnName(1), "has_nested_value");

  ASSERT_TRUE(iter->NextRow());

  zetasql_test__::KitchenSinkPB_Nested nested_value;
  nested_value.set_nested_int64(10);
  nested_value.add_nested_repeated_int64(100);
  nested_value.add_nested_repeated_int64(101);
  absl::Cord bytes = SerializeToCord(nested_value);

  const ProtoType* nested_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(
      zetasql_test__::KitchenSinkPB_Nested::descriptor(), &nested_type));

  EXPECT_EQ(iter->GetValue(0), Value::Proto(nested_type, bytes));
  EXPECT_EQ(iter->GetValue(1), Bool(true));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest, SameProtoFieldWithHasBitAndNoHasBit) {
  PreparedQuery query(
      "select col.has_nested_value, col.nested_value from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "has_nested_value");
  ASSERT_EQ(iter->GetColumnName(1), "nested_value");

  ASSERT_TRUE(iter->NextRow());

  EXPECT_EQ(iter->GetValue(0), Bool(true));

  zetasql_test__::KitchenSinkPB_Nested nested_value;
  nested_value.set_nested_int64(10);
  nested_value.add_nested_repeated_int64(100);
  nested_value.add_nested_repeated_int64(101);
  absl::Cord bytes = SerializeToCord(nested_value);

  const ProtoType* nested_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(
      zetasql_test__::KitchenSinkPB_Nested::descriptor(), &nested_type));

  EXPECT_EQ(iter->GetValue(1), Value::Proto(nested_type, bytes));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest, SameFieldPathTwice) {
  PreparedQuery query(
      "select col.nested_value.nested_int64, "
      "       col.nested_value.nested_int64 "
      "from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "nested_int64");
  ASSERT_EQ(iter->GetColumnName(1), "nested_int64");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Int64(10));
  EXPECT_EQ(iter->GetValue(1), Int64(10));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // Two deserializations: one for the column and one for the nested value.
  EXPECT_EQ(GetNumProtoDeserializations(), 2);
}

TEST_F(PreparedQueryProtoTest, TwoFieldPaths) {
  PreparedQuery query(
      "select col.nested_value.nested_int64, "
      "       col.nested_value.nested_repeated_int64 "
      "from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "nested_int64");
  ASSERT_EQ(iter->GetColumnName(1), "nested_repeated_int64");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Int64(10));
  EXPECT_EQ(iter->GetValue(1), values::Int64Array({100, 101}));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // Two deserializations: one for the column and one for the nested value.
  EXPECT_EQ(GetNumProtoDeserializations(), 2);
}

TEST_F(PreparedQueryProtoTest, ArrayPath) {
  PreparedQuery query(
      "SELECT value FROM TestTable t, "
      "UNNEST(t.col.nested_repeated_value.nested_int64) value",
      EvaluatorOptions());
  SetupContextCallback(&query);
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(
      FEATURE_V_1_3_UNNEST_AND_FLATTEN_ARRAYS);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(language_options), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 1);
  ASSERT_EQ(iter->GetColumnName(0), "value");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Int64(20));
  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Int64(30));
  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // Three deserializations: one for the column and one for each nested proto.
  EXPECT_EQ(GetNumProtoDeserializations(), 3);
}

TEST_F(PreparedQueryProtoTest, FieldAndSubfield) {
  PreparedQuery query(
      "select col.nested_value, col.nested_value.nested_int64 from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "nested_value");
  ASSERT_EQ(iter->GetColumnName(1), "nested_int64");

  ASSERT_TRUE(iter->NextRow());

  zetasql_test__::KitchenSinkPB_Nested nested_value;
  PopulateNestedProto(/*key=*/1, &nested_value);

  absl::Cord bytes = SerializeToCord(nested_value);

  const ProtoType* nested_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(
      zetasql_test__::KitchenSinkPB_Nested::descriptor(), &nested_type));

  EXPECT_EQ(iter->GetValue(0), Value::Proto(nested_type, bytes));
  EXPECT_EQ(iter->GetValue(1), Int64(10));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // Two deserializations: one for the column and one for the nested value.
  EXPECT_EQ(GetNumProtoDeserializations(), 2);
}

TEST_F(PreparedQueryProtoTest, SubfieldAndField) {
  PreparedQuery query(
      "select col.nested_value.nested_int64, col.nested_value from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "nested_int64");
  ASSERT_EQ(iter->GetColumnName(1), "nested_value");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Int64(10));

  zetasql_test__::KitchenSinkPB_Nested nested_value;
  PopulateNestedProto(/*key=*/1, &nested_value);
  absl::Cord bytes = SerializeToCord(nested_value);

  const ProtoType* nested_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(
      zetasql_test__::KitchenSinkPB_Nested::descriptor(), &nested_type));

  EXPECT_EQ(iter->GetValue(1), Value::Proto(nested_type, bytes));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // Two deserializations: one for the column and one for the nested value.
  EXPECT_EQ(GetNumProtoDeserializations(), 2);
}

TEST_F(PreparedQueryProtoTest, FieldAndSubSubField) {
  PreparedQuery query(
      "select col.rewrapped_nullable_int, "
      "       col.rewrapped_nullable_int.value.value "
      "from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "rewrapped_nullable_int");
  ASSERT_EQ(iter->GetColumnName(1), "value");

  ASSERT_TRUE(iter->NextRow());

  zetasql_test__::RewrappedNullableInt rewrapped_nullable_int;
  rewrapped_nullable_int.mutable_value()->set_value(1000);
  absl::Cord bytes = SerializeToCord(rewrapped_nullable_int);

  const ProtoType* rewrapped_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(
      zetasql_test__::RewrappedNullableInt::descriptor(), &rewrapped_type));

  EXPECT_EQ(iter->GetValue(0), Value::Proto(rewrapped_type, bytes));
  EXPECT_EQ(iter->GetValue(1), Int32(1000));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // Three deserializations: one for the column, one for rewrapped_nullable_int,
  // and one for rewrapped_nullable_int.value.
  EXPECT_EQ(GetNumProtoDeserializations(), 3);
}

TEST_F(PreparedQueryProtoTest, SubSubFieldAndField) {
  PreparedQuery query(
      "select col.rewrapped_nullable_int.value.value, "
      "       col.rewrapped_nullable_int "
      "from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "value");
  ASSERT_EQ(iter->GetColumnName(1), "rewrapped_nullable_int");

  ASSERT_TRUE(iter->NextRow());
  EXPECT_EQ(iter->GetValue(0), Int32(1000));

  zetasql_test__::RewrappedNullableInt rewrapped_nullable_int;
  rewrapped_nullable_int.mutable_value()->set_value(1000);
  absl::Cord bytes = SerializeToCord(rewrapped_nullable_int);

  const ProtoType* rewrapped_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(
      zetasql_test__::RewrappedNullableInt::descriptor(), &rewrapped_type));

  EXPECT_EQ(iter->GetValue(1), Value::Proto(rewrapped_type, bytes));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // Three deserializations: one for the column, one for rewrapped_nullable_int,
  // and one for rewrapped_nullable_int.value.
  EXPECT_EQ(GetNumProtoDeserializations(), 3);
}

TEST_F(PreparedQueryProtoTest, Complex) {
  PreparedQuery query(
      "select col.nested_value, "
      "       col.nested_value.nested_int64, "
      "       col.nested_value.nested_repeated_int64, "
      "       col.int64_key_1 "
      "from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 4);
  ASSERT_EQ(iter->GetColumnName(0), "nested_value");
  ASSERT_EQ(iter->GetColumnName(1), "nested_int64");
  ASSERT_EQ(iter->GetColumnName(2), "nested_repeated_int64");
  ASSERT_EQ(iter->GetColumnName(3), "int64_key_1");

  ASSERT_TRUE(iter->NextRow());

  zetasql_test__::KitchenSinkPB_Nested nested_value;
  nested_value.set_nested_int64(10);
  nested_value.add_nested_repeated_int64(100);
  nested_value.add_nested_repeated_int64(101);
  absl::Cord bytes = SerializeToCord(nested_value);

  const ProtoType* nested_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(
      zetasql_test__::KitchenSinkPB_Nested::descriptor(), &nested_type));

  EXPECT_EQ(iter->GetValue(0), Value::Proto(nested_type, bytes));
  EXPECT_EQ(iter->GetValue(1), Int64(10));
  EXPECT_EQ(iter->GetValue(2), values::Int64Array({100, 101}));
  EXPECT_EQ(iter->GetValue(3), Int64(1));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // Two deserializations: one for the column and one for the nested value.
  EXPECT_EQ(GetNumProtoDeserializations(), 2);
}

TEST_F(PreparedQueryProtoTest, WithRepeatedFieldOffsets) {
  PreparedQuery query(
      "select col.nested_repeated_value[offset(0)] as v1, "
      "       col.nested_repeated_value[offset(0)] as v2 "
      "from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "v1");
  ASSERT_EQ(iter->GetColumnName(1), "v2");

  ASSERT_TRUE(iter->NextRow());

  zetasql_test__::KitchenSinkPB_Nested nested_proto;
  nested_proto.set_nested_int64(20);
  absl::Cord bytes = SerializeToCord(nested_proto);

  const ProtoType* nested_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(
      zetasql_test__::KitchenSinkPB_Nested::descriptor(), &nested_type));

  const Value nested_value = Value::Proto(nested_type, bytes);

  EXPECT_EQ(iter->GetValue(0), nested_value);
  EXPECT_EQ(iter->GetValue(1), nested_value);

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // One deserialization (for the column).
  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest, WithRepeatedFieldOffsetsAndFieldAccesses) {
  PreparedQuery query(
      "select col.nested_repeated_value[offset(0)].nested_int64, "
      "       col.nested_repeated_value[offset(0)].nested_int64 "
      "from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "nested_int64");
  ASSERT_EQ(iter->GetColumnName(1), "nested_int64");

  ASSERT_TRUE(iter->NextRow());

  EXPECT_EQ(iter->GetValue(0), Int64(20));
  EXPECT_EQ(iter->GetValue(1), Int64(20));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // The column is only deserialized once, but nested_repeated_value[offset(0)]
  // is deserialized twice.
  EXPECT_EQ(GetNumProtoDeserializations(), 3);
}

TEST_F(PreparedQueryProtoTest,
       MessageFieldWithRepeatedFieldOffsetsAndFieldAccesses) {
  PreparedQuery query(
      "select col.OptionalGroup.OptionalGroupNested[offset(0)].int64_val, "
      "       col.OptionalGroup.OptionalGroupNested[offset(0)].int64_val "
      "from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "int64_val");
  ASSERT_EQ(iter->GetColumnName(1), "int64_val");

  ASSERT_TRUE(iter->NextRow());

  EXPECT_EQ(iter->GetValue(0), Int64(1001));
  EXPECT_EQ(iter->GetValue(1), Int64(1001));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // The column and the optional group are only deserialized once, but the array
  // element is deserialized twice.
  EXPECT_EQ(GetNumProtoDeserializations(), 4);
}

TEST_F(PreparedQueryProtoTest, Subquery) {
  PreparedQuery query(
      "select a.int64_key_1, a.int64_key_2\n"
      "from (select (new zetasql_test__.KitchenSinkPB(\n"
      "                  1 as int64_key_1, 2 as int64_key_2)) a)",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "int64_key_1");
  ASSERT_EQ(iter->GetColumnName(1), "int64_key_2");

  ASSERT_TRUE(iter->NextRow());

  EXPECT_EQ(iter->GetValue(0), Int64(1));
  EXPECT_EQ(iter->GetValue(1), Int64(2));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // The proto is only deserialized once.
  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest, ProtoFromStruct) {
  PreparedQuery query("select s.kitchen_sink from TestTable2",
                      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 1);
  ASSERT_EQ(iter->GetColumnName(0), "kitchen_sink");

  ASSERT_TRUE(iter->NextRow());

  EXPECT_EQ(iter->GetValue(0), GetProtoValue(10));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // The proto is extracted from a struct but not deserialized.
  EXPECT_EQ(GetNumProtoDeserializations(), 0);
}

TEST_F(PreparedQueryProtoTest, FieldFromProtoFromStruct) {
  PreparedQuery query("select s.kitchen_sink.int64_key_1 from TestTable2",
                      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 1);
  ASSERT_EQ(iter->GetColumnName(0), "int64_key_1");

  ASSERT_TRUE(iter->NextRow());

  EXPECT_EQ(iter->GetValue(0), Int64(10));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest, TwoFieldsFromProtoFromStruct) {
  PreparedQuery query(
      "select s.kitchen_sink.int64_key_1, s.kitchen_sink.int64_key_2 "
      "from TestTable2",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "int64_key_1");
  ASSERT_EQ(iter->GetColumnName(1), "int64_key_2");

  ASSERT_TRUE(iter->NextRow());

  EXPECT_EQ(iter->GetValue(0), Int64(10));
  EXPECT_EQ(iter->GetValue(1), Int64(11));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest, SameFieldTwiceFromProtoFromStruct) {
  PreparedQuery query(
      "select s.kitchen_sink.int64_key_1, s.kitchen_sink.int64_key_1 "
      "from TestTable2",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "int64_key_1");
  ASSERT_EQ(iter->GetColumnName(1), "int64_key_1");

  ASSERT_TRUE(iter->NextRow());

  EXPECT_EQ(iter->GetValue(0), Int64(10));
  EXPECT_EQ(iter->GetValue(1), Int64(10));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest,
       SameFieldTwiceFromProtoFromStructOnceWithHasBit) {
  PreparedQuery query(
      "select s.kitchen_sink.int64_key_1, s.kitchen_sink.has_int64_key_1 "
      "from TestTable2",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "int64_key_1");
  ASSERT_EQ(iter->GetColumnName(1), "has_int64_key_1");

  ASSERT_TRUE(iter->NextRow());

  EXPECT_EQ(iter->GetValue(0), Int64(10));
  EXPECT_EQ(iter->GetValue(1), Bool(true));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  EXPECT_EQ(GetNumProtoDeserializations(), 1);
}

TEST_F(PreparedQueryProtoTest, MixedProtoAndStructFieldPaths) {
  PreparedQuery query(
      "select"
      " kitchen_sink.int64_key_1, kitchen_sink.int64_key_2,"
      " kitchen_sink.nested_value, kitchen_sink.nested_value.nested_int64,"
      " kitchen_sink.nested_value.nested_repeated_int64,"
      " s.kitchen_sink.int64_key_1, s.kitchen_sink.int64_key_2,"
      " s.kitchen_sink.nested_value, s.kitchen_sink.nested_value.nested_int64,"
      " s.kitchen_sink.nested_value.nested_repeated_int64,"
      " s.s.kitchen_sink.int64_key_1, s.s.kitchen_sink.int64_key_2,"
      " s.s.kitchen_sink.nested_value,"
      " s.s.kitchen_sink.nested_value.nested_int64,"
      " s.s.kitchen_sink.nested_value.nested_repeated_int64 "
      "from TestTable2",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 15);
  ASSERT_EQ(iter->GetColumnName(0), "int64_key_1");
  ASSERT_EQ(iter->GetColumnName(1), "int64_key_2");
  ASSERT_EQ(iter->GetColumnName(2), "nested_value");
  ASSERT_EQ(iter->GetColumnName(3), "nested_int64");
  ASSERT_EQ(iter->GetColumnName(4), "nested_repeated_int64");
  ASSERT_EQ(iter->GetColumnName(5), "int64_key_1");
  ASSERT_EQ(iter->GetColumnName(6), "int64_key_2");
  ASSERT_EQ(iter->GetColumnName(7), "nested_value");
  ASSERT_EQ(iter->GetColumnName(8), "nested_int64");
  ASSERT_EQ(iter->GetColumnName(9), "nested_repeated_int64");
  ASSERT_EQ(iter->GetColumnName(10), "int64_key_1");
  ASSERT_EQ(iter->GetColumnName(11), "int64_key_2");
  ASSERT_EQ(iter->GetColumnName(12), "nested_value");
  ASSERT_EQ(iter->GetColumnName(13), "nested_int64");
  ASSERT_EQ(iter->GetColumnName(14), "nested_repeated_int64");

  ASSERT_TRUE(iter->NextRow());

  zetasql_test__::KitchenSinkPB_Nested expected_nested1;
  zetasql_test__::KitchenSinkPB_Nested expected_nested2;
  zetasql_test__::KitchenSinkPB_Nested expected_nested3;

  PopulateNestedProto(/*key=*/1, &expected_nested1);
  PopulateNestedProto(/*key=*/10, &expected_nested2);
  PopulateNestedProto(/*key=*/100, &expected_nested3);

  absl::Cord serialized_expected_nested1 = SerializeToCord(expected_nested1);
  absl::Cord serialized_expected_nested2 = SerializeToCord(expected_nested2);
  absl::Cord serialized_expected_nested3 = SerializeToCord(expected_nested3);

  const ProtoType* nested_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(
      zetasql_test__::KitchenSinkPB_Nested::descriptor(), &nested_type));

  const Value expected_nested1_value =
      Value::Proto(nested_type, serialized_expected_nested1);
  const Value expected_nested2_value =
      Value::Proto(nested_type, serialized_expected_nested2);
  const Value expected_nested3_value =
      Value::Proto(nested_type, serialized_expected_nested3);

  EXPECT_EQ(iter->GetValue(0), Int64(1));
  EXPECT_EQ(iter->GetValue(1), Int64(2));
  EXPECT_EQ(iter->GetValue(2), expected_nested1_value);
  EXPECT_EQ(iter->GetValue(3), Int64(10));
  EXPECT_EQ(iter->GetValue(4), values::Int64Array({100, 101}));
  EXPECT_EQ(iter->GetValue(5), Int64(10));
  EXPECT_EQ(iter->GetValue(6), Int64(11));
  EXPECT_EQ(iter->GetValue(7), expected_nested2_value);
  EXPECT_EQ(iter->GetValue(8), Int64(100));
  EXPECT_EQ(iter->GetValue(9), values::Int64Array({1000, 1001}));
  EXPECT_EQ(iter->GetValue(10), Int64(100));
  EXPECT_EQ(iter->GetValue(11), Int64(101));
  EXPECT_EQ(iter->GetValue(12), expected_nested3_value);
  EXPECT_EQ(iter->GetValue(13), Int64(1000));
  EXPECT_EQ(iter->GetValue(14), values::Int64Array({10000, 10001}));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // We deserialize the following protos:
  // - kitchen_sink
  // - kitchen_sink.nested_value
  // - s.kitchen_sink
  // - s.kitchen_sink.nested_value
  // - s.s.kitchen_sink
  // - s.s.kitchen_sink.nested_value
  EXPECT_EQ(GetNumProtoDeserializations(), 6);
}

TEST_F(PreparedQueryProtoTest, ArbitraryProtoValuedExpression) {
  PreparedQuery query(
      "select new zetasql_test__.TestOptionalFields().value, "
      "       new zetasql_test__.TestOptionalFields().value "
      "from TestTable",
      EvaluatorOptions());
  SetupContextCallback(&query);
  ZETASQL_ASSERT_OK(query.Prepare(AnalyzerOptions(), catalog_.get()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       query.ExecuteAfterPrepare());
  ASSERT_EQ(iter->NumColumns(), 2);
  ASSERT_EQ(iter->GetColumnName(0), "value");
  ASSERT_EQ(iter->GetColumnName(1), "value");

  ASSERT_TRUE(iter->NextRow());

  EXPECT_EQ(iter->GetValue(0), Int64(0));
  EXPECT_EQ(iter->GetValue(1), Int64(0));

  ASSERT_FALSE(iter->NextRow());
  ZETASQL_EXPECT_OK(iter->Status());

  // We have to deserialize the proto-valued expression twice because it is not
  // of the form column.field_path.
  EXPECT_EQ(GetNumProtoDeserializations(), 2);
}

}  // namespace
}  // namespace zetasql
