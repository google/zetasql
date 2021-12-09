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

#include "zetasql/analyzer/substitute.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/base/logging.h"
#include "zetasql/analyzer/all_rewriters.h"
#include "zetasql/base/testing/status_matchers.h"  
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/evaluator.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/testing/test_catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace {
using testing::ElementsAre;
using testing::HasSubstr;
using zetasql_base::testing::StatusIs;

class ExpressionSubstitutorTest : public ::testing::Test {
 public:
  ExpressionSubstitutorTest()
      : catalog_("catalog"),
        test_table_("TestTable"),
        test_column_("TestTable", "Int64Col", types::Int64Type()) {}

 protected:
  void SetUp() override {
    // Setup TestTable
    catalog_.AddZetaSQLFunctions();
    ZETASQL_CHECK_OK(test_table_.AddColumn(&test_column_, false));
    catalog_.AddTable(&test_table_);

    options_.CreateDefaultArenasIfNotSet();
    seq_.GetNext();  // Avoid column id's of 0, which are forbidden.
    options_.set_column_id_sequence_number(&seq_);
    options_.mutable_language()->EnableLanguageFeature(
        FEATURE_V_1_3_INLINE_LAMBDA_ARGUMENT);
    options_.set_record_parse_locations(true);

    col1_ = ResolvedColumn(
        static_cast<int>(options_.column_id_sequence_number()->GetNext()),
        zetasql::IdString::MakeGlobal("t"),
        zetasql::IdString::MakeGlobal("col1"), type_factory_.get_int64());
    col2_ = ResolvedColumn(
        static_cast<int>(options_.column_id_sequence_number()->GetNext()),
        zetasql::IdString::MakeGlobal("t"),
        zetasql::IdString::MakeGlobal("col2"), type_factory_.get_int64());
    col1_ref_ =
        MakeResolvedColumnRef(col1_.type(), col1_, /*is_correlated=*/false);
    col2_ref_ =
        MakeResolvedColumnRef(col2_.type(), col2_, /*is_correlated=*/false);

    // It's tricky to construct a lambda directly, so we analyze an expression
    // that includes a lambda then use the contained lambda for substitute. This
    // works out well because it's how we also expect substitute to typically be
    // used.
    constexpr absl::string_view sql_with_array_filter = R"sql(
  ( SELECT ARRAY_FILTER([1, 2, 3], (a,b) -> a+b<Int64Col)
    FROM TestTable t )
  )sql";
    // Disable rewrites to preserve the ResolvedInlineLambdas.
    options_.set_enabled_rewrites({});
    ZETASQL_ASSERT_OK(AnalyzeExpressionForAssignmentToType(
        sql_with_array_filter, options_, &catalog_, &type_factory_,
        types::Int64ArrayType(), &filter_lambda_output_));
    const ResolvedFunctionCall* array_filter_call =
        filter_lambda_output_->resolved_expr()
            ->GetAs<ResolvedSubqueryExpr>()
            ->subquery()
            ->GetAs<ResolvedProjectScan>()
            ->expr_list(0)
            ->expr()
            ->GetAs<ResolvedFunctionCall>();
    filter_lambda_ =
        array_filter_call->generic_argument_list(1)->inline_lambda();
  }

  zetasql_base::SequenceNumber seq_;
  AnalyzerOptions options_;
  TypeFactory type_factory_;
  TestCatalog catalog_;
  SimpleTable test_table_;
  SimpleColumn test_column_;

  ResolvedColumn col1_;
  ResolvedColumn col2_;
  std::unique_ptr<ResolvedColumnRef> col1_ref_;
  std::unique_ptr<ResolvedColumnRef> col2_ref_;

  std::unique_ptr<const AnalyzerOutput> filter_lambda_output_;
  const ResolvedInlineLambda* filter_lambda_;
};

TEST_F(ExpressionSubstitutorTest, RequiresArenas) {
  {
    AnalyzerOptions options = options_;
    options.set_column_id_sequence_number(nullptr);
    EXPECT_THAT(AnalyzeSubstitute(options, catalog_, type_factory_, "1 + 2", {})
                    .status(),
                zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
  }
  {
    AnalyzerOptions options = options_;
    options.set_id_string_pool(nullptr);
    EXPECT_THAT(AnalyzeSubstitute(options, catalog_, type_factory_, "1 + 2", {})
                    .status(),
                zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
  }
  {
    AnalyzerOptions options = options_;
    options.set_arena(nullptr);
    EXPECT_THAT(AnalyzeSubstitute(options, catalog_, type_factory_, "1 + 2", {})
                    .status(),
                zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
  }
}

TEST_F(ExpressionSubstitutorTest, ColumnReferences) {
  // Make sure the subquery's parameter list is set up properly when the
  // expressions being substituted in contain column references.
  auto result = *AnalyzeSubstitute(
      options_, catalog_, type_factory_, "col1 + col2 + col1 + col2",
      {{"col1", col1_ref_.get()}, {"col2", col2_ref_.get()}});
  ASSERT_TRUE(result->type()->IsInt64());
  const auto& parameter_list =
      result->GetAs<ResolvedSubqueryExpr>()->parameter_list();
  ASSERT_EQ(parameter_list.size(), 2);
  ASSERT_EQ(parameter_list[0]->column(), col1_);
  ASSERT_EQ(parameter_list[1]->column(), col2_);
  ASSERT_FALSE(parameter_list[0]->is_correlated());
  ASSERT_FALSE(parameter_list[1]->is_correlated());
}

TEST_F(ExpressionSubstitutorTest, SubstituteExpressions) {
  std::unique_ptr<const AnalyzerOutput> array_arg_analyzer_output;
  ZETASQL_ASSERT_OK(AnalyzeExpressionForAssignmentToType(
      "[1, 2, 3, 4, 5, 6, 7, 8, 9]", options_, &catalog_, &type_factory_,
      types::Int64ArrayType(), &array_arg_analyzer_output));

  std::unique_ptr<const AnalyzerOutput> base_arg_analyzer_output;
  ZETASQL_ASSERT_OK(AnalyzeExpressionForAssignmentToType(
      "(SELECT x FROM UNNEST([1, 2, 3]) x WHERE x > 2)", options_, &catalog_,
      &type_factory_, types::Int64Type(), &base_arg_analyzer_output));

  auto result = *AnalyzeSubstitute(
      options_, catalog_, type_factory_,
      "ARRAY(SELECT x FROM UNNEST(array_arg) x WHERE MOD(x, "
      "base_arg) = 0)",
      {{"array_arg", array_arg_analyzer_output->resolved_expr()},
       {"base_arg", base_arg_analyzer_output->resolved_expr()}});

  ZETASQL_VLOG(1) << result->DebugString();

  EXPECT_EQ(result->DebugString(), R"(SubqueryExpr
+-type=ARRAY<INT64>
+-subquery_type=SCALAR
+-subquery=
  +-ProjectScan
    +-column_list=[$expr_subquery.$col1#11]
    +-expr_list=
    | +-$col1#11 :=
    |   +-SubqueryExpr
    |     +-type=ARRAY<INT64>
    |     +-subquery_type=ARRAY
    |     +-parameter_list=
    |     | +-ColumnRef(type=ARRAY<INT64>, column=$subquery1.array_arg#8)
    |     | +-ColumnRef(type=INT64, column=$subquery1.base_arg#9)
    |     +-subquery=
    |       +-ProjectScan
    |         +-column_list=[$array.x#10]
    |         +-input_scan=
    |           +-FilterScan
    |             +-column_list=[$array.x#10]
    |             +-input_scan=
    |             | +-ArrayScan
    |             |   +-column_list=[$array.x#10]
    |             |   +-array_expr=
    |             |   | +-ColumnRef(type=ARRAY<INT64>, column=$subquery1.array_arg#8, is_correlated=TRUE)
    |             |   +-element_column=$array.x#10
    |             +-filter_expr=
    |               +-FunctionCall(ZetaSQL:$equal(INT64, INT64) -> BOOL)
    |                 +-FunctionCall(ZetaSQL:mod(INT64, INT64) -> INT64)
    |                 | +-ColumnRef(type=INT64, column=$array.x#10)
    |                 | +-ColumnRef(type=INT64, column=$subquery1.base_arg#9, is_correlated=TRUE)
    |                 +-Literal(type=INT64, value=0)
    +-input_scan=
      +-ProjectScan
        +-column_list=$subquery1.[array_arg#8, base_arg#9]
        +-expr_list=
        | +-array_arg#8 := Literal(parse_location=0-27, type=ARRAY<INT64>, value=[1, 2, 3, 4, 5, 6, 7, 8, 9])
        | +-base_arg#9 :=
        |   +-SubqueryExpr
        |     +-parse_location=1-46
        |     +-type=INT64
        |     +-subquery_type=SCALAR
        |     +-subquery=
        |       +-ProjectScan
        |         +-parse_location=1-46
        |         +-column_list=[$array.x#7]
        |         +-input_scan=
        |           +-FilterScan
        |             +-column_list=[$array.x#7]
        |             +-input_scan=
        |             | +-ArrayScan
        |             |   +-column_list=[$array.x#7]
        |             |   +-array_expr=
        |             |   | +-Literal(parse_location=22-31, type=ARRAY<INT64>, value=[1, 2, 3])
        |             |   +-element_column=$array.x#7
        |             +-filter_expr=
        |               +-FunctionCall(ZetaSQL:$greater(INT64, INT64) -> BOOL)
        |                 +-ColumnRef(type=INT64, column=$array.x#7)
        |                 +-Literal(parse_location=45-46, type=INT64, value=2)
        +-input_scan=
          +-SingleRowScan
)");

  EXPECT_THAT(PreparedExpression(result.get(), {}).Execute()->elements(),
              ElementsAre(Value::Int64(3), Value::Int64(6), Value::Int64(9)));
}

TEST_F(ExpressionSubstitutorTest, SubstituteLambda) {
  // Run substitute
  constexpr absl::string_view input_sql = R"sql(
  ARRAY(SELECT x FROM UNNEST([1,2,3]) x WITH OFFSET off
        WHERE INVOKE(@mylambda, x, off))
  )sql";
  auto result =
      *AnalyzeSubstitute(options_, catalog_, type_factory_, input_sql, {},
                         /*lambdas=*/{{"mylambda", filter_lambda_}});

  ZETASQL_VLOG(1) << result->DebugString();
  // NOTE:
  //   * This snippet is for ARRAY_FILTER call.
  //   * The $less function call is using 'x' and 'off' columns instead of 'a'
  //   and 'b' columns.
  //   * TestTable.Int64Col ref is put into parameter_list of two SubqueryExprs.
  //   * TestTable.Int64Col ref in the outermost is not correlated.
  //   * TestTable.Int64Col ref in the inner one is correlated.
  EXPECT_EQ(result->DebugString(), R"(SubqueryExpr
+-type=ARRAY<INT64>
+-subquery_type=SCALAR
+-parameter_list=
| +-ColumnRef(type=INT64, column=TestTable.Int64Col#3)
+-subquery=
  +-ProjectScan
    +-column_list=[$expr_subquery.$col1#9]
    +-expr_list=
    | +-$col1#9 :=
    |   +-SubqueryExpr
    |     +-type=ARRAY<INT64>
    |     +-subquery_type=ARRAY
    |     +-parameter_list=
    |     | +-ColumnRef(type=INT64, column=TestTable.Int64Col#3, is_correlated=TRUE)
    |     +-subquery=
    |       +-ProjectScan
    |         +-column_list=[$array.x#7]
    |         +-input_scan=
    |           +-FilterScan
    |             +-column_list=[$array.x#7, $array_offset.off#8]
    |             +-input_scan=
    |             | +-ArrayScan
    |             |   +-column_list=[$array.x#7, $array_offset.off#8]
    |             |   +-array_expr=
    |             |   | +-Literal(type=ARRAY<INT64>, value=[1, 2, 3])
    |             |   +-element_column=$array.x#7
    |             |   +-array_offset_column=
    |             |     +-ColumnHolder(column=$array_offset.off#8)
    |             +-filter_expr=
    |               +-FunctionCall(ZetaSQL:$less(INT64, INT64) -> BOOL)
    |                 +-FunctionCall(ZetaSQL:$add(INT64, INT64) -> INT64)
    |                 | +-ColumnRef(type=INT64, column=$array.x#7)
    |                 | +-ColumnRef(type=INT64, column=$array_offset.off#8)
    |                 +-ColumnRef(parse_location=49-57, type=INT64, column=TestTable.Int64Col#3, is_correlated=TRUE)
    +-input_scan=
      +-SingleRowScan
)");
}

TEST_F(ExpressionSubstitutorTest,
       SubstituteLambdaWithCorrelatedInvokeArgument) {
  // Run substitute
  // NOTE: x and off arguments of INVOKE are correlated ColumnRefs.
  constexpr absl::string_view input_sql = R"sql(
  ARRAY(SELECT x FROM UNNEST([1,2,3]) x WITH OFFSET off
        WHERE (SELECT INVOKE(@mylambda, x, off)))
  )sql";
  auto result =
      *AnalyzeSubstitute(options_, catalog_, type_factory_, input_sql, {},
                         /*lambdas=*/{{"mylambda", filter_lambda_}});

  ZETASQL_VLOG(1) << result->DebugString();
  // Note: ZetaSQL:$add arguments are correlated.
  EXPECT_EQ(result->DebugString(), R"(SubqueryExpr
+-type=ARRAY<INT64>
+-subquery_type=SCALAR
+-parameter_list=
| +-ColumnRef(type=INT64, column=TestTable.Int64Col#3)
+-subquery=
  +-ProjectScan
    +-column_list=[$expr_subquery.$col1#10]
    +-expr_list=
    | +-$col1#10 :=
    |   +-SubqueryExpr
    |     +-type=ARRAY<INT64>
    |     +-subquery_type=ARRAY
    |     +-parameter_list=
    |     | +-ColumnRef(type=INT64, column=TestTable.Int64Col#3, is_correlated=TRUE)
    |     +-subquery=
    |       +-ProjectScan
    |         +-column_list=[$array.x#7]
    |         +-input_scan=
    |           +-FilterScan
    |             +-column_list=[$array.x#7, $array_offset.off#8]
    |             +-input_scan=
    |             | +-ArrayScan
    |             |   +-column_list=[$array.x#7, $array_offset.off#8]
    |             |   +-array_expr=
    |             |   | +-Literal(type=ARRAY<INT64>, value=[1, 2, 3])
    |             |   +-element_column=$array.x#7
    |             |   +-array_offset_column=
    |             |     +-ColumnHolder(column=$array_offset.off#8)
    |             +-filter_expr=
    |               +-SubqueryExpr
    |                 +-type=BOOL
    |                 +-subquery_type=SCALAR
    |                 +-parameter_list=
    |                 | +-ColumnRef(type=INT64, column=$array.x#7)
    |                 | +-ColumnRef(type=INT64, column=$array_offset.off#8)
    |                 | +-ColumnRef(type=INT64, column=TestTable.Int64Col#3, is_correlated=TRUE)
    |                 +-subquery=
    |                   +-ProjectScan
    |                     +-column_list=[$expr_subquery.$col1#9]
    |                     +-expr_list=
    |                     | +-$col1#9 :=
    |                     |   +-FunctionCall(ZetaSQL:$less(INT64, INT64) -> BOOL)
    |                     |     +-FunctionCall(ZetaSQL:$add(INT64, INT64) -> INT64)
    |                     |     | +-ColumnRef(type=INT64, column=$array.x#7, is_correlated=TRUE)
    |                     |     | +-ColumnRef(type=INT64, column=$array_offset.off#8, is_correlated=TRUE)
    |                     |     +-ColumnRef(parse_location=49-57, type=INT64, column=TestTable.Int64Col#3, is_correlated=TRUE)
    |                     +-input_scan=
    |                       +-SingleRowScan
    +-input_scan=
      +-SingleRowScan
)");
}

TEST_F(ExpressionSubstitutorTest, SubstituteErrors) {
  // It's tricky to construct a lambda directly, so we analyze an expression
  // that includes a lambda then use the contained lambda for substitute. This
  // works out well because it's how we also expect substitute to typically be
  // used.
  constexpr absl::string_view sql_with_array_filter = R"sql(
  ( SELECT ARRAY_FILTER([1, 2, 3], (a,b) -> a+b<Int64Col)
    FROM TestTable t )
  )sql";
  std::unique_ptr<const AnalyzerOutput> filter_lambda_output;
  // Disable rewrites to preserve the ResolvedInlineLambdas.
  options_.set_enabled_rewrites({});
  ZETASQL_ASSERT_OK(AnalyzeExpressionForAssignmentToType(
      sql_with_array_filter, options_, &catalog_, &type_factory_,
      types::Int64ArrayType(), &filter_lambda_output));
  const ResolvedFunctionCall* array_filter_call =
      filter_lambda_output->resolved_expr()
          ->GetAs<ResolvedSubqueryExpr>()
          ->subquery()
          ->GetAs<ResolvedProjectScan>()
          ->expr_list(0)
          ->expr()
          ->GetAs<ResolvedFunctionCall>();
  const ResolvedInlineLambda* lambda =
      array_filter_call->generic_argument_list(1)->inline_lambda();

  {
    // Use of undefined variable
    constexpr absl::string_view input_sql = "col2 + 1";
    auto result = AnalyzeSubstitute(options_, catalog_, type_factory_,
                                    input_sql, {{"col1", col1_ref_.get()}});
    ASSERT_THAT(result.status(), StatusIs(absl::StatusCode::kInvalidArgument,
                                          HasSubstr("Unrecognized name")));
  }

  {
    // Using lambda as normal query parameter.
    constexpr absl::string_view input_sql = "@lambda AND FALSE";
    auto result =
        AnalyzeSubstitute(options_, catalog_, type_factory_, input_sql,
                          {{"col1", col1_ref_.get()}}, {{"lambda", lambda}});
    ASSERT_THAT(
        result.status(),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr("Lambda can only be used as first argument of INVOKE")));
  }

  {
    // First argument of INVOKE is not named query parameter
    constexpr absl::string_view input_sql =
        "(SELECT INVOKE(element + 1,  element) FROM (SELECT 1 as element))";
    auto result =
        AnalyzeSubstitute(options_, catalog_, type_factory_, input_sql,
                          {{"col1", col1_ref_.get()}}, {{"lambda", lambda}});
    ASSERT_THAT(
        result.status(),
        StatusIs(absl::StatusCode::kInvalidArgument,
                 HasSubstr("First argument to invoke must be a parameter with "
                           "the name of the lambda to be invoked")));
  }

  {
    // Lambda not found for INVOKE
    constexpr absl::string_view input_sql =
        "(SELECT INVOKE(@lambda2,  element) FROM (SELECT 1 as element))";
    auto result =
        AnalyzeSubstitute(options_, catalog_, type_factory_, input_sql,
                          {{"col1", col1_ref_.get()}}, {{"lambda", lambda}});
    ASSERT_THAT(result.status(),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("Query parameter 'lambda2' not found")));
  }

  {
    // Using projected variable as lambda
    constexpr absl::string_view input_sql =
        "(SELECT INVOKE(@col1,  element) FROM (SELECT 1 as element))";
    auto result =
        AnalyzeSubstitute(options_, catalog_, type_factory_, input_sql,
                          {{"col1", col1_ref_.get()}}, {{"lambda", lambda}});
    ASSERT_THAT(result.status(),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("No lambda named col1 is found")));
  }
}
}  // namespace
}  // namespace zetasql
