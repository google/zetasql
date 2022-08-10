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

#include "zetasql/resolved_ast/rewrite_utils.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_format.h"

namespace zetasql {
namespace {

TEST(ColumnFactory, NoSequence) {
  ColumnFactory factory(10);
  ResolvedColumn column =
      factory.MakeCol("table", "column", types::StringType());

  EXPECT_EQ(column.column_id(), 11);
  EXPECT_EQ(column.type(), types::StringType());
  EXPECT_EQ(column.table_name(), "table");
  EXPECT_EQ(column.name(), "column");

  EXPECT_EQ(factory.max_column_id(), 11);
}

TEST(ColumnFactory, NoSequenceAnnotated) {
  ColumnFactory factory(10);
  ResolvedColumn column =
      factory.MakeCol("table", "column", {types::StringType(), nullptr});

  EXPECT_EQ(column.column_id(), 11);
  EXPECT_EQ(column.type(), types::StringType());
  EXPECT_EQ(column.table_name(), "table");
  EXPECT_EQ(column.name(), "column");

  EXPECT_EQ(factory.max_column_id(), 11);
}

TEST(ColumnFactory, WithSequenceBehind) {
  zetasql_base::SequenceNumber sequence;
  ColumnFactory factory(5, &sequence);
  ResolvedColumn column =
      factory.MakeCol("table", "column", types::Int32Type());

  EXPECT_EQ(column.column_id(), 6);
  EXPECT_EQ(column.type(), types::Int32Type());
  EXPECT_EQ(column.table_name(), "table");
  EXPECT_EQ(column.name(), "column");

  // Sequence should have been used.
  EXPECT_EQ(7, sequence.GetNext());
  EXPECT_EQ(factory.max_column_id(), 6);
}

TEST(ColumnFactory, WithSequenceBehindAnnotated) {
  zetasql_base::SequenceNumber sequence;
  ColumnFactory factory(5, &sequence);
  ResolvedColumn column =
      factory.MakeCol("table", "column", {types::Int32Type(), nullptr});

  EXPECT_EQ(column.column_id(), 6);
  EXPECT_EQ(column.type(), types::Int32Type());
  EXPECT_EQ(column.table_name(), "table");
  EXPECT_EQ(column.name(), "column");

  // Sequence should have been used.
  EXPECT_EQ(7, sequence.GetNext());
  EXPECT_EQ(factory.max_column_id(), 6);
}

TEST(ColumnFactory, WithSequenceAhead) {
  zetasql_base::SequenceNumber sequence;
  for (int i = 0; i < 10; ++i) {
    sequence.GetNext();
  }

  ColumnFactory factory(0, &sequence);
  ResolvedColumn column =
      factory.MakeCol("table", "column", types::Int32Type());

  // Should be well past the max column seen passed in of 0.
  EXPECT_EQ(column.column_id(), 10);
  EXPECT_EQ(column.type(), types::Int32Type());
  EXPECT_EQ(column.table_name(), "table");
  EXPECT_EQ(column.name(), "column");

  // Should still get the right max_column_id.
  EXPECT_EQ(11, sequence.GetNext());
  EXPECT_EQ(factory.max_column_id(), 10);
}

TEST(ColumnFactory, WithSequenceAheadAnnotated) {
  zetasql_base::SequenceNumber sequence;
  for (int i = 0; i < 10; ++i) {
    sequence.GetNext();
  }

  ColumnFactory factory(0, &sequence);
  ResolvedColumn column =
      factory.MakeCol("table", "column", {types::Int32Type(), nullptr});

  // Should be well past the max column seen passed in of 0.
  EXPECT_EQ(column.column_id(), 10);
  EXPECT_EQ(column.type(), types::Int32Type());
  EXPECT_EQ(column.table_name(), "table");
  EXPECT_EQ(column.name(), "column");

  // Should still get the right max_column_id.
  EXPECT_EQ(11, sequence.GetNext());
  EXPECT_EQ(factory.max_column_id(), 10);
}

TEST(ColumnFactory, ColumnCollationTest) {
  zetasql_base::SequenceNumber sequence;
  ColumnFactory column_factory(0, &sequence);

  std::unique_ptr<AnnotationMap> annotation_map =
      AnnotationMap::Create(types::StringType());
  annotation_map->SetAnnotation(static_cast<int>(AnnotationKind::kCollation),
                                SimpleValue::String("und:ci"));

  ResolvedColumn collate_column = column_factory.MakeCol(
      "test", "collate", {types::StringType(), annotation_map.get()});
  ZETASQL_ASSERT_OK_AND_ASSIGN(Collation collation,
                       Collation::MakeCollation(*annotation_map));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Collation column_collation,
      Collation::MakeCollation(*collate_column.type_annotation_map()));

  ASSERT_TRUE(collation.Equals(column_collation));
}

TEST(RewriteUtilsTest, CopyAndReplaceColumns) {
  zetasql_base::SequenceNumber sequence;
  ColumnFactory factory(0, &sequence);
  SimpleTable table("tab", {{"col", types::Int64Type()}});
  std::unique_ptr<ResolvedScan> input = MakeResolvedTableScan(
      {factory.MakeCol("t", "c", types::Int64Type())}, &table, nullptr);
  EXPECT_EQ(input->column_list(0).column_id(), 1);

  // Copy 'input' several times. The first time a new column is allocated but
  // subsequent copies will use the column already populated in 'map'.
  ColumnReplacementMap map;
  for (int i = 0; i < 5; ++i) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ResolvedScan> output,
                         CopyResolvedASTAndRemapColumns(*input, factory, map));
    EXPECT_EQ(output->column_list(0).column_id(), 2);
    EXPECT_EQ(map.size(), 1);
  }

  // Repeat the experiment but feed the output of each iteration into the
  // input of the next. In this case we should get a new column each iteration
  // with a incremented column_id.
  map = {};
  for (int i = 1; i < 5; ++i) {
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ResolvedScan> output,
                         CopyResolvedASTAndRemapColumns(*input, factory, map));
    // 2 columns for setup and first loop plus 1 for each iteration of this loop
    EXPECT_EQ(output->column_list(0).column_id(), i + 2);
    EXPECT_EQ(map.size(), i);
    input = std::move(output);
  }
}

TEST(RewriteUtilsTest, SortUniqueColumnRefs) {
  const Type* type = types::StringType();
  zetasql_base::SequenceNumber sequence;
  ColumnFactory factory(0, &sequence);
  ResolvedColumn cola = factory.MakeCol("table", "cola", type);
  ResolvedColumn colb = factory.MakeCol("table", "colb", type);
  ResolvedColumn colc = factory.MakeCol("table", "colc", type);

  bool kCorrelated = true;
  std::vector<std::unique_ptr<const ResolvedColumnRef>> column_refs;
  column_refs.emplace_back(MakeResolvedColumnRef(type, colb, kCorrelated));
  column_refs.emplace_back(MakeResolvedColumnRef(type, cola, kCorrelated));
  column_refs.emplace_back(MakeResolvedColumnRef(type, cola, kCorrelated));
  column_refs.emplace_back(MakeResolvedColumnRef(type, colb, kCorrelated));
  column_refs.emplace_back(MakeResolvedColumnRef(type, colc, kCorrelated));
  column_refs.emplace_back(MakeResolvedColumnRef(type, colc, !kCorrelated));
  SortUniqueColumnRefs(column_refs);

  ASSERT_EQ(column_refs.size(), 4);
  EXPECT_EQ(column_refs[0]->column(), cola);
  EXPECT_EQ(column_refs[1]->column(), colb);
  EXPECT_EQ(column_refs[2]->column(), colc);
  EXPECT_EQ(column_refs[3]->column(), colc);
  EXPECT_FALSE(column_refs[2]->is_correlated());
  EXPECT_TRUE(column_refs[3]->is_correlated());
}

class FunctionCallBuilderTest : public ::testing::Test {
 public:
  FunctionCallBuilderTest()
      : catalog_("function_builder_catalog"),
        fn_builder_(analyzer_options_, catalog_) {
    analyzer_options_.mutable_language()->SetSupportsAllStatementKinds();
    catalog_.AddZetaSQLFunctions();
  }

  AnalyzerOptions analyzer_options_;
  SimpleCatalog catalog_;
  FunctionCallBuilder fn_builder_;
};

TEST_F(FunctionCallBuilderTest, LikeTest) {
  std::unique_ptr<ResolvedExpr> input = MakeResolvedLiteral(
      types::StringType(), Value::String("bar"), /*has_explicit_type=*/true);
  ASSERT_NE(input, nullptr);
  std::unique_ptr<ResolvedExpr> pattern = MakeResolvedLiteral(
      types::StringType(), Value::String("%r"), /*has_explicit_type=*/true);
  ASSERT_NE(pattern, nullptr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const ResolvedExpr> like_fn,
                       fn_builder_.Like(std::move(input), std::move(pattern)));
  EXPECT_EQ(like_fn->DebugString(), absl::StripLeadingAsciiWhitespace(R"(
FunctionCall(ZetaSQL:$like(STRING, STRING) -> BOOL)
+-Literal(type=STRING, value="bar", has_explicit_type=TRUE)
+-Literal(type=STRING, value="%r", has_explicit_type=TRUE)
)"));
}

TEST_F(FunctionCallBuilderTest, CaseNoValueElseTest) {
  std::vector<std::unique_ptr<const ResolvedExpr>> condition_args;
  std::vector<std::unique_ptr<const ResolvedExpr>> result_args;

  condition_args.push_back(MakeResolvedLiteral(
      types::BoolType(), Value::Bool(true), /*has_explicit_type=*/true));
  result_args.push_back(MakeResolvedLiteral(
      types::StringType(), Value::String("foo"), /*has_explicit_type=*/true));
  condition_args.push_back(MakeResolvedLiteral(
      types::BoolType(), Value::Bool(false), /*has_explicit_type=*/true));
  result_args.push_back(MakeResolvedLiteral(
      types::StringType(), Value::String("bar"), /*has_explicit_type=*/true));

  std::unique_ptr<ResolvedExpr> else_result = MakeResolvedLiteral(
      types::StringType(), Value::String("baz"), /*has_explicit_type=*/true);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const ResolvedExpr> case_fn,
      fn_builder_.CaseNoValue(std::move(condition_args), std::move(result_args),
                              std::move(else_result)));
  EXPECT_EQ(case_fn->DebugString(), absl::StripLeadingAsciiWhitespace(R"(
FunctionCall(ZetaSQL:$case_no_value(repeated(2) BOOL, repeated(2) STRING, STRING) -> STRING)
+-Literal(type=BOOL, value=true, has_explicit_type=TRUE)
+-Literal(type=STRING, value="foo", has_explicit_type=TRUE)
+-Literal(type=BOOL, value=false, has_explicit_type=TRUE)
+-Literal(type=STRING, value="bar", has_explicit_type=TRUE)
+-Literal(type=STRING, value="baz", has_explicit_type=TRUE)
)"));
}

TEST_F(FunctionCallBuilderTest, CaseNoValueNoElseTest) {
  std::vector<std::unique_ptr<const ResolvedExpr>> condition_args;
  std::vector<std::unique_ptr<const ResolvedExpr>> result_args;

  condition_args.push_back(MakeResolvedLiteral(
      types::BoolType(), Value::Bool(true), /*has_explicit_type=*/true));
  result_args.push_back(MakeResolvedLiteral(
      types::StringType(), Value::String("foo"), /*has_explicit_type=*/true));
  condition_args.push_back(MakeResolvedLiteral(
      types::BoolType(), Value::Bool(false), /*has_explicit_type=*/true));
  result_args.push_back(MakeResolvedLiteral(
      types::StringType(), Value::String("bar"), /*has_explicit_type=*/true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const ResolvedExpr> case_fn,
      fn_builder_.CaseNoValue(std::move(condition_args), std::move(result_args),
                              nullptr));
  EXPECT_EQ(case_fn->DebugString(), absl::StripLeadingAsciiWhitespace(R"(
FunctionCall(ZetaSQL:$case_no_value(repeated(2) BOOL, repeated(2) STRING) -> STRING)
+-Literal(type=BOOL, value=true, has_explicit_type=TRUE)
+-Literal(type=STRING, value="foo", has_explicit_type=TRUE)
+-Literal(type=BOOL, value=false, has_explicit_type=TRUE)
+-Literal(type=STRING, value="bar", has_explicit_type=TRUE)
)"));
}

TEST_F(FunctionCallBuilderTest, NotTest) {
  std::unique_ptr<ResolvedExpr> input = MakeResolvedLiteral(
      types::BoolType(), Value::Bool(true), /*has_explicit_type=*/true);
  ASSERT_NE(input, nullptr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const ResolvedExpr> not_fn,
                       fn_builder_.Not(std::move(input)));
  EXPECT_EQ(not_fn->DebugString(), absl::StripLeadingAsciiWhitespace(R"(
FunctionCall(ZetaSQL:$not(BOOL) -> BOOL)
+-Literal(type=BOOL, value=true, has_explicit_type=TRUE)
)"));
}

class LikeAnyAllSubqueryScanBuilderTest
    : public ::testing::TestWithParam<ResolvedSubqueryExpr::SubqueryType> {
 public:
  LikeAnyAllSubqueryScanBuilderTest()
      : column_factory_(10, &sequence_),
        catalog_("subquery_scan_builder_catalog"),
        scan_builder_(&analyzer_options_, &catalog_, &column_factory_) {
    analyzer_options_.mutable_language()->SetSupportsAllStatementKinds();
    catalog_.AddZetaSQLFunctions();
  }

  zetasql_base::SequenceNumber sequence_;
  ColumnFactory column_factory_;
  AnalyzerOptions analyzer_options_;
  TypeFactory type_factory_;
  SimpleCatalog catalog_;
  LikeAnyAllSubqueryScanBuilder scan_builder_;
};

TEST_P(LikeAnyAllSubqueryScanBuilderTest, BuildAggregateScan) {
  ResolvedSubqueryExpr::SubqueryType subquery_type = GetParam();

  std::unique_ptr<const AnalyzerOutput> analyzer_expression;
  ZETASQL_ASSERT_OK(AnalyzeExpression("'a' IN (SELECT 'b')", analyzer_options_,
                              &catalog_, &type_factory_, &analyzer_expression));

  const ResolvedSubqueryExpr* subquery_expr =
      analyzer_expression->resolved_expr()->GetAs<ResolvedSubqueryExpr>();
  const ResolvedExpr* input_expr = subquery_expr->in_expr();
  ASSERT_NE(input_expr, nullptr);
  const ResolvedScan* expr_subquery = subquery_expr->subquery();
  ASSERT_NE(expr_subquery, nullptr);

  ColumnReplacementMap map;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResolvedScan> subquery_scan,
      CopyResolvedASTAndRemapColumns(*expr_subquery, column_factory_, map));
  ASSERT_EQ(subquery_scan->column_list_size(), 1);

  ResolvedColumn input_column =
      column_factory_.MakeCol("input", "input_expr", input_expr->type());
  ResolvedColumn subquery_column = subquery_scan->column_list(0);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<ResolvedAggregateScan> aggregate_scan,
                       scan_builder_.BuildAggregateScan(
                           input_column, subquery_column,
                           std::move(subquery_scan), subquery_type));

  std::string logical_function;
  if (subquery_type == ResolvedSubqueryExpr::LIKE_ANY) {
    logical_function = "logical_or";
  } else if (subquery_type == ResolvedSubqueryExpr::LIKE_ALL) {
    logical_function = "logical_and";
  }

  // The ColumnFactory was instantiated with the highest allocated column ID as
  // 10 to reflect that this scan is part of larger ResolvedAST with other
  // columns. Here, the columns start at 11 because that is the column ID after
  // that last column ID in the original ResolvedAST. The order of the column
  // IDs is arbitrary and is set to match what the code does.
  // This tests the DebugString of the newly created scan to check that the
  // ResolvedAST matches the expected ResolvedAST.
  // clang-format off
  EXPECT_EQ(
      aggregate_scan->DebugString(),
      absl::StripLeadingAsciiWhitespace(absl::StrFormat(R"(
AggregateScan
+-column_list=aggregate.[like_agg_col#13, null_agg_col#14]
+-input_scan=
| +-ProjectScan
|   +-column_list=[$expr_subquery.$col1#11]
|   +-expr_list=
|   | +-$col1#11 := Literal(type=STRING, value="b")
|   +-input_scan=
|     +-SingleRowScan
+-aggregate_list=
  +-like_agg_col#13 :=
  | +-AggregateFunctionCall(ZetaSQL:%s(BOOL) -> BOOL)
  |   +-FunctionCall(ZetaSQL:$like(STRING, STRING) -> BOOL)
  |     +-ColumnRef(type=STRING, column=input.input_expr#12, is_correlated=TRUE)
  |     +-ColumnRef(type=STRING, column=$expr_subquery.$col1#11)
  +-null_agg_col#14 :=
    +-AggregateFunctionCall(ZetaSQL:logical_or(BOOL) -> BOOL)
      +-FunctionCall(ZetaSQL:$is_null(STRING) -> BOOL)
        +-ColumnRef(type=STRING, column=$expr_subquery.$col1#11)
)", logical_function)));
  // clang-format on
}

INSTANTIATE_TEST_SUITE_P(BuildAggregateScan, LikeAnyAllSubqueryScanBuilderTest,
                         testing::Values(ResolvedSubqueryExpr::LIKE_ANY,
                                         ResolvedSubqueryExpr::LIKE_ALL));

}  // namespace
}  // namespace zetasql
