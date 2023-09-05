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

#include "zetasql/base/testing/status_matchers.h"  
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/time/time.h"

ABSL_DECLARE_FLAG(double, zetasql_stack_usage_proportion_warning);
ABSL_DECLARE_FLAG(bool, zetasql_disable_rewriter_checker);

using ::testing::Gt;

namespace zetasql {

TEST(RewriteResolvedAstTest, RewriterDoesNotConflictWithExpressionColumnNames) {
  // The map function rewriters use a variable called "m" and a variable called
  // "k" with AnalyzeSubstitute to produce the rewrites. We want to verify that
  // these variables don't conflict with an expression column in the analyzer
  // options.
  AnalyzerOptions options;
  options.mutable_language()->EnableMaximumLanguageFeatures();
  ZETASQL_CHECK_OK(options.AddExpressionColumn("k", types::Int64Type()));

  TypeFactory types;
  const Type* map_type;
  ZETASQL_CHECK_OK(types.MakeProtoType(
      zetasql_test__::MessageWithMapField::descriptor(), &map_type));
  ZETASQL_CHECK_OK(options.AddExpressionColumn("mapproto", map_type));

  SimpleCatalog catalog("catalog", &types);
  catalog.AddBuiltinFunctions(BuiltinFunctionOptions::AllReleasedFunctions());

  std::unique_ptr<const AnalyzerOutput> output;
  auto status = zetasql::AnalyzeExpression(
      "mapproto.string_int32_map[SAFE_KEY('foo')] + k", options, &catalog,
      &types, &output);
  EXPECT_TRUE(status.ok()) << status;
  if (output == nullptr) return;

  // Before we fixed the bug, this expression would not have analyzed correctly.
  // After, note that we can still see the user-provided resolved expression
  // columns in the appropriate places in the AST, but these names do not
  // conflict with the internal variable names for the map function rewrite
  // rules.
  EXPECT_THAT(output->resolved_expr()->DebugString(),
              R"sql(FunctionCall(ZetaSQL:$add(INT64, INT64) -> INT64)
+-Cast(INT32 -> INT64)
| +-SubqueryExpr
|   +-type=INT32
|   +-subquery_type=SCALAR
|   +-subquery=
|     +-ProjectScan
|       +-column_list=[$expr_subquery.$col1#6]
|       +-expr_list=
|       | +-$col1#6 :=
|       |   +-FunctionCall(ZetaSQL:$case_no_value(repeated(2) BOOL, repeated(2) INT32, INT32) -> INT32)
|       |     +-FunctionCall(ZetaSQL:$is_null(ARRAY<PROTO<zetasql_test__.MessageWithMapField.StringInt32MapEntry>>) -> BOOL)
|       |     | +-ColumnRef(type=ARRAY<PROTO<zetasql_test__.MessageWithMapField.StringInt32MapEntry>>, column=$subquery1.m#2)
|       |     +-Literal(type=INT32, value=NULL)
|       |     +-FunctionCall(ZetaSQL:$is_null(STRING) -> BOOL)
|       |     | +-ColumnRef(type=STRING, column=$subquery1.k#1)
|       |     +-Literal(type=INT32, value=NULL)
|       |     +-SubqueryExpr
|       |       +-type=INT32
|       |       +-subquery_type=SCALAR
|       |       +-parameter_list=
|       |       | +-ColumnRef(type=STRING, column=$subquery1.k#1)
|       |       | +-ColumnRef(type=ARRAY<PROTO<zetasql_test__.MessageWithMapField.StringInt32MapEntry>>, column=$subquery1.m#2)
|       |       +-subquery=
|       |         +-LimitOffsetScan
|       |           +-column_list=[$expr_subquery.value#5]
|       |           +-input_scan=
|       |           | +-OrderByScan
|       |           |   +-column_list=[$expr_subquery.value#5]
|       |           |   +-is_ordered=TRUE
|       |           |   +-input_scan=
|       |           |   | +-ProjectScan
|       |           |   |   +-column_list=[$array.elem#3, $array_offset.offset#4, $expr_subquery.value#5]
|       |           |   |   +-expr_list=
|       |           |   |   | +-value#5 :=
|       |           |   |   |   +-GetProtoField
|       |           |   |   |     +-type=INT32
|       |           |   |   |     +-expr=
|       |           |   |   |     | +-ColumnRef(type=PROTO<zetasql_test__.MessageWithMapField.StringInt32MapEntry>, column=$array.elem#3)
|       |           |   |   |     +-field_descriptor=value
|       |           |   |   |     +-default_value=0
|       |           |   |   +-input_scan=
|       |           |   |     +-FilterScan
|       |           |   |       +-column_list=[$array.elem#3, $array_offset.offset#4]
|       |           |   |       +-input_scan=
|       |           |   |       | +-ArrayScan
|       |           |   |       |   +-column_list=[$array.elem#3, $array_offset.offset#4]
|       |           |   |       |   +-array_expr=
|       |           |   |       |   | +-ColumnRef(type=ARRAY<PROTO<zetasql_test__.MessageWithMapField.StringInt32MapEntry>>, column=$subquery1.m#2, is_correlated=TRUE)
|       |           |   |       |   +-element_column=$array.elem#3
|       |           |   |       |   +-array_offset_column=
|       |           |   |       |     +-ColumnHolder(column=$array_offset.offset#4)
|       |           |   |       +-filter_expr=
|       |           |   |         +-FunctionCall(ZetaSQL:$equal(STRING, STRING) -> BOOL)
|       |           |   |           +-GetProtoField
|       |           |   |           | +-type=STRING
|       |           |   |           | +-expr=
|       |           |   |           | | +-ColumnRef(type=PROTO<zetasql_test__.MessageWithMapField.StringInt32MapEntry>, column=$array.elem#3)
|       |           |   |           | +-field_descriptor=key
|       |           |   |           | +-default_value=""
|       |           |   |           +-ColumnRef(type=STRING, column=$subquery1.k#1, is_correlated=TRUE)
|       |           |   +-order_by_item_list=
|       |           |     +-OrderByItem
|       |           |       +-column_ref=
|       |           |       | +-ColumnRef(type=INT64, column=$array_offset.offset#4)
|       |           |       +-is_descending=TRUE
|       |           +-limit=
|       |             +-Literal(type=INT64, value=1)
|       +-input_scan=
|         +-ProjectScan
|           +-column_list=$subquery1.[k#1, m#2]
|           +-expr_list=
|           | +-k#1 := Literal(type=STRING, value="foo")
|           | +-m#2 :=
|           |   +-GetProtoField
|           |     +-type=ARRAY<PROTO<zetasql_test__.MessageWithMapField.StringInt32MapEntry>>
|           |     +-expr=
|           |     | +-ExpressionColumn(type=PROTO<zetasql_test__.MessageWithMapField>, name="mapproto")
|           |     +-field_descriptor=string_int32_map
|           |     +-default_value=[]
|           +-input_scan=
|             +-SingleRowScan
+-ExpressionColumn(type=INT64, name="k")
)sql");
}

TEST(RewriteResolvedAstTest, RewriterWarnsComplextyJustOnce) {
  double old_value =
      absl::GetFlag(FLAGS_zetasql_stack_usage_proportion_warning);
  // Always warn.
  absl::SetFlag(&FLAGS_zetasql_stack_usage_proportion_warning, 0.0);
  AnalyzerOptions options;
  options.mutable_language()->EnableMaximumLanguageFeatures();
  ZETASQL_CHECK_OK(options.AddExpressionColumn("k", types::Int64Type()));

  TypeFactory types;
  const Type* map_type;
  ZETASQL_CHECK_OK(types.MakeProtoType(
      zetasql_test__::MessageWithMapField::descriptor(), &map_type));
  ZETASQL_CHECK_OK(options.AddExpressionColumn("mapproto", map_type));

  SimpleCatalog catalog("catalog", &types);
  catalog.AddBuiltinFunctions(BuiltinFunctionOptions::AllReleasedFunctions());

  std::unique_ptr<const AnalyzerOutput> output;
  auto status = zetasql::AnalyzeExpression(
      "mapproto.string_int32_map[SAFE_KEY('foo')] + k", options, &catalog,
      &types, &output);
  ZETASQL_EXPECT_OK(status);

  EXPECT_THAT(output->deprecation_warnings(),
              ::testing::ElementsAre(zetasql_base::testing::StatusIs(
                  absl::StatusCode::kResourceExhausted)));
  absl::SetFlag(&FLAGS_zetasql_stack_usage_proportion_warning, old_value);
}

TEST(RewriteResolvedAstTest, RewriteWithRewriteDetectedDisabled) {
  absl::SetFlag(&FLAGS_zetasql_disable_rewriter_checker, true);
  AnalyzerOptions options;
  options.mutable_language()->EnableMaximumLanguageFeatures();
  ZETASQL_CHECK_OK(options.AddExpressionColumn("k", types::Int64Type()));

  TypeFactory types;
  const Type* map_type;
  ZETASQL_CHECK_OK(types.MakeProtoType(
      zetasql_test__::MessageWithMapField::descriptor(), &map_type));
  ZETASQL_CHECK_OK(options.AddExpressionColumn("mapproto", map_type));

  SimpleCatalog catalog("catalog", &types);
  catalog.AddBuiltinFunctions(BuiltinFunctionOptions::AllReleasedFunctions());

  std::unique_ptr<const AnalyzerOutput> output;
  auto status =
      AnalyzeExpression("mapproto.string_int32_map[SAFE_KEY('foo')] + k",
                        options, &catalog, &types, &output);
  ZETASQL_EXPECT_OK(status);
  EXPECT_TRUE(output->resolved_expr() != nullptr);
  EXPECT_THAT(output->runtime_info().rewriters_timed_value().elapsed_duration(),
              Gt(absl::Nanoseconds(0)));
}

}  // namespace zetasql
