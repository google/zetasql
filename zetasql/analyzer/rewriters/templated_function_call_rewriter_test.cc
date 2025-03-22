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
#include <utility>
#include <vector>

#include "zetasql/base/atomic_sequence_num.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/rewriter_interface.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {

using ::testing::Eq;
using ::testing::NotNull;

TEST(TemplatedFunctionCallRewriterTest, RewriterNotApplied) {
  TypeFactory types;
  SimpleCatalog catalog("catalog", &types);
  catalog.AddBuiltinFunctions(BuiltinFunctionOptions::AllReleasedFunctions());

  auto table = std::make_unique<SimpleTable>(
      "T",
      std::vector<SimpleTable::NameAndType>{{"string_col", types.get_string()},
                                            {"int64_col", types.get_int64()}});
  catalog.AddOwnedTable(std::move(table));

  zetasql_base::SequenceNumber sequence_number;
  AnalyzerOptions options;
  options.set_column_id_sequence_number(&sequence_number);
  options.mutable_language()->DisableAllLanguageFeatures();
  options.enable_rewrite(
      ResolvedASTRewrite::
          REWRITE_APPLY_ENABLED_REWRITES_TO_TEMPLATED_FUNCTION_CALLS);

  std::unique_ptr<const AnalyzerOutput> output;
  ZETASQL_ASSERT_OK(AnalyzeStatement(R"sql(SELECT int64_col FROM T)sql", options,
                             &catalog, &types, &output));
  ASSERT_THAT(output->resolved_statement(), NotNull());

  // One ID for each table column: string_col#1, int64_col#2
  EXPECT_THAT(output->max_column_id(), Eq(2));

  // There are no templated function calls in the query, so the rewriter should
  // not be applied. As such, the next sequence number should be max_column_id
  // + 1.
  EXPECT_THAT(sequence_number.GetNext(), Eq(3));
}

TEST(TemplatedFunctionCallRewriterTest, RewriterIsApplied) {
  TypeFactory types;
  SimpleCatalog catalog("catalog", &types);
  catalog.AddBuiltinFunctions(BuiltinFunctionOptions::AllReleasedFunctions());

  auto table = std::make_unique<SimpleTable>(
      "T",
      std::vector<SimpleTable::NameAndType>{{"string_col", types.get_string()},
                                            {"int64_col", types.get_int64()}});
  catalog.AddOwnedTable(std::move(table));

  // Add a templated SQL UDF which calls TYPEOF which is implemented via
  // ZetaSQL rewrites.
  catalog.AddOwnedFunction(new TemplatedSQLFunction(
      {"CallTypeOfTemplatedFn"},
      FunctionSignature(types::StringType(),
                        {FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                              FunctionArgumentType::REQUIRED)},
                        /*context_id=*/1),
      /*argument_names=*/{"x"}, ParseResumeLocation::FromString("TYPEOF(x)")));

  zetasql_base::SequenceNumber sequence_number;
  AnalyzerOptions options;
  options.set_column_id_sequence_number(&sequence_number);
  options.mutable_language()->DisableAllLanguageFeatures();
  options.enable_rewrite(
      ResolvedASTRewrite::
          REWRITE_APPLY_ENABLED_REWRITES_TO_TEMPLATED_FUNCTION_CALLS);

  std::unique_ptr<const AnalyzerOutput> output;
  ZETASQL_ASSERT_OK(AnalyzeStatement(
      R"sql(SELECT CallTypeOfTemplatedFn(int64_col) FROM T)sql", options,
      &catalog, &types, &output));
  ASSERT_THAT(output->resolved_statement(), NotNull());

  // One ID for each table column: string_col#1, int64_col#2, and the output
  // column from the function call: $col1#3
  EXPECT_THAT(output->max_column_id(), Eq(3));

  // When a rewriter is applied, the rewriter library needs to compute the
  // max_columnn_id. To do this, the library must call GetNext() on the sequence
  // number, which increments the sequence number to 4, which is why this call
  // returns 5.
  EXPECT_THAT(sequence_number.GetNext(), Eq(5));
}

}  // namespace zetasql
