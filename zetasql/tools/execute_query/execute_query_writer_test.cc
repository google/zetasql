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

#include "zetasql/tools/execute_query/execute_query_writer.h"

#include "google/protobuf/descriptor.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/simple_catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {

TEST(ExecuteQueryStreamWriterTest, Parsed) {
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQueryStreamWriter{output}.parsed("short parser string"));
  EXPECT_EQ(output.str(), "short parser string\n");
}

TEST(ExecuteQueryStreamWriterTest, ResolvedSelect) {
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_EXPECT_OK(
      AnalyzeStatement("SELECT 1", {}, nullptr, nullptr, &analyzer_output));
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQueryStreamWriter{output}.resolved(
      *analyzer_output->resolved_statement()));
  EXPECT_EQ(output.str(), R"(QueryStmt
+-output_column_list=
| +-$query.$col1#1 AS `$col1` [INT64]
+-query=
  +-ProjectScan
    +-column_list=[$query.$col1#1]
    +-expr_list=
    | +-$col1#1 := Literal(type=INT64, value=1)
    +-input_scan=
      +-SingleRowScan

)");
}

TEST(ExecuteQueryStreamWriterTest, Explain) {
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_EXPECT_OK(
      AnalyzeStatement("SELECT 1", {}, nullptr, nullptr, &analyzer_output));
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQueryStreamWriter{output}.explained(
      *analyzer_output->resolved_statement(), "short explanation"));
  EXPECT_EQ(output.str(), "short explanation\n");
}

TEST(ExecuteQueryStreamWriterTest, Executed) {
  std::unique_ptr<const AnalyzerOutput> analyzer_output;
  ZETASQL_EXPECT_OK(
      AnalyzeStatement("SELECT 1", {}, nullptr, nullptr, &analyzer_output));
  SimpleTable test_table{"TestTable", {{"col", types::Int64Type()}}};
  test_table.SetContents({{values::Int64(123)}});
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iter,
                       test_table.CreateEvaluatorTableIterator({0}));
  std::ostringstream output;
  ZETASQL_EXPECT_OK(ExecuteQueryStreamWriter{output}.executed(
      *analyzer_output->resolved_statement(), std::move(iter)));
  EXPECT_EQ(output.str(), R"(+-----+
| col |
+-----+
| 123 |
+-----+

)");
}

}  // namespace zetasql
