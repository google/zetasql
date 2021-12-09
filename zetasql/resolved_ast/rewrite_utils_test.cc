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

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

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

TEST(ColumnFactory, WithSequenceAhead) {
  zetasql_base::SequenceNumber sequence;
  for (int i = 0; i < 10; ++i) { sequence.GetNext(); }

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

}  // namespace
}  // namespace zetasql
