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

#include "zetasql/examples/tpch/catalog/tpch_catalog.h"

#include <cstdint>
#include <memory>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/simple_catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {

static void CheckRowCount(SimpleCatalog& catalog, const char* table_name,
                          int expected_num_rows) {
  const Table* table;
  ZETASQL_ASSERT_OK(catalog.GetTable(table_name, &table));

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EvaluatorTableIterator> iterator,
                       table->CreateEvaluatorTableIterator(/*column_idxs=*/{}));

  int64_t num_rows = 0;
  while (iterator->NextRow()) {
    ++num_rows;
  }
  ZETASQL_ASSERT_OK(iterator->Status());
  EXPECT_EQ(num_rows, expected_num_rows);
}

TEST(TpchCatalog, Make) {
  // Run the test twice to make sure that when we make a new catalog, the
  // cached values from the previous catalog still work.
  for (int iter = 0; iter < 2; ++iter) {
    auto make_result = MakeTpchCatalog();
    ZETASQL_EXPECT_OK(make_result);

    SimpleCatalog& catalog = *make_result.value();
    EXPECT_EQ(catalog.tables().size(), 8);

    // Test a sample of one expected table and column.
    const Table* table;
    ZETASQL_EXPECT_OK(catalog.GetTable("Orders", &table));
    EXPECT_EQ(table->NumColumns(), 9);
    EXPECT_EQ(table->GetColumn(3)->Name(), "O_TOTALPRICE");
    EXPECT_EQ(table->GetColumn(3)->GetType()->DebugString(), "DOUBLE");

    // Check reading content from all the tables works, and we get the
    // right row counts.  Case insenitive table name lookup works.
    CheckRowCount(catalog, "customer", 150);
    CheckRowCount(catalog, "lineITEM", 6005);
    CheckRowCount(catalog, "nation", 25);
    CheckRowCount(catalog, "orders", 1500);
    CheckRowCount(catalog, "part", 200);
    CheckRowCount(catalog, "partsupp", 800);
    CheckRowCount(catalog, "region", 5);
    CheckRowCount(catalog, "supplier", 10);

    // Check the first row of Orders, where we can check the value of one column
    // of each supported type.
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<EvaluatorTableIterator> iterator,
        table->CreateEvaluatorTableIterator(/*column_idxs=*/{}));
    ASSERT_TRUE(iterator->NextRow());

    EXPECT_EQ(iterator->GetColumnName(1), "O_CUSTKEY");
    EXPECT_EQ(iterator->GetColumnType(1)->DebugString(), "UINT64");
    EXPECT_EQ(iterator->GetValue(1).DebugString(), "37");

    EXPECT_EQ(iterator->GetColumnName(3), "O_TOTALPRICE");
    EXPECT_EQ(iterator->GetColumnType(3)->DebugString(), "DOUBLE");
    EXPECT_EQ(iterator->GetValue(3).DebugString(), "131251.81");

    EXPECT_EQ(iterator->GetColumnName(4), "O_ORDERDATE");
    EXPECT_EQ(iterator->GetColumnType(4)->DebugString(), "DATE");
    EXPECT_EQ(iterator->GetValue(4).DebugString(), "1996-01-02");

    EXPECT_EQ(iterator->GetColumnName(5), "O_ORDERPRIORITY");
    EXPECT_EQ(iterator->GetColumnType(5)->DebugString(), "STRING");
    EXPECT_EQ(iterator->GetValue(5).DebugString(), "\"5-LOW\"");

    EXPECT_EQ(iterator->GetColumnName(7), "O_SHIPPRIORITY");
    EXPECT_EQ(iterator->GetColumnType(7)->DebugString(), "INT64");
    EXPECT_EQ(iterator->GetValue(7).DebugString(), "0");
  }
}

}  // namespace zetasql
