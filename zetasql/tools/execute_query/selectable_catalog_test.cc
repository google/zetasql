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

#include "zetasql/tools/execute_query/selectable_catalog.h"

#include <string>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/catalog.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"

namespace zetasql {

TEST(SelectableCatalog, GetSelectableCatalogs) {
  auto selectable_catalogs = GetSelectableCatalogs();
  EXPECT_GE(selectable_catalogs.size(), 2);

  EXPECT_EQ(selectable_catalogs[0]->name(), "none");
  EXPECT_EQ(selectable_catalogs[1]->name(), "sample");
}

TEST(SelectableCatalog, FindSelectableCatalog) {
  EXPECT_FALSE(FindSelectableCatalog("bad").ok());
}

TEST(SelectableCatalog, FindSelectableCatalog_none) {
  auto found = FindSelectableCatalog("none");
  ZETASQL_ASSERT_OK(found);
  SelectableCatalog* selectable = found.value();

  EXPECT_EQ(selectable->name(), "none");
  auto get_catalog_result = selectable->GetCatalog();
  ZETASQL_ASSERT_OK(get_catalog_result);
  // There are no tables in this catalog to look up.
}

// Test that catalog `catalog_name` can be found, and that it includes a table
// called `table_name`.
static void TestCatalog(absl::string_view catalog_name,
                        absl::string_view table_name) {
  auto found = FindSelectableCatalog(catalog_name);
  ZETASQL_ASSERT_OK(found);
  SelectableCatalog* selectable = found.value();

  EXPECT_EQ(selectable->name(), catalog_name);
  auto get_catalog_result = selectable->GetCatalog();
  ZETASQL_ASSERT_OK(get_catalog_result);
  Catalog* catalog = get_catalog_result.value();

  const Table* table;
  ZETASQL_EXPECT_OK(catalog->FindTable({std::string(table_name)}, &table));
  EXPECT_EQ(table->Name(), table_name);
}

TEST(SelectableCatalog, FindSelectableCatalog_sample) {
  TestCatalog("sample", "TestTable");
}

TEST(SelectableCatalog, FindSelectableCatalog_tpch) {
  TestCatalog("tpch", "LineItem");
}

}  // namespace zetasql
