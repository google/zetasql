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

#include "zetasql/testdata/sample_catalog.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/sql_view.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"

namespace zetasql {

TEST(SampleCatalogTest, ValueTableView) {
  LanguageOptions options;
  options.EnableMaximumLanguageFeatures();
  TypeFactory type_factory;
  SampleCatalog sample(options, &type_factory);

  for (auto& [name, is_value_table] :
       std::vector<std::pair<absl::string_view, bool>>{
           {"OneStructView", false},
           {"AsStructView", true},
           {"OneScalarView", false},
           {"AsScalarView", true},
       }) {
    const Table* tab = nullptr;
    ZETASQL_ASSERT_OK(sample.catalog()->GetTable(std::string(name), &tab));

    const SQLView* view = tab->GetAs<SQLView>();
    ASSERT_NE(view, nullptr);
    EXPECT_EQ(view->IsValueTable(), is_value_table) << "For: " << name;
    EXPECT_GE(view->NumColumns(), 1);
    EXPECT_EQ(view->sql_security(), SQLView::kSecurityInvoker);
  }

  for (absl::string_view name :
       {"DefinerRightsView", "UnspecifiedRightsView"}) {
    const Table* tab = nullptr;
    ZETASQL_ASSERT_OK(sample.catalog()->GetTable(std::string(name), &tab));
    const SQLView* view = tab->GetAs<SQLView>();
    ASSERT_NE(view, nullptr);
    EXPECT_EQ(view->sql_security(), SQLView::kSecurityDefiner);
  }
}

TEST(SampleCatalogTest, DefinerRightsTvfHaveTheRightSecuritySettings) {
  LanguageOptions options;
  options.EnableMaximumLanguageFeatures();
  TypeFactory type_factory;
  SampleCatalog sample(options, &type_factory);

  const TableValuedFunction* tvf = nullptr;
  ZETASQL_ASSERT_OK(sample.catalog()->GetTableValuedFunction("DefinerRightsTvf", &tvf));
  EXPECT_EQ(tvf->sql_security(),
            ResolvedCreateStatementEnums::SQL_SECURITY_DEFINER);
}

TEST(SampleCatalogTest, TemplatedDefinerRightsTvfHaveTheRightSecuritySettings) {
  LanguageOptions options;
  options.EnableMaximumLanguageFeatures();
  TypeFactory type_factory;
  SampleCatalog sample(options, &type_factory);

  const TableValuedFunction* tvf = nullptr;
  ZETASQL_ASSERT_OK(sample.catalog()->GetTableValuedFunction(
      "definer_rights_templated_tvf", &tvf));
  EXPECT_EQ(tvf->sql_security(),
            ResolvedCreateStatementEnums::SQL_SECURITY_DEFINER);
}

TEST(SampleCatalogTest,
     DefinerRightsScalarFunctionHasTheRightSecuritySettings) {
  LanguageOptions options;
  options.EnableMaximumLanguageFeatures();
  TypeFactory type_factory;
  SampleCatalog sample(options, &type_factory);

  const Function* function = nullptr;
  ZETASQL_ASSERT_OK(sample.catalog()->GetFunction("scalar_function_definer_rights",
                                          &function));
  EXPECT_EQ(function->sql_security(),
            ResolvedCreateStatementEnums::SQL_SECURITY_DEFINER);
}

TEST(SampleCatalogTest,
     TemplatedDefinerRightsScalarFunctionHasTheRightSecuritySettings) {
  LanguageOptions options;
  options.EnableMaximumLanguageFeatures();
  TypeFactory type_factory;
  SampleCatalog sample(options, &type_factory);

  const Function* function = nullptr;
  ZETASQL_ASSERT_OK(sample.catalog()->GetFunction("templated_scalar_definer_rights",
                                          &function));
  EXPECT_EQ(function->sql_security(),
            ResolvedCreateStatementEnums::SQL_SECURITY_DEFINER);
}

TEST(SampleCatalogTest, SequenceFunction) {
  LanguageOptions options;
  options.EnableMaximumLanguageFeatures();
  TypeFactory type_factory;
  SampleCatalog sample(options, &type_factory);

  const Function* function = nullptr;
  ZETASQL_ASSERT_OK(sample.catalog()->GetFunction("fn_with_sequence_arg", &function));
  EXPECT_NE(function, nullptr);
}

// Compare output on column listing and Find methods for tables with each
// ColumnListMode.
TEST(SampleCatalogTest, LazyTables) {
  LanguageOptions options;
  options.EnableMaximumLanguageFeatures();
  TypeFactory type_factory;
  SampleCatalog sample(options, &type_factory);

  Catalog::FindOptions find_options;
  Table::LazyColumnsTableScanContext context;

  // Test the three KeyValue tables with the different ColumnListModes.
  const Table* table_default = nullptr;
  ZETASQL_ASSERT_OK(sample.catalog()->GetTable("KeyValue", &table_default));
  const Table* table_lazy = nullptr;
  ZETASQL_ASSERT_OK(sample.catalog()->GetTable("KeyValueLazy", &table_lazy));
  const Table* table_find_only = nullptr;
  ZETASQL_ASSERT_OK(sample.catalog()->GetTable("KeyValueFindOnly", &table_find_only));

  EXPECT_EQ(table_default->GetColumnListMode(), Table::ColumnListMode::DEFAULT);
  EXPECT_EQ(table_lazy->GetColumnListMode(), Table::ColumnListMode::LAZY);
  EXPECT_EQ(table_find_only->GetColumnListMode(),
            Table::ColumnListMode::FIND_ONLY);

  for (const Table* table : {table_default, table_lazy, table_find_only}) {
    // NumColumns, GetColumn and FindColumnByName work on DEFAULT table only.
    if (table == table_default) {
      EXPECT_EQ(table->GetColumnListMode(), Table::ColumnListMode::DEFAULT);
      EXPECT_TRUE(table->HasColumnList());
      EXPECT_EQ(2, table->NumColumns());
      EXPECT_EQ(table->GetColumn(0)->Name(), "Key");
      EXPECT_EQ(table->FindColumnByName("keY")->Name(), "Key");
      EXPECT_EQ(nullptr, table->FindColumnByName("BadCol"));
    } else {
      EXPECT_EQ(0, table->NumColumns());
      EXPECT_EQ(nullptr, table->FindColumnByName("Key"));
    }

    // ListLazyColumns works except on FIND_ONLY tables.
    if (table == table_find_only) {
      EXPECT_FALSE(table->SupportsListLazyColumns());
    } else {
      EXPECT_TRUE(table->SupportsListLazyColumns());

      ZETASQL_ASSERT_OK_AND_ASSIGN(auto list_result,
                           table->ListLazyColumns(&context, find_options));
      EXPECT_EQ(list_result.size(), 2);
      EXPECT_EQ(list_result[0]->Name(), "Key");
      EXPECT_EQ(list_result[1]->Name(), "Value");
    }

    // FindLazyColumn is always supported.
    ZETASQL_ASSERT_OK_AND_ASSIGN(auto find_result,
                         table->FindLazyColumn("Key", &context, find_options));
    ASSERT_TRUE(find_result != nullptr);
    EXPECT_EQ(find_result->Name(), "Key");

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        find_result, table->FindLazyColumn("BadCol", &context, find_options));
    EXPECT_EQ(find_result, nullptr);

    ZETASQL_ASSERT_OK_AND_ASSIGN(auto find_multi_result,
                         table->FindLazyColumns({"keY", "bad", "vaLUE"},
                                                &context, find_options));
    ASSERT_EQ(find_multi_result.size(), 3);
    EXPECT_EQ(find_multi_result[0].value()->Name(), "Key");
    EXPECT_EQ(find_multi_result[1].value(), nullptr);
    EXPECT_EQ(find_multi_result[2].value()->Name(), "Value");

    for (int mode = 0; mode < 2; ++mode) {
      // Try making an iterator, reading column Key.
      // In mode 0, use column_index_list (in default ColumnListMode only).
      // In mode 1, use table_column_list.
      std::unique_ptr<EvaluatorTableIterator> iterator;
      if (mode == 0) {
        if (table != table_default) continue;

        ZETASQL_ASSERT_OK_AND_ASSIGN(iterator,
                             table->CreateEvaluatorTableIterator({0}));
      } else {
        ZETASQL_ASSERT_OK_AND_ASSIGN(
            const Column* key_column,
            table->FindLazyColumn("Key", &context, find_options));
        ZETASQL_ASSERT_OK_AND_ASSIGN(
            iterator,
            table->CreateEvaluatorTableIteratorFromColumns({key_column}));
      }

      // The values in the column are always {1,2}.
      EXPECT_TRUE(iterator->NextRow());
      EXPECT_EQ(iterator->GetValue(0).int64_value(), 1);

      EXPECT_TRUE(iterator->NextRow());
      EXPECT_EQ(iterator->GetValue(0).int64_value(), 2);

      EXPECT_FALSE(iterator->NextRow());
      ZETASQL_EXPECT_OK(iterator->Status());
    }
  }
}

}  // namespace zetasql
