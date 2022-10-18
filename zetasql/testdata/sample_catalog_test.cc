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

#include "zetasql/base/testing/status_matchers.h"
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

}  // namespace zetasql
