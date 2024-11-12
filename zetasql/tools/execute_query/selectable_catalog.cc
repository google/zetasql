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

#include <memory>
#include <string>
#include <vector>

#include "zetasql/examples/tpch/catalog/tpch_catalog.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/testdata/sample_catalog_impl.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// Get an initialized SampleCatalogImpl or an error.
static absl::StatusOr<std::unique_ptr<SampleCatalogImpl>> GetSampleCatalog() {
  auto catalog = std::make_unique<SampleCatalogImpl>();
  ZETASQL_RETURN_IF_ERROR(catalog->LoadCatalogImpl(
      ZetaSQLBuiltinFunctionOptions(LanguageOptions())));
  return catalog;
}

static std::vector<SelectableCatalog*>* InitSelectableCatalogsVector() {
  auto catalogs = std::make_unique<std::vector<SelectableCatalog*>>();

  catalogs->push_back(new SelectableCatalog("none", "Empty catalog", [] {
    static auto catalog = new SimpleCatalog("simple_catalog");
    return catalog;
  }));

  catalogs->push_back(new SelectableCatalog(
      "sample", "Analyzer-test schema", []() -> absl::StatusOr<Catalog*> {
        static auto* catalog =
            new absl::StatusOr<std::unique_ptr<SampleCatalogImpl>>(
                GetSampleCatalog());
        ZETASQL_RETURN_IF_ERROR(catalog->status());
        return catalog->value()->catalog();
      }));

  catalogs->push_back(new SelectableCatalog(
      "tpch", "TPCH tables (1MB)", []() -> absl::StatusOr<Catalog*> {
        static auto* catalog =
            new absl::StatusOr<std::unique_ptr<Catalog>>(MakeTpchCatalog());

        ZETASQL_RETURN_IF_ERROR(catalog->status());
        return catalog->value().get();
      }));

  return catalogs.release();
}

const std::vector<SelectableCatalog*>& GetSelectableCatalogs() {
  static const auto* catalogs = InitSelectableCatalogsVector();
  return *catalogs;
}

std::string GetSelectableCatalogDescriptionsForFlag() {
  std::vector<std::string> values;
  for (const SelectableCatalog* catalog : GetSelectableCatalogs()) {
    values.push_back(
        absl::StrCat(catalog->name(), ": ", catalog->description()));
  }
  return absl::StrJoin(values, "\n");
}

absl::StatusOr<SelectableCatalog*> FindSelectableCatalog(
    absl::string_view name) {
  for (SelectableCatalog* catalog : GetSelectableCatalogs()) {
    if (catalog->name() == name) {
      return catalog;
    }
  }
  return zetasql_base::InvalidArgumentErrorBuilder() << "Catalog not found: " << name;
}

}  // namespace zetasql
