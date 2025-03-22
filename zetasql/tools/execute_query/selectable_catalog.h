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

#ifndef ZETASQL_TOOLS_EXECUTE_QUERY_SELECTABLE_CATALOG_H_
#define ZETASQL_TOOLS_EXECUTE_QUERY_SELECTABLE_CATALOG_H_

#include <functional>
#include <string>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql {

// This describes a Catalog that can be selected in execute_query.
class SelectableCatalog {
 public:
  // This is a callback that returns a Catalog.
  // The callback retains ownership of the returned Catalog.
  using GetCatalogCallback =
      std::function<absl::StatusOr<Catalog*>(const LanguageOptions&)>;

  SelectableCatalog(const std::string& name, const std::string& description,
                    GetCatalogCallback callback)
      : name_(name),
        description_(description),
        get_catalog_callback_(callback) {}
  virtual ~SelectableCatalog() = default;

  SelectableCatalog(const SelectableCatalog&) = delete;

  // The name can be used to select this catalog in flags, options, etc.
  virtual absl::string_view name() const { return name_; }

  // This is a description of this catalog.
  virtual absl::string_view description() const { return description_; }

  // This callback returns the actual Catalog object.
  // It may be lazily created, and then shared across multiple requests.
  // The caller does not take ownership, so this Catalog must stay alive.
  virtual absl::StatusOr<Catalog*> GetCatalog(
      const LanguageOptions& language_options) {
    return get_catalog_callback_(language_options);
  }

 private:
  std::string name_;
  std::string description_;

  GetCatalogCallback get_catalog_callback_;
};

struct SelectableCatalogInfo {
  absl::string_view name;
  absl::string_view description;
};

// Get names and descriptions of all known SelectableCatalogs.
const std::vector<SelectableCatalogInfo>& GetSelectableCatalogsInfo();

// Get descriptions of the SelectableCatalogs, for use in flag help.
// Formatted like "name: description\n" for each flag.
std::string GetSelectableCatalogDescriptionsForFlag();

// Find the SelectableCatalog called `name` and return it, or an error.
absl::StatusOr<SelectableCatalog*> FindSelectableCatalog(
    absl::string_view name);

}  // namespace zetasql

#endif  // ZETASQL_TOOLS_EXECUTE_QUERY_SELECTABLE_CATALOG_H_
