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

#include "zetasql/public/multi_catalog.h"

#include <string>

#include "zetasql/base/logging.h"
#include "absl/status/status.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {

class Procedure;
class TableValuedFunction;

absl::Status MultiCatalog::Create(
    absl::string_view name, const std::vector<Catalog*>& catalog_list,
    std::unique_ptr<MultiCatalog>* multi_catalog) {
  for (const Catalog* catalog : catalog_list) {
    ZETASQL_RET_CHECK_NE(nullptr, catalog)
        << "MultiCatalog does not support NULL catalogs";
  }
  multi_catalog->reset(new MultiCatalog(name, catalog_list));
  return absl::OkStatus();
}

void MultiCatalog::AppendCatalog(Catalog* catalog) {
  ZETASQL_CHECK(catalog != nullptr);
  catalog_list_.push_back(catalog);
}

absl::Status MultiCatalog::FindTable(const absl::Span<const std::string>& path,
                                     const Table** table,
                                     const FindOptions& options) {
  for (Catalog* catalog : catalog_list_) {
    const absl::Status find_status = catalog->FindTable(path, table, options);
    if (!absl::IsNotFound(find_status)) {
      return find_status;
    }
  }
  return TableNotFoundError(path);
}

absl::Status MultiCatalog::FindModel(const absl::Span<const std::string>& path,
                                     const Model** model,
                                     const FindOptions& options) {
  for (Catalog* catalog : catalog_list_) {
    const absl::Status find_status = catalog->FindModel(path, model, options);
    if (!absl::IsNotFound(find_status)) {
      return find_status;
    }
  }
  return ModelNotFoundError(path);
}

absl::Status MultiCatalog::FindFunction(
    const absl::Span<const std::string>& path, const Function** function,
    const FindOptions& options) {
  for (Catalog* catalog : catalog_list_) {
    const absl::Status find_status =
        catalog->FindFunction(path, function, options);
    if (!absl::IsNotFound(find_status)) {
      return find_status;
    }
  }
  return FunctionNotFoundError(path);
}

absl::Status MultiCatalog::FindTableValuedFunction(
    const absl::Span<const std::string>& path,
    const TableValuedFunction** function, const FindOptions& options) {
  for (Catalog* catalog : catalog_list_) {
    const absl::Status find_status =
        catalog->FindTableValuedFunction(path, function, options);
    if (!absl::IsNotFound(find_status)) {
      return find_status;
    }
  }
  return TableValuedFunctionNotFoundError(path);
}

absl::Status MultiCatalog::FindProcedure(
    const absl::Span<const std::string>& path, const Procedure** procedure,
    const FindOptions& options) {
  for (Catalog* catalog : catalog_list_) {
    const absl::Status find_status =
        catalog->FindProcedure(path, procedure, options);
    if (!absl::IsNotFound(find_status)) {
      return find_status;
    }
  }
  return ProcedureNotFoundError(path);
}

absl::Status MultiCatalog::FindType(const absl::Span<const std::string>& path,
                                    const Type** type,
                                    const FindOptions& options) {
  for (Catalog* catalog : catalog_list_) {
    const absl::Status find_status = catalog->FindType(path, type, options);
    if (!absl::IsNotFound(find_status)) {
      return find_status;
    }
  }
  return TypeNotFoundError(path);
}

absl::Status MultiCatalog::FindConstantWithPathPrefix(
    const absl::Span<const std::string> path, int* num_names_consumed,
    const Constant** constant, const FindOptions& options) {
  for (Catalog* catalog : catalog_list_) {
    const absl::Status find_status = catalog->FindConstantWithPathPrefix(
        path, num_names_consumed, constant, options);
    if (!absl::IsNotFound(find_status)) {
      return find_status;
    }
  }
  return TypeNotFoundError(path);
}

std::string MultiCatalog::SuggestTable(
    const absl::Span<const std::string>& mistyped_path) {
  for (Catalog* catalog : catalog_list_) {
    const std::string suggestion = catalog->SuggestTable(mistyped_path);
    if (!suggestion.empty()) {
      return suggestion;
    }
  }
  return "";
}

std::string MultiCatalog::SuggestFunction(
    const absl::Span<const std::string>& mistyped_path) {
  for (Catalog* catalog : catalog_list_) {
    const std::string suggestion = catalog->SuggestFunction(mistyped_path);
    if (!suggestion.empty()) {
      return suggestion;
    }
  }
  return "";
}

std::string MultiCatalog::SuggestTableValuedFunction(
    const absl::Span<const std::string>& mistyped_path) {
  for (Catalog* catalog : catalog_list_) {
    const std::string suggestion =
        catalog->SuggestTableValuedFunction(mistyped_path);
    if (!suggestion.empty()) {
      return suggestion;
    }
  }
  return "";
}

std::string MultiCatalog::SuggestConstant(
    const absl::Span<const std::string>& mistyped_path) {
  for (Catalog* catalog : catalog_list_) {
    const std::string suggestion = catalog->SuggestConstant(mistyped_path);
    if (!suggestion.empty()) {
      return suggestion;
    }
  }
  return "";
}

std::vector<std::string> MultiCatalog::CatalogNames() const {
  std::vector<std::string> names;
  for (const Catalog* catalog : catalog_list_) {
    names.push_back(catalog->FullName());
  }
  return names;
}

}  // namespace zetasql
