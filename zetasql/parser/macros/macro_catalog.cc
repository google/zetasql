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

#include "zetasql/parser/macros/macro_catalog.h"

#include <optional>
#include <string>
#include <utility>

#include "absl/container/node_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {
namespace macros {

absl::Status MacroCatalog::RegisterMacro(MacroInfo macro_info) {
  std::string macro_name(macro_info.name());
  auto [it, success] =
      options_.allow_overwrite
          ? macros_.insert_or_assign(macro_name, std::move(macro_info))
          : macros_.insert({macro_name, std::move(macro_info)});
  return success ? absl::OkStatus()
                 : absl::AlreadyExistsError(
                       absl::StrCat("Macro ", macro_name, " already exists"));
}

std::optional<MacroInfo> MacroCatalog::Find(
    absl::string_view macro_name) const {
  auto it = macros_.find(macro_name);
  if (it == macros_.end()) {
    return std::nullopt;
  }
  return it->second;
}

}  // namespace macros
}  // namespace parser
}  // namespace zetasql
