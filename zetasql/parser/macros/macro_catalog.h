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

#ifndef ZETASQL_PARSER_MACROS_MACRO_CATALOG_H_
#define ZETASQL_PARSER_MACROS_MACRO_CATALOG_H_

#include <string>

#include "absl/container/flat_hash_map.h"

namespace zetasql {
namespace parser {
namespace macros {

// Represents the catalog of existing macros and their definitions.
// This will likely develop into an interface for more sophisticated catalog in
// the future, like catalog.h, with multi-part paths.
// For now is matching existing catalog APIs for compatibility.
using MacroCatalog = absl::flat_hash_map<std::string, std::string>;

}  // namespace macros
}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_MACROS_MACRO_CATALOG_H_
