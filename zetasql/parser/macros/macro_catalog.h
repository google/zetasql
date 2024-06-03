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

#include <optional>
#include <string>

#include "zetasql/public/parse_location.h"
#include "absl/container/node_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {
namespace macros {

// Keep this cheap to copy.
struct MacroInfo {
  // The contents of the source where this macro was defined. This is needed
  // when printing error messages to show the definition in its context.
  absl::string_view source_text;

  // Location of the macro definition, starting from the DEFINE keyword.
  ParseLocationRange location;

  // Location of the macro name.
  ParseLocationRange name_location;

  // Location of the start of the macro body.
  ParseLocationRange body_location;

  // Optional line and column where the macro definition starts. Useful when
  // the full original input source is unavailable, and `source_text` contains
  // only the definition source, to report accurate locations.
  // Note that these are not themselves the offsets, but rather the actual
  // line and column in the original input source, 1-based.
  // The offsets are simply computed by subtracting 1.
  int definition_start_line = 1;
  int definition_start_column = 1;

  // Returns the name of this macro.
  absl::string_view name() const {
    return name_location.GetTextFrom(source_text);
  }

  // Returns the body of this macro.
  absl::string_view body() const {
    return body_location.GetTextFrom(source_text);
  }
};

// Keep this cheap to copy.
struct MacroCatalogOptions {
  // If enabled, allows overwriting existing macro definitions in the catalog
  // when registering a macro with an existing name.
  bool allow_overwrite = false;
};

// Represents the catalog of existing macros and their definitions.
// This will likely develop into an interface for more sophisticated catalog in
// the future, like catalog.h, with multi-part paths.
class MacroCatalog {
 public:
  explicit MacroCatalog(MacroCatalogOptions options = {}) : options_(options) {}

  // Registers the given macro. Returns an error if it fails, i.e. because a
  // macro with this name already exists and `allow_overwrite_` is not enabled.
  absl::Status RegisterMacro(MacroInfo macro_info);

  // Returns the MacroInfo for the given name, or nullopt if not found.
  std::optional<MacroInfo> Find(absl::string_view macro_name) const;

 private:
  MacroCatalogOptions options_;

  // Uses node_hash_map<> for pointer stability.
  absl::node_hash_map<std::string, MacroInfo> macros_;
};

}  // namespace macros
}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_MACROS_MACRO_CATALOG_H_
