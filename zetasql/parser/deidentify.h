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

#ifndef ZETASQL_PARSER_DEIDENTIFY_H_
#define ZETASQL_PARSER_DEIDENTIFY_H_

#include <string>

#include "zetasql/public/language_options.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace parser {

// Return cleaned SQL with comments stripped, all identifiers relabelled
// consistently starting from A and then literals replaced by ? like parameters.
absl::StatusOr<std::string> DeidentifySQLIdentifiersAndLiterals(
    absl::string_view input,
    const zetasql::LanguageOptions& language_options =
        zetasql::LanguageOptions::MaximumFeatures());
}  // namespace parser
}  // namespace zetasql

#endif  // ZETASQL_PARSER_DEIDENTIFY_H_
