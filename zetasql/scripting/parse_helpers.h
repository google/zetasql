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

#ifndef ZETASQL_SCRIPTING_PARSE_HELPERS_H_
#define ZETASQL_SCRIPTING_PARSE_HELPERS_H_

#include "zetasql/parser/parser.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/scripting/parsed_script.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace zetasql {
// Parses a script and verifies that it passes some basic checks:
// - The nesting depth of scripting constructs is within allowable limits
// - All BREAK and CONTINUE statements happen within a loop
// - Variable declarations do not have the same name as another variable in
//   the same block or any enclosing block.
absl::StatusOr<std::unique_ptr<ParserOutput>> ParseAndValidateScript(
    absl::string_view script_string, const ParserOptions& parser_options,
    ErrorMessageMode error_message_mode);

}  // namespace zetasql

#endif  // ZETASQL_SCRIPTING_PARSE_HELPERS_H_
