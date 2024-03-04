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

#ifndef ZETASQL_PUBLIC_LITERAL_REMOVER_H_
#define ZETASQL_PUBLIC_LITERAL_REMOVER_H_

#include <string>

#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace zetasql {

// Maps a ResolvedLiteral to a new parameter name (excluding @).
using LiteralReplacementMap =
    absl::flat_hash_map<const ResolvedLiteral*, std::string>;
using GeneratedParameterMap = absl::flat_hash_map<std::string, Value>;
using IgnoredOptionNames =
    absl::flat_hash_set<std::string, zetasql_base::StringViewCaseHash,
                        zetasql_base::StringViewCaseEqual>;

struct LiteralReplacementOptions {
  // Set of option names to ignore in literal replacements.
  IgnoredOptionNames ignored_option_names;

  // Scrub literals in the LIMIT, OFFSET clauses.
  bool scrub_limit_offset = true;
};

// Replaces literals in <sql> by new query parameters and returns the new query
// in <result_sql>. A valid resolved statement <stmt> is required.
// The mapping from literals to parameter names is returned in <literal_map>.
// Generated parameter names are returned in <generated_parameters>. They are
// guaranteed to be unique and not collide with those already present in <stmt>.
//
// This can return errors that point at a location in the input. How this
// location is reported is given by <options.error_message_mode()>.
absl::Status ReplaceLiteralsByParameters(
    absl::string_view sql,
    const LiteralReplacementOptions& literal_replacement_options,
    const AnalyzerOptions& analyzer_options, const ResolvedStatement* stmt,
    LiteralReplacementMap* literal_map,
    GeneratedParameterMap* generated_parameters, std::string* result_sql);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_LITERAL_REMOVER_H_
