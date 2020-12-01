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

#include <map>
#include <string>

#include "zetasql/public/analyzer_output.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/container/node_hash_set.h"
#include "absl/status/status.h"

namespace zetasql {

// Maps a ResolvedLiteral to a new parameter name (excluding @).
typedef std::map<const ResolvedLiteral*, std::string> LiteralReplacementMap;
typedef std::map<std::string, zetasql::Value> GeneratedParameterMap;

// Replaces literals in <sql> by new query parameters and returns the new query
// in <result_sql>. <analyzer_output> must have been produced using the option
// 'record_parse_locations', otherwise no replacement is done. The mapping from
// literals to parameter names is returned in <literal_parameter_map>. Generated
// parameter names are returned in <generated_parameters>. They are guaranteed
// to be unique and not collide with those already present in <analyzer_output>.
//
// This can return errors that point at a location in the input. How this
// location is reported is given by <options.error_message_mode()>.
absl::Status ReplaceLiteralsByParameters(
    const std::string& sql,
    const absl::node_hash_set<std::string>& option_names_to_ignore,
    const AnalyzerOptions& analyzer_options,
    const AnalyzerOutput* analyzer_output, LiteralReplacementMap* literal_map,
    GeneratedParameterMap* generated_parameters, std::string* result_sql);

// Same as above only all options will be ignored.
absl::Status ReplaceLiteralsByParameters(
    const std::string& sql, const AnalyzerOptions& analyzer_options,
    const AnalyzerOutput* analyzer_output, LiteralReplacementMap* literal_map,
    GeneratedParameterMap* generated_parameters, std::string* result_sql);

// Same as above, but taking a ResolvedStatement in place of AnalyzerOutput,
// which consists of a ResolvedStatement along with many other fields.
absl::Status ReplaceLiteralsByParameters(
    const std::string& sql,
    const absl::node_hash_set<std::string>& option_names_to_ignore,
    const AnalyzerOptions& analyzer_options, const ResolvedStatement* stmt,
    LiteralReplacementMap* literal_map,
    GeneratedParameterMap* generated_parameters, std::string* result_sql);
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_LITERAL_REMOVER_H_
