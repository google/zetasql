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

#ifndef ZETASQL_PUBLIC_SIMPLE_CATALOG_UTIL_H_
#define ZETASQL_PUBLIC_SIMPLE_CATALOG_UTIL_H_

#include <memory>
#include <optional>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/function.h"
#include "zetasql/public/simple_catalog.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace zetasql {

// Compiles `create_sql_stmt` and adds the resulting function to `catalog`.
//
// `create_sql_stmt`: Must be a CREATE FUNCTION statement that specifies a SQL
//     defined functions. Non-SQL functions are not supported by this utility.
// `analyzer_options`: Analyzer options used to analyze `create_sql_stmt`.
// `allow_persistent_function`: Unless this is set to true, the utility is
//     restricted to CREATE TEMP FUNCTION. Use with caution, SimpleCatalog does
//     not fully support a distinction between temp and persistent functions.
// `function_options`: FunctionOptions to be applied to the created function.
//     If not supplied a reasonable default for user-defined functions is used.
// `analyzer_output`: Analyzer outputs from compiling `create_sql_stmt`. The
//     lifetime of `analyzer_output` must exceed the lifetime of `catalog`.
//     The language options must support RESOLVED_CREATE_FUNCTION_STMT.
// `catalog`: A SimpleCatalog that will own the created SQLFunction* object.
absl::Status AddFunctionFromCreateFunction(
    absl::string_view create_sql_stmt, const AnalyzerOptions& analyzer_options,
    bool allow_persistent_function,
    std::optional<FunctionOptions> function_options,
    std::unique_ptr<const AnalyzerOutput>& analyzer_output,
    SimpleCatalog& catalog);

// Creates a Function object from a ResolvedCreateFunctionStmt.
// `create_function_stmt` must outlive the returned Function.
// `function_options` - if provided will be used to construct the Function.
// `group_name` if provided will be used to set the Function group name.
absl::StatusOr<std::unique_ptr<Function>> MakeFunctionFromCreateFunction(
    const ResolvedCreateFunctionStmt& create_function_stmt,
    std::optional<FunctionOptions> function_options = std::nullopt);

// Adds a `Table` object to `catalog` for the view defined by
// `create_view_stmt`.
//
// `create_view_stmt`: Must be a CREATE VIEW statement.
// `analyzer_options`: Analyzer options used to analyze `create_view_stmt`.
// `allow_non_temp`: If false, require statements to specify `TEMP` views.
// `analyzer_output`: Analyzer outputs from compiling `create_view_stmt`. The
//     lifetime of `analyzer_output` must exceed the lifetime of `catalog`.
//     `analyzer_options.language()` must support
//     `RESOLVED_CREATE_FUNCTION_STMT`.
// `catalog`: A SimpleCatalog that will own the created SQLFunction* object.
absl::Status AddViewFromCreateView(
    absl::string_view create_view_stmt, const AnalyzerOptions& analyzer_options,
    bool allow_non_temp, std::unique_ptr<const AnalyzerOutput>& analyzer_output,
    SimpleCatalog& catalog);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_SIMPLE_CATALOG_UTIL_H_
