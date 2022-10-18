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

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/simple_catalog.h"
#include "absl/status/status.h"

namespace zetasql {

// Compiles 'create_sql_stmt' and adds the resulting function to 'catalog'.
//
// 'create_sql_stmt': Must be a CREATE FUNCTION statement that specifies a SQL
//     defined functions. Non-SQL functions are not supported by this utility.
// 'analyzer_options': Analyzer options used to analyze 'create_sql_stmt'.
// 'analyzer_output': Analyzer outputs from compiling 'create_sql_stmt'. The
//     lifetime of 'analyzer_output` must exceed the lifetime of 'catalog'.
//     The language options must support RESOLVED_CREATE_FUNCTION_STMT.
// 'catalog': A SimpleCatalog that will own the created SQLFunction* object.
// 'allow_persistent_function': Unless this is set to true, the utility is
//     restricted to CREATE TEMP FUNCTION. Use with caution, SimpleCatalog does
//     not fully support a distinction between temp and persistent functions.
absl::Status AddFunctionFromCreateFunction(
    absl::string_view create_sql_stmt, const AnalyzerOptions& analyzer_options,
    std::unique_ptr<const AnalyzerOutput>& analyzer_output,
    SimpleCatalog& catalog, bool allow_persistent_function = false);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_SIMPLE_CATALOG_UTIL_H_
