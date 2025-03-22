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

#ifndef ZETASQL_PUBLIC_MEASURE_EXPRESSION_H_
#define ZETASQL_PUBLIC_MEASURE_EXPRESSION_H_

#include <memory>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
namespace zetasql {

// A measure column definition.
struct MeasureColumnDef {
  absl::string_view name;
  absl::string_view expression;
};
// Adds measure columns to a SimpleTable.
// `table` is the table to which the measure columns should be added. `table`
// cannot allow duplicate column names.
// `measures` is a vector of name+expression pairs for the new measure
//   columns. The expressions are resolved against the columns in `table`.
// `type_factory` is used to create types.
// `catalog` should contain builtin functions which can be referenced in
//   measure expressions.
// `analyzer_options` is the analyzer options used when resolving the measure
//   expressions, and should enable any features required for those expressions.
//   This object will be modified by this function as required for analyzing the
//   measure expressions, and should not be re-used for purposes other than this
//   function call.
// The returned vector of AnalyzerOutputs corresponds to the resolved ASTs of
// each of the measure columns, and must outlive the table.
absl::StatusOr<std::vector<std::unique_ptr<const AnalyzerOutput>>>
AddMeasureColumnsToTable(SimpleTable& table,
                         std::vector<MeasureColumnDef> measures,
                         TypeFactory& type_factory, Catalog& catalog,
                         AnalyzerOptions& analyzer_options);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_MEASURE_EXPRESSION_H_
