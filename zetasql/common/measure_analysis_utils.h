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

#ifndef ZETASQL_COMMON_MEASURE_ANALYSIS_UTILS_H_
#define ZETASQL_COMMON_MEASURE_ANALYSIS_UTILS_H_

#include <memory>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
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
  bool is_pseudo_column = false;
};

// Analyzes `measure_expr` against non-measure columns of `table` and returns
// the resolved expression.
//
// `measure_expr` is an aggregate expression that defines the measure; see
// (broken link) for details.
//
// Pre-conditions:
//   - `table` must not have any duplicate column names.
//   - `analyzer_options` must not have any expression columns or a lookup
//     expression callback.
//
// Post-conditions:
//   - `analyzer_output` will be populated with the resolved expression; the
//     caller must ensure that `analyzer_output` outlives the returned
//     ResolvedExpr.
absl::StatusOr<const ResolvedExpr*> AnalyzeMeasureExpressionInternal(
    absl::string_view measure_expr, const Table& table, Catalog& catalog,
    TypeFactory& type_factory, AnalyzerOptions analyzer_options,
    std::unique_ptr<const AnalyzerOutput>& analyzer_output);

// Adds measure columns to a SimpleTable.
// `table` is the table to which the measure columns should be added. `table`
// cannot allow duplicate column names.
// `measures` is a vector of name+expression pairs for the new measure
//   columns. The expressions are resolved against the columns in `table`.
// `type_factory` is used to create types.
// `catalog` should contain builtin functions which can be referenced in
//   measure expressions.
// `analyzer_options` is used when resolving the measure expressions and should
//   enable any features required for those expressions.
// The returned vector of AnalyzerOutputs corresponds to the resolved ASTs of
// each of the measure columns, and must outlive the table.
absl::StatusOr<std::vector<std::unique_ptr<const AnalyzerOutput>>>
AddMeasureColumnsToTable(SimpleTable& table,
                         std::vector<MeasureColumnDef> measures,
                         TypeFactory& type_factory, Catalog& catalog,
                         AnalyzerOptions analyzer_options);

}  // namespace zetasql

#endif  // ZETASQL_COMMON_MEASURE_ANALYSIS_UTILS_H_
