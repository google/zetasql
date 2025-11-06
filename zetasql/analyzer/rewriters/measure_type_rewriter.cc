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

#include "zetasql/analyzer/rewriters/measure_type_rewriter.h"

#include <memory>
#include <string>
#include <utility>

#include "zetasql/analyzer/rewriters/measure_type_rewriter_util.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/rewriter_interface.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

class MeasureTypeRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, std::unique_ptr<const ResolvedNode> input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    if (!options.language().LanguageFeatureEnabled(
            FEATURE_MULTILEVEL_AGGREGATION)) {
      return absl::UnimplementedError(
          "Multi-level aggregation is needed to rewrite measures.");
    }
    if (!options.language().LanguageFeatureEnabled(FEATURE_GROUP_BY_STRUCT)) {
      return absl::UnimplementedError(
          "Grouping by STRUCT types is needed to rewrite measures.");
    }

    ZETASQL_RET_CHECK(options.id_string_pool() != nullptr);
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(/*max_seen_col_id=*/0,
                                 *options.id_string_pool(),
                                 *options.column_id_sequence_number());
    MeasureExpansionInfoMap measure_expansion_info_map;

    // Step 1: Find unsupported query shapes, and return an error if any are
    // found.
    ZETASQL_RETURN_IF_ERROR(HasUnsupportedQueryShape(input.get()));

    // Step 2: Get information required to rewrite all relevant grain scans.
    // Note that a grain scan is only rewritten if it is the source for a
    // measure column that is invoked via the AGGREGATE function.
    ZETASQL_ASSIGN_OR_RETURN(GrainScanInfoMap grain_scan_info_map,
                     GetGrainScanInfo(input.get(), measure_expansion_info_map,
                                      column_factory));

    // Step 3: Populate both `grain_scan_info_map` and
    // `measure_expansion_info_map` with information about the STRUCT-typed
    // columns that will need to be projected to expand measure columns.
    ZETASQL_RETURN_IF_ERROR(PopulateStructColumnInfo(
        grain_scan_info_map, measure_expansion_info_map, type_factory,
        *options.id_string_pool(), column_factory));

    // Step 4: Perform the rewrite.
    const Function* any_value_fn = nullptr;
    ZETASQL_RET_CHECK_OK(catalog.FindFunction({"any_value"}, &any_value_fn,
                                      options.find_options()));
    FunctionCallBuilder function_call_builder(options, catalog, type_factory);
    return RewriteMeasures(std::move(input), std::move(grain_scan_info_map),
                           any_value_fn, function_call_builder,
                           options.language(), column_factory,
                           measure_expansion_info_map);
  }

  std::string Name() const override { return "MeasureTypeRewriter"; }
};

const Rewriter* GetMeasureTypeRewriter() {
  static const Rewriter* rewriter = new MeasureTypeRewriter();
  return rewriter;
}

}  // namespace zetasql
