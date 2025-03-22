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
#include "absl/container/flat_hash_map.h"
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
            FEATURE_V_1_4_MULTILEVEL_AGGREGATION)) {
      return absl::UnimplementedError(
          "Multi-level aggregation is needed to rewrite measures.");
    }
    // TODO: b/350555383 - Relax the rewriter logic to support rewriting
    // measures when grouping by STRUCT is not supported.
    if (!options.language().LanguageFeatureEnabled(
            FEATURE_V_1_2_GROUP_BY_STRUCT)) {
      return absl::UnimplementedError(
          "Grouping by STRUCT types is needed to rewrite measures.");
    }
    ZETASQL_RET_CHECK(options.id_string_pool() != nullptr);
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(/*max_seen_col_id=*/0,
                                 *options.id_string_pool(),
                                 *options.column_id_sequence_number());
    MeasureExpansionInfoMap measure_expansion_info_map;
    // Step 1: Get information required to rewrite all relevant grain scans.
    // Note that a grain scan is only rewritten if it is the source for a
    // measure column that is invoked via the AGGREGATE function.
    // Note: `auto` is used to handle a ZetaSQL specific scenario where code
    // replacement to `ZETASQL_ASSIGN_OR_RETURN` doesn't handle the returned
    // argument correctly if wrapped in parentheses.
    ZETASQL_ASSIGN_OR_RETURN(auto grain_scan_info_map,
                     GetGrainScanInfo(input.get(), measure_expansion_info_map,
                                      column_factory));

    // Step 2: Populate both `grain_scan_info` and `measure_expansion_info_map`
    // with information about the STRUCT-typed columns that will replace measure
    // columns that need to be expanded.
    for (auto& [grain_scan, grain_scan_info] : grain_scan_info_map) {
      ZETASQL_RETURN_IF_ERROR(PopulateStructColumnInfo(
          grain_scan, grain_scan_info, measure_expansion_info_map, type_factory,
          *options.id_string_pool()));
    }

    // Step 3: Perform the rewrite.
    const Function* any_value_fn = nullptr;
    ZETASQL_RET_CHECK_OK(catalog.FindFunction({"any_value"}, &any_value_fn,
                                      options.find_options()));
    return RewriteMeasures(std::move(input), std::move(grain_scan_info_map),
                           any_value_fn, column_factory,
                           measure_expansion_info_map);
    // TODO: b/350555383 - : Augment validator to ensure columnrefs produce the
    // same type as the column being referenced.
    // TODO: b/350555383 - Add a test to ensure that the measure rewriter fails
    // if multi-level aggregation is not supported.
    // TODO: b/350555383 - Add a test to ensure that the measure rewriter fails
    // if grouping by STRUCT is not supported.
    // TODO: b/350555383 - Add tests with complex measure expressions (ratio
    // metrics, multi-level aggregates, subqueries etc.)
    // TODO: b/350555383 - Add tests with multiple measure columns, where only
    // some need to be expanded.
    // TODO: b/350555383 - Add tests where a measure column is scanned in 2 or
    // more different table scans, and only invoked via AGGREGATE from one of
    // the scans.
  }

  std::string Name() const override { return "MeasureTypeRewriter"; }
};

const Rewriter* GetMeasureTypeRewriter() {
  static const Rewriter* rewriter = new MeasureTypeRewriter();
  return rewriter;
}

}  // namespace zetasql
