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

#ifndef ZETASQL_REFERENCE_IMPL_MEASURE_EVALUATION_H_
#define ZETASQL_REFERENCE_IMPL_MEASURE_EVALUATION_H_

#include "zetasql/public/catalog.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace zetasql {

// Maintains the mapping between a MEASURE-typed ResolvedColumn and the
// expression used to compute the measure. The expression used to compute the
// measure is stored in the catalog (not the ResolvedColumn or the type), hence
// why we need to track it here.
class MeasureColumnToExprMapping {
 public:
  MeasureColumnToExprMapping() = default;
  // Disallow copy, but allow move.
  MeasureColumnToExprMapping(const MeasureColumnToExprMapping& other) = delete;
  MeasureColumnToExprMapping& operator=(
      const MeasureColumnToExprMapping& other) = delete;
  MeasureColumnToExprMapping(MeasureColumnToExprMapping&& other) = default;
  MeasureColumnToExprMapping& operator=(MeasureColumnToExprMapping&& other) =
      default;

  // Track expressions for any measure columns emitted by `table_scan`.
  absl::Status TrackMeasureColumnsEmittedByTableScan(
      const ResolvedTableScan& table_scan);

  // Resolved measure columns may be renamed with a different column id when
  // propagating past certain types of scans.  (e.g. ResolvedWithRefScans). This
  // method associates renamed measure columns emitted by `with_ref_scan` with
  // the measure expression currently associated with measure columns emitted by
  // `with_subquery_scan`.
  absl::Status TrackMeasureColumnsRenamedByWithRefScan(
      const ResolvedWithRefScan& with_ref_scan,
      const ResolvedScan& with_subquery_scan);

  // Find the measure expression for the given `column`.
  absl::StatusOr<const ResolvedExpr*> GetMeasureExpr(
      const ResolvedColumn& column) const;

 private:
  // Adds a MEASURE-typed `column` with the given `expr`.
  absl::Status AddMeasureColumnWithExpr(const ResolvedColumn& column,
                                        const ResolvedExpr* expr);

  absl::flat_hash_map<ResolvedColumn, const ResolvedExpr*>
      measure_column_to_expr_;
};

}  // namespace zetasql

#endif  // ZETASQL_REFERENCE_IMPL_MEASURE_EVALUATION_H_
