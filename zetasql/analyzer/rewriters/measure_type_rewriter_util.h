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

#ifndef ZETASQL_ANALYZER_REWRITERS_MEASURE_TYPE_REWRITER_UTIL_H_
#define ZETASQL_ANALYZER_REWRITERS_MEASURE_TYPE_REWRITER_UTIL_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/column_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/container/btree_map.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {

// Holds information needed to expand a measure column.
struct MeasureExpansionInfo {
  // Measure expression stored in `measure_catalog_column`.
  const ResolvedExpr* measure_expr = nullptr;
  // The `STRUCT` typed column that will be used to replace the measure column.
  ResolvedColumn struct_column;
  // A list of renamed measure columns resulting from the original measure
  // column requiring expansion. Only populated for measure columns that
  // originate from a GrainScan.
  std::vector<ResolvedColumn> renamed_measure_columns;
};

using MeasureExpansionInfoMap =
    absl::flat_hash_map<ResolvedColumn, MeasureExpansionInfo>;

// Holds information needed to rewrite a grain scan from which measure columns
// originate.
class GrainScanInfo {
 public:
  static absl::StatusOr<GrainScanInfo> CreateFromTableScan(
      const ResolvedTableScan* grain_scan,
      MeasureExpansionInfoMap& measure_expansion_info_map,
      ColumnFactory& column_factory);

  // Allow move, but not copy.
  GrainScanInfo(const GrainScanInfo& other) = delete;
  GrainScanInfo& operator=(const GrainScanInfo& other) = delete;
  GrainScanInfo(GrainScanInfo&& other) = default;
  GrainScanInfo& operator=(GrainScanInfo&& other) = default;

  // `ColumnToProject` represents a column that needs to be projected from a
  // grain scan as part of the measure expansion rewrite. There are 2 types of
  // columns that need to be projected from a grain scan:
  //
  // 1. Columns that must be projected to preserve query semantics. This
  //    includes non-measure columns that are already projected AND measure
  //    columns that are already projected but NOT invoked via the AGGREGATE
  //    function.
  //
  //    (Measure columns that are already projected from the grain scan AND
  //    invoked via the AGGREGATE function will NOT be projected, but rewritten
  //    to a STRUCT column instead).
  //
  // 2. Columns that are not yet projected from the grain scan, but need to be
  //    projected to support measure expansion. This includes columns referenced
  //    by the measure expression and any row identity columns.
  struct ColumnToProject {
    // The `ResolvedColumn` to project. This may be an existing column already
    // projected from the grain scan, or a new column to project.
    // of type 1 or type 2 above).
    ResolvedColumn resolved_column;
    // Indicates whether `resolved_column` is a row identity column.
    bool is_row_identity_column = false;
    // The index of the column in the grain scan's catalog table.
    int catalog_column_index = -1;
  };

  // Mark column with `column_name` as a column that needs to be projected from
  // `grain_scan`. If the column is already projected by `grain_scan`, it will
  // be added to `columns_to_project_`. Else, a new `ResolvedColumn` will be
  // created and added to `columns_to_project_`.
  //
  // No-op if `column_name` is already in `columns_to_project_`.
  //
  // Assumption: The table underlying the `grain_scan` has a column with
  // `column_name`.
  absl::Status MarkColumnForProjection(std::string column_name,
                                       const ResolvedTableScan* grain_scan,
                                       bool mark_row_identity_column);

  // Add a `STRUCT` typed column to compute. This `STRUCT` typed column will be
  // projected and used to replace a measure column that needs to be expanded.
  void AddStructComputedColumn(
      std::unique_ptr<const ResolvedComputedColumn> struct_computed_column) {
    struct_computed_columns_.push_back(std::move(struct_computed_column));
  }

  // Get the names of all row identity columns that need to be projected.
  absl::btree_set<std::string> GetRowIdentityColumnNames() const;

  absl::StatusOr<ColumnToProject> GetColumnToProject(
      std::string column_name) const {
    auto it = columns_to_project_.find(column_name);
    ZETASQL_RET_CHECK(it != columns_to_project_.end());
    return it->second;
  }

  std::vector<ColumnToProject> GetAllColumnsToProject() const {
    std::vector<ColumnToProject> columns_to_project;
    for (const auto& [column_name, column_to_project] : columns_to_project_) {
      columns_to_project.push_back(column_to_project);
    }
    return columns_to_project;
  }

  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
  release_struct_computed_columns() {
    return std::move(struct_computed_columns_);
  }

  ColumnFactory& column_factory() const { return column_factory_; }

 private:
  GrainScanInfo(std::string scan_name, ColumnFactory& column_factory)
      : scan_name_(std::move(scan_name)), column_factory_(column_factory) {}

  // The name of the source scan. Used when creating new columns to project.
  std::string scan_name_;
  // Column factory used to create new columns to project.
  ColumnFactory& column_factory_;
  // Contains columns that are already projected from the grain scan, in
  // addition to columns that need to be projected to expand measure columns.
  // The key is the column name in the catalog table.
  absl::btree_map<std::string, ColumnToProject> columns_to_project_;
  // STRUCT typed columns to compute for each measure column that needs to be
  // expanded. Each STRUCT column will have 2 top-level fields:
  //
  // 1. `referenced_columns`: A STRUCT typed field containing the set of columns
  //    referenced by the measure expression.
  // 2. `key_columns`: A STRUCT typed field containing the set of row identity
  //    columns used for grain-locking the measure expression.
  //
  // For example, a
  // measure column with expression `SUM(A + B)` on a table with row identity
  // columns `id_1` and `id_2` will have a STRUCT type like:
  //
  // STRUCT<
  //   referenced_columns STRUCT<A INT64, B INT64>,
  //   key_columns STRUCT<id_1 INT64, id_2 INT64
  // >
  std::vector<std::unique_ptr<const ResolvedComputedColumn>>
      struct_computed_columns_;
};

// Traverses the ResolvedAST to gather information about grain scans that need
// to be rewritten.
//
// Returns a mapping from a grain scan to the `GrainScanInfo` needed to rewrite
// it. Also populates `measure_expansion_info_map` with partial information
// about measure columns that need to be expanded (only measure columns that
// originate from a grain scan are added to `measure_expansion_info_map`).
absl::StatusOr<absl::flat_hash_map<const ResolvedTableScan*, GrainScanInfo>>
GetGrainScanInfo(const ResolvedNode* input,
                 MeasureExpansionInfoMap& measure_expansion_info_map,
                 ColumnFactory& column_factory);

// Populate both `grain_scan_info` and `measure_expansion_info_map` with
// information about the STRUCT-typed columns that will replace measure columns
// that need to be expanded.
//
// `grain_scan_info` will be updated to project the `STRUCT` typed columns and
// any columns needed to construct the `STRUCT` typed columns.
//
// `MeasureExpansionInfo` values in `measure_expansion_info_map` will have their
// `struct_column` field populated. Renamed measure columns will also be added
// as keys to `measure_expansion_info_map`.
absl::Status PopulateStructColumnInfo(
    const ResolvedTableScan* grain_scan, GrainScanInfo& grain_scan_info,
    MeasureExpansionInfoMap& measure_expansion_info_map,
    TypeFactory& type_factory, IdStringPool& id_string_pool);

// Rewrite the ResolvedAST to expand measure columns. This includes:
//
// 1. Rewriting grain scans to project columns needed for measure expansion
//    using a specially-constructed STRUCT-typed column.
// 2. Replacing measure columns with their corresponding STRUCT-typed columns.
// 3. Rewriting measure expressions to use-multi-level aggregation to grain-lock
//    and also reference columns from the STRUCT-typed columns.
absl::StatusOr<std::unique_ptr<const ResolvedNode>> RewriteMeasures(
    std::unique_ptr<const ResolvedNode> input,
    absl::flat_hash_map<const ResolvedTableScan*, GrainScanInfo>
        grain_scan_info_map,
    const Function* any_value_fn, ColumnFactory& column_factory,
    MeasureExpansionInfoMap& measure_expansion_info_map);

}  // namespace zetasql

#endif  // ZETASQL_ANALYZER_REWRITERS_MEASURE_TYPE_REWRITER_UTIL_H_
