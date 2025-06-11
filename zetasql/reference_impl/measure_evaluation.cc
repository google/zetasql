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

#include "zetasql/reference_impl/measure_evaluation.h"

#include <utility>

#include "zetasql/public/catalog.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

absl::Status MeasureColumnToExprMapping::TrackMeasureColumnsEmittedByTableScan(
    const ResolvedTableScan& table_scan) {
  // If there are no measure columns emitted by the table scan, we can skip this
  // method. We deliberately do not check `column_index_list` at this point
  // because there are many legacy cases where it is not populated.
  if (!absl::c_any_of(table_scan.column_list(),
                      [](const ResolvedColumn& column) {
                        return column.type()->IsMeasureType();
                      })) {
    return absl::OkStatus();
  }
  // If here, we know that there are measure columns emitted by the table scan.
  // `column_index_list` must be populated.
  ZETASQL_RET_CHECK_EQ(table_scan.column_list_size(),
               table_scan.column_index_list_size());
  for (int idx = 0; idx < table_scan.column_list_size(); ++idx) {
    const ResolvedColumn& resolved_column = table_scan.column_list(idx);
    if (resolved_column.type()->IsMeasureType()) {
      const int table_column_index = table_scan.column_index_list(idx);
      const Column* column = table_scan.table()->GetColumn(table_column_index);
      ZETASQL_RET_CHECK(column->HasMeasureExpression() &&
                column->GetExpression()->HasResolvedExpression());
      const ResolvedExpr* measure_expr =
          column->GetExpression()->GetResolvedExpression();
      ZETASQL_RETURN_IF_ERROR(AddMeasureColumnWithExpr(resolved_column, measure_expr));
    }
  }
  return absl::OkStatus();
}

absl::Status
MeasureColumnToExprMapping::TrackMeasureColumnsRenamedByWithRefScan(
    const ResolvedWithRefScan& with_ref_scan,
    const ResolvedScan& with_subquery_scan) {
  ZETASQL_RET_CHECK_EQ(with_ref_scan.column_list_size(),
               with_subquery_scan.column_list_size());
  for (int idx = 0; idx < with_ref_scan.column_list_size(); ++idx) {
    const ResolvedColumn& renamed_column = with_ref_scan.column_list(idx);
    const ResolvedColumn& original_column = with_subquery_scan.column_list(idx);
    ZETASQL_RET_CHECK(original_column.type()->Equals(renamed_column.type()));
    if (original_column.type()->IsMeasureType()) {
      ZETASQL_ASSIGN_OR_RETURN(const ResolvedExpr* measure_expr,
                       GetMeasureExpr(original_column));
      ZETASQL_RETURN_IF_ERROR(AddMeasureColumnWithExpr(renamed_column, measure_expr));
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<const ResolvedExpr*> MeasureColumnToExprMapping::GetMeasureExpr(
    const ResolvedColumn& column) const {
  if (auto it = measure_column_to_expr_.find(column);
      it != measure_column_to_expr_.end()) {
    return it->second;
  }
  return absl::NotFoundError(absl::StrCat("Column not found: ", column.name()));
}

absl::Status MeasureColumnToExprMapping::AddMeasureColumnWithExpr(
    const ResolvedColumn& column, const ResolvedExpr* expr) {
  ZETASQL_RET_CHECK(column.type()->IsMeasureType());
  ZETASQL_RET_CHECK(expr != nullptr);
  if (!measure_column_to_expr_.insert({column, expr}).second) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Duplicate measure column: " << column.name();
  }
  return absl::OkStatus();
}

}  // namespace zetasql
