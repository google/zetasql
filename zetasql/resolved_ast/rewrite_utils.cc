//
// Copyright 2019 ZetaSQL Authors
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

#include "zetasql/resolved_ast/rewrite_utils.h"

namespace zetasql {

ResolvedColumn ColIdAllocator::MakeCol(const std::string& table_name,
                                       const std::string& col_name,
                                       const Type* type) {
  return ResolvedColumn(++max_col_id_, table_name, col_name, type);
}

absl::Status ColIdCounter::DefaultVisit(const ResolvedNode* node) {
  if (node->IsScan()) {
    const ResolvedScan* scan = node->GetAs<ResolvedScan>();
    for (const ResolvedColumn& col : scan->column_list()) {
      VisitColumn(col);
    }
  }
  return ResolvedASTVisitor::DefaultVisit(node);
}

absl::Status ColIdCounter::VisitResolvedComputedColumn(
    const ResolvedComputedColumn* node) {
  VisitColumn(node->column());
  return ResolvedASTVisitor::DefaultVisit(node);
}

void ColIdCounter::VisitColumn(const ResolvedColumn& col) {
  if (col.column_id() > max_col_id_) {
    VLOG(1) << "Saw new max col id " << col.column_id();
    max_col_id_ = col.column_id();
  }
}

}  // namespace zetasql
