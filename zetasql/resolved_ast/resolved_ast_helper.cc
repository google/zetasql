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

#include "zetasql/resolved_ast/resolved_ast_helper.h"

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"

namespace zetasql {

absl::Status ColumnRefVisitor::VisitResolvedColumnRef(
    const ResolvedColumnRef* node) {
  if (!IsLocalColumn(node->column())) {
    // Subclass should overwrite this function to read or collect ColumnRef
    // information.
  }
  return absl::OkStatus();
}

absl::Status ColumnRefVisitor::VisitResolvedSubqueryExpr(
    const ResolvedSubqueryExpr* node) {
  for (const auto& column : node->parameter_list()) {
    ZETASQL_RETURN_IF_ERROR(VisitResolvedColumnRef(column.get()));
  }
  if (node->in_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(node->in_expr()->Accept(this));
  }
  // Cut off traversal once we hit a subquery. Column refs inside subquery are
  // either internal or already collected in parameter_list.
  return absl::OkStatus();
}

absl::Status ColumnRefVisitor::VisitResolvedInlineLambda(
    const ResolvedInlineLambda* node) {
  for (const auto& column_ref : node->parameter_list()) {
    ZETASQL_RETURN_IF_ERROR(VisitResolvedColumnRef(column_ref.get()));
  }
  // Cut off traversal once we hit a lambda. Column refs inside lambda body
  // are either internal or already collected in parameter_list.
  return absl::OkStatus();
}

absl::Status ColumnRefVisitor::VisitResolvedWithExpr(
    const ResolvedWithExpr* node) {
  // Exclude the assignment columns because they are internal.
  for (int i = 0; i < node->assignment_list_size(); ++i) {
    local_columns_.insert(node->assignment_list(i)->column());
  }
  return ResolvedASTVisitor::VisitResolvedWithExpr(node);
}

class ColumnRefCollectorUnowned : public ColumnRefVisitor {
 public:
  explicit ColumnRefCollectorUnowned(
      absl::flat_hash_set<const ResolvedColumnRef*>& column_refs)
      : column_refs_(column_refs) {}

 private:
  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* node) override {
    if (!IsLocalColumn(node->column())) {
      column_refs_.insert(node);
    }
    return absl::OkStatus();
  }

  absl::flat_hash_set<const ResolvedColumnRef*>& column_refs_;
};

absl::StatusOr<absl::flat_hash_set<const ResolvedColumnRef*>>
CollectFreeColumnRefs(const ResolvedNode& node) {
  absl::flat_hash_set<const ResolvedColumnRef*> column_refs;
  ColumnRefCollectorUnowned column_ref_collector(column_refs);
  ZETASQL_RETURN_IF_ERROR(node.Accept(&column_ref_collector));
  return column_refs;
}

const ResolvedComputedColumn* FindProjectComputedColumn(
    const ResolvedProjectScan* project, const ResolvedColumn& column) {
  for (const auto& computed_column : project->expr_list()) {
    if (computed_column->column() == column) {
      return computed_column.get();
    }
  }
  return nullptr;
}

const ResolvedExpr* FindProjectExpr(const ResolvedProjectScan* project,
                                    const ResolvedColumn& column) {
  const ResolvedComputedColumn* computed_column =
      FindProjectComputedColumn(project, column);
  if (computed_column != nullptr) {
    return computed_column->expr();
  }
  return nullptr;
}

}  // namespace zetasql
