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

#include "zetasql/resolved_ast/rewrite_utils.h"

#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"

namespace zetasql {
namespace {

// A visitor that changes ResolvedColumnRef nodes to be correlated.
class CorrelateColumnRefVisitor : public ResolvedASTDeepCopyVisitor {
 private:
  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* node) override {
    PushNodeToStack(MakeResolvedColumnRef(node->type(), node->column(), true));
    return absl::OkStatus();
  }
};

// A visitor which collects the ResolvedColumnRef that are referenced.
class ColumnRefCollector : public ResolvedASTVisitor {
 public:
  explicit ColumnRefCollector(
      std::vector<std::unique_ptr<const ResolvedColumnRef>>* column_refs)
      : column_refs_(column_refs) {}

 private:
  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* node) override {
    column_refs_->push_back(MakeResolvedColumnRef(node->type(), node->column(),
                                                  node->is_correlated()));
    return ResolvedASTVisitor::VisitResolvedColumnRef(node);
  }
  std::vector<std::unique_ptr<const ResolvedColumnRef>>* column_refs_;
};

}  // namespace

ResolvedColumn ColumnFactory::MakeCol(const std::string& table_name,
                                      const std::string& col_name,
                                      const Type* type) {
  if (sequence_ == nullptr) {
    ++max_col_id_;
  } else {
    while (true) {
      // Allocate from the sequence, but make sure it's higher than the max we
      // should start from.
      int next_col_id = static_cast<int>(sequence_->GetNext());
      if (next_col_id > max_col_id_) {
        max_col_id_ = next_col_id;
        break;
      }
    }
  }
  return ResolvedColumn(max_col_id_, table_name, col_name, type);
}

zetasql_base::StatusOr<std::unique_ptr<ResolvedExpr>> CorrelateColumnRefs(
    const ResolvedExpr& expr) {
  CorrelateColumnRefVisitor correlator;
  ZETASQL_RETURN_IF_ERROR(expr.Accept(&correlator));
  return correlator.ConsumeRootNode<ResolvedExpr>();
}

absl::Status CollectColumnRefs(
    const ResolvedNode& node,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>* column_refs) {
  ColumnRefCollector column_ref_collector(column_refs);
  return node.Accept(&column_ref_collector);
}

}  // namespace zetasql
