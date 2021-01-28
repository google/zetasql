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

#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"

namespace zetasql {
namespace {

// A visitor that changes ResolvedColumnRef nodes to be correlated.
class CorrelateColumnRefVisitor : public ResolvedASTDeepCopyVisitor {
 private:
  std::unique_ptr<ResolvedColumnRef> CorrelatedColumnRef(
      const ResolvedColumnRef& ref) {
    return MakeResolvedColumnRef(ref.type(), ref.column(), true);
  }

  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* node) override {
    if (in_subquery_) {
      return ResolvedASTDeepCopyVisitor::VisitResolvedColumnRef(node);
    }
    PushNodeToStack(CorrelatedColumnRef(*node));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr* node) override {
    ++in_subquery_;
    absl::Status s =
        ResolvedASTDeepCopyVisitor::VisitResolvedSubqueryExpr(node);
    --in_subquery_;

    // If this is the first subquery encountered, we need to correlate the
    // column references in the parameter list and for the in expression.
    if (!in_subquery_) {
      std::unique_ptr<ResolvedSubqueryExpr> expr =
          ConsumeTopOfStack<ResolvedSubqueryExpr>();
      for (auto& column_ref : expr->parameter_list()) {
        if (!column_ref->is_correlated()) {
          const_cast<ResolvedColumnRef*>(column_ref.get())
              ->set_is_correlated(true);
        }
      }
      if (expr->in_expr() != nullptr) {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> in_expr,
                         ProcessNode(expr->in_expr()));
        expr->set_in_expr(std::move(in_expr));
      }
      PushNodeToStack(std::move(expr));
    }
    return s;
  }

  // Tracks if we're inside a subquery. We stop correlating when we're inside a
  // subquery as column references are either already correlated or don't need
  // to be.
  int in_subquery_ = 0;
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
    return absl::OkStatus();
  }

  absl::Status VisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr* node) override {
    for (const auto& column : node->parameter_list()) {
      ZETASQL_RETURN_IF_ERROR(VisitResolvedColumnRef(column.get()));
    }
    if (node->in_expr() != nullptr) {
      ZETASQL_RETURN_IF_ERROR(node->in_expr()->Accept(this));
    }
    // Cut off traversal once we hit a subquery.
    return absl::OkStatus();
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
  return ResolvedColumn(max_col_id_,
                        zetasql::IdString::MakeGlobal(table_name),
                        zetasql::IdString::MakeGlobal(col_name), type);
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
