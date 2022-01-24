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

#include "zetasql/analyzer/rewriters/let_expr_rewriter.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// A visitor that rewrites ResolvedLetExpr into a subquery expression.
class LetExprRewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  explicit LetExprRewriterVisitor(ColumnFactory* column_factory)
      : column_factory_(column_factory) {}

 private:
  absl::Status VisitResolvedLetExpr(const ResolvedLetExpr* node) override;

  ColumnFactory* column_factory_;
};

absl::Status LetExprRewriterVisitor::VisitResolvedLetExpr(
    const ResolvedLetExpr* node) {
  // TODO: In case assignments are 'cheap' and non-volatile, inline them
  //     instead of using a subquery. We need to define 'cheap'.

  // Copy the let expr with references to free variables correlated.
  ZETASQL_ASSIGN_OR_RETURN(auto let_expr, CorrelateColumnRefs(*node));

  // Build assignments projection.
  std::vector<ResolvedColumn> assignment_columns;
  std::vector<std::unique_ptr<const ResolvedComputedColumn>> assignment_exprs;
  for (int i = 0; i < let_expr->assignment_list_size(); ++i) {
    const ResolvedComputedColumn* assignment = let_expr->assignment_list(i);
    assignment_columns.emplace_back(assignment->column());
    ZETASQL_ASSIGN_OR_RETURN(auto assigned_expr, ProcessNode(assignment->expr()));
    assignment_exprs.emplace_back(MakeResolvedComputedColumn(
        assignment->column(), std::move(assigned_expr)));
  }
  auto assignments_projection =
      MakeResolvedProjectScan(assignment_columns, std::move(assignment_exprs),
                              MakeResolvedSingleRowScan());

  // Build result expression projection.
  ResolvedColumn result_column =
      column_factory_->MakeCol("$let_expr", "injected", let_expr->type());
  ZETASQL_ASSIGN_OR_RETURN(auto result_expr, ProcessNode(let_expr->expr()));
  std::vector<std::unique_ptr<const ResolvedComputedColumn>> result_exprs;
  result_exprs.emplace_back(
      MakeResolvedComputedColumn(result_column, std::move(result_expr)));
  auto result_projection =
      MakeResolvedProjectScan({result_column}, std::move(result_exprs),
                              std::move(assignments_projection));

  // Build subquery.
  std::vector<std::unique_ptr<const ResolvedColumnRef>> column_refs;
  ZETASQL_RETURN_IF_ERROR(CollectColumnRefs(*node, &column_refs));
  PushNodeToStack(MakeResolvedSubqueryExpr(
      let_expr->type(), ResolvedSubqueryExpr::SCALAR, std::move(column_refs),
      /*in_expr=*/nullptr, std::move(result_projection)));
  return absl::OkStatus();
}

}  // namespace

class LetExprRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());
    LetExprRewriterVisitor rewriter(&column_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedNode> result,
                     rewriter.ConsumeRootNode<ResolvedNode>());
    return result;
  }

  std::string Name() const override { return "LetExprRewriter"; }
};

const Rewriter* GetLetExprRewriter() {
  static const auto* const kRewriter = new LetExprRewriter;
  return kRewriter;
}

}  // namespace zetasql
