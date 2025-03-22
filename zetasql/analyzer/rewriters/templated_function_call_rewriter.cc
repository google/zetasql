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

#include "zetasql/analyzer/rewriters/templated_function_call_rewriter.h"

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "zetasql/analyzer/rewriters/rewriter_relevance_checker.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/templated_sql_tvf.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_rewrite_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/btree_set.h"
#include "absl/status/statusor.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

template <typename T>
static absl::StatusOr<std::unique_ptr<const T>> DownCastUniquePtr(
    std::unique_ptr<const ResolvedNode> node) {
  return std::unique_ptr<const T>(node.release()->GetAs<T>());
}

using ApplyRewritesFn =
    std::function<absl::StatusOr<std::unique_ptr<const ResolvedNode>>(
        std::unique_ptr<const ResolvedNode>)>;

class TemplatedFunctionCallVisitor : public ResolvedASTRewriteVisitor {
 public:
  explicit TemplatedFunctionCallVisitor(ApplyRewritesFn apply_rewriters_fn)
      : apply_rewriters_fn_(apply_rewriters_fn) {}

  template <typename T>
  absl::StatusOr<std::unique_ptr<const T>> RewriteNode(
      std::unique_ptr<const ResolvedNode> node) {
    // Apply rewrites to the node.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedNode> rewritten_node,
                     apply_rewriters_fn_(std::move(node)));
    // Apply the visitor to the rewritten node to rewrite nested templated
    // function calls.
    ZETASQL_ASSIGN_OR_RETURN(rewritten_node, this->VisitAll(std::move(rewritten_node)));
    return DownCastUniquePtr<T>(std::move(rewritten_node));
  }

  absl::StatusOr<std::shared_ptr<TemplatedSQLFunctionCall>>
  CopyAndRewriteTemplatedSQLFunctionCall(
      const absl::btree_set<ResolvedASTRewrite>& applicable_rewrites,
      const TemplatedSQLFunctionCall& function_call) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> expr_copy,
                     ResolvedASTDeepCopyVisitor::Copy(function_call.expr()));
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<const ResolvedExpr> rewritten_function_expr,
        RewriteNode<ResolvedExpr>(std::move(expr_copy)));

    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        rewritten_aggregate_expression_list;
    if (!function_call.aggregate_expression_list().empty()) {
      rewritten_aggregate_expression_list.reserve(
          function_call.aggregate_expression_list().size());
      for (const std::unique_ptr<const ResolvedComputedColumn>&
               aggregate_expression :
           function_call.aggregate_expression_list()) {
        ZETASQL_ASSIGN_OR_RETURN(
            std::unique_ptr<const ResolvedComputedColumn> expr_copy,
            ResolvedASTDeepCopyVisitor::Copy(aggregate_expression.get()));
        ZETASQL_ASSIGN_OR_RETURN(
            std::unique_ptr<const ResolvedComputedColumn>
                rewritten_aggregate_expression,
            RewriteNode<ResolvedComputedColumn>(std::move(expr_copy)));
        rewritten_aggregate_expression_list.push_back(
            std::move(rewritten_aggregate_expression));
      }
    }
    return std::make_shared<TemplatedSQLFunctionCall>(
        std::move(rewritten_function_expr),
        std::move(rewritten_aggregate_expression_list));
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedFunctionCall(
      std::unique_ptr<const ResolvedFunctionCall> function_call) override {
    if (function_call->function_call_info() == nullptr ||
        !function_call->function_call_info()->Is<TemplatedSQLFunctionCall>()) {
      return std::move(function_call);
    }

    ZETASQL_ASSIGN_OR_RETURN(
        absl::btree_set<ResolvedASTRewrite> applicable_rewrites,
        FindRelevantRewriters(function_call.get(),
                              /*check_templated_function_calls=*/true));

    if (applicable_rewrites.empty()) {
      return std::move(function_call);
    }

    TemplatedSQLFunctionCall* templated_function_call =
        function_call->function_call_info()->GetAs<TemplatedSQLFunctionCall>();

    ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<TemplatedSQLFunctionCall> rewritten_call,
                     CopyAndRewriteTemplatedSQLFunctionCall(
                         applicable_rewrites, *templated_function_call));

    return ToBuilder(std::move(function_call))
        .set_function_call_info(rewritten_call)
        .Build();
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedAggregateFunctionCall(
      std::unique_ptr<const ResolvedAggregateFunctionCall> function_call)
      override {
    if (function_call->function_call_info() == nullptr ||
        !function_call->function_call_info()->Is<TemplatedSQLFunctionCall>()) {
      return std::move(function_call);
    }

    ZETASQL_ASSIGN_OR_RETURN(
        absl::btree_set<ResolvedASTRewrite> applicable_rewrites,
        FindRelevantRewriters(function_call.get(),
                              /*check_templated_function_calls=*/true));

    if (applicable_rewrites.empty()) {
      return std::move(function_call);
    }

    TemplatedSQLFunctionCall* templated_function_call =
        function_call->function_call_info()->GetAs<TemplatedSQLFunctionCall>();

    ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<TemplatedSQLFunctionCall> rewritten_call,
                     CopyAndRewriteTemplatedSQLFunctionCall(
                         applicable_rewrites, *templated_function_call));

    return ToBuilder(std::move(function_call))
        .set_function_call_info(rewritten_call)
        .Build();
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>> PostVisitResolvedTVFScan(
      std::unique_ptr<const ResolvedTVFScan> tvf_scan) override {
    if (tvf_scan->signature() == nullptr ||
        !tvf_scan->signature()->Is<TemplatedSQLTVFSignature>()) {
      return std::move(tvf_scan);
    }
    auto* templated_tvf_signature =
        tvf_scan->signature()->GetAs<TemplatedSQLTVFSignature>();

    ZETASQL_ASSIGN_OR_RETURN(absl::btree_set<ResolvedASTRewrite> applicable_rewrites,
                     FindRelevantRewriters(
                         templated_tvf_signature->resolved_templated_query(),
                         /*check_templated_function_calls=*/true));

    if (applicable_rewrites.empty()) {
      return std::move(tvf_scan);
    }

    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedQueryStmt> query_copy,
                     ResolvedASTDeepCopyVisitor::Copy(
                         templated_tvf_signature->resolved_templated_query()));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedQueryStmt> rewritten_query,
                     RewriteNode<ResolvedQueryStmt>(std::move(query_copy)));

    std::shared_ptr<TemplatedSQLTVFSignature> rewritten_signature =
        templated_tvf_signature->CopyWithoutResolvedTemplatedQuery();
    rewritten_signature->set_resolved_templated_query(
        std::move(rewritten_query));

    return ToBuilder(std::move(tvf_scan))
        .set_signature(rewritten_signature)
        .Build();
  }

 private:
  ApplyRewritesFn apply_rewriters_fn_;
};

absl::StatusOr<std::unique_ptr<const ResolvedNode>>
RewriteTemplatedFunctionCalls(
    std::function<absl::StatusOr<std::unique_ptr<const ResolvedNode>>(
        std::unique_ptr<const ResolvedNode>)>
        rewriters_func,
    std::unique_ptr<const ResolvedNode> input) {
  TemplatedFunctionCallVisitor visitor(rewriters_func);
  return visitor.VisitAll(std::move(input));
}

}  // namespace zetasql
