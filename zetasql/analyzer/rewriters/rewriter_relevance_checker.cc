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

#include "zetasql/analyzer/rewriters/rewriter_relevance_checker.h"

#include <memory>
#include <optional>
#include <vector>

#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/sql_tvf.h"
#include "zetasql/public/sql_view.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/templated_sql_tvf.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/btree_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// Implements FindRelevantRewrites.
class RewriteApplicabilityChecker : public ResolvedASTVisitor {
 public:
  RewriteApplicabilityChecker(
      bool check_templated_function_calls,
      absl::btree_set<ResolvedASTRewrite>* applicable_rewrites)
      : check_templated_function_calls_(check_templated_function_calls),
        applicable_rewrites_(applicable_rewrites) {}

  absl::Status VisitResolvedFlatten(const ResolvedFlatten* node) override {
    applicable_rewrites_->insert(REWRITE_FLATTEN);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedWithExpr(const ResolvedWithExpr* node) override {
    applicable_rewrites_->insert(REWRITE_WITH_EXPR);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedPivotScan(const ResolvedPivotScan* node) override {
    applicable_rewrites_->insert(REWRITE_PIVOT);
    for (const auto& pivot_expr : node->pivot_expr_list()) {
      ZETASQL_RET_CHECK(pivot_expr->Is<ResolvedAggregateFunctionCall>());
      const auto& aggregate_func_call =
          pivot_expr->GetAs<ResolvedAggregateFunctionCall>();
      if (aggregate_func_call->function()->Is<SQLFunctionInterface>() ||
          aggregate_func_call->function()->Is<TemplatedSQLFunction>()) {
        applicable_rewrites_->insert(REWRITE_INLINE_SQL_UDAS);
      }
    }
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedUnpivotScan(
      const ResolvedUnpivotScan* node) override {
    applicable_rewrites_->insert(REWRITE_UNPIVOT);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedAnonymizedAggregateScan(
      const ResolvedAnonymizedAggregateScan* node) override {
    ZETASQL_RETURN_IF_ERROR(VisitResolvedAggregateScanBasePrivate(node));
    applicable_rewrites_->insert(REWRITE_ANONYMIZATION);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedDifferentialPrivacyAggregateScan(
      const ResolvedDifferentialPrivacyAggregateScan* node) override {
    ZETASQL_RETURN_IF_ERROR(VisitResolvedAggregateScanBasePrivate(node));
    applicable_rewrites_->insert(REWRITE_ANONYMIZATION);
    return DefaultVisit(node);
  }

  void RegisterRewritesDeclaredOnFunctionCall(
      const ResolvedFunctionCallBase* node) {
    // Identify functions that have rewriting configured in their function
    // signatures, add those rewriters to the relevant set.
    const FunctionSignature& signature = node->signature();
    if (signature.HasEnabledRewriteImplementation()) {
      applicable_rewrites_->insert(
          signature.options().rewrite_options()->rewriter());
    }
  }

  absl::Status VisitResolvedAnalyticFunctionCall(
      const ResolvedAnalyticFunctionCall* node) override {
    RegisterRewritesDeclaredOnFunctionCall(node);
    return DefaultVisit(node);
  }

  absl::Status VisitTemplatedSQLFunctionCall(
      const TemplatedSQLFunctionCall& function_call) {
    ZETASQL_RETURN_IF_ERROR(function_call.expr()->Accept(this));
    for (const auto& aggregate_expression :
         function_call.aggregate_expression_list()) {
      ZETASQL_RETURN_IF_ERROR(aggregate_expression->Accept(this));
    }
    return absl::OkStatus();
  }

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {

    RegisterRewritesDeclaredOnFunctionCall(node);

    if (node->function()->Is<SQLFunctionInterface>() ||
        node->function()->Is<TemplatedSQLFunction>()) {
      applicable_rewrites_->insert(REWRITE_INLINE_SQL_FUNCTIONS);
    }

    // Traverse into the function call and check for applicable rewrites.
    if (check_templated_function_calls_ &&
        node->function_call_info() != nullptr &&
        node->function_call_info()->Is<TemplatedSQLFunctionCall>()) {
      const auto* function_call =
          node->function_call_info()->GetAs<TemplatedSQLFunctionCall>();
      ZETASQL_RETURN_IF_ERROR(VisitTemplatedSQLFunctionCall(*function_call));
    }

    if (!node->function()->IsZetaSQLBuiltin()) {
      return DefaultVisit(node);
    }

    // TODO: Migrate remaining functions (that do not have lambda
    //     type arguments for now) to use FunctionSignatureRewriteOptions.
    switch (node->signature().context_id()) {
      case FN_PROTO_MAP_AT_KEY:
      case FN_SAFE_PROTO_MAP_AT_KEY:
      case FN_PROTO_MAP_CONTAINS_KEY:
      case FN_PROTO_MODIFY_MAP:
        applicable_rewrites_->insert(REWRITE_PROTO_MAP_FNS);
        break;
      case FN_ANON_COUNT:
      case FN_ANON_COUNT_STAR:
      case FN_ANON_SUM_INT64:
      case FN_ANON_SUM_UINT64:
      case FN_ANON_SUM_DOUBLE:
      case FN_ANON_SUM_NUMERIC:
      case FN_ANON_AVG_DOUBLE:
      case FN_ANON_AVG_NUMERIC:
      case FN_ANON_VAR_POP_DOUBLE:
      case FN_ANON_VAR_POP_DOUBLE_ARRAY:
      case FN_ANON_STDDEV_POP_DOUBLE:
      case FN_ANON_STDDEV_POP_DOUBLE_ARRAY:
      case FN_ANON_PERCENTILE_CONT_DOUBLE:
      case FN_ANON_PERCENTILE_CONT_DOUBLE_ARRAY:
        applicable_rewrites_->insert(REWRITE_ANONYMIZATION);
        break;
      case FN_STRING_ARRAY_LIKE_ANY:
      case FN_BYTE_ARRAY_LIKE_ANY:
      case FN_STRING_LIKE_ANY:
      case FN_BYTE_LIKE_ANY:
      case FN_STRING_ARRAY_LIKE_ALL:
      case FN_BYTE_ARRAY_LIKE_ALL:
      case FN_STRING_LIKE_ALL:
      case FN_BYTE_LIKE_ALL:
      case FN_STRING_NOT_LIKE_ANY:
      case FN_BYTE_NOT_LIKE_ANY:
      case FN_STRING_ARRAY_NOT_LIKE_ANY:
      case FN_BYTE_ARRAY_NOT_LIKE_ANY:
      case FN_STRING_NOT_LIKE_ALL:
      case FN_BYTE_NOT_LIKE_ALL:
      case FN_STRING_ARRAY_NOT_LIKE_ALL:
      case FN_BYTE_ARRAY_NOT_LIKE_ALL:
        applicable_rewrites_->insert(REWRITE_LIKE_ANY_ALL);
        break;
      default:
        break;
    }
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedAggregateFunctionCall(
      const ResolvedAggregateFunctionCall* node) override {
    // Traverse into the function call and check for applicable rewrites.
    if (check_templated_function_calls_ &&
        node->function_call_info() != nullptr &&
        node->function_call_info()->Is<TemplatedSQLFunctionCall>()) {
      const auto* function_call =
          node->function_call_info()->GetAs<TemplatedSQLFunctionCall>();
      ZETASQL_RETURN_IF_ERROR(VisitTemplatedSQLFunctionCall(*function_call));
    }
    RegisterRewritesDeclaredOnFunctionCall(node);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedTableScan(const ResolvedTableScan* node) override {
    if (node->table()->Is<SQLView>()) {
      applicable_rewrites_->insert(
          ResolvedASTRewrite::REWRITE_INLINE_SQL_VIEWS);
    }
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedInsertStmt(
      const ResolvedInsertStmt* node) override {
    if (!node->row_list().empty() && node->table_scan() != nullptr) {
      applicable_rewrites_->insert(
          ResolvedASTRewrite::REWRITE_INSERT_DML_VALUES);
    }
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedTVFScan(const ResolvedTVFScan* node) override {
    if (node->tvf()->Is<SQLTableValuedFunction>() ||
        node->tvf()->Is<TemplatedSQLTVF>()) {
      applicable_rewrites_->insert(ResolvedASTRewrite::REWRITE_INLINE_SQL_TVFS);
    }

    if (check_templated_function_calls_ && node->signature() != nullptr &&
        node->signature()->Is<TemplatedSQLTVFSignature>()) {
      auto* templated_tvf_signature =
          node->signature()->GetAs<TemplatedSQLTVFSignature>();
      ZETASQL_RETURN_IF_ERROR(
          templated_tvf_signature->resolved_templated_query()->Accept(this));
    }

    return DefaultVisit(node);
  }

  absl::Status VisitResolvedSetOperationScan(
      const ResolvedSetOperationScan* node) override {
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedAggregateScan(
      const ResolvedAggregateScan* node) override {
    ZETASQL_RETURN_IF_ERROR(VisitResolvedAggregateScanBasePrivate(node));
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedAggregationThresholdAggregateScan(
      const ResolvedAggregationThresholdAggregateScan* node) override {
    ZETASQL_RETURN_IF_ERROR(VisitResolvedAggregateScanBasePrivate(node));
    applicable_rewrites_->insert(REWRITE_AGGREGATION_THRESHOLD);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedArrayScan(const ResolvedArrayScan* node) override {
    if (node->array_expr_list_size() > 1) {
      applicable_rewrites_->insert(REWRITE_MULTIWAY_UNNEST);
    }
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedAssertScan(
      const ResolvedAssertScan* node) override {
    applicable_rewrites_->insert(REWRITE_PIPE_ASSERT);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedPipeIfScan(
      const ResolvedPipeIfScan* node) override {
    applicable_rewrites_->insert(REWRITE_PIPE_IF);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedGeneralizedQueryStmt(
      const ResolvedGeneralizedQueryStmt* node) override {
    applicable_rewrites_->insert(REWRITE_GENERALIZED_QUERY_STMT);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedUpdateConstructor(
      const ResolvedUpdateConstructor* node) override {
    applicable_rewrites_->insert(
        ResolvedASTRewrite::REWRITE_UPDATE_CONSTRUCTOR);
    return DefaultVisit(node);
  }

 private:
  // If true, traverse into the resolved function call for TemplatedSQLFunction
  // and TemplatedSQLTVF to check for applicable rewrites.
  bool check_templated_function_calls_;

  absl::btree_set<ResolvedASTRewrite>* applicable_rewrites_;

  absl::Status VisitResolvedAggregateScanBasePrivate(
      const ResolvedAggregateScanBase* node) {
    for (const std::unique_ptr<const ResolvedGroupingSetBase>&
             grouping_set_base : node->grouping_set_list()) {
      if (grouping_set_base->Is<ResolvedRollup>() ||
          grouping_set_base->Is<ResolvedCube>()) {
        applicable_rewrites_->insert(ResolvedASTRewrite::REWRITE_GROUPING_SET);
        break;
      }
    }
    for (const auto& aggregate_comp_col : node->aggregate_list()) {
      ZETASQL_RET_CHECK(aggregate_comp_col->Is<ResolvedComputedColumnImpl>());
      const auto& aggregate_expr =
          aggregate_comp_col->GetAs<ResolvedComputedColumnImpl>()->expr();
      ZETASQL_RET_CHECK(aggregate_expr->Is<ResolvedAggregateFunctionCall>());
      const auto& aggregate_func_call =
          aggregate_expr->GetAs<ResolvedAggregateFunctionCall>();
      if (aggregate_func_call->function()->Is<SQLFunctionInterface>() ||
          aggregate_func_call->function()->Is<TemplatedSQLFunction>()) {
        applicable_rewrites_->insert(REWRITE_INLINE_SQL_UDAS);
      }
      if (aggregate_func_call->order_by_item_list_size() > 0 ||
          aggregate_func_call->limit() != nullptr) {
        applicable_rewrites_->insert(REWRITE_ORDER_BY_AND_LIMIT_IN_AGGREGATE);
      }
    }

    return absl::OkStatus();
  }
};

absl::StatusOr<absl::btree_set<ResolvedASTRewrite>> FindRelevantRewriters(
    const ResolvedNode* node, bool check_templated_function_calls) {
  ZETASQL_RET_CHECK(node != nullptr);
  absl::btree_set<ResolvedASTRewrite> applicable_rewrites_;
  RewriteApplicabilityChecker checker(check_templated_function_calls,
                                      &applicable_rewrites_);
  ZETASQL_RETURN_IF_ERROR(node->Accept(&checker));
  return applicable_rewrites_;
}

}  // namespace zetasql
