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

#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"

namespace zetasql {

// Implements FindRelevantRewrites.
class RewriteApplicabilityChecker : public ResolvedASTVisitor {
 public:
  explicit RewriteApplicabilityChecker(
      absl::btree_set<ResolvedASTRewrite>* applicable_rewrites)
      : applicable_rewrites_(applicable_rewrites) {}

  absl::Status VisitResolvedFlatten(const ResolvedFlatten* node) override {
    applicable_rewrites_->insert(REWRITE_FLATTEN);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedLetExpr(const ResolvedLetExpr* node) override {
    applicable_rewrites_->insert(REWRITE_LET_EXPR);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedPivotScan(const ResolvedPivotScan* node) override {
    applicable_rewrites_->insert(REWRITE_PIVOT);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedUnpivotScan(
      const ResolvedUnpivotScan* node) override {
    applicable_rewrites_->insert(REWRITE_UNPIVOT);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedAnonymizedAggregateScan(
      const ResolvedAnonymizedAggregateScan* node) override {
    applicable_rewrites_->insert(REWRITE_ANONYMIZATION);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    if (node->function()->Is<SQLFunctionInterface>() ||
        node->function()->Is<TemplatedSQLFunction>()) {
      applicable_rewrites_->insert(REWRITE_INLINE_SQL_FUNCTIONS);
    }

    if (!node->function()->IsZetaSQLBuiltin()) {
      return DefaultVisit(node);
    }

    // This switch is only for ZetaSQL built-ins.
    switch (node->signature().context_id()) {
      case FN_PROTO_MAP_AT_KEY:
      case FN_SAFE_PROTO_MAP_AT_KEY:
      case FN_CONTAINS_KEY:
      case FN_MODIFY_MAP:
        applicable_rewrites_->insert(REWRITE_PROTO_MAP_FNS);
        break;
      case FN_ARRAY_TRANSFORM:
      case FN_ARRAY_TRANSFORM_WITH_INDEX:
      case FN_ARRAY_FILTER:
      case FN_ARRAY_FILTER_WITH_INDEX:
        applicable_rewrites_->insert(REWRITE_ARRAY_FILTER_TRANSFORM);
        break;
      case FN_ARRAY_INCLUDES:
      case FN_ARRAY_INCLUDES_LAMBDA:
      case FN_ARRAY_INCLUDES_ANY:
        applicable_rewrites_->insert(REWRITE_ARRAY_INCLUDES);
        break;
      case FN_TYPEOF:
        applicable_rewrites_->insert(REWRITE_TYPEOF_FUNCTION);
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
      default:
        break;
    }
    return DefaultVisit(node);
  }

 private:
  absl::btree_set<ResolvedASTRewrite>* applicable_rewrites_;
};

absl::StatusOr<absl::btree_set<ResolvedASTRewrite>> FindRelevantRewriters(
    const ResolvedNode* node) {
  absl::btree_set<ResolvedASTRewrite> applicable_rewrites_;
  RewriteApplicabilityChecker checker(&applicable_rewrites_);
  ZETASQL_RETURN_IF_ERROR(node->Accept(&checker));
  return applicable_rewrites_;
}

}  // namespace zetasql
