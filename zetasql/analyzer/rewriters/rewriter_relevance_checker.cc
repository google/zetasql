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

#include <optional>

#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/sql_tvf.h"
#include "zetasql/public/sql_view.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/templated_sql_tvf.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "absl/status/status.h"

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

  absl::Status VisitResolvedWithExpr(const ResolvedWithExpr* node) override {
    applicable_rewrites_->insert(REWRITE_WITH_EXPR);
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

  absl::Status VisitResolvedDifferentialPrivacyAggregateScan(
      const ResolvedDifferentialPrivacyAggregateScan* node) override {
    applicable_rewrites_->insert(REWRITE_ANONYMIZATION);
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    // Identify functions that have rewriting configured in their function
    // signatures, add those rewriters to the relevant set.
    const FunctionSignature& signature = node->signature();
    if (signature.HasEnabledRewriteImplementation()) {
      applicable_rewrites_->insert(
          signature.options().rewrite_options()->rewriter());
      return DefaultVisit(node);
    }

    if (node->function()->Is<SQLFunctionInterface>() ||
        node->function()->Is<TemplatedSQLFunction>()) {
      applicable_rewrites_->insert(REWRITE_INLINE_SQL_FUNCTIONS);
    }

    if (!node->function()->IsZetaSQLBuiltin()) {
      return DefaultVisit(node);
    }

    // TODO: Migrate remaining functions (that do not have lambda
    //     type arguments for now) to use FunctionSignatureRewriteOptions.
    switch (node->signature().context_id()) {
      case FN_PROTO_MAP_AT_KEY:
      case FN_SAFE_PROTO_MAP_AT_KEY:
      case FN_CONTAINS_KEY:
      case FN_MODIFY_MAP:
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
        applicable_rewrites_->insert(REWRITE_LIKE_ANY_ALL);
        break;
      default:
        break;
    }
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedTableScan(const ResolvedTableScan* node) override {
    if (node->table()->Is<SQLView>()) {
      applicable_rewrites_->insert(
          ResolvedASTRewrite::REWRITE_INLINE_SQL_VIEWS);
    }
    return DefaultVisit(node);
  }

  absl::Status VisitResolvedTVFScan(const ResolvedTVFScan* node) override {
    if (node->tvf()->Is<SQLTableValuedFunction>() ||
        node->tvf()->Is<TemplatedSQLTVF>()) {
      applicable_rewrites_->insert(ResolvedASTRewrite::REWRITE_INLINE_SQL_TVFS);
    }
    return DefaultVisit(node);
  }

 private:
  absl::btree_set<ResolvedASTRewrite>* applicable_rewrites_;
};

absl::StatusOr<absl::btree_set<ResolvedASTRewrite>> FindRelevantRewriters(
    const ResolvedNode* node) {
  ZETASQL_RET_CHECK(node != nullptr);
  absl::btree_set<ResolvedASTRewrite> applicable_rewrites_;
  RewriteApplicabilityChecker checker(&applicable_rewrites_);
  ZETASQL_RETURN_IF_ERROR(node->Accept(&checker));
  return applicable_rewrites_;
}

}  // namespace zetasql
