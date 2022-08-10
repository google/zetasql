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

#include "zetasql/analyzer/rewriters/like_any_all_rewriter.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/analyzer/substitute.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// Template for rewriting LIKE ANY with null handling for cases:
//   SELECT <input> LIKE ANY UNNEST([]) -> FALSE
//   SELECT NULL LIKE ANY {{UNNEST(<patterns>)|(<patterns>)}} -> NULL
//   SELECT <input> LIKE ANY {{UNNEST([NULL, ...])|(NULL, ...)}}
//     -> TRUE (if other matches present), NULL (if no other matches
//     present)
// Arguments:
//   input - STRING or BYTES
//   patterns - ARRAY<STRING> or ARRAY<BYTES>
// Returns: BOOL
// Semantic Rules:
//   If patterns is empty or NULL, return FALSE
//   If input is NULL, return NULL
//   If LOGICAL_OR(input LIKE pattern) is TRUE, return TRUE
//   If patterns contains any NULL values, return NULL
//   Otherwise, return FALSE
constexpr absl::string_view kLikeAnyTemplate = R"(
(SELECT
  CASE
    WHEN patterns IS NULL OR ARRAY_LENGTH(patterns) = 0 THEN FALSE
    WHEN input IS NULL THEN NULL
    WHEN LOGICAL_OR(input LIKE pattern) THEN TRUE
    WHEN LOGICAL_OR(pattern IS NULL) THEN NULL
    ELSE FALSE
  END
FROM UNNEST(patterns) as pattern)
)";

// Template for rewriting LIKE ALL with null handling for cases:
//   SELECT <input> LIKE ALL UNNEST([]) -> TRUE
//   SELECT NULL LIKE ALL {{UNNEST(<patterns>)|(<patterns>)}} -> NULL
//   SELECT <input> LIKE ALL {{UNNEST([NULL, ...])|(NULL, ...)}} -> NULL
// Arguments:
//   input - STRING or BYTES
//   patterns - ARRAY<STRING> or ARRAY<BYTES>
// Returns: BOOL
// Semantic Rules:
//   If patterns is empty or NULL, return TRUE
//   If input is NULL, return NULL
//   If LOGICAL_AND(input LIKE pattern) is FALSE, return FALSE
//   If patterns contains any NULL values, return NULL
//   Otherwise, return TRUE
constexpr absl::string_view kLikeAllTemplate = R"(
(SELECT
  CASE
    WHEN patterns IS NULL OR ARRAY_LENGTH(patterns) = 0 THEN TRUE
    WHEN input IS NULL THEN NULL
    WHEN NOT LOGICAL_AND(input LIKE pattern) THEN FALSE
    WHEN LOGICAL_OR(pattern IS NULL) THEN NULL
    ELSE TRUE
  END
  FROM UNNEST(patterns) as pattern)
)";

class LikeAnyAllRewriteVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  LikeAnyAllRewriteVisitor(const AnalyzerOptions* analyzer_options,
                           Catalog* catalog, TypeFactory* type_factory)
      : analyzer_options_(analyzer_options),
        catalog_(catalog),
        fn_builder_(*analyzer_options, *catalog),
        type_factory_(type_factory) {}

 private:
  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override;

  // Rewrites a function of the form:
  //   input LIKE {{ANY|ALL}} (pattern1, [...])
  // to use the LOGICAL_OR aggregation function with the LIKE operator
  absl::Status RewriteLikeAnyAll(const ResolvedFunctionCall* node);

  // Rewrites a function of the form:
  //   input LIKE {{ANY|ALL}} UNNEST(<array-expression>)
  // to use the LOGICAL_OR aggregation function with the LIKE operator
  absl::Status RewriteLikeAnyAllArray(const ResolvedFunctionCall* node);

  absl::Status RewriteLikeAnyAllArrayWithAggregate(
      std::unique_ptr<const ResolvedExpr> input_expr,
      std::unique_ptr<const ResolvedExpr> patterns_array_expr,
      absl::string_view template_string);

  const AnalyzerOptions* analyzer_options_;
  Catalog* catalog_;
  FunctionCallBuilder fn_builder_;
  TypeFactory* type_factory_;
};

absl::Status LikeAnyAllRewriteVisitor::VisitResolvedFunctionCall(
    const ResolvedFunctionCall* node) {
  if (IsBuiltInFunctionIdEq(node, FN_STRING_LIKE_ANY) ||
      IsBuiltInFunctionIdEq(node, FN_BYTE_LIKE_ANY) ||
      IsBuiltInFunctionIdEq(node, FN_STRING_LIKE_ALL) ||
      IsBuiltInFunctionIdEq(node, FN_BYTE_LIKE_ALL)) {
    return RewriteLikeAnyAll(node);
  } else if (IsBuiltInFunctionIdEq(node, FN_STRING_ARRAY_LIKE_ANY) ||
             IsBuiltInFunctionIdEq(node, FN_BYTE_ARRAY_LIKE_ANY) ||
             IsBuiltInFunctionIdEq(node, FN_STRING_ARRAY_LIKE_ALL) ||
             IsBuiltInFunctionIdEq(node, FN_BYTE_ARRAY_LIKE_ALL)) {
    return RewriteLikeAnyAllArray(node);
  }
  return CopyVisitResolvedFunctionCall(node);
}

absl::Status LikeAnyAllRewriteVisitor::RewriteLikeAnyAll(
    const ResolvedFunctionCall* node) {
  ZETASQL_RET_CHECK_GE(node->argument_list_size(), 2)
      << "LIKE ANY should have at least 2 arguments. Got: "
      << node->DebugString();

  // Extract and process the input
  const ResolvedExpr* input_expr = node->argument_list(0);
  ZETASQL_RET_CHECK_NE(input_expr, nullptr);
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> rewritten_input_expr,
                   ProcessNode(input_expr));
  const Type* input_type = rewritten_input_expr->type();
  ZETASQL_RET_CHECK(input_type->IsString() || input_type->IsBytes());

  // Extract and process the list of pattern arguments and use a helper function
  // to create a $make_array function to pack the patterns together
  std::vector<std::unique_ptr<ResolvedExpr>> pattern_elements_list;
  for (int i = 1; i < node->argument_list_size(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> rewritten_pattern_expr,
                     ProcessNode(node->argument_list(i)));
    pattern_elements_list.push_back(std::move(rewritten_pattern_expr));
  }

  const ArrayType* pattern_array_type;
  ZETASQL_RETURN_IF_ERROR(
      type_factory_->MakeArrayType(input_type, &pattern_array_type));
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedExpr> patterns_array_expr,
      fn_builder_.MakeArray(pattern_array_type, pattern_elements_list));

  if (IsBuiltInFunctionIdEq(node, FN_STRING_LIKE_ANY) ||
      IsBuiltInFunctionIdEq(node, FN_BYTE_LIKE_ANY)) {
    return RewriteLikeAnyAllArrayWithAggregate(std::move(rewritten_input_expr),
                                               std::move(patterns_array_expr),
                                               kLikeAnyTemplate);
  }
  return RewriteLikeAnyAllArrayWithAggregate(std::move(rewritten_input_expr),
                                             std::move(patterns_array_expr),
                                             kLikeAllTemplate);
}

absl::Status LikeAnyAllRewriteVisitor::RewriteLikeAnyAllArray(
    const ResolvedFunctionCall* node) {
  // Extract LIKE ANY arguments when given an array of patterns
  ZETASQL_RET_CHECK_EQ(node->argument_list_size(), 2)
      << "LIKE ANY with UNNEST has exactly 2 arguments. Got: "
      << node->DebugString();
  const ResolvedExpr* input_expr = node->argument_list(0);
  ZETASQL_RET_CHECK_NE(input_expr, nullptr);
  const ResolvedExpr* patterns_array_expr = node->argument_list(1);
  ZETASQL_RET_CHECK_NE(patterns_array_expr, nullptr);

  // Process child nodes first, so any nested rewrites are handled
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> rewritten_input_expr,
                   ProcessNode(input_expr));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> rewritten_patterns_array_expr,
                   ProcessNode(patterns_array_expr));

  if (IsBuiltInFunctionIdEq(node, FN_STRING_ARRAY_LIKE_ANY) ||
      IsBuiltInFunctionIdEq(node, FN_BYTE_ARRAY_LIKE_ANY)) {
    return RewriteLikeAnyAllArrayWithAggregate(
        std::move(rewritten_input_expr),
        std::move(rewritten_patterns_array_expr), kLikeAnyTemplate);
  }
  return RewriteLikeAnyAllArrayWithAggregate(
      std::move(rewritten_input_expr), std::move(rewritten_patterns_array_expr),
      kLikeAllTemplate);
}

absl::Status LikeAnyAllRewriteVisitor::RewriteLikeAnyAllArrayWithAggregate(
    std::unique_ptr<const ResolvedExpr> input_expr,
    std::unique_ptr<const ResolvedExpr> patterns_array_expr,
    absl::string_view template_string) {
  // Takes in the input expression, pattern array, and a template string that
  // differs between LIKE ANY and LIKE ALL expressions. AnalyzeSubstitute is
  // used with the function arguments to create the rewritten resolved AST.

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedExpr> result,
      AnalyzeSubstitute(*analyzer_options_, *catalog_, *type_factory_,
                        template_string,
                        {{"input", input_expr.get()},
                         {"patterns", patterns_array_expr.get()}}));
  PushNodeToStack(std::move(result));
  return absl::OkStatus();
}

}  // namespace

class LikeAnyAllRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.id_string_pool() != nullptr);
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    LikeAnyAllRewriteVisitor rewriter(&options, &catalog, &type_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "LikeAnyAllRewriter"; }
};

const Rewriter* GetLikeAnyAllRewriter() {
  static const auto* kRewriter = new LikeAnyAllRewriter;
  return kRewriter;
}

}  // namespace zetasql
