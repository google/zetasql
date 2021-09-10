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

#include <memory>
#include <string>
#include <utility>

#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/analyzer/substitute.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// A visitor that rewrites ResolvedFunctionCalls with ResolvedInlineLambdas into
// ResolvedSubqueryExpr.
class FunctionWithInlineLambdaRewriterVisitor
    : public ResolvedASTDeepCopyVisitor {
 public:
  explicit FunctionWithInlineLambdaRewriterVisitor(
      const AnalyzerOptions& analyzer_options,
      absl::Span<const Rewriter* const> rewriters, Catalog* catalog,
      TypeFactory* type_factory)
      : analyzer_options_(analyzer_options),
        rewriters_(rewriters),
        catalog_(catalog),
        type_factory_(type_factory),
        rewrite_array_filter_transform_(
            analyzer_options_.rewrite_enabled(REWRITE_ARRAY_FILTER_TRANSFORM)),
        rewrite_array_includes_(
            analyzer_options_.rewrite_enabled(REWRITE_ARRAY_INCLUDES)) {}

 private:
  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override;

  absl::Status RewriteArrayFilter(const ResolvedFunctionCall* node);

  absl::Status RewriteArrayTransform(const ResolvedFunctionCall* node);

  absl::Status RewriteArrayIncludes(const ResolvedFunctionCall* node);

  absl::Status RewriteArrayIncludesLambda(const ResolvedFunctionCall* node);

  absl::Status RewriteArrayIncludesAny(const ResolvedFunctionCall* node);

  const AnalyzerOptions& analyzer_options_;
  absl::Span<const Rewriter* const> rewriters_;
  Catalog* catalog_;
  TypeFactory* type_factory_;
  bool rewrite_array_filter_transform_;
  bool rewrite_array_includes_;
};

absl::Status FunctionWithInlineLambdaRewriterVisitor::VisitResolvedFunctionCall(
    const ResolvedFunctionCall* node) {
  if (rewrite_array_filter_transform_) {
    if (node->signature().context_id() ==
            FunctionSignatureId::FN_ARRAY_FILTER ||
        node->signature().context_id() ==
            FunctionSignatureId::FN_ARRAY_FILTER_WITH_INDEX) {
      return RewriteArrayFilter(node);
    }
    if (node->signature().context_id() ==
            FunctionSignatureId::FN_ARRAY_TRANSFORM ||
        node->signature().context_id() ==
            FunctionSignatureId::FN_ARRAY_TRANSFORM_WITH_INDEX) {
      return RewriteArrayTransform(node);
    }
  }

  if (rewrite_array_includes_) {
    if (node->signature().context_id() ==
        FunctionSignatureId::FN_ARRAY_INCLUDES) {
      return RewriteArrayIncludes(node);
    }
    if (node->signature().context_id() ==
        FunctionSignatureId::FN_ARRAY_INCLUDES_LAMBDA) {
      return RewriteArrayIncludesLambda(node);
    }
    if (node->signature().context_id() ==
        FunctionSignatureId::FN_ARRAY_INCLUDES_ANY) {
      return RewriteArrayIncludesAny(node);
    }
  }

  return CopyVisitResolvedFunctionCall(node);
}

absl::Status FunctionWithInlineLambdaRewriterVisitor::RewriteArrayFilter(
    const ResolvedFunctionCall* node) {
  // Extract ARRAY_FILTER arguments.
  ZETASQL_RET_CHECK_EQ(node->generic_argument_list_size(), 2)
      << "ARRAY_FILTER has at least 2 arguments. Got: " << node->DebugString();
  const ResolvedExpr* array_input = node->generic_argument_list(0)->expr();
  ZETASQL_RET_CHECK_NE(array_input, nullptr);
  const ResolvedInlineLambda* lambda =
      node->generic_argument_list(1)->inline_lambda();
  ZETASQL_RET_CHECK_NE(lambda, nullptr);

  // Process child nodes first, so that ARRAY_FILTER inside lambda body are
  // rewritten.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_array_input,
                   ProcessNode(array_input));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedInlineLambda> processed_lambda,
                   ProcessNode(lambda));

  // Template that has null hanlding and ordering.
  constexpr absl::string_view kFilterTemplate = R"(
    IF (array_input IS NULL,
        NULL,
        ARRAY(SELECT element
          FROM UNNEST(array_input) AS element WITH OFFSET off
          WHERE INVOKE(@lambda, element, off)
          ORDER BY off))
  )";
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedExpr> res,
      AnalyzeSubstitute(
          analyzer_options_, rewriters_, *catalog_, *type_factory_,
          kFilterTemplate,
          /*variables=*/{{"array_input", processed_array_input.get()}},
          /*lambdas=*/{{"lambda", processed_lambda.get()}}));
  PushNodeToStack(std::move(res));

  return absl::OkStatus();
}

absl::Status FunctionWithInlineLambdaRewriterVisitor::RewriteArrayTransform(
    const ResolvedFunctionCall* node) {
  // Extract ARRAY_TRANSFORM arguments.
  ZETASQL_RET_CHECK_EQ(node->generic_argument_list_size(), 2);
  const ResolvedExpr* array_input = node->generic_argument_list(0)->expr();
  ZETASQL_RET_CHECK_NE(array_input, nullptr);
  const ResolvedInlineLambda* inline_lambda =
      node->generic_argument_list(1)->inline_lambda();
  ZETASQL_RET_CHECK_NE(inline_lambda, nullptr);

  // Process child nodes first, so that ARRAY_TRANSFORM inside lambda body are
  // rewritten.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_array_input,
                   ProcessNode(array_input));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedInlineLambda> processed_lambda,
                   ProcessNode(inline_lambda));

  // Template that has null hanlding and ordering.
  constexpr absl::string_view kTransformTemplate = R"(
    IF (array_input IS NULL,
        NULL,
        ARRAY(SELECT INVOKE(@lambda, element, off)
          FROM UNNEST(array_input) AS element WITH OFFSET off
          ORDER BY off))
  )";
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedExpr> res,
      AnalyzeSubstitute(
          analyzer_options_, rewriters_, *catalog_, *type_factory_,
          kTransformTemplate,
          /*variables=*/{{"array_input", processed_array_input.get()}},
          /*lambdas=*/{{"lambda", processed_lambda.get()}}));
  PushNodeToStack(std::move(res));

  return absl::OkStatus();
}

absl::Status FunctionWithInlineLambdaRewriterVisitor::RewriteArrayIncludes(
    const ResolvedFunctionCall* node) {
  ZETASQL_RET_CHECK_EQ(node->argument_list_size(), 2)
      << "ARRAY_INCLUDES should have 2 arguments. Got: " << node->DebugString();
  const ResolvedExpr* array_input = node->argument_list(0);
  ZETASQL_RET_CHECK_NE(array_input, nullptr);
  const ResolvedExpr* target = node->argument_list(1);
  ZETASQL_RET_CHECK_NE(target, nullptr);

  // Process child nodes first, so that ARRAY_INCLUDES inside arguments are
  // rewritten.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_array_input,
                   ProcessNode(array_input));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_target,
                   ProcessNode(target));

  // Template with null hanlding.
  constexpr absl::string_view kIncludesTemplate = R"(
    IF (array_input IS NULL OR target is NULL,
        NULL,
        EXISTS(SELECT 1 FROM UNNEST(array_input) AS element
               WHERE element = target))
  )";
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedExpr> res,
      AnalyzeSubstitute(analyzer_options_, rewriters_, *catalog_,
                        *type_factory_, kIncludesTemplate,
                        /*variables=*/
                        {{"array_input", processed_array_input.get()},
                         {"target", processed_target.get()}}));
  PushNodeToStack(std::move(res));
  return absl::OkStatus();
}

absl::Status
FunctionWithInlineLambdaRewriterVisitor::RewriteArrayIncludesLambda(
    const ResolvedFunctionCall* node) {
  // Extract ARRAY_INCLUDES arguments.
  ZETASQL_RET_CHECK_EQ(node->argument_list_size(), 0);
  ZETASQL_RET_CHECK_EQ(node->generic_argument_list_size(), 2)
      << "ARRAY_INCLUDES should have 2 arguments. Got: " << node->DebugString();
  const ResolvedExpr* array_input = node->generic_argument_list(0)->expr();
  ZETASQL_RET_CHECK_NE(array_input, nullptr);
  const ResolvedInlineLambda* inline_lambda =
      node->generic_argument_list(1)->inline_lambda();
  ZETASQL_RET_CHECK_NE(inline_lambda, nullptr);

  // Process child nodes first, so that ARRAY_INCLUDES inside arguments are
  // rewritten.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_array_input,
                   ProcessNode(array_input));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedInlineLambda> processed_lambda,
                   ProcessNode(inline_lambda));

  // Template with null hanlding.
  constexpr absl::string_view kIncludesTemplate = R"(
    IF (array_input IS NULL,
        NULL,
        EXISTS(SELECT 1 FROM UNNEST(array_input) AS element
               WHERE INVOKE(@lambda, element)))
  )";

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedExpr> res,
      AnalyzeSubstitute(
          analyzer_options_, rewriters_, *catalog_, *type_factory_,
          kIncludesTemplate,
          /*variables=*/{{"array_input", processed_array_input.get()}},
          /*lambdas=*/{{"lambda", processed_lambda.get()}}));
  PushNodeToStack(std::move(res));

  return absl::OkStatus();
}

absl::Status FunctionWithInlineLambdaRewriterVisitor::RewriteArrayIncludesAny(
    const ResolvedFunctionCall* node) {
  ZETASQL_RET_CHECK_EQ(node->argument_list_size(), 2)
      << "ARRAY_INCLUDES should have 2 arguments. Got: " << node->DebugString();
  const ResolvedExpr* array_input = node->argument_list(0);
  ZETASQL_RET_CHECK_NE(array_input, nullptr);
  const ResolvedExpr* target = node->argument_list(1);
  ZETASQL_RET_CHECK_NE(target, nullptr);

  // Process child nodes first, so that ARRAY_INCLUDES inside arguments are
  // rewritten.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_array_input,
                   ProcessNode(array_input));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_target,
                   ProcessNode(target));

  // Template with null hanlding.
  constexpr absl::string_view kIncludesTemplate = R"(
    IF (array_input IS NULL OR target is NULL,
        NULL,
        EXISTS(SELECT 1 FROM UNNEST(array_input) AS element
               WHERE element IN UNNEST(target)))
  )";
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedExpr> res,
      AnalyzeSubstitute(analyzer_options_, rewriters_, *catalog_,
                        *type_factory_, kIncludesTemplate,
                        /*variables=*/
                        {{"array_input", processed_array_input.get()},
                         {"target", processed_target.get()}}));
  PushNodeToStack(std::move(res));
  return absl::OkStatus();
}

}  // namespace

class ArrayFunctionRewriter : public Rewriter {
 public:
  bool ShouldRewrite(const AnalyzerOptions& analyzer_options,
                     const AnalyzerOutput& analyzer_output) const override {
    return (analyzer_output.analyzer_output_properties()
                .has_array_filter_or_transform &&
            analyzer_options.rewrite_enabled(REWRITE_ARRAY_FILTER_TRANSFORM)) ||
           (analyzer_output.analyzer_output_properties().has_array_includes &&
            analyzer_options.rewrite_enabled(REWRITE_ARRAY_INCLUDES));
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options,
      absl::Span<const Rewriter* const> rewriters, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.id_string_pool() != nullptr);
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    FunctionWithInlineLambdaRewriterVisitor rewriter(options, rewriters,
                                                     &catalog, &type_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    if (options.rewrite_enabled(REWRITE_ARRAY_FILTER_TRANSFORM)) {
      output_properties.has_array_filter_or_transform = false;
    }
    if (options.rewrite_enabled(REWRITE_ARRAY_INCLUDES)) {
      output_properties.has_array_includes = false;
    }
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "ArrayFunctionRewriter"; }
};

const Rewriter* GetArrayFunctionsRewriter() {
  static const auto* const kRewriter = new ArrayFunctionRewriter;
  return kRewriter;
}

}  // namespace zetasql
