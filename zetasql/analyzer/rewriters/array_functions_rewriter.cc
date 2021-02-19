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

#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/analyzer/substitute.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "zetasql/resolved_ast/validator.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
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
        type_factory_(type_factory) {}

 private:
  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override;

  absl::Status RewriteArrayFilter(const ResolvedFunctionCall* node);

  absl::Status RewriteArrayTransform(const ResolvedFunctionCall* node);

  const AnalyzerOptions& analyzer_options_;
  absl::Span<const Rewriter* const> rewriters_;
  Catalog* catalog_;
  TypeFactory* type_factory_;
};

absl::Status FunctionWithInlineLambdaRewriterVisitor::VisitResolvedFunctionCall(
    const ResolvedFunctionCall* node) {
  if (node->signature().context_id() == FunctionSignatureId::FN_ARRAY_FILTER ||
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

}  // namespace

class ArrayFunctionRewriter : public Rewriter {
 public:
  bool ShouldRewrite(const AnalyzerOptions& analyzer_options,
                     const AnalyzerOutput& analyzer_output) const override {
    return analyzer_options.rewrite_enabled(
               REWRITE_ARRAY_FUNCTIONS_WITH_LAMBDA) &&
           analyzer_output.analyzer_output_properties()
               .has_array_functions_to_rewrite;
  }

  zetasql_base::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options,
      absl::Span<const Rewriter* const> rewriters, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.id_string_pool() != nullptr);
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    FunctionWithInlineLambdaRewriterVisitor rewriter(options, rewriters,
                                                     &catalog, &type_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "ArrayFunctionRewriter"; }
};

const Rewriter* GetArrayFunctionsRewriter() {
  static const auto* const kRewriter = new ArrayFunctionRewriter;
  return kRewriter;
}

}  // namespace zetasql
