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
#include <vector>

#include "zetasql/analyzer/substitute.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/rewriter_interface.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

class BuiltinFunctionInlinerVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  BuiltinFunctionInlinerVisitor(const AnalyzerOptions& analyzer_options,
                                Catalog* catalog, TypeFactory* type_factory)
      : analyzer_options_(analyzer_options),
        catalog_(catalog),
        type_factory_(type_factory) {}

 private:
  absl::Status ValidateFunctionArgumentTypeOptions(
      const FunctionArgumentTypeOptions& options, int arg_idx) {
    ZETASQL_RET_CHECK(options.has_argument_name())
        << "Functions with configured inlining must provide argument names. "
        << "Missing argument name for argument " << arg_idx;
    return absl::OkStatus();
  }
  absl::Status Rewrite(const ResolvedFunctionCall* node,
                       absl::string_view rewrite_template) {
    // generic_argument list is only set if at least one argument of this
    // function call is not a ResolvedExpr e.g. a ResolvedInlineLambda
    // otherwise argument_list is used and all arguments are ResolvedExprs.
    bool use_generic_arguments = node->generic_argument_list_size() != 0;
    int num_arguments = use_generic_arguments
                            ? node->generic_argument_list_size()
                            : node->argument_list_size();
    ZETASQL_RET_CHECK_EQ(num_arguments, node->signature().arguments().size())
        << "Number of arguments provided " << num_arguments
        << " does not match the number of arguments expected by the function "
        << " signature " << node->signature().arguments().size();

    absl::flat_hash_map<std::string, const ResolvedInlineLambda*> lambdas;
    absl::flat_hash_map<std::string, const ResolvedExpr*> variables;
    std::vector<std::unique_ptr<ResolvedExpr>> processed_args;
    std::vector<std::unique_ptr<ResolvedInlineLambda>> processed_lambdas;
    for (int i = 0; i < num_arguments; ++i) {
      const ResolvedFunctionArgument* arg =
          use_generic_arguments ? node->generic_argument_list(i) : nullptr;
      if (use_generic_arguments && arg->inline_lambda() != nullptr) {
        const ResolvedInlineLambda* lambda = arg->inline_lambda();
        ZETASQL_ASSIGN_OR_RETURN(processed_lambdas.emplace_back(), ProcessNode(lambda));

        const FunctionArgumentTypeOptions& arg_options =
            node->signature().arguments()[i].options();
        ZETASQL_RETURN_IF_ERROR(ValidateFunctionArgumentTypeOptions(arg_options, i));

        const auto& [_, no_conflict] = lambdas.try_emplace(
            arg_options.argument_name(), processed_lambdas.back().get());
        ZETASQL_RET_CHECK(no_conflict)
            << "Duplicate lambda argument name not allowed for inlined"
            << "built-in function: " << arg_options.argument_name();
      } else {
        ZETASQL_RET_CHECK(!use_generic_arguments ||
                  (use_generic_arguments && arg->expr() != nullptr))
            << "REWRITE_BUILTIN_FUNCTION_INLINER only supports normal "
            << "expression arguments or function-typed arguments.";
        const ResolvedExpr* arg = use_generic_arguments
                                      ? node->generic_argument_list(i)->expr()
                                      : node->argument_list(i);
        ZETASQL_RET_CHECK_NE(arg, nullptr);
        ZETASQL_ASSIGN_OR_RETURN(processed_args.emplace_back(), ProcessNode(arg));

        const FunctionArgumentTypeOptions& arg_options =
            node->signature().arguments()[i].options();
        ZETASQL_RETURN_IF_ERROR(ValidateFunctionArgumentTypeOptions(arg_options, i));

        const auto& [_, no_conflict] = variables.try_emplace(
            arg_options.argument_name(), processed_args.back().get());
        ZETASQL_RET_CHECK(no_conflict)
            << "Duplicate argument name not allowed for inlined built-in "
            << "function: " << arg_options.argument_name();
      }
    }

    bool is_safe =
        node->error_mode() == ResolvedFunctionCallBase::SAFE_ERROR_MODE;
    if (is_safe) {
      ZETASQL_RETURN_IF_ERROR(CheckCatalogSupportsSafeMode(
          node->function()->SQLName(), analyzer_options_, *catalog_));
    }

    // A generic template to handle SAFE version function expression.
    constexpr absl::string_view kSafeExprTemplate = "NULLIFERROR($0)";

    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<ResolvedExpr> rewritten_expr,
        AnalyzeSubstitute(
            analyzer_options_, *catalog_, *type_factory_,
            is_safe ? absl::Substitute(kSafeExprTemplate, rewrite_template)
                    : rewrite_template,
            variables, lambdas));
    PushNodeToStack(std::move(rewritten_expr));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    if (!node->signature().options().rewrite_options().has_value()) {
      return CopyVisitResolvedFunctionCall(node);
    }
    const FunctionSignatureRewriteOptions& rewrite_options =
        node->signature().options().rewrite_options().value();
    if (rewrite_options.enabled() &&
        rewrite_options.rewriter() == REWRITE_BUILTIN_FUNCTION_INLINER) {
      return Rewrite(node, rewrite_options.sql());
    }
    return CopyVisitResolvedFunctionCall(node);
  }

  const AnalyzerOptions& analyzer_options_;
  Catalog* catalog_;
  TypeFactory* type_factory_;
};

class BuiltinFunctionInliner : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK_NE(options.id_string_pool(), nullptr);
    ZETASQL_RET_CHECK_NE(options.column_id_sequence_number(), nullptr);
    BuiltinFunctionInlinerVisitor rewriter(options, &catalog, &type_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "BuiltinFunctionInliner"; }
};

}  // namespace

const Rewriter* GetBuiltinFunctionInliner() {
  static const auto* const kRewriter = new BuiltinFunctionInliner;
  return kRewriter;
}

}  // namespace zetasql
