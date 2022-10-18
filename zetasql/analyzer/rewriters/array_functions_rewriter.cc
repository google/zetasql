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
#include <variant>

#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/analyzer/substitute.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/builtin_function.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// There are currently separate flags for rewriting FILTER/TRANSFORM vs
// INCLUDES. Until we unify those flags, we need to keep the behavior controlled
// separately.
enum ArrayFunctionRewriterKind {
  ARRAY_FILTER_TRANSFORM_REWRITER,
  ARRAY_INCLUDES_REWRITER,
};

// Visitor to rewrite all array function calls
class ArrayFunctionRewriteVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  ArrayFunctionRewriteVisitor(const AnalyzerOptions& analyzer_options,
                              Catalog* catalog, TypeFactory* type_factory,
                              ArrayFunctionRewriterKind kind)
      : analyzer_options_(analyzer_options),
        catalog_(catalog),
        type_factory_(type_factory),
        kind_(kind) {}

  // Rewrites array function calls after choosing the appropriate template
  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    // If not empty, the template that has null hanlding and ordering.
    absl::string_view rewrite_template;

    switch (node->signature().context_id()) {
      case FunctionSignatureId::FN_ARRAY_FILTER:
      case FunctionSignatureId::FN_ARRAY_FILTER_WITH_INDEX:
        if (kind_ == ARRAY_FILTER_TRANSFORM_REWRITER) {
          rewrite_template = R"(
            IF (array_input IS NULL,
                NULL,
                ARRAY(
                  SELECT element
                  FROM UNNEST(array_input) AS element WITH OFFSET off
                  WHERE INVOKE(@lambda, element, off)
                  ORDER BY off
                )
              )
            )";
        }
        break;
      case FunctionSignatureId::FN_ARRAY_TRANSFORM:
      case FunctionSignatureId::FN_ARRAY_TRANSFORM_WITH_INDEX:
        if (kind_ == ARRAY_FILTER_TRANSFORM_REWRITER) {
          rewrite_template = R"(
            IF (array_input IS NULL,
                NULL,
                ARRAY(
                  SELECT INVOKE(@lambda, element, off)
                  FROM UNNEST(array_input) AS element WITH OFFSET off
                  ORDER BY off
                )
            )
          )";
        }
        break;
      case FunctionSignatureId::FN_ARRAY_INCLUDES:
        if (kind_ == ARRAY_INCLUDES_REWRITER) {
          rewrite_template = R"(
            IF (array_input IS NULL OR target is NULL,
            NULL,
            EXISTS(SELECT 1
                   FROM UNNEST(array_input) AS element
                   WHERE element = target)
            )
          )";
        }
        break;
      case FunctionSignatureId::FN_ARRAY_INCLUDES_LAMBDA:
        if (kind_ == ARRAY_INCLUDES_REWRITER) {
          rewrite_template = R"(
            IF (array_input IS NULL,
                NULL,
                EXISTS(SELECT 1
                       FROM UNNEST(array_input) AS element
                       WHERE INVOKE(@lambda, element)
                )
            )
          )";
        }
        break;
      case FunctionSignatureId::FN_ARRAY_INCLUDES_ANY:
        if (kind_ == ARRAY_INCLUDES_REWRITER) {
          rewrite_template = R"(
            IF (array_input IS NULL OR target is NULL,
                NULL,
                EXISTS(SELECT 1
                       FROM UNNEST(array_input) AS element
                       WHERE element IN UNNEST(target)
                )
            )
          )";
        }
        break;
      case FunctionSignatureId::FN_ARRAY_INCLUDES_ALL:
        if (kind_ == ARRAY_INCLUDES_REWRITER) {
          rewrite_template = R"(
            IF (array_input IS NULL OR target is NULL, NULL,
                IF (ARRAY_LENGTH(target) = 0,
                    TRUE,
                    (SELECT LOGICAL_AND(IFNULL(element IN UNNEST(array_input), FALSE))
                     FROM UNNEST(target) AS element)))
            )";
        }
        break;
      default:
        break;
    }

    return rewrite_template.empty()
               ? CopyVisitResolvedFunctionCall(node)
               : RewriteArrayFunctionCall(node, rewrite_template);
  }

 private:
  // Rewrites the given function call node with the input subquery template
  absl::Status RewriteArrayFunctionCall(
      const ResolvedFunctionCall* node,
      const absl::string_view unsafe_template) {
    const ResolvedExpr* array_input;
    const ResolvedExpr* target = nullptr;
    const ResolvedInlineLambda* lambda = nullptr;

    if (node->generic_argument_list_size() == 2) {
      array_input = node->generic_argument_list(0)->expr();
      ZETASQL_RET_CHECK_NE(array_input, nullptr);
      lambda = node->generic_argument_list(1)->inline_lambda();
      ZETASQL_RET_CHECK_NE(lambda, nullptr);

    } else {
      ZETASQL_RET_CHECK_EQ(node->argument_list_size(), 2);
      array_input = node->argument_list(0);
      ZETASQL_RET_CHECK_NE(array_input, nullptr);
      target = node->argument_list(1);
      ZETASQL_RET_CHECK_NE(target, nullptr);
    }

    // Process child nodes first, for any potential similar rewrites in them.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_array_input,
                     ProcessNode(array_input));

    absl::flat_hash_map<std::string, const ResolvedExpr*> variables{
        {"array_input", processed_array_input.get()},
    };

    std::unique_ptr<ResolvedExpr> processed_target;
    if (target != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(processed_target, ProcessNode(target));
      variables.insert({"target", processed_target.get()});
    }

    std::unique_ptr<ResolvedInlineLambda> processed_lambda;
    absl::flat_hash_map<std::string, const ResolvedInlineLambda*> lambdas{};
    if (lambda != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(processed_lambda, ProcessNode(lambda));
      lambdas.insert({"lambda", processed_lambda.get()});
    }

    absl::string_view chosen_temlpate = unsafe_template;

    // Holds the value for chosen_template. Only computed if actually needed.
    std::string safe_template;
    if (node->error_mode() == ResolvedFunctionCall::SAFE_ERROR_MODE) {
      safe_template = absl::StrCat("NULLIFERROR(", unsafe_template, ")");
      chosen_temlpate = safe_template;
    }

    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<ResolvedExpr> res,
        AnalyzeSubstitute(analyzer_options_, *catalog_, *type_factory_,
                          chosen_temlpate, variables, lambdas));
    PushNodeToStack(std::move(res));

    return absl::OkStatus();
  }

  const AnalyzerOptions& analyzer_options_;
  Catalog* catalog_;
  TypeFactory* type_factory_;
  const ArrayFunctionRewriterKind kind_;
};

// Rewriter for array function calls
class ArrayFunctionsRewriter : public Rewriter {
 public:
  explicit ArrayFunctionsRewriter(ArrayFunctionRewriterKind kind)
      : kind_(kind) {}
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.id_string_pool() != nullptr);
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ArrayFunctionRewriteVisitor rewriter(options, &catalog, &type_factory,
                                         kind_);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override {
    switch (kind_) {
      case ARRAY_FILTER_TRANSFORM_REWRITER:
        return "ArrayFilterTransformRewriter";
      case ARRAY_INCLUDES_REWRITER:
        return "ArrayIncludesRewriter";
    }
  }

 private:
  const ArrayFunctionRewriterKind kind_;
};

}  // namespace

const Rewriter* GetArrayFilterTransformRewriter() {
  static const auto* const kRewriter =
      new ArrayFunctionsRewriter(ARRAY_FILTER_TRANSFORM_REWRITER);
  return kRewriter;
}

const Rewriter* GetArrayIncludesRewriter() {
  static const auto* const kRewriter =
      new ArrayFunctionsRewriter(ARRAY_INCLUDES_REWRITER);
  return kRewriter;
}

}  // namespace zetasql
