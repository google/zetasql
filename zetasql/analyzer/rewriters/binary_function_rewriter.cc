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
#include "zetasql/public/analyzer_output_properties.h"
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
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// The rewriter visitor for unary scalar functions.
class RewriteBinaryFunctionVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  RewriteBinaryFunctionVisitor(const AnalyzerOptions& analyzer_options,
                               Catalog* catalog, TypeFactory* type_factory)
      : analyzer_options_(analyzer_options),
        catalog_(catalog),
        type_factory_(type_factory) {}

 private:
  absl::Status Rewrite(const ResolvedFunctionCall* node,
                       absl::string_view rewrite_template) {
    ZETASQL_RET_CHECK_EQ(node->argument_list_size(), 2)
        << node->function()->SQLName()
        << " should have 2 arguments. Got: " << node->DebugString();
    const ResolvedExpr* first_input = node->argument_list(0);
    ZETASQL_RET_CHECK_NE(first_input, nullptr);
    const ResolvedExpr* second_input = node->argument_list(1);
    ZETASQL_RET_CHECK_NE(second_input, nullptr);

    bool is_safe =
        node->error_mode() == ResolvedFunctionCallBase::SAFE_ERROR_MODE;

    // Process child node first, so that input arguments are rewritten.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_first_input,
                     ProcessNode(first_input));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_second_input,
                     ProcessNode(second_input));

    // A generic template to handle SAFE version function expression.
    constexpr absl::string_view kSafeExprTemplate = "NULLIFERROR($0)";

    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<ResolvedExpr> rewritten_expr,
        AnalyzeSubstitute(
            analyzer_options_, *catalog_, *type_factory_,
            is_safe ? absl::Substitute(kSafeExprTemplate, rewrite_template)
                    : rewrite_template,
            /*variables=*/
            {{"first_input", processed_first_input.get()},
             {"second_input", processed_second_input.get()}}));
    PushNodeToStack(std::move(rewritten_expr));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    // Templates with null hanlding.
    // * ARRAY_OFFSETS(array, target)
    // Note that, rewrite of the non-lambda version signature is interchangeable
    // with that of the lambda version ARRAY_OFFSETS(array, lambda),
    // where the default lambda for the non-lambda version is `e -> e = target`.
    constexpr absl::string_view kArrayOffsetsTemplate = R"sql(
    IF(first_input IS NULL OR second_input IS NULL,
      NULL,
      ARRAY(
        SELECT offset
        FROM UNNEST(first_input) AS e WITH OFFSET
        WHERE e = second_input
        ORDER BY offset
      ))
    )sql";

    // * ARRAY_FIND_ALL(array, target)
    // Note that, rewrite of the non-lambda version signature is interchangeable
    // with that of the lambda version ARRAY_FIND_ALL(array, lambda),
    // where the default lambda for the non-lambda version is `e -> e = target`.
    constexpr absl::string_view kArrayFindAllTemplate = R"sql(
    IF(first_input IS NULL OR second_input IS NULL,
      NULL,
      ARRAY(
        SELECT e
        FROM UNNEST(first_input) AS e WITH OFFSET
        WHERE e = second_input
        ORDER BY offset
      ))
    )sql";
    if (IsBuiltInFunctionIdEq(node, FN_ARRAY_OFFSETS)) {
      return Rewrite(node, kArrayOffsetsTemplate);
    } else if (IsBuiltInFunctionIdEq(node, FN_ARRAY_FIND_ALL)) {
      return Rewrite(node, kArrayFindAllTemplate);
    }
    return CopyVisitResolvedFunctionCall(node);
  }

  const AnalyzerOptions& analyzer_options_;
  Catalog* catalog_;
  TypeFactory* type_factory_;
};

class BinaryFunctionRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK_NE(options.id_string_pool(), nullptr);
    ZETASQL_RET_CHECK_NE(options.column_id_sequence_number(), nullptr);
    RewriteBinaryFunctionVisitor rewriter(options, &catalog, &type_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "BinaryFunctionRewriter"; }
};

}  // namespace

const Rewriter* GetBinaryFunctionRewriter() {
  static const auto* const kRewriter = new BinaryFunctionRewriter;
  return kRewriter;
}

}  // namespace zetasql
