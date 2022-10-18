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
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// The rewriter visitor for unary scalar functions.
class RewriteTernaryFunctionVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  RewriteTernaryFunctionVisitor(const AnalyzerOptions& analyzer_options,
                                Catalog* catalog, TypeFactory* type_factory)
      : analyzer_options_(analyzer_options),
        catalog_(catalog),
        type_factory_(type_factory) {}

 private:
  absl::Status Rewrite(const ResolvedFunctionCall* node,
                       absl::string_view rewrite_template) {
    ZETASQL_RET_CHECK_EQ(node->argument_list_size(), 3)
        << node->function()->SQLName()
        << " should have 3 arguments. Got: " << node->DebugString();
    const ResolvedExpr* first_input = node->argument_list(0);
    ZETASQL_RET_CHECK_NE(first_input, nullptr);
    const ResolvedExpr* second_input = node->argument_list(1);
    ZETASQL_RET_CHECK_NE(second_input, nullptr);
    const ResolvedExpr* third_input = node->argument_list(2);
    ZETASQL_RET_CHECK_NE(third_input, nullptr);

    bool is_safe =
        node->error_mode() == ResolvedFunctionCallBase::SAFE_ERROR_MODE;

    // Process child node first, so that input arguments are rewritten.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_first_input,
                     ProcessNode(first_input));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_second_input,
                     ProcessNode(second_input));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_third_input,
                     ProcessNode(third_input));

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
             {"second_input", processed_second_input.get()},
             {"third_input", processed_third_input.get()}}));
    PushNodeToStack(std::move(rewritten_expr));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    // Templates with null hanlding.
    // * ARRAY_SLICE(first_input, second_input, third_input) represents
    //   ARRAY_SLICE(array, start, end)
    constexpr absl::string_view kArraySliceTemplate = R"sql(
    CASE
      WHEN first_input IS NULL OR second_input IS NULL OR third_input IS NULL THEN NULL
      WHEN ARRAY_LENGTH(first_input) = 0 THEN []
      ELSE WITH(
        start_offset AS IF(second_input < 0, second_input + ARRAY_LENGTH(first_input), second_input),
        end_offset AS IF(third_input < 0, third_input + ARRAY_LENGTH(first_input), third_input),
        ARRAY(
          SELECT e
          FROM UNNEST(first_input) AS e WITH OFFSET idx
          WHERE idx BETWEEN start_offset AND end_offset
          ORDER BY idx
        )
      )
    END
    )sql";

    // * ARRAY_OFFSET(array, target, find_mode)
    // Note that, rewrite of the non-lambda version signature is interchangeable
    // with that of the lambda version ARRAY_FIND(array, lambda, find_mode),
    // where the default lambda for the non-lambda version is `e -> e = target`.
    constexpr absl::string_view kArrayOffsetTemplate = R"sql(
    IF(first_input IS NULL OR second_input IS NULL OR third_input IS NULL,
      NULL,
      CASE third_input
        WHEN 'FIRST' THEN (
          SELECT offset
          FROM UNNEST(first_input) AS e WITH OFFSET
          WHERE e = second_input
          ORDER BY offset LIMIT 1
        )
        WHEN 'LAST' THEN (
          SELECT offset
          FROM UNNEST(first_input) AS e WITH OFFSET
          WHERE e = second_input
          ORDER BY offset DESC LIMIT 1
        )
        ELSE ERROR(CONCAT('ARRAY_FIND_MODE ', third_input, ' in ARRAY_OFFSET is unsupported.'))
      END
    ))sql";

    // * ARRAY_FIND(array, target, find_mode)
    // Note that, rewrite of the non-lambda version signature is interchangeable
    // with that of the lambda version ARRAY_FIND(array, lambda, find_mode),
    // where the default lambda for the non-lambda version is `e -> e = target`.
    constexpr absl::string_view kArrayFindTemplate = R"sql(
    IF(first_input IS NULL OR second_input IS NULL OR third_input IS NULL,
      NULL,
      CASE third_input
        WHEN 'FIRST' THEN (
          SELECT e
          FROM UNNEST(first_input) AS e WITH OFFSET
          WHERE e = second_input
          ORDER BY offset LIMIT 1
        )
        WHEN 'LAST' THEN (
          SELECT e
          FROM UNNEST(first_input) AS e WITH OFFSET
          WHERE e = second_input
          ORDER BY offset DESC LIMIT 1
        )
        ELSE ERROR(CONCAT('ARRAY_FIND_MODE ', third_input, ' ARRAY_FIND_MODE in ARRAY_FIND is unsupported.'))
      END
    )
    )sql";
    if (IsBuiltInFunctionIdEq(node, FN_ARRAY_SLICE)) {
      return Rewrite(node, kArraySliceTemplate);
    } else if (IsBuiltInFunctionIdEq(node, FN_ARRAY_OFFSET)) {
      return Rewrite(node, kArrayOffsetTemplate);
    } else if (IsBuiltInFunctionIdEq(node, FN_ARRAY_FIND)) {
      return Rewrite(node, kArrayFindTemplate);
    }
    return CopyVisitResolvedFunctionCall(node);
  }

  const AnalyzerOptions& analyzer_options_;
  Catalog* catalog_;
  TypeFactory* type_factory_;
};

class TernaryFunctionRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK_NE(options.id_string_pool(), nullptr);
    ZETASQL_RET_CHECK_NE(options.column_id_sequence_number(), nullptr);
    RewriteTernaryFunctionVisitor rewriter(options, &catalog, &type_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "TernaryFunctionRewriter"; }
};

}  // namespace

const Rewriter* GetTernaryFunctionRewriter() {
  static const auto* const kRewriter = new TernaryFunctionRewriter;
  return kRewriter;
}

}  // namespace zetasql
