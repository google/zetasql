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
#include "zetasql/public/types/array_type.h"
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
class RewriteUnaryFunctionVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  RewriteUnaryFunctionVisitor(const AnalyzerOptions& analyzer_options,
                              Catalog* catalog, TypeFactory* type_factory)
      : analyzer_options_(analyzer_options),
        catalog_(catalog),
        type_factory_(type_factory) {}

 private:
  absl::Status Rewrite(const ResolvedFunctionCall* node,
                       absl::string_view rewrite_template) {
    ZETASQL_RET_CHECK_EQ(node->argument_list_size(), 1)
        << node->function()->SQLName()
        << " should have 1 arguments. Got: " << node->DebugString();
    const ResolvedExpr* input = node->argument_list(0);
    ZETASQL_RET_CHECK_NE(input, nullptr);
    bool is_safe =
        node->error_mode() == ResolvedFunctionCallBase::SAFE_ERROR_MODE;

    // Process child node first, so that input array argument is rewritten.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> processed_input,
                     ProcessNode(input));

    // A generic template to handle SAFE version function expression.
    constexpr absl::string_view kSafeExprTemplate = "NULLIFERROR($0)";

    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<ResolvedExpr> rewritten_expr,
        AnalyzeSubstitute(
            analyzer_options_, *catalog_, *type_factory_,
            is_safe ? absl::Substitute(kSafeExprTemplate, rewrite_template)
                    : rewrite_template,
            /*variables=*/
            {{"input", processed_input.get()}}));
    PushNodeToStack(std::move(rewritten_expr));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    // Templates with null hanlding.
    constexpr absl::string_view kArrayFirstTemplate = R"(
    CASE
      WHEN input IS NULL THEN NULL
      WHEN ARRAY_LENGTH(input) = 0 THEN ERROR('ARRAY_FIRST cannot get the first element of an empty array')
      ELSE input[OFFSET(0)]
    END
    )";
    constexpr absl::string_view kArrayLastTemplate = R"(
    CASE
      WHEN input IS NULL THEN NULL
      WHEN ARRAY_LENGTH(input) = 0 THEN ERROR('ARRAY_LAST cannot get the last element of an empty array')
      ELSE input[ORDINAL(ARRAY_LENGTH(input))]
    END
    )";
    constexpr absl::string_view kArraySumTemplate = R"(
    IF (input IS NULL, NULL, (
      SELECT SUM(e)
      FROM UNNEST(input) AS e))
    )";
    constexpr absl::string_view kArrayAvgTemplate = R"(
    IF (input IS NULL, NULL, (
      SELECT AVG(e)
      FROM UNNEST(input) AS e))
    )";
    constexpr absl::string_view kArrayMinTemplate = R"(
    IF (input IS NULL, NULL, (
      SELECT e
      FROM UNNEST(input) AS e WITH OFFSET AS idx
      WHERE e IS NOT NULL
      ORDER BY e ASC, idx ASC
      LIMIT 1))
    )";
    constexpr absl::string_view kArrayMaxFPTemplate = R"(
    IF (input IS NULL, NULL, (
      SELECT e
      FROM UNNEST(input) AS e WITH OFFSET AS idx
      WHERE e IS NOT NULL
      ORDER BY IS_NAN(e) DESC, e DESC, idx ASC
      LIMIT 1))
    )";
    constexpr absl::string_view kArrayMaxNonFPTemplate = R"(
    IF (input IS NULL, NULL, (
      SELECT e
      FROM UNNEST(input) AS e WITH OFFSET AS idx
      WHERE e IS NOT NULL
      ORDER BY e DESC, idx ASC
      LIMIT 1))
    )";
    if (IsBuiltInFunctionIdEq(node, FN_ARRAY_FIRST)) {
      return Rewrite(node, kArrayFirstTemplate);
    } else if (IsBuiltInFunctionIdEq(node, FN_ARRAY_LAST)) {
      return Rewrite(node, kArrayLastTemplate);
    } else if (IsBuiltInFunctionIdEq(node, FN_ARRAY_SUM_INT32) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_SUM_INT64) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_SUM_UINT32) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_SUM_UINT64) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_SUM_FLOAT) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_SUM_DOUBLE) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_SUM_NUMERIC) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_SUM_BIGNUMERIC) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_SUM_INTERVAL)) {
      return Rewrite(node, kArraySumTemplate);
    } else if (IsBuiltInFunctionIdEq(node, FN_ARRAY_AVG_INT32) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_AVG_INT64) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_AVG_UINT32) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_AVG_UINT64) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_AVG_FLOAT) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_AVG_DOUBLE) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_AVG_NUMERIC) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_AVG_BIGNUMERIC) ||
               IsBuiltInFunctionIdEq(node, FN_ARRAY_AVG_INTERVAL)) {
      return Rewrite(node, kArrayAvgTemplate);
    } else if (IsBuiltInFunctionIdEq(node, FN_ARRAY_MIN)) {
      return Rewrite(node, kArrayMinTemplate);
    } else if (IsBuiltInFunctionIdEq(node, FN_ARRAY_MAX)) {
      const ResolvedExpr* arg = node->argument_list(0);
      const ArrayType* array_type = arg->type()->AsArray();
      ZETASQL_RET_CHECK_NE(array_type, nullptr);
      if (array_type->element_type()->IsFloatingPoint()) {
        return Rewrite(node, kArrayMaxFPTemplate);
      }
      return Rewrite(node, kArrayMaxNonFPTemplate);
    }
    return CopyVisitResolvedFunctionCall(node);
  }

  const AnalyzerOptions& analyzer_options_;
  Catalog* catalog_;
  TypeFactory* type_factory_;
};

class UnaryFunctionRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK_NE(options.id_string_pool(), nullptr);
    ZETASQL_RET_CHECK_NE(options.column_id_sequence_number(), nullptr);
    RewriteUnaryFunctionVisitor rewriter(options, &catalog, &type_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "UnaryFunctionRewriter"; }
};

}  // namespace

const Rewriter* GetUnaryFunctionRewriter() {
  static const auto* const kRewriter = new UnaryFunctionRewriter;
  return kRewriter;
}

}  // namespace zetasql
