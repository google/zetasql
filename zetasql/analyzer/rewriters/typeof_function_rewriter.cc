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

#include "zetasql/analyzer/rewriters/typeof_function_rewriter.h"

#include <memory>
#include <string>
#include <utility>

#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// A visitor that rewrites ResolvedFunctionCalls with to TYPEOF(<source>) to
// IF(TRUE, <string literal>, CAST(<source> IS NULL AS STRING)). This shape
// acomplishes a couple goles:
// 1) Engines that enable the rewrite do not have to implement execution logic
//    for typeof.
// 2) Engines will see the original source expression in case it needs to track
//    object access for permission checks or expression sorts for supported-ness
//    checks.
class TypeofFunctionRewriteVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  TypeofFunctionRewriteVisitor(const AnalyzerOptions& analyzer_options,
                               Catalog* catalog, TypeFactory* type_factory)
      : analyzer_options_(analyzer_options),
        fn_builder_(analyzer_options, *catalog),
        type_factory_(type_factory) {}

 private:
  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override;

  absl::Status RewriteTypeof(const ResolvedFunctionCall* node);

  const AnalyzerOptions& analyzer_options_;
  FunctionCallBuilder fn_builder_;
  TypeFactory* type_factory_;
};

absl::Status TypeofFunctionRewriteVisitor::VisitResolvedFunctionCall(
    const ResolvedFunctionCall* node) {
  if (node->function() != nullptr &&
      node->signature().context_id() == FunctionSignatureId::FN_TYPEOF &&
      node->function()->IsZetaSQLBuiltin()) {
    return RewriteTypeof(node);
  }
  return CopyVisitResolvedFunctionCall(node);
}

absl::Status TypeofFunctionRewriteVisitor::RewriteTypeof(
    const ResolvedFunctionCall* node) {
  ZETASQL_RET_CHECK_EQ(node->argument_list_size(), 1)
      << "TYPEOF has 1 expression argument. Got: " << node->DebugString();
  const ResolvedExpr* original_expr = node->argument_list(0);
  ZETASQL_RET_CHECK_NE(original_expr, nullptr);

  std::unique_ptr<ResolvedExpr> true_literal =
      MakeResolvedLiteral(type_factory_->get_bool(), Value::Bool(true),
                          /*has_explicit_type=*/true);
  std::unique_ptr<ResolvedExpr> typename_literal =
      MakeResolvedLiteral(type_factory_->get_string(),
                          Value::String(original_expr->type()->TypeName(
                              analyzer_options_.language().product_mode())),
                          /*has_explicit_type=*/true);

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> source_processed,
                   ProcessNode(original_expr));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> source_is_null,
                   fn_builder_.IsNull(std::move(source_processed)));
  std::unique_ptr<ResolvedExpr> souce_cast_as_string =
      MakeResolvedCast(types::StringType(), std::move(source_is_null),
                       /*return_null_on_error=*/false);

  ZETASQL_ASSIGN_OR_RETURN(
      auto resolved_if,
      fn_builder_.If(std::move(true_literal), std::move(typename_literal),
                     std::move(souce_cast_as_string)));
  PushNodeToStack(std::move(resolved_if));
  return absl::OkStatus();
}

}  // namespace

class TypeofFunctionRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.id_string_pool() != nullptr);
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    TypeofFunctionRewriteVisitor rewriter(options, &catalog, &type_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "TypeofFunctionRewriter"; }
};

const Rewriter* GetTypeofFunctionRewriter() {
  static const auto* const kRewriter = new TypeofFunctionRewriter;
  return kRewriter;
}

}  // namespace zetasql
