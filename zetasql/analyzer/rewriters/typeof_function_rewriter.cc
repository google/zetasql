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
#include <vector>

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
#include "zetasql/resolved_ast/resolved_ast_builder.h"
#include "zetasql/resolved_ast/resolved_ast_rewrite_visitor.h"
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
// accomplishes a couple goals:
// 1) Engines that enable the rewrite do not have to implement execution logic
//    for typeof.
// 2) Engines will see the original source expression in case it needs to track
//    object access for permission checks or expression sorts for supported-ness
//    checks.
class TypeofFunctionRewriteVisitor : public ResolvedASTRewriteVisitor {
 public:
  TypeofFunctionRewriteVisitor(const AnalyzerOptions& analyzer_options,
                               Catalog* catalog, TypeFactory* type_factory)
      : analyzer_options_(analyzer_options),
        fn_builder_(analyzer_options, *catalog),
        type_factory_(type_factory) {}

 private:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedFunctionCall(
      std::unique_ptr<const ResolvedFunctionCall> node) override;

  absl::StatusOr<std::unique_ptr<const ResolvedNode>> RewriteTypeof(
      std::unique_ptr<const ResolvedFunctionCall> node);

  const AnalyzerOptions& analyzer_options_;
  FunctionCallBuilder fn_builder_;
  TypeFactory* type_factory_;
};

absl::StatusOr<std::unique_ptr<const ResolvedNode>>
TypeofFunctionRewriteVisitor::PostVisitResolvedFunctionCall(
    std::unique_ptr<const ResolvedFunctionCall> node) {
  if (!IsBuiltInFunctionIdEq(node.get(), FN_TYPEOF)) {
    return node;
  }
  if (node->hint_list_size() > 0) {
    return ::zetasql_base::UnimplementedErrorBuilder()
           << "The TYPEOF() operator does not support hints.";
  }
  if (node->argument_list_size() != 1) {
    return ::zetasql_base::UnimplementedErrorBuilder()
           << "Unexpected number of input arguments to TYPEOF, expecting 1"
           << "but saw " << node->argument_list_size();
  }
  return RewriteTypeof(std::move(node));
}

absl::StatusOr<std::unique_ptr<const ResolvedNode>>
TypeofFunctionRewriteVisitor::RewriteTypeof(
    std::unique_ptr<const ResolvedFunctionCall> node) {
  // We only need the input arguments from the input node.
  std::vector<std::unique_ptr<const ResolvedExpr>> argument_list =
      ToBuilder(std::move(node)).release_argument_list();

  std::unique_ptr<const ResolvedExpr> original_expr =
      std::move(argument_list[0]);

  ZETASQL_RET_CHECK(original_expr != nullptr);

  std::unique_ptr<const ResolvedExpr> true_literal =
      MakeResolvedLiteral(type_factory_->get_bool(), Value::Bool(true),
                          /*has_explicit_type=*/true);
  std::unique_ptr<const ResolvedExpr> typename_literal =
      MakeResolvedLiteral(type_factory_->get_string(),
                          Value::String(original_expr->type()->TypeName(
                              analyzer_options_.language().product_mode())),
                          /*has_explicit_type=*/true);

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> source_is_null,
                   fn_builder_.IsNull(std::move(original_expr)));

  std::unique_ptr<const ResolvedExpr> source_cast_as_string =
      MakeResolvedCast(types::StringType(), std::move(source_is_null),
                       /*return_null_on_error=*/false);

  return fn_builder_.If(std::move(true_literal), std::move(typename_literal),
                        std::move(source_cast_as_string));
}

}  // namespace

class TypeofFunctionRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, std::unique_ptr<const ResolvedNode> input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.id_string_pool() != nullptr);
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    TypeofFunctionRewriteVisitor rewriter(options, &catalog, &type_factory);
    return rewriter.VisitAll(std::move(input));
  }

  std::string Name() const override { return "TypeofFunctionRewriter"; }
};

const Rewriter* GetTypeofFunctionRewriter() {
  static const auto* const kRewriter = new TypeofFunctionRewriter;
  return kRewriter;
}

}  // namespace zetasql
