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

#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

static absl::StatusOr<bool> IsCallInlinable(const ResolvedFunctionCall* call) {
  const Function* function = call->function();
  ZETASQL_RET_CHECK(function != nullptr);
  if (!function->Is<SQLFunctionInterface>()) {
    return false;
  }
  if (call->hint_list_size() > 0) {
    // Function inlining leaves no place to attach function call hints. It's not
    // clear that inlining a function call with hints is the right thing to do.
    return absl::UnimplementedError(
        absl::StrCat("Hinted calls to SQL defined function '", function->Name(),
                     "' are not supported."));
  }
  if (call->error_mode() == ResolvedFunctionCall::SAFE_ERROR_MODE) {
    // TODO: Implement support for ResolvedIfError or equivalent.
    return absl::UnimplementedError(
        absl::StrCat("SAFE. calls to SQL defined functions such as '",
                     function->Name(), "' are not implemented."));
  }
  return true;
}

// A visitor that replaces calls to SQL UDFs with the resolved function body.
class SqlFunctionInlineVistor : public ResolvedASTDeepCopyVisitor {
 public:
  explicit SqlFunctionInlineVistor(ColumnFactory* column_factory)
      : column_factory_(column_factory) {}

 private:
  absl::Status VisitResolvedFunctionCall(
      const ResolvedFunctionCall* node) override {
    ZETASQL_ASSIGN_OR_RETURN(bool is_inlinable, IsCallInlinable(node));
    if (is_inlinable) {
      const auto* sql_fn = node->function()->GetAs<SQLFunctionInterface>();
      std::vector<std::string> arg_names = sql_fn->GetArgumentNames();

      return InlineSqlFunction(node, arg_names, sql_fn->FunctionExpression());
    }
    return CopyVisitResolvedFunctionCall(node);
  }

  // This function replaces a ResolvedFunctionCall that invokes a SQL function
  // with an expression that computes the function result directly. The
  absl::Status InlineSqlFunction(const ResolvedFunctionCall* call,
                                 absl::Span<const std::string> argument_names,
                                 const ResolvedExpr* fn_expression) {
    ZETASQL_RET_CHECK_EQ(call->argument_list_size(), argument_names.size());
    ZETASQL_RET_CHECK_EQ(call->generic_argument_list_size(), 0);
    ZETASQL_RET_CHECK_NE(column_factory_, nullptr);

    // The input function body is potentially owned by a catalog or some other
    // component. Copy the body so that its column ids are compatible with the
    // invoking query and the expression is locally owned.
    ColumnReplacementMap column_map;
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> body_expr,
                     CopyResolvedASTAndRemapColumns(
                         *fn_expression, *column_factory_, column_map));

    // Nullary functions get special treatment because we don't have to do any
    // special argument processing.
    if (argument_names.empty()) {
      PushNodeToStack(std::move(body_expr));
      return absl::OkStatus();
    }

    return absl::UnimplementedError(
        "Calls to SQL defined functions with arguments are not implemented.");
  }

  ColumnFactory* column_factory_;
};

class SqlFunctionInliner : public Rewriter {
 public:
  bool ShouldRewrite(const AnalyzerOptions& analyzer_options,
                     const AnalyzerOutput& analyzer_output) const override {
    return analyzer_output.analyzer_output_properties().has_sql_function_call;
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());

    SqlFunctionInlineVistor rewriter(&column_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "SqlFunctionInliner"; }
};

}  // namespace

const Rewriter* GetSqlFunctionInliner() {
  static const auto* const kRewriter = new SqlFunctionInliner;
  return kRewriter;
}

}  // namespace zetasql
