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

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/varsetter.h"
#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/cleanup/cleanup.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

using ArgNameToColumnMap =
    absl::flat_hash_map</*argument_name=*/absl::string_view,
                        std::unique_ptr<const ResolvedColumnRef>>;

// Helps copying a SQL function body.
//
// This rewriter replaces argument references with references to the columns
// that contain the argument values. Those columns are handled by the function
// inlining rewrite rules and provided in 'arg_map'. A subtlety of this task
// involves subqueries in the function body. The argument columns will be
// correlated in those subqueries and must be added to those subqueries
// parameter lists.
class ResolvedArgumentRefReplacer : public ResolvedASTDeepCopyVisitor {
 public:
  static absl::StatusOr<std::unique_ptr<ResolvedExpr>> ReplaceArgs(
      std::unique_ptr<const ResolvedExpr> fn_body,
      ArgNameToColumnMap& arg_map) {
    ResolvedArgumentRefReplacer arg_replacer(arg_map);
    ZETASQL_RETURN_IF_ERROR(fn_body->Accept(&arg_replacer));
    return arg_replacer.ConsumeRootNode<ResolvedExpr>();
  }

  explicit ResolvedArgumentRefReplacer(ArgNameToColumnMap& arg_map)
      : arg_map_(arg_map) {}

  absl::Status VisitResolvedArgumentRef(
      const ResolvedArgumentRef* node) override {
    // Function argument references will be ResolvedArgumentRef when a
    // function's body is resolved as part of the CREATE FUNCTION statement.
    return ReferenceArgumentColumn(node->name());
  }

  absl::Status VisitResolvedExpressionColumn(
      const ResolvedExpressionColumn* node) override {
    // Function argument references will be ResolvedExpressionColumn when a
    // function's body is resolved using AnalyzeExpressionForAssignmentToType.
    return ReferenceArgumentColumn(node->name());
  }

  absl::Status ReferenceArgumentColumn(absl::string_view arg_name) {
    const std::unique_ptr<const ResolvedColumnRef>* column =
        zetasql_base::FindOrNull(arg_map_, arg_name);
    if (is_in_with_entry_) {
      return absl::UnimplementedError(
          "SQL defined functions that reference arguments in an embedded WITH "
          "clause are not implemented.");
    }
    ZETASQL_RET_CHECK_NE(column, nullptr);
    const ResolvedColumnRef* arg_ref = column->get();
    ZETASQL_RET_CHECK_NE(arg_ref, nullptr);
    if (IsCopyingSubqueryInFunctionBody()) {
      args_referenced_in_subquery_.value().insert(arg_ref->column());
    }
    PushNodeToStack(MakeResolvedColumnRef(arg_ref->type(), arg_ref->column(),
                                          IsCopyingSubqueryInFunctionBody()));
    return absl::OkStatus();
  }

  bool IsCopyingSubqueryInFunctionBody() {
    return args_referenced_in_subquery_.has_value();
  }

  absl::Status VisitResolvedWithEntry(const ResolvedWithEntry* node) override {
    auto cleanup = zetasql_base::VarSetter(&is_in_with_entry_, true);
    return ResolvedASTDeepCopyVisitor::VisitResolvedWithEntry(node);
  }

  template <typename T>
  absl::Status CopySubqueryOrLambdaWithNewArgument(
      const T* node, std::function<absl::Status()> copy_visit) {
    absl::optional<ArgColumnSet> arg_columns_referenced = ArgColumnSet{};
    {
      // This cleanup implements a scoped swap. Its like zetasql_base::VarSetter but also
      // swaps the temporary object state back into the local variable so it may
      // be used like as an output variable too.
      absl::Cleanup cleanup = [this, &arg_columns_referenced]() {
        arg_columns_referenced.swap(args_referenced_in_subquery_);
      };
      arg_columns_referenced.swap(args_referenced_in_subquery_);
      ZETASQL_RETURN_IF_ERROR(copy_visit());
    }
    T* copy = GetUnownedTopOfStack<T>();
    for (auto& arg_column : arg_columns_referenced.value()) {
      copy->add_parameter_list(MakeResolvedColumnRef(
          arg_column.type(), arg_column, IsCopyingSubqueryInFunctionBody()));
      // If we are nested inside subqueries, then any arguments referenced in
      // this subquery are automatically referenced in the containing subquery.
      if (IsCopyingSubqueryInFunctionBody()) {
        args_referenced_in_subquery_.value().insert(arg_column);
      }
    }
    // Sort the parameter list because the analyzer tests are sensitive to this
    // order, and will otherwise be flaky. The absl::flat_hash_map that we use
    // to populate this list on line 116 above does not have a stable iteration
    // order.
    // TODO: Consider changing the debug string for this field so that
    //    it is not sensitive to the order items are added to the list.
    using Item = std::unique_ptr<const ResolvedColumnRef>;
    auto& param_list = const_cast<std::vector<Item>&>(copy->parameter_list());
    std::sort(
        param_list.begin(), param_list.end(),
        [](const Item& a, const Item& b) { return a->column() < b->column(); });
    return absl::OkStatus();
  }

  absl::Status VisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr* node) override {
    return CopySubqueryOrLambdaWithNewArgument(
        node, [this, node]() { return CopyVisitResolvedSubqueryExpr(node); });
  }

  absl::Status VisitResolvedInlineLambda(
      const ResolvedInlineLambda* node) override {
    return CopySubqueryOrLambdaWithNewArgument(
        node, [this, node]() { return CopyVisitResolvedInlineLambda(node); });
  }

 protected:
  // Function bodies may have multiple levels of subqueries inside them. Each
  // subquery in the inlined expression that references argument columns needs
  // to include the argument columns in its correlated parameter list.
  // 'args_referenced_in_subquery_' keeps track of which argument columns are
  // referenced in the current subquery being copied so that parameter lists
  // can be properly constructed.
  using ArgColumnSet = absl::flat_hash_set<ResolvedColumn>;
  absl::optional<ArgColumnSet> args_referenced_in_subquery_;

  // Track if copying is under a WITH entry (which must be a with on subquery).
  // Argument references in WITH scan are not supported.
  bool is_in_with_entry_ = false;

  // Function body expressions have references to function arguments that are
  // either ResolvedArgumentRef or ResolvedExpressionColumn depending on how
  // the function body was analyzed. The inlining process will replace those
  // argument references will column references, and the ArgNameToColumnMap is
  // used to track what column id replaces what argument name.
  ArgNameToColumnMap& arg_map_;
};

// Helper function that checks to see if a ResolvedFunctionCall is a call to a
// function that may be inlined. If the function call is inlininable, metadata
// that is useful to the inliner is populated in 'arg_names' and
// 'fn_expression'.
//
// 'arg_names' is the name of the arguments to this function call.
// 'fn_expression' is the ResolvedAST representation of the function body. These
//     nodes may not be owned by the SQL statement being rewritten. For example,
//     they may be owned by the catalog implementation.
static absl::StatusOr<bool> IsCallInlinableAndCollectInfo(
    const ResolvedFunctionCall* call, std::vector<std::string>& arg_names,
    const ResolvedExpr*& fn_expression) {
  const Function* function = call->function();
  ZETASQL_RET_CHECK(function != nullptr);
  if (function->Is<SQLFunctionInterface>()) {
    auto sql_fn = call->function()->GetAs<SQLFunctionInterface>();
    arg_names = sql_fn->GetArgumentNames();
    fn_expression = sql_fn->FunctionExpression();
  } else if (function->Is<TemplatedSQLFunction>()) {
    auto sql_fn = call->function()->GetAs<TemplatedSQLFunction>();
    auto fn_call_info =
        call->function_call_info()->GetAs<TemplatedSQLFunctionCall>();
    ZETASQL_RET_CHECK_NE(fn_call_info, nullptr);
    arg_names = sql_fn->GetArgumentNames();
    fn_expression = fn_call_info->expr();
  } else {
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
    std::vector<std::string> arg_names;
    const ResolvedExpr* fn_expression;
    ZETASQL_ASSIGN_OR_RETURN(bool is_inlinable, IsCallInlinableAndCollectInfo(
                                            node, arg_names, fn_expression));
    if (is_inlinable) {
      ZETASQL_RET_CHECK_NE(fn_expression, nullptr)
          << "No function expression supplied with resolved call to SQL "
          << "function " << node->DebugString();
      return InlineSqlFunction(node, arg_names, fn_expression);
    }
    return CopyVisitResolvedFunctionCall(node);
  }

  // This function replaces a ResolvedFunctionCall that invokes a SQL function
  // with an expression that computes the function result directly. The
  // transformation looks a bit like this (the syntax for ResolvedLetExpr is
  // imagined, there is no user syntax for LetExpr):
  //
  // MySqlFunction(arg0=>Expr0, arg1=>Expr1)
  // ~~>
  // LET (
  //   arg0 := Expr0,
  //   arg1 := Expr1
  // ) IN FunctionBodyExpr
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

    std::vector<std::unique_ptr<const ResolvedComputedColumn>> arg_exprs;
    ArgNameToColumnMap args = ArgNameToColumnMap{};
    for (int i = 0; i < call->argument_list_size(); ++i) {
      // Copy the reference expression.
      ZETASQL_RETURN_IF_ERROR(call->argument_list(i)->Accept(this));
      auto arg_expr = ConsumeTopOfStack<ResolvedExpr>();
      ResolvedColumn arg_column = column_factory_->MakeCol(
          absl::StrCat("$inlined_", call->function()->Name()),
          argument_names[i], arg_expr->type());
      args[argument_names[i]] =
          MakeResolvedColumnRef(arg_expr->type(), arg_column,
                                /*is_correlated=*/false);
      arg_exprs.push_back(
          MakeResolvedComputedColumn(arg_column, std::move(arg_expr)));
    }

    // Rewrite the function body so so that it references the columns in
    // arg_exprs rather than having ResolvedArgumnetRefs
    ZETASQL_ASSIGN_OR_RETURN(body_expr, ResolvedArgumentRefReplacer::ReplaceArgs(
                                    std::move(body_expr), args));

    PushNodeToStack(MakeResolvedLetExpr(call->type(), std::move(arg_exprs),
                                        std::move(body_expr)));
    return absl::OkStatus();
  }

  ColumnFactory* column_factory_;
};

class SqlFunctionInliner : public Rewriter {
 public:
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
