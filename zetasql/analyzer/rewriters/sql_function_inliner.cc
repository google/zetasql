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

#include "zetasql/analyzer/rewriters/sql_function_inliner.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/varsetter.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/rewriter_interface.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/sql_tvf.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/templated_sql_tvf.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_builder.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_ast_rewrite_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

using ArgRefBuilder =
    std::function<absl::StatusOr<std::unique_ptr<const ResolvedExpr>>(bool)>;
using ArgNameToExprMap =
    absl::flat_hash_map</*argument_name=*/absl::string_view, ArgRefBuilder>;

using ArgScanBuilder =
    std::function<absl::StatusOr<std::unique_ptr<const ResolvedScan>>(
        const ResolvedScan* arg_scan)>;
using ArgNameToScanMap =
    absl::flat_hash_map</*argument_name=*/absl::string_view, ArgScanBuilder>;

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
  template <class T>
  static absl::StatusOr<std::unique_ptr<T>> ReplaceArgs(
      std::unique_ptr<T> fn_body, ArgNameToExprMap& scalar_arg_map,
      ArgNameToScanMap& table_arg_map) {
    ResolvedArgumentRefReplacer arg_replacer(scalar_arg_map, table_arg_map);
    ZETASQL_RETURN_IF_ERROR(fn_body->Accept(&arg_replacer));
    return arg_replacer.ConsumeRootNode<T>();
  }

  ResolvedArgumentRefReplacer(ArgNameToExprMap& scalar_arg_map,
                              ArgNameToScanMap& table_arg_map)
      : scalar_arg_map_(scalar_arg_map), table_arg_map_(table_arg_map) {}

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
    ArgRefBuilder* ref_builder = zetasql_base::FindOrNull(scalar_arg_map_, arg_name);
    ZETASQL_RET_CHECK_NE(ref_builder, nullptr) << arg_name;
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> arg_ref,
                     (*ref_builder)(IsCopyingSubqueryInFunctionBody()));
    if (is_in_with_entry_) {
      return absl::UnimplementedError(
          "SQL defined functions that contain argument references inside "
          "embedded WITH clauses are not implemented.");
    }
    if (arg_ref->Is<ResolvedColumnRef>() && IsCopyingSubqueryInFunctionBody()) {
      const ResolvedColumnRef* column_ref = arg_ref->GetAs<ResolvedColumnRef>();
      ZETASQL_RET_CHECK_NE(column_ref, nullptr);
      args_referenced_in_subquery_.value().insert(column_ref->column());
    }
    PushNodeToStack(
        absl::WrapUnique(const_cast<ResolvedExpr*>(arg_ref.release())));
    return absl::OkStatus();
  }

  bool IsCopyingSubqueryInFunctionBody() {
    return args_referenced_in_subquery_.has_value();
  }

  absl::Status VisitResolvedWithEntry(const ResolvedWithEntry* node) override {
    auto cleanup = zetasql_base::VarSetter(&is_in_with_entry_, true);
    return ResolvedASTDeepCopyVisitor::VisitResolvedWithEntry(node);
  }

  absl::Status VisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr* node) override {
    std::optional<ArgColumnSet> arg_columns_referenced = ArgColumnSet{};
    std::unique_ptr<const ResolvedScan> subquery_scan;
    {
      // This cleanup implements a scoped swap. Its like zetasql_base::VarSetter but also
      // swaps the temporary object state back into the local variable so it may
      // be used like as an output variable too.
      absl::Cleanup cleanup = [this, &arg_columns_referenced]() {
        arg_columns_referenced.swap(args_referenced_in_subquery_);
      };
      arg_columns_referenced.swap(args_referenced_in_subquery_);
      ZETASQL_ASSIGN_OR_RETURN(subquery_scan, ProcessNode(node->subquery()));
    }
    ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedSubqueryExpr(node));
    ResolvedSubqueryExprBuilder subquery_builder =
        ToBuilder(ConsumeTopOfStack<ResolvedSubqueryExpr>())
            .set_subquery(std::move(subquery_scan));
    for (auto& arg_column : arg_columns_referenced.value()) {
      subquery_builder.add_parameter_list(MakeResolvedColumnRef(
          arg_column.type(), arg_column, IsCopyingSubqueryInFunctionBody()));
      // If we are nested inside subqueries, then any arguments referenced in
      // this subquery are automatically referenced in the containing subquery.
      if (IsCopyingSubqueryInFunctionBody()) {
        args_referenced_in_subquery_.value().insert(arg_column);
      }
    }
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedSubqueryExpr> copy,
                     std::move(subquery_builder).Build());
    PushNodeToStack(
        absl::WrapUnique(const_cast<ResolvedSubqueryExpr*>(copy.release())));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedInlineLambda(
      const ResolvedInlineLambda* node) override {
    std::optional<ArgColumnSet> arg_columns_referenced = ArgColumnSet{};
    {
      // This cleanup implements a scoped swap. Its like zetasql_base::VarSetter but also
      // swaps the temporary object state back into the local variable so it may
      // be used like as an output variable too.
      absl::Cleanup cleanup = [this, &arg_columns_referenced]() {
        arg_columns_referenced.swap(args_referenced_in_subquery_);
      };
      arg_columns_referenced.swap(args_referenced_in_subquery_);
      ZETASQL_RETURN_IF_ERROR(CopyVisitResolvedInlineLambda(node));
    }
    ResolvedInlineLambda* copy = GetUnownedTopOfStack<ResolvedInlineLambda>();
    for (auto& arg_column : arg_columns_referenced.value()) {
      copy->add_parameter_list(MakeResolvedColumnRef(
          arg_column.type(), arg_column, IsCopyingSubqueryInFunctionBody()));
      // If we are nested inside subqueries, then any arguments referenced in
      // this subquery are automatically referenced in the containing subquery.
      if (IsCopyingSubqueryInFunctionBody()) {
        args_referenced_in_subquery_.value().insert(arg_column);
      }
    }
    return absl::OkStatus();
  }

  absl::Status VisitResolvedRelationArgumentScan(
      const ResolvedRelationArgumentScan* node) override {
    absl::string_view arg_name = node->name();
    ArgScanBuilder* scan_builder = zetasql_base::FindOrNull(table_arg_map_, arg_name);
    ZETASQL_RET_CHECK_NE(scan_builder, nullptr);
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedScan> arg_scan,
                     (*scan_builder)(node));
    PushNodeToStack(
        absl::WrapUnique(const_cast<ResolvedScan*>(arg_scan.release())));
    return absl::OkStatus();
  }

 protected:
  // Function bodies may have multiple levels of subqueries inside them. Each
  // subquery in the inlined expression that references argument columns needs
  // to include the argument columns in its correlated parameter list.
  // 'args_referenced_in_subquery_' keeps track of which argument columns are
  // referenced in the current subquery being copied so that parameter lists
  // can be properly constructed. We use a btree set here since it determines an
  // order for the eventual parameter list vector.
  using ArgColumnSet = absl::btree_set<ResolvedColumn>;
  std::optional<ArgColumnSet> args_referenced_in_subquery_;

  // Track if copying is under a WITH entry (which must be a with on subquery).
  // Argument references in WITH scan are not supported.
  bool is_in_with_entry_ = false;

  // Function body expressions have references to function arguments that are
  // either ResolvedArgumentRef or ResolvedExpressionColumn depending on how
  // the function body was analyzed. The inlining process will replace those
  // argument references will column references, and the ArgNameToExprMap is
  // used to track what column id replaces what argument name.
  ArgNameToExprMap& scalar_arg_map_;

  // Like 'scalar_arg_map_' but pertaining to TVF table arguments.
  ArgNameToScanMap& table_arg_map_;
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
  return true;
}

// A visitor that replaces calls to SQL UDFs with the resolved function body.
class SqlFunctionInlineVistor : public ResolvedASTDeepCopyVisitor {
 public:
  SqlFunctionInlineVistor(const AnalyzerOptions& analyzer_options,
                          Catalog& catalog, ColumnFactory* column_factory,
                          TypeFactory& type_factory)
      : column_factory_(column_factory),
        fn_builder_(analyzer_options, catalog, type_factory) {}

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
  // transformation looks a bit like this:
  //
  // MySqlFunction(arg0=>Expr0, arg1=>Expr1)
  // ~~>
  // WITH (
  //   arg0 AS Expr0,
  //   arg1 AS Expr1,
  //   FunctionBodyExpr
  // )
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

    if (call->error_mode() == ResolvedFunctionCall::SAFE_ERROR_MODE) {
      ZETASQL_RETURN_IF_ERROR(
          fn_builder_.CheckCatalogSupportsSafeMode(call->function()->Name()));
      Value null_value = Value::Null(body_expr->type());
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> iferror_call,
                       fn_builder_.IfError(std::move(body_expr),
                                           MakeResolvedLiteral(null_value)));
      body_expr =
          absl::WrapUnique(const_cast<ResolvedExpr*>(iferror_call.release()));
    }

    // Nullary functions get special treatment because we don't have to do any
    // special argument processing.
    if (argument_names.empty()) {
      PushNodeToStack(std::move(body_expr));
      return absl::OkStatus();
    }

    std::vector<std::unique_ptr<const ResolvedComputedColumn>> arg_exprs;
    ArgNameToExprMap args = ArgNameToExprMap{};
    for (int i = 0; i < call->argument_list_size(); ++i) {
      // Copy the reference expression.
      ZETASQL_RETURN_IF_ERROR(call->argument_list(i)->Accept(this));
      auto arg_expr = ConsumeTopOfStack<ResolvedExpr>();
      ResolvedColumn arg_column = column_factory_->MakeCol(
          absl::StrCat("$inlined_", call->function()->Name()),
          argument_names[i], arg_expr->annotated_type());
      args[argument_names[i]] = [type = arg_expr->type(),
                                 arg_column](bool is_correlated) {
        return MakeResolvedColumnRef(type, arg_column, is_correlated);
      };
      arg_exprs.push_back(
          MakeResolvedComputedColumn(arg_column, std::move(arg_expr)));
    }

    // Rewrite the function body so so that it references the columns in
    // arg_exprs rather than having ResolvedArgumentRefs
    ArgNameToScanMap table_args;
    ZETASQL_ASSIGN_OR_RETURN(body_expr, ResolvedArgumentRefReplacer::ReplaceArgs(
                                    std::move(body_expr), args, table_args));

    PushNodeToStack(MakeResolvedWithExpr(call->type(), std::move(arg_exprs),
                                         std::move(body_expr)));
    return absl::OkStatus();
  }

  ColumnFactory* column_factory_;
  FunctionCallBuilder fn_builder_;
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

    SqlFunctionInlineVistor rewriter(options, catalog, &column_factory,
                                     type_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "SqlFunctionInliner"; }
};

// A visitor that replaces calls to SQL TVFs with the resolved function body.
class SqlTableFunctionInlineVistor : public ResolvedASTDeepCopyVisitor {
 public:
  explicit SqlTableFunctionInlineVistor(ColumnFactory* column_factory)
      : column_factory_(column_factory) {}

 private:
  absl::StatusOr<bool> IsCallInlinable(const ResolvedTVFScan* scan) {
    if (scan->hint_list_size() > 0) {
      // Function inlining leaves no place to hang function call hints. It's not
      // clear that inlining a function call with hints is even the right thing
      // to do.
      return false;
    }
    const TableValuedFunction* function = scan->tvf();
    ZETASQL_RET_CHECK_NE(function, nullptr)
        << "Expected ResolvedTableFunctionScan to have non-null function";
    return function->Is<SQLTableValuedFunction>() ||
           function->Is<TemplatedSQLTVF>();
  }

  absl::Status VisitResolvedTVFScan(const ResolvedTVFScan* tvf_scan) override {
    ZETASQL_ASSIGN_OR_RETURN(bool inlinable, IsCallInlinable(tvf_scan));
    if (inlinable) {
      return InlineTVF(tvf_scan);
    }
    return CopyVisitResolvedTVFScan(tvf_scan);
  }

  absl::Status ErrorIfArgumentIsCorrelated(const ResolvedNode& arg,
                                           int64_t arg_number,
                                           absl::string_view arg_name) {
    std::vector<std::unique_ptr<const ResolvedColumnRef>> free_vars;
    ZETASQL_RETURN_IF_ERROR(CollectColumnRefs(arg, &free_vars));
    if (!free_vars.empty()) {
      return absl::UnimplementedError(absl::StrCat(
          "TVF arguments that reference columns are not supported. ", "Arg #",
          arg_number, " ('", arg_name, "') references column '",
          free_vars[0]->column().name(), "'."));
    }
    return absl::OkStatus();
  }

  // This function replaces a ResolvedTVFScan that invokes a SQL table function
  // with a query that computes the function result directly. The
  // transformation looks a bit like this:
  //
  // SELECT ... FROM MyTvf() AS t;
  // ~~>
  // (SELECT ... FROM (tvf_query) AS t
  absl::Status InlineTVF(const ResolvedTVFScan* scan) {
    ZETASQL_RET_CHECK_NE(scan, nullptr);
    ZETASQL_RET_CHECK_NE(column_factory_, nullptr);
    const ResolvedScan* query = nullptr;
    std::vector<std::string> argument_names;
    if (scan->tvf()->Is<SQLTableValuedFunction>()) {
      const auto* sql_tvf = scan->tvf()->GetAs<SQLTableValuedFunction>();
      ZETASQL_RET_CHECK_NE(sql_tvf, nullptr);
      query = sql_tvf->query();
      ZETASQL_RET_CHECK_NE(query, nullptr);
      argument_names = sql_tvf->GetArgumentNames();
    } else if (scan->tvf()->Is<TemplatedSQLTVF>()) {
      const auto* sql_tvf = scan->tvf()->GetAs<TemplatedSQLTVF>();
      ZETASQL_RET_CHECK_NE(sql_tvf, nullptr);
      query = scan->signature()
                  ->GetAs<TemplatedSQLTVFSignature>()
                  ->resolved_templated_query()
                  ->query();
      argument_names = sql_tvf->GetArgumentNames();
    } else {
      return absl::InternalError(
          "Inlining only supports SQL TVFs and TemplateTVFs.");
    }

    // The input function body is potentially owned by a catalog or some other
    // component. Copy the body so that its column ids are compatible with the
    // invoking query and the scan is locally owned.
    ColumnReplacementMap column_map;
    for (int i = 0; i < scan->column_list_size(); ++i) {
      column_map.insert({query->column_list()[scan->column_index_list()[i]],
                         scan->column_list()[i]});
    }

    std::unique_ptr<ResolvedScan> body_scan;
    if (scan->tvf()->sql_security() ==
        ResolvedCreateStatementEnums::SQL_SECURITY_DEFINER) {
      ZETASQL_ASSIGN_OR_RETURN(
          body_scan,
          ReplaceScanColumns(
              *column_factory_, *query, scan->column_index_list(),
              CreateReplacementColumns(*column_factory_, scan->column_list())));
      body_scan = MakeResolvedExecuteAsRoleScan(
          scan->column_list(), std::move(body_scan),
          /*original_inlined_view=*/nullptr, scan->tvf());
    } else {
      // TODO We should decide what to do in the case of
      // UNSPECIFIED, to be consistent with VIEWs and the desired behavior.
      ZETASQL_ASSIGN_OR_RETURN(body_scan, ReplaceScanColumns(*column_factory_, *query,
                                                     scan->column_index_list(),
                                                     scan->column_list()));
    }

    // Nullary functions get special treatment because we don't have to do any
    // special argument processing.
    if (scan->argument_list_size() == 0) {
      PushNodeToStack(std::move(body_scan));
      return absl::OkStatus();
    }

    ZETASQL_RET_CHECK_EQ(argument_names.size(), scan->argument_list_size());

    // The inlined TVF will become a subquery that contains one CTE query per
    // table argument and one CTE query that computes all scalar arguments with
    // as-if-once semantics.
    std::vector<std::unique_ptr<const ResolvedWithEntry>> with_entry_list;

    // Copy the argument expressions with some extra bookkeeping to build
    // required information for copying the function body expression.
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> scalar_arg_exprs;
    std::vector<ResolvedColumn> arg_columns;
    ArgNameToExprMap scalar_args = ArgNameToExprMap{};
    std::string scalars_cte_name =
        absl::StrCat("$inlined_", scan->tvf()->Name(), "_scalar_args");
    ArgNameToScanMap table_args = ArgNameToScanMap{};
    for (int i = 0; i < scan->argument_list_size(); ++i) {
      const ResolvedFunctionArgument* arg = scan->argument_list(i);
      const std::string& arg_name = argument_names[i];
      if (scan->argument_list(i)->scan() != nullptr) {
        ZETASQL_ASSIGN_OR_RETURN(auto arg_scan, ProcessNode<ResolvedScan>(arg->scan()));
        ZETASQL_RET_CHECK_GE(scan->argument_list_size(), 1);
        ZETASQL_RETURN_IF_ERROR(
            ErrorIfArgumentIsCorrelated(*arg_scan, i + 1, arg_name));
        const std::string& arg_cte_name = argument_names[i];
        with_entry_list.emplace_back(
            MakeResolvedWithEntry(arg_cte_name, std::move(arg_scan)));
        table_args[argument_names[i]] =
            [arg_cte_name](const ResolvedScan* arg_scan)
            -> absl::StatusOr<std::unique_ptr<const ResolvedScan>> {
          auto with_ref =
              ResolvedWithRefScanBuilder().set_with_query_name(arg_cte_name);
          for (ResolvedColumn col : arg_scan->column_list()) {
            with_ref.add_column_list(col);
          }
          return std::move(with_ref).Build();
        };
        continue;
      }
      const ResolvedExpr* argument = scan->argument_list(i)->expr();
      if (argument == nullptr) {
        return absl::UnimplementedError(
            absl::StrCat("TVF argument #", i + 1, " ('", arg_name,
                         "') is not an argument kind supported by inlining."));
      }
      ZETASQL_RET_CHECK_NE(argument, nullptr);
      ZETASQL_RETURN_IF_ERROR(ErrorIfArgumentIsCorrelated(*argument, i + 1, arg_name));
      ZETASQL_RETURN_IF_ERROR(argument->Accept(this));
      auto arg_expr = ConsumeTopOfStack<ResolvedExpr>();
      scalar_args[argument_names[i]] =
          [scan, &arg_columns, projected_col_index = arg_columns.size(),
           scalars_cte_name, this](bool is_correlated)
          -> absl::StatusOr<std::unique_ptr<const ResolvedExpr>> {
        ZETASQL_RET_CHECK_LT(projected_col_index, arg_columns.size());
        std::string scan_name = absl::StrCat("$inlined_", scan->tvf()->Name());
        auto with_ref =
            ResolvedWithRefScanBuilder().set_with_query_name(scalars_cte_name);
        ResolvedProjectScanBuilder project;
        ResolvedSubqueryExprBuilder subquery;
        for (int i = 0; i < arg_columns.size(); ++i) {
          ResolvedColumn col = column_factory_->MakeCol(
              scan_name, arg_columns[i].name(), arg_columns[i].type());
          with_ref.add_column_list(col);
          if (i == projected_col_index) {
            project.add_column_list(col);
            subquery.set_type(col.type());
          }
        }

        return std::move(subquery)
            .set_subquery_type(ResolvedSubqueryExpr::SCALAR)
            .set_in_expr(nullptr)
            .set_subquery(
                std::move(project).set_input_scan(std::move(with_ref)))
            .Build();
      };
      ResolvedColumn arg_column = column_factory_->MakeCol(
          absl::StrCat("$inlined_", scan->tvf()->Name()), arg_name,
          arg_expr->annotated_type());
      scalar_arg_exprs.push_back(
          MakeResolvedComputedColumn(arg_column, std::move(arg_expr)));
      arg_columns.push_back(arg_column);
    }
    if (!scalar_arg_exprs.empty()) {
      with_entry_list.emplace_back(MakeResolvedWithEntry(
          scalars_cte_name,
          MakeResolvedProjectScan(arg_columns, std::move(scalar_arg_exprs),
                                  MakeResolvedSingleRowScan())));
    }

    // Rewrite the function body so so that it references the columns in
    // scalar_arg_exprs rather than having ResolvedArgumentRefs
    ZETASQL_ASSIGN_OR_RETURN(body_scan,
                     ResolvedArgumentRefReplacer::ReplaceArgs(
                         std::move(body_scan), scalar_args, table_args));

    ZETASQL_RET_CHECK(!with_entry_list.empty());
    // This variable prevents use-after move ambiguity in the following stmt.
    const std::vector<ResolvedColumn>& columns = body_scan->column_list();
    PushNodeToStack(MakeResolvedWithScan(columns, std::move(with_entry_list),
                                         std::move(body_scan),
                                         /*recursive=*/false));
    return absl::OkStatus();
  }

 private:
  ColumnFactory* column_factory_;
};

class SqlTvfInliner : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());
    SqlTableFunctionInlineVistor rewriter(&column_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    return rewriter.ConsumeRootNode<ResolvedNode>();
  }

  std::string Name() const override { return "SqlTvfInliner"; }
};

class SqlAggregateFunctionInlineVisitor : public ResolvedASTRewriteVisitor {
 public:
  explicit SqlAggregateFunctionInlineVisitor(ColumnFactory& column_factory)
      : column_factory_(column_factory) {}

 private:
  // The data-structures representing concrete and template SQL functions are
  // different. This struct lets us gather the common bits so that inlining
  // logic can be more agnostic to whether the function was concrete or a
  // template.
  struct AggregateFnDetails {
    const ResolvedAggregateFunctionCall* call;
    const ResolvedExpr* expr;
    const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
        aggregate_expression_list;
    std::vector<std::string> arg_names;
    ResolvedColumn computed_column;
  };

  // Check to see if the function is a SQL-defined aggregate and return details
  // required for inlining if so. Otherwise return std::nullopt to indicate the
  // function is not SQL-defined. An error will be returned in case the function
  // is SQL-defined but has a shape not supported by the inliner.
  absl::StatusOr<std::optional<AggregateFnDetails>> IsInlineable(
      const ResolvedAggregateFunctionCall* call) {
    const Function* function = call->function();
    if (!function->Is<SQLFunctionInterface>() &&
        !function->Is<TemplatedSQLFunction>()) {
      return std::nullopt;
    }
    const ParseLocationRange* error_location =
        call->GetParseLocationRangeOrNULL();
    if (call->error_mode() == ResolvedFunctionCall::SAFE_ERROR_MODE) {
      // TODO: Support SAFE mode calls using IFERROR.
      return MakeSqlErrorAtStart(error_location)
             << "SAFE mode calls to aggregate function " << function->SQLName()
             << " are not supported";
    }
    if (call->distinct()) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "DISTINCT is not supported on calls to aggregate function "
             << function->SQLName();
    }
    if (call->limit() != nullptr) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "LIMIT is not supported on calls to aggregate function "
             << function->SQLName();
    }
    if (call->order_by_item_list_size() > 0) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "ORDER BY is not supported on calls to aggregate function "
             << function->SQLName();
    }
    if (call->having_modifier() != nullptr) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "HAVING is not supported on calls to aggregate function "
             << function->SQLName();
    }
    if (call->null_handling_modifier() ==
        ResolvedNonScalarFunctionCallBase::RESPECT_NULLS) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "RESPECT NULLS is not supported on calls to aggregate function "
             << function->SQLName();
    }
    if (call->null_handling_modifier() ==
        ResolvedNonScalarFunctionCallBase::IGNORE_NULLS) {
      // TODO: Decide semantics for this clause before inlining it.
      return MakeSqlErrorAtStart(error_location)
             << "IGNORE NULLS is not supported on calls to aggregate function "
             << function->SQLName();
    }
    if (function->Is<SQLFunctionInterface>()) {
      auto* fn = function->GetAs<SQLFunctionInterface>();
      std::vector<FunctionArgumentType> agg_args;
      std::vector<FunctionArgumentType> non_agg_args;
      for (const auto& arg : call->signature().arguments()) {
        if (arg.options().is_not_aggregate()) {
          non_agg_args.push_back(arg);
        } else {
          agg_args.push_back(arg);
        }
      }
      return AggregateFnDetails{
          .call = call,
          .expr = fn->FunctionExpression(),
          .aggregate_expression_list = *fn->aggregate_expression_list(),
          .arg_names = fn->GetArgumentNames(),
      };
    }
    if (function->Is<TemplatedSQLFunction>()) {
      auto* fn = function->GetAs<TemplatedSQLFunction>();
      auto fn_call_info =
          call->function_call_info()->GetAs<TemplatedSQLFunctionCall>();
      ZETASQL_RET_CHECK(fn_call_info != nullptr);
      return AggregateFnDetails{
          .call = call,
          .expr = fn_call_info->expr(),
          .aggregate_expression_list =
              fn_call_info->aggregate_expression_list(),
          .arg_names = fn->GetArgumentNames(),
      };
    }
    ZETASQL_RET_CHECK_FAIL() << "Return should be unreachable.";
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedAggregateScan(
      std::unique_ptr<const ResolvedAggregateScan> node) override {
    absl::flat_hash_map<const ResolvedAggregateFunctionCall*,
                        AggregateFnDetails>
        calls_to_inline;
    for (const auto& column : node->aggregate_list()) {
      const auto* col = column->GetAs<ResolvedComputedColumnImpl>();
      ZETASQL_RET_CHECK(col->expr()->Is<ResolvedAggregateFunctionCall>());
      const ResolvedAggregateFunctionCall* aggr_function_call =
          col->expr()->GetAs<ResolvedAggregateFunctionCall>();
      ZETASQL_ASSIGN_OR_RETURN(std::optional<AggregateFnDetails> details,
                       IsInlineable(aggr_function_call));
      if (details.has_value()) {
        calls_to_inline.emplace(aggr_function_call, *details);
      }
    }
    if (calls_to_inline.empty()) {
      return node;
    }
    ResolvedAggregateScanBuilder aggr_builder = ToBuilder(std::move(node));
    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
        old_aggregates = aggr_builder.release_aggregate_list();

    // The aggregations included in the aggregate scan post-rewrite.
    std::vector<std::unique_ptr<const ResolvedComputedColumnBase>>
        new_aggregates;
    // The column list produced by thew new aggregate scan post-rewrite.
    std::vector<ResolvedColumn> new_aggr_col_list;

    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        pre_aggregate_exprs;
    std::vector<ResolvedColumn> pre_aggregate_cols =
        aggr_builder.input_scan()->column_list();

    // Expressions computed by a new project scan after aggregation.
    std::vector<std::unique_ptr<const ResolvedComputedColumn>>
        post_aggregate_exprs;
    // Columns that were part of the aggregate scan pre-rewrite that are not
    // included in the aggregate scan column list post-rewrite.
    absl::flat_hash_set<ResolvedColumn> columns_to_remove_from_aggr;

    // The new aggregate scan after inlinine will no longer have calls to the
    // SQL-defined aggregate functions. This section is building the new
    // list of aggregates for the new aggregate scan, as well as collecting
    // expressions for the post-aggregate project scan that will host the
    // expression from the SQL-defined aggregate that modifies or combines the
    // results of any aggregations called internally.
    for (auto& aggr_column : old_aggregates) {
      ZETASQL_RET_CHECK(aggr_column->Is<ResolvedComputedColumn>());
      auto aggr = aggr_column->GetAs<ResolvedComputedColumn>();
      ZETASQL_RET_CHECK(aggr->expr()->Is<ResolvedAggregateFunctionCall>());
      const ResolvedAggregateFunctionCall* aggr_function_call =
          aggr->expr()->GetAs<ResolvedAggregateFunctionCall>();

      if (!calls_to_inline.contains(aggr_function_call)) {
        new_aggregates.emplace_back(std::move(aggr_column));
        continue;
      }
      AggregateFnDetails& details = calls_to_inline.at(aggr_function_call);
      details.computed_column = aggr->column();
      columns_to_remove_from_aggr.insert(aggr->column());
      std::string function_name = aggr_function_call->function()->Name();

      ResolvedAggregateFunctionCallBuilder aggr_expr_builder = ToBuilder(
          absl::WrapUnique(const_cast<ResolvedComputedColumn*>(
                               aggr->GetAs<ResolvedComputedColumn>())
                               ->release_expr()
                               .release()
                               ->GetAs<ResolvedAggregateFunctionCall>()));
      auto aggr_args = aggr_expr_builder.release_argument_list();
      ArgNameToExprMap aggregate_args;
      ArgNameToExprMap non_aggregate_args;
      FunctionSignature signature = aggr_expr_builder.signature();

      // This logic assumes no repeated args.
      ZETASQL_RET_CHECK_EQ(details.arg_names.size(), aggr_args.size());
      for (int i = 0; i < aggr_args.size(); ++i) {
        bool is_non_aggregate_arg =
            signature.arguments()[i].options().is_not_aggregate();
        std::unique_ptr<const ResolvedExpr>& arg = aggr_args[i];
        if (is_non_aggregate_arg) {
          // If we ever extend non-aggregate args beyond these types, the
          // rewriter will need to change to accommodate as-if-evaluated-once
          // semantics. The ResolvedAST is not expressive enough for that right
          // now without introducing an artificial array construction above the
          // aggregation which some query optimizers would not remove. The
          // expressive power that is needed is a lateral join with a single row
          // table on the LHS.
          const ResolvedExpr* without_cast = arg.get();
          // LINT.IfChange(non_aggregate_args_def)
          while (without_cast->node_kind() == RESOLVED_CAST) {
            without_cast = without_cast->GetAs<ResolvedCast>()->expr();
          }
          ResolvedNodeKind expr_kind = without_cast->node_kind();
          ZETASQL_RET_CHECK(expr_kind == RESOLVED_LITERAL ||
                    expr_kind == RESOLVED_PARAMETER ||
                    expr_kind == RESOLVED_ARGUMENT_REF);
          // LINT.ThenChange(../expr_resolver_helper.cc:non_aggregate_args_def)
          auto arg_replacement_builder = [&arg, this](bool is_correlated) {
            // Making a copy like this is only safe because the expressions
            // that are allowed as non-aggregate args are immutable and
            // trivial to evaluate.
            ColumnReplacementMap no_replacements;
            return CopyResolvedASTAndRemapColumns(*arg, this->column_factory_,
                                                  no_replacements);
          };
          // this collection is used exclusively by the post-aggregate
          // expression.
          non_aggregate_args.emplace(details.arg_names[i],
                                     arg_replacement_builder);
          // this collection is used for the arguments to the aggregate
          // functions. non-aggregate args can be used there too, so we add
          // these args to both collections.
          aggregate_args.emplace(details.arg_names[i], arg_replacement_builder);
        } else {
          // This is an aggregate arg.
          ResolvedColumn new_arg_column = column_factory_.MakeCol(
              absl::StrCat("$inlined_", function_name), details.arg_names[i],
              arg->annotated_type());
          ZETASQL_ASSIGN_OR_RETURN(auto new_arg_computed_col,
                           ResolvedComputedColumnBuilder()
                               .set_column(new_arg_column)
                               .set_expr(std::move(arg))
                               .Build());
          pre_aggregate_exprs.push_back(std::move(new_arg_computed_col));
          pre_aggregate_cols.push_back(new_arg_column);
          aggregate_args.emplace(
              details.arg_names[i], [new_arg_column](bool is_correlated) {
                return MakeResolvedColumnRef(new_arg_column.type(),
                                             new_arg_column, is_correlated);
              });
        }
      }

      // SQL-defined aggregates have any aggregations internal to the function
      // body already factored out. Those aggregations will be promoted into the
      // new copy of the AggregateScan. Those are processed in this loop,
      // collecting the processed aggregations in `new_aggregates` and also the
      // columns they are written into in `new_aggr_col_list`. These lists will
      // later be used to build the new AggregateScan.

      // Aggregates that are internal to the function body, once copied, have
      // new column id. This map is used to replace references to those column
      // ids in the post-aggregate expression.
      ColumnReplacementMap internal_aggregate_remapping;
      ColumnReplacementMap no_replacements;
      ArgNameToScanMap no_table_args;
      for (auto& aggr_computed_col : details.aggregate_expression_list) {
        ZETASQL_ASSIGN_OR_RETURN(
            auto new_aggr_computed_col,
            CopyResolvedASTAndRemapColumns(*aggr_computed_col, column_factory_,
                                           no_replacements));
        ZETASQL_ASSIGN_OR_RETURN(new_aggr_computed_col,
                         ResolvedArgumentRefReplacer::ReplaceArgs(
                             std::move(new_aggr_computed_col), aggregate_args,
                             no_table_args));
        internal_aggregate_remapping.emplace(aggr_computed_col->column(),
                                             new_aggr_computed_col->column());
        new_aggr_col_list.push_back(new_aggr_computed_col->column());
        new_aggregates.push_back(std::move(new_aggr_computed_col));
      }
      ZETASQL_ASSIGN_OR_RETURN(
          auto post_aggregate_function_body,
          CopyResolvedASTAndRemapColumns(*details.expr, column_factory_,
                                         internal_aggregate_remapping));
      ZETASQL_ASSIGN_OR_RETURN(auto post_aggregate_expr,
                       ResolvedArgumentRefReplacer::ReplaceArgs(
                           std::move(post_aggregate_function_body),
                           non_aggregate_args, no_table_args));
      ZETASQL_ASSIGN_OR_RETURN(auto post_aggregate_computed_col,
                       ResolvedComputedColumnBuilder()
                           .set_column(details.computed_column)
                           .set_expr(std::move(post_aggregate_expr))
                           .Build());
      post_aggregate_exprs.emplace_back(std::move(post_aggregate_computed_col));
    }

    // The post-aggregation Project will have the same column list as the input
    // aggregate scan. The new aggregate scan will not have columns associated
    // with re-written function calls.
    std::vector<ResolvedColumn> post_aggregate_column_list =
        aggr_builder.column_list();
    for (const auto& old_aggr_col : aggr_builder.column_list()) {
      if (!columns_to_remove_from_aggr.contains(old_aggr_col)) {
        new_aggr_col_list.push_back(old_aggr_col);
      }
    }
    auto aggr_input = aggr_builder.release_input_scan();
    if (!pre_aggregate_exprs.empty()) {
      ZETASQL_ASSIGN_OR_RETURN(aggr_input,
                       ResolvedProjectScanBuilder()
                           .set_input_scan(std::move(aggr_input))
                           .set_expr_list(std::move(pre_aggregate_exprs))
                           .set_column_list(std::move(pre_aggregate_cols))
                           .Build());
    }
    return ResolvedProjectScanBuilder()
        .set_input_scan(std::move(aggr_builder)
                            .set_input_scan(std::move(aggr_input))
                            .set_aggregate_list(std::move(new_aggregates))
                            .set_column_list(std::move(new_aggr_col_list)))
        .set_expr_list(std::move(post_aggregate_exprs))
        .set_column_list(std::move(post_aggregate_column_list))
        .Build();
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedAggregationThresholdAggregateScan(
      std::unique_ptr<const ResolvedAggregationThresholdAggregateScan> node)
      override {
    for (const auto& computed_column : node->aggregate_list()) {
      ZETASQL_RET_CHECK(computed_column->Is<ResolvedComputedColumnImpl>());
      auto computed_col = computed_column->GetAs<ResolvedComputedColumnImpl>();
      if (computed_col->expr()->Is<ResolvedAggregateFunctionCall>() &&
          (computed_col->expr()
               ->GetAs<ResolvedAggregateFunctionCall>()
               ->function()
               ->Is<SQLFunctionInterface>() ||
           computed_col->expr()
               ->GetAs<ResolvedAggregateFunctionCall>()
               ->function()
               ->Is<TemplatedSQLFunction>())) {
        return MakeSqlErrorAtStart(computed_col->expr()
                                       ->GetAs<ResolvedAggregateFunctionCall>()
                                       ->GetParseLocationRangeOrNULL())
               << "Aggregation threshold is not supported with user defined "
                  "aggregate function";
      }
    }
    return node;
  }

  absl::StatusOr<std::unique_ptr<const ResolvedNode>>
  PostVisitResolvedPivotScan(
      std::unique_ptr<const ResolvedPivotScan> node) override {
    absl::flat_hash_map<const ResolvedAggregateFunctionCall*,
                        AggregateFnDetails>
        calls_to_inline;
    for (const auto& expr : node->pivot_expr_list()) {
      ZETASQL_RET_CHECK(expr->Is<ResolvedAggregateFunctionCall>());
      const auto* call = expr->GetAs<ResolvedAggregateFunctionCall>();
      ZETASQL_ASSIGN_OR_RETURN(std::optional<AggregateFnDetails> details,
                       IsInlineable(call));
      if (details.has_value()) {
        calls_to_inline.emplace(call, *details);
      }
    }
    if (!calls_to_inline.empty()) {
      return absl::InvalidArgumentError(
          "SQL-defined aggregate functions are not supported in PIVOT");
    }
    return node;
  }

 private:
  ColumnFactory& column_factory_;
};

class SqlUdaInliner : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, std::unique_ptr<const ResolvedNode> input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());
    SqlAggregateFunctionInlineVisitor rewriter(column_factory);
    return rewriter.VisitAll(std::move(input));
  }

  std::string Name() const override { return "SqlUdaInliner"; }
};

}  // namespace

const Rewriter* GetSqlFunctionInliner() {
  static const auto* const kRewriter = new SqlFunctionInliner;
  return kRewriter;
}

const Rewriter* GetSqlTvfInliner() {
  static const auto* const kRewriter = new SqlTvfInliner;
  return kRewriter;
}

const Rewriter* GetSqlAggregateInliner() {
  static const auto* const kRewriter = new SqlUdaInliner;
  return kRewriter;
}

}  // namespace zetasql
