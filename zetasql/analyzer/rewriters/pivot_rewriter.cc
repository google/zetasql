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

#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/rewriters/rewriter_interface.h"
#include "zetasql/analyzer/substitute.h"
#include "zetasql/common/aggregate_null_handling.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/status/status.h"

namespace zetasql {
namespace {

constexpr char kPivot[] = "$pivot";
constexpr char kPivotExprArg[] = "$pivot_expr_arg";

class PivotRewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  explicit PivotRewriterVisitor(Catalog* catalog, TypeFactory* type_factory,
                                ColumnFactory* column_factory,
                                const AnalyzerOptions* analyzer_options,
                                absl::Span<const Rewriter* const> rewriters)
      : analyzer_options_(analyzer_options),
        catalog_(catalog),
        type_factory_(type_factory),
        column_factory_(column_factory),
        rewriters_(rewriters) {}

  PivotRewriterVisitor(const PivotRewriterVisitor&) = delete;
  PivotRewriterVisitor& operator=(const PivotRewriterVisitor&) = delete;

 private:
  absl::Status VisitResolvedPivotScan(const ResolvedPivotScan* node) override;

  // Returns an aggregate function call representing a single pivot expression
  // over a subset of input where <pivot_value_expr> matches <pivot_column>.
  // If <agg_fn_arg_column> is present, the argument to the aggregate function
  // will be a column ref to this column, rather than using the subtree inside
  // of <pivot_expr> itself; this ensures that non-deterministic expressions
  // evaluate the same for each row, even across different pivot columns.
  //
  // For example, in this query:
  //   SELECT * FROM t PIVOT(SUM(x + 1) s, COUNT(*) c FOR y + 1 IN (0, 1));
  //
  // MakeAggregateExpr() will be called four times. The first two times:
  // - <pivot_expr> will be SUM(x)
  // - <pivot_value_expr> will be 0 (1st time) or 1 (2nd time)
  // - <pivot_column> will hold the result of (y + 1)
  // - <agg_fn_arg_column> will hold the result of x + 1
  //
  // And, the next two times:
  // - <pivot_expr> will be COUNT(*)
  // - <pivot_value_expr> will be 0 (3rd time) or 1 (4th time)
  // - <pivot_column> will hold the result of (y + 1)
  // - <agg_fn_arg_column> will be absl::nullopt (since there's no argument).
  zetasql_base::StatusOr<std::unique_ptr<const ResolvedExpr>> MakeAggregateExpr(
      const ResolvedExpr* pivot_expr, const ResolvedExpr* pivot_value_expr,
      const ResolvedColumn& pivot_column,
      const std::vector<ResolvedColumn>& agg_fn_arg_columns);

  // Wraps the input scan of a pivot with a project scan, adding computed
  // columns holding the result of the FOR expression, plus each argument to the
  // aggregate function in each PIVOT expression. The resultant scan will be
  // used as the input to the AggregateScan used to represent the rewritten
  // PivotScan.
  //
  // <pivot_expr_arg_columns> is an output parameter, which is modified to hold
  // the argument columns to each pivot expression.
  // On output, the i'th element is the single argument to the i'th pivot
  // expression. If a given pivot expression does not take an argument (e.g. it
  // is "COUNT(*)"), the ResolvedColumn is blank.
  zetasql_base::StatusOr<std::unique_ptr<ResolvedScan>> AddExprColumnsToPivotInput(
      const ResolvedPivotScan* pivot_scan,
      const ResolvedColumn& for_expr_column,
      std::vector<std::vector<ResolvedColumn>>& pivot_expr_arg_columns);

  // Verifies that <call> is supported by this rewriter implementation.
  //
  // A aggregate function call is supported as a pivot expression if all of
  // the following conditions apply:
  //  - Function call has exactly one argument
  //  - Function is known to ignore all input rows where the argument is NULL
  //  - The HAVING MIN and HAVING MAX clauses are not present.
  //
  // There is one exception to the above; COUNT(*) is supported, in spite of
  // having zero arguments instead of one.
  absl::Status VerifyAggregateFunctionIsSupported(
      const ResolvedAggregateFunctionCall* call);

  // Implements MakeAggregateExpr() for the case where the pivot expression is
  // a call to the COUNT(*) function. COUNT(*) uses a different implementation
  // strategy from other functions, for which the COUNT(*) call is translated to
  // COUNTIF().
  zetasql_base::StatusOr<std::unique_ptr<const ResolvedExpr>> RewriteCountStarPivotExpr(
      ResolvedAggregateFunctionCall* call,
      std::unique_ptr<ResolvedExpr> pivot_value_expr,
      const ResolvedColumn& pivot_column);

  const AnalyzerOptions* analyzer_options_;
  Catalog* const catalog_;
  TypeFactory* type_factory_;
  ColumnFactory* const column_factory_;
  absl::Span<const Rewriter* const> rewriters_;
};

zetasql_base::StatusOr<std::unique_ptr<ResolvedScan>>
PivotRewriterVisitor::AddExprColumnsToPivotInput(
    const ResolvedPivotScan* pivot_scan, const ResolvedColumn& for_expr_column,
    std::vector<std::vector<ResolvedColumn>>& pivot_expr_arg_columns) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedScan> input_scan_copy,
                   ProcessNode(pivot_scan->input_scan()));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> for_expr_copy,
                   ProcessNode(pivot_scan->for_expr()));

  std::vector<ResolvedColumn> column_list(
      pivot_scan->input_scan()->column_list().begin(),
      pivot_scan->input_scan()->column_list().end());
  column_list.push_back(for_expr_column);

  std::vector<std::unique_ptr<ResolvedComputedColumn>> expr_list;
  expr_list.push_back(
      MakeResolvedComputedColumn(for_expr_column, std::move(for_expr_copy)));

  for (const auto& pivot_expr : pivot_scan->pivot_expr_list()) {
    const ResolvedAggregateFunctionCall* call =
        pivot_expr->GetAs<ResolvedAggregateFunctionCall>();
    ZETASQL_RETURN_IF_ERROR(VerifyAggregateFunctionIsSupported(call));
    pivot_expr_arg_columns.emplace_back();

    for (const auto& arg : call->argument_list()) {
      if (IsConstantExpression(arg.get())) {
        // Constant expressions are ok to clone, rather than project. In most
        // cases, this doesn't matter, but some functions take constant
        // arguments that can't be read from a projected column (the delimiter
        // argument of STRING_AGG() is one such example). For simplicity, we
        // take the clone approach for all constant expressions, whether needed
        // or not.
        //
        // In this case, just push a dummy column into the projected column list
        // for the current pivot expr. We'll copy the argument later, where it
        // is used.
        pivot_expr_arg_columns.back().emplace_back();
        continue;
      }
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> argument_copy,
                       ProcessNode(arg.get()));

      ResolvedColumn projected_arg_col = column_factory_->MakeCol(
          kPivot, kPivotExprArg, argument_copy->type());
      expr_list.push_back(MakeResolvedComputedColumn(projected_arg_col,
                                                     std::move(argument_copy)));
      pivot_expr_arg_columns.back().push_back(projected_arg_col);
      column_list.push_back(projected_arg_col);
    }
  }

  return MakeResolvedProjectScan(column_list, std::move(expr_list),
                                 std::move(input_scan_copy));
}

absl::Status PivotRewriterVisitor::VisitResolvedPivotScan(
    const ResolvedPivotScan* node) {
  ResolvedColumn pivot_col = column_factory_->MakeCol("$pivot", "$pivot_value",
                                                      node->for_expr()->type());

  std::vector<std::vector<ResolvedColumn>> agg_fn_argument_columns;

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedScan> input_with_pivot_column,
      AddExprColumnsToPivotInput(node, pivot_col, agg_fn_argument_columns));

  std::vector<std::unique_ptr<ResolvedComputedColumn>> aggregate_list;

  for (const auto& pivot_column : node->pivot_column_list()) {
    const ResolvedExpr* pivot_expr =
        node->pivot_expr_list()[pivot_column->pivot_expr_index()].get();
    const ResolvedExpr* pivot_value_expr =
        node->pivot_value_list()[pivot_column->pivot_value_index()].get();
    const ResolvedColumn& output_col = pivot_column->column();

    ZETASQL_CHECK_LE(pivot_column->pivot_expr_index(), agg_fn_argument_columns.size());
    const std::vector<ResolvedColumn>& agg_fn_arg_columns =
        agg_fn_argument_columns[pivot_column->pivot_expr_index()];

    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> agg_expr,
                     MakeAggregateExpr(pivot_expr, pivot_value_expr, pivot_col,
                                       agg_fn_arg_columns));
    std::unique_ptr<ResolvedComputedColumn> agg_computed_column =
        MakeResolvedComputedColumn(output_col, std::move(agg_expr));
    aggregate_list.push_back(std::move(agg_computed_column));
  }

  std::vector<std::unique_ptr<ResolvedComputedColumn>> group_by_list;
  group_by_list.reserve(node->group_by_list_size());
  for (const auto& group_by : node->group_by_list()) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedComputedColumn> group_by_item,
                     ProcessNode(group_by.get()));
    group_by_list.push_back(std::move(group_by_item));
  }

  PushNodeToStack(MakeResolvedAggregateScan(
      node->column_list(), std::move(input_with_pivot_column),
      std::move(group_by_list), std::move(aggregate_list),
      /*grouping_set_list=*/{},
      /*rollup_column_list=*/{}));
  return absl::OkStatus();
}

absl::Status PivotRewriterVisitor::VerifyAggregateFunctionIsSupported(
    const ResolvedAggregateFunctionCall* call) {
  if (call->signature().context_id() == FN_COUNT_STAR) {
    // COUNT(*) has special implementation and is supported.
    return absl::OkStatus();
  }
  ZETASQL_RET_CHECK(call->signature().IsConcrete());
  if (call->signature().NumConcreteArguments() == 0) {
    // Zero-argument signatures other than COUNT(*) are not supported.
    return MakeUnimplementedErrorAtPoint(
               call->GetParseLocationOrNULL()->start())
           << "Use of aggregate function " << call->function()->SQLName()
           << " as PIVOT expression is not supported";
  }

  if (call->having_modifier() != nullptr) {
    // Support for HAVING MIN/MAX through the rewriter requires additional work.
    // We would need to value we're taking the MIN/MAX of to exclude rows where
    // the pivot column does not match the pivot value.
    //
    // As HAVING MIN/MAX is not a commonly-used feature, this is low priority.
    return MakeUnimplementedErrorAtPoint(
               call->GetParseLocationOrNULL()->start())
           << "Use of HAVING inside an aggregate function used as a PIVOT "
              "expression is not supported";
  }

  switch (call->null_handling_modifier()) {
    case ResolvedAggregateFunctionCall::IGNORE_NULLS:
      // Function call is explicitly annotated as ignoring nulls.
      return absl::OkStatus();
    case ResolvedAggregateFunctionCall::RESPECT_NULLS:
      return MakeUnimplementedErrorAtPoint(
                 call->GetParseLocationOrNULL()->start())
             << "Use of RESPECT NULLS in aggregate function used as a PIVOT "
                "expression is not supported";
    case ResolvedAggregateFunctionCall::DEFAULT_NULL_HANDLING:
      if (IgnoresNullArguments(call)) {
        return absl::OkStatus();
      }
      // The function call cannot not supported because it does/might
      // respect NULL inputs, which would break our rewrite strategy of
      // replacing the input argument with NULL when the pivot value does
      // not match. Provide a suitable error message indicating whether
      // the function could be supported if IGNORE NULLS were added.
      if (call->function()
              ->function_options()
              .supports_null_handling_modifier &&
          analyzer_options_->language().LanguageFeatureEnabled(
              FEATURE_V_1_1_NULL_HANDLING_MODIFIER_IN_AGGREGATE)) {
        return MakeUnimplementedErrorAtPoint(
                   call->GetParseLocationOrNULL()->start())
               << "Use of aggregate function " << call->function()->SQLName()
               << " as PIVOT expression is not supported unless IGNORE "
                  "NULLS is specified";
      }
      return MakeUnimplementedErrorAtPoint(
                 call->GetParseLocationOrNULL()->start())
             << "Use of aggregate function " << call->function()->SQLName()
             << " as PIVOT expression is not supported";
  }
}

zetasql_base::StatusOr<std::unique_ptr<const ResolvedExpr>>
PivotRewriterVisitor::RewriteCountStarPivotExpr(
    ResolvedAggregateFunctionCall* call,
    std::unique_ptr<ResolvedExpr> pivot_value_expr,
    const ResolvedColumn& pivot_column) {
  // Replace
  //  COUNT(*)
  // with
  //  COUNTIF(<pivot_column> IS NOT DISTINCT FROM <pivot value expr>)
  //
  std::unique_ptr<ResolvedExpr> pivot_column_ref = MakeResolvedColumnRef(
      pivot_column.type(), pivot_column, /*is_correlated=*/false);
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedExpr> countif_arg,
      AnalyzeSubstitute(*analyzer_options_, rewriters_, *catalog_,
                        *type_factory_,
                        "pivot_column IS NOT DISTINCT FROM pivot_value",
                        {{"pivot_column", pivot_column_ref.get()},
                         {"pivot_value", pivot_value_expr.get()}}),
      _.With(ExpectAnalyzeSubstituteSuccess));

  std::vector<std::unique_ptr<const ResolvedExpr>> countif_args;
  countif_args.push_back(std::move(countif_arg));

  const Function* countif_fn;
  ZETASQL_RET_CHECK_OK(
      catalog_->FindFunction({"countif"}, &countif_fn, /*options=*/{}));
  FunctionArgumentType int64_arg = FunctionArgumentType(types::Int64Type(), 1);
  FunctionArgumentType bool_arg = FunctionArgumentType(types::BoolType(), 1);
  FunctionSignature countif_sig(int64_arg, {bool_arg}, FN_COUNTIF);

  return MakeResolvedAggregateFunctionCall(
      types::Int64Type(), countif_fn, countif_sig, std::move(countif_args), {},
      call->error_mode(), call->distinct(), call->null_handling_modifier(),
      call->release_having_modifier(), call->release_order_by_item_list(),
      call->release_limit(), call->function_call_info());
}

zetasql_base::StatusOr<std::unique_ptr<const ResolvedExpr>>
PivotRewriterVisitor::MakeAggregateExpr(
    const ResolvedExpr* pivot_expr, const ResolvedExpr* pivot_value_expr,
    const ResolvedColumn& pivot_column,
    const std::vector<ResolvedColumn>& agg_fn_arg_columns) {
  // This condition guaranteed by the resolver and this check
  // really belongs in the validator; however, the validator currently has no
  // way to call IsConstantExpression() without creating a circular build
  // dependency, so adding a check here, just to make sure.
  // TODO: Refactor the code so that this check and be moved to the
  // validator, where it belongs.
  ZETASQL_RET_CHECK(IsConstantExpression(pivot_value_expr));

  ZETASQL_RET_CHECK_EQ(pivot_expr->node_kind(), RESOLVED_AGGREGATE_FUNCTION_CALL)
      << "Should have function call; this should have been checked upstream";
  const ResolvedAggregateFunctionCall* call =
      pivot_expr->GetAs<ResolvedAggregateFunctionCall>();
  ZETASQL_RETURN_IF_ERROR(VerifyAggregateFunctionIsSupported(call));

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> pivot_expr_copy,
                   ProcessNode(pivot_expr));

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> pivot_value_expr_copy,
                   ProcessNode(pivot_value_expr));

  ResolvedAggregateFunctionCall* call_copy =
      pivot_expr_copy->GetAs<ResolvedAggregateFunctionCall>();

  std::vector<std::string> fn_name_path = call->function()->FunctionNamePath();
  if (call->signature().context_id() == FN_COUNT_STAR) {
    return RewriteCountStarPivotExpr(
        call_copy, std::move(pivot_value_expr_copy), pivot_column);
  }

  // General case for remaining aggregate functions. The resolver has already
  // checked that these are single-argument functions which ignore null inputs.
  std::unique_ptr<ResolvedExpr> pivot_column_ref = MakeResolvedColumnRef(
      pivot_column.type(), pivot_column, /*is_correlated=*/false);
  std::vector<std::unique_ptr<const ResolvedExpr>> agg_fn_args;

  // Because "ignores nulls" behavior means skipping rows when *any* input
  // argument is NULL, we only need to add the IF clause to check the pivot
  // value on the first argument. The rest can just be used directly.
  if (!agg_fn_arg_columns.empty()) {
    std::unique_ptr<ResolvedExpr> orig_arg;
    if (agg_fn_arg_columns[0].IsInitialized()) {
      orig_arg = MakeResolvedColumnRef(agg_fn_arg_columns[0].type(),
                                       agg_fn_arg_columns[0],
                                       /*is_correlated=*/false);
    } else {
      ZETASQL_ASSIGN_OR_RETURN(orig_arg, ProcessNode(call->argument_list(0)));
    }
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<ResolvedExpr> agg_fn_arg,
        AnalyzeSubstitute(
            *analyzer_options_, rewriters_, *catalog_, *type_factory_,
            "IF(pivot_column IS NOT DISTINCT FROM pivot_value, orig_arg, NULL)",
            {{"pivot_column", pivot_column_ref.get()},
             {"pivot_value", pivot_value_expr_copy.get()},
             {"orig_arg", orig_arg.get()}}),
        _.With(ExpectAnalyzeSubstituteSuccess));

    agg_fn_args.push_back(std::move(agg_fn_arg));

    for (int i = 1; i < agg_fn_arg_columns.size(); ++i) {
      if (agg_fn_arg_columns[i].IsInitialized()) {
        agg_fn_args.push_back(MakeResolvedColumnRef(
            agg_fn_arg_columns[i].type(), agg_fn_arg_columns[i],
            /*is_correlated=*/false));
      } else {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> arg_copy,
                         ProcessNode(call->argument_list(i)));
        agg_fn_args.push_back(std::move(arg_copy));
      }
    }
  }

  return MakeResolvedAggregateFunctionCall(
      call->type(), call->function(), call->signature(), std::move(agg_fn_args),
      {}, call->error_mode(), call->distinct(), call->null_handling_modifier(),
      call_copy->release_having_modifier(),
      call_copy->release_order_by_item_list(), call_copy->release_limit(),
      call->function_call_info());
}
}  // namespace

class PivotRewriter : public Rewriter {
 public:
  std::string Name() const override { return "PivotRewriter"; }

  bool ShouldRewrite(const AnalyzerOptions& analyzer_options,
                     const AnalyzerOutput& analyzer_output) const override {
    return analyzer_output.analyzer_output_properties().has_pivot &&
           analyzer_options.rewrite_enabled(REWRITE_PIVOT);
  }

  zetasql_base::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options,
      absl::Span<const Rewriter* const> rewriters, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());

    // Force-enable IS NOT DISTINCT FROM, since we make use of it in subqueries
    // we pass to AnalyzeSubstitute(). It is assumed that any engine making use
    // of the PIVOT rewriter has a valid $is_not_distinct_from implementation,
    // even if the engine does not support the IS NOT DISTINCT FROM syntax at
    // the end-user level.
    std::unique_ptr<AnalyzerOptions> analyzer_options_with_distinct;
    const AnalyzerOptions* analyzer_options_to_use = &options;
    if (!options.language().LanguageFeatureEnabled(FEATURE_V_1_3_IS_DISTINCT)) {
      analyzer_options_with_distinct =
          absl::make_unique<AnalyzerOptions>(options);
      analyzer_options_with_distinct->mutable_language()->EnableLanguageFeature(
          FEATURE_V_1_3_IS_DISTINCT);
      analyzer_options_to_use = analyzer_options_with_distinct.get();
    }

    PivotRewriterVisitor visitor(&catalog, &type_factory, &column_factory,
                                 analyzer_options_to_use, rewriters);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&visitor));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedNode> result,
                     visitor.ConsumeRootNode<ResolvedStatement>());
    output_properties.has_pivot = false;
    return result;
  }
};

const Rewriter* GetPivotRewriter() {
  static const auto* const kRewriter = new PivotRewriter;
  return kRewriter;
}

}  // namespace zetasql
