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
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// A visitor that rewrites ResolvedFlatten nodes into standard UNNESTs.
class FlattenRewriterVisitor : public ResolvedASTDeepCopyVisitor {
 public:
  explicit FlattenRewriterVisitor(const AnalyzerOptions* options,
                                  Catalog* catalog,
                                  ColumnFactory* column_factory)
      : fn_builder_(*options, *catalog), column_factory_(column_factory) {}

 private:
  absl::Status VisitResolvedArrayScan(const ResolvedArrayScan* node) override;

  absl::Status VisitResolvedFlatten(const ResolvedFlatten* node) override;

  // Takes the components of a ResolvedFlatten (its expr, 'flatten_expr' and its
  // 'get_field_list' and converts it into a resulting ResolvedScan that is
  // functionally equivalent.
  //
  // When 'flatten_expr' uses ColumnRefs from a scan, 'input_scan' must be
  // provided to be that input scan.
  //
  // When 'order_results' is true, the generated scan ends with an OrderByScan
  // to retain order (using offsets from array scans).
  //
  // 'in_subquery' should be set to indicate whether the resulting scan will be
  // in a subquery or not. If it is, then column references need to be
  // correlated to access the column outside the subquery it's in.
  //
  // Note that the final OrderByScan is not needed for a case like
  // SELECT ... FROM t, UNNEST(t.a.b.c) since the UNNEST produces an unordered
  // relation. The final OrderByScan is needed for explicit FLATTEN(t.a.b.c) or
  // UNNEST with OFFSET.
  //
  // The result is the last column in the output scan's column list.
  absl::StatusOr<std::unique_ptr<ResolvedScan>> FlattenToScan(
      std::unique_ptr<ResolvedExpr> flatten_expr,
      const std::vector<std::unique_ptr<const ResolvedExpr>>& get_field_list,
      std::unique_ptr<ResolvedScan> input_scan, bool order_results,
      bool in_subquery);

  FunctionCallBuilder fn_builder_;
  ColumnFactory* column_factory_;
};

absl::Status FlattenRewriterVisitor::VisitResolvedArrayScan(
    const ResolvedArrayScan* node) {
  if (!node->array_expr()->Is<ResolvedFlatten>()) {
    return CopyVisitResolvedArrayScan(node);
  }
  const ResolvedFlatten* flatten = node->array_expr()->GetAs<ResolvedFlatten>();
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedScan> input_scan,
                   ProcessNode(node->input_scan()));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> join_expr,
                   ProcessNode(node->join_expr()));

  bool need_offset_column = node->array_offset_column() != nullptr;
  if (need_offset_column || join_expr != nullptr) {
    // If we need an offset column, we rewrite each row to a subquery to
    // generate a single array and then do an array scan over that. This allows
    // us to have a single ordered offset.
    //
    // Without doing so we end up with one offset per repeated pivot and no way
    // to combine them.
    //
    // TODO: Avoid using a subquery for joins. For now we also do this
    // for joins to handle the case where the flatten ends up with a ProjectScan
    // instead of an ArrayScan. A better solution would be to stop adding the
    // ProjectScan if the final element path is a scalar and instead to rewrite
    // column references to the output to do the Get*Field there instead. This
    // is a significantly more complex change but would avoid needing the
    // subquery.
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> flatten_expr,
                     ProcessNode(flatten->expr()));
    ZETASQL_ASSIGN_OR_RETURN(flatten_expr, CorrelateColumnRefs(*flatten_expr));
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<ResolvedScan> scan,
        FlattenToScan(std::move(flatten_expr), flatten->get_field_list(),
                      MakeResolvedSingleRowScan(), need_offset_column,
                      /*in_subquery=*/true));

    std::vector<std::unique_ptr<const ResolvedColumnRef>> column_refs;
    ZETASQL_RETURN_IF_ERROR(CollectColumnRefs(*flatten, &column_refs));
    if (scan->column_list_size() > 1) {
      // Subquery must produce one value. Remove unneeded intermediary columns.
      // TODO: This can be removed if we avoid using subquery for joins.
      std::vector<ResolvedColumn> column_list;
      column_list.push_back(scan->column_list().back());
      scan->set_column_list(std::move(column_list));
    }
    std::unique_ptr<ResolvedSubqueryExpr> subquery = MakeResolvedSubqueryExpr(
        flatten->type(), ResolvedSubqueryExpr::ARRAY, std::move(column_refs),
        /*in_expr=*/nullptr, std::move(scan));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedColumnHolder> offset_column,
                     ProcessNode(node->array_offset_column()));
    PushNodeToStack(MakeResolvedArrayScan(
        node->column_list(), std::move(input_scan), std::move(subquery),
        node->element_column(), std::move(offset_column), std::move(join_expr),
        node->is_outer()));
    return absl::OkStatus();
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> flatten_expr,
                   ProcessNode(flatten->expr()));
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedScan> scan,
      FlattenToScan(std::move(flatten_expr), flatten->get_field_list(),
                    std::move(input_scan), /*order_results=*/false,
                    /*in_subquery=*/false));

  // Project the flatten result back to the expected output column.
  std::vector<std::unique_ptr<const ResolvedComputedColumn>> expr_list;
  expr_list.push_back(MakeResolvedComputedColumn(
      node->element_column(),
      MakeResolvedColumnRef(scan->column_list().back().type(),
                            scan->column_list().back(),
                            /*is_correlated=*/false)));
  PushNodeToStack(MakeResolvedProjectScan(
      node->column_list(), std::move(expr_list), std::move(scan)));
  return absl::OkStatus();
}

absl::Status FlattenRewriterVisitor::VisitResolvedFlatten(
    const ResolvedFlatten* node) {
  // Define a column to represent the result of evaluating the input. We want
  // the input value referenced both by null checking and flattening, so we use
  // a column to ensure it is only evaluated once.
  ResolvedColumn flatten_expr_column = column_factory_->MakeCol(
      "$flatten_input", "injected", node->expr()->type());

  // To avoid returning an empty array if the input is NULL, we rewrite to
  // explicitly return NULL in that case. The flatten rewrite would return an
  // empty array.
  //
  // TODO: Use AnalyzeSubstitute once it's ready.

  // Check if the input expression is NULL.
  std::unique_ptr<ResolvedExpr> input_col =
      MakeResolvedColumnRef(flatten_expr_column.type(), flatten_expr_column,
                            /*is_correlated=*/false);
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> if_condition,
                   fn_builder_.IsNull(std::move(input_col)));
  // If so, we return NULL.
  std::unique_ptr<ResolvedExpr> if_then =
      MakeResolvedLiteral(Value::Null(node->type()));
  // Otherwise, return the flattened result.
  std::vector<std::unique_ptr<const ResolvedColumnRef>> column_refs;
  column_refs.push_back(MakeResolvedColumnRef(flatten_expr_column.type(),
                                              flatten_expr_column,
                                              /*is_correlated=*/false));
  for (const auto& get_field : node->get_field_list()) {
    ZETASQL_RETURN_IF_ERROR(CollectColumnRefs(*get_field, &column_refs));
  }
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedScan> rewritten_flatten,
      FlattenToScan(
          MakeResolvedColumnRef(flatten_expr_column.type(), flatten_expr_column,
                                /*is_correlated=*/true),
          node->get_field_list(), /*input_scan=*/nullptr,
          /*order_results=*/true, /*in_subquery=*/true));
  std::unique_ptr<ResolvedExpr> if_else = MakeResolvedSubqueryExpr(
      node->type(), ResolvedSubqueryExpr::ARRAY, std::move(column_refs),
      /*in_expr=*/nullptr, std::move(rewritten_flatten));

  ZETASQL_ASSIGN_OR_RETURN(auto resolved_if,
                   fn_builder_.If(std::move(if_condition), std::move(if_then),
                                  std::move(if_else)));

  // Use a ResolvedLetExpr to populate the input variable.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> flatten_expr,
                   ProcessNode(node->expr()));
  std::vector<std::unique_ptr<const ResolvedComputedColumn>> let_assignments;
  let_assignments.push_back(
      MakeResolvedComputedColumn(flatten_expr_column, std::move(flatten_expr)));
  PushNodeToStack(MakeResolvedLetExpr(node->type(), std::move(let_assignments),
                                      std::move(resolved_if)));
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<ResolvedScan>>
FlattenRewriterVisitor::FlattenToScan(
    std::unique_ptr<ResolvedExpr> flatten_expr,
    const std::vector<std::unique_ptr<const ResolvedExpr>>& get_field_list,
    std::unique_ptr<ResolvedScan> input_scan, bool order_results,
    bool in_subquery) {
  std::vector<ResolvedColumn> column_list;
  if (input_scan != nullptr) column_list = input_scan->column_list();
  ResolvedColumn column = column_factory_->MakeCol(
      "$flatten", "injected", flatten_expr->type()->AsArray()->element_type());
  column_list.push_back(column);

  std::vector<ResolvedColumn> offset_columns;
  ResolvedColumn offset_column;
  if (order_results) {
    offset_column =
        column_factory_->MakeCol("$offset", "injected", types::Int64Type());
    offset_columns.push_back(offset_column);
    column_list.push_back(offset_column);
  }

  std::unique_ptr<ResolvedScan> scan = MakeResolvedArrayScan(
      column_list, std::move(input_scan), std::move(flatten_expr), column,
      order_results ? MakeResolvedColumnHolder(offset_column) : nullptr,
      /*join_expr=*/nullptr, /*is_outer=*/false);

  // Keep track of pending Get*Field on non-array fields.
  std::unique_ptr<const ResolvedExpr> input;

  for (const auto& const_get_field : get_field_list) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> get_field,
                     ProcessNode(const_get_field.get()));
    // Change the input from the FlattenedArg to instead be a ColumnRef or the
    // built-up non-array expression.
    if (input == nullptr) {
      input = MakeResolvedColumnRef(column.type(), column,
                                    /*is_correlated=*/false);
    }
    ResolvedExpr* to_set_input = get_field.get();
    if (get_field->Is<ResolvedFunctionCall>()) {
      if (in_subquery) {
        ZETASQL_ASSIGN_OR_RETURN(get_field, CorrelateColumnRefs(*get_field));
      }
      ResolvedFunctionCall* call = get_field->GetAs<ResolvedFunctionCall>();
      ZETASQL_RET_CHECK_EQ(2, call->argument_list_size());
      to_set_input = const_cast<ResolvedExpr*>(
          get_field->GetAs<ResolvedFunctionCall>()->argument_list(0));
    }
    if (to_set_input->Is<ResolvedGetProtoField>()) {
      to_set_input->GetAs<ResolvedGetProtoField>()->set_expr(std::move(input));
    } else if (to_set_input->Is<ResolvedGetStructField>()) {
      to_set_input->GetAs<ResolvedGetStructField>()->set_expr(std::move(input));
    } else if (to_set_input->Is<ResolvedGetJsonField>()) {
      to_set_input->GetAs<ResolvedGetJsonField>()->set_expr(std::move(input));
    } else {
      ZETASQL_RET_CHECK_FAIL() << "Unsupported node: " << to_set_input->DebugString();
    }
    input = nullptr;  // already null, but avoids ClangTidy "use after free"

    if (!get_field->type()->IsArray()) {
      // Not an array so can't turn it into an ArrayScan.
      // Collect as input for next array.
      input = std::move(get_field);
    } else {
      column = column_factory_->MakeCol(
          "$flatten", "injected", get_field->type()->AsArray()->element_type());
      column_list.push_back(column);

      if (order_results) {
        offset_column =
            column_factory_->MakeCol("$offset", "injected", types::Int64Type());
        offset_columns.push_back(offset_column);
        column_list.push_back(offset_column);
      }
      scan = MakeResolvedArrayScan(
          column_list, std::move(scan), std::move(get_field), column,
          order_results ? MakeResolvedColumnHolder(offset_column) : nullptr,
          /*join_expr=*/nullptr,
          /*is_outer=*/false);
    }
  }

  if (input != nullptr) {
    // We have leftover "gets" that resulted in non-arrays.
    // Use a ProjectScan to resolve them to the expected column.
    column = column_factory_->MakeCol("$flatten", "injected", input->type());
    // node->type()->AsArray()->element_type());
    column_list.push_back(column);
    std::vector<std::unique_ptr<const ResolvedComputedColumn>> expr_list;
    expr_list.push_back(MakeResolvedComputedColumn(column, std::move(input)));
    scan = MakeResolvedProjectScan(column_list, std::move(expr_list),
                                   std::move(scan));
  }

  if (order_results) {
    std::vector<std::unique_ptr<const ResolvedOrderByItem>> order_by;
    order_by.reserve(offset_columns.size());
    for (const ResolvedColumn& c : offset_columns) {
      order_by.push_back(MakeResolvedOrderByItem(
          MakeResolvedColumnRef(c.type(), c, /*is_correlated=*/false),
          /*collation_name=*/nullptr, /*is_descending=*/false,
          ResolvedOrderByItemEnums::ORDER_UNSPECIFIED));
    }
    scan =
        MakeResolvedOrderByScan({column}, std::move(scan), std::move(order_by));
    scan->set_is_ordered(true);
  }
  return scan;
}

}  // namespace

class FlattenRewriter : public Rewriter {
 public:
  absl::StatusOr<std::unique_ptr<const ResolvedNode>> Rewrite(
      const AnalyzerOptions& options, const ResolvedNode& input,
      Catalog& catalog, TypeFactory& type_factory,
      AnalyzerOutputProperties& output_properties) const override {
    ZETASQL_RET_CHECK(options.column_id_sequence_number() != nullptr);
    ColumnFactory column_factory(0, options.id_string_pool().get(),
                                 options.column_id_sequence_number());
    FlattenRewriterVisitor rewriter(&options, &catalog, &column_factory);
    ZETASQL_RETURN_IF_ERROR(input.Accept(&rewriter));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedNode> result,
                     rewriter.ConsumeRootNode<ResolvedNode>());
    return result;
  }

  std::string Name() const override { return "FlattenRewriter"; }
};

const Rewriter* GetFlattenRewriter() {
  static const auto* const kRewriter = new FlattenRewriter;
  return kRewriter;
}

}  // namespace zetasql
