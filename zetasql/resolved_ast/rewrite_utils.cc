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

#include "zetasql/resolved_ast/rewrite_utils.h"

#include <string>
#include <utility>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"

namespace zetasql {
namespace {

// A visitor that changes ResolvedColumnRef nodes to be correlated.
class CorrelateColumnRefVisitor : public ResolvedASTDeepCopyVisitor {
 private:
  // Logic that determines whether an individual ResolvedColumnRef should be
  // marked as correlated in the output tree.
  bool ShouldBeCorrelated(const ResolvedColumnRef& ref) {
    if (in_subquery_or_lambda_ || local_columns_.contains(ref.column())) {
      // Columns in 'local_columns_' and columns inside subqueries or lambda
      // bodies are fully local to the expression. We shouldn't change the
      // is_correlated state for local columns.
      return ref.is_correlated();
    }
    return true;
  }

  std::unique_ptr<ResolvedColumnRef> CorrelateColumnRef(
      const ResolvedColumnRef& ref) {
    return MakeResolvedColumnRef(ref.type(), ref.column(),
                                 ShouldBeCorrelated(ref));
  }

  template <class T>
  void CorrelateParameterList(T* node) {
    for (auto& column_ref : node->parameter_list()) {
      const_cast<ResolvedColumnRef*>(column_ref.get())
          ->set_is_correlated(ShouldBeCorrelated(*column_ref));
    }
  }

  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* node) override {
    PushNodeToStack(CorrelateColumnRef(*node));
    return absl::OkStatus();
  }

  absl::Status VisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr* node) override {
    ++in_subquery_or_lambda_;
    absl::Status s =
        ResolvedASTDeepCopyVisitor::VisitResolvedSubqueryExpr(node);
    --in_subquery_or_lambda_;

    // If this is the first lambda or subquery encountered, we need to correlate
    // the column references in the parameter list and for the in expression.
    // Column refererences of outer columns are already correlated.
    if (!in_subquery_or_lambda_) {
      std::unique_ptr<ResolvedSubqueryExpr> expr =
          ConsumeTopOfStack<ResolvedSubqueryExpr>();
      CorrelateParameterList(expr.get());
      if (expr->in_expr() != nullptr) {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> in_expr,
                         ProcessNode(expr->in_expr()));
        expr->set_in_expr(std::move(in_expr));
      }
      PushNodeToStack(std::move(expr));
    }
    return s;
  }

  absl::Status VisitResolvedInlineLambda(
      const ResolvedInlineLambda* node) override {
    ++in_subquery_or_lambda_;
    absl::Status s =
        ResolvedASTDeepCopyVisitor::VisitResolvedInlineLambda(node);
    --in_subquery_or_lambda_;

    // If this is the first lambda or subquery encountered, we need to correlate
    // the column references in the parameter list. Column references of outer
    // columns are already correlated.
    if (!in_subquery_or_lambda_) {
      std::unique_ptr<ResolvedInlineLambda> expr =
          ConsumeTopOfStack<ResolvedInlineLambda>();
      CorrelateParameterList(expr.get());
      PushNodeToStack(std::move(expr));
    }
    return absl::OkStatus();
  }

  absl::Status VisitResolvedLetExpr(const ResolvedLetExpr* node) override {
    // Exclude the assignment columns because they are internal.
    for (int i = 0; i < node->assignment_list_size(); ++i) {
      local_columns_.insert(node->assignment_list(i)->column());
    }
    return ResolvedASTDeepCopyVisitor::VisitResolvedLetExpr(node);
  }

  // Columns that are local to an expression -- that is they are defined,
  // populated, and consumed fully within the expression -- should not be
  // correlated by this code.
  absl::flat_hash_set<ResolvedColumn> local_columns_;

  // Tracks if we're inside a subquery. We stop correlating when we're inside a
  // subquery as column references are either already correlated or don't need
  // to be.
  int in_subquery_or_lambda_ = 0;
};

// A visitor which collects the ResolvedColumnRef that are referenced, but not
// local to this expression.
class ColumnRefCollector : public ResolvedASTVisitor {
 public:
  explicit ColumnRefCollector(
      std::vector<std::unique_ptr<const ResolvedColumnRef>>* column_refs,
      bool correlate)
      : column_refs_(column_refs), correlate_(correlate) {}

 private:
  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* node) override {
    if (!local_columns_.contains(node->column())) {
      column_refs_->push_back(MakeResolvedColumnRef(
          node->type(), node->column(), correlate_ || node->is_correlated()));
    }
    return absl::OkStatus();
  }

  absl::Status VisitResolvedSubqueryExpr(
      const ResolvedSubqueryExpr* node) override {
    for (const auto& column : node->parameter_list()) {
      ZETASQL_RETURN_IF_ERROR(VisitResolvedColumnRef(column.get()));
    }
    if (node->in_expr() != nullptr) {
      ZETASQL_RETURN_IF_ERROR(node->in_expr()->Accept(this));
    }
    // Cut off traversal once we hit a subquery. Column refs inside subquery are
    // either internal or already collected in parameter_list.
    return absl::OkStatus();
  }

  absl::Status VisitResolvedInlineLambda(
      const ResolvedInlineLambda* node) override {
    for (const auto& column_ref : node->parameter_list()) {
      ZETASQL_RETURN_IF_ERROR(VisitResolvedColumnRef(column_ref.get()));
    }
    // Cut off traversal once we hit a lambda. Column refs inside lambda body
    // are either internal or already collected in parameter_list.
    return absl::OkStatus();
  }

  absl::Status VisitResolvedLetExpr(const ResolvedLetExpr* node) override {
    // Exclude the assignment columns because they are internal.
    for (int i = 0; i < node->assignment_list_size(); ++i) {
      local_columns_.insert(node->assignment_list(i)->column());
    }
    return ResolvedASTVisitor::VisitResolvedLetExpr(node);
  }

  // Columns that are local to an expression -- that is they are defined,
  // populated, and consumed fully within the expression -- should not be
  // collected by this code.
  absl::flat_hash_set<ResolvedColumn> local_columns_;

  std::vector<std::unique_ptr<const ResolvedColumnRef>>* column_refs_;
  bool correlate_;
};

}  // namespace

ResolvedColumn ColumnFactory::MakeCol(const std::string& table_name,
                                      const std::string& col_name,
                                      const Type* type) {
  if (sequence_ == nullptr) {
    ++max_col_id_;
  } else {
    while (true) {
      // Allocate from the sequence, but make sure it's higher than the max we
      // should start from.
      int next_col_id = static_cast<int>(sequence_->GetNext());
      if (next_col_id > max_col_id_) {
        max_col_id_ = next_col_id;
        break;
      }
    }
  }
  if (id_string_pool_ != nullptr) {
    return ResolvedColumn(max_col_id_, id_string_pool_->Make(table_name),
                          id_string_pool_->Make(col_name), type);
  } else {
    return ResolvedColumn(max_col_id_,
                          zetasql::IdString::MakeGlobal(table_name),
                          zetasql::IdString::MakeGlobal(col_name), type);
  }
}

absl::StatusOr<std::unique_ptr<ResolvedExpr>> CorrelateColumnRefsImpl(
    const ResolvedExpr& expr) {
  CorrelateColumnRefVisitor correlator;
  ZETASQL_RETURN_IF_ERROR(expr.Accept(&correlator));
  return correlator.ConsumeRootNode<ResolvedExpr>();
}

absl::Status CollectColumnRefs(
    const ResolvedNode& node,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>* column_refs,
    bool correlate) {
  ColumnRefCollector column_ref_collector(column_refs, correlate);
  return node.Accept(&column_ref_collector);
}

// A visitor that copies a ResolvedAST with columns ids allocated by a
// different ColumnFactory and remaps the columns so that columns in the copy
// are allocated by 'column_factory'.
class ColumnRemappingResolvedASTDeepCopyVisitor
    : public ResolvedASTDeepCopyVisitor {
 public:
  ColumnRemappingResolvedASTDeepCopyVisitor(ColumnReplacementMap& column_map,
                                            ColumnFactory& column_factory)
      : column_map_(column_map), column_factory_(column_factory) {}

  absl::StatusOr<ResolvedColumn> CopyResolvedColumn(
      const ResolvedColumn& column) override {
    if (!column_map_.contains(column)) {
      column_map_[column] = column_factory_.MakeCol(
          column.table_name(), column.name(), column.type());
    }
    return column_map_[column];
  }

 private:
  // Map from the column ID in the input ResolvedAST to the column allocated
  // from column_factory_.
  ColumnReplacementMap& column_map_;

  // All ResolvedColumns in the copied ResolvedAST will have new column ids
  // allocated by ColumnFactory.
  ColumnFactory& column_factory_;
};

absl::StatusOr<std::unique_ptr<ResolvedNode>>
CopyResolvedASTAndRemapColumnsImpl(const ResolvedNode& input_tree,
                                   ColumnFactory& column_factory,
                                   ColumnReplacementMap& column_map) {
  ColumnRemappingResolvedASTDeepCopyVisitor visitor(column_map, column_factory);
  ZETASQL_RETURN_IF_ERROR(input_tree.Accept(&visitor));
  return visitor.ConsumeRootNode<ResolvedNode>();
}

absl::StatusOr<std::unique_ptr<ResolvedFunctionCall>> FunctionCallBuilder::If(
    std::unique_ptr<const ResolvedExpr> condition,
    std::unique_ptr<const ResolvedExpr> then_case,
    std::unique_ptr<const ResolvedExpr> else_case) {
  ZETASQL_RET_CHECK_NE(condition.get(), nullptr);
  ZETASQL_RET_CHECK_NE(then_case.get(), nullptr);
  ZETASQL_RET_CHECK_NE(else_case.get(), nullptr);
  ZETASQL_RET_CHECK(condition->type()->IsBool());
  ZETASQL_RET_CHECK(then_case->type()->Equals(else_case->type()));

  const Function* if_fn;
  ZETASQL_RET_CHECK_OK(
      catalog_.FindFunction({"if"}, &if_fn, analyzer_options_.find_options()));
  ZETASQL_RET_CHECK_NE(if_fn, nullptr);
  FunctionArgumentType condition_arg(condition->type(), 1);
  FunctionArgumentType arg(then_case->type(), 1);
  FunctionSignature if_signature(arg, {condition_arg, arg, arg}, FN_IF);
  const Type* result_type = then_case->type();
  std::vector<std::unique_ptr<const ResolvedExpr>> if_args(3);
  if_args[0] = std::move(condition);
  if_args[1] = std::move(then_case);
  if_args[2] = std::move(else_case);
  return MakeResolvedFunctionCall(result_type, if_fn, if_signature,
                                  std::move(if_args),
                                  ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

absl::StatusOr<std::unique_ptr<ResolvedFunctionCall>>
FunctionCallBuilder::IsNull(std::unique_ptr<const ResolvedExpr> arg) {
  ZETASQL_RET_CHECK_NE(arg.get(), nullptr);

  const Function* is_null_fn;
  ZETASQL_RET_CHECK_OK(catalog_.FindFunction({"$is_null"}, &is_null_fn,
                                     analyzer_options_.find_options()));
  ZETASQL_RET_CHECK_NE(is_null_fn, nullptr);
  FunctionSignature is_null_signature(
      FunctionArgumentType(types::BoolType(), 1),
      {FunctionArgumentType(arg->type(), 1)}, FN_IS_NULL);
  std::vector<std::unique_ptr<const ResolvedExpr>> is_null_args(1);
  is_null_args[0] = std::move(arg);
  return MakeResolvedFunctionCall(types::BoolType(), is_null_fn,
                                  is_null_signature, std::move(is_null_args),
                                  ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

}  // namespace zetasql
