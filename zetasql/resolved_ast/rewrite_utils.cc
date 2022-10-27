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

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_builder.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

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
    std::unique_ptr<ResolvedColumnRef> resolved_column_ref =
        MakeResolvedColumnRef(ref.type(), ref.column(),
                              ShouldBeCorrelated(ref));
    resolved_column_ref->set_type_annotation_map(ref.type_annotation_map());
    return resolved_column_ref;
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

  absl::Status VisitResolvedWithExpr(const ResolvedWithExpr* node) override {
    // Exclude the assignment columns because they are internal.
    for (int i = 0; i < node->assignment_list_size(); ++i) {
      local_columns_.insert(node->assignment_list(i)->column());
    }
    return ResolvedASTDeepCopyVisitor::VisitResolvedWithExpr(node);
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
      std::unique_ptr<ResolvedColumnRef> resolved_column_ref =
          MakeResolvedColumnRef(node->type(), node->column(),
                                correlate_ || node->is_correlated());
      resolved_column_ref->set_type_annotation_map(node->type_annotation_map());
      column_refs_->push_back(std::move(resolved_column_ref));
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

  absl::Status VisitResolvedWithExpr(const ResolvedWithExpr* node) override {
    // Exclude the assignment columns because they are internal.
    for (int i = 0; i < node->assignment_list_size(); ++i) {
      local_columns_.insert(node->assignment_list(i)->column());
    }
    return ResolvedASTVisitor::VisitResolvedWithExpr(node);
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
  UpdateMaxColId();
  if (id_string_pool_ != nullptr) {
    return ResolvedColumn(max_col_id_, id_string_pool_->Make(table_name),
                          id_string_pool_->Make(col_name), type);
  } else {
    return ResolvedColumn(max_col_id_,
                          zetasql::IdString::MakeGlobal(table_name),
                          zetasql::IdString::MakeGlobal(col_name), type);
  }
}

ResolvedColumn ColumnFactory::MakeCol(const std::string& table_name,
                                      const std::string& col_name,
                                      AnnotatedType annotated_type) {
  UpdateMaxColId();
  if (id_string_pool_ != nullptr) {
    return ResolvedColumn(max_col_id_, id_string_pool_->Make(table_name),
                          id_string_pool_->Make(col_name), annotated_type);
  } else {
    return ResolvedColumn(
        max_col_id_, zetasql::IdString::MakeGlobal(table_name),
        zetasql::IdString::MakeGlobal(col_name), annotated_type);
  }
}

void ColumnFactory::UpdateMaxColId() {
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

void SortUniqueColumnRefs(
    std::vector<std::unique_ptr<const ResolvedColumnRef>>& column_refs) {
  // Compare two referenced columns.
  auto cmp = [](const std::unique_ptr<const ResolvedColumnRef>& l,
                const std::unique_ptr<const ResolvedColumnRef>& r) {
    if (l->column().column_id() != r->column().column_id()) {
      return l->column().column_id() < r->column().column_id();
    }
    return l->is_correlated() < r->is_correlated();
  };

  auto eq = [](const std::unique_ptr<const ResolvedColumnRef>& l,
               const std::unique_ptr<const ResolvedColumnRef>& r) {
    return l->column().column_id() == r->column().column_id() &&
           l->is_correlated() == r->is_correlated();
  };

  // Erase any duplicates from the referenced columns list.
  std::sort(column_refs.begin(), column_refs.end(), cmp);
  column_refs.erase(std::unique(column_refs.begin(), column_refs.end(), eq),
                    column_refs.end());
}

absl::Status CollectSortUniqueColumnRefs(
    const ResolvedNode& node,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>& column_refs,
    bool correlate) {
  ZETASQL_RETURN_IF_ERROR(CollectColumnRefs(node, &column_refs, correlate));
  SortUniqueColumnRefs(column_refs);
  return absl::OkStatus();
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
  ZETASQL_RET_CHECK(if_fn->IsZetaSQLBuiltin());
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

absl::StatusOr<std::unique_ptr<ResolvedScan>> ReplaceScanColumns(
    ColumnFactory& column_factory, const ResolvedScan& scan,
    const std::vector<int>& target_column_indices,
    const std::vector<ResolvedColumn>& replacement_columns_to_use) {
  // Initialize a map from the column ids in the VIEW/TVF definition to the
  // column ids in the invoking query to remap the columns that were consumed
  // by the TableScan.
  ZETASQL_RET_CHECK_EQ(replacement_columns_to_use.size(), target_column_indices.size());
  ColumnReplacementMap column_map;
  for (int i = 0; i < target_column_indices.size(); ++i) {
    int column_idx = target_column_indices[i];
    ZETASQL_RET_CHECK_GT(scan.column_list_size(), column_idx);
    column_map[scan.column_list(column_idx)] = replacement_columns_to_use[i];
  }

  return CopyResolvedASTAndRemapColumns(scan, column_factory, column_map);
}

std::vector<ResolvedColumn> CreateReplacementColumns(
    ColumnFactory& column_factory,
    const std::vector<ResolvedColumn>& column_list) {
  std::vector<ResolvedColumn> replacement_columns;
  replacement_columns.reserve(column_list.size());

  for (const ResolvedColumn& old_column : column_list) {
    replacement_columns.push_back(column_factory.MakeCol(
        old_column.table_name(), old_column.name(), old_column.type()));
  }

  return replacement_columns;
}

absl::StatusOr<std::unique_ptr<ResolvedFunctionCall>>
FunctionCallBuilder::IsNull(std::unique_ptr<const ResolvedExpr> arg) {
  ZETASQL_RET_CHECK_NE(arg.get(), nullptr);

  const Function* is_null_fn;
  ZETASQL_RET_CHECK_OK(catalog_.FindFunction({"$is_null"}, &is_null_fn,
                                     analyzer_options_.find_options()));
  ZETASQL_RET_CHECK_NE(is_null_fn, nullptr);
  ZETASQL_RET_CHECK(is_null_fn->IsZetaSQLBuiltin());
  FunctionSignature is_null_signature(
      FunctionArgumentType(types::BoolType(), 1),
      {FunctionArgumentType(arg->type(), 1)}, FN_IS_NULL);
  std::vector<std::unique_ptr<const ResolvedExpr>> is_null_args(1);
  is_null_args[0] = std::move(arg);
  return MakeResolvedFunctionCall(types::BoolType(), is_null_fn,
                                  is_null_signature, std::move(is_null_args),
                                  ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::IfError(std::unique_ptr<const ResolvedExpr> try_expr,
                             std::unique_ptr<const ResolvedExpr> handle_expr) {
  ZETASQL_RET_CHECK_NE(try_expr.get(), nullptr);
  ZETASQL_RET_CHECK_NE(handle_expr.get(), nullptr);
  ZETASQL_RET_CHECK(try_expr->type()->Equals(handle_expr->type()))
      << "Expected try_expr->type().Equals(handle_expr->type()) to be true, "
      << "but it was false. try_expr->type(): "
      << try_expr->type()->DebugString()
      << ", handle_expr->type(): " << handle_expr->type()->DebugString();

  const Function* iferror_fn;
  ZETASQL_RET_CHECK_OK(catalog_.FindFunction({"iferror"}, &iferror_fn,
                                     analyzer_options_.find_options()));
  ZETASQL_RET_CHECK_NE(iferror_fn, nullptr);
  ZETASQL_RET_CHECK(iferror_fn->IsZetaSQLBuiltin());

  FunctionArgumentType arg_type(try_expr->type(), 1);

  return ResolvedFunctionCallBuilder()
      .set_type(arg_type.type())
      .set_function(iferror_fn)
      .set_signature({arg_type, {arg_type, arg_type}, FN_IFERROR})
      .add_argument_list(std::move(try_expr))
      .add_argument_list(std::move(handle_expr))
      .set_function_call_info(std::make_shared<ResolvedFunctionCallInfo>())
      .Build();
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::MakeArray(
    const ArrayType* array_type,
    std::vector<std::unique_ptr<ResolvedExpr>>& elements) {
  const Function* make_array_fn;
  ZETASQL_RET_CHECK_OK(catalog_.FindFunction({"$make_array"}, &make_array_fn,
                                     analyzer_options_.find_options()))
      << "Engine does not support make_array function";
  ZETASQL_RET_CHECK(make_array_fn->IsZetaSQLBuiltin());

  ZETASQL_RET_CHECK_NE(make_array_fn, nullptr);
  FunctionArgumentType make_array_arg(array_type->element_type(),
                                      FunctionArgumentType::REPEATED,
                                      static_cast<int>(elements.size()));
  FunctionSignature make_array_signature(
      array_type, {make_array_arg},
      make_array_fn->GetSignature(0)->context_id());
  make_array_signature.SetConcreteResultType(array_type);

  return MakeResolvedFunctionCall(array_type, make_array_fn,
                                  make_array_signature, std::move(elements),
                                  ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::Like(std::unique_ptr<ResolvedExpr> input,
                          std::unique_ptr<ResolvedExpr> pattern) {
  ZETASQL_RET_CHECK_NE(input.get(), nullptr);
  ZETASQL_RET_CHECK_NE(pattern.get(), nullptr);
  ZETASQL_RET_CHECK(input->type()->Equals(pattern->type()))
      << "input type does not match pattern type. input->type(): "
      << input->type()->DebugString()
      << ", pattern->type(): " << pattern->type()->DebugString();

  FunctionSignatureId context_id;
  if (input->type()->Equals(types::StringType())) {
    context_id = FN_STRING_LIKE;
  } else if (input->type()->Equals(types::BytesType())) {
    context_id = FN_BYTE_LIKE;
  } else {
    ZETASQL_RET_CHECK_FAIL() << "input type is not STRING or BYTES. input->type(): "
                     << input->type()->DebugString();
  }

  const Function* like_fn;
  ZETASQL_RET_CHECK_OK(catalog_.FindFunction({"$like"}, &like_fn,
                                     analyzer_options_.find_options()))
      << "Engine does not support $like function";
  ZETASQL_RET_CHECK(like_fn->IsZetaSQLBuiltin());
  ZETASQL_RET_CHECK_NE(like_fn, nullptr);

  FunctionArgumentType input_arg(input->type(), 1);
  FunctionArgumentType pattern_arg(pattern->type(), 1);
  FunctionSignature like_signature(FunctionArgumentType(types::BoolType(), 1),
                                   {input_arg, pattern_arg}, context_id);
  std::vector<std::unique_ptr<const ResolvedExpr>> like_fn_args(2);
  like_fn_args[0] = std::move(input);
  like_fn_args[1] = std::move(pattern);

  return MakeResolvedFunctionCall(types::BoolType(), like_fn, like_signature,
                                  std::move(like_fn_args),
                                  ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

absl::StatusOr<std::unique_ptr<const ResolvedExpr>>
FunctionCallBuilder::CaseNoValue(
    std::vector<std::unique_ptr<const ResolvedExpr>> conditions,
    std::vector<std::unique_ptr<const ResolvedExpr>> results,
    std::unique_ptr<const ResolvedExpr> else_result) {
  ZETASQL_RET_CHECK_GT(conditions.size(), 0);
  ZETASQL_RET_CHECK_EQ(conditions.size(), results.size());
  const Function* case_fn;
  ZETASQL_RET_CHECK_OK(catalog_.FindFunction({"$case_no_value"}, &case_fn,
                                     analyzer_options_.find_options()))
      << "Engine does not support $case_no_value function";
  ZETASQL_RET_CHECK(case_fn->IsZetaSQLBuiltin());
  ZETASQL_RET_CHECK_NE(case_fn, nullptr);

  const Type* result_type = results[0]->type();
  std::vector<std::unique_ptr<const ResolvedExpr>> case_fn_args;
  for (int i = 0; i < conditions.size(); ++i) {
    ZETASQL_RET_CHECK(conditions[i]->type()->IsBool());
    ZETASQL_RET_CHECK(results[i]->type()->Equals(result_type));
    case_fn_args.push_back(std::move(conditions[i]));
    case_fn_args.push_back(std::move(results[i]));
  }

  FunctionArgumentType condition_expr_arg(types::BoolType(),
                                          FunctionArgumentType::REPEATED,
                                          static_cast<int>(conditions.size()));
  FunctionArgumentType result_arg(result_type, FunctionArgumentType::REPEATED,
                                  static_cast<int>(results.size()));
  FunctionArgumentType final_result_arg(result_type, 1);
  FunctionArgumentTypeList case_arg_types = {condition_expr_arg, result_arg};
  if (else_result != nullptr) {
    ZETASQL_RET_CHECK(else_result->type()->Equals(result_type));
    case_arg_types.push_back(final_result_arg);
    case_fn_args.push_back(std::move(else_result));
  }
  FunctionSignature case_signature(final_result_arg, case_arg_types,
                                   FN_CASE_NO_VALUE);

  return MakeResolvedFunctionCall(result_type, case_fn, case_signature,
                                  std::move(case_fn_args),
                                  ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::Not(std::unique_ptr<const ResolvedExpr> expression) {
  ZETASQL_RET_CHECK_NE(expression.get(), nullptr);
  ZETASQL_RET_CHECK(expression->type()->Equals(types::BoolType()))
      << "Type of expression is not a BOOL: expression->type(): "
      << expression->type()->DebugString();

  const Function* not_fn;
  ZETASQL_RET_CHECK_OK(catalog_.FindFunction({"$not"}, &not_fn,
                                     analyzer_options_.find_options()))
      << "Engine does not support $not function";
  ZETASQL_RET_CHECK(not_fn->IsZetaSQLBuiltin());
  ZETASQL_RET_CHECK_NE(not_fn, nullptr);

  FunctionArgumentType bool_argument_type(types::BoolType(), 1);
  FunctionSignature not_signature(bool_argument_type, {bool_argument_type},
                                  FN_NOT);
  std::vector<std::unique_ptr<const ResolvedExpr>> not_fn_args(1);
  not_fn_args[0] = std::move(expression);

  return MakeResolvedFunctionCall(types::BoolType(), not_fn, not_signature,
                                  std::move(not_fn_args),
                                  ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

absl::StatusOr<std::unique_ptr<ResolvedAggregateScan>>
LikeAnyAllSubqueryScanBuilder::BuildAggregateScan(
    ResolvedColumn& input_column, ResolvedColumn& subquery_column,
    std::unique_ptr<const ResolvedScan> input_scan,
    ResolvedSubqueryExpr::SubqueryType subquery_type) {
  std::vector<std::unique_ptr<const ResolvedComputedColumn>> aggregate_list;
  std::vector<ResolvedColumn> column_list;

  // Create a LOGICAL_OR/AND(input LIKE pattern) function using ColumnRefs to
  // the input and subquery columns, and add it as a column to the
  // AggregateScan.
  // Maps to:
  // +-like_agg_col#3=AggregateFunctionCall(
  //       LOGICAL_OR/AND(input_expr#1 LIKE pattern_col#2) -> BOOL)
  //         // OR for ANY, AND for ALL
  // in the ResolvedAST.
  std::unique_ptr<ResolvedColumnRef> like_input_column_ref =
      MakeResolvedColumnRef(input_column.type(), input_column,
                            /*is_correlated=*/true);
  std::unique_ptr<ResolvedColumnRef> subquery_column_ref_like =
      MakeResolvedColumnRef(subquery_column.type(), subquery_column,
                            /*is_correlated=*/false);
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> like_fn,
                   fn_builder_.Like(std::move(like_input_column_ref),
                                    std::move(subquery_column_ref_like)));
  FunctionSignatureId context_id;
  if (subquery_type == ResolvedSubqueryExpr::LIKE_ANY) {
    context_id = FN_LOGICAL_OR;
  } else if (subquery_type == ResolvedSubqueryExpr::LIKE_ALL) {
    context_id = FN_LOGICAL_AND;
  } else {
    ZETASQL_RET_CHECK_FAIL()
        << "Subquery type can only be LIKE_ANY or LIKE_ALL. Subquery type: "
        << ResolvedSubqueryExprEnums_SubqueryType_Name(subquery_type);
  }
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedAggregateFunctionCall>
                       logical_operation_like_fn,
                   AggregateLogicalOperation(context_id, std::move(like_fn)));
  ResolvedColumn like_column =
      column_factory_->MakeCol("aggregate", "like_agg_col", types::BoolType());
  std::unique_ptr<ResolvedComputedColumn> like_computed_column =
      MakeResolvedComputedColumn(like_column,
                                 std::move(logical_operation_like_fn));
  column_list.push_back(like_column);
  aggregate_list.push_back(std::move(like_computed_column));

  // Create a LOGICAL_OR(pattern IS NULL) function using ColumnRefs to the
  // subquery column, and add it as a column to the AggregateScan.
  // Maps to:
  // +-null_agg_col#4=AggregateFunctionCall(
  //       LOGICAL_OR(pattern_col#2 IS NULL) -> BOOL)
  // in the ResolvedAST.
  std::unique_ptr<ResolvedColumnRef> subquery_column_ref_contains_null =
      MakeResolvedColumnRef(subquery_column.type(), subquery_column,
                            /*is_correlated=*/false);
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedExpr> is_null_fn,
      fn_builder_.IsNull(std::move(subquery_column_ref_contains_null)));
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedAggregateFunctionCall> contains_null_fn,
      AggregateLogicalOperation(FN_LOGICAL_OR, std::move(is_null_fn)));
  ResolvedColumn contains_null_column =
      column_factory_->MakeCol("aggregate", "null_agg_col", types::BoolType());
  std::unique_ptr<ResolvedComputedColumn> contains_null_computed_column =
      MakeResolvedComputedColumn(contains_null_column,
                                 std::move(contains_null_fn));
  aggregate_list.push_back(std::move(contains_null_computed_column));
  column_list.push_back(contains_null_column);

  // Maps to:
  // AggregateScan
  //   +-input_scan=SubqueryScan  // User input subquery
  //     +-pattern_col#2=subquery_column
  //   +-like_agg_col#3=AggregateFunctionCall(
  //         LOGICAL_OR/AND(input_expr#1 LIKE pattern_col#2) -> BOOL)
  //           // OR for ANY, AND for ALL
  //   +-null_agg_col#4=AggregateFunctionCall(
  //         LOGICAL_OR(pattern_col#2 IS NULL) -> BOOL)
  // in the ResolvedAST
  return MakeResolvedAggregateScan(column_list, std::move(input_scan),
                                   /*group_by_list=*/{},
                                   std::move(aggregate_list),
                                   /*grouping_set_list=*/{},
                                   /*rollup_column_list=*/{});
}

absl::StatusOr<std::unique_ptr<const ResolvedAggregateFunctionCall>>
LikeAnyAllSubqueryScanBuilder::AggregateLogicalOperation(
    FunctionSignatureId context_id,
    std::unique_ptr<const ResolvedExpr> expression) {
  ZETASQL_RET_CHECK_EQ(expression->type(), types::BoolType());

  std::string logical_fn;
  if (context_id == FN_LOGICAL_OR) {
    logical_fn = "logical_or";
  } else if (context_id == FN_LOGICAL_AND) {
    logical_fn = "logical_and";
  } else {
    ZETASQL_RET_CHECK_FAIL() << "Function context_id did not match LOGICAL_OR or "
                        "LOGICAL_AND. context_id: "
                     << FunctionSignatureId_Name(context_id);
  }

  const Function* logical_operation_fn;
  ZETASQL_RET_CHECK_OK(catalog_->FindFunction({logical_fn}, &logical_operation_fn,
                                      analyzer_options_->find_options()))
      << "Engine does not support " << logical_fn << " function";
  ZETASQL_RET_CHECK(logical_operation_fn->IsZetaSQLBuiltin());
  ZETASQL_RET_CHECK_NE(logical_operation_fn, nullptr);

  FunctionSignature logical_operation_signature(
      {types::BoolType(), 1}, {{types::BoolType(), 1}}, context_id);
  std::vector<std::unique_ptr<const ResolvedExpr>> logical_operation_args;
  logical_operation_args.push_back(std::move(expression));

  return MakeResolvedAggregateFunctionCall(
      types::BoolType(), logical_operation_fn, logical_operation_signature,
      std::move(logical_operation_args),
      ResolvedFunctionCallBaseEnums::DEFAULT_ERROR_MODE, /*distinct=*/false,
      ResolvedNonScalarFunctionCallBaseEnums::DEFAULT_NULL_HANDLING,
      /*having_modifier=*/nullptr, /*order_by_item_list=*/{},
      /*limit=*/nullptr);
}

bool IsBuiltInFunctionIdEq(const ResolvedFunctionCall* const function_call,
                           FunctionSignatureId function_signature_id) {
  ZETASQL_DCHECK(function_call->function() != nullptr)
      << "Expected function_call->function() to not be null";
  return function_call->function() != nullptr &&
         function_call->signature().context_id() == function_signature_id &&
         function_call->function()->IsZetaSQLBuiltin();
}

zetasql_base::StatusBuilder MakeUnimplementedErrorAtNode(const ResolvedNode* node) {
  zetasql_base::StatusBuilder builder = zetasql_base::UnimplementedErrorBuilder();
  if (node != nullptr && node->GetParseLocationOrNULL() != nullptr) {
    builder.Attach(
        node->GetParseLocationOrNULL()->start().ToInternalErrorLocation());
  }
  return builder;
}

}  // namespace zetasql
