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
#include "zetasql/public/annotation/collation.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_builder.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_ast_helper.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_collation.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace {

// A visitor to check whether the ResolvedAST has ResolvedGroupingCall nodes.
class GroupingCallDetectorVisitor : public ResolvedASTVisitor {
 public:
  explicit GroupingCallDetectorVisitor(bool* has_grouping_call)
      : has_grouping_call_(has_grouping_call) {}

  absl::Status VisitResolvedAggregateScan(
      const ResolvedAggregateScan* node) override {
    if (!node->grouping_call_list().empty()) {
      *has_grouping_call_ = true;
    }
    return DefaultVisit(node);
  }

 private:
  bool* has_grouping_call_;
};

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
    // Column references of outer columns are already correlated.
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
class ColumnRefCollectorOwned : public ColumnRefVisitor {
 public:
  explicit ColumnRefCollectorOwned(
      std::vector<std::unique_ptr<const ResolvedColumnRef>>* column_refs,
      bool correlate)
      : column_refs_(column_refs), correlate_(correlate) {}

 private:
  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* node) override {
    if (!IsLocalColumn(node->column())) {
      std::unique_ptr<ResolvedColumnRef> resolved_column_ref =
          MakeResolvedColumnRef(node->type(), node->column(),
                                correlate_ || node->is_correlated());
      resolved_column_ref->set_type_annotation_map(node->type_annotation_map());
      column_refs_->push_back(std::move(resolved_column_ref));
    }
    return absl::OkStatus();
  }

  std::vector<std::unique_ptr<const ResolvedColumnRef>>* column_refs_;
  bool correlate_;
};

}  // namespace

ResolvedColumn ColumnFactory::MakeCol(absl::string_view table_name,
                                      absl::string_view col_name,
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

ResolvedColumn ColumnFactory::MakeCol(absl::string_view table_name,
                                      absl::string_view col_name,
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
  ColumnRefCollectorOwned column_ref_collector(column_refs, correlate);
  return node.Accept(&column_ref_collector);
}

absl::Status RemoveUnusedColumnRefs(
    const ResolvedNode& node,
    std::vector<std::unique_ptr<const ResolvedColumnRef>>& column_refs) {
  std::vector<std::unique_ptr<const ResolvedColumnRef>> refs;
  ZETASQL_RETURN_IF_ERROR(CollectColumnRefs(node, &refs));
  absl::flat_hash_set<int> referenced_column_ids;
  for (const auto& ref : refs) {
    referenced_column_ids.insert(ref->column().column_id());
  }

  column_refs.erase(std::remove_if(column_refs.begin(), column_refs.end(),
                                   [&](const auto& ref) {
                                     return !referenced_column_ids.contains(
                                         ref->column().column_id());
                                   }),
                    column_refs.end());
  return absl::OkStatus();
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
          column.table_name(), column.name(), column.annotated_type());
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

// TODO: Propagate annotations correctly for this function, if
// needed, after creating resolved function node.
absl::StatusOr<std::unique_ptr<ResolvedFunctionCall>> FunctionCallBuilder::If(
    std::unique_ptr<const ResolvedExpr> condition,
    std::unique_ptr<const ResolvedExpr> then_case,
    std::unique_ptr<const ResolvedExpr> else_case) {
  ZETASQL_RET_CHECK_NE(condition.get(), nullptr);
  ZETASQL_RET_CHECK_NE(then_case.get(), nullptr);
  ZETASQL_RET_CHECK_NE(else_case.get(), nullptr);
  ZETASQL_RET_CHECK(condition->type()->IsBool());
  ZETASQL_RET_CHECK(then_case->type()->Equals(else_case->type()))
      << "Inconsistent types of then_case and else_case: "
      << then_case->type()->DebugString() << " vs "
      << else_case->type()->DebugString();

  const Function* if_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("if", &if_fn));
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
    absl::Span<const ResolvedColumn> column_list) {
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

  const Function* is_null_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("$is_null", &is_null_fn));
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
FunctionCallBuilder::AnyIsNull(
    std::vector<std::unique_ptr<const ResolvedExpr>> args) {
  std::vector<std::unique_ptr<const ResolvedExpr>> is_nulls;
  is_nulls.reserve(args.size());
  for (const auto& arg : args) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> arg_copy,
                     ResolvedASTDeepCopyVisitor::Copy(arg.get()));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const ResolvedExpr> is_null,
                     IsNull(std::move(arg_copy)));
    is_nulls.push_back(std::move(is_null));
  }
  return Or(std::move(is_nulls));
}

// TODO: Propagate annotations correctly for this function, if
// needed, after creating resolved function node.
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

  const Function* iferror_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("iferror", &iferror_fn));

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
FunctionCallBuilder::Error(const std::string& error_text,
                           const Type* target_type) {
  std::unique_ptr<const ResolvedExpr> error_expr =
      MakeResolvedLiteral(types::StringType(), Value::StringValue(error_text));
  return Error(std::move(error_expr), target_type);
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::Error(std::unique_ptr<const ResolvedExpr> error_expr,
                           const Type* target_type) {
  ZETASQL_RET_CHECK_NE(error_expr.get(), nullptr);
  ZETASQL_RET_CHECK(error_expr->type()->IsString());

  const Function* error_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("error", &error_fn));
  FunctionArgumentType arg_type(types::StringType(), /*num_occurrences=*/1);
  if (target_type == nullptr) {
    target_type = types::Int64Type();
  }
  FunctionArgumentType return_type(target_type, /*num_occurrences=*/1);
  return ResolvedFunctionCallBuilder()
      .set_type(return_type.type())
      .set_function(error_fn)
      .set_signature({return_type, {arg_type}, FN_ERROR})
      .add_argument_list(std::move(error_expr))
      .set_error_mode(ResolvedFunctionCall::DEFAULT_ERROR_MODE)
      .set_function_call_info(std::make_shared<ResolvedFunctionCallInfo>())
      .Build();
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::MakeArray(
    const Type* element_type,
    std::vector<std::unique_ptr<const ResolvedExpr>>& elements) {
  ZETASQL_RET_CHECK(element_type != nullptr);
  const Function* make_array_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("$make_array", &make_array_fn));
  ZETASQL_RET_CHECK(make_array_fn != nullptr);

  // make_array has only one signature in catalog.
  ZETASQL_RET_CHECK_EQ(make_array_fn->signatures().size(), 1);
  const FunctionSignature* catalog_signature = make_array_fn->GetSignature(0);
  ZETASQL_RET_CHECK(catalog_signature != nullptr);

  // Construct arguments type and result type to pass to FunctionSignature.
  const ArrayType* array_type;
  ZETASQL_RETURN_IF_ERROR(type_factory_.MakeArrayType(element_type, &array_type));
  FunctionArgumentType result_type(array_type,
                                   catalog_signature->result_type().options(),
                                   /*num_occurrences=*/1);
  FunctionArgumentType arguments_type(array_type->element_type(),
                                      catalog_signature->argument(0).options(),
                                      static_cast<int>(elements.size()));
  FunctionSignature make_array_signature(result_type, {arguments_type},
                                         catalog_signature->context_id(),
                                         catalog_signature->options());

  std::unique_ptr<ResolvedFunctionCall> resolved_function =
      MakeResolvedFunctionCall(array_type, make_array_fn, make_array_signature,
                               std::move(elements),
                               ResolvedFunctionCall::DEFAULT_ERROR_MODE);
  ZETASQL_RETURN_IF_ERROR(annotation_propagator_.CheckAndPropagateAnnotations(
      /*error_node=*/nullptr, resolved_function.get()));
  return resolved_function;
}

// TODO: Propagate annotations correctly for this function, if
// needed, after creating resolved function node.
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

  const Function* like_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("$like", &like_fn));

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

// TODO: Propagate annotations correctly for this function, if
// needed, after creating resolved function node.
absl::StatusOr<std::unique_ptr<const ResolvedExpr>>
FunctionCallBuilder::CaseNoValue(
    std::vector<std::unique_ptr<const ResolvedExpr>> conditions,
    std::vector<std::unique_ptr<const ResolvedExpr>> results,
    std::unique_ptr<const ResolvedExpr> else_result) {
  ZETASQL_RET_CHECK_GT(conditions.size(), 0);
  ZETASQL_RET_CHECK_EQ(conditions.size(), results.size());
  const Function* case_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("$case_no_value", &case_fn));

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

  const Function* not_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("$not", &not_fn));

  FunctionArgumentType bool_argument_type(types::BoolType(), 1);
  FunctionSignature not_signature(bool_argument_type, {bool_argument_type},
                                  FN_NOT);
  std::vector<std::unique_ptr<const ResolvedExpr>> not_fn_args(1);
  not_fn_args[0] = std::move(expression);

  return MakeResolvedFunctionCall(types::BoolType(), not_fn, not_signature,
                                  std::move(not_fn_args),
                                  ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

// TODO: Propagate annotations correctly for this function, if
// needed, after creating resolved function node.
absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::Equal(std::unique_ptr<const ResolvedExpr> left_expr,
                           std::unique_ptr<const ResolvedExpr> right_expr) {
  ZETASQL_RET_CHECK_NE(left_expr.get(), nullptr);
  ZETASQL_RET_CHECK_NE(right_expr.get(), nullptr);
  ZETASQL_RET_CHECK(left_expr->type()->Equals(right_expr->type()));
  ZETASQL_RET_CHECK(left_expr->type()->SupportsEquality());

  const Function* equal_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("$equal", &equal_fn));

  FunctionSignature equal_signature(
      {types::BoolType(), 1}, {{left_expr->type(), 1}, {right_expr->type(), 1}},
      FN_EQUAL);
  std::vector<std::unique_ptr<const ResolvedExpr>> equal_fn_args(2);
  equal_fn_args[0] = std::move(left_expr);
  equal_fn_args[1] = std::move(right_expr);

  return ResolvedFunctionCallBuilder()
      .set_type(types::BoolType())
      .set_function(equal_fn)
      .set_signature(equal_signature)
      .set_argument_list(std::move(equal_fn_args))
      .set_error_mode(ResolvedFunctionCall::DEFAULT_ERROR_MODE)
      .set_function_call_info(std::make_shared<ResolvedFunctionCallInfo>())
      .Build();
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::NotEqual(std::unique_ptr<const ResolvedExpr> left_expr,
                              std::unique_ptr<const ResolvedExpr> right_expr) {
  ZETASQL_RET_CHECK_NE(left_expr.get(), nullptr);
  ZETASQL_RET_CHECK_NE(right_expr.get(), nullptr);
  ZETASQL_RET_CHECK(left_expr->type()->Equals(right_expr->type()));
  ZETASQL_RET_CHECK(left_expr->type()->SupportsEquality());

  const Function* not_equal_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("$not_equal", &not_equal_fn));

  // Only the first signature has collation enabled in function signature
  // options.
  ZETASQL_RET_CHECK_GT(not_equal_fn->signatures().size(), 1);
  const FunctionSignature* catalog_signature = not_equal_fn->GetSignature(0);
  ZETASQL_RET_CHECK(catalog_signature != nullptr);
  ZETASQL_RET_CHECK_EQ(catalog_signature->arguments().size(), 2);

  FunctionArgumentType result_type(types::BoolType(),
                                   catalog_signature->result_type().options(),
                                   /*num_occurrences=*/1);
  FunctionArgumentType left_arg_type(left_expr->type(),
                                     catalog_signature->argument(0).options(),
                                     /*num_occurrences=*/1);
  FunctionArgumentType right_arg_type(right_expr->type(),
                                      catalog_signature->argument(1).options(),
                                      /*num_occurrences=*/1);

  FunctionSignature not_equal_signature(
      result_type, {left_arg_type, right_arg_type},
      catalog_signature->context_id(), catalog_signature->options());
  std::vector<std::unique_ptr<const ResolvedExpr>> args;
  args.reserve(2);
  args.push_back(std::move(left_expr));
  args.push_back(std::move(right_expr));

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedFunctionCall> resolved_function,
      ResolvedFunctionCallBuilder()
          .set_type(types::BoolType())
          .set_function(not_equal_fn)
          .set_signature(not_equal_signature)
          .set_argument_list(std::move(args))
          .set_error_mode(ResolvedFunctionCall::DEFAULT_ERROR_MODE)
          .set_function_call_info(std::make_shared<ResolvedFunctionCallInfo>())
          .BuildMutable());
  // Attach type annotation to `collation_list` if there is any and it is
  // consistent in all arguments with annotation.
  auto annotation_map = CollationAnnotation().GetCollationFromFunctionArguments(
      /*error_location=*/nullptr, *resolved_function,
      FunctionEnums::AFFECTS_OPERATION);
  if (annotation_map.ok() && annotation_map.value() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        ResolvedCollation resolved_collation,
        ResolvedCollation::MakeResolvedCollation(*annotation_map.value()));
    resolved_function->add_collation_list(std::move(resolved_collation));
  }
  // We don't need to propagate type annotation map for this function because
  // the return type is not STRING.
  return resolved_function;
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::FunctionCallWithSameTypeArgumentsSupportingOrdering(
    std::vector<std::unique_ptr<const ResolvedExpr>> expressions,
    absl::string_view builtin_function_name) {
  ZETASQL_RET_CHECK_GE(expressions.size(), 1);
  ZETASQL_RET_CHECK_NE(expressions[0].get(), nullptr);

  const Type* type = expressions[0]->type();
  ZETASQL_RET_CHECK(type->SupportsOrdering(analyzer_options_.language(),
                                   /*type_description=*/nullptr));
  for (int i = 1; i < expressions.size(); ++i) {
    ZETASQL_RET_CHECK(expressions[i]->type()->Equals(type))
        << "Type of expression " << i << " is not the same as the first one: "
        << expressions[i]->type()->DebugString() << " vs "
        << type->DebugString();
  }
  const Function* fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog(builtin_function_name, &fn));
  ZETASQL_RET_CHECK(fn != nullptr);

  ZETASQL_RET_CHECK_EQ(fn->signatures().size(), 1);
  const FunctionSignature* catalog_signature = fn->GetSignature(0);
  ZETASQL_RET_CHECK(catalog_signature != nullptr);

  // Construct arguments type and result type to pass to FunctionSignature.
  FunctionArgumentType result_type(
      type, catalog_signature->result_type().options(), /*num_occurrences=*/1);
  FunctionArgumentType arguments_type(type,
                                      catalog_signature->argument(0).options(),
                                      static_cast<int>(expressions.size()));
  FunctionSignature concrete_signature(result_type, {arguments_type},
                                       catalog_signature->context_id(),
                                       catalog_signature->options());
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedFunctionCall> resolved_function,
      ResolvedFunctionCallBuilder()
          .set_type(type)
          .set_function(fn)
          .set_signature(concrete_signature)
          .set_argument_list(std::move(expressions))
          .set_error_mode(ResolvedFunctionCall::DEFAULT_ERROR_MODE)
          .set_function_call_info(std::make_shared<ResolvedFunctionCallInfo>())
          .BuildMutable());

  ZETASQL_RETURN_IF_ERROR(annotation_propagator_.CheckAndPropagateAnnotations(
      /*error_node=*/nullptr, resolved_function.get()));
  return resolved_function;
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::Least(
    std::vector<std::unique_ptr<const ResolvedExpr>> expressions) {
  return FunctionCallWithSameTypeArgumentsSupportingOrdering(
      std::move(expressions), "least");
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::Greatest(
    std::vector<std::unique_ptr<const ResolvedExpr>> expressions) {
  return FunctionCallWithSameTypeArgumentsSupportingOrdering(
      std::move(expressions), "greatest");
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::Coalesce(
    std::vector<std::unique_ptr<const ResolvedExpr>> expressions) {
  ZETASQL_RET_CHECK_GE(expressions.size(), 1);
  ZETASQL_RET_CHECK_NE(expressions[0].get(), nullptr);

  InputArgumentTypeSet arg_set;
  for (int i = 0; i < expressions.size(); ++i) {
    arg_set.Insert(InputArgumentType(expressions[i]->type()));
  }
  const Type* super_type = nullptr;
  ZETASQL_RETURN_IF_ERROR(coercer_.GetCommonSuperType(arg_set, &super_type));

  const Function* coalesce_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("coalesce", &coalesce_fn));
  ZETASQL_RET_CHECK(coalesce_fn != nullptr);

  ZETASQL_RET_CHECK_EQ(coalesce_fn->signatures().size(), 1);
  const FunctionSignature* catalog_signature = coalesce_fn->GetSignature(0);
  ZETASQL_RET_CHECK(catalog_signature != nullptr);

  // Construct arguments type and result type to pass to FunctionSignature.
  FunctionArgumentType result_type(super_type,
                                   catalog_signature->result_type().options(),
                                   /*num_occurrences=*/1);
  FunctionArgumentType arguments_type(super_type,
                                      catalog_signature->argument(0).options(),
                                      static_cast<int>(expressions.size()));
  FunctionSignature coalesce_signature(result_type, {arguments_type},
                                       catalog_signature->context_id(),
                                       catalog_signature->options());
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedFunctionCall> resolved_function,
      ResolvedFunctionCallBuilder()
          .set_type(super_type)
          .set_function(coalesce_fn)
          .set_signature(coalesce_signature)
          .set_argument_list(std::move(expressions))
          .set_error_mode(ResolvedFunctionCall::DEFAULT_ERROR_MODE)
          .set_function_call_info(std::make_shared<ResolvedFunctionCallInfo>())
          .BuildMutable());

  ZETASQL_RETURN_IF_ERROR(annotation_propagator_.CheckAndPropagateAnnotations(
      /*error_node=*/nullptr, resolved_function.get()));
  return resolved_function;
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::Less(std::unique_ptr<const ResolvedExpr> left_expr,
                          std::unique_ptr<const ResolvedExpr> right_expr) {
  ZETASQL_RET_CHECK_NE(left_expr.get(), nullptr);
  ZETASQL_RET_CHECK_NE(right_expr.get(), nullptr);
  ZETASQL_RET_CHECK(left_expr->type()->Equals(right_expr->type()))
      << "Type of expression are not the same: "
      << left_expr->type()->DebugString() << " vs "
      << right_expr->type()->DebugString();
  std::string unused_type_description;
  ZETASQL_RET_CHECK(left_expr->type()->SupportsOrdering(analyzer_options_.language(),
                                                &unused_type_description));

  const Function* less_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("$less", &less_fn));

  // Only the first signature has collation enabled in function signature
  // options.
  ZETASQL_RET_CHECK_GT(less_fn->signatures().size(), 1);
  const FunctionSignature* catalog_signature = less_fn->GetSignature(0);
  ZETASQL_RET_CHECK(catalog_signature != nullptr);
  ZETASQL_RET_CHECK_EQ(catalog_signature->arguments().size(), 2);

  FunctionArgumentType result_type(types::BoolType(),
                                   catalog_signature->result_type().options(),
                                   /*num_occurrences=*/1);
  FunctionArgumentType left_arg_type(left_expr->type(),
                                     catalog_signature->argument(0).options(),
                                     /*num_occurrences=*/1);
  FunctionArgumentType right_arg_type(right_expr->type(),
                                      catalog_signature->argument(1).options(),
                                      /*num_occurrences=*/1);

  FunctionSignature less_signature(result_type, {left_arg_type, right_arg_type},
                                   catalog_signature->context_id(),
                                   catalog_signature->options());
  std::vector<std::unique_ptr<const ResolvedExpr>> args;
  args.reserve(2);
  args.push_back(std::move(left_expr));
  args.push_back(std::move(right_expr));

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ResolvedFunctionCall> resolved_function,
      ResolvedFunctionCallBuilder()
          .set_type(types::BoolType())
          .set_function(less_fn)
          .set_signature(less_signature)
          .set_argument_list(std::move(args))
          .set_error_mode(ResolvedFunctionCall::DEFAULT_ERROR_MODE)
          .set_function_call_info(std::make_shared<ResolvedFunctionCallInfo>())
          .BuildMutable());
  // Attach type annotation to `collation_list` if there is any and it is
  // consistent in all arguments with annotation.
  auto annotation_map = CollationAnnotation().GetCollationFromFunctionArguments(
      /*error_location=*/nullptr, *resolved_function,
      FunctionEnums::AFFECTS_OPERATION);
  if (annotation_map.ok() && annotation_map.value() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        ResolvedCollation resolved_collation,
        ResolvedCollation::MakeResolvedCollation(*annotation_map.value()));
    resolved_function->add_collation_list(std::move(resolved_collation));
  }
  // We don't need to propagate type annotation map for this function because
  // the return type is not STRING.
  return resolved_function;
}

namespace {
absl::StatusOr<FunctionSignature> GetBinaryFunctionSignatureFromArgumentTypes(
    const Function* function, const Type* left_expr_type,
    const Type* right_expr_type) {
  // Go through the list of possible function signatures and check if a
  // signature with 2 arguments that match the types `left_expr_type` and
  // `right_expr_type` is present. If so, return the signature. Otherwise return
  // an error.
  for (const FunctionSignature& signature : function->signatures()) {
    FunctionArgumentTypeList function_argument_type_list =
        signature.arguments();
    if (function_argument_type_list.size() != 2) {
      continue;
    }
    if (function_argument_type_list[0].type() == nullptr ||
        function_argument_type_list[1].type() == nullptr) {
      // Types can be null, if they are unspecified (e.g. ANY).
      // Such types are ignored here since an exact match is desired.
      continue;
    }
    if (left_expr_type->Equals(function_argument_type_list[0].type()) &&
        right_expr_type->Equals(function_argument_type_list[1].type())) {
      return signature;
    }
  }
  ZETASQL_RET_CHECK_FAIL() << "No builtin function with name " << function->Name()
                   << " and argument types " << left_expr_type->DebugString()
                   << " and " << right_expr_type->DebugString() << " available";
}
}  // namespace

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::GreaterOrEqual(
    std::unique_ptr<const ResolvedExpr> left_expr,
    std::unique_ptr<const ResolvedExpr> right_expr) {
  ZETASQL_RET_CHECK_NE(left_expr.get(), nullptr);
  ZETASQL_RET_CHECK_NE(right_expr.get(), nullptr);

  std::string unused_type_description;
  ZETASQL_RET_CHECK(left_expr->type()->SupportsOrdering(analyzer_options_.language(),
                                                &unused_type_description))
      << "GreaterOrEqual called for non-order-able type "
      << left_expr->type()->DebugString();
  ZETASQL_RET_CHECK(right_expr->type()->SupportsOrdering(analyzer_options_.language(),
                                                 &unused_type_description))
      << "GreaterOrEqual called for non-order-able type "
      << right_expr->type()->DebugString();

  const Function* greater_or_equal_function = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("$greater_or_equal",
                                                &greater_or_equal_function));
  std::unique_ptr<FunctionSignature> signature;
  if (!left_expr->type()->Equals(right_expr->type())) {
    // Unequal types can happen, but are only supported if the respective
    // function signature can be found in the catalog. An example of this is the
    // signature FunctionSignatureId::FN_GREATER_OR_EQUAL_INT64_UINT64.
    ZETASQL_ASSIGN_OR_RETURN(
        FunctionSignature unequal_types_signature,
        GetBinaryFunctionSignatureFromArgumentTypes(
            greater_or_equal_function, left_expr->type(), right_expr->type()));
    signature = std::make_unique<FunctionSignature>(unequal_types_signature);
  } else {
    signature = std::make_unique<FunctionSignature>(
        FunctionSignature({types::BoolType(), 1},
                          {{left_expr->type(), 1}, {right_expr->type(), 1}},
                          FN_GREATER_OR_EQUAL));
  }

  std::vector<std::unique_ptr<const ResolvedExpr>> arguments;
  arguments.emplace_back(std::move(left_expr));
  arguments.emplace_back(std::move(right_expr));

  return MakeResolvedFunctionCall(
      signature->result_type().type(), greater_or_equal_function, *signature,
      std::move(arguments), ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::Subtract(std::unique_ptr<const ResolvedExpr> minuend,
                              std::unique_ptr<const ResolvedExpr> subtrahend) {
  ZETASQL_RET_CHECK_NE(minuend.get(), nullptr);
  ZETASQL_RET_CHECK_NE(subtrahend.get(), nullptr);
  const Function* subtract_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("$subtract", &subtract_fn));
  ZETASQL_ASSIGN_OR_RETURN(FunctionSignature signature,
                   GetBinaryFunctionSignatureFromArgumentTypes(
                       subtract_fn, minuend->type(), subtrahend->type()));

  std::vector<std::unique_ptr<const ResolvedExpr>> arguments;
  arguments.emplace_back(std::move(minuend));
  arguments.emplace_back(std::move(subtrahend));

  return MakeResolvedFunctionCall(signature.result_type().type(), subtract_fn,
                                  signature, std::move(arguments),
                                  ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::SafeSubtract(
    std::unique_ptr<const ResolvedExpr> minuend,
    std::unique_ptr<const ResolvedExpr> subtrahend) {
  ZETASQL_RET_CHECK_NE(minuend.get(), nullptr);
  ZETASQL_RET_CHECK_NE(subtrahend.get(), nullptr);
  const Function* safe_subtract_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(
      GetBuiltinFunctionFromCatalog("safe_subtract", &safe_subtract_fn));
  ZETASQL_ASSIGN_OR_RETURN(FunctionSignature signature,
                   GetBinaryFunctionSignatureFromArgumentTypes(
                       safe_subtract_fn, minuend->type(), subtrahend->type()));

  std::vector<std::unique_ptr<const ResolvedExpr>> arguments;
  arguments.emplace_back(std::move(minuend));
  arguments.emplace_back(std::move(subtrahend));

  // Below, the error mode is chosen to be `DEFAULT_ERROR_MODE`, because
  // F1 does not support `SAFE_ERROR_MODE` in combination with `SAFE_SUBTRACT`.
  return MakeResolvedFunctionCall(
      signature.result_type().type(), safe_subtract_fn, signature,
      std::move(arguments), ResolvedFunctionCall::DEFAULT_ERROR_MODE);
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::And(
    std::vector<std::unique_ptr<const ResolvedExpr>> expressions) {
  return NaryLogic("$and", FN_AND, std::move(expressions));
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::Or(
    std::vector<std::unique_ptr<const ResolvedExpr>> expressions) {
  return NaryLogic("$or", FN_OR, std::move(expressions));
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::NaryLogic(
    absl::string_view op_catalog_name, FunctionSignatureId op_function_id,
    std::vector<std::unique_ptr<const ResolvedExpr>> expressions) {
  ZETASQL_RET_CHECK_GE(expressions.size(), 2);
  ZETASQL_RET_CHECK(absl::c_all_of(expressions, [](const auto& expr) {
    return expr->type()->Equals(types::BoolType());
  }));

  const Function* fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog(op_catalog_name, &fn));

  FunctionSignature signature(
      {types::BoolType(), 1},
      {{types::BoolType(), FunctionArgumentType::REPEATED,
        static_cast<int>(expressions.size())}},
      op_function_id);
  return ResolvedFunctionCallBuilder()
      .set_type(types::BoolType())
      .set_function(fn)
      .set_signature(signature)
      .set_argument_list(std::move(expressions))
      .set_error_mode(ResolvedFunctionCall::DEFAULT_ERROR_MODE)
      .set_function_call_info(std::make_shared<ResolvedFunctionCallInfo>())
      .Build();
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::ArrayLength(
    std::unique_ptr<const ResolvedExpr> array_expr) {
  ZETASQL_RET_CHECK_NE(array_expr.get(), nullptr);
  ZETASQL_RET_CHECK(array_expr->type()->IsArray());
  const Function* array_length_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(
      GetBuiltinFunctionFromCatalog("array_length", &array_length_fn));

  ZETASQL_RET_CHECK_EQ(array_length_fn->signatures().size(), 1);
  const FunctionSignature* catalog_signature = array_length_fn->GetSignature(0);
  ZETASQL_RET_CHECK(catalog_signature != nullptr);
  ZETASQL_RET_CHECK_EQ(catalog_signature->arguments().size(), 1);

  FunctionArgumentType result_type(types::Int64Type(),
                                   catalog_signature->result_type().options(),
                                   /*num_occurrences=*/1);
  FunctionArgumentType arg_type(array_expr->type(),
                                catalog_signature->argument(0).options(),
                                /*num_occurrences=*/1);

  FunctionSignature concrete_signature(result_type, {arg_type},
                                       catalog_signature->context_id(),
                                       catalog_signature->options());
  std::vector<std::unique_ptr<const ResolvedExpr>> args;
  args.push_back(std::move(array_expr));

  return ResolvedFunctionCallBuilder()
      .set_type(types::Int64Type())
      .set_function(array_length_fn)
      .set_signature(concrete_signature)
      .set_argument_list(std::move(args))
      .set_error_mode(ResolvedFunctionCall::DEFAULT_ERROR_MODE)
      .set_function_call_info(std::make_shared<ResolvedFunctionCallInfo>())
      .Build();
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::ArrayAtOffset(
    std::unique_ptr<const ResolvedExpr> array_expr,
    std::unique_ptr<const ResolvedExpr> offset_expr) {
  ZETASQL_RET_CHECK(array_expr->type()->IsArray());
  ZETASQL_RET_CHECK_EQ(offset_expr->type(), types::Int64Type());

  const Function* array_at_offset_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(
      GetBuiltinFunctionFromCatalog("$array_at_offset", &array_at_offset_fn));

  ZETASQL_RET_CHECK_EQ(array_at_offset_fn->signatures().size(), 1);
  const FunctionSignature* catalog_signature =
      array_at_offset_fn->GetSignature(0);
  ZETASQL_RET_CHECK(catalog_signature != nullptr);
  ZETASQL_RET_CHECK_EQ(catalog_signature->arguments().size(), 2);

  FunctionArgumentType result_type(
      array_expr->type()->AsArray()->element_type(),
      catalog_signature->result_type().options(),
      /*num_occurrences=*/1);
  FunctionArgumentType array_arg(array_expr->type(),
                                 catalog_signature->argument(0).options(),
                                 /*num_occurrences=*/1);
  FunctionArgumentType offset_arg(offset_expr->type(),
                                  catalog_signature->argument(1).options(),
                                  /*num_occurrences=*/1);

  FunctionSignature concrete_signature(result_type, {array_arg, offset_arg},
                                       catalog_signature->context_id(),
                                       catalog_signature->options());
  std::vector<std::unique_ptr<const ResolvedExpr>> args;
  args.push_back(std::move(array_expr));
  args.push_back(std::move(offset_expr));

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<const ResolvedFunctionCall> resolved_function,
      ResolvedFunctionCallBuilder()
          .set_type(result_type.type())
          .set_function(array_at_offset_fn)
          .set_signature(concrete_signature)
          .set_argument_list(std::move(args))
          .set_error_mode(ResolvedFunctionCall::DEFAULT_ERROR_MODE)
          .set_function_call_info(std::make_shared<ResolvedFunctionCallInfo>())
          .Build());

  ZETASQL_RETURN_IF_ERROR(annotation_propagator_.CheckAndPropagateAnnotations(
      /*error_node=*/nullptr,
      const_cast<ResolvedFunctionCall*>(resolved_function.get())));
  return resolved_function;
}

// Returns the FunctionSignatureId of MOD corresponding to `input_type`.
static absl::StatusOr<FunctionSignatureId> GetModSignatureIdForInputType(
    const Function* mod_fn, const Type* input_type) {
  if (input_type == types::Int64Type()) {
    return FN_MOD_INT64;
  }
  if (input_type == types::Uint64Type()) {
    return FN_MOD_UINT64;
  }
  if (input_type == types::NumericType()) {
    return FN_MOD_NUMERIC;
  }
  if (input_type == types::BigNumericType()) {
    return FN_MOD_BIGNUMERIC;
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "Unsupported input type for mod: ", input_type->DebugString()));
}

// Returns the FunctionSignature of MOD corresponding to `input_type`.
static absl::StatusOr<const FunctionSignature*> GetModSignature(
    const Function* mod_fn, const Type* input_type) {
  ZETASQL_ASSIGN_OR_RETURN(FunctionSignatureId mod_signature_id,
                   GetModSignatureIdForInputType(mod_fn, input_type));
  const FunctionSignature* catalog_signature = nullptr;
  for (const FunctionSignature& signature : mod_fn->signatures()) {
    if (signature.context_id() == mod_signature_id) {
      catalog_signature = &signature;
      break;
    }
  }
  if (catalog_signature == nullptr) {
    switch (mod_signature_id) {
      case FN_MOD_NUMERIC:
        return absl::InvalidArgumentError(
            "The provided catalog does not have the FN_MOD_NUMERIC signature. "
            "Did you forget to enable FEATURE_NUMERIC_TYPE?");
      case FN_MOD_BIGNUMERIC:
        return absl::InvalidArgumentError(
            "The provided catalog does not have the FN_MOD_BIGNUMERIC "
            "signature. Did you forget to enable FEATURE_BIGNUMERIC_TYPE?");
      default:
        ZETASQL_RET_CHECK_FAIL();
    }
  }
  return catalog_signature;
}

absl::StatusOr<std::unique_ptr<const ResolvedFunctionCall>>
FunctionCallBuilder::Mod(std::unique_ptr<const ResolvedExpr> dividend_expr,
                         std::unique_ptr<const ResolvedExpr> divisor_expr) {
  ZETASQL_RET_CHECK_EQ(dividend_expr->type(), divisor_expr->type());
  const Type* input_type = dividend_expr->type();

  const Function* mod_fn = nullptr;
  ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionFromCatalog("mod", &mod_fn));

  ZETASQL_ASSIGN_OR_RETURN(const FunctionSignature* catalog_signature,
                   GetModSignature(mod_fn, input_type));
  ZETASQL_RET_CHECK_EQ(catalog_signature->arguments().size(), 2);

  FunctionArgumentType result_type(catalog_signature->result_type().type(),
                                   catalog_signature->result_type().options(),
                                   /*num_occurrences=*/1);
  FunctionArgumentType dividend_arg(dividend_expr->type(),
                                    catalog_signature->argument(0).options(),
                                    /*num_occurrences=*/1);
  FunctionArgumentType divisor_arg(divisor_expr->type(),
                                   catalog_signature->argument(1).options(),
                                   /*num_occurrences=*/1);

  FunctionSignature concrete_signature(result_type, {dividend_arg, divisor_arg},
                                       catalog_signature->context_id(),
                                       catalog_signature->options());

  std::vector<std::unique_ptr<const ResolvedExpr>> args;
  args.push_back(std::move(dividend_expr));
  args.push_back(std::move(divisor_expr));

  return ResolvedFunctionCallBuilder()
      .set_type(result_type.type())
      .set_function(mod_fn)
      .set_signature(concrete_signature)
      .set_argument_list(std::move(args))
      .set_error_mode(ResolvedFunctionCall::DEFAULT_ERROR_MODE)
      .set_function_call_info(std::make_shared<ResolvedFunctionCallInfo>())
      .Build();
}

absl::StatusOr<bool> CatalogSupportsBuiltinFunction(
    absl::string_view function_name, const AnalyzerOptions& analyzer_options,
    Catalog& catalog) {
  const Function* fn;
  absl::Status find_status = catalog.FindFunction(
      {std::string(function_name)}, &fn, analyzer_options.find_options());
  if (find_status.ok()) {
    return fn != nullptr && fn->IsZetaSQLBuiltin();
  }
  if (absl::IsNotFound(find_status)) {
    return false;
  }
  return find_status;
}

absl::Status CheckCatalogSupportsSafeMode(
    absl::string_view function_name, const AnalyzerOptions& analyzer_options,
    Catalog& catalog) {
  ZETASQL_ASSIGN_OR_RETURN(
      bool supports_safe_mode,
      CatalogSupportsBuiltinFunction("NULLIFERROR", analyzer_options, catalog));
  // In case NULLIFERROR is supported through rewrite (a common case) then we
  // also need to check for the IFERROR function.
  if (supports_safe_mode && analyzer_options.enabled_rewrites().contains(
                                REWRITE_NULLIFERROR_FUNCTION)) {
    ZETASQL_ASSIGN_OR_RETURN(
        supports_safe_mode,
        CatalogSupportsBuiltinFunction("IFERROR", analyzer_options, catalog));
  }
  if (!supports_safe_mode) {
    return absl::UnimplementedError(absl::StrCat(
        "SAFE mode calls to ", function_name, " are not supported."));
  }
  return absl::OkStatus();
}

// Checks whether the ResolvedAST has grouping function related nodes.
absl::StatusOr<bool> HasGroupingCallNode(const ResolvedNode* node) {
  bool has_grouping_call = false;
  GroupingCallDetectorVisitor visitor(&has_grouping_call);
  ZETASQL_RETURN_IF_ERROR(node->Accept(&visitor));
  return has_grouping_call;
}

absl::Status FunctionCallBuilder::GetBuiltinFunctionFromCatalog(
    absl::string_view function_name, const Function** fn_out) {
  ZETASQL_RET_CHECK_NE(fn_out, nullptr);
  ZETASQL_RET_CHECK_EQ(*fn_out, nullptr);
  ZETASQL_RETURN_IF_ERROR(catalog_.FindFunction({std::string(function_name)}, fn_out,
                                        analyzer_options_.find_options()));
  if (fn_out == nullptr || *fn_out == nullptr ||
      !(*fn_out)->IsZetaSQLBuiltin()) {
    return absl::NotFoundError(absl::Substitute(
        "Required built-in function \"$0\" not available.", function_name));
  }
  return absl::OkStatus();
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
                                   /*rollup_column_list=*/{},
                                   /*grouping_call_list=*/{});
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
  ABSL_DCHECK(function_call->function() != nullptr)
      << "Expected function_call->function() to not be null";
  return function_call->function() != nullptr &&
         function_call->signature().context_id() == function_signature_id &&
         function_call->function()->IsZetaSQLBuiltin();
}

zetasql_base::StatusBuilder MakeUnimplementedErrorAtNode(const ResolvedNode* node) {
  zetasql_base::StatusBuilder builder = zetasql_base::UnimplementedErrorBuilder();
  if (node != nullptr && node->GetParseLocationOrNULL() != nullptr) {
    builder.AttachPayload(
        node->GetParseLocationOrNULL()->start().ToInternalErrorLocation());
  }
  return builder;
}

// Visitor that collects correlated columns.
class CorrelatedColumnRefCollector : public ResolvedASTVisitor {
 public:
  const absl::flat_hash_set<ResolvedColumn>& GetCorrelatedColumns() const {
    return correlated_columns_;
  }

 private:
  absl::Status VisitResolvedColumnRef(const ResolvedColumnRef* ref) override {
    const ResolvedColumn& col = ref->column();
    // Only collect the external columns when they are correlated within the
    // visited node. We ignore the internal columns who also appear in a
    // correlated reference because it is used in a nested subquery.
    //
    // For example, for the following query and the visited node.
    // select (
    //   select (                             <= Visited node
    //       select
    //       from InnerTable
    //       where InnerTable.col = Table.col and
    //             InnerTable.col = OuterTable.col
    //       limit 1
    //   ) from Table
    // ) from OuterTable
    //
    //  Here for the visited node, OuterTable.col is returned as a correlated
    //  column; however Table.col will NOT be returned because Table.col is only
    //  correlated in the inner subquery of the visited node.
    if (ref->is_correlated()) {
      if (!uncorrelated_column_ids_.contains(col.column_id())) {
        correlated_columns_.insert(col);
      }
    } else {
      correlated_columns_.erase(col);
      uncorrelated_column_ids_.insert(col.column_id());
    }
    return absl::OkStatus();
  }

  absl::flat_hash_set<ResolvedColumn> correlated_columns_;
  absl::flat_hash_set<int> uncorrelated_column_ids_;
};

absl::StatusOr<absl::flat_hash_set<ResolvedColumn>> GetCorrelatedColumnSet(
    const ResolvedNode& node) {
  absl::flat_hash_set<ResolvedColumn> column_set;
  CorrelatedColumnRefCollector visitor;
  ZETASQL_RETURN_IF_ERROR(node.Accept(&visitor));
  for (const ResolvedColumn& column : visitor.GetCorrelatedColumns()) {
    column_set.insert(column);
  }
  return column_set;
}

}  // namespace zetasql
