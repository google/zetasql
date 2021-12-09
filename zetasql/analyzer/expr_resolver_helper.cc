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

#include <memory>
#include <string>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/id_string.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

absl::StatusOr<bool> IsConstantExpression(const ResolvedExpr* expr) {

  switch (expr->node_kind()) {
    case RESOLVED_CONSTANT:
    case RESOLVED_LITERAL:
    case RESOLVED_PARAMETER:
    case RESOLVED_SYSTEM_VARIABLE:
    case RESOLVED_ARGUMENT_REF:
    case RESOLVED_EXPRESSION_COLUMN:
    case RESOLVED_DMLDEFAULT:
      // These can't contain ColumnRefs and are constant for this query.
      return true;

    case RESOLVED_COLUMN_REF:
    case RESOLVED_AGGREGATE_FUNCTION_CALL:
    case RESOLVED_ANALYTIC_FUNCTION_CALL:
    case RESOLVED_SUBQUERY_EXPR:
      // These are always treated as non-constant expressions.
      return false;

    case RESOLVED_FUNCTION_CALL: {
      // Scalar function calls are treated as constant expressions based on
      // FunctionOptions::volatility.
      const ResolvedFunctionCall* function_call =
          expr->GetAs<ResolvedFunctionCall>();
      if (function_call->function()->function_options().volatility ==
          FunctionEnums::VOLATILE) {
        return false;
      }
      for (const std::unique_ptr<const ResolvedExpr>& arg :
           function_call->argument_list()) {
        ZETASQL_ASSIGN_OR_RETURN(bool arg_is_constant_expr,
                         IsConstantExpression(arg.get()));
        if (!arg_is_constant_expr) {
          return false;
        }
      }
      for (const std::unique_ptr<const ResolvedFunctionArgument>& arg :
           function_call->generic_argument_list()) {
        if (arg->expr() != nullptr) {
          ZETASQL_ASSIGN_OR_RETURN(bool arg_is_constant_expr,
                           IsConstantExpression(arg->expr()));
          if (!arg_is_constant_expr) {
            return false;
          }
        } else if (arg->inline_lambda() != nullptr) {
          return false;
        } else {
          ZETASQL_DCHECK(false) << "Unexpected function argument: "
                        << arg->DebugString() << "\n of function call: "
                        << function_call->DebugString();
          return false;
        }
      }
      return true;
    }

    case RESOLVED_CAST:
      return IsConstantExpression(
          expr->GetAs<ResolvedCast>()->expr());

    case RESOLVED_GET_STRUCT_FIELD:
      return IsConstantExpression(
          expr->GetAs<ResolvedGetStructField>()->expr());

    case RESOLVED_GET_PROTO_FIELD:
      return IsConstantExpression(
          expr->GetAs<ResolvedGetProtoField>()->expr());

    case RESOLVED_GET_JSON_FIELD:
      return IsConstantExpression(
          expr->GetAs<ResolvedGetJsonField>()->expr());

    case RESOLVED_FLATTEN:
      for (const auto& arg : expr->GetAs<ResolvedFlatten>()->get_field_list()) {
        ZETASQL_ASSIGN_OR_RETURN(bool arg_is_constant_expr,
                         IsConstantExpression(arg.get()));
        if (arg_is_constant_expr) return false;
      }
      return IsConstantExpression(expr->GetAs<ResolvedFlatten>()->expr());

    case RESOLVED_FLATTENED_ARG:
      // These represent the result of a previous resolved flatten expression or
      // get. They may or may not be constant, but if they are not then they
      // would fail IsConstantExpression for an earlier step in RESOLVED_FLATTEN
      // above. If we said they were not constant then RESOLVED_FLATTEN would
      // never be able to be constant even if expr and all get_field_list were.
      return true;

    case RESOLVED_REPLACE_FIELD: {
      const ResolvedReplaceField* replace_field =
          expr->GetAs<ResolvedReplaceField>();
      ZETASQL_ASSIGN_OR_RETURN(bool replace_field_is_constant_expr,
                       IsConstantExpression(replace_field->expr()));
      if (!replace_field_is_constant_expr) {
        return false;
      }
      for (const std::unique_ptr<const ResolvedReplaceFieldItem>&
               replace_field_item : replace_field->replace_field_item_list()) {
        ZETASQL_ASSIGN_OR_RETURN(bool replace_field_item_is_constant_expr,
                         IsConstantExpression(replace_field_item->expr()));
        if (!replace_field_item_is_constant_expr) {
          return false;
        }
      }
      return true;
    }

    case RESOLVED_MAKE_STRUCT: {
      // No code coverage on this because we don't currently have syntax to
      // generate it as a top-level expression.
      const ResolvedMakeStruct* make_struct = expr->GetAs<ResolvedMakeStruct>();
      for (const std::unique_ptr<const ResolvedExpr>& child_expr :
           make_struct->field_list()) {
        ZETASQL_ASSIGN_OR_RETURN(bool child_expr_is_constant_expr,
                         IsConstantExpression(child_expr.get()));
        if (!child_expr_is_constant_expr) {
          return false;
        }
      }
      return true;
    }

    case RESOLVED_FILTER_FIELD: {
      return IsConstantExpression(expr->GetAs<ResolvedFilterField>()->expr());
    }

    case RESOLVED_MAKE_PROTO: {
      // No code coverage on this because we don't currently have syntax to
      // generate it as a top-level expression.
      const ResolvedMakeProto* make_proto = expr->GetAs<ResolvedMakeProto>();
      for (const std::unique_ptr<const ResolvedMakeProtoField>& field :
           make_proto->field_list()) {
        ZETASQL_ASSIGN_OR_RETURN(bool field_is_constant_expr,
                         IsConstantExpression(field->expr()));
        if (!field_is_constant_expr) {
          return false;
        }
      }
      return true;
    }

    case RESOLVED_LET_EXPR: {
      const auto* let_expr = expr->GetAs<ResolvedLetExpr>();
      ZETASQL_ASSIGN_OR_RETURN(bool expr_is_constant_expr,
                       IsConstantExpression(let_expr->expr()));
      if (!expr_is_constant_expr) {
        return false;
      }
      for (int i = 0; i < let_expr->assignment_list_size(); ++i) {
        ZETASQL_ASSIGN_OR_RETURN(
            bool assignment_is_constant_expr,
            IsConstantExpression(let_expr->assignment_list(i)->expr()));
        if (!assignment_is_constant_expr) {
          return false;
        }
      }
      return true;
    }

    default:
      // Update the static_assert above if adding or removing cases in
      // this switch.
      ZETASQL_RET_CHECK_FAIL() << "Unhandled expression type "
                       << expr->node_kind_string()
                       << " in IsConstantExpression";
      return false;
  }
}

ExprResolutionInfo::ExprResolutionInfo(
    const NameScope* name_scope_in, const NameScope* aggregate_name_scope_in,
    bool allows_aggregation_in, bool allows_analytic_in,
    bool use_post_grouping_columns_in, const char* clause_name_in,
    QueryResolutionInfo* query_resolution_info_in,
    const ASTExpression* top_level_ast_expr_in, IdString column_alias_in)
    : name_scope(name_scope_in),
      aggregate_name_scope(aggregate_name_scope_in),
      allows_aggregation(allows_aggregation_in),
      allows_analytic(allows_analytic_in),
      clause_name(clause_name_in),
      query_resolution_info(query_resolution_info_in),
      use_post_grouping_columns(use_post_grouping_columns_in),
      top_level_ast_expr(top_level_ast_expr_in),
      column_alias(column_alias_in) {}

ExprResolutionInfo::ExprResolutionInfo(
    const NameScope* name_scope_in,
    QueryResolutionInfo* query_resolution_info_in,
    const ASTExpression* top_level_ast_expr_in, IdString column_alias_in)
    : ExprResolutionInfo(
          name_scope_in, name_scope_in, true /* allows_aggregation */,
          true /* allows_analytic */, false /* use_post_grouping_columns */,
          "" /* clause_name */, query_resolution_info_in, top_level_ast_expr_in,
          column_alias_in) {}

ExprResolutionInfo::ExprResolutionInfo(
    const NameScope* name_scope_in,
    const char* clause_name_in)
    : ExprResolutionInfo(
          name_scope_in, name_scope_in,
          false /* allows_aggregation */, false /* allows_analytic */,
          false /* use_post_grouping_columns */,
          clause_name_in, nullptr /* query_resolution_info */) {}

ExprResolutionInfo::ExprResolutionInfo(ExprResolutionInfo* parent)
    : ExprResolutionInfo(parent->name_scope, parent->aggregate_name_scope,
                         parent->allows_aggregation, parent->allows_analytic,
                         parent->use_post_grouping_columns, parent->clause_name,
                         parent->query_resolution_info,
                         parent->top_level_ast_expr, parent->column_alias) {
  // Hack because I can't use initializer syntax and a delegated constructor.
  const_cast<ExprResolutionInfo*&>(this->parent) = parent;
}

ExprResolutionInfo::ExprResolutionInfo(ExprResolutionInfo* parent,
                                       const NameScope* name_scope_in,
                                       const char* clause_name_in,
                                       bool allows_analytic_in)
    : ExprResolutionInfo(name_scope_in, parent->aggregate_name_scope,
                         parent->allows_aggregation, allows_analytic_in,
                         parent->use_post_grouping_columns, clause_name_in,
                         parent->query_resolution_info,
                         parent->top_level_ast_expr, parent->column_alias) {
  // Hack because I can't use initializer syntax and a delegated constructor.
  const_cast<ExprResolutionInfo*&>(this->parent) = parent;
}

ExprResolutionInfo::~ExprResolutionInfo() {
  // Propagate has_aggregation and has_analytic up the tree to the caller.
  // We assume all child ExprResolutionInfo objects will go out of scope
  // before the caller's has_ fields are examined.
  if (parent != nullptr) {
    if (has_aggregation) {
      parent->has_aggregation = true;
    }
    if (has_analytic) {
      parent->has_analytic = true;
    }
  }
}

bool ExprResolutionInfo::is_post_distinct() const {
  if (query_resolution_info != nullptr) {
    return query_resolution_info->is_post_distinct();
  }
  return false;
}

std::string ExprResolutionInfo::DebugString() const {
  std::string debugstring;
  absl::StrAppend(&debugstring, "\nname_scope: ",
                  (name_scope != nullptr ? name_scope->DebugString() : "NULL"));
  absl::StrAppend(
      &debugstring, "\naggregate_name_scope: ",
      (aggregate_name_scope != nullptr ? aggregate_name_scope->DebugString()
                                       : "NULL"));
  absl::StrAppend(&debugstring, "\nallows_aggregation: ", allows_aggregation);
  absl::StrAppend(&debugstring, "\nhas_aggregation: ", has_aggregation);
  absl::StrAppend(&debugstring, "\nallows_analytic: ", allows_analytic);
  absl::StrAppend(&debugstring, "\nhas_analytic: ", has_analytic);
  absl::StrAppend(&debugstring, "\nclause_name: ", clause_name);
  absl::StrAppend(&debugstring,
                  "\nuse_post_grouping_columns: ", use_post_grouping_columns);
  absl::StrAppend(
      &debugstring, "\nQueryResolutionInfo:\n",
      (query_resolution_info != nullptr ? query_resolution_info->DebugString()
                                        : "NULL"));
  return debugstring;
}

}  // namespace zetasql
