//
// Copyright 2019 ZetaSQL Authors
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

#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/base/string_numbers.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

bool IsConstantExpression(const ResolvedExpr* expr) {

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
        if (!IsConstantExpression(arg.get())) {
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

    case RESOLVED_FLATTEN:
      for (const auto& arg : expr->GetAs<ResolvedFlatten>()->get_field_list()) {
        if (!IsConstantExpression(arg.get())) return false;
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
      if (!IsConstantExpression(replace_field->expr())) {
        return false;
      }
      for (const std::unique_ptr<const ResolvedReplaceFieldItem>&
               replace_field_item : replace_field->replace_field_item_list()) {
        if (!IsConstantExpression(replace_field_item->expr())) {
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
        if (!IsConstantExpression(child_expr.get())) {
          return false;
        }
      }
      return true;
    }

    case RESOLVED_MAKE_PROTO: {
      // No code coverage on this because we don't currently have syntax to
      // generate it as a top-level expression.
      const ResolvedMakeProto* make_proto = expr->GetAs<ResolvedMakeProto>();
      for (const std::unique_ptr<const ResolvedMakeProtoField>& field :
           make_proto->field_list()) {
        if (!IsConstantExpression(field->expr())) {
          return false;
        }
      }
      return true;
    }

    default:
      // Update the static_assert above if adding or removing cases in
      // this switch.
      LOG(DFATAL)
          << "Unhandled expression type " << expr->node_kind_string()
          << " in IsConstantExpression";
      return false;
  }
}

const Type* SelectColumnState::GetType() const {
  if (resolved_select_column.IsInitialized()) {
    return resolved_select_column.type();
  }
  if (resolved_expr != nullptr) {
    return resolved_expr->type();
  }
  return nullptr;
}

std::string SelectColumnState::DebugString(absl::string_view indent) const {
  std::string debug_string;
  absl::StrAppend(&debug_string, indent, "expr:\n   ", ast_expr->DebugString(),
                  "\n");
  absl::StrAppend(&debug_string, indent, "alias: ", alias.ToStringView(), "\n");
  absl::StrAppend(&debug_string, indent, "is_explicit: ", is_explicit, "\n");
  absl::StrAppend(&debug_string, indent,
                  "select_list_position: ", select_list_position, "\n");
  absl::StrAppend(
      &debug_string, indent, "resolved_expr:\n  ",
      (resolved_expr != nullptr ? resolved_expr->DebugString() : "<null>"),
      "\n");
  absl::StrAppend(&debug_string, indent, "resolved_computed_column:\n  ",
                  (resolved_computed_column != nullptr
                       ? resolved_computed_column->DebugString()
                       : "<null>"),
                  "\n");
  absl::StrAppend(&debug_string, indent, "has_aggregation: ", has_aggregation,
                  "\n");
  absl::StrAppend(&debug_string, indent, "has_analytic: ", has_analytic, "\n");
  absl::StrAppend(&debug_string, indent,
                  "is_group_by_column: ", is_group_by_column, "\n");
  absl::StrAppend(&debug_string, indent, "resolved_select_column: ",
                  (resolved_select_column.IsInitialized()
                       ? resolved_select_column.DebugString()
                       : "<uninitialized>"),
                  "\n");
  absl::StrAppend(&debug_string, indent,
                  "resolved_pre_group_by_select_column: ",
                  (resolved_pre_group_by_select_column.IsInitialized()
                       ? resolved_pre_group_by_select_column.DebugString()
                       : "<uninitialized>"));
  return debug_string;
}

SelectColumnState* SelectColumnStateList::AddSelectColumn(
    const ASTExpression* ast_expr, IdString alias,
    bool is_explicit) {
  SelectColumnState* select_column_state = new SelectColumnState(
      ast_expr, alias, is_explicit, -1 /* select_list_position */);
  AddSelectColumn(select_column_state);
  return select_column_state;
}

void SelectColumnStateList::AddSelectColumn(
    SelectColumnState* select_column_state) {
  DCHECK_EQ(select_column_state->select_list_position, -1);
  select_column_state->select_list_position = select_column_state_list_.size();
  // Save a mapping from the alias to this SelectColumnState. The mapping is
  // later used for validations performed by
  // FindAndValidateSelectColumnStateByAlias().
  const IdString alias = select_column_state->alias;
  if (!IsInternalAlias(alias)) {
    if (!zetasql_base::InsertIfNotPresent(&column_alias_to_state_list_position_, alias,
                                 select_column_state->select_list_position)) {
      // Now ambiguous.
      column_alias_to_state_list_position_[alias] = -1;
    }
  }
  select_column_state_list_.push_back(absl::WrapUnique(select_column_state));
}

absl::Status SelectColumnStateList::FindAndValidateSelectColumnStateByAlias(
    const char* clause_name, const ASTNode* ast_location,
    IdString alias,
    const ExprResolutionInfo* expr_resolution_info,
    const SelectColumnState** select_column_state) const {
  *select_column_state = nullptr;
  // TODO Should probably do this more generally with name scoping.
  const int* state_list_position =
      zetasql_base::FindOrNull(column_alias_to_state_list_position_, alias);
  if (state_list_position != nullptr) {
    if (*state_list_position == -1) {
      return MakeSqlErrorAt(ast_location)
             << "Name " << alias << " in " << clause_name
             << " is ambiguous; it may refer to multiple columns in the"
                " SELECT-list";
    } else {
      const SelectColumnState* found_select_column_state =
          select_column_state_list_[*state_list_position].get();
      ZETASQL_RETURN_IF_ERROR(ValidateAggregateAndAnalyticSupport(
          alias.ToStringView(), ast_location, found_select_column_state,
          expr_resolution_info));
      *select_column_state = found_select_column_state;
    }
  }
  return absl::OkStatus();
}

absl::Status SelectColumnStateList::FindAndValidateSelectColumnStateByOrdinal(
    const std::string& expr_description, const ASTNode* ast_location,
    const int64_t ordinal, const ExprResolutionInfo* expr_resolution_info,
    const SelectColumnState** select_column_state) const {
  *select_column_state = nullptr;
  if (ordinal < 1 || ordinal > select_column_state_list_.size()) {
    return MakeSqlErrorAt(ast_location)
           << expr_description
           << " is out of SELECT column number range: " << ordinal;
  }
  const SelectColumnState* found_select_column_state =
      select_column_state_list_[ordinal - 1].get();  // Convert to 0-based.
  ZETASQL_RETURN_IF_ERROR(ValidateAggregateAndAnalyticSupport(
      absl::StrCat(ordinal), ast_location, found_select_column_state,
      expr_resolution_info));
  *select_column_state = found_select_column_state;
  return absl::OkStatus();
}

absl::Status SelectColumnStateList::ValidateAggregateAndAnalyticSupport(
    const absl::string_view& column_description, const ASTNode* ast_location,
    const SelectColumnState* select_column_state,
    const ExprResolutionInfo* expr_resolution_info) {
  if (select_column_state->has_aggregation &&
      !expr_resolution_info->allows_aggregation) {
    return MakeSqlErrorAt(ast_location)
           << "Column " << column_description
           << " contains an aggregation function, which is not allowed in "
           << expr_resolution_info->clause_name
           << (expr_resolution_info->is_post_distinct()
                   ? " after SELECT DISTINCT"
                   : "");
  }
  if (select_column_state->has_analytic &&
      !expr_resolution_info->allows_analytic) {
    return MakeSqlErrorAt(ast_location)
           << "Column " << column_description
           << " contains an analytic function, which is not allowed in "
           << expr_resolution_info->clause_name
           << (expr_resolution_info->is_post_distinct()
                   ? " after SELECT DISTINCT"
                   : "");
  }
  return absl::OkStatus();
}

SelectColumnState* SelectColumnStateList::GetSelectColumnState(
    int select_list_position) {
  CHECK_GE(select_list_position, 0);
  CHECK_LT(select_list_position, select_column_state_list_.size());
  return select_column_state_list_[select_list_position].get();
}

const SelectColumnState* SelectColumnStateList::GetSelectColumnState(
    int select_list_position) const {
  CHECK_GE(select_list_position, 0);
  CHECK_LT(select_list_position, select_column_state_list_.size());
  return select_column_state_list_[select_list_position].get();
}

const std::vector<std::unique_ptr<SelectColumnState>>&
SelectColumnStateList::select_column_state_list() const {
  return select_column_state_list_;
}

const ResolvedColumnList SelectColumnStateList::resolved_column_list() const {
  ResolvedColumnList resolved_column_list;
  resolved_column_list.reserve(select_column_state_list_.size());
  for (const std::unique_ptr<SelectColumnState>& select_column_state :
       select_column_state_list_) {
    resolved_column_list.push_back(select_column_state->resolved_select_column);
  }
  return resolved_column_list;
}

int SelectColumnStateList::Size() const {
  return select_column_state_list_.size();
}

std::string SelectColumnStateList::DebugString() const {
  std::string debug_string("SelectColumnStateList, size = ");
  absl::StrAppend(&debug_string, Size(), "\n");
  for (int idx = 0; idx < Size(); ++idx) {
    absl::StrAppend(&debug_string, "    [", idx, "]:\n",
                    GetSelectColumnState(idx)->DebugString("       "), "\n");
  }
  absl::StrAppend(&debug_string, "  alias map:\n");
  for (const auto& alias_to_position : column_alias_to_state_list_position_) {
    absl::StrAppend(&debug_string, "    ",
                    alias_to_position.first.ToStringView(), " : ",
                    alias_to_position.second, "\n");
  }
  return debug_string;
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

InputArgumentType GetInputArgumentTypeForExpr(const ResolvedExpr* expr) {
  if (expr->type()->IsStruct() &&
      expr->node_kind() == RESOLVED_MAKE_STRUCT) {
    const ResolvedMakeStruct* struct_expr = expr->GetAs<ResolvedMakeStruct>();
    std::vector<const ResolvedExpr*> fields;
    fields.reserve(struct_expr->field_list_size());
    for (int i = 0; i < struct_expr->field_list_size(); ++i) {
      fields.push_back(struct_expr->field_list(i));
    }
    std::vector<InputArgumentType> field_types;
    GetInputArgumentTypesForExprList(&fields, &field_types);
    // We construct a custom InputArgumentType for structs that may have
    // some literal and some non-literal fields.
    return InputArgumentType(expr->type()->AsStruct(), field_types);
  }

  // Literals that were explicitly casted (i.e., the original expression was
  // 'CAST(<literal> AS <type>)') are treated like non-literals with
  // respect to subsequent coercion.
  if (expr->node_kind() == RESOLVED_LITERAL &&
      !expr->GetAs<ResolvedLiteral>()->has_explicit_type()) {
    if (expr->GetAs<ResolvedLiteral>()->value().is_null()) {
      // This is a literal NULL that does not have an explicit type, so
      // it can coerce to anything.
      return InputArgumentType::UntypedNull();
    }
    // This is a literal empty array that does not have an explicit type,
    // so it can coerce to any array type.
    if (expr->GetAs<ResolvedLiteral>()->value().is_empty_array()) {
      return InputArgumentType::UntypedEmptyArray();
    }
    return InputArgumentType(expr->GetAs<ResolvedLiteral>()->value());
  }

  if (expr->node_kind() == RESOLVED_PARAMETER &&
      expr->GetAs<ResolvedParameter>()->is_untyped()) {
    // Undeclared parameters can be coerced to any type.
    return InputArgumentType::UntypedQueryParameter();
  }

  if (expr->node_kind() == RESOLVED_FUNCTION_CALL &&
      expr->GetAs<ResolvedFunctionCall>()->function()->FullName(
          true /* include_group */) == "ZetaSQL:error") {
    // This is an ERROR(message) function call.  We special case this to
    // make the output argument coercible to anything so expressions like
    //   IF(<condition>, <value>, ERROR("message"))
    // work for any value type.
    //
    // Note that this case does not apply if ERROR() is wrapped in a CAST, since
    // that expression has an explicit type. For example,
    // COALESCE('abc', CAST(ERROR('def') AS BYTES)) fails because BYTES does not
    // implicitly coerce to STRING.
    return InputArgumentType::UntypedNull();
  }

  return InputArgumentType(expr->type(),
                           expr->node_kind() == RESOLVED_PARAMETER);
}

void GetInputArgumentTypesForExprList(
    const std::vector<const ResolvedExpr*>* arguments,
    std::vector<InputArgumentType>* input_arguments) {
  input_arguments->clear();
  input_arguments->reserve(arguments->size());
  for (const ResolvedExpr* argument : *arguments) {
    input_arguments->push_back(GetInputArgumentTypeForExpr(argument));
  }
}

void GetInputArgumentTypesForExprList(
    const std::vector<std::unique_ptr<const ResolvedExpr>>& arguments,
    std::vector<InputArgumentType>* input_arguments) {
  input_arguments->clear();
  input_arguments->reserve(arguments.size());
  for (const std::unique_ptr<const ResolvedExpr>& argument : arguments) {
    input_arguments->push_back(GetInputArgumentTypeForExpr(argument.get()));
  }
}

bool IsSameExpressionForGroupBy(const ResolvedExpr* expr1,
                                const ResolvedExpr* expr2) {
  if (expr1->node_kind() != expr2->node_kind() ||
      !expr1->type()->Equals(expr2->type())) {
    return false;
  }

  // Need to make sure that all non-default fields of expressions were accessed
  // to make sure that if new fields are added, this function checks them or
  // expressions are not considered the same.
  expr1->ClearFieldsAccessed();
  expr2->ClearFieldsAccessed();

  switch (expr1->node_kind()) {
    case RESOLVED_LITERAL: {
      const ResolvedLiteral* lit1 = expr1->GetAs<ResolvedLiteral>();
      const ResolvedLiteral* lit2 = expr2->GetAs<ResolvedLiteral>();
      // Value::Equals does the right thing for GROUP BY - NULLs, NaNs, infs
      // etc are considered equal.
      if (!lit1->value().Equals(lit2->value())) {
        return false;
      }
      break;
    }
    case RESOLVED_PARAMETER: {
      const ResolvedParameter* param1 = expr1->GetAs<ResolvedParameter>();
      const ResolvedParameter* param2 = expr2->GetAs<ResolvedParameter>();
      // Parameter names are normalized, so case sensitive comparison is ok.
      if (param1->name() != param2->name() ||
          param1->position() != param2->position()) {
        return false;
      }
      break;
    }
    case RESOLVED_EXPRESSION_COLUMN: {
      const ResolvedExpressionColumn* col1 =
          expr1->GetAs<ResolvedExpressionColumn>();
      const ResolvedExpressionColumn* col2 =
          expr2->GetAs<ResolvedExpressionColumn>();
      // Engine could be case sensitive for column names, so stay conservative
      // and do case sensitive comparison.
      if (col1->name() != col2->name()) {
        return false;
      }
      break;
    }
    case RESOLVED_COLUMN_REF: {
      const ResolvedColumnRef* ref1 = expr1->GetAs<ResolvedColumnRef>();
      const ResolvedColumnRef* ref2 = expr2->GetAs<ResolvedColumnRef>();
      if (ref1->column().column_id() != ref2->column().column_id()) {
        return false;
      }
      break;
    }
    case RESOLVED_GET_STRUCT_FIELD: {
      const ResolvedGetStructField* str1 =
          expr1->GetAs<ResolvedGetStructField>();
      const ResolvedGetStructField* str2 =
          expr2->GetAs<ResolvedGetStructField>();
      if (str1->field_idx() != str2->field_idx() ||
          !IsSameExpressionForGroupBy(str1->expr(), str2->expr())) {
        return false;
      }
      break;
    }
    case RESOLVED_GET_PROTO_FIELD: {
      const ResolvedGetProtoField* proto_field1 =
          expr1->GetAs<ResolvedGetProtoField>();
      const ResolvedGetProtoField* proto_field2 =
          expr2->GetAs<ResolvedGetProtoField>();
      return proto_field1->expr()->type()->kind() ==
                 proto_field2->expr()->type()->kind() &&
             proto_field1->field_descriptor()->number() ==
                 proto_field2->field_descriptor()->number() &&
             proto_field1->default_value() == proto_field2->default_value() &&
             proto_field1->get_has_bit() == proto_field2->get_has_bit() &&
             proto_field1->format() == proto_field2->format() &&
             IsSameExpressionForGroupBy(proto_field1->expr(),
                                        proto_field2->expr());
      break;
    }
    case RESOLVED_CAST: {
      const ResolvedCast* cast1 = expr1->GetAs<ResolvedCast>();
      const ResolvedCast* cast2 = expr2->GetAs<ResolvedCast>();
      if (!IsSameExpressionForGroupBy(cast1->expr(), cast2->expr())) {
        return false;
      }
      if (cast1->return_null_on_error() != cast2->return_null_on_error()) {
        return false;
      }
      break;
    }
    case RESOLVED_FUNCTION_CALL: {
      const ResolvedFunctionCall* func1 = expr1->GetAs<ResolvedFunctionCall>();
      const ResolvedFunctionCall* func2 = expr2->GetAs<ResolvedFunctionCall>();
      if (func1->function() != func2->function()) {
        return false;
      }
      if (func1->error_mode() != func2->error_mode()) {
        return false;
      }
      if (func1->function()->function_options().volatility ==
          FunctionEnums::VOLATILE) {
        return false;
      }
      const std::vector<std::unique_ptr<const ResolvedExpr>>& arg1_list =
          func1->argument_list();
      const std::vector<std::unique_ptr<const ResolvedExpr>>& arg2_list =
          func2->argument_list();
      if (arg1_list.size() != arg2_list.size()) {
        return false;
      }
      for (int idx = 0; idx < arg1_list.size(); ++idx) {
        if (!IsSameExpressionForGroupBy(arg1_list[idx].get(),
                                        arg2_list[idx].get())) {
          return false;
        }
      }
      break;
    }
    default:
      // Without explicit support for this type of expression, pessimistically
      // say that they are not equal.
      // TODO: Add the following:
      // - RESOLVED_MAKE_STRUCT
      return false;
  }

  absl::Status status;
  status.Update(expr1->CheckFieldsAccessed());
  status.Update(expr2->CheckFieldsAccessed());
  if (!status.ok()) {
    LOG(DFATAL) << status;
    return false;
  }

  return true;
}

}  // namespace zetasql
