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

#include "zetasql/resolved_ast/validator.h"

#include <algorithm>
#include <string>
#include <type_traits>

#include "zetasql/base/logging.h"
#include "zetasql/base/varsetter.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/constant.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/base/case.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/stl_util.h"
#include "zetasql/base/canonical_errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

Validator::Validator(const LanguageOptions& language_options)
    : language_options_(language_options) {}

static bool IsEmptyWindowFrame(const ResolvedWindowFrame& window_frame) {
  const ResolvedWindowFrameExpr* frame_start_expr = window_frame.start_expr();
  const ResolvedWindowFrameExpr* frame_end_expr = window_frame.end_expr();

  switch (frame_start_expr->boundary_type()) {
    case ResolvedWindowFrameExpr::UNBOUNDED_FOLLOWING:
      return true;
    case ResolvedWindowFrameExpr::CURRENT_ROW:
      if (frame_end_expr->boundary_type() ==
          ResolvedWindowFrameExpr::OFFSET_PRECEDING) {
        return true;
      }
      break;
    case ResolvedWindowFrameExpr::OFFSET_FOLLOWING:
      switch (frame_end_expr->boundary_type()) {
        case ResolvedWindowFrameExpr::OFFSET_PRECEDING:
        case ResolvedWindowFrameExpr::CURRENT_ROW:
          return true;
          break;
        default:
          break;
      }
      break;
    default:
      break;
  }
  if (frame_end_expr->boundary_type() ==
      ResolvedWindowFrameExpr::UNBOUNDED_PRECEDING) {
    return true;
  }
  return false;
}

zetasql_base::Status Validator::ValidateResolvedParameter(
    const ResolvedParameter* resolved_param) const {
  ZETASQL_RET_CHECK(nullptr != resolved_param);
  // If the parameter has a name, it must not have a position and vice versa.
  const bool has_name = !resolved_param->name().empty();
  const bool has_position = resolved_param->position() > 0;
  if (has_name == has_position) {
    return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
           << "Parameter is expected to have a name or a position but not "
              "both: "
           << resolved_param->DebugString();
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::CheckColumnIsPresentInColumnSet(
    const ResolvedColumn& column,
    const std::set<ResolvedColumn>& visible_columns) const {
  if (!zetasql_base::ContainsKey(visible_columns, column)) {
    return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
           << "Incorrect reference to column " << column.DebugString();
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::CheckColumnList(
    const ResolvedScan* scan,
    const std::set<ResolvedColumn>& visible_columns) const {
  ZETASQL_RET_CHECK(nullptr != scan);
  for (const ResolvedColumn& column : scan->column_list()) {
    if (!zetasql_base::ContainsKey(visible_columns, column)) {
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Column list contains column " << column.DebugString()
             << " not visible in scan node\n"
             << scan->DebugString();
    }
  }
  return ::zetasql_base::OkStatus();
}

Validator::Validator() {}

Validator::~Validator() {}

zetasql_base::Status Validator::ValidateResolvedExprList(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const std::vector<std::unique_ptr<const ResolvedExpr>>& expr_list) const {
  for (const auto& expr_iter : expr_list) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         expr_iter.get()));
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCast(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedCast* resolved_cast) const {
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       resolved_cast->expr()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedConstant(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedConstant* resolved_constant) const {

  ZETASQL_RET_CHECK(resolved_constant->constant() != nullptr)
      << "ResolvedConstant does not have a Constant:\n"
      << resolved_constant->DebugString();
  ZETASQL_RET_CHECK(
      resolved_constant->constant()->type()->Equals(resolved_constant->type()))
      << "Expected ResolvedConstant of type "
      << resolved_constant->constant()->type()->DebugString() << ", found "
      << resolved_constant->type()->DebugString();

  return zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedFunctionCallBase(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedFunctionCallBase* resolved_function_call) const {

  ZETASQL_RET_CHECK(resolved_function_call->function() != nullptr)
      << "ResolvedFunctionCall does not have a Function:\n"
      << resolved_function_call->DebugString();

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExprList(
      visible_columns, visible_parameters,
      resolved_function_call->argument_list()));

  const FunctionSignature& signature = resolved_function_call->signature();
  ZETASQL_RET_CHECK(signature.IsConcrete())
      << "ResolvedFunctionCall must have a concrete signature:\n"
      << resolved_function_call->DebugString();
  ZETASQL_RET_CHECK(resolved_function_call->type()->Equals(
      signature.result_type().type()));

  const int num_args = signature.NumConcreteArguments();
  ZETASQL_RET_CHECK_EQ(resolved_function_call->argument_list_size(), num_args)
      << resolved_function_call->DebugString()
      << "\nSignature: " << signature.DebugString();
  for (int i = 0; i < num_args; ++i) {
    ZETASQL_RET_CHECK(resolved_function_call->argument_list(i)->type()->Equals(
        signature.ConcreteArgumentType(i)));
  }

  if (resolved_function_call->error_mode() ==
      ResolvedFunctionCallBase::SAFE_ERROR_MODE) {
    ZETASQL_RET_CHECK(resolved_function_call->function()->SupportsSafeErrorMode())
        << "Function " << resolved_function_call->function()->FullName(false)
        << "does not support SAFE error mode";
  }

  if (resolved_function_call->node_kind() == RESOLVED_FUNCTION_CALL) {
    const ResolvedFunctionCallInfo* info =
        resolved_function_call->GetAs<ResolvedFunctionCall>()
            ->function_call_info().get();
    if (info->Is<TemplatedSQLFunctionCall>()) {
      const TemplatedSQLFunctionCall* templated_info =
          info->GetAs<TemplatedSQLFunctionCall>();
      ZETASQL_RET_CHECK(templated_info->expr()->type()->Equals(
          resolved_function_call->signature().result_type().type()));
    }
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateStandaloneResolvedExpr(
    const ResolvedExpr* expr) const {
  const zetasql_base::Status status =
      ValidateResolvedExpr({} /* visible_columns */,
                           {} /* visible_parameters */,
                           expr);
  if (!status.ok()) {
    if (status.code() == zetasql_base::StatusCode::kResourceExhausted) {
      // Don't wrap a resource exhausted status into internal error. This error
      // may still occur for a valid properly resolved expression (stack
      // exhaustion in case of deeply nested expression). There exist cases
      // where the validator uses more stack than parsing/analysis (b/65294961).
      return status;
    }
    return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
           << "Resolved AST validation failed: " << status.message() << "\n"
           << expr->DebugString();
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedExpr(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedExpr* expr) const {

  ZETASQL_RET_CHECK(nullptr != expr);
  ZETASQL_RET_CHECK(expr->type() != nullptr)
      << "ResolvedExpr does not have a Type:\n" << expr->DebugString();

  switch (expr->node_kind()) {
    case RESOLVED_LITERAL:
    case RESOLVED_EXPRESSION_COLUMN:
    case RESOLVED_DMLDEFAULT:
      // No validation required.
      break;
    case RESOLVED_PARAMETER:
      ZETASQL_RETURN_IF_ERROR(
          ValidateResolvedParameter(expr->GetAs<ResolvedParameter>()));
      break;
    case RESOLVED_COLUMN_REF: {
      const ResolvedColumnRef* column_ref = expr->GetAs<ResolvedColumnRef>();
      ZETASQL_RETURN_IF_ERROR(CheckColumnIsPresentInColumnSet(
          column_ref->column(),
          column_ref->is_correlated() ? visible_parameters : visible_columns));
      break;
    }
    case RESOLVED_ARGUMENT_REF:
      ZETASQL_RET_CHECK(
          zetasql_base::ContainsKey(allowed_argument_kinds_,
                           expr->GetAs<ResolvedArgumentRef>()->argument_kind()))
          << "ResolvedArgumentRef with unexpected kind:\n"
          << expr->DebugString();
      break;
    case RESOLVED_CAST:
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedCast(
          visible_columns, visible_parameters,
          expr->GetAs<ResolvedCast>()));
      break;
    case RESOLVED_CONSTANT: {
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedConstant(
          visible_columns, visible_parameters,
          expr->GetAs<ResolvedConstant>()));
      break;
    }
    case RESOLVED_FUNCTION_CALL: {
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedFunctionCallBase(
          visible_columns, visible_parameters,
          expr->GetAs<ResolvedFunctionCall>()));
      break;
    }
    case RESOLVED_AGGREGATE_FUNCTION_CALL: {
      auto* aggregate_function_call =
          expr->GetAs<ResolvedAggregateFunctionCall>();
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedFunctionCallBase(
          visible_columns, visible_parameters, aggregate_function_call));

      if (aggregate_function_call->having_modifier() != nullptr) {
        ZETASQL_RETURN_IF_ERROR(
            ValidateResolvedExpr(
                visible_columns,
                visible_parameters,
                aggregate_function_call->having_modifier()->having_expr()));
      }
      // Since some aggregate validations depends on the input scan,
      // the ORDER BY and LIMIT to the aggregate arguments are not validated
      // here, but in ValidateResolvedAggregateScan().
      break;
    }
    case RESOLVED_ANALYTIC_FUNCTION_CALL: {
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedFunctionCallBase(
          visible_columns, visible_parameters,
          expr->GetAs<ResolvedAnalyticFunctionCall>()));
      // Since some window frame validations depends on the window ORDER BY,
      // the window frame is not validated here, but in
      // ValidateResolvedWindow().
      break;
    }
    case RESOLVED_MAKE_STRUCT:
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedExprList(
          visible_columns, visible_parameters,
          expr->GetAs<ResolvedMakeStruct>()->field_list()));
      break;
    case RESOLVED_MAKE_PROTO: {
      for (const auto& resolved_make_proto_field :
           expr->GetAs<ResolvedMakeProto>()->field_list()) {
        ZETASQL_RET_CHECK(nullptr != resolved_make_proto_field);
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
            visible_columns, visible_parameters,
            resolved_make_proto_field->expr()));
      }
      break;
    }
    case RESOLVED_GET_STRUCT_FIELD: {
      const ResolvedGetStructField* get_struct_field =
          expr->GetAs<ResolvedGetStructField>();
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
          visible_columns, visible_parameters, get_struct_field->expr()));
      ZETASQL_RET_CHECK(get_struct_field->expr()->type()->IsStruct());
      ZETASQL_RET_CHECK_GE(get_struct_field->field_idx(), 0);
      ZETASQL_RET_CHECK_LT(get_struct_field->field_idx(),
                   get_struct_field->expr()->type()->AsStruct()->num_fields());
      break;
    }
    case RESOLVED_GET_PROTO_FIELD:
      ZETASQL_RETURN_IF_ERROR(
          ValidateResolvedGetProtoFieldExpr(
              visible_columns, visible_parameters,
              expr->GetAs<ResolvedGetProtoField>()));
      break;
    case RESOLVED_SUBQUERY_EXPR:
      ZETASQL_RETURN_IF_ERROR(
          ValidateResolvedSubqueryExpr(visible_columns, visible_parameters,
                                       expr->GetAs<ResolvedSubqueryExpr>()));
      break;
    case RESOLVED_REPLACE_FIELD:
      ZETASQL_RETURN_IF_ERROR(
          ValidateResolvedReplaceField(visible_columns, visible_parameters,
                                       expr->GetAs<ResolvedReplaceField>()));
      break;
    default:
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Unhandled node kind: " << expr->node_kind_string()
             << " in ValidateResolvedExpr";
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedGetProtoFieldExpr(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedGetProtoField* get_proto_field) const {
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      visible_columns, visible_parameters, get_proto_field->expr()));
  ZETASQL_RET_CHECK(get_proto_field->expr()->type()->IsProto());
  // Match proto full_name rather than using pointer equality because
  // the FieldDescriptor is allowed to be an extension, which may
  // come from a different DescriptorPool.
  ZETASQL_RET_CHECK_EQ(
      get_proto_field->expr()->type()->AsProto()->descriptor()->full_name(),
      get_proto_field->field_descriptor()->containing_type()->full_name())
          << "Mismatched proto message "
          << get_proto_field->expr()->type()->DebugString()
          << " and field "
          << get_proto_field->field_descriptor()->full_name();
  if (get_proto_field->field_descriptor()->is_required() ||
      get_proto_field->get_has_bit()) {
    ZETASQL_RET_CHECK(!get_proto_field->default_value().is_valid());
    ZETASQL_RET_CHECK(!get_proto_field->return_default_value_when_unset());
  } else {
    if (get_proto_field->return_default_value_when_unset()) {
      ZETASQL_RET_CHECK(!get_proto_field->type()->IsProto());
      ZETASQL_RET_CHECK(ProtoType::GetUseDefaultsExtension(
                    get_proto_field->field_descriptor()) ||
                get_proto_field->expr()
                        ->type()
                        ->AsProto()
                        ->descriptor()
                        ->file()
                        ->syntax() == google::protobuf::FileDescriptor::SYNTAX_PROTO3);
    }
    ZETASQL_RET_CHECK(get_proto_field->default_value().is_valid());
    ZETASQL_RET_CHECK(get_proto_field->type()->Equals(
        get_proto_field->default_value().type()));
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedReplaceField(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedReplaceField* replace_field) const {
  for (const std::unique_ptr<const ResolvedReplaceFieldItem>&
           replace_field_item : replace_field->replace_field_item_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         replace_field_item->expr()));

    ZETASQL_RET_CHECK(!replace_field_item->struct_index_path().empty() ||
              !replace_field_item->proto_field_path().empty());
    const StructType* current_struct_type =
        replace_field->expr()->type()->AsStruct();
    const zetasql::Type* last_struct_field_type;
    for (const int field_index : replace_field_item->struct_index_path()) {
      // Ensure that the field path is valid by checking field containment.
      ZETASQL_RET_CHECK(current_struct_type != nullptr);
      ZETASQL_RET_CHECK_GE(field_index, 0);
      ZETASQL_RET_CHECK_LT(field_index, current_struct_type->num_fields());
      last_struct_field_type = current_struct_type->field(field_index).type;
      current_struct_type = last_struct_field_type->AsStruct();
    }

    if (!replace_field_item->proto_field_path().empty()) {
      const std::string base_proto_name =
          replace_field_item->struct_index_path().empty()
              ? replace_field->expr()
                    ->type()
                    ->AsProto()
                    ->descriptor()
                    ->full_name()
              : last_struct_field_type->AsProto()->descriptor()->full_name();
      std::string containing_proto_name = base_proto_name;
      for (const google::protobuf::FieldDescriptor* field :
           replace_field_item->proto_field_path()) {
        // Ensure that the field path is valid by checking field containment.
        ZETASQL_RET_CHECK(!containing_proto_name.empty())
            << "Unable to identify parent message of field: "
            << field->full_name();
        ZETASQL_RET_CHECK_EQ(containing_proto_name,
                     field->containing_type()->full_name())
            << "Mismatched proto message " << containing_proto_name
            << " and field " << field->full_name();
        if (field->message_type() != nullptr) {
          containing_proto_name = field->message_type()->full_name();
        } else {
          containing_proto_name.clear();
        }
      }
    }
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedSubqueryExpr(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedSubqueryExpr* resolved_subquery_expr) const {
  ZETASQL_RET_CHECK_EQ(
      resolved_subquery_expr->subquery_type() == ResolvedSubqueryExpr::IN,
      resolved_subquery_expr->in_expr() != nullptr)
      << "Subquery expressions of IN type should have <in_expr> populated";

  if (resolved_subquery_expr->in_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         resolved_subquery_expr->in_expr()));
  }

  std::set<ResolvedColumn> subquery_parameters;
  for (const std::unique_ptr<const ResolvedColumnRef>& column_ref :
       resolved_subquery_expr->parameter_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         column_ref.get()));
    subquery_parameters.insert(column_ref->column());
  }
  // The subquery sees only its own parameters, not the
  // visible_parameters that were passed in.
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(resolved_subquery_expr->subquery(),
                                       subquery_parameters));

  switch (resolved_subquery_expr->subquery_type()) {
    case ResolvedSubqueryExpr::SCALAR:
    case ResolvedSubqueryExpr::ARRAY:
    case ResolvedSubqueryExpr::IN:
      ZETASQL_RET_CHECK_EQ(resolved_subquery_expr->subquery()->column_list_size(), 1)
          << "Expression subquery must produce exactly one column";
      if (resolved_subquery_expr->in_expr() != nullptr) {
        const Type* in_expr_type = resolved_subquery_expr->in_expr()->type();
        const Type* in_subquery_type =
            resolved_subquery_expr->subquery()->column_list(0).type();
        ZETASQL_RET_CHECK(in_expr_type->SupportsEquality());
        ZETASQL_RET_CHECK(in_subquery_type->SupportsEquality());

        const bool argument_types_equal =
            in_expr_type->Equals(in_subquery_type);
        const bool argument_types_int64_and_uint64 =
            (in_expr_type->IsInt64() && in_subquery_type->IsUint64()) ||
            (in_expr_type->IsUint64() && in_subquery_type->IsInt64());
        ZETASQL_RET_CHECK(argument_types_equal || argument_types_int64_and_uint64);
      }
      break;
    case ResolvedSubqueryExpr::EXISTS:
      break;
  }

  ZETASQL_RETURN_IF_ERROR(ValidateHintList(resolved_subquery_expr->hint_list()));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedComputedColumn(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedComputedColumn* computed_column) const {
  ZETASQL_RET_CHECK(nullptr != computed_column);

  const ResolvedExpr* expr = computed_column->expr();
  ZETASQL_RET_CHECK(nullptr != expr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       expr));
  ZETASQL_RET_CHECK(computed_column->column().type()->Equals(expr->type()))
      << computed_column->DebugString()  // includes newline
      << "column: " << computed_column->column().DebugString()
      << " type: " << computed_column->column().type()->DebugString();
  // TODO: Add a more general check to handle any ResolvedExpr
  // (not just RESOLVED_COLUMN_REF).  The ResolvedExpr should not
  // reference the ResolvedColumn to be computed.
  if (computed_column->expr()->node_kind() == RESOLVED_COLUMN_REF &&
      computed_column->column() ==
        computed_column->expr()->GetAs<ResolvedColumnRef>()->column()) {
    return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
           << "ResolvedComputedColumn expression cannot reference itself: "
           << computed_column->DebugString();
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedComputedColumnList(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
        computed_column_list)
    const {
  for (const auto& computed_column : computed_column_list) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateResolvedComputedColumn(visible_columns, visible_parameters,
                                       computed_column.get()));
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedOutputColumn(
    const std::set<ResolvedColumn>& visible_columns,
    const ResolvedOutputColumn* output_column) const {
  ZETASQL_RET_CHECK(nullptr != output_column);

  ZETASQL_RETURN_IF_ERROR(CheckColumnIsPresentInColumnSet(
      output_column->column(), visible_columns));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedOutputColumnList(
    const std::vector<ResolvedColumn>& visible_columns,
    const std::vector<std::unique_ptr<const ResolvedOutputColumn>>&
        output_column_list,
    bool is_value_table) const {
  ZETASQL_RET_CHECK(!output_column_list.empty())
      << "Statement must produce at least one output column";
  const std::set<ResolvedColumn> visible_columns_set(
      visible_columns.begin(), visible_columns.end());
  for (const auto& output_column : output_column_list) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateResolvedOutputColumn(visible_columns_set, output_column.get()));
  }
  if (is_value_table) {
    if (output_column_list.size() != 1) {
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Statement producing a value table must produce exactly one "
                "column; this one has "
             << output_column_list.size();
    }
    if (!IsInternalAlias(output_column_list[0]->name())) {
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Statement producing a value table must produce an anonymous "
                "column; this one has name "
             << ToIdentifierLiteral(output_column_list[0]->name());
    }
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::AddColumnList(const ResolvedColumnList& column_list,
                                      std::set<ResolvedColumn>* visible_columns)
    const {
  ZETASQL_RET_CHECK(nullptr != visible_columns);
  for (const ResolvedColumn& column : column_list) {
    visible_columns->insert(column);
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::AddColumnFromComputedColumn(
    const ResolvedComputedColumn* computed_column,
    std::set<ResolvedColumn>* visible_columns) const {
  ZETASQL_RET_CHECK(nullptr != visible_columns && nullptr != computed_column);
  visible_columns->insert(computed_column->column());
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::AddColumnsFromComputedColumnList(
    const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
        computed_column_list,
    std::set<ResolvedColumn>* visible_columns) const {
  ZETASQL_RET_CHECK(nullptr != visible_columns);
  for (const auto& computed_column : computed_column_list) {
    ZETASQL_RETURN_IF_ERROR(AddColumnFromComputedColumn(computed_column.get(),
                                                visible_columns));
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedTableScan(
    const ResolvedTableScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) const {

  const Table* table = scan->table();
  ZETASQL_RET_CHECK(nullptr != table);

  if (scan->for_system_time_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr({}, visible_parameters,
                                         scan->for_system_time_expr()));
    ZETASQL_RET_CHECK(scan->for_system_time_expr()->type()->IsTimestamp())
        << "TableScan has for_system_type_expr with non-TIMESTAMP type: "
        << scan->for_system_time_expr()->type()->DebugString();
  }

  // Ideally, column_index_list should always be the same size as column_list.
  // However, for historical reason, some clients don't provide a
  // column_index_list and they match the table column by column name instead
  // of indexes. Therefore there's an exception here to allow empty
  // column_index_list. Once all the client migrations are done, this exception
  // should be removed.
  // TODO: remove this exception.
  if (scan->column_index_list_size() == 0) {
    return zetasql_base::OkStatus();
  }

  // Checks that all columns have corresponding indexes.
  ZETASQL_RET_CHECK_EQ(scan->column_list_size(), scan->column_index_list_size());
  const int num_columns = table->NumColumns();

  for (const int index : scan->column_index_list()) {
    ZETASQL_RET_CHECK_GE(index, 0);
    ZETASQL_RET_CHECK_LT(index, num_columns);
    ZETASQL_RET_CHECK(nullptr != table->GetColumn(index));
  }

  return zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedJoinScan(
    const ResolvedJoinScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) const {

  ZETASQL_RET_CHECK(nullptr != scan->left_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->left_scan(), visible_parameters));
  ZETASQL_RET_CHECK(nullptr != scan->right_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->right_scan(), visible_parameters));

  std::set<ResolvedColumn> left_visible_columns, right_visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->left_scan()->column_list(), &left_visible_columns));
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->right_scan()->column_list(), &right_visible_columns));
  // Both left and right scans should not have any common column references
  // introduced in the visible set for the on_condition.
  ZETASQL_RET_CHECK(!zetasql_base::SortedContainersHaveIntersection(left_visible_columns,
                                                   right_visible_columns));

  const std::set<ResolvedColumn> visible_columns =
      zetasql_base::STLSetUnion(left_visible_columns, right_visible_columns);
  if (nullptr != scan->join_expr()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         scan->join_expr()));
    ZETASQL_RET_CHECK(scan->join_expr()->type()->IsBool())
        << "JoinScan has join_expr with non-BOOL type: "
        << scan->join_expr()->type()->DebugString();
  }
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedArrayScan(
    const ResolvedArrayScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) const {

  std::set<ResolvedColumn> visible_columns;
  if (nullptr != scan->input_scan()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(),
                                         visible_parameters));
    ZETASQL_RETURN_IF_ERROR(
        AddColumnList(scan->input_scan()->column_list(), &visible_columns));
  }
  ZETASQL_RET_CHECK(nullptr != scan->array_expr());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       scan->array_expr()));
  ZETASQL_RET_CHECK(scan->array_expr()->type()->IsArray())
      << "ArrayScan of non-ARRAY type: "
      << scan->array_expr()->type()->DebugString();
  visible_columns.insert(scan->element_column());
  if (nullptr != scan->array_offset_column()) {
    visible_columns.insert(scan->array_offset_column()->column());
  }
  if (nullptr != scan->join_expr()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         scan->join_expr()));
    ZETASQL_RET_CHECK(scan->join_expr()->type()->IsBool())
        << "ArrayScan has join_expr with non-BOOL type: "
        << scan->join_expr()->type()->DebugString();
  }
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedFilterScan(
    const ResolvedFilterScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) const {

  ZETASQL_RET_CHECK(nullptr != scan->input_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(), visible_parameters));

  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->input_scan()->column_list(), &visible_columns));
  ZETASQL_RET_CHECK(nullptr != scan->filter_expr());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       scan->filter_expr()));
  ZETASQL_RET_CHECK(scan->filter_expr()->type()->IsBool())
      << "FilterScan has expression with non-BOOL type: "
      << scan->filter_expr()->type()->DebugString();
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedAggregateComputedColumn(
    const ResolvedComputedColumn* computed_column,
    const std::set<ResolvedColumn>& input_scan_visible_columns,
    const std::set<ResolvedColumn>& visible_parameters) const {
  ZETASQL_RET_CHECK_EQ(computed_column->expr()->node_kind(),
               RESOLVED_AGGREGATE_FUNCTION_CALL);
  const ResolvedAggregateFunctionCall* aggregate_function_call =
      computed_column->expr()->GetAs<ResolvedAggregateFunctionCall>();
  const Function* aggregate_function = aggregate_function_call->function();
  const std::string& function_name = aggregate_function->Name();
  if (!aggregate_function->SupportsOrderingArguments() &&
      !aggregate_function_call->order_by_item_list().empty()) {
    return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
           << "Aggregate function " << function_name
           << " does not support ordering arguments,"
           << " but has an ORDER BY clause:\n"
           << aggregate_function->DebugString();
  }
  if (!aggregate_function->SupportsLimitArguments() &&
      aggregate_function_call->limit() != nullptr) {
    return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
           << "Aggregate function " << function_name
           << " does not support limiting arguments,"
           << " but has a LIMIT clause:\n"
           << aggregate_function->DebugString();
  }

  for (const auto& order_by_item :
       aggregate_function_call->order_by_item_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(input_scan_visible_columns,
                                         visible_parameters,
                                         order_by_item->column_ref()));
  }

  if (aggregate_function_call->limit() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateArgumentIsInt64Constant(
        aggregate_function_call->limit()));
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedAggregateScanBase(
    const ResolvedAggregateScanBase* scan,
    const std::set<ResolvedColumn>& visible_parameters,
    std::set<ResolvedColumn>* input_scan_visible_columns) const {

  ZETASQL_RET_CHECK(nullptr != scan->input_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(), visible_parameters));

  ZETASQL_RETURN_IF_ERROR(AddColumnList(scan->input_scan()->column_list(),
                                input_scan_visible_columns));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedComputedColumnList(
      *input_scan_visible_columns, visible_parameters, scan->group_by_list()));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedComputedColumnList(
      *input_scan_visible_columns, visible_parameters, scan->aggregate_list()));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedAggregateScan(
    const ResolvedAggregateScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) const {

  std::set<ResolvedColumn> input_scan_visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedAggregateScanBase(
      scan, visible_parameters, &input_scan_visible_columns));

  if (!scan->grouping_set_list().empty()) {
    // There should be a grouping set for every prefix of the rollup list,
    // including the empty one.
    ZETASQL_RET_CHECK_EQ(scan->grouping_set_list_size(),
                 scan->rollup_column_list_size() + 1);

    std::set<ResolvedColumn> group_by_columns;
    for (const auto& group_by_column : scan->group_by_list()) {
      group_by_columns.insert(group_by_column->column());
    }

    // group_by_columns should be non-empty, and each item in the rollup list or
    // a grouping set should be a computed column from group_by_columns.
    ZETASQL_RET_CHECK(!group_by_columns.empty());
    std::set<ResolvedColumn> rollup_columns;
    for (const auto& column_ref : scan->rollup_column_list()) {
      ZETASQL_RETURN_IF_ERROR(CheckColumnIsPresentInColumnSet(column_ref->column(),
                                                      group_by_columns));
      rollup_columns.insert(column_ref->column());
    }
    // All group by columns should also be rollup columns. We can't use
    // std::set_intersect because ResolvedColumn does not support assignment.
    for (const ResolvedColumn& group_by_column : group_by_columns) {
      ZETASQL_RETURN_IF_ERROR(
          CheckColumnIsPresentInColumnSet(group_by_column, rollup_columns));
    }

    for (const auto& grouping_set : scan->grouping_set_list()) {
      // Columns should be unique within each grouping set.
      std::set<ResolvedColumn> grouping_set_columns;
      for (const auto& column_ref : grouping_set->group_by_column_list()) {
        ZETASQL_RETURN_IF_ERROR(CheckColumnIsPresentInColumnSet(column_ref->column(),
                                                        group_by_columns));
        ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(&grouping_set_columns,
                                          column_ref->column()));
      }
    }
  } else {
    // Presence of grouping sets should indicate that there is a rollup list.
    ZETASQL_RET_CHECK(scan->rollup_column_list().empty());
  }

  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(AddColumnsFromComputedColumnList(
      scan->group_by_list(), &visible_columns));
  ZETASQL_RETURN_IF_ERROR(AddColumnsFromComputedColumnList(
      scan->aggregate_list(), &visible_columns));
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  // Validates other constructs in aggregates such as ORDER BY and LIMIT.
  for (const auto& computed_column : scan->aggregate_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedAggregateComputedColumn(
        computed_column.get(),
        input_scan_visible_columns, visible_parameters));
  }
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status ValidatePercentArgument(const ResolvedExpr* expr) {
  ZETASQL_RET_CHECK(expr != nullptr);
  ZETASQL_RET_CHECK(expr->node_kind() == RESOLVED_LITERAL ||
            expr->node_kind() == RESOLVED_PARAMETER)
      << "PERCENT argument is of incorrect kind: "
      << expr->node_kind_string();

  ZETASQL_RET_CHECK(expr->type()->IsInt64() || expr->type()->IsDouble())
      << "PERCENT argument must be either a double or an int64";

  if (expr->node_kind() == RESOLVED_LITERAL) {
    // If a literal, we validate its value.
    const Value value = expr->GetAs<ResolvedLiteral>()->value();
    bool is_valid = false;
    if (value.type()->IsInt64()) {
      is_valid = (!value.is_null() &&
                  value.int64_value() >= 0 && value.int64_value() <= 100);
    } else {
      ZETASQL_RET_CHECK(value.type()->IsDouble());
      is_valid = (!value.is_null() &&
                  value.double_value() >= 0.0 && value.double_value() <= 100.0);
    }
    if (!is_valid) {
      return ::zetasql_base::UnknownErrorBuilder(ZETASQL_LOC)
             << "PERCENT argument value must be in the range [0, 100]";
    }
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedSampleScan(
    const ResolvedSampleScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) const {

  ZETASQL_RET_CHECK(nullptr != scan->input_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(), visible_parameters));

  ZETASQL_RET_CHECK(!scan->method().empty());
  ZETASQL_RET_CHECK(nullptr != scan->size());

  const ResolvedSampleScan::SampleUnit unit = scan->unit();
  if (unit == ResolvedSampleScan::ROWS) {
    ZETASQL_RETURN_IF_ERROR(ValidateArgumentIsInt64Constant(scan->size()));
  } else {
    ZETASQL_RET_CHECK_EQ(unit, ResolvedSampleScan::PERCENT);
    ZETASQL_RETURN_IF_ERROR(ValidatePercentArgument(scan->size()));
  }

  if (scan->repeatable_argument() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateArgumentIsInt64Constant(scan->repeatable_argument()));
  }

  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->input_scan()->column_list(), &visible_columns));
  if (nullptr != scan->weight_column()) {
    visible_columns.insert(scan->weight_column()->column());
  }
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  if (!scan->partition_by_list().empty()) {
    ZETASQL_RET_CHECK_EQ(ResolvedSampleScan::ROWS, unit);
    ZETASQL_RET_CHECK_EQ("RESERVOIR", absl::AsciiStrToUpper(scan->method()));
    for (const auto& partition_by_expr : scan->partition_by_list()) {
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
          visible_columns, /*visible_parameters=*/{}, partition_by_expr.get()));
    }
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedAnalyticScan(
    const ResolvedAnalyticScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) const {

  ZETASQL_RET_CHECK(nullptr != scan->input_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(), visible_parameters));

  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(AddColumnList(scan->input_scan()->column_list(),
                                &visible_columns));

  for (const std::unique_ptr<const ResolvedAnalyticFunctionGroup>& group :
       scan->function_group_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedAnalyticFunctionGroup(
        group.get(), visible_columns, visible_parameters));
  }

  for (const std::unique_ptr<const ResolvedAnalyticFunctionGroup>& group :
         scan->function_group_list()) {
    ZETASQL_RETURN_IF_ERROR(AddColumnsFromComputedColumnList(
        group->analytic_function_list(), &visible_columns));
  }
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedAnalyticFunctionGroup(
    const ResolvedAnalyticFunctionGroup* group,
    const std::set<ResolvedColumn>& input_visible_columns,
    const std::set<ResolvedColumn>& visible_parameters) const {

  for (const auto& computed_column : group->analytic_function_list()) {
    const ResolvedAnalyticFunctionCall* analytic_function_call =
        computed_column->expr()->GetAs<ResolvedAnalyticFunctionCall>();
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        input_visible_columns, visible_parameters, analytic_function_call));

    const Function* analytic_function = analytic_function_call->function();
    const std::string function_name = analytic_function->Name();
    if (!analytic_function->SupportsOverClause()) {
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Function " << function_name
             << " cannot be used in an analytic function call, since it does "
                "not"
                " support an OVER clause";
    }
    if (analytic_function_call->distinct()) {
      if (!analytic_function_call->function()->IsAggregate()) {
        return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
               << "Cannot specify DISTINCT for a non-aggregate analytic "
                  "function:\n"
               << analytic_function_call->DebugString();
      }
      if (analytic_function_call->argument_list_size() == 0) {
        return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
               << "DISTINCT function call " << function_name
               << " does not have an argument:\n"
               << analytic_function_call->DebugString();
      }
      if (group->order_by() != nullptr) {
        return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
               << "Cannot specify a window ORDER BY clause in a DISTINCT "
                  "analytic "
                  "function call:\n"
               << analytic_function_call->DebugString();
      }
      if (analytic_function_call->window_frame() != nullptr) {
        const ResolvedWindowFrame* window_frame =
            analytic_function_call->window_frame();
        if (window_frame->start_expr()->boundary_type() !=
                ResolvedWindowFrameExpr::UNBOUNDED_PRECEDING ||
            window_frame->end_expr()->boundary_type() !=
                ResolvedWindowFrameExpr::UNBOUNDED_FOLLOWING) {
          return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
                 << "The window frame for a DISTINCT analytic function call "
                    "must "
                    "be UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING:\n"
                 << analytic_function_call->DebugString();
        }
      }
    }

    if (!analytic_function->SupportsWindowFraming() &&
        analytic_function_call->window_frame() != nullptr) {
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Analytic function " << function_name
             << " does not support framing, but has a window framing clause:\n"
             << analytic_function_call->DebugString();
    }
    if (analytic_function->RequiresWindowOrdering() &&
        group->order_by() == nullptr) {
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Analytic function " << function_name
             << " must have a window ORDER BY clause:\n"
             << group->DebugString();
    }
    if (!analytic_function->SupportsWindowOrdering() &&
        group->order_by() != nullptr) {
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Analytic function " << function_name
             << " does not support a window ORDER BY clause:\n"
             << analytic_function_call->DebugString();
    }

    if (analytic_function_call->window_frame() != nullptr) {
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedWindowFrame(
          input_visible_columns, visible_parameters, group->order_by(),
          analytic_function_call->window_frame()));
    }
  }

  if (group->partition_by() != nullptr) {
    for (const auto& column_ref :
         group->partition_by()->partition_by_list()) {
      std::string no_partitioning_type;
      if (!column_ref->type()->SupportsPartitioning(language_options_,
                                                    &no_partitioning_type)) {
        return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
               << "Type of PARTITIONING expressions " << no_partitioning_type
               << " does not support partitioning:\n"
               << column_ref->DebugString();
      }
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
          input_visible_columns, visible_parameters, column_ref.get()));
    }
  }

  if (group->order_by() != nullptr) {
    for (const auto& order_by_item : group->order_by()->order_by_item_list()) {
      if (!order_by_item->column_ref()->type()->SupportsOrdering(
              language_options_, /*type_description=*/nullptr)) {
        return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
               << "Type of ORDERING expressions "
               << order_by_item->column_ref()->type()->DebugString()
               << " does not support ordering:\n"
               << order_by_item->column_ref()->DebugString();
      }
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(input_visible_columns,
                                           visible_parameters,
                                           order_by_item->column_ref()));
    }
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedWindowFrame(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedWindowOrdering* window_ordering,
    const ResolvedWindowFrame* window_frame) const {
  ZETASQL_RET_CHECK(window_frame->start_expr() != nullptr &&
            window_frame->end_expr() != nullptr)
      << "Window frame must specify both the starting and the ending boundary"
         ":\n"
      << window_frame->DebugString();
  ZETASQL_RET_CHECK(window_frame->frame_unit() == ResolvedWindowFrame::ROWS ||
            window_frame->frame_unit() == ResolvedWindowFrame::RANGE)
      << "Unhandled window frame unit "
      << window_frame->GetFrameUnitString() << ":\n"
      << window_frame->DebugString();

  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedWindowFrameExpr(visible_columns, visible_parameters,
                                      window_ordering,
                                      window_frame->frame_unit(),
                                      window_frame->start_expr()));
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedWindowFrameExpr(visible_columns, visible_parameters,
                                      window_ordering,
                                      window_frame->frame_unit(),
                                      window_frame->end_expr()));

  if (IsEmptyWindowFrame(*window_frame)) {
    return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
           << "Window frame must be non-empty";
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedWindowFrameExpr(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedWindowOrdering* window_ordering,
    const ResolvedWindowFrame::FrameUnit& frame_unit,
    const ResolvedWindowFrameExpr* window_frame_expr) const {
  switch (window_frame_expr->boundary_type()) {
    case ResolvedWindowFrameExpr::UNBOUNDED_PRECEDING:
    case ResolvedWindowFrameExpr::CURRENT_ROW:
    case ResolvedWindowFrameExpr::UNBOUNDED_FOLLOWING:
      if (window_frame_expr->expression() != nullptr) {
        return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
               << "Window frame boundary of type "
               << window_frame_expr->GetBoundaryTypeString()
               << " cannot have an offset expression:\n"
               << window_frame_expr->DebugString();
      }
      break;
    case ResolvedWindowFrameExpr::OFFSET_PRECEDING:
    case ResolvedWindowFrameExpr::OFFSET_FOLLOWING: {
      if (window_frame_expr->expression() == nullptr) {
        return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
               << "Window frame boundary of type "
               << window_frame_expr->GetBoundaryTypeString()
               << " must specify an offset expression:\n"
               << window_frame_expr->DebugString();
      }
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
          visible_columns, visible_parameters,
          window_frame_expr->expression()));

      const ResolvedExpr* order_expr = nullptr;
      if (frame_unit == ResolvedWindowFrame::RANGE) {
        if (window_ordering == nullptr ||
            window_ordering->order_by_item_list_size() != 1) {
          return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
                 << "Must have exactly one ordering key for a RANGE-based "
                    "window"
                 << " with an offset boundary:\n"
                 << window_ordering->DebugString();
        }
        order_expr = window_ordering->order_by_item_list(0)->column_ref();
      }

      ZETASQL_RETURN_IF_ERROR(ValidateResolvedWindowFrameExprType(
          frame_unit, order_expr, *window_frame_expr->expression()));
      break;
    }
    default:
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Unhandled window boundary type:\n"
             << window_frame_expr->GetBoundaryTypeString();
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedWindowFrameExprType(
    const ResolvedWindowFrame::FrameUnit& frame_unit,
    const ResolvedExpr* window_ordering_expr,
    const ResolvedExpr& window_frame_expr) const {
  switch (frame_unit) {
    case ResolvedWindowFrame::ROWS:
      if (!window_frame_expr.type()->IsInt64()) {
        return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
               << "ROWS-based window boundary expression must be INT64 type, "
                  "but "
                  "has type "
               << window_frame_expr.type()->DebugString() << ":\n"
               << window_frame_expr.DebugString();
      }
      break;
    case ResolvedWindowFrame::RANGE:
      ZETASQL_RET_CHECK(window_ordering_expr != nullptr);
      if (!window_ordering_expr->type()->IsNumerical()) {
        return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
               << "Ordering expression must be numeric type in a RANGE-based "
                  "window, but has type "
               << window_ordering_expr->type()->DebugString() << ":\n"
               << window_ordering_expr->DebugString();
      }
      if (!window_ordering_expr->type()->Equals(window_frame_expr.type())) {
        return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
               << "RANGE-based window boundary expression has a different type "
                  "with the ordering expression ("
               << window_frame_expr.type()->DebugString() << " vs. "
               << window_ordering_expr->type()->DebugString() << "):\n"
               << window_frame_expr.DebugString();
      }
      break;
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedSetOperationScan(
    const ResolvedSetOperationScan* set_op_scan,
    const std::set<ResolvedColumn>& visible_parameters) const {

  ZETASQL_RET_CHECK_GE(set_op_scan->input_item_list_size(), 2);

  for (const auto& input_item : set_op_scan->input_item_list()) {
    const ResolvedScan* input_scan = input_item->scan();
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(input_scan, visible_parameters));

    const std::set<ResolvedColumn> produced_columns(
        input_scan->column_list().begin(), input_scan->column_list().end());

    // <set_op_scan>'s output <column_list> matches 1:1 with the
    // <output_column_list> from the input item, but not necessarily with
    // the input item's <input_scan>'s <column_list>.
    ZETASQL_RET_CHECK_EQ(input_item->output_column_list_size(),
                 set_op_scan->column_list_size());
    for (int i = 0; i < set_op_scan->column_list_size(); ++i) {
      const ResolvedColumn& input_column = input_item->output_column_list(i);
      ZETASQL_RET_CHECK(set_op_scan->column_list(i).type()->Equals(input_column.type()))
          << "SetOperation input column type does not match output type";

      ZETASQL_RET_CHECK(zetasql_base::ContainsKey(produced_columns, input_column))
          << "SetOperation input scan does not produce column referenced in "
             "output_column_list: "
          << input_column.DebugString();
    }
  }

  return ::zetasql_base::OkStatus();
}

static bool IsResolvedLiteralOrParameter(ResolvedNodeKind kind) {
  return kind == RESOLVED_LITERAL || kind == RESOLVED_PARAMETER;
}

zetasql_base::Status Validator::ValidateArgumentIsInt64Constant(
    const ResolvedExpr* expr) const {
  ZETASQL_RET_CHECK(expr != nullptr);

  ZETASQL_RET_CHECK(IsResolvedLiteralOrParameter(expr->node_kind()) ||
            (expr->node_kind() == RESOLVED_CAST &&
             expr->type()->IsInt64() &&
             IsResolvedLiteralOrParameter(
                 expr->GetAs<ResolvedCast>()->expr()->node_kind())))
      << "LIMIT ... OFFSET ... arg is of incorrect node kind: "
      << expr->node_kind_string();

  ZETASQL_RET_CHECK(expr->type()->IsInt64())
      << "LIMIT ... OFFSET .... literal must be an integer";

  if (expr->node_kind() == RESOLVED_LITERAL) {
    // If a literal, we can also validate its value.
    const Value value = expr->GetAs<ResolvedLiteral>()->value();
    ZETASQL_RET_CHECK(value.type()->IsInt64());
    ZETASQL_RET_CHECK(!value.is_null())
        << "Unexpected literal with null value: " << value.DebugString();
    ZETASQL_RET_CHECK_GE(value.int64_value(), 0);
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedLimitOffsetScan(
    const ResolvedLimitOffsetScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) const {

  ZETASQL_RET_CHECK(scan->limit() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateArgumentIsInt64Constant(scan->limit()));

  if (scan->offset() != nullptr) {
    // OFFSET is optional.
    ZETASQL_RETURN_IF_ERROR(ValidateArgumentIsInt64Constant(scan->offset()));
  }

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(),
                                       visible_parameters));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedProjectScan(
    const ResolvedProjectScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) const {

  ZETASQL_RET_CHECK(nullptr != scan->input_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(), visible_parameters));

  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->input_scan()->column_list(), &visible_columns));
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedComputedColumnList(visible_columns, visible_parameters,
                                         scan->expr_list()));
  ZETASQL_RETURN_IF_ERROR(
      AddColumnsFromComputedColumnList(scan->expr_list(), &visible_columns));
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedTVFScan(
    const ResolvedTVFScan* resolved_tvf_scan,
    const std::set<ResolvedColumn>& visible_parameters) const {
  ZETASQL_RET_CHECK_EQ(resolved_tvf_scan->argument_list_size(),
               resolved_tvf_scan->signature()->input_arguments().size());
  for (int arg_idx = 0; arg_idx < resolved_tvf_scan->argument_list_size();
       ++arg_idx) {
    const ResolvedTVFArgument* resolved_tvf_arg =
        resolved_tvf_scan->argument_list(arg_idx);
    ZETASQL_RETURN_IF_ERROR(
        ValidateResolvedTVFArgument(visible_parameters, resolved_tvf_arg));

    // If the function signature specifies a required schema for the relation
    // argument, check that the column names and types are a superset of those
    // from the scan.
    if (resolved_tvf_arg->scan() != nullptr) {
      const TableValuedFunction* tvf = resolved_tvf_scan->tvf();
      // We currently expect table valued function to have only one signature
      // since function overloading is not supported.
      ZETASQL_RET_CHECK_EQ(tvf->NumSignatures(), 1);
      // This check is only done if the argument is a fixed relation where the
      // relation input schema is available. Table valued functions can have
      // templated argument types (e.g. ANY_TABLE) but those will not be
      // checked here.
      if (!tvf->GetSignature(0)->argument(arg_idx).IsFixedRelation()) {
        continue;
      }
      const FunctionArgumentTypeOptions& options =
          tvf->GetSignature(0)->argument(arg_idx).options();
      const TVFRelation& required_input_schema =
          options.relation_input_schema();

      ZETASQL_RET_CHECK_LT(arg_idx,
                   resolved_tvf_scan->signature()->input_arguments().size());
      const TVFInputArgumentType& tvf_signature_arg =
          resolved_tvf_scan->signature()->argument(arg_idx);
      ZETASQL_RET_CHECK(tvf_signature_arg.is_relation());
      const TVFRelation& input_relation = tvf_signature_arg.relation();
      ZETASQL_RETURN_IF_ERROR(ValidateRelationSchemaInResolvedTVFArgument(
          required_input_schema, input_relation, resolved_tvf_arg));
    } else if (resolved_tvf_arg->model() != nullptr) {
      const TableValuedFunction* tvf = resolved_tvf_scan->tvf();
      // We currently expect table valued function to have only one signature
      // since function overloading is not supported.
      ZETASQL_RET_CHECK_EQ(tvf->NumSignatures(), 1);
    }
  }

  ZETASQL_RET_CHECK(resolved_tvf_scan->signature() != nullptr);
  if (resolved_tvf_scan->signature()->result_schema().is_value_table()) {
    const TVFRelation& schema = resolved_tvf_scan->signature()->result_schema();
    int64_t num_pseudo_columns = std::count_if(
        schema.columns().begin(), schema.columns().end(),
        [](const TVFSchemaColumn& column) { return column.is_pseudo_column; });
    ZETASQL_RET_CHECK_EQ(1, schema.num_columns() - num_pseudo_columns);
    ZETASQL_RET_CHECK(!schema.column(0).is_pseudo_column);
  } else {
    ZETASQL_RET_CHECK_NE(0,
                 resolved_tvf_scan->signature()->result_schema().num_columns());
  }

  ZETASQL_RETURN_IF_ERROR(ValidateHintList(resolved_tvf_scan->hint_list()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedRelationArgumentScan(
    const ResolvedRelationArgumentScan* arg_ref,
    const std::set<ResolvedColumn>& visible_parameters) const {
  // If we're currently validating a ResolvedCreateTableFunctionStmt, find the
  // argument in the current CREATE TABLE FUNCTION statement with the same name
  // as 'arg_ref'.
  if (current_create_table_function_stmt_ != nullptr) {
    ZETASQL_RET_CHECK(std::any_of(
        current_create_table_function_stmt_->argument_name_list().begin(),
        current_create_table_function_stmt_->argument_name_list().end(),
        [arg_ref](const std::string& arg_name) {
          return zetasql_base::CaseEqual(arg_ref->name(), arg_name);
        }));
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedWithScan(
    const ResolvedWithScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) const {

  // The main query can be correlated. The aliased subqueries cannot.
  ZETASQL_RET_CHECK(nullptr != scan->query());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->query(), visible_parameters));

  for (const auto& with_entry : scan->with_entry_list()) {
    ZETASQL_RET_CHECK(nullptr != with_entry);
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(with_entry->with_subquery(),
                                         {} /* visible_parameters */));
  }

  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->query()->column_list(), &visible_columns));
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedStatement(
    const ResolvedStatement* statement) {
  ZETASQL_RET_CHECK(nullptr != statement);

  zetasql_base::Status status;
  switch (statement->node_kind()) {
    case RESOLVED_QUERY_STMT:
      status = ValidateResolvedQueryStmt(statement->GetAs<ResolvedQueryStmt>());
      break;
    case RESOLVED_EXPLAIN_STMT:
      status = ValidateResolvedStatement(
          statement->GetAs<ResolvedExplainStmt>()->statement());
      break;
    case RESOLVED_CREATE_DATABASE_STMT:
      status = ValidateResolvedCreateDatabaseStmt(
          statement->GetAs<ResolvedCreateDatabaseStmt>());
      break;
    case RESOLVED_CREATE_INDEX_STMT:
      status = ValidateResolvedIndexStmt(
          statement->GetAs<ResolvedCreateIndexStmt>());
      break;
    case RESOLVED_CREATE_TABLE_STMT:
      status = ValidateResolvedCreateTableStmt(
          statement->GetAs<ResolvedCreateTableStmt>());
      break;
    case RESOLVED_CREATE_TABLE_AS_SELECT_STMT:
      status = ValidateResolvedCreateTableAsSelectStmt(
          statement->GetAs<ResolvedCreateTableAsSelectStmt>());
      break;
    case RESOLVED_CREATE_MODEL_STMT:
      status = ValidateResolvedCreateModelStmt(
          statement->GetAs<ResolvedCreateModelStmt>());
      break;
    case RESOLVED_CREATE_VIEW_STMT:
      status = ValidateResolvedCreateViewStmt(
          statement->GetAs<ResolvedCreateViewStmt>());
      break;
    case RESOLVED_CREATE_MATERIALIZED_VIEW_STMT:
      status = ValidateResolvedCreateMaterializedViewStmt(
          statement->GetAs<ResolvedCreateMaterializedViewStmt>());
      break;
    case RESOLVED_CREATE_EXTERNAL_TABLE_STMT:
      status = ValidateResolvedCreateExternalTableStmt(
          statement->GetAs<ResolvedCreateExternalTableStmt>());
      break;
    case RESOLVED_CREATE_ROW_ACCESS_POLICY_STMT:
      status = ValidateResolvedCreateRowAccessPolicyStmt(
          statement->GetAs<ResolvedCreateRowAccessPolicyStmt>());
      break;
    case RESOLVED_CREATE_CONSTANT_STMT:
      status = ValidateResolvedCreateConstantStmt(
          statement->GetAs<ResolvedCreateConstantStmt>());
      break;
    case RESOLVED_CREATE_FUNCTION_STMT:
      status = ValidateResolvedCreateFunctionStmt(
          statement->GetAs<ResolvedCreateFunctionStmt>());
      break;
    case RESOLVED_CREATE_TABLE_FUNCTION_STMT:
      status = ValidateResolvedCreateTableFunctionStmt(
          statement->GetAs<ResolvedCreateTableFunctionStmt>());
      break;
    case RESOLVED_CREATE_PROCEDURE_STMT:
      status = ValidateResolvedCreateProcedureStmt(
          statement->GetAs<ResolvedCreateProcedureStmt>());
      break;
    case RESOLVED_EXPORT_DATA_STMT:
      status = ValidateResolvedExportDataStmt(
          statement->GetAs<ResolvedExportDataStmt>());
      break;
    case RESOLVED_CALL_STMT:
      status = ValidateResolvedCallStmt(statement->GetAs<ResolvedCallStmt>());
      break;
    case RESOLVED_DEFINE_TABLE_STMT:
      status = ValidateResolvedDefineTableStmt(
          statement->GetAs<ResolvedDefineTableStmt>());
      break;
    case RESOLVED_DESCRIBE_STMT:
      status = ValidateResolvedDescribeStmt(
          statement->GetAs<ResolvedDescribeStmt>());
      break;
    case RESOLVED_SHOW_STMT:
      status = ValidateResolvedShowStmt(
          statement->GetAs<ResolvedShowStmt>());
      break;
    case RESOLVED_BEGIN_STMT:
      status = ValidateResolvedBeginStmt(
          statement->GetAs<ResolvedBeginStmt>());
      break;
    case RESOLVED_SET_TRANSACTION_STMT:
      status = ValidateResolvedSetTransactionStmt(
          statement->GetAs<ResolvedSetTransactionStmt>());
      break;
    case RESOLVED_COMMIT_STMT:
      status =
          ValidateResolvedCommitStmt(statement->GetAs<ResolvedCommitStmt>());
      break;
    case RESOLVED_ROLLBACK_STMT:
      status = ValidateResolvedRollbackStmt(
          statement->GetAs<ResolvedRollbackStmt>());
      break;
    case RESOLVED_START_BATCH_STMT:
      status = ValidateResolvedStartBatchStmt(
          statement->GetAs<ResolvedStartBatchStmt>());
      break;
    case RESOLVED_RUN_BATCH_STMT:
      status = ValidateResolvedRunBatchStmt(
          statement->GetAs<ResolvedRunBatchStmt>());
      break;
    case RESOLVED_ABORT_BATCH_STMT:
      status = ValidateResolvedAbortBatchStmt(
          statement->GetAs<ResolvedAbortBatchStmt>());
      break;
    case RESOLVED_DROP_STMT:
      status = ValidateResolvedDropStmt(
          statement->GetAs<ResolvedDropStmt>());
      break;
    case RESOLVED_DROP_MATERIALIZED_VIEW_STMT:
      status = ValidateResolvedDropMaterializedViewStmt(
          statement->GetAs<ResolvedDropMaterializedViewStmt>());
      break;
    case RESOLVED_DROP_FUNCTION_STMT:
      status = ValidateResolvedDropFunctionStmt(
          statement->GetAs<ResolvedDropFunctionStmt>());
      break;
    case RESOLVED_DROP_ROW_ACCESS_POLICY_STMT:
      status = ValidateResolvedDropRowAccessPolicyStmt(
          statement->GetAs<ResolvedDropRowAccessPolicyStmt>());
      break;
    case RESOLVED_GRANT_STMT:
      status = ValidateResolvedGrantStmt(
          statement->GetAs<ResolvedGrantStmt>());
      break;
    case RESOLVED_REVOKE_STMT:
      status = ValidateResolvedRevokeStmt(
          statement->GetAs<ResolvedRevokeStmt>());
      break;
    case RESOLVED_INSERT_STMT:
      status = ValidateResolvedInsertStmt(
          statement->GetAs<ResolvedInsertStmt>());
      break;
    case RESOLVED_DELETE_STMT:
      status = ValidateResolvedDeleteStmt(
          statement->GetAs<ResolvedDeleteStmt>());
      break;
    case RESOLVED_UPDATE_STMT:
      status = ValidateResolvedUpdateStmt(
          statement->GetAs<ResolvedUpdateStmt>());
      break;
    case RESOLVED_MERGE_STMT:
      status = ValidateResolvedMergeStmt(statement->GetAs<ResolvedMergeStmt>());
      break;
    case RESOLVED_TRUNCATE_STMT:
      status = ValidateResolvedTruncateStmt(
          statement->GetAs<ResolvedTruncateStmt>());
      break;
    case RESOLVED_ALTER_ROW_POLICY_STMT:
      status = ValidateResolvedAlterRowPolicyStmt(
          statement->GetAs<ResolvedAlterRowPolicyStmt>());
      break;
    case RESOLVED_ALTER_MATERIALIZED_VIEW_STMT:
      status = ValidateResolvedAlterObjectStmt(
          statement->GetAs<ResolvedAlterMaterializedViewStmt>());
      break;
    case RESOLVED_ALTER_TABLE_SET_OPTIONS_STMT:
      status = ValidateResolvedAlterTableSetOptionsStmt(
          statement->GetAs<ResolvedAlterTableSetOptionsStmt>());
      break;
    case RESOLVED_ALTER_TABLE_STMT:
      status = ValidateResolvedAlterObjectStmt(
          statement->GetAs<ResolvedAlterTableStmt>());
      break;
    case RESOLVED_ALTER_VIEW_STMT:
      status = ValidateResolvedAlterObjectStmt(
          statement->GetAs<ResolvedAlterViewStmt>());
      break;
    case RESOLVED_RENAME_STMT:
      status = ValidateResolvedRenameStmt(
          statement->GetAs<ResolvedRenameStmt>());
      break;
    case RESOLVED_IMPORT_STMT:
      status = ValidateResolvedImportStmt(
          statement->GetAs<ResolvedImportStmt>());
      break;
    case RESOLVED_MODULE_STMT:
      status = ValidateResolvedModuleStmt(
          statement->GetAs<ResolvedModuleStmt>());
      break;
    case RESOLVED_ASSERT_STMT:
      status =
          ValidateResolvedAssertStmt(statement->GetAs<ResolvedAssertStmt>());
      break;
    default:
      ZETASQL_RET_CHECK_FAIL() << "Cannot validate statement of type "
                       << statement->node_kind_string();
  }

  status.Update(ValidateHintList(statement->hint_list()));

  if (!status.ok()) {
    if (status.code() == zetasql_base::StatusCode::kResourceExhausted) {
      // Don't wrap a resource exhausted status into internal error. This error
      // may still occur for a valid properly resolved expression (stack
      // exhaustion in case of deeply nested expression). There exist cases
      // where the validator uses more stack than parsing/analysis (b/65294961).
      return status;
    }
    return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
           << "Resolved AST validation failed: " << status.message() << "\n"
           << statement->DebugString();
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCreateDatabaseStmt(
    const ResolvedCreateDatabaseStmt* stmt) const {
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedIndexStmt(
    const ResolvedCreateIndexStmt* stmt) const {
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));

  ZETASQL_RET_CHECK(stmt->table_scan() != nullptr);
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(AddColumnList(stmt->table_scan()->column_list(),
                                &visible_columns));

  for (const auto& index_unnest_column : stmt->unnest_expressions_list()) {
    ZETASQL_RET_CHECK(index_unnest_column->array_expr() != nullptr);
    ZETASQL_RET_CHECK(index_unnest_column->array_expr()->type()->IsArray())
        << "CREATE INDEX Unnest non-ARRAY type: "
        << index_unnest_column->array_expr()->type()->DebugString();
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns,
                                         /*visible_parameters=*/{},
                                         index_unnest_column->array_expr()));

    visible_columns.insert(index_unnest_column->element_column());
    if (index_unnest_column->array_offset_column() != nullptr) {
      visible_columns.insert(
          index_unnest_column->array_offset_column()->column());
    }
  }

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedComputedColumnList(
      visible_columns,
      /*visible_parameters=*/{}, stmt->computed_columns_list()));
  ZETASQL_RETURN_IF_ERROR(AddColumnsFromComputedColumnList(
      stmt->computed_columns_list(), &visible_columns));

  for (const auto& item : stmt->index_item_list()) {
    ZETASQL_RETURN_IF_ERROR(CheckColumnIsPresentInColumnSet(
        item->column_ref()->column(), visible_columns));
  }

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExprList(visible_columns,
                                           /*visible_parameters=*/{},
                                           stmt->storing_expression_list()));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCreateTableStmtBase(
    const ResolvedCreateTableStmtBase* stmt) const {
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  // Build the list of visible_columns.
  std::set<ResolvedColumn> visible_columns;
  for (const auto& column_definition : stmt->column_definition_list()) {
    if (!zetasql_base::InsertIfNotPresent(&visible_columns,
                                 column_definition->column())) {
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Column already used: "
             << column_definition->column().DebugString();
    }
  }

  for (const auto& column_definition : stmt->column_definition_list()) {
    if (column_definition->annotations() != nullptr) {
      ZETASQL_RETURN_IF_ERROR(ValidateColumnAnnotations(
          column_definition->annotations()));
    }
    ZETASQL_RET_CHECK(column_definition->type() != nullptr);
    if (column_definition->generated_column_info() != nullptr) {
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedGeneratedColumnInfo(
          column_definition.get(), visible_columns));
    }
  }
  for (const auto& pseudo_column : stmt->pseudo_column_list()) {
    if (!zetasql_base::InsertIfNotPresent(&visible_columns, pseudo_column)) {
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Column already used: " << pseudo_column.DebugString();
    }
  }
  if (stmt->primary_key() != nullptr) {
    std::set<int> column_indexes;
    for (const int i : stmt->primary_key()->column_offset_list()) {
      if (i >= stmt->column_definition_list().size()) {
        return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
            << "Invalid column index " << i << " in PRIMARY KEY";
      }
      if (zetasql_base::ContainsKey(column_indexes, i)) {
        return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
            << "Duplicate column index " << i << " in PRIMARY KEY";
      }
      column_indexes.insert(i);
    }
    ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->primary_key()->option_list()));
  }

  // Used to detect duplicate constraint names.
  absl::flat_hash_set<std::string> constraint_names;

  // Validate foreign keys.
  ZETASQL_RET_CHECK(stmt->foreign_key_list().empty()
            || language_options_.LanguageFeatureEnabled(FEATURE_FOREIGN_KEYS))
      << "Foreign keys are not supported";
  for (const auto& foreign_key : stmt->foreign_key_list()) {
    if (!foreign_key->constraint_name().empty()) {
      ZETASQL_RET_CHECK(constraint_names.insert(foreign_key->constraint_name()).second)
          << "Duplicate constraint name: " << foreign_key->constraint_name();
    }
    const auto& referencing_offsets =
        foreign_key->referencing_column_offset_list();
    ZETASQL_RET_CHECK(!referencing_offsets.empty())
        << "Missing foreign key column offsets";
    const auto& referenced_offsets =
        foreign_key->referenced_column_offset_list();
    ZETASQL_RET_CHECK_EQ(referencing_offsets.size(), referenced_offsets.size())
        << "Size of " << referencing_offsets.size()
        << " for the foreign key referencing column offset list is not the "
        << "same as the size of " << referenced_offsets.size()
        << " for the referenced column offset list";
    const auto& column_definitions = stmt->column_definition_list();
    absl::flat_hash_set<int> referencing_set;
    for (int offset : referencing_offsets) {
      ZETASQL_RET_CHECK(offset >= 0 && offset < column_definitions.size())
          << "Invalid foreign key referencing column at offset " << offset;
      ZETASQL_RET_CHECK(referencing_set.insert(offset).second)
          << "Duplicate foreign key referencing column at offset " << offset;
      auto const& column_definition = column_definitions[offset];
      ZETASQL_RET_CHECK(column_definition->type()->SupportsEquality(language_options_))
          << "Foreign key referencing column at offset" << offset
          << " does not support equality";
    }
    const auto* referenced_table = foreign_key->referenced_table();
    ZETASQL_RET_CHECK_NE(referenced_table, nullptr)
        << "Missing foreign key referenced table";
    absl::flat_hash_set<int> referenced_set;
    for (int offset : referenced_offsets) {
      ZETASQL_RET_CHECK(offset >= 0 && offset < referenced_table->NumColumns())
          << "Invalid foreign key referenced column at offset " << offset;
      ZETASQL_RET_CHECK(referenced_set.insert(offset).second)
          << "Duplicate foreign key referenced column at offset " << offset;
      const auto* type = referenced_table->GetColumn(offset)->GetType();
      ZETASQL_RET_CHECK(type->SupportsEquality(language_options_))
          << "Foreign key referenced column at offset" << offset
          << " does not support equality";
    }
    ZETASQL_RETURN_IF_ERROR(ValidateHintList(foreign_key->option_list()));
  }

  // Validate check constraints.
  ZETASQL_RET_CHECK(stmt->check_constraint_list().empty() ||
            language_options_.LanguageFeatureEnabled(FEATURE_CHECK_CONSTRAINT));
  for (const auto& check_constraint : stmt->check_constraint_list()) {
    if (!check_constraint->constraint_name().empty()) {
      ZETASQL_RET_CHECK(
          constraint_names.insert(check_constraint->constraint_name()).second)
          << "Duplicate constraint name: "
          << check_constraint->constraint_name();
    }
    ZETASQL_RET_CHECK(check_constraint->expression() != nullptr)
        << "Missing expression in CHECK constraint";
    ZETASQL_RET_CHECK(check_constraint->expression()->type()->IsBool())
        << "CHECK constraint expects a boolean expression; got "
        << check_constraint->expression()->type()->ShortTypeName(
               language_options_.product_mode());
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns,
                                         /* visible_parameters= */ {},
                                         check_constraint->expression()));
  }

  for (const auto& partition_by_expr : stmt->partition_by_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        visible_columns, {} /* visible_parameters */, partition_by_expr.get()));
  }
  for (const auto& cluster_by_expr : stmt->cluster_by_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        visible_columns, {} /* visible_parameters */, cluster_by_expr.get()));
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCreateTableStmt(
    const ResolvedCreateTableStmt* stmt) const {
  return ValidateResolvedCreateTableStmtBase(stmt);
}

zetasql_base::Status Validator::ValidateResolvedGeneratedColumnInfo(
    const ResolvedColumnDefinition* column_definition,
    const std::set<ResolvedColumn>& visible_columns) const {
  const ResolvedGeneratedColumnInfo* generated_column_info =
      column_definition->generated_column_info();
  ZETASQL_RET_CHECK(generated_column_info->expression() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns,
                                       /* visible_parameters = */ {},
                                       generated_column_info->expression()));
  ZETASQL_RET_CHECK(generated_column_info->expression()->type() != nullptr);
  ZETASQL_RET_CHECK(column_definition->type() != nullptr);
  ZETASQL_RET_CHECK(generated_column_info->expression()->type()->Equals(
      column_definition->type()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCreateTableAsSelectStmt(
    const ResolvedCreateTableAsSelectStmt* stmt) const {
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(
      stmt->query(), {} /* visible_parameters */));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(
      stmt->query()->column_list(), stmt->output_column_list(),
      stmt->is_value_table()));
  const int num_columns = stmt->column_definition_list_size();
  if (num_columns != stmt->output_column_list_size()) {
    return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
        << "Inconsistent length between column definition list ("
        << stmt->column_definition_list_size()
        << ") and output column list ("
        << stmt->output_column_list_size()
        << ")";
  }
  for (int i = 0; i < num_columns; ++i) {
    const ResolvedOutputColumn* output_column = stmt->output_column_list(i);
    const ResolvedColumnDefinition* column_def =
        stmt->column_definition_list(i);
    const std::string& output_column_name = output_column->name();
    const std::string& column_def_name = column_def->name();
    if (output_column_name != column_def_name) {
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
          << "Output column name '" << output_column_name
          << "' is different from column definition name '"
          << column_def_name << "' for column " << (i + 1);
    }
    const Type* output_type = output_column->column().type();
    const Type* defined_type = column_def->type();
    if (!output_type->Equals(defined_type)) {
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
          << "Output column type " << output_type->DebugString()
          << " is different from column definition type "
          << defined_type->DebugString() << " for column " << (i + 1)
          << " (" << column_def_name << ")";
    }
  }
  return ValidateResolvedCreateTableStmtBase(stmt);
}

zetasql_base::Status Validator::ValidateResolvedCreateModelStmt(
    const ResolvedCreateModelStmt* stmt) const {
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedScan(stmt->query(), {} /* visible_parameters */));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(stmt->query()->column_list(),
                                                   stmt->output_column_list(),
                                                   /*is_value_table=*/false));
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCreateViewStmt(
    const ResolvedCreateViewStmt* stmt) const {
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(
      stmt->query(), {} /* visible_parameters */));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(
      stmt->query()->column_list(), stmt->output_column_list(),
      stmt->is_value_table()));
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCreateMaterializedViewStmt(
    const ResolvedCreateMaterializedViewStmt* stmt) const {
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedScan(stmt->query(), {} /* visible_parameters */));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(stmt->query()->column_list(),
                                                   stmt->output_column_list(),
                                                   stmt->is_value_table()));
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCreateExternalTableStmt(
    const ResolvedCreateExternalTableStmt* stmt) const {
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCreateRowAccessPolicyStmt(
    const ResolvedCreateRowAccessPolicyStmt* stmt) const {

  ZETASQL_RET_CHECK(stmt->table_scan() != nullptr);
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(stmt->table_scan()->column_list(), &visible_columns));

  const ResolvedExpr* predicate = stmt->predicate();
  ZETASQL_RET_CHECK(predicate != nullptr);
  ZETASQL_RET_CHECK(predicate->type()->IsBool())
      << "CreateRowAccessPolicyStmt has predicate with non-BOOL type: "
      << predicate->type()->DebugString();
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      visible_columns, {}  /* visible_parameters */, predicate));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCreateConstantStmt(
    const ResolvedCreateConstantStmt* stmt) {
  ZETASQL_RET_CHECK(stmt->expr() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      {} /* visible_columns */, {} /* visible_parameters */, stmt->expr()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCreateFunctionStmt(
    const ResolvedCreateFunctionStmt* stmt) {
  bool contains_templated_args =
      std::any_of(stmt->signature().arguments().begin(),
                  stmt->signature().arguments().end(),
                  [](const FunctionArgumentType& arg_type) {
                    return arg_type.IsTemplated();
                  });
  if (contains_templated_args) {
    // This function declaration contained one or more templated argument types.
    // In this case the resolver sets the 'language' field to SQL and the 'code'
    // field to contain the original std::string contents of the SQL expression body.
    // The 'arguments' are empty in this case since the templated type
    // information is present in the function signature instead.
    if (stmt->signature().result_type().IsTemplated()) {
      ZETASQL_RET_CHECK_EQ("SQL", stmt->language());
    }
    ZETASQL_RET_CHECK(stmt->function_expression() == nullptr);
    ZETASQL_RET_CHECK(!stmt->code().empty());
  } else {
    // This function declaration did not contain any templated argument types.
    // In this case the function should have a concrete return type.
    ZETASQL_RET_CHECK(stmt->return_type()->Equals(
        stmt->signature().result_type().type()));
  }
  ZETASQL_RET_CHECK(stmt->return_type() != nullptr);

  // For non-aggregates, no columns are visible.  For aggregates, columns
  // created by the aggregate expressions are visible.
  std::set<ResolvedColumn> visible_columns;

  if (!stmt->aggregate_expression_list().empty()) {
    ZETASQL_RET_CHECK(stmt->is_aggregate());
    ZETASQL_RET_CHECK(stmt->function_expression() != nullptr);

    zetasql_base::VarSetter<ArgumentKindSet> setter(
        &allowed_argument_kinds_,
        {ResolvedArgumentDefEnums::AGGREGATE,
         ResolvedArgumentDefEnums::NOT_AGGREGATE});

    for (const auto& computed_column : stmt->aggregate_expression_list()) {
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedAggregateComputedColumn(
          computed_column.get(),
          {} /* visible_columns */, {} /* visible_parameters */));
    }

    ZETASQL_RETURN_IF_ERROR(AddColumnsFromComputedColumnList(
        stmt->aggregate_expression_list(), &visible_columns));
  }

  if (stmt->function_expression() != nullptr) {
    zetasql_base::VarSetter<ArgumentKindSet> setter(
        &allowed_argument_kinds_,
        {stmt->is_aggregate()
            ? ResolvedArgumentDefEnums::NOT_AGGREGATE
            : ResolvedArgumentDefEnums::SCALAR});

    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        visible_columns, {} /* visible_parameters */,
        stmt->function_expression()));
    ZETASQL_RET_CHECK(stmt->function_expression()->type()->Equals(stmt->return_type()));

    // Check no query parameter in function body.
    std::vector<const ResolvedNode*> parameter_nodes;
    stmt->function_expression()->GetDescendantsWithKinds({RESOLVED_PARAMETER},
                                                         &parameter_nodes);
    ZETASQL_RET_CHECK(parameter_nodes.empty());
  }
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return ::zetasql_base::OkStatus();
}

// Check that the function signature does not contain unsupported templated
// argument types.
static zetasql_base::Status CheckFunctionArgumentType(
    const FunctionArgumentTypeList& argument_type_list,
    absl::string_view statement_type) {
  for (const FunctionArgumentType& arg_type : argument_type_list) {
    switch (arg_type.kind()) {
      case ARG_TYPE_FIXED:
      case ARG_TYPE_ARBITRARY:
      case ARG_TYPE_RELATION:
        continue;
      default:
        ZETASQL_RET_CHECK_FAIL() << "Unexpected " << statement_type
                         << " argument type: " << arg_type.DebugString();
    }
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCreateTableFunctionStmt(
    const ResolvedCreateTableFunctionStmt* stmt) {
  ZETASQL_RET_CHECK_EQ(stmt->argument_name_list().size(),
               stmt->signature().arguments().size());
  ZETASQL_RETURN_IF_ERROR(CheckFunctionArgumentType(stmt->signature().arguments(),
                                            "CREATE TABLE FUNCTION"));
  if (stmt->query() != nullptr) {
    ZETASQL_RET_CHECK(!stmt->language().empty());

    zetasql_base::VarSetter<ArgumentKindSet> allowed_arg_kinds_setter(
        &allowed_argument_kinds_,
        {ResolvedArgumentDefEnums::SCALAR});
    zetasql_base::VarSetter<const ResolvedCreateTableFunctionStmt*> stmt_setter(
        &current_create_table_function_stmt_, stmt);

    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(stmt->query(),
                                         {} /* visible_parameters */));
    ZETASQL_RET_CHECK(!stmt->output_column_list().empty());

    // Check no query parameter in table function body.
    std::vector<const ResolvedNode*> parameter_nodes;
    stmt->query()->GetDescendantsWithKinds({RESOLVED_PARAMETER},
                                           &parameter_nodes);
    ZETASQL_RET_CHECK(parameter_nodes.empty());
  }

  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  if (stmt->signature().IsTemplated()) {
    ZETASQL_RET_CHECK(stmt->output_column_list().empty());
    ZETASQL_RET_CHECK(stmt->query() == nullptr);
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCreateProcedureStmt(
    const ResolvedCreateProcedureStmt* stmt) {
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  ZETASQL_RET_CHECK_EQ(stmt->argument_name_list().size(),
               stmt->signature().arguments().size());
  ZETASQL_RETURN_IF_ERROR(CheckFunctionArgumentType(stmt->signature().arguments(),
                                            "CREATE PROCEDURE"));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedExportDataStmt(
    const ResolvedExportDataStmt* stmt) const {
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(
      stmt->query(), {} /* visible_parameters */));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(
      stmt->query()->column_list(), stmt->output_column_list(),
      stmt->is_value_table()));
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCallStmt(
    const ResolvedCallStmt* stmt) const {

  ZETASQL_RET_CHECK(stmt->procedure() != nullptr)
      << "ResolvedCallStmt does not have a Procedure:\n" << stmt->DebugString();

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExprList({}, {}, stmt->argument_list()));

  ZETASQL_RET_CHECK(stmt->signature().IsConcrete())
       << "ResolvedCallStmt must have a concrete signature:\n"
       << stmt->DebugString();
  const int num_args = stmt->signature().NumConcreteArguments();
  ZETASQL_RET_CHECK_EQ(stmt->argument_list_size(), num_args);
  for (int i = 0; i < num_args; ++i) {
    ZETASQL_RET_CHECK(stmt->argument_list(i)->type()->Equals(
        stmt->signature().ConcreteArgumentType(i)));
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedDefineTableStmt(
    const ResolvedDefineTableStmt* stmt) const {
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedDescribeStmt(
    const ResolvedDescribeStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedShowStmt(
    const ResolvedShowStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedBeginStmt(
    const ResolvedBeginStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedSetTransactionStmt(
    const ResolvedSetTransactionStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedCommitStmt(
    const ResolvedCommitStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedRollbackStmt(
    const ResolvedRollbackStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedStartBatchStmt(
    const ResolvedStartBatchStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedRunBatchStmt(
    const ResolvedRunBatchStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedAbortBatchStmt(
    const ResolvedAbortBatchStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedDropStmt(
    const ResolvedDropStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedDropFunctionStmt(
    const ResolvedDropFunctionStmt* stmt) const {
  ZETASQL_RET_CHECK_EQ(stmt->signature() == nullptr, stmt->arguments() == nullptr);
  if (stmt->signature() != nullptr) {
    const bool has_relation_args =
        std::any_of(stmt->signature()->signature().arguments().begin(),
                    stmt->signature()->signature().arguments().end(),
                    [](const FunctionArgumentType& arg_type) {
                      return arg_type.IsRelation();
                    });
    if (has_relation_args) {
      ZETASQL_RET_CHECK_EQ(0, stmt->arguments()->arg_list_size());
    } else {
      ZETASQL_RET_CHECK_EQ(stmt->signature()->signature().arguments().size(),
                   stmt->arguments()->arg_list_size());
    }
    ZETASQL_RET_CHECK(stmt->signature()->signature().result_type().IsVoid());
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedDropMaterializedViewStmt(
    const ResolvedDropMaterializedViewStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedDropRowAccessPolicyStmt(
    const ResolvedDropRowAccessPolicyStmt* stmt) const {
  ZETASQL_RET_CHECK(!(stmt->is_drop_all() && stmt->is_if_exists()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedGrantStmt(
    const ResolvedGrantStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedRevokeStmt(
    const ResolvedRevokeStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedOrderByScan(
    const ResolvedOrderByScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) const {

  ZETASQL_RET_CHECK(nullptr != scan->input_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(), visible_parameters));

  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->input_scan()->column_list(), &visible_columns));
  for (const auto& order_by_item : scan->order_by_item_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         order_by_item->column_ref()));
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedQueryStmt(
    const ResolvedQueryStmt* query) const {
  ZETASQL_RET_CHECK(nullptr != query);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(query->query(),
                                       {} /* visible_parameters */));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(
      query->query()->column_list(), query->output_column_list(),
      query->is_value_table()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedScan(
    const ResolvedScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) const {
  ZETASQL_RET_CHECK(nullptr != scan);

  switch (scan->node_kind()) {
    case RESOLVED_SINGLE_ROW_SCAN:
    case RESOLVED_WITH_REF_SCAN:
      // No validation is required.
      break;
    case RESOLVED_TABLE_SCAN:
      ZETASQL_RETURN_IF_ERROR(
          ValidateResolvedTableScan(scan->GetAs<ResolvedTableScan>(),
                                    visible_parameters));
      break;
    case RESOLVED_JOIN_SCAN:
      ZETASQL_RETURN_IF_ERROR(
          ValidateResolvedJoinScan(scan->GetAs<ResolvedJoinScan>(),
                                   visible_parameters));
      break;
    case RESOLVED_ARRAY_SCAN:
      ZETASQL_RETURN_IF_ERROR(
          ValidateResolvedArrayScan(scan->GetAs<ResolvedArrayScan>(),
                                    visible_parameters));
      break;
    case RESOLVED_FILTER_SCAN:
      ZETASQL_RETURN_IF_ERROR(
          ValidateResolvedFilterScan(scan->GetAs<ResolvedFilterScan>(),
                                     visible_parameters));
      break;
    case RESOLVED_AGGREGATE_SCAN:
      ZETASQL_RETURN_IF_ERROR(
          ValidateResolvedAggregateScan(scan->GetAs<ResolvedAggregateScan>(),
                                        visible_parameters));
      break;
    case RESOLVED_SET_OPERATION_SCAN:
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedSetOperationScan(
          scan->GetAs<ResolvedSetOperationScan>(), visible_parameters));
      break;
    case RESOLVED_PROJECT_SCAN:
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedProjectScan(
          scan->GetAs<ResolvedProjectScan>(), visible_parameters));
      break;
    case RESOLVED_ORDER_BY_SCAN:
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedOrderByScan(
          scan->GetAs<ResolvedOrderByScan>(), visible_parameters));
      break;
    case RESOLVED_LIMIT_OFFSET_SCAN:
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedLimitOffsetScan(
          scan->GetAs<ResolvedLimitOffsetScan>(), visible_parameters));
      break;
    case RESOLVED_WITH_SCAN:
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedWithScan(
          scan->GetAs<ResolvedWithScan>(), visible_parameters));
      break;
    case RESOLVED_ANALYTIC_SCAN:
      ZETASQL_RETURN_IF_ERROR(
          ValidateResolvedAnalyticScan(scan->GetAs<ResolvedAnalyticScan>(),
                                       visible_parameters));
      break;
    case RESOLVED_SAMPLE_SCAN:
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedSampleScan(
          scan->GetAs<ResolvedSampleScan>(), visible_parameters));
      break;
    case RESOLVED_TVFSCAN:
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedTVFScan(scan->GetAs<ResolvedTVFScan>(),
                                              visible_parameters));
      break;
    case RESOLVED_RELATION_ARGUMENT_SCAN:
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedRelationArgumentScan(
          scan->GetAs<ResolvedRelationArgumentScan>(), visible_parameters));
      break;
    default:
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Unhandled node kind: " << scan->node_kind_string()
             << " in ValidateResolvedScan";
  }

  if (scan->is_ordered()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScanOrdering(scan));
  }

  ZETASQL_RETURN_IF_ERROR(ValidateHintList(scan->hint_list()));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedScanOrdering(
    const ResolvedScan* scan) const {
  ZETASQL_RET_CHECK(nullptr != scan);

  const ResolvedScan* input_scan = nullptr;

  switch (scan->node_kind()) {
    // OrderByScan can always produce an ordered result.
    case RESOLVED_ORDER_BY_SCAN:
      return ::zetasql_base::OkStatus();

    // These scans can produce an ordered result if their input was ordered.
    // These cases fill in <input_scan>, which is checked below the switch.
    case RESOLVED_PROJECT_SCAN:
      input_scan = scan->GetAs<ResolvedProjectScan>()->input_scan();
      break;
    case RESOLVED_LIMIT_OFFSET_SCAN:
      input_scan = scan->GetAs<ResolvedLimitOffsetScan>()->input_scan();
      break;
    case RESOLVED_WITH_SCAN:
      input_scan = scan->GetAs<ResolvedWithScan>()->query();
      break;

    // For all other scan types, is_ordered is not allowed.
    default:
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Node kind: " << scan->node_kind_string()
             << " cannot have is_ordered=true:\n"
             << scan->DebugString();
  }

  ZETASQL_RET_CHECK(input_scan != nullptr);
  if (!input_scan->is_ordered()) {
    return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
           << "Node has is_ordered=true but its input does not:\n"
           << scan->DebugString();
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateHintList(
    const std::vector<std::unique_ptr<const ResolvedOption>>& hint_list) const {
  for (const std::unique_ptr<const ResolvedOption>& hint : hint_list) {
    // The value in a Hint must be a constant so we don't pass any visible
    // column names.
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr({}, {}, hint->value()));
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateColumnAnnotations(
    const ResolvedColumnAnnotations* annotations) const {
  ZETASQL_RET_CHECK(annotations != nullptr);
  for (const std::unique_ptr<const ResolvedColumnAnnotations>& child :
       annotations->child_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateColumnAnnotations(child.get()));
  }
  return ValidateHintList(annotations->option_list());
}

template <class STMT>
zetasql_base::Status Validator::ValidateResolvedDMLStmt(
    const STMT* stmt,
    const ResolvedColumn* array_element_column,
    std::set<ResolvedColumn>* visible_columns) const {
  visible_columns->clear();
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->hint_list()));

  if (array_element_column == nullptr) {
    // Non-nested DML.
    ZETASQL_RET_CHECK(stmt->table_scan() != nullptr);
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(stmt->table_scan(),
                                         {} /* visible_parameters */));

    ZETASQL_RETURN_IF_ERROR(
        AddColumnList(stmt->table_scan()->column_list(), visible_columns));
  } else {
    // Nested DML.
    ZETASQL_RET_CHECK(stmt->table_scan() == nullptr);
    // The array element is not visible in nested INSERTs.
    if (!std::is_same<STMT, ResolvedInsertStmt>::value) {
      visible_columns->insert(*array_element_column);
    }
  }

  if (stmt->assert_rows_modified() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateArgumentIsInt64Constant(stmt->assert_rows_modified()->rows()));
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedInsertStmt(
    const ResolvedInsertStmt* stmt,
    const std::set<ResolvedColumn>* outer_visible_columns,
    const ResolvedColumn* array_element_column) const {
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedDMLStmt(stmt, array_element_column,
                                          &visible_columns));

  std::vector<ResolvedColumn> inserted_columns;

  ZETASQL_RET_CHECK_EQ(array_element_column == nullptr,
               outer_visible_columns == nullptr);
  if (array_element_column == nullptr) {
    // Non-nested INSERTs.
    ZETASQL_RET_CHECK_GT(stmt->insert_column_list_size(), 0);
    for (const ResolvedColumn& column : stmt->insert_column_list()) {
      ZETASQL_RETURN_IF_ERROR(CheckColumnIsPresentInColumnSet(
          column, visible_columns));
    }
    inserted_columns = stmt->insert_column_list();
  } else {
    // Nested INSERTs.
    ZETASQL_RET_CHECK(stmt->table_scan() == nullptr);
    ZETASQL_RET_CHECK_EQ(stmt->insert_column_list_size(), 0)
        << "insert_column_list not supported on nested INSERTs";
    ZETASQL_RET_CHECK_EQ(stmt->insert_mode(), ResolvedInsertStmt::OR_ERROR)
        << "insert_mode not supported on nested INSERTs";

    ZETASQL_RET_CHECK(visible_columns.empty());
    inserted_columns.push_back(*array_element_column);

    visible_columns.insert(outer_visible_columns->begin(),
                           outer_visible_columns->end());
  }

  ZETASQL_RET_CHECK_EQ(stmt->query() != nullptr,
               stmt->query_output_column_list_size() > 0);

  if (stmt->query() != nullptr) {
    ZETASQL_RET_CHECK_EQ(stmt->row_list_size(), 0)
        << "INSERT has both query and VALUES";

    std::set<ResolvedColumn> visible_parameters;
    for (const std::unique_ptr<const ResolvedColumnRef>& parameter :
         stmt->query_parameter_list()) {
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns,
                                           /*visible_parameters=*/{},
                                           parameter.get()));
      visible_parameters.insert(parameter->column());
    }

    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(stmt->query(), visible_parameters));

    const std::set<ResolvedColumn> produced_columns(
        stmt->query()->column_list().begin(),
        stmt->query()->column_list().end());

    const ResolvedColumnList& query_output_column_list =
        stmt->query_output_column_list();

    ZETASQL_RET_CHECK_EQ(query_output_column_list.size(), inserted_columns.size());
    for (int i = 0; i < query_output_column_list.size(); ++i) {
      ZETASQL_RET_CHECK(query_output_column_list[i].type()->Equals(
          inserted_columns[i].type()));
      ZETASQL_RET_CHECK(zetasql_base::ContainsKey(produced_columns, query_output_column_list[i]))
          << "InsertStmt query does not produce column referenced in "
             "query_output_column_list: "
          << query_output_column_list[i].DebugString();
    }
  } else {
    ZETASQL_RET_CHECK_GT(stmt->row_list_size(), 0)
        << "INSERT has neither query nor VALUES";
    for (const std::unique_ptr<const ResolvedInsertRow>& insert_row :
         stmt->row_list()) {
      ZETASQL_RET_CHECK_EQ(insert_row->value_list_size(), inserted_columns.size());
      for (int i = 0; i < insert_row->value_list_size(); ++i) {
        const ResolvedDMLValue* dml_value = insert_row->value_list(i);
        ZETASQL_RET_CHECK(dml_value->value() != nullptr);
        const ResolvedExpr* expr = dml_value->value();
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
            visible_columns, {} /* visible_parameters */, expr));
        ZETASQL_RET_CHECK(expr->type()->Equals(inserted_columns[i].type()));
      }
    }
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedDeleteStmt(
    const ResolvedDeleteStmt* stmt,
    const std::set<ResolvedColumn>* outer_visible_columns,
    const ResolvedColumn* array_element_column) const {
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedDMLStmt(stmt, array_element_column,
                                          &visible_columns));
  if (outer_visible_columns != nullptr) {
    visible_columns.insert(outer_visible_columns->begin(),
                           outer_visible_columns->end());
  }

  if (array_element_column == nullptr) {
    // Top-level DELETE.
    ZETASQL_RET_CHECK(stmt->array_offset_column() == nullptr);
  } else {
    // Nested DELETE.
    ZETASQL_RET_CHECK(stmt->table_scan() == nullptr);
  }

  if (stmt->array_offset_column() != nullptr) {
    visible_columns.insert(stmt->array_offset_column()->column());
  }

  ZETASQL_RET_CHECK(stmt->where_expr() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      visible_columns, {} /* visible_parameters */, stmt->where_expr()));
  ZETASQL_RET_CHECK(stmt->where_expr()->type()->IsBool())
      << "DeleteStmt has WHERE expression with non-BOOL type: "
      << stmt->where_expr()->type()->DebugString();
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::CheckExprIsPath(const ResolvedExpr* expr,
                                        const ResolvedColumnRef** ref) const {
  switch (expr->node_kind()) {
    case RESOLVED_COLUMN_REF:
      *ref = expr->GetAs<ResolvedColumnRef>();
      return ::zetasql_base::OkStatus();
    case RESOLVED_GET_PROTO_FIELD:
      return CheckExprIsPath(expr->GetAs<ResolvedGetProtoField>()->expr(), ref);
    case RESOLVED_GET_STRUCT_FIELD:
      return CheckExprIsPath(expr->GetAs<ResolvedGetStructField>()->expr(),
                             ref);
    default:
      ZETASQL_RET_CHECK_FAIL()
          << "Expression is not a path: " << expr->node_kind_string();
  }
}

zetasql_base::Status Validator::ValidateResolvedUpdateStmt(
    const ResolvedUpdateStmt* stmt,
    const std::set<ResolvedColumn>* outer_visible_columns,
    const ResolvedColumn* array_element_column) const {
  std::set<ResolvedColumn> target_visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedDMLStmt(stmt, array_element_column,
                                          &target_visible_columns));

  if (array_element_column == nullptr) {
    // Top-level UPDATE.
    ZETASQL_RET_CHECK(stmt->array_offset_column() == nullptr);
    ZETASQL_RET_CHECK_EQ(stmt->table_scan()->column_index_list().size(),
                 stmt->column_access_list().size());
  } else {
    // Nested UPDATE.
    ZETASQL_RET_CHECK(stmt->table_scan() == nullptr);
    ZETASQL_RET_CHECK(stmt->from_scan() == nullptr);
    ZETASQL_RET_CHECK_EQ(stmt->column_access_list().size(), 0);
  }

  std::set<ResolvedColumn> all_visible_columns(target_visible_columns);
  if (stmt->from_scan() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(stmt->from_scan(),
                                         {} /* visible_parameters */));
    ZETASQL_RETURN_IF_ERROR(
        AddColumnList(stmt->from_scan()->column_list(), &all_visible_columns));
  }
  if (stmt->array_offset_column() != nullptr) {
    all_visible_columns.insert(stmt->array_offset_column()->column());
  }
  if (outer_visible_columns != nullptr) {
    all_visible_columns.insert(outer_visible_columns->begin(),
                               outer_visible_columns->end());
  }

  ZETASQL_RET_CHECK_GT(stmt->update_item_list_size(), 0);
  for (const std::unique_ptr<const ResolvedUpdateItem>& item :
       stmt->update_item_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedUpdateItem(
        item.get(), /*allow_nested_statements=*/true,
        /*array_element_column=*/nullptr, target_visible_columns,
        all_visible_columns));
  }

  ZETASQL_RET_CHECK(stmt->where_expr() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      all_visible_columns, {} /* visible parameters */, stmt->where_expr()));
  ZETASQL_RET_CHECK(stmt->where_expr()->type()->IsBool())
      << "UpdateStmt has WHERE expression with non-BOOL type: "
      << stmt->where_expr()->type()->DebugString();

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedUpdateItem(
    const ResolvedUpdateItem* item, bool allow_nested_statements,
    const ResolvedColumn* array_element_column,
    const std::set<ResolvedColumn>& target_visible_columns,
    const std::set<ResolvedColumn>& offset_and_where_visible_columns) const {

  ZETASQL_RET_CHECK(item->target() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      target_visible_columns, {} /* visible_parameters */, item->target()));

  const ResolvedColumnRef* expr_column_ref = nullptr;
  ZETASQL_RETURN_IF_ERROR(CheckExprIsPath(item->target(), &expr_column_ref));
  ZETASQL_RET_CHECK(expr_column_ref);
  if (array_element_column != nullptr) {
    ZETASQL_RET_CHECK(zetasql_base::ContainsKey(target_visible_columns, *array_element_column));
    ZETASQL_RET_CHECK(!zetasql_base::ContainsKey(offset_and_where_visible_columns,
                                *array_element_column));
    ZETASQL_RET_CHECK_EQ(array_element_column->column_id(),
                 expr_column_ref->column().column_id());
  }

  const Type* target_type = item->target()->type();
  if (item->set_value() != nullptr) {
    // Flat SET {target} = {value} clause.
    ZETASQL_RET_CHECK(item->element_column() == nullptr);
    ZETASQL_RET_CHECK_EQ(item->array_update_list_size(), 0);
    ZETASQL_RET_CHECK_EQ(item->delete_list_size(), 0);
    ZETASQL_RET_CHECK_EQ(item->update_list_size(), 0);
    ZETASQL_RET_CHECK_EQ(item->insert_list_size(), 0);

    ZETASQL_RET_CHECK(item->set_value()->value() != nullptr);
    ZETASQL_RET_CHECK(target_type->Equals(item->set_value()->value()->type()));
  } else {
    // Two Cases:
    // 1) SET {target_array}[<expr>]{optional_remainder} = {value} clause.
    // 2) Nested DML statement.
    ZETASQL_RET_CHECK(target_type->IsArray());
    ZETASQL_RET_CHECK(item->element_column() != nullptr);
    const ResolvedColumn& element_column = item->element_column()->column();
    ZETASQL_RET_CHECK(element_column.IsInitialized());
    ZETASQL_RET_CHECK(
        element_column.type()->Equals(target_type->AsArray()->element_type()));

    if (item->array_update_list_size() > 0) {
      // Array element modification.
      ZETASQL_RET_CHECK_EQ(item->delete_list_size(), 0);
      ZETASQL_RET_CHECK_EQ(item->update_list_size(), 0);
      ZETASQL_RET_CHECK_EQ(item->insert_list_size(), 0);

      for (const auto& array_update_item : item->array_update_list()) {
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedUpdateArrayItem(
            array_update_item.get(), element_column, target_visible_columns,
            offset_and_where_visible_columns));
      }
    } else {
      // Nested DML statement.
      ZETASQL_RET_CHECK_GT(item->delete_list_size() +
                   item->update_list_size() +
                   item->insert_list_size(), 0);
      ZETASQL_RET_CHECK(allow_nested_statements)
          << "nested deletes: " << item->delete_list_size()
          << " nested updates: " << item->update_list_size()
          << " nested inserts: " << item->insert_list_size();
      for (const auto& delete_stmt : item->delete_list()) {
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedDeleteStmt(
            delete_stmt.get(), &offset_and_where_visible_columns,
            &element_column));
      }
      for (const auto& update_stmt : item->update_list()) {
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedUpdateStmt(
            update_stmt.get(), &offset_and_where_visible_columns,
            &element_column));
      }
      for (const auto& insert_stmt : item->insert_list()) {
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedInsertStmt(
            insert_stmt.get(), &offset_and_where_visible_columns,
            &element_column));
      }
    }
  }

  return zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedUpdateArrayItem(
    const ResolvedUpdateArrayItem* item, const ResolvedColumn& element_column,
    const std::set<ResolvedColumn>& target_visible_columns,
    const std::set<ResolvedColumn>& offset_and_where_visible_columns) const {
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(offset_and_where_visible_columns,
                                       /*visible_parameters=*/{},
                                       item->offset()));
  ZETASQL_RET_CHECK_EQ(item->offset()->type()->kind(), TYPE_INT64);

  std::set<ResolvedColumn> child_target_visible_columns(target_visible_columns);
  child_target_visible_columns.insert(element_column);
  // We don't allow [] in the target of a nested DML statement, and
  // gen_resolved_ast.py documents that a ResolvedUpdateItem child of a
  // ResolvedUpdateArrayItem node cannot have nested statements.
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedUpdateItem(
      item->update_item(), /*allow_nested_statements=*/false, &element_column,
      child_target_visible_columns, offset_and_where_visible_columns));

  return zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedMergeWhen(
    const ResolvedMergeWhen* merge_when,
    const std::set<ResolvedColumn>& all_visible_columns,
    const std::set<ResolvedColumn>& source_visible_columns,
    const std::set<ResolvedColumn>& target_visible_columns) const {
  const std::set<ResolvedColumn>* visible_columns = nullptr;
  switch (merge_when->match_type()) {
    // For WHEN MATCHED and WHEN NOT MATCHED BY SOURCE clauses, only UPDATE and
    // DELETE are allowed.
    case ResolvedMergeWhen::MATCHED:
      visible_columns = &all_visible_columns;
      ZETASQL_RET_CHECK_NE(ResolvedMergeWhen::INSERT, merge_when->action_type());
      break;
    case ResolvedMergeWhen::NOT_MATCHED_BY_SOURCE:
      visible_columns = &target_visible_columns;
      ZETASQL_RET_CHECK_NE(ResolvedMergeWhen::INSERT, merge_when->action_type());
      break;
    // For WHEN NOT MATCHED BY TARGET merge_when, only INSERT is allowed.
    case ResolvedMergeWhen::NOT_MATCHED_BY_TARGET:
      visible_columns = &source_visible_columns;
      ZETASQL_RET_CHECK_EQ(ResolvedMergeWhen::INSERT, merge_when->action_type());
      break;
  }

  if (merge_when->match_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(*visible_columns,
                                         {} /* visible_parameters */,
                                         merge_when->match_expr()));
  }

  switch (merge_when->action_type()) {
    case ResolvedMergeWhen::INSERT:
      ZETASQL_RET_CHECK(!merge_when->insert_column_list().empty());
      ZETASQL_RET_CHECK_NE(nullptr, merge_when->insert_row());
      ZETASQL_RET_CHECK_EQ(merge_when->insert_column_list_size(),
                   merge_when->insert_row()->value_list_size());
      ZETASQL_RET_CHECK(merge_when->update_item_list().empty());

      for (const ResolvedColumn& column : merge_when->insert_column_list()) {
        ZETASQL_RETURN_IF_ERROR(
            CheckColumnIsPresentInColumnSet(column, target_visible_columns));
      }
      for (int i = 0; i < merge_when->insert_row()->value_list_size(); ++i) {
        const ResolvedDMLValue* dml_value =
            merge_when->insert_row()->value_list(i);
        ZETASQL_RET_CHECK_NE(nullptr, dml_value->value());
        const ResolvedExpr* expr = dml_value->value();
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
            *visible_columns, {} /* visible_parameters */, expr));
        ZETASQL_RET_CHECK(
            expr->type()->Equals(merge_when->insert_column_list(i).type()));
      }
      break;
    case ResolvedMergeWhen::UPDATE:
      ZETASQL_RET_CHECK(!merge_when->update_item_list().empty());
      ZETASQL_RET_CHECK(merge_when->insert_column_list().empty());
      ZETASQL_RET_CHECK_EQ(nullptr, merge_when->insert_row());

      for (const std::unique_ptr<const ResolvedUpdateItem>& item :
           merge_when->update_item_list()) {
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedUpdateItem(
            item.get(), /*allow_nested_statements=*/false,
            /*array_element_column=*/nullptr, target_visible_columns,
            all_visible_columns));
      }
      break;
    case ResolvedMergeWhen::DELETE:
      ZETASQL_RET_CHECK(merge_when->update_item_list().empty());
      ZETASQL_RET_CHECK(merge_when->insert_column_list().empty());
      ZETASQL_RET_CHECK_EQ(nullptr, merge_when->insert_row());
      break;
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedMergeStmt(
    const ResolvedMergeStmt* stmt) const {
  ZETASQL_RET_CHECK_NE(nullptr, stmt->table_scan());
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedScan(stmt->table_scan(), {} /* visible_parameters */));
  std::set<ResolvedColumn> target_visible_columns;
  ZETASQL_RETURN_IF_ERROR(AddColumnList(stmt->table_scan()->column_list(),
                                &target_visible_columns));
  ZETASQL_RET_CHECK_EQ(stmt->table_scan()->column_index_list().size(),
               stmt->column_access_list().size());

  ZETASQL_RET_CHECK_NE(nullptr, stmt->from_scan());
  std::set<ResolvedColumn> source_visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedScan(stmt->from_scan(), {} /* visible_parameters */));
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(stmt->from_scan()->column_list(), &source_visible_columns));

  std::set<ResolvedColumn> all_visible_columns =
      zetasql_base::STLSetUnion(source_visible_columns, target_visible_columns);
  if (nullptr != stmt->merge_expr()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        all_visible_columns, {} /* visible_parameters */, stmt->merge_expr()));
  }

  ZETASQL_RET_CHECK(!stmt->when_clause_list().empty());
  for (const auto& when_clause : stmt->when_clause_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedMergeWhen(
        when_clause.get(), all_visible_columns, source_visible_columns,
        target_visible_columns));
  }
  return ::zetasql_base::OkStatus();
}

// Truncate statement is not supported in nested-DML, however, this is
// enforced by the parser.
zetasql_base::Status Validator::ValidateResolvedTruncateStmt(
    const ResolvedTruncateStmt* stmt) const {
  ZETASQL_RET_CHECK_NE(nullptr, stmt->table_scan());
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedScan(stmt->table_scan(), {} /* visible_parameters */));
  std::set<ResolvedColumn> target_visible_columns;
  ZETASQL_RETURN_IF_ERROR(AddColumnList(stmt->table_scan()->column_list(),
                                &target_visible_columns));

  if (stmt->where_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        target_visible_columns, {} /* visible_parameters */,
        stmt->where_expr()));
    ZETASQL_RET_CHECK(stmt->where_expr()->type()->IsBool())
        << "TruncateStmt has WHERE expression with non-BOOL type: "
        << stmt->where_expr()->type()->DebugString();
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedAlterTableSetOptionsStmt(
    const ResolvedAlterTableSetOptionsStmt* stmt) const {
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedAlterRowPolicyStmt(
    const ResolvedAlterRowPolicyStmt* stmt) const {

  ZETASQL_RET_CHECK(!stmt->name().empty());

  bool has_new_name = !stmt->new_name().empty();

  ZETASQL_RET_CHECK(stmt->table_scan() != nullptr);
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(stmt->table_scan()->column_list(), &visible_columns));

  bool has_grantee_list = stmt->grantee_list_size() > 0 ||
                          stmt->grantee_expr_list_size() > 0;

  const ResolvedExpr* predicate = stmt->predicate();
  bool has_predicate = predicate != nullptr;
  if (has_predicate) {
    ZETASQL_RET_CHECK(predicate->type()->IsBool())
        << "AlterRowPolicyStmt has predicate with non-BOOL type: "
        << predicate->type()->DebugString();
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        visible_columns, {}  /* visible_parameters */, predicate));
  }

  if (has_new_name || has_grantee_list || has_predicate) {
    return ::zetasql_base::OkStatus();
  }

  return MakeSqlError() << "ALTER ROW POLICY must at least have one of the "
                           "RENAME, TO or USING clause.";
}

zetasql_base::Status Validator::ValidateResolvedRenameStmt(
    const ResolvedRenameStmt* stmt) const {
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedImportStmt(
    const ResolvedImportStmt* stmt) const {

  if (stmt->import_kind() == ResolvedImportStmt::MODULE) {
    // In the ResolvedAST, both the name_path and alias_path should be
    // set (with implicit aliases added if necessary). But file_path
    // should not be set.
    ZETASQL_RET_CHECK(!stmt->name_path().empty()) << stmt->DebugString();
    ZETASQL_RET_CHECK(!stmt->alias_path().empty()) << stmt->DebugString();

    ZETASQL_RET_CHECK(stmt->file_path().empty()) << stmt->DebugString();
    ZETASQL_RET_CHECK(stmt->into_alias_path().empty()) << stmt->DebugString();
  } else if (stmt->import_kind() == ResolvedImportStmt::PROTO) {
    // In the ResolvedAST, the file_path should be set while name_path and
    // alias_path should not be set.  The into_alias_path may or may not
    // be set.
    ZETASQL_RET_CHECK(!stmt->file_path().empty()) << stmt->DebugString();

    ZETASQL_RET_CHECK(stmt->name_path().empty()) << stmt->DebugString();
    ZETASQL_RET_CHECK(stmt->alias_path().empty()) << stmt->DebugString();
  }

  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedModuleStmt(
    const ResolvedModuleStmt* stmt) const {

  ZETASQL_RET_CHECK(!stmt->name_path().empty());
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedAssertStmt(
    const ResolvedAssertStmt* stmt) const {
  ZETASQL_RET_CHECK(stmt->expression() != nullptr);
  ZETASQL_RET_CHECK(stmt->expression()->type()->IsBool());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      /*visible_columns=*/{}, /*visible_parameters=*/{}, stmt->expression()));
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedTVFArgument(
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedTVFArgument* resolved_tvf_arg) const {
  ZETASQL_RET_CHECK(resolved_tvf_arg != nullptr);
  if (resolved_tvf_arg->expr() != nullptr) {
    // This is a TVF scalar argument. Validate the input expression,
    // and pass through the 'visible_parameters' because the
    // expression may be correlated.
    ZETASQL_RET_CHECK(resolved_tvf_arg->scan() == nullptr);
    ZETASQL_RET_CHECK(resolved_tvf_arg->model() == nullptr);
    ZETASQL_RET_CHECK_EQ(0, resolved_tvf_arg->argument_column_list_size());
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr({} /* visible_columns */,
                                         visible_parameters,
                                         resolved_tvf_arg->expr()));
  } else if (resolved_tvf_arg->model() != nullptr) {
    // This is a TVF model argument.
    ZETASQL_RET_CHECK(resolved_tvf_arg->scan() == nullptr);
    ZETASQL_RET_CHECK(resolved_tvf_arg->expr() == nullptr);
  } else {
    // Otherwise: this is a TVF relation argument. Validate the input relation,
    // passing through the 'visible_parameters' because correlation references
    // may be present in the scan.
    ZETASQL_RET_CHECK(resolved_tvf_arg->expr() == nullptr);
    ZETASQL_RET_CHECK(resolved_tvf_arg->model() == nullptr);
    ZETASQL_RET_CHECK(resolved_tvf_arg->scan() != nullptr);
    ZETASQL_RET_CHECK_GT(resolved_tvf_arg->argument_column_list_size(), 0);
    ZETASQL_RETURN_IF_ERROR(
        ValidateResolvedScan(resolved_tvf_arg->scan(), visible_parameters));
    // Verify that columns in <argument_column_list> are actually available
    // in <scan>.
    const std::set<ResolvedColumn> produced_columns(
        resolved_tvf_arg->scan()->column_list().begin(),
        resolved_tvf_arg->scan()->column_list().end());
    for (const ResolvedColumn& argument_column :
         resolved_tvf_arg->argument_column_list()) {
      ZETASQL_RET_CHECK(zetasql_base::ContainsKey(produced_columns, argument_column))
          << "TVFArgument scan does not produce column referenced in "
             "argument_column_list: "
          << argument_column.DebugString();
    }
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateRelationSchemaInResolvedTVFArgument(
    const TVFRelation& required_input_schema,
    const TVFRelation& input_relation,
    const ResolvedTVFArgument* resolved_tvf_arg) const {
  ZETASQL_RET_CHECK(resolved_tvf_arg != nullptr);
  ZETASQL_RET_CHECK(resolved_tvf_arg->scan() != nullptr);
  // Check that the provided columns match position-wise with the required
  // columns. The provided input relation must have exactly the same number
  // of columns as the required input relation, and each (required,
  // provided) column pair must have the same names and types.
  ZETASQL_RET_CHECK_EQ(input_relation.num_columns(),
               required_input_schema.num_columns());
  ZETASQL_RET_CHECK_EQ(input_relation.num_columns(),
               resolved_tvf_arg->argument_column_list_size());
  if (required_input_schema.is_value_table()) {
    ZETASQL_RET_CHECK_EQ(1, input_relation.num_columns());
    ZETASQL_RET_CHECK_EQ(1, resolved_tvf_arg->argument_column_list_size());
    ZETASQL_RET_CHECK(input_relation.column(0).type->Equals(
        resolved_tvf_arg->argument_column_list(0).type()));
  } else {
    for (int col_idx = 0; col_idx < input_relation.columns().size();
         ++col_idx) {
      ZETASQL_RET_CHECK(input_relation.column(col_idx).type->Equals(
          required_input_schema.column(col_idx).type));
      ZETASQL_RET_CHECK(zetasql_base::StringCaseEqual(input_relation.column(col_idx).name,
                                required_input_schema.column(col_idx).name))
          << "input relation column name: "
          << input_relation.column(col_idx).name
          << ", required relation column name: "
          << required_input_schema.column(col_idx).name;
      ZETASQL_RET_CHECK(input_relation.column(col_idx).type->Equals(
          resolved_tvf_arg->argument_column_list(col_idx).type()));
    }
  }
  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedAlterObjectStmt(
    const ResolvedAlterObjectStmt* stmt) const {
  for (const auto& alter_action : stmt->alter_action_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedAlterAction(alter_action.get()));
  }

  // Validate we don't drop or create any column twice.
  std::set<std::string, zetasql_base::StringCaseLess> new_columns, columns_to_drop;
  for (const auto& action : stmt->alter_action_list()) {
    if (action->node_kind() == RESOLVED_ADD_COLUMN_ACTION) {
      const std::string name =
          action->GetAs<ResolvedAddColumnAction>()->column_definition()->name();
      ZETASQL_RET_CHECK(new_columns.insert(name).second)
          << "Column added twice: " << name;
    } else if (action->node_kind() == RESOLVED_DROP_COLUMN_ACTION) {
      const std::string name = action->GetAs<ResolvedDropColumnAction>()->name();
      ZETASQL_RET_CHECK(columns_to_drop.insert(name).second)
          << "Column dropped twice: " << name;
      ZETASQL_RET_CHECK(new_columns.find(name) == new_columns.end())
          << "Newly added column is being dropped: " << name;
    }
  }

  return ::zetasql_base::OkStatus();
}

zetasql_base::Status Validator::ValidateResolvedAlterAction(
    const ResolvedAlterAction* action) const {
  switch (action->node_kind()) {
    case RESOLVED_SET_OPTIONS_ACTION:
      ZETASQL_RETURN_IF_ERROR(ValidateHintList(
          action->GetAs<ResolvedSetOptionsAction>()->option_list()));
      break;
    case RESOLVED_ADD_COLUMN_ACTION: {
      auto* column_definition =
          action->GetAs<ResolvedAddColumnAction>()->column_definition();
      if (column_definition->annotations() != nullptr) {
        ZETASQL_RETURN_IF_ERROR(
            ValidateColumnAnnotations(column_definition->annotations()));
      }
      ZETASQL_RET_CHECK(column_definition->type() != nullptr);
      ZETASQL_RET_CHECK(!column_definition->name().empty());
    } break;
    case RESOLVED_DROP_COLUMN_ACTION:
      ZETASQL_RET_CHECK(!action->GetAs<ResolvedDropColumnAction>()->name().empty());
      break;
    default:
      return ::zetasql_base::InternalErrorBuilder(ZETASQL_LOC)
             << "Unhandled node kind: " << action->node_kind_string()
             << " in ValidateResolvedAlterAction";
  }
  return ::zetasql_base::OkStatus();
}

}  // namespace zetasql
