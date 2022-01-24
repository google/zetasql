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

#include "zetasql/resolved_ast/validator.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <type_traits>

#include "zetasql/base/logging.h"
#include "zetasql/base/varsetter.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/analyzer/filter_fields_path_validator.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/constant.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/rewrite_utils.h"
#include "zetasql/base/case.h"
#include "absl/cleanup/cleanup.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/stl_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

// Replacements for ZETASQL_RET_CHECK()-related macros within the validator.
// These macros behave similarly to the originals, but also record the node at
// the top of the context stack so that it get emphasized in the tree dump.
//
// Note: These macros are only valid inside of the Validator class.
#define VALIDATOR_RET_CHECK(expr) ZETASQL_RET_CHECK(expr) << RecordContext()
#define VALIDATOR_RET_CHECK_EQ(a, b) ZETASQL_RET_CHECK_EQ(a, b) << RecordContext()
#define VALIDATOR_RET_CHECK_NE(a, b) ZETASQL_RET_CHECK_NE(a, b) << RecordContext()
#define VALIDATOR_RET_CHECK_GE(a, b) ZETASQL_RET_CHECK_GE(a, b) << RecordContext()
#define VALIDATOR_RET_CHECK_GT(a, b) ZETASQL_RET_CHECK_GT(a, b) << RecordContext()
#define VALIDATOR_RET_CHECK_LE(a, b) ZETASQL_RET_CHECK_LE(a, b) << RecordContext()
#define VALIDATOR_RET_CHECK_LT(a, b) ZETASQL_RET_CHECK_LT(a, b) << RecordContext()
#define VALIDATOR_RET_CHECK_FAIL() ZETASQL_RET_CHECK_FAIL() << RecordContext()
#define VALIDATOR_RET_CHECK_OK(s) ZETASQL_RET_CHECK_OK(s) << RecordContext()

namespace zetasql {

Validator::Validator(const LanguageOptions& language_options,
                     ValidatorOptions validator_options)
    : options_(validator_options), language_options_(language_options) {}

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

absl::Status Validator::ValidateResolvedParameter(
    const ResolvedParameter* resolved_param) {
  PushErrorContext push(this, resolved_param);
  VALIDATOR_RET_CHECK(nullptr != resolved_param);
  // If the parameter has a name, it must not have a position and vice versa.
  const bool has_name = !resolved_param->name().empty();
  const bool has_position = resolved_param->position() > 0;
  if (has_name == has_position) {
    return InternalErrorBuilder()
           << "Parameter is expected to have a name or a position but not "
              "both: "
           << resolved_param->DebugString();
  }
  return absl::OkStatus();
}

absl::Status Validator::CheckColumnIsPresentInColumnSet(
    const ResolvedColumn& column,
    const std::set<ResolvedColumn>& visible_columns) {
  if (!zetasql_base::ContainsKey(visible_columns, column)) {
    return InternalErrorBuilder()
           << "Incorrect reference to column " << column.DebugString();
  }
  return absl::OkStatus();
}

absl::Status Validator::CheckColumnList(
    const ResolvedScan* scan, const std::set<ResolvedColumn>& visible_columns) {
  PushErrorContext push(this, scan);
  VALIDATOR_RET_CHECK(nullptr != scan);
  for (const ResolvedColumn& column : scan->column_list()) {
    if (!zetasql_base::ContainsKey(visible_columns, column)) {
      return InternalErrorBuilder()
             << "Column list contains column " << column.DebugString()
             << " not visible in scan node\n"
             << scan->DebugString();
    }
  }
  return absl::OkStatus();
}

Validator::Validator() {}

Validator::~Validator() {}

absl::Status Validator::ValidateResolvedExprList(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const std::vector<std::unique_ptr<const ResolvedExpr>>& expr_list) {
  for (const auto& expr_iter : expr_list) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         expr_iter.get()));
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedFunctionArgumentList(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const std::vector<std::unique_ptr<const ResolvedFunctionArgument>>&
        expr_list) {
  for (const auto& expr_iter : expr_list) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedFunctionArgument(
        visible_columns, visible_parameters, expr_iter.get()));
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCast(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedCast* resolved_cast) {
  PushErrorContext push(this, resolved_cast);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       resolved_cast->expr()));
  if (resolved_cast->format() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         resolved_cast->format()));
    VALIDATOR_RET_CHECK(resolved_cast->format()->type()->IsString());
  }
  if (resolved_cast->time_zone() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         resolved_cast->time_zone()));
    VALIDATOR_RET_CHECK(resolved_cast->time_zone()->type()->IsString());
  }
  if (!resolved_cast->type_parameters().IsEmpty()) {
    ZETASQL_RETURN_IF_ERROR(resolved_cast->type()->ValidateResolvedTypeParameters(
        resolved_cast->type_parameters(), language_options_.product_mode()));
  }
  resolved_cast->return_null_on_error();  // Mark field as visited.
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedConstant(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedConstant* resolved_constant) {
  PushErrorContext push(this, resolved_constant);

  VALIDATOR_RET_CHECK(resolved_constant->constant() != nullptr)
      << "ResolvedConstant does not have a Constant:\n"
      << resolved_constant->DebugString();
  VALIDATOR_RET_CHECK(
      resolved_constant->constant()->type()->Equals(resolved_constant->type()))
      << "Expected ResolvedConstant of type "
      << resolved_constant->constant()->type()->DebugString() << ", found "
      << resolved_constant->type()->DebugString();

  return absl::OkStatus();
}

absl::Status Validator::ValidateGenericArgumentsAgainstConcreteArguments(
    const ResolvedFunctionCallBase* resolved_function_call,
    const FunctionSignature& signature) {
  PushErrorContext push(this, resolved_function_call);
  bool only_expr_is_used = true;
  for (int i = 0; i < resolved_function_call->generic_argument_list_size();
       i++) {
    const ResolvedFunctionArgument* generic_arg =
        resolved_function_call->generic_argument_list(i);
    const FunctionArgumentType& concrete_argument =
        signature.ConcreteArgument(i);
    if (generic_arg->expr() != nullptr) {
      VALIDATOR_RET_CHECK(
          concrete_argument.type() != nullptr &&
          generic_arg->expr()->type()->Equals(concrete_argument.type()))
          << "Arg index: " << i
          << " function: " << resolved_function_call->DebugString();
    } else if (generic_arg->inline_lambda() != nullptr) {
      only_expr_is_used = false;

      const ResolvedInlineLambda* lambda = generic_arg->inline_lambda();
      const FunctionArgumentType::ArgumentTypeLambda& concrete_lambda =
          concrete_argument.lambda();
      VALIDATOR_RET_CHECK_EQ(lambda->argument_list().size(),
                             concrete_lambda.argument_types().size());
      // Check lambda argument matches concrete argument. This shouldn't trigger
      // under known use cases. But still doing the check just in case.
      for (int i = 0; i < lambda->argument_list_size(); i++) {
        VALIDATOR_RET_CHECK(lambda->argument_list(i).type()->Equals(
            concrete_lambda.argument_types()[i].type()))
            << i << "lambda argument type: "
            << lambda->argument_list(i).type()->DebugString()
            << " concrete type: " << concrete_lambda.argument_types()[i].type();
      }
      VALIDATOR_RET_CHECK(
          lambda->body()->type()->Equals(concrete_lambda.body_type().type()))
          << " lambda body type: " << lambda->body()->type()->DebugString()
          << " concrete body type: "
          << concrete_lambda.body_type().type()->DebugString();
    } else {
      only_expr_is_used = false;
      // For non expr arguments, we need to check argument type against the type
      // required by the signature. We should arrange to share the code with
      // TVFScan validation.
      VALIDATOR_RET_CHECK_FAIL()
          << "Unexpected function argument with index " << i
          << " of function call: " << resolved_function_call->DebugString();
    }
  }
  VALIDATOR_RET_CHECK(resolved_function_call->generic_argument_list_size() ==
                          0 ||
                      !only_expr_is_used)
      << "If all arguments are ResolvedExpressions, argument_list should be "
         "used instead of generic_argument_list";
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedFilterField(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedFilterField* filter_field) {
  PushErrorContext push(this, filter_field);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       filter_field->expr()));

  const auto& expr_type = filter_field->expr()->type();
  VALIDATOR_RET_CHECK(expr_type->IsProto());

  const auto& filter_field_arg_list = filter_field->filter_field_arg_list();
  FilterFieldsPathValidator validator(expr_type->AsProto()->descriptor());
  for (const auto& filter_field_arg : filter_field_arg_list) {
    VALIDATOR_RET_CHECK(!filter_field_arg->field_descriptor_path().empty());
    VALIDATOR_RET_CHECK_OK(
        validator.ValidateFieldPath(filter_field_arg->include(),
                                    filter_field_arg->field_descriptor_path()));
  }
  VALIDATOR_RET_CHECK_OK(
      validator.FinalValidation(filter_field->reset_cleared_required_fields()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedFunctionCallBase(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedFunctionCallBase* resolved_function_call) {
  PushErrorContext push(this, resolved_function_call);

  VALIDATOR_RET_CHECK(resolved_function_call->function() != nullptr)
      << "ResolvedFunctionCall does not have a Function:\n"
      << resolved_function_call->DebugString();

  VALIDATOR_RET_CHECK(resolved_function_call->argument_list_size() == 0 ||
                      resolved_function_call->generic_argument_list_size() == 0)
      << "Only one of argument_list and generic_argument_list can be "
         "non-empty. "
         "Function call: "
      << resolved_function_call->DebugString();

  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedExprList(visible_columns, visible_parameters,
                               resolved_function_call->argument_list()));

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedFunctionArgumentList(
      visible_columns, visible_parameters,
      resolved_function_call->generic_argument_list()));

  const FunctionSignature& signature = resolved_function_call->signature();
  VALIDATOR_RET_CHECK(signature.IsConcrete())
      << "ResolvedFunctionCall must have a concrete signature:\n"
      << resolved_function_call->DebugString();
  VALIDATOR_RET_CHECK(
      resolved_function_call->type()->Equals(signature.result_type().type()))
      << "Resolved function call type: "
      << resolved_function_call->type()->DebugString()
      << ", signature result type: "
      << signature.result_type().type()->DebugString();

  const int num_concrete_args = signature.NumConcreteArguments();
  const int num_resolved_args =
      resolved_function_call->argument_list_size() == 0
          ? resolved_function_call->generic_argument_list_size()
          : resolved_function_call->argument_list_size();
  VALIDATOR_RET_CHECK_EQ(num_resolved_args, num_concrete_args)
      << resolved_function_call->DebugString()
      << "\nSignature: " << signature.DebugString();
  for (int i = 0; i < resolved_function_call->argument_list_size(); i++) {
    VALIDATOR_RET_CHECK(
        resolved_function_call->argument_list(i)->type()->Equals(
            signature.ConcreteArgumentType(i)));
  }

  ZETASQL_RETURN_IF_ERROR(ValidateGenericArgumentsAgainstConcreteArguments(
      resolved_function_call, signature));

  if (resolved_function_call->error_mode() ==
      ResolvedFunctionCallBase::SAFE_ERROR_MODE) {
    VALIDATOR_RET_CHECK(
        resolved_function_call->function()->SupportsSafeErrorMode())
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
      VALIDATOR_RET_CHECK(templated_info->expr()->type()->Equals(
          resolved_function_call->signature().result_type().type()));
    }
    ZETASQL_RETURN_IF_ERROR(ValidateHintList(resolved_function_call->hint_list()));
  }

  VALIDATOR_RET_CHECK(resolved_function_call->collation_list().size() <= 1);

  return absl::OkStatus();
}

absl::Status Validator::ValidateStandaloneResolvedExpr(
    const ResolvedExpr* expr) {
  Reset();
  const absl::Status status =
      ValidateResolvedExpr({} /* visible_columns */,
                           {} /* visible_parameters */,
                           expr);
  if (!status.ok()) {
    if (status.code() == absl::StatusCode::kResourceExhausted) {
      // Don't wrap a resource exhausted status into internal error. This error
      // may still occur for a valid properly resolved expression (stack
      // exhaustion in case of deeply nested expression). There exist cases
      // where the validator uses more stack than parsing/analysis (b/65294961).
      return status;
    }
    return InternalErrorBuilder()
           << "Resolved AST validation failed: " << status.message() << "\n"
           << expr->DebugString({{error_context_, "(validation failed here)"}});
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedExpr(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedExpr* expr) {
  PushErrorContext push(this, expr);

  VALIDATOR_RET_CHECK(nullptr != expr);
  VALIDATOR_RET_CHECK(expr->type() != nullptr)
      << "ResolvedExpr does not have a Type:\n"
      << expr->DebugString();
  if (expr->type_annotation_map() != nullptr) {
    VALIDATOR_RET_CHECK(
        expr->type_annotation_map()->HasCompatibleStructure(expr->type()));
  }

  // Do not add new checks inline here because they increase stack space used in
  // this recursive function. Add a new function for each case instead. Also,
  // avoid creating any unnecessary temporary objects (e.g. by calling
  // ZETASQL_RETURN_IF_ERROR(Validate...) rather than return Validate...).
  switch (expr->node_kind()) {
    case RESOLVED_LITERAL:
    case RESOLVED_EXPRESSION_COLUMN:
    case RESOLVED_DMLDEFAULT:
    case RESOLVED_SYSTEM_VARIABLE:
      // No validation required.
      expr->MarkFieldsAccessed();
      break;
    case RESOLVED_PARAMETER:
      return ValidateResolvedParameter(expr->GetAs<ResolvedParameter>());
    case RESOLVED_COLUMN_REF: {
      const ResolvedColumnRef* column_ref = expr->GetAs<ResolvedColumnRef>();
      return CheckColumnIsPresentInColumnSet(
          column_ref->column(),
          column_ref->is_correlated() ? visible_parameters : visible_columns);
    }
    case RESOLVED_ARGUMENT_REF:
      VALIDATOR_RET_CHECK(allowed_argument_kinds_.contains(
          expr->GetAs<ResolvedArgumentRef>()->argument_kind()))
          << "ResolvedArgumentRef with unexpected kind:\n"
          << expr->DebugString();
      break;
    case RESOLVED_CAST:
      return ValidateResolvedCast(visible_columns, visible_parameters,
                                  expr->GetAs<ResolvedCast>());
    case RESOLVED_CONSTANT:
      return ValidateResolvedConstant(visible_columns, visible_parameters,
                                      expr->GetAs<ResolvedConstant>());
    case RESOLVED_FUNCTION_CALL: {
      return ValidateResolvedFunctionCallBase(
          visible_columns, visible_parameters,
          expr->GetAs<ResolvedFunctionCall>());
    }
    case RESOLVED_AGGREGATE_FUNCTION_CALL: {
      return ValidateResolvedAggregateFunctionCall(
          visible_columns, visible_parameters,
          expr->GetAs<ResolvedAggregateFunctionCall>());
    }
    case RESOLVED_ANALYTIC_FUNCTION_CALL: {
      return ValidateResolvedAnalyticFunctionCall(
          visible_columns, visible_parameters,
          expr->GetAs<ResolvedAnalyticFunctionCall>());
    }
    case RESOLVED_MAKE_STRUCT:
      return ValidateResolvedExprList(
          visible_columns, visible_parameters,
          expr->GetAs<ResolvedMakeStruct>()->field_list());
    case RESOLVED_MAKE_PROTO: {
      for (const auto& resolved_make_proto_field :
           expr->GetAs<ResolvedMakeProto>()->field_list()) {
        VALIDATOR_RET_CHECK(nullptr != resolved_make_proto_field);
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
            visible_columns, visible_parameters,
            resolved_make_proto_field->expr()));
        resolved_make_proto_field->field_descriptor();  // Mark visited.
        resolved_make_proto_field->format();  // Mark visited.
      }
      break;
    }
    case RESOLVED_GET_STRUCT_FIELD: {
      const ResolvedGetStructField* get_struct_field =
          expr->GetAs<ResolvedGetStructField>();
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
          visible_columns, visible_parameters, get_struct_field->expr()));
      VALIDATOR_RET_CHECK(get_struct_field->expr()->type()->IsStruct());
      VALIDATOR_RET_CHECK_GE(get_struct_field->field_idx(), 0);
      VALIDATOR_RET_CHECK_LT(
          get_struct_field->field_idx(),
          get_struct_field->expr()->type()->AsStruct()->num_fields());
      break;
    }
    case RESOLVED_GET_PROTO_FIELD:
      return ValidateResolvedGetProtoFieldExpr(
          visible_columns, visible_parameters,
          expr->GetAs<ResolvedGetProtoField>());
    case RESOLVED_GET_JSON_FIELD:
      return ValidateResolvedGetJsonFieldExpr(
          visible_columns, visible_parameters,
          expr->GetAs<ResolvedGetJsonField>());
    case RESOLVED_FLATTEN:
      return ValidateResolvedFlatten(visible_columns, visible_parameters,
                                     expr->GetAs<ResolvedFlatten>());
    case RESOLVED_FLATTENED_ARG:
      return ValidateResolvedFlattenedArg(expr->GetAs<ResolvedFlattenedArg>());
    case RESOLVED_SUBQUERY_EXPR:
      return ValidateResolvedSubqueryExpr(visible_columns, visible_parameters,
                                          expr->GetAs<ResolvedSubqueryExpr>());
    case RESOLVED_LET_EXPR:
      return ValidateResolvedLetExpr(visible_columns, visible_parameters,
                                     expr->GetAs<ResolvedLetExpr>());
    case RESOLVED_REPLACE_FIELD:
      return ValidateResolvedReplaceField(visible_columns, visible_parameters,
                                          expr->GetAs<ResolvedReplaceField>());
    case RESOLVED_FILTER_FIELD:
      return ValidateResolvedFilterField(visible_columns, visible_parameters,
                                         expr->GetAs<ResolvedFilterField>());
    default:
      return ::zetasql_base::InternalErrorBuilder()
             << "Unhandled node kind: " << expr->node_kind_string()
             << " in ValidateResolvedExpr";
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateOrderByAndLimitClausesOfAggregateFunctionCall(
    const std::set<ResolvedColumn>& input_scan_visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedAggregateFunctionCall* aggregate_function_call) {
  PushErrorContext push(this, aggregate_function_call);
  const Function* aggregate_function = aggregate_function_call->function();
  const std::string& function_name = aggregate_function->Name();
  if (!aggregate_function->SupportsOrderingArguments() &&
      !aggregate_function_call->order_by_item_list().empty()) {
    return InternalErrorBuilder() << "Aggregate function " << function_name
                                  << " does not support ordering arguments,"
                                  << " but has an ORDER BY clause:\n"
                                  << aggregate_function->DebugString();
  }
  if (!aggregate_function->SupportsLimitArguments() &&
      aggregate_function_call->limit() != nullptr) {
    return InternalErrorBuilder() << "Aggregate function " << function_name
                                  << " does not support limiting arguments,"
                                  << " but has a LIMIT clause:\n"
                                  << aggregate_function->DebugString();
  }

  if (aggregate_function_call->with_group_rows_subquery() == nullptr) {
    for (const auto& order_by_item :
         aggregate_function_call->order_by_item_list()) {
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedOrderByItem(
          input_scan_visible_columns, visible_parameters, order_by_item.get()));
    }
  } else {
    std::set<ResolvedColumn> visible_columns;
    ZETASQL_RETURN_IF_ERROR(AddColumnList(
        aggregate_function_call->with_group_rows_subquery()->column_list(),
        &visible_columns));
    for (const auto& order_by_item :
         aggregate_function_call->order_by_item_list()) {
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedOrderByItem(
          visible_columns, visible_parameters, order_by_item.get()));
    }
  }

  if (aggregate_function_call->limit() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateArgumentIsInt64Constant(aggregate_function_call->limit()));
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAggregateFunctionCall(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedAggregateFunctionCall* aggregate_function_call) {
  PushErrorContext push(this, aggregate_function_call);
  VALIDATOR_RET_CHECK_EQ(aggregate_function_call->generic_argument_list_size(),
                         0)
      << "Aggregate functions do not support generic arguments yet";

  std::unique_ptr<std::set<ResolvedColumn>> group_rows_columns;
  const std::set<ResolvedColumn>* function_visible_columns = &visible_columns;
  if (aggregate_function_call->with_group_rows_subquery() != nullptr) {
    // Validate subquery
    {
      // If the aggregate function has WITH GROUP ROWS, then we must have a set
      // of input columns from the related FROM clause.
      std::optional<std::set<ResolvedColumn>> prev =
          input_columns_for_group_rows_;
      input_columns_for_group_rows_.emplace(visible_columns);
      auto cleanup = absl::MakeCleanup(
          [this, &prev]() { input_columns_for_group_rows_.swap(prev); });
      // The subquery sees only its own parameters, not the
      // visible_parameters that were passed in.
      std::set<ResolvedColumn> subquery_parameters;
      for (const std::unique_ptr<const ResolvedColumnRef>& column_ref :
           aggregate_function_call->with_group_rows_parameter_list()) {
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
            visible_columns, visible_parameters, column_ref.get()));
        subquery_parameters.insert(column_ref->column());
      }
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(
          aggregate_function_call->with_group_rows_subquery(),
          subquery_parameters));
    }
    group_rows_columns = absl::make_unique<std::set<ResolvedColumn>>();
    ZETASQL_RETURN_IF_ERROR(AddColumnList(
        aggregate_function_call->with_group_rows_subquery()->column_list(),
        group_rows_columns.get()));
    function_visible_columns = group_rows_columns.get();
  }

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedFunctionCallBase(
      *function_visible_columns, visible_parameters, aggregate_function_call));

  // Mark fields as visited.
  aggregate_function_call->distinct();
  aggregate_function_call->null_handling_modifier();

  if (aggregate_function_call->having_modifier() != nullptr) {
    auto* having_modifier = aggregate_function_call->having_modifier();
    ZETASQL_RETURN_IF_ERROR(
        ValidateResolvedExpr(
            *function_visible_columns,
            visible_parameters,
            having_modifier->having_expr()));

    having_modifier->kind();  // Mark visited.
  }
  // Since some aggregate validations depends on the input scan,
  // the ORDER BY and LIMIT to the aggregate arguments are not validated
  // here, but in ValidateResolvedAggregateScan().
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAnalyticFunctionCall(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedAnalyticFunctionCall* call) {
  PushErrorContext push(this, call);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedFunctionCallBase(
      visible_columns, visible_parameters, call));

  // Mark fields as visited.
  call->distinct();
  call->null_handling_modifier();

  // Since some window frame validations depends on the window ORDER BY,
  // the window frame is not validated here, but in
  // ValidateResolvedWindow().
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedGetProtoFieldExpr(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedGetProtoField* get_proto_field) {
  PushErrorContext push(this, get_proto_field);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      visible_columns, visible_parameters, get_proto_field->expr()));
  VALIDATOR_RET_CHECK(get_proto_field->expr()->type()->IsProto());
  // Match proto full_name rather than using pointer equality because
  // the FieldDescriptor is allowed to be an extension, which may
  // come from a different DescriptorPool.
  VALIDATOR_RET_CHECK_EQ(
      get_proto_field->expr()->type()->AsProto()->descriptor()->full_name(),
      get_proto_field->field_descriptor()->containing_type()->full_name())
      << "Mismatched proto message "
      << get_proto_field->expr()->type()->DebugString() << " and field "
      << get_proto_field->field_descriptor()->full_name();
  if (get_proto_field->field_descriptor()->is_required() ||
      get_proto_field->get_has_bit()) {
    VALIDATOR_RET_CHECK(!get_proto_field->default_value().is_valid());
    VALIDATOR_RET_CHECK(!get_proto_field->return_default_value_when_unset());
  } else {
    if (get_proto_field->return_default_value_when_unset()) {
      VALIDATOR_RET_CHECK(!get_proto_field->type()->IsProto());
      VALIDATOR_RET_CHECK(ProtoType::GetUseDefaultsExtension(
                              get_proto_field->field_descriptor()) ||
                          get_proto_field->expr()
                                  ->type()
                                  ->AsProto()
                                  ->descriptor()
                                  ->file()
                                  ->syntax() ==
                              google::protobuf::FileDescriptor::SYNTAX_PROTO3);
    }
    VALIDATOR_RET_CHECK(get_proto_field->default_value().is_valid());
    VALIDATOR_RET_CHECK(get_proto_field->type()->Equals(
        get_proto_field->default_value().type()));
  }
  if (get_proto_field->get_has_bit()) {
    VALIDATOR_RET_CHECK(get_proto_field->type()->IsBool());
  }
  get_proto_field->format();  // Mark field as visited.
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedGetJsonFieldExpr(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedGetJsonField* get_json_field) {
  PushErrorContext push(this, get_json_field);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       get_json_field->expr()));
  VALIDATOR_RET_CHECK(get_json_field->expr()->type()->IsJson());
  VALIDATOR_RET_CHECK(!get_json_field->field_name().empty());
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedFlatten(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedFlatten* flatten) {
  PushErrorContext push(this, flatten);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       flatten->expr()));
  VALIDATOR_RET_CHECK(flatten->expr()->type()->IsArray());
  const Type* scalar_type = flatten->expr()->type()->AsArray()->element_type();
  VALIDATOR_RET_CHECK(scalar_type->IsProto() || scalar_type->IsStruct() ||
                      scalar_type->IsJson());

  bool seen_proto = scalar_type->IsProto();
  bool seen_json = scalar_type->IsJson();
  for (const auto& get_field_expr : flatten->get_field_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         get_field_expr.get()));
    auto validate_get_field = [this, &seen_proto, &seen_json](
                                  const ResolvedExpr& expr) -> absl::Status {
      if (expr.node_kind() == RESOLVED_GET_STRUCT_FIELD) {
        VALIDATOR_RET_CHECK(!seen_proto);
        VALIDATOR_RET_CHECK(!seen_json);
        VALIDATOR_RET_CHECK_EQ(
            expr.GetAs<ResolvedGetStructField>()->expr()->node_kind(),
            RESOLVED_FLATTENED_ARG);
      } else if (expr.node_kind() == RESOLVED_GET_JSON_FIELD) {
        seen_json = true;
        VALIDATOR_RET_CHECK_EQ(
            expr.GetAs<ResolvedGetJsonField>()->expr()->node_kind(),
            RESOLVED_FLATTENED_ARG);
      } else if (expr.node_kind() == RESOLVED_GET_PROTO_FIELD) {
        VALIDATOR_RET_CHECK(!seen_json);
        seen_proto = true;
        VALIDATOR_RET_CHECK_EQ(
            expr.GetAs<ResolvedGetProtoField>()->expr()->node_kind(),
            RESOLVED_FLATTENED_ARG);
      } else {
        VALIDATOR_RET_CHECK_FAIL()
            << "Unexpected node kind: " << expr.DebugString();
      }
      return absl::OkStatus();
    };
    if (get_field_expr->Is<ResolvedFunctionCall>()) {
      // The get field calls can optionally have an array offset which always
      // take two arguments, the array and the offset.
      const ResolvedFunctionCall* resolved_function_call =
          get_field_expr->GetAs<ResolvedFunctionCall>();
      const std::string& function_name =
          resolved_function_call->function()->Name();
      VALIDATOR_RET_CHECK(function_name == "$array_at_offset" ||
                          function_name == "$array_at_ordinal" ||
                          function_name == "$safe_array_at_offset" ||
                          function_name == "$safe_array_at_ordinal")
          << function_name;
      VALIDATOR_RET_CHECK_EQ(2, resolved_function_call->argument_list_size());
      ZETASQL_RETURN_IF_ERROR(
          validate_get_field(*resolved_function_call->argument_list(0)));
    } else {
      ZETASQL_RETURN_IF_ERROR(validate_get_field(*get_field_expr));
    }
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedFlattenedArg(
    const ResolvedFlattenedArg* flattened_arg) {
  PushErrorContext push(this, flattened_arg);
  VALIDATOR_RET_CHECK(flattened_arg->type()->IsProto() ||
                      flattened_arg->type()->IsStruct() ||
                      flattened_arg->type()->IsJson());
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedReplaceField(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedReplaceField* replace_field) {
  PushErrorContext push(this, replace_field);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       replace_field->expr()));

  for (const std::unique_ptr<const ResolvedReplaceFieldItem>&
           replace_field_item : replace_field->replace_field_item_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         replace_field_item->expr()));

    VALIDATOR_RET_CHECK(!replace_field_item->struct_index_path().empty() ||
                        !replace_field_item->proto_field_path().empty());
    const StructType* current_struct_type =
        replace_field->expr()->type()->AsStruct();
    const zetasql::Type* last_struct_field_type;
    for (const int field_index : replace_field_item->struct_index_path()) {
      // Ensure that the field path is valid by checking field containment.
      VALIDATOR_RET_CHECK(current_struct_type != nullptr);
      VALIDATOR_RET_CHECK_GE(field_index, 0);
      VALIDATOR_RET_CHECK_LT(field_index, current_struct_type->num_fields());
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
        VALIDATOR_RET_CHECK(!containing_proto_name.empty())
            << "Unable to identify parent message of field: "
            << field->full_name();
        VALIDATOR_RET_CHECK_EQ(containing_proto_name,
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
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedInlineLambda(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedInlineLambda* resolved_lambda) {
  PushErrorContext push(this, resolved_lambda);
  // Build column set from argument list of lambda.
  std::set<ResolvedColumn> argument_columns;
  for (const ResolvedColumn& argument : resolved_lambda->argument_list()) {
    argument_columns.insert(argument);
  }

  // Build column set from parameter list of lambda.
  std::set<ResolvedColumn> body_visible_parameters;
  for (const std::unique_ptr<const ResolvedColumnRef>& column_ref :
       resolved_lambda->parameter_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         column_ref.get()));
    body_visible_parameters.insert(column_ref->column());
  }

  return ValidateResolvedExpr(argument_columns, body_visible_parameters,
                              resolved_lambda->body());
}

absl::Status Validator::ValidateResolvedSubqueryExpr(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedSubqueryExpr* resolved_subquery_expr) {
  PushErrorContext push(this, resolved_subquery_expr);
  ResolvedSubqueryExpr::SubqueryType type =
      resolved_subquery_expr->subquery_type();
  VALIDATOR_RET_CHECK_EQ((type == ResolvedSubqueryExpr::IN) ||
                             (type == ResolvedSubqueryExpr::LIKE_ANY) ||
                             (type == ResolvedSubqueryExpr::LIKE_ALL),
                         resolved_subquery_expr->in_expr() != nullptr)
      << "Subquery expressions of "
      << ResolvedSubqueryExprEnums::SubqueryType_Name(type)
      << " should have <in_expr> populated";

  if (resolved_subquery_expr->in_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         resolved_subquery_expr->in_expr()));
  }

  if (!resolved_subquery_expr->in_collation().Empty()) {
    VALIDATOR_RET_CHECK_EQ(type, ResolvedSubqueryExpr::IN)
        << "<in_collation> should only be populated for Subquery expressions "
           "of IN type. Subquery expression type is "
        << ResolvedSubqueryExprEnums::SubqueryType_Name(type);
    VALIDATOR_RET_CHECK_NE(resolved_subquery_expr->in_expr(), nullptr);
    // We only check that <in_collation> is compatible with the type of
    // <in_expr> here given the type of subquery column should match that of
    // <in_expr> (which is checked afterwards).
    VALIDATOR_RET_CHECK(
        resolved_subquery_expr->in_collation().HasCompatibleStructure(
            resolved_subquery_expr->in_expr()->type()));
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

  if (options_.validate_no_unreferenced_subquery_params) {
    // Validate that the subquery does not include parameters that are
    // unreferenced.
    std::vector<std::unique_ptr<const ResolvedColumnRef>> refs;
    ZETASQL_RETURN_IF_ERROR(
        CollectColumnRefs(*resolved_subquery_expr->subquery(), &refs));
    absl::flat_hash_set<int> referenced_column_ids;
    for (auto& ref : refs) {
      if (ref->is_correlated()) {
        referenced_column_ids.insert(ref->column().column_id());
      }
    }
    for (auto& param : subquery_parameters) {
      VALIDATOR_RET_CHECK(referenced_column_ids.contains(param.column_id()))
          << "Expression subquery does not reference correlated column "
          << "parameter: " << param.DebugString();
    }
  }

  switch (resolved_subquery_expr->subquery_type()) {
    case ResolvedSubqueryExpr::SCALAR:
    case ResolvedSubqueryExpr::ARRAY:
    case ResolvedSubqueryExpr::IN:
      VALIDATOR_RET_CHECK_EQ(
          resolved_subquery_expr->subquery()->column_list_size(), 1)
          << "Expression subquery must produce exactly one column";
      if (resolved_subquery_expr->in_expr() != nullptr) {
        const Type* in_expr_type = resolved_subquery_expr->in_expr()->type();
        const Type* in_subquery_type =
            resolved_subquery_expr->subquery()->column_list(0).type();
        VALIDATOR_RET_CHECK(in_expr_type->SupportsEquality());
        VALIDATOR_RET_CHECK(in_subquery_type->SupportsEquality());

        const bool argument_types_equal =
            in_expr_type->Equals(in_subquery_type);
        const bool argument_types_int64_and_uint64 =
            (in_expr_type->IsInt64() && in_subquery_type->IsUint64()) ||
            (in_expr_type->IsUint64() && in_subquery_type->IsInt64());
        VALIDATOR_RET_CHECK(argument_types_equal ||
                            argument_types_int64_and_uint64);
      }
      break;
    case ResolvedSubqueryExpr::LIKE_ANY:
    case ResolvedSubqueryExpr::LIKE_ALL: {
      VALIDATOR_RET_CHECK_EQ(
          resolved_subquery_expr->subquery()->column_list_size(), 1)
          << "Expression subquery must produce exactly one column";
      const Type* like_expr_type = resolved_subquery_expr->in_expr()->type();
      const Type* like_subquery_type =
          resolved_subquery_expr->subquery()->column_list(0).type();
      VALIDATOR_RET_CHECK(like_expr_type->Equivalent(like_subquery_type));
      VALIDATOR_RET_CHECK(like_expr_type->IsString() ||
                          like_expr_type->IsBytes());
      break;
    }
    case ResolvedSubqueryExpr::EXISTS:
      break;
  }

  ZETASQL_RETURN_IF_ERROR(ValidateHintList(resolved_subquery_expr->hint_list()));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedLetExpr(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedLetExpr* resolved_let_expr) {
  std::set<ResolvedColumn> expr_visible_columns = visible_columns;
  for (const auto& assignment : resolved_let_expr->assignment_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         assignment->expr()));
    ZETASQL_RETURN_IF_ERROR(
        AddColumnFromComputedColumn(assignment.get(), &expr_visible_columns));
  }
  return ValidateResolvedExpr(expr_visible_columns, visible_parameters,
                              resolved_let_expr->expr());
}

absl::Status Validator::ValidateResolvedComputedColumn(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedComputedColumn* computed_column) {
  VALIDATOR_RET_CHECK(nullptr != computed_column);
  PushErrorContext push(this, computed_column);

  const ResolvedExpr* expr = computed_column->expr();
  VALIDATOR_RET_CHECK(nullptr != expr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       expr));
  VALIDATOR_RET_CHECK(computed_column->column().type()->Equals(expr->type()))
      << computed_column->DebugString()  // includes newline
      << "column: " << computed_column->column().DebugString()
      << " type: " << computed_column->column().type()->DebugString();
  // TODO: Add a more general check to handle any ResolvedExpr
  // (not just RESOLVED_COLUMN_REF).  The ResolvedExpr should not
  // reference the ResolvedColumn to be computed.
  if (computed_column->expr()->node_kind() == RESOLVED_COLUMN_REF &&
      computed_column->column() ==
        computed_column->expr()->GetAs<ResolvedColumnRef>()->column()) {
    return InternalErrorBuilder()
           << "ResolvedComputedColumn expression cannot reference itself: "
           << computed_column->DebugString();
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedComputedColumnList(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
        computed_column_list) {
  for (const auto& computed_column : computed_column_list) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateResolvedComputedColumn(visible_columns, visible_parameters,
                                       computed_column.get()));
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedOutputColumn(
    const std::set<ResolvedColumn>& visible_columns,
    const ResolvedOutputColumn* output_column) {
  VALIDATOR_RET_CHECK(nullptr != output_column);
  PushErrorContext push(this, output_column);
  ZETASQL_RETURN_IF_ERROR(CheckColumnIsPresentInColumnSet(
      output_column->column(), visible_columns));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedOutputColumnList(
    const std::vector<ResolvedColumn>& visible_columns,
    const std::vector<std::unique_ptr<const ResolvedOutputColumn>>&
        output_column_list,
    bool is_value_table) {
  VALIDATOR_RET_CHECK(!output_column_list.empty())
      << "Statement must produce at least one output column";
  const std::set<ResolvedColumn> visible_columns_set(
      visible_columns.begin(), visible_columns.end());
  for (const auto& output_column : output_column_list) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateResolvedOutputColumn(visible_columns_set, output_column.get()));
  }
  if (is_value_table) {
    if (output_column_list.size() != 1) {
      return InternalErrorBuilder()
             << "Statement producing a value table must produce exactly one "
                "column; this one has "
             << output_column_list.size();
    }
    if (!IsInternalAlias(output_column_list[0]->name())) {
      return InternalErrorBuilder()
             << "Statement producing a value table must produce an anonymous "
                "column; this one has name "
             << ToIdentifierLiteral(output_column_list[0]->name());
    }
  }
  return absl::OkStatus();
}

absl::Status Validator::AddColumnList(
    const ResolvedColumnList& column_list,
    std::set<ResolvedColumn>* visible_columns) {
  VALIDATOR_RET_CHECK_NE(visible_columns, nullptr);
  for (const ResolvedColumn& column : column_list) {
    VALIDATOR_RET_CHECK(column_ids_seen_.contains(column.column_id()))
        << "Undefined column " << column.DebugString();
    visible_columns->insert(column);
  }
  return absl::OkStatus();
}

absl::Status Validator::AddColumnList(
    const ResolvedColumnList& column_list,
    absl::flat_hash_set<ResolvedColumn>* visible_columns) {
  VALIDATOR_RET_CHECK_NE(visible_columns, nullptr);
  for (const ResolvedColumn& column : column_list) {
    VALIDATOR_RET_CHECK(column_ids_seen_.contains(column.column_id()))
        << "Undefined column " << column.DebugString();
    visible_columns->insert(column);
  }
  return absl::OkStatus();
}

absl::Status Validator::AddColumn(
    const ResolvedColumn& column,
    absl::flat_hash_set<ResolvedColumn>* visible_columns) {
  VALIDATOR_RET_CHECK_NE(visible_columns, nullptr);
  VALIDATOR_RET_CHECK(column_ids_seen_.contains(column.column_id()))
      << "Undefined column " << column.DebugString();
  visible_columns->insert(column);
  return absl::OkStatus();
}

absl::Status Validator::AddColumnFromComputedColumn(
    const ResolvedComputedColumn* computed_column,
    std::set<ResolvedColumn>* visible_columns) {
  VALIDATOR_RET_CHECK(nullptr != visible_columns && nullptr != computed_column);
  PushErrorContext push(this, computed_column);
  ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(computed_column->column()));
  visible_columns->insert(computed_column->column());
  return absl::OkStatus();
}

absl::Status Validator::AddColumnsFromComputedColumnList(
    const std::vector<std::unique_ptr<const ResolvedComputedColumn>>&
        computed_column_list,
    std::set<ResolvedColumn>* visible_columns) {
  VALIDATOR_RET_CHECK(nullptr != visible_columns);
  for (const auto& computed_column : computed_column_list) {
    ZETASQL_RETURN_IF_ERROR(AddColumnFromComputedColumn(computed_column.get(),
                                                visible_columns));
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedTableScan(
    const ResolvedTableScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);

  const Table* table = scan->table();
  VALIDATOR_RET_CHECK(nullptr != table);

  if (scan->for_system_time_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr({}, visible_parameters,
                                         scan->for_system_time_expr()));
    VALIDATOR_RET_CHECK(scan->for_system_time_expr()->type()->IsTimestamp())
        << "TableScan has for_system_type_expr with non-TIMESTAMP type: "
        << scan->for_system_time_expr()->type()->DebugString();
  }

  // Make sure column ids are unique
  for (const ResolvedColumn& column : scan->column_list()) {
    ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(column));
  }

  // Ideally, column_index_list should always be the same size as column_list.
  // However, for historical reasons, some clients don't provide a
  // column_index_list and they match the table column by ResolvedColumn name
  // (this violates the contract of ResolvedColumn name() which defines it as
  // semantically meaningless). Therefore there's an exception here to allow an
  // empty column_index_list. Once all the client migrations are done, this
  // exception should be removed.
  //
  // TODO: Remove this exception.
  if (scan->column_index_list_size() == 0) {
    return absl::OkStatus();
  }

  // Checks that all columns have corresponding indexes.
  VALIDATOR_RET_CHECK_EQ(scan->column_list_size(),
                         scan->column_index_list_size());
  const int num_columns = table->NumColumns();

  for (const int index : scan->column_index_list()) {
    VALIDATOR_RET_CHECK_GE(index, 0);
    VALIDATOR_RET_CHECK_LT(index, num_columns);
    VALIDATOR_RET_CHECK_NE(table->GetColumn(index), nullptr);
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedJoinScan(
    const ResolvedJoinScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);

  VALIDATOR_RET_CHECK(nullptr != scan->left_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->left_scan(), visible_parameters));
  VALIDATOR_RET_CHECK(nullptr != scan->right_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->right_scan(), visible_parameters));

  scan->join_type();  // Mark field as visited.

  std::set<ResolvedColumn> left_visible_columns, right_visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->left_scan()->column_list(), &left_visible_columns));
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->right_scan()->column_list(), &right_visible_columns));
  // Both left and right scans should not have any common column references
  // introduced in the visible set for the on_condition.
  VALIDATOR_RET_CHECK(!zetasql_base::SortedContainersHaveIntersection(
      left_visible_columns, right_visible_columns));

  const std::set<ResolvedColumn> visible_columns =
      zetasql_base::STLSetUnion(left_visible_columns, right_visible_columns);
  if (nullptr != scan->join_expr()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         scan->join_expr()));
    VALIDATOR_RET_CHECK(scan->join_expr()->type()->IsBool())
        << "JoinScan has join_expr with non-BOOL type: "
        << scan->join_expr()->type()->DebugString();
  }
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedArrayScan(
    const ResolvedArrayScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);

  std::set<ResolvedColumn> visible_columns;
  if (nullptr != scan->input_scan()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(),
                                         visible_parameters));
    ZETASQL_RETURN_IF_ERROR(
        AddColumnList(scan->input_scan()->column_list(), &visible_columns));
  }
  VALIDATOR_RET_CHECK(nullptr != scan->array_expr());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       scan->array_expr()));
  VALIDATOR_RET_CHECK(scan->array_expr()->type()->IsArray())
      << "ArrayScan of non-ARRAY type: "
      << scan->array_expr()->type()->DebugString();
  ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(scan->element_column()));
  visible_columns.insert(scan->element_column());
  if (nullptr != scan->array_offset_column()) {
    ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(scan->array_offset_column()->column()));
    visible_columns.insert(scan->array_offset_column()->column());
  }
  if (nullptr != scan->join_expr()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         scan->join_expr()));
    VALIDATOR_RET_CHECK(scan->join_expr()->type()->IsBool())
        << "ArrayScan has join_expr with non-BOOL type: "
        << scan->join_expr()->type()->DebugString();
  }
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  scan->is_outer();  // Mark field as visited.

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedFilterScan(
    const ResolvedFilterScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);

  VALIDATOR_RET_CHECK(nullptr != scan->input_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(), visible_parameters));

  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->input_scan()->column_list(), &visible_columns));
  VALIDATOR_RET_CHECK(nullptr != scan->filter_expr());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       scan->filter_expr()));
  VALIDATOR_RET_CHECK(scan->filter_expr()->type()->IsBool())
      << "FilterScan has expression with non-BOOL type: "
      << scan->filter_expr()->type()->DebugString();
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAggregateComputedColumn(
    const ResolvedComputedColumn* computed_column,
    const std::set<ResolvedColumn>& input_scan_visible_columns,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, computed_column);
  VALIDATOR_RET_CHECK_EQ(computed_column->expr()->node_kind(),
                         RESOLVED_AGGREGATE_FUNCTION_CALL);
  const ResolvedAggregateFunctionCall* aggregate_function_call =
      computed_column->expr()->GetAs<ResolvedAggregateFunctionCall>();

  return ValidateOrderByAndLimitClausesOfAggregateFunctionCall(
      input_scan_visible_columns, visible_parameters, aggregate_function_call);
}

absl::Status Validator::ValidateResolvedAggregateScanBase(
    const ResolvedAggregateScanBase* scan,
    const std::set<ResolvedColumn>& visible_parameters,
    std::set<ResolvedColumn>* input_scan_visible_columns) {
  PushErrorContext push(this, scan);

  VALIDATOR_RET_CHECK(nullptr != scan->input_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(), visible_parameters));

  ZETASQL_RETURN_IF_ERROR(AddColumnList(scan->input_scan()->column_list(),
                                input_scan_visible_columns));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedComputedColumnList(
      *input_scan_visible_columns, visible_parameters, scan->group_by_list()));
  if (language_options_.LanguageFeatureEnabled(
          FEATURE_V_1_3_COLLATION_SUPPORT)) {
    if (scan->collation_list_size() != 0) {
      ZETASQL_RET_CHECK_EQ(scan->collation_list_size(), scan->group_by_list_size());
      for (int i = 0; i < scan->collation_list_size(); i++) {
        ZETASQL_RET_CHECK(scan->collation_list(i).HasCompatibleStructure(
            scan->group_by_list(i)->expr()->type()))
            << "Collation must have compatible structure with the type of "
               "the element in group_by_list with the same index";
      }
    }
  } else {
    ZETASQL_RET_CHECK_EQ(scan->collation_list_size(), 0);
  }
  // TODO: consider capturing input_columns_for_group_rows_ here and
  // not in ValidateResolvedAggregateFunctionCall(). This way we can avoid extra
  // copies when multiple WITH GROUP_ROWS are used in the same aggregate scan;
  // also we could validate the case if for some way WITH GROUP_ROWS is used
  // outside of aggregate scan.

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedComputedColumnList(
      *input_scan_visible_columns, visible_parameters, scan->aggregate_list()));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAggregateScan(
    const ResolvedAggregateScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);

  std::set<ResolvedColumn> input_scan_visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedAggregateScanBase(
      scan, visible_parameters, &input_scan_visible_columns));

  if (!scan->grouping_set_list().empty()) {
    // There should be a grouping set for every prefix of the rollup list,
    // including the empty one.
    VALIDATOR_RET_CHECK_EQ(scan->grouping_set_list_size(),
                           scan->rollup_column_list_size() + 1);

    std::set<ResolvedColumn> group_by_columns;
    for (const auto& group_by_column : scan->group_by_list()) {
      group_by_columns.insert(group_by_column->column());
    }

    // group_by_columns should be non-empty, and each item in the rollup list or
    // a grouping set should be a computed column from group_by_columns.
    VALIDATOR_RET_CHECK(!group_by_columns.empty());
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
        VALIDATOR_RET_CHECK(zetasql_base::InsertIfNotPresent(&grouping_set_columns,
                                                    column_ref->column()));
      }
    }
  } else {
    // Presence of grouping sets should indicate that there is a rollup list.
    VALIDATOR_RET_CHECK(scan->rollup_column_list().empty());
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
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAnonymizedAggregateScan(
    const ResolvedAnonymizedAggregateScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);

  std::set<ResolvedColumn> input_scan_visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedAggregateScanBase(
      scan, visible_parameters, &input_scan_visible_columns));

  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(AddColumnsFromComputedColumnList(scan->aggregate_list(),
                                                   &visible_columns));
  if (scan->k_threshold_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(CheckColumnIsPresentInColumnSet(
        scan->k_threshold_expr()->column(), visible_columns));
  }
  ZETASQL_RETURN_IF_ERROR(AddColumnsFromComputedColumnList(scan->group_by_list(),
                                                   &visible_columns));
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  for (const auto& computed_column : scan->aggregate_list()) {
    VALIDATOR_RET_CHECK_EQ(computed_column->expr()->node_kind(),
                           RESOLVED_AGGREGATE_FUNCTION_CALL);
  }

  for (const auto& option : scan->anonymization_option_list()) {
    VALIDATOR_RET_CHECK(option->qualifier().empty())
        << "Anonymization options must not have a qualifier, but found "
        << option->qualifier();
    const Type* expected_option_type;
    if (absl::AsciiStrToLower(option->name()) == "delta" ||
        absl::AsciiStrToLower(option->name()) == "epsilon") {
      expected_option_type = types::DoubleType();
    } else if (absl::AsciiStrToLower(option->name()) == "kappa" ||
               absl::AsciiStrToLower(option->name()) == "k_threshold") {
      expected_option_type = types::Int64Type();
    } else {
      VALIDATOR_RET_CHECK_FAIL()
          << "Invalid anonymization option name: " << option->name();
    }
    VALIDATOR_RET_CHECK(option->value()->type()->Equals(expected_option_type));
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        visible_columns, visible_parameters, option->value()));
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidatePercentArgument(const ResolvedExpr* expr) {
  VALIDATOR_RET_CHECK(expr != nullptr);
  PushErrorContext push(this, expr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      /*visible_columns=*/{}, /*visible_parameters=*/{}, expr));
  VALIDATOR_RET_CHECK(expr->node_kind() == RESOLVED_LITERAL ||
                      expr->node_kind() == RESOLVED_PARAMETER)
      << "PERCENT argument is of incorrect kind: " << expr->node_kind_string();

  VALIDATOR_RET_CHECK(expr->type()->IsInt64() || expr->type()->IsDouble())
      << "PERCENT argument must be either a double or an int64";

  if (expr->node_kind() == RESOLVED_LITERAL) {
    // If a literal, we validate its value.
    const Value value = expr->GetAs<ResolvedLiteral>()->value();
    bool is_valid = false;
    if (value.type()->IsInt64()) {
      is_valid = (!value.is_null() &&
                  value.int64_value() >= 0 && value.int64_value() <= 100);
    } else {
      VALIDATOR_RET_CHECK(value.type()->IsDouble());
      is_valid = (!value.is_null() &&
                  value.double_value() >= 0.0 && value.double_value() <= 100.0);
    }
    if (!is_valid) {
      return ::zetasql_base::UnknownErrorBuilder()
             << "PERCENT argument value must be in the range [0, 100]";
    }
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedSampleScan(
    const ResolvedSampleScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);

  VALIDATOR_RET_CHECK(nullptr != scan->input_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(), visible_parameters));

  VALIDATOR_RET_CHECK(!scan->method().empty());
  VALIDATOR_RET_CHECK(nullptr != scan->size());

  const ResolvedSampleScan::SampleUnit unit = scan->unit();
  if (unit == ResolvedSampleScan::ROWS) {
    ZETASQL_RETURN_IF_ERROR(ValidateArgumentIsInt64Constant(scan->size()));
  } else {
    VALIDATOR_RET_CHECK_EQ(unit, ResolvedSampleScan::PERCENT);
    ZETASQL_RETURN_IF_ERROR(ValidatePercentArgument(scan->size()));
  }

  VALIDATOR_RET_CHECK(scan->size() != nullptr);
  if (scan->repeatable_argument() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateArgumentIsInt64Constant(scan->repeatable_argument()));
  }

  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->input_scan()->column_list(), &visible_columns));
  if (nullptr != scan->weight_column()) {
    ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(scan->weight_column()->column()));
    visible_columns.insert(scan->weight_column()->column());
  }
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  if (!scan->partition_by_list().empty()) {
    VALIDATOR_RET_CHECK_EQ(ResolvedSampleScan::ROWS, unit);
    VALIDATOR_RET_CHECK_EQ("RESERVOIR", absl::AsciiStrToUpper(scan->method()));
    for (const auto& partition_by_expr : scan->partition_by_list()) {
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
          visible_columns, /*visible_parameters=*/{}, partition_by_expr.get()));
    }
  }

  return absl::OkStatus();
}

absl::Status Validator::CheckUniqueColumnId(const ResolvedColumn& column) {
  VALIDATOR_RET_CHECK(
      zetasql_base::InsertIfNotPresent(&column_ids_seen_, column.column_id()))
      << "Duplicate column id " << column.column_id() << " in column "
      << column.DebugString();
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAnalyticScan(
    const ResolvedAnalyticScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);

  VALIDATOR_RET_CHECK(nullptr != scan->input_scan());
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

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAnalyticFunctionGroup(
    const ResolvedAnalyticFunctionGroup* group,
    const std::set<ResolvedColumn>& input_visible_columns,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, group);

  for (const auto& computed_column : group->analytic_function_list()) {
    const ResolvedAnalyticFunctionCall* analytic_function_call =
        computed_column->expr()->GetAs<ResolvedAnalyticFunctionCall>();
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        input_visible_columns, visible_parameters, analytic_function_call));

    const Function* analytic_function = analytic_function_call->function();
    const std::string function_name = analytic_function->Name();
    if (!analytic_function->SupportsOverClause()) {
      return InternalErrorBuilder()
             << "Function " << function_name
             << " cannot be used in an analytic function call, since it does "
                "not"
                " support an OVER clause";
    }
    if (analytic_function_call->distinct()) {
      if (!analytic_function_call->function()->IsAggregate()) {
        return InternalErrorBuilder()
               << "Cannot specify DISTINCT for a non-aggregate analytic "
                  "function:\n"
               << analytic_function_call->DebugString();
      }
      VALIDATOR_RET_CHECK_EQ(
          analytic_function_call->generic_argument_list_size(), 0)
          << "Analytic functions do not support generic arguments yet";
      if (analytic_function_call->argument_list_size() == 0) {
        return InternalErrorBuilder()
               << "DISTINCT function call " << function_name
               << " does not have an argument:\n"
               << analytic_function_call->DebugString();
      }
      if (group->order_by() != nullptr) {
        return InternalErrorBuilder()
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
          return InternalErrorBuilder()
                 << "The window frame for a DISTINCT analytic function call "
                    "must "
                    "be UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING:\n"
                 << analytic_function_call->DebugString();
        }
      }
    }

    if (!analytic_function->SupportsWindowFraming() &&
        analytic_function_call->window_frame() != nullptr) {
      return InternalErrorBuilder()
             << "Analytic function " << function_name
             << " does not support framing, but has a window framing clause:\n"
             << analytic_function_call->DebugString();
    }
    if (analytic_function->RequiresWindowOrdering() &&
        group->order_by() == nullptr) {
      return InternalErrorBuilder() << "Analytic function " << function_name
                                    << " must have a window ORDER BY clause:\n"
                                    << group->DebugString();
    }
    if (!analytic_function->SupportsWindowOrdering() &&
        group->order_by() != nullptr) {
      return InternalErrorBuilder()
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
        return InternalErrorBuilder()
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
        return InternalErrorBuilder()
               << "Type of ORDERING expressions "
               << order_by_item->column_ref()->type()->DebugString()
               << " does not support ordering:\n"
               << order_by_item->column_ref()->DebugString();
      }
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedOrderByItem(
          input_visible_columns, visible_parameters, order_by_item.get()));
    }
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedWindowFrame(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedWindowOrdering* window_ordering,
    const ResolvedWindowFrame* window_frame) {
  PushErrorContext push(this, window_frame);
  VALIDATOR_RET_CHECK(window_frame->start_expr() != nullptr &&
                      window_frame->end_expr() != nullptr)
      << "Window frame must specify both the starting and the ending boundary"
         ":\n"
      << window_frame->DebugString();
  VALIDATOR_RET_CHECK(window_frame->frame_unit() == ResolvedWindowFrame::ROWS ||
                      window_frame->frame_unit() == ResolvedWindowFrame::RANGE)
      << "Unhandled window frame unit " << window_frame->GetFrameUnitString()
      << ":\n"
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
    return InternalErrorBuilder() << "Window frame must be non-empty";
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedWindowFrameExpr(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedWindowOrdering* window_ordering,
    const ResolvedWindowFrame::FrameUnit& frame_unit,
    const ResolvedWindowFrameExpr* window_frame_expr) {
  PushErrorContext push(this, window_frame_expr);
  switch (window_frame_expr->boundary_type()) {
    case ResolvedWindowFrameExpr::UNBOUNDED_PRECEDING:
    case ResolvedWindowFrameExpr::CURRENT_ROW:
    case ResolvedWindowFrameExpr::UNBOUNDED_FOLLOWING:
      if (window_frame_expr->expression() != nullptr) {
        return InternalErrorBuilder()
               << "Window frame boundary of type "
               << window_frame_expr->GetBoundaryTypeString()
               << " cannot have an offset expression:\n"
               << window_frame_expr->DebugString();
      }
      break;
    case ResolvedWindowFrameExpr::OFFSET_PRECEDING:
    case ResolvedWindowFrameExpr::OFFSET_FOLLOWING: {
      if (window_frame_expr->expression() == nullptr) {
        return InternalErrorBuilder()
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
          return InternalErrorBuilder()
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
      return InternalErrorBuilder()
             << "Unhandled window boundary type:\n"
             << window_frame_expr->GetBoundaryTypeString();
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedWindowFrameExprType(
    const ResolvedWindowFrame::FrameUnit& frame_unit,
    const ResolvedExpr* window_ordering_expr,
    const ResolvedExpr& window_frame_expr) {
  switch (frame_unit) {
    case ResolvedWindowFrame::ROWS: {
      PushErrorContext push(this, &window_frame_expr);
      if (!window_frame_expr.type()->IsInt64()) {
        return InternalErrorBuilder()
               << "ROWS-based window boundary expression must be INT64 type, "
                  "but "
                  "has type "
               << window_frame_expr.type()->DebugString() << ":\n"
               << window_frame_expr.DebugString();
      }
      break;
    }
    case ResolvedWindowFrame::RANGE: {
      VALIDATOR_RET_CHECK(window_ordering_expr != nullptr);
      PushErrorContext push(this, window_ordering_expr);
      if (!window_ordering_expr->type()->IsNumerical()) {
        return InternalErrorBuilder()
               << "Ordering expression must be numeric type in a RANGE-based "
                  "window, but has type "
               << window_ordering_expr->type()->DebugString() << ":\n"
               << window_ordering_expr->DebugString();
      }
      if (!window_ordering_expr->type()->Equals(window_frame_expr.type())) {
        return InternalErrorBuilder()
               << "RANGE-based window boundary expression has a different type "
                  "with the ordering expression ("
               << window_frame_expr.type()->DebugString() << " vs. "
               << window_ordering_expr->type()->DebugString() << "):\n"
               << window_frame_expr.DebugString();
      }
      break;
    }
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedSetOperationItem(
    const ResolvedSetOperationItem* input_item,
    const ResolvedColumnList& output_column_list,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, input_item);
  const ResolvedScan* input_scan = input_item->scan();
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(input_scan, visible_parameters));

  const std::set<ResolvedColumn> produced_columns(
      input_scan->column_list().begin(), input_scan->column_list().end());

  // <output_column_list>, which corresponds with the column list of the
  // enclosing set operation, matches 1:1 with the <output_column_list> from the
  // input item, but not necessarily with the input item's <input_scan>'s
  // <column_list>.
  VALIDATOR_RET_CHECK_EQ(input_item->output_column_list_size(),
                         output_column_list.size());
  for (int i = 0; i < output_column_list.size(); ++i) {
    const ResolvedColumn& input_column = input_item->output_column_list(i);
    VALIDATOR_RET_CHECK(
        output_column_list.at(i).type()->Equals(input_column.type()))
        << "SetOperation input column type does not match output type";

    VALIDATOR_RET_CHECK(zetasql_base::ContainsKey(produced_columns, input_column))
        << "SetOperation input scan does not produce column referenced in "
           "output_column_list: "
        << input_column.DebugString();
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedSetOperationScan(
    const ResolvedSetOperationScan* set_op_scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, set_op_scan);
  VALIDATOR_RET_CHECK_GE(set_op_scan->input_item_list_size(), 2);

  for (const auto& input_item : set_op_scan->input_item_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedSetOperationItem(
        input_item.get(), set_op_scan->column_list(), visible_parameters));
  }
    set_op_scan->op_type();  // Mark field as visited.

  // Each output column should be unique. In particular, set operation scans
  // should not reuse column ids from their inputs.
  for (const ResolvedColumn& column : set_op_scan->column_list()) {
    ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(column));
  }

  return absl::OkStatus();
}
static bool IsResolvedLiteralOrParameter(ResolvedNodeKind kind) {
  return kind == RESOLVED_LITERAL || kind == RESOLVED_PARAMETER;
}

absl::Status Validator::ValidateArgumentIsInt64Constant(
    const ResolvedExpr* expr) {
  VALIDATOR_RET_CHECK(expr != nullptr);
  PushErrorContext push(this, expr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      /*visible_columns=*/{}, /*visible_parameters=*/{}, expr));

  VALIDATOR_RET_CHECK(IsResolvedLiteralOrParameter(expr->node_kind()) ||
                      (expr->node_kind() == RESOLVED_CAST &&
                       expr->type()->IsInt64() &&
                       IsResolvedLiteralOrParameter(
                           expr->GetAs<ResolvedCast>()->expr()->node_kind())))
      << "LIMIT ... OFFSET ... arg is of incorrect node kind: "
      << expr->node_kind_string();

  VALIDATOR_RET_CHECK(expr->type()->IsInt64())
      << "LIMIT ... OFFSET .... literal must be an integer";

  if (expr->node_kind() == RESOLVED_LITERAL) {
    // If a literal, we can also validate its value.
    const Value value = expr->GetAs<ResolvedLiteral>()->value();
    VALIDATOR_RET_CHECK(value.type()->IsInt64());
    VALIDATOR_RET_CHECK(!value.is_null())
        << "Unexpected literal with null value: " << value.DebugString();
    VALIDATOR_RET_CHECK_GE(value.int64_value(), 0);
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedLimitOffsetScan(
    const ResolvedLimitOffsetScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);

  VALIDATOR_RET_CHECK(scan->limit() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateArgumentIsInt64Constant(scan->limit()));

  if (scan->offset() != nullptr) {
    // OFFSET is optional.
    ZETASQL_RETURN_IF_ERROR(ValidateArgumentIsInt64Constant(scan->offset()));
  }

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(),
                                       visible_parameters));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedProjectScan(
    const ResolvedProjectScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);

  VALIDATOR_RET_CHECK(nullptr != scan->input_scan());
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

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedTVFScan(
    const ResolvedTVFScan* resolved_tvf_scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, resolved_tvf_scan);

  VALIDATOR_RET_CHECK_EQ(
      resolved_tvf_scan->argument_list_size(),
      resolved_tvf_scan->signature()->input_arguments().size());
  const FunctionSignature* function_call_signature =
      resolved_tvf_scan->function_call_signature().get();
  // It is possible the <function_call_signature> field is not set.
  if (function_call_signature != nullptr) {
    VALIDATOR_RET_CHECK_EQ(resolved_tvf_scan->argument_list_size(),
                           function_call_signature->NumConcreteArguments());
  }

  std::vector<int> table_argument_offsets;
  std::vector<int> descriptor_offsets;
  for (int arg_idx = 0; arg_idx < resolved_tvf_scan->argument_list_size();
       ++arg_idx) {
    const ResolvedFunctionArgument* resolved_arg =
        resolved_tvf_scan->argument_list(arg_idx);

    ZETASQL_RETURN_IF_ERROR(ValidateResolvedFunctionArgument(
        /*visible_columns=*/{}, visible_parameters, resolved_arg));

    if (resolved_arg->scan() != nullptr) {
      table_argument_offsets.push_back(arg_idx);
    }

    if (resolved_arg->descriptor_arg() != nullptr &&
        resolved_arg->descriptor_arg()->descriptor_column_list_size() > 0) {
      descriptor_offsets.push_back(arg_idx);
    }

    const FunctionArgumentType* concrete_arg =
        (function_call_signature == nullptr
             ? nullptr
             : &function_call_signature->ConcreteArgument(arg_idx));

    // If the function signature specifies a required schema for the relation
    // argument, check that the column names and types are a superset of those
    // from the scan.
    if (resolved_arg->scan() != nullptr) {
      const TableValuedFunction* tvf = resolved_tvf_scan->tvf();
      // We currently expect table valued function to have only one signature
      // since function overloading is not supported.
      VALIDATOR_RET_CHECK_EQ(tvf->NumSignatures(), 1);
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

      VALIDATOR_RET_CHECK_LT(
          arg_idx, resolved_tvf_scan->signature()->input_arguments().size());
      const TVFInputArgumentType& tvf_signature_arg =
          resolved_tvf_scan->signature()->argument(arg_idx);
      VALIDATOR_RET_CHECK(tvf_signature_arg.is_relation());
      const TVFRelation& input_relation = tvf_signature_arg.relation();
      ZETASQL_RETURN_IF_ERROR(ValidateRelationSchemaInResolvedFunctionArgument(
          required_input_schema, input_relation, resolved_arg));

      if (concrete_arg != nullptr) {
        VALIDATOR_RET_CHECK(concrete_arg->IsFixedRelation());
        VALIDATOR_RET_CHECK_EQ(input_relation,
                               concrete_arg->options().relation_input_schema());
      }
    } else if (resolved_arg->model() != nullptr) {
      if (concrete_arg != nullptr) {
        VALIDATOR_RET_CHECK(concrete_arg->IsModel());
      }
      const TableValuedFunction* tvf = resolved_tvf_scan->tvf();
      // We currently expect table valued function to have only one signature
      // since function overloading is not supported.
      VALIDATOR_RET_CHECK_EQ(tvf->NumSignatures(), 1);
    } else if (resolved_arg->expr() != nullptr) {
      VALIDATOR_RET_CHECK(resolved_arg->expr()->type() != nullptr);
      if (concrete_arg != nullptr) {
        VALIDATOR_RET_CHECK(concrete_arg->type() != nullptr);
        VALIDATOR_RET_CHECK(
            resolved_arg->expr()->type()->Equals(concrete_arg->type()));
      }
    }
  }

  // If there is any descriptor in tvf call with resolved column names, should
  // check table arguments to validate that those descriptor ResolvedColumns can
  // be projected from one of the table arguments.
  for (int descriptor_arg_index : descriptor_offsets) {
    const ResolvedFunctionArgument* resolved_descriptor_arg =
        resolved_tvf_scan->argument_list(descriptor_arg_index);
    const std::vector<ResolvedColumn>& descriptor_column_vector =
        resolved_descriptor_arg->argument_column_list();
    bool validationPassed = false;
    for (int table_arg_index : table_argument_offsets) {
      if (validationPassed) {
        break;
      }
      const ResolvedFunctionArgument* resolved_scan_arg =
          resolved_tvf_scan->argument_list(table_arg_index);
      const std::vector<ResolvedColumn>& table_argument_column_vector =
          resolved_scan_arg->argument_column_list();
      int descriptor_column_index;
      for (descriptor_column_index = 0;
           descriptor_column_index < descriptor_column_vector.size();
           descriptor_column_index++) {
        if (std::find(table_argument_column_vector.begin(),
                      table_argument_column_vector.end(),
                      descriptor_column_vector[descriptor_column_index]) !=
            table_argument_column_vector.end()) {
          break;
        }
      }

      if (descriptor_column_index == descriptor_column_vector.size()) {
        validationPassed = true;
      }
    }

    VALIDATOR_RET_CHECK(validationPassed)
        << "ResolvedDescriptor has at least one resolved column in "
           "descriptor_column_list that cannot be projected in any table "
           "argument in the same TVF call"
        << resolved_descriptor_arg->descriptor_arg()->DebugString();
  }

  // Verify the result schema is valid. Also verify the result schema in
  // `signature()` and `matched_signature` match.
  VALIDATOR_RET_CHECK(resolved_tvf_scan->signature() != nullptr);
  const TVFRelation& result_schema =
      resolved_tvf_scan->signature()->result_schema();
  if (result_schema.is_value_table()) {
    int64_t num_pseudo_columns = std::count_if(
        result_schema.columns().begin(), result_schema.columns().end(),
        [](const TVFSchemaColumn& column) { return column.is_pseudo_column; });
    VALIDATOR_RET_CHECK_EQ(1, result_schema.num_columns() - num_pseudo_columns);
    VALIDATOR_RET_CHECK(!result_schema.column(0).is_pseudo_column);
  } else {
    VALIDATOR_RET_CHECK_NE(0, result_schema.num_columns());
  }
  if (function_call_signature != nullptr) {
    VALIDATOR_RET_CHECK(
        function_call_signature->result_type().IsFixedRelation());
    const TVFRelation& result_schema_in_matched_signature =
        function_call_signature->result_type()
            .options()
            .relation_input_schema();
    VALIDATOR_RET_CHECK_EQ(result_schema, result_schema_in_matched_signature);
  }

  // Each output column should have a unique column id. Mark the column id
  // as seen and verify that we haven't already seen it.
  for (const ResolvedColumn& column : resolved_tvf_scan->column_list()) {
    ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(column));
  }

  // Ideally, column_index_list should always be the same size as column_list.
  // However, for historical reasons, some clients don't provide a
  // column_index_list and they match the table column by ResolvedColumn name
  // instead (this violates the contract of ResolvedColumn name()). Therefore
  // there's an exception here to allow an empty column_index_list. Once all
  // the client migrations are done, this exception should be removed.
  //
  // TODO: Remove this exception.
  if (resolved_tvf_scan->column_index_list_size() > 0) {
    // Check that all columns have corresponding indexes.
    VALIDATOR_RET_CHECK_EQ(resolved_tvf_scan->column_list_size(),
                           resolved_tvf_scan->column_index_list_size());

    VALIDATOR_RET_CHECK_NE(resolved_tvf_scan->signature(), nullptr);
    const TVFRelation result_schema =
        resolved_tvf_scan->signature()->result_schema();

    // Check that all indexes are valid indexes into the TVF's result schema,
    // and that the type of the corresponding columns matches.
    for (int column_index = 0;
         column_index < resolved_tvf_scan->column_list_size(); ++column_index) {
      const int tvf_schema_column_index =
          resolved_tvf_scan->column_index_list()[column_index];
      VALIDATOR_RET_CHECK_GE(tvf_schema_column_index, 0);
      VALIDATOR_RET_CHECK_LT(tvf_schema_column_index,
                             result_schema.num_columns());
      VALIDATOR_RET_CHECK(
          resolved_tvf_scan->column_list(column_index)
              .type()
              ->Equals(result_schema.column(tvf_schema_column_index).type));
    }
  }

  ZETASQL_RETURN_IF_ERROR(ValidateHintList(resolved_tvf_scan->hint_list()));
  resolved_tvf_scan->tvf();  // Mark field as visited.
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedRelationArgumentScan(
    const ResolvedRelationArgumentScan* arg_ref,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, arg_ref);

  // If we're currently validating a ResolvedCreateTableFunctionStmt, find the
  // argument in the current CREATE TABLE FUNCTION statement with the same name
  // as 'arg_ref'.
  if (current_create_table_function_stmt_ != nullptr) {
    VALIDATOR_RET_CHECK(std::any_of(
        current_create_table_function_stmt_->argument_name_list().begin(),
        current_create_table_function_stmt_->argument_name_list().end(),
        [arg_ref](const std::string& arg_name) {
          return zetasql_base::CaseEqual(arg_ref->name(), arg_name);
        }));
  }

  // Make sure the produced columns are unique and mark them as seen.
  for (const ResolvedColumn& column : arg_ref->column_list()) {
    ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(column));
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedWithScan(
    const ResolvedWithScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);

  // The main query can be correlated. The aliased subqueries cannot.
  VALIDATOR_RET_CHECK(nullptr != scan->query());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->query(), visible_parameters));

  for (const auto& with_entry : scan->with_entry_list()) {
    VALIDATOR_RET_CHECK(nullptr != with_entry);
    if (scan->recursive()) {
      ++nested_recursive_context_count_;
    }
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(with_entry->with_subquery(),
                                         /*visible_parameters=*/{}));
    with_entry->with_query_name();  // Mark visited.
    if (scan->recursive()) {
      --nested_recursive_context_count_;
    }
  }

  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->query()->column_list(), &visible_columns));
  ZETASQL_RETURN_IF_ERROR(CheckColumnList(scan, visible_columns));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedReturningClause(
    const ResolvedReturningClause* returning,
    std::set<ResolvedColumn>& visible_columns) {
  PushErrorContext push(this, returning);

  for (const auto& computed_column : returning->expr_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns,
                                         /*visible_parameters=*/{},
                                         computed_column->expr()));
  }
  ZETASQL_RETURN_IF_ERROR(AddColumnsFromComputedColumnList(returning->expr_list(),
                                                   &visible_columns));
  int output_size = returning->output_column_list_size();
  if (returning->action_column() != nullptr) {
    VALIDATOR_RET_CHECK_GE(output_size, 1);

    const ResolvedOutputColumn* action_column =
        returning->output_column_list().at(output_size - 1).get();
    VALIDATOR_RET_CHECK(action_column->column().type()->IsString());
    bool is_internal_name =
        IsInternalAlias(action_column->column().table_name());
    VALIDATOR_RET_CHECK_EQ(is_internal_name, true);
    output_size -= 1;
  }

  // Checks output columns other than the generated action column
  for (int i = 0; i < output_size; i++) {
    const auto& resolved_output_column = returning->output_column_list().at(i);
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumn(visible_columns,
                                                 resolved_output_column.get()));
  }
  return absl::OkStatus();
}

void Validator::Reset() {
  nested_recursive_context_count_ = 0;
  nested_recursive_scans_.clear();
  column_ids_seen_.clear();
  context_stack_.clear();
  error_context_ = nullptr;
}

absl::Status Validator::ValidateResolvedStatement(
    const ResolvedStatement* statement) {
  Reset();
  return ValidateResolvedStatementInternal(statement);
}

absl::Status Validator::ValidateResolvedStatementInternal(
    const ResolvedStatement* statement) {
  VALIDATOR_RET_CHECK(nullptr != statement);
  PushErrorContext push(this, statement);

  absl::Status status;
  switch (statement->node_kind()) {
    case RESOLVED_QUERY_STMT:
      status = ValidateResolvedQueryStmt(statement->GetAs<ResolvedQueryStmt>());
      break;
    case RESOLVED_EXPLAIN_STMT:
      status = ValidateResolvedStatementInternal(
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
    case RESOLVED_CREATE_SCHEMA_STMT:
      status = ValidateResolvedCreateSchemaStmt(
          statement->GetAs<ResolvedCreateSchemaStmt>());
      break;
    case RESOLVED_CREATE_SNAPSHOT_TABLE_STMT:
      status = ValidateResolvedCreateSnapshotTableStmt(
          statement->GetAs<ResolvedCreateSnapshotTableStmt>());
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
    case RESOLVED_CREATE_PRIVILEGE_RESTRICTION_STMT:
      status = ValidateResolvedCreatePrivilegeRestrictionStmt(
          statement->GetAs<ResolvedCreatePrivilegeRestrictionStmt>());
      break;
    case RESOLVED_ALTER_PRIVILEGE_RESTRICTION_STMT:
      status = ValidateResolvedAlterPrivilegeRestrictionStmt(
          statement->GetAs<ResolvedAlterPrivilegeRestrictionStmt>());
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
    case RESOLVED_CLONE_DATA_STMT:
      status = ValidateResolvedCloneDataStmt(
          statement->GetAs<ResolvedCloneDataStmt>());
      break;
    case RESOLVED_EXPORT_DATA_STMT:
      status = ValidateResolvedExportDataStmt(
          statement->GetAs<ResolvedExportDataStmt>());
      break;
    case RESOLVED_EXPORT_MODEL_STMT:
      status = ValidateResolvedExportModelStmt(
          statement->GetAs<ResolvedExportModelStmt>());
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
    case RESOLVED_DROP_SNAPSHOT_TABLE_STMT:
      status = ValidateResolvedDropSnapshotTableStmt(
          statement->GetAs<ResolvedDropSnapshotTableStmt>());
      break;
    case RESOLVED_DROP_TABLE_FUNCTION_STMT:
      status = ValidateResolvedDropTableFunctionStmt(
          statement->GetAs<ResolvedDropTableFunctionStmt>());
      break;
    case RESOLVED_DROP_PRIVILEGE_RESTRICTION_STMT:
      status = ValidateResolvedDropPrivilegeRestrictionStmt(
          statement->GetAs<ResolvedDropPrivilegeRestrictionStmt>());
      break;
    case RESOLVED_DROP_ROW_ACCESS_POLICY_STMT:
      status = ValidateResolvedDropRowAccessPolicyStmt(
          statement->GetAs<ResolvedDropRowAccessPolicyStmt>());
      break;
    case RESOLVED_DROP_SEARCH_INDEX_STMT:
      status = ValidateResolvedDropSearchIndexStmt(
          statement->GetAs<ResolvedDropSearchIndexStmt>());
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
    case RESOLVED_ALTER_ROW_ACCESS_POLICY_STMT:
      status = ValidateResolvedAlterRowAccessPolicyStmt(
          statement->GetAs<ResolvedAlterRowAccessPolicyStmt>());
      break;
    case RESOLVED_ALTER_ALL_ROW_ACCESS_POLICIES_STMT:
      status = ValidateResolvedAlterAllRowAccessPoliciesStmt(
          statement->GetAs<ResolvedAlterAllRowAccessPoliciesStmt>());
      break;
    case RESOLVED_ALTER_MATERIALIZED_VIEW_STMT:
      status = ValidateResolvedAlterObjectStmt(
          statement->GetAs<ResolvedAlterMaterializedViewStmt>());
      break;
    case RESOLVED_ALTER_TABLE_SET_OPTIONS_STMT:
      status = ValidateResolvedAlterTableSetOptionsStmt(
          statement->GetAs<ResolvedAlterTableSetOptionsStmt>());
      break;
    case RESOLVED_ALTER_DATABASE_STMT:
      status = ValidateResolvedAlterObjectStmt(
          statement->GetAs<ResolvedAlterDatabaseStmt>());
      break;
    case RESOLVED_ALTER_SCHEMA_STMT:
      status = ValidateResolvedAlterObjectStmt(
          statement->GetAs<ResolvedAlterSchemaStmt>());
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
    case RESOLVED_ANALYZE_STMT:
      status =
          ValidateResolvedAnalyzeStmt(statement->GetAs<ResolvedAnalyzeStmt>());
      break;
    case RESOLVED_ASSERT_STMT:
      status =
          ValidateResolvedAssertStmt(statement->GetAs<ResolvedAssertStmt>());
      break;
    case RESOLVED_ASSIGNMENT_STMT:
      status = ValidateResolvedAssignmentStmt(
          statement->GetAs<ResolvedAssignmentStmt>());
      break;
    case RESOLVED_EXECUTE_IMMEDIATE_STMT:
      status = ValidateResolvedExecuteImmediateStmt(
          statement->GetAs<ResolvedExecuteImmediateStmt>());
      break;
    case RESOLVED_CREATE_ENTITY_STMT:
      status = ValidateResolvedCreateEntityStmt(
          statement->GetAs<ResolvedCreateEntityStmt>());
      break;
    case RESOLVED_ALTER_ENTITY_STMT:
      status = ValidateResolvedAlterEntityStmt(
          statement->GetAs<ResolvedAlterEntityStmt>());
      break;
    case RESOLVED_AUX_LOAD_DATA_STMT:
      status = ValidateResolvedAuxLoadDataStmt(
          statement->GetAs<ResolvedAuxLoadDataStmt>());
      break;
    default:
      VALIDATOR_RET_CHECK_FAIL() << "Cannot validate statement of type "
                                 << statement->node_kind_string();
  }

  status.Update(ValidateHintList(statement->hint_list()));

  if (!status.ok()) {
    if (status.code() == absl::StatusCode::kResourceExhausted) {
      // Don't wrap a resource exhausted status into internal error. This error
      // may still occur for a valid properly resolved expression (stack
      // exhaustion in case of deeply nested expression). There exist cases
      // where the validator uses more stack than parsing/analysis (b/65294961).
      return status;
    }
    return ::zetasql_base::InternalErrorBuilder()
           << "Resolved AST validation failed: " << status.message() << "\n"
           << statement->DebugString(
                  {{error_context_, "(validation failed here)"}});
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateDatabaseStmt(
    const ResolvedCreateDatabaseStmt* stmt) {
  PushErrorContext push(this, stmt);
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedIndexStmt(
    const ResolvedCreateIndexStmt* stmt) {
  PushErrorContext push(this, stmt);
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));

  VALIDATOR_RET_CHECK(stmt->table_scan() != nullptr);
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedTableScan(stmt->table_scan(), /*visible_parameters=*/{}));
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(AddColumnList(stmt->table_scan()->column_list(),
                                &visible_columns));

  for (const auto& index_unnest_column : stmt->unnest_expressions_list()) {
    VALIDATOR_RET_CHECK(index_unnest_column->array_expr() != nullptr);
    VALIDATOR_RET_CHECK(index_unnest_column->array_expr()->type()->IsArray())
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

  if (stmt->index_all_columns()) {
    VALIDATOR_RET_CHECK(stmt->index_item_list().empty());
    VALIDATOR_RET_CHECK(stmt->is_search());
  }

  for (const auto& item : stmt->index_item_list()) {
    ZETASQL_RETURN_IF_ERROR(CheckColumnIsPresentInColumnSet(
        item->column_ref()->column(), visible_columns));
  }

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExprList(visible_columns,
                                           /*visible_parameters=*/{},
                                           stmt->storing_expression_list()));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateSchemaStmt(
    const ResolvedCreateSchemaStmt* stmt) {
  PushErrorContext push(this, stmt);
  if (stmt->collation_name() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateCollateExpr(stmt->collation_name()));
  }
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedForeignKey(
    const ResolvedForeignKey* foreign_key,
    const std::vector<const Type*> column_types,
    absl::flat_hash_set<std::string>* constraint_names) {
  PushErrorContext push(this, foreign_key);
  if (!foreign_key->constraint_name().empty()) {
    VALIDATOR_RET_CHECK(
        constraint_names->insert(foreign_key->constraint_name()).second)
        << "Duplicate constraint name: " << foreign_key->constraint_name();
  }
  const auto& referencing_offsets =
      foreign_key->referencing_column_offset_list();
  VALIDATOR_RET_CHECK(!referencing_offsets.empty())
      << "Missing foreign key column offsets";
  const auto& referenced_offsets = foreign_key->referenced_column_offset_list();
  VALIDATOR_RET_CHECK_EQ(referencing_offsets.size(), referenced_offsets.size())
      << "Size of " << referencing_offsets.size()
      << " for the foreign key referencing column offset list is not the "
      << "same as the size of " << referenced_offsets.size()
      << " for the referenced column offset list";
  absl::flat_hash_set<int> referencing_set;
  for (int offset : referencing_offsets) {
    VALIDATOR_RET_CHECK(offset >= 0 && offset < column_types.size())
        << "Invalid foreign key referencing column at offset " << offset;
    VALIDATOR_RET_CHECK(referencing_set.insert(offset).second)
        << "Duplicate foreign key referencing column at offset " << offset;
    VALIDATOR_RET_CHECK(
        column_types[offset]->SupportsEquality(language_options_))
        << "Foreign key referencing column at offset" << offset
        << " does not support equality";
  }
  const auto* referenced_table = foreign_key->referenced_table();
  VALIDATOR_RET_CHECK_NE(referenced_table, nullptr)
      << "Missing foreign key referenced table";
  absl::flat_hash_set<int> referenced_set;
  for (int offset : referenced_offsets) {
    VALIDATOR_RET_CHECK(offset >= 0 && offset < referenced_table->NumColumns())
        << "Invalid foreign key referenced column at offset " << offset;
    VALIDATOR_RET_CHECK(referenced_set.insert(offset).second)
        << "Duplicate foreign key referenced column at offset " << offset;
    const auto* type = referenced_table->GetColumn(offset)->GetType();
    VALIDATOR_RET_CHECK(type->SupportsEquality(language_options_))
        << "Foreign key referenced column at offset" << offset
        << " does not support equality";
  }

  return ValidateHintList(foreign_key->option_list());
}

absl::Status Validator::ValidateResolvedPrimaryKey(
    const std::vector<const Type*>& resolved_column_types,
    const ResolvedPrimaryKey* primary_key,
    absl::flat_hash_set<std::string>* seen_constraint_names) {
  PushErrorContext push(this, primary_key);

  if (!primary_key->constraint_name().empty()) {
    auto result = seen_constraint_names->insert(primary_key->constraint_name());
    VALIDATOR_RET_CHECK(result.second)
        << "Duplicate constraint name: " << primary_key->constraint_name();
  }

  if (resolved_column_types.empty()) {
    VALIDATOR_RET_CHECK_EQ(primary_key->column_offset_list_size(), 0);
  } else {
    VALIDATOR_RET_CHECK_EQ(primary_key->column_offset_list_size(),
                           primary_key->column_name_list_size());

    std::set<int> column_indexes;
    for (const int i : primary_key->column_offset_list()) {
      if (i >= resolved_column_types.size() || i < 0) {
        return InternalErrorBuilder()
               << "Invalid column index " << i << " in PRIMARY KEY";
      }
      if (zetasql_base::ContainsKey(column_indexes, i)) {
        return InternalErrorBuilder()
               << "Duplicate column index " << i << " in PRIMARY KEY";
      }
      column_indexes.insert(i);
    }
  }

  return ValidateHintList(primary_key->option_list());
}

absl::Status Validator::ValidateColumnDefinitions(
    const std::vector<std::unique_ptr<const ResolvedColumnDefinition>>&
        column_definitions,
    std::set<ResolvedColumn>* visible_columns) {
  for (const auto& column_definition : column_definitions) {
    if (!zetasql_base::InsertIfNotPresent(visible_columns,
                                 column_definition->column())) {
      return InternalErrorBuilder()
             << "Column already used: "
             << column_definition->column().DebugString();
    }
  }
  for (const auto& column_definition : column_definitions) {
    if (column_definition->annotations() != nullptr) {
      ZETASQL_RETURN_IF_ERROR(ValidateColumnAnnotations(
          column_definition->annotations()));
      ZETASQL_ASSIGN_OR_RETURN(TypeParameters full_type_parameters,
                       column_definition->GetFullTypeParameters());
      ZETASQL_RETURN_IF_ERROR(column_definition->type()->ValidateResolvedTypeParameters(
          full_type_parameters, language_options_.product_mode()));
    }
    VALIDATOR_RET_CHECK(column_definition->type() != nullptr);
    if (column_definition->generated_column_info() != nullptr) {
      VALIDATOR_RET_CHECK(column_definition->default_value() == nullptr);
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedGeneratedColumnInfo(
          column_definition.get(), *visible_columns));
    }

    if (column_definition->default_value() != nullptr) {
      VALIDATOR_RET_CHECK(column_definition->generated_column_info() ==
                          nullptr);
      ZETASQL_RETURN_IF_ERROR(ValidateResolvedColumnDefaultValue(
          column_definition->default_value(), column_definition->type()));
    }
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateTableStmtBase(
    const ResolvedCreateTableStmtBase* stmt,
    std::set<ResolvedColumn>* visible_columns) {
  PushErrorContext push(this, stmt);
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  ZETASQL_RETURN_IF_ERROR(ValidateColumnDefinitions(stmt->column_definition_list(),
                                            visible_columns));

  const Table* like_table = stmt->like_table();
  if (like_table != nullptr) {
    // Column information in the like_table will be extracted into column
    // definition list.
    VALIDATOR_RET_CHECK_EQ(like_table->NumColumns(), visible_columns->size())
        << "Number of columns does not match in like_table and column "
           "definition list";
  }
  for (const auto& pseudo_column : stmt->pseudo_column_list()) {
    if (!zetasql_base::InsertIfNotPresent(visible_columns, pseudo_column)) {
      return InternalErrorBuilder()
             << "Column already used: " << pseudo_column.DebugString();
    }
  }

  // Used to detect duplicate constraint names.
  absl::flat_hash_set<std::string> constraint_names;

  if (stmt->primary_key() != nullptr) {
    std::vector<const Type*> column_types;
    for (const auto& column_definition : stmt->column_definition_list()) {
      column_types.push_back(column_definition->type());
    }
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedPrimaryKey(
        column_types, stmt->primary_key(), &constraint_names));
  }

  // Validate foreign keys.
  VALIDATOR_RET_CHECK(
      stmt->foreign_key_list().empty() ||
      language_options_.LanguageFeatureEnabled(FEATURE_FOREIGN_KEYS))
      << "Foreign keys are not supported";
  const auto& column_definitions = stmt->column_definition_list();
  std::vector<const Type*> column_types;
  column_types.reserve(column_definitions.size());
  for (const auto& column_definition : column_definitions) {
    column_types.push_back(column_definition->type());
  }
  for (const auto& foreign_key : stmt->foreign_key_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedForeignKey(foreign_key.get(), column_types,
                                               &constraint_names));
  }

  // Validate check constraints.
  VALIDATOR_RET_CHECK(
      stmt->check_constraint_list().empty() ||
      language_options_.LanguageFeatureEnabled(FEATURE_CHECK_CONSTRAINT));
  for (const auto& check_constraint : stmt->check_constraint_list()) {
    if (!check_constraint->constraint_name().empty()) {
      VALIDATOR_RET_CHECK(
          constraint_names.insert(check_constraint->constraint_name()).second)
          << "Duplicate constraint name: "
          << check_constraint->constraint_name();
    }
    VALIDATOR_RET_CHECK(check_constraint->expression() != nullptr)
        << "Missing expression in ZETASQL_CHECK constraint";
    VALIDATOR_RET_CHECK(check_constraint->expression()->type()->IsBool())
        << "CHECK constraint expects a boolean expression; got "
        << check_constraint->expression()->type()->ShortTypeName(
               language_options_.product_mode());
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(*visible_columns,
                                         /* visible_parameters= */ {},
                                         check_constraint->expression()));
  }

  // Validate collation.
  if (stmt->collation_name() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateCollateExpr(stmt->collation_name()));
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateTableStmt(
    const ResolvedCreateTableStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK(stmt->like_table() == nullptr ||
                      stmt->clone_from() == nullptr)
      << "CLONE and LIKE cannot both be used for CREATE TABLE";
  VALIDATOR_RET_CHECK(stmt->like_table() == nullptr ||
                      stmt->copy_from() == nullptr)
      << "COPY and LIKE cannot both be used for CREATE TABLE";
  VALIDATOR_RET_CHECK(stmt->clone_from() == nullptr ||
                      stmt->copy_from() == nullptr)
      << "COPY and CLONE cannot both be used for CREATE TABLE";
  if (stmt->clone_from() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedCloneDataSource(stmt->clone_from()));
  }
  if (stmt->copy_from() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedCloneDataSource(stmt->copy_from()));
  }
  // Build and store the list of visible_columns.
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedCreateTableStmtBase(stmt, &visible_columns));
  for (const auto& partition_by_expr : stmt->partition_by_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        visible_columns, {} /* visible_parameters */, partition_by_expr.get()));
  }
  for (const auto& cluster_by_expr : stmt->cluster_by_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        visible_columns, {} /* visible_parameters */, cluster_by_expr.get()));
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateSnapshotTableStmt(
    const ResolvedCreateSnapshotTableStmt* stmt) {
  PushErrorContext push(this, stmt);

  VALIDATOR_RET_CHECK(stmt->clone_from() != nullptr)
      << "CLONE must be provided for CREATE SNAPSHOT TABLE";
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedCloneDataSource(stmt->clone_from()));

  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedGeneratedColumnInfo(
    const ResolvedColumnDefinition* column_definition,
    const std::set<ResolvedColumn>& visible_columns) {
  PushErrorContext push(this, column_definition);
  const ResolvedGeneratedColumnInfo* generated_column_info =
      column_definition->generated_column_info();
  VALIDATOR_RET_CHECK(generated_column_info->expression() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns,
                                       /* visible_parameters = */ {},
                                       generated_column_info->expression()));
  VALIDATOR_RET_CHECK(generated_column_info->expression()->type() != nullptr);
  VALIDATOR_RET_CHECK(column_definition->type() != nullptr);
  VALIDATOR_RET_CHECK(generated_column_info->expression()->type()->Equals(
      column_definition->type()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateTableAsSelectStmt(
    const ResolvedCreateTableAsSelectStmt* stmt) {
  PushErrorContext push(this, stmt);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(
      stmt->query(), {} /* visible_parameters */));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(
      stmt->query()->column_list(), stmt->output_column_list(),
      stmt->is_value_table()));
  const int num_columns = stmt->column_definition_list_size();
  if (num_columns != stmt->output_column_list_size()) {
    return InternalErrorBuilder()
           << "Inconsistent length between column definition list ("
           << stmt->column_definition_list_size()
           << ") and output column list (" << stmt->output_column_list_size()
           << ")";
  }
  for (int i = 0; i < num_columns; ++i) {
    const ResolvedOutputColumn* output_column = stmt->output_column_list(i);
    const ResolvedColumnDefinition* column_def =
        stmt->column_definition_list(i);
    const std::string& output_column_name = output_column->name();
    const std::string& column_def_name = column_def->name();
    if (output_column_name != column_def_name) {
      return InternalErrorBuilder()
             << "Output column name '" << output_column_name
             << "' is different from column definition name '"
             << column_def_name << "' for column " << (i + 1);
    }
    const Type* output_type = output_column->column().type();
    const Type* defined_type = column_def->type();
    if (!output_type->Equals(defined_type)) {
      return InternalErrorBuilder()
             << "Output column type " << output_type->DebugString()
             << " is different from column definition type "
             << defined_type->DebugString() << " for column " << (i + 1) << " ("
             << column_def_name << ")";
    }
  }
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedCreateTableStmtBase(stmt, &visible_columns));
  for (const auto& partition_by_expr : stmt->partition_by_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        visible_columns, {} /* visible_parameters */, partition_by_expr.get()));
  }
  for (const auto& cluster_by_expr : stmt->cluster_by_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        visible_columns, {} /* visible_parameters */, cluster_by_expr.get()));
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateModelStmt(
    const ResolvedCreateModelStmt* stmt) {
  PushErrorContext push(this, stmt);
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedScan(stmt->query(), {} /* visible_parameters */));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(stmt->query()->column_list(),
                                                   stmt->output_column_list(),
                                                   /*is_value_table=*/false));
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  if (!stmt->transform_list().empty()) {
    // Validate transform_input_column_list is used properly in transform_list.
    std::set<ResolvedColumn> transform_input_cols;
    for (const auto& transform_input_col :
         stmt->transform_input_column_list()) {
      transform_input_cols.insert(transform_input_col->column());
    }
    for (const auto& group : stmt->transform_analytic_function_group_list()) {
      ZETASQL_RETURN_IF_ERROR(AddColumnsFromComputedColumnList(
          group->analytic_function_list(), &transform_input_cols));
    }
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedComputedColumnList(transform_input_cols, {},
                                                       stmt->transform_list()));

    // Validate transform_list is used properly in transform_output_column_list.
    std::vector<ResolvedColumn> transform_resolved_cols;
    for (const auto& transform_col : stmt->transform_list()) {
      transform_resolved_cols.push_back(transform_col->column());
    }
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(
        transform_resolved_cols, stmt->transform_output_column_list(),
        /*is_value_table=*/false));
  } else {
    // All transform related fields should be empty if there is no TRANSFORM
    // clause.
    VALIDATOR_RET_CHECK(stmt->transform_input_column_list().empty());
    VALIDATOR_RET_CHECK(stmt->transform_output_column_list().empty());
    VALIDATOR_RET_CHECK(stmt->transform_analytic_function_group_list().empty());
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateViewStmt(
    const ResolvedCreateViewStmt* stmt) {
  PushErrorContext push(this, stmt);
  if (stmt->recursive()) {
    ++nested_recursive_context_count_;
  }
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedScan(stmt->query(), {} /* visible_parameters */));
  if (stmt->recursive()) {
    --nested_recursive_context_count_;
  }
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(
      stmt->query()->column_list(), stmt->output_column_list(),
      stmt->is_value_table()));
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateMaterializedViewStmt(
    const ResolvedCreateMaterializedViewStmt* stmt) {
  PushErrorContext push(this, stmt);
  if (stmt->recursive()) {
    ++nested_recursive_context_count_;
  }
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedScan(stmt->query(), {} /* visible_parameters */));
  if (stmt->recursive()) {
    --nested_recursive_context_count_;
  }
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(stmt->query()->column_list(),
                                                   stmt->output_column_list(),
                                                   stmt->is_value_table()));
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedWithPartitionColumns(
    const ResolvedWithPartitionColumns* with_partition_columns,
    std::set<ResolvedColumn>* visible_columns) {
  PushErrorContext push(this, with_partition_columns);
  if (with_partition_columns != nullptr) {
    for (const auto& column_definition :
         with_partition_columns->column_definition_list()) {
      if (!zetasql_base::InsertIfNotPresent(visible_columns,
                                   column_definition->column())) {
        return InternalErrorBuilder()
               << "Column already used: "
               << column_definition->column().DebugString();
      }
    }

    for (const auto& column_definition :
         with_partition_columns->column_definition_list()) {
      // column annotation are not supported for column definition in
      // with_partition_columns.
      VALIDATOR_RET_CHECK(column_definition->annotations() == nullptr);
      VALIDATOR_RET_CHECK(column_definition->type() != nullptr);
    }
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateExternalTableStmt(
    const ResolvedCreateExternalTableStmt* stmt) {
  PushErrorContext push(this, stmt);
  // Build the visible_columns.
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedCreateTableStmtBase(stmt, &visible_columns));
  return ValidateResolvedWithPartitionColumns(stmt->with_partition_columns(),
                                              &visible_columns);
}

absl::Status Validator::ValidateResolvedCreatePrivilegeRestrictionStmt(
    const ResolvedCreatePrivilegeRestrictionStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK(!stmt->column_privilege_list().empty());
  VALIDATOR_RET_CHECK(!stmt->name_path().empty());
  VALIDATOR_RET_CHECK(absl::AsciiStrToLower(stmt->object_type()) == "table" ||
                      absl::AsciiStrToLower(stmt->object_type()) == "view");
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateRowAccessPolicyStmt(
    const ResolvedCreateRowAccessPolicyStmt* stmt) {
  PushErrorContext push(this, stmt);

  VALIDATOR_RET_CHECK(stmt->table_scan() != nullptr);
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedTableScan(stmt->table_scan(), /*visible_parameters=*/{}));
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(stmt->table_scan()->column_list(), &visible_columns));

  const ResolvedExpr* predicate = stmt->predicate();
  VALIDATOR_RET_CHECK(predicate != nullptr);
  VALIDATOR_RET_CHECK(predicate->type()->IsBool())
      << "CreateRowAccessPolicyStmt has predicate with non-BOOL type: "
      << predicate->type()->DebugString();
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      visible_columns, {}  /* visible_parameters */, predicate));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateConstantStmt(
    const ResolvedCreateConstantStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK(stmt->expr() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      {} /* visible_columns */, {} /* visible_parameters */, stmt->expr()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateFunctionStmt(
    const ResolvedCreateFunctionStmt* stmt) {
  PushErrorContext push(this, stmt);
  bool contains_templated_args =
      std::any_of(stmt->signature().arguments().begin(),
                  stmt->signature().arguments().end(),
                  [](const FunctionArgumentType& arg_type) {
                    return arg_type.IsTemplated();
                  });
  if (contains_templated_args) {
    // This function declaration contained one or more templated argument types.
    // In this case the resolver sets the 'language' field to SQL and the 'code'
    // field to contain the original string contents of the SQL expression body.
    // The 'arguments' are empty in this case since the templated type
    // information is present in the function signature instead.
    if (stmt->signature().result_type().IsTemplated()) {
      VALIDATOR_RET_CHECK_EQ("SQL", stmt->language());
    }
    VALIDATOR_RET_CHECK(stmt->function_expression() == nullptr);
    VALIDATOR_RET_CHECK(!stmt->code().empty());
  } else {
    // This function declaration did not contain any templated argument types.
    // In this case the function should have a concrete return type.
    VALIDATOR_RET_CHECK(
        stmt->return_type()->Equals(stmt->signature().result_type().type()));
  }
  VALIDATOR_RET_CHECK(stmt->return_type() != nullptr);

  // For non-aggregates, no columns are visible.  For aggregates, columns
  // created by the aggregate expressions are visible.
  std::set<ResolvedColumn> visible_columns;

  if (!stmt->aggregate_expression_list().empty()) {
    VALIDATOR_RET_CHECK(stmt->is_aggregate());
    VALIDATOR_RET_CHECK(stmt->function_expression() != nullptr);

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
    VALIDATOR_RET_CHECK(
        stmt->function_expression()->type()->Equals(stmt->return_type()));

    // Check no query parameter in function body.
    std::vector<const ResolvedNode*> parameter_nodes;
    stmt->function_expression()->GetDescendantsWithKinds({RESOLVED_PARAMETER},
                                                         &parameter_nodes);
    VALIDATOR_RET_CHECK(parameter_nodes.empty());
  }

  // Check that is_remote is true iff language is "REMOTE".
  VALIDATOR_RET_CHECK_EQ(zetasql_base::CaseEqual(stmt->language(), "REMOTE"),
                         stmt->is_remote())
      << "is_remote is true iff language is \"REMOTE\"";

  if (stmt->is_remote() &&
      language_options_.LanguageFeatureEnabled(FEATURE_V_1_3_REMOTE_FUNCTION)) {
    // For a remote function, its code should be empty.
    VALIDATOR_RET_CHECK(stmt->code().empty());
  }

  if (stmt->connection() != nullptr) {
    VALIDATOR_RET_CHECK(stmt->is_remote());
  }

  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return absl::OkStatus();
}

// Check that the function signature does not contain unsupported templated
// argument types.
absl::Status Validator::CheckFunctionArgumentType(
    const FunctionArgumentTypeList& argument_type_list,
    absl::string_view statement_type) {
  for (const FunctionArgumentType& arg_type : argument_type_list) {
    switch (arg_type.kind()) {
      case ARG_TYPE_FIXED:
      case ARG_TYPE_ARBITRARY:
      case ARG_TYPE_RELATION:
        continue;
      default:
        VALIDATOR_RET_CHECK_FAIL()
            << "Unexpected " << statement_type
            << " argument type: " << arg_type.DebugString();
    }
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateTableFunctionStmt(
    const ResolvedCreateTableFunctionStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK_EQ(stmt->argument_name_list().size(),
                         stmt->signature().arguments().size());
  ZETASQL_RETURN_IF_ERROR(CheckFunctionArgumentType(stmt->signature().arguments(),
                                            "CREATE TABLE FUNCTION"));
  if (stmt->query() != nullptr) {
    VALIDATOR_RET_CHECK(!stmt->language().empty());

    zetasql_base::VarSetter<ArgumentKindSet> allowed_arg_kinds_setter(
        &allowed_argument_kinds_,
        {ResolvedArgumentDefEnums::SCALAR});
    zetasql_base::VarSetter<const ResolvedCreateTableFunctionStmt*> stmt_setter(
        &current_create_table_function_stmt_, stmt);

    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(stmt->query(),
                                         {} /* visible_parameters */));
    VALIDATOR_RET_CHECK(!stmt->output_column_list().empty());

    // Check no query parameter in table function body.
    std::vector<const ResolvedNode*> parameter_nodes;
    stmt->query()->GetDescendantsWithKinds({RESOLVED_PARAMETER},
                                           &parameter_nodes);
    VALIDATOR_RET_CHECK(parameter_nodes.empty());
  }

  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  if (stmt->signature().IsTemplated()) {
    VALIDATOR_RET_CHECK(stmt->output_column_list().empty());
    VALIDATOR_RET_CHECK(stmt->query() == nullptr);
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateProcedureStmt(
    const ResolvedCreateProcedureStmt* stmt) {
  PushErrorContext push(this, stmt);
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  VALIDATOR_RET_CHECK_EQ(stmt->argument_name_list().size(),
                         stmt->signature().arguments().size());
  ZETASQL_RETURN_IF_ERROR(CheckFunctionArgumentType(stmt->signature().arguments(),
                                            "CREATE PROCEDURE"));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCreateEntityStmt(
    const ResolvedCreateEntityStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK(!stmt->entity_type().empty());
  VALIDATOR_RET_CHECK(stmt->entity_body_json().empty() ||
                      stmt->entity_body_text().empty())
      << "At most one of JSON or TEXT literals can be non-empty";
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAlterEntityStmt(
    const ResolvedAlterEntityStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK(!stmt->entity_type().empty());
  for (const auto& action : stmt->alter_action_list()) {
    if (action->node_kind() == RESOLVED_SET_AS_ACTION) {
      auto* set_as_action = action->GetAs<ResolvedSetAsAction>();
      VALIDATOR_RET_CHECK(set_as_action->entity_body_json().empty() !=
                          set_as_action->entity_body_text().empty())
          << "Exactly one of JSON or TEXT literals should be non-empty";
    }
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateCompatibleSchemaForClone(const Table* source,
                                                         const Table* target) {
  VALIDATOR_RET_CHECK_EQ(source->NumColumns(), target->NumColumns());
  for (int i = 0; i < source->NumColumns(); i++) {
    const Column* from = source->GetColumn(i);
    const Column* to = target->GetColumn(i);
    VALIDATOR_RET_CHECK(from->GetType()->Equals(to->GetType()))
        << "Incompatible column type: " << from->GetType()->DebugString()
        << " vs. " << to->GetType()->DebugString();
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateSingleCloneDataSource(
    const ResolvedScan* source, const Table* target) {
  PushErrorContext push(this, source);
  switch (source->node_kind()) {
    case RESOLVED_TABLE_SCAN:
      if (target != nullptr) {
        ZETASQL_RETURN_IF_ERROR(ValidateCompatibleSchemaForClone(
            source->GetAs<ResolvedTableScan>()->table(), target));
      }
      break;
    case RESOLVED_FILTER_SCAN:
      VALIDATOR_RET_CHECK(
          source->GetAs<ResolvedFilterScan>()->input_scan()->node_kind() ==
          RESOLVED_TABLE_SCAN)
          << "Bad scan type: "
          << source->GetAs<ResolvedFilterScan>()->node_kind_string();
      break;
    default:
      return InternalErrorBuilder()
             << "Bad scan type: " << source->node_kind_string();
  }
  return ValidateResolvedScan(source, {} /* visible_parameters */);
}

absl::Status Validator::ValidateResolvedCloneDataSource(
    const ResolvedScan* source, const Table* target) {
  PushErrorContext push(this, source);
  if (source->node_kind() == RESOLVED_SET_OPERATION_SCAN) {
    const ResolvedSetOperationScan* scan =
        source->GetAs<ResolvedSetOperationScan>();
    VALIDATOR_RET_CHECK_EQ(scan->op_type(),
                           ResolvedSetOperationScan::UNION_ALL);
    for (const auto& input : scan->input_item_list()) {
      ZETASQL_RETURN_IF_ERROR(ValidateSingleCloneDataSource(input->scan(), target));
    }
    return absl::OkStatus();
  }
  return ValidateSingleCloneDataSource(source, target);
}

absl::Status Validator::ValidateResolvedCloneDataStmt(
    const ResolvedCloneDataStmt* stmt) {
  PushErrorContext push(this, stmt);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedTableScan(stmt->target_table(),
                                            /*visible_parameters=*/{}));
  return ValidateResolvedCloneDataSource(stmt->clone_from(),
                                         stmt->target_table()->table());
}

absl::Status Validator::ValidateResolvedExportDataStmt(
    const ResolvedExportDataStmt* stmt) {
  PushErrorContext push(this, stmt);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(
      stmt->query(), {} /* visible_parameters */));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(
      stmt->query()->column_list(), stmt->output_column_list(),
      stmt->is_value_table()));
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedExportModelStmt(
    const ResolvedExportModelStmt* stmt) {
  PushErrorContext push(this, stmt);
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCallStmt(const ResolvedCallStmt* stmt) {
  PushErrorContext push(this, stmt);

  VALIDATOR_RET_CHECK(stmt->procedure() != nullptr)
      << "ResolvedCallStmt does not have a Procedure:\n"
      << stmt->DebugString();

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExprList({}, {}, stmt->argument_list()));

  VALIDATOR_RET_CHECK(stmt->signature().IsConcrete())
      << "ResolvedCallStmt must have a concrete signature:\n"
      << stmt->DebugString();
  const int num_args = stmt->signature().NumConcreteArguments();
  VALIDATOR_RET_CHECK_EQ(stmt->argument_list_size(), num_args);
  for (int i = 0; i < num_args; ++i) {
    VALIDATOR_RET_CHECK(stmt->argument_list(i)->type()->Equals(
        stmt->signature().ConcreteArgumentType(i)));
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedDefineTableStmt(
    const ResolvedDefineTableStmt* stmt) {
  PushErrorContext push(this, stmt);
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedDescribeStmt(
    const ResolvedDescribeStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedShowStmt(const ResolvedShowStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedBeginStmt(
    const ResolvedBeginStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedSetTransactionStmt(
    const ResolvedSetTransactionStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedCommitStmt(
    const ResolvedCommitStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedRollbackStmt(
    const ResolvedRollbackStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedStartBatchStmt(
    const ResolvedStartBatchStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedRunBatchStmt(
    const ResolvedRunBatchStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAbortBatchStmt(
    const ResolvedAbortBatchStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedDropStmt(const ResolvedDropStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedDropFunctionStmt(
    const ResolvedDropFunctionStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK_EQ(stmt->signature() == nullptr,
                         stmt->arguments() == nullptr);
  if (stmt->signature() != nullptr) {
    const bool has_relation_args =
        std::any_of(stmt->signature()->signature().arguments().begin(),
                    stmt->signature()->signature().arguments().end(),
                    [](const FunctionArgumentType& arg_type) {
                      return arg_type.IsRelation();
                    });
    if (has_relation_args) {
      VALIDATOR_RET_CHECK_EQ(0, stmt->arguments()->arg_list_size());
    } else {
      VALIDATOR_RET_CHECK_EQ(stmt->signature()->signature().arguments().size(),
                             stmt->arguments()->arg_list_size());
    }
    VALIDATOR_RET_CHECK(stmt->signature()->signature().result_type().IsVoid());
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedDropTableFunctionStmt(
    const ResolvedDropTableFunctionStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedDropSnapshotTableStmt(
    const ResolvedDropSnapshotTableStmt* stmt) {
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedDropMaterializedViewStmt(
    const ResolvedDropMaterializedViewStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedDropPrivilegeRestrictionStmt(
    const ResolvedDropPrivilegeRestrictionStmt* stmt) {
  VALIDATOR_RET_CHECK(!stmt->column_privilege_list().empty());
  VALIDATOR_RET_CHECK(absl::AsciiStrToLower(stmt->object_type()) == "table" ||
                      absl::AsciiStrToLower(stmt->object_type()) == "view");
  VALIDATOR_RET_CHECK(!stmt->name_path().empty());
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedDropRowAccessPolicyStmt(
    const ResolvedDropRowAccessPolicyStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK(!(stmt->is_drop_all() && stmt->is_if_exists()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedDropSearchIndexStmt(
    const ResolvedDropSearchIndexStmt* stmt) {
  return absl::OkStatus();
}
absl::Status Validator::ValidateResolvedGrantStmt(
    const ResolvedGrantStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedRevokeStmt(
    const ResolvedRevokeStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedOrderByScan(
    const ResolvedOrderByScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);

  VALIDATOR_RET_CHECK(nullptr != scan->input_scan());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(), visible_parameters));

  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->input_scan()->column_list(), &visible_columns));
  for (const auto& order_by_item : scan->order_by_item_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedOrderByItem(
        visible_columns, visible_parameters, order_by_item.get()));
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedOrderByItem(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedOrderByItem* item) {
  PushErrorContext push(this, item);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                       item->column_ref()));
  if (item->collation_name() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         item->collation_name()));
    VALIDATOR_RET_CHECK(item->collation_name()->type()->IsString())
        << "collation_name must have type STRING";
    if (language_options_.LanguageFeatureEnabled(
            FEATURE_V_1_3_COLLATION_SUPPORT)) {
      if (item->collation_name()->Is<ResolvedLiteral>()) {
        VALIDATOR_RET_CHECK(!item->collation().Empty());
        VALIDATOR_RET_CHECK_EQ(item->collation_name()
                                   ->GetAs<ResolvedLiteral>()
                                   ->value()
                                   .string_value(),
                               item->collation().CollationName());
      } else {
        VALIDATOR_RET_CHECK(item->collation_name()->Is<ResolvedParameter>());
        VALIDATOR_RET_CHECK(item->collation().Empty());
      }
    }
  }
  item->is_descending();  // Mark field as visited.
  item->null_order();  // Mark field as visited.
  if (!item->collation().Empty()) {
    VALIDATOR_RET_CHECK(
        item->collation().HasCompatibleStructure(item->column_ref()->type()))
        << "collation must have compatible structure with the type of "
           "column_ref";
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedQueryStmt(
    const ResolvedQueryStmt* query) {
  VALIDATOR_RET_CHECK(nullptr != query);
  PushErrorContext push(this, query);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(query->query(),
                                       {} /* visible_parameters */));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedOutputColumnList(
      query->query()->column_list(), query->output_column_list(),
      query->is_value_table()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedScan(
    const ResolvedScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  VALIDATOR_RET_CHECK(nullptr != scan);
  PushErrorContext push(this, scan);

  // We assign the status in each switch branch and call ZETASQL_RETURN_IF_ERROR only
  // once. This is because ZETASQL_RETURN_IF_ERROR introduces temporary variables on
  // each call, which are not eliminated between the switch branches. This
  // substantially increases this stack size of this function, causing stack
  // exhaustion issues in some compilation modes.
  absl::Status scan_subtype_status;
  switch (scan->node_kind()) {
    case RESOLVED_SINGLE_ROW_SCAN:
    case RESOLVED_WITH_REF_SCAN:
      scan->MarkFieldsAccessed();
      // Make sure column ids are unique
      for (const ResolvedColumn& column : scan->column_list()) {
        scan_subtype_status.Update(CheckUniqueColumnId(column));
      }
      break;
    case RESOLVED_TABLE_SCAN:
      scan_subtype_status = ValidateResolvedTableScan(
          scan->GetAs<ResolvedTableScan>(), visible_parameters);
      break;
    case RESOLVED_JOIN_SCAN:
      scan_subtype_status = ValidateResolvedJoinScan(
          scan->GetAs<ResolvedJoinScan>(), visible_parameters);
      break;
    case RESOLVED_ARRAY_SCAN:
      scan_subtype_status = ValidateResolvedArrayScan(
          scan->GetAs<ResolvedArrayScan>(), visible_parameters);
      break;
    case RESOLVED_FILTER_SCAN:
      scan_subtype_status = ValidateResolvedFilterScan(
          scan->GetAs<ResolvedFilterScan>(), visible_parameters);
      break;
    case RESOLVED_AGGREGATE_SCAN:
      scan_subtype_status = ValidateResolvedAggregateScan(
          scan->GetAs<ResolvedAggregateScan>(), visible_parameters);
      break;
    case RESOLVED_ANONYMIZED_AGGREGATE_SCAN:
      scan_subtype_status = ValidateResolvedAnonymizedAggregateScan(
          scan->GetAs<ResolvedAnonymizedAggregateScan>(), visible_parameters);
      break;
    case RESOLVED_SET_OPERATION_SCAN:
      scan_subtype_status = ValidateResolvedSetOperationScan(
          scan->GetAs<ResolvedSetOperationScan>(), visible_parameters);
      break;
    case RESOLVED_PROJECT_SCAN:
      scan_subtype_status = ValidateResolvedProjectScan(
          scan->GetAs<ResolvedProjectScan>(), visible_parameters);
      break;
    case RESOLVED_ORDER_BY_SCAN:
      scan_subtype_status = ValidateResolvedOrderByScan(
          scan->GetAs<ResolvedOrderByScan>(), visible_parameters);
      break;
    case RESOLVED_LIMIT_OFFSET_SCAN:
      scan_subtype_status = ValidateResolvedLimitOffsetScan(
          scan->GetAs<ResolvedLimitOffsetScan>(), visible_parameters);
      break;
    case RESOLVED_WITH_SCAN:
      scan_subtype_status = ValidateResolvedWithScan(
          scan->GetAs<ResolvedWithScan>(), visible_parameters);
      break;
    case RESOLVED_ANALYTIC_SCAN:
      scan_subtype_status = ValidateResolvedAnalyticScan(
          scan->GetAs<ResolvedAnalyticScan>(), visible_parameters);
      break;
    case RESOLVED_SAMPLE_SCAN:
      scan_subtype_status = ValidateResolvedSampleScan(
          scan->GetAs<ResolvedSampleScan>(), visible_parameters);
      break;
    case RESOLVED_TVFSCAN:
      scan_subtype_status = ValidateResolvedTVFScan(
          scan->GetAs<ResolvedTVFScan>(), visible_parameters);
      break;
    case RESOLVED_RELATION_ARGUMENT_SCAN:
      scan_subtype_status = ValidateResolvedRelationArgumentScan(
          scan->GetAs<ResolvedRelationArgumentScan>(), visible_parameters);
      break;
    case RESOLVED_RECURSIVE_SCAN:
      scan_subtype_status = ValidateResolvedRecursiveScan(
          scan->GetAs<ResolvedRecursiveScan>(), visible_parameters);
      break;
    case RESOLVED_RECURSIVE_REF_SCAN:
      scan_subtype_status = ValidateResolvedRecursiveRefScan(
          scan->GetAs<ResolvedRecursiveRefScan>());
      break;
    case RESOLVED_PIVOT_SCAN:
      scan_subtype_status = ValidateResolvedPivotScan(
          scan->GetAs<ResolvedPivotScan>(), visible_parameters);
      break;
    case RESOLVED_UNPIVOT_SCAN:
      scan_subtype_status = ValidateResolvedUnpivotScan(
          scan->GetAs<ResolvedUnpivotScan>(), visible_parameters);
      break;
    case RESOLVED_GROUP_ROWS_SCAN:
      ZETASQL_RETURN_IF_ERROR(
          ValidateGroupRowsScan(scan->GetAs<ResolvedGroupRowsScan>()));
      break;
    default:
      return InternalErrorBuilder()
             << "Unhandled node kind: " << scan->node_kind_string()
             << " in ValidateResolvedScan";
  }
  ZETASQL_RETURN_IF_ERROR(scan_subtype_status);

  if (scan->is_ordered()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScanOrdering(scan));
  }

  ZETASQL_RETURN_IF_ERROR(ValidateHintList(scan->hint_list()));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedRecursiveScan(
    const ResolvedRecursiveScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);
  VALIDATOR_RET_CHECK(
      language_options_.LanguageFeatureEnabled(FEATURE_V_1_3_WITH_RECURSIVE))
      << "Found recursive scan, but WITH RECURSIVE is disabled in language "
         "features";
  VALIDATOR_RET_CHECK_GE(nested_recursive_context_count_, 1)
      << "Recursive scan detected in non-recursive context";
  VALIDATOR_RET_CHECK(scan->non_recursive_term() != nullptr);
  VALIDATOR_RET_CHECK(scan->recursive_term() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedSetOperationItem(
      scan->non_recursive_term(), scan->column_list(), visible_parameters));
  nested_recursive_scans_.push_back(RecursiveScanInfo(scan));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedSetOperationItem(
      scan->recursive_term(), scan->column_list(), visible_parameters));
  VALIDATOR_RET_CHECK_EQ(nested_recursive_scans_.back().scan, scan);
  VALIDATOR_RET_CHECK(nested_recursive_scans_.back().saw_recursive_ref)
      << "Recursive scan generated without a recursive reference in the "
         "recursive term:\n"
      << scan->DebugString();
  nested_recursive_scans_.pop_back();
  scan->op_type();  // Mark field as visited.

  for (const ResolvedColumn& column : scan->column_list()) {
    ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(column));
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedRecursiveRefScan(
    const ResolvedRecursiveRefScan* scan) {
  PushErrorContext push(this, scan);
  VALIDATOR_RET_CHECK(!nested_recursive_scans_.empty())
      << "ResolvedRecursiveRefScan() detected outside a recursive UNION term";
  VALIDATOR_RET_CHECK(!nested_recursive_scans_.back().saw_recursive_ref)
      << "Recursive scan contains multiple recursive refrences in its "
         "recursive term:\n"
      << nested_recursive_scans_.back().scan->DebugString();
  nested_recursive_scans_.back().saw_recursive_ref = true;

  for (const ResolvedColumn& column : scan->column_list()) {
    ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(column));
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedScanOrdering(const ResolvedScan* scan) {
  VALIDATOR_RET_CHECK(nullptr != scan);
  PushErrorContext push(this, scan);

  const ResolvedScan* input_scan = nullptr;

  switch (scan->node_kind()) {
    // OrderByScan can always produce an ordered result.
    case RESOLVED_ORDER_BY_SCAN:
      return absl::OkStatus();

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
      return InternalErrorBuilder() << "Node kind: " << scan->node_kind_string()
                                    << " cannot have is_ordered=true:\n"
                                    << scan->DebugString();
  }

  VALIDATOR_RET_CHECK(input_scan != nullptr);
  if (!input_scan->is_ordered()) {
    return InternalErrorBuilder()
           << "Node has is_ordered=true but its input does not:\n"
           << scan->DebugString();
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateGroupRowsScan(
    const ResolvedGroupRowsScan* scan) {
  VALIDATOR_RET_CHECK_NE(nullptr, scan);
  PushErrorContext push(this, scan);

  // A group rows scan can only appear in a group rows subquery associated with
  // an aggregate function and scan, so the input columns must be set.
  VALIDATOR_RET_CHECK(input_columns_for_group_rows_.has_value());
  VALIDATOR_RET_CHECK(!input_columns_for_group_rows_.value().empty());

  // Validate that columns defined in GROUP_ROWS() are all column references
  // pointing to some columns that are actually available from the input of
  // aggregation scan where WITH GROUP_ROWS was used.
  std::set<ResolvedColumn> refs;
  absl::flat_hash_set<ResolvedColumn> resolved_columns;
  for (const auto& column : scan->input_column_list()) {
    ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(column->column()));
    resolved_columns.insert(column->column());
    VALIDATOR_RET_CHECK(column->expr()->Is<ResolvedColumnRef>());
    const ResolvedColumnRef* ref = column->expr()->GetAs<ResolvedColumnRef>();
    VALIDATOR_RET_CHECK(
        zetasql_base::ContainsKey(input_columns_for_group_rows_.value(), ref->column()));
  }

  // Validate that all columns produced by GROUP_ROWS() scan are in fact defined
  // in it.
  for (const ResolvedColumn& column : scan->column_list()) {
    VALIDATOR_RET_CHECK(resolved_columns.contains(column));
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateHintList(
    const std::vector<std::unique_ptr<const ResolvedOption>>& hint_list) {
  for (const std::unique_ptr<const ResolvedOption>& hint : hint_list) {
    // The value in a Hint must be a constant so we don't pass any visible
    // column names.
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr({}, {}, hint->value()));
    hint->name();  // Mark field as visited.
    hint->qualifier();  // Mark field as visited.
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedTableAndColumnInfo(
    const ResolvedTableAndColumnInfo* table_and_column_info) {
  PushErrorContext push(this, table_and_column_info);
  if (!table_and_column_info->column_index_list().empty()) {
    // Verifying that the column index is in the valid range for the table, and
    // that there are no duplicates.
    std::set<int> column_indexes;
    for (const int column_index : table_and_column_info->column_index_list()) {
      VALIDATOR_RET_CHECK_LT(column_index,
                             table_and_column_info->table()->NumColumns());
      VALIDATOR_RET_CHECK_GE(column_index, 0);
      VALIDATOR_RET_CHECK(column_indexes.insert(column_index).second);
    }
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedTableAndColumnInfoList(
    const std::vector<std::unique_ptr<const ResolvedTableAndColumnInfo>>&
        table_and_column_info_list) {
  std::set<std::string, zetasql_base::CaseLess> table_names;
  for (const std::unique_ptr<const ResolvedTableAndColumnInfo>&
           table_and_column_info : table_and_column_info_list) {
    const std::string table_name = table_and_column_info->table()->FullName();
    VALIDATOR_RET_CHECK(table_names.insert(table_name).second) << table_name;
    ZETASQL_RETURN_IF_ERROR(
        ValidateResolvedTableAndColumnInfo(table_and_column_info.get()));
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateCollateExpr(
    const ResolvedExpr* resolved_collate) {
  PushErrorContext push(this, resolved_collate);
  VALIDATOR_RET_CHECK(resolved_collate != nullptr);
  VALIDATOR_RET_CHECK(resolved_collate->node_kind() == RESOLVED_LITERAL)
      << "COLLATE must be followed by a string literal";
  VALIDATOR_RET_CHECK(resolved_collate->type()->IsString())
      << "COLLATE must be applied to type STRING";
  return absl::OkStatus();
}

absl::Status Validator::ValidateColumnAnnotations(
    const ResolvedColumnAnnotations* annotations) {
  PushErrorContext push(this, annotations);
  VALIDATOR_RET_CHECK(annotations != nullptr);
  if (annotations->collation_name() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateCollateExpr(annotations->collation_name()));
  }
  for (const std::unique_ptr<const ResolvedColumnAnnotations>& child :
       annotations->child_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateColumnAnnotations(child.get()));
  }
  return ValidateHintList(annotations->option_list());
}

absl::Status Validator::ValidateUpdatedAnnotations(
    const ResolvedColumnAnnotations* annotations) {
  PushErrorContext push(this, annotations);
  VALIDATOR_RET_CHECK(annotations != nullptr);
  VALIDATOR_RET_CHECK_EQ(annotations->option_list_size(), 0);
  VALIDATOR_RET_CHECK(!annotations->not_null());
  if (annotations->collation_name() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateCollateExpr(annotations->collation_name()));
  }
  for (const std::unique_ptr<const ResolvedColumnAnnotations>& child :
       annotations->child_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateColumnAnnotations(child.get()));
  }
  return ValidateHintList(annotations->option_list());
}

template <class STMT>
absl::Status Validator::ValidateResolvedDMLStmt(
    const STMT* stmt, const ResolvedColumn* array_element_column,
    std::set<ResolvedColumn>* visible_columns) {
  PushErrorContext push(this, stmt);
  visible_columns->clear();
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->hint_list()));

  if (array_element_column == nullptr) {
    // Non-nested DML.
    VALIDATOR_RET_CHECK(stmt->table_scan() != nullptr);
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(stmt->table_scan(),
                                         {} /* visible_parameters */));

    ZETASQL_RETURN_IF_ERROR(
        AddColumnList(stmt->table_scan()->column_list(), visible_columns));
  } else {
    // Nested DML.
    VALIDATOR_RET_CHECK(stmt->table_scan() == nullptr);
    // The array element is not visible in nested INSERTs.
    if (!std::is_same<STMT, ResolvedInsertStmt>::value) {
      visible_columns->insert(*array_element_column);
    }
  }

  if (stmt->assert_rows_modified() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateArgumentIsInt64Constant(stmt->assert_rows_modified()->rows()));
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedInsertStmt(
    const ResolvedInsertStmt* stmt,
    const std::set<ResolvedColumn>* outer_visible_columns,
    const ResolvedColumn* array_element_column) {
  PushErrorContext push(this, stmt);
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedDMLStmt(stmt, array_element_column,
                                          &visible_columns));

  std::vector<ResolvedColumn> inserted_columns;

  VALIDATOR_RET_CHECK_EQ(array_element_column == nullptr,
                         outer_visible_columns == nullptr);
  if (array_element_column == nullptr) {
    // Non-nested INSERTs.
    VALIDATOR_RET_CHECK_GT(stmt->insert_column_list_size(), 0);
    for (const ResolvedColumn& column : stmt->insert_column_list()) {
      ZETASQL_RETURN_IF_ERROR(CheckColumnIsPresentInColumnSet(
          column, visible_columns));
    }
    inserted_columns = stmt->insert_column_list();
  } else {
    // Nested INSERTs.
    VALIDATOR_RET_CHECK(stmt->table_scan() == nullptr);
    VALIDATOR_RET_CHECK_EQ(stmt->insert_column_list_size(), 0)
        << "insert_column_list not supported on nested INSERTs";
    VALIDATOR_RET_CHECK_EQ(stmt->insert_mode(), ResolvedInsertStmt::OR_ERROR)
        << "insert_mode not supported on nested INSERTs";

    VALIDATOR_RET_CHECK(visible_columns.empty());
    inserted_columns.push_back(*array_element_column);

    visible_columns.insert(outer_visible_columns->begin(),
                           outer_visible_columns->end());
  }

  VALIDATOR_RET_CHECK_EQ(stmt->query() != nullptr,
                         stmt->query_output_column_list_size() > 0);

  if (stmt->query() != nullptr) {
    VALIDATOR_RET_CHECK_EQ(stmt->row_list_size(), 0)
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

    VALIDATOR_RET_CHECK_EQ(query_output_column_list.size(),
                           inserted_columns.size());
    for (int i = 0; i < query_output_column_list.size(); ++i) {
      VALIDATOR_RET_CHECK(query_output_column_list[i].type()->Equals(
          inserted_columns[i].type()));
      VALIDATOR_RET_CHECK(
          zetasql_base::ContainsKey(produced_columns, query_output_column_list[i]))
          << "InsertStmt query does not produce column referenced in "
             "query_output_column_list: "
          << query_output_column_list[i].DebugString();
    }
  } else {
    VALIDATOR_RET_CHECK_GT(stmt->row_list_size(), 0)
        << "INSERT has neither query nor VALUES";
    for (const std::unique_ptr<const ResolvedInsertRow>& insert_row :
         stmt->row_list()) {
      VALIDATOR_RET_CHECK_EQ(insert_row->value_list_size(),
                             inserted_columns.size());
      for (int i = 0; i < insert_row->value_list_size(); ++i) {
        const ResolvedDMLValue* dml_value = insert_row->value_list(i);
        VALIDATOR_RET_CHECK(dml_value->value() != nullptr);
        const ResolvedExpr* expr = dml_value->value();
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
            visible_columns, {} /* visible_parameters */, expr));
        VALIDATOR_RET_CHECK(expr->type()->Equals(inserted_columns[i].type()));
      }
    }
  }

  if (stmt->returning() != nullptr) {
    // Returning clause is only valid on top-level UPDATE.
    VALIDATOR_RET_CHECK_EQ(array_element_column, nullptr);
    ZETASQL_RETURN_IF_ERROR(
        ValidateResolvedReturningClause(stmt->returning(), visible_columns));
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedDeleteStmt(
    const ResolvedDeleteStmt* stmt,
    const std::set<ResolvedColumn>* outer_visible_columns,
    const ResolvedColumn* array_element_column) {
  PushErrorContext push(this, stmt);
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedDMLStmt(stmt, array_element_column,
                                          &visible_columns));
  if (outer_visible_columns != nullptr) {
    visible_columns.insert(outer_visible_columns->begin(),
                           outer_visible_columns->end());
  }

  if (array_element_column == nullptr) {
    // Top-level DELETE.
    VALIDATOR_RET_CHECK(stmt->array_offset_column() == nullptr);
  } else {
    // Nested DELETE.
    VALIDATOR_RET_CHECK(stmt->table_scan() == nullptr);
  }

  if (stmt->array_offset_column() != nullptr) {
    visible_columns.insert(stmt->array_offset_column()->column());
  }

  VALIDATOR_RET_CHECK(stmt->where_expr() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      visible_columns, {} /* visible_parameters */, stmt->where_expr()));
  VALIDATOR_RET_CHECK(stmt->where_expr()->type()->IsBool())
      << "DeleteStmt has WHERE expression with non-BOOL type: "
      << stmt->where_expr()->type()->DebugString();
  if (stmt->returning() != nullptr) {
    // Returning clause is only valid on top-level DELETE.
    VALIDATOR_RET_CHECK_EQ(array_element_column, nullptr);
    ZETASQL_RETURN_IF_ERROR(
        ValidateResolvedReturningClause(stmt->returning(), visible_columns));
  }
  return absl::OkStatus();
}

absl::Status Validator::CheckExprIsPath(const ResolvedExpr* expr,
                                        const ResolvedColumnRef** ref) {
  PushErrorContext push(this, expr);
  switch (expr->node_kind()) {
    case RESOLVED_COLUMN_REF:
      *ref = expr->GetAs<ResolvedColumnRef>();
      return absl::OkStatus();
    case RESOLVED_GET_PROTO_FIELD:
      return CheckExprIsPath(expr->GetAs<ResolvedGetProtoField>()->expr(), ref);
    case RESOLVED_GET_STRUCT_FIELD:
      return CheckExprIsPath(expr->GetAs<ResolvedGetStructField>()->expr(),
                             ref);
    case RESOLVED_GET_JSON_FIELD:
      return CheckExprIsPath(expr->GetAs<ResolvedGetJsonField>()->expr(), ref);
    default:
      VALIDATOR_RET_CHECK_FAIL()
          << "Expression is not a path: " << expr->node_kind_string();
  }
}

absl::Status Validator::ValidateResolvedUpdateStmt(
    const ResolvedUpdateStmt* stmt,
    const std::set<ResolvedColumn>* outer_visible_columns,
    const ResolvedColumn* array_element_column) {
  PushErrorContext push(this, stmt);
  std::set<ResolvedColumn> target_visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedDMLStmt(stmt, array_element_column,
                                          &target_visible_columns));

  if (array_element_column == nullptr) {
    // Top-level UPDATE.
    VALIDATOR_RET_CHECK(stmt->array_offset_column() == nullptr);
    VALIDATOR_RET_CHECK_EQ(stmt->table_scan()->column_index_list().size(),
                           stmt->column_access_list().size());
  } else {
    // Nested UPDATE.
    VALIDATOR_RET_CHECK(stmt->table_scan() == nullptr);
    VALIDATOR_RET_CHECK(stmt->from_scan() == nullptr);
    VALIDATOR_RET_CHECK_EQ(stmt->column_access_list().size(), 0);
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

  VALIDATOR_RET_CHECK_GT(stmt->update_item_list_size(), 0);
  for (const std::unique_ptr<const ResolvedUpdateItem>& item :
       stmt->update_item_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedUpdateItem(
        item.get(), /*allow_nested_statements=*/true,
        /*array_element_column=*/nullptr, target_visible_columns,
        all_visible_columns));
  }

  VALIDATOR_RET_CHECK(stmt->where_expr() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      all_visible_columns, {} /* visible parameters */, stmt->where_expr()));
  VALIDATOR_RET_CHECK(stmt->where_expr()->type()->IsBool())
      << "UpdateStmt has WHERE expression with non-BOOL type: "
      << stmt->where_expr()->type()->DebugString();

  if (stmt->returning() != nullptr) {
    // Returning clause is only valid on top-level UPDATE.
    VALIDATOR_RET_CHECK_EQ(array_element_column, nullptr);
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedReturningClause(stmt->returning(),
                                                    target_visible_columns));
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedUpdateItem(
    const ResolvedUpdateItem* item, bool allow_nested_statements,
    const ResolvedColumn* array_element_column,
    const std::set<ResolvedColumn>& target_visible_columns,
    const std::set<ResolvedColumn>& offset_and_where_visible_columns) {
  PushErrorContext push(this, item);

  VALIDATOR_RET_CHECK(item->target() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      target_visible_columns, {} /* visible_parameters */, item->target()));

  const ResolvedColumnRef* expr_column_ref = nullptr;
  ZETASQL_RETURN_IF_ERROR(CheckExprIsPath(item->target(), &expr_column_ref));
  VALIDATOR_RET_CHECK(expr_column_ref);
  if (array_element_column != nullptr) {
    VALIDATOR_RET_CHECK(
        zetasql_base::ContainsKey(target_visible_columns, *array_element_column));
    VALIDATOR_RET_CHECK(!zetasql_base::ContainsKey(offset_and_where_visible_columns,
                                          *array_element_column));
    VALIDATOR_RET_CHECK_EQ(array_element_column->column_id(),
                           expr_column_ref->column().column_id());
  }

  const Type* target_type = item->target()->type();
  if (item->set_value() != nullptr) {
    // Flat SET {target} = {value} clause.
    VALIDATOR_RET_CHECK(item->element_column() == nullptr);
    VALIDATOR_RET_CHECK_EQ(item->array_update_list_size(), 0);
    VALIDATOR_RET_CHECK_EQ(item->delete_list_size(), 0);
    VALIDATOR_RET_CHECK_EQ(item->update_list_size(), 0);
    VALIDATOR_RET_CHECK_EQ(item->insert_list_size(), 0);

    VALIDATOR_RET_CHECK(item->set_value()->value() != nullptr);
    VALIDATOR_RET_CHECK(
        target_type->Equals(item->set_value()->value()->type()));
  } else {
    // Two Cases:
    // 1) SET {target_array}[<expr>]{optional_remainder} = {value} clause.
    // 2) Nested DML statement.
    VALIDATOR_RET_CHECK(target_type->IsArray());
    VALIDATOR_RET_CHECK(item->element_column() != nullptr);
    const ResolvedColumn& element_column = item->element_column()->column();
    VALIDATOR_RET_CHECK(element_column.IsInitialized());
    VALIDATOR_RET_CHECK(
        element_column.type()->Equals(target_type->AsArray()->element_type()));

    if (item->array_update_list_size() > 0) {
      // Array element modification.
      VALIDATOR_RET_CHECK_EQ(item->delete_list_size(), 0);
      VALIDATOR_RET_CHECK_EQ(item->update_list_size(), 0);
      VALIDATOR_RET_CHECK_EQ(item->insert_list_size(), 0);

      for (const auto& array_update_item : item->array_update_list()) {
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedUpdateArrayItem(
            array_update_item.get(), element_column, target_visible_columns,
            offset_and_where_visible_columns));
      }
    } else {
      // Nested DML statement.
      VALIDATOR_RET_CHECK_GT(item->delete_list_size() +
                                 item->update_list_size() +
                                 item->insert_list_size(),
                             0);
      VALIDATOR_RET_CHECK(allow_nested_statements)
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

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedUpdateArrayItem(
    const ResolvedUpdateArrayItem* item, const ResolvedColumn& element_column,
    const std::set<ResolvedColumn>& target_visible_columns,
    const std::set<ResolvedColumn>& offset_and_where_visible_columns) {
  PushErrorContext push(this, item);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(offset_and_where_visible_columns,
                                       /*visible_parameters=*/{},
                                       item->offset()));
  VALIDATOR_RET_CHECK_EQ(item->offset()->type()->kind(), TYPE_INT64);

  std::set<ResolvedColumn> child_target_visible_columns(target_visible_columns);
  child_target_visible_columns.insert(element_column);
  // We don't allow [] in the target of a nested DML statement, and
  // gen_resolved_ast.py documents that a ResolvedUpdateItem child of a
  // ResolvedUpdateArrayItem node cannot have nested statements.
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedUpdateItem(
      item->update_item(), /*allow_nested_statements=*/false, &element_column,
      child_target_visible_columns, offset_and_where_visible_columns));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedMergeWhen(
    const ResolvedMergeWhen* merge_when,
    const std::set<ResolvedColumn>& all_visible_columns,
    const std::set<ResolvedColumn>& source_visible_columns,
    const std::set<ResolvedColumn>& target_visible_columns) {
  PushErrorContext push(this, merge_when);
  const std::set<ResolvedColumn>* visible_columns = nullptr;
  switch (merge_when->match_type()) {
    // For WHEN MATCHED and WHEN NOT MATCHED BY SOURCE clauses, only UPDATE and
    // DELETE are allowed.
    case ResolvedMergeWhen::MATCHED:
      visible_columns = &all_visible_columns;
      VALIDATOR_RET_CHECK_NE(ResolvedMergeWhen::INSERT,
                             merge_when->action_type());
      break;
    case ResolvedMergeWhen::NOT_MATCHED_BY_SOURCE:
      visible_columns = &target_visible_columns;
      VALIDATOR_RET_CHECK_NE(ResolvedMergeWhen::INSERT,
                             merge_when->action_type());
      break;
    // For WHEN NOT MATCHED BY TARGET merge_when, only INSERT is allowed.
    case ResolvedMergeWhen::NOT_MATCHED_BY_TARGET:
      visible_columns = &source_visible_columns;
      VALIDATOR_RET_CHECK_EQ(ResolvedMergeWhen::INSERT,
                             merge_when->action_type());
      break;
  }

  if (merge_when->match_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(*visible_columns,
                                         {} /* visible_parameters */,
                                         merge_when->match_expr()));
  }

  switch (merge_when->action_type()) {
    case ResolvedMergeWhen::INSERT:
      VALIDATOR_RET_CHECK(!merge_when->insert_column_list().empty());
      VALIDATOR_RET_CHECK_NE(nullptr, merge_when->insert_row());
      VALIDATOR_RET_CHECK_EQ(merge_when->insert_column_list_size(),
                             merge_when->insert_row()->value_list_size());
      VALIDATOR_RET_CHECK(merge_when->update_item_list().empty());

      for (const ResolvedColumn& column : merge_when->insert_column_list()) {
        ZETASQL_RETURN_IF_ERROR(
            CheckColumnIsPresentInColumnSet(column, target_visible_columns));
      }
      for (int i = 0; i < merge_when->insert_row()->value_list_size(); ++i) {
        const ResolvedDMLValue* dml_value =
            merge_when->insert_row()->value_list(i);
        VALIDATOR_RET_CHECK_NE(nullptr, dml_value->value());
        const ResolvedExpr* expr = dml_value->value();
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
            *visible_columns, {} /* visible_parameters */, expr));
        VALIDATOR_RET_CHECK(
            expr->type()->Equals(merge_when->insert_column_list(i).type()));
      }
      break;
    case ResolvedMergeWhen::UPDATE:
      VALIDATOR_RET_CHECK(!merge_when->update_item_list().empty());
      VALIDATOR_RET_CHECK(merge_when->insert_column_list().empty());
      VALIDATOR_RET_CHECK_EQ(nullptr, merge_when->insert_row());

      for (const std::unique_ptr<const ResolvedUpdateItem>& item :
           merge_when->update_item_list()) {
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedUpdateItem(
            item.get(), /*allow_nested_statements=*/false,
            /*array_element_column=*/nullptr, target_visible_columns,
            all_visible_columns));
      }
      break;
    case ResolvedMergeWhen::DELETE:
      VALIDATOR_RET_CHECK(merge_when->update_item_list().empty());
      VALIDATOR_RET_CHECK(merge_when->insert_column_list().empty());
      VALIDATOR_RET_CHECK_EQ(nullptr, merge_when->insert_row());
      break;
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedMergeStmt(
    const ResolvedMergeStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK_NE(nullptr, stmt->table_scan());
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedScan(stmt->table_scan(), {} /* visible_parameters */));
  std::set<ResolvedColumn> target_visible_columns;
  ZETASQL_RETURN_IF_ERROR(AddColumnList(stmt->table_scan()->column_list(),
                                &target_visible_columns));
  VALIDATOR_RET_CHECK_EQ(stmt->table_scan()->column_index_list().size(),
                         stmt->column_access_list().size());

  VALIDATOR_RET_CHECK_NE(nullptr, stmt->from_scan());
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

  VALIDATOR_RET_CHECK(!stmt->when_clause_list().empty());
  for (const auto& when_clause : stmt->when_clause_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedMergeWhen(
        when_clause.get(), all_visible_columns, source_visible_columns,
        target_visible_columns));
  }
  return absl::OkStatus();
}

// Truncate statement is not supported in nested-DML, however, this is
// enforced by the parser.
absl::Status Validator::ValidateResolvedTruncateStmt(
    const ResolvedTruncateStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK_NE(nullptr, stmt->table_scan());
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedScan(stmt->table_scan(), {} /* visible_parameters */));
  std::set<ResolvedColumn> target_visible_columns;
  ZETASQL_RETURN_IF_ERROR(AddColumnList(stmt->table_scan()->column_list(),
                                &target_visible_columns));

  if (stmt->where_expr() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        target_visible_columns, {} /* visible_parameters */,
        stmt->where_expr()));
    VALIDATOR_RET_CHECK(stmt->where_expr()->type()->IsBool())
        << "TruncateStmt has WHERE expression with non-BOOL type: "
        << stmt->where_expr()->type()->DebugString();
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAlterTableSetOptionsStmt(
    const ResolvedAlterTableSetOptionsStmt* stmt) {
  PushErrorContext push(this, stmt);
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAlterPrivilegeRestrictionStmt(
    const ResolvedAlterPrivilegeRestrictionStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK(!stmt->column_privilege_list().empty());
  VALIDATOR_RET_CHECK(absl::AsciiStrToLower(stmt->object_type()) == "table" ||
                      absl::AsciiStrToLower(stmt->object_type()) == "view");
  VALIDATOR_RET_CHECK(!stmt->name_path().empty());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedAlterObjectStmt(stmt));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAlterRowAccessPolicyStmt(
    const ResolvedAlterRowAccessPolicyStmt* stmt) {
  PushErrorContext push(this, stmt);

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedTableScan(stmt->table_scan(),
                                            /*visible_parameters=*/{}));

  VALIDATOR_RET_CHECK(!stmt->name().empty());
  VALIDATOR_RET_CHECK(stmt->table_scan() != nullptr);
  // Check that the last element of the name path, which should be the table
  // name, matches the table scan name.
  VALIDATOR_RET_CHECK(zetasql_base::CaseEqual(
      stmt->name_path().back(), stmt->table_scan()->table()->Name()));
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(stmt->table_scan()->column_list(), &visible_columns));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedAlterObjectStmt(stmt));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAlterAllRowAccessPoliciesStmt(
    const ResolvedAlterAllRowAccessPoliciesStmt* stmt) {
  PushErrorContext push(this, stmt);

  VALIDATOR_RET_CHECK(!stmt->name_path().empty());
  VALIDATOR_RET_CHECK(stmt->table_scan() != nullptr);

  VALIDATOR_RET_CHECK_EQ(1, stmt->alter_action_list_size())
      << "ALTER ALL ROW ACCESS POLICIES expects exactly one revoke action";

  const ResolvedAlterAction* action = stmt->alter_action_list(0);
  VALIDATOR_RET_CHECK_EQ(RESOLVED_REVOKE_FROM_ACTION, action->node_kind())
      << "Currently only REVOKE action is supported in ALTER ALL ROW ACCESS "
         "POLICIES statement";
  auto* revoke_from = action->GetAs<ResolvedRevokeFromAction>();
  if (revoke_from->is_revoke_from_all()) {
    VALIDATOR_RET_CHECK(revoke_from->revokee_expr_list().empty());
  } else {
    VALIDATOR_RET_CHECK(!revoke_from->revokee_expr_list().empty());
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedRenameStmt(
    const ResolvedRenameStmt* stmt) {
  PushErrorContext push(this, stmt);
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedImportStmt(
    const ResolvedImportStmt* stmt) {
  PushErrorContext push(this, stmt);

  if (stmt->import_kind() == ResolvedImportStmt::MODULE) {
    // In the ResolvedAST, both the name_path and alias_path should be
    // set (with implicit aliases added if necessary). But file_path
    // should not be set.
    VALIDATOR_RET_CHECK(!stmt->name_path().empty()) << stmt->DebugString();
    VALIDATOR_RET_CHECK(!stmt->alias_path().empty()) << stmt->DebugString();

    VALIDATOR_RET_CHECK(stmt->file_path().empty()) << stmt->DebugString();
    VALIDATOR_RET_CHECK(stmt->into_alias_path().empty()) << stmt->DebugString();
  } else if (stmt->import_kind() == ResolvedImportStmt::PROTO) {
    // In the ResolvedAST, the file_path should be set while name_path and
    // alias_path should not be set.  The into_alias_path may or may not
    // be set.
    VALIDATOR_RET_CHECK(!stmt->file_path().empty()) << stmt->DebugString();

    VALIDATOR_RET_CHECK(stmt->name_path().empty()) << stmt->DebugString();
    VALIDATOR_RET_CHECK(stmt->alias_path().empty()) << stmt->DebugString();
  }

  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedModuleStmt(
    const ResolvedModuleStmt* stmt) {
  PushErrorContext push(this, stmt);

  VALIDATOR_RET_CHECK(!stmt->name_path().empty());
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAssignmentStmt(
    const ResolvedAssignmentStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK(stmt->target() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      /*visible_columns=*/{}, /*visible_parameters=*/{}, stmt->target()));

  // Verify that the target is an l-value.  Currently, only system variables
  // and fields of system variables are supported.
  const ResolvedNode* node = stmt->target();
  while (node->node_kind() != RESOLVED_SYSTEM_VARIABLE) {
    switch (node->node_kind()) {
      case RESOLVED_GET_STRUCT_FIELD:
        node = node->GetAs<ResolvedGetStructField>()->expr();
        break;
      case RESOLVED_GET_PROTO_FIELD:
        node = node->GetAs<ResolvedGetProtoField>()->expr();
        break;
      default:
        VALIDATOR_RET_CHECK_FAIL()
            << "Expected l-value; got " << node->node_kind_string();
    }
  }

  VALIDATOR_RET_CHECK(stmt->expr() != nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      /*visible_columns=*/{}, /*visible_parameters=*/{}, stmt->expr()));
  VALIDATOR_RET_CHECK(stmt->expr()->type()->Equals(stmt->target()->type()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAnalyzeStmt(
    const ResolvedAnalyzeStmt* stmt) {
  PushErrorContext push(this, stmt);
  if (!stmt->option_list().empty()) {
    ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  }
  if (!stmt->table_and_column_index_list().empty()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedTableAndColumnInfoList(
        stmt->table_and_column_index_list()));
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAssertStmt(
    const ResolvedAssertStmt* stmt) {
  PushErrorContext push(this, stmt);
  VALIDATOR_RET_CHECK(stmt->expression() != nullptr);
  VALIDATOR_RET_CHECK(stmt->expression()->type()->IsBool());
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
      /*visible_columns=*/{}, /*visible_parameters=*/{}, stmt->expression()));
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedFunctionArgument(
    const std::set<ResolvedColumn>& visible_columns,
    const std::set<ResolvedColumn>& visible_parameters,
    const ResolvedFunctionArgument* resolved_arg) {
  PushErrorContext push(this, resolved_arg);
  VALIDATOR_RET_CHECK(resolved_arg != nullptr);
  int fields_set = 0;
  if (resolved_arg->expr() != nullptr) {
    ++fields_set;
    // This is a TVF scalar argument. Validate the input expression,
    // and pass through the 'visible_parameters' because the
    // expression may be correlated.
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(visible_columns, visible_parameters,
                                         resolved_arg->expr()));
  }
  if (resolved_arg->model() != nullptr) {
    ++fields_set;
    resolved_arg->model()->model();  // Mark field as visited.
  }
  if (resolved_arg->connection() != nullptr) {
    ++fields_set;
    resolved_arg->connection()->connection();  // Mark field as visited.
  }
  if (resolved_arg->descriptor_arg() != nullptr) {
    ++fields_set;
    const ResolvedDescriptor* descriptor = resolved_arg->descriptor_arg();
    if (descriptor->descriptor_column_list_size() > 0) {
      VALIDATOR_RET_CHECK_EQ(descriptor->descriptor_column_list_size(),
                             descriptor->descriptor_column_name_list_size());
    }
    // We just mark the column list as visited without validating.
    // The columns that are referencable come from the sibling argument
    // with relation type, which we don't have easy access to.
    descriptor->descriptor_column_list();  // Mark field as visited.

    descriptor->descriptor_column_name_list();  // Mark field as visited.
  }
  if (resolved_arg->scan() != nullptr) {
    ++fields_set;
    // This is a TVF relation argument. Validate the input relation,
    // passing through the 'visible_parameters' because correlation references
    // may be present in the scan.
    VALIDATOR_RET_CHECK_GT(resolved_arg->argument_column_list_size(), 0);
    ZETASQL_RETURN_IF_ERROR(
        ValidateResolvedScan(resolved_arg->scan(), visible_parameters));
    // Verify that columns in <argument_column_list> are actually available
    // in <scan>.
    const std::set<ResolvedColumn> produced_columns(
        resolved_arg->scan()->column_list().begin(),
        resolved_arg->scan()->column_list().end());
    for (const ResolvedColumn& argument_column :
         resolved_arg->argument_column_list()) {
      VALIDATOR_RET_CHECK(zetasql_base::ContainsKey(produced_columns, argument_column))
          << "TVFArgument scan does not produce column referenced in "
             "argument_column_list: "
          << argument_column.DebugString();
    }
  } else {
    VALIDATOR_RET_CHECK_EQ(0, resolved_arg->argument_column_list_size());
  }
  if (resolved_arg->inline_lambda() != nullptr) {
    ++fields_set;
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedInlineLambda(
        visible_columns, visible_parameters, resolved_arg->inline_lambda()));
  }
  VALIDATOR_RET_CHECK_EQ(1, fields_set)
      << "ResolvedTVFArgument should have exactly one field set";
  return absl::OkStatus();
}

absl::Status Validator::ValidateRelationSchemaInResolvedFunctionArgument(
    const TVFRelation& required_input_schema, const TVFRelation& input_relation,
    const ResolvedFunctionArgument* resolved_arg) {
  PushErrorContext push(this, resolved_arg);
  VALIDATOR_RET_CHECK(resolved_arg != nullptr);
  VALIDATOR_RET_CHECK(resolved_arg->scan() != nullptr);
  // Check that the provided columns match position-wise with the required
  // columns. The provided input relation must have exactly the same number
  // of columns as the required input relation, and each (required,
  // provided) column pair must have the same names and types.
  VALIDATOR_RET_CHECK_EQ(input_relation.num_columns(),
                         required_input_schema.num_columns());
  VALIDATOR_RET_CHECK_EQ(input_relation.num_columns(),
                         resolved_arg->argument_column_list_size());
  if (required_input_schema.is_value_table()) {
    VALIDATOR_RET_CHECK_EQ(1, input_relation.num_columns());
    VALIDATOR_RET_CHECK_EQ(1, resolved_arg->argument_column_list_size());
    VALIDATOR_RET_CHECK(input_relation.column(0).type->Equals(
        resolved_arg->argument_column_list(0).type()));
  } else {
    for (int col_idx = 0; col_idx < input_relation.columns().size();
         ++col_idx) {
      VALIDATOR_RET_CHECK(input_relation.column(col_idx).type->Equals(
          required_input_schema.column(col_idx).type));
      VALIDATOR_RET_CHECK(
          zetasql_base::CaseEqual(input_relation.column(col_idx).name,
                                 required_input_schema.column(col_idx).name))
          << "input relation column name: "
          << input_relation.column(col_idx).name
          << ", required relation column name: "
          << required_input_schema.column(col_idx).name;
      VALIDATOR_RET_CHECK(input_relation.column(col_idx).type->Equals(
          resolved_arg->argument_column_list(col_idx).type()));
    }
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAlterObjectStmt(
    const ResolvedAlterObjectStmt* stmt) {
  PushErrorContext push(this, stmt);
  for (const auto& alter_action : stmt->alter_action_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedAlterAction(alter_action.get()));
  }

  // Validate we don't drop or create any column twice.
  std::set<std::string, zetasql_base::CaseLess> new_columns,
      columns_to_drop;

  // Validate there exists either rename column actions only or other alter
  // actions only in this alter statement.
  bool has_rename_column = false;
  bool has_non_rename_column = false;
  for (const auto& action : stmt->alter_action_list()) {
    if (action->node_kind() == RESOLVED_ADD_COLUMN_ACTION) {
      const std::string name =
          action->GetAs<ResolvedAddColumnAction>()->column_definition()->name();
      VALIDATOR_RET_CHECK(new_columns.insert(name).second)
          << "Column added twice: " << name;
    } else if (action->node_kind() == RESOLVED_DROP_COLUMN_ACTION) {
      const std::string name =
          action->GetAs<ResolvedDropColumnAction>()->name();
      VALIDATOR_RET_CHECK(columns_to_drop.insert(name).second)
          << "Column dropped twice: " << name;
      VALIDATOR_RET_CHECK(new_columns.find(name) == new_columns.end())
          << "Newly added column is being dropped: " << name;
    }
    if (action->node_kind() == RESOLVED_RENAME_COLUMN_ACTION) {
      has_rename_column = true;
    } else {
      has_non_rename_column = true;
    }
    VALIDATOR_RET_CHECK(!has_rename_column || !has_non_rename_column)
        << "RENAME COLUMN cannot be used with other alter statement.";
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateAddForeignKeyAction(
    const ResolvedAddConstraintAction* action,
    absl::flat_hash_set<std::string>* constraint_names) {
  ZETASQL_RET_CHECK(action->constraint()->node_kind() == RESOLVED_FOREIGN_KEY &&
            action->table() != nullptr);

  const auto* foreign_key = action->constraint()->GetAs<ResolvedForeignKey>();
  const Table* referencing_table = action->table();
  std::vector<const Type*> column_types;
  column_types.reserve(referencing_table->NumColumns());
  for (int i = 0; i < referencing_table->NumColumns(); i++) {
    column_types.push_back(referencing_table->GetColumn(i)->GetType());
  }
  ZETASQL_RETURN_IF_ERROR(
      ValidateResolvedForeignKey(foreign_key, column_types, constraint_names));

  VALIDATOR_RET_CHECK(foreign_key->referencing_column_list().size() ==
                      foreign_key->referencing_column_offset_list().size());
  for (int i = 0; i < foreign_key->referencing_column_list().size(); i++) {
    absl::string_view name = foreign_key->referencing_column_list(i);
    int offset = foreign_key->referencing_column_offset_list(i);
    const Column* referencing_column = referencing_table->GetColumn(offset);
    VALIDATOR_RET_CHECK(referencing_column != nullptr);
    VALIDATOR_RET_CHECK(referencing_column->Name() == name);
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAlterAction(
    const ResolvedAlterAction* action) {
  PushErrorContext push(this, action);
  absl::flat_hash_set<std::string> seen_constraints;
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
        ZETASQL_ASSIGN_OR_RETURN(TypeParameters full_type_parameters,
                         column_definition->GetFullTypeParameters());
        ZETASQL_RETURN_IF_ERROR(
            column_definition->type()->ValidateResolvedTypeParameters(
                full_type_parameters, language_options_.product_mode()));
      }
      VALIDATOR_RET_CHECK(column_definition->type() != nullptr);
      VALIDATOR_RET_CHECK(!column_definition->name().empty());
      if (column_definition->default_value() != nullptr) {
        VALIDATOR_RET_CHECK_EQ(column_definition->generated_column_info(),
                               nullptr);
        ZETASQL_RETURN_IF_ERROR(ValidateResolvedColumnDefaultValue(
            column_definition->default_value(), column_definition->type()));
      }
    } break;
    case RESOLVED_ADD_TO_RESTRICTEE_LIST_ACTION:
      // Nothing to do
      break;
    case RESOLVED_DROP_COLUMN_ACTION:
      VALIDATOR_RET_CHECK(
          !action->GetAs<ResolvedDropColumnAction>()->name().empty());
      break;
    case RESOLVED_RENAME_COLUMN_ACTION: {
      ZETASQL_RET_CHECK(!action->GetAs<ResolvedRenameColumnAction>()->name().empty());
      ZETASQL_RET_CHECK(
          !action->GetAs<ResolvedRenameColumnAction>()->new_name().empty());
    } break;
    case RESOLVED_GRANT_TO_ACTION:
      VALIDATOR_RET_CHECK(
          action->GetAs<ResolvedGrantToAction>()->grantee_expr_list_size() > 0);
      break;
    case RESOLVED_RESTRICT_TO_ACTION:
      // Nothing to do
      break;
    case RESOLVED_REMOVE_FROM_RESTRICTEE_LIST_ACTION:
      // Nothing to do
      break;
    case RESOLVED_FILTER_USING_ACTION: {
      auto* filter_using = action->GetAs<ResolvedFilterUsingAction>();
      VALIDATOR_RET_CHECK(filter_using->predicate() != nullptr);
      VALIDATOR_RET_CHECK(filter_using->predicate()->type()->IsBool())
          << "FilterUsing AlterRowAccessPolicy action has predicate with "
             "non-BOOL type: "
          << filter_using->predicate()->type()->DebugString();
    } break;
    case RESOLVED_REVOKE_FROM_ACTION: {
      auto* revoke_from = action->GetAs<ResolvedRevokeFromAction>();
      if (revoke_from->is_revoke_from_all()) {
        VALIDATOR_RET_CHECK(revoke_from->revokee_expr_list().empty());
      } else {
        VALIDATOR_RET_CHECK(!revoke_from->revokee_expr_list().empty());
      }
    } break;
    case RESOLVED_RENAME_TO_ACTION:
      VALIDATOR_RET_CHECK(
          !action->GetAs<ResolvedRenameToAction>()->new_path().empty());
      break;
    case RESOLVED_ADD_CONSTRAINT_ACTION: {
      const auto* add_constraint = action->GetAs<ResolvedAddConstraintAction>();
      if (add_constraint->constraint()->node_kind() == RESOLVED_FOREIGN_KEY &&
          add_constraint->table() != nullptr) {
        return ValidateAddForeignKeyAction(add_constraint, &seen_constraints);
      } else if (add_constraint->constraint()->node_kind() ==
                 RESOLVED_PRIMARY_KEY) {
        const auto* primary_key =
            add_constraint->constraint()->GetAs<ResolvedPrimaryKey>();
        std::vector<const Type*> column_types;
        if (add_constraint->table() != nullptr) {
          for (int i = 0; i < add_constraint->table()->NumColumns(); i++) {
            column_types.push_back(
                add_constraint->table()->GetColumn(i)->GetType());
          }
        }
        return ValidateResolvedPrimaryKey(column_types, primary_key,
                                          &seen_constraints);
      }
    } break;
    case RESOLVED_DROP_CONSTRAINT_ACTION:
      VALIDATOR_RET_CHECK(
          !action->GetAs<ResolvedDropConstraintAction>()->name().empty());
      break;
    case RESOLVED_DROP_PRIMARY_KEY_ACTION:
      // Nothing to do
      break;
    case RESOLVED_ALTER_COLUMN_OPTIONS_ACTION:
      VALIDATOR_RET_CHECK(
          !action->GetAs<ResolvedAlterColumnOptionsAction>()->column().empty());
      ZETASQL_RETURN_IF_ERROR(ValidateHintList(
          action->GetAs<ResolvedAlterColumnOptionsAction>()->option_list()));
      break;
    case RESOLVED_ALTER_COLUMN_SET_DATA_TYPE_ACTION: {
      const auto* set_data_type =
          action->GetAs<ResolvedAlterColumnSetDataTypeAction>();
      VALIDATOR_RET_CHECK(!set_data_type->column().empty());
      VALIDATOR_RET_CHECK(set_data_type->updated_type() != nullptr);
      ZETASQL_RETURN_IF_ERROR(
          set_data_type->updated_type()->ValidateResolvedTypeParameters(
              set_data_type->updated_type_parameters(),
              language_options_.product_mode()));
      if (set_data_type->updated_annotations() != nullptr) {
        VALIDATOR_RET_CHECK(set_data_type->updated_annotations());
        // Validate collation annotations if exist.
        ZETASQL_RETURN_IF_ERROR(
            ValidateUpdatedAnnotations(set_data_type->updated_annotations()));
      }
    } break;
    case RESOLVED_ALTER_COLUMN_DROP_NOT_NULL_ACTION:
      VALIDATOR_RET_CHECK(!action->GetAs<ResolvedAlterColumnDropNotNullAction>()
                               ->column()
                               .empty());
      break;
    case RESOLVED_ALTER_COLUMN_SET_DEFAULT_ACTION: {
      const auto* set_default =
          action->GetAs<ResolvedAlterColumnSetDefaultAction>();
      VALIDATOR_RET_CHECK(!set_default->column().empty());
      const ResolvedColumnDefaultValue* default_value =
          set_default->default_value();
      VALIDATOR_RET_CHECK(default_value != nullptr);
      ZETASQL_RETURN_IF_ERROR(
          ValidateResolvedColumnDefaultValue(default_value,
                                             /*column_type=*/nullptr,
                                             /*skip_check_type_match=*/true));
    } break;
    case RESOLVED_ALTER_COLUMN_DROP_DEFAULT_ACTION:
      VALIDATOR_RET_CHECK(!action->GetAs<ResolvedAlterColumnDropDefaultAction>()
                               ->column()
                               .empty());
      break;
    case RESOLVED_SET_COLLATE_CLAUSE:
      VALIDATOR_RET_CHECK(
          action->GetAs<ResolvedSetCollateClause>()->collation_name() !=
          nullptr);
      ZETASQL_RETURN_IF_ERROR(ValidateCollateExpr(
          action->GetAs<ResolvedSetCollateClause>()->collation_name()));
      break;
    default:
      return InternalErrorBuilder()
             << "Unhandled node kind: " << action->node_kind_string()
             << " in ValidateResolvedAlterAction";
  }
  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedColumnDefaultValue(
    const ResolvedColumnDefaultValue* default_value,
    const zetasql::Type* column_type, bool skip_check_type_match) {
  VALIDATOR_RET_CHECK_NE(default_value->expression(), nullptr);
  VALIDATOR_RET_CHECK(!default_value->sql().empty());
  if (!skip_check_type_match) {
    VALIDATOR_RET_CHECK(
        default_value->expression()->type()->Equals(column_type));
  }
  return ValidateResolvedExpr(
      /*visible_columns=*/{}, /*visible_parameters=*/{},
      default_value->expression());
}

absl::Status Validator::ValidateResolvedExecuteImmediateStmt(
    const ResolvedExecuteImmediateStmt* stmt) {
  PushErrorContext push(this, stmt);

  VALIDATOR_RET_CHECK(stmt->sql()->type()->IsString())
      << "SQL must evaluate to type STRING";
  absl::flat_hash_set<std::string> seen;
  for (const std::string& into_identifier : stmt->into_identifier_list()) {
    VALIDATOR_RET_CHECK(
        seen.insert(absl::AsciiStrToLower(into_identifier)).second)
        << "The same parameter cannot be assigned multiple times in an INTO "
           "clause";
  }

  seen.clear();
  bool expecting_names = false;
  for (int i = 0; i < stmt->using_argument_list_size(); i++) {
    const ResolvedExecuteImmediateArgument* expr = stmt->using_argument_list(i);
    bool has_name = !expr->name().empty();
    if (has_name) {
      VALIDATOR_RET_CHECK(
          seen.insert(absl::AsciiStrToLower(expr->name())).second)
          << "The same parameter cannot be assigned multiple times in an INTO "
             "clause";
    }
    if (i == 0) {
      expecting_names = has_name;
    } else {
      VALIDATOR_RET_CHECK(expecting_names == has_name)
          << "Cannot mix named and positional parameters";
    }
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedPivotScan(
    const ResolvedPivotScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  PushErrorContext push(this, scan);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(), visible_parameters));

  std::set<ResolvedColumn> input_column_set;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->input_scan()->column_list(), &input_column_set));

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExprList(input_column_set, visible_parameters,
                                           scan->pivot_expr_list()));

  // ValidateResolvedExprList() does not cover ORDER BY or LIMIT clauses inside
  // of an aggregate function call; they must be validated separately.
  for (const auto& expr : scan->pivot_expr_list()) {
    VALIDATOR_RET_CHECK_EQ(expr->node_kind(), RESOLVED_AGGREGATE_FUNCTION_CALL);
    const ResolvedAggregateFunctionCall* aggregate_function_call =
        expr->GetAs<ResolvedAggregateFunctionCall>();
    ZETASQL_RETURN_IF_ERROR(ValidateOrderByAndLimitClausesOfAggregateFunctionCall(
        input_column_set, visible_parameters, aggregate_function_call));
  }

  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(input_column_set, visible_parameters,
                                       scan->for_expr()));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedExprList(/*visible_columns=*/{},
                                           /*visible_parameters=*/{},
                                           scan->pivot_value_list()));

  for (const ResolvedColumn& column : scan->column_list()) {
    ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(column));
  }

  absl::flat_hash_set<ResolvedColumn> output_columns_created;

  // Validate groupby columns
  absl::flat_hash_set<ResolvedColumn> groupby_input_columns_seen;
  for (const auto& column : scan->group_by_list()) {
    VALIDATOR_RET_CHECK_EQ(column->expr()->node_kind(), RESOLVED_COLUMN_REF)
        << "ComputedColumn in group-by column of pivot scan should be a "
           "ResolvedColumnRef";
    const ResolvedColumn& input_column =
        column->expr()->GetAs<ResolvedColumnRef>()->column();

    // Validate input column
    VALIDATOR_RET_CHECK(
        zetasql_base::InsertIfNotPresent(&groupby_input_columns_seen, input_column))
        << "Duplicate input column " << input_column.DebugString()
        << " in ResolvedPivotScan's group_by_column_list";
    VALIDATOR_RET_CHECK(zetasql_base::ContainsKey(input_column_set, input_column))
        << "ResolvedPivotScan's groupby input column not in input column list: "
        << input_column.DebugString();

    VALIDATOR_RET_CHECK(
        zetasql_base::InsertIfNotPresent(&output_columns_created, column->column()))
        << "Output column appears multiple times in group-by/pivot lists";
  }

  // Validate pivot columns.
  absl::flat_hash_set<int> groupby_column_indexes_seen;
  absl::flat_hash_set<std::pair<int, int>> pivot_column_indexes_seen;
  VALIDATOR_RET_CHECK_LE(scan->pivot_column_list_size(),
                         scan->column_list_size());
  for (const auto& column : scan->pivot_column_list()) {
    VALIDATOR_RET_CHECK_GE(column->pivot_expr_index(), 0);
    VALIDATOR_RET_CHECK_LT(column->pivot_expr_index(),
                           scan->pivot_expr_list_size());
    ZETASQL_RET_CHECK_GE(column->pivot_value_index(), 0);
    VALIDATOR_RET_CHECK_LT(column->pivot_value_index(),
                           scan->pivot_value_list_size());

    VALIDATOR_RET_CHECK(
        zetasql_base::InsertIfNotPresent(&pivot_column_indexes_seen,
                                std::make_pair(column->pivot_expr_index(),
                                               column->pivot_value_index())))
        << "Multiple output columns referring to pivot expr index "
        << column->pivot_expr_index() << ", pivot value index "
        << column->pivot_value_index();

    VALIDATOR_RET_CHECK(
        zetasql_base::InsertIfNotPresent(&output_columns_created, column->column()))
        << "Output column appears multiple times in group-by/pivot lists";
  }

  // Make sure all output columns are either pivot columns or group-by columns.
  for (const ResolvedColumn& column : scan->column_list()) {
    VALIDATOR_RET_CHECK(output_columns_created.contains(column));
  }

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedUnpivotScan(
    const ResolvedUnpivotScan* scan,
    const std::set<ResolvedColumn>& visible_parameters) {
  VALIDATOR_RET_CHECK_NE(scan, nullptr);
  PushErrorContext push(this, scan);
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedScan(scan->input_scan(), visible_parameters));

  // Insert all columns in this set. This includes input_scan columns, unpivot
  // value columns, unpivot label columns and additional columns generated for
  // output like 'SELECT 1'.
  absl::flat_hash_set<ResolvedColumn> all_columns;
  absl::flat_hash_set<ResolvedColumn> input_source_columns;
  absl::flat_hash_set<ResolvedColumn> output_columns;
  ZETASQL_RETURN_IF_ERROR(
      AddColumnList(scan->input_scan()->column_list(), &all_columns));
  input_source_columns.insert(scan->input_scan()->column_list().begin(),
                              scan->input_scan()->column_list().end());
  for (auto& column : scan->column_list()) {
    // Columns from input scan are already checked for uniqueness in the
    // ValidateResolvedScan above.
    if (!all_columns.contains(column)) {
      ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(column));
      all_columns.insert(column);
    }
  }
  for (auto& column : scan->value_column_list()) {
    if (!all_columns.contains(column)) {
      ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(column));
      all_columns.insert(column);
    }
  }
  if (!all_columns.contains(scan->label_column())) {
    ZETASQL_RETURN_IF_ERROR(CheckUniqueColumnId(scan->label_column()));
    all_columns.insert(scan->label_column());
  }

  for (auto& column : scan->column_list()) {
    VALIDATOR_RET_CHECK(all_columns.contains(column))
        << "The output columns must be a subset of input columns and the newly "
           "generated unpivot columns.";
    output_columns.insert(column);
  }

  absl::flat_hash_set<ResolvedColumn> input_columns_in_output;
  for (const std::unique_ptr<const ResolvedComputedColumn>& column :
       scan->projected_input_column_list()) {
    ZETASQL_RET_CHECK_EQ(column->expr()->node_kind(), RESOLVED_COLUMN_REF)
        << "ComputedColumn in projected input columns of unpivot scan should be"
           "a ResolvedColumnRef";
    const ResolvedColumn& input_column =
        column->expr()->GetAs<ResolvedColumnRef>()->column();
    input_columns_in_output.insert(input_column);
    const ResolvedColumn col = column->column();
    ZETASQL_RET_CHECK(col.column_id() > 0);
    ZETASQL_RET_CHECK(input_source_columns.contains(input_column))
        << "The expressions for the columns in the projected_input_column_list "
           "in unpivot output should be a reference to columns in the "
           "input_scan";
  }

  // Check that the order of columns in the output is input columns in order,
  // value column in order, and label column. Not all input columns are present
  // in the output and hence we aren't directly matching the list here.
  std::vector<ResolvedColumn> all_columns_list;
  for (const std::unique_ptr<const ResolvedComputedColumn>& input_col :
       scan->projected_input_column_list()) {
    all_columns_list.push_back(input_col->column());
  }
  all_columns_list.insert(std::end(all_columns_list),
                          std::begin(scan->value_column_list()),
                          std::end(scan->value_column_list()));
  all_columns_list.push_back(scan->label_column());
  int ordered_column_position = 0;
  for (auto& column : scan->column_list()) {
    while (ordered_column_position < all_columns_list.size() &&
           column.column_id() !=
               all_columns_list.at(ordered_column_position).column_id()) {
      ordered_column_position++;
    }
    VALIDATOR_RET_CHECK_LT(ordered_column_position, all_columns_list.size())
        << "The output columns must be in the order of input columns(in the "
           "order that they appear in input_scan), value columns (in the "
           "order that they are defined in UNPIVOT) and then label column.";
  }

  absl::flat_hash_set<ResolvedColumn> input_columns_seen;
  for (const auto& input_column_set : scan->unpivot_arg_list()) {
    VALIDATOR_RET_CHECK_EQ(scan->value_column_list().size(),
                           input_column_set->column_list_size())
        << "Each column group should be of the same size as value_column_list";
    for (const auto& input_column : input_column_set->column_list()) {
      ResolvedColumn input_column_res = input_column->column();
      VALIDATOR_RET_CHECK_EQ(input_column->node_kind(), RESOLVED_COLUMN_REF)
          << "Column in unpivot_input_column_list of unpivot scan should be a "
             "ResolvedColumnRef";
      VALIDATOR_RET_CHECK(
          zetasql_base::InsertIfNotPresent(&input_columns_seen, input_column_res))
          << "Duplicate input column " << input_column_res.DebugString()
          << " in ResolvedUnpivotScan's input_column_list";
      ZETASQL_RET_CHECK(!input_columns_in_output.contains(input_column_res))
          << "Columns from the unpivot_arg_list (inside UNPIVOT IN clause) are "
             "not present in the unpivot output";
    }
  }

  VALIDATOR_RET_CHECK_EQ(scan->label_list_size(),
                         scan->unpivot_arg_list_size());
  for (const auto& val : scan->label_list()) {
    VALIDATOR_RET_CHECK_EQ(val->value().type(), scan->label_column().type());
  }
  // Simply visit this boolean value node for validation purposes.
  scan->include_nulls();

  return absl::OkStatus();
}

absl::Status Validator::ValidateResolvedAuxLoadDataStmt(
      const ResolvedAuxLoadDataStmt* stmt) {
  PushErrorContext push(this, stmt);
  ZETASQL_RETURN_IF_ERROR(ValidateHintList(stmt->option_list()));
  std::set<ResolvedColumn> visible_columns;
  ZETASQL_RETURN_IF_ERROR(ValidateColumnDefinitions(stmt->column_definition_list(),
                                            &visible_columns));
  ZETASQL_RETURN_IF_ERROR(ValidateResolvedWithPartitionColumns(
      stmt->with_partition_columns(), &visible_columns));

  // Add the output columns in case the table schema is empty.
  for (const auto& c : stmt->output_column_list()) {
    visible_columns.insert(c->column());
  }

  // Pseudo columns shouldn't be in output_column_list. But they are still
  // visiblee for CLUSTER BY and PARTITION BY.
  for (const auto& pseudo_column : stmt->pseudo_column_list()) {
    if (!zetasql_base::InsertIfNotPresent(&visible_columns, pseudo_column)) {
      return InternalErrorBuilder()
             << "Column already used: " << pseudo_column.DebugString();
    }
  }
  for (const auto& partition_by_expr : stmt->partition_by_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        visible_columns, {} /* visible_parameters */, partition_by_expr.get()));
  }
  for (const auto& cluster_by_expr : stmt->cluster_by_list()) {
    ZETASQL_RETURN_IF_ERROR(ValidateResolvedExpr(
        visible_columns, {} /* visible_parameters */, cluster_by_expr.get()));
  }
  return ValidateHintList(stmt->from_files_option_list());
}

std::string Validator::RecordContext() {
  if (!context_stack_.empty()) {
    error_context_ = context_stack_.back();
  }
  return "";
}

}  // namespace zetasql
