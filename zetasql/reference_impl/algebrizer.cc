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

#include "zetasql/reference_impl/algebrizer.h"

#include <functional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/compliance/type_helpers.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "zetasql/reference_impl/common.h"
#include "zetasql/reference_impl/function.h"
#include "zetasql/reference_impl/proto_util.h"
#include "zetasql/reference_impl/tuple.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/canonical_errors.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

using zetasql::types::BoolType;
using zetasql::types::Int64Type;

namespace zetasql {

// Returns true if the given aggregate function should ignore NULL values in
// the input arguments.
static bool IgnoresNullArguments(
    const ResolvedNonScalarFunctionCallBase* aggregate_function) {
  static const std::unordered_set<std::string>* const
      kFunctionsNotIgnoreNullSet = new std::unordered_set<std::string>(
          {"array_agg", "any_value", "approx_top_count", "approx_top_sum"});

  switch (aggregate_function->null_handling_modifier()) {
    case ResolvedNonScalarFunctionCallBase::DEFAULT_NULL_HANDLING:
      return !zetasql_base::ContainsKey(*kFunctionsNotIgnoreNullSet,
                               aggregate_function->function()->Name());
    case ResolvedNonScalarFunctionCallBase::RESPECT_NULLS:
      return false;
    case ResolvedNonScalarFunctionCallBase::IGNORE_NULLS:
      return true;
  }
}

static absl::Status CheckHints(
    const std::vector<std::unique_ptr<const ResolvedOption>>& hint_list) {
  for (const auto& hint : hint_list) {
    // Ignore all hints meant for a specific different engine.
    if (!hint->qualifier().empty() && hint->qualifier() != "reference_impl") {
      continue;
    }
    // All hints are currently unsupported in the reference_impl, so we
    // always give an error.
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Unsupported hint: " << hint->qualifier()
           << (hint->qualifier().empty() ? "" : ".") << hint->name();
  }
  return absl::OkStatus();
}

Algebrizer::Algebrizer(const LanguageOptions& language_options,
                       const AlgebrizerOptions& algebrizer_options,
                       TypeFactory* type_factory, Parameters* parameters,
                       ParameterMap* column_map,
                       SystemVariablesAlgebrizerMap* system_variables_map)
    : language_options_(language_options),
      algebrizer_options_(algebrizer_options),
      column_to_variable_(absl::make_unique<ColumnToVariableMapping>(
          absl::make_unique<VariableGenerator>())),
      variable_gen_(column_to_variable_->variable_generator()),
      parameters_(parameters),
      column_map_(column_map),
      system_variables_map_(system_variables_map),
      type_factory_(type_factory),
      next_column_(0) {}

zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeCast(
    const ResolvedCast* cast) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> arg,
                   AlgebrizeExpression(cast->expr()));
  ZETASQL_ASSIGN_OR_RETURN(
      auto null_on_error_exp,
      ConstExpr::Create(Value::Bool(cast->return_null_on_error())));

  std::vector<std::unique_ptr<ValueExpr>> args;
  args.push_back(std::move(arg));
  args.push_back(std::move(null_on_error_exp));

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ValueExpr> function_call,
      BuiltinScalarFunction::CreateCall(FunctionKind::kCast, language_options_,
                                        cast->type(), std::move(args)));
  return function_call;
}

zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeFunctionCall(
    const ResolvedFunctionCall* function_call) {
  int num_arguments = function_call->argument_list_size();
  std::vector<std::unique_ptr<ValueExpr>> arguments;
  for (int i = 0; i < num_arguments; ++i) {
    const ResolvedExpr* argument_expr = function_call->argument_list(i);
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> argument,
                     AlgebrizeExpression(argument_expr));
    arguments.push_back(std::move(argument));
  }
  const std::string& name = function_call->function()->FullName(false);
  const ResolvedFunctionCallBase::ErrorMode& error_mode =
      function_call->error_mode();

  if (error_mode == ResolvedFunctionCallBase::SAFE_ERROR_MODE &&
      !function_call->function()->SupportsSafeErrorMode()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Function " << name << "does not support SAFE error mode";
  }

  // User-defined functions.
  if (!function_call->function()->IsZetaSQLBuiltin()) {
    auto callback = function_call->function()->GetFunctionEvaluatorFactory();
    if (callback == nullptr) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "User-defined function " << name << " has no evaluator. "
             << "Use FunctionOptions to supply one.";
    }
    auto status_or_evaluator = callback(function_call->signature());
    ZETASQL_RETURN_IF_ERROR(status_or_evaluator.status());
    auto evaluator = status_or_evaluator.value();
    if (evaluator == nullptr) {
      return ::zetasql_base::InternalErrorBuilder()
             << "NULL evaluator returned for user-defined function " << name;
    }
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> function_call,
                     ScalarFunctionCallExpr::Create(
                         absl::make_unique<UserDefinedScalarFunction>(
                             evaluator, function_call->type(), name),
                         std::move(arguments), error_mode));
    return function_call;
  }

  // Built-in functions.
  FunctionKind kind;
  if (name == "$not_equal") {
    return AlgebrizeNotEqual(std::move(arguments));
  } else if (name == "$greater") {
    kind = FunctionKind::kLess;
    std::swap(arguments[0], arguments[1]);
  } else if (name == "$greater_or_equal") {
    kind = FunctionKind::kLessOrEqual;
    std::swap(arguments[0], arguments[1]);
  } else if (name == "if") {
    return AlgebrizeIf(function_call->type(), std::move(arguments));
  } else if (name == "ifnull") {
    return AlgebrizeIfNull(function_call->type(), std::move(arguments));
  } else if (name == "nullif") {
    return AlgebrizeNullIf(function_call->type(), std::move(arguments));
  } else if (name == "coalesce") {
    return AlgebrizeCoalesce(function_call->type(), std::move(arguments));
  } else if (name == "$case_no_value") {
    return AlgebrizeCaseNoValue(function_call->type(), std::move(arguments));
  } else if (name == "$case_with_value") {
    return AlgebrizeCaseWithValue(function_call->type(), std::move(arguments));
  } else if (name == "$in") {
    return AlgebrizeIn(function_call->type(), std::move(arguments));
  } else if (name == "$between") {
    return AlgebrizeBetween(function_call->type(), std::move(arguments));
  } else if (name == "$make_array") {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> new_array_expr,
                     NewArrayExpr::Create(function_call->type()->AsArray(),
                                          std::move(arguments)));
    return new_array_expr;
  } else if (name == "$in_array") {
    return AlgebrizeInArray(std::move(arguments[0]), std::move(arguments[1]));
  } else {
    zetasql_base::StatusOr<FunctionKind> status_or_kind =
        BuiltinFunctionCatalog::GetKindByName(name);
    if (!status_or_kind.ok()) {
      return status_or_kind.status();
    }
    kind = status_or_kind.value();
  }
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> function_call_expr,
                   BuiltinScalarFunction::CreateCall(
                       kind, language_options_, function_call->type(),
                       std::move(arguments), error_mode));
  return function_call_expr;
}

// CASE WHEN w1 THEN t1 ELSE e END =
//     IfExpr(w1, t1, e)
// CASE WHEN w1 THEN t1 WHEN w2 THEN t2 ELSE e END =
//     IfExpr(w1, t1, IfExpr(w2, t2, e))
// etc.
zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeCaseNoValue(
    const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args) {
  ZETASQL_RET_CHECK_LE(2, args.size());
  bool has_else = args.size() % 2 == 1;
  int i = args.size() - 1;
  ZETASQL_ASSIGN_OR_RETURN(auto null_expr, ConstExpr::Create(Value::Null(output_type)));
  std::unique_ptr<ValueExpr> result(has_else ? std::move(args[i--])
                                             : std::move(null_expr));
  while (i > 0) {
    std::unique_ptr<ValueExpr> then = std::move(args[i--]);
    std::unique_ptr<ValueExpr> when = std::move(args[i--]);
    ZETASQL_ASSIGN_OR_RETURN(result, IfExpr::Create(std::move(when), std::move(then),
                                            std::move(result)));
  }
  ZETASQL_RET_CHECK_EQ(-1, i);
  return result;
}

// CASE v WHEN w1 THEN t1 ELSE e END =
//     If(v=w1, t1, e)
// CASE v WHEN w1 THEN t1 WHEN w2 THEN t2 ELSE e END =
//     LetExpr(x:=v, IfExpr(x=w1, t1, IfExpr(x=w2, t2, e)))
// etc.
zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeCaseWithValue(
    const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args) {
  ZETASQL_RET_CHECK_LE(2, args.size());
  bool has_else = args.size() % 2 == 0;
  int i = args.size() - 1;
  ZETASQL_ASSIGN_OR_RETURN(auto null_expr, ConstExpr::Create(Value::Null(output_type)));
  std::unique_ptr<ValueExpr> result(has_else ? std::move(args[i--])
                                             : std::move(null_expr));
  // Empty x means we don't need LetExpr, i.e., we have a single WHEN/THEN.
  const VariableId x =
      args.size() > 4 ? variable_gen_->GetNewVariableName("x") : VariableId();
  while (i > 0) {
    std::unique_ptr<ValueExpr> then = std::move(args[i--]);
    std::unique_ptr<ValueExpr> when = std::move(args[i--]);

    std::unique_ptr<ValueExpr> value;
    if (x.is_valid()) {
      ZETASQL_ASSIGN_OR_RETURN(value, DerefExpr::Create(x, args[0]->output_type()));
    } else {
      value = std::move(args[0]);
    }

    std::vector<std::unique_ptr<ValueExpr>> cond_args;
    cond_args.push_back(std::move(value));
    cond_args.push_back(std::move(when));

    ZETASQL_ASSIGN_OR_RETURN(auto cond, BuiltinScalarFunction::CreateCall(
                                    FunctionKind::kEqual, language_options_,
                                    BoolType(), std::move(cond_args)));
    ZETASQL_ASSIGN_OR_RETURN(result, IfExpr::Create(std::move(cond), std::move(then),
                                            std::move(result)));
  }
  ZETASQL_RET_CHECK_EQ(0, i);
  if (x.is_valid()) {
    std::vector<std::unique_ptr<ExprArg>> expr_args;
    expr_args.push_back(absl::make_unique<ExprArg>(x, std::move(args[0])));
    ZETASQL_ASSIGN_OR_RETURN(result,
                     LetExpr::Create(std::move(expr_args), std::move(result)));
  }
  return result;
}

// a != b  ->  !(a = b)
zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeNotEqual(
    std::vector<std::unique_ptr<ValueExpr>> args) {
  ZETASQL_RET_CHECK_EQ(2, args.size());
  ZETASQL_ASSIGN_OR_RETURN(auto equal, BuiltinScalarFunction::CreateCall(
                                   FunctionKind::kEqual, language_options_,
                                   BoolType(), std::move(args)));

  std::vector<std::unique_ptr<ValueExpr>> not_args;
  not_args.push_back(std::move(equal));

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<ValueExpr> function_call,
      BuiltinScalarFunction::CreateCall(FunctionKind::kNot, language_options_,
                                        BoolType(), std::move(not_args)));
  return function_call;
}

// If(v0, v1, v2) = IfExpr(v0, v1, v2)
zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeIf(
    const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args) {
  ZETASQL_RET_CHECK_EQ(3, args.size());
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> if_expr,
                   IfExpr::Create(std::move(args[0]), std::move(args[1]),
                                  std::move(args[2])));
  return if_expr;
}

// IfNull(v0, v1) = LetExpr(x:=v0, IfExpr(IsNull(x), v1, x))
zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeIfNull(
    const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args) {
  ZETASQL_RET_CHECK_EQ(2, args.size());
  const VariableId x = variable_gen_->GetNewVariableName("x");

  ZETASQL_ASSIGN_OR_RETURN(auto deref_x, DerefExpr::Create(x, output_type));

  std::vector<std::unique_ptr<ValueExpr>> is_null_args;
  is_null_args.push_back(std::move(deref_x));

  ZETASQL_ASSIGN_OR_RETURN(auto is_null, BuiltinScalarFunction::CreateCall(
                                     FunctionKind::kIsNull, language_options_,
                                     BoolType(), std::move(is_null_args)));

  ZETASQL_ASSIGN_OR_RETURN(auto deref_x_again, DerefExpr::Create(x, output_type));

  ZETASQL_ASSIGN_OR_RETURN(auto if_op,
                   IfExpr::Create(std::move(is_null), std::move(args[1]),
                                  std::move(deref_x_again)));

  std::vector<std::unique_ptr<ExprArg>> let_assign;
  let_assign.push_back(absl::make_unique<ExprArg>(x, std::move(args[0])));

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> let_expr,
                   LetExpr::Create(std::move(let_assign), std::move(if_op)));
  return let_expr;
}

// NullIf(v0, v1) = LetExpr(x:=v0, IfExpr(x=v1, NULL, x))
zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeNullIf(
    const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args) {
  ZETASQL_RET_CHECK_EQ(2, args.size());
  const VariableId x = variable_gen_->GetNewVariableName("x");

  ZETASQL_ASSIGN_OR_RETURN(auto deref_x, DerefExpr::Create(x, output_type));

  std::vector<std::unique_ptr<ValueExpr>> equal_args;
  equal_args.push_back(std::move(deref_x));
  equal_args.push_back(std::move(args[1]));

  ZETASQL_ASSIGN_OR_RETURN(auto equal, BuiltinScalarFunction::CreateCall(
                                   FunctionKind::kEqual, language_options_,
                                   BoolType(), std::move(equal_args)));
  ZETASQL_ASSIGN_OR_RETURN(auto null_constant,
                   ConstExpr::Create(Value::Null(output_type)));
  ZETASQL_ASSIGN_OR_RETURN(auto deref_x_again, DerefExpr::Create(x, output_type));
  ZETASQL_ASSIGN_OR_RETURN(auto if_op,
                   IfExpr::Create(std::move(equal), std::move(null_constant),
                                  std::move(deref_x_again)));

  std::vector<std::unique_ptr<ExprArg>> let_assign;
  let_assign.push_back(absl::make_unique<ExprArg>(x, std::move(args[0])));

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> let_expr,
                   LetExpr::Create(std::move(let_assign), std::move(if_op)));
  return let_expr;
}

// Coalesce(v1, v2) = LetExpr(x:=v1, IfExpr(IsNull(x), v2, x))
// Coalesce(v1, v2, ...) = Coalesce(v1, Coalesce(v2, ...))
zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeCoalesce(
    const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args) {
  ZETASQL_RET_CHECK_LE(1, args.size());
  int i = args.size() - 1;
  std::unique_ptr<ValueExpr> result = std::move(args[i--]);
  while (i >= 0) {
    const VariableId x = variable_gen_->GetNewVariableName("x");

    ZETASQL_ASSIGN_OR_RETURN(auto deref_x, DerefExpr::Create(x, output_type));

    std::vector<std::unique_ptr<ValueExpr>> is_null_args;
    is_null_args.push_back(std::move(deref_x));

    ZETASQL_ASSIGN_OR_RETURN(auto is_null, BuiltinScalarFunction::CreateCall(
                                       FunctionKind::kIsNull, language_options_,
                                       BoolType(), std::move(is_null_args)));
    ZETASQL_ASSIGN_OR_RETURN(auto deref_x_again, DerefExpr::Create(x, output_type));
    ZETASQL_ASSIGN_OR_RETURN(auto if_op,
                     IfExpr::Create(std::move(is_null), std::move(result),
                                    std::move(deref_x_again)));

    std::vector<std::unique_ptr<ExprArg>> let_assign;
    let_assign.push_back(absl::make_unique<ExprArg>(x, std::move(args[i--])));

    ZETASQL_ASSIGN_OR_RETURN(result,
                     LetExpr::Create(std::move(let_assign), std::move(if_op)));
  }
  ZETASQL_RET_CHECK_EQ(-1, i);
  return result;
}

// In(v, v1, v2, ...) = LetExpr(x:=v, Or(x=v1, x=v2, ...))
zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeIn(
    const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args) {
  ZETASQL_RET_CHECK_GE(args.size(), 2);
  const VariableId x = variable_gen_->GetNewVariableName("x");
  std::vector<std::unique_ptr<ValueExpr>> or_args;
  for (int i = 0; i < args.size() - 1; ++i) {
    ZETASQL_ASSIGN_OR_RETURN(auto deref_x, DerefExpr::Create(x, output_type));

    std::vector<std::unique_ptr<ValueExpr>> or_arg_args;
    or_arg_args.push_back(std::move(deref_x));
    or_arg_args.push_back(std::move(args[i + 1]));

    ZETASQL_ASSIGN_OR_RETURN(auto or_arg, BuiltinScalarFunction::CreateCall(
                                      FunctionKind::kEqual, language_options_,
                                      BoolType(), std::move(or_arg_args)));
    or_args.push_back(std::move(or_arg));
  }
  ZETASQL_ASSIGN_OR_RETURN(auto or_op, BuiltinScalarFunction::CreateCall(
                                   FunctionKind::kOr, language_options_,
                                   BoolType(), std::move(or_args)));

  std::vector<std::unique_ptr<ExprArg>> let_assign;
  let_assign.push_back(absl::make_unique<ExprArg>(x, std::move(args[0])));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> let_expr,
                   LetExpr::Create(std::move(let_assign), std::move(or_op)));
  return let_expr;
}

// Between(v, min, max) = LetExpr(x:=v, And(min<=x, x<=max))
zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeBetween(
    const Type* output_type, std::vector<std::unique_ptr<ValueExpr>> args) {
  ZETASQL_RET_CHECK_EQ(args.size(), 3);
  const VariableId x = variable_gen_->GetNewVariableName("x");

  ZETASQL_ASSIGN_OR_RETURN(auto deref_x, DerefExpr::Create(x, output_type));
  std::vector<std::unique_ptr<ValueExpr>> first_le_args;
  first_le_args.push_back(std::move(args[1]));
  first_le_args.push_back(std::move(deref_x));

  ZETASQL_ASSIGN_OR_RETURN(auto first_le,
                   BuiltinScalarFunction::CreateCall(
                       FunctionKind::kLessOrEqual, language_options_,
                       BoolType(), std::move(first_le_args)));

  ZETASQL_ASSIGN_OR_RETURN(auto deref_x_again, DerefExpr::Create(x, output_type));
  std::vector<std::unique_ptr<ValueExpr>> second_le_args;
  second_le_args.push_back(std::move(deref_x_again));
  second_le_args.push_back(std::move(args[2]));

  ZETASQL_ASSIGN_OR_RETURN(auto second_le,
                   BuiltinScalarFunction::CreateCall(
                       FunctionKind::kLessOrEqual, language_options_,
                       BoolType(), std::move(second_le_args)));

  std::vector<std::unique_ptr<ValueExpr>> and_args;
  and_args.push_back(std::move(first_le));
  and_args.push_back(std::move(second_le));

  ZETASQL_ASSIGN_OR_RETURN(auto and_op, BuiltinScalarFunction::CreateCall(
                                    FunctionKind::kAnd, language_options_,
                                    BoolType(), std::move(and_args)));

  std::vector<std::unique_ptr<ExprArg>> let_assign;
  let_assign.push_back(absl::make_unique<ExprArg>(x, std::move(args[0])));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> let_expr,
                   LetExpr::Create(std::move(let_assign), std::move(and_op)));
  return let_expr;
}

zetasql_base::StatusOr<std::unique_ptr<AggregateArg>>
Algebrizer::AlgebrizeAggregateFn(
    const VariableId& variable,
    const ResolvedExpr* expr) {
  ZETASQL_RET_CHECK(expr->node_kind() == RESOLVED_AGGREGATE_FUNCTION_CALL ||
            expr->node_kind() == RESOLVED_ANALYTIC_FUNCTION_CALL)
      << expr->node_kind_string();
  const ResolvedNonScalarFunctionCallBase* aggregate_function =
      expr->GetAs<ResolvedNonScalarFunctionCallBase>();
  const std::string name = aggregate_function->function()->FullName(false);
  std::vector<std::unique_ptr<ValueExpr>> arguments;
  for (int i = 0; i < aggregate_function->argument_list_size(); ++i) {
    const ResolvedExpr* argument_expr = aggregate_function->argument_list(i);
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> argument,
                     AlgebrizeExpression(argument_expr));
    arguments.push_back(std::move(argument));
  }

  const Type* type = aggregate_function->type();
  if (!aggregate_function->function()->IsZetaSQLBuiltin()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
        << "User-defined aggregate functions are unsupported: " << name;
  }

  FunctionKind kind;
  int num_input_fields;
  if (name == "$count_star") {
    kind = FunctionKind::kCount;
    num_input_fields = 0;
  } else {
    ZETASQL_ASSIGN_OR_RETURN(kind, BuiltinFunctionCatalog::GetKindByName(name));
    switch (kind) {
      case FunctionKind::kApproxTopSum:
      case FunctionKind::kCorr:
      case FunctionKind::kCovarPop:
      case FunctionKind::kCovarSamp:
        num_input_fields = 2;
        break;
      default:
        num_input_fields = 1;
        break;
    }
  }
  ZETASQL_RET_CHECK_LE(num_input_fields, arguments.size()) << name;

  const Type* input_type;
  switch (num_input_fields) {
    case 0:
      input_type = types::EmptyStructType();
      break;
    case 1:
      input_type = arguments[0]->output_type();
      break;
    default: {
      std::vector<StructType::StructField> fields;
      fields.reserve(num_input_fields);
      for (int i = 0; i < num_input_fields; ++i) {
        fields.push_back({"", arguments[i]->output_type()});
      }
      const StructType* struct_type;
      ZETASQL_RET_CHECK_OK(type_factory_->MakeStructType(fields, &struct_type));
      input_type = struct_type;
      break;
    }
  }

  std::unique_ptr<ValueExpr> having_expr;
  AggregateArg::HavingModifierKind having_kind = AggregateArg::kHavingNone;
  std::vector<std::unique_ptr<KeyArg>> order_keys;
  std::unique_ptr<ValueExpr> limit;
  if (expr->node_kind() == RESOLVED_AGGREGATE_FUNCTION_CALL) {
    const ResolvedAggregateFunctionCall* resolved_aggregate_func =
        expr->GetAs<ResolvedAggregateFunctionCall>();

    if (resolved_aggregate_func->having_modifier() != nullptr &&
        resolved_aggregate_func->having_modifier()->having_expr() != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(
          having_expr,
          AlgebrizeExpression(
              resolved_aggregate_func->having_modifier()->having_expr()));
      if (resolved_aggregate_func->having_modifier()->kind() ==
          ResolvedAggregateHavingModifier::MAX) {
        having_kind = AggregateArg::kHavingMax;
      } else {
        having_kind = AggregateArg::kHavingMin;
      }
    }

    if (!resolved_aggregate_func->order_by_item_list().empty()) {
      absl::flat_hash_map<int, VariableId> column_to_id_map;
      // It is safe to remove correlated column references, because they
      // are constant and do not affect the order of the input to
      // the aggregate function.
      ZETASQL_RETURN_IF_ERROR(AlgebrizeOrderByItems(
          true /* drop_correlated_columns */, false /* create_new_ids */,
          resolved_aggregate_func->order_by_item_list(),
          &column_to_id_map, &order_keys));
    }
    if (resolved_aggregate_func->limit() != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(limit,
                       AlgebrizeExpression(resolved_aggregate_func->limit()));
    }
  }

  std::unique_ptr<BuiltinAggregateFunction> function;
  switch (kind) {
    case FunctionKind::kCorr:
    case FunctionKind::kCovarPop:
    case FunctionKind::kCovarSamp:
      function = absl::make_unique<BinaryStatFunction>(kind, type, input_type);
      break;
    default:
      function = absl::make_unique<BuiltinAggregateFunction>(
          kind, type, num_input_fields, input_type,
          IgnoresNullArguments(aggregate_function));
      break;
  }

  const AggregateArg::Distinctness distinctness =
      (aggregate_function->distinct() ||
       (kind == FunctionKind::kApproxCountDistinct))
          ? AggregateArg::kDistinct
          : AggregateArg::kAll;
  return AggregateArg::Create(
      variable, std::move(function), std::move(arguments), distinctness,
      std::move(having_expr), having_kind, std::move(order_keys),
      std::move(limit), aggregate_function->error_mode());
}

zetasql_base::StatusOr<std::unique_ptr<NewStructExpr>> Algebrizer::MakeStruct(
    const ResolvedMakeStruct* make_struct) {
  DCHECK(make_struct->type()->IsStruct());
  const StructType* struct_type = make_struct->type()->AsStruct();

  // Build a list of arguments.
  std::vector<std::unique_ptr<ExprArg>> arguments;
  DCHECK_EQ(struct_type->num_fields(), make_struct->field_list_size());
  for (int i = 0; i < struct_type->num_fields(); ++i) {
    const ResolvedExpr* field_expr = make_struct->field_list()[i].get();
    DCHECK(field_expr->type()->Equals(struct_type->field(i).type));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> algebrized_field_expr,
                     AlgebrizeExpression(field_expr));
    // Record the field value.
    arguments.push_back(
        absl::make_unique<ExprArg>(std::move(algebrized_field_expr)));
  }
  // Build the row value.
  return NewStructExpr::Create(struct_type, std::move(arguments));
}

zetasql_base::StatusOr<std::unique_ptr<FieldValueExpr>>
Algebrizer::AlgebrizeGetStructField(
    const ResolvedGetStructField* get_struct_field) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> value,
                   AlgebrizeExpression(get_struct_field->expr()));
  return FieldValueExpr::Create(get_struct_field->field_idx(),
                                std::move(value));
}

static ProtoFieldAccessInfo CreateProtoFieldAccessInfo(
    const ResolvedGetProtoField& get_proto_field) {
  ProtoFieldAccessInfo info;
  info.field_info.descriptor = get_proto_field.field_descriptor();
  info.field_info.format = get_proto_field.format();
  info.field_info.type = get_proto_field.type();
  info.field_info.get_has_bit = get_proto_field.get_has_bit();
  info.field_info.default_value = get_proto_field.default_value();

  info.return_default_value_when_unset =
      get_proto_field.return_default_value_when_unset();

  return info;
}

zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeGetProtoField(
    const ResolvedGetProtoField* get_proto_field) {
  // Represent 'get_proto_field' as 'base_expression'.'path[0]'.'path[1]'. ...
  std::vector<absl::variant<const ResolvedGetProtoField*,
                            const ResolvedGetStructField*>>
      path;
  const ResolvedExpr* base_expression = get_proto_field;
  while (base_expression->node_kind() == RESOLVED_GET_PROTO_FIELD ||
         base_expression->node_kind() == RESOLVED_GET_STRUCT_FIELD) {
    // Build 'path' in reverse order as we traverse down 'base_expression'.
    if (base_expression->node_kind() == RESOLVED_GET_PROTO_FIELD) {
      const ResolvedGetProtoField* get_field =
          base_expression->GetAs<ResolvedGetProtoField>();
      path.push_back(get_field);
      base_expression = get_field->expr();
    } else {
      ZETASQL_RET_CHECK_EQ(base_expression->node_kind(), RESOLVED_GET_STRUCT_FIELD);
      const ResolvedGetStructField* get_field =
          base_expression->GetAs<ResolvedGetStructField>();
      path.push_back(get_field);
      base_expression = get_field->expr();
    }
  }
  // Put 'path' in the correct order.
  std::reverse(path.begin(), path.end());

  const ResolvedNodeKind node_kind = base_expression->node_kind();
  if (algebrizer_options_.consolidate_proto_field_accesses) {
    switch (node_kind) {
      case RESOLVED_COLUMN_REF:
        // In this case we can optimize by caching. See GetProtoFieldExpr in
        // operator.h for details
      case RESOLVED_PARAMETER:
        // Another case where we can optimize.
      case RESOLVED_EXPRESSION_COLUMN:
        // Essentially the same thing as a parameter.        .
        return AlgebrizeGetProtoFieldOfPath(base_expression, path);
      default:
        break;
    }
  }

  // No optimization. Just produce a chain of GetProtoFieldExpr nodes on top of
  // 'base_expression.<struct_fields>' with no opportunity to share state with
  // other chains. In particular, this means that for a query like: SELECT
  // proto.repeated_field[0].field, proto.repeated_field[0].field, we will only
  // extract proto.repeated_field once, but we will extract the "[0].field"
  // portion twice.
  //
  // To do this, first we write the field path as
  // base_expression.<struct_fields>.<proto_fields>, and then we set
  // 'base_expression' to point to base_expression.<struct_fields>. Then we
  // handle <proto_fields> below.
  std::vector<const ResolvedGetProtoField*> proto_field_path;
  proto_field_path.reserve(path.size());
  bool seen_get_proto_field = false;
  for (const auto& get_field : path) {
    if (seen_get_proto_field) {
      ZETASQL_RET_CHECK(
          absl::holds_alternative<const ResolvedGetProtoField*>(get_field));
      proto_field_path.push_back(
          absl::get<const ResolvedGetProtoField*>(get_field));
    } else if (absl::holds_alternative<const ResolvedGetProtoField*>(
                   get_field)) {
      seen_get_proto_field = true;
      proto_field_path.push_back(
          absl::get<const ResolvedGetProtoField*>(get_field));
    } else {
      ZETASQL_RET_CHECK(
          absl::holds_alternative<const ResolvedGetStructField*>(get_field));
      base_expression = absl::get<const ResolvedGetStructField*>(get_field);
    }
  }
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> base_value_expr,
                   AlgebrizeExpression(base_expression));

  std::unique_ptr<GetProtoFieldExpr> last_get_proto_field_expr;
  bool first = true;
  for (const ResolvedGetProtoField* get_proto_field : proto_field_path) {
    ZETASQL_ASSIGN_OR_RETURN(ProtoFieldRegistry * registry,
                     AddProtoFieldRegistry(/*id=*/absl::nullopt));

    ZETASQL_ASSIGN_OR_RETURN(
        const ProtoFieldReader* field_reader,
        AddProtoFieldReader(
            /*id=*/absl::nullopt, CreateProtoFieldAccessInfo(*get_proto_field),
            registry));

    ZETASQL_ASSIGN_OR_RETURN(
        last_get_proto_field_expr,
        GetProtoFieldExpr::Create(first ? std::move(base_value_expr)
                                        : std::move(last_get_proto_field_expr),
                                  field_reader));
    first = false;
  }
  return std::move(last_get_proto_field_expr);
}

zetasql_base::StatusOr<std::unique_ptr<ValueExpr>>
Algebrizer::AlgebrizeGetProtoFieldOfPath(
    const ResolvedExpr* column_or_param_expr,
    const std::vector<absl::variant<const ResolvedGetProtoField*,
                                    const ResolvedGetStructField*>>& path) {
  SharedProtoFieldPath column_and_field_path;
  switch (column_or_param_expr->node_kind()) {
    case RESOLVED_COLUMN_REF:
      column_and_field_path.column_or_param = ColumnOrParameter(
          column_or_param_expr->GetAs<ResolvedColumnRef>()->column());
      break;
    case RESOLVED_PARAMETER:
      column_and_field_path.column_or_param =
          ColumnOrParameter(*column_or_param_expr->GetAs<ResolvedParameter>());
      break;
    case RESOLVED_EXPRESSION_COLUMN:
      column_and_field_path.column_or_param = ColumnOrParameter(
          *column_or_param_expr->GetAs<ResolvedExpressionColumn>());
      break;
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unexpected node kind: "
                       << ResolvedNodeKind_Name(
                              column_or_param_expr->node_kind())
                       << " in AlgebrizeGetProtoFieldOfPath()";
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> base_expr,
                   AlgebrizeExpression(column_or_param_expr));
  for (const auto& element : path) {
    if (absl::holds_alternative<const ResolvedGetProtoField*>(element)) {
      const ResolvedGetProtoField* get_proto_field =
          absl::get<const ResolvedGetProtoField*>(element);
      ProtoFieldRegistry* registry =
          zetasql_base::FindPtrOrNull(proto_field_registry_map_, column_and_field_path);
      if (registry == nullptr) {
        ZETASQL_ASSIGN_OR_RETURN(registry,
                         AddProtoFieldRegistry(column_and_field_path));
      }

      ProtoFieldAccessInfo access_info =
          CreateProtoFieldAccessInfo(*get_proto_field);
      column_and_field_path.field_path.push_back(
          ProtoOrStructField(ProtoOrStructField::PROTO_FIELD,
                             access_info.field_info.descriptor->number()));

      ProtoFieldReader* reader =
          access_info.field_info.get_has_bit
              ? nullptr
              : zetasql_base::FindPtrOrNull(get_proto_field_reader_map_,
                                   column_and_field_path);
      if (reader == nullptr) {
        ZETASQL_ASSIGN_OR_RETURN(reader, AddProtoFieldReader(column_and_field_path,
                                                     access_info, registry));
      }

      ZETASQL_ASSIGN_OR_RETURN(base_expr,
                       GetProtoFieldExpr::Create(std::move(base_expr), reader));
    } else {
      ZETASQL_RET_CHECK(
          absl::holds_alternative<const ResolvedGetStructField*>(element));
      const ResolvedGetStructField* get_struct_field =
          absl::get<const ResolvedGetStructField*>(element);
      column_and_field_path.field_path.push_back(ProtoOrStructField(
          ProtoOrStructField::STRUCT_FIELD, get_struct_field->field_idx()));
      ZETASQL_ASSIGN_OR_RETURN(base_expr,
                       FieldValueExpr::Create(get_struct_field->field_idx(),
                                              std::move(base_expr)));
    }
  }

  return base_expr;
}

zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeSubqueryExpr(
    const ResolvedSubqueryExpr* subquery_expr) {
  // Access 'parameters' to suppress the resolver check for non-accessed
  // expressions.
  for (const auto& parameter : subquery_expr->parameter_list()) {
    parameter->column();
  }
  // We will restore 'column_to_variable_' after algebrizing the subquery
  // to avoid any side-effect caused by the subquery.
  const ColumnToVariableMapping::Map original_column_to_variable =
      column_to_variable_->map();
  ZETASQL_RETURN_IF_ERROR(CheckHints(subquery_expr->hint_list()));
  const ResolvedScan* scan = subquery_expr->subquery();
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> relation, AlgebrizeScan(scan));

  const ResolvedColumnList& output_columns = scan->column_list();
  switch (subquery_expr->subquery_type()) {
    case ResolvedSubqueryExpr::EXISTS: {
      // In theory, we don't need a new expression for this, and can instead do
      // some function of SingleValueExpr(LimitOp(<relation>, LIMIT 1)). The
      // problem is that if the relation has more than one row, the LimitOp will
      // mark the operation as non-deterministic which will prevent random query
      // testing of this feature.
      ZETASQL_ASSIGN_OR_RETURN(auto subquery_valueop,
                       ExistsExpr::Create(std::move(relation)));
      return std::unique_ptr<ValueExpr>(std::move(subquery_valueop));
    }
    case ResolvedSubqueryExpr::SCALAR: {
      // A single column which may be a struct or an array.
      DCHECK_EQ(output_columns.size(), 1);
      const VariableId& var =
          column_to_variable_->GetVariableNameFromColumn(&output_columns[0]);
      ZETASQL_ASSIGN_OR_RETURN(auto deref,
                       DerefExpr::Create(var, output_columns[0].type()));
      column_to_variable_->set_map(original_column_to_variable);
      ZETASQL_ASSIGN_OR_RETURN(
          auto single_value_expr,
          SingleValueExpr::Create(std::move(deref), std::move(relation)));
      return std::unique_ptr<ValueExpr>(std::move(single_value_expr));
    }
    case ResolvedSubqueryExpr::ARRAY: {
      // Either a single scalar column or a struct column.
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<ValueExpr> nest_expr,
          NestSingleColumnRelation(output_columns, std::move(relation),
                                   /*is_with_table=*/false));
      column_to_variable_->set_map(original_column_to_variable);
      return std::move(nest_expr);
    }
    case ResolvedSubqueryExpr::IN: {
      ZETASQL_RET_CHECK_EQ(1, scan->column_list().size());
      const VariableId haystack_var =
          column_to_variable_->GetVariableNameFromColumn(
              &scan->column_list()[0]);
      // Restore 'column_to_variable_' before algebrizing the IN expression,
      // because the expression cannot reference columns produced by the
      // IN subquery.
      column_to_variable_->set_map(original_column_to_variable);
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> in_value,
                       AlgebrizeExpression(subquery_expr->in_expr()));
      return AlgebrizeInRelation(std::move(in_value), haystack_var,
                                 std::move(relation));
    }
    default:
      return ::zetasql_base::InternalErrorBuilder()
             << "Unknown type of resolved subquery: "
             << subquery_expr->subquery_type();
  }
}

zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeInArray(
    std::unique_ptr<ValueExpr> in_value,
    std::unique_ptr<ValueExpr> array_value) {
  const VariableId haystack_var =
      variable_gen_->GetNewVariableName("_in_element");
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<RelationalOp> haystack_rel,
      ArrayScanOp::Create(haystack_var, VariableId(), /* position */
                          {},                         /* fields */
                          std::move(array_value)));
  return AlgebrizeInRelation(
      std::move(in_value), haystack_var, std::move(haystack_rel));
}

// Algebrizes (in_value IN haystack_rel) as follows:
// LetOp{$_needle := in_value}(
//   SingleValueExpr(_matches,
//     AggregateOp{_matches := OR_AGG($_needle = $haystack)}(haystack_rel)))
zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeInRelation(
    std::unique_ptr<ValueExpr> in_value, const VariableId& haystack_var,
    std::unique_ptr<RelationalOp> haystack_rel) {
  const VariableId needle_var = variable_gen_->GetNewVariableName("_needle");
  const VariableId matches_var = variable_gen_->GetNewVariableName("_matches");

  ZETASQL_ASSIGN_OR_RETURN(auto deref_needle_var,
                   DerefExpr::Create(needle_var, in_value->output_type()));
  ZETASQL_ASSIGN_OR_RETURN(auto deref_haystack_var,
                   DerefExpr::Create(haystack_var, in_value->output_type()));

  std::vector<std::unique_ptr<ValueExpr>> equal_args;
  equal_args.push_back(std::move(deref_needle_var));
  equal_args.push_back(std::move(deref_haystack_var));

  ZETASQL_ASSIGN_OR_RETURN(
      auto equality_comparison,
      BuiltinScalarFunction::CreateCall(FunctionKind::kEqual, language_options_,
                                        BoolType(), std::move(equal_args)));

  std::vector<std::unique_ptr<ValueExpr>> agg_func_args;
  agg_func_args.push_back(std::move(equality_comparison));

  ZETASQL_ASSIGN_OR_RETURN(
      auto agg_arg,
      AggregateArg::Create(
          matches_var,
          absl::make_unique<BuiltinAggregateFunction>(
              FunctionKind::kOrAgg, BoolType(), /*num_input_fields=*/1,
              BoolType(), false /* ignores_null */),
          std::move(agg_func_args)));

  std::vector<std::unique_ptr<AggregateArg>> agg_args;
  agg_args.push_back(std::move(agg_arg));

  ZETASQL_ASSIGN_OR_RETURN(auto agg_rel, AggregateOp::Create(
                                     /*keys=*/{}, std::move(agg_args),
                                     std::move(haystack_rel)));

  // Create a scalar expression from the aggregate.
  ZETASQL_ASSIGN_OR_RETURN(auto deref_matches_var,
                   DerefExpr::Create(matches_var, BoolType()));
  ZETASQL_ASSIGN_OR_RETURN(auto singleton,
                   SingleValueExpr::Create(std::move(deref_matches_var),
                                           std::move(agg_rel)));

  std::vector<std::unique_ptr<ExprArg>> let_assign;
  let_assign.push_back(
      absl::make_unique<ExprArg>(needle_var, std::move(in_value)));
  ZETASQL_ASSIGN_OR_RETURN(auto let_expr, LetExpr::Create(std::move(let_assign),
                                                  std::move(singleton)));
  return std::unique_ptr<ValueExpr>(std::move(let_expr));
}

zetasql_base::StatusOr<std::unique_ptr<ValueExpr>>
Algebrizer::AlgebrizeStandaloneExpression(const ResolvedExpr* expr) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> value_expr,
                   AlgebrizeExpression(expr));

  // If we have any WITH clauses, create a LetExpr that binds the names of
  // subqueries to array expressions.  WITH subqueries cannot be correlated so
  // we can attach them all in one batch at the top of the query, and that will
  // ensure we run each of them exactly once.
  if (!with_subquery_let_assignments_.empty()) {
    ZETASQL_ASSIGN_OR_RETURN(value_expr,
                     LetExpr::Create(std::move(with_subquery_let_assignments_),
                                     std::move(value_expr)));
  }
  // Sanity check - WITH map should be cleared as WITH clauses go out of scope.
  ZETASQL_RET_CHECK(with_map_.empty());

  return WrapWithRootExpr(std::move(value_expr));
}

zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeExpression(
    const ResolvedExpr* expr) {
  if (!expr->type()->IsSupportedType(language_options_)) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Type not found: "
           << expr->type()->TypeName(language_options_.product_mode());
  }

  std::unique_ptr<ValueExpr> val_op;
  switch (expr->node_kind()) {
    case RESOLVED_LITERAL: {
      ZETASQL_ASSIGN_OR_RETURN(
          val_op, ConstExpr::Create(expr->GetAs<ResolvedLiteral>()->value()));
      break;
    }
    case RESOLVED_CONSTANT: {
      const Constant* constant = expr->GetAs<ResolvedConstant>()->constant();
      if (constant->Is<SimpleConstant>()) {
        ZETASQL_ASSIGN_OR_RETURN(
            val_op,
            ConstExpr::Create(constant->GetAs<SimpleConstant>()->value()));
      } else {
        return ::zetasql_base::UnimplementedErrorBuilder()
               << "Unhandled constant implementation algebrizing an "
                  "expression: "
               << constant->DebugString();
      }
      break;
    }
    case RESOLVED_COLUMN_REF: {
      // Create a single, typed DerefExpr operator for a single column.
      const ResolvedColumn& column = expr->GetAs<ResolvedColumnRef>()->column();
      ZETASQL_ASSIGN_OR_RETURN(
          const VariableId variable_id,
          column_to_variable_->LookupVariableNameForColumn(&column));
      ZETASQL_ASSIGN_OR_RETURN(val_op, DerefExpr::Create(variable_id, expr->type()));
      break;
    }
    case RESOLVED_FUNCTION_CALL: {
      ZETASQL_ASSIGN_OR_RETURN(
          val_op, AlgebrizeFunctionCall(expr->GetAs<ResolvedFunctionCall>()));
      break;
    }
    case RESOLVED_AGGREGATE_FUNCTION_CALL:
      return ::zetasql_base::InternalErrorBuilder()
             << "AlgebrizeExpression called with an aggregate expression";
    case RESOLVED_CAST: {
      ZETASQL_ASSIGN_OR_RETURN(val_op, AlgebrizeCast(expr->GetAs<ResolvedCast>()));
      break;
    }
    case RESOLVED_MAKE_STRUCT: {
      ZETASQL_ASSIGN_OR_RETURN(val_op, MakeStruct(expr->GetAs<ResolvedMakeStruct>()));
      break;
    }
    case RESOLVED_GET_PROTO_FIELD: {
      ZETASQL_ASSIGN_OR_RETURN(
          val_op, AlgebrizeGetProtoField(expr->GetAs<ResolvedGetProtoField>()));
      break;
    }
    case RESOLVED_SUBQUERY_EXPR: {
      ZETASQL_ASSIGN_OR_RETURN(
          val_op, AlgebrizeSubqueryExpr(expr->GetAs<ResolvedSubqueryExpr>()));
      break;
    }
    case RESOLVED_SYSTEM_VARIABLE: {
      const ResolvedSystemVariable* system_variable =
          expr->GetAs<ResolvedSystemVariable>();
      ZETASQL_ASSIGN_OR_RETURN(
          val_op, DerefExpr::Create(
                      variable_gen_->GetVariableNameFromSystemVariable(
                          system_variable->name_path(), system_variables_map_),
                      expr->type()));
      break;
    }
    case RESOLVED_PARAMETER: {
      const ResolvedParameter* parameter = expr->GetAs<ResolvedParameter>();
      const bool is_named_parameter = !parameter->name().empty();
      ZETASQL_RET_CHECK_EQ(is_named_parameter, parameters_->is_named())
          << "The parameter kind (named versus positional) is not consistent "
             "with the parameters provided to the statement";

      if (parameters_->is_named()) {
        ZETASQL_ASSIGN_OR_RETURN(
            val_op,
            DerefExpr::Create(
                variable_gen_->GetVariableNameFromParameter(
                    parameter->name(), &parameters_->named_parameters()),
                expr->type()));
      } else {
        ZETASQL_ASSIGN_OR_RETURN(
            val_op,
            DerefExpr::Create(variable_gen_->GetVariableNameFromParameter(
                                  parameter->position(),
                                  &parameters_->positional_parameters()),
                              expr->type()));
      }
      break;
    }
    case RESOLVED_EXPRESSION_COLUMN: {
      // Column passed as a parameter into an expression.
      ZETASQL_ASSIGN_OR_RETURN(
          val_op,
          DerefExpr::Create(
              variable_gen_->GetVariableNameFromParameter(
                  expr->GetAs<ResolvedExpressionColumn>()->name(), column_map_),
              expr->type()));
      break;
    }
    case RESOLVED_GET_STRUCT_FIELD: {
      ZETASQL_ASSIGN_OR_RETURN(val_op, AlgebrizeGetStructField(
                                   expr->GetAs<ResolvedGetStructField>()));
      break;
    }
    case RESOLVED_MAKE_PROTO: {
      auto make_proto = expr->GetAs<ResolvedMakeProto>();
      std::vector<MakeProtoFunction::FieldAndFormat> fields;
      std::vector<std::unique_ptr<ValueExpr>> arguments;
      for (const auto& field : make_proto->field_list()) {
        const google::protobuf::FieldDescriptor* field_descr = field->field_descriptor();
        ZETASQL_RETURN_IF_ERROR(ProtoUtil::CheckIsSupportedFieldFormat(
            field->format(), field_descr));
        fields.emplace_back(field_descr, field->format());
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> value_op,
                         AlgebrizeExpression(field->expr()));
        arguments.push_back(std::move(value_op));
      }
      ZETASQL_ASSIGN_OR_RETURN(val_op, ScalarFunctionCallExpr::Create(
                                   absl::make_unique<MakeProtoFunction>(
                                       expr->type()->AsProto(), fields),
                                   std::move(arguments)));
      break;
    }
    case RESOLVED_DMLDEFAULT: {
      // In the reference implementation, the default value is always NULL.
      ZETASQL_ASSIGN_OR_RETURN(val_op, ConstExpr::Create(Value::Null(expr->type())));
      break;
    }
    case RESOLVED_REPLACE_FIELD: {
      // TODO: Modify the ResolvedAST to propagate the field paths in
      // a tree format to assist with traversal of the serialized proto.
      auto replace_fields = expr->GetAs<ResolvedReplaceField>();
      std::vector<ReplaceFieldsFunction::StructAndProtoPath> field_paths;
      field_paths.reserve(replace_fields->replace_field_item_list_size());
      // <arguments> will store root object to be modified as well as the new
      // field values.
      std::vector<std::unique_ptr<ValueExpr>> arguments;
      arguments.reserve(replace_fields->replace_field_item_list_size() + 1);
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> root_expr,
                       AlgebrizeExpression(replace_fields->expr()));
      arguments.push_back(std::move(root_expr));
      for (const auto& replace_field_item :
           replace_fields->replace_field_item_list()) {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> new_value,
                         AlgebrizeExpression(replace_field_item->expr()));
        arguments.push_back(std::move(new_value));
        field_paths.emplace_back(replace_field_item->struct_index_path(),
                                 replace_field_item->proto_field_path());
      }
      ZETASQL_ASSIGN_OR_RETURN(val_op, ScalarFunctionCallExpr::Create(
                                   absl::make_unique<ReplaceFieldsFunction>(
                                       expr->type(), field_paths),
                                   std::move(arguments)));
      break;
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unhandled node type algebrizing an expression: "
             << expr->node_kind_string();
  }
  return val_op;
}

// Returns the set of columns referenced by 'expr'.
static zetasql_base::StatusOr<absl::flat_hash_set<ResolvedColumn>> GetReferencedColumns(
    const ResolvedExpr* expr) {
  // ResolvedASTVisitor that records the set of referenced columns.
  class ReferencedColumnsVisitor : public ResolvedASTVisitor {
   public:
    ReferencedColumnsVisitor() {}
    ReferencedColumnsVisitor(const ReferencedColumnsVisitor&) = delete;
    ReferencedColumnsVisitor& operator=(const ReferencedColumnsVisitor&) =
        delete;

    const absl::flat_hash_set<ResolvedColumn>& columns() const {
      return columns_;
    }

    absl::Status VisitResolvedColumnRef(
        const ResolvedColumnRef* node) override {
      columns_.insert(node->column());
      return DefaultVisit(node);
    }

   private:
    absl::flat_hash_set<ResolvedColumn> columns_;
  };

  ReferencedColumnsVisitor visitor;
  ZETASQL_RETURN_IF_ERROR(expr->Accept(&visitor));
  return visitor.columns();
}

// Returns true if 'expr' is known to be non-volatile (per
// FunctionEnums::VOLATILE).
static bool IsNonVolatile(const ResolvedExpr* expr) {
  switch (expr->node_kind()) {
    case RESOLVED_FUNCTION_CALL: {
      const ResolvedFunctionCall* function_call =
          expr->GetAs<ResolvedFunctionCall>();
      if (function_call->function()->function_options().volatility ==
          FunctionEnums::VOLATILE) {
        return false;
      }
      for (int i = 0; i < function_call->argument_list_size(); ++i) {
        if (!IsNonVolatile(function_call->argument_list(i))) {
          return false;
        }
      }
      return true;
    }

    case RESOLVED_GET_PROTO_FIELD:
      return IsNonVolatile(expr->GetAs<ResolvedGetProtoField>()->expr());
    case RESOLVED_GET_STRUCT_FIELD:
      return IsNonVolatile(expr->GetAs<ResolvedGetStructField>()->expr());
    case RESOLVED_CAST:
      return IsNonVolatile(expr->GetAs<ResolvedCast>()->expr());
    case RESOLVED_MAKE_PROTO: {
      for (const auto& field : expr->GetAs<ResolvedMakeProto>()->field_list()) {
        if (!IsNonVolatile(field->expr())) {
          return false;
        }
      }
      return true;
    }
    case RESOLVED_MAKE_STRUCT: {
      for (const auto& field_expr :
           expr->GetAs<ResolvedMakeStruct>()->field_list()) {
        if (!IsNonVolatile(field_expr.get())) {
          return false;
        }
      }
      return true;
    }

    case RESOLVED_EXPRESSION_COLUMN:
    case RESOLVED_LITERAL:
    case RESOLVED_CONSTANT:
    case RESOLVED_COLUMN_REF:
    case RESOLVED_PARAMETER:
      return true;
    default:
      return false;
  }
}

zetasql_base::StatusOr<std::unique_ptr<Algebrizer::FilterConjunctInfo>>
Algebrizer::FilterConjunctInfo::Create(const ResolvedExpr* conjunct) {
  auto info = absl::make_unique<FilterConjunctInfo>();
  info->conjunct = conjunct;
  info->is_non_volatile = IsNonVolatile(info->conjunct);
  ZETASQL_ASSIGN_OR_RETURN(info->referenced_columns,
                   GetReferencedColumns(info->conjunct));

  if (conjunct->node_kind() != RESOLVED_FUNCTION_CALL) return info;

  const ResolvedFunctionCall* function_call =
      conjunct->GetAs<ResolvedFunctionCall>();
  const Function* function = function_call->function();

  info->arguments.reserve(function_call->argument_list_size());
  for (int i = 0; i < function_call->argument_list_size(); ++i) {
    info->arguments.push_back(function_call->argument_list(i));
  }

  info->argument_columns.reserve(info->arguments.size());
  for (const ResolvedExpr* argument : info->arguments) {
    ZETASQL_ASSIGN_OR_RETURN(absl::flat_hash_set<ResolvedColumn> columns,
                     GetReferencedColumns(argument));
    info->argument_columns.push_back(std::move(columns));
  }

  if (!function->IsZetaSQLBuiltin()) return info;

  const std::string name = function->FullName(/*include_group=*/false);

  if (name == "$less" || name == "$less_or_equal") {
    info->kind = kLE;
  } else if (name == "$greater" || name == "$greater_or_equal") {
    info->kind = kGE;
  } else if (name == "$equal") {
    info->kind = kEquals;
  } else if (name == "$between") {
    info->kind = kBetween;
  } else if (name == "$in") {
    info->kind = kIn;
  } else if (name == "$in_array") {
    info->kind = kInArray;
  } else {
    info->kind = kOther;
  }

  return info;
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
Algebrizer::AlgebrizeSingleRowScan() {
  ZETASQL_ASSIGN_OR_RETURN(auto const_expr, ConstExpr::Create(Value::Int64(1)));
  ZETASQL_ASSIGN_OR_RETURN(auto enum_op, EnumerateOp::Create(std::move(const_expr)));
  return std::unique_ptr<RelationalOp>(std::move(enum_op));
}

::zetasql_base::StatusOr<std::unique_ptr<ArrayScanOp>>
Algebrizer::CreateScanOfTableAsArray(const ResolvedScan* scan,
                                     bool is_value_table,
                                     std::unique_ptr<ValueExpr> table_expr) {
  const ResolvedColumnList& column_list = scan->column_list();
  auto element_type = table_expr->output_type()->AsArray()->element_type();
  if (!is_value_table) {
    // List of fields emitted by the table.
    std::vector<std::pair<VariableId, int>> fields;
    DCHECK_EQ(column_list.size(), element_type->AsStruct()->num_fields());
    fields.reserve(column_list.size());
    for (int i = 0; i < column_list.size(); ++i) {
      fields.emplace_back(std::make_pair(
          column_to_variable_->GetVariableNameFromColumn(&column_list[i]), i));
    }
    return ArrayScanOp::Create(VariableId() /* element */,
                               VariableId() /* position */, fields,
                               std::move(table_expr));
  } else {
    // Value table, e.g., array of protos. Leave 'fields' empty.
    return ArrayScanOp::Create(
        column_to_variable_->GetVariableNameFromColumn(
            &column_list[0]) /* element */,
        VariableId() /* position */,
        std::vector<std::pair<VariableId, int>>() /* fields */,
        std::move(table_expr));
  }
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> Algebrizer::AlgebrizeTableScan(
    const ResolvedTableScan* table_scan,
    std::vector<FilterConjunctInfo*>* active_conjuncts) {
  std::unique_ptr<ValueExpr> system_time_expr;
  if (table_scan->for_system_time_expr() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(system_time_expr,
                     AlgebrizeExpression(table_scan->for_system_time_expr()));
  }

  if (algebrizer_options_.use_arrays_for_tables) {
    // Construct the list of fields of the SELECT and place a scan operator
    // over the table.  The scan is required because a table has array type.
    const std::string& table_name = table_scan->table()->Name();

    ZETASQL_ASSIGN_OR_RETURN(const ArrayType* table_type,
                     CreateTableArrayType(table_scan->column_list(),
                                          table_scan->table()->IsValueTable(),
                                          type_factory_));
    ZETASQL_ASSIGN_OR_RETURN(auto table_as_array_expr,
                     TableAsArrayExpr::Create(table_name, table_type));
    return CreateScanOfTableAsArray(table_scan,
                                    table_scan->table()->IsValueTable(),
                                    std::move(table_as_array_expr));
  } else {
    const ResolvedColumnList& column_list = table_scan->column_list();
    const std::vector<int>& column_idx_list = table_scan->column_index_list();
    ZETASQL_RET_CHECK_EQ(column_list.size(), column_idx_list.size());

    // Figure out the column names and variables.
    std::vector<std::string> column_names;
    column_names.reserve(column_list.size());
    std::vector<VariableId> variables;
    variables.reserve(column_list.size());
    TableScanColumnInfoMap column_info_map;
    column_info_map.reserve(column_list.size());
    for (int i = 0; i < column_list.size(); ++i) {
      const ResolvedColumn& column = column_list[i];
      column_names.push_back(column.name());
      const VariableId variable =
          column_to_variable_->GetVariableNameFromColumn(&column);
      variables.push_back(variable);
      ZETASQL_RET_CHECK(
          column_info_map.emplace(column, std::make_pair(variable, i)).second);
    }

    // Create ColumnFilterArgs from 'conjunct_infos'.
    std::vector<std::unique_ptr<ColumnFilterArg>> and_filters;
    if (algebrizer_options_.push_down_filters) {
      // Iterate over 'active_conjuncts' in reverse order because it's a stack.
      for (auto i = active_conjuncts->rbegin(); i != active_conjuncts->rend();
           ++i) {
        const FilterConjunctInfo& info = **i;
        ZETASQL_RETURN_IF_ERROR(TryAlgebrizeFilterConjunctAsColumnFilterArgs(
            column_info_map, info, &and_filters));
        // We cannot mark 'info' redundant here because EvaluatorTableIterator
        // does not guarantee that it will honor 'and_filters' (so we need to
        // keep the conjunct in a filter somewhere above it).
      }
    }

    return EvaluatorTableScanOp::Create(
        table_scan->table(), table_scan->alias(), column_idx_list, column_names,
        variables, std::move(and_filters), std::move(system_time_expr));
  }
}

// Returns true if any element of 'a' is in 'b'.
static bool Intersects(const absl::flat_hash_set<ResolvedColumn>& a,
                       const absl::flat_hash_set<ResolvedColumn>& b) {
  for (const ResolvedColumn& column : a) {
    if (b.contains(column)) return true;
  }
  return false;
}

absl::Status Algebrizer::TryAlgebrizeFilterConjunctAsColumnFilterArgs(
    const TableScanColumnInfoMap& column_info_map,
    const FilterConjunctInfo& conjunct_info,
    std::vector<std::unique_ptr<ColumnFilterArg>>* and_filters) {
  if (!conjunct_info.is_non_volatile) return absl::OkStatus();

  absl::flat_hash_set<ResolvedColumn> table_columns;
  table_columns.reserve(column_info_map.size());
  for (const auto& entry : column_info_map) {
    ZETASQL_RET_CHECK(table_columns.insert(entry.first).second);
  }

  FilterConjunctInfo::Kind conjunct_kind = conjunct_info.kind;
  switch (conjunct_kind) {
    case FilterConjunctInfo::kLE:
    case FilterConjunctInfo::kGE:
    case FilterConjunctInfo::kEquals: {
      ZETASQL_RET_CHECK_EQ(conjunct_info.arguments.size(), 2);
      int left_idx = 0;
      int right_idx = 1;

      const ResolvedExpr* left_hand_side = conjunct_info.arguments[left_idx];
      const ResolvedExpr* right_hand_side = conjunct_info.arguments[right_idx];

      if (right_hand_side->node_kind() == RESOLVED_COLUMN_REF &&
          table_columns.contains(
              right_hand_side->GetAs<ResolvedColumnRef>()->column())) {
        std::swap(left_idx, right_idx);
        std::swap(left_hand_side, right_hand_side);
        if (conjunct_kind == FilterConjunctInfo::kLE) {
          conjunct_kind = FilterConjunctInfo::kGE;
        } else if (conjunct_kind == FilterConjunctInfo::kGE) {
          conjunct_kind = FilterConjunctInfo::kLE;
        }
      }

      if (left_hand_side->node_kind() != RESOLVED_COLUMN_REF) {
        return absl::OkStatus();
      }
      const ResolvedColumn& column =
          left_hand_side->GetAs<ResolvedColumnRef>()->column();
      const std::pair<VariableId, int>* variable_and_column_idx =
          zetasql_base::FindOrNull(column_info_map, column);
      if (variable_and_column_idx == nullptr) return absl::OkStatus();

      // For example, we can't push down a filter of the form column1 = column2.
      // One side has to be independent of the table row.
      if (Intersects(conjunct_info.argument_columns[right_idx],
                     table_columns)) {
        return absl::OkStatus();
      }

      if (conjunct_kind == FilterConjunctInfo::kEquals) {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> element,
                         AlgebrizeExpression(right_hand_side));

        std::vector<std::unique_ptr<ValueExpr>> elements;
        elements.push_back(std::move(element));

        ZETASQL_ASSIGN_OR_RETURN(auto filter, InListColumnFilterArg::Create(
                                          variable_and_column_idx->first,
                                          variable_and_column_idx->second,
                                          std::move(elements)));
        and_filters->push_back(std::move(filter));
      } else {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> arg,
                         AlgebrizeExpression(right_hand_side));
        const HalfUnboundedColumnFilterArg::Kind arg_kind =
            (conjunct_kind == FilterConjunctInfo::kLE
                 ? HalfUnboundedColumnFilterArg::kLE
                 : HalfUnboundedColumnFilterArg::kGE);

        ZETASQL_ASSIGN_OR_RETURN(auto filter, HalfUnboundedColumnFilterArg::Create(
                                          variable_and_column_idx->first,
                                          variable_and_column_idx->second,
                                          arg_kind, std::move(arg)));
        and_filters->push_back(std::move(filter));
      }
      break;
    }
    case FilterConjunctInfo::kBetween: {
      ZETASQL_RET_CHECK_EQ(conjunct_info.arguments.size(), 3);

      if (conjunct_info.arguments[0]->node_kind() != RESOLVED_COLUMN_REF) {
        return absl::OkStatus();
      }
      const ResolvedColumn& column =
          conjunct_info.arguments[0]->GetAs<ResolvedColumnRef>()->column();
      const std::pair<VariableId, int>* variable_and_column_idx =
          zetasql_base::FindOrNull(column_info_map, column);
      if (variable_and_column_idx == nullptr) return absl::OkStatus();

      if (Intersects(conjunct_info.argument_columns[1], table_columns)) {
        return absl::OkStatus();
      }

      if (Intersects(conjunct_info.argument_columns[2], table_columns)) {
        return absl::OkStatus();
      }

      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> lower_bound,
                       AlgebrizeExpression(conjunct_info.arguments[1]));
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> upper_bound,
                       AlgebrizeExpression(conjunct_info.arguments[2]));

      ZETASQL_ASSIGN_OR_RETURN(
          auto lower_bound_restriction,
          HalfUnboundedColumnFilterArg::Create(
              variable_and_column_idx->first, variable_and_column_idx->second,
              HalfUnboundedColumnFilterArg::kGE, std::move(lower_bound)));
      ZETASQL_ASSIGN_OR_RETURN(
          auto upper_bound_restriction,
          HalfUnboundedColumnFilterArg::Create(
              variable_and_column_idx->first, variable_and_column_idx->second,
              HalfUnboundedColumnFilterArg::kLE, std::move(upper_bound)));

      and_filters->push_back(std::move(lower_bound_restriction));
      and_filters->push_back(std::move(upper_bound_restriction));
      break;
    }
    case FilterConjunctInfo::kIn:
    case FilterConjunctInfo::kInArray: {
      ZETASQL_RET_CHECK(!conjunct_info.arguments.empty());

      if (conjunct_info.arguments[0]->node_kind() != RESOLVED_COLUMN_REF) {
        return absl::OkStatus();
      }
      const ResolvedColumn& column =
          conjunct_info.arguments[0]->GetAs<ResolvedColumnRef>()->column();
      const std::pair<VariableId, int>* variable_and_column_idx =
          zetasql_base::FindOrNull(column_info_map, column);
      if (variable_and_column_idx == nullptr) return absl::OkStatus();

      for (int i = 1; i < conjunct_info.arguments.size(); ++i) {
        if (Intersects(conjunct_info.argument_columns[i], table_columns)) {
          return absl::OkStatus();
        }
      }

      // Algebrize all the arguments except for the first one.
      std::vector<std::unique_ptr<ValueExpr>> elements;
      for (int i = 1; i < conjunct_info.arguments.size(); ++i) {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> element,
                         AlgebrizeExpression(conjunct_info.arguments[i]));
        elements.push_back(std::move(element));
      }

      std::unique_ptr<ColumnFilterArg> filter;
      if (conjunct_kind == FilterConjunctInfo::kInArray) {
        ZETASQL_RET_CHECK_EQ(elements.size(), 1);
        ZETASQL_ASSIGN_OR_RETURN(filter, InArrayColumnFilterArg::Create(
                                     variable_and_column_idx->first,
                                     variable_and_column_idx->second,
                                     std::move(elements[0])));
      } else {
        ZETASQL_ASSIGN_OR_RETURN(
            filter, InListColumnFilterArg::Create(
                        variable_and_column_idx->first,
                        variable_and_column_idx->second, std::move(elements)));
      }
      and_filters->push_back(std::move(filter));
      break;
    }
    case FilterConjunctInfo::kOther:
      break;
  }

  return absl::OkStatus();
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> Algebrizer::AlgebrizeWithScan(
    const ResolvedWithScan* scan) {
  // Each named subquery is nested as an array, which is then unnested when
  // referenced in other subquerieries or in the main query. Named subqueries
  // are stored in with_map_ to be used for algebrizing WithRef scans that
  // reference those subqueries.
  // Save the old with_map_ with names that are visible in the outer scope.
  const absl::flat_hash_map<std::string, ExprArg*> old_with_map = with_map_;
  for (const auto& with_entry : scan->with_entry_list()) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> subquery,
                     AlgebrizeScan(with_entry->with_subquery()));
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<ArrayNestExpr> nested_subquery,
        NestRelationInStruct(with_entry->with_subquery()->column_list(),
                             std::move(subquery),
                             /*is_with_table=*/true));
    const VariableId subquery_variable =
        variable_gen_->GetNewVariableName(with_entry->with_query_name());
    ExprArg* arg = new ExprArg(subquery_variable, std::move(nested_subquery));
    // Record a mapping from subquery name to ExprArg.
    with_map_[with_entry->with_query_name()] = arg;
    with_subquery_let_assignments_.emplace_back(arg);  // Takes ownership.
  }
  // Algebrize and nest the main query.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> nested_query,
                   AlgebrizeScan(scan->query()));
  with_map_ = old_with_map;  // Restore original state.
  return std::move(nested_query);
}

// Called only while AlgebrizeWithScan() sits in an earlier stack frame.
zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> Algebrizer::AlgebrizeWithRefScan(
    const ResolvedWithRefScan* scan) {
  const auto& it =
      with_map_.find(scan->GetAs<ResolvedWithRefScan>()->with_query_name());
  ZETASQL_RET_CHECK(it != with_map_.end());
  const ExprArg* arg = it->second;
  ZETASQL_ASSIGN_OR_RETURN(
      auto deref_arg,
      DerefExpr::Create(arg->variable(), arg->value_expr()->output_type()));
  return CreateScanOfTableAsArray(scan, /*is_value_table=*/false,
                                  std::move(deref_arg));
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> Algebrizer::AlgebrizeArrayScan(
    const ResolvedArrayScan* array_scan,
    std::vector<FilterConjunctInfo*>* active_conjuncts) {
  if (array_scan->input_scan() == nullptr) {
    ZETASQL_RET_CHECK(array_scan->join_expr() == nullptr);
    return AlgebrizeArrayScanWithoutJoin(array_scan, active_conjuncts);
  } else {
    const JoinOp::JoinKind join_kind =
        array_scan->is_outer() ? JoinOp::kOuterApply : JoinOp::kCrossApply;

    std::vector<ResolvedColumn> right_output_columns;
    right_output_columns.push_back(array_scan->element_column());
    if (array_scan->array_offset_column() != nullptr) {
      right_output_columns.push_back(
          array_scan->array_offset_column()->column());
    }

    auto right_scan_algebrizer_cb =
        [this,
         array_scan](std::vector<FilterConjunctInfo*>* active_conjuncts_arg)
        -> zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> {
      const absl::flat_hash_set<ResolvedColumn> input_columns(
          array_scan->input_scan()->column_list().begin(),
          array_scan->input_scan()->column_list().end());

      for (FilterConjunctInfo* info : *active_conjuncts_arg) {
        ZETASQL_RET_CHECK(!info->redundant);
        ZETASQL_RET_CHECK(!Intersects(info->referenced_columns, input_columns));
      }

      return AlgebrizeArrayScanWithoutJoin(array_scan, active_conjuncts_arg);
    };

    return AlgebrizeJoinScanInternal(
        join_kind, array_scan->join_expr(), array_scan->input_scan(),
        right_output_columns, right_scan_algebrizer_cb, active_conjuncts);
  }
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
Algebrizer::AlgebrizeArrayScanWithoutJoin(
    const ResolvedArrayScan* array_scan,
    std::vector<FilterConjunctInfo*>* active_conjuncts) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> array,
                   AlgebrizeExpression(array_scan->array_expr()));

  const VariableId array_element_in =
      column_to_variable_->GetVariableNameFromColumn(
          &array_scan->element_column());

  VariableId array_position_in;
  if (array_scan->array_offset_column() != nullptr) {
    array_position_in = column_to_variable_->GetVariableNameFromColumn(
        &array_scan->array_offset_column()->column());
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> rel_op,
                   ArrayScanOp::Create(array_element_in, array_position_in,
                                       /*fields=*/{}, std::move(array)));
  return MaybeApplyFilterConjuncts(std::move(rel_op), active_conjuncts);
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
Algebrizer::AlgebrizeLimitOffsetScan(const ResolvedLimitOffsetScan* scan) {
  ZETASQL_RET_CHECK(scan->limit() != nullptr);

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> limit,
                   AlgebrizeExpression(scan->limit()));
  std::unique_ptr<ValueExpr> offset;
  if (scan->offset() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(offset, AlgebrizeExpression(scan->offset()));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(offset, ConstExpr::Create(Value::Int64(0)));
  }

  if (algebrizer_options_.allow_order_by_limit_operator &&
      scan->input_scan()->node_kind() == RESOLVED_ORDER_BY_SCAN) {
    const ResolvedOrderByScan* input_scan =
        scan->input_scan()->GetAs<ResolvedOrderByScan>();
    return AlgebrizeOrderByScan(input_scan, std::move(limit),
                                std::move(offset));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> input,
                     AlgebrizeScan(scan->input_scan()));
    return LimitOp::Create(std::move(limit), std::move(offset),
                           std::move(input), scan->is_ordered());
  }
}

absl::Status Algebrizer::AddFilterConjunctsTo(
    const ResolvedExpr* expr,
    std::vector<std::unique_ptr<FilterConjunctInfo>>* conjunct_infos) {
  if (expr->node_kind() == RESOLVED_FUNCTION_CALL) {
    const ResolvedFunctionCall* function_call =
        expr->GetAs<ResolvedFunctionCall>();
    const Function* function = function_call->function();
    if (function->IsZetaSQLBuiltin()) {
      const zetasql_base::StatusOr<FunctionKind> status_or_kind =
          BuiltinFunctionCatalog::GetKindByName(
              function->FullName(/*include_group=*/false));
      if (status_or_kind.ok() && status_or_kind.value() == FunctionKind::kAnd) {
        for (const std::unique_ptr<const ResolvedExpr>& arg :
             function_call->argument_list()) {
          ZETASQL_RETURN_IF_ERROR(AddFilterConjunctsTo(arg.get(), conjunct_infos));
        }
        return absl::OkStatus();
      }
    }
  }

  ZETASQL_ASSIGN_OR_RETURN(auto conjunct_info, FilterConjunctInfo::Create(expr));
  conjunct_infos->push_back(std::move(conjunct_info));
  return absl::OkStatus();
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> Algebrizer::AlgebrizeJoinScan(
    const ResolvedJoinScan* join_scan,
    std::vector<FilterConjunctInfo*>* active_conjuncts) {
  JoinOp::JoinKind join_kind;
  switch (join_scan->join_type()) {
    case ResolvedJoinScan::INNER:
      join_kind = JoinOp::kInnerJoin;
      break;
    case ResolvedJoinScan::LEFT:
      join_kind = JoinOp::kLeftOuterJoin;
      break;
    case ResolvedJoinScan::RIGHT:
      join_kind = JoinOp::kRightOuterJoin;
      break;
    case ResolvedJoinScan::FULL:
      join_kind = JoinOp::kFullOuterJoin;
      break;
  }

  const ResolvedScan* right_scan = join_scan->right_scan();
  auto right_scan_algebrizer_cb =
      [this, right_scan](std::vector<FilterConjunctInfo*>* active_conjuncts) {
        return AlgebrizeScan(right_scan, active_conjuncts);
      };
  return AlgebrizeJoinScanInternal(
      join_kind, join_scan->join_expr(), join_scan->left_scan(),
      right_scan->column_list(), right_scan_algebrizer_cb, active_conjuncts);
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
Algebrizer::AlgebrizeJoinScanInternal(
    JoinOp::JoinKind join_kind, const ResolvedExpr* join_expr,
    const ResolvedScan* left_scan,
    const std::vector<ResolvedColumn>& right_output_column_list,
    const RightScanAlgebrizerCb& right_scan_algebrizer_cb,
    std::vector<FilterConjunctInfo*>* active_conjuncts) {
  std::vector<std::unique_ptr<FilterConjunctInfo>> conjunct_infos;
  if (join_expr != nullptr) {
    ZETASQL_RETURN_IF_ERROR(AddFilterConjunctsTo(join_expr, &conjunct_infos));
  }
  const absl::flat_hash_set<ResolvedColumn> left_output_columns(
      left_scan->column_list().begin(), left_scan->column_list().end());
  const absl::flat_hash_set<ResolvedColumn> right_output_columns(
      right_output_column_list.begin(), right_output_column_list.end());
  std::vector<FilterConjunctInfo*> join_condition_conjuncts_with_push_down;
  std::vector<FilterConjunctInfo*> left_conjuncts_with_push_down;
  std::vector<FilterConjunctInfo*> right_conjuncts_with_push_down;
  if (algebrizer_options_.push_down_filters) {
    // Try to rewrite the join as an equivalent inner join to facilitate filter
    // pushdown.
    for (FilterConjunctInfo* conjunct_info : *active_conjuncts) {
      ZETASQL_RETURN_IF_ERROR(
          NarrowJoinKindForFilterConjunct(*conjunct_info, left_output_columns,
                                          right_output_columns, &join_kind));
    }

    // Iterate over 'active_conjuncts' to find the ones that can actually be
    // pushed down.
    for (FilterConjunctInfo* conjunct_info : *active_conjuncts) {
      bool push_down_to_join_condition;
      bool push_down_to_left_input;
      bool push_down_to_right_input;
      ZETASQL_RETURN_IF_ERROR(CanPushFilterConjunctIntoJoin(
          *conjunct_info, join_kind, left_output_columns, right_output_columns,
          &push_down_to_join_condition, &push_down_to_left_input,
          &push_down_to_right_input));
      if (push_down_to_join_condition) {
        join_condition_conjuncts_with_push_down.push_back(conjunct_info);
      }
      if (push_down_to_left_input) {
        left_conjuncts_with_push_down.push_back(conjunct_info);
      }
      if (push_down_to_right_input) {
        right_conjuncts_with_push_down.push_back(conjunct_info);
      }
    }
  }

  // Iterate over the new conjuncts (in reverse order, because they are in a
  // stack), determining which of them can be pushed down and which should
  // remain in the join condition.
  for (auto i = conjunct_infos.rbegin(); i != conjunct_infos.rend(); ++i) {
    FilterConjunctInfo* conjunct_info = i->get();
    bool push_down_to_left_input = false;
    bool push_down_to_right_input = false;
    if (algebrizer_options_.push_down_filters &&
        (join_kind == JoinOp::kInnerJoin || join_kind == JoinOp::kCrossApply)) {
      // For simplicity, we only support pushdowns from the join condition to
      // the join inputs for inner joins. Note that the code above handles the
      // case of WHERE clause pushdown (above the join), and also potentially
      // rewrites a left/right/full join to an inner join, so this code runs.
      ZETASQL_RETURN_IF_ERROR(CanPushFilterConjunctDownFromInnerJoinCondition(
          *conjunct_info, left_output_columns, right_output_columns,
          &push_down_to_left_input, &push_down_to_right_input));
    }
    if (push_down_to_left_input) {
      left_conjuncts_with_push_down.push_back(conjunct_info);
    }
    if (push_down_to_right_input) {
      right_conjuncts_with_push_down.push_back(conjunct_info);
    }
    if (!push_down_to_left_input && !push_down_to_right_input) {
      join_condition_conjuncts_with_push_down.push_back(conjunct_info);
    }
  }

  // Sanity check that there are no redundant conjuncts yet.
  for (const FilterConjunctInfo* conjunct_info : *active_conjuncts) {
    ZETASQL_RET_CHECK(!conjunct_info->redundant);
  }

  // Algebrize the left side of the join. Because we checked whether a
  // ConjunctInfo was non-volatile when we constructed
  // 'left_conjuncts_with_push_down', all entries in
  // 'left_conjuncts_with_push_down' are marked as redundant. However, we must
  // temporarily unmark them in order to algebrize the right side of the join,
  // because 'left_conjuncts_with_push_down' and
  // 'right_conjuncts_with_push_down' may overlap.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> left,
                   AlgebrizeScan(left_scan, &left_conjuncts_with_push_down));
  for (FilterConjunctInfo* info : left_conjuncts_with_push_down) {
    ZETASQL_RET_CHECK(info->redundant);
    info->redundant = false;
  }

  // Algebrize the right side of the join. Because we checked whether a
  // ConjunctInfo was non-volatile when we constructed
  // 'right_conjuncts_with_push_down', all entries in
  // 'right_conjuncts_with_push_down' are marked as redundant.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> right,
                   right_scan_algebrizer_cb(&right_conjuncts_with_push_down));

  for (const FilterConjunctInfo* info : right_conjuncts_with_push_down) {
    ZETASQL_RET_CHECK(info->redundant);
  }

  // Remark all entries in 'left_conjuncts_with_push_down' as redundant.
  for (FilterConjunctInfo* info : left_conjuncts_with_push_down) {
    info->redundant = true;
  }

  // Incorporate conjuncts into the hash join where allowed/possible.
  std::vector<JoinOp::HashJoinEqualityExprs> hash_join_equality_exprs;
  if (algebrizer_options_.allow_hash_join) {
    switch (join_kind) {
      case JoinOp::kInnerJoin:
      case JoinOp::kLeftOuterJoin:
      case JoinOp::kRightOuterJoin:
      case JoinOp::kFullOuterJoin:
        ZETASQL_RETURN_IF_ERROR(AlgebrizeJoinConditionForHashJoin(
            left_output_columns, right_output_columns,
            &join_condition_conjuncts_with_push_down,
            &hash_join_equality_exprs));
        break;
      case JoinOp::kCrossApply:
      case JoinOp::kOuterApply:
        // Hash join is not supported for correlated joins.
        break;
    }
  }

  // Algebrize all of the non-redundant remaining conjuncts for use in the join
  // condition. Iterate in reverse order to de-stackify the ordering.
  std::vector<std::unique_ptr<ValueExpr>> algebrized_conjuncts;
  for (auto i = join_condition_conjuncts_with_push_down.rbegin();
       i != join_condition_conjuncts_with_push_down.rend(); ++i) {
    FilterConjunctInfo* conjunct_info = *i;
    if (!conjunct_info->redundant) {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> algebrized_conjunct,
                       AlgebrizeExpression(conjunct_info->conjunct));
      algebrized_conjuncts.push_back(std::move(algebrized_conjunct));
      conjunct_info->redundant = true;
    }
  }

  // Algebrize the join condition using the algebrized conjuncts.
  std::unique_ptr<ValueExpr> remaining_join_expr;
  if (algebrized_conjuncts.empty()) {
    ZETASQL_ASSIGN_OR_RETURN(remaining_join_expr,
                     ConstExpr::Create(values::Bool(true)));
  } else if (algebrized_conjuncts.size() == 1) {
    remaining_join_expr = std::move(algebrized_conjuncts[0]);
  } else {
    ZETASQL_ASSIGN_OR_RETURN(remaining_join_expr,
                     BuiltinScalarFunction::CreateCall(
                         FunctionKind::kAnd, language_options_,
                         types::BoolType(), std::move(algebrized_conjuncts),
                         ResolvedFunctionCallBase::DEFAULT_ERROR_MODE));
  }

  // Determine the outputs.
  std::vector<std::unique_ptr<ExprArg>> left_output, right_output;
  switch (join_kind) {
    case JoinOp::kInnerJoin:
    case JoinOp::kCrossApply:
      // no NULL-extension of left or right input
      break;
    case JoinOp::kLeftOuterJoin:
    case JoinOp::kOuterApply:
      ZETASQL_RETURN_IF_ERROR(
          RemapJoinColumns(right_output_column_list, &right_output));
      break;
    case JoinOp::kRightOuterJoin:
      ZETASQL_RETURN_IF_ERROR(RemapJoinColumns(left_scan->column_list(), &left_output));
      break;
    case JoinOp::kFullOuterJoin:
      ZETASQL_RETURN_IF_ERROR(RemapJoinColumns(left_scan->column_list(), &left_output));
      ZETASQL_RETURN_IF_ERROR(
          RemapJoinColumns(right_output_column_list, &right_output));
      break;
  }

  // Algebrize the join.
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<RelationalOp> join_op,
      JoinOp::Create(join_kind, std::move(hash_join_equality_exprs),
                     std::move(remaining_join_expr), std::move(left),
                     std::move(right), std::move(left_output),
                     std::move(right_output)));

  return join_op;
}

absl::Status Algebrizer::NarrowJoinKindForFilterConjunct(
    const FilterConjunctInfo& conjunct_info,
    const absl::flat_hash_set<ResolvedColumn>& left_output_columns,
    const absl::flat_hash_set<ResolvedColumn>& right_output_columns,
    JoinOp::JoinKind* join_kind) {
  if (conjunct_info.kind == FilterConjunctInfo::kOther ||
      *join_kind == JoinOp::kInnerJoin || *join_kind == JoinOp::kCrossApply) {
    return absl::OkStatus();
  }

  bool has_left_column_arg = false;
  bool has_right_column_arg = false;

  for (const ResolvedExpr* argument : conjunct_info.arguments) {
    if (argument->node_kind() != RESOLVED_COLUMN_REF) continue;
    if (left_output_columns.contains(
            argument->GetAs<ResolvedColumnRef>()->column())) {
      has_left_column_arg = true;
    }
    if (right_output_columns.contains(
            argument->GetAs<ResolvedColumnRef>()->column())) {
      has_right_column_arg = true;
    }
  }

  switch (*join_kind) {
    case JoinOp::kInnerJoin:
    case JoinOp::kCrossApply:
      // Handled above.
      ZETASQL_RET_CHECK_FAIL()
          << "Unexpected join kind in TightenJoinKindForFilterConjunct(): "
          << JoinOp::JoinKindToString(*join_kind);
    case JoinOp::kLeftOuterJoin:
      // Consider a conjunct of the form f(left columns) = <right column>
      // applied above a left join. Any tuples coming out of the join that are
      // right-padded with NULLs will cause this predicate to evaluate to NULL,
      // and therefore the predicate won't apply. Thus, it is safe to rewrite
      // the join to an inner join to facilitate pushing down the filter.
      //
      // Generalizing to arbitrary non-kOther conjuncts, it is enough to know
      // that at least one of the arguments is NULL whenever all the right join
      // columns are NULL, since that case the conjunct is NULL
      //
      // TODO: With more work, we could do better here by handling more
      // cases (i.e., handle kOther and arbitrary expressions for the
      // arguments, not just columns).
      if (has_right_column_arg) {
        *join_kind = JoinOp::kInnerJoin;
      }
      return absl::OkStatus();
    case JoinOp::kOuterApply:
      // Same as left outer join.
      if (has_right_column_arg) {
        *join_kind = JoinOp::kCrossApply;
      }
      return absl::OkStatus();
    case JoinOp::kRightOuterJoin:
      // Symmetric to left outer join.
      if (has_left_column_arg) {
        *join_kind = JoinOp::kInnerJoin;
      }
      return absl::OkStatus();
    case JoinOp::kFullOuterJoin:
      // Analogous to the left/right outer join cases.
      if (has_left_column_arg && has_right_column_arg) {
        *join_kind = JoinOp::kInnerJoin;
      } else if (has_left_column_arg) {
        *join_kind = JoinOp::kLeftOuterJoin;
      } else if (has_right_column_arg) {
        *join_kind = JoinOp::kRightOuterJoin;
      }
      return absl::OkStatus();
  }
}

absl::Status Algebrizer::CanPushFilterConjunctIntoJoin(
    const FilterConjunctInfo& conjunct_info, JoinOp::JoinKind join_kind,
    const absl::flat_hash_set<ResolvedColumn>& left_output_columns,
    const absl::flat_hash_set<ResolvedColumn>& right_output_columns,
    bool* push_down_to_join_condition, bool* push_down_to_left_input,
    bool* push_down_to_right_input) {
  *push_down_to_join_condition = false;
  *push_down_to_left_input = false;
  *push_down_to_right_input = false;

  if (!conjunct_info.is_non_volatile) return absl::OkStatus();

  const bool references_left_column =
      Intersects(conjunct_info.referenced_columns, left_output_columns);
  const bool references_right_column =
      Intersects(conjunct_info.referenced_columns, right_output_columns);

  switch (join_kind) {
    case JoinOp::kInnerJoin:
    case JoinOp::kCrossApply:
      // We can freely push down any conjunct through an inner join.
      ZETASQL_RETURN_IF_ERROR(CanPushFilterConjunctDownFromInnerJoinCondition(
          conjunct_info, left_output_columns, right_output_columns,
          push_down_to_left_input, push_down_to_right_input));
      *push_down_to_join_condition =
          !*push_down_to_left_input && !*push_down_to_right_input;
      return absl::OkStatus();
    case JoinOp::kLeftOuterJoin:
    case JoinOp::kOuterApply:
      // Conjuncts that don't reference the right can be pushed down.
      if (!references_right_column) {
        *push_down_to_left_input = true;
      }
      return absl::OkStatus();
    case JoinOp::kRightOuterJoin:
      // Symmetric to left outer join.
      if (!references_left_column) {
        *push_down_to_right_input = true;
      }
      return absl::OkStatus();
    case JoinOp::kFullOuterJoin:
      // Analogous to left/right outer join.
      return absl::OkStatus();
  }
}

absl::Status Algebrizer::CanPushFilterConjunctDownFromInnerJoinCondition(
    const FilterConjunctInfo& conjunct_info,
    const absl::flat_hash_set<ResolvedColumn>& left_output_columns,
    const absl::flat_hash_set<ResolvedColumn>& right_output_columns,
    bool* push_down_to_left_input, bool* push_down_to_right_input) {
  *push_down_to_left_input = false;
  *push_down_to_right_input = false;

  if (!conjunct_info.is_non_volatile) return absl::OkStatus();

  const bool references_left_column =
      Intersects(conjunct_info.referenced_columns, left_output_columns);
  const bool references_right_column =
      Intersects(conjunct_info.referenced_columns, right_output_columns);

  *push_down_to_left_input = !references_right_column;
  *push_down_to_right_input = !references_left_column;

  return absl::OkStatus();
}

absl::Status Algebrizer::AlgebrizeJoinConditionForHashJoin(
    const absl::flat_hash_set<ResolvedColumn>& left_output_columns,
    const absl::flat_hash_set<ResolvedColumn>& right_output_columns,
    std::vector<FilterConjunctInfo*>* conjuncts_with_push_down,
    std::vector<JoinOp::HashJoinEqualityExprs>* hash_join_equality_exprs) {
  for (auto i = conjuncts_with_push_down->rbegin();
       i != conjuncts_with_push_down->rend(); ++i) {
    FilterConjunctInfo* conjunct_info = *i;
    ZETASQL_RET_CHECK(!conjunct_info->redundant);

    JoinOp::HashJoinEqualityExprs equality_exprs;
    ZETASQL_ASSIGN_OR_RETURN(
        const bool populated_equality_exprs,
        TryAlgebrizeFilterConjunctAsHashJoinEqualityExprs(
            *conjunct_info, left_output_columns, right_output_columns,
            hash_join_equality_exprs->size(), &equality_exprs));
    if (populated_equality_exprs) {
      hash_join_equality_exprs->push_back(std::move(equality_exprs));
      conjunct_info->redundant = true;
    }
  }

  return absl::OkStatus();
}

// Returns true if 'a' is a subset of 'b'.
static bool IsSubsetOf(const absl::flat_hash_set<ResolvedColumn>& a,
                       const absl::flat_hash_set<ResolvedColumn>& b) {
  for (const ResolvedColumn& column : a) {
    if (!b.contains(column)) return false;
  }
  return true;
}

zetasql_base::StatusOr<bool>
Algebrizer::TryAlgebrizeFilterConjunctAsHashJoinEqualityExprs(
    const FilterConjunctInfo& conjunct_info,
    const absl::flat_hash_set<ResolvedColumn>& left_output_columns,
    const absl::flat_hash_set<ResolvedColumn>& right_output_columns,
    int num_previous_equality_exprs,
    JoinOp::HashJoinEqualityExprs* equality_exprs) {
  if (!conjunct_info.is_non_volatile) return false;
  if (conjunct_info.kind != FilterConjunctInfo::kEquals) return false;

  ZETASQL_RET_CHECK_EQ(conjunct_info.arguments.size(), 2);
  const ResolvedExpr* first_arg = conjunct_info.arguments[0];
  const ResolvedExpr* second_arg = conjunct_info.arguments[1];
  const absl::flat_hash_set<ResolvedColumn>* first_arg_columns =
      &conjunct_info.argument_columns[0];
  const absl::flat_hash_set<ResolvedColumn>* second_arg_columns =
      &conjunct_info.argument_columns[1];

  if (IsSubsetOf(*first_arg_columns, right_output_columns) &&
      IsSubsetOf(*second_arg_columns, left_output_columns)) {
    std::swap(first_arg, second_arg);
    std::swap(first_arg_columns, second_arg_columns);
  }

  if (!IsSubsetOf(*first_arg_columns, left_output_columns)) {
    return false;
  }
  if (!IsSubsetOf(*second_arg_columns, right_output_columns)) {
    return false;
  }

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> algebrized_first_arg,
                   AlgebrizeExpression(first_arg));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> algebrized_second_arg,
                   AlgebrizeExpression(second_arg));

  const int next_var_number = num_previous_equality_exprs + 1;
  const VariableId left_var =
      variable_gen_->GetNewVariableName(absl::StrCat("a", next_var_number));
  const VariableId right_var =
      variable_gen_->GetNewVariableName(absl::StrCat("b", next_var_number));

  equality_exprs->left_expr =
      absl::make_unique<ExprArg>(left_var, std::move(algebrized_first_arg));
  equality_exprs->right_expr =
      absl::make_unique<ExprArg>(right_var, std::move(algebrized_second_arg));

  return true;
}
absl::Status Algebrizer::RemapJoinColumns(
    const ResolvedColumnList& columns,
    std::vector<std::unique_ptr<ExprArg>>* output) {
  absl::flat_hash_set<int> columns_seen;
  for (int i = 0; i < columns.size(); ++i) {
    if (!zetasql_base::InsertIfNotPresent(&columns_seen, columns[i].column_id())) {
      continue;  // Skip columns that we have already seen.
    }
    VariableId old_var =
        column_to_variable_->GetVariableNameFromColumn(&columns[i]);
    VariableId new_var =
        column_to_variable_->AssignNewVariableToColumn(&columns[i]);
    ZETASQL_ASSIGN_OR_RETURN(auto deref_expr,
                     DerefExpr::Create(old_var, columns[i].type()));
    output->push_back(
        absl::make_unique<ExprArg>(new_var, std::move(deref_expr)));
  }
  return absl::OkStatus();
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> Algebrizer::AlgebrizeFilterScan(
    const ResolvedFilterScan* filter_scan,
    std::vector<FilterConjunctInfo*>* active_conjuncts) {
  const ResolvedScan* input_scan = filter_scan->input_scan();
  const ResolvedExpr* filter_expr = filter_scan->filter_expr();

  std::vector<std::unique_ptr<FilterConjunctInfo>> conjunct_infos;
  ZETASQL_RETURN_IF_ERROR(AddFilterConjunctsTo(filter_expr, &conjunct_infos));
  // Push the new conjuncts onto 'active_conjuncts' in reverse order (because
  // it's a stack).
  for (auto i = conjunct_infos.rbegin(); i != conjunct_infos.rend(); ++i) {
    active_conjuncts->push_back(i->get());
  }

  // Algebrize the input scan.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> input,
                   AlgebrizeScan(input_scan, active_conjuncts));
  // Restore 'active_conjuncts'.
  for (const std::unique_ptr<FilterConjunctInfo>& info : conjunct_infos) {
    ZETASQL_RET_CHECK(info.get() == active_conjuncts->back());
    active_conjuncts->pop_back();
  }

  // Drop any FilterConjunctInfos that are now redundant.
  std::vector<std::unique_ptr<ValueExpr>> algebrized_conjuncts;
  algebrized_conjuncts.reserve(conjunct_infos.size());
  for (std::unique_ptr<FilterConjunctInfo>& info : conjunct_infos) {
    if (!info->redundant) {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> algebrized_conjunct,
                       AlgebrizeExpression(info->conjunct));
      algebrized_conjuncts.push_back(std::move(algebrized_conjunct));
    }
  }

  // Algebrize the filter.
  return ApplyAlgebrizedFilterConjuncts(std::move(input),
                                        std::move(algebrized_conjuncts));
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
Algebrizer::ApplyAlgebrizedFilterConjuncts(
    std::unique_ptr<RelationalOp> input,
    std::vector<std::unique_ptr<ValueExpr>> algebrized_conjuncts) {
  std::unique_ptr<RelationalOp> rel_op;
  if (algebrized_conjuncts.empty()) {
    // No FilterOp needed, just use the input directly.
    rel_op = std::move(input);
  } else {
    std::unique_ptr<ValueExpr> filter;
    if (algebrized_conjuncts.size() == 1) {
      filter = std::move(algebrized_conjuncts[0]);
    } else {
      ZETASQL_ASSIGN_OR_RETURN(
          filter, BuiltinScalarFunction::CreateCall(
                      FunctionKind::kAnd, language_options_, types::BoolType(),
                      std::move(algebrized_conjuncts),
                      ResolvedFunctionCallBase::DEFAULT_ERROR_MODE));
    }

    ZETASQL_ASSIGN_OR_RETURN(rel_op,
                     FilterOp::Create(std::move(filter), std::move(input)));
  }
  return rel_op;
}

zetasql_base::StatusOr<std::unique_ptr<AggregateOp>>
Algebrizer::AlgebrizeAggregateScan(
    const ResolvedAggregateScan* aggregate_scan) {
  // Algebrize the relational input of the aggregate.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> input,
                   AlgebrizeScan(aggregate_scan->input_scan()));
  // Build the list of grouping keys.
  std::vector<std::unique_ptr<KeyArg>> keys;
  for (const std::unique_ptr<const ResolvedComputedColumn>& key_expr :
       aggregate_scan->group_by_list()) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> key,
                     AlgebrizeExpression(key_expr->expr()));
    const VariableId key_variable_name =
        column_to_variable_->AssignNewVariableToColumn(&key_expr->column());
    keys.push_back(
        absl::make_unique<KeyArg>(key_variable_name, std::move(key)));
  }

  // Build the set of output columns.
  absl::flat_hash_set<ResolvedColumn> output_columns;
  output_columns.reserve(aggregate_scan->column_list_size());
  for (const ResolvedColumn& column : aggregate_scan->column_list()) {
    ZETASQL_RET_CHECK(output_columns.insert(column).second) << column.DebugString();
  }

  // Build the list of aggregate functions.
  std::vector<std::unique_ptr<AggregateArg>> aggregators;
  for (const std::unique_ptr<const ResolvedComputedColumn>& agg_expr :
       aggregate_scan->aggregate_list()) {
    const ResolvedColumn& column = agg_expr->column();
    // Sanity check that all aggregate functions appear in the output column
    // list, so that we don't accidentally return an aggregate function that
    // the analyzer pruned from the scan. (If it did that, it should have
    // pruned the aggregate function as well.)
    ZETASQL_RET_CHECK(output_columns.contains(column)) << column.DebugString();

    // Add the aggregate function to the output.
    const VariableId agg_variable_name =
        column_to_variable_->AssignNewVariableToColumn(&column);
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<AggregateArg> agg,
                     AlgebrizeAggregateFn(
                         agg_variable_name,
                         agg_expr->expr()));
    aggregators.push_back(std::move(agg));
  }
  return AggregateOp::Create(std::move(keys), std::move(aggregators),
                             std::move(input));
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> Algebrizer::AlgebrizeAnalyticScan(
    const ResolvedAnalyticScan* analytic_scan) {
  // Algebrize the input scan.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> relation_op,
                   AlgebrizeScan(analytic_scan->input_scan()));

  // Algebrize each ResolvedAnalyticFunctionGroup sequentially.
  std::set<ResolvedColumn> input_columns(
      analytic_scan->input_scan()->column_list().begin(),
      analytic_scan->input_scan()->column_list().end());
  bool first = true;
  for (const std::unique_ptr<const ResolvedAnalyticFunctionGroup>& group :
       analytic_scan->function_group_list()) {
    ZETASQL_ASSIGN_OR_RETURN(relation_op,
                     AlgebrizeAnalyticFunctionGroup(
                         input_columns, group.get(), std::move(relation_op),
                         /*input_is_from_same_analytic_scan=*/!first));
    first = false;
    for (const std::unique_ptr<const ResolvedComputedColumn>& analytic_column :
         group->analytic_function_list()) {
      ZETASQL_RET_CHECK(
          zetasql_base::InsertIfNotPresent(&input_columns, analytic_column->column()));
    }
  }

  return std::move(relation_op);
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
Algebrizer::AlgebrizeAnalyticFunctionGroup(
    const std::set<ResolvedColumn>& input_resolved_columns,
    const ResolvedAnalyticFunctionGroup* analytic_group,
    std::unique_ptr<RelationalOp> input_relation_op,
    bool input_is_from_same_analytic_scan) {
  const ResolvedWindowPartitioning* partition_by =
      analytic_group->partition_by();
  const ResolvedWindowOrdering* order_by =
      analytic_group->order_by();

  // Create a SortOp if there are non-correlated partitioning or ordering
  // expressions.
  if (partition_by != nullptr || order_by != nullptr) {
    // We use a stable sort in case there are two windows (in different analytic
    // function groups) ordering by the same thing. They must see the tuples in
    // the same order. Ideally the resolver would not give us two analytic
    // function groups with the same window, and would instead consolidate into
    // one analytic function group: b/123518026.
    ZETASQL_ASSIGN_OR_RETURN(
        input_relation_op,
        MaybeCreateSortForAnalyticOperator(
            input_resolved_columns, analytic_group,
            std::move(input_relation_op), input_is_from_same_analytic_scan));
  }

  std::vector<std::unique_ptr<KeyArg>> partition_keys;
  std::vector<std::unique_ptr<KeyArg>> order_keys;
  absl::flat_hash_map<int, VariableId> column_to_id_map;

  if (partition_by != nullptr) {
    // Create KeyArgs for partitioning expressions.
    ZETASQL_RETURN_IF_ERROR(AlgebrizePartitionExpressions(
        partition_by, &column_to_id_map, &partition_keys));
  }

  if (order_by != nullptr) {
    // Create KeyArgs for ordering expressions.
    // Do not create new VariableIds, because each ordering expression
    // references a distinct column produced by the SortOp we have just created.
    // Do not drop the correlated columns, because we may need them to compute
    // range-based window boundaries.
    ZETASQL_RETURN_IF_ERROR(AlgebrizeOrderByItems(
        false /* drop_correlated_columns */, false /* create_new_ids */,
        order_by->order_by_item_list(), &column_to_id_map, &order_keys));
  }

  std::vector<std::unique_ptr<AnalyticArg>> analytic_args;
  for (const std::unique_ptr<const ResolvedComputedColumn>& analytic_column :
       analytic_group->analytic_function_list()) {
    ZETASQL_RET_CHECK_EQ(RESOLVED_ANALYTIC_FUNCTION_CALL,
                 analytic_column->expr()->node_kind());
    const ResolvedAnalyticFunctionCall* analytic_function_call =
        static_cast<const ResolvedAnalyticFunctionCall*>(
            analytic_column->expr());

    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<AnalyticArg> analytic_arg,
                     AlgebrizeAnalyticFunctionCall(
                         column_to_variable_->AssignNewVariableToColumn(
                             &analytic_column->column()),
                         analytic_function_call));
    analytic_args.push_back(std::move(analytic_arg));
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<RelationalOp> analytic_op,
      AnalyticOp::Create(
          std::move(partition_keys), std::move(order_keys),
          std::move(analytic_args), std::move(input_relation_op),
          // Don't allow scrambling between AnalyticOps representing
          // AnalyticFunctionGroups corresponding to the same AnalyticScan
          // because that might cause ties between partitioning and ordering
          // keys to be resolved differently for windows that are conceptually
          // the same. As above, ideally the resolver would not give us two
          // analytic function groups with the same window, and would instead
          // consolidate into one analytic function group: b/123518026.
          /*preserves_order=*/input_is_from_same_analytic_scan));
  return analytic_op;
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
Algebrizer::MaybeCreateSortForAnalyticOperator(
    const std::set<ResolvedColumn>& input_resolved_columns,
    const ResolvedAnalyticFunctionGroup* analytic_group,
    std::unique_ptr<RelationalOp> input_relation_op, bool require_stable_sort) {
  std::vector<std::unique_ptr<KeyArg>> sort_keys;
  // Map from each referenced column to its VariableId from the input.
  absl::flat_hash_map<int, VariableId> column_to_id_map;

  const ResolvedWindowPartitioning* partition_by =
      analytic_group->partition_by();
  if (partition_by != nullptr) {
    ZETASQL_RETURN_IF_ERROR(AlgebrizePartitionExpressions(
        partition_by, &column_to_id_map, &sort_keys));
  }

  const ResolvedWindowOrdering* order_by =
      analytic_group->order_by();
  if (order_by != nullptr) {
    // Create a new VariableId for each ordering expression, because an
    // ordering expression might appear in the order by list multiple times.
    // The SortOp produces a column for each occurrence, and
    // each output column cannot have the same VariableId. We do not eliminate
    // duplicate ordering columns because they can have different collation
    // names. If the collation names are parameters, we do not know the value
    // until we evaluate them.
    ZETASQL_RETURN_IF_ERROR(AlgebrizeOrderByItems(
        true /* drop_correlated_columns */, true /* create_new_ids */,
        order_by->order_by_item_list(), &column_to_id_map, &sort_keys));
  }

  if (sort_keys.empty()) {
    return std::move(input_relation_op);
  }

  ZETASQL_RET_CHECK(!column_to_id_map.empty());

  // Create ExprArgs for other expressions that are not partitioning or
  // ordering expressions.
  std::vector<std::unique_ptr<ExprArg>> non_sort_expressions;
  for (const ResolvedColumn& input_column : input_resolved_columns) {
    if (!zetasql_base::InsertIfNotPresent(
            &column_to_id_map, input_column.column_id(),
            column_to_variable_->GetVariableNameFromColumn(&input_column))) {
      continue;
    }

    // 'column_to_id_map' stores the VariableIds produced by the input.
    // Do not use GetVariableNameFromColumn so that we won't reference
    // a column output by the SortOp itself.
    const VariableId var =
        zetasql_base::FindOrDie(column_to_id_map, input_column.column_id());
    const VariableId new_var =
        column_to_variable_->AssignNewVariableToColumn(&input_column);
    ZETASQL_ASSIGN_OR_RETURN(auto deref_expr,
                     DerefExpr::Create(var, input_column.type()));
    non_sort_expressions.push_back(
        absl::make_unique<ExprArg>(new_var, std::move(deref_expr)));
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<RelationalOp> sort_op,
      SortOp::Create(std::move(sort_keys), std::move(non_sort_expressions),
                     /*limit=*/nullptr, /*offset=*/nullptr,
                     std::move(input_relation_op),
                     /*is_order_preserving=*/true, require_stable_sort));
  return sort_op;
}

absl::Status Algebrizer::AlgebrizeOrderByItems(
    bool drop_correlated_columns, bool create_new_ids,
    const std::vector<std::unique_ptr<const ResolvedOrderByItem>>&
        order_by_items,
    absl::flat_hash_map<int, VariableId>* column_to_id_map,
    std::vector<std::unique_ptr<KeyArg>>* order_by_keys) {
  for (const std::unique_ptr<const ResolvedOrderByItem>& order_by_item :
       order_by_items) {
    if (drop_correlated_columns &&
        order_by_item->column_ref()->is_correlated()) {
      // Access the column field to prevent ZetaSQL from complaining that
      // the field is not accessed.
      ZETASQL_RET_CHECK(order_by_item->column_ref()->column().type() != nullptr);
      continue;
    }

    const ResolvedColumn* order_column =
        &order_by_item->column_ref()->column();

    // Both column and collation defines the sort key. As we do not know
    // collation name until we evaluate it, we cannot ignore duplicate columns
    // because they can be associated with different collations.
    zetasql_base::InsertIfNotPresent(
        column_to_id_map, order_column->column_id(),
        column_to_variable_->GetVariableNameFromColumn(order_column));

    // Do not use GetVariableNameFromColumn, because the VariableId is no longer
    // the input VariableId if the column appears multiple times and we assigned
    // a new one to it.
    const VariableId key_in =
        zetasql_base::FindOrDie(*column_to_id_map, order_column->column_id());
    const VariableId key_out =
        (create_new_ids
             ? column_to_variable_->AssignNewVariableToColumn(order_column)
             : key_in);

    std::unique_ptr<ValueExpr> sort_collation;
    if (order_by_item->collation_name() != nullptr) {
      ZETASQL_ASSIGN_OR_RETURN(sort_collation,
                       AlgebrizeExpression(order_by_item->collation_name()));
    }

    const KeyArg::SortOrder sort_order =
        (order_by_item->is_descending() ? KeyArg::kDescending :
                                          KeyArg::kAscending);
    KeyArg::NullOrder null_order = KeyArg::kDefaultNullOrder;
    switch (order_by_item->null_order()) {
      case ResolvedOrderByItemEnums::NULLS_FIRST:
        null_order = KeyArg::kNullsFirst;
        break;
      case ResolvedOrderByItemEnums::NULLS_LAST:
        null_order = KeyArg::kNullsLast;
        break;
      case ResolvedOrderByItemEnums::ORDER_UNSPECIFIED:
        break;
      default:
        ZETASQL_RET_CHECK_FAIL() << "Unexpected null order: "
                         << ResolvedOrderByItemEnums::NullOrderMode_Name(
                                order_by_item->null_order());
    }
    ZETASQL_ASSIGN_OR_RETURN(auto deref_key,
                     DerefExpr::Create(key_in, order_column->type()));
    order_by_keys->push_back(absl::make_unique<KeyArg>(
        key_out, std::move(deref_key), sort_order, null_order));
    order_by_keys->back()->set_collation(std::move(sort_collation));
  }

  return absl::OkStatus();
}

absl::Status Algebrizer::AlgebrizePartitionExpressions(
    const ResolvedWindowPartitioning* partition_by,
    absl::flat_hash_map<int, VariableId>* column_to_id_map,
    std::vector<std::unique_ptr<KeyArg>>* partition_by_keys) {
  for (const std::unique_ptr<const ResolvedColumnRef>& partition_column_ref :
       partition_by->partition_by_list()) {
    if (partition_column_ref->is_correlated()) {
      // Access the column field to prevent ZetaSQL from complaining that
      // the field is not accessed.
      ZETASQL_RET_CHECK(partition_column_ref->column().type() != nullptr);
      continue;
    }

    const ResolvedColumn* partition_column = &partition_column_ref->column();
    if (!zetasql_base::InsertIfNotPresent(
            column_to_id_map, partition_column->column_id(),
            column_to_variable_->GetVariableNameFromColumn(partition_column))) {
      // Skip duplicate partitioning keys.
      continue;
    }

    const VariableId key =
        zetasql_base::FindOrDie(*column_to_id_map, partition_column->column_id());
    ZETASQL_ASSIGN_OR_RETURN(auto deref_key,
                     DerefExpr::Create(key, partition_column->type()));
    partition_by_keys->push_back(absl::make_unique<KeyArg>(
        key, std::move(deref_key), KeyArg::kAscending));
  }

  return absl::OkStatus();
}

zetasql_base::StatusOr<std::unique_ptr<AnalyticArg>>
Algebrizer::AlgebrizeAnalyticFunctionCall(
    const VariableId& variable,
    const ResolvedAnalyticFunctionCall* analytic_function_call) {
  std::unique_ptr<WindowFrameArg> window_frame;
  if (analytic_function_call->window_frame() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(window_frame, AlgebrizeWindowFrame(
                                       analytic_function_call->window_frame()));
  }

  if (!analytic_function_call->function()->IsZetaSQLBuiltin()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Non-ZetaSQL built-in functions are unsupported: "
           << analytic_function_call->function()->Name();
  }

  if (!analytic_function_call->function()->SupportsOverClause()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Function " << analytic_function_call->function()->Name()
           << " is not an analytic function";
  }

  if (analytic_function_call->function()->mode() == Function::AGGREGATE) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<AggregateArg> aggregate_arg,
                     AlgebrizeAggregateFn(
                         variable,
                         analytic_function_call));
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<AnalyticArg> analytic_arg,
                     AggregateAnalyticArg::Create(
                         std::move(window_frame), std::move(aggregate_arg),
                         analytic_function_call->error_mode()));
    return analytic_arg;
  }

  std::vector<std::unique_ptr<ValueExpr>> arguments;
  for (const std::unique_ptr<const ResolvedExpr>& resolved_argument :
       analytic_function_call->argument_list()) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> value_expr,
                     AlgebrizeExpression(resolved_argument.get()));
    arguments.push_back(std::move(value_expr));
  }

  std::unique_ptr<AnalyticFunctionBody> function;
  std::vector<std::unique_ptr<ValueExpr>> non_const_arguments;
  std::vector<std::unique_ptr<ValueExpr>> const_arguments;

  const FunctionSignatureId function_id =
      static_cast<FunctionSignatureId>(
          analytic_function_call->signature().context_id());
  switch (function_id) {
    case FN_DENSE_RANK:
      function = absl::make_unique<DenseRankFunction>();
      break;
    case FN_RANK:
      function = absl::make_unique<RankFunction>();
      break;
    case FN_ROW_NUMBER:
      function = absl::make_unique<RowNumberFunction>();
      break;
    case FN_PERCENT_RANK:
      function = absl::make_unique<PercentRankFunction>();
      break;
    case FN_CUME_DIST:
      function = absl::make_unique<CumeDistFunction>();
      break;
    case FN_NTILE:
      function = absl::make_unique<NtileFunction>();
      ZETASQL_RET_CHECK_EQ(1, arguments.size());
      const_arguments.push_back(std::move(arguments[0]));
      break;
    case FN_LEAD:
      function =
          absl::make_unique<LeadFunction>(analytic_function_call->type());
      ZETASQL_RET_CHECK(!arguments.empty());
      non_const_arguments.push_back(std::move(arguments[0]));
      // Fill in the default arguments if not provided.
      if (arguments.size() > 1) {
        const_arguments.push_back(std::move(arguments[1]));
      } else {
        ZETASQL_ASSIGN_OR_RETURN(auto const_expr, ConstExpr::Create(Value::Int64(1)));
        const_arguments.push_back(std::move(const_expr));
      }
      if (arguments.size() > 2) {
        const ResolvedExpr* default_expression =
            analytic_function_call->argument_list(2);
        if (!IsConstantExpression(default_expression)) {
          return ::zetasql_base::InvalidArgumentErrorBuilder()
                 << "The third argument (the default expression) to LEAD "
                 << "must be constant";
        }
        const_arguments.push_back(std::move(arguments[2]));
      } else {
        ZETASQL_ASSIGN_OR_RETURN(
            auto const_expr,
            ConstExpr::Create(Value::Null(analytic_function_call->type())));
        const_arguments.push_back(std::move(const_expr));
      }
      break;
    case FN_LAG:
      function = absl::make_unique<LagFunction>(analytic_function_call->type());
      ZETASQL_RET_CHECK(!arguments.empty());
      non_const_arguments.push_back(std::move(arguments[0]));
      // Fill in the default arguments if not provided.
      if (arguments.size() > 1) {
        const_arguments.push_back(std::move(arguments[1]));
      } else {
        ZETASQL_ASSIGN_OR_RETURN(auto const_expr, ConstExpr::Create(Value::Int64(1)));
        const_arguments.push_back(std::move(const_expr));
      }
      if (arguments.size() > 2) {
        const ResolvedExpr* default_expression =
            analytic_function_call->argument_list(2);
        if (!IsConstantExpression(default_expression)) {
          return ::zetasql_base::InvalidArgumentErrorBuilder()
                 << "The third argument (the default expression) to LAG "
                 << "must be constant";
        }
        const_arguments.push_back(std::move(arguments[2]));
      } else {
        ZETASQL_ASSIGN_OR_RETURN(
            auto const_expr,
            ConstExpr::Create(Value::Null(analytic_function_call->type())));
        const_arguments.push_back(std::move(const_expr));
      }
      break;
    case FN_FIRST_VALUE:
      function = absl::make_unique<FirstValueFunction>(
          analytic_function_call->type(),
          analytic_function_call->null_handling_modifier());
      ZETASQL_RET_CHECK_EQ(1, arguments.size());
      non_const_arguments.push_back(std::move(arguments[0]));
      break;
    case FN_LAST_VALUE:
      function = absl::make_unique<LastValueFunction>(
          analytic_function_call->type(),
          analytic_function_call->null_handling_modifier());
      ZETASQL_RET_CHECK_EQ(1, arguments.size());
      non_const_arguments.push_back(std::move(arguments[0]));
      break;
    case FN_NTH_VALUE:
      function = absl::make_unique<NthValueFunction>(
          analytic_function_call->type(),
          analytic_function_call->null_handling_modifier());
      ZETASQL_RET_CHECK_EQ(2, arguments.size());
      non_const_arguments.push_back(std::move(arguments[0]));
      const_arguments.push_back(std::move(arguments[1]));
      break;
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Function " << analytic_function_call->function()->Name()
             << " not yet implemented";
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<AnalyticArg> analytic_arg,
      NonAggregateAnalyticArg::Create(
          variable, std::move(window_frame), std::move(function),
          std::move(non_const_arguments), std::move(const_arguments),
          analytic_function_call->error_mode()));
  return analytic_arg;
}

zetasql_base::StatusOr<std::unique_ptr<WindowFrameArg>>
Algebrizer::AlgebrizeWindowFrame(const ResolvedWindowFrame* window_frame) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<WindowFrameBoundaryArg> start_boundary,
                   AlgebrizeWindowFrameExpr(window_frame->start_expr()));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<WindowFrameBoundaryArg> end_boundary,
                   AlgebrizeWindowFrameExpr(window_frame->end_expr()));

  switch (window_frame->frame_unit()) {
    case ResolvedWindowFrame::ROWS:
      return WindowFrameArg::Create(WindowFrameArg::kRows,
                                    std::move(start_boundary),
                                    std::move(end_boundary));
    case ResolvedWindowFrame::RANGE:
      return WindowFrameArg::Create(WindowFrameArg::kRange,
                                    std::move(start_boundary),
                                    std::move(end_boundary));
  }
}

zetasql_base::StatusOr<std::unique_ptr<WindowFrameBoundaryArg>>
Algebrizer::AlgebrizeWindowFrameExpr(
    const ResolvedWindowFrameExpr* window_frame_expr) {
  std::unique_ptr<ValueExpr> boundary_expr;
  if (window_frame_expr->expression() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(boundary_expr,
                     AlgebrizeExpression(window_frame_expr->expression()));
  }

  WindowFrameBoundaryArg::BoundaryType boundary_type;
  switch (window_frame_expr->boundary_type()) {
    case ResolvedWindowFrameExpr::UNBOUNDED_PRECEDING:
      boundary_type = WindowFrameBoundaryArg::kUnboundedPreceding;
      break;
    case ResolvedWindowFrameExpr::OFFSET_PRECEDING:
      boundary_type = WindowFrameBoundaryArg::kOffsetPreceding;
      break;
    case ResolvedWindowFrameExpr::CURRENT_ROW:
      boundary_type = WindowFrameBoundaryArg::kCurrentRow;
      break;
    case ResolvedWindowFrameExpr::OFFSET_FOLLOWING:
      boundary_type = WindowFrameBoundaryArg::kOffsetFollowing;
      break;
    case ResolvedWindowFrameExpr::UNBOUNDED_FOLLOWING:
      boundary_type = WindowFrameBoundaryArg::kUnboundedFollowing;
      break;
  }

  return WindowFrameBoundaryArg::Create(boundary_type,
                                        std::move(boundary_expr));
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
Algebrizer::AlgebrizeSetOperationScan(
    const ResolvedSetOperationScan* set_scan) {
  if (set_scan->op_type() == ResolvedSetOperationScan::UNION_ALL ||
      set_scan->op_type() == ResolvedSetOperationScan::UNION_DISTINCT) {
    return AlgebrizeUnionScan(set_scan);
  } else {
    return AlgebrizeExceptIntersectScan(set_scan);
  }
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> Algebrizer::AlgebrizeUnionScan(
    const ResolvedSetOperationScan* set_scan) {
  const ResolvedColumnList& output_columns = set_scan->column_list();
  int num_columns = output_columns.size();
  int num_input_relations = set_scan->input_item_list_size();
  // Algebrize all children first to ensure that no errors arise later.
  std::vector<std::unique_ptr<RelationalOp>> children;
  for (int i = 0; i < num_input_relations; ++i) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> child,
                     AlgebrizeScan(set_scan->input_item_list(i)->scan()));
    children.push_back(std::move(child));
  }
  // There is one set of column mappings per input relation of the union.
  std::vector<UnionAllOp::Input> column_mappings(num_input_relations);
  for (int i = 0; i < num_input_relations; ++i) {
    column_mappings[i].first = std::move(children[i]);
    // Connect the output columns with the input columns.
    for (int j = 0; j < num_columns; ++j) {
      ResolvedColumn column =
          set_scan->input_item_list(i)->output_column_list(j);
      VariableId variable =
          column_to_variable_->GetVariableNameFromColumn(&column);
      ZETASQL_ASSIGN_OR_RETURN(auto deref,
                       DerefExpr::Create(variable, output_columns[j].type()));
      column_mappings[i].second.push_back(absl::make_unique<ExprArg>(
          column_to_variable_->GetVariableNameFromColumn(&output_columns[j]),
          std::move(deref)));
    }
  }
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> union_op,
                   UnionAllOp::Create(std::move(column_mappings)));
  if (set_scan->op_type() == ResolvedSetOperationScan::UNION_ALL) {
    return union_op;
  }
  // UNION DISTINCT needs an extra GROUP BY
  std::vector<std::unique_ptr<KeyArg>> keys;
  for (int j = 0; j < num_columns; j++) {
    // output_columns are unique, no need to eliminate duplicates.
    VariableId old_variable =
        column_to_variable_->GetVariableNameFromColumn(&output_columns[j]);
    VariableId new_variable =
        column_to_variable_->AssignNewVariableToColumn(&output_columns[j]);
    ZETASQL_ASSIGN_OR_RETURN(auto deref,
                     DerefExpr::Create(old_variable, output_columns[j].type()));
    keys.push_back(absl::make_unique<KeyArg>(new_variable, std::move(deref)));
  }
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> aggr_op,
                   AggregateOp::Create(std::move(keys), {} /* aggregators */,
                                       std::move(union_op)));
  return aggr_op;
}

// Algebrization of INTERSECT / EXCEPT is done as follows.
// Let Ri(X) be a query returning columns Xi = xi1,...,xiM
//
// R0 INTERSECT DISTINCT ... RN:
//   WITH T AS
//     (SELECT X0, SUM(bit0) cnt0, ..., SUM(bitN) cntN,
//      FROM (SELECT X0, 1 bit0, 0 bit1, ..., 0 bitN FROM R0 UNION ALL
//            SELECT X1, 0 bit0, 1 bit1, ..., 0 bitN FROM R1 UNION ALL ...
//            SELECT XN, 0 bit0, 0 bit1, ..., 1 bitN FROM RN) AS U
//      GROUP BY X0)
//   SELECT X0 FROM T WHERE cnt0 > 0 AND ... cntN > 0
//
// R0 EXCEPT DISTINCT ... RN:
//   WITH T AS ...
//   SELECT X0 FROM T WHERE cnt0 > 0 AND cnt1 = 0 AND ... cntN = 0
//
// R0 INTERSECT ALL ... RN:
//   WITH T AS ...
//   SELECT X0 FROM T, Enumerate(Least(cnt0, ..., cntN))
//
// R0 EXCEPT ALL RN:
//   WITH T AS ...
//   SELECT X0 FROM T, Enumerate(cnt0 - cnt1 - ... - cntN)
//
// enumerate(min, max) is a function that returns an array of integers between
// min (incl.) and max (excl.).
zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
Algebrizer::AlgebrizeExceptIntersectScan(
    const ResolvedSetOperationScan* set_scan) {
  const ResolvedColumnList& output_columns = set_scan->column_list();
  int num_columns = output_columns.size();
  int num_input_relations = set_scan->input_item_list_size();
  // Algebrize all children first to ensure that no errors arise later.
  std::vector<std::unique_ptr<RelationalOp>> children;
  for (int i = 0; i < num_input_relations; ++i) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> child,
                     AlgebrizeScan(set_scan->input_item_list(i)->scan()));
    children.push_back(std::move(child));
  }
  // U = (SELECT X0, 1 bit0, 0 bit1, ..., 0 bitN FROM R0 UNION ALL
  //      SELECT X1, 0 bit0, 1 bit1, ..., 0 bitN FROM R1 UNION ALL ...
  //      SELECT XN, 0 bit0, 0 bit1, ..., 1 bitN FROM RN)

  // There is one set of column mappings per input relation of the union.
  std::vector<UnionAllOp::Input> union_inputs(num_input_relations);
  // Create bit variables.
  std::vector<VariableId> bit_variables;
  bit_variables.reserve(num_input_relations);
  for (int i = 0; i < num_input_relations; i++) {
    bit_variables.push_back(
        variable_gen_->GetNewVariableName(absl::StrCat("bit", i)));
  }
  for (int rel_idx = 0; rel_idx < num_input_relations; rel_idx++) {
    union_inputs[rel_idx].first = std::move(children[rel_idx]);
    // ProjectScan all existing columns of the input.
    for (int j = 0; j < num_columns; j++) {
      const ResolvedColumn column =
          set_scan->input_item_list(rel_idx)->output_column_list(j);
      const VariableId variable =
          column_to_variable_->GetVariableNameFromColumn(&column);
      ZETASQL_ASSIGN_OR_RETURN(auto deref,
                       DerefExpr::Create(variable, output_columns[j].type()));
      union_inputs[rel_idx].second.push_back(absl::make_unique<ExprArg>(
          column_to_variable_->GetVariableNameFromColumn(&output_columns[j]),
          std::move(deref)));
    }
    // Add bit columns.
    for (int bit_idx = 0; bit_idx < num_input_relations; bit_idx++) {
      // bit variable i is set to 1 iff its index matches the relation index.
      ZETASQL_ASSIGN_OR_RETURN(
          auto const_expr,
          ConstExpr::Create(Value::Int64(bit_idx == rel_idx ? 1 : 0)));
      union_inputs[rel_idx].second.push_back(absl::make_unique<ExprArg>(
          bit_variables[bit_idx], std::move(const_expr)));
    }
  }

  // Construct the actual union.
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> query_u,
                   UnionAllOp::Create(std::move(union_inputs)));

  // Aggregate the result of the union:
  // T = SELECT X0, SUM(bit0) cnt0, ..., SUM(bitN) cntN FROM U GROUP BY X0
  // Keys: X0
  std::vector<std::unique_ptr<KeyArg>> keys;
  for (int j = 0; j < num_columns; j++) {
    VariableId old_variable =
        column_to_variable_->GetVariableNameFromColumn(&output_columns[j]);
    VariableId new_variable =
        column_to_variable_->AssignNewVariableToColumn(&output_columns[j]);
    ZETASQL_ASSIGN_OR_RETURN(auto deref,
                     DerefExpr::Create(old_variable, output_columns[j].type()));
    keys.push_back(absl::make_unique<KeyArg>(new_variable, std::move(deref)));
  }
  // Aggregators: SUM(bit_i) cnt_i
  std::vector<VariableId> cnt_vars;
  std::vector<std::unique_ptr<AggregateArg>> aggregators;
  for (int i = 0; i < num_input_relations; i++) {
    const VariableId cnt_var =
        variable_gen_->GetNewVariableName(absl::StrCat("cnt", i));
    cnt_vars.push_back(cnt_var);

    ZETASQL_ASSIGN_OR_RETURN(auto deref_bit,
                     DerefExpr::Create(bit_variables[i], types::Int64Type()));

    std::vector<std::unique_ptr<ValueExpr>> agg_func_args;
    agg_func_args.push_back(std::move(deref_bit));

    ZETASQL_ASSIGN_OR_RETURN(
        auto agg_arg,
        AggregateArg::Create(cnt_var,
                             absl::make_unique<BuiltinAggregateFunction>(
                                 FunctionKind::kSum, types::Int64Type(),
                                 /*num_input_fields=*/1, types::Int64Type()),
                             std::move(agg_func_args)));
    aggregators.push_back(std::move(agg_arg));
  }
  ZETASQL_ASSIGN_OR_RETURN(auto query_t, AggregateOp::Create(
      std::move(keys), std::move(aggregators), std::move(query_u)));

  // Add filter or cross-apply depending on the kind of set operation.
  switch (set_scan->op_type()) {
    case ResolvedSetOperationScan::EXCEPT_DISTINCT:
      // SELECT X0 FROM T WHERE 0 < cnt0 AND 0 = cnt1 AND ... 0 = cntN
    case ResolvedSetOperationScan::INTERSECT_DISTINCT: {
      // SELECT X0 FROM T WHERE 0 < cnt0 AND 0 < cnt1 AND ... 0 < cntN
      std::vector<std::unique_ptr<ValueExpr>> predicates;
      for (int i = 0; i < num_input_relations; i++) {
        auto fct_kind = (i > 0 && set_scan->op_type() ==
                         ResolvedSetOperationScan::EXCEPT_DISTINCT) ?
            FunctionKind::kEqual : FunctionKind::kLess;

        ZETASQL_ASSIGN_OR_RETURN(auto const_zero, ConstExpr::Create(Value::Int64(0)));
        ZETASQL_ASSIGN_OR_RETURN(auto deref_cnt,
                         DerefExpr::Create(cnt_vars[i], types::Int64Type()));

        std::vector<std::unique_ptr<ValueExpr>> predicate_args;
        predicate_args.push_back(std::move(const_zero));
        predicate_args.push_back(std::move(deref_cnt));

        ZETASQL_ASSIGN_OR_RETURN(auto predicate,
                         BuiltinScalarFunction::CreateCall(
                             fct_kind, language_options_, types::BoolType(),
                             std::move(predicate_args)));
        predicates.push_back(std::move(predicate));
      }
      ZETASQL_ASSIGN_OR_RETURN(auto condition,
                       BuiltinScalarFunction::CreateCall(
                           FunctionKind::kAnd, language_options_,
                           types::BoolType(), std::move(predicates)));
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<RelationalOp> filter_op,
          FilterOp::Create(std::move(condition), std::move(query_t)));
      return filter_op;
    }

    case ResolvedSetOperationScan::EXCEPT_ALL: {
      // rest = cnt0 - cnt1 - ... - cntN
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> rest,
                       DerefExpr::Create(cnt_vars[0], types::Int64Type()));
      for (int i = 1; i < num_input_relations; i++) {
        ZETASQL_ASSIGN_OR_RETURN(auto deref_cnt,
                         DerefExpr::Create(cnt_vars[i], types::Int64Type()));

        std::vector<std::unique_ptr<ValueExpr>> args;
        args.push_back(std::move(rest));
        args.push_back(std::move(deref_cnt));

        ZETASQL_ASSIGN_OR_RETURN(rest, BuiltinScalarFunction::CreateCall(
                                   FunctionKind::kSubtract, language_options_,
                                   types::Int64Type(), std::move(args)));
      }
      // SELECT X0 FROM T, Enumerate(rest)
      ZETASQL_ASSIGN_OR_RETURN(auto enum_op, EnumerateOp::Create(std::move(rest)));
      ZETASQL_ASSIGN_OR_RETURN(auto join_expr, ConstExpr::Create(Value::Bool(true)));
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<RelationalOp> join_op,
          JoinOp::Create(JoinOp::kCrossApply, /*equality_exprs=*/{},
                         std::move(join_expr), std::move(query_t),
                         std::move(enum_op), {}, {}));  // no variable remapping
      return join_op;
    }

    case ResolvedSetOperationScan::INTERSECT_ALL: {
      // SELECT X0 FROM T, Enumerate(Least(cnt0, ..., cntN))
      std::vector<std::unique_ptr<ValueExpr>> cnt_args;
      cnt_args.reserve(num_input_relations);
      for (int i = 0; i < num_input_relations; i++) {
        ZETASQL_ASSIGN_OR_RETURN(auto deref,
                         DerefExpr::Create(cnt_vars[i], types::Int64Type()));
        cnt_args.push_back(std::move(deref));
      }
      ZETASQL_ASSIGN_OR_RETURN(auto least,
                       BuiltinScalarFunction::CreateCall(
                           FunctionKind::kLeast, language_options_,
                           types::Int64Type(), std::move(cnt_args)));
      ZETASQL_ASSIGN_OR_RETURN(auto enum_op, EnumerateOp::Create(std::move(least)));
      ZETASQL_ASSIGN_OR_RETURN(auto join_expr, ConstExpr::Create(Value::Bool(true)));
      ZETASQL_ASSIGN_OR_RETURN(
          std::unique_ptr<RelationalOp> join_op,
          JoinOp::Create(JoinOp::kCrossApply, /*equality_exprs=*/{},
                         std::move(join_expr), std::move(query_t),
                         std::move(enum_op), {}, {}));  // no variable remapping
      return join_op;
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unimplemented set operation: " << set_scan->op_type();
  }
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> Algebrizer::AlgebrizeProjectScan(
    const ResolvedProjectScan* resolved_project,
    std::vector<FilterConjunctInfo*>* active_conjuncts) {
  // Determine the new columns and their definitions.
  absl::flat_hash_set<ResolvedColumn> defined_columns;
  const std::vector<std::unique_ptr<const ResolvedComputedColumn>>& expr_list =
      resolved_project->expr_list();
  const ResolvedColumnList& column_list = resolved_project->column_list();
  ZETASQL_RET_CHECK(!column_list.empty());
  std::vector<std::pair<ResolvedColumn, const ResolvedExpr*>>
      defined_columns_and_exprs;
  for (const ResolvedColumn& column : column_list) {
    const ResolvedExpr* local_definition;
    if (FindColumnDefinition(expr_list, column.column_id(),
                             &local_definition)) {
      // The column is defined by this SELECT.
      ZETASQL_RET_CHECK(defined_columns.insert(column).second);
      defined_columns_and_exprs.emplace_back(column, local_definition);
    }
  }

  // Determine the active conjuncts for the input scan and then algebrize
  // it. Note that volatile conjuncts can be pushed through projections because
  // they will only be evaluated once in both places.
  std::vector<FilterConjunctInfo*> input_active_conjuncts;
  for (FilterConjunctInfo* info : *active_conjuncts) {
    ZETASQL_RET_CHECK(!info->redundant);
    if (!Intersects(info->referenced_columns, defined_columns)) {
      input_active_conjuncts.push_back(info);
    }
  }
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<RelationalOp> input,
      AlgebrizeScan(resolved_project->input_scan(), &input_active_conjuncts));

  // Assign variables to the new columns and algebrize their definitions.
  std::vector<std::unique_ptr<ExprArg>> arguments;
  arguments.reserve(defined_columns_and_exprs.size());
  for (const auto& entry : defined_columns_and_exprs) {
    const ResolvedColumn& column = entry.first;
    const ResolvedExpr* expr = entry.second;
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> argument,
                     AlgebrizeExpression(expr));
    const VariableId variable =
        column_to_variable_->AssignNewVariableToColumn(&column);
    arguments.push_back(
        absl::make_unique<ExprArg>(variable, std::move(argument)));
  }

  // If no columns were defined by this project then just drop it.
  if (!arguments.empty()) {
    ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> compute_op,
                     ComputeOp::Create(std::move(arguments), std::move(input)));
    return compute_op;
  } else {
    // Since 'arguments' is empty we drop the project, but that project might
    // destroy order so in that case we must update the ordered property of the
    // relation.
    ZETASQL_RETURN_IF_ERROR(
        input->set_is_order_preserving(resolved_project->is_ordered()));
    return std::move(input);
  }
}

zetasql_base::StatusOr<std::unique_ptr<SortOp>> Algebrizer::AlgebrizeOrderByScan(
    const ResolvedOrderByScan* scan, std::unique_ptr<ValueExpr> limit,
    std::unique_ptr<ValueExpr> offset) {
  ZETASQL_RET_CHECK_EQ(limit == nullptr, offset == nullptr);

  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> input,
                   AlgebrizeScan(scan->input_scan()));
  // 'order_by_item_list' form the key.
  std::vector<std::unique_ptr<KeyArg>> keys;
  absl::flat_hash_map<int, VariableId> column_to_id_map;

  ZETASQL_RETURN_IF_ERROR(AlgebrizeOrderByItems(
      true /* drop_correlated_columns */, true /* create_new_ids */,
      scan->order_by_item_list(), &column_to_id_map, &keys));

  // Output columns that are not present in 'order_by_item_list' are non-key
  // sort values.
  std::vector<std::unique_ptr<ExprArg>> values;
  for (int i = 0; i < scan->column_list().size(); i++) {
    const ResolvedColumn* column = &scan->column_list()[i];
    if (!zetasql_base::InsertIfNotPresent(
            &column_to_id_map, column->column_id(),
            column_to_variable_->GetVariableNameFromColumn(column))) {
      continue;  // Skip columns that we have already seen.
    }
    const VariableId value_in =
        zetasql_base::FindOrDie(column_to_id_map, column->column_id());
    const VariableId value_out =
        column_to_variable_->AssignNewVariableToColumn(column);
    ZETASQL_ASSIGN_OR_RETURN(auto deref, DerefExpr::Create(value_in, column->type()));
    values.push_back(absl::make_unique<ExprArg>(value_out, std::move(deref)));
  }
  for (const auto& arg : keys) {
    ZETASQL_RETURN_IF_ERROR(ValidateTypeSupportsOrderComparison(arg->type()));
  }
  return SortOp::Create(std::move(keys), std::move(values), std::move(limit),
                        std::move(offset), std::move(input), scan->is_ordered(),
                        /*is_stable_sort=*/false);
}

// Algebrize a resolved scan operator.
zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> Algebrizer::AlgebrizeScan(
    const ResolvedScan* scan,
    std::vector<FilterConjunctInfo*>* active_conjuncts) {
  ZETASQL_RETURN_IF_ERROR(CheckHints(scan->hint_list()));
  const int original_active_conjuncts_size = active_conjuncts->size();
  std::unique_ptr<RelationalOp> rel_op;
  switch (scan->node_kind()) {
    case RESOLVED_SINGLE_ROW_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(rel_op, AlgebrizeSingleRowScan());
      break;
    }
    case RESOLVED_TABLE_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(rel_op,
                       AlgebrizeTableScan(scan->GetAs<ResolvedTableScan>(),
                                          active_conjuncts));
      break;
    }
    case RESOLVED_JOIN_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(
          rel_op,
          AlgebrizeJoinScan(scan->GetAs<ResolvedJoinScan>(), active_conjuncts));
      break;
    }
    case RESOLVED_ARRAY_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(rel_op,
                       AlgebrizeArrayScan(scan->GetAs<ResolvedArrayScan>(),
                                          active_conjuncts));
      break;
    }
    case RESOLVED_FILTER_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(rel_op,
                       AlgebrizeFilterScan(scan->GetAs<ResolvedFilterScan>(),
                                           active_conjuncts));
      break;
    }
    case RESOLVED_AGGREGATE_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(
          rel_op, AlgebrizeAggregateScan(scan->GetAs<ResolvedAggregateScan>()));
      break;
    }
    case RESOLVED_SET_OPERATION_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(rel_op, AlgebrizeSetOperationScan(
                                   scan->GetAs<ResolvedSetOperationScan>()));
      break;
    }
    case RESOLVED_PROJECT_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(rel_op,
                       AlgebrizeProjectScan(scan->GetAs<ResolvedProjectScan>(),
                                            active_conjuncts));
      break;
    }
    case RESOLVED_ORDER_BY_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(
          rel_op, AlgebrizeOrderByScan(scan->GetAs<ResolvedOrderByScan>(),
                                       /*limit=*/nullptr, /*offset=*/nullptr));
      break;
    }
    case RESOLVED_LIMIT_OFFSET_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(rel_op, AlgebrizeLimitOffsetScan(
                                   scan->GetAs<ResolvedLimitOffsetScan>()));
      break;
    }
    case RESOLVED_WITH_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(rel_op,
                       AlgebrizeWithScan(scan->GetAs<ResolvedWithScan>()));
      break;
    }
    case RESOLVED_WITH_REF_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(
          rel_op, AlgebrizeWithRefScan(scan->GetAs<ResolvedWithRefScan>()));
      break;
    }
    case RESOLVED_ANALYTIC_SCAN: {
      ZETASQL_ASSIGN_OR_RETURN(
          rel_op, AlgebrizeAnalyticScan(scan->GetAs<ResolvedAnalyticScan>()));
      break;
    }
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unhandled node type algebrizing a scan: "
             << scan->DebugString();
  }
  ZETASQL_RET_CHECK_EQ(active_conjuncts->size(), original_active_conjuncts_size);

  // Crete a FilterOp for any conjuncts that cannot be pushed down further.
  return MaybeApplyFilterConjuncts(std::move(rel_op), active_conjuncts);
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
Algebrizer::MaybeApplyFilterConjuncts(
    std::unique_ptr<RelationalOp> input,
    std::vector<FilterConjunctInfo*>* active_conjuncts) {
  std::vector<std::unique_ptr<ValueExpr>> algebrized_conjuncts;
  if (algebrizer_options_.push_down_filters) {
    // Iterate over 'active_conjuncts' in reverse order because it's a stack.
    for (auto i = active_conjuncts->rbegin(); i != active_conjuncts->rend();
         ++i) {
      FilterConjunctInfo* conjunct_info = *i;
      if (!conjunct_info->redundant) {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> algebrized_conjunct,
                         AlgebrizeExpression(conjunct_info->conjunct));
        algebrized_conjuncts.push_back(std::move(algebrized_conjunct));
        conjunct_info->redundant = true;
      }
    }
  }

  return ApplyAlgebrizedFilterConjuncts(std::move(input),
                                        std::move(algebrized_conjuncts));
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>> Algebrizer::AlgebrizeScan(
    const ResolvedScan* scan) {
  std::vector<FilterConjunctInfo*> active_conjuncts;
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> relational_op,
                   AlgebrizeScan(scan, &active_conjuncts));
  return relational_op;
}

bool Algebrizer::FindColumnDefinition(
    const std::vector<std::unique_ptr<const ResolvedComputedColumn>>& expr_list,
    int column_id, const ResolvedExpr** definition) {
  (*definition) = nullptr;
  for (int i = 0; i < expr_list.size(); ++i) {
    if (column_id == expr_list[i]->column().column_id()) {
      // The column is defined by this expression.
      (*definition) = expr_list[i]->expr();
      return true;
    }
  }
  return false;
}

zetasql_base::StatusOr<std::unique_ptr<ArrayNestExpr>> Algebrizer::NestRelationInStruct(
    const ResolvedColumnList& output_columns,
    std::unique_ptr<RelationalOp> relation, bool is_with_table) {
  // Pack the output columns in the ArrayNestExpr.
  const bool kNonValueType = false;
  ZETASQL_ASSIGN_OR_RETURN(
      const ArrayType* array_type,
      CreateTableArrayType(output_columns, kNonValueType, type_factory_));
  const StructType* table_struct = array_type->element_type()->AsStruct();
  std::vector<std::unique_ptr<ExprArg>> arguments;
  for (int i = 0; i < output_columns.size(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(
        auto deref,
        DerefExpr::Create(
            column_to_variable_->GetVariableNameFromColumn(&output_columns[i]),
            output_columns[i].type()));
    arguments.push_back(absl::make_unique<ExprArg>(std::move(deref)));
  }
  ZETASQL_ASSIGN_OR_RETURN(auto struct_op,
                   NewStructExpr::Create(table_struct, std::move(arguments)));
  return ArrayNestExpr::Create(array_type, std::move(struct_op),
                               std::move(relation), is_with_table);
}

zetasql_base::StatusOr<std::unique_ptr<ArrayNestExpr>>
Algebrizer::NestSingleColumnRelation(const ResolvedColumnList& output_columns,
                                     std::unique_ptr<RelationalOp> relation,
                                     bool is_with_table) {
  // A single column which may be a struct.
  ZETASQL_RET_CHECK_EQ(output_columns.size(), 1);
  ZETASQL_ASSIGN_OR_RETURN(
      auto deref_expr,
      DerefExpr::Create(
          column_to_variable_->GetVariableNameFromColumn(&output_columns[0]),
          output_columns[0].type()));
  const Type* column_type = output_columns[0].type();
  const ArrayType* array_type;
  ZETASQL_RETURN_IF_ERROR(type_factory_->MakeArrayType(column_type, &array_type));
  return ArrayNestExpr::Create(array_type, std::move(deref_expr),
                               std::move(relation), is_with_table);
}

zetasql_base::StatusOr<std::unique_ptr<ValueExpr>>
Algebrizer::AlgebrizeRootScanAsValueExpr(
    const ResolvedColumnList& output_columns, bool is_value_table,
    const ResolvedScan* scan) {
  ZETASQL_RETURN_IF_ERROR(CheckHints(scan->hint_list()));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> relation, AlgebrizeScan(scan));
  // Put a ArrayNestExpr at the root so that we return a single ValueExpr.
  std::unique_ptr<ValueExpr> value;
  // If ResolvedQueryStmt::is_value_table is true, the output is a value
  // table, i.e., we should get ARRAY<PROTO> rather than
  // ARRAY<STRUCT<PROTO>>.
  if (is_value_table) {
    ZETASQL_ASSIGN_OR_RETURN(
        value, NestSingleColumnRelation(output_columns, std::move(relation),
                                        /*is_with_table=*/false));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(value,
                     NestRelationInStruct(output_columns, std::move(relation),
                                          /*is_with_table=*/false));
  }
  // If we have any WITH clauses, create a LetExpr that binds the names of
  // subqueries to array expressions.  WITH subqueries cannot be correlated
  // so we can attach them all in one batch at the top of the query, and that
  // will ensure we run each of them exactly once.
  if (!with_subquery_let_assignments_.empty()) {
    ZETASQL_ASSIGN_OR_RETURN(value,
                     LetExpr::Create(std::move(with_subquery_let_assignments_),
                                     std::move(value)));
  }
  // Sanity check - WITH map should be cleared as WITH clauses go out of scope.
  ZETASQL_RET_CHECK(with_map_.empty());

  return WrapWithRootExpr(std::move(value));
}

zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::WrapWithRootExpr(
    std::unique_ptr<ValueExpr> value_expr) {
  return RootExpr::Create(std::move(value_expr), GetRootData());
}

std::unique_ptr<RootData> Algebrizer::GetRootData() {
  auto root_data = absl::make_unique<RootData>();
  root_data->registries = std::move(proto_field_registries_);
  root_data->field_readers = std::move(get_proto_field_readers_);

  proto_field_registries_.clear();
  get_proto_field_readers_.clear();

  return root_data;
}

zetasql_base::StatusOr<std::unique_ptr<RelationalOp>>
Algebrizer::AlgebrizeQueryStatementAsRelation(
    const ResolvedQueryStmt* query, ResolvedColumnList* output_column_list,
    std::vector<std::string>* output_column_names,
    std::vector<VariableId>* output_column_variables) {
  ZETASQL_RETURN_IF_ERROR(CheckHints(query->hint_list()));
  const ResolvedScan* scan = query->query();
  ZETASQL_RETURN_IF_ERROR(CheckHints(scan->hint_list()));
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> relation, AlgebrizeScan(scan));

  for (const std::unique_ptr<const ResolvedOutputColumn>& output_column :
       query->output_column_list()) {
    const ResolvedColumn& column = output_column->column();
    output_column_list->push_back(column);

    output_column_names->push_back(output_column->name());

    const VariableId& var =
        column_to_variable_->GetVariableNameFromColumn(&column);
    output_column_variables->push_back(var);
  }

  // Dummy access for CheckFieldsAccessed().
  query->is_value_table();
  ZETASQL_RETURN_IF_ERROR(query->CheckFieldsAccessed());

  // If we have any WITH clauses, create a LetOp that binds the names of
  // subqueries to array expressions.  WITH subqueries cannot be correlated
  // so we can attach them all in one batch at the top of the query, and that
  // will ensure we run each of them exactly once.
  if (!with_subquery_let_assignments_.empty()) {
    ZETASQL_ASSIGN_OR_RETURN(relation,
                     LetOp::Create(std::move(with_subquery_let_assignments_),
                                   std::move(relation)));
  }
  // Sanity check - WITH map should be cleared as WITH clauses go out of scope.
  ZETASQL_RET_CHECK(with_map_.empty());

  ZETASQL_ASSIGN_OR_RETURN(relation,
                   RootOp::Create(std::move(relation), GetRootData()));

  VLOG(2) << "Algebrized tree:\n" << relation->DebugString(true);
  return relation;
}

zetasql_base::StatusOr<std::unique_ptr<ValueExpr>> Algebrizer::AlgebrizeDMLStatement(
    const ResolvedStatement* ast_root) {
  const ResolvedTableScan* resolved_table_scan;
  auto resolved_scan_map = absl::make_unique<ResolvedScanMap>();
  auto resolved_expr_map = absl::make_unique<ResolvedExprMap>();
  ZETASQL_RETURN_IF_ERROR(AlgebrizeDescendantsOfDMLStatement(
      ast_root, resolved_scan_map.get(), resolved_expr_map.get(),
      &resolved_table_scan));

  const Table* table = resolved_table_scan->table();
  const ResolvedColumnList& column_list = resolved_table_scan->column_list();
  ZETASQL_RET_CHECK_EQ(column_list.size(), table->NumColumns());
  ZETASQL_ASSIGN_OR_RETURN(
      const ArrayType* table_array_type,
      CreateTableArrayType(column_list, table->IsValueTable(), type_factory_));
  const StructType* key_type = nullptr;
  if (table->PrimaryKey().has_value()) {
    ZETASQL_ASSIGN_OR_RETURN(
        key_type, CreatePrimaryKeyType(column_list, table->PrimaryKey().value(),
                                       type_factory_));
  }
  ZETASQL_ASSIGN_OR_RETURN(const StructType* dml_output_type,
                   CreateDMLOutputType(table_array_type, type_factory_));

  // It is safe to move 'column_to_variable_' into the DML ValueExpr because
  // this algebrizer can only be used once. Also, 'column_to_variable_' is
  // passed as a const, so we can be sure that no new query parameters or
  // columns will be discovered after this point (particularly during
  // evaluation).
  variable_gen_ = nullptr;  // Owned by 'column_to_variable_'.
  std::unique_ptr<ValueExpr> value_expr;
  switch (ast_root->node_kind()) {
    // TODO: Add MERGE support.
    case RESOLVED_DELETE_STMT: {
      ZETASQL_ASSIGN_OR_RETURN(
          value_expr,
          DMLDeleteValueExpr::Create(
              table, table_array_type, key_type, dml_output_type,
              ast_root->GetAs<ResolvedDeleteStmt>(), &column_list,
              std::move(column_to_variable_), std::move(resolved_scan_map),
              std::move(resolved_expr_map)));
      break;
    }
    case RESOLVED_UPDATE_STMT: {
      ZETASQL_ASSIGN_OR_RETURN(
          value_expr,
          DMLUpdateValueExpr::Create(
              table, table_array_type, key_type, dml_output_type,
              ast_root->GetAs<ResolvedUpdateStmt>(), &column_list,
              std::move(column_to_variable_), std::move(resolved_scan_map),
              std::move(resolved_expr_map)));
      break;
    }
    case RESOLVED_INSERT_STMT: {
      ZETASQL_ASSIGN_OR_RETURN(
          value_expr,
          DMLInsertValueExpr::Create(
              table, table_array_type, key_type, dml_output_type,
              ast_root->GetAs<ResolvedInsertStmt>(), &column_list,
              std::move(column_to_variable_), std::move(resolved_scan_map),
              std::move(resolved_expr_map)));

      break;
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "AlgebrizeDMLStatement() does not support node kind "
                       << ResolvedNodeKind_Name(ast_root->node_kind());
      break;
  }

  return WrapWithRootExpr(std::move(value_expr));
}

absl::Status Algebrizer::AlgebrizeDescendantsOfDMLStatement(
    const ResolvedStatement* ast_root, ResolvedScanMap* resolved_scan_map,
    ResolvedExprMap* resolved_expr_map,
    const ResolvedTableScan** resolved_table_scan) {
  // Note that we have to algebrize all scans before we algebrize any
  // expressions, because there is at least one place in AlgebrizeScan()
  // (specifically, AlgebrizeProjectScan()), where we assign a new variable to a
  // column even if there is already a known column of the same name. Perhaps
  // that could be generalized, but it seems reasonable to create all columns
  // before resolving any expressions.

  // TODO: Add MERGE support.
  const ResolvedTableScan* resolved_table_scan_or_null = nullptr;
  switch (ast_root->node_kind()) {
    case RESOLVED_DELETE_STMT: {
      const ResolvedDeleteStmt* stmt = ast_root->GetAs<ResolvedDeleteStmt>();

      resolved_table_scan_or_null = stmt->table_scan();
      if (resolved_table_scan_or_null != nullptr) {
        ZETASQL_RETURN_IF_ERROR(PopulateResolvedScanMap(resolved_table_scan_or_null,
                                                resolved_scan_map));
      }

      if (stmt->array_offset_column() != nullptr) {
        // The array offset column can only be set for nested DELETEs.
        ZETASQL_RET_CHECK(resolved_table_scan_or_null == nullptr);
        column_to_variable_->AssignNewVariableToColumn(
            &stmt->array_offset_column()->column());
      }

      ZETASQL_RETURN_IF_ERROR(
          PopulateResolvedExprMap(stmt->where_expr(), resolved_expr_map));

      const ResolvedAssertRowsModified* assert_rows_modified =
          stmt->assert_rows_modified();
      if (assert_rows_modified != nullptr) {
        ZETASQL_RETURN_IF_ERROR(PopulateResolvedExprMap(assert_rows_modified->rows(),
                                                resolved_expr_map));
      }
      break;
    }
    case RESOLVED_UPDATE_STMT: {
      const ResolvedUpdateStmt* stmt = ast_root->GetAs<ResolvedUpdateStmt>();

      resolved_table_scan_or_null = stmt->table_scan();
      if (resolved_table_scan_or_null != nullptr) {
        ZETASQL_RETURN_IF_ERROR(PopulateResolvedScanMap(resolved_table_scan_or_null,
                                                resolved_scan_map));
      }

      if (stmt->from_scan() != nullptr) {
        ZETASQL_RETURN_IF_ERROR(
            PopulateResolvedScanMap(stmt->from_scan(), resolved_scan_map));
      }

      if (stmt->array_offset_column() != nullptr) {
        // The array offset column can only be set for nested UPDATEs.
        ZETASQL_RET_CHECK(resolved_table_scan_or_null == nullptr);
        column_to_variable_->AssignNewVariableToColumn(
            &stmt->array_offset_column()->column());
      }

      ZETASQL_RETURN_IF_ERROR(
          PopulateResolvedExprMap(stmt->where_expr(), resolved_expr_map));

      const ResolvedAssertRowsModified* assert_rows_modified =
          stmt->assert_rows_modified();
      if (assert_rows_modified != nullptr) {
        ZETASQL_RETURN_IF_ERROR(PopulateResolvedExprMap(assert_rows_modified->rows(),
                                                resolved_expr_map));
      }

      for (const std::unique_ptr<const ResolvedUpdateItem>& item :
           stmt->update_item_list()) {
        ZETASQL_RETURN_IF_ERROR(AlgebrizeDescendantsOfUpdateItem(
            item.get(), resolved_scan_map, resolved_expr_map));
      }
      break;
    }
    case RESOLVED_INSERT_STMT: {
      const ResolvedInsertStmt* stmt = ast_root->GetAs<ResolvedInsertStmt>();

      resolved_table_scan_or_null = stmt->table_scan();
      if (resolved_table_scan_or_null != nullptr) {
        ZETASQL_RETURN_IF_ERROR(
            PopulateResolvedScanMap(stmt->table_scan(), resolved_scan_map));
      }

      if (stmt->query() != nullptr) {
        ZETASQL_RETURN_IF_ERROR(
            PopulateResolvedScanMap(stmt->query(), resolved_scan_map));
      }

      const ResolvedAssertRowsModified* assert_rows_modified =
          stmt->assert_rows_modified();
      if (assert_rows_modified != nullptr) {
        ZETASQL_RETURN_IF_ERROR(PopulateResolvedExprMap(assert_rows_modified->rows(),
                                                resolved_expr_map));
      }

      for (const std::unique_ptr<const ResolvedInsertRow>& row :
           stmt->row_list()) {
        for (const std::unique_ptr<const ResolvedDMLValue>& dml_value :
             row->value_list()) {
          ZETASQL_RETURN_IF_ERROR(
              PopulateResolvedExprMap(dml_value->value(), resolved_expr_map));
        }
      }

      // Dummy accesses for ResolvedInsertStmt::CheckFieldsAccessed().
      //
      // The reference implementation does not enforce that only certain columns
      // are visible in certain places, so there is no need to look at the query
      // parameter list.
      for (const std::unique_ptr<const ResolvedColumnRef>& parameter :
           stmt->query_parameter_list()) {
        parameter->column();
      }
      // Similarly, the query output column list is the same as the column list
      // inside the query, so there is no need to look at it either.
      stmt->query_output_column_list();

      break;
    }
    default:
      ZETASQL_RET_CHECK_FAIL()
          << "AlgebrizeDescendantsOfDMLStatement() does not support node kind "
          << ResolvedNodeKind_Name(ast_root->node_kind());
  }

  // The caller must pass a non-NULL 'resolved_table_scan' if and only if this
  // is a top-level DML statement.
  ZETASQL_RET_CHECK_EQ(resolved_table_scan == nullptr,
               resolved_table_scan_or_null == nullptr);
  if (resolved_table_scan != nullptr) {
    *resolved_table_scan = resolved_table_scan_or_null;
  }

  return absl::OkStatus();
}

absl::Status Algebrizer::AlgebrizeDescendantsOfUpdateItem(
    const ResolvedUpdateItem* update_item, ResolvedScanMap* resolved_scan_map,
    ResolvedExprMap* resolved_expr_map) {
  ZETASQL_RETURN_IF_ERROR(
      PopulateResolvedExprMap(update_item->target(), resolved_expr_map));

  if (update_item->set_value() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(PopulateResolvedExprMap(update_item->set_value()->value(),
                                            resolved_expr_map));
  }

  if (update_item->element_column() != nullptr) {
    column_to_variable_->AssignNewVariableToColumn(
        &update_item->element_column()->column());
  }

  for (const std::unique_ptr<const ResolvedUpdateArrayItem>& array_item :
       update_item->array_update_list()) {
    ZETASQL_RETURN_IF_ERROR(
        PopulateResolvedExprMap(array_item->offset(), resolved_expr_map));
    ZETASQL_RETURN_IF_ERROR(AlgebrizeDescendantsOfUpdateItem(
        array_item->update_item(), resolved_scan_map, resolved_expr_map));
  }

  std::vector<const ResolvedStatement*> nested_dml_stmts;
  for (const std::unique_ptr<const ResolvedDeleteStmt>& delete_stmt :
       update_item->delete_list()) {
    nested_dml_stmts.push_back(delete_stmt.get());
  }
  for (const std::unique_ptr<const ResolvedUpdateStmt>& update_stmt :
       update_item->update_list()) {
    nested_dml_stmts.push_back(update_stmt.get());
  }
  for (const std::unique_ptr<const ResolvedInsertStmt>& insert_stmt :
       update_item->insert_list()) {
    nested_dml_stmts.push_back(insert_stmt.get());
  }
  for (const ResolvedStatement* dml_stmt : nested_dml_stmts) {
    ZETASQL_RETURN_IF_ERROR(AlgebrizeDescendantsOfDMLStatement(
        dml_stmt, resolved_scan_map, resolved_expr_map,
        /*resolved_table_scan=*/nullptr));
  }

  return absl::OkStatus();
}

absl::Status Algebrizer::PopulateResolvedScanMap(
    const ResolvedScan* resolved_scan, ResolvedScanMap* resolved_scan_map) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<RelationalOp> relational_op,
                   AlgebrizeScan(resolved_scan));
  const auto ret =
      resolved_scan_map->emplace(resolved_scan, std::move(relational_op));
  ZETASQL_RET_CHECK(ret.second);
  return absl::OkStatus();
}

absl::Status Algebrizer::PopulateResolvedExprMap(
    const ResolvedExpr* resolved_expr, ResolvedExprMap* resolved_expr_map) {
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ValueExpr> value_expr,
                   AlgebrizeExpression(resolved_expr));
  const auto ret =
      resolved_expr_map->emplace(resolved_expr, std::move(value_expr));
  ZETASQL_RET_CHECK(ret.second);
  return absl::OkStatus();
}

zetasql_base::StatusOr<ProtoFieldRegistry*> Algebrizer::AddProtoFieldRegistry(
    const absl::optional<SharedProtoFieldPath>& id) {
  const int registry_id = static_cast<int>(proto_field_registries_.size());

  auto registry = absl::make_unique<ProtoFieldRegistry>(registry_id);

  ProtoFieldRegistry* ptr = registry.get();
  proto_field_registries_.push_back(std::move(registry));
  if (id.has_value()) {
    ZETASQL_RET_CHECK(proto_field_registry_map_.emplace(id.value(), ptr).second);
  }
  return ptr;
}

zetasql_base::StatusOr<ProtoFieldReader*> Algebrizer::AddProtoFieldReader(
    const absl::optional<SharedProtoFieldPath>& id,
    const ProtoFieldAccessInfo& access_info, ProtoFieldRegistry* registry) {
  const int reader_id = static_cast<int>(get_proto_field_readers_.size());

  auto reader =
      absl::make_unique<ProtoFieldReader>(reader_id, access_info, registry);

  ProtoFieldReader* ptr = reader.get();
  get_proto_field_readers_.push_back(std::move(reader));
  // The check for get_has_bit might not be necessary here because the resolver
  // may always use BOOL for that case, but it doesn't hurt to be safe.
  if (id.has_value() && !access_info.field_info.get_has_bit &&
      access_info.field_info.type->IsProto()) {
    ZETASQL_RET_CHECK(get_proto_field_reader_map_.emplace(id.value(), ptr).second);
  }
  return ptr;
}

std::string Algebrizer::SharedProtoFieldPath::DebugString() const {
  std::string s = column_or_param.DebugString();

  std::vector<std::string> path_strs;
  path_strs.reserve(field_path.size());
  for (const ProtoOrStructField& field : field_path) {
    path_strs.push_back(field.DebugString());
  }

  return absl::StrCat(s, ".", absl::StrJoin(path_strs, "."));
}

static absl::Status VerifyParameters(const Parameters* parameters) {
  if (parameters->is_named()) {
    ZETASQL_RET_CHECK(parameters->named_parameters().empty());
  } else {
    ZETASQL_RET_CHECK(parameters->positional_parameters().empty());
  }
  return absl::OkStatus();
}

absl::Status Algebrizer::AlgebrizeStatement(
    const LanguageOptions& language_options,
    const AlgebrizerOptions& algebrizer_options, TypeFactory* type_factory,
    const ResolvedStatement* ast_root, std::unique_ptr<ValueExpr>* output,
    Parameters* parameters, ParameterMap* column_map,
    SystemVariablesAlgebrizerMap* system_variables_map) {
  ZETASQL_RETURN_IF_ERROR(VerifyParameters(parameters));

  Algebrizer single_use_algebrizer(language_options, algebrizer_options,
                                   type_factory, parameters, column_map,
                                   system_variables_map);
  switch (ast_root->node_kind()) {
    case RESOLVED_QUERY_STMT: {
      const ResolvedQueryStmt* stmt = ast_root->GetAs<ResolvedQueryStmt>();
      ZETASQL_RETURN_IF_ERROR(CheckHints(stmt->hint_list()));
      const ResolvedScan* scan = stmt->query();
      ZETASQL_RETURN_IF_ERROR(CheckHints(scan->hint_list()));
      // Weirdly, the output_column_list of the statement may contain column
      // aliases not present in the output columns of 'query', so that the
      // statement wrapper acts as another PROJECT node. Compensate for this by
      // creating a column list with column names from the statement.
      // TODO: fix this.
      IdStringPool id_string_pool;
      ResolvedColumnList output_column_list;
      for (const auto& it :
           ast_root->GetAs<ResolvedQueryStmt>()->output_column_list()) {
        // TODO IdString conversion shouldn't be needed here.
        // We should have IdStrings in ResolvedOutputColumn.
        output_column_list.emplace_back(
            it->column().column_id(),
            id_string_pool.Make(it->column().table_name()),
            id_string_pool.Make(it->name()), it->column().type());
      }
      ZETASQL_ASSIGN_OR_RETURN(*output,
                       single_use_algebrizer.AlgebrizeRootScanAsValueExpr(
                           output_column_list, stmt->is_value_table(), scan));
      ZETASQL_RETURN_IF_ERROR(ast_root->CheckFieldsAccessed());
      break;
    }
    // TODO: Add MERGE support.
    //
    // For DML statements, the algebrizer tree is just a hack wrapper around
    // 'ast_root' that does not actually traverse the resolved AST. Thus, we
    // can't check that all the resolved AST nodes are accessed yet. We check
    // that all the fields in the resolved AST are accessed after evaluation.
    case RESOLVED_DELETE_STMT:
    case RESOLVED_UPDATE_STMT:
    case RESOLVED_INSERT_STMT: {
      ZETASQL_ASSIGN_OR_RETURN(*output,
                       single_use_algebrizer.AlgebrizeDMLStatement(ast_root));
      break;
    }
    default:
      ZETASQL_RET_CHECK_FAIL()
          << "AlgebrizeStatement() does not support ResolvedNodeKind "
          << ResolvedNodeKind_Name(ast_root->node_kind());
      break;
  }

  VLOG(2) << "Algebrized tree:\n" << output->get()->DebugString(true);
  return absl::OkStatus();
}

absl::Status Algebrizer::AlgebrizeQueryStatementAsRelation(
    const LanguageOptions& language_options,
    const AlgebrizerOptions& algebrizer_options, TypeFactory* type_factory,
    const ResolvedQueryStmt* ast_root, ResolvedColumnList* output_column_list,
    std::unique_ptr<RelationalOp>* output,
    std::vector<std::string>* output_column_names,
    std::vector<VariableId>* output_column_variables, Parameters* parameters,
    ParameterMap* column_map,
    SystemVariablesAlgebrizerMap* system_variables_map) {
  ZETASQL_RETURN_IF_ERROR(VerifyParameters(parameters));
  Algebrizer single_use_algebrizer(language_options, algebrizer_options,
                                   type_factory, parameters, column_map,
                                   system_variables_map);
  ZETASQL_ASSIGN_OR_RETURN(*output,
                   single_use_algebrizer.AlgebrizeQueryStatementAsRelation(
                       ast_root, output_column_list, output_column_names,
                       output_column_variables));
  return absl::OkStatus();
}

absl::Status Algebrizer::AlgebrizeExpression(
    const LanguageOptions& language_options,
    const AlgebrizerOptions& algebrizer_options, TypeFactory* type_factory,
    const ResolvedExpr* ast_root, std::unique_ptr<ValueExpr>* output,
    Parameters* parameters, ParameterMap* column_map,
    SystemVariablesAlgebrizerMap* system_variables_map) {
  ZETASQL_RETURN_IF_ERROR(VerifyParameters(parameters));

  Algebrizer single_use_algebrizer(language_options, algebrizer_options,
                                   type_factory, parameters, column_map,
                                   system_variables_map);
  ZETASQL_ASSIGN_OR_RETURN(
      *output, single_use_algebrizer.AlgebrizeStandaloneExpression(ast_root));

  VLOG(2) << "Algebrized tree:\n" << output->get()->DebugString();
  return ast_root->CheckFieldsAccessed();
}

}  // namespace zetasql
