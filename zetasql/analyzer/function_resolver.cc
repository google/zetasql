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

#include "zetasql/analyzer/function_resolver.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/function_signature_matcher.h"
#include "zetasql/analyzer/input_argument_type_resolver_helper.h"
#include "zetasql/analyzer/name_scope.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/parser/parser.h"
#include "zetasql/proto/internal_error_location.pb.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/annotation/collation.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/cast.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/cycle_detector.h"
#include "zetasql/public/error_helpers.h"
#include "zetasql/public/error_location.pb.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/id_string.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/parse_resume_location.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_enums.pb.h"
#include "zetasql/resolved_ast/resolved_ast_helper.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/base/case.h"
#include "absl/base/attributes.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

FunctionResolver::FunctionResolver(Catalog* catalog, TypeFactory* type_factory,
                                   Resolver* resolver)
    : catalog_(catalog), type_factory_(type_factory), resolver_(resolver) {
}

static const std::string* const kBitwiseNotFnName =
    new std::string("$bitwise_not");
static const std::string* const kInvalidUnaryOperatorFnName =
    new std::string("$invalid_unary_operator");
static const std::string* const kNotFnName = new std::string("$not");
static const std::string* const kUnaryMinusFnName =
    new std::string("$unary_minus");
static const std::string* const kUnaryPlusFnName =
    new std::string("$unary_plus");
static const std::string* const kIsNullFnName = new std::string("$is_null");

const std::string& FunctionResolver::UnaryOperatorToFunctionName(
    ASTUnaryExpression::Op op) {
  switch (op) {
    case ASTUnaryExpression::NOT:
      return *kNotFnName;
    case ASTUnaryExpression::MINUS:
      return *kUnaryMinusFnName;
    case ASTUnaryExpression::PLUS:
      // Note that this function definition does not actually exist.  The
      // resolver treats this as a no-op and effectively removes it from the
      // resolved tree.
      return *kUnaryPlusFnName;
    case ASTUnaryExpression::BITWISE_NOT:
      return *kBitwiseNotFnName;
    case ASTUnaryExpression::NOT_SET:
      return *kInvalidUnaryOperatorFnName;
    case ASTUnaryExpression::IS_UNKNOWN:
    case ASTUnaryExpression::IS_NOT_UNKNOWN:
      // As a side note, IS [NOT] UNKNOWN reuses the existing "is_null" function
      // name since they share the exact same engine implementation. "is_null"
      // is currently implemented as a binary operator which can recognize if a
      // preceding keyword NOT is presented. But unary operator does not support
      // that so two unary operators are created to be mapped to the same name.
      return *kIsNullFnName;
  }
}

static const std::string* const kAddFnName = new std::string("$add");
static const std::string* const kBitwiseAndFnName =
    new std::string("$bitwise_and");
static const std::string* const kBitwiseOrFnName =
    new std::string("$bitwise_or");
static const std::string* const kBitwiseXorFnName =
    new std::string("$bitwise_xor");
static const std::string* const kConcatOpFnName = new std::string("$concat_op");
static const std::string* const kDivideFnName = new std::string("$divide");
static const std::string* const kEqualFnName = new std::string("$equal");
static const std::string* const kGreaterFnName = new std::string("$greater");
static const std::string* const kGreaterOrEqualFnName =
    new std::string("$greater_or_equal");
static const std::string* const kLessFnName = new std::string("$less");
static const std::string* const kLessOrEqualFnName =
    new std::string("$less_or_equal");
static const std::string* const kLikeFnName = new std::string("$like");
static const std::string* const kMultiplyFnName = new std::string("$multiply");
static const std::string* const kNotEqualFnName = new std::string("$not_equal");
static const std::string* const kSubtractFnName = new std::string("$subtract");
static const std::string* const kDistinctOpFnName =
    new std::string("$is_distinct_from");
static const std::string* const kNotDistinctOpFnName =
    new std::string("$is_not_distinct_from");

static std::string* kInvalidBinaryOperatorStr =
    new std::string("$invalid_binary_operator");

const std::string& FunctionResolver::BinaryOperatorToFunctionName(
    ASTBinaryExpression::Op op, bool is_not, bool* not_handled) {
  if (not_handled != nullptr) {
    *not_handled = false;
  }
  switch (op) {
    case ASTBinaryExpression::DIVIDE:
      return *kDivideFnName;
    case ASTBinaryExpression::EQ:
      return *kEqualFnName;
    case ASTBinaryExpression::NE:
    case ASTBinaryExpression::NE2:
      return *kNotEqualFnName;
    case ASTBinaryExpression::GT:
      return *kGreaterFnName;
    case ASTBinaryExpression::GE:
      return *kGreaterOrEqualFnName;
    case ASTBinaryExpression::LT:
      return *kLessFnName;
    case ASTBinaryExpression::LE:
      return *kLessOrEqualFnName;
    case ASTBinaryExpression::MINUS:
      return *kSubtractFnName;
    case ASTBinaryExpression::MULTIPLY:
      return *kMultiplyFnName;
    case ASTBinaryExpression::PLUS:
      return *kAddFnName;
    case ASTBinaryExpression::LIKE:
      return *kLikeFnName;
    case ASTBinaryExpression::BITWISE_OR:
      return *kBitwiseOrFnName;
    case ASTBinaryExpression::BITWISE_XOR:
      return *kBitwiseXorFnName;
    case ASTBinaryExpression::BITWISE_AND:
      return *kBitwiseAndFnName;
    case ASTBinaryExpression::IS:
    case ASTBinaryExpression::NOT_SET:
      return *kInvalidBinaryOperatorStr;
    case ASTBinaryExpression::CONCAT_OP:
      return *kConcatOpFnName;
    case ASTBinaryExpression::DISTINCT:
      if (is_not) {
        ZETASQL_CHECK(not_handled != nullptr);
        *not_handled = true;
        return *kNotDistinctOpFnName;
      } else {
        return *kDistinctOpFnName;
      }
  }
}

absl::StatusOr<bool> FunctionResolver::SignatureMatches(
    const std::vector<const ASTNode*>& arg_ast_nodes,
    const std::vector<InputArgumentType>& input_arguments,
    const FunctionSignature& signature, bool allow_argument_coercion,
    const NameScope* name_scope,
    std::unique_ptr<FunctionSignature>* result_signature,
    SignatureMatchResult* signature_match_result,
    std::vector<FunctionArgumentOverride>* arg_overrides) const {
  ResolveLambdaCallback lambda_resolve_callback =
      [resolver = this->resolver_, name_scope](
          const ASTLambda* ast_lambda, absl::Span<const IdString> arg_names,
          absl::Span<const Type* const> arg_types, const Type* body_result_type,
          bool allow_argument_coercion,
          std::unique_ptr<const ResolvedInlineLambda>* resolved_expr_out) {
        ZETASQL_DCHECK(name_scope != nullptr);
        return resolver->ResolveLambda(
            ast_lambda, arg_names, arg_types, body_result_type,
            allow_argument_coercion, name_scope, resolved_expr_out);
      };
  return FunctionSignatureMatchesWithStatus(
      resolver_->language(), coercer(), arg_ast_nodes, input_arguments,
      signature, allow_argument_coercion, type_factory_,
      &lambda_resolve_callback, result_signature, signature_match_result,
      arg_overrides);
}

// Get the parse location from a ResolvedNode, if it has one stored in it.
// Otherwise, fall back to the location on an ASTNode.
// Can be used as
//   return MakeSqlErrorAtPoint(GetLocationFromResolvedNode(node, ast_node))
static ParseLocationPoint GetLocationFromResolvedNode(
    const ResolvedNode* node, const ASTNode* fallback) {
  ZETASQL_DCHECK(fallback != nullptr);
  const ParseLocationRange* range = node->GetParseLocationOrNULL();
  if (range != nullptr) {
    return range->start();
  } else {
    return GetErrorLocationPoint(fallback, /*include_leftmost_child=*/true);
  }
}

// static
absl::Status FunctionResolver::CheckCreateAggregateFunctionProperties(
    const ResolvedExpr& resolved_expr,
    const ASTNode* sql_function_body_location,
    const ExprResolutionInfo* expr_info,
    QueryResolutionInfo* query_info) {
  if (expr_info->has_aggregation) {
    ZETASQL_RET_CHECK(query_info->group_by_columns_to_compute().empty());
    ZETASQL_RET_CHECK(!query_info->aggregate_columns_to_compute().empty());

    // TODO: If we have an aggregate with ORDER BY inside, we normally
    // make a Project first to create columns, so the ResolvedAggregateScan can
    // reference them with just a ColumnRef (not a full Expr).  We don't have a
    // way to represent that Project in the ResolvedCreateFunction node, so for
    // now, we detect that case here and give an error.
    if (!query_info->select_list_columns_to_compute_before_aggregation()
             ->empty()) {
      const std::string message =
          "Function body with aggregate functions with ORDER BY "
          "not currently supported";
      if (sql_function_body_location != nullptr) {
        return MakeSqlErrorAt(sql_function_body_location) << message;
      } else {
        return MakeSqlError() << message;
      }
    }
  }

  // Give an error if an aggregate argument is referenced in a non-aggregated
  // way, i.e. outside of an AggregateFunctionCall. This implementation is a bit
  // of a hack.  We traverse the ResolvedExpr after extracting and clearing the
  // aggregate function calls and see if we have any ResolvedArgumentRefs
  // remaining.  This was easier than tracking state during expression
  // resolution so we can give an error immediately when we see the reference,
  // but means that we don't have the AST node to use for error location.
  // Instead, we save the parse location into the ResolvedArgumentRef at
  // construction time.
  std::vector<const ResolvedNode*> found_nodes;
  resolved_expr.GetDescendantsWithKinds({RESOLVED_ARGUMENT_REF}, &found_nodes);
  for (const ResolvedNode* found_node : found_nodes) {
    const ResolvedArgumentRef* arg = found_node->GetAs<ResolvedArgumentRef>();
    if (arg->argument_kind() != ResolvedArgumentRef::NOT_AGGREGATE) {
      // Use the location we stored in the ResolvedArgumentRef when
      // we constructed it.
      const std::string message =
          absl::StrCat("Function argument ", ToIdentifierLiteral(arg->name()),
                       " cannot be referenced outside aggregate function calls"
                       " unless marked as NOT AGGREGATE");
      if (sql_function_body_location != nullptr) {
        return MakeSqlErrorAtPoint(
                   GetLocationFromResolvedNode(arg, sql_function_body_location))
               << message;
      } else {
        return MakeSqlError() << message;
      }
    }
  }
  return absl::OkStatus();
}

absl::Status FunctionResolver::GetFunctionArgumentIndexMappingPerSignature(
    const std::string& function_name, const FunctionSignature& signature,
    const ASTNode* ast_location,
    const std::vector<const ASTNode*>& arg_locations,
    const std::vector<std::pair<const ASTNamedArgument*, int>>& named_arguments,
    int num_repeated_args_repetitions,
    bool always_include_omitted_named_arguments_in_index_mapping,
    std::vector<FunctionResolver::ArgIndexPair>* index_mapping) const {
  // Make sure the language feature is enabled.
  if (!named_arguments.empty() &&
      !resolver_->language().LanguageFeatureEnabled(FEATURE_NAMED_ARGUMENTS)) {
    return MakeSqlErrorAt(named_arguments[0].first)
           << "Named arguments are not supported";
  }

  // Build a set of all argument names in the function signature argument
  // options.
  std::set<std::string, zetasql_base::CaseLess>
      argument_names_from_signature_options;
  int last_arg_index_with_default = -1;
  int last_named_arg_index = -1;
  for (int i = 0; i < signature.arguments().size(); ++i) {
    const FunctionArgumentType& arg_type = signature.arguments()[i];
    if (arg_type.options().has_argument_name()) {
      ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(&argument_names_from_signature_options,
                                        arg_type.options().argument_name()))
          << "Duplicate named argument " << arg_type.options().argument_name()
          << " found in signature for function " << function_name;
      last_named_arg_index = i;
    }
    if (arg_type.GetDefault().has_value()) {
      last_arg_index_with_default = i;
    }
  }

  // Make the reservation for the largest number of arguments that are possibly
  // to be handled and put into the list.
  index_mapping->reserve(
      signature.NumRequiredArguments() + signature.NumOptionalArguments() +
      signature.NumRepeatedArguments() * num_repeated_args_repetitions);

  // Build a map from each argument name to the index in which the named
  // argument appears in <arguments> and <arg_locations>.
  std::map<std::string, int, zetasql_base::CaseLess>
      argument_names_to_indexes;
  int first_named_arg_index_in_call = std::numeric_limits<int>::max();
  int last_named_arg_index_in_call = -1;
  for (int i = 0; i < named_arguments.size(); ++i) {
    const std::pair<const ASTNamedArgument*, int>& named_arg =
        named_arguments[i];
    // Map the argument name to the index in which it appears in the function
    // call. If the name already exists in the map, this is a duplicate named
    // argument which is not allowed.
    const std::string provided_arg_name =
        named_arg.first->name()->GetAsString();
    if (!zetasql_base::InsertIfNotPresent(&argument_names_to_indexes, provided_arg_name,
                                 named_arg.second)) {
      return MakeSqlErrorAt(named_arg.first)
             << "Duplicate named argument " << provided_arg_name
             << " found in call to function " << function_name;
    }
    // Make sure the provided argument name exists in the function signature.
    if (!zetasql_base::ContainsKey(argument_names_from_signature_options,
                          provided_arg_name)) {
      return MakeSqlErrorAt(named_arg.first)
             << "Named argument " << provided_arg_name
             << " not found in signature for call to function "
             << function_name;
    }
    // Keep track of the first and last named argument index.
    first_named_arg_index_in_call =
        std::min(first_named_arg_index_in_call, named_arg.second);
    last_named_arg_index_in_call =
        std::max(last_named_arg_index_in_call, named_arg.second);
  }

  // Check that named arguments are not followed by positional arguments.
  ZETASQL_RET_CHECK_LE(arg_locations.size(), std::numeric_limits<int32_t>::max());
  int num_provided_args = static_cast<int>(arg_locations.size());
  if (!named_arguments.empty() &&
      (last_named_arg_index_in_call - first_named_arg_index_in_call >=
           named_arguments.size() ||
       last_named_arg_index_in_call + 1 < num_provided_args)) {
    return MakeSqlErrorAt(named_arguments.back().first)
           << "Call to function " << function_name << " must not specify "
           << "positional arguments after named arguments; named arguments "
           << "must be specified last in the argument list";
  }

  // Iterate through the function signature and rearrange the provided arguments
  // using the 'argument_names_to_indexes' map.
  int call_arg_index = 0;
  int first_repeated = signature.FirstRepeatedArgumentIndex();
  int last_repeated = signature.LastRepeatedArgumentIndex();

  for (int i = 0; i < signature.arguments().size(); ++i) {
    const FunctionArgumentType& arg_type = signature.arguments()[i];
    const std::string& signature_arg_name =
        arg_type.options().has_argument_name()
            ? arg_type.options().argument_name()
            : "";
    const int* index =
        signature_arg_name.empty()
            ? nullptr
            : zetasql_base::FindOrNull(argument_names_to_indexes, signature_arg_name);
    // For positional arguments that appear before any named arguments appear,
    // simply retain their locations and argument types.
    if ((named_arguments.empty() ||
         call_arg_index < named_arguments[0].second) &&
        (call_arg_index < arg_locations.size() || signature_arg_name.empty())) {
      // Make sure that the function signature does not specify an optional name
      // for this positional argument that also appears later as a named
      // argument in the function call.
      if (!signature_arg_name.empty() && index != nullptr) {
        return MakeSqlErrorAt(arg_locations.at(*index))
               << "Named argument " << signature_arg_name << " is invalid "
               << "because this call to function " << function_name
               << " also includes a positional argument corresponding to the "
               << "same name in the function signature";
      }
      // Make sure that the function signature does not specify an argument
      // name positionally when the options require that it must be named.
      if (!signature_arg_name.empty() &&
          arg_type.options().argument_name_is_mandatory()) {
        return MakeSqlErrorAt(arg_locations.at(call_arg_index))
               << "Positional argument is invalid because this function "
               << "restricts that this argument is referred to by name \""
               << signature_arg_name << "\" only";
      }

      // Skip the repeated part if we run into it but the repetition is zero.
      if (num_repeated_args_repetitions == 0 && i >= first_repeated &&
          i <= last_repeated) {
        i = last_repeated;
        continue;
      }

      if (call_arg_index < num_provided_args) {
        index_mapping->push_back(
            {.signature_arg_index = i, .call_arg_index = call_arg_index++});
      } else if (i <= last_arg_index_with_default ||
                 (always_include_omitted_named_arguments_in_index_mapping &&
                  i <= last_named_arg_index)) {
        // If the current argument was omitted but it or an argument after it
        // has a default value in function signature, then add an entry to the
        // index_mapping.
        index_mapping->push_back(
            {.signature_arg_index = i, .call_arg_index = -1});
      }

      if (i == last_repeated) {
        --num_repeated_args_repetitions;
        if (num_repeated_args_repetitions > 0) {
          i = first_repeated - 1;
        }
      }

      continue;
    }

    // Lookup the required argument name from the map of provided named
    // arguments. If not found, return an error reporting the missing required
    // argument name.
    if (index == nullptr) {
      if (arg_type.required()) {
        return MakeSqlErrorAt(ast_location)
               << "Call to function " << function_name
               << " does not include the required named argument '"
               << signature_arg_name << "'";
      }

      if (arg_type.optional() &&
          (always_include_omitted_named_arguments_in_index_mapping ||
           !named_arguments.empty() ||
           i <= last_arg_index_with_default)) {
        index_mapping->push_back(
            {.signature_arg_index = i, .call_arg_index = -1});
      }
      continue;
    }

    // Repeated argument types may never have required argument names.
    ZETASQL_RET_CHECK(!arg_type.repeated())
        << "Call to function " << function_name << " includes named "
        << "argument " << signature_arg_name << " referring to a repeated "
        << "argument type, which is not supported";

    ZETASQL_RET_CHECK_LT(*index, num_provided_args);

    index_mapping->push_back(
        {.signature_arg_index = i, .call_arg_index = *index});
  }
  return absl::OkStatus();
}

// static
absl::Status FunctionResolver::
    ReorderInputArgumentTypesPerIndexMappingAndInjectDefaultValues(
        const FunctionSignature& signature,
        absl::Span<const ArgIndexPair> index_mapping,
        std::vector<InputArgumentType>* input_argument_types) {
  std::vector<InputArgumentType> orig_input_argument_types =
      std::move(*input_argument_types);
  input_argument_types->clear();

  for (const ArgIndexPair& p : index_mapping) {
    if (p.call_arg_index >= 0) {
      input_argument_types->emplace_back(
            std::move(orig_input_argument_types[p.call_arg_index]));
    } else {
      ZETASQL_RET_CHECK_LE(0, p.signature_arg_index);
      // The argument was omitted from the function call so we add an entry into
      // <input_argument_types>, using the default value if present, or NULL if
      // not.
      const FunctionArgumentType& arg_type =
          signature.arguments()[p.signature_arg_index];
      ZETASQL_RET_CHECK(arg_type.optional());
      const absl::optional<Value>& opt_default = arg_type.GetDefault();

      if (opt_default.has_value()) {
        input_argument_types->emplace_back(opt_default.value(),
                                           /*is_default_argument_value=*/true);
      } else if (arg_type.type() != nullptr) {
        input_argument_types->emplace_back(Value::Null(arg_type.type()));
      } else {
        input_argument_types->emplace_back(InputArgumentType::UntypedNull());
      }
    }
  }
  return absl::OkStatus();
}

namespace {
// Helper function to generate a ResolvedExpr node for an injected function
// argument whose value is either its default value or NULL.
absl::StatusOr<std::unique_ptr<ResolvedExpr>>
MakeResolvedLiteralForInjectedArgument(const InputArgumentType& input_arg_type,
                                       const Type* value_type) {
  // There should be a default value specified in <input_arg_type>,
  // which is either a default value provided in function definition,
  // or an untyped NULL acting as a default default value.
  ZETASQL_RET_CHECK(input_arg_type.is_literal() || input_arg_type.is_untyped_null());
  if (input_arg_type.is_literal()) {
    return MakeResolvedLiteral(*input_arg_type.literal_value());
  }
  if (value_type != nullptr) {
    return MakeResolvedLiteral(value_type, Value::Null(value_type));
  }
  return MakeResolvedLiteral(Value::Null(types::Int64Type()));
}
}  // namespace

// static
absl::Status
FunctionResolver::ReorderArgumentExpressionsPerIndexMapping(
    absl::string_view function_name, const FunctionSignature& signature,
    absl::Span<const ArgIndexPair> index_mapping, const ASTNode* ast_location,
    const std::vector<InputArgumentType>& input_argument_types,
    std::vector<const ASTNode*>* arg_locations,
    std::vector<std::unique_ptr<const ResolvedExpr>>* resolved_args,
    std::vector<ResolvedTVFArg>* resolved_tvf_args) {
  ZETASQL_RET_CHECK_EQ(index_mapping.size(), input_argument_types.size());

  std::vector<const ASTNode*> orig_arg_locations;
  std::vector<std::unique_ptr<const ResolvedExpr>> orig_resolved_args;
  std::vector<ResolvedTVFArg> orig_resolved_tvf_args;
  if (arg_locations != nullptr) {
    orig_arg_locations = std::move(*arg_locations);
    arg_locations->clear();
  }
  if (resolved_args != nullptr) {
    orig_resolved_args = std::move(*resolved_args);
    resolved_args->clear();
  }
  if (resolved_tvf_args != nullptr) {
    orig_resolved_tvf_args = std::move(*resolved_tvf_args);
    resolved_tvf_args->clear();
  }

  for (int i = 0; i < index_mapping.size(); ++i) {
    const ArgIndexPair& aip = index_mapping[i];
    if (aip.call_arg_index >= 0) {
      if (arg_locations != nullptr) {
        arg_locations->emplace_back(orig_arg_locations[aip.call_arg_index]);
      }
      if (resolved_args != nullptr) {
        resolved_args->emplace_back(
            std::move(orig_resolved_args[aip.call_arg_index]));
      }
      if (resolved_tvf_args != nullptr) {
        resolved_tvf_args->emplace_back(
            std::move(orig_resolved_tvf_args[aip.call_arg_index]));
      }
    } else {
      ZETASQL_RET_CHECK_LE(0, aip.signature_arg_index);
      const FunctionArgumentType& arg_type =
          signature.arguments()[aip.signature_arg_index];
      const InputArgumentType& input_arg_type = input_argument_types[i];

      ZETASQL_RET_CHECK(arg_type.optional());

      if (arg_locations != nullptr) {
        arg_locations->emplace_back(ast_location);
      }

      if (resolved_args != nullptr) {
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> resolved_arg,
                         MakeResolvedLiteralForInjectedArgument(
                             input_arg_type, arg_type.type()));
        resolved_args->emplace_back(std::move(resolved_arg));
      }
      if (resolved_tvf_args != nullptr) {
        if (!arg_type.IsScalar()) {
          return MakeSqlErrorAt(ast_location)
                 << "Call to table valued function " << function_name
                 << " does not specify a value for the non-scalar argument "
                 << (arg_type.options().has_argument_name()
                         ? arg_type.options().argument_name()
                         : "");
        }
        ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<ResolvedExpr> resolved_arg,
                         MakeResolvedLiteralForInjectedArgument(
                             input_arg_type, arg_type.type()));
        ResolvedTVFArg arg;
        arg.SetExpr(std::move(resolved_arg));
        resolved_tvf_args->emplace_back(std::move(arg));
      }
    }
  }
  return absl::OkStatus();
}

std::string FunctionResolver::GenerateErrorMessageWithSupportedSignatures(
    const Function* function,
    const std::string& prefix_message) const {
  int num_signatures = 0;
  const std::string supported_signatures =
      function->GetSupportedSignaturesUserFacingText(resolver_->language(),
                                                     &num_signatures);
  if (!supported_signatures.empty()) {
    return absl::StrCat(prefix_message, ". Supported signature",
                        (num_signatures > 1 ? "s" : ""), ": ",
                        supported_signatures);
  } else {
    if (function->GetSupportedSignaturesCallback() == nullptr) {
      // If we do not have any supported signatures and there is
      // no custom callback for producing the signature messages,
      // then we provide an error message as if the function did
      // not exist at all (similar to the error message produced in
      // Resolver::LookupFunctionFromCatalog()). Note that it does
      // not make sense to try to suggest a different function name
      // in this context (like we do in LookupFunctionFromCatalog()).
      return absl::StrCat("Function not found: ", function->SQLName());
    } else {
      // In some cases, like for 'IN', we do not produce a suggested
      // signature.  But we still want to get a 'no matching signature'
      // error message since it indicates the invalid arguments (rather
      // than a 'function not found' message, which would be odd in
      // this case since IN does exist).
      return prefix_message;
    }
  }
}

// TODO: Eventually we want to keep track of the closest
// signature even if there is no match, so that we can provide a good
// error message.  Currently, this code takes an early exit if a signature
// does not match and does not accurately determine how close the signature
// was, nor does it keep track of the best non-matching signature.
absl::StatusOr<const FunctionSignature*>
FunctionResolver::FindMatchingSignature(
    const Function* function,
    const ASTNode* ast_location,
    const std::vector<const ASTNode*>& arg_locations_in,
    const std::vector<std::pair<const ASTNamedArgument*, int>>& named_arguments,
    const NameScope* name_scope,
    std::vector<InputArgumentType>* input_arguments,
    std::vector<FunctionArgumentOverride>* arg_overrides,
    std::vector<ArgIndexPair>* arg_index_mapping) const {
  std::unique_ptr<FunctionSignature> best_result_signature;
  SignatureMatchResult best_result;
  std::vector<FunctionArgumentOverride> best_result_arg_overrides;
  bool seen_matched_signature_with_lambda = false;

  ZETASQL_VLOG(6) << "FindMatchingSignature for function: "
          << function->DebugString(/*verbose=*/true) << "\n  for arguments: "
          << InputArgumentType::ArgumentsToString(
                 *input_arguments, ProductMode::PRODUCT_INTERNAL);

  ZETASQL_RET_CHECK_LE(arg_locations_in.size(), std::numeric_limits<int32_t>::max());
  const int num_provided_args = static_cast<int>(arg_locations_in.size());
  const int num_signatures = function->NumSignatures();
  std::vector<InputArgumentType> original_input_arguments = *input_arguments;
  for (const FunctionSignature& signature : function->signatures()) {
    int repetitions = 0;
    int optionals = 0;
    // If a user calls a function with an internal signature, we won't match it.
    // Only when a fake ASTNode is used to call the function by the rewriter,
    // the internal signature will be matched.
    if (signature.IsInternal() && !arg_locations_in.empty() &&
        arg_locations_in[0]->node_kind() != FakeASTNode::kConcreteNodeKind) {
      continue;
    }
    if (!SignatureArgumentCountMatches(signature, num_provided_args,
                                       &repetitions, &optionals)) {
      if (num_signatures == 1) {
        // TODO: Given that we support optional arguments with
        // defaults, an error message that says the 'number of function
        // arguments does not match' is not very informative. It may be better
        // to use the more general 'no matching signature' error message form,
        // like:  "No matching signature for <function_name> for argument types:
        // INT64.  Supported signature(s): ...". Improve this.
        return MakeSqlErrorAt(ast_location)
               << GenerateErrorMessageWithSupportedSignatures(
                      function,
                      absl::StrCat("Number of arguments does not match for ",
                                   function->QualifiedSQLName()));
      }
      continue;
    }

    std::vector<ArgIndexPair> index_mapping;
    absl::Status status = GetFunctionArgumentIndexMappingPerSignature(
        function->FullName(), signature, ast_location, arg_locations_in,
        named_arguments, repetitions,
        /*always_include_omitted_named_arguments_in_index_mapping=*/true,
        &index_mapping);
    if (!status.ok()) {
      // If <status> was not ok then the given signature is not a match.
      // If there are additional signatures then we will proceed to the next
      // one. Otherwise, we return <status> which has more detailed information
      // about why the signature did not match (rather than return a more
      // generic error later).
      if (num_signatures == 1) {
        return status;
      } else {
        continue;
      }
    }

    std::vector<InputArgumentType> input_arguments_copy =
        original_input_arguments;
    if (!index_mapping.empty()) {
      ZETASQL_RETURN_IF_ERROR(
          ReorderInputArgumentTypesPerIndexMappingAndInjectDefaultValues(
              signature, index_mapping, &input_arguments_copy));
    }

    std::unique_ptr<FunctionSignature> result_signature;
    SignatureMatchResult signature_match_result;
    std::vector<FunctionArgumentOverride> sig_arg_overrides;
    ZETASQL_ASSIGN_OR_RETURN(
        const bool is_match,
        SignatureMatches(arg_locations_in, input_arguments_copy, signature,
                         function->ArgumentsAreCoercible(), name_scope,
                         &result_signature, &signature_match_result,
                         &sig_arg_overrides));
    if (!is_match) {
      continue;
    }

    ZETASQL_RET_CHECK(result_signature != nullptr);
    ZETASQL_ASSIGN_OR_RETURN(
        const bool argument_constraints_satisfied,
        result_signature->CheckArgumentConstraints(input_arguments_copy));
    if (!argument_constraints_satisfied) {
      // If this signature has argument constraints and they are not
      // satisfied then ignore the signature.
      continue;
    }

    ZETASQL_VLOG(6) << "Found signature for input arguments: "
            << InputArgumentType::ArgumentsToString(
                   input_arguments_copy, ProductMode::PRODUCT_INTERNAL)
            << "\nfunction signature: "
            << signature.DebugString(/*function_name=*/"",
                                     /*verbose=*/true)
            << "\nresult signature: "
            << result_signature->DebugString(/*function_name=*/"",
                                             /*verbose=*/true)
            << "\n  cost: " << signature_match_result.DebugString();

    if (best_result_signature != nullptr) {
      // When the other arguments are not enough to distinguish which
      // signature to use, we're left only with the lambdas, which can't
      // distinguish between two overloads. If this ZETASQL_CHECK fails, an engine has
      // set up its catalog with a function signature that ZetaSQL doesn't
      // mean to support for the time being. This shouldn't happen as
      // Function::CheckMultipleSignatureMatchingSameFunctionCall() validation
      // should have screened that.
      // Another intersting example function shape is the following:
      //   Func(T1, LAMBDA(T1, T1) -> INT64)
      //   Func(T1, LAMBDA(T1, T1) -> STRING)
      // These two signatures cannot match any actual function call at the
      // same time as one expression could only be coerced to either string or
      // int64_t. But our restriction is still restricting this for simplicity
      // and lack of use case.
      ZETASQL_RET_CHECK(!seen_matched_signature_with_lambda ||
                sig_arg_overrides.empty())
          << "Multiple matched signature with lambda is not supported";
    }

    if ((best_result_signature == nullptr) ||
        (signature_match_result.IsCloserMatchThan(best_result))) {
      best_result_signature = std::move(result_signature);
      best_result = signature_match_result;
      if (!sig_arg_overrides.empty()) {
        ZETASQL_RET_CHECK(arg_overrides != nullptr)
            << "Function call has lambdas but nowhere to put them";
      }
      seen_matched_signature_with_lambda =
          seen_matched_signature_with_lambda || !sig_arg_overrides.empty();
      best_result_arg_overrides = std::move(sig_arg_overrides);
      *input_arguments = std::move(input_arguments_copy);
      *arg_index_mapping = std::move(index_mapping);
    } else {
      ZETASQL_VLOG(4) << "Found duplicate signature matches for function: "
              << function->DebugString() << "\nGiven input arguments: "
              << InputArgumentType::ArgumentsToString(
                     input_arguments_copy, ProductMode::PRODUCT_INTERNAL)
              << "\nBest result signature: "
              << best_result_signature->DebugString()
              << "\n  cost: " << best_result.DebugString()
              << "\nDuplicate signature: " << result_signature->DebugString()
              << "\n  cost: " << signature_match_result.DebugString();
    }
  }

  if (arg_overrides != nullptr) {
    *arg_overrides = std::move(best_result_arg_overrides);
  }
  return best_result_signature.release();
}

static void ConvertMakeStructToLiteralIfAllExplicitLiteralFields(
    std::unique_ptr<const ResolvedExpr>* argument) {
  if (!(*argument)->type()->IsStruct() ||
      argument->get()->node_kind() != RESOLVED_MAKE_STRUCT) {
    return;
  }

  ResolvedMakeStruct* struct_expr = const_cast<ResolvedMakeStruct*>(
      argument->get()->GetAs<ResolvedMakeStruct>());
  std::vector<Value> field_values;
  for (const auto& field_expr : struct_expr->field_list()) {
    if (field_expr->node_kind() != RESOLVED_LITERAL ||
        !field_expr->GetAs<ResolvedLiteral>()->has_explicit_type()) {
      // This field is not a literal or is not explicitly typed so do not
      // collapse this into a literal.  Return with <argument> unchanged.
      return;
    }
    field_values.push_back(field_expr->GetAs<ResolvedLiteral>()->value());
  }
  // All fields were explicitly typed literals, so construct a literal for
  // this MakeStruct, and mark this new Struct literal as explicitly typed.
  *argument = MakeResolvedLiteral(
      (*argument)->type(),
      Value::Struct((*argument)->type()->AsStruct(), field_values),
      /*has_explicit_type=*/true);
}

absl::Status ExtractStructFieldLocations(
    const StructType* to_struct_type,
    const ASTNode* ast_location,
    std::vector<const ASTNode*>* field_arg_locations) {
  // Skip through gratuitous casts in the AST so that we can get the field
  // argument locations.
  const ASTNode* cast_free_ast_location = ast_location;
  while (cast_free_ast_location->node_kind() == AST_CAST_EXPRESSION) {
    const ASTCastExpression* ast_cast =
        cast_free_ast_location->GetAs<ASTCastExpression>();
    cast_free_ast_location = ast_cast->expr();
  }

  switch (cast_free_ast_location->node_kind()) {
    case AST_STRUCT_CONSTRUCTOR_WITH_PARENS: {
      const ASTStructConstructorWithParens* ast_struct =
          cast_free_ast_location->GetAs<ASTStructConstructorWithParens>();
      ZETASQL_DCHECK_EQ(ast_struct->field_expressions().size(),
                to_struct_type->num_fields());
      *field_arg_locations = ToLocations(ast_struct->field_expressions());
      break;
    }
    case AST_STRUCT_CONSTRUCTOR_WITH_KEYWORD: {
      const ASTStructConstructorWithKeyword* ast_struct =
          cast_free_ast_location->GetAs<ASTStructConstructorWithKeyword>();
      ZETASQL_DCHECK_EQ(ast_struct->fields().size(), to_struct_type->num_fields());
      // Strip "AS <alias>" clauses from field arg locations.
      for (const ASTStructConstructorArg* arg : ast_struct->fields()) {
        field_arg_locations->push_back(arg->expression());
      }
      break;
    }
    default: {
      ZETASQL_RET_CHECK_FAIL() << "Cannot obtain the AST expressions for field "
                       << "arguments of struct constructor:\n"
                       << ast_location->DebugString();
    }
  }
  ZETASQL_RET_CHECK_EQ(field_arg_locations->size(), to_struct_type->num_fields());
  return absl::OkStatus();
}

absl::Status FunctionResolver::AddCastOrConvertLiteral(
    const ASTNode* ast_location, AnnotatedType annotated_target_type,
    std::unique_ptr<const ResolvedExpr> format,
    std::unique_ptr<const ResolvedExpr> time_zone,
    const TypeParameters& type_params, const ResolvedScan* scan,
    bool set_has_explicit_type, bool return_null_on_error,
    std::unique_ptr<const ResolvedExpr>* argument) const {
  ZETASQL_RET_CHECK_NE(ast_location, nullptr);

  const Type* target_type = annotated_target_type.type;
  const AnnotationMap* target_type_annotation_map =
      annotated_target_type.annotation_map;

  // If this conversion is a no-op we can return early.
  if (target_type->Equals(argument->get()->type()) && !set_has_explicit_type &&
      AnnotationMap::HasEqualAnnotations(argument->get()->type_annotation_map(),
                                         target_type_annotation_map,
                                         CollationAnnotation::GetId())) {
    // These fields should only be used for explicit casts when
    // set_has_explicit_type is true.
    ZETASQL_RET_CHECK(type_params.IsEmpty());
    ZETASQL_RET_CHECK_EQ(format.get(), nullptr);
    ZETASQL_RET_CHECK_EQ(time_zone.get(), nullptr);
    return absl::OkStatus();
  }

  // We add the casts field-by-field for struct expressions. We will collapse
  // a ResolvedMakeStruct to a struct literal if all the fields are literals
  // and <set_has_explicit_type> is true. If the MakeStruct is the result of a
  // path expression rather than an explicit struct constructor with fields, use
  // a struct cast.
  if (target_type->IsStruct() &&
      argument->get()->node_kind() == RESOLVED_MAKE_STRUCT &&
      ast_location->node_kind() != AST_PATH_EXPRESSION) {
    ZETASQL_RET_CHECK(target_type_annotation_map == nullptr ||
              target_type_annotation_map->HasCompatibleStructure(target_type));
    // Remove constness so that we can add casts on the field expressions inside
    // the struct.
    ResolvedMakeStruct* struct_expr = const_cast<ResolvedMakeStruct*>(
        argument->get()->GetAs<ResolvedMakeStruct>());
    const StructType* to_struct_type = target_type->AsStruct();
    ZETASQL_RET_CHECK_EQ(struct_expr->field_list_size(), to_struct_type->num_fields());
    ZETASQL_RET_CHECK(type_params.MatchType(to_struct_type));

    std::vector<const ASTNode*> field_arg_locations;
    // If we can't obtain the locations of field arguments and replace literals
    // inside that expression, their parse locations will be wrong.
    ZETASQL_RETURN_IF_ERROR(ExtractStructFieldLocations(
        to_struct_type, ast_location, &field_arg_locations));

    std::vector<std::unique_ptr<const ResolvedExpr>> field_exprs =
        struct_expr->release_field_list();
    for (int i = 0; i < to_struct_type->num_fields(); ++i) {
      const AnnotationMap* field_type_annotation_map =
          target_type_annotation_map == nullptr
              ? nullptr
              : target_type_annotation_map->AsStructMap()->field(i);
      if (to_struct_type->field(i).type->Equals(field_exprs[i]->type()) &&
          AnnotationMap::HasEqualAnnotations(
              field_exprs[i]->type_annotation_map(), field_type_annotation_map,
              CollationAnnotation::GetId())) {
        if (field_exprs[i]->node_kind() == RESOLVED_LITERAL &&
            set_has_explicit_type &&
            !field_exprs[i]->GetAs<ResolvedLiteral>()->has_explicit_type()) {
          // This field has the same Type, but is a literal that needs to
          // have it set as has_explicit_type so we must replace the
          // expression.
          const AnnotationMap* original_type_annotation_map =
              field_exprs[i]->type_annotation_map();
          field_exprs[i] = resolver_->MakeResolvedLiteral(
              field_arg_locations[i],
              field_exprs[i]->GetAs<ResolvedLiteral>()->value().type(),
              field_exprs[i]->GetAs<ResolvedLiteral>()->value(),
              /*has_explicit_type=*/true);
          // TODO: Currently we set <type_annotation_map> on the
          // output of MakeResolvedLiteral function. Consider passing
          // <annotated_type> to the MakeResolvedLiteral and set the
          // <type_annotation_map> inside.
          const_cast<ResolvedExpr*>(field_exprs[i].get())
              ->set_type_annotation_map(original_type_annotation_map);
        }
      }

      const TypeParameters& child_params =
          type_params.IsEmpty() ? TypeParameters() : type_params.child(i);

      // We pass nullptr for 'format' and 'time_zone' here because there is
      // currently no way to define format strings for individual struct fields.
      const absl::Status cast_status = AddCastOrConvertLiteral(
          field_arg_locations[i],
          AnnotatedType(to_struct_type->field(i).type,
                        field_type_annotation_map),
          /*format=*/nullptr, /*time_zone=*/nullptr, child_params, scan,
          set_has_explicit_type, return_null_on_error, &field_exprs[i]);
      if (!cast_status.ok()) {
        // Propagate "Out of stack space" errors.
        // TODO: Propagate internal, unimplemented, etc
        if (cast_status.code() == absl::StatusCode::kResourceExhausted) {
          return cast_status;
        }
        return MakeSqlErrorAt(field_arg_locations[i]) << cast_status.message();
      }
    }
    auto make_struct =
        MakeResolvedMakeStruct(target_type, std::move(field_exprs));
    ZETASQL_RETURN_IF_ERROR(resolver_->CheckAndPropagateAnnotations(
        /*error_node=*/nullptr, make_struct.get()));
    *argument = std::move(make_struct);

    // If all the fields are now explicitly casted literals, then we can
    // convert this MakeStruct into a Literal instead.
    ConvertMakeStructToLiteralIfAllExplicitLiteralFields(argument);

    return absl::OkStatus();
  } else if ((*argument)->node_kind() == RESOLVED_FUNCTION_CALL &&
             (*argument)->GetAs<ResolvedFunctionCall>()->function()->FullName(
                 /*include_group=*/true) == "ZetaSQL:error") {
    // This is an ERROR(message) function call.  We special case this to
    // make the output argument coercible to anything so expressions like
    //   IF(<condition>, <value>, ERROR("message"))
    // work for any value type.
    const ResolvedFunctionCall* old_call =
        (*argument)->GetAs<ResolvedFunctionCall>();
    FunctionSignature new_signature = old_call->signature();
    new_signature.SetConcreteResultType(target_type);
    *argument = MakeResolvedFunctionCall(
        target_type, old_call->function(), new_signature,
        const_cast<ResolvedFunctionCall*>(old_call)->release_argument_list(),
        old_call->error_mode());
    // Add a no-op cast to represent that the return type of ERROR(), which is
    // usually coercible to everything, is now explicitly specified. For
    // example, COALESCE('abc', CAST(ERROR('def') AS BYTES)) fails because BYTES
    // does not implicitly coerce to STRING.
    //
    // Also note that the value of 'return_null_on_error' does not matter here
    // because this no-op cast cannot fail.
    *argument = MakeResolvedCast(target_type, std::move(*argument),
                                 return_null_on_error);
    return absl::OkStatus();
  }

  const ResolvedLiteral* argument_literal = nullptr;
  if (argument->get()->node_kind() == RESOLVED_LITERAL) {
    argument_literal = argument->get()->GetAs<ResolvedLiteral>();
  } else if (argument->get()->node_kind() == RESOLVED_COLUMN_REF &&
             scan != nullptr &&
             scan->node_kind() == RESOLVED_PROJECT_SCAN) {
    // TODO This FindProjectExpr uses a linear scan, so converting
    // N expressions one by one is potentially N^2.  Maybe build a map somehow.
    const ResolvedExpr* found_expr =
        FindProjectExpr(
            scan->GetAs<ResolvedProjectScan>(),
            argument->get()->GetAs<ResolvedColumnRef>()->column());
    if (found_expr != nullptr &&
        found_expr->node_kind() == RESOLVED_LITERAL) {
      argument_literal = found_expr->GetAs<ResolvedLiteral>();
    }
  }

  // Implicitly convert literals if possible.  When casting to a parameterized
  // type, we first cast to the base type here, and then add an explicit cast to
  // the parameterized type after this implicit conversion.
  //
  // TODO: Should this look at time_zone and type_params too?
  if (argument_literal != nullptr && format == nullptr) {
    std::unique_ptr<const ResolvedLiteral> converted_literal;
    ZETASQL_RETURN_IF_ERROR(ConvertLiteralToType(
        ast_location, argument_literal, target_type, scan,
        set_has_explicit_type, return_null_on_error, &converted_literal));
    *argument = std::move(converted_literal);
  }

  // Assign type to undeclared parameters.
  ZETASQL_ASSIGN_OR_RETURN(
      const bool type_assigned,
      resolver_->MaybeAssignTypeToUndeclaredParameter(argument, target_type));
  if (type_assigned && type_params.IsEmpty()) {
    return absl::OkStatus();
  }

  return resolver_->ResolveCastWithResolvedArgument(
      ast_location, annotated_target_type, std::move(format),
      std::move(time_zone), type_params, return_null_on_error, argument);
}

absl::Status FunctionResolver::AddCastOrConvertLiteral(
    const ASTNode* ast_location, const Type* target_type,
    std::unique_ptr<const ResolvedExpr> format,
    std::unique_ptr<const ResolvedExpr> time_zone,
    const TypeParameters& type_params, const ResolvedScan* scan,
    bool set_has_explicit_type, bool return_null_on_error,
    std::unique_ptr<const ResolvedExpr>* argument) const {
  return AddCastOrConvertLiteral(
      ast_location, AnnotatedType(target_type, /*annotation_map=*/nullptr),
      std::move(format), std::move(time_zone), type_params, scan,
      set_has_explicit_type, return_null_on_error, std::move(argument));
}

namespace {
bool GetFloatImage(
    const absl::flat_hash_map<int, std::string>& float_literal_images,
    const ASTNode* ast_location, const ResolvedLiteral* argument_literal,
    absl::string_view* float_image) {
  const std::string* float_image_ptr = zetasql_base::FindOrNull(
      float_literal_images, argument_literal->float_literal_id());
  if (float_image_ptr != nullptr) {
    *float_image = *float_image_ptr;
    return true;
  }
  switch (ast_location->node_kind()) {
    case AST_FLOAT_LITERAL:
      *float_image = ast_location->GetAs<ASTFloatLiteral>()->image();
      return true;
    case AST_INT_LITERAL:
      *float_image = ast_location->GetAs<ASTIntLiteral>()->image();
      return true;
    default:
      return false;
  }
}
}  // namespace

absl::Status FunctionResolver::ConvertLiteralToType(
    const ASTNode* ast_location, const ResolvedLiteral* argument_literal,
    const Type* target_type, const ResolvedScan* scan,
    bool set_has_explicit_type, bool return_null_on_error,
    std::unique_ptr<const ResolvedLiteral>* converted_literal) const {
  const Value* argument_value = &argument_literal->value();
  absl::StatusOr<Value> coerced_literal_value;  // Initialized to UNKNOWN
  absl::string_view float_literal_image;
  if (argument_value->is_null()) {
    coerced_literal_value = Value::Null(target_type);
  } else if (argument_value->is_empty_array() &&
             !argument_literal->has_explicit_type() && target_type->IsArray()) {
    // Coerces an untyped empty array to an empty array of the target_type.
    coerced_literal_value =
        Value::Array(target_type->AsArray(), {} /* values */);
  } else if (argument_value->type()->IsStruct()) {
    // TODO: Make this clearer by factoring it out to a helper function
    // that returns an absl::StatusOr<Value>, making 'success' unnecessary and
    // allowing for a more detailed error message (like for string -> proto
    // conversion below).
    bool success = true;
    if (!target_type->IsStruct() ||
        argument_value->num_fields() != target_type->AsStruct()->num_fields()) {
      success = false;
    }

    // We construct the coerced literal field-by-field for structs.
    std::vector<Value> coerced_field_literals;
    for (int i = 0; i < argument_value->num_fields() && success; ++i) {
      const Type* target_field_type = target_type->AsStruct()->field(i).type;
      // Parse locations of the literals created below are irrelevant because
      // in case of success we create a new literal struct containing these
      // literals.
      auto field_literal =
          MakeResolvedLiteral(target_field_type, argument_value->field(i));
      std::unique_ptr<const ResolvedLiteral> coerced_field_literal;
      if (ConvertLiteralToType(ast_location, field_literal.get(),
                               target_field_type, scan, set_has_explicit_type,
                               return_null_on_error, &coerced_field_literal)
              .ok()) {
        ZETASQL_DCHECK_EQ(field_literal->node_kind(), RESOLVED_LITERAL);
        coerced_field_literals.push_back(coerced_field_literal->value());
      } else {
        success = false;
      }
    }

    if (success) {
      coerced_literal_value =
          Value::Struct(target_type->AsStruct(), coerced_field_literals);
    }
  } else if (argument_value->type()->IsFloatingPoint() &&
             (target_type->IsNumericType() ||
              target_type->IsBigNumericType()) &&
             GetFloatImage(resolver_->float_literal_images_, ast_location,
                           argument_literal, &float_literal_image)) {
    // If we are casting a floating point literal into a NUMERIC or BIGNUMERIC,
    // reparse the original literal image instead of converting the floating
    // point value to preserve precision.
    if (target_type->IsNumericType()) {
      ZETASQL_ASSIGN_OR_RETURN(NumericValue coerced_numeric,
                       NumericValue::FromString(float_literal_image));
      coerced_literal_value = Value::Numeric(coerced_numeric);
    } else if (target_type->IsBigNumericType()) {
      ZETASQL_ASSIGN_OR_RETURN(BigNumericValue coerced_bignumeric,
                       BigNumericValue::FromString(float_literal_image));
      coerced_literal_value = Value::BigNumeric(coerced_bignumeric);
    }
  } else {
    // <coerced_literal_value> gets populated only when we can successfully
    // cast <argument_value> to <target_type>.
    coerced_literal_value =
        CastValue(*argument_value, resolver_->default_time_zone(),
                  resolver_->language(), target_type, catalog_);
  }

  if (!coerced_literal_value.status().ok()) {
    // If return_null_on_error is set to true, in-place converts the literal
    // to a NULL value of target_type. Otherwise, returns an error.
    if (return_null_on_error) {
      *converted_literal = resolver_->MakeResolvedLiteral(
          ast_location, Value::Null(target_type), set_has_explicit_type);
      return absl::OkStatus();
    } else {
      zetasql_base::StatusBuilder builder =
          MakeSqlErrorAt(ast_location)
          << "Could not cast "
          << (argument_literal->has_explicit_type() ? "" : "literal ")
          << argument_value->DebugString() << " to type "
          << target_type->DebugString();
      // Give a more detailed error message for string/bytes -> proto
      // conversions, which can have subtle issues.
      absl::string_view error_message =
          coerced_literal_value.status().message();
      const Type* argument_type = argument_value->type();
      if ((argument_type->IsString() || argument_type->IsBytes()) &&
          target_type->IsProto() && !error_message.empty()) {
        builder << " (" << error_message << ")";
      }
      return builder;
    }
  }

  bool has_explicit_type = argument_literal->has_explicit_type();
  if (set_has_explicit_type) {
    has_explicit_type = true;
  }
  auto replacement_literal = MakeResolvedLiteral(
      target_type, coerced_literal_value.value(), has_explicit_type);
  // The float literal cache entry (if there is one) is no longer valid after
  // replacement.
  resolver_->float_literal_images_.erase(argument_literal->float_literal_id());
  if (resolver_->analyzer_options_.parse_location_record_type() !=
      PARSE_LOCATION_RECORD_NONE) {
    // Copy parse location to the replacement literal.
    if (argument_literal->GetParseLocationRangeOrNULL() != nullptr) {
      replacement_literal->SetParseLocationRange(
          *argument_literal->GetParseLocationRangeOrNULL());
    }
    // Remove parse location on the original literal.  We don't want to
    // do a replacement based on that one because it has the original type,
    // not the inferred type, and it would have the same string location
    // as the replacement literal.
    const_cast<ResolvedLiteral*>(argument_literal)->ClearParseLocationRange();
  }
  *converted_literal = std::move(replacement_literal);

  return absl::OkStatus();
}

absl::Status FunctionResolver::ResolveGeneralFunctionCall(
    const ASTNode* ast_location,
    const std::vector<const ASTNode*>& arg_locations,
    const std::vector<std::string>& function_name_path, bool is_analytic,
    std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
    std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
    const Type* expected_result_type,
    std::unique_ptr<ResolvedFunctionCall>* resolved_expr_out) {
  const Function* function;
  ResolvedFunctionCallBase::ErrorMode error_mode;
  ZETASQL_RETURN_IF_ERROR(resolver_->LookupFunctionFromCatalog(
      ast_location, function_name_path,
      Resolver::FunctionNotFoundHandleMode::kReturnError, &function,
      &error_mode));
  return ResolveGeneralFunctionCall(
      ast_location, arg_locations, function, error_mode, is_analytic,
      std::move(arguments), std::move(named_arguments), expected_result_type,
      /*name_scope=*/nullptr, resolved_expr_out);
}

absl::Status FunctionResolver::ResolveGeneralFunctionCall(
    const ASTNode* ast_location,
    const std::vector<const ASTNode*>& arg_locations,
    const std::string& function_name, bool is_analytic,
    std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
    std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
    const Type* expected_result_type,
    std::unique_ptr<ResolvedFunctionCall>* resolved_expr_out) {
  const std::vector<std::string> function_name_path = {function_name};
  return ResolveGeneralFunctionCall(
      ast_location, arg_locations, function_name_path, is_analytic,
      std::move(arguments), std::move(named_arguments), expected_result_type,
      resolved_expr_out);
}

// Shorthand to make ResolvedFunctionArgument from ResolvedExpr
static std::unique_ptr<ResolvedFunctionArgument> MakeResolvedFunctionArgument(
    std::unique_ptr<const ResolvedExpr> expr) {
  std::unique_ptr<ResolvedFunctionArgument> function_argument =
      MakeResolvedFunctionArgument();
  function_argument->set_expr(std::move(expr));
  return function_argument;
}

absl::Status FunctionResolver::ResolveCollationForCollateFunction(
    const ASTNode* error_location, ResolvedFunctionCall* function_call) {
  ZETASQL_RET_CHECK_EQ(function_call->argument_list_size(), 2);
  const ResolvedExpr* arg_1 = function_call->argument_list(1);
  // Pre-resolution check has made sure the second argument is a string literal.
  ZETASQL_RET_CHECK(arg_1->type()->IsString() &&
            arg_1->node_kind() == RESOLVED_LITERAL &&
            !arg_1->GetAs<ResolvedLiteral>()->value().is_null());
  const ResolvedLiteral* literal = arg_1->GetAs<ResolvedLiteral>();
  const_cast<ResolvedLiteral*>(literal)->set_preserve_in_literal_remover(
      true);
  const std::string& collation_name =
      arg_1->GetAs<ResolvedLiteral>()->value().string_value();
  if (!collation_name.empty()) {
    std::unique_ptr<AnnotationMap> mutable_annotation_map;
    if (function_call->type_annotation_map() == nullptr) {
      mutable_annotation_map = AnnotationMap::Create(function_call->type());
    } else {
      mutable_annotation_map = function_call->type_annotation_map()->Clone();
    }
    mutable_annotation_map->SetAnnotation<CollationAnnotation>(
        SimpleValue::String(collation_name));
    ZETASQL_ASSIGN_OR_RETURN(
        const AnnotationMap* annotation_map,
        type_factory_->TakeOwnership(std::move(mutable_annotation_map)));
    function_call->set_type_annotation_map(annotation_map);
  }
  return absl::OkStatus();
}

absl::Status FunctionResolver::ResolveGeneralFunctionCall(
    const ASTNode* ast_location,
    const std::vector<const ASTNode*>& arg_locations_in,
    const Function* function, ResolvedFunctionCallBase::ErrorMode error_mode,
    bool is_analytic,
    std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
    std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
    const Type* expected_result_type, const NameScope* name_scope,
    std::unique_ptr<ResolvedFunctionCall>* resolved_expr_out) {

  std::vector<const ASTNode*> arg_locations = arg_locations_in;
  ZETASQL_RET_CHECK(ast_location != nullptr);
  ZETASQL_RET_CHECK_EQ(arg_locations.size(), arguments.size());

  // For binary operators, point the error message at the operator in the
  // middle rather that at the start of the leftmost argument.
  // This works for some operators but not others, depending on the
  // construction rules in the parser.
  // TODO Figure out how to get error location for 'abc=def'
  // to point at the '='.
  const bool include_leftmost_child =
      (ast_location->node_kind() == AST_BINARY_EXPRESSION);

  if (is_analytic && !function->SupportsOverClause()) {
    return MakeSqlErrorAt(ast_location)
           << function->QualifiedSQLName(/*capitalize_qualifier=*/true)
           << " does not support an OVER clause";
  }

  std::vector<InputArgumentType> input_argument_types;
  GetInputArgumentTypesForGenericArgumentList(arg_locations, arguments,
                                              &input_argument_types);

  // Check initial argument constraints, if any.
  if (function->PreResolutionConstraints() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(StatusWithInternalErrorLocation(
        function->CheckPreResolutionArgumentConstraints(input_argument_types,
                                                        resolver_->language()),
        ast_location, include_leftmost_child));
  }

  std::unique_ptr<const FunctionSignature> result_signature;
  std::vector<FunctionArgumentOverride> arg_overrides;
  std::vector<ArgIndexPair> arg_reorder_index_mapping;

  ZETASQL_ASSIGN_OR_RETURN(
      const FunctionSignature* signature,
      FindMatchingSignature(function, ast_location, arg_locations,
                            named_arguments, name_scope, &input_argument_types,
                            &arg_overrides, &arg_reorder_index_mapping));
  result_signature.reset(signature);

  if (nullptr == result_signature) {
    return MakeSqlErrorAtNode(ast_location, include_leftmost_child)
           << GenerateErrorMessageWithSupportedSignatures(
                  function,
                  function->GetNoMatchingFunctionSignatureErrorMessage(
                      input_argument_types,
                      resolver_->language().product_mode()));
  }

  ZETASQL_RET_CHECK(result_signature->HasConcreteArguments());
  if (!function->Is<TemplatedSQLFunction>()) {
    ZETASQL_RET_CHECK(result_signature->IsConcrete())
        << result_signature->DebugString();
  }

  ZETASQL_RETURN_IF_ERROR(ReorderArgumentExpressionsPerIndexMapping(
      function->SQLName(), *result_signature, arg_reorder_index_mapping,
      ast_location, input_argument_types, &arg_locations, &arguments,
      /*resolved_tvf_args=*/nullptr));

  const auto BadArgErrorPrefix = [&result_signature, &named_arguments,
                                  function](int idx) {
    if (function->GetBadArgumentErrorPrefixCallback() != nullptr) {
      return function->GetBadArgumentErrorPrefixCallback()(*result_signature,
                                                           idx);
    }
    const FunctionArgumentType& argument = result_signature->argument(idx);
    if (argument.has_argument_name()) {
      // Check whether function call was using named argument or positional
      // argument, and if it was named - use the name in the error message.
      for (const auto& named_arg : named_arguments) {
        if (zetasql_base::CaseEqual(named_arg.first->name()->GetAsString(),
                                   argument.argument_name())) {
          return absl::StrCat("Argument '", argument.argument_name(), "' to ",
                              function->SQLName());
        }
      }
    }
    if (result_signature->NumConcreteArguments() == 1) {
      return absl::StrCat("The argument to ", function->SQLName());
    } else {
      return absl::StrCat("Argument ", idx + 1, " to ", function->SQLName());
    }
  };
  const ProductMode product_mode = resolver_->language().product_mode();

  int arg_override_index = 0;
  for (int idx = 0; idx < arguments.size(); ++idx) {
    // The ZETASQL_RET_CHECK above ensures that the arguments are concrete for both
    // templated and non-templated functions.
    const FunctionArgumentType& concrete_argument =
        result_signature->ConcreteArgument(idx);
    if (concrete_argument.IsLambda()) {
      ZETASQL_RET_CHECK(arguments[idx] == nullptr);
      ZETASQL_RET_CHECK(arg_overrides.size() > arg_override_index);

      const FunctionArgumentOverride& arg_override =
          arg_overrides[arg_override_index];
      ZETASQL_RET_CHECK_EQ(arg_override.index, idx);
      const ResolvedInlineLambda* lambda =
          arg_override.argument->inline_lambda();
      const FunctionArgumentType::ArgumentTypeLambda& concrete_lambda =
          concrete_argument.lambda();
      ZETASQL_RET_CHECK_EQ(lambda->argument_list().size(),
                   concrete_lambda.argument_types().size());
      // Check that lambda argument matches concrete argument. This shouldn't
      // trigger under known use cases. But still doing the check to be safe.
      // An example is fn_fp_T_LAMBDA_T(1, e->e>0, 1.0) for signature
      // fn_fp_T_LAMBDA_T(T1, T1->BOOL, T1). e is infered to be INT64 from
      // argument 1, but T1 is later decided to be DOUBLE.
      for (int i = 0; i < lambda->argument_list_size(); i++) {
        ZETASQL_RET_CHECK(lambda->argument_list(i).type()->Equals(
            concrete_lambda.argument_types()[i].type()))
            << "Failed to infer the type of lambda argument at index " << i
            << ". It is inferred from arguments preceding the lambda to "
               "be "
            << lambda->argument_list(i).type()->ShortTypeName(product_mode)
            << " but found to be "
            << concrete_lambda.argument_types()[i].type()->ShortTypeName(
                   product_mode)
            << " after considering arguments following the lambda";
      }
      // Check that lambda body type matches concrete argument.
      if (!lambda->body()->type()->Equals(concrete_lambda.body_type().type())) {
        return MakeSqlErrorAt(arg_locations[idx])
               << "Lambda body is resolved to have type "
               << lambda->body()->type()->ShortTypeName(product_mode)
               << " but the signature requires it to be "
               << concrete_lambda.body_type().type()->ShortTypeName(
                      product_mode);
      }
      continue;
    }
    ZETASQL_DCHECK(arguments[idx] != nullptr);

    if (concrete_argument.options().must_support_equality() &&
        !concrete_argument.type()->SupportsEquality(resolver_->language())) {
      return MakeSqlErrorAt(arg_locations[idx])
             << BadArgErrorPrefix(idx) << " must support equality; Type "
             << concrete_argument.type()->ShortTypeName(product_mode)
             << " does not";
    }
    if (concrete_argument.options().must_support_ordering() &&
        !concrete_argument.type()->SupportsOrdering(
            resolver_->language(), /*type_description=*/nullptr)) {
      return MakeSqlErrorAt(arg_locations[idx])
             << BadArgErrorPrefix(idx) << " must support ordering; Type "
             << concrete_argument.type()->ShortTypeName(product_mode)
             << " does not";
    }
    if (concrete_argument.options().must_support_grouping() &&
        !concrete_argument.type()->SupportsGrouping(resolver_->language())) {
      return MakeSqlErrorAt(arg_locations[idx])
             << BadArgErrorPrefix(idx) << " must support grouping; Type "
             << concrete_argument.type()->ShortTypeName(product_mode)
             << " does not";
    }

    // If we have a cast of a parameter, we want to check the expression inside
    // the cast.  Even if the query just has a parameter, when we unparse, we
    // may get a cast of a parameter, and that should be legal too.
    const ResolvedExpr* unwrapped_argument = arguments[idx].get();
    while (unwrapped_argument->node_kind() == RESOLVED_CAST) {
      unwrapped_argument = unwrapped_argument->GetAs<ResolvedCast>()->expr();
    }
    // We currently use the same validation for must_be_constant and
    // is_not_aggregate, except that is_not_aggregate also allows
    // ResolvedArgumentRefs with kind NOT_AGGREGATE, so that we can have SQL
    // UDF bodies that wrap calls with NOT_AGGREGATE arguments.
    // TODO We may want to generalize these to use IsConstantExpression,
    // so we can allow any constant expression for these arguments.
    if (concrete_argument.must_be_constant() ||
        concrete_argument.options().is_not_aggregate()) {
      switch (unwrapped_argument->node_kind()) {
        case RESOLVED_PARAMETER:
        case RESOLVED_LITERAL:
        case RESOLVED_CONSTANT:
          break;
        case RESOLVED_ARGUMENT_REF:
          // A NOT_AGGREGATE argument is allowed (for is_not_aggregate mode),
          // but any other argument type should fall through to the error case.
          if (!concrete_argument.must_be_constant() &&
              unwrapped_argument->GetAs<ResolvedArgumentRef>()->argument_kind()
                  == ResolvedArgumentRef::NOT_AGGREGATE) {
            break;
          }
          ABSL_FALLTHROUGH_INTENDED;
        default:
          return MakeSqlErrorAt(arg_locations[idx])
                 << BadArgErrorPrefix(idx)
                 << " must be a literal or query parameter";
      }
    }

    const Type* target_type = concrete_argument.type();
    if (!(arguments[idx])->type()->Equals(target_type)) {
      // We keep the original <type_annotation_map> when coercing function
      // arguments. These <type_annotation_map> of arguments will be processed
      // to detemine the <type_annotation_map> of function call in a later
      // stage.
      ZETASQL_RETURN_IF_ERROR(resolver_->CoerceExprToType(
          arg_locations[idx],
          AnnotatedType(target_type, arguments[idx]->type_annotation_map()),
          Resolver::kExplicitCoercion, &arguments[idx]));
      // Update the argument type with the casted one, so that the
      // PostResolutionArgumentConstraintsCallback and the
      // ComputeResultTypeCallback can get the exact types passed to function.
      input_argument_types[idx] =
          GetInputArgumentTypeForExpr(arguments[idx].get());
    }

    // If we have a literal argument value, check it against the value
    // constraints for that argument.
    if (arguments[idx]->node_kind() == RESOLVED_LITERAL) {
      const Value& value = arguments[idx]->GetAs<ResolvedLiteral>()->value();
      ZETASQL_RETURN_IF_ERROR(CheckArgumentValueConstraints(
          arg_locations[idx], idx, value, concrete_argument,
          BadArgErrorPrefix));
    }
  }

  if (function->PostResolutionConstraints() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(StatusWithInternalErrorLocation(
        function->CheckPostResolutionArgumentConstraints(
            *result_signature, input_argument_types, resolver_->language()),
        ast_location, include_leftmost_child));
  }

  if (function->GetComputeResultTypeCallback() != nullptr) {
    // Note that the result type of SQL functions cannot be overridden, since
    // the result type is determined by the type of the resolved SQL expression.
    ZETASQL_RET_CHECK(!function->Is<TemplatedSQLFunction>()) << function->DebugString();
    ZETASQL_RET_CHECK(!function->Is<SQLFunctionInterface>()) << function->DebugString();
    CycleDetector owned_cycle_detector;
    const absl::StatusOr<const Type*> result_type =
        function->GetComputeResultTypeCallback()(
            catalog_, type_factory_, &owned_cycle_detector, *result_signature,
            input_argument_types, resolver_->analyzer_options());
    ZETASQL_RETURN_IF_ERROR(StatusWithInternalErrorLocation(
        result_type.status(), ast_location, include_leftmost_child));
    ZETASQL_RET_CHECK(result_type.value() != nullptr);

    std::unique_ptr<FunctionSignature> new_signature(
        new FunctionSignature(*result_signature));
    new_signature->SetConcreteResultType(result_type.value());

    result_signature = std::move(new_signature);
    ZETASQL_RET_CHECK(result_signature->IsConcrete())
        << "result_signature: '" << result_signature->DebugString() << "'";
  }

  std::shared_ptr<ResolvedFunctionCallInfo> function_call_info(
      new ResolvedFunctionCallInfo);
  if (function->Is<TemplatedSQLFunction>()) {
    const TemplatedSQLFunction* sql_function =
        function->GetAs<TemplatedSQLFunction>();
    // Call the TemplatedSQLFunction::Resolve() method to get the output type.
    // Use a new empty cycle detector, or the cycle detector from an enclosing
    // Resolver if we are analyzing one or more templated function calls.
    CycleDetector owned_cycle_detector;
    AnalyzerOptions analyzer_options = resolver_->analyzer_options_;
    if (analyzer_options.find_options().cycle_detector() == nullptr) {
      analyzer_options.mutable_find_options()->set_cycle_detector(
          &owned_cycle_detector);
    }
    const absl::Status resolve_status = ResolveTemplatedSQLFunctionCall(
        ast_location, *sql_function, analyzer_options, input_argument_types,
        &function_call_info);

    if (!resolve_status.ok()) {
      // The Resolve method returned an error status that reflects the
      // <analyzer_options> ErrorMessageMode.  We want to return an error
      // status here that indicates that the function is invalid along with
      // the function call location, so create a new error status with an
      // ErrorSource based on the <resolve_status>.
      return WrapNestedErrorStatus(
          ast_location, absl::StrCat("Invalid function ", sql_function->Name()),
          resolve_status, analyzer_options.error_message_mode());
    }
    std::unique_ptr<FunctionSignature> new_signature(
        new FunctionSignature(*result_signature));
    new_signature->SetConcreteResultType(
        static_cast<const TemplatedSQLFunctionCall*>(
            function_call_info.get())->expr()->type());
    result_signature = std::move(new_signature);
    ZETASQL_RET_CHECK(result_signature->IsConcrete())
        << "result_signature: '" << result_signature->DebugString() << "'";
  }

  ZETASQL_RET_CHECK(!result_signature->result_type().IsVoid());

  // We transform the concatenation operator (||) function into the
  // corresponding CONCAT or ARRAY_CONCAT function call here. Engines never
  // see the concatenation operator function in the ResolvedAST, and only
  // see the canonical CONCAT/ARRAY_CONCAT function call.
  if (function->Name() == "$concat_op") {
    const Function* concat_op_function;
    ResolvedFunctionCallBase::ErrorMode concat_op_error_mode;
    std::vector<std::string> function_name_path;
    std::unique_ptr<const FunctionSignature> concat_op_result_signature;
    arg_reorder_index_mapping.clear();

    if (result_signature->result_type().type()->IsArray()) {
      function_name_path.push_back("array_concat");
      ZETASQL_RETURN_IF_ERROR(resolver_->LookupFunctionFromCatalog(
          ast_location, function_name_path,
          Resolver::FunctionNotFoundHandleMode::kReturnError,
          &concat_op_function, &concat_op_error_mode));
      ZETASQL_ASSIGN_OR_RETURN(const FunctionSignature* matched_signature,
                       FindMatchingSignature(concat_op_function, ast_location,
                                             arg_locations_in, named_arguments,
                                             /*name_scope=*/nullptr,
                                             &input_argument_types,
                                             /*arg_overrides=*/nullptr,
                                             &arg_reorder_index_mapping));
      concat_op_result_signature.reset(matched_signature);
    } else {
      function_name_path.push_back("concat");
      ZETASQL_RETURN_IF_ERROR(resolver_->LookupFunctionFromCatalog(
          ast_location, function_name_path,
          Resolver::FunctionNotFoundHandleMode::kReturnError,
          &concat_op_function, &concat_op_error_mode));
      ZETASQL_ASSIGN_OR_RETURN(
          const FunctionSignature* matched_signature,
          FindMatchingSignature(concat_op_function, ast_location,
                                arg_locations_in, named_arguments,
                                /*name_scope=*/nullptr, &input_argument_types,
                                /*arg_overrides=*/nullptr,
                                &arg_reorder_index_mapping));
      concat_op_result_signature.reset(matched_signature);
    }
    *resolved_expr_out = MakeResolvedFunctionCall(
        concat_op_result_signature->result_type().type(), concat_op_function,
        *concat_op_result_signature, std::move(arguments),
        /*generic_argument_list=*/{}, concat_op_error_mode, function_call_info);
  } else if (!arg_overrides.empty()) {
    // Replace the nullptr placeholders with resolved lambdas in
    // <arg_overrides>. We have lambdas so need to use <generic_argument_list>
    // instead of <argument_list>.
    std::vector<std::unique_ptr<const ResolvedFunctionArgument>>
        generic_argument_list;
    generic_argument_list.reserve(arguments.size());
    int arg_override_index = 0;
    // Merge <arguments> and <arg_overrides> into a list of
    // ResolvedFunctionArguments.
    for (int arg_index = 0; arg_index < arguments.size(); arg_index++) {
      if (arguments[arg_index] != nullptr) {
        // Wrap ResolvedExpr as ResolvedFunctionArgument
        generic_argument_list.push_back(
            MakeResolvedFunctionArgument(std::move(arguments[arg_index])));
      } else {
        ZETASQL_RET_CHECK(arg_overrides.size() > arg_override_index);
        ZETASQL_RET_CHECK_EQ(arg_overrides[arg_override_index].index, arg_index);
        generic_argument_list.push_back(
            std::move(arg_overrides[arg_override_index].argument));
        arg_override_index++;
      }
    }
    arguments.clear();
    *resolved_expr_out = MakeResolvedFunctionCall(
        result_signature->result_type().type(), function, *result_signature,
        /*argument_list=*/{}, std::move(generic_argument_list), error_mode,
        function_call_info);
  } else {
    // If there is no lambda argument, we specify <argument_list> so that
    // non-lambda functions stay compatible with existing engine
    // implementations.
    *resolved_expr_out = MakeResolvedFunctionCall(
        result_signature->result_type().type(), function, *result_signature,
        std::move(arguments), /*generic_argument_list=*/{}, error_mode,
        function_call_info);
  }
  ZETASQL_RETURN_IF_ERROR(resolver_->MaybeResolveCollationForFunctionCallBase(
      /*error_location=*/ast_location, (*resolved_expr_out).get()));
  ZETASQL_RETURN_IF_ERROR(resolver_->CheckAndPropagateAnnotations(
      /*error_node=*/ast_location, (*resolved_expr_out).get()));

  if ((*resolved_expr_out)->function()->GetGroup() ==
          Function::kZetaSQLFunctionGroupName &&
      (*resolved_expr_out)->signature().context_id() ==
          FunctionSignatureId::FN_COLLATE) {
    ZETASQL_RETURN_IF_ERROR(ResolveCollationForCollateFunction(
        /*error_location=*/ast_location, (*resolved_expr_out).get()));
  }

  if (ast_location->node_kind() == zetasql::ASTNodeKind::AST_FUNCTION_CALL) {
    auto ast_function_call = ast_location->GetAs<ASTFunctionCall>();
    resolver_->MaybeRecordFunctionCallParseLocation(ast_function_call,
                                                    resolved_expr_out->get());
  } else if (resolver_->analyzer_options().parse_location_record_type() ==
             PARSE_LOCATION_RECORD_FULL_NODE_SCOPE) {
    resolver_->MaybeRecordParseLocation(ast_location, resolved_expr_out->get());
  }
  return absl::OkStatus();
}

absl::Status FunctionResolver::MakeFunctionExprAnalysisError(
    const TemplatedSQLFunction& function, absl::string_view message) {
  std::string result =
      absl::StrCat("Analysis of function ", function.FullName(), " failed");
  if (!message.empty()) {
    absl::StrAppend(&result, ":\n", message);
  }
  return MakeSqlError() << result;
}

// This is a helper method when parsing or analyzing the function's SQL
// expression.  If 'status' is OK, also returns OK. Otherwise, returns a
// new error forwarding any nested errors in 'status' obtained from the
// nested parsing or analysis.
absl::Status FunctionResolver::ForwardNestedResolutionAnalysisError(
    const TemplatedSQLFunction& function, const absl::Status& status,
    ErrorMessageMode mode) {
  ParseResumeLocation parse_resume_location = function.GetParseResumeLocation();
  absl::Status new_status;
  if (status.ok()) {
    return absl::OkStatus();
  } else if (HasErrorLocation(status)) {
    new_status = MakeFunctionExprAnalysisError(function, "");
    zetasql::internal::AttachPayload(
        &new_status,
        SetErrorSourcesFromStatus(
            zetasql::internal::GetPayload<ErrorLocation>(status), status,
            mode, std::string(parse_resume_location.input())));
  } else {
    new_status = StatusWithInternalErrorLocation(
        MakeFunctionExprAnalysisError(function, ""),
        ParseLocationPoint::FromByteOffset(
            parse_resume_location.filename(),
            parse_resume_location.byte_position()));
    zetasql::internal::AttachPayload(
        &new_status,
        SetErrorSourcesFromStatus(
            zetasql::internal::GetPayload<InternalErrorLocation>(new_status),
            status, mode, std::string(parse_resume_location.input())));
  }

  // Update the <new_status> based on <mode>.
  return MaybeUpdateErrorFromPayload(
      mode, parse_resume_location.input(),
      ConvertInternalErrorLocationToExternal(new_status,
                                             parse_resume_location.input()));
}

absl::Status FunctionResolver::ResolveTemplatedSQLFunctionCall(
    const ASTNode* ast_location, const TemplatedSQLFunction& function,
    const AnalyzerOptions& analyzer_options,
    const std::vector<InputArgumentType>& actual_arguments,
    std::shared_ptr<ResolvedFunctionCallInfo>* function_call_info_out) {
  // Check if this function calls itself. If so, return an error. Otherwise, add
  // a pointer to this class to the cycle detector in the analyzer options.
  CycleDetector::ObjectInfo object(
      function.FullName(), &function,
      analyzer_options.find_options().cycle_detector());
  // TODO: Attach proper error locations to the returned Status.
  ZETASQL_RETURN_IF_ERROR(object.DetectCycle("function"));

  // Build a map for the function arguments.
  IdStringHashMapCase<std::unique_ptr<ResolvedArgumentRef>> function_arguments;
  ZETASQL_RET_CHECK_EQ(function.GetArgumentNames().size(), actual_arguments.size());
  // Templated SQL functions only support one signature for now.
  ZETASQL_RET_CHECK_EQ(1, function.NumSignatures());
  ZETASQL_RET_CHECK_GE(function.signatures()[0].arguments().size(),
               actual_arguments.size());
  for (int i = 0; i < actual_arguments.size(); ++i) {
    const IdString arg_name =
        analyzer_options.id_string_pool()->Make(function.GetArgumentNames()[i]);
    const InputArgumentType& arg_type = actual_arguments[i];
    if (zetasql_base::ContainsKey(function_arguments, arg_name)) {
      // TODO: Attach proper error locations to the returned Status.
      return MakeFunctionExprAnalysisError(
          function,
          absl::StrCat("Duplicate argument name ", arg_name.ToString()));
    }
    // Figure out the argument kind to use depending on whether this is a scalar
    // or aggregate function and whether the corresponding argument in the
    // function signature is marked "NOT AGGREGATE".
    ResolvedArgumentDefEnums::ArgumentKind arg_kind;
    if (function.IsAggregate()) {
      if (function.signatures()[0].argument(i).options().is_not_aggregate()) {
        arg_kind = ResolvedArgumentDefEnums::NOT_AGGREGATE;
      } else {
        arg_kind = ResolvedArgumentDefEnums::AGGREGATE;
      }
    } else {
      arg_kind = ResolvedArgumentDefEnums::SCALAR;
    }
    function_arguments[arg_name] =
        MakeResolvedArgumentRef(arg_type.type(), arg_name.ToString(), arg_kind);
  }

  // Create a separate new parser and parse the function's SQL expression from
  // the <parse_resume_location_>. Use the same ID string pool as the
  // original parser.
  ParserOptions parser_options(analyzer_options.id_string_pool(),
                               analyzer_options.arena());
  std::unique_ptr<ParserOutput> parser_output;
  ZETASQL_RETURN_IF_ERROR(ForwardNestedResolutionAnalysisError(
      function,
      ParseExpression(function.GetParseResumeLocation(), parser_options,
                      &parser_output),
      analyzer_options.error_message_mode()));
  Catalog* catalog = catalog_;
  if (function.resolution_catalog() != nullptr) {
    catalog = function.resolution_catalog();
  }

  // Create a separate new resolver and resolve the function's SQL expression,
  // using the specified function arguments.
  // If resolution_catalog_ is set, it is used instead of the catalog passed
  // as the function argument.
  // Otherwise, the catalog passed as the argument is used, and it may include
  // names that were not previously available when the function was initially
  // declared.
  Resolver resolver(catalog, type_factory_, &analyzer_options);

  NameScope empty_name_scope;
  QueryResolutionInfo query_resolution_info(&resolver);
  ExprResolutionInfo expr_resolution_info(
      &empty_name_scope, &empty_name_scope,
      /*allows_aggregation_in=*/function.IsAggregate(),
      /*allows_analytic_in=*/false,
      /*use_post_grouping_columns_in=*/false,
      /*clause_name_in=*/"templated SQL function call", &query_resolution_info);

  std::unique_ptr<const ResolvedExpr> resolved_sql_body;
  ZETASQL_RETURN_IF_ERROR(ForwardNestedResolutionAnalysisError(
      function,
      resolver.ResolveExprWithFunctionArguments(
          function.GetParseResumeLocation().input(),
          parser_output->expression(), &function_arguments,
          &expr_resolution_info, &resolved_sql_body),
      analyzer_options.error_message_mode()));

  if (function.IsAggregate()) {
    const absl::Status status =
        FunctionResolver::CheckCreateAggregateFunctionProperties(
            *resolved_sql_body, /*sql_function_body_location=*/nullptr,
            &expr_resolution_info, &query_resolution_info);
    if (!status.ok()) {
      return ForwardNestedResolutionAnalysisError(
          function, MakeFunctionExprAnalysisError(function, status.message()),
          analyzer_options.error_message_mode());
    }
  }

  // Check the result type of the resolved expression against the expected
  // concrete return type of the function signature, if any. If the types do not
  // match, add a coercion or return an error.

  // Note that ZetaSQL does not yet support overloaded templated functions.
  // So we check that there is exactly one signature and retrieve it.
  ZETASQL_RET_CHECK_EQ(1, function.NumSignatures());
  const FunctionArgumentType& expected_type =
      function.signatures()[0].result_type();
  if (expected_type.kind() == ARG_TYPE_FIXED) {
    if (absl::Status status = resolver_->CoerceExprToType(
            ast_location, expected_type.type(), Resolver::kImplicitCoercion,
            "Function declared to return $0 but the function body produces "
            "incompatible type $1",
            &resolved_sql_body);
        !status.ok()) {
      // TODO: Propagate internal errors, unimplemented, etc
      return MakeFunctionExprAnalysisError(function, status.message());
    }
  }

  function_call_info_out->reset(new TemplatedSQLFunctionCall(
      std::move(resolved_sql_body),
      query_resolution_info.release_aggregate_columns_to_compute()));

  return absl::OkStatus();
}

namespace {
template <typename T>
absl::Status CheckRange(
    T value, const ASTNode* arg_location, int idx,
    const FunctionArgumentTypeOptions& options,
    const std::function<std::string(int)>& BadArgErrorPrefix) {
  // Currently all ranges have integer bounds.
  if (options.has_min_value()) {
    const int64_t min_value = options.min_value();
    if (!(value >= T(min_value))) {  // handles value = NaN
      if (options.has_max_value()) {
        return MakeSqlErrorAt(arg_location)
               << BadArgErrorPrefix(idx) << " must be between "
               << min_value << " and " << options.max_value();
      } else {
        return MakeSqlErrorAt(arg_location)
               << BadArgErrorPrefix(idx) << " must be at least " << min_value;
      }
    }
  }
  if (options.has_max_value()) {
    const int64_t max_value = options.max_value();
    if (!(value <= T(max_value))) {  // handles value = NaN
      if (options.has_min_value()) {
        return MakeSqlErrorAt(arg_location)
               << BadArgErrorPrefix(idx) << " must be between "
               << options.min_value() << " and " << max_value;
      } else {
        return MakeSqlErrorAt(arg_location)
               << BadArgErrorPrefix(idx) << " must be at most " << max_value;
      }
    }
  }
  return absl::OkStatus();
}
}  // namespace

absl::Status FunctionResolver::CheckArgumentValueConstraints(
    const ASTNode* arg_location, int idx, const Value& value,
    const FunctionArgumentType& concrete_argument,
    const std::function<std::string(int)>& BadArgErrorPrefix) const {
  ZETASQL_RET_CHECK(concrete_argument.IsConcrete());
  const FunctionArgumentTypeOptions& options = concrete_argument.options();
  if (value.is_null()) {
    if (options.must_be_non_null()) {
      return MakeSqlErrorAt(arg_location)
             << BadArgErrorPrefix(idx) << " must be non-NULL";
    }
  } else {
    switch (value.type_kind()) {
      case TYPE_INT64:
        return CheckRange<int64_t>(value.int64_value(), arg_location, idx,
                                   options, BadArgErrorPrefix);
      case TYPE_INT32:
        return CheckRange<int64_t>(value.int32_value(), arg_location, idx,
                                   options, BadArgErrorPrefix);
      case TYPE_UINT32:
        return CheckRange<int64_t>(value.uint32_value(), arg_location, idx,
                                   options, BadArgErrorPrefix);
      case TYPE_DOUBLE:
        return CheckRange<double>(value.double_value(), arg_location, idx,
                                  options, BadArgErrorPrefix);
      case TYPE_FLOAT:
        return CheckRange<double>(value.float_value(), arg_location, idx,
                                  options, BadArgErrorPrefix);
      case TYPE_NUMERIC:
        return CheckRange<NumericValue>(value.numeric_value(), arg_location,
                                        idx, options, BadArgErrorPrefix);
      case TYPE_BIGNUMERIC:
        return CheckRange<BigNumericValue>(value.bignumeric_value(),
                                           arg_location, idx, options,
                                           BadArgErrorPrefix);
      default:
        // For other types including UINT64, range check is not supported now.
        ZETASQL_RET_CHECK(!options.has_min_value());
        ZETASQL_RET_CHECK(!options.has_max_value());
    }
  }
  return absl::OkStatus();
}

const Coercer& FunctionResolver::coercer() const {
  ZETASQL_DCHECK(resolver_ != nullptr);
  return resolver_->coercer_;
}

}  // namespace zetasql
