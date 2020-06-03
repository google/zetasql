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

#include "zetasql/analyzer/function_resolver.h"

#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

#include "zetasql/base/logging.h"
#include "zetasql/analyzer/expr_resolver_helper.h"
#include "zetasql/analyzer/query_resolver_helper.h"
#include "zetasql/analyzer/resolver.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/public/function_signature.h"
#include <cstdint>
#include "zetasql/parser/ast_node_kind.h"
#include "zetasql/parser/parse_tree.h"
#include "zetasql/parser/parse_tree_errors.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/cast.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/cycle_detector.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/sql_function.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/templated_sql_function.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_helper.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "zetasql/base/case.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

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

static std::string* kInvalidBinaryOperatorStr =
    new std::string("$invalid_binary_operator");

const std::string& FunctionResolver::BinaryOperatorToFunctionName(
    ASTBinaryExpression::Op op) {
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
  }
}

std::string FunctionResolver::SignatureArgumentKindTypeSet::DebugString()
    const {
  switch (kind_) {
    case UNTYPED_NULL:
      return "UNTYPED NULL";
    case UNTYPED_EMPTY_ARRAY:
      return "UNTYPED []";
    case TYPED_ARGUMENTS: {
      std::string ret;
      bool first = true;
      for (const InputArgumentType& argument : typed_arguments_.arguments()) {
        absl::StrAppend(&ret, (first ? "" : ", "), argument.DebugString());
        first = false;
      }
      return ret;
    }
  }
}

std::string FunctionResolver::ArgKindToInputTypesMapDebugString(
    const ArgKindToInputTypesMap& map) {
  std::string debug_string;
  for (const auto& map_entry : map) {
    absl::StrAppend(
        &debug_string,
        FunctionArgumentType::SignatureArgumentKindToString(map_entry.first),
        ":\n    ", map_entry.second.DebugString(), "\n");
  }
  return debug_string;
}

static SignatureArgumentKind RelatedTemplatedKind(SignatureArgumentKind kind) {
  switch (kind) {
    case ARG_TYPE_ANY_1:
      return ARG_ARRAY_TYPE_ANY_1;
    case ARG_TYPE_ANY_2:
      return ARG_ARRAY_TYPE_ANY_2;
    case ARG_ARRAY_TYPE_ANY_1:
      return ARG_TYPE_ANY_1;
    case ARG_ARRAY_TYPE_ANY_2:
      return ARG_TYPE_ANY_2;
    default:
      break;
  }
  LOG(DFATAL) << "Unexpected RelatedTemplatedKind: "
              << FunctionArgumentType::SignatureArgumentKindToString(kind);
  // To placate the compiler.
  return kind;
}

bool FunctionResolver::GetConcreteArgument(
    const FunctionArgumentType& argument,
    int num_occurrences,
    const ArgKindToResolvedTypeMap& templated_argument_map,
    std::unique_ptr<FunctionArgumentType>* output_argument) const {
  DCHECK_NE(argument.kind(), ARG_TYPE_ARBITRARY);
  output_argument->reset();
  if (argument.IsTemplated() && !argument.IsRelation() && !argument.IsModel() &&
      !argument.IsConnection()) {
    const Type* const* found_type =
        zetasql_base::FindOrNull(templated_argument_map, argument.kind());
    if (found_type == nullptr) {
      return false;
    }
    *output_argument = absl::make_unique<FunctionArgumentType>(
        *found_type, argument.options(), num_occurrences);
  } else if (argument.IsRelation()) {
    // Table-valued functions should return ARG_TYPE_RELATION. There is no Type
    // object in this case, so return a new FunctionArgumentType with
    // ARG_TYPE_RELATION and the specified number of occurrences.
    *output_argument = absl::make_unique<FunctionArgumentType>(
        ARG_TYPE_RELATION, argument.options(), num_occurrences);
  } else if (argument.IsModel()) {
    *output_argument = absl::make_unique<FunctionArgumentType>(
        ARG_TYPE_MODEL, argument.options(), num_occurrences);
  } else if (argument.IsConnection()) {
    *output_argument = absl::make_unique<FunctionArgumentType>(
        ARG_TYPE_CONNECTION, argument.options(), num_occurrences);
  } else {
    *output_argument = absl::make_unique<FunctionArgumentType>(
        argument.type(), argument.options(), num_occurrences);
  }
  return true;
}

FunctionArgumentTypeList FunctionResolver::GetConcreteArguments(
    const std::vector<InputArgumentType>& input_arguments,
    const FunctionSignature& signature,
    int repetitions,
    int optionals,
    const ArgKindToResolvedTypeMap& templated_argument_map) const {
  if (signature.NumOptionalArguments() == 0 &&
      signature.NumRepeatedArguments() == 0) {
    // Fast path for functions without optional or repeated arguments
    // to resolve.
    FunctionArgumentTypeList resolved_argument_list;
    resolved_argument_list.reserve(signature.arguments().size());
    for (int i = 0; i < signature.arguments().size(); ++i) {
      const FunctionArgumentType& argument = signature.argument(i);
      if (argument.kind() == ARG_TYPE_ARBITRARY) {
        // For arbitrary type arguments the type is derived from the input.
        resolved_argument_list.emplace_back(
            input_arguments[i].type(), argument.cardinality(), 1);
      } else {
        std::unique_ptr<FunctionArgumentType> argument_type;
        // GetConcreteArgument may fail if templated argument's type is not
        // in the map. This can only happen if num_occurrences=0, so it is
        // not expected here.
        CHECK(GetConcreteArgument(argument, 1 /* num_occurrences */,
                                  templated_argument_map, &argument_type));
        resolved_argument_list.push_back(*argument_type);
      }
    }
    return resolved_argument_list;
  }

  bool has_repeated_arbitrary = false;
  for (const FunctionArgumentType& argument : signature.arguments()) {
    if (argument.repeated() && argument.kind() == ARG_TYPE_ARBITRARY) {
      has_repeated_arbitrary = true;
    }
  }

  FunctionArgumentTypeList resolved_argument_list;
  resolved_argument_list.reserve(signature.arguments().size());
  int first_repeated_index = signature.FirstRepeatedArgumentIndex();
  int last_repeated_index = signature.LastRepeatedArgumentIndex();
  int input_position = 0;
  for (int i = 0; i < signature.arguments().size(); ++i) {
    const FunctionArgumentType& argument = signature.argument(i);
    int num_occurrences = 1;
    if (argument.repeated()) {
      num_occurrences =
          (has_repeated_arbitrary && repetitions > 0) ? 1 : repetitions;
    } else if (argument.optional()) {
      if (optionals == 0) {
        num_occurrences = 0;
      }
      optionals -= num_occurrences;
    }
    if (argument.kind() == ARG_TYPE_ARBITRARY) {
      // For arbitrary type arguments the type is derived from the input
      // if available.
      if (num_occurrences > 0) {
        resolved_argument_list.emplace_back(
            input_arguments[input_position].type(), argument.cardinality(), 1);
      } else {
        resolved_argument_list.emplace_back(
            argument.kind(), argument.cardinality(), num_occurrences);
      }
    } else {
      std::unique_ptr<FunctionArgumentType> argument_type;
      // GetConcreteArgument may fail if templated argument's type is not
      // in the map. This can only happen if num_occurrences=0.
      if (!GetConcreteArgument(argument, num_occurrences,
                               templated_argument_map, &argument_type)) {
        DCHECK_EQ(0, num_occurrences);
        argument_type = absl::make_unique<FunctionArgumentType>(
            argument.kind(), argument.cardinality(), 0);
      }
      resolved_argument_list.push_back(*argument_type);
    }

    if (i == last_repeated_index) {
      // This is the end of the block of repeated arguments.
      // Decrease "repetitions" by the num_occurrences we've just output,
      // And, if necessary, go back to the first repeated argument again.
      repetitions -= num_occurrences;
      if (repetitions > 0) {
        i = first_repeated_index - 1;
      }
    }
    input_position += num_occurrences;
  }
  DCHECK_EQ(0, optionals);
  return resolved_argument_list;
}

bool FunctionResolver::SignatureArgumentCountMatches(
    const std::vector<InputArgumentType>& input_arguments,
    const FunctionSignature& signature,
    int* repetitions,
    int* optionals) const {
  const int num_required = signature.NumRequiredArguments();

  *repetitions = 0;
  *optionals = 0;

  if (num_required == input_arguments.size()) {
    // Fast path: exactly the required arguments passed, return early.
    return true;
  }

  const int num_repeated = signature.NumRepeatedArguments();
  const int num_optional = signature.NumOptionalArguments();

  // Initially qualify the signature based on the number of arguments, taking
  // into account optional and repeated arguments.  Find x and y such that:
  //   input_arguments.size() = sig.num_required + x*sig.num_repeated + y
  // where 0 < y <= sig.num_optional.
  if (num_repeated > 0) {
    while (input_arguments.size() >
           num_required + *repetitions * num_repeated + num_optional) {
      ++(*repetitions);
    }
  }
  if (num_optional <
      input_arguments.size() - num_required - *repetitions * num_repeated) {
    // We do not have enough optionals to match the arguments size, and
    // repeating the repeated block again would require too many arguments.
    return false;
  }
  *optionals =
      input_arguments.size() - num_required - *repetitions * num_repeated;

  return true;
}

static bool IsArgKind_ARRAY_ANY_K(SignatureArgumentKind kind) {
  return kind == ARG_ARRAY_TYPE_ANY_1 ||
         kind == ARG_ARRAY_TYPE_ANY_2;
}
static bool IsArgKind_ANY_K(SignatureArgumentKind kind) {
  return kind == ARG_TYPE_ANY_1 ||
         kind == ARG_TYPE_ANY_2;
}

bool FunctionResolver::CheckArgumentTypesAndCollectTemplatedArguments(
    const std::vector<InputArgumentType>& input_arguments,
    const FunctionSignature& signature,
    int repetitions,
    bool allow_argument_coercion,
    ArgKindToInputTypesMap* templated_argument_map,
    SignatureMatchResult* signature_match_result) const {
  const int repeated_idx_start = signature.FirstRepeatedArgumentIndex();
  const int repeated_idx_end = signature.LastRepeatedArgumentIndex();
  int signature_arg_idx = 0;
  int repetition_idx = 0;
  for (int arg_idx = 0; arg_idx < input_arguments.size(); ++arg_idx) {
    const InputArgumentType& input_argument = input_arguments[arg_idx];
    const FunctionArgumentType& signature_argument =
        signature.argument(signature_arg_idx);
    // Compare the input argument to the signature argument. Note that
    // the array types are handled differently because they have the same
    // kind even if the element types are different.
    if (signature_argument.IsRelation() != input_argument.is_relation()) {
      // Relation signature argument types match only relation input arguments.
      // No other signature argument types match relation input arguments.
      return false;
    }
    if (signature_argument.IsModel() != input_argument.is_model()) {
      // Model signature argument types match only model input arguments.
      // No other signature argument types match model input arguments.
      return false;
    }
    if (signature_argument.IsConnection() != input_argument.is_connection()) {
      // Connection signature argument types match only connection input
      // arguments.
      // No other signature argument types match connection input arguments.
      return false;
    }
    if (signature_argument.IsRelation()) {
      bool signature_matches = false;
      const absl::Status status = CheckRelationArgumentTypes(
          arg_idx, input_argument, signature_argument, allow_argument_coercion,
          signature_match_result, &signature_matches);
      ZETASQL_DCHECK_OK(status);
      if (!signature_matches) return false;
    } else if (signature_argument.IsModel()) {
      DCHECK(input_argument.is_model());
      // We currently only support ANY MODEL signatures and there is no need to
      // to check for coercion given that the models are templated.
    } else if (signature_argument.IsConnection()) {
      DCHECK(input_argument.is_connection());
      // We currently only support ANY CONNECTION signatures and there is no
      // need to to check for coercion given that the connections are templated.
    } else if (signature_argument.kind() == ARG_TYPE_ARBITRARY) {
      // Arbitrary kind arguments match any input argument type.
    } else if (!signature_argument.IsTemplated()) {
      // Input argument type must either be equivalent or (if coercion is
      // allowed) coercible to signature argument type.
      if (!input_argument.type()->Equivalent(signature_argument.type()) &&
          (!allow_argument_coercion ||
           (!coercer().CoercesTo(input_argument, signature_argument.type(),
                                 false /* is_explicit */,
                                 signature_match_result) &&
            !signature_argument.AllowCoercionFrom(input_argument.type())))) {
        return false;
      }
    } else if (input_argument.is_untyped()) {
      // Templated argument, input is an untyped NULL or empty array. We create
      // an empty entry for them if one does not already exist.
      const SignatureArgumentKind kind = signature_argument.kind();
      SignatureArgumentKindTypeSet& type_set = (*templated_argument_map)[kind];
      if (type_set.kind() != SignatureArgumentKindTypeSet::TYPED_ARGUMENTS &&
          input_argument.is_untyped_empty_array()) {
        type_set.SetToUntypedEmptyArray();
      }
      // When adding an entry for ARRAY_ANY_K, we must also have one for ANY_K.
      if (IsArgKind_ARRAY_ANY_K(kind)) {
        // Initializes to UNTYPED_NULL if not already set.
        (*templated_argument_map)[RelatedTemplatedKind(kind)];
      }
    } else {
      // Templated argument, input is not null.
      SignatureArgumentKind signature_argument_kind = signature_argument.kind();

      // If it is a templated array type, but the argument type is not
      // an array, then they do not match. Undeclared query parameters are
      // coercible to arrays.
      if (IsArgKind_ARRAY_ANY_K(signature_argument_kind) &&
          !input_argument.type()->IsArray()) {
        return false;
      }

      // If it is a templated enum/proto type, but the argument type is not
      // an enum/proto/struct, then they do not match.
      if ((signature_argument_kind == ARG_ENUM_ANY &&
           !input_argument.type()->IsEnum()) ||
          (signature_argument_kind == ARG_PROTO_ANY &&
           !input_argument.type()->IsProto()) ||
          (signature_argument_kind == ARG_STRUCT_ANY &&
           !input_argument.type()->IsStruct())) {
        return false;
      }

      // Collect input arguments related to a signature's templated argument
      // for subsequent type coercion.
      (*templated_argument_map)[signature_argument_kind].InsertTypedArgument(
          input_argument);

      // If ARRAY_ANY_K is associated with type ARRAY<T> in
      // 'templated_argument_map', then we always bind ANY_K to T.
      if (IsArgKind_ARRAY_ANY_K(signature_argument_kind)) {
        const Type* input_argument_element_type =
            input_argument.type()->AsArray()->element_type();

        InputArgumentType new_argument;
        if (input_argument.is_literal()) {
          // Any value will do.
          new_argument =
              InputArgumentType(Value::Null(input_argument_element_type));
        } else {
          // Handles the non-literal and query parameter cases.
          new_argument = InputArgumentType(input_argument_element_type,
                                           input_argument.is_query_parameter());
        }

        const SignatureArgumentKind related_kind =
            RelatedTemplatedKind(signature_argument_kind);
        (*templated_argument_map)[related_kind].InsertTypedArgument(
            new_argument);
      }
    }

    // Update signature_arg_idx, rewinding for repeated blocks the appropriate
    // number of times.
    if (signature_argument.repeated() &&
        signature_arg_idx == repeated_idx_end &&
        repetition_idx < repetitions - 1) {
      // Need to iterate through the repeated block again.
      ++repetition_idx;
      signature_arg_idx = repeated_idx_start;
    } else {
      ++signature_arg_idx;
    }
  }

  // If the result type is ARRAY_ANY_K and there is an entry for ANY_K, make
  // sure we have an entry for ARRAY_ANY_K, adding an untyped NULL if necessary.
  const SignatureArgumentKind result_kind = signature.result_type().kind();
  if (IsArgKind_ARRAY_ANY_K(result_kind) &&
      zetasql_base::ContainsKey(*templated_argument_map,
                       RelatedTemplatedKind(result_kind))) {
    // Creates an UNTYPED_NULL if no entry exists.
    (*templated_argument_map)[result_kind];
  }

  return true;
}

absl::Status FunctionResolver::CheckRelationArgumentTypes(
    int arg_idx, const InputArgumentType& input_argument,
    const FunctionArgumentType& signature_argument,
    bool allow_argument_coercion,
    SignatureMatchResult* signature_match_result,
    bool* signature_matches) const {
  if (!signature_argument.options().has_relation_input_schema()) {
    // Do nothing. As long as the input argument is a relation, the
    // signature matches.
    *signature_matches = true;
    return absl::OkStatus();
  }
  const TVFRelation& provided_schema = input_argument.relation_input_schema();
  const TVFRelation& required_schema =
      signature_argument.options().relation_input_schema();

  // Store the set of required column names for later reference.
  std::set<std::string, zetasql_base::StringCaseLess> required_col_names;
  for (const TVFRelation::Column& column : required_schema.columns()) {
    required_col_names.emplace(column.name);
  }

  // The input relation argument specifies a required schema. Start by building
  // a map from each provided column's name to its index in the ordered list of
  // columns in the relation.
  std::map<std::string, int, zetasql_base::StringCaseLess>
      provided_col_name_to_required_col_idx;
  for (int provided_col_idx = 0;
       provided_col_idx < provided_schema.num_columns(); ++provided_col_idx) {
    const std::string& provided_col_name =
        provided_schema.column(provided_col_idx).name;
    if (zetasql_base::ContainsKey(required_col_names, provided_col_name)) {
      if (!zetasql_base::InsertOrUpdate(&provided_col_name_to_required_col_idx,
                               provided_col_name, provided_col_idx)) {
        // There was a duplicate column name in the input relation. This is
        // invalid.
        signature_match_result->set_tvf_bad_call_error_message(absl::StrCat(
            "Table-valued function does not allow duplicate input ",
            "columns named \"", provided_col_name, "\" for argument ",
            arg_idx + 1));
        signature_match_result->set_tvf_bad_argument_index(arg_idx);
        *signature_matches = false;
        return absl::OkStatus();
      }
    } else if (!signature_argument.options()
                    .extra_relation_input_columns_allowed() &&
               !required_schema.is_value_table() &&
               !provided_schema.is_value_table()) {
      // There was a column name in the input relation not specified in the
      // required output schema, and the signature does not allow this.
      signature_match_result->set_tvf_bad_call_error_message(
          absl::StrCat("Function does not allow extra input column named \"",
                       provided_col_name, "\" for argument ", arg_idx + 1));
      signature_match_result->set_tvf_bad_argument_index(arg_idx);
      *signature_matches = false;
      return absl::OkStatus();
    }
  }

  // Check that each provided column is either equivalent or coercible to
  // the corresponding required column.
  const int num_required_cols = required_schema.num_columns();
  for (int required_col_idx = 0; required_col_idx < num_required_cols;
       ++required_col_idx) {
    const std::string& required_col_name =
        required_schema.column(required_col_idx).name;
    const Type* required_col_type =
        required_schema.column(required_col_idx).type;

    // Find the index of the matching column in the input relation.
    int provided_col_idx = -1;
    if (required_schema.is_value_table()) {
      provided_col_idx = 0;
      if (provided_schema.num_columns() != 1) {
        // The required value table was not found in the provided input
        // relation. Generate a descriptive error message.
        ZETASQL_RET_CHECK_EQ(1, required_schema.num_columns());
        signature_match_result->set_tvf_bad_call_error_message(
            absl::StrCat("Expected value table of type ",
                         required_schema.column(0).type->ShortTypeName(
                             resolver_->product_mode()),
                         " for argument ", arg_idx + 1));
        signature_match_result->set_tvf_bad_argument_index(arg_idx);
        *signature_matches = false;
        return absl::OkStatus();
      }
    } else {
      const int* lookup = zetasql_base::FindOrNull(provided_col_name_to_required_col_idx,
                                          required_col_name);
      if (lookup == nullptr) {
        // The required column name was not found in the provided input
        // relation. Generate a descriptive error message.
        signature_match_result->set_tvf_bad_call_error_message(absl::StrCat(
            "Required column \"", required_col_name,
            "\" not found in table passed as argument ", arg_idx + 1));
        signature_match_result->set_tvf_bad_argument_index(arg_idx);
        *signature_matches = false;
        return absl::OkStatus();
      }
      provided_col_idx = *lookup;
    }

    // Compare the required column type with the provided column type.
    const Type* provided_col_type =
        provided_schema.column(provided_col_idx).type;
    if (provided_col_type->Equals(required_col_type)) {
      // The provided column type is acceptable. Continue.
    } else if (allow_argument_coercion &&
               coercer().CoercesTo(InputArgumentType(provided_col_type),
                                   required_col_type, false /* is_explicit */,
                                   signature_match_result)) {
      // Make a note to coerce the relation argument later and continue.
      signature_match_result->tvf_map_arg_col_nums_to_coerce_type(
          arg_idx, provided_col_idx, required_col_type);
    } else {
      // The provided column type is invalid. Mark the argument index and
      // column name to return a descriptive error later.
      signature_match_result->set_tvf_bad_call_error_message(absl::StrCat(
          "Invalid type ",
          provided_col_type->ShortTypeName(resolver_->product_mode()),
          (required_schema.is_value_table()
               ? " for value table column with expected type \""
               : absl::StrCat(" for column \"", required_col_name, " ")),
          required_col_type->ShortTypeName(resolver_->product_mode()),
          "\" of argument ", arg_idx + 1));
      signature_match_result->set_tvf_bad_argument_index(arg_idx);
      *signature_matches = false;
      return absl::OkStatus();
    }
  }
  *signature_matches = true;
  return absl::OkStatus();
}

bool FunctionResolver::DetermineResolvedTypesForTemplatedArguments(
  const ArgKindToInputTypesMap& templated_argument_map,
  ArgKindToResolvedTypeMap* resolved_templated_arguments) const {
  for (const auto& templated_argument_entry : templated_argument_map) {
    const SignatureArgumentKind& kind = templated_argument_entry.first;

    const SignatureArgumentKindTypeSet& type_set =
        templated_argument_entry.second;

    if (!IsArgKind_ARRAY_ANY_K(kind)) {
      switch (type_set.kind()) {
        case SignatureArgumentKindTypeSet::UNTYPED_NULL:
          if (kind == ARG_PROTO_ANY || kind == ARG_STRUCT_ANY ||
              kind == ARG_ENUM_ANY) {
            // For a templated proto, enum, or struct type we do not know what
            // the actual type is given just a NULL argument, so we cannot match
            // the signature with all untyped null arguments.
            return false;
          }
          // Untyped non-array arguments have type INT64. InsertOrDie() is safe
          // because 'kind' only occurs once in 'templated_argument_map'.
          zetasql_base::InsertOrDie(resolved_templated_arguments, kind,
                           types::Int64Type());
          break;
        case SignatureArgumentKindTypeSet::UNTYPED_EMPTY_ARRAY:
          if (kind == ARG_PROTO_ANY || kind == ARG_STRUCT_ANY ||
              kind == ARG_ENUM_ANY) {
            // An untyped empty array cannot be matched to a templated proto,
            // enum, or struct type.
            return false;
          }
          // Untyped array arguments have type ARRAY<INT64>. InsertOrDie() is
          // safe because 'kind' only occurs once in 'templated_argument_map'.
          zetasql_base::InsertOrDie(resolved_templated_arguments, kind,
                           types::Int64ArrayType());
          break;
        case SignatureArgumentKindTypeSet::TYPED_ARGUMENTS: {
          const Type* common_supertype = coercer().GetCommonSuperType(
              type_set.typed_arguments());
          if (common_supertype == nullptr) {
            return false;
          }
          // InsertOrDie() is safe because 'kind' only occurs once in
          // 'templated_argument_map'.
          zetasql_base::InsertOrDie(resolved_templated_arguments, kind,
                           common_supertype);
          break;
        }
      }
    } else {
      // For ARRAY_ANY_K, also consider the ArrayType whose element type is
      // bound to ANY_K.
      const SignatureArgumentKind related_kind = RelatedTemplatedKind(kind);
      const Type** element_type =
          zetasql_base::FindOrNull(*resolved_templated_arguments, related_kind);
      // ANY_K is handled before ARRAY_ANY_K.
      DCHECK(element_type != nullptr);

      if ((*element_type)->IsArray()) {
        // Arrays of arrays are not supported.
        return false;
      }

      const Type* new_array_type;
      ZETASQL_CHECK_OK(type_factory_->MakeArrayType(*element_type, &new_array_type));

      // Check that any input arguments coerce to 'new_array_type'.
      if (type_set.kind() == SignatureArgumentKindTypeSet::TYPED_ARGUMENTS) {
        for (const InputArgumentType& argument :
             type_set.typed_arguments().arguments()) {
          SignatureMatchResult unused_result;
          if (!coercer().CoercesTo(argument, new_array_type,
                                   /*is_explicit=*/false, &unused_result)) {
            return false;
          }
        }
      }
      // Use 'new_array_type'. InsertOrDie() is safe because 'kind' only occurs
      // once in 'templated_argument_map'.
      zetasql_base::InsertOrDie(resolved_templated_arguments, kind, new_array_type);
    }
  }

  return true;
}

bool FunctionResolver::SignatureMatches(
    const std::vector<InputArgumentType>& input_arguments,
    const FunctionSignature& signature,
    bool allow_argument_coercion,
    std::unique_ptr<FunctionSignature>* result_signature,
    SignatureMatchResult* signature_match_result) const {
  result_signature->reset();

  int repetitions = 0;
  int optionals = 0;

  // Initially qualify the signature based on the number of arguments, taking
  // into account optional and repeated arguments.  Find x and y such that:
  //   input_arguments.size() = sig.num_required + x*sig.num_repeated + y
  // where 0 < y <= sig.num_optional.
  if (!SignatureArgumentCountMatches(input_arguments, signature, &repetitions,
                                     &optionals)) {
    return false;
  }

  // The signature matches based on just the number of arguments.  Now
  // check for type compatibility and collect all the templated types so
  // we can make sure they match.  <signature_match_result> is updated for
  // non-matched arguments and non-templated arguments.
  ArgKindToInputTypesMap templated_argument_map;
  SignatureMatchResult local_signature_match_result;
  if (!CheckArgumentTypesAndCollectTemplatedArguments(
          input_arguments, signature, repetitions, allow_argument_coercion,
          &templated_argument_map, &local_signature_match_result)) {
    signature_match_result->UpdateFromResult(local_signature_match_result);
    return false;
  }

  // Determine the resolved type associated with the templated types in
  // the signature.
  // TODO: Need to consider allow_argument_coercion here.  We do
  // not currently have a function definition that needs this.
  ArgKindToResolvedTypeMap resolved_templated_arguments;
  if (!DetermineResolvedTypesForTemplatedArguments(
          templated_argument_map, &resolved_templated_arguments)) {
    signature_match_result->UpdateFromResult(local_signature_match_result);
    return false;
  }

  // Sanity check to verify that templated array element types and their
  // corresponding templated types match.
  std::vector<std::pair<SignatureArgumentKind, SignatureArgumentKind>> kinds
      ({{ARG_TYPE_ANY_1, ARG_ARRAY_TYPE_ANY_1},
        {ARG_TYPE_ANY_2, ARG_ARRAY_TYPE_ANY_2}});
  for (const auto& kind : kinds) {
    const Type** arg_type =
        zetasql_base::FindOrNull(resolved_templated_arguments, kind.first);
    if (arg_type != nullptr) {
      const Type** arg_related_type =
          zetasql_base::FindOrNull(resolved_templated_arguments, kind.second);
      if (arg_related_type != nullptr) {
        if ((*arg_type)->IsArray()) {
          DCHECK((*arg_type)->AsArray()->element_type()->
                   Equals(*arg_related_type))
              << "arg_type: " << (*arg_type)->DebugString()
              << "\nelement_type: "
              << (*arg_type)->AsArray()->element_type()->DebugString()
              << "\narg_related_type: " << (*arg_related_type)->DebugString();
        } else {
          DCHECK((*arg_related_type)->IsArray());
          DCHECK((*arg_related_type)->AsArray()->element_type()->
                   Equals(*arg_type));
        }
      }
    }
  }

  // Construct a concrete return signature (result type) if possible.  For
  // templated functions, determining a concrete result type is not always
  // possible here (computing the return type will be done later in
  // ResolveGeneralFunctionCall for TemplatedSQLFunctions).  In this
  // templated function case, the return type is arbitrary.
  std::unique_ptr<FunctionArgumentType> result_type;
  if (signature.result_type().kind() == ARG_TYPE_ARBITRARY) {
    result_type =
        absl::make_unique<FunctionArgumentType>(signature.result_type());
  } else if (!GetConcreteArgument(signature.result_type(),
                                  1 /* num_occurrences */,
                                  resolved_templated_arguments, &result_type)) {
    signature_match_result->UpdateFromResult(local_signature_match_result);
    return false;
  }
  *result_signature = absl::make_unique<FunctionSignature>(
      *result_type,
      GetConcreteArguments(input_arguments, signature, repetitions, optionals,
                           resolved_templated_arguments),
      signature.context_id());
  if (signature.IsDeprecated()) {
    (*result_signature)->SetIsDeprecated(true);
  }
  (*result_signature)
      ->SetAdditionalDeprecationWarnings(
          signature.AdditionalDeprecationWarnings());

  // We have a matching concrete signature, so update <signature_match_result>
  // for all arguments as compared to this signature.
  for (int idx = 0; idx < input_arguments.size(); ++idx) {
    if (input_arguments[idx].is_relation() || input_arguments[idx].is_model() ||
        input_arguments[idx].is_connection()) {
      // The cost of matching a relation/model/connection-type argument is not
      // currently considered in the SignatureMatchResult.
      continue;
    }
    const Type* input_argument_type = input_arguments[idx].type();
    const Type* signature_argument_type =
        (*result_signature)->ConcreteArgumentType(idx);
    if (!input_argument_type->Equals(signature_argument_type)) {
      if (input_arguments[idx].is_untyped()) {
        // Ideally, the coercion cost should be 0 if the argument is any
        // type so <signature_match_result> should be left unchanged.  However,
        // in order to preserve existing behavior we have to treat it like
        // it has literal coercion cost (i.e., the cost of coercing an
        // INT64 NULL literal to the target type).  We cannot modify this
        // without changing the result type of some function calls, like
        // 'ROUND(NULL)' which currently returns FLOAT but would return
        // DOUBLE if coercion cost was 0.  TODO: Figure out if
        // we can change this, without completely breaking existing
        // implementations and queries.
        signature_match_result->incr_literals_coerced();
        signature_match_result->incr_literals_distance(
            GetLiteralCoercionCost(Value::NullInt64(),
                                   signature_argument_type));
      } else if (input_arguments[idx].is_literal()) {
        signature_match_result->incr_literals_coerced();
        signature_match_result->incr_literals_distance(
            GetLiteralCoercionCost(*input_arguments[idx].literal_value(),
                                   signature_argument_type));
      } else {
        signature_match_result->incr_non_literals_coerced();
        signature_match_result->incr_non_literals_distance(
            Type::GetTypeCoercionCost(signature_argument_type->kind(),
                                      input_argument_type->kind()));
      }
    }
  }

  // Propagate information about coercing TVF relation arguments to the final
  // signature match statistics, if applicable.
  for (const std::pair<const std::pair<int, int>, const Type*>& kv :
       local_signature_match_result.tvf_arg_col_nums_to_coerce_type()) {
    signature_match_result->tvf_map_arg_col_nums_to_coerce_type(
        kv.first.first, kv.first.second, kv.second);
  }

  return true;
}

// Get the parse location from a ResolvedNode, if it has one stored in it.
// Otherwise, fall back to the location on an ASTNode.
// Can be used as
//   return MakeSqlErrorAtPoint(GetLocationFromResolvedNode(node, ast_node))
static ParseLocationPoint GetLocationFromResolvedNode(
    const ResolvedNode* node, const ASTNode* fallback) {
  DCHECK(fallback != nullptr);
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

absl::Status FunctionResolver::ProcessNamedArguments(
    const std::string& function_name, const FunctionSignature& signature,
    const ASTNode* ast_location,
    const std::vector<std::pair<const ASTNamedArgument*, int>>& named_arguments,
    bool return_error_if_named_arguments_do_not_match_signature,
    bool* named_arguments_match_signature,
    std::vector<const ASTNode*>* arg_locations,
    std::vector<std::unique_ptr<const ResolvedExpr>>* expr_args,
    std::vector<InputArgumentType>* input_arg_types,
    std::vector<ResolvedTVFArg>* tvf_arg_types) const {
  // Make sure the language feature is enabled.
  if (!named_arguments.empty() &&
      !resolver_->language().LanguageFeatureEnabled(FEATURE_NAMED_ARGUMENTS)) {
    return MakeSqlErrorAt(named_arguments[0].first)
           << "Named arguments are not supported";
  }
  // Build a set of all argument names in the function signature argument
  // options.
  std::set<std::string, zetasql_base::StringCaseLess> argument_names_from_signature_options;
  for (const FunctionArgumentType& arg_type : signature.arguments()) {
    if (arg_type.options().has_argument_name()) {
      ZETASQL_RET_CHECK(zetasql_base::InsertIfNotPresent(&argument_names_from_signature_options,
                                        arg_type.options().argument_name()))
          << "Duplicate named argument " << arg_type.options().argument_name()
          << " found in signature for function " << function_name;
    }
  }
  // Build a map from each argument name to the index in which the named
  // argument appears in <arguments> and <arg_locations>.
  std::map<std::string, int, zetasql_base::StringCaseLess> argument_names_to_indexes;
  int first_named_arg_index = std::numeric_limits<int>::max();
  int last_named_arg_index = -1;
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
      *named_arguments_match_signature = false;
      if (return_error_if_named_arguments_do_not_match_signature) {
        return MakeSqlErrorAt(named_arg.first)
               << "Named argument " << provided_arg_name
               << " not found in signature for call to function "
               << function_name;
      }
      return absl::OkStatus();
    }
    // Keep track of the first and last named argument index.
    first_named_arg_index = std::min(first_named_arg_index, named_arg.second);
    last_named_arg_index = std::max(last_named_arg_index, named_arg.second);
  }
    // Check that named arguments are not followed by positional arguments.
  absl::optional<size_t> num_provided_args;
  if (expr_args != nullptr) {
    num_provided_args = expr_args->size();
  }
  if (input_arg_types != nullptr) {
    if (num_provided_args.has_value()) {
      ZETASQL_RET_CHECK_EQ(
          *num_provided_args, input_arg_types->size());
    }
    num_provided_args = input_arg_types->size();
  }
  if (tvf_arg_types != nullptr) {
    num_provided_args = tvf_arg_types->size();
  }
  if (!named_arguments.empty() &&
      (last_named_arg_index - first_named_arg_index >= named_arguments.size() ||
       last_named_arg_index < num_provided_args.value_or(1) - 1)) {
    return MakeSqlErrorAt(named_arguments.back().first)
           << "Call to function " << function_name << " must not specify "
           << "positional arguments after named arguments; named arguments "
           << "must be specified last in the argument list";
  }
  // Iterate through the function signature and rearrange the provided arguments
  // using the 'argument_names_to_indexes' map.
  std::vector<const ASTNode*> new_arg_locations;
  std::vector<std::unique_ptr<const ResolvedExpr>> new_expr_args;
  std::vector<InputArgumentType> new_input_arg_types;
  std::vector<ResolvedTVFArg> new_tvf_arg_types;
  for (int i = 0; i < signature.arguments().size(); ++i) {
    const FunctionArgumentType& arg_type = signature.arguments()[i];
    const std::string& signature_arg_name =
        arg_type.options().has_argument_name()
            ? arg_type.options().argument_name()
            : "";
    const int* index =
        zetasql_base::FindOrNull(argument_names_to_indexes, signature_arg_name);
    // For positional arguments that appear before any named arguments appear,
    // simply retain their locations and argument types.
    if (named_arguments.empty() || i < named_arguments[0].second) {
      if (arg_locations != nullptr && i < arg_locations->size()) {
        new_arg_locations.push_back(arg_locations->at(i));
      }
      if (expr_args != nullptr && i < expr_args->size()) {
        new_expr_args.push_back(std::move(expr_args->at(i)));
      }
      if (input_arg_types != nullptr && i < input_arg_types->size()) {
        new_input_arg_types.push_back(std::move(input_arg_types->at(i)));
      }
      if (tvf_arg_types != nullptr && i < tvf_arg_types->size()) {
        new_tvf_arg_types.push_back(std::move(tvf_arg_types->at(i)));
      }
      // Make sure that the function signature does not specify an optional name
      // for this positional argument that also appears later as a named
      // argument in the function call.
      if (!signature_arg_name.empty() && index != nullptr) {
        *named_arguments_match_signature = false;
        if (return_error_if_named_arguments_do_not_match_signature) {
          return MakeSqlErrorAt(arg_locations->at(*index))
                 << "Named argument " << signature_arg_name << " is invalid "
                 << "because this call to function " << function_name
                 << " also includes a positional argument corresponding to the "
                 << "same name in the function signature";
        }
        return absl::OkStatus();
      }
      // Make sure that the function signature does not specify an argument
      // name positionally when the options require that it must be named.
      if (!signature_arg_name.empty() &&
          arg_type.options().argument_name_is_mandatory()) {
        *named_arguments_match_signature = false;
        if (return_error_if_named_arguments_do_not_match_signature) {
          return MakeSqlErrorAt(arg_locations->at(i))
                 << "Positional argument is invalid because this function "
                 << "restricts that this argument is referred to by name \""
                 << signature_arg_name << "\" only";
        }
        return absl::OkStatus();
      }
      continue;
    }
    // Lookup the required argument name from the map of provided named
    // arguments. If not found, return an error reporting the missing required
    // argument name.
    if (index == nullptr && arg_type.required()) {
      *named_arguments_match_signature = false;
      if (return_error_if_named_arguments_do_not_match_signature) {
        return MakeSqlErrorAt(ast_location)
               << "Call to function " << function_name
               << " does not include required argument name "
               << signature_arg_name;
      }
      return absl::OkStatus();
    }
    // Repeated argument types may never have required argument names.
    ZETASQL_RET_CHECK_NE(arg_type.cardinality(), FunctionArgumentType::REPEATED)
        << "Call to function " << function_name << " includes named "
        << "argument " << signature_arg_name << " referring to a repeated "
        << "argument type, which is not supported";
    if (arg_locations != nullptr) {
      if (index != nullptr) {
        new_arg_locations.push_back(arg_locations->at(*index));
      } else {
        new_arg_locations.push_back(ast_location);
      }
    }
    if (expr_args != nullptr) {
      if (index != nullptr) {
        new_expr_args.push_back(std::move(expr_args->at(*index)));
      } else {
        // Pass NULL if an optional argument was not named in the function call.
        // Note that by this point we have enforced that the argument is
        // optional by checking against all required and repeated argument types
        // above.
        ZETASQL_RET_CHECK(arg_type.optional());
        new_expr_args.emplace_back(MakeResolvedLiteral(
            arg_type.type(), zetasql::Value::Null(arg_type.type())));
      }
    }
    if (input_arg_types != nullptr) {
      if (index != nullptr) {
        new_input_arg_types.push_back(std::move(input_arg_types->at(*index)));
      } else {
        ZETASQL_RET_CHECK(arg_type.optional());
        new_input_arg_types.emplace_back(
            InputArgumentType(zetasql::Value::Null(arg_type.type())));
      }
    }
    if (tvf_arg_types != nullptr) {
      if (index != nullptr) {
        new_tvf_arg_types.push_back(std::move(tvf_arg_types->at(*index)));
      } else {
        if (arg_type.IsRelation()) {
          *named_arguments_match_signature = false;
          if (return_error_if_named_arguments_do_not_match_signature) {
            return MakeSqlErrorAt(ast_location)
                   << "Call to table valued function " << function_name
                   << "does not specify a value for table argument "
                   << signature_arg_name;
          }
          return absl::OkStatus();
        }
        // Pass NULL if an optional argument was not named in the function call.
        // Note that by this point we have enforced that the argument is
        // optional by checking against all required and repeated argument types
        // above.
        ZETASQL_RET_CHECK(arg_type.optional());
        std::unique_ptr<ResolvedExpr> expr = MakeResolvedLiteral(
            arg_type.type(), zetasql::Value::Null(arg_type.type()));
        ResolvedTVFArg arg;
        arg.SetExpr(std::move(expr));
        new_tvf_arg_types.emplace_back(std::move(arg));
      }
    }
  }
  // Append any remaining provided argument locations and values and move the
  // new argument location and value vectors to the originals.
  // Note that the former step is required in the presence of repeated arguments
  // in the function signature, even though these must be specified
  // positionally.
  if (arg_locations != nullptr) {
    for (size_t i = signature.arguments().size(); i < arg_locations->size();
         ++i) {
      new_arg_locations.push_back(arg_locations->at(i));
    }
    *arg_locations = std::move(new_arg_locations);
  }
  if (expr_args != nullptr) {
    for (size_t i = signature.arguments().size(); i < expr_args->size(); ++i) {
      new_expr_args.push_back(std::move(expr_args->at(i)));
    }
    *expr_args = std::move(new_expr_args);
  }
  if (input_arg_types != nullptr) {
    for (size_t i = signature.arguments().size(); i < input_arg_types->size();
         ++i) {
      new_input_arg_types.push_back(std::move(input_arg_types->at(i)));
    }
    *input_arg_types = std::move(new_input_arg_types);
  }
  if (tvf_arg_types != nullptr) {
    for (size_t i = signature.arguments().size(); i < tvf_arg_types->size();
         ++i) {
      new_tvf_arg_types.push_back(std::move(tvf_arg_types->at(i)));
    }
    *tvf_arg_types = std::move(new_tvf_arg_types);
  }
  *named_arguments_match_signature = true;
  return absl::OkStatus();
}

// TODO: Eventually we want to keep track of the closest
// signature even if there is no match, so that we can provide a good
// error message.  Currently, this code takes an early exit if a signature
// does not match and does not accurately determine how close the signature
// was, nor does it keep track of the best non-matching signature.
zetasql_base::StatusOr<const FunctionSignature*>
FunctionResolver::FindMatchingSignature(
    const Function* function,
    const std::vector<InputArgumentType>& input_arguments_in,
    const ASTNode* ast_location,
    const std::vector<const ASTNode*>& arg_locations_in,
    const std::vector<std::pair<const ASTNamedArgument*, int>>& named_arguments)
    const {
  std::unique_ptr<FunctionSignature> best_result_signature;
  SignatureMatchResult best_result;

  VLOG(6) << "FindMatchingSignature for function: "
          << function->DebugString(/*verbose=*/true) << "\n  for arguments: "
          << InputArgumentType::ArgumentsToString(
                 input_arguments_in, ProductMode::PRODUCT_INTERNAL);

  for (const FunctionSignature& signature : function->signatures()) {
    // Check if the function call contains any named arguments, and rearrange
    // 'input_arguments' appropriately if so.
    std::vector<InputArgumentType> input_arguments = input_arguments_in;
    std::vector<const ASTNode*> arg_locations = arg_locations_in;
    bool named_arguments_match_signature = false;
    ZETASQL_RETURN_IF_ERROR(ProcessNamedArguments(
        function->FullName(), signature, ast_location, named_arguments,
        /*return_error_if_named_arguments_do_not_match_signature=*/
        (function->NumSignatures() == 1), &named_arguments_match_signature,
        &arg_locations, /*expr_args=*/nullptr, &input_arguments,
        /*tvf_arg_types=*/nullptr));
    if (!named_arguments_match_signature) {
      continue;
    }
    std::unique_ptr<FunctionSignature> result_signature;
    SignatureMatchResult signature_match_result;
    if (SignatureMatches(input_arguments, signature,
                         function->ArgumentsAreCoercible(), &result_signature,
                         &signature_match_result)) {
      if (!signature.CheckArgumentConstraints(input_arguments)) {
        // If this signature has argument constraints and they are not
        // satisfied then ignore the signature.
        continue;
      }

      VLOG(6) << "Found signature for input arguments: "
              << InputArgumentType::ArgumentsToString(
                     input_arguments, ProductMode::PRODUCT_INTERNAL)
              << "\nfunction signature: "
              << signature.DebugString(/*function_name=*/"",
                                       /*verbose=*/true)
              << "\nresult signature: "
              << result_signature->DebugString(/*function_name=*/"",
                                               /*verbose=*/true)
              << "\n  cost: " << signature_match_result.DebugString();

      if ((best_result_signature == nullptr) ||
          (signature_match_result.IsCloserMatchThan(best_result))) {
        best_result_signature = std::move(result_signature);
        best_result = signature_match_result;
      } else {
        VLOG(4) << "Found duplicate signature matches for function: "
                << function->DebugString() << "\nGiven input arguments: "
                << InputArgumentType::ArgumentsToString(
                       input_arguments, ProductMode::PRODUCT_INTERNAL)
                << "\nBest result signature: "
                << best_result_signature->DebugString()
                << "\n  cost: " << best_result.DebugString()
                << "\nDuplicate signature: " << result_signature->DebugString()
                << "\n  cost: " << signature_match_result.DebugString();
      }
    }
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
      true /* has_explicit_type */);
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
      DCHECK_EQ(ast_struct->field_expressions().size(),
                to_struct_type->num_fields());
      *field_arg_locations = ToLocations(ast_struct->field_expressions());
      break;
    }
    case AST_STRUCT_CONSTRUCTOR_WITH_KEYWORD: {
      const ASTStructConstructorWithKeyword* ast_struct =
          cast_free_ast_location->GetAs<ASTStructConstructorWithKeyword>();
      DCHECK_EQ(ast_struct->fields().size(), to_struct_type->num_fields());
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

// TODO The intent of this function seems to be for the caller to
// check that the types don't match before it calls this.  It may simplify
// things if the check happens inside this function instead, and this turns
// into a no-op if there's no change required.  I tried add an early bailout
// if the types matched and a few tests started failing, mostly because of
// differences in has_explicit_type.  This could probably be simplified.
absl::Status FunctionResolver::AddCastOrConvertLiteral(
    const ASTNode* ast_location, const Type* target_type,
    const ResolvedScan* scan, bool set_has_explicit_type,
    bool return_null_on_error,
    std::unique_ptr<const ResolvedExpr>* argument) const {
  ZETASQL_RET_CHECK(ast_location != nullptr);

  // We add the casts field-by-field for struct expressions.  We will collapse
  // a ResolvedMakeStruct to a struct literal if all the fields are literals
  // and <set_has_explicit_type> is true. If the MakeStruct is the result of a
  // path expression rather than an explicit struct constructor with fields, use
  // a struct cast.
  if (target_type->IsStruct() &&
      argument->get()->node_kind() == RESOLVED_MAKE_STRUCT &&
      ast_location->node_kind() != AST_PATH_EXPRESSION) {
    // Remove constness so that we can add casts on the field expressions inside
    // the struct.
    ResolvedMakeStruct* struct_expr = const_cast<ResolvedMakeStruct*>(
        argument->get()->GetAs<ResolvedMakeStruct>());
    const StructType* to_struct_type = target_type->AsStruct();
    ZETASQL_RET_CHECK_EQ(struct_expr->field_list_size(), to_struct_type->num_fields());

    std::vector<const ASTNode*> field_arg_locations;
    // If we can't obtain the locations of field arguments and replace literals
    // inside that expression, their parse locations will be wrong.
    ZETASQL_RETURN_IF_ERROR(ExtractStructFieldLocations(
        to_struct_type, ast_location, &field_arg_locations));

    std::vector<std::unique_ptr<const ResolvedExpr>> field_exprs =
        struct_expr->release_field_list();
    for (int i = 0; i < to_struct_type->num_fields(); ++i) {
      if (to_struct_type->field(i).type->Equals(field_exprs[i]->type())) {
        if (field_exprs[i]->node_kind() == RESOLVED_LITERAL &&
            set_has_explicit_type &&
            !field_exprs[i]->GetAs<ResolvedLiteral>()->has_explicit_type()) {
          // This field has the same Type, but is a literal that needs to
          // have it set as has_explicit_type so we must replace the
          // expression.
          field_exprs[i] = resolver_->MakeResolvedLiteral(
              field_arg_locations[i],
              field_exprs[i]->GetAs<ResolvedLiteral>()->value().type(),
              field_exprs[i]->GetAs<ResolvedLiteral>()->value(),
              true /* has_explicit_type */);
        }
      }

      const absl::Status cast_status = AddCastOrConvertLiteral(
          field_arg_locations[i], to_struct_type->field(i).type, scan,
          set_has_explicit_type, return_null_on_error, &field_exprs[i]);
      if (!cast_status.ok()) {
        // Propagate "Out of stack space" errors.
        if (cast_status.code() == absl::StatusCode::kResourceExhausted) {
          return cast_status;
        }
        return MakeSqlErrorAt(field_arg_locations[i]) << cast_status.message();
      }
    }
    *argument = MakeResolvedMakeStruct(target_type, std::move(field_exprs));

    // If all the fields are now explicitly casted literals, then we can
    // convert this MakeStruct into a Literal instead.
    ConvertMakeStructToLiteralIfAllExplicitLiteralFields(argument);

    return absl::OkStatus();
  } else if ((*argument)->node_kind() == RESOLVED_FUNCTION_CALL &&
             (*argument)->GetAs<ResolvedFunctionCall>()->function()->FullName(
                 true /* include_group */) == "ZetaSQL:error") {
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

  if (argument_literal != nullptr) {
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
  if (type_assigned) {
    return absl::OkStatus();
  }

  return resolver_->ResolveCastWithResolvedArgument(
      ast_location, target_type, return_null_on_error, argument);
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
  zetasql_base::StatusOr<Value> coerced_literal_value;  // Initialized to UNKNOWN
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
    // that returns a zetasql_base::StatusOr<Value>, making 'success' unnecessary and
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
        DCHECK_EQ(field_literal->node_kind(), RESOLVED_LITERAL);
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
                  resolver_->language(), target_type);
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

  auto replacement_literal = MakeResolvedLiteral(
      target_type, coerced_literal_value.value(), set_has_explicit_type);
  // The float literal cache entry (if there is one) is no longer valid after
  // replacement.
  resolver_->float_literal_images_.erase(argument_literal->float_literal_id());
  if (resolver_->analyzer_options_.record_parse_locations()) {
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
  ZETASQL_RETURN_IF_ERROR(
      resolver_->LookupFunctionFromCatalog(ast_location, function_name_path,
                                           &function, &error_mode));
  return ResolveGeneralFunctionCall(
      ast_location, arg_locations, function, error_mode, is_analytic,
      std::move(arguments), std::move(named_arguments), expected_result_type,
      resolved_expr_out);
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

absl::Status FunctionResolver::ResolveGeneralFunctionCall(
    const ASTNode* ast_location,
    const std::vector<const ASTNode*>& arg_locations_in,
    const Function* function, ResolvedFunctionCallBase::ErrorMode error_mode,
    bool is_analytic,
    std::vector<std::unique_ptr<const ResolvedExpr>> arguments,
    std::vector<std::pair<const ASTNamedArgument*, int>> named_arguments,
    const Type* expected_result_type,
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
           << function->QualifiedSQLName(true /* capitalize_qualifier */)
           << " does not support an OVER clause";
  }

  std::vector<InputArgumentType> input_argument_types;
  GetInputArgumentTypesForExprList(arguments, &input_argument_types);

  // Check initial argument constraints, if any.
  if (function->PreResolutionConstraints() != nullptr) {
    ZETASQL_RETURN_IF_ERROR(StatusWithInternalErrorLocation(
        function->CheckArgumentConstraints(
            input_argument_types, resolver_->language(),
            function->PreResolutionConstraints()),
        ast_location, include_leftmost_child));
  }

  std::unique_ptr<const FunctionSignature> result_signature;
  ZETASQL_ASSIGN_OR_RETURN(
      const FunctionSignature* signature,
      FindMatchingSignature(function, input_argument_types, ast_location,
                            arg_locations, named_arguments));
  result_signature.reset(signature);

  if (nullptr == result_signature) {
    ProductMode product_mode = resolver_->language().product_mode();
    const std::string supported_signatures =
        function->GetSupportedSignaturesUserFacingText(resolver_->language());
    if (!supported_signatures.empty()) {
      return MakeSqlErrorAtNode(ast_location, include_leftmost_child)
             << function->GetNoMatchingFunctionSignatureErrorMessage(
                    input_argument_types, product_mode)
             << ". Supported signature"
             // When there are multiple signatures, say "signatures", otherwise
             // say "signature".
             << (function->signatures().size() > 1 ? "s" : "") << ": "
             << supported_signatures;
    } else {
      if (function->GetSupportedSignaturesCallback() == nullptr) {
        // If we do not have any supported signatures and there is
        // no custom callback for producing the signature messages,
        // then we provide an error message as if the function did
        // not exist at all (similar to the error message produced in
        // Resolver::LookupFunctionFromCatalog()). Note that it does
        // not make sense to try to suggest a different function name
        // in this context (like we do in LookupFunctionFromCatalog()).
        return MakeSqlErrorAtNode(ast_location, include_leftmost_child)
               << "Function not found: " << function->SQLName();
      } else {
        // In some cases, like for 'IN', we do not produce a suggested
        // signature.  But we still want to get a 'no matching signature'
        // error message since it indicates the invalid arguments (rather
        // than a 'function not found' message, which would be odd in
        // this case since IN does exist).
        return MakeSqlErrorAtNode(ast_location, include_leftmost_child)
               << function->GetNoMatchingFunctionSignatureErrorMessage(
                      input_argument_types, product_mode);
      }
    }
  }
  // If the function call includes any named arguments, call the
  // ProcessNamedArguments method to update the order of 'arg_locations',
  // 'arguments', and 'input_argument_types' appropriately.
  bool named_arguments_match_signature = false;
  ZETASQL_RETURN_IF_ERROR(ProcessNamedArguments(
      function->FullName(), *result_signature, ast_location, named_arguments,
      /*return_error_if_named_arguments_do_not_match_signature=*/false,
      &named_arguments_match_signature, &arg_locations, &arguments,
      &input_argument_types, /*tvf_arg_types=*/nullptr));
  ZETASQL_RET_CHECK(named_arguments_match_signature);
  ZETASQL_RET_CHECK(result_signature->HasConcreteArguments());
  if (!function->Is<TemplatedSQLFunction>()) {
    ZETASQL_RET_CHECK(result_signature->IsConcrete());
  }

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

  for (int idx = 0; idx < arguments.size(); ++idx) {
    // The ZETASQL_RET_CHECK above ensures that the arguments are concrete for both
    // templated and non-templated functions.
    const FunctionArgumentType& concrete_argument =
        result_signature->ConcreteArgument(idx);
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
      ZETASQL_RETURN_IF_ERROR(AddCastOrConvertLiteral(
          arg_locations[idx], target_type, nullptr /* scan */,
          false /* set_has_explicit_type */, false /* return_null_on_error */,
          &arguments[idx]));
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
    std::vector<InputArgumentType> input_arguments;
    GetInputArgumentTypesForExprList(arguments, &input_arguments);
    ZETASQL_RETURN_IF_ERROR(StatusWithInternalErrorLocation(
        function->CheckArgumentConstraints(
            input_arguments, resolver_->language(),
            function->PostResolutionConstraints()),
        ast_location, include_leftmost_child));
  }

  if (function->GetComputeResultTypeCallback() != nullptr) {
    // Note that the result type of SQL functions cannot be overridden, since
    // the result type is determined by the type of the resolved SQL expression.
    ZETASQL_RET_CHECK(!function->Is<TemplatedSQLFunction>())
        << function->DebugString();
    ZETASQL_RET_CHECK(!function->Is<SQLFunctionInterface>())
        << function->DebugString();
    std::vector<InputArgumentType> input_arguments;
    GetInputArgumentTypesForExprList(arguments, &input_arguments);
    CycleDetector owned_cycle_detector;
    const zetasql_base::StatusOr<const Type*> result_type =
        function->GetComputeResultTypeCallback()(
            catalog_, type_factory_, &owned_cycle_detector, input_arguments,
            resolver_->analyzer_options());
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
    std::vector<InputArgumentType> input_arguments;
    GetInputArgumentTypesForExprList(arguments, &input_arguments);
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
        ast_location, *sql_function, analyzer_options, input_arguments,
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

    if (result_signature->result_type().type()->IsArray()) {
      function_name_path.push_back("array_concat");
      ZETASQL_RETURN_IF_ERROR(resolver_->LookupFunctionFromCatalog(
          ast_location, function_name_path, &concat_op_function,
          &concat_op_error_mode));
      ZETASQL_ASSIGN_OR_RETURN(const FunctionSignature* matched_signature,
                       FindMatchingSignature(
                           concat_op_function, input_argument_types,
                           ast_location, arg_locations_in, named_arguments));
      concat_op_result_signature.reset(matched_signature);
    } else {
      function_name_path.push_back("concat");
      ZETASQL_RETURN_IF_ERROR(resolver_->LookupFunctionFromCatalog(
          ast_location, function_name_path, &concat_op_function,
          &concat_op_error_mode));
      ZETASQL_ASSIGN_OR_RETURN(const FunctionSignature* matched_signature,
                       FindMatchingSignature(
                           concat_op_function, input_argument_types,
                           ast_location, arg_locations_in, named_arguments));
      concat_op_result_signature.reset(matched_signature);
    }
    *resolved_expr_out = MakeResolvedFunctionCall(
        concat_op_result_signature->result_type().type(), concat_op_function,
        *concat_op_result_signature, std::move(arguments), concat_op_error_mode,
        function_call_info);
  } else {
    *resolved_expr_out = MakeResolvedFunctionCall(
        result_signature->result_type().type(), function, *result_signature,
        std::move(arguments), error_mode, function_call_info);
  }

  if (ast_location->node_kind() == zetasql::ASTNodeKind::AST_FUNCTION_CALL) {
    auto ast_function_call = ast_location->GetAs<ASTFunctionCall>();
    resolver_->MaybeRecordParseLocation(ast_function_call->function(),
                                        resolved_expr_out->get());
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
  const Type* return_type = resolved_sql_body->type();
  // Note that ZetaSQL does not yet support overloaded templated functions.
  // So we check that there is exactly one signature and retrieve it.
  ZETASQL_RET_CHECK_EQ(1, function.NumSignatures());
  const FunctionArgumentType& expected_type =
      function.signatures()[0].result_type();
  if (expected_type.kind() == ARG_TYPE_FIXED &&
      !return_type->Equals(expected_type.type())) {
    const InputArgumentType input_argument_type =
        GetInputArgumentTypeForExpr(resolved_sql_body.get());
    SignatureMatchResult result;
    if (coercer().CoercesTo(input_argument_type, expected_type.type(),
                            /*is_explicit=*/false, &result)) {
      const absl::Status status = this->AddCastOrConvertLiteral(
          ast_location, expected_type.type(), /*scan=*/nullptr,
          /*set_has_explicit_type=*/false, /*return_null_on_error=*/false,
          &resolved_sql_body);
      if (!status.ok()) {
        return MakeFunctionExprAnalysisError(function, status.message());
      }
    } else {
      return MakeFunctionExprAnalysisError(
          function,
          absl::StrCat("Function declared to return ",
                       expected_type.type()->ShortTypeName(
                           analyzer_options.language().product_mode()),
                       " but the function body produces incompatible type ",
                       return_type->ShortTypeName(
                           analyzer_options.language().product_mode())));
    }
  }

  // Return the final TemplatedSQLUDFCall with the resolved expression.
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
  static_assert(std::is_same<T, int64_t>::value || std::is_same<T, double>::value,
                "CheckRange supports only int64_t and double");
  // Currently all ranges have integer bounds.
  if (options.has_min_value()) {
    const int64_t min_value = options.min_value();
    if (!(value >= min_value)) {  // handles value = NaN
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
    if (!(value <= max_value)) {  // handles value = NaN
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
      default:
        // For other types including UINT64, range check is not supported now.
        ZETASQL_RET_CHECK(!options.has_min_value());
        ZETASQL_RET_CHECK(!options.has_max_value());
    }
  }
  return absl::OkStatus();
}

const Coercer& FunctionResolver::coercer() const {
  DCHECK(resolver_ != nullptr);
  return resolver_->coercer_;
}

}  // namespace zetasql
