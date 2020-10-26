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

#include "zetasql/analyzer/function_signature_matcher.h"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/value.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "zetasql/base/case.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"

namespace zetasql {
namespace {
// This class performs signature matching during ZetaSQL analysis.
// The functions determine if an argument list can use a function signature.
class FunctionSignatureMatcher {
 public:
  // The class does not take ownership of any of the arguments.
  FunctionSignatureMatcher(const LanguageOptions& language_options,
                           const Coercer& coercer, TypeFactory* type_factory);
  FunctionSignatureMatcher(const FunctionSignatureMatcher&) = delete;
  FunctionSignatureMatcher& operator=(const FunctionSignatureMatcher&) = delete;

  // Determines if the function signature matches the argument list, returning
  // a non-templated signature if true.  If <allow_argument_coercion> is TRUE
  // then function arguments can be coerced to the required signature
  // type(s), otherwise they must be an exact match.
  bool SignatureMatches(const std::vector<InputArgumentType>& input_arguments,
                        const FunctionSignature& signature,
                        bool allow_argument_coercion,
                        std::unique_ptr<FunctionSignature>* result_signature,
                        SignatureMatchResult* signature_match_result) const;

 private:
  const LanguageOptions& language_;  // Not owned.
  const Coercer& coercer_;           // Not owned.
  TypeFactory* type_factory_;        // Not owned.

  // Represents the argument types corresponding to a SignatureArgumentKind.
  // There are three possibilities:
  // 1) The object represents an untyped NULL.
  // 2) The object represents an untyped empty array.
  // 3) The object represents a list of typed arguments.
  // An object in state i and can move to state j if i < j.
  //
  // The main purpose of this class is to keep track of the types associated
  // with a templated SignatureArgumentKind in
  // CheckArgumentTypesAndCollectTemplatedArguments(). There, if we encounter a
  // typed argument for a templated SignatureArgumentKind, we add that to the
  // set. But if there are no typed arguments, then we need to know whether
  // there is an untyped NULL or empty array, because that affects the type we
  // will infer for the SignatureArgumentKind.
  class SignatureArgumentKindTypeSet {
   public:
    enum Kind { UNTYPED_NULL, UNTYPED_EMPTY_ARRAY, TYPED_ARGUMENTS };

    // Creates a set with kind UNTYPED_NULL.
    SignatureArgumentKindTypeSet() : kind_(UNTYPED_NULL) {}
    SignatureArgumentKindTypeSet(const SignatureArgumentKindTypeSet&) = delete;
    SignatureArgumentKindTypeSet& operator=(
        const SignatureArgumentKindTypeSet&) = delete;

    Kind kind() const { return kind_; }

    // Changes the set to kind UNTYPED_EMPTY_ARRAY. Cannot be called if
    // InsertTypedArgument() has already been called.
    void SetToUntypedEmptyArray() {
      ZETASQL_DCHECK(kind_ != TYPED_ARGUMENTS);
      kind_ = UNTYPED_EMPTY_ARRAY;
    }

    // Changes the set to kind TYPED_ARGUMENTS, and adds a typed argument to
    // the set of typed arguments.
    bool InsertTypedArgument(const InputArgumentType& input_argument) {
      // Typed arguments have precedence over untyped arguments.
      ZETASQL_DCHECK(!input_argument.is_untyped());
      kind_ = TYPED_ARGUMENTS;
      return typed_arguments_.Insert(input_argument);
    }

    // Returns the set of typed arguments corresponding to this object. Can only
    // be called if 'kind() == TYPED_ARGUMENTS'.
    const InputArgumentTypeSet& typed_arguments() const {
      ZETASQL_DCHECK_EQ(kind_, TYPED_ARGUMENTS);
      return typed_arguments_;
    }

    std::string DebugString() const;

   private:
    Kind kind_;
    // Does not contain any untyped arguments. Only valid if 'kind_' is
    // TYPED_ARGUMENTS.
    InputArgumentTypeSet typed_arguments_;
  };

  // Maps templated arguments (ARG_TYPE_ANY_1, etc.) to a set of input argument
  // types. See CheckArgumentTypesAndCollectTemplatedArguments() for details.
  typedef std::map<SignatureArgumentKind, SignatureArgumentKindTypeSet>
      ArgKindToInputTypesMap;

  // Maps templated arguments (ARG_TYPE_ANY_1, etc.) to the
  // resolved (possibly coerced) Type each resolved to in a particular function
  // call.
  typedef std::map<SignatureArgumentKind, const Type*> ArgKindToResolvedTypeMap;

  static std::string ArgKindToInputTypesMapDebugString(
      const ArgKindToInputTypesMap& map);

  // Returns the concrete argument type for a given <function_argument_type>,
  // using the mapping from templated to concrete argument types in
  // <templated_argument_map>.  Also used for result types.
  bool GetConcreteArgument(
      const FunctionArgumentType& argument, int num_occurrences,
      const ArgKindToResolvedTypeMap& templated_argument_map,
      std::unique_ptr<FunctionArgumentType>* output_argument) const;

  // Returns a list of concrete arguments by calling GetConcreteArgument()
  // on each entry and setting num_occurrences_ with appropriate argument
  // counts.
  FunctionArgumentTypeList GetConcreteArguments(
      const std::vector<InputArgumentType>& input_arguments,
      const FunctionSignature& signature, int repetitions, int optionals,
      const ArgKindToResolvedTypeMap& templated_argument_map) const;

  // Determines if the argument list count matches signature, returning the
  // number of times each repeated argument repeats and the number of
  // optional arguments present if true.
  bool SignatureArgumentCountMatches(
      const std::vector<InputArgumentType>& input_arguments,
      const FunctionSignature& signature, int* repetitions,
      int* optionals) const;

  // Returns if input argument types match the signature argument types, and
  // updates related templated argument type information.
  //
  // <repetitions> identifies the number of times that repeated arguments
  // repeat.
  //
  // Also populates <templated_argument_map> with a key for every templated
  // SignatureArgumentKind that appears in the signature (including the result
  // type) and <input_arguments>.  The corresponding value is the list of typed
  // arguments that occur for that SignatureArgumentKind. (The list may be empty
  // in the case of untyped arguments.)
  //
  // There is also some special handling for ANY_K: if we see an argument (typed
  // or untyped) for ARRAY_ANY_K, we act as if we also saw the corresponding
  // array element argument for ANY_K, and add an entry to
  // <templated_argument_map> even if ANY_K is not in the signature.
  //
  // Likewise for maps, if we see the map type, we also act as if we've seen
  // the key and value types, and vice versa. Note that the key type does
  // not imply we've seen the value type, nor does the value imply the key.
  bool CheckArgumentTypesAndCollectTemplatedArguments(
      const std::vector<InputArgumentType>& input_arguments,
      const FunctionSignature& signature, int repetitions,
      bool allow_argument_coercion,
      ArgKindToInputTypesMap* templated_argument_map,
      SignatureMatchResult* signature_match_result) const;

  // Returns if a single input argument type matches the corresponding signature
  // argument type, and updates related templated argument type information.
  //
  // Updates <templated_argument_map> for templated SignatureArgumentKind. The
  // corresponding value is the list of typed arguments that occur for that
  // SignatureArgumentKind. (The list may be empty in the case of untyped
  // arguments.)
  bool CheckSingleInputArgumentTypeAndCollectTemplatedArgument(
      const int arg_idx, const InputArgumentType& input_argument,
      const FunctionArgumentType& signature_argument,
      bool allow_argument_coercion,
      ArgKindToInputTypesMap* templated_argument_map,
      SignatureMatchResult* signature_match_result) const;

  // This method is only relevant for table-valued functions. It returns true in
  // 'signature_matches' if a relation input argument type matches a signature
  // argument type, and sets information in 'signature_match_result' either way.
  absl::Status CheckRelationArgumentTypes(
      int arg_idx, const InputArgumentType& input_argument,
      const FunctionArgumentType& signature_argument,
      bool allow_argument_coercion,
      SignatureMatchResult* signature_match_result,
      bool* signature_matches) const;

  // Determines the resolved Type related to all of the templated types present
  // in a function signature. <templated_argument_map> must have been populated
  // by CheckArgumentTypesAndCollectTemplatedArguments().
  bool DetermineResolvedTypesForTemplatedArguments(
      const ArgKindToInputTypesMap& templated_argument_map,
      ArgKindToResolvedTypeMap* resolved_templated_arguments) const;
};

FunctionSignatureMatcher::FunctionSignatureMatcher(
    const LanguageOptions& language_options, const Coercer& coercer,
    TypeFactory* type_factory)
    : language_(language_options),
      coercer_(coercer),
      type_factory_(type_factory) {}

std::string
FunctionSignatureMatcher::SignatureArgumentKindTypeSet::DebugString() const {
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

std::string FunctionSignatureMatcher::ArgKindToInputTypesMapDebugString(
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

SignatureArgumentKind RelatedTemplatedKind(SignatureArgumentKind kind) {
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
  ZETASQL_LOG(DFATAL) << "Unexpected RelatedTemplatedKind: "
              << FunctionArgumentType::SignatureArgumentKindToString(kind);
  // To placate the compiler.
  return kind;
}

bool FunctionSignatureMatcher::GetConcreteArgument(
    const FunctionArgumentType& argument, int num_occurrences,
    const ArgKindToResolvedTypeMap& templated_argument_map,
    std::unique_ptr<FunctionArgumentType>* output_argument) const {
  ZETASQL_DCHECK_NE(argument.kind(), ARG_TYPE_ARBITRARY);
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

FunctionArgumentTypeList FunctionSignatureMatcher::GetConcreteArguments(
    const std::vector<InputArgumentType>& input_arguments,
    const FunctionSignature& signature, int repetitions, int optionals,
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
        ZETASQL_CHECK(GetConcreteArgument(argument, 1 /* num_occurrences */,
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
        ZETASQL_DCHECK_EQ(0, num_occurrences);
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
  ZETASQL_DCHECK_EQ(0, optionals);
  return resolved_argument_list;
}

bool FunctionSignatureMatcher::SignatureArgumentCountMatches(
    const std::vector<InputArgumentType>& input_arguments,
    const FunctionSignature& signature, int* repetitions,
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

bool IsArgKind_ARRAY_ANY_K(SignatureArgumentKind kind) {
  return kind == ARG_ARRAY_TYPE_ANY_1 ||
         kind == ARG_ARRAY_TYPE_ANY_2;
}

bool FunctionSignatureMatcher::CheckArgumentTypesAndCollectTemplatedArguments(
    const std::vector<InputArgumentType>& input_arguments,
    const FunctionSignature& signature, int repetitions,
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
    if (!CheckSingleInputArgumentTypeAndCollectTemplatedArgument(
            arg_idx, input_argument, signature_argument,
            allow_argument_coercion, templated_argument_map,
            signature_match_result)) {
      return false;
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
  // We do the same for PROTO_MAP_ANY if we see entries for the key or value.
  const SignatureArgumentKind result_kind = signature.result_type().kind();
  if (IsArgKind_ARRAY_ANY_K(result_kind) &&
      zetasql_base::ContainsKey(*templated_argument_map,
                       RelatedTemplatedKind(result_kind))) {
    // Creates an UNTYPED_NULL if no entry exists.
    (*templated_argument_map)[result_kind];
  }
  if (result_kind == ARG_PROTO_MAP_ANY &&
      (zetasql_base::ContainsKey(*templated_argument_map, ARG_PROTO_MAP_KEY_ANY) ||
       zetasql_base::ContainsKey(*templated_argument_map, ARG_PROTO_MAP_VALUE_ANY))) {
    (*templated_argument_map)[result_kind];
  }

  return true;
}

// Utility used by above function
bool FunctionSignatureMatcher::
    CheckSingleInputArgumentTypeAndCollectTemplatedArgument(
        const int arg_idx, const InputArgumentType& input_argument,
        const FunctionArgumentType& signature_argument,
        bool allow_argument_coercion,
        ArgKindToInputTypesMap* templated_argument_map,
        SignatureMatchResult* signature_match_result) const {
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
    ZETASQL_DCHECK(input_argument.is_model());
    // We currently only support ANY MODEL signatures and there is no need to
    // to check for coercion given that the models are templated.
  } else if (signature_argument.IsConnection()) {
    ZETASQL_DCHECK(input_argument.is_connection());
    // We currently only support ANY CONNECTION signatures and there is no
    // need to to check for coercion given that the connections are templated.
  } else if (signature_argument.kind() == ARG_TYPE_ARBITRARY) {
    // Arbitrary kind arguments match any input argument type.
  } else if (!signature_argument.IsTemplated()) {
    // Input argument type must either be equivalent or (if coercion is
    // allowed) coercible to signature argument type.
    if (!input_argument.type()->Equivalent(signature_argument.type()) &&
        (!allow_argument_coercion ||
         (!coercer_.CoercesTo(input_argument, signature_argument.type(),
                              false /* is_explicit */,
                              signature_match_result) &&
          !signature_argument.AllowCoercionFrom(input_argument.type())))) {
      return false;
    }
  } else if (input_argument.is_untyped()) {
    // Templated argument, input is an untyped NULL, empty array or empty map.
    // We create an empty entry for them if one does not already exist.
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
    if (kind == ARG_PROTO_MAP_ANY) {
      // As above, we must initialize the types for proto maps. We register
      // both the key and value types.
      (*templated_argument_map)[ARG_PROTO_MAP_KEY_ANY];
      (*templated_argument_map)[ARG_PROTO_MAP_VALUE_ANY];
    }
    if (kind == ARG_PROTO_MAP_KEY_ANY || kind == ARG_PROTO_MAP_VALUE_ANY) {
      // We should always see the map type if we see the key or the value.
      // But they don't imply that we should see each other. For example,
      // DELETE_KEY(map, key) would not include the value type in its
      // signature's template types.
      (*templated_argument_map)[ARG_PROTO_MAP_ANY];
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

    if (signature_argument_kind == ARG_PROTO_MAP_ANY &&
        !IsProtoMap(input_argument.type())) {
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

    auto MakeConcreteArgument = [&](const Type* type) {
      if (input_argument.is_literal()) {
        // Any value will do.
        return InputArgumentType(Value::Null(type));
      } else {
        // Handles the non-literal and query parameter cases.
        return InputArgumentType(type, input_argument.is_query_parameter());
      }
    };

    // If ARRAY_ANY_K is associated with type ARRAY<T> in
    // 'templated_argument_map', then we always bind ANY_K to T.
    if (IsArgKind_ARRAY_ANY_K(signature_argument_kind)) {
      InputArgumentType new_argument = MakeConcreteArgument(
          input_argument.type()->AsArray()->element_type());
      const SignatureArgumentKind related_kind =
          RelatedTemplatedKind(signature_argument_kind);
      (*templated_argument_map)[related_kind].InsertTypedArgument(new_argument);
    }

    if (signature_argument_kind == ARG_PROTO_MAP_ANY) {
      // If this is a proto map argument, we can infer the templated types
      // for the key and value.
      const ProtoType* map_entry_type =
          input_argument.type()->AsArray()->element_type()->AsProto();
      const Type* key_type;
      if (!type_factory_
               ->GetProtoFieldType(map_entry_type->map_key(), &key_type)
               .ok()) {
        return false;
      }
      const Type* value_type;
      if (!type_factory_
               ->GetProtoFieldType(map_entry_type->map_value(), &value_type)
               .ok()) {
        return false;
      }
      (*templated_argument_map)[ARG_PROTO_MAP_KEY_ANY].InsertTypedArgument(
          MakeConcreteArgument(key_type));
      (*templated_argument_map)[ARG_PROTO_MAP_VALUE_ANY].InsertTypedArgument(
          MakeConcreteArgument(value_type));
    }
  }

  return true;
}

absl::Status FunctionSignatureMatcher::CheckRelationArgumentTypes(
    int arg_idx, const InputArgumentType& input_argument,
    const FunctionArgumentType& signature_argument,
    bool allow_argument_coercion, SignatureMatchResult* signature_match_result,
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
                             language_.product_mode()),
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
               coercer_.CoercesTo(InputArgumentType(provided_col_type),
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
          provided_col_type->ShortTypeName(language_.product_mode()),
          (required_schema.is_value_table()
               ? " for value table column with expected type \""
               : absl::StrCat(" for column \"", required_col_name, " ")),
          required_col_type->ShortTypeName(language_.product_mode()),
          "\" of argument ", arg_idx + 1));
      signature_match_result->set_tvf_bad_argument_index(arg_idx);
      *signature_matches = false;
      return absl::OkStatus();
    }
  }
  *signature_matches = true;
  return absl::OkStatus();
}

bool FunctionSignatureMatcher::DetermineResolvedTypesForTemplatedArguments(
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
          const Type* common_supertype =
              coercer_.GetCommonSuperType(type_set.typed_arguments());
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
      ZETASQL_DCHECK(element_type != nullptr);

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
          if (!coercer_.CoercesTo(argument, new_array_type,
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

bool FunctionSignatureMatcher::SignatureMatches(
    const std::vector<InputArgumentType>& input_arguments,
    const FunctionSignature& signature, bool allow_argument_coercion,
    std::unique_ptr<FunctionSignature>* result_signature,
    SignatureMatchResult* signature_match_result) const {
  if (!signature.options().check_all_required_features_are_enabled(
          language_.GetEnabledLanguageFeatures())) {
    return false;
  }

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
          ZETASQL_DCHECK((*arg_type)->AsArray()->element_type()->
                   Equals(*arg_related_type))
              << "arg_type: " << (*arg_type)->DebugString()
              << "\nelement_type: "
              << (*arg_type)->AsArray()->element_type()->DebugString()
              << "\narg_related_type: " << (*arg_related_type)->DebugString();
        } else {
          ZETASQL_DCHECK((*arg_related_type)->IsArray());
          ZETASQL_DCHECK((*arg_related_type)->AsArray()->element_type()->
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
      signature.context_id(), signature.options());

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
}  // namespace

bool FunctionSignatureMatches(
    const LanguageOptions& language_options, const Coercer& coercer,
    const std::vector<InputArgumentType>& input_arguments,
    const FunctionSignature& signature, bool allow_argument_coercion,
    TypeFactory* type_factory,
    std::unique_ptr<FunctionSignature>* result_signature,
    SignatureMatchResult* signature_match_result) {
  FunctionSignatureMatcher signature_matcher(language_options, coercer,
                                             type_factory);
  return signature_matcher.SignatureMatches(
      input_arguments, signature, allow_argument_coercion, result_signature,
      signature_match_result);
}
}  // namespace zetasql
