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

#include "zetasql/public/coercer.h"

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/cast.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "absl/algorithm/container.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"

namespace zetasql {
namespace {
using SuperTypesMap = absl::flat_hash_map<TypeKind, const std::set<TypeKind>>;
}  // namespace

static const SuperTypesMap* InitializeSuperTypesMap() {
  SuperTypesMap* val = new SuperTypesMap;
  // Initialize supertypes map.
  std::vector<std::pair<TypeKind, std::vector<TypeKind>>> raw = {
      {TYPE_BOOL, {TYPE_BOOL}},
      {TYPE_INT32,
       {TYPE_INT32, TYPE_INT64, TYPE_NUMERIC, TYPE_BIGNUMERIC, TYPE_DOUBLE}},
      {TYPE_INT64, {TYPE_INT64, TYPE_NUMERIC, TYPE_BIGNUMERIC, TYPE_DOUBLE}},
      {TYPE_UINT32,
       {TYPE_INT64, TYPE_UINT32, TYPE_UINT64, TYPE_NUMERIC, TYPE_BIGNUMERIC,
        TYPE_DOUBLE}},
      {TYPE_UINT64, {TYPE_UINT64, TYPE_NUMERIC, TYPE_BIGNUMERIC, TYPE_DOUBLE}},
      {TYPE_FLOAT, {TYPE_FLOAT, TYPE_DOUBLE}},
      {TYPE_DOUBLE, {TYPE_DOUBLE}},
      {TYPE_STRING, {TYPE_STRING}},
      {TYPE_BYTES, {TYPE_BYTES}},
      {TYPE_ENUM, {TYPE_ENUM}},
      {TYPE_PROTO, {TYPE_PROTO}},
      {TYPE_ARRAY, {TYPE_ARRAY}},
      {TYPE_DATE, {TYPE_DATE}},
      {TYPE_TIMESTAMP, {TYPE_TIMESTAMP}},
      // TODO: Add relevant tests for TIME and DATETIME.
      {TYPE_TIME, {TYPE_TIME}},
      {TYPE_DATETIME, {TYPE_DATETIME}},
      {TYPE_GEOGRAPHY, {TYPE_GEOGRAPHY}},
      {TYPE_NUMERIC, {TYPE_NUMERIC, TYPE_BIGNUMERIC, TYPE_DOUBLE}},
      {TYPE_BIGNUMERIC, {TYPE_BIGNUMERIC, TYPE_DOUBLE}}};

  for (const auto& entry : raw) {
    TypeKind type = entry.first;
    std::set<TypeKind> supertypes;
    for (const auto& supertype : entry.second) {
      CHECK(supertypes.emplace(supertype).second)
          << "Duplicate supertype entry " << TypeKind_Name(supertype)
          << " while processing " << TypeKind_Name(type);
    }
    CHECK(zetasql_base::ContainsKey(supertypes, type))
        << "Missing self in list of supertypes for " << TypeKind_Name(type);
    CHECK(val->emplace(type, std::move(supertypes)).second)
        << "Duplicate entry " << TypeKind_Name(type);
  }
  return val;
}

// Map from a TypeKind to a set of TypeKinds that are its supertypes.
// Every TypeKind is a supertype of itself.
static const SuperTypesMap& supertypes() {
  static const SuperTypesMap* supertypes = InitializeSuperTypesMap();
  return *supertypes;
}

int GetLiteralCoercionCost(const Value& literal_value, const Type* to_type) {
  if (literal_value.is_null()) {
    // The cost of coercing NULL to any type is 1.
    // TODO: Seems like this should be 0.  Try it and see what
    // breaks/changes.
    return 1;
  }
  return Type::GetTypeCoercionCost(to_type->kind(),
                                   literal_value.type()->kind());
}

static bool GetCastFunctionType(
    const TypeKind from_kind, const TypeKind to_kind,
    CastFunctionType* cast_type) {
  const CastFunctionProperty* cast_function_property =
      zetasql_base::FindOrNull(GetZetaSQLCasts(), TypeKindPair(from_kind, to_kind));
  if (cast_function_property == nullptr) {
    return false;
  }
  if (cast_type != nullptr) {
    *cast_type = cast_function_property->type;
  }
  return true;
}

void Coercer::StripFieldAliasesFromStructType(const Type** struct_type) const {
  if (!(*struct_type)->IsStruct()) {
    // Nothing to strip here.
    return;
  }

  const StructType* in_type = (*struct_type)->AsStruct();
  std::vector<StructType::StructField> struct_field_types;
  for (int i = 0; i < in_type->num_fields(); ++i) {
    const Type* field_type = in_type->field(i).type;
    if (field_type->IsStruct()) {
      StripFieldAliasesFromStructType(&field_type);
    }
    struct_field_types.push_back({"" /* anonymous field */, field_type});
  }
  ZETASQL_CHECK_OK(type_factory_->MakeStructType(struct_field_types, struct_type));
}

const StructType* Coercer::GetCommonStructSuperType(
    const InputArgumentTypeSet& argument_set) const {
  DCHECK(!argument_set.arguments().empty());
  const InputArgumentType* dominant_argument = argument_set.dominant_argument();
  DCHECK(dominant_argument != nullptr);

  const Type* common_type = dominant_argument->type();
  for (const InputArgumentType& argument : argument_set.arguments()) {
    if (!argument.type()->IsStruct() && !argument.is_untyped()) {
      // Structs and non-struct have no common supertype, so if one
      // of the arguments is non-struct then there is no common struct
      // supertype.
      return nullptr;
    }

    if (common_type != nullptr && !common_type->Equals(argument.type())) {
      common_type = nullptr;
    }
  }
  // If all types are equal, return that type.
  if (common_type != nullptr) {
    DCHECK(common_type->IsStruct());
    return common_type->AsStruct();
  }

  DCHECK(dominant_argument->type()->IsStruct());
  const StructType* dominant_struct = dominant_argument->type()->AsStruct();
  const int num_struct_fields = dominant_struct->num_fields();
  std::vector<InputArgumentTypeSet> struct_field_argument_sets(
      num_struct_fields);

  // We insert the fields arguments from dominant_argument first as we
  // need to ensure that we always use field aliases from the
  // dominant_argument only, even for nested structs.
  DCHECK_EQ(dominant_argument->field_types_size(), num_struct_fields)
      << "Dominant argument: " << dominant_argument->DebugString()
      << "Argument set: " << argument_set.ToString(true /* verbose */);

  for (int i = 0; i < num_struct_fields; ++i) {
    // We have identified the dominant argument, and its fields should
    // also be dominant even if they are an untyped NULL.  This properly
    // handles struct supertype cases such as:
    //
    //   SELECT [ NULL,
    //            (NULL, NULL),
    //            struct<a int32_t, b int64_t>(1, 2) ]
    //
    // In this example, the second argument is dominant so the resulting
    // type should be struct<int32_t, int64_t> with anonymous fields since
    // this dominant argument does not have field names.  If we did not
    // allow untyped NULLs to be dominant in the field argument sets,
    // then the field names from the third argument would leak through.
    struct_field_argument_sets[i].Insert(
        dominant_argument->field_type(i), true /* set_dominant */);
  }

  for (const InputArgumentType& argument_type : argument_set.arguments()) {
    if (argument_type.is_untyped()) {
      continue;
    }

    const StructType* struct_type = argument_type.type()->AsStruct();
    if (struct_type->num_fields() != num_struct_fields) {
      return nullptr;
    }
    DCHECK_EQ(argument_type.field_types_size(), num_struct_fields);
    for (int i = 0; i < num_struct_fields; ++i) {
      struct_field_argument_sets[i].Insert(argument_type.field_type(i));
    }
  }

  std::vector<StructType::StructField> supertyped_field_types;
  for (int i = 0; i < num_struct_fields; ++i) {
    const Type* common_field_type = GetCommonSuperType(
        struct_field_argument_sets[i]);
    if (common_field_type == nullptr) {
      return nullptr;
    } else {
      // If the dominant_argument field is an untyped NULL, we would pick the
      // field aliases for the nested struct from some other non-NULL field
      // argument which is incorrect.  To fix this, we strip off the
      // field aliases from the nested struct supertype here.
      if (common_field_type->IsStruct() &&
          dominant_argument->field_type(i).is_untyped()) {
        StripFieldAliasesFromStructType(&common_field_type);
      }

      supertyped_field_types.push_back(
          {dominant_argument->type()->AsStruct()->field(i).name,
           common_field_type});
    }
  }

  const StructType* supertyped_struct_type = nullptr;
  ZETASQL_CHECK_OK(type_factory_->MakeStructType(supertyped_field_types,
                                         &supertyped_struct_type));
  return supertyped_struct_type;
}

const ArrayType* Coercer::GetCommonArraySuperType(
    const InputArgumentTypeSet& argument_set,
    bool treat_query_parameters_as_literals) const {
  // First build an InputArgumentTypeSet corresponding to the element types in
  // 'argument_set'.
  InputArgumentTypeSet element_argument_set;
  for (const InputArgumentType& argument : argument_set.arguments()) {
    if (argument.is_untyped()) continue;
    const Type* argument_type = argument.type();
    if (!argument_type->IsArray()) return nullptr;
    const Type* element_type = argument_type->AsArray()->element_type();

    InputArgumentType element_argument;
    if (argument.is_literal()) {
      // Just choose an arbitrary literal value for the element.
      element_argument = InputArgumentType(Value::Null(element_type));
    } else {
      element_argument =
          InputArgumentType(element_type, argument.is_query_parameter());
    }

    const bool is_dominant_argument =
        (argument_set.dominant_argument() == &argument);
    element_argument_set.Insert(element_argument, is_dominant_argument);
  }

  // Next, compute the supertype for the elements.
  const Type* element_supertype = GetCommonSuperTypeImpl(
      element_argument_set, treat_query_parameters_as_literals);
  if (element_supertype == nullptr) return nullptr;

  // Finally, attempt to coerce the individual arrays to the new array type.
  const ArrayType* new_array_type;
  ZETASQL_CHECK_OK(type_factory_->MakeArrayType(element_supertype, &new_array_type));
  for (const InputArgumentType& argument : argument_set.arguments()) {
    SignatureMatchResult result;
    if (!CoercesTo(argument, new_array_type, /*is_explicit=*/false, &result)) {
      return nullptr;
    }
  }

  return new_array_type;
}

const Type* Coercer::GetCommonSuperType(
    const InputArgumentTypeSet& argument_set) const {
  const Type* common_supertype = GetCommonSuperTypeImpl(
      argument_set, false /* treat_parameters_as_literals */);
  if (common_supertype != nullptr) {
    return common_supertype;
  }
  return GetCommonSuperTypeImpl(argument_set,
                                true /* treat_parameters_as_literals */);
}

// TODO: Add some unit tests for untyped empty array to coercer_test
// and coercer_supertypes_test.
const Type* Coercer::GetCommonSuperTypeImpl(
    const InputArgumentTypeSet& argument_set,
    bool treat_parameters_as_literals) const {
  if (argument_set.empty()) {
    return nullptr;
  }
  const InputArgumentType* dominant_argument =
      argument_set.dominant_argument();
  if (dominant_argument != nullptr) {
    DCHECK(absl::c_linear_search(argument_set.arguments(), *dominant_argument));
  }

  for (const InputArgumentType& argument : argument_set.arguments()) {
    // There are dedicated methods for special cases.
    if (argument.type()->IsStruct()) {
      return GetCommonStructSuperType(argument_set);
    }
    if (argument.type()->IsArray() && !argument.is_untyped()) {
      return GetCommonArraySuperType(argument_set,
                                     treat_parameters_as_literals);
    }
  }

  std::vector<const InputArgumentType*> typed_arguments;
  bool has_untyped_empty_array = false;
  bool has_floating_point_input = false;
  bool has_numeric_input = false;
  bool has_bignumeric_input = false;
  for (const InputArgumentType& argument : argument_set.arguments()) {
    // Ignore untyped arguments since they do not contribute towards
    // supertyping if any other argument type is present. Also keeps
    // track if there is untyped empty array in the argument.
    if (argument.is_untyped()) {
      if (argument.is_untyped_empty_array()) {
        has_untyped_empty_array = true;
      }
      continue;
    }
    if (argument.type()->IsFloatingPoint()) {
      has_floating_point_input = true;
    }
    if (argument.type()->IsNumericType()) {
      has_numeric_input = true;
    }
    if (argument.type()->IsBigNumericType()) {
      has_bignumeric_input = true;
    }
    typed_arguments.push_back(&argument);
  }

  if (typed_arguments.empty()) {
    if (!has_untyped_empty_array) {
      // All arguments were untyped NULL, so return the default NULL
      // type (INT64).
      return type_factory_->get_int64();
    } else {
      // There are untyped empty arrays and possibly untyped NULLs.
      // In this case, return the default ARRAY<INT64> type.
      const ArrayType* array_type = nullptr;
      if (type_factory_->MakeArrayType(type_factory_->get_int64(),
                                       &array_type).ok()) {
        return array_type;
      } else {
        return nullptr;
      }
    }
  }

  DCHECK(dominant_argument != nullptr);
  const Type* common_type = dominant_argument->type();
  for (int i = 0; i < typed_arguments.size(); ++i) {
    const Type* type = typed_arguments[i]->type();
    if (nullptr != common_type && !common_type->Equals(type)) {
      common_type = nullptr;
    }
  }

  // If all types are equal, then return the dominant type.
  if (nullptr != common_type) {
    return common_type;
  }

  // For each non-literal argument, get the set of its supertypes.  Also,
  // get the set of literal argument types.
  std::vector<const std::set<TypeKind>*> non_literal_candidate_supertypes;
  InputArgumentTypeSet literal_types;
  // Always add the dominant type (as a non-literal) to literal_types
  // so its type will be treated as the first type.
  literal_types.Insert(InputArgumentType(dominant_argument->type()));

  for (const InputArgumentType& it : argument_set.arguments()) {
    if (it.is_untyped()) {
      // Ignore untyped NULL arguments since they coerce to any type.
      continue;
    }
    if (it.is_literal() ||
        (treat_parameters_as_literals && it.is_query_parameter())) {
      // This argument is a literal or parameter to be treated like a literal.
      // We add a new InputArgumentType with just a Type (and no Value or
      // parameter) to <literals_types> for later handling in case
      // there are no non-literals present.
      literal_types.Insert(InputArgumentType(it.type()));
      continue;
    }
    const std::set<TypeKind>* type_kinds =
        zetasql_base::FindOrNull(supertypes(), it.type()->kind());
    DCHECK(type_kinds != nullptr);
    non_literal_candidate_supertypes.push_back(type_kinds);
  }

  VLOG(6) << "non_literal_candidate_supertypes.size(): "
          << non_literal_candidate_supertypes.size();

  if (non_literal_candidate_supertypes.empty()) {
    DCHECK(!literal_types.empty());
    // We did not have any non-literal arguments but we did have literal
    // arguments.  The <literal_types> list contains related
    // arguments that are neither literals nor parameters and we perform
    // supertype analysis over them instead.  Since these arguments are
    // not literals or parameters, recursion will not happen more than
    // once and <treat_parameters_as_literals> is irrelevant.
    return GetCommonSuperTypeImpl(literal_types,
                                  treat_parameters_as_literals);
  }

  // Find the most specific TypeKind (i.e., lowest Type::KindSpecificity())
  // that appears in all sets.  We do this by sorting the TypeKinds in the
  // first set by type specificity from most to least specific, then iterate
  // through the sorted TypeKinds and return the first one that appears in
  // all the sets.
  std::vector<TypeKind> initial_candidates;
  for (const TypeKind& kind : *non_literal_candidate_supertypes[0]) {
    initial_candidates.push_back(kind);
  }
  std::sort(initial_candidates.begin(), initial_candidates.end(),
            Type::KindSpecificityLess);  // Most specific first.

  std::vector<TypeKind>
      common_supertype_kinds;  // Most specific to least specific.
  for (const TypeKind& kind : initial_candidates) {
    bool found_in_all = true;
    for (const std::set<TypeKind>* supertypes :
         non_literal_candidate_supertypes) {
      if (!zetasql_base::ContainsKey(*supertypes, kind)) {
        found_in_all = false;
        break;
      }
    }
    if (found_in_all) {
      common_supertype_kinds.push_back(kind);
    }
  }
  if (common_supertype_kinds.empty()) {
    VLOG(6) << "No common supertype found, return NULL";
    return nullptr;
  }

  VLOG(6) << "common_supertype_kinds: "
          << Type::TypeKindListToString(common_supertype_kinds,
                                        PRODUCT_INTERNAL);

  // Iterate through the common kinds, from most specific to least specific.
  // For each candidate type, see if all the literals can implicitly coerce.
  // If so, this is the common supertype.  If not, check the next candidate
  // type.
  for (const TypeKind& most_specific_kind : common_supertype_kinds) {
    // Even though signed and unsigned integers have double as a common
    // supertype, double is not an allowed supertype of combinations of
    // signed/unsigned integers unless a floating point numeric (non-literal
    // or literal) is also present in the arguments.
    // Similarly, NUMERIC and BIGNUMERIC are not allowed as a supertype unless
    // an argument of the type is present.
    if ((most_specific_kind == TYPE_DOUBLE && !has_floating_point_input) ||
        (most_specific_kind == TYPE_NUMERIC && !has_numeric_input) ||
        (most_specific_kind == TYPE_BIGNUMERIC && !has_bignumeric_input)) {
      continue;
    }

    const Type* candidate_type = nullptr;
    if (Type::IsSimpleType(most_specific_kind)) {
      candidate_type = type_factory_->MakeSimpleType(most_specific_kind);
    } else {
      // For non-simple types to have a supertype, all non-literals must have
      // the same kind, which should match the kind of the dominant argument.
      // All complex types should be equivalent (implying they are coercible),
      // and we want to pick the dominant argument type as the supertype.
      candidate_type = dominant_argument->type();
      if (candidate_type == nullptr ||
          candidate_type->kind() != most_specific_kind) {
        continue;
      }
      // Check that all other non-literal args are equivalent to the dominant
      // argument.  Otherwise, coercion would not be allowed.
      bool all_equivalent = true;
      for (const auto& arg : typed_arguments) {
        if (!(arg->is_literal() || (treat_parameters_as_literals &&
                                    arg->is_query_parameter())) &&
            !arg->type()->Equivalent(candidate_type)) {
          VLOG(6) << "Argument " << arg->DebugString()
                  << " does not have equivalent type to "
                  << candidate_type->DebugString();;
          all_equivalent = false;
          break;
        }
      }
      if (!all_equivalent) {
        continue;
      }
    }
    DCHECK(nullptr != candidate_type);
    VLOG(6) << "candidate type: " << candidate_type->DebugString();

    // We have a candidate.  Check parameters and literals to see if they
    // can all be implicitly coerced.
    bool parameters_and_literals_can_coerce = true;
    for (const InputArgumentType& argument : argument_set.arguments()) {
      SignatureMatchResult result;
      if (argument.is_untyped()) {
        // Untyped nulls coerce to any type.
        continue;
      } else if (argument.is_literal() &&
                 !LiteralCoercesTo(*argument.literal_value(), candidate_type,
                                   false /* not explicit */, &result)) {
        VLOG(6) << "Literal argument "
                << argument.literal_value()->FullDebugString()
                << " does not coerce to candidate type "
                << candidate_type->DebugString();
        parameters_and_literals_can_coerce = false;
        break;
      } else if (treat_parameters_as_literals &&
                 argument.is_query_parameter() &&
                 !ParameterCoercesTo(argument.type(), candidate_type,
                                     false /* not explicit */, &result)) {
        VLOG(6) << "Parameter argument " << argument.DebugString()
                << " does not coerce to candidate type "
                << candidate_type->DebugString();
        parameters_and_literals_can_coerce = false;
        break;
      }
    }
    if (parameters_and_literals_can_coerce) {
      return candidate_type;
    }
  }
  return nullptr;  // No common supertype.
}

bool Coercer::CoercesTo(const InputArgumentType& from_argument,
                        const Type* to_type, bool is_explicit,
                        SignatureMatchResult* result) const {
  if (from_argument.is_untyped()) {
    // This is an untyped NULL, so the cost of coercion is considered as 0
    // and <result> is left unchanged.
    // TODO: Should this be consistent with untyped NULL coercion
    // cost defined in SignatureMatches?  We have to force it to be non-zero
    // there to avoid changing signature matching (and therefore the
    // result type for 'ROUND(NULL)', etc.
    return true;
  }
  if (from_argument.type()->IsStruct()) {
    return StructCoercesTo(from_argument, to_type, is_explicit, result);
  }
  if (from_argument.type()->IsArray()) {
    return ArrayCoercesTo(from_argument, to_type, is_explicit, result);
  }
  if (from_argument.literal_value() != nullptr) {
    return LiteralCoercesTo(*(from_argument.literal_value()), to_type,
                            is_explicit, result);
  }
  if (from_argument.is_query_parameter()) {
    return ParameterCoercesTo(from_argument.type(), to_type, is_explicit,
                              result);
  }
  return TypeCoercesTo(from_argument.type(), to_type, is_explicit, result);
}

bool Coercer::AssignableTo(const InputArgumentType& from_argument,
                           const Type* to_type, bool is_explicit,
                           SignatureMatchResult* result) const {
  if (CoercesTo(from_argument, to_type, is_explicit, result)) {
    return true;
  }
  if ((from_argument.type()->IsInt64() && to_type->IsInt32()) ||
      (from_argument.type()->IsUint64() && to_type->IsUint32())) {
    return true;
  }
  return false;
}

bool Coercer::ParameterCoercesTo(const Type* from_type, const Type* to_type,
                                 bool is_explicit,
                                 SignatureMatchResult* result) const {
  if (from_type->IsStruct()) {
    return StructCoercesTo(
        InputArgumentType(from_type, true /* is_query_parameter */), to_type,
        is_explicit, result);
  }
  if (from_type->IsArray()) {
    return ArrayCoercesTo(
        InputArgumentType(from_type, /*is_query_parameter=*/true), to_type,
        is_explicit, result);
  }

  const CastFunctionProperty* property = zetasql_base::FindOrNull(
      GetZetaSQLCasts(), TypeKindPair(from_type->kind(), to_type->kind()));
  if (property != nullptr &&
      (SupportsParameterCoercion(property->type) ||
       (is_explicit && SupportsExplicitCast(property->type))) &&
      (from_type->IsSimpleType() || to_type->IsSimpleType() ||
       from_type->Equivalent(to_type))) {
    // Count these the same as literal coercion.  This is because
    // it is a useful property to have the same coercion cost and stability
    // between two queries where one uses literals and the other uses
    // same-type parameters interchanged with the literals.
    result->incr_literals_coerced();
    result->incr_literals_distance(property->cost);
    return true;
  }
  result->incr_non_matched_arguments();
  return false;
}

bool Coercer::TypeCoercesTo(const Type* from_type, const Type* to_type,
                            bool is_explicit,
                            SignatureMatchResult* result) const {
  const CastFunctionProperty* property = zetasql_base::FindOrNull(
      GetZetaSQLCasts(), TypeKindPair(from_type->kind(), to_type->kind()));
  if (property == nullptr) {
    result->incr_non_matched_arguments();
    return false;
  }
  if (from_type->IsStruct()) {
    return StructCoercesTo(InputArgumentType(from_type), to_type, is_explicit,
                           result);
  }
  if (from_type->IsArray()) {
    return ArrayCoercesTo(InputArgumentType(from_type), to_type, is_explicit,
                          result);
  }
  if (!is_explicit && !SupportsImplicitCoercion(property->type)) {
    result->incr_non_matched_arguments();
    return false;
  }
  // Enum and proto types can coerce to a (same or different) type if
  // the from/to types are equivalent.
  if (!from_type->IsSimpleType() && !to_type->IsSimpleType()) {
    if (!from_type->Equivalent(to_type)) {
      result->incr_non_matched_arguments();
      return false;
    }
  }

  result->incr_non_literals_coerced();
  result->incr_non_literals_distance(property->cost);
  return true;
}

bool Coercer::StructCoercesTo(const InputArgumentType& struct_argument,
                              const Type* to_type, bool is_explicit,
                              SignatureMatchResult* result) const {
  // Expected invariant.
  DCHECK(struct_argument.type()->IsStruct());
  const StructType* from_struct = struct_argument.type()->AsStruct();
  if (!to_type->IsStruct() ||
      from_struct->num_fields() != to_type->AsStruct()->num_fields()) {
    result->incr_non_matched_arguments();
    return false;
  }

  const StructType* to_struct = to_type->AsStruct();
  if (struct_argument.is_literal_null()) {
    // We still must check the field types for coercibility.
    for (int idx = 0; idx < to_struct->num_fields(); ++idx) {
      SignatureMatchResult local_result;
      if (!CoercesTo(InputArgumentType(from_struct->field(idx).type),
                     to_struct->field(idx).type,
                     is_explicit, &local_result)) {
        result->incr_non_matched_arguments();
        return false;
      }
    }
    // Coercion distance of NULL to any type is 1.
    result->incr_literals_coerced();
    result->incr_literals_distance(
        GetLiteralCoercionCost(*struct_argument.literal_value(), to_type));
    return true;
  }

  // Note that this can be empty if the input argument has no fields (for
  // example, 'STRUCT<>()')
  const std::vector<InputArgumentType>& struct_field_types =
      struct_argument.field_types();
  // We expect a non-NULL struct with matching number of fields.
  DCHECK(!struct_argument.is_literal_null());

  std::vector<Value> coerced_field_values;
  for (int idx = 0; idx < to_struct->num_fields(); ++idx) {
    // Test expected invariant that the field types in the struct and the
    // InputArgumentType field types are the same.
    DCHECK(struct_field_types[idx].type()->Equals(
             from_struct->field(idx).type))
        << "struct_field_types[" << idx << "]: "
        << struct_field_types[idx].type()->DebugString()
        << "; from_struct->field(" << idx << ").type: "
        << from_struct->field(idx).type->DebugString();
    if (!from_struct->field(idx).type->Equals(to_struct->field(idx).type)) {
      SignatureMatchResult local_result;
      if (struct_field_types[idx].is_literal()) {
        if (!LiteralCoercesTo(*struct_field_types[idx].literal_value(),
                              to_struct->field(idx).type, is_explicit,
                              &local_result)) {
          result->incr_non_matched_arguments();
          return false;
        }
      } else {
        // The struct field is not a literal.
        if (!CoercesTo(struct_field_types[idx],
                       to_struct->field(idx).type,
                       is_explicit, &local_result)) {
          result->incr_non_matched_arguments();
          return false;
        }
      }
      result->UpdateFromResult(local_result);
    }
  }
  return true;
}

bool Coercer::ArrayCoercesTo(const InputArgumentType& array_argument,
                             const Type* to_type, bool is_explicit,
                             SignatureMatchResult* result) const {
  DCHECK(array_argument.type()->IsArray());
  const ArrayType* from_array = array_argument.type()->AsArray();
  if (from_array->Equivalent(to_type)) return true;

  if (language_options_.LanguageFeatureEnabled(
          FEATURE_V_1_1_CAST_DIFFERENT_ARRAY_TYPES) &&
      to_type->IsArray()) {
    const Type* from_element_type = from_array->element_type();

    const ArrayType* to_array = to_type->AsArray();
    const Type* to_element_type = to_array->element_type();

    if (array_argument.is_literal() || array_argument.is_query_parameter()) {
      InputArgumentType element_arg;
      if (array_argument.is_literal()) {
        // Just check whether a NULL element coerces; any value will do.
        element_arg = InputArgumentType(Value::Null(from_element_type));
      } else {
        element_arg = InputArgumentType(from_element_type,
                                        /*is_query_parameter=*/true);
      }
      return CoercesTo(element_arg, to_element_type, is_explicit, result);
    }

    if (is_explicit) {
      return TypeCoercesTo(from_element_type, to_element_type, is_explicit,
                           result);
    }
  }

  result->incr_non_matched_arguments();
  return false;
}

bool Coercer::LiteralCoercesTo(const Value& literal_value, const Type* to_type,
                               bool is_explicit,
                               SignatureMatchResult* result) const {
  SignatureMatchResult local_result;
  if (TypeCoercesTo(literal_value.type(), to_type, is_explicit,
                    &local_result)) {
    // General Type coercion is allowed independent of literalness.
    result->incr_literals_coerced();
    result->incr_literals_distance(local_result.non_literals_distance());
    return true;
  }
  if (literal_value.type()->IsStruct()) {
    // Structs are coerced on a field-by-field basis.
    return StructCoercesTo(InputArgumentType(literal_value), to_type,
                           is_explicit, result);
  }
  if (!literal_value.type()->IsSimpleType()) {
    // Non-struct complex-typed literals (enum, proto, array) coerce exactly
    // like non-literals, so if the general Type coercion check failed
    // above, then the complex literal does not coerce.
    result->incr_non_matched_arguments();
    return false;
  }
  // There are a few additional special cases for literal coercion in addition
  // to general implicit coercion.  For example, narrowing coercions are
  // required because the parser produces INT64, DOUBLE, and STRING literals but
  // not other INT, FLOAT or DATE/TIMESTAMP literals, and we need to be able
  // to swizzle them to these other literal types in order to provide desired
  // behavior.
  CastFunctionType cast_type;
  const bool is_valid_cast = GetCastFunctionType(literal_value.type()->kind(),
                                                 to_type->kind(), &cast_type);
  if (is_valid_cast &&
      (SupportsLiteralCoercion(cast_type) ||
       (is_explicit && SupportsExplicitCast(cast_type)))) {
    // TODO: We may want to consider implicitly coercing a STRING
    // literal to DATE/TIMESTAMP to be an exact match (or distance 0) since we
    // don't produce DATE/TIMESTAMP literals in the parser.  The parsed
    // representation is STRING and matching this to a function signature
    // seems like it should logically be considered as an exact match.  Same
    // for narrowing a DOUBLE to FLOAT, and INT64 to other INT types.
    result->incr_literals_coerced();
    result->incr_literals_distance(GetLiteralCoercionCost(literal_value,
                                                          to_type));
    return true;
  }
  result->incr_non_matched_arguments();
  return false;
}

}  // namespace zetasql
