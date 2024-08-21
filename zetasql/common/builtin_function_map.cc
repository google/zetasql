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

#include <string>
#include <vector>

#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/map_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/functional/bind_front.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {
constexpr absl::string_view kMapFromArray = "MAP_FROM_ARRAY";
constexpr absl::string_view kMapEntriesSorted = "MAP_ENTRIES_SORTED";
constexpr absl::string_view kMapEntriesUnsorted = "MAP_ENTRIES_UNSORTED";
}

static absl::Status CheckMapFromArrayPreResolutionArguments(
    absl::Span<const InputArgumentType> arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() != 1) {
    return MakeSqlError() << "No matching signature for function "
                          << kMapFromArray
                          << ". Supported signature: " << kMapFromArray
                          << "(ARRAY<STRUCT<T1, T2>>)";
  }

  if (arguments[0].is_untyped()) {
    return MakeSqlError()
           << kMapFromArray
           << " result type cannot be determined from "
              "argument "
           << arguments[0].UserFacingName(language_options.product_mode())
           << ". Consider casting the argument to ARRAY<STRUCT<T1, T2>> so "
              "that key type T1 and value type T2 can be determined from the "
              "argument";
  }
  return absl::OkStatus();
}

static absl::StatusOr<const Type*> ComputeMapFromArrayResultType(
    Catalog* catalog, TypeFactory* type_factory, CycleDetector* cycle_detector,
    const FunctionSignature& signature,
    absl::Span<const InputArgumentType> arguments,
    const AnalyzerOptions& analyzer_options) {
  ZETASQL_RET_CHECK_EQ(arguments.size(), 1);
  auto& input_argument = arguments[0];

  auto make_error_struct_arr_expected =
      [&]() {
        return MakeSqlError()
               << kMapFromArray
               << " input argument must be an array of structs, but got type "
               << input_argument.type()->TypeName(
                      analyzer_options.language().product_mode());
      };
  if (!input_argument.type()->IsArray()) {
    return make_error_struct_arr_expected();
  }

  auto* array_element_type = input_argument.type()->AsArray()->element_type();

  if (!array_element_type->IsStruct()) {
    return make_error_struct_arr_expected();
  }

  auto* struct_type = array_element_type->AsStruct();
  if (struct_type->num_fields() != 2) {
    return MakeSqlError()
           << kMapFromArray << " input array must be of type "
           << "ARRAY<STRUCT<T1, T2>>, but found a struct member with "
           << struct_type->num_fields() << " fields";
  }

  if (!struct_type->field(0).type->SupportsGrouping(
          analyzer_options.language())) {
    return MakeSqlError() << kMapFromArray
                          << " expected a groupable key, but got a key of type "
                          << struct_type->field(0).type->TypeName(
                                 analyzer_options.language().product_mode())
                          << ", which does not support grouping";
  }
  return type_factory->MakeMapType(struct_type->field(0).type,
                                   struct_type->field(1).type);
}

static absl::StatusOr<const Type*> ComputeMapEntriesFunctionResultType(
    absl::string_view function_name, bool require_orderable_key,
    Catalog* catalog, TypeFactory* type_factory, CycleDetector* cycle_detector,
    const FunctionSignature& signature,
    absl::Span<const InputArgumentType> arguments,
    const AnalyzerOptions& analyzer_options) {
  ZETASQL_RET_CHECK_EQ(arguments.size(), 1);
  auto& input_argument = arguments[0];

  if (!input_argument.type()->IsMap()) {
    return MakeSqlError()
           << function_name
           << " input argument must be of type MAP<K, V>, but got type "
           << arguments[0].type()->TypeName(
                  analyzer_options.language().product_mode());
  }

  const Type* map_key_type = GetMapKeyType(input_argument.type());
  std::string ordering_type_description;
  if (require_orderable_key &&
      !map_key_type->SupportsOrdering(analyzer_options.language(),
                                      &ordering_type_description)) {
    return MakeSqlError() << function_name
                          << " map key type must be orderable, but was not: "
                          << ordering_type_description << " is not orderable";
  }

  const Type* struct_type;
  ZETASQL_RET_CHECK_OK(type_factory->MakeStructType(
      {StructType::StructField("key", map_key_type),
       StructType::StructField("value",
                               GetMapValueType(input_argument.type()))},
      &struct_type));

  const Type* array_type;
  ZETASQL_RET_CHECK_OK(type_factory->MakeArrayType(struct_type, &array_type));
  return array_type;
}

static inline absl::StatusOr<const MapType*> GetMapTypeFromInputArg(
    const InputArgumentType& map_arg) {
  ZETASQL_RET_CHECK(map_arg.type() != nullptr && map_arg.type()->IsMap())
      << "Input must be a map";
  return map_arg.type()->AsMap();
}

static inline absl::Status MakeErrorIfMapElementTypeNotOrderable(
    absl::string_view function_name, const Type* orderable_type,
    const LanguageOptions& language_options) {
  std::string ordering_type_description;
  if (!orderable_type->SupportsOrdering(language_options,
                                        &ordering_type_description)) {
    return MakeSqlError() << function_name << ": MAP element type "
                          << ordering_type_description << " is not orderable";
  }
  return absl::OkStatus();
}

enum class KeyOrValueSelector {
  kKey,
  kValue,
};

static absl::Status CheckOrderableMapArgumentConstraint(
    absl::string_view fn_name, int map_arg_idx, KeyOrValueSelector key_or_value,
    absl::Span<const InputArgumentType> arguments,
    const LanguageOptions& language_options) {
  ZETASQL_RET_CHECK_EQ(arguments.size(), 1);
  ZETASQL_ASSIGN_OR_RETURN(const MapType* map_type,
                   GetMapTypeFromInputArg(arguments[map_arg_idx]));

  const Type* orderable_type = (key_or_value == KeyOrValueSelector::kKey)
                                   ? map_type->key_type()
                                   : map_type->value_type();

  return MakeErrorIfMapElementTypeNotOrderable(fn_name, orderable_type,
                                               language_options);
}

void GetMapCoreFunctions(TypeFactory* type_factory,
                         const ZetaSQLBuiltinFunctionOptions& options,
                         NameToFunctionMap* functions) {
  // MAP_FROM_ARRAY(ARRAY<STRUCT<K,V>> entries) -> MAP<K,V>
  InsertFunction(
      functions, options, "map_from_array", Function::SCALAR,
      {{ARG_TYPE_ARBITRARY,
        {ARG_ARRAY_TYPE_ANY_1},
        FN_MAP_FROM_ARRAY,
        // TODO: Collation support for MAP<> type.
        FunctionSignatureOptions().set_rejects_collation()}},
      FunctionOptions()
          .set_compute_result_type_callback(&ComputeMapFromArrayResultType)
          .set_pre_resolution_argument_constraint(
              &CheckMapFromArrayPreResolutionArguments)
          .AddRequiredLanguageFeature(FEATURE_V_1_4_MAP_TYPE));

  // MAP_ENTRIES_SORTED(MAP<K,V> input_map) -> ARRAY<STRUCT<K,V>>
  InsertFunction(
      functions, options, "map_entries_sorted", Function::SCALAR,
      {
          {ARG_TYPE_ARBITRARY, {ARG_MAP_TYPE_ANY_1_2}, FN_MAP_ENTRIES_SORTED},
      },
      FunctionOptions()
          .set_compute_result_type_callback(absl::bind_front(
              &ComputeMapEntriesFunctionResultType, kMapEntriesSorted,
              /*require_orderable_key=*/true))
          .AddRequiredLanguageFeature(FEATURE_V_1_4_MAP_TYPE));

  InsertFunction(
      functions, options, "map_entries_unsorted", Function::SCALAR,
      {
          {ARG_TYPE_ARBITRARY, {ARG_MAP_TYPE_ANY_1_2}, FN_MAP_ENTRIES_UNSORTED},
      },
      FunctionOptions()
          .set_compute_result_type_callback(absl::bind_front(
              &ComputeMapEntriesFunctionResultType, kMapEntriesUnsorted,
              /*require_orderable_key=*/false))
          .AddRequiredLanguageFeature(FEATURE_V_1_4_MAP_TYPE));
  InsertFunction(
      functions, options, "map_get", Function::SCALAR,
      {
          {ARG_TYPE_ANY_2,
           {ARG_MAP_TYPE_ANY_1_2,
            ARG_TYPE_ANY_1,
            {ARG_TYPE_ANY_2, FunctionEnums::OPTIONAL}},
           FN_MAP_GET},
      },
      FunctionOptions().AddRequiredLanguageFeature(FEATURE_V_1_4_MAP_TYPE));
  InsertFunction(
      functions, options, "map_contains_key", Function::SCALAR,
      {
          {type_factory->get_bool(),
           {ARG_MAP_TYPE_ANY_1_2, ARG_TYPE_ANY_1},
           FN_MAP_CONTAINS_KEY},
      },
      FunctionOptions().AddRequiredLanguageFeature(FEATURE_V_1_4_MAP_TYPE));

  const FunctionArgumentType input_map_argument_type{
      ARG_MAP_TYPE_ANY_1_2, FunctionArgumentTypeOptions().set_argument_name(
                                "input_map", kPositionalOnly)};
  constexpr absl::string_view kMapKeysSortedSql = R"sql(
      (SELECT FLATTEN(MAP_ENTRIES_SORTED(input_map).key))
    )sql";
  InsertFunction(functions, options, "map_keys_sorted", Function::SCALAR,
                 {
                     {ARG_ARRAY_TYPE_ANY_1,
                      {input_map_argument_type},
                      FN_MAP_KEYS_SORTED,
                      FunctionSignatureOptions().set_rewrite_options(
                          FunctionSignatureRewriteOptions()
                              .set_rewriter(REWRITE_BUILTIN_FUNCTION_INLINER)
                              .set_sql(kMapKeysSortedSql))},
                 },
                 FunctionOptions()
                     .set_pre_resolution_argument_constraint(absl::bind_front(
                         CheckOrderableMapArgumentConstraint, "MAP_KEYS_SORTED",
                         /*map_arg_idx=*/0, KeyOrValueSelector::kKey))
                     .AddRequiredLanguageFeature(FEATURE_V_1_4_MAP_TYPE));

  constexpr absl::string_view kMapKeysUnsortedSql = R"sql(
      (SELECT FLATTEN(MAP_ENTRIES_UNSORTED(input_map).key))
    )sql";
  InsertFunction(
      functions, options, "map_keys_unsorted", Function::SCALAR,
      {
          {ARG_ARRAY_TYPE_ANY_1,
           {input_map_argument_type},
           FN_MAP_KEYS_UNSORTED,
           FunctionSignatureOptions().set_rewrite_options(
               FunctionSignatureRewriteOptions()
                   .set_rewriter(REWRITE_BUILTIN_FUNCTION_INLINER)
                   .set_sql(kMapKeysUnsortedSql))},
      },
      FunctionOptions().AddRequiredLanguageFeature(FEATURE_V_1_4_MAP_TYPE));

  constexpr absl::string_view kMapValuesSortedSql = R"sql(
      (
        SELECT IF(
          input_map IS NULL,
          NULL,
          ARRAY(SELECT entries.value
                  FROM UNNEST(MAP_ENTRIES_UNSORTED(input_map)) AS entries
                  ORDER BY entries.value))
      )
    )sql";
  InsertFunction(functions, options, "map_values_sorted", Function::SCALAR,
                 {
                     {ARG_ARRAY_TYPE_ANY_2,
                      {input_map_argument_type},
                      FN_MAP_VALUES_SORTED,
                      FunctionSignatureOptions().set_rewrite_options(
                          FunctionSignatureRewriteOptions()
                              .set_rewriter(REWRITE_BUILTIN_FUNCTION_INLINER)
                              .set_sql(kMapValuesSortedSql))},
                 },
                 FunctionOptions()
                     .set_pre_resolution_argument_constraint(absl::bind_front(
                         CheckOrderableMapArgumentConstraint,
                         "MAP_VALUES_SORTED", 0, KeyOrValueSelector::kValue))
                     .AddRequiredLanguageFeature(FEATURE_V_1_4_MAP_TYPE));

  constexpr absl::string_view kMapValuesUnsortedSql = R"sql(
      (SELECT FLATTEN(MAP_ENTRIES_UNSORTED(input_map).value))
    )sql";
  InsertFunction(
      functions, options, "map_values_unsorted", Function::SCALAR,
      {
          {ARG_ARRAY_TYPE_ANY_2,
           {input_map_argument_type},
           FN_MAP_VALUES_UNSORTED,
           FunctionSignatureOptions().set_rewrite_options(
               FunctionSignatureRewriteOptions()
                   .set_rewriter(REWRITE_BUILTIN_FUNCTION_INLINER)
                   .set_sql(kMapValuesUnsortedSql))},
      },
      FunctionOptions().AddRequiredLanguageFeature(FEATURE_V_1_4_MAP_TYPE));

  constexpr absl::string_view kMapValuesSortedByKeySql = R"sql(
      (SELECT FLATTEN(MAP_ENTRIES_SORTED(input_map).value))
    )sql";
  InsertFunction(
      functions, options, "map_values_sorted_by_key", Function::SCALAR,
      {
          {ARG_ARRAY_TYPE_ANY_2,
           {input_map_argument_type},
           FN_MAP_VALUES_SORTED_BY_KEY,
           FunctionSignatureOptions().set_rewrite_options(
               FunctionSignatureRewriteOptions()
                   .set_rewriter(REWRITE_BUILTIN_FUNCTION_INLINER)
                   .set_sql(kMapValuesSortedByKeySql))},
      },
      FunctionOptions()
          .set_pre_resolution_argument_constraint(absl::bind_front(
              CheckOrderableMapArgumentConstraint, "MAP_VALUES_SORTED_BY_KEY",
              0, KeyOrValueSelector::kKey))
          .AddRequiredLanguageFeature(FEATURE_V_1_4_MAP_TYPE));

  constexpr absl::string_view kMapEmptySql = R"sql(
      (SELECT ARRAY_LENGTH(MAP_ENTRIES_UNSORTED(input_map)) = 0)
    )sql";
  InsertFunction(
      functions, options, "map_empty", Function::SCALAR,
      {
          {type_factory->get_bool(),
           {input_map_argument_type},
           FN_MAP_EMPTY,
           FunctionSignatureOptions().set_rewrite_options(
               FunctionSignatureRewriteOptions()
                   .set_rewriter(REWRITE_BUILTIN_FUNCTION_INLINER)
                   .set_sql(kMapEmptySql))},
      },
      FunctionOptions().AddRequiredLanguageFeature(FEATURE_V_1_4_MAP_TYPE));

  const FunctionArgumentTypeList arglist_map_and_kv_pairs = {
      input_map_argument_type,
      ARG_TYPE_ANY_1,
      ARG_TYPE_ANY_2,
      // The function library treats multiple repeated arguments as interleaved.
      // Thus, this creates the signature of (MAP<K,V>, K, V, K, V, ...).
      {ARG_TYPE_ANY_1, FunctionArgumentType::REPEATED},
      {ARG_TYPE_ANY_2, FunctionArgumentType::REPEATED},
  };
  FunctionOptions map_insert_function_options =
      FunctionOptions().AddRequiredLanguageFeature(FEATURE_V_1_4_MAP_TYPE);
  InsertFunction(
      functions, options, "map_insert", Function::SCALAR,
      {{ARG_MAP_TYPE_ANY_1_2, arglist_map_and_kv_pairs, FN_MAP_INSERT}},
      map_insert_function_options);
  InsertFunction(functions, options, "map_insert_or_replace", Function::SCALAR,
                 {{ARG_MAP_TYPE_ANY_1_2, arglist_map_and_kv_pairs,
                   FN_MAP_INSERT_OR_REPLACE}},
                 map_insert_function_options);
  InsertFunction(functions, options, "map_replace", Function::SCALAR,
                 {
                     {ARG_MAP_TYPE_ANY_1_2, arglist_map_and_kv_pairs,
                      FN_MAP_REPLACE_KV_PAIRS},
                     {ARG_MAP_TYPE_ANY_1_2,
                      {input_map_argument_type,
                       ARG_TYPE_ANY_1,
                       {ARG_TYPE_ANY_1, FunctionArgumentType::REPEATED},
                       FunctionArgumentType::Lambda(
                           {ARG_TYPE_ANY_2}, ARG_TYPE_ANY_2,
                           FunctionArgumentTypeOptions().set_argument_name(
                               "value", kPositionalOnly))},
                      FN_MAP_REPLACE_K_REPEATED_V_LAMBDA},
                 },
                 map_insert_function_options);
}

}  // namespace zetasql
