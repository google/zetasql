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

#include <initializer_list>
#include <string>
#include <vector>

#include "google/protobuf/timestamp.pb.h"
#include "google/protobuf/wrappers.pb.h"
#include "google/type/date.pb.h"
#include "google/type/timeofday.pb.h"
#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/strings/string_view.h"

namespace zetasql {

void GetHllCountFunctions(TypeFactory* type_factory,
                          const ZetaSQLBuiltinFunctionOptions& options,
                          NameToFunctionMap* functions) {
  const Type* bytes_type = type_factory->get_bytes();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* string_type = type_factory->get_string();
  const Type* numeric_type = type_factory->get_numeric();
  const Type* bignumeric_type = type_factory->get_bignumeric();

  constexpr Function::Mode AGGREGATE = Function::AGGREGATE;
  constexpr Function::Mode SCALAR = Function::SCALAR;
  constexpr FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&CheckHasNumericTypeArgument);

  FunctionSignatureOptions has_bignumeric_type_argument;
  has_bignumeric_type_argument.set_constraints(&CheckHasBigNumericTypeArgument);

  // The second argument must be an integer literal between 10 and 24,
  // and cannot be NULL.
  FunctionArgumentTypeOptions hll_init_arg;
  hll_init_arg.set_is_not_aggregate();
  hll_init_arg.set_must_be_non_null();
  hll_init_arg.set_cardinality(OPTIONAL);
  hll_init_arg.set_min_value(10);
  hll_init_arg.set_max_value(24);

  InsertSimpleNamespaceFunction(
      functions, options, "hll_count", "merge", AGGREGATE,
      {{int64_type, {bytes_type}, FN_HLL_COUNT_MERGE}},
      DefaultAggregateFunctionOptions());
  InsertSimpleNamespaceFunction(
      functions, options, "hll_count", "extract", SCALAR,
      {{int64_type, {bytes_type}, FN_HLL_COUNT_EXTRACT}});
  InsertNamespaceFunction(
      functions, options, "hll_count", "init", AGGREGATE,
      {{bytes_type,
        {int64_type, {int64_type, hll_init_arg}},
        FN_HLL_COUNT_INIT_INT64},
       {bytes_type,
        {uint64_type, {int64_type, hll_init_arg}},
        FN_HLL_COUNT_INIT_UINT64},
       {bytes_type,
        {numeric_type, {int64_type, hll_init_arg}},
        FN_HLL_COUNT_INIT_NUMERIC,
        has_numeric_type_argument},
       {bytes_type,
        {bignumeric_type, {int64_type, hll_init_arg}},
        FN_HLL_COUNT_INIT_BIGNUMERIC,
        has_bignumeric_type_argument},
       {bytes_type,
        {string_type, {int64_type, hll_init_arg}},
        FN_HLL_COUNT_INIT_STRING,
        FunctionSignatureOptions().set_uses_operation_collation()},
       {bytes_type,
        {bytes_type, {int64_type, hll_init_arg}},
        FN_HLL_COUNT_INIT_BYTES}},
      DefaultAggregateFunctionOptions());
  InsertSimpleNamespaceFunction(
      functions, options, "hll_count", "merge_partial", AGGREGATE,
      {{bytes_type, {bytes_type}, FN_HLL_COUNT_MERGE_PARTIAL}},
      DefaultAggregateFunctionOptions());
}

void GetD3ACountFunctions(TypeFactory* type_factory,
                          const ZetaSQLBuiltinFunctionOptions& options,
                          NameToFunctionMap* functions) {
  const Type* bytes_type = type_factory->get_bytes();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* string_type = type_factory->get_string();
  const Type* numeric_type = type_factory->get_numeric();
  const Type* bignumeric_type = type_factory->get_bignumeric();

  constexpr Function::Mode AGGREGATE = Function::AGGREGATE;
  constexpr Function::Mode SCALAR = Function::SCALAR;
  constexpr FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&CheckHasNumericTypeArgument);

  FunctionSignatureOptions has_bignumeric_type_argument;
  has_bignumeric_type_argument.set_constraints(&CheckHasBigNumericTypeArgument);

  // The second argument `weight` is required to avoid misusage as HLL_COUNT and
  // ensure that the user is using it in the cases they need deletions.
  // We don't set `is_not_aggregate` as true because each call can have a
  // different value depending on input row or smth similar.
  FunctionArgumentTypeOptions d3a_weight_arg;
  d3a_weight_arg.set_must_be_non_null();

  // The third argument must be an integer literal between 4 and 24,
  // and cannot be NULL.
  FunctionArgumentTypeOptions d3a_precision_arg;
  d3a_precision_arg.set_is_not_aggregate();
  d3a_precision_arg.set_must_be_non_null();
  d3a_precision_arg.set_cardinality(OPTIONAL);
  d3a_precision_arg.set_min_value(4);
  d3a_precision_arg.set_max_value(24);

  InsertSimpleNamespaceFunction(
      functions, options, "d3a_count", "merge", AGGREGATE,
      {{int64_type, {bytes_type}, FN_D3A_COUNT_MERGE}},
      DefaultAggregateFunctionOptions());
  InsertSimpleNamespaceFunction(
      functions, options, "d3a_count", "merge_partial", AGGREGATE,
      {{bytes_type, {bytes_type}, FN_D3A_COUNT_MERGE_PARTIAL}},
      DefaultAggregateFunctionOptions());
  InsertSimpleNamespaceFunction(
      functions, options, "d3a_count", "extract", SCALAR,
      {{int64_type, {bytes_type}, FN_D3A_COUNT_EXTRACT}});
  InsertSimpleNamespaceFunction(
      functions, options, "d3a_count", "to_hll", SCALAR,
      {{bytes_type, {bytes_type}, FN_D3A_COUNT_TO_HLL}});
  InsertNamespaceFunction(
      functions, options, "d3a_count", "init", AGGREGATE,
      {{bytes_type,
        {int64_type,
         {int64_type, d3a_weight_arg},
         {int64_type, d3a_precision_arg}},
        FN_D3A_COUNT_INIT_INT64},
       {bytes_type,
        {uint64_type,
         {int64_type, d3a_weight_arg},
         {int64_type, d3a_precision_arg}},
        FN_D3A_COUNT_INIT_UINT64},
       {bytes_type,
        {numeric_type,
         {int64_type, d3a_weight_arg},
         {int64_type, d3a_precision_arg}},
        FN_D3A_COUNT_INIT_NUMERIC,
        has_numeric_type_argument},
       {bytes_type,
        {bignumeric_type,
         {int64_type, d3a_weight_arg},
         {int64_type, d3a_precision_arg}},
        FN_D3A_COUNT_INIT_BIGNUMERIC,
        has_bignumeric_type_argument},
       {bytes_type,
        {string_type,
         {int64_type, d3a_weight_arg},
         {int64_type, d3a_precision_arg}},
        FN_D3A_COUNT_INIT_STRING,
        FunctionSignatureOptions().set_uses_operation_collation()},
       {bytes_type,
        {bytes_type,
         {int64_type, d3a_weight_arg},
         {int64_type, d3a_precision_arg}},
        FN_D3A_COUNT_INIT_BYTES}},
      DefaultAggregateFunctionOptions().set_supports_distinct_modifier(false));
}

void GetKllQuantilesFunctions(TypeFactory* type_factory,
                              const ZetaSQLBuiltinFunctionOptions& options,
                              NameToFunctionMap* functions) {
  const Type* int64_type = type_factory->get_int64();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* double_type = type_factory->get_double();
  const Type* bytes_type = type_factory->get_bytes();
  const Type* int64_array_type = types::Int64ArrayType();
  const Type* uint64_array_type = types::Uint64ArrayType();
  const Type* double_array_type = types::DoubleArrayType();

  constexpr Function::Mode AGGREGATE = Function::AGGREGATE;
  constexpr Function::Mode SCALAR = Function::SCALAR;
  constexpr FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  // By default, all built-in aggregate functions can be used as analytic
  // functions, and all the KllQuantilesFunctions do not allow external usage.
  FunctionOptions
      aggregate_analytic_function_options_and_not_allow_external_usage =
          DefaultAggregateFunctionOptions().set_allow_external_usage(false);

  // The optional second argument of 'init', the approximation precision or
  // inverse epsilon ('inv_eps'), must be an integer >= 2 and cannot be NULL.
  FunctionArgumentTypeOptions init_inv_eps_arg;
  init_inv_eps_arg.set_is_not_aggregate();
  init_inv_eps_arg.set_must_be_non_null();
  init_inv_eps_arg.set_cardinality(OPTIONAL);
  init_inv_eps_arg.set_min_value(2);

  // Init functions include a weight parameter only if NAMED_ARGUMENTS enabled.
  if (options.language_options.LanguageFeatureEnabled(
          zetasql::FEATURE_V_1_3_KLL_WEIGHTS)) {
    // Explicitly set default value for precision (detailed in (broken link))
    init_inv_eps_arg.set_default(Value::Int64(1000));

    // There is an additional optional argument for input weights.
    FunctionArgumentTypeOptions init_weights_arg;
    init_weights_arg.set_cardinality(OPTIONAL);
    init_weights_arg.set_argument_name("weight", kNamedOnly);
    init_weights_arg.set_default(Value::Int64(1));

    // Init functions with weight parameter
    InsertNamespaceFunction(
        functions, options, "kll_quantiles", "init_int64", AGGREGATE,
        {{bytes_type,
          {int64_type,
           {int64_type, init_inv_eps_arg},
           {int64_type, init_weights_arg}},
          FN_KLL_QUANTILES_INIT_INT64}},
        aggregate_analytic_function_options_and_not_allow_external_usage);
    InsertNamespaceFunction(
        functions, options, "kll_quantiles", "init_uint64", AGGREGATE,
        {{bytes_type,
          {uint64_type,
           {int64_type, init_inv_eps_arg},
           {int64_type, init_weights_arg}},
          FN_KLL_QUANTILES_INIT_UINT64}},
        aggregate_analytic_function_options_and_not_allow_external_usage);
    InsertNamespaceFunction(
        functions, options, "kll_quantiles", "init_double", AGGREGATE,
        {{bytes_type,
          {double_type,
           {int64_type, init_inv_eps_arg},
           {int64_type, init_weights_arg}},
          FN_KLL_QUANTILES_INIT_DOUBLE}},
        aggregate_analytic_function_options_and_not_allow_external_usage);
  } else {
    // init functions with no weight parameter
    InsertNamespaceFunction(
        functions, options, "kll_quantiles", "init_int64", AGGREGATE,
        {{bytes_type,
          {int64_type, {int64_type, init_inv_eps_arg}},
          FN_KLL_QUANTILES_INIT_INT64}},
        aggregate_analytic_function_options_and_not_allow_external_usage);
    InsertNamespaceFunction(
        functions, options, "kll_quantiles", "init_uint64", AGGREGATE,
        {{bytes_type,
          {uint64_type, {int64_type, init_inv_eps_arg}},
          FN_KLL_QUANTILES_INIT_UINT64}},
        aggregate_analytic_function_options_and_not_allow_external_usage);
    InsertNamespaceFunction(
        functions, options, "kll_quantiles", "init_double", AGGREGATE,
        {{bytes_type,
          {double_type, {int64_type, init_inv_eps_arg}},
          FN_KLL_QUANTILES_INIT_DOUBLE}},
        aggregate_analytic_function_options_and_not_allow_external_usage);
  }

  // Merge_partial
  InsertSimpleNamespaceFunction(
      functions, options, "kll_quantiles", "merge_partial", AGGREGATE,
      {{bytes_type, {bytes_type}, FN_KLL_QUANTILES_MERGE_PARTIAL}},
      aggregate_analytic_function_options_and_not_allow_external_usage);

  // The second argument of aggregate function 'merge', the number of
  // equidistant quantiles that should be returned; must be a non-aggregate
  //  integer >= 2 and cannot be NULL.
  FunctionArgumentTypeOptions num_quantiles_merge_arg;
  num_quantiles_merge_arg.set_is_not_aggregate();
  num_quantiles_merge_arg.set_must_be_non_null();
  num_quantiles_merge_arg.set_min_value(2);

  // Merge
  InsertNamespaceFunction(
      functions, options, "kll_quantiles", "merge_int64", AGGREGATE,
      {{int64_array_type,
        // TODO: b/219883981 - Add support for interpolation option for all
        // merge/ extract/merge_point/extract_point functions if the feature
        // gets prioritized.
        {bytes_type, {int64_type, num_quantiles_merge_arg}},
        FN_KLL_QUANTILES_MERGE_INT64}},
      aggregate_analytic_function_options_and_not_allow_external_usage);
  InsertNamespaceFunction(
      functions, options, "kll_quantiles", "merge_uint64", AGGREGATE,
      {{uint64_array_type,
        {bytes_type, {int64_type, num_quantiles_merge_arg}},
        FN_KLL_QUANTILES_MERGE_UINT64}},
      aggregate_analytic_function_options_and_not_allow_external_usage);
  InsertNamespaceFunction(
      functions, options, "kll_quantiles", "merge_double", AGGREGATE,
      {{double_array_type,
        {bytes_type, {int64_type, num_quantiles_merge_arg}},
        FN_KLL_QUANTILES_MERGE_DOUBLE}},
      aggregate_analytic_function_options_and_not_allow_external_usage);

  // The second argument of scalar function 'extract', the number of
  // equidistant quantiles that should be returned; must be an integer >= 2 and
  // cannot be NULL.
  FunctionArgumentTypeOptions num_quantiles_extract_arg;
  num_quantiles_extract_arg.set_must_be_non_null();
  num_quantiles_extract_arg.set_min_value(2);

  // Extract
  InsertNamespaceFunction(
      functions, options, "kll_quantiles", "extract_int64", SCALAR,
      {{int64_array_type,
        {bytes_type, {int64_type, num_quantiles_extract_arg}},
        FN_KLL_QUANTILES_EXTRACT_INT64}},
      FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(
      functions, options, "kll_quantiles", "extract_uint64", SCALAR,
      {{uint64_array_type,
        {bytes_type, {int64_type, num_quantiles_extract_arg}},
        FN_KLL_QUANTILES_EXTRACT_UINT64}},
      FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(
      functions, options, "kll_quantiles", "extract_double", SCALAR,
      {{double_array_type,
        {bytes_type, {int64_type, num_quantiles_extract_arg}},
        FN_KLL_QUANTILES_EXTRACT_DOUBLE}},
      FunctionOptions().set_allow_external_usage(false));

  // The second argument of aggregate function 'merge_point', phi, must be a
  // non-aggregate double in [0, 1] and cannot be null.
  FunctionArgumentTypeOptions phi_merge_arg;
  phi_merge_arg.set_is_not_aggregate();
  phi_merge_arg.set_must_be_non_null();
  phi_merge_arg.set_min_value(0);
  phi_merge_arg.set_max_value(1);

  // Merge_point
  InsertNamespaceFunction(
      functions, options, "kll_quantiles", "merge_point_int64", AGGREGATE,
      {{int64_type,
        {bytes_type, {double_type, phi_merge_arg}},
        FN_KLL_QUANTILES_MERGE_POINT_INT64}},
      aggregate_analytic_function_options_and_not_allow_external_usage);
  InsertNamespaceFunction(
      functions, options, "kll_quantiles", "merge_point_uint64", AGGREGATE,
      {{uint64_type,
        {bytes_type, {double_type, phi_merge_arg}},
        FN_KLL_QUANTILES_MERGE_POINT_UINT64}},
      aggregate_analytic_function_options_and_not_allow_external_usage);
  InsertNamespaceFunction(
      functions, options, "kll_quantiles", "merge_point_double", AGGREGATE,
      {{double_type,
        {bytes_type, {double_type, phi_merge_arg}},
        FN_KLL_QUANTILES_MERGE_POINT_DOUBLE}},
      aggregate_analytic_function_options_and_not_allow_external_usage);

  // The second argument of scalar function 'extract_point', phi, must be a
  // double in [0, 1] and cannot be null.
  FunctionArgumentTypeOptions phi_extract_arg;
  phi_extract_arg.set_must_be_non_null();
  phi_extract_arg.set_min_value(0);
  phi_extract_arg.set_max_value(1);

  // Extract_point
  InsertNamespaceFunction(functions, options, "kll_quantiles",
                          "extract_point_int64", SCALAR,
                          {{int64_type,
                            {bytes_type, {double_type, phi_extract_arg}},
                            FN_KLL_QUANTILES_EXTRACT_POINT_INT64}},
                          FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles",
                          "extract_point_uint64", SCALAR,
                          {{uint64_type,
                            {bytes_type, {double_type, phi_extract_arg}},
                            FN_KLL_QUANTILES_EXTRACT_POINT_UINT64}},
                          FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles",
                          "extract_point_double", SCALAR,
                          {{double_type,
                            {bytes_type, {double_type, phi_extract_arg}},
                            FN_KLL_QUANTILES_EXTRACT_POINT_DOUBLE}},
                          FunctionOptions().set_allow_external_usage(false));
}

}  // namespace zetasql
