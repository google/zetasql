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

#include <ctype.h>

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/proto/anon_output_with_report.pb.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/functions/differential_privacy.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/bind_front.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// Create a `FunctionSignatureOptions` that configures a SQL definition that
// will be inlined by `REWRITE_BUILTIN_FUNCTION_INLINER`.
static FunctionSignatureOptions SetDefinitionForInlining(absl::string_view sql,
                                                         bool enabled = true) {
  return FunctionSignatureOptions().set_rewrite_options(
      FunctionSignatureRewriteOptions()
          .set_enabled(enabled)
          .set_rewriter(REWRITE_BUILTIN_FUNCTION_INLINER)
          .set_sql(sql));
}

static bool IsRewriteEnabled(FunctionSignatureId id,
                             const ZetaSQLBuiltinFunctionOptions& options,
                             bool default_value = true) {
  auto found_override = options.rewrite_enabled.find(id);
  return found_override != options.rewrite_enabled.end()
             ? found_override->second
             : default_value;
}

void GetArrayMiscFunctions(TypeFactory* type_factory,
                           const ZetaSQLBuiltinFunctionOptions& options,
                           NameToFunctionMap* functions) {
  const Type* bool_type = type_factory->get_bool();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* numeric_type = type_factory->get_numeric();
  const Type* bignumeric_type = type_factory->get_bignumeric();
  const Type* double_type = type_factory->get_double();
  const Type* date_type = type_factory->get_date();
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* datepart_type = types::DatePartEnumType();

  const Type* string_type = type_factory->get_string();
  const Type* bytes_type = type_factory->get_bytes();
  const Type* array_string_type = types::StringArrayType();
  const Type* array_bytes_type = types::BytesArrayType();
  const Type* int64_array_type = types::Int64ArrayType();
  const Type* uint64_array_type = types::Uint64ArrayType();
  const Type* numeric_array_type = types::NumericArrayType();
  const Type* bignumeric_array_type = types::BigNumericArrayType();
  const Type* double_array_type = types::DoubleArrayType();
  const Type* date_array_type = types::DateArrayType();
  const Type* timestamp_array_type = types::TimestampArrayType();

  const Function::Mode SCALAR = Function::SCALAR;

  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;
  const FunctionArgumentType::ArgumentCardinality REPEATED =
      FunctionArgumentType::REPEATED;

  // ARRAY_LENGTH(expr1): returns the length of the array
  InsertSimpleFunction(functions, options, "array_length", SCALAR,
                       {{int64_type, {ARG_ARRAY_TYPE_ANY_1}, FN_ARRAY_LENGTH}});

  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_3_UNNEST_AND_FLATTEN_ARRAYS)) {
    // This function is only used during internal resolution and will never
    // appear in a resolved AST. Instead a ResolvedFlatten node will be
    // generated.
    // TODO: Flatten function disallows collations on input arrays.
    // This constraint is temporary and we will supported collated arrays for
    // Flatten later.
    InsertFunction(functions, options, "flatten", SCALAR,
                   {{ARG_ARRAY_TYPE_ANY_1,
                     {ARG_ARRAY_TYPE_ANY_1},
                     FN_FLATTEN,
                     FunctionSignatureOptions().set_rejects_collation(true)}});
  }

  // Usage: [...], ARRAY[...], ARRAY<T>[...]
  // * Array elements would be the list of expressions enclosed within [].
  // * T (if mentioned) would define the array element type. Otherwise the
  //   common supertype among all the elements would define the element type.
  // * All element types when not equal should implicitly coerce to the defined
  //   element type.
  InsertFunction(
      functions, options, "$make_array", SCALAR,
      {{{ARG_ARRAY_TYPE_ANY_1,
         FunctionArgumentTypeOptions().set_uses_array_element_for_collation()},
        {{ARG_TYPE_ANY_1, REPEATED}},
        FN_MAKE_ARRAY}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("array[...]")
          .set_get_sql_callback(&MakeArrayFunctionSQL));

  // ARRAY_CONCAT(repeated array): returns the concatenation of the input
  // arrays.
  InsertSimpleFunction(
      functions, options, "array_concat", SCALAR,
      {{ARG_ARRAY_TYPE_ANY_1,
        {ARG_ARRAY_TYPE_ANY_1, {ARG_ARRAY_TYPE_ANY_1, REPEATED}},
        FN_ARRAY_CONCAT}},
      FunctionOptions().set_pre_resolution_argument_constraint(
          &CheckArrayConcatArguments));

  // ARRAY_TO_STRING: returns concatentation of elements of the input array.
  InsertFunction(
      functions, options, "array_to_string", SCALAR,
      {{string_type,
        {{array_string_type,
          FunctionArgumentTypeOptions().set_uses_array_element_for_collation()},
         string_type,
         {string_type, OPTIONAL}},
        FN_ARRAY_TO_STRING},
       {bytes_type,
        {array_bytes_type, bytes_type, {bytes_type, OPTIONAL}},
        FN_ARRAY_TO_BYTES}});

  // ARRAY_REVERSE: returns the input array with its elements in reverse order.
  InsertSimpleFunction(
      functions, options, "array_reverse", SCALAR,
      {{ARG_ARRAY_TYPE_ANY_1, {ARG_ARRAY_TYPE_ANY_1}, FN_ARRAY_REVERSE}});

  // ARRAY_IS_DISTINCT: returns true if the array has no duplicate entries.
  InsertSimpleFunction(
      functions, options, "array_is_distinct", SCALAR,
      {{bool_type, {ARG_ARRAY_TYPE_ANY_1}, FN_ARRAY_IS_DISTINCT}},
      FunctionOptions().set_pre_resolution_argument_constraint(
          &CheckArrayIsDistinctArguments));

  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&HasNumericTypeArgument);
  FunctionSignatureOptions has_bignumeric_type_argument;
  has_bignumeric_type_argument.set_constraints(&HasBigNumericTypeArgument);

  // Usage: generate_array(begin_range, end_range, [step]).
  // Returns an array spanning the range [begin_range, end_range] with a step
  // size of 'step', or 1 if unspecified.
  // - If begin_range is greater than end_range and 'step' is positive, returns
  //   an empty array.
  // - If begin_range is greater than end_range and 'step' is negative, returns
  //   an array spanning [end_range, begin_range] with a step size of -'step'.
  // - If 'step' is 0 or +/-inf, raises an error.
  // - If any input is nan, raises an error.
  // - If any input is null, returns a null array.
  // Implementations may enforce a limit on the number of elements in an array.
  // In the reference implementation, for instance, the limit is 16000.
  InsertFunction(
      functions, options, "generate_array", SCALAR,
      {{int64_array_type,
        {int64_type, int64_type, {int64_type, OPTIONAL}},
        FN_GENERATE_ARRAY_INT64},
       {uint64_array_type,
        {uint64_type, uint64_type, {uint64_type, OPTIONAL}},
        FN_GENERATE_ARRAY_UINT64},
       {numeric_array_type,
        {numeric_type, numeric_type, {numeric_type, OPTIONAL}},
        FN_GENERATE_ARRAY_NUMERIC,
        has_numeric_type_argument},
       {bignumeric_array_type,
        {bignumeric_type, bignumeric_type, {bignumeric_type, OPTIONAL}},
        FN_GENERATE_ARRAY_BIGNUMERIC,
        has_bignumeric_type_argument},
       {double_array_type,
        {double_type, double_type, {double_type, OPTIONAL}},
        FN_GENERATE_ARRAY_DOUBLE}});
  InsertSimpleFunction(
      functions, options, "generate_date_array", SCALAR,
      {{date_array_type,
        {date_type,
         date_type,
         {int64_type, OPTIONAL},
         {datepart_type, OPTIONAL}},
        FN_GENERATE_DATE_ARRAY}},
      FunctionOptions()
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForGenerateDateOrTimestampArrayFunction)
          .set_pre_resolution_argument_constraint(
              &CheckGenerateDateArrayArguments)
          .set_get_sql_callback(absl::bind_front(
              &GenerateDateTimestampArrayFunctionSQL, "GENERATE_DATE_ARRAY")));
  InsertSimpleFunction(
      functions, options, "generate_timestamp_array", SCALAR,
      {{timestamp_array_type,
        {timestamp_type, timestamp_type, int64_type, datepart_type},
        FN_GENERATE_TIMESTAMP_ARRAY}},
      FunctionOptions()
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForGenerateDateOrTimestampArrayFunction)
          .set_pre_resolution_argument_constraint(
              &CheckGenerateTimestampArrayArguments)
          .set_get_sql_callback(
              absl::bind_front(&GenerateDateTimestampArrayFunctionSQL,
                               "GENERATE_TIMESTAMP_ARRAY")));

  FunctionArgumentType array_input_for_collation(
      ARG_ARRAY_TYPE_ANY_1,
      FunctionArgumentTypeOptions()
          .set_uses_array_element_for_collation()
          .set_argument_name("input_array", kPositionalOnly));
  constexpr absl::string_view kArrayFirstSql = R"sql(
      CASE
        WHEN input_array IS NULL THEN NULL
        WHEN ARRAY_LENGTH(input_array) = 0
          THEN
              ERROR(
                'ARRAY_FIRST cannot get the first element of an empty array')
        ELSE input_array[OFFSET(0)]
        END
    )sql";
  InsertFunction(functions, options, "array_first", SCALAR,
                 {{ARG_TYPE_ANY_1,
                   {array_input_for_collation},
                   FN_ARRAY_FIRST,
                   SetDefinitionForInlining(kArrayFirstSql)}});

  constexpr absl::string_view kArrayLastSql = R"sql(
      CASE
        WHEN input_array IS NULL THEN NULL
        WHEN ARRAY_LENGTH(input_array) = 0
          THEN
            ERROR('ARRAY_LAST cannot get the last element of an empty array')
        ELSE input_array[ORDINAL(ARRAY_LENGTH(input_array))]
      END
    )sql";
  InsertFunction(functions, options, "array_last", SCALAR,
                 /*signatures=*/
                 {{ARG_TYPE_ANY_1,
                   {array_input_for_collation},
                   FN_ARRAY_LAST,
                   SetDefinitionForInlining(kArrayLastSql)}});
}

static FunctionSignatureOnHeap UnaryArrayFuncConcreteSig(
    const Type* output_type, const Type* input_type, FunctionSignatureId id,
    absl::string_view sql) {
  FunctionArgumentType input_arg{
      input_type, FunctionArgumentTypeOptions().set_argument_name(
                      "input_array", kPositionalOnly)};
  return FunctionSignatureOnHeap(output_type, {input_arg}, id,
                                 SetDefinitionForInlining(sql));
}

static FunctionSignatureOnHeap ArraySumSig(const Type* output_type,
                                           const Type* input_type,
                                           FunctionSignatureId id) {
  constexpr absl::string_view kArraySumSql = R"sql(
      IF(
        input_array IS NULL,
        NULL,
        (SELECT SUM(e) FROM UNNEST(input_array) AS e))
    )sql";
  return UnaryArrayFuncConcreteSig(output_type, input_type, id, kArraySumSql);
}

static FunctionSignatureOnHeap ArrayAvgSig(const Type* output_type,
                                           const Type* input_type,
                                           FunctionSignatureId id) {
  constexpr absl::string_view kArrayAvgSql = R"sql(
      IF(
        input_array IS NULL,
        NULL,
        (SELECT AVG(e) FROM UNNEST(input_array) AS e))
    )sql";
  return UnaryArrayFuncConcreteSig(output_type, input_type, id, kArrayAvgSql);
}

void GetArrayAggregationFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions) {
  const Type* int64_type = type_factory->get_int64();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* numeric_type = type_factory->get_numeric();
  const Type* bignumeric_type = type_factory->get_bignumeric();
  const Type* double_type = type_factory->get_double();
  const Type* float_type = type_factory->get_float();
  const Type* interval_type = types::IntervalType();

  const Type* int32_array_type = types::Int32ArrayType();
  const Type* int64_array_type = types::Int64ArrayType();
  const Type* uint32_array_type = types::Uint32ArrayType();
  const Type* uint64_array_type = types::Uint64ArrayType();
  const Type* numeric_array_type = types::NumericArrayType();
  const Type* bignumeric_array_type = types::BigNumericArrayType();
  const Type* float_array_type = types::FloatArrayType();
  const Type* double_array_type = types::DoubleArrayType();
  const Type* interval_array_type = types::IntervalArrayType();

  const Function::Mode SCALAR = Function::SCALAR;

  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_4_ARRAY_AGGREGATION_FUNCTIONS)
  ) {
    // ARRAY_SUM follows the same input and output types mapping rule defined
    // for SUM, which implicitly coerced the following types:
    //   INT32 -> INT64, UINT32 -> UINT64, and FLOAT -> DOUBLE
    // Note that there are no defined rules for ARRAY<T1> type to be implicitly
    // coerced to ARRAY<T2>. For example:
    //   ARRAY<INT32> -> ARRAY<INT64>
    //   ARRAY<UINT32> -> ARRAY<UINT64>
    //   ARRAY<FLOAT> -> ARRAY<DOUBLE>
    // So concrete typed signatures are required to fill the gap for the above
    // mapping.
    InsertFunction(
        functions, options, "array_sum", SCALAR,
        {ArraySumSig(int64_type, int32_array_type, FN_ARRAY_SUM_INT32),
         ArraySumSig(uint64_type, uint32_array_type, FN_ARRAY_SUM_UINT32),
         ArraySumSig(double_type, float_array_type, FN_ARRAY_SUM_FLOAT),
         ArraySumSig(int64_type, int64_array_type, FN_ARRAY_SUM_INT64),
         ArraySumSig(uint64_type, uint64_array_type, FN_ARRAY_SUM_UINT64),
         ArraySumSig(double_type, double_array_type, FN_ARRAY_SUM_DOUBLE),
         ArraySumSig(numeric_type, numeric_array_type, FN_ARRAY_SUM_NUMERIC),
         ArraySumSig(bignumeric_type, bignumeric_array_type,
                     FN_ARRAY_SUM_BIGNUMERIC),
         ArraySumSig(interval_type, interval_array_type,
                     FN_ARRAY_SUM_INTERVAL)});
    InsertFunction(
        functions, options, "array_avg", SCALAR,
        {ArrayAvgSig(double_type, int32_array_type, FN_ARRAY_AVG_INT32),
         ArrayAvgSig(double_type, int64_array_type, FN_ARRAY_AVG_INT64),
         ArrayAvgSig(double_type, uint32_array_type, FN_ARRAY_AVG_UINT32),
         ArrayAvgSig(double_type, uint64_array_type, FN_ARRAY_AVG_UINT64),
         ArrayAvgSig(double_type, float_array_type, FN_ARRAY_AVG_FLOAT),
         ArrayAvgSig(double_type, double_array_type, FN_ARRAY_AVG_DOUBLE),
         ArrayAvgSig(numeric_type, numeric_array_type, FN_ARRAY_AVG_NUMERIC),
         ArrayAvgSig(bignumeric_type, bignumeric_array_type,
                     FN_ARRAY_AVG_BIGNUMERIC),
         ArrayAvgSig(interval_type, interval_array_type,
                     FN_ARRAY_AVG_INTERVAL)});
  }

  if (!options.language_options.LanguageFeatureEnabled(
          FEATURE_DISABLE_ARRAY_MIN_AND_MAX)) {
    FunctionArgumentType array_min_max_arg(
        ARG_ARRAY_TYPE_ANY_1,
        FunctionArgumentTypeOptions()
            .set_uses_array_element_for_collation()
            .set_array_element_must_support_ordering()
            .set_argument_name("input_array", kPositionalOnly));

    constexpr absl::string_view kArrayMinSql = R"sql(
        IF(
          input_array IS NULL,
          NULL,
          (
            SELECT e
            FROM UNNEST(input_array) AS e WITH OFFSET AS idx
            WHERE e IS NOT NULL
            ORDER BY e ASC, idx ASC LIMIT 1
          ))
      )sql";
    InsertFunction(functions, options, "array_min", SCALAR,
                   {{ARG_TYPE_ANY_1,
                     {array_min_max_arg},
                     FN_ARRAY_MIN,
                     SetDefinitionForInlining(kArrayMinSql)
                         .set_uses_operation_collation()}});

    constexpr absl::string_view kArrayMaxFPSql = R"sql(
        IF(
          input_array IS NULL,
          NULL,
          (
            SELECT e
            FROM UNNEST(input_array) AS e WITH OFFSET AS idx
            WHERE e IS NOT NULL
            ORDER BY IS_NAN(e) DESC, e DESC, idx ASC LIMIT 1
          ))
      )sql";

    constexpr absl::string_view kArrayMaxSql = R"sql(
        IF(
          input_array IS NULL,
          NULL,
          (
            SELECT e
            FROM UNNEST(input_array) AS e WITH OFFSET AS idx
            WHERE e IS NOT NULL
            ORDER BY e DESC, idx ASC LIMIT 1
          ))
      )sql";

    InsertFunction(
        functions, options, "array_max", SCALAR,
        {UnaryArrayFuncConcreteSig(float_type, float_array_type,
                                   FN_ARRAY_MAX_FLOAT, kArrayMaxFPSql),
         UnaryArrayFuncConcreteSig(double_type, double_array_type,
                                   FN_ARRAY_MAX_DOUBLE, kArrayMaxFPSql),
         {ARG_TYPE_ANY_1,
          {array_min_max_arg},
          FN_ARRAY_MAX,
          SetDefinitionForInlining(kArrayMaxSql)
              .set_uses_operation_collation()}});
  }
}

void GetArraySlicingFunctions(TypeFactory* type_factory,
                              const ZetaSQLBuiltinFunctionOptions& options,
                              NameToFunctionMap* functions) {
  FunctionArgumentType array_to_slice_arg(
      ARG_ARRAY_TYPE_ANY_1, FunctionArgumentTypeOptions().set_argument_name(
                                "array_to_slice", kPositionalOnly));
  FunctionArgumentType start_offset_arg(
      type_factory->get_int64(),
      FunctionArgumentTypeOptions().set_argument_name("start_offset",
                                                      kPositionalOnly));
  FunctionArgumentType end_offset_arg(
      type_factory->get_int64(),
      FunctionArgumentTypeOptions().set_argument_name("end_offset",
                                                      kPositionalOnly));

  constexpr absl::string_view kArraySliceSql = R"sql(
      CASE
        WHEN
          array_to_slice IS NULL
          OR start_offset IS NULL
          OR end_offset IS NULL
          THEN NULL
        WHEN ARRAY_LENGTH(array_to_slice) = 0
          THEN []
        ELSE
          WITH(
            start_offset AS
              IF(
                start_offset < 0,
                start_offset + ARRAY_LENGTH(array_to_slice),
                start_offset),
            end_offset AS
              IF(
                end_offset < 0,
                end_offset + ARRAY_LENGTH(array_to_slice),
                end_offset),
            ARRAY(
              SELECT e
              FROM UNNEST(array_to_slice) AS e WITH OFFSET idx
              WHERE idx BETWEEN start_offset AND end_offset
              ORDER BY idx
            ))
        END
    )sql";
  InsertFunction(functions, options, "array_slice", Function::SCALAR,
                 {{ARG_ARRAY_TYPE_ANY_1,
                   {array_to_slice_arg, start_offset_arg, end_offset_arg},
                   FN_ARRAY_SLICE,
                   SetDefinitionForInlining(kArraySliceSql)}});

  FunctionArgumentType input_array_arg(
      ARG_ARRAY_TYPE_ANY_1, FunctionArgumentTypeOptions().set_argument_name(
                                "input_array", kPositionalOnly));
  FunctionArgumentType n_arg(
      type_factory->get_int64(),
      FunctionArgumentTypeOptions().set_argument_name("n", kPositionalOnly));

  constexpr absl::string_view kArrayFirstNSql = R"sql(
      CASE
        WHEN input_array IS NULL OR n IS NULL
          THEN NULL
        WHEN n < 0
          THEN
            ERROR("The n argument to ARRAY_FIRST_N must not be negative.")
        ELSE
          ARRAY(
            SELECT e
            FROM UNNEST(input_array) AS e WITH OFFSET
            WHERE offset < n
            ORDER BY offset
          )
      END
    )sql";
  InsertFunction(
      functions, options, "array_first_n", Function::SCALAR,
      {{ARG_ARRAY_TYPE_ANY_1,
        {input_array_arg, n_arg},
        FN_ARRAY_FIRST_N,
        SetDefinitionForInlining(kArrayFirstNSql, true)
            .add_required_language_feature(FEATURE_V_1_4_FIRST_AND_LAST_N)}});

  constexpr absl::string_view kArrayLastNSql = R"sql(
      CASE
        WHEN input_array IS NULL OR n IS NULL
          THEN NULL
        WHEN n < 0
          THEN ERROR("The n argument to ARRAY_LAST_N must not be negative.")
        ELSE
          WITH (
            start_offset AS ARRAY_LENGTH(input_array) - n,
            ARRAY(
              SELECT e
              FROM UNNEST(input_array) AS e WITH OFFSET
              WHERE offset >= start_offset
              ORDER BY offset
            )
          )
      END
    )sql";
  InsertFunction(
      functions, options, "array_last_n", Function::SCALAR,
      {{ARG_ARRAY_TYPE_ANY_1,
        {input_array_arg, n_arg},
        FN_ARRAY_LAST_N,
        SetDefinitionForInlining(kArrayLastNSql, true)
            .add_required_language_feature(FEATURE_V_1_4_FIRST_AND_LAST_N)}});

  constexpr absl::string_view kArrayRemoveFirstNSql = R"sql(
      CASE
        WHEN input_array IS NULL OR n IS NULL
          THEN NULL
        WHEN n < 0
          THEN ERROR(
            "The n argument to ARRAY_REMOVE_FIRST_N must not be negative.")
        ELSE
          ARRAY(
            SELECT e
            FROM UNNEST(input_array) AS e WITH OFFSET
            WHERE offset >= n
            ORDER BY offset
          )
      END
    )sql";
  InsertFunction(
      functions, options, "array_remove_first_n", Function::SCALAR,
      {{ARG_ARRAY_TYPE_ANY_1,
        {input_array_arg, n_arg},
        FN_ARRAY_REMOVE_FIRST_N,
        SetDefinitionForInlining(kArrayRemoveFirstNSql, true)
            .add_required_language_feature(FEATURE_V_1_4_FIRST_AND_LAST_N)}});

  constexpr absl::string_view kArrayRemoveLastNSql = R"sql(
      CASE
        WHEN input_array IS NULL OR n IS NULL
          THEN NULL
        WHEN n < 0
          THEN ERROR(
            "The n argument to ARRAY_REMOVE_LAST_N must not be negative.")
        ELSE
          WITH (
            end_offset AS ARRAY_LENGTH(input_array) - n,
            ARRAY(
              SELECT e
              FROM UNNEST(input_array) AS e WITH OFFSET
              WHERE offset < end_offset
              ORDER BY offset
            )
          )
      END
    )sql";
  InsertFunction(
      functions, options, "array_remove_last_n", Function::SCALAR,
      {{ARG_ARRAY_TYPE_ANY_1,
        {input_array_arg, n_arg},
        FN_ARRAY_REMOVE_LAST_N,
        SetDefinitionForInlining(kArrayRemoveLastNSql, true)
            .add_required_language_feature(FEATURE_V_1_4_FIRST_AND_LAST_N)}});
}

absl::Status GetArrayFindFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions, NameToTypeMap* types) {
  const Type* array_find_mode_type = types::ArrayFindModeEnumType();

  // TODO: implement the behavior below for all lambda signatures
  // in the ARRAY_FIND family.
  // If there is collation attached to ARG_ARRAY_TYPE_ANY_1, the collation is
  // always attached to lambda argument ARG_TYPE_ANY_1 and used during the
  // resolution of the body of the lambda function.

  FunctionArgumentType input_array_arg(
      ARG_ARRAY_TYPE_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_uses_array_element_for_collation()
          .set_argument_name("input_array", kPositionalOnly));
  FunctionArgumentType input_array_arg_for_lambda_sig(
      ARG_ARRAY_TYPE_ANY_1,
      FunctionArgumentTypeOptions()
          .set_uses_array_element_for_collation()
          .set_argument_name("input_array", kPositionalOnly));
  FunctionArgumentType target_element_arg(
      ARG_TYPE_ANY_1,
      FunctionArgumentTypeOptions()
          .set_must_support_equality()
          .set_argument_name("target_element", kPositionalOnly));
  // The default value for optional enum argument is "FIRST".
  FunctionArgumentType find_mode_arg(
      array_find_mode_type,
      FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
          .set_default(Value::Enum(array_find_mode_type->AsEnum(), 1))
          .set_argument_name("find_mode", kPositionalOnly));
  FunctionArgumentType lambda_arg = FunctionArgumentType::Lambda(
      {ARG_TYPE_ANY_1}, type_factory->get_bool(),
      FunctionArgumentTypeOptions().set_argument_name("condition",
                                                      kPositionalOnly));

  constexpr absl::string_view kArrayOffsetSql = R"sql(
      IF(
        input_array IS NULL OR target_element IS NULL OR find_mode IS NULL,
        NULL,
        CASE find_mode
          WHEN 'FIRST'
            THEN (
              SELECT offset
              FROM UNNEST(input_array) AS e WITH OFFSET
              WHERE e = target_element
              ORDER BY offset LIMIT 1
            )
          WHEN 'LAST'
            THEN (
              SELECT offset
              FROM UNNEST(input_array) AS e WITH OFFSET
              WHERE e = target_element
              ORDER BY offset DESC LIMIT 1
            )
          ELSE
            ERROR(
              CONCAT(
                'ARRAY_FIND_MODE ',
                CAST(find_mode AS STRING),
                ' in ARRAY_OFFSET is unsupported.'))
          END)
    )sql";
  constexpr absl::string_view kArrayOffsetLambdaSql = R"sql(
      IF(
        input_array IS NULL OR find_mode IS NULL,
        NULL,
        CASE find_mode
          WHEN 'FIRST'
            THEN (
              SELECT offset
              FROM UNNEST(input_array) AS e WITH OFFSET
              WHERE condition(e)
              ORDER BY offset LIMIT 1
            )
          WHEN 'LAST'
            THEN (
              SELECT offset
              FROM UNNEST(input_array) AS e WITH OFFSET
              WHERE condition(e)
              ORDER BY offset DESC LIMIT 1
            )
          ELSE
            ERROR(
              CONCAT(
                'ARRAY_FIND_MODE ',
                CAST(find_mode AS STRING),
                ' in ARRAY_OFFSET is unsupported.'))
          END)
    )sql";
  ZETASQL_RETURN_IF_ERROR(InsertFunctionAndTypes(
      functions, types, options, "array_offset", Function::SCALAR,
      {{type_factory->get_int64(),
        {input_array_arg, target_element_arg, find_mode_arg},
        FN_ARRAY_OFFSET,
        SetDefinitionForInlining(kArrayOffsetSql)
            .set_uses_operation_collation()},
       {type_factory->get_int64(),
        {input_array_arg_for_lambda_sig, lambda_arg, find_mode_arg},
        FN_ARRAY_OFFSET_LAMBDA,
        SetDefinitionForInlining(kArrayOffsetLambdaSql)
            .set_uses_operation_collation()}},
      /*function_options=*/{}, /*types_to_insert=*/{array_find_mode_type}));

  constexpr absl::string_view kArrayOffsetsSql = R"sql(
      IF(input_array IS NULL OR target_element IS NULL,
        NULL,
        ARRAY(
          SELECT offset
          FROM UNNEST(input_array) AS e WITH OFFSET
          WHERE e = target_element
          ORDER BY offset
        ))
    )sql";
  constexpr absl::string_view kArrayOffsetsLambdaSql = R"sql(
      IF(input_array IS NULL,
        NULL,
        ARRAY(
          SELECT offset
          FROM UNNEST(input_array) AS e WITH OFFSET
          WHERE condition(e)
          ORDER BY offset
        ))
    )sql";
  InsertFunction(functions, options, "array_offsets", Function::SCALAR,
                 {{types::Int64ArrayType(),
                   {input_array_arg, target_element_arg},
                   FN_ARRAY_OFFSETS,
                   SetDefinitionForInlining(kArrayOffsetsSql)
                       .set_uses_operation_collation()},
                  {types::Int64ArrayType(),
                   {input_array_arg_for_lambda_sig, lambda_arg},
                   FN_ARRAY_OFFSETS_LAMBDA,
                   SetDefinitionForInlining(kArrayOffsetsLambdaSql)
                       .set_uses_operation_collation()}});

  constexpr absl::string_view kArrayFindSql = R"sql(
      IF(
        input_array IS NULL OR target_element IS NULL OR find_mode IS NULL,
        NULL,
        CASE find_mode
          WHEN 'FIRST'
            THEN (
              SELECT e
              FROM UNNEST(input_array) AS e WITH OFFSET
              WHERE e = target_element
              ORDER BY offset LIMIT 1
            )
          WHEN 'LAST'
            THEN (
              SELECT e
              FROM UNNEST(input_array) AS e WITH OFFSET
              WHERE e = target_element
              ORDER BY offset DESC LIMIT 1
            )
          ELSE
            ERROR(
              CONCAT(
                'ARRAY_FIND_MODE ',
                CAST(find_mode AS STRING),
                ' ARRAY_FIND_MODE in ARRAY_FIND is unsupported.'))
          END)
    )sql";
  constexpr absl::string_view kArrayFindLambdaSql = R"sql(
      IF(
        input_array IS NULL OR find_mode IS NULL,
        NULL,
        CASE find_mode
          WHEN 'FIRST'
            THEN (
              SELECT e
              FROM UNNEST(input_array) AS e WITH OFFSET
              WHERE condition(e)
              ORDER BY offset LIMIT 1
            )
          WHEN 'LAST'
            THEN (
              SELECT e
              FROM UNNEST(input_array) AS e WITH OFFSET
              WHERE condition(e)
              ORDER BY offset DESC LIMIT 1
            )
          ELSE
            ERROR(
              CONCAT(
                'ARRAY_FIND_MODE ',
                CAST(find_mode AS STRING),
                ' ARRAY_FIND_MODE in ARRAY_FIND is unsupported.'))
          END)
    )sql";
  ZETASQL_RETURN_IF_ERROR(InsertFunctionAndTypes(
      functions, types, options, "array_find", Function::SCALAR,
      {{ARG_TYPE_ANY_1,
        {input_array_arg, target_element_arg, find_mode_arg},
        FN_ARRAY_FIND,
        SetDefinitionForInlining(kArrayFindSql).set_uses_operation_collation()},
       {ARG_TYPE_ANY_1,
        {input_array_arg_for_lambda_sig, lambda_arg, find_mode_arg},
        FN_ARRAY_FIND_LAMBDA,
        SetDefinitionForInlining(kArrayFindLambdaSql)
            .set_uses_operation_collation()}},
      /*function_options=*/{}, /*types_to_insert=*/{array_find_mode_type}));

  constexpr absl::string_view kArrayFindAllSql = R"sql(
      IF(input_array IS NULL OR target_element IS NULL,
        NULL,
        ARRAY(
          SELECT e
          FROM UNNEST(input_array) AS e WITH OFFSET
          WHERE e = target_element
          ORDER BY offset
        ))
    )sql";
  constexpr absl::string_view kArrayFindAllLambdaSql = R"sql(
      IF(input_array IS NULL,
        NULL,
        ARRAY(
          SELECT e
          FROM UNNEST(input_array) AS e WITH OFFSET
          WHERE condition(e)
          ORDER BY offset
        ))
    )sql";
  InsertFunction(
      functions, options, "array_find_all", Function::SCALAR,
      {{{ARG_ARRAY_TYPE_ANY_1,
         FunctionArgumentTypeOptions().set_uses_array_element_for_collation()},
        {input_array_arg, target_element_arg},
        FN_ARRAY_FIND_ALL,
        SetDefinitionForInlining(kArrayFindAllSql)
            .set_uses_operation_collation()},
       {{ARG_ARRAY_TYPE_ANY_1,
         FunctionArgumentTypeOptions().set_uses_array_element_for_collation()},
        {input_array_arg_for_lambda_sig, lambda_arg},
        FN_ARRAY_FIND_ALL_LAMBDA,
        SetDefinitionForInlining(kArrayFindAllLambdaSql)
            .set_uses_operation_collation()}});

  return absl::OkStatus();
}

void GetArrayFilteringFunctions(TypeFactory* type_factory,
                                const ZetaSQLBuiltinFunctionOptions& options,
                                NameToFunctionMap* functions) {
  FunctionArgumentType input_array_arg(
      ARG_ARRAY_TYPE_ANY_1, FunctionArgumentTypeOptions().set_argument_name(
                                "array_to_filter", kPositionalOnly));
  FunctionArgumentType output_array(ARG_ARRAY_TYPE_ANY_1);
  FunctionArgumentType filter_function_arg = FunctionArgumentType::Lambda(
      {ARG_TYPE_ANY_1}, type_factory->get_bool(),
      FunctionArgumentTypeOptions().set_argument_name("condition",
                                                      kPositionalOnly));
  FunctionArgumentType filter_function_arg_with_offset =
      FunctionArgumentType::Lambda(
          {ARG_TYPE_ANY_1, type_factory->get_int64()}, type_factory->get_bool(),
          FunctionArgumentTypeOptions().set_argument_name("condition",
                                                          kPositionalOnly));

  // TODO: implement the behavior below
  // If there is collation attached to ARG_ARRAY_TYPE_ANY_1, the collation is
  // always attached to lambda argument ARG_TYPE_ANY_1 and used during the
  // resolution of the body of the lambda function.

  constexpr absl::string_view kArrayFilterSql = R"sql(
      IF (array_to_filter IS NULL,
          NULL,
          ARRAY(
            SELECT element
            FROM UNNEST(array_to_filter) AS element WITH OFFSET off
            WHERE condition(element)
            ORDER BY off
          )
        )
      )sql";

  constexpr absl::string_view kArrayFilterSqlWithOffset = R"sql(
      IF (array_to_filter IS NULL,
          NULL,
          ARRAY(
            SELECT element
            FROM UNNEST(array_to_filter) AS element WITH OFFSET off
            WHERE condition(element, off)
            ORDER BY off
          )
        )
      )sql";

  InsertFunction(
      functions, options, "array_filter", Function::SCALAR,
      {{output_array,
        {input_array_arg, filter_function_arg},
        FN_ARRAY_FILTER,
        SetDefinitionForInlining(kArrayFilterSql,
                                 IsRewriteEnabled(FN_ARRAY_FILTER, options))},
       {output_array,
        {input_array_arg, filter_function_arg_with_offset},
        FN_ARRAY_FILTER_WITH_INDEX,
        SetDefinitionForInlining(
            kArrayFilterSqlWithOffset,
            IsRewriteEnabled(FN_ARRAY_FILTER_WITH_INDEX, options))}},
      FunctionOptions().set_supports_safe_error_mode(
          options.language_options.LanguageFeatureEnabled(
              FEATURE_V_1_4_SAFE_FUNCTION_CALL_WITH_LAMBDA_ARGS)));
}

void GetArrayTransformFunctions(TypeFactory* type_factory,
                                const ZetaSQLBuiltinFunctionOptions& options,
                                NameToFunctionMap* functions) {
  FunctionArgumentType input_array_arg(
      ARG_ARRAY_TYPE_ANY_1,
      FunctionArgumentTypeOptions()
          .set_argument_name("array_to_transform", kPositionalOnly)
          .set_argument_collation_mode(FunctionEnums::AFFECTS_NONE));
  FunctionArgumentType output_array(
      ARG_ARRAY_TYPE_ANY_2,
      FunctionArgumentTypeOptions().set_uses_array_element_for_collation());
  FunctionArgumentType transform_function_arg = FunctionArgumentType::Lambda(
      {ARG_TYPE_ANY_1}, ARG_TYPE_ANY_2,
      FunctionArgumentTypeOptions().set_argument_name("transformation",
                                                      kPositionalOnly));
  FunctionArgumentType transform_function_arg_with_offset =
      FunctionArgumentType::Lambda(
          {ARG_TYPE_ANY_1, type_factory->get_int64()}, ARG_TYPE_ANY_2,
          FunctionArgumentTypeOptions().set_argument_name("transformation",
                                                          kPositionalOnly));

  constexpr absl::string_view kArrayTransformSql = R"sql(
      IF (array_to_transform IS NULL,
          NULL,
          ARRAY(
            SELECT transformation(element)
            FROM UNNEST(array_to_transform) AS element WITH OFFSET off
            ORDER BY off
          )
      )
      )sql";

  constexpr absl::string_view kArrayTransformWithIndexSql = R"sql(
      IF (array_to_transform IS NULL,
          NULL,
          ARRAY(
            SELECT transformation(element, off)
            FROM UNNEST(array_to_transform) AS element WITH OFFSET off
            ORDER BY off
          )
      )
      )sql";

  // The collation propagation on the signature:
  // (
  //   ARRAY_TYPE_ANY_1,
  //   Lambda(TYPE_ANY_1 [, int64]) -> TYPE_ANY_2)
  // ) -> ARRAY_TYPE_ANY_2
  //
  // 1) on the first argument, setting collation_mode to AFFECTS_NONE so that
  // the collation on the ARRAY_TYPE_ANY_1 doesn't directly propagate to the
  // return type.
  // TODO: implement the behavior in 2)
  // 2) the lambda resolution is not affected by the collation_mode setting. If
  // there is collation attached to ARG_ARRAY_TYPE_ANY_1, the collation is
  // always attached to lambda argument ARG_TYPE_ANY_1 and used during the
  // resolution of the body of the lambda function.
  // 3) the collation of return type ARRAY_TYPE_ANY_2 is decided by lambda
  // return type TYPE_ANY_2.
  InsertFunction(
      functions, options, "array_transform", Function::SCALAR,
      {{output_array,
        {input_array_arg, transform_function_arg},
        FN_ARRAY_TRANSFORM,
        SetDefinitionForInlining(
            kArrayTransformSql, IsRewriteEnabled(FN_ARRAY_TRANSFORM, options))},
       {output_array,
        {input_array_arg, transform_function_arg_with_offset},
        FN_ARRAY_TRANSFORM_WITH_INDEX,
        SetDefinitionForInlining(
            kArrayTransformWithIndexSql,
            IsRewriteEnabled(FN_ARRAY_TRANSFORM_WITH_INDEX, options))}},
      FunctionOptions().set_supports_safe_error_mode(
          options.language_options.LanguageFeatureEnabled(
              FEATURE_V_1_4_SAFE_FUNCTION_CALL_WITH_LAMBDA_ARGS)));
}

void GetArrayIncludesFunctions(TypeFactory* type_factory,
                               const ZetaSQLBuiltinFunctionOptions& options,
                               NameToFunctionMap* functions) {
  const Type* bool_type = type_factory->get_bool();
  FunctionArgumentType array_to_search_arg(
      ARG_ARRAY_TYPE_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_argument_name("array_to_search", kPositionalOnly));
  FunctionArgumentType array_to_search_arg_2(
      ARG_ARRAY_TYPE_ANY_1, FunctionArgumentTypeOptions().set_argument_name(
                                "array_to_search", kPositionalOnly));
  FunctionArgumentType search_value_arg(
      ARG_TYPE_ANY_1, FunctionArgumentTypeOptions().set_argument_name(
                          "search_value", kPositionalOnly));
  FunctionArgumentType search_values_arg(
      ARG_ARRAY_TYPE_ANY_1,
      FunctionArgumentTypeOptions()
          .set_array_element_must_support_equality()
          .set_argument_name("search_values", kPositionalOnly));
  FunctionArgumentType array_include_lambda_arg = FunctionArgumentType::Lambda(
      {ARG_TYPE_ANY_1}, type_factory->get_bool(),
      FunctionArgumentTypeOptions().set_argument_name("condition",
                                                      kPositionalOnly));

  constexpr absl::string_view kArrayIncludesSql = R"sql(
      IF (array_to_search IS NULL OR search_value is NULL,
      NULL,
      EXISTS(SELECT 1
              FROM UNNEST(array_to_search) AS element
              WHERE element = search_value)
      )
      )sql";

  constexpr absl::string_view kArrayIncludesLambdaSql = R"sql(
      IF (array_to_search IS NULL,
          NULL,
          EXISTS(SELECT 1
                  FROM UNNEST(array_to_search) AS element
                  WHERE condition(element)
          )
      )
      )sql";

  constexpr absl::string_view kArrayIncludesAnySql = R"sql(
      IF (array_to_search IS NULL OR search_values is NULL,
          NULL,
          EXISTS(SELECT 1
                  FROM UNNEST(array_to_search) AS element
                  WHERE element IN UNNEST(search_values)
          )
      )
      )sql";

  constexpr absl::string_view kArrayIncludesAllSql = R"sql(
      IF (array_to_search IS NULL OR search_values is NULL, NULL,
          IF (ARRAY_LENGTH(search_values) = 0,
              TRUE,
              (SELECT LOGICAL_AND(IFNULL(element IN UNNEST(array_to_search), FALSE))
                FROM UNNEST(search_values) AS element)))
      )sql";

  // TODO: implement the behavior below
  // If there is collation attached to ARG_ARRAY_TYPE_ANY_1, the collation is
  // always attached to lambda argument ARG_TYPE_ANY_1 and used during the
  // resolution of the body of the lambda function.
  InsertFunction(
      functions, options, "array_includes", Function::SCALAR,
      {{bool_type,
        {array_to_search_arg, search_value_arg},
        FN_ARRAY_INCLUDES,
        SetDefinitionForInlining(kArrayIncludesSql,
                                 IsRewriteEnabled(FN_ARRAY_INCLUDES, options))},
       {bool_type,
        {array_to_search_arg_2, array_include_lambda_arg},
        FN_ARRAY_INCLUDES_LAMBDA,
        SetDefinitionForInlining(
            kArrayIncludesLambdaSql,
            IsRewriteEnabled(FN_ARRAY_INCLUDES_LAMBDA, options))}},
      FunctionOptions().set_supports_safe_error_mode(
          options.language_options.LanguageFeatureEnabled(
              FEATURE_V_1_4_SAFE_FUNCTION_CALL_WITH_LAMBDA_ARGS)));

  InsertFunction(functions, options, "array_includes_any", Function::SCALAR,
                 {{bool_type,
                   {array_to_search_arg, search_values_arg},
                   FN_ARRAY_INCLUDES_ANY,
                   SetDefinitionForInlining(
                       kArrayIncludesAnySql,
                       IsRewriteEnabled(FN_ARRAY_INCLUDES_ANY, options))}},
                 FunctionOptions().set_supports_safe_error_mode(
                     options.language_options.LanguageFeatureEnabled(
                         FEATURE_V_1_4_SAFE_FUNCTION_CALL_WITH_LAMBDA_ARGS)));

  InsertFunction(functions, options, "array_includes_all", Function::SCALAR,
                 {{bool_type,
                   {array_to_search_arg, search_values_arg},
                   FN_ARRAY_INCLUDES_ALL,
                   SetDefinitionForInlining(
                       kArrayIncludesAllSql,
                       IsRewriteEnabled(FN_ARRAY_INCLUDES_ALL, options))}},
                 FunctionOptions().set_supports_safe_error_mode(
                     options.language_options.LanguageFeatureEnabled(
                         FEATURE_V_1_4_SAFE_FUNCTION_CALL_WITH_LAMBDA_ARGS)));
}

}  // namespace zetasql
