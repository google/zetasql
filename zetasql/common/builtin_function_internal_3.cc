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

#include <ctype.h>

#include <algorithm>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/timestamp.pb.h"
#include "google/protobuf/wrappers.pb.h"
#include "google/type/date.pb.h"
#include "google/type/timeofday.pb.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/cycle_detector.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "zetasql/base/case.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

class AnalyzerOptions;

using ::absl::bind_front;

void GetStringFunctions(TypeFactory* type_factory,
                        const ZetaSQLBuiltinFunctionOptions& options,
                        NameToFunctionMap* functions) {
  const Type* string_type = type_factory->get_string();
  const Type* bytes_type = type_factory->get_bytes();
  const Type* int64_type = type_factory->get_int64();
  const Type* bool_type = type_factory->get_bool();
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* normalize_mode_type = types::NormalizeModeEnumType();

  const Function::Mode SCALAR = Function::SCALAR;

  const FunctionArgumentType::ArgumentCardinality REPEATED =
      FunctionArgumentType::REPEATED;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  FunctionArgumentTypeOptions concat_option_1;
  if (options.language_options.LanguageFeatureEnabled(
          zetasql::FEATURE_V_1_3_CONCAT_MIXED_TYPES)) {
    concat_option_1.set_allow_coercion_from(&CanStringConcatCoerceFrom);
  }

  FunctionArgumentTypeOptions concat_option_n;
  concat_option_n.set_cardinality(REPEATED);
  if (options.language_options.LanguageFeatureEnabled(
          zetasql::FEATURE_V_1_3_CONCAT_MIXED_TYPES)) {
    concat_option_n.set_allow_coercion_from(&CanStringConcatCoerceFrom);
  }

  InsertFunction(
      functions, options, "concat", SCALAR,
      {{string_type,
        {{string_type, concat_option_1}, {string_type, concat_option_n}},
        FN_CONCAT_STRING},
       {bytes_type, {bytes_type, {bytes_type, REPEATED}}, FN_CONCAT_BYTES}});

  InsertFunction(functions, options, "strpos", SCALAR,
                 {{int64_type, {string_type, string_type}, FN_STRPOS_STRING},
                  {int64_type, {bytes_type, bytes_type}, FN_STRPOS_BYTES}});

  InsertFunction(functions, options, "lower", SCALAR,
                 {{string_type, {string_type}, FN_LOWER_STRING},
                  {bytes_type, {bytes_type}, FN_LOWER_BYTES}});

  InsertFunction(functions, options, "upper", SCALAR,
                 {{string_type, {string_type}, FN_UPPER_STRING},
                  {bytes_type, {bytes_type}, FN_UPPER_BYTES}});

  InsertFunction(functions, options, "length", SCALAR,
                 {{int64_type, {string_type}, FN_LENGTH_STRING},
                  {int64_type, {bytes_type}, FN_LENGTH_BYTES}});

  InsertFunction(functions, options, "byte_length", SCALAR,
                 {{int64_type, {string_type}, FN_BYTE_LENGTH_STRING},
                  {int64_type, {bytes_type}, FN_BYTE_LENGTH_BYTES}});

  InsertFunction(functions, options, "char_length", SCALAR,
                 {{int64_type, {string_type}, FN_CHAR_LENGTH_STRING}},
                 FunctionOptions().set_alias_name("character_length"));

  InsertFunction(
      functions, options, "starts_with", SCALAR,
      {{bool_type, {string_type, string_type}, FN_STARTS_WITH_STRING},
       {bool_type, {bytes_type, bytes_type}, FN_STARTS_WITH_BYTES}});

  InsertFunction(functions, options, "ends_with", SCALAR,
                 {{bool_type, {string_type, string_type}, FN_ENDS_WITH_STRING},
                  {bool_type, {bytes_type, bytes_type}, FN_ENDS_WITH_BYTES}});

  InsertFunction(functions, options, "substr", SCALAR,
                 {{string_type,
                   {string_type, int64_type, {int64_type, OPTIONAL}},
                   FN_SUBSTR_STRING},
                  {bytes_type,
                   {bytes_type, int64_type, {int64_type, OPTIONAL}},
                   FN_SUBSTR_BYTES}});

  InsertFunction(
      functions, options, "trim", SCALAR,
      {{string_type, {string_type, {string_type, OPTIONAL}}, FN_TRIM_STRING},
       {bytes_type, {bytes_type, bytes_type}, FN_TRIM_BYTES}});

  InsertFunction(
      functions, options, "ltrim", SCALAR,
      {{string_type, {string_type, {string_type, OPTIONAL}}, FN_LTRIM_STRING},
       {bytes_type, {bytes_type, bytes_type}, FN_LTRIM_BYTES}});

  InsertFunction(
      functions, options, "rtrim", SCALAR,
      {{string_type, {string_type, {string_type, OPTIONAL}}, FN_RTRIM_STRING},
       {bytes_type, {bytes_type, bytes_type}, FN_RTRIM_BYTES}});

  InsertFunction(functions, options, "lpad", SCALAR,
                 {{string_type,
                   {string_type, int64_type, {string_type, OPTIONAL}},
                   FN_LPAD_STRING},
                  {bytes_type,
                   {bytes_type, int64_type, {bytes_type, OPTIONAL}},
                   FN_LPAD_BYTES}});

  InsertFunction(functions, options, "rpad", SCALAR,
                 {{string_type,
                   {string_type, int64_type, {string_type, OPTIONAL}},
                   FN_RPAD_STRING},
                  {bytes_type,
                   {bytes_type, int64_type, {bytes_type, OPTIONAL}},
                   FN_RPAD_BYTES}});

  InsertFunction(functions, options, "left", SCALAR,
                 {{string_type, {string_type, int64_type}, FN_LEFT_STRING},
                  {bytes_type, {bytes_type, int64_type}, FN_LEFT_BYTES}});
  InsertFunction(functions, options, "right", SCALAR,
                 {{string_type, {string_type, int64_type}, FN_RIGHT_STRING},
                  {bytes_type, {bytes_type, int64_type}, FN_RIGHT_BYTES}});

  InsertFunction(functions, options, "repeat", SCALAR,
                 {{string_type, {string_type, int64_type}, FN_REPEAT_STRING},
                  {bytes_type, {bytes_type, int64_type}, FN_REPEAT_BYTES}});

  InsertFunction(functions, options, "reverse", SCALAR,
                 {{string_type, {string_type}, FN_REVERSE_STRING},
                  {bytes_type, {bytes_type}, FN_REVERSE_BYTES}});

  InsertFunction(
      functions, options, "replace", SCALAR,
      {{string_type,
        {string_type, string_type, string_type},
        FN_REPLACE_STRING},
       {bytes_type, {bytes_type, bytes_type, bytes_type}, FN_REPLACE_BYTES}});

  InsertFunction(functions, options, "string", SCALAR,
                 {{string_type,
                   {timestamp_type, {string_type, OPTIONAL}},
                   FN_STRING_FROM_TIMESTAMP}});

  const ArrayType* string_array_type = types::StringArrayType();
  const ArrayType* bytes_array_type = types::BytesArrayType();
  const ArrayType* int64_array_type = types::Int64ArrayType();

  InsertFunction(
      functions, options, "split", SCALAR,
      // Note that the delimiter second parameter is optional for the STRING
      // version, but is required for the BYTES version.
      {{string_array_type,
        {string_type, {string_type, OPTIONAL}},
        FN_SPLIT_STRING},
       {bytes_array_type, {bytes_type, bytes_type}, FN_SPLIT_BYTES}});

  InsertFunction(
      functions, options, "safe_convert_bytes_to_string", SCALAR,
      {{string_type, {bytes_type}, FN_SAFE_CONVERT_BYTES_TO_STRING}});

  InsertFunction(functions, options, "normalize", SCALAR,
                 {
                     {string_type,
                      {string_type, {normalize_mode_type, OPTIONAL}},
                      FN_NORMALIZE_STRING},
                 });
  InsertFunction(functions, options, "normalize_and_casefold", SCALAR,
                 {{string_type,
                   {string_type, {normalize_mode_type, OPTIONAL}},
                   FN_NORMALIZE_AND_CASEFOLD_STRING}});
  InsertFunction(functions, options, "to_base32", SCALAR,
                 {{string_type, {bytes_type}, FN_TO_BASE32}});
  InsertFunction(functions, options, "from_base32", SCALAR,
                 {{bytes_type, {string_type}, FN_FROM_BASE32}});
  InsertFunction(functions, options, "to_base64", SCALAR,
                 {{string_type, {bytes_type}, FN_TO_BASE64}});
  InsertFunction(functions, options, "from_base64", SCALAR,
                 {{bytes_type, {string_type}, FN_FROM_BASE64}});
  InsertFunction(functions, options, "to_hex", SCALAR,
                 {{string_type, {bytes_type}, FN_TO_HEX}});
  InsertFunction(functions, options, "from_hex", SCALAR,
                 {{bytes_type, {string_type}, FN_FROM_HEX}});
  InsertFunction(functions, options, "to_code_points", SCALAR,
                 {{int64_array_type, {string_type}, FN_TO_CODE_POINTS_STRING},
                  {int64_array_type, {bytes_type}, FN_TO_CODE_POINTS_BYTES}});
  InsertFunction(functions, options, "code_points_to_string", SCALAR,
                 {{string_type, {int64_array_type}, FN_CODE_POINTS_TO_STRING}});
  InsertFunction(functions, options, "code_points_to_bytes", SCALAR,
                 {{bytes_type, {int64_array_type}, FN_CODE_POINTS_TO_BYTES}});
}

void GetRegexFunctions(TypeFactory* type_factory,

                       const ZetaSQLBuiltinFunctionOptions& options,
                       NameToFunctionMap* functions) {
  const Type* string_type = type_factory->get_string();
  const Type* bytes_type = type_factory->get_bytes();
  const Type* bool_type = type_factory->get_bool();

  const Function::Mode SCALAR = Function::SCALAR;

  InsertFunction(
      functions, options, "regexp_match", SCALAR,
      {{bool_type, {string_type, string_type}, FN_REGEXP_MATCH_STRING},
       {bool_type, {bytes_type, bytes_type}, FN_REGEXP_MATCH_BYTES}},
      FunctionOptions().set_allow_external_usage(false));
  InsertFunction(
      functions, options, "regexp_contains", SCALAR,
      {{bool_type, {string_type, string_type}, FN_REGEXP_CONTAINS_STRING},
       {bool_type, {bytes_type, bytes_type}, FN_REGEXP_CONTAINS_BYTES}});

  InsertFunction(
      functions, options, "regexp_extract", SCALAR,
      {{string_type, {string_type, string_type}, FN_REGEXP_EXTRACT_STRING},
       {bytes_type, {bytes_type, bytes_type}, FN_REGEXP_EXTRACT_BYTES}});

  InsertFunction(functions, options, "regexp_replace", SCALAR,
                 {{string_type,
                   {string_type, string_type, string_type},
                   FN_REGEXP_REPLACE_STRING},
                  {bytes_type,
                   {bytes_type, bytes_type, bytes_type},
                   FN_REGEXP_REPLACE_BYTES}});
  const ArrayType* string_array_type = types::StringArrayType();
  const ArrayType* bytes_array_type = types::BytesArrayType();

  InsertFunction(functions, options, "regexp_extract_all", SCALAR,
                 {{string_array_type,
                   {string_type, string_type},
                   FN_REGEXP_EXTRACT_ALL_STRING},
                  {bytes_array_type,
                   {bytes_type, bytes_type},
                   FN_REGEXP_EXTRACT_ALL_BYTES}});
}

void GetProto3ConversionFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions) {
  const Function::Mode SCALAR = Function::SCALAR;
  const Type* bool_type = type_factory->get_bool();
  const Type* int32_type = type_factory->get_int32();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint32_type = type_factory->get_uint32();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* double_type = type_factory->get_double();
  const Type* date_type = type_factory->get_date();
  const Type* string_type = type_factory->get_string();
  const Type* bytes_type = type_factory->get_bytes();
  const Type* proto_timestamp_type = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      google::protobuf::Timestamp::descriptor(), &proto_timestamp_type));
  const Type* proto_date_type = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(google::type::Date::descriptor(),
                                       &proto_date_type));
  const Type* proto_time_of_day_type = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(google::type::TimeOfDay::descriptor(),
                                       &proto_time_of_day_type));
  const Type* proto_double_wrapper = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      google::protobuf::DoubleValue::descriptor(), &proto_double_wrapper));
  const Type* proto_float_wrapper = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      google::protobuf::FloatValue::descriptor(), &proto_float_wrapper));
  const Type* proto_int64_wrapper = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      google::protobuf::Int64Value::descriptor(), &proto_int64_wrapper));
  const Type* proto_uint64_wrapper = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      google::protobuf::UInt64Value::descriptor(), &proto_uint64_wrapper));
  const Type* proto_int32_wrapper = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      google::protobuf::Int32Value::descriptor(), &proto_int32_wrapper));
  const Type* proto_uint32_wrapper = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      google::protobuf::UInt32Value::descriptor(), &proto_uint32_wrapper));
  const Type* proto_bool_wrapper = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      google::protobuf::BoolValue::descriptor(), &proto_bool_wrapper));
  const Type* proto_string_wrapper = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      google::protobuf::StringValue::descriptor(), &proto_string_wrapper));
  const Type* proto_bytes_wrapper = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeProtoType(
      google::protobuf::BytesValue::descriptor(), &proto_bytes_wrapper));
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* time_type = type_factory->get_time();
  const Type* float_type = type_factory->get_float();

  std::vector<FunctionSignatureOnHeap> from_proto_signatures = {
      {timestamp_type, {proto_timestamp_type}, FN_FROM_PROTO_TIMESTAMP},
      {timestamp_type, {timestamp_type}, FN_FROM_PROTO_IDEMPOTENT_TIMESTAMP},
      {date_type, {proto_date_type}, FN_FROM_PROTO_DATE},
      {date_type, {date_type}, FN_FROM_PROTO_IDEMPOTENT_DATE},
      {double_type, {proto_double_wrapper}, FN_FROM_PROTO_DOUBLE},
      {double_type, {double_type}, FN_FROM_PROTO_IDEMPOTENT_DOUBLE},
      {float_type, {proto_float_wrapper}, FN_FROM_PROTO_FLOAT},
      {float_type, {float_type}, FN_FROM_PROTO_IDEMPOTENT_FLOAT},
      {int64_type, {proto_int64_wrapper}, FN_FROM_PROTO_INT64},
      {int64_type, {int64_type}, FN_FROM_PROTO_IDEMPOTENT_INT64},
      {uint64_type, {proto_uint64_wrapper}, FN_FROM_PROTO_UINT64},
      {uint64_type, {uint64_type}, FN_FROM_PROTO_IDEMPOTENT_UINT64},
      {int32_type, {proto_int32_wrapper}, FN_FROM_PROTO_INT32},
      {int32_type, {int32_type}, FN_FROM_PROTO_IDEMPOTENT_INT32},
      {uint32_type, {proto_uint32_wrapper}, FN_FROM_PROTO_UINT32},
      {uint32_type, {uint32_type}, FN_FROM_PROTO_IDEMPOTENT_UINT32},
      {bool_type, {proto_bool_wrapper}, FN_FROM_PROTO_BOOL},
      {bool_type, {bool_type}, FN_FROM_PROTO_IDEMPOTENT_BOOL},
      {bytes_type, {proto_bytes_wrapper}, FN_FROM_PROTO_BYTES},
      {bytes_type, {bytes_type}, FN_FROM_PROTO_IDEMPOTENT_BYTES},
      {string_type, {proto_string_wrapper}, FN_FROM_PROTO_STRING},
      {string_type, {string_type}, FN_FROM_PROTO_IDEMPOTENT_STRING}};

  std::vector<FunctionSignatureOnHeap> to_proto_signatures = {
      {proto_timestamp_type, {timestamp_type}, FN_TO_PROTO_TIMESTAMP},
      {proto_timestamp_type,
       {proto_timestamp_type},
       FN_TO_PROTO_IDEMPOTENT_TIMESTAMP},
      {proto_date_type, {date_type}, FN_TO_PROTO_DATE},
      {proto_date_type, {proto_date_type}, FN_TO_PROTO_IDEMPOTENT_DATE},
      {proto_double_wrapper, {double_type}, FN_TO_PROTO_DOUBLE},
      {proto_double_wrapper,
       {proto_double_wrapper},
       FN_TO_PROTO_IDEMPOTENT_DOUBLE},
      {proto_float_wrapper, {float_type}, FN_TO_PROTO_FLOAT},
      {proto_float_wrapper,
       {proto_float_wrapper},
       FN_TO_PROTO_IDEMPOTENT_FLOAT},
      {proto_int64_wrapper, {int64_type}, FN_TO_PROTO_INT64},
      {proto_int64_wrapper,
       {proto_int64_wrapper},
       FN_TO_PROTO_IDEMPOTENT_INT64},
      {proto_uint64_wrapper, {uint64_type}, FN_TO_PROTO_UINT64},
      {proto_uint64_wrapper,
       {proto_uint64_wrapper},
       FN_TO_PROTO_IDEMPOTENT_UINT64},
      {proto_int32_wrapper, {int32_type}, FN_TO_PROTO_INT32},
      {proto_int32_wrapper,
       {proto_int32_wrapper},
       FN_TO_PROTO_IDEMPOTENT_INT32},
      {proto_uint32_wrapper, {uint32_type}, FN_TO_PROTO_UINT32},
      {proto_uint32_wrapper,
       {proto_uint32_wrapper},
       FN_TO_PROTO_IDEMPOTENT_UINT32},
      {proto_bool_wrapper, {bool_type}, FN_TO_PROTO_BOOL},
      {proto_bool_wrapper, {proto_bool_wrapper}, FN_TO_PROTO_IDEMPOTENT_BOOL},
      {proto_bytes_wrapper, {bytes_type}, FN_TO_PROTO_BYTES},
      {proto_bytes_wrapper,
       {proto_bytes_wrapper},
       FN_TO_PROTO_IDEMPOTENT_BYTES},
      {proto_string_wrapper, {string_type}, FN_TO_PROTO_STRING},
      {proto_string_wrapper,
       {proto_string_wrapper},
       FN_TO_PROTO_IDEMPOTENT_STRING}};

  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_2_CIVIL_TIME)) {
    from_proto_signatures.push_back(
        {time_type, {proto_time_of_day_type}, FN_FROM_PROTO_TIME_OF_DAY});
    from_proto_signatures.push_back(
        {time_type, {time_type}, FN_FROM_PROTO_IDEMPOTENT_TIME});
    to_proto_signatures.push_back(
        {proto_time_of_day_type, {time_type}, FN_TO_PROTO_TIME});
    to_proto_signatures.push_back({proto_time_of_day_type,
                                   {proto_time_of_day_type},
                                   FN_TO_PROTO_IDEMPOTENT_TIME_OF_DAY});
  }

  InsertFunction(functions, options, "from_proto", SCALAR,
                 from_proto_signatures,
                 FunctionOptions().set_allow_external_usage(false));

  InsertFunction(functions, options, "to_proto", SCALAR, to_proto_signatures,
                 FunctionOptions().set_allow_external_usage(false));
}

void GetMiscellaneousFunctions(TypeFactory* type_factory,
                               const ZetaSQLBuiltinFunctionOptions& options,
                               NameToFunctionMap* functions) {
  const Type* bool_type = type_factory->get_bool();
  const Type* int32_type = type_factory->get_int32();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint32_type = type_factory->get_uint32();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* numeric_type = type_factory->get_numeric();
  const Type* bignumeric_type = type_factory->get_bignumeric();
  const Type* double_type = type_factory->get_double();
  const Type* date_type = type_factory->get_date();
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* datepart_type = types::DatePartEnumType();

  const Type* string_type = type_factory->get_string();
  const Type* bytes_type = type_factory->get_bytes();
  const ArrayType* array_string_type;
  ZETASQL_CHECK_OK(type_factory->MakeArrayType(string_type, &array_string_type));
  const ArrayType* array_bytes_type;
  ZETASQL_CHECK_OK(type_factory->MakeArrayType(bytes_type, &array_bytes_type));
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

  InsertFunction(
      functions, options, "if", SCALAR,
      {{ARG_TYPE_ANY_1, {bool_type, ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_IF}});

  // COALESCE(expr1, ..., exprN): returns the first non-null expression.
  // In particular, COALESCE is used to express the output of FULL JOIN.
  InsertFunction(functions, options, "coalesce", SCALAR,
                 {{ARG_TYPE_ANY_1, {{ARG_TYPE_ANY_1, REPEATED}}, FN_COALESCE}});

  // IFNULL(expr1, expr2): if expr1 is not null, returns expr1, else expr2
  InsertFunction(
      functions, options, "ifnull", SCALAR,
      {{ARG_TYPE_ANY_1, {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_IFNULL}});

  // NULLIF(expr1, expr2): NULL if expr1 = expr2, otherwise returns expr1.
  InsertFunction(
      functions, options, "nullif", SCALAR,
      {{ARG_TYPE_ANY_1, {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_NULLIF}},
      FunctionOptions().set_post_resolution_argument_constraint(
          bind_front(&CheckArgumentsSupportEquality, "NULLIF")));

  // ARRAY_LENGTH(expr1): returns the length of the array
  InsertFunction(functions, options, "array_length", SCALAR,
                 {{int64_type, {ARG_ARRAY_TYPE_ANY_1}, FN_ARRAY_LENGTH}});

  // array[OFFSET(i)] gets an array element by zero-based position.
  // array[ORDINAL(i)] gets an array element by one-based position.
  // If the array or offset is NULL, a NULL of the array element type is
  // returned. If the position is off either end of the array a OUT_OF_RANGE
  // error is returned. The SAFE_ variants of the functions have the same
  // semantics with the exception of returning NULL rather than OUT_OF_RANGE
  // for a position that is out of bounds.
  InsertFunction(functions, options, "$array_at_offset", SCALAR,
                 {{ARG_TYPE_ANY_1,
                   {ARG_ARRAY_TYPE_ANY_1, int64_type},
                   FN_ARRAY_AT_OFFSET}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_sql_name("array[offset()]")
                     .set_get_sql_callback(&ArrayAtOffsetFunctionSQL));
  InsertFunction(functions, options, "$array_at_ordinal", SCALAR,
                 {{ARG_TYPE_ANY_1,
                   {ARG_ARRAY_TYPE_ANY_1, int64_type},
                   FN_ARRAY_AT_ORDINAL}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_sql_name("array[ordinal()]")
                     .set_get_sql_callback(&ArrayAtOrdinalFunctionSQL));
  InsertFunction(functions, options, "$safe_array_at_offset", SCALAR,
                 {{ARG_TYPE_ANY_1,
                   {ARG_ARRAY_TYPE_ANY_1, int64_type},
                   FN_SAFE_ARRAY_AT_OFFSET}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_sql_name("array[safe_offset()]")
                     .set_get_sql_callback(&SafeArrayAtOffsetFunctionSQL));
  InsertFunction(functions, options, "$safe_array_at_ordinal", SCALAR,
                 {{ARG_TYPE_ANY_1,
                   {ARG_ARRAY_TYPE_ANY_1, int64_type},
                   FN_SAFE_ARRAY_AT_ORDINAL}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_sql_name("array[safe_ordinal()]")
                     .set_get_sql_callback(&SafeArrayAtOrdinalFunctionSQL));

  // Usage: [...], ARRAY[...], ARRAY<T>[...]
  // * Array elements would be the list of expressions enclosed within [].
  // * T (if mentioned) would define the array element type. Otherwise the
  //   common supertype among all the elements would define the element type.
  // * All element types when not equal should implicitly coerce to the defined
  //   element type.
  InsertFunction(
      functions, options, "$make_array", SCALAR,
      {{ARG_ARRAY_TYPE_ANY_1, {{ARG_TYPE_ANY_1, REPEATED}}, FN_MAKE_ARRAY}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("array[...]")
          .set_get_sql_callback(&MakeArrayFunctionSQL));

  // ARRAY_CONCAT(repeated array): returns the concatenation of the input
  // arrays.
  InsertFunction(functions, options, "array_concat", SCALAR,
                 {{ARG_ARRAY_TYPE_ANY_1,
                   {ARG_ARRAY_TYPE_ANY_1, {ARG_ARRAY_TYPE_ANY_1, REPEATED}},
                   FN_ARRAY_CONCAT}},
                 FunctionOptions().set_pre_resolution_argument_constraint(
                     &CheckArrayConcatArguments));

  // $concat_op ("||"：CONCAT/ARRAY_CONCAT): returns the concatenation of
  // the inputs.
  // This function and its signatures are only used during internal resolution,
  // and that the canonical representations in the ResolvedAST are the
  // CONCAT/ARRAY_CONCAT function calls based on the types of the arguments.
  FunctionArgumentTypeOptions concat_option;
  if (options.language_options.LanguageFeatureEnabled(
          zetasql::FEATURE_V_1_3_CONCAT_MIXED_TYPES)) {
    concat_option.set_allow_coercion_from(&CanStringConcatCoerceFrom);
  }

  InsertFunction(
      functions, options, "$concat_op", SCALAR,
      {{string_type,
        {{string_type, concat_option}, {string_type, concat_option}},
        FN_CONCAT_OP_STRING},
       {bytes_type, {bytes_type, bytes_type}, FN_CONCAT_OP_BYTES},
       {ARG_ARRAY_TYPE_ANY_1,
        {ARG_ARRAY_TYPE_ANY_1, ARG_ARRAY_TYPE_ANY_1},
        FN_ARRAY_CONCAT_OP}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("||")
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "||")));

  // ARRAY_TO_STRING: returns concatentation of elements of the input array.
  InsertFunction(functions, options, "array_to_string", SCALAR,
                 {{string_type,
                   {array_string_type, string_type, {string_type, OPTIONAL}},
                   FN_ARRAY_TO_STRING},
                  {bytes_type,
                   {array_bytes_type, bytes_type, {bytes_type, OPTIONAL}},
                   FN_ARRAY_TO_BYTES}});

  // ARRAY_REVERSE: returns the input array with its elements in reverse order.
  InsertFunction(
      functions, options, "array_reverse", SCALAR,
      {{ARG_ARRAY_TYPE_ANY_1, {ARG_ARRAY_TYPE_ANY_1}, FN_ARRAY_REVERSE}});

  // RANGE_BUCKET: returns the bucket of the item in the array.
  InsertFunction(
      functions, options, "range_bucket", SCALAR,
      {{int64_type, {ARG_TYPE_ANY_1, ARG_ARRAY_TYPE_ANY_1}, FN_RANGE_BUCKET}},
      FunctionOptions().set_pre_resolution_argument_constraint(
          &CheckRangeBucketArguments));

  // From the SQL language perspective, the ELSE clause is optional for both
  // CASE statement signatures.  However, the parser will normalize the
  // CASE expressions so they always have the ELSE, and therefore it is defined
  // here as a required argument in the function signatures.
  InsertFunction(
      functions, options, "$case_with_value", SCALAR,
      {{ARG_TYPE_ANY_1,
        {ARG_TYPE_ANY_2,
         {ARG_TYPE_ANY_2, REPEATED},
         {ARG_TYPE_ANY_1, REPEATED},
         {ARG_TYPE_ANY_1}},
        FN_CASE_WITH_VALUE}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("case")
          .set_supported_signatures_callback(&EmptySupportedSignatures)
          .set_get_sql_callback(&CaseWithValueFunctionSQL)
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckFirstArgumentSupportsEquality,
                         "CASE (with value comparison)")));

  InsertFunction(
      functions, options, "$case_no_value", SCALAR,
      {{ARG_TYPE_ANY_1,
        {{bool_type, REPEATED}, {ARG_TYPE_ANY_1, REPEATED}, {ARG_TYPE_ANY_1}},
        FN_CASE_NO_VALUE}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("case")
          .set_supported_signatures_callback(&EmptySupportedSignatures)
          .set_get_sql_callback(&CaseNoValueFunctionSQL)
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForCaseNoValueFunction));

  InsertFunction(functions, options, "bit_cast_to_int32", SCALAR,
                 {{int32_type, {int32_type}, FN_BIT_CAST_INT32_TO_INT32},
                  {int32_type, {uint32_type}, FN_BIT_CAST_UINT32_TO_INT32}},
                 FunctionOptions().set_allow_external_usage(false));
  InsertFunction(functions, options, "bit_cast_to_int64", SCALAR,
                 {{int64_type, {int64_type}, FN_BIT_CAST_INT64_TO_INT64},
                  {int64_type, {uint64_type}, FN_BIT_CAST_UINT64_TO_INT64}},
                 FunctionOptions().set_allow_external_usage(false));
  InsertFunction(functions, options, "bit_cast_to_uint32", SCALAR,
                 {{uint32_type, {uint32_type}, FN_BIT_CAST_UINT32_TO_UINT32},
                  {uint32_type, {int32_type}, FN_BIT_CAST_INT32_TO_UINT32}},
                 FunctionOptions().set_allow_external_usage(false));
  InsertFunction(functions, options, "bit_cast_to_uint64", SCALAR,
                 {{uint64_type, {uint64_type}, FN_BIT_CAST_UINT64_TO_UINT64},
                  {uint64_type, {int64_type}, FN_BIT_CAST_INT64_TO_UINT64}},
                 FunctionOptions().set_allow_external_usage(false));

  FunctionOptions function_is_stable;
  function_is_stable.set_volatility(FunctionEnums::STABLE);

  InsertFunction(functions, options, "session_user", SCALAR,
                 {{string_type, {}, FN_SESSION_USER}}, function_is_stable);

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
  InsertFunction(
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
          .set_get_sql_callback(bind_front(
              &GenerateDateTimestampArrayFunctionSQL, "GENERATE_DATE_ARRAY")));
  InsertFunction(
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
              bind_front(&GenerateDateTimestampArrayFunctionSQL,
                         "GENERATE_TIMESTAMP_ARRAY")));

  FunctionOptions function_is_volatile;
  function_is_volatile.set_volatility(FunctionEnums::VOLATILE);

  InsertFunction(functions, options, "rand", SCALAR,
                 {{double_type, {}, FN_RAND}}, function_is_volatile);

  InsertFunction(functions, options, "generate_uuid", SCALAR,
                 {{string_type, {}, FN_GENERATE_UUID}}, function_is_volatile);

  InsertFunction(functions, options, "json_extract", SCALAR,
                 {{string_type, {string_type, string_type}, FN_JSON_EXTRACT}},
                 FunctionOptions().set_pre_resolution_argument_constraint(
                     &CheckJsonArguments));
  InsertFunction(functions, options, "json_query", SCALAR,
                 {{string_type, {string_type, string_type}, FN_JSON_QUERY}},
                 FunctionOptions().set_pre_resolution_argument_constraint(
                     &CheckJsonArguments));

  InsertFunction(
      functions, options, "json_extract_scalar", SCALAR,
      {{string_type, {string_type, string_type}, FN_JSON_EXTRACT_SCALAR}},
      FunctionOptions().set_pre_resolution_argument_constraint(
          &CheckJsonArguments));
  InsertFunction(functions, options, "json_value", SCALAR,
                 {{string_type, {string_type, string_type}, FN_JSON_VALUE}},
                 FunctionOptions().set_pre_resolution_argument_constraint(
                     &CheckJsonArguments));
  InsertFunction(functions, options, "json_extract_array", SCALAR,
                 {{array_string_type,
                   {string_type, {string_type, OPTIONAL}},
                   FN_JSON_EXTRACT_ARRAY}},
                 FunctionOptions().set_pre_resolution_argument_constraint(
                     &CheckJsonArguments));

  InsertFunction(functions, options, "to_json_string", SCALAR,
                 {{string_type,
                   {ARG_TYPE_ANY_1, {bool_type, OPTIONAL}},
                   FN_TO_JSON_STRING}});

  // The signature is declared as
  //   ERROR(string) -> int64_t
  // but this is special-cased in the resolver so that the result can be
  // coerced to anything, similar to untyped NULL.  This allows using this
  // in expressions like IF(<condition>, <value>, ERROR("message"))
  // for any value type.  It would be preferable to declare this with an
  // undefined or templated return type, but that is not allowed.
  InsertFunction(functions, options, "error", SCALAR,
                 {{int64_type, {string_type}, FN_ERROR}});

  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_3_PROTO_DEFAULT_IF_NULL)) {
    // This signature is declared as taking input of any type, however it
    // actually takes input of all non-Proto types.
    //
    // This is not a regular function, as it does not get resolved to a
    // function call. It essentially acts as wrapper on a normal field access.
    // When this function is encountered in the resolver, we ensure the input is
    // a valid field access and return a ResolvedGetProtoField, but with its
    // <return_default_value_when_unset> field set to true.
    //
    // This is added to the catalog to prevent collisions if a similar function
    // is defined by an engine. Also, it allows us to use
    // FunctionSignatureOptions to define constraints and deprecation info for
    // this special function.
    InsertFunction(
        functions, options, "proto_default_if_null", SCALAR,
        {{ARG_TYPE_ANY_1, {ARG_TYPE_ANY_1}, FN_PROTO_DEFAULT_IF_NULL}},
        FunctionOptions().set_allow_external_usage(false));
  }

  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_3_ENUM_VALUE_DESCRIPTOR_PROTO)) {
    const Type* enum_value_descriptor_proto_type = nullptr;
    ZETASQL_CHECK_OK(type_factory->MakeProtoType(
        google::protobuf::EnumValueDescriptorProto::descriptor(),
        &enum_value_descriptor_proto_type));
    // ENUM_VALUE_DESCRIPTOR_PROTO(ENUM): Returns the
    // google::protobuf::EnumValueDescriptorProto corresponding to the input Enum value.
    InsertFunction(functions, options, "enum_value_descriptor_proto", SCALAR,
                   {{enum_value_descriptor_proto_type,
                     {ARG_ENUM_ANY},
                     FN_ENUM_VALUE_DESCRIPTOR_PROTO}},
                   FunctionOptions().set_compute_result_type_callback(
                       &GetOrMakeEnumValueDescriptorType));
  }
}

void GetNumericFunctions(TypeFactory* type_factory,
                         const ZetaSQLBuiltinFunctionOptions& options,
                         NameToFunctionMap* functions) {
  const Type* bool_type = type_factory->get_bool();
  const Type* int32_type = type_factory->get_int32();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint32_type = type_factory->get_uint32();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* float_type = type_factory->get_float();
  const Type* double_type = type_factory->get_double();
  const Type* numeric_type = type_factory->get_numeric();
  const Type* bignumeric_type = type_factory->get_bignumeric();

  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality REPEATED =
      FunctionArgumentType::REPEATED;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  FunctionSignatureOptions has_floating_point_argument;
  has_floating_point_argument.set_constraints(&HasFloatingPointArgument);
  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&HasNumericTypeArgument);
  FunctionSignatureOptions has_bignumeric_type_argument;
  has_bignumeric_type_argument.set_constraints(&HasBigNumericTypeArgument);

  InsertFunction(functions, options, "abs", SCALAR,
                 {{int32_type, {int32_type}, FN_ABS_INT32},
                  {int64_type, {int64_type}, FN_ABS_INT64},
                  {uint32_type, {uint32_type}, FN_ABS_UINT32},
                  {uint64_type, {uint64_type}, FN_ABS_UINT64},
                  {float_type, {float_type}, FN_ABS_FLOAT},
                  {double_type, {double_type}, FN_ABS_DOUBLE},
                  {numeric_type, {numeric_type}, FN_ABS_NUMERIC},
                  {bignumeric_type, {bignumeric_type}, FN_ABS_BIGNUMERIC}});

  InsertFunction(functions, options, "sign", SCALAR,
                 {{int32_type, {int32_type}, FN_SIGN_INT32},
                  {int64_type, {int64_type}, FN_SIGN_INT64},
                  {uint32_type, {uint32_type}, FN_SIGN_UINT32},
                  {uint64_type, {uint64_type}, FN_SIGN_UINT64},
                  {float_type, {float_type}, FN_SIGN_FLOAT},
                  {double_type, {double_type}, FN_SIGN_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_SIGN_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type},
                   FN_SIGN_BIGNUMERIC,
                   has_bignumeric_type_argument}});

  InsertFunction(
      functions, options, "round", SCALAR,
      {{float_type, {float_type}, FN_ROUND_FLOAT},
       {double_type, {double_type}, FN_ROUND_DOUBLE},
       {numeric_type,
        {numeric_type},
        FN_ROUND_NUMERIC,
        has_numeric_type_argument},
       {bignumeric_type,
        {bignumeric_type},
        FN_ROUND_BIGNUMERIC,
        has_bignumeric_type_argument},
       {float_type, {float_type, int64_type}, FN_ROUND_WITH_DIGITS_FLOAT},
       {double_type, {double_type, int64_type}, FN_ROUND_WITH_DIGITS_DOUBLE},
       {numeric_type,
        {numeric_type, int64_type},
        FN_ROUND_WITH_DIGITS_NUMERIC,
        has_numeric_type_argument},
       {bignumeric_type,
        {bignumeric_type, int64_type},
        FN_ROUND_WITH_DIGITS_BIGNUMERIC,
        has_bignumeric_type_argument}});
  InsertFunction(
      functions, options, "trunc", SCALAR,
      {{float_type, {float_type}, FN_TRUNC_FLOAT},
       {double_type, {double_type}, FN_TRUNC_DOUBLE},
       {numeric_type,
        {numeric_type},
        FN_TRUNC_NUMERIC,
        has_numeric_type_argument},
       {bignumeric_type,
        {bignumeric_type},
        FN_TRUNC_BIGNUMERIC,
        has_bignumeric_type_argument},
       {float_type, {float_type, int64_type}, FN_TRUNC_WITH_DIGITS_FLOAT},
       {double_type, {double_type, int64_type}, FN_TRUNC_WITH_DIGITS_DOUBLE},
       {numeric_type,
        {numeric_type, int64_type},
        FN_TRUNC_WITH_DIGITS_NUMERIC,
        has_numeric_type_argument},
       {bignumeric_type,
        {bignumeric_type, int64_type},
        FN_TRUNC_WITH_DIGITS_BIGNUMERIC,
        has_bignumeric_type_argument}});
  InsertFunction(functions, options, "ceil", SCALAR,
                 {{float_type, {float_type}, FN_CEIL_FLOAT},
                  {double_type, {double_type}, FN_CEIL_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_CEIL_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type},
                   FN_CEIL_BIGNUMERIC,
                   has_bignumeric_type_argument}},
                 FunctionOptions().set_alias_name("ceiling"));
  InsertFunction(functions, options, "floor", SCALAR,
                 {{float_type, {float_type}, FN_FLOOR_FLOAT},
                  {double_type, {double_type}, FN_FLOOR_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_FLOOR_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type},
                   FN_FLOOR_BIGNUMERIC,
                   has_bignumeric_type_argument}});

  InsertFunction(functions, options, "is_inf", SCALAR,
                 {{bool_type, {double_type}, FN_IS_INF}});
  InsertFunction(functions, options, "is_nan", SCALAR,
                 {{bool_type, {double_type}, FN_IS_NAN}});

  InsertFunction(
      functions, options, "ieee_divide", SCALAR,
      {{double_type, {double_type, double_type}, FN_IEEE_DIVIDE_DOUBLE},
       {float_type, {float_type, float_type}, FN_IEEE_DIVIDE_FLOAT}});

  InsertFunction(
      functions, options, "greatest", SCALAR,
      {{ARG_TYPE_ANY_1, {{ARG_TYPE_ANY_1, REPEATED}}, FN_GREATEST}},
      FunctionOptions().set_pre_resolution_argument_constraint(
          bind_front(&CheckMinMaxGreatestLeastArguments, "GREATEST")));

  InsertFunction(functions, options, "least", SCALAR,
                 {{ARG_TYPE_ANY_1, {{ARG_TYPE_ANY_1, REPEATED}}, FN_LEAST}},
                 FunctionOptions().set_pre_resolution_argument_constraint(
                     bind_front(&CheckMinMaxGreatestLeastArguments, "LEAST")));

  InsertFunction(functions, options, "mod", SCALAR,
                 {{int64_type, {int64_type, int64_type}, FN_MOD_INT64},
                  {uint64_type, {uint64_type, uint64_type}, FN_MOD_UINT64},
                  {numeric_type,
                   {numeric_type, numeric_type},
                   FN_MOD_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type, bignumeric_type},
                   FN_MOD_BIGNUMERIC,
                   has_bignumeric_type_argument}});

  InsertFunction(functions, options, "div", SCALAR,
                 {{int64_type, {int64_type, int64_type}, FN_DIV_INT64},
                  {uint64_type, {uint64_type, uint64_type}, FN_DIV_UINT64},
                  {numeric_type,
                   {numeric_type, numeric_type},
                   FN_DIV_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type, bignumeric_type},
                   FN_DIV_BIGNUMERIC,
                   has_bignumeric_type_argument}});

  // The SAFE versions of arithmetic operators (+, -, *, /, <unary minus>) have
  // the same signatures as the operators themselves.
  InsertFunction(functions, options, "safe_add", SCALAR,
                 {{int64_type, {int64_type, int64_type}, FN_SAFE_ADD_INT64},
                  {uint64_type, {uint64_type, uint64_type}, FN_SAFE_ADD_UINT64},
                  {double_type,
                   {double_type, double_type},
                   FN_SAFE_ADD_DOUBLE,
                   has_floating_point_argument},
                  {numeric_type,
                   {numeric_type, numeric_type},
                   FN_SAFE_ADD_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type, bignumeric_type},
                   FN_SAFE_ADD_BIGNUMERIC,
                   has_bignumeric_type_argument}});

  InsertFunction(
      functions, options, "safe_subtract", SCALAR,
      {{int64_type, {int64_type, int64_type}, FN_SAFE_SUBTRACT_INT64},
       {int64_type, {uint64_type, uint64_type}, FN_SAFE_SUBTRACT_UINT64},
       {numeric_type,
        {numeric_type, numeric_type},
        FN_SAFE_SUBTRACT_NUMERIC,
        has_numeric_type_argument},
       {bignumeric_type,
        {bignumeric_type, bignumeric_type},
        FN_SAFE_SUBTRACT_BIGNUMERIC,
        has_bignumeric_type_argument},
       {double_type,
        {double_type, double_type},
        FN_SAFE_SUBTRACT_DOUBLE,
        has_floating_point_argument}});

  InsertFunction(
      functions, options, "safe_multiply", SCALAR,
      {{int64_type, {int64_type, int64_type}, FN_SAFE_MULTIPLY_INT64},
       {uint64_type, {uint64_type, uint64_type}, FN_SAFE_MULTIPLY_UINT64},
       {double_type,
        {double_type, double_type},
        FN_SAFE_MULTIPLY_DOUBLE,
        has_floating_point_argument},
       {numeric_type,
        {numeric_type, numeric_type},
        FN_SAFE_MULTIPLY_NUMERIC,
        has_numeric_type_argument},
       {bignumeric_type,
        {bignumeric_type, bignumeric_type},
        FN_SAFE_MULTIPLY_BIGNUMERIC,
        has_bignumeric_type_argument}});

  InsertFunction(
      functions, options, "safe_divide", SCALAR,
      {{double_type, {double_type, double_type}, FN_SAFE_DIVIDE_DOUBLE},
       {numeric_type,
        {numeric_type, numeric_type},
        FN_SAFE_DIVIDE_NUMERIC,
        has_numeric_type_argument},
       {bignumeric_type,
        {bignumeric_type, bignumeric_type},
        FN_SAFE_DIVIDE_BIGNUMERIC,
        has_bignumeric_type_argument}});

  InsertFunction(functions, options, "safe_negate", SCALAR,
                 {{int32_type, {int32_type}, FN_SAFE_UNARY_MINUS_INT32},
                  {int64_type, {int64_type}, FN_SAFE_UNARY_MINUS_INT64},
                  {float_type, {float_type}, FN_SAFE_UNARY_MINUS_FLOAT},
                  {double_type, {double_type}, FN_SAFE_UNARY_MINUS_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_SAFE_UNARY_MINUS_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type},
                   FN_SAFE_UNARY_MINUS_BIGNUMERIC,
                   has_bignumeric_type_argument}},
                 FunctionOptions().set_arguments_are_coercible(false));

  InsertFunction(functions, options, "sqrt", SCALAR,
                 {{double_type, {double_type}, FN_SQRT_DOUBLE}});
  InsertFunction(functions, options, "pow", SCALAR,
                 {{double_type, {double_type, double_type}, FN_POW_DOUBLE},
                  {numeric_type,
                   {numeric_type, numeric_type},
                   FN_POW_NUMERIC,
                   has_numeric_type_argument}},
                 FunctionOptions().set_alias_name("power"));
  InsertFunction(functions, options, "exp", SCALAR,
                 {{double_type, {double_type}, FN_EXP_DOUBLE}});
  InsertFunction(functions, options, "ln", SCALAR,
                 {{double_type, {double_type}, FN_NATURAL_LOGARITHM_DOUBLE}});
  InsertFunction(functions, options, "log", SCALAR,
                 {{double_type,
                   {double_type, {double_type, OPTIONAL}},
                   FN_LOGARITHM_DOUBLE}});
  InsertFunction(functions, options, "log10", SCALAR,
                 {{double_type, {double_type}, FN_DECIMAL_LOGARITHM_DOUBLE}});
}

void GetTrigonometricFunctions(TypeFactory* type_factory,
                               const ZetaSQLBuiltinFunctionOptions& options,
                               NameToFunctionMap* functions) {
  const Type* double_type = type_factory->get_double();

  const Function::Mode SCALAR = Function::SCALAR;

  InsertFunction(functions, options, "cos", SCALAR,
                 {{double_type, {double_type}, FN_COS_DOUBLE}});
  InsertFunction(functions, options, "cosh", SCALAR,
                 {{double_type, {double_type}, FN_COSH_DOUBLE}});
  InsertFunction(functions, options, "acos", SCALAR,
                 {{double_type, {double_type}, FN_ACOS_DOUBLE}});
  InsertFunction(functions, options, "acosh", SCALAR,
                 {{double_type, {double_type}, FN_ACOSH_DOUBLE}});
  InsertFunction(functions, options, "sin", SCALAR,
                 {{double_type, {double_type}, FN_SIN_DOUBLE}});
  InsertFunction(functions, options, "sinh", SCALAR,
                 {{double_type, {double_type}, FN_SINH_DOUBLE}});
  InsertFunction(functions, options, "asin", SCALAR,
                 {{double_type, {double_type}, FN_ASIN_DOUBLE}});
  InsertFunction(functions, options, "asinh", SCALAR,
                 {{double_type, {double_type}, FN_ASINH_DOUBLE}});
  InsertFunction(functions, options, "tan", SCALAR,
                 {{double_type, {double_type}, FN_TAN_DOUBLE}});
  InsertFunction(functions, options, "tanh", SCALAR,
                 {{double_type, {double_type}, FN_TANH_DOUBLE}});
  InsertFunction(functions, options, "atan", SCALAR,
                 {{double_type, {double_type}, FN_ATAN_DOUBLE}});
  InsertFunction(functions, options, "atanh", SCALAR,
                 {{double_type, {double_type}, FN_ATANH_DOUBLE}});
  InsertFunction(functions, options, "atan2", SCALAR,
                 {{double_type, {double_type, double_type}, FN_ATAN2_DOUBLE}});
}

void GetMathFunctions(TypeFactory* type_factory,

                      const ZetaSQLBuiltinFunctionOptions& options,
                      NameToFunctionMap* functions) {
  GetNumericFunctions(type_factory, options, functions);
  GetTrigonometricFunctions(type_factory, options, functions);
}

void GetNetFunctions(TypeFactory* type_factory,
                     const ZetaSQLBuiltinFunctionOptions& options,
                     NameToFunctionMap* functions) {
  const Type* bool_type = type_factory->get_bool();
  const Type* bytes_type = type_factory->get_bytes();
  const Type* int32_type = type_factory->get_int32();
  const Type* int64_type = type_factory->get_int64();
  const Type* string_type = type_factory->get_string();

  const Function::Mode SCALAR = Function::SCALAR;

  InsertNamespaceFunction(functions, options, "net", "format_ip", SCALAR,
                          {{string_type, {int64_type}, FN_NET_FORMAT_IP}},
                          FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "net", "parse_ip", SCALAR,
                          {{int64_type, {string_type}, FN_NET_PARSE_IP}},
                          FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(
      functions, options, "net", "format_packed_ip", SCALAR,
      {{string_type, {bytes_type}, FN_NET_FORMAT_PACKED_IP}},
      FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "net", "parse_packed_ip", SCALAR,
                          {{bytes_type, {string_type}, FN_NET_PARSE_PACKED_IP}},
                          FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(
      functions, options, "net", "ip_in_net", SCALAR,
      {{bool_type, {string_type, string_type}, FN_NET_IP_IN_NET}},
      FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(
      functions, options, "net", "make_net", SCALAR,
      {{string_type, {string_type, int32_type}, FN_NET_MAKE_NET}},
      FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "net", "host", SCALAR,
                          {{string_type, {string_type}, FN_NET_HOST}});
  InsertNamespaceFunction(functions, options, "net", "reg_domain", SCALAR,
                          {{string_type, {string_type}, FN_NET_REG_DOMAIN}});
  InsertNamespaceFunction(functions, options, "net", "public_suffix", SCALAR,
                          {{string_type, {string_type}, FN_NET_PUBLIC_SUFFIX}});
  InsertNamespaceFunction(functions, options, "net", "ip_from_string", SCALAR,
                          {{bytes_type, {string_type}, FN_NET_IP_FROM_STRING}});
  InsertNamespaceFunction(
      functions, options, "net", "safe_ip_from_string", SCALAR,
      {{bytes_type, {string_type}, FN_NET_SAFE_IP_FROM_STRING}});
  InsertNamespaceFunction(functions, options, "net", "ip_to_string", SCALAR,
                          {{string_type, {bytes_type}, FN_NET_IP_TO_STRING}});
  InsertNamespaceFunction(
      functions, options, "net", "ip_net_mask", SCALAR,
      {{bytes_type, {int64_type, int64_type}, FN_NET_IP_NET_MASK}});
  InsertNamespaceFunction(
      functions, options, "net", "ip_trunc", SCALAR,
      {{bytes_type, {bytes_type, int64_type}, FN_NET_IP_TRUNC}});
  InsertNamespaceFunction(functions, options, "net", "ipv4_from_int64", SCALAR,
                          {{bytes_type, {int64_type}, FN_NET_IPV4_FROM_INT64}});
  InsertNamespaceFunction(functions, options, "net", "ipv4_to_int64", SCALAR,
                          {{int64_type, {bytes_type}, FN_NET_IPV4_TO_INT64}});
}

void GetHllCountFunctions(TypeFactory* type_factory,

                          const ZetaSQLBuiltinFunctionOptions& options,
                          NameToFunctionMap* functions) {
  const Type* bytes_type = type_factory->get_bytes();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* string_type = type_factory->get_string();
  const Type* numeric_type = type_factory->get_numeric();

  const Function::Mode AGGREGATE = Function::AGGREGATE;
  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&HasNumericTypeArgument);

  // The second argument must be an integer literal between 10 and 24,
  // and cannot be NULL.
  FunctionArgumentTypeOptions hll_init_arg;
  hll_init_arg.set_is_not_aggregate();
  hll_init_arg.set_must_be_non_null();
  hll_init_arg.set_cardinality(OPTIONAL);
  hll_init_arg.set_min_value(10);
  hll_init_arg.set_max_value(24);

  InsertNamespaceFunction(functions, options, "hll_count", "merge", AGGREGATE,
                          {{int64_type, {bytes_type}, FN_HLL_COUNT_MERGE}});
  InsertNamespaceFunction(functions, options, "hll_count", "extract", SCALAR,
                          {{int64_type, {bytes_type}, FN_HLL_COUNT_EXTRACT}});
  InsertNamespaceFunction(functions, options, "hll_count", "init", AGGREGATE,
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
                            {string_type, {int64_type, hll_init_arg}},
                            FN_HLL_COUNT_INIT_STRING},
                           {bytes_type,
                            {bytes_type, {int64_type, hll_init_arg}},
                            FN_HLL_COUNT_INIT_BYTES}});
  InsertNamespaceFunction(
      functions, options, "hll_count", "merge_partial", AGGREGATE,
      {{bytes_type, {bytes_type}, FN_HLL_COUNT_MERGE_PARTIAL}});
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

  const Function::Mode AGGREGATE = Function::AGGREGATE;
  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  // The optional second argument of 'init', the approximation precision or
  // inverse epsilon ('inv_eps'), must be an integer >= 2 and cannot be NULL.
  FunctionArgumentTypeOptions init_inv_eps_arg;
  init_inv_eps_arg.set_is_not_aggregate();
  init_inv_eps_arg.set_must_be_non_null();
  init_inv_eps_arg.set_cardinality(OPTIONAL);
  init_inv_eps_arg.set_min_value(2);

  // Init
  InsertNamespaceFunction(functions, options, "kll_quantiles", "init_int64",
                          AGGREGATE,
                          {{bytes_type,
                            {int64_type, {int64_type, init_inv_eps_arg}},
                            FN_KLL_QUANTILES_INIT_INT64}},
                          FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles", "init_uint64",
                          AGGREGATE,
                          {{bytes_type,
                            {uint64_type, {int64_type, init_inv_eps_arg}},
                            FN_KLL_QUANTILES_INIT_UINT64}},
                          FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles", "init_double",
                          AGGREGATE,
                          {{bytes_type,
                            {double_type, {int64_type, init_inv_eps_arg}},
                            FN_KLL_QUANTILES_INIT_DOUBLE}},
                          FunctionOptions().set_allow_external_usage(false));

  // Merge_partial
  InsertNamespaceFunction(
      functions, options, "kll_quantiles", "merge_partial", AGGREGATE,
      {{bytes_type, {bytes_type}, FN_KLL_QUANTILES_MERGE_PARTIAL}},
      FunctionOptions().set_allow_external_usage(false));

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
        // TODO: Add support for interpolation option for all merge/
        // extract/merge_point/extract_point functions.
        {bytes_type, {int64_type, num_quantiles_merge_arg}},
        FN_KLL_QUANTILES_MERGE_INT64}},
      FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles", "merge_uint64",
                          AGGREGATE,
                          {{uint64_array_type,
                            {bytes_type, {int64_type, num_quantiles_merge_arg}},
                            FN_KLL_QUANTILES_MERGE_UINT64}},
                          FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles", "merge_double",
                          AGGREGATE,
                          {{double_array_type,
                            {bytes_type, {int64_type, num_quantiles_merge_arg}},
                            FN_KLL_QUANTILES_MERGE_DOUBLE}},
                          FunctionOptions().set_allow_external_usage(false));

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
  InsertNamespaceFunction(functions, options, "kll_quantiles",
                          "merge_point_int64", AGGREGATE,
                          {{int64_type,
                            {bytes_type, {double_type, phi_merge_arg}},
                            FN_KLL_QUANTILES_MERGE_POINT_INT64}},
                          FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles",
                          "merge_point_uint64", AGGREGATE,
                          {{uint64_type,
                            {bytes_type, {double_type, phi_merge_arg}},
                            FN_KLL_QUANTILES_MERGE_POINT_UINT64}},
                          FunctionOptions().set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles",
                          "merge_point_double", AGGREGATE,
                          {{double_type,
                            {bytes_type, {double_type, phi_merge_arg}},
                            FN_KLL_QUANTILES_MERGE_POINT_DOUBLE}},
                          FunctionOptions().set_allow_external_usage(false));

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

void GetHashingFunctions(TypeFactory* type_factory,
                         const ZetaSQLBuiltinFunctionOptions& options,
                         NameToFunctionMap* functions) {
  const Function::Mode SCALAR = Function::SCALAR;
  const Type* int64_type = types::Int64Type();
  const Type* string_type = types::StringType();
  const Type* bytes_type = types::BytesType();

  InsertFunction(functions, options, "md5", SCALAR,
                 {{bytes_type, {bytes_type}, FN_MD5_BYTES},
                  {bytes_type, {string_type}, FN_MD5_STRING}});
  InsertFunction(functions, options, "sha1", SCALAR,
                 {{bytes_type, {bytes_type}, FN_SHA1_BYTES},
                  {bytes_type, {string_type}, FN_SHA1_STRING}});
  InsertFunction(functions, options, "sha256", SCALAR,
                 {{bytes_type, {bytes_type}, FN_SHA256_BYTES},
                  {bytes_type, {string_type}, FN_SHA256_STRING}});
  InsertFunction(functions, options, "sha512", SCALAR,
                 {{bytes_type, {bytes_type}, FN_SHA512_BYTES},
                  {bytes_type, {string_type}, FN_SHA512_STRING}});

  InsertFunction(functions, options, "farm_fingerprint", SCALAR,
                 {{int64_type, {bytes_type}, FN_FARM_FINGERPRINT_BYTES},
                  {int64_type, {string_type}, FN_FARM_FINGERPRINT_STRING}});
}

void GetEncryptionFunctions(TypeFactory* type_factory,
                            const ZetaSQLBuiltinFunctionOptions& options,
                            NameToFunctionMap* functions) {
  const Function::Mode SCALAR = Function::SCALAR;
  const Type* string_type = types::StringType();
  const Type* bytes_type = types::BytesType();
  const Type* int64_type = types::Int64Type();

  const FunctionOptions encryption_required =
      FunctionOptions().add_required_language_feature(FEATURE_ENCRYPTION);

  // KEYS.NEW_KEYSET is volatile since it generates a random key for each
  // invocation.
  InsertNamespaceFunction(
      functions, options, "keys", "new_keyset", SCALAR,
      {{bytes_type, {string_type}, FN_KEYS_NEW_KEYSET}},
      FunctionOptions(encryption_required)
          .set_volatility(FunctionEnums::VOLATILE)
          .set_pre_resolution_argument_constraint(bind_front(
              &CheckIsSupportedKeyType, "KEYS.NEW_KEYSET",
              GetSupportedKeyTypes(), /*key_type_argument_index=*/0)));

  InsertNamespaceFunction(
      functions, options, "keys", "add_key_from_raw_bytes", SCALAR,
      {{bytes_type,
        {bytes_type, string_type, bytes_type},
        FN_KEYS_ADD_KEY_FROM_RAW_BYTES}},
      FunctionOptions(encryption_required)
          .set_pre_resolution_argument_constraint(bind_front(
              &CheckIsSupportedKeyType, "KEYS.ADD_KEY_FROM_RAW_BYTES",
              GetSupportedRawKeyTypes(), /*key_type_argument_index=*/1)));

  // KEYS.ROTATE_KEYSET is volatile since it generates a random key for each
  // invocation.
  InsertNamespaceFunction(
      functions, options, "keys", "rotate_keyset", SCALAR,
      {{bytes_type, {bytes_type, string_type}, FN_KEYS_ROTATE_KEYSET}},
      FunctionOptions(encryption_required)
          .set_volatility(FunctionEnums::VOLATILE)
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckIsSupportedKeyType, "KEYS.ROTATE_KEYSET",
                         GetSupportedKeyTypes(),
                         /*key_type_argument_index=*/1)));

  InsertNamespaceFunction(functions, options, "keys", "keyset_length", SCALAR,
                          {{int64_type, {bytes_type}, FN_KEYS_KEYSET_LENGTH}},
                          encryption_required);
  InsertNamespaceFunction(functions, options, "keys", "keyset_to_json", SCALAR,
                          {{string_type, {bytes_type}, FN_KEYS_KEYSET_TO_JSON}},
                          encryption_required);
  InsertNamespaceFunction(
      functions, options, "keys", "keyset_from_json", SCALAR,
      {{bytes_type, {string_type}, FN_KEYS_KEYSET_FROM_JSON}},
      encryption_required);

  // AEAD.ENCRYPT is volatile since it generates a random IV (initialization
  // vector) for each invocation so that encrypting the same plaintext results
  // in different ciphertext.
  InsertNamespaceFunction(functions, options, "aead", "encrypt", SCALAR,
                          {{bytes_type,
                            {bytes_type, string_type, string_type},
                            FN_AEAD_ENCRYPT_STRING},
                           {bytes_type,
                            {bytes_type, bytes_type, bytes_type},
                            FN_AEAD_ENCRYPT_BYTES}},
                          FunctionOptions(encryption_required)
                              .set_volatility(FunctionEnums::VOLATILE));

  InsertNamespaceFunction(functions, options, "aead", "decrypt_string", SCALAR,
                          {{string_type,
                            {bytes_type, bytes_type, string_type},
                            FN_AEAD_DECRYPT_STRING}},
                          encryption_required);

  InsertNamespaceFunction(functions, options, "aead", "decrypt_bytes", SCALAR,
                          {{bytes_type,
                            {bytes_type, bytes_type, bytes_type},
                            FN_AEAD_DECRYPT_BYTES}},
                          encryption_required);

  // KMS.ENCRYPT is volatile since KMS encryption may use an algorithm that
  // produces different ciphertext for repeated calls with the same plaintext.
  InsertNamespaceFunction(
      functions, options, "kms", "encrypt", SCALAR,
      {{bytes_type, {string_type, string_type}, FN_KMS_ENCRYPT_STRING},
       {bytes_type, {string_type, bytes_type}, FN_KMS_ENCRYPT_BYTES}},
      FunctionOptions(encryption_required)
          .set_volatility(FunctionEnums::VOLATILE));

  InsertNamespaceFunction(
      functions, options, "kms", "decrypt_string", SCALAR,
      {{string_type, {string_type, bytes_type}, FN_KMS_DECRYPT_STRING}},
      encryption_required);

  InsertNamespaceFunction(
      functions, options, "kms", "decrypt_bytes", SCALAR,
      {{bytes_type, {string_type, bytes_type}, FN_KMS_DECRYPT_BYTES}},
      encryption_required);
}

void GetGeographyFunctions(TypeFactory* type_factory,
                           const ZetaSQLBuiltinFunctionOptions& options,
                           NameToFunctionMap* functions) {
  const Function::Mode SCALAR = Function::SCALAR;
  const Function::Mode AGGREGATE = Function::AGGREGATE;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  const Type* bool_type = types::BoolType();
  const Type* int64_type = types::Int64Type();
  const Type* double_type = types::DoubleType();
  const Type* string_type = types::StringType();
  const Type* bytes_type = types::BytesType();
  const Type* geography_type = types::GeographyType();
  const ArrayType* geography_array_type = types::GeographyArrayType();

  const FunctionOptions geography_required =
      FunctionOptions().add_required_language_feature(
          zetasql::FEATURE_GEOGRAPHY);

  const FunctionArgumentTypeOptions optional_const_arg_options =
      FunctionArgumentTypeOptions().set_must_be_constant().set_cardinality(
          OPTIONAL);

  // Constructors
  InsertFunction(
      functions, options, "st_geogpoint", SCALAR,
      {{geography_type, {double_type, double_type}, FN_ST_GEOG_POINT}},
      geography_required);
  InsertFunction(
      functions, options, "st_makeline", SCALAR,
      {{geography_type, {geography_type, geography_type}, FN_ST_MAKE_LINE},
       {geography_type, {geography_array_type}, FN_ST_MAKE_LINE_ARRAY}},
      geography_required);
  InsertFunction(functions, options, "st_makepolygon", SCALAR,
                 {{geography_type,
                   {geography_type, {geography_array_type, OPTIONAL}},
                   FN_ST_MAKE_POLYGON}},
                 geography_required);
  InsertFunction(
      functions, options, "st_makepolygonoriented", SCALAR,
      {{geography_type, {geography_array_type}, FN_ST_MAKE_POLYGON_ORIENTED}},
      geography_required);

  // Transformations
  InsertFunction(
      functions, options, "st_intersection", SCALAR,
      {{geography_type, {geography_type, geography_type}, FN_ST_INTERSECTION}},
      geography_required);
  InsertFunction(
      functions, options, "st_union", SCALAR,
      {{geography_type, {geography_type, geography_type}, FN_ST_UNION},
       {geography_type, {geography_array_type}, FN_ST_UNION_ARRAY}},
      geography_required);
  InsertFunction(
      functions, options, "st_difference", SCALAR,
      {{geography_type, {geography_type, geography_type}, FN_ST_DIFFERENCE}},
      geography_required);
  InsertFunction(functions, options, "st_unaryunion", SCALAR,
                 {{geography_type, {geography_type}, FN_ST_UNARY_UNION}},
                 geography_required);
  InsertFunction(functions, options, "st_centroid", SCALAR,
                 {{geography_type, {geography_type}, FN_ST_CENTROID}},
                 geography_required);
  InsertFunction(functions, options, "st_buffer", SCALAR,
                 {{geography_type,
                   {geography_type,
                    double_type,
                    {int64_type, OPTIONAL},
                    {bool_type, OPTIONAL}},
                   FN_ST_BUFFER}},
                 geography_required);
  InsertFunction(
      functions, options, "st_bufferwithtolerance", SCALAR,
      {{geography_type,
        {geography_type, double_type, double_type, {bool_type, OPTIONAL}},
        FN_ST_BUFFER_WITH_TOLERANCE}},
      geography_required);
  InsertFunction(
      functions, options, "st_simplify", SCALAR,
      {{geography_type, {geography_type, double_type}, FN_ST_SIMPLIFY}},
      geography_required);
  InsertFunction(
      functions, options, "st_snaptogrid", SCALAR,
      {{geography_type, {geography_type, double_type}, FN_ST_SNAP_TO_GRID}},
      geography_required);
  InsertFunction(functions, options, "st_closestpoint", SCALAR,
                 {{geography_type,
                   {geography_type, geography_type, {bool_type, OPTIONAL}},
                   FN_ST_CLOSEST_POINT}},
                 geography_required);
  InsertFunction(functions, options, "st_boundary", SCALAR,
                 {{geography_type, {geography_type}, FN_ST_BOUNDARY}},
                 geography_required);

  // Predicates
  InsertFunction(functions, options, "st_equals", SCALAR,
                 {{bool_type, {geography_type, geography_type}, FN_ST_EQUALS}},
                 geography_required);
  InsertFunction(
      functions, options, "st_intersects", SCALAR,
      {{bool_type, {geography_type, geography_type}, FN_ST_INTERSECTS}},
      geography_required);
  InsertFunction(
      functions, options, "st_contains", SCALAR,
      {{bool_type, {geography_type, geography_type}, FN_ST_CONTAINS}},
      geography_required);
  InsertFunction(functions, options, "st_within", SCALAR,
                 {{bool_type, {geography_type, geography_type}, FN_ST_WITHIN}},
                 geography_required);
  InsertFunction(functions, options, "st_covers", SCALAR,
                 {{bool_type, {geography_type, geography_type}, FN_ST_COVERS}},
                 geography_required);
  InsertFunction(
      functions, options, "st_coveredby", SCALAR,
      {{bool_type, {geography_type, geography_type}, FN_ST_COVEREDBY}},
      geography_required);
  InsertFunction(
      functions, options, "st_disjoint", SCALAR,
      {{bool_type, {geography_type, geography_type}, FN_ST_DISJOINT}},
      geography_required);
  InsertFunction(functions, options, "st_touches", SCALAR,
                 {{bool_type, {geography_type, geography_type}, FN_ST_TOUCHES}},
                 geography_required);
  InsertFunction(
      functions, options, "st_intersectsbox", SCALAR,
      {{bool_type,
        {geography_type, double_type, double_type, double_type, double_type},
        FN_ST_INTERSECTS_BOX}},
      geography_required);
  InsertFunction(
      functions, options, "st_dwithin", SCALAR,
      {{bool_type,
        {geography_type, geography_type, double_type, {bool_type, OPTIONAL}},
        FN_ST_DWITHIN}},
      geography_required);

  // Accessors
  InsertFunction(functions, options, "st_isempty", SCALAR,
                 {{bool_type, {geography_type}, FN_ST_IS_EMPTY}},
                 geography_required);
  InsertFunction(functions, options, "st_iscollection", SCALAR,
                 {{bool_type, {geography_type}, FN_ST_IS_COLLECTION}},
                 geography_required);
  InsertFunction(functions, options, "st_dimension", SCALAR,
                 {{int64_type, {geography_type}, FN_ST_DIMENSION}},
                 geography_required);
  InsertFunction(functions, options, "st_numpoints", SCALAR,
                 {{int64_type, {geography_type}, FN_ST_NUM_POINTS}},
                 geography_required.Copy().set_alias_name("st_npoints"));
  InsertFunction(functions, options, "st_dump", SCALAR,
                 {{geography_array_type, {geography_type}, FN_ST_DUMP}},
                 geography_required);

  // Measures
  InsertFunction(
      functions, options, "st_length", SCALAR,
      {{double_type, {geography_type, {bool_type, OPTIONAL}}, FN_ST_LENGTH}},
      geography_required);
  InsertFunction(
      functions, options, "st_perimeter", SCALAR,
      {{double_type, {geography_type, {bool_type, OPTIONAL}}, FN_ST_PERIMETER}},
      geography_required);
  InsertFunction(
      functions, options, "st_area", SCALAR,
      {{double_type, {geography_type, {bool_type, OPTIONAL}}, FN_ST_AREA}},
      geography_required);
  InsertFunction(functions, options, "st_distance", SCALAR,
                 {{double_type,
                   {geography_type, geography_type, {bool_type, OPTIONAL}},
                   FN_ST_DISTANCE}},
                 geography_required);
  InsertFunction(functions, options, "st_maxdistance", SCALAR,
                 {{double_type,
                   {geography_type, geography_type, {bool_type, OPTIONAL}},
                   FN_ST_MAX_DISTANCE}},
                 geography_required);

  // Parsers/Formatters
  InsertFunction(functions, options, "st_astext", SCALAR,
                 {{string_type, {geography_type}, FN_ST_AS_TEXT}},
                 geography_required);
  InsertFunction(functions, options, "st_askml", SCALAR,
                 {{string_type, {geography_type}, FN_ST_AS_KML}},
                 geography_required);
  InsertFunction(functions, options, "st_asgeojson", SCALAR,
                 {{string_type,
                   {geography_type, {int64_type, OPTIONAL}},
                   FN_ST_AS_GEO_JSON}},
                 geography_required);
  InsertFunction(functions, options, "st_asbinary", SCALAR,
                 {{bytes_type, {geography_type}, FN_ST_AS_BINARY}},
                 geography_required);
  InsertFunction(
      functions, options, "st_geohash", SCALAR,
      {{string_type, {geography_type, {int64_type, OPTIONAL}}, FN_ST_GEOHASH}},
      geography_required);
  InsertFunction(
      functions, options, "st_geogpointfromgeohash", SCALAR,
      {{geography_type, {string_type}, FN_ST_GEOG_POINT_FROM_GEOHASH}},
      geography_required);

  // TODO: when named parameters are available, make second
  // parameter a mandatory named argument `oriented`.
  InsertFunction(functions, options, "st_geogfromtext", SCALAR,
                 {{geography_type,
                   {string_type, {bool_type, optional_const_arg_options}},
                   FN_ST_GEOG_FROM_TEXT}},
                 geography_required);
  InsertFunction(functions, options, "st_geogfromkml", SCALAR,
                 {{geography_type, {string_type}, FN_ST_GEOG_FROM_KML}},
                 geography_required);
  InsertFunction(functions, options, "st_geogfromgeojson", SCALAR,
                 {{geography_type, {string_type}, FN_ST_GEOG_FROM_GEO_JSON}},
                 geography_required);
  InsertFunction(functions, options, "st_geogfromwkb", SCALAR,
                 {{geography_type, {bytes_type}, FN_ST_GEOG_FROM_WKB}},
                 geography_required);

  // Aggregate
  InsertFunction(functions, options, "st_union_agg", AGGREGATE,
                 {{geography_type, {geography_type}, FN_ST_UNION_AGG}},
                 geography_required);
  InsertFunction(functions, options, "st_accum", AGGREGATE,
                 {{geography_array_type, {geography_type}, FN_ST_ACCUM}},
                 geography_required);
  InsertFunction(functions, options, "st_centroid_agg", AGGREGATE,
                 {{geography_type, {geography_type}, FN_ST_CENTROID_AGG}},
                 geography_required);

  // Other
  InsertFunction(functions, options, "st_x", SCALAR,
                 {{double_type, {geography_type}, FN_ST_X}},
                 geography_required);
  InsertFunction(functions, options, "st_y", SCALAR,
                 {{double_type, {geography_type}, FN_ST_Y}},
                 geography_required);
}

}  // namespace zetasql
