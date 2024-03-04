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


#include <cstdint>
#include <initializer_list>
#include <limits>
#include <string>
#include <vector>

#include "google/protobuf/timestamp.pb.h"
#include "google/protobuf/wrappers.pb.h"
#include "google/type/date.pb.h"
#include "google/type/timeofday.pb.h"
#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/builtin_function.pb.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/proto_util.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/functional/bind_front.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
class AnalyzerOptions;

static FunctionSignatureOptions SetRewriter(ResolvedASTRewrite rewriter) {
  return FunctionSignatureOptions().set_rewrite_options(
      FunctionSignatureRewriteOptions().set_rewriter(rewriter));
}

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

void GetStringFunctions(TypeFactory* type_factory,
                        const ZetaSQLBuiltinFunctionOptions& options,
                        NameToFunctionMap* functions) {
  const Type* string_type = type_factory->get_string();
  const Type* bytes_type = type_factory->get_bytes();
  const Type* int64_type = type_factory->get_int64();
  const Type* bool_type = type_factory->get_bool();
  const Type* json_type = types::JsonType();
  const Type* date_type = type_factory->get_date();
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* time_type = type_factory->get_time();
  const Type* datetime_type = type_factory->get_datetime();
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
                 {{int64_type,
                   {string_type, string_type},
                   FN_STRPOS_STRING,
                   FunctionSignatureOptions().set_uses_operation_collation()},
                  {int64_type, {bytes_type, bytes_type}, FN_STRPOS_BYTES}});

  InsertSimpleFunction(functions, options, "lower", SCALAR,
                       {{string_type, {string_type}, FN_LOWER_STRING},
                        {bytes_type, {bytes_type}, FN_LOWER_BYTES}});

  InsertSimpleFunction(functions, options, "upper", SCALAR,
                       {{string_type, {string_type}, FN_UPPER_STRING},
                        {bytes_type, {bytes_type}, FN_UPPER_BYTES}});

  InsertSimpleFunction(functions, options, "length", SCALAR,
                       {{int64_type, {string_type}, FN_LENGTH_STRING},
                        {int64_type, {bytes_type}, FN_LENGTH_BYTES}});

  InsertSimpleFunction(functions, options, "byte_length", SCALAR,
                       {{int64_type, {string_type}, FN_BYTE_LENGTH_STRING},
                        {int64_type, {bytes_type}, FN_BYTE_LENGTH_BYTES}},
                       FunctionOptions().set_alias_name("octet_length"));

  InsertSimpleFunction(functions, options, "char_length", SCALAR,
                       {{int64_type, {string_type}, FN_CHAR_LENGTH_STRING}},
                       FunctionOptions().set_alias_name("character_length"));

  InsertFunction(functions, options, "starts_with", SCALAR,
                 {{bool_type,
                   {string_type, string_type},
                   FN_STARTS_WITH_STRING,
                   FunctionSignatureOptions().set_uses_operation_collation()},
                  {bool_type, {bytes_type, bytes_type}, FN_STARTS_WITH_BYTES}});

  InsertFunction(functions, options, "ends_with", SCALAR,
                 {{bool_type,
                   {string_type, string_type},
                   FN_ENDS_WITH_STRING,
                   FunctionSignatureOptions().set_uses_operation_collation()},
                  {bool_type, {bytes_type, bytes_type}, FN_ENDS_WITH_BYTES}});

  FunctionOptions substr_options;
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_3_ADDITIONAL_STRING_FUNCTIONS)) {
    substr_options.set_alias_name("substring");
  }
  InsertSimpleFunction(functions, options, "substr", SCALAR,
                       {{string_type,
                         {string_type, int64_type, {int64_type, OPTIONAL}},
                         FN_SUBSTR_STRING},
                        {bytes_type,
                         {bytes_type, int64_type, {int64_type, OPTIONAL}},
                         FN_SUBSTR_BYTES}},
                       substr_options);

  InsertSimpleFunction(
      functions, options, "trim", SCALAR,
      {{string_type, {string_type, {string_type, OPTIONAL}}, FN_TRIM_STRING},
       {bytes_type, {bytes_type, bytes_type}, FN_TRIM_BYTES}});

  InsertSimpleFunction(
      functions, options, "ltrim", SCALAR,
      {{string_type, {string_type, {string_type, OPTIONAL}}, FN_LTRIM_STRING},
       {bytes_type, {bytes_type, bytes_type}, FN_LTRIM_BYTES}});

  InsertSimpleFunction(
      functions, options, "rtrim", SCALAR,
      {{string_type, {string_type, {string_type, OPTIONAL}}, FN_RTRIM_STRING},
       {bytes_type, {bytes_type, bytes_type}, FN_RTRIM_BYTES}});

  InsertSimpleFunction(functions, options, "lpad", SCALAR,
                       {{string_type,
                         {string_type, int64_type, {string_type, OPTIONAL}},
                         FN_LPAD_STRING},
                        {bytes_type,
                         {bytes_type, int64_type, {bytes_type, OPTIONAL}},
                         FN_LPAD_BYTES}});

  InsertSimpleFunction(functions, options, "rpad", SCALAR,
                       {{string_type,
                         {string_type, int64_type, {string_type, OPTIONAL}},
                         FN_RPAD_STRING},
                        {bytes_type,
                         {bytes_type, int64_type, {bytes_type, OPTIONAL}},
                         FN_RPAD_BYTES}});

  InsertSimpleFunction(
      functions, options, "left", SCALAR,
      {{string_type, {string_type, int64_type}, FN_LEFT_STRING},
       {bytes_type, {bytes_type, int64_type}, FN_LEFT_BYTES}});
  InsertSimpleFunction(
      functions, options, "right", SCALAR,
      {{string_type, {string_type, int64_type}, FN_RIGHT_STRING},
       {bytes_type, {bytes_type, int64_type}, FN_RIGHT_BYTES}});

  InsertSimpleFunction(
      functions, options, "repeat", SCALAR,
      {{string_type, {string_type, int64_type}, FN_REPEAT_STRING},
       {bytes_type, {bytes_type, int64_type}, FN_REPEAT_BYTES}});

  InsertSimpleFunction(functions, options, "reverse", SCALAR,
                       {{string_type, {string_type}, FN_REVERSE_STRING},
                        {bytes_type, {bytes_type}, FN_REVERSE_BYTES}});

  InsertFunction(
      functions, options, "replace", SCALAR,
      {{string_type,
        {string_type, string_type, string_type},
        FN_REPLACE_STRING,
        FunctionSignatureOptions().set_uses_operation_collation()},
       {bytes_type, {bytes_type, bytes_type, bytes_type}, FN_REPLACE_BYTES}});

  InsertFunction(functions, options, "format", SCALAR,
                 {{string_type,
                   {string_type,
                    {ARG_TYPE_ARBITRARY, FunctionArgumentTypeOptions()
                                             .set_argument_collation_mode(
                                                 FunctionEnums::AFFECTS_NONE)
                                             .set_cardinality(REPEATED)}},
                   FN_FORMAT_STRING}},
                 FunctionOptions().set_post_resolution_argument_constraint(
                     &CheckFormatPostResolutionArguments));

  FunctionSignatureOptions date_time_constructor_options =
      FunctionSignatureOptions().add_required_language_feature(
          FEATURE_V_1_3_DATE_TIME_CONSTRUCTORS);
  std::vector<FunctionSignatureOnHeap> string_signatures{
      {string_type,
       {timestamp_type, {string_type, OPTIONAL}},
       FN_STRING_FROM_TIMESTAMP}};
  string_signatures.push_back({string_type,
                               {date_type},
                               FN_STRING_FROM_DATE,
                               date_time_constructor_options});
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_2_CIVIL_TIME)) {
    string_signatures.push_back({string_type,
                                 {time_type},
                                 FN_STRING_FROM_TIME,
                                 date_time_constructor_options});
    string_signatures.push_back({string_type,
                                 {datetime_type},
                                 FN_STRING_FROM_DATETIME,
                                 date_time_constructor_options});
  }
  if (options.language_options.LanguageFeatureEnabled(FEATURE_JSON_TYPE) &&
      options.language_options.LanguageFeatureEnabled(
          FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS)) {
    string_signatures.push_back({string_type, {json_type}, FN_JSON_TO_STRING});
  }
  InsertFunction(functions, options, "string", SCALAR, string_signatures);

  const ArrayType* string_array_type = types::StringArrayType();
  const ArrayType* bytes_array_type = types::BytesArrayType();
  const ArrayType* int64_array_type = types::Int64ArrayType();

  InsertFunction(
      functions, options, "split", SCALAR,
      // Note that the delimiter second parameter is optional for the STRING
      // version, but is required for the BYTES version.
      {{{string_array_type,
         FunctionArgumentTypeOptions().set_uses_array_element_for_collation()},
        {string_type, {string_type, OPTIONAL}},
        FN_SPLIT_STRING,
        FunctionSignatureOptions().set_uses_operation_collation()},
       {bytes_array_type, {bytes_type, bytes_type}, FN_SPLIT_BYTES}});

  InsertSimpleFunction(
      functions, options, "safe_convert_bytes_to_string", SCALAR,
      {{string_type, {bytes_type}, FN_SAFE_CONVERT_BYTES_TO_STRING}});

  InsertSimpleFunction(functions, options, "normalize", SCALAR,
                       {
                           {string_type,
                            {string_type, {normalize_mode_type, OPTIONAL}},
                            FN_NORMALIZE_STRING},
                       });
  InsertSimpleFunction(functions, options, "normalize_and_casefold", SCALAR,
                       {{string_type,
                         {string_type, {normalize_mode_type, OPTIONAL}},
                         FN_NORMALIZE_AND_CASEFOLD_STRING}});
  InsertSimpleFunction(functions, options, "to_base32", SCALAR,
                       {{string_type, {bytes_type}, FN_TO_BASE32}});
  InsertSimpleFunction(functions, options, "from_base32", SCALAR,
                       {{bytes_type, {string_type}, FN_FROM_BASE32}});
  InsertSimpleFunction(functions, options, "to_base64", SCALAR,
                       {{string_type, {bytes_type}, FN_TO_BASE64}});
  InsertSimpleFunction(functions, options, "from_base64", SCALAR,
                       {{bytes_type, {string_type}, FN_FROM_BASE64}});
  InsertSimpleFunction(functions, options, "to_hex", SCALAR,
                       {{string_type, {bytes_type}, FN_TO_HEX}});
  InsertSimpleFunction(functions, options, "from_hex", SCALAR,
                       {{bytes_type, {string_type}, FN_FROM_HEX}});
  InsertSimpleFunction(
      functions, options, "to_code_points", SCALAR,
      {{int64_array_type, {string_type}, FN_TO_CODE_POINTS_STRING},
       {int64_array_type, {bytes_type}, FN_TO_CODE_POINTS_BYTES}});
  InsertSimpleFunction(
      functions, options, "code_points_to_string", SCALAR,
      {{string_type, {int64_array_type}, FN_CODE_POINTS_TO_STRING}});
  InsertSimpleFunction(
      functions, options, "code_points_to_bytes", SCALAR,
      {{bytes_type, {int64_array_type}, FN_CODE_POINTS_TO_BYTES}});
  InsertSimpleFunction(functions, options, "ascii", SCALAR,
                       {{int64_type, {string_type}, FN_ASCII_STRING},
                        {int64_type, {bytes_type}, FN_ASCII_BYTES}});
  InsertSimpleFunction(functions, options, "unicode", SCALAR,
                       {{int64_type, {string_type}, FN_UNICODE_STRING}});
  InsertSimpleFunction(functions, options, "chr", SCALAR,
                       {{string_type, {int64_type}, FN_CHR_STRING}});

  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_3_ADDITIONAL_STRING_FUNCTIONS)) {
    InsertFunction(functions, options, "instr", SCALAR,
                   {{int64_type,
                     {string_type,
                      string_type,
                      {int64_type, OPTIONAL},
                      {int64_type, OPTIONAL}},
                     FN_INSTR_STRING,
                     FunctionSignatureOptions().set_uses_operation_collation()},
                    {int64_type,
                     {bytes_type,
                      bytes_type,
                      {int64_type, OPTIONAL},
                      {int64_type, OPTIONAL}},
                     FN_INSTR_BYTES}});
    InsertSimpleFunction(functions, options, "soundex", SCALAR,
                         {{string_type, {string_type}, FN_SOUNDEX_STRING}});
    InsertFunction(functions, options, "translate", SCALAR,
                   {{string_type,
                     {string_type, string_type, string_type},
                     FN_TRANSLATE_STRING,
                     FunctionSignatureOptions().set_rejects_collation()},
                    {bytes_type,
                     {bytes_type, bytes_type, bytes_type},
                     FN_TRANSLATE_BYTES}});
    InsertFunction(functions, options, "initcap", SCALAR,
                   {{string_type,
                     {string_type, {string_type, OPTIONAL}},
                     FN_INITCAP_STRING,
                     FunctionSignatureOptions().set_rejects_collation()}});
  }

  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_3_COLLATION_SUPPORT)) {
    InsertFunction(
        functions, options, "collate", SCALAR,
        {{string_type,
          {string_type, string_type},
          FN_COLLATE,
          // Doesn't propagate collation from the argument. The collation on the
          // return type is decided by the resolver.
          FunctionSignatureOptions().set_propagates_collation(false)}},
        FunctionOptions().set_pre_resolution_argument_constraint(
            [](const std::vector<InputArgumentType>& args,
               const LanguageOptions&) -> absl::Status {
              // Make sure the second argument is a string literal.
              if (args.size() >= 2 && !ArgumentIsStringLiteral(args[1])) {
                return MakeSqlError() << "The second argument of COLLATE() "
                                         "must be a string literal";
              }
              return absl::OkStatus();
            }));
  }

  std::vector<FunctionSignatureOnHeap> edit_distance_signature = {
      {int64_type,
       {string_type,
        string_type,
        {int64_type,
         FunctionArgumentTypeOptions()
             .set_cardinality(FunctionEnums::OPTIONAL)
             .set_argument_name("max_distance", kNamedOnly)
             .set_default(values::Int64(std::numeric_limits<int64_t>::max()))}},
       FN_EDIT_DISTANCE},
  };
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_4_ENABLE_EDIT_DISTANCE_BYTES)) {
    edit_distance_signature.push_back(
        {int64_type,
         {bytes_type,
          bytes_type,
          {int64_type, FunctionArgumentTypeOptions()
                           .set_cardinality(FunctionEnums::OPTIONAL)
                           .set_argument_name("max_distance", kNamedOnly)
                           .set_default(values::Int64(
                               std::numeric_limits<int64_t>::max()))}},
         FN_EDIT_DISTANCE_BYTES});
  }
  InsertFunction(functions, options, "edit_distance", SCALAR,
                 edit_distance_signature, FunctionOptions());
}

void GetRegexFunctions(TypeFactory* type_factory,
                       const ZetaSQLBuiltinFunctionOptions& options,
                       NameToFunctionMap* functions) {
  const Type* string_type = type_factory->get_string();
  const Type* bytes_type = type_factory->get_bytes();
  const Type* bool_type = type_factory->get_bool();
  const Type* int64_type = type_factory->get_int64();

  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  const Function::Mode SCALAR = Function::SCALAR;

  InsertFunction(functions, options, "regexp_match", SCALAR,
                 {{bool_type,
                   {string_type, string_type},
                   FN_REGEXP_MATCH_STRING,
                   FunctionSignatureOptions().set_rejects_collation()},
                  {bool_type, {bytes_type, bytes_type}, FN_REGEXP_MATCH_BYTES}},
                 FunctionOptions().set_allow_external_usage(false));
  InsertFunction(
      functions, options, "regexp_contains", SCALAR,
      {{bool_type,
        {string_type, string_type},
        FN_REGEXP_CONTAINS_STRING,
        FunctionSignatureOptions().set_rejects_collation()},
       {bool_type, {bytes_type, bytes_type}, FN_REGEXP_CONTAINS_BYTES}});

  FunctionArgumentTypeList regexp_extract_string_args = {string_type,
                                                         string_type};
  FunctionArgumentTypeList regexp_extract_bytes_args = {bytes_type, bytes_type};
  FunctionOptions regexp_extract_options;
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_3_ALLOW_REGEXP_EXTRACT_OPTIONALS)) {
    regexp_extract_string_args.insert(
        regexp_extract_string_args.end(),
        {{int64_type, OPTIONAL}, {int64_type, OPTIONAL}});
    regexp_extract_bytes_args.insert(
        regexp_extract_bytes_args.end(),
        {{int64_type, OPTIONAL}, {int64_type, OPTIONAL}});
    regexp_extract_options.set_alias_name("regexp_substr");
  }
  InsertFunction(
      functions, options, "regexp_extract", SCALAR,
      {{string_type, regexp_extract_string_args, FN_REGEXP_EXTRACT_STRING,
        FunctionSignatureOptions().set_rejects_collation()},
       {bytes_type, regexp_extract_bytes_args, FN_REGEXP_EXTRACT_BYTES}},
      regexp_extract_options);

  InsertFunction(functions, options, "regexp_instr", SCALAR,
                 {{int64_type,
                   {string_type,
                    string_type,
                    {int64_type, OPTIONAL},
                    {int64_type, OPTIONAL},
                    {int64_type, OPTIONAL}},
                   FN_REGEXP_INSTR_STRING,
                   FunctionSignatureOptions().set_rejects_collation()},
                  {int64_type,
                   {bytes_type,
                    bytes_type,
                    {int64_type, OPTIONAL},
                    {int64_type, OPTIONAL},
                    {int64_type, OPTIONAL}},
                   FN_REGEXP_INSTR_BYTES}});

  InsertFunction(functions, options, "regexp_replace", SCALAR,
                 {{string_type,
                   {string_type, string_type, string_type},
                   FN_REGEXP_REPLACE_STRING,
                   FunctionSignatureOptions().set_rejects_collation()},
                  {bytes_type,
                   {bytes_type, bytes_type, bytes_type},
                   FN_REGEXP_REPLACE_BYTES}});
  const ArrayType* string_array_type = types::StringArrayType();
  const ArrayType* bytes_array_type = types::BytesArrayType();

  InsertFunction(functions, options, "regexp_extract_all", SCALAR,
                 {{string_array_type,
                   {string_type, string_type},
                   FN_REGEXP_EXTRACT_ALL_STRING,
                   FunctionSignatureOptions().set_rejects_collation()},
                  {bytes_array_type,
                   {bytes_type, bytes_type},
                   FN_REGEXP_EXTRACT_ALL_BYTES}});
}

absl::Status GetProto3ConversionFunctions(
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
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeProtoType(
      google::protobuf::Timestamp::descriptor(), &proto_timestamp_type));
  const Type* proto_date_type = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeProtoType(google::type::Date::descriptor(),
                                              &proto_date_type));
  const Type* proto_time_of_day_type = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeProtoType(
      google::type::TimeOfDay::descriptor(), &proto_time_of_day_type));
  const Type* proto_double_wrapper = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeProtoType(
      google::protobuf::DoubleValue::descriptor(), &proto_double_wrapper));
  const Type* proto_float_wrapper = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeProtoType(
      google::protobuf::FloatValue::descriptor(), &proto_float_wrapper));
  const Type* proto_int64_wrapper = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeProtoType(
      google::protobuf::Int64Value::descriptor(), &proto_int64_wrapper));
  const Type* proto_uint64_wrapper = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeProtoType(
      google::protobuf::UInt64Value::descriptor(), &proto_uint64_wrapper));
  const Type* proto_int32_wrapper = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeProtoType(
      google::protobuf::Int32Value::descriptor(), &proto_int32_wrapper));
  const Type* proto_uint32_wrapper = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeProtoType(
      google::protobuf::UInt32Value::descriptor(), &proto_uint32_wrapper));
  const Type* proto_bool_wrapper = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeProtoType(
      google::protobuf::BoolValue::descriptor(), &proto_bool_wrapper));
  const Type* proto_string_wrapper = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeProtoType(
      google::protobuf::StringValue::descriptor(), &proto_string_wrapper));
  const Type* proto_bytes_wrapper = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeProtoType(
      google::protobuf::BytesValue::descriptor(), &proto_bytes_wrapper));
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* time_type = type_factory->get_time();
  const Type* float_type = type_factory->get_float();

  std::initializer_list<FunctionSignatureProxy> from_proto_signature_proxies{
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
  std::vector<FunctionSignatureOnHeap> from_proto_signatures(
      from_proto_signature_proxies.begin(), from_proto_signature_proxies.end());

  std::initializer_list<FunctionSignatureProxy> to_proto_signatures_proxies = {
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
  std::vector<FunctionSignatureOnHeap> to_proto_signatures(
      to_proto_signatures_proxies.begin(), to_proto_signatures_proxies.end());
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
  return absl::OkStatus();
}

void GetErrorHandlingFunctions(TypeFactory* type_factory,
                               const ZetaSQLBuiltinFunctionOptions& options,
                               NameToFunctionMap* functions) {
  const Function::Mode SCALAR = Function::SCALAR;
  const Type* bool_type = type_factory->get_bool();
  const Type* int64_type = type_factory->get_int64();
  const Type* string_type = type_factory->get_string();

  // The signature is declared as
  //   ERROR(string) -> int64_t
  // but this is special-cased in the resolver so that the result can be
  // coerced to anything, similar to untyped NULL.  This allows using this
  // in expressions like IF(<condition>, <value>, ERROR("message"))
  // for any value type.  It would be preferable to declare this with an
  // undefined or templated return type, but that is not allowed.
  InsertSimpleFunction(functions, options, "error", SCALAR,
                       {{int64_type, {string_type}, FN_ERROR}});

  InsertSimpleFunction(
      functions, options, "iferror", SCALAR,
      {{ARG_TYPE_ANY_1, {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_IFERROR}},
      FunctionOptions().set_may_suppress_side_effects(true));

  InsertFunction(functions, options, "iserror", SCALAR,
                 {{bool_type, {ARG_TYPE_ANY_1}, FN_ISERROR}},
                 FunctionOptions().set_may_suppress_side_effects(true));

  InsertFunction(functions, options, "nulliferror", SCALAR,
                 {{ARG_TYPE_ANY_1,
                   {ARG_TYPE_ANY_1},
                   FN_NULLIFERROR,
                   SetRewriter(REWRITE_NULLIFERROR_FUNCTION)}},
                 FunctionOptions().set_may_suppress_side_effects(true));
}

static FunctionSignatureOnHeap NullIfZeroSig(const Type* type,
                                             FunctionSignatureId id) {
  constexpr absl::string_view kNullIfZeroTemplate = R"sql(
    NULLIF(input, 0)
  )sql";
  FunctionArgumentType input_arg{
      type, FunctionArgumentTypeOptions().set_argument_name("input",
                                                            kPositionalOnly)};
  return FunctionSignatureOnHeap(
      type, {input_arg}, id,
      FunctionSignatureOptions().set_rewrite_options(
          FunctionSignatureRewriteOptions()
              .set_enabled(true)
              .set_rewriter(REWRITE_BUILTIN_FUNCTION_INLINER)
              .set_sql(kNullIfZeroTemplate)));
}

static FunctionSignatureOnHeap ZeroIfNullSig(const Type* type,
                                             FunctionSignatureId id) {
  constexpr absl::string_view kZeroIfNullTemplate = R"sql(
    IFNULL(input, 0)
  )sql";
  FunctionArgumentType input_arg{
      type, FunctionArgumentTypeOptions().set_argument_name("input",
                                                            kPositionalOnly)};
  return FunctionSignatureOnHeap(
      type, {input_arg}, id,
      FunctionSignatureOptions().set_rewrite_options(
          FunctionSignatureRewriteOptions()
              .set_enabled(true)
              .set_rewriter(REWRITE_BUILTIN_FUNCTION_INLINER)
              .set_sql(kZeroIfNullTemplate)));
}

void GetConditionalFunctions(TypeFactory* type_factory,
                             const ZetaSQLBuiltinFunctionOptions& options,
                             NameToFunctionMap* functions) {
  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality REPEATED =
      FunctionArgumentType::REPEATED;
  const Type* bool_type = type_factory->get_bool();
  const Type* int32_type = type_factory->get_int32();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint32_type = type_factory->get_uint32();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* double_type = type_factory->get_double();
  const Type* float_type = type_factory->get_float();
  const Type* numeric_type = type_factory->get_numeric();
  const Type* bignumeric_type = type_factory->get_bignumeric();

  InsertSimpleFunction(
      functions, options, "if", SCALAR,
      {{ARG_TYPE_ANY_1, {bool_type, ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_IF}},
      FunctionOptions().set_may_suppress_side_effects(true));

  // COALESCE(expr1, ..., exprN): returns the first non-null expression.
  // In particular, COALESCE is used to express the output of FULL JOIN.
  InsertSimpleFunction(
      functions, options, "coalesce", SCALAR,
      {{ARG_TYPE_ANY_1, {{ARG_TYPE_ANY_1, REPEATED}}, FN_COALESCE}},
      FunctionOptions().set_may_suppress_side_effects(true));

  // IFNULL(expr1, expr2): if expr1 is not null, returns expr1, else expr2
  InsertSimpleFunction(
      functions, options, "ifnull", SCALAR,
      {{ARG_TYPE_ANY_1, {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_IFNULL}},
      FunctionOptions().set_may_suppress_side_effects(true));

  bool uses_operation_collation_for_nullif =
      options.language_options.LanguageFeatureEnabled(
          zetasql::FEATURE_V_1_4_USE_OPERATION_COLLATION_FOR_NULLIF);
  // NULLIF(expr1, expr2): NULL if expr1 = expr2, otherwise returns expr1.
  InsertFunction(
      functions, options, "nullif", SCALAR,
      {{ARG_TYPE_ANY_1,
        {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1},
        FN_NULLIF,
        FunctionSignatureOptions().set_uses_operation_collation(
            uses_operation_collation_for_nullif)}},
      FunctionOptions().set_post_resolution_argument_constraint(
          absl::bind_front(&CheckArgumentsSupportEquality, "NULLIF")));

  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_4_NULLIFZERO_ZEROIFNULL)) {
    // ZEROIFNULL(expr): if expr is not null, returns expr, else 0.
    InsertFunction(
        functions, options, "zeroifnull", SCALAR,
        {{ZeroIfNullSig(int32_type, FN_ZEROIFNULL_INT32),
          ZeroIfNullSig(uint32_type, FN_ZEROIFNULL_UINT32),
          ZeroIfNullSig(float_type, FN_ZEROIFNULL_FLOAT),
          ZeroIfNullSig(int64_type, FN_ZEROIFNULL_INT64),
          ZeroIfNullSig(uint64_type, FN_ZEROIFNULL_UINT64),
          ZeroIfNullSig(double_type, FN_ZEROIFNULL_DOUBLE),
          ZeroIfNullSig(numeric_type, FN_ZEROIFNULL_NUMERIC),
          ZeroIfNullSig(bignumeric_type, FN_ZEROIFNULL_BIGNUMERIC)}});

    // NULLIFZERO(expr): NULL if expr = 0 otherwise returns expr.
    InsertFunction(
        functions, options, "nullifzero", SCALAR,
        {{NullIfZeroSig(int32_type, FN_NULLIFZERO_INT32),
          NullIfZeroSig(uint32_type, FN_NULLIFZERO_UINT32),
          NullIfZeroSig(float_type, FN_NULLIFZERO_FLOAT),
          NullIfZeroSig(int64_type, FN_NULLIFZERO_INT64),
          NullIfZeroSig(uint64_type, FN_NULLIFZERO_UINT64),
          NullIfZeroSig(double_type, FN_NULLIFZERO_DOUBLE),
          NullIfZeroSig(numeric_type, FN_NULLIFZERO_NUMERIC),
          NullIfZeroSig(bignumeric_type, FN_NULLIFZERO_BIGNUMERIC)}});
  }

  // From the SQL language perspective, the ELSE clause is optional for both
  // CASE statement signatures.  However, the parser will normalize the
  // CASE expressions so they always have the ELSE, and therefore it is defined
  // here as a required argument in the function signatures.
  //
  // CASE (<T2>) WHEN (/*repeated*/ <T2>)
  //             THEN (/*repeated*/ <T1>)
  //             ELSE (<T1>) END
  // <T2> arguments are marked 'AFFECTS_OPERATION' to be considered in
  // calculating operation collation.
  // <T1> arguments are marked 'AFFECTS_PROPAGATION' to be considered
  // in calculating propagation collation.
  InsertFunction(
      functions, options, "$case_with_value", SCALAR,
      {{ARG_TYPE_ANY_1,
        {{ARG_TYPE_ANY_2,
          FunctionArgumentTypeOptions().set_argument_collation_mode(
              FunctionEnums::AFFECTS_OPERATION)},
         {ARG_TYPE_ANY_2,
          FunctionArgumentTypeOptions()
              .set_argument_collation_mode(FunctionEnums::AFFECTS_OPERATION)
              .set_cardinality(REPEATED)},
         {ARG_TYPE_ANY_1,
          FunctionArgumentTypeOptions()
              .set_argument_collation_mode(FunctionEnums::AFFECTS_PROPAGATION)
              .set_cardinality(REPEATED)},
         {ARG_TYPE_ANY_1,
          FunctionArgumentTypeOptions().set_argument_collation_mode(
              FunctionEnums::AFFECTS_PROPAGATION)}},
        FN_CASE_WITH_VALUE,
        FunctionSignatureOptions().set_uses_operation_collation()}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_may_suppress_side_effects(true)
          .set_sql_name("case")
          .set_hide_supported_signatures(true)
          .set_get_sql_callback(&CaseWithValueFunctionSQL)
          .set_pre_resolution_argument_constraint(
              absl::bind_front(&CheckFirstArgumentSupportsEquality,
                               "CASE (with value comparison)")));

  InsertSimpleFunction(
      functions, options, "$case_no_value", SCALAR,
      {{ARG_TYPE_ANY_1,
        {{bool_type, REPEATED}, {ARG_TYPE_ANY_1, REPEATED}, {ARG_TYPE_ANY_1}},
        FN_CASE_NO_VALUE}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_may_suppress_side_effects(true)
          .set_sql_name("case")
          .set_hide_supported_signatures(true)
          .set_get_sql_callback(&CaseNoValueFunctionSQL)
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForCaseNoValueFunction));

  // Internal function $with_side_effects(expression ANY_1, payload BYTES).
  // Enabled only when FEATURE_V_1_4_ENFORCE_CONDITIONAL_EVALUATION is on.
  //
  // If the payload is not NULL, applies the side effect (e.g. raise the error
  // described by the payload). Otherwise, returns the first argument. This is
  // important for conditional evaluation and correct handling of side effects
  // when an expression get split across scans. For example, in the query
  //   SELECT IF(a, b, SUM(c/d)) FROM t
  // the division `c/d` should not cause the query to fail when `a` is
  // true, even if d is zero, because it's in the false branch. This holds even
  // as SUM(c/d) is separated from the larger IF() expression to be placed on an
  // AggregateScan.
  //
  // The aforementioned LanguageFeature changes the resulting resolved AST to
  // propagate deferred side-effect values, and to specify when and where
  // exactly the deferred side effect is handled, using
  // ResolvedDeferredComputedColumn and the internal function
  // $with_side_effects().
  //
  // See (broken link)
  InsertFunction(
      functions, options, "$with_side_effects", SCALAR,
      {{ARG_TYPE_ANY_1,
        {{ARG_TYPE_ANY_1}, {types::BytesType()}},
        FN_WITH_SIDE_EFFECTS,
        FunctionSignatureOptions().set_is_internal(true)}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .add_required_language_feature(
              LanguageFeature::FEATURE_V_1_4_ENFORCE_CONDITIONAL_EVALUATION));
}

void GetMiscellaneousFunctions(TypeFactory* type_factory,
                               const ZetaSQLBuiltinFunctionOptions& options,
                               NameToFunctionMap* functions) {
  const Type* int32_type = type_factory->get_int32();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint32_type = type_factory->get_uint32();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* double_type = type_factory->get_double();
  const Type* string_type = type_factory->get_string();
  const Type* bytes_type = type_factory->get_bytes();

  const Function::Mode SCALAR = Function::SCALAR;

  // Is a particular key present in a proto map?
  InsertSimpleFunction(functions, options, "contains_key", SCALAR,
                       {{type_factory->get_bool(),
                         {ARG_PROTO_MAP_ANY, ARG_PROTO_MAP_KEY_ANY},
                         FN_CONTAINS_KEY}},
                       FunctionOptions().add_required_language_feature(
                           LanguageFeature::FEATURE_V_1_3_PROTO_MAPS));

  // Is a particular key present in a proto map?
  std::initializer_list<FunctionArgumentTypeProxy> modify_map_args = {
      ARG_PROTO_MAP_ANY,
      {ARG_PROTO_MAP_KEY_ANY, FunctionArgumentType::REPEATED},
      {ARG_PROTO_MAP_VALUE_ANY, FunctionArgumentType::REPEATED},
  };

  InsertSimpleFunction(
      functions, options, "modify_map", SCALAR,
      {{ARG_PROTO_MAP_ANY, modify_map_args, FN_MODIFY_MAP}},
      FunctionOptions()
          .add_required_language_feature(
              LanguageFeature::FEATURE_V_1_3_PROTO_MAPS)
          .set_pre_resolution_argument_constraint(
              [](const std::vector<InputArgumentType>& args,
                 const LanguageOptions& opts) -> absl::Status {
                if (args.size() < 3 || args.size() % 2 == 0) {
                  return MakeSqlError()
                         << "MODIFY_MAP must take a protocol buffer map "
                            "as the first argument then one or more key-value "
                            "pairs as the subsequent arguments.";
                }
                return absl::OkStatus();
              })
          .set_no_matching_signature_callback(
              [=](absl::string_view qualified_function_name,
                  const std::vector<InputArgumentType>& args,
                  const ProductMode& product_mode) {
                std::string ret = absl::StrCat("No matching signature for ",
                                               qualified_function_name, "(");
                for (int i = 0; i < args.size(); ++i) {
                  if (i > 0) absl::StrAppend(&ret, ", ");
                  absl::StrAppend(&ret, args[i].UserFacingName(product_mode));
                }
                absl::StrAppend(&ret, "); ");
                if (args.empty() || !IsProtoMap(args[0].type())) {
                  absl::StrAppend(&ret, "first argument must be a proto map");
                } else {
                  absl::StrAppend(&ret,
                                  "some key or value did not match the map's "
                                  "key or value type");
                }
                return ret;
              }));

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
          .set_get_sql_callback(absl::bind_front(&InfixFunctionSQL, "||")));

  // RANGE_BUCKET: returns the bucket of the item in the array.
  InsertFunction(
      functions, options, "range_bucket", SCALAR,
      {{int64_type,
        {ARG_TYPE_ANY_1,
         {ARG_ARRAY_TYPE_ANY_1, FunctionArgumentTypeOptions()
                                    .set_uses_array_element_for_collation()}},
        FN_RANGE_BUCKET,
        FunctionSignatureOptions().set_uses_operation_collation()}},
      FunctionOptions().set_pre_resolution_argument_constraint(
          &CheckRangeBucketArguments));

  InsertSimpleFunction(
      functions, options, "bit_cast_to_int32", SCALAR,
      {{int32_type, {int32_type}, FN_BIT_CAST_INT32_TO_INT32},
       {int32_type, {uint32_type}, FN_BIT_CAST_UINT32_TO_INT32}},
      FunctionOptions().set_allow_external_usage(false));
  InsertSimpleFunction(
      functions, options, "bit_cast_to_int64", SCALAR,
      {{int64_type, {int64_type}, FN_BIT_CAST_INT64_TO_INT64},
       {int64_type, {uint64_type}, FN_BIT_CAST_UINT64_TO_INT64}},
      FunctionOptions().set_allow_external_usage(false));
  InsertSimpleFunction(
      functions, options, "bit_cast_to_uint32", SCALAR,
      {{uint32_type, {uint32_type}, FN_BIT_CAST_UINT32_TO_UINT32},
       {uint32_type, {int32_type}, FN_BIT_CAST_INT32_TO_UINT32}},
      FunctionOptions().set_allow_external_usage(false));
  InsertSimpleFunction(
      functions, options, "bit_cast_to_uint64", SCALAR,
      {{uint64_type, {uint64_type}, FN_BIT_CAST_UINT64_TO_UINT64},
       {uint64_type, {int64_type}, FN_BIT_CAST_INT64_TO_UINT64}},
      FunctionOptions().set_allow_external_usage(false));

  FunctionOptions function_is_stable;
  function_is_stable.set_volatility(FunctionEnums::STABLE);

  InsertSimpleFunction(functions, options, "session_user", SCALAR,
                       {{string_type, {}, FN_SESSION_USER}},
                       function_is_stable);

  FunctionOptions function_is_volatile;
  function_is_volatile.set_volatility(FunctionEnums::VOLATILE);

  InsertSimpleFunction(functions, options, "rand", SCALAR,
                       {{double_type, {}, FN_RAND}}, function_is_volatile);

  InsertSimpleFunction(functions, options, "generate_uuid", SCALAR,
                       {{string_type, {}, FN_GENERATE_UUID}},
                       function_is_volatile);

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
    // This is added to the catalog to handle collisions correctly if a similar
    // function is defined by an engine. Also, it allows us to use
    // FunctionSignatureOptions to define constraints and deprecation info for
    // this special function.
    InsertSimpleFunction(
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
    InsertSimpleFunction(functions, options, "enum_value_descriptor_proto",
                         SCALAR,
                         {{enum_value_descriptor_proto_type,
                           {ARG_ENUM_ANY},
                           FN_ENUM_VALUE_DESCRIPTOR_PROTO}},
                         FunctionOptions().set_compute_result_type_callback(
                             &GetOrMakeEnumValueDescriptorType));
  }
}

// This function requires <type_factory>, <functions> to be not nullptr.
void GetSubscriptFunctions(TypeFactory* type_factory,
                           const ZetaSQLBuiltinFunctionOptions& options,
                           NameToFunctionMap* functions) {
  const Function::Mode SCALAR = Function::SCALAR;
  const Type* int64_type = type_factory->get_int64();

  // array[OFFSET(i)] gets an array element by zero-based position.
  // array[ORDINAL(i)] gets an array element by one-based position.
  // If the array or offset is NULL, a NULL of the array element type is
  // returned. If the position is off either end of the array a OUT_OF_RANGE
  // error is returned. The SAFE_ variants of the functions have the same
  // semantics with the exception of returning NULL rather than OUT_OF_RANGE
  // for a position that is out of bounds.
  InsertFunction(
      functions, options, "$array_at_offset", SCALAR,
      {{ARG_TYPE_ANY_1,
        {
            {ARG_ARRAY_TYPE_ANY_1, FunctionArgumentTypeOptions()
                                       .set_uses_array_element_for_collation()},
            int64_type,
        },
        FN_ARRAY_AT_OFFSET}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("array[offset()]")
          .set_get_sql_callback(&ArrayAtOffsetFunctionSQL));
  InsertFunction(
      functions, options, "$array_at_ordinal", SCALAR,
      {{ARG_TYPE_ANY_1,
        {{ARG_ARRAY_TYPE_ANY_1,
          FunctionArgumentTypeOptions().set_uses_array_element_for_collation()},
         int64_type},
        FN_ARRAY_AT_ORDINAL}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("array[ordinal()]")
          .set_get_sql_callback(&ArrayAtOrdinalFunctionSQL));
  InsertFunction(
      functions, options, "$safe_array_at_offset", SCALAR,
      {{ARG_TYPE_ANY_1,
        {{ARG_ARRAY_TYPE_ANY_1,
          FunctionArgumentTypeOptions().set_uses_array_element_for_collation()},
         int64_type},
        FN_SAFE_ARRAY_AT_OFFSET}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("array[safe_offset()]")
          .set_get_sql_callback(&SafeArrayAtOffsetFunctionSQL));
  InsertFunction(
      functions, options, "$safe_array_at_ordinal", SCALAR,
      {{ARG_TYPE_ANY_1,
        {{ARG_ARRAY_TYPE_ANY_1,
          FunctionArgumentTypeOptions().set_uses_array_element_for_collation()},
         int64_type},
        FN_SAFE_ARRAY_AT_ORDINAL}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("array[safe_ordinal()]")
          .set_get_sql_callback(&SafeArrayAtOrdinalFunctionSQL));

  // array[KEY(key)] gets the array element corresponding to key if present, or
  // an error if not present.
  // array[SAFE_KEY(key)] gets the array element corresponding to a key if
  // present, or else NULL.
  // In both cases, if the array or the arg is NULL, the result is NULL.
  InsertSimpleFunction(functions, options, "$proto_map_at_key", SCALAR,
                       {{ARG_PROTO_MAP_VALUE_ANY,
                         {ARG_PROTO_MAP_ANY, ARG_PROTO_MAP_KEY_ANY},
                         FN_PROTO_MAP_AT_KEY}},
                       FunctionOptions()
                           .set_supports_safe_error_mode(false)
                           .set_sql_name("array[key()]")
                           .set_get_sql_callback(&ProtoMapAtKeySQL)
                           .add_required_language_feature(
                               LanguageFeature::FEATURE_V_1_3_PROTO_MAPS));
  InsertSimpleFunction(functions, options, "$safe_proto_map_at_key", SCALAR,
                       {{ARG_PROTO_MAP_VALUE_ANY,
                         {ARG_PROTO_MAP_ANY, ARG_PROTO_MAP_KEY_ANY},
                         FN_SAFE_PROTO_MAP_AT_KEY}},
                       FunctionOptions()
                           .set_supports_safe_error_mode(false)
                           .set_sql_name("array[safe_key()]")
                           .set_get_sql_callback(&SafeProtoMapAtKeySQL)
                           .add_required_language_feature(
                               LanguageFeature::FEATURE_V_1_3_PROTO_MAPS));

  // The analyzer has been extended to recognize the subscript operator ([]).
  // ZetaSQLcurrently only supports this for JSON, iff the JSON feature is
  // enabled.
  //
  // The analyzer has also been extended to recognize generic subscript
  // with offset/ordinal syntax, but ZetaSQL has not defined any type that
  // actually supports this yet.
  std::vector<FunctionSignatureOnHeap> function_signatures;
  if (options.language_options.LanguageFeatureEnabled(FEATURE_JSON_TYPE)) {
    const Type* int64_type = type_factory->get_int64();
    const Type* json_type = types::JsonType();
    const Type* string_type = type_factory->get_string();
    function_signatures.push_back(
        {json_type, {json_type, int64_type}, FN_JSON_SUBSCRIPT_INT64});
    function_signatures.push_back(
        {json_type, {json_type, string_type}, FN_JSON_SUBSCRIPT_STRING});
  }
  InsertFunction(functions, options, "$subscript", Function::SCALAR,
                 function_signatures,
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_get_sql_callback(&SubscriptFunctionSQL)
                     .set_hide_supported_signatures(true)
                     .set_no_matching_signature_callback(
                         absl::bind_front(&NoMatchingSignatureForSubscript,
                                          /*offset_or_ordinal=*/"")));

  // Create functions with no signatures for other subscript functions
  // that have special handling in the analyzer.
  const std::vector<FunctionSignatureOnHeap> empty_signatures;
  InsertFunction(functions, options, "$subscript_with_key", Function::SCALAR,
                 empty_signatures,
                 FunctionOptions()
                     .set_supports_safe_error_mode(true)
                     .set_get_sql_callback(&SubscriptWithKeyFunctionSQL)
                     .set_hide_supported_signatures(true)
                     .set_no_matching_signature_callback(
                         absl::bind_front(&NoMatchingSignatureForSubscript,
                                          /*offset_or_ordinal=*/"KEY")));
  InsertFunction(functions, options, "$subscript_with_offset", Function::SCALAR,
                 empty_signatures,
                 FunctionOptions()
                     .set_supports_safe_error_mode(true)
                     .set_get_sql_callback(&SubscriptWithOffsetFunctionSQL)
                     .set_hide_supported_signatures(true)
                     .set_no_matching_signature_callback(
                         absl::bind_front(&NoMatchingSignatureForSubscript,
                                          /*offset_or_ordinal=*/"OFFSET")));
  InsertFunction(functions, options, "$subscript_with_ordinal",
                 Function::SCALAR, empty_signatures,
                 FunctionOptions()
                     .set_supports_safe_error_mode(true)
                     .set_get_sql_callback(&SubscriptWithOrdinalFunctionSQL)
                     .set_hide_supported_signatures(true)
                     .set_no_matching_signature_callback(
                         absl::bind_front(&NoMatchingSignatureForSubscript,
                                          /*offset_or_ordinal=*/"ORDINAL")));
}

void GetJSONFunctions(TypeFactory* type_factory,
                      const ZetaSQLBuiltinFunctionOptions& options,
                      NameToFunctionMap* functions) {
  const Type* int32_type = types::Int32Type();
  const Type* int64_type = types::Int64Type();
  const Type* uint32_type = types::Uint32Type();
  const Type* uint64_type = types::Uint64Type();
  const Type* double_type = types::DoubleType();
  const Type* float_type = types::FloatType();
  const Type* bool_type = type_factory->get_bool();
  const Type* string_type = type_factory->get_string();
  const Type* json_type = types::JsonType();
  const ArrayType* array_int32_type = types::Int32ArrayType();
  const ArrayType* array_int64_type = types::Int64ArrayType();
  const ArrayType* array_uint32_type = types::Uint32ArrayType();
  const ArrayType* array_uint64_type = types::Uint64ArrayType();
  const ArrayType* array_double_type = types::DoubleArrayType();
  const ArrayType* array_float_type = types::FloatArrayType();
  const ArrayType* array_bool_type = types::BoolArrayType();
  const ArrayType* array_string_type = types::StringArrayType();
  const ArrayType* array_json_type = types::JsonArrayType();

  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality REPEATED =
      FunctionArgumentType::REPEATED;

  FunctionArgumentTypeOptions json_path_argument_options;
  FunctionArgumentTypeOptions repeated_json_path_argument_options(REPEATED);
  FunctionArgumentTypeOptions optional_json_path_argument_options(
      FunctionArgumentType::OPTIONAL);
  optional_json_path_argument_options.set_default(Value::String("$"));
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_ENABLE_CONSTANT_EXPRESSION_IN_JSON_PATH)) {
    json_path_argument_options.set_must_be_constant_expression();
    repeated_json_path_argument_options.set_must_be_constant_expression();
    optional_json_path_argument_options.set_must_be_constant_expression();
  } else {
    json_path_argument_options.set_must_be_constant();
    repeated_json_path_argument_options.set_must_be_constant();
    optional_json_path_argument_options.set_must_be_constant();
  }

  FunctionArgumentType json_path_argument =
      FunctionArgumentType(string_type, json_path_argument_options);
  FunctionArgumentType repeated_json_path_argument =
      FunctionArgumentType(string_type, repeated_json_path_argument_options);
  FunctionArgumentType optional_json_path_argument =
      FunctionArgumentType(string_type, optional_json_path_argument_options);

  std::vector<FunctionSignatureOnHeap> json_extract_signatures = {
      {string_type,
       {string_type, json_path_argument},
       FN_JSON_EXTRACT,
       FunctionSignatureOptions().set_rejects_collation()}};
  std::vector<FunctionSignatureOnHeap> json_query_signatures = {
      {string_type,
       {string_type, json_path_argument},
       FN_JSON_QUERY,
       FunctionSignatureOptions().set_rejects_collation()}};
  std::vector<FunctionSignatureOnHeap> json_extract_scalar_signatures = {
      {string_type,
       {string_type, optional_json_path_argument},
       FN_JSON_EXTRACT_SCALAR,
       FunctionSignatureOptions().set_rejects_collation()}};
  std::vector<FunctionSignatureOnHeap> json_value_signatures = {
      {string_type,
       {string_type, optional_json_path_argument},
       FN_JSON_VALUE,
       FunctionSignatureOptions().set_rejects_collation()}};

  std::vector<FunctionSignatureOnHeap> json_extract_array_signatures = {
      {array_string_type,
       {string_type, optional_json_path_argument},
       FN_JSON_EXTRACT_ARRAY,
       FunctionSignatureOptions().set_rejects_collation()}};
  std::vector<FunctionSignatureOnHeap> json_extract_string_array_signatures = {
      {array_string_type,
       {string_type, optional_json_path_argument},
       FN_JSON_EXTRACT_STRING_ARRAY,
       FunctionSignatureOptions().set_rejects_collation()}};
  std::vector<FunctionSignatureOnHeap> json_query_array_signatures = {
      {array_string_type,
       {string_type, optional_json_path_argument},
       FN_JSON_QUERY_ARRAY,
       FunctionSignatureOptions().set_rejects_collation()}};
  std::vector<FunctionSignatureOnHeap> json_value_array_signatures = {
      {array_string_type,
       {string_type, optional_json_path_argument},
       FN_JSON_VALUE_ARRAY,
       FunctionSignatureOptions().set_rejects_collation()}};

  if (options.language_options.LanguageFeatureEnabled(FEATURE_JSON_TYPE)) {
    json_extract_signatures.push_back(
        {json_type, {json_type, json_path_argument}, FN_JSON_EXTRACT_JSON});
    json_query_signatures.push_back(
        {json_type, {json_type, json_path_argument}, FN_JSON_QUERY_JSON});
    json_extract_scalar_signatures.push_back(
        {string_type,
         {json_type, optional_json_path_argument},
         FN_JSON_EXTRACT_SCALAR_JSON});
    json_value_signatures.push_back({string_type,
                                     {json_type, optional_json_path_argument},
                                     FN_JSON_VALUE_JSON});
    json_extract_array_signatures.push_back(
        {array_json_type,
         {json_type, optional_json_path_argument},
         FN_JSON_EXTRACT_ARRAY_JSON});
    json_extract_string_array_signatures.push_back(
        {array_string_type,
         {json_type, optional_json_path_argument},
         FN_JSON_EXTRACT_STRING_ARRAY_JSON});
    json_query_array_signatures.push_back(
        {array_json_type,
         {json_type, optional_json_path_argument},
         FN_JSON_QUERY_ARRAY_JSON});
    json_value_array_signatures.push_back(
        {array_string_type,
         {json_type, optional_json_path_argument},
         FN_JSON_VALUE_ARRAY_JSON});

    InsertFunction(functions, options, "to_json", SCALAR,
                   {{json_type,
                     {ARG_TYPE_ANY_1,
                      {bool_type, FunctionArgumentTypeOptions()
                                      .set_cardinality(FunctionEnums::OPTIONAL)
                                      .set_argument_name(
                                          "stringify_wide_numbers", kNamedOnly)
                                      .set_default(values::Bool(false))}},
                     FN_TO_JSON}});
    InsertFunction(
        functions, options, "parse_json", SCALAR,
        {{json_type,
          {string_type,
           FunctionArgumentType(
               string_type,
               FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
                   .set_argument_name("wide_number_mode", kNamedOnly)
                   .set_default(Value::String("exact")))},
          FN_PARSE_JSON}});

    if (options.language_options.LanguageFeatureEnabled(
            FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS)) {
      zetasql::FunctionOptions function_options;
      if (options.language_options.product_mode() == PRODUCT_INTERNAL) {
        function_options.set_alias_name("double");
      }
      InsertFunction(functions, options, "int64", SCALAR,
                     {{int64_type, {json_type}, FN_JSON_TO_INT64}});
      InsertFunction(functions, options, "float64", SCALAR,
                     {{double_type,
                       {json_type,
                        {string_type,
                         FunctionArgumentTypeOptions()
                             .set_cardinality(FunctionEnums::OPTIONAL)
                             .set_argument_name("wide_number_mode", kNamedOnly)
                             .set_default(Value::String("round"))}},
                       FN_JSON_TO_DOUBLE}},
                     function_options);
      InsertFunction(functions, options, "bool", SCALAR,
                     {{bool_type, {json_type}, FN_JSON_TO_BOOL}});
      InsertFunction(functions, options, "json_type", SCALAR,
                     {{string_type, {json_type}, FN_JSON_TYPE}});
    }

    if (options.language_options.LanguageFeatureEnabled(
            FEATURE_JSON_LAX_VALUE_EXTRACTION_FUNCTIONS)) {
      InsertFunction(functions, options, "lax_bool", SCALAR,
                     {{bool_type, {json_type}, FN_JSON_LAX_TO_BOOL}});
      InsertFunction(functions, options, "lax_int64", SCALAR,
                     {{int64_type, {json_type}, FN_JSON_LAX_TO_INT64}});
      zetasql::FunctionOptions function_options;
      if (options.language_options.product_mode() == PRODUCT_INTERNAL) {
        function_options.set_alias_name("lax_double");
      }
      InsertFunction(functions, options, "lax_float64", SCALAR,
                     {{double_type, {json_type}, FN_JSON_LAX_TO_DOUBLE}},
                     function_options);
      InsertFunction(functions, options, "lax_string", SCALAR,
                     {{string_type, {json_type}, FN_JSON_LAX_TO_STRING}});
    }

    if (options.language_options.LanguageFeatureEnabled(
            FEATURE_V_1_4_JSON_ARRAY_VALUE_EXTRACTION_FUNCTIONS)) {
      InsertFunction(functions, options, "bool_array", SCALAR,
                     {{array_bool_type, {json_type}, FN_JSON_TO_BOOL_ARRAY}});

      InsertFunction(
          functions, options, "float64_array", SCALAR,
          {{array_double_type,
            {json_type,
             {string_type,
              FunctionArgumentTypeOptions()
                  .set_cardinality(FunctionEnums::OPTIONAL)
                  .set_argument_name("wide_number_mode", kNamedOnly)
                  .set_default(Value::String("round"))}},
            FN_JSON_TO_FLOAT64_ARRAY}},
          zetasql::FunctionOptions().set_alias_name(
              options.language_options.product_mode() == PRODUCT_INTERNAL
                  ? "double_array"
                  : ""));

      InsertFunction(functions, options, "int64_array", SCALAR,
                     {{array_int64_type, {json_type}, FN_JSON_TO_INT64_ARRAY}});

      InsertFunction(
          functions, options, "string_array", SCALAR,
          {{array_string_type, {json_type}, FN_JSON_TO_STRING_ARRAY}});

      if (options.language_options.LanguageFeatureEnabled(
              FEATURE_JSON_LAX_VALUE_EXTRACTION_FUNCTIONS)) {
        InsertFunction(
            functions, options, "lax_bool_array", SCALAR,
            {{array_bool_type, {json_type}, FN_JSON_LAX_TO_BOOL_ARRAY}});
        InsertFunction(
            functions, options, "lax_float64_array", SCALAR,
            {{array_double_type, {json_type}, FN_JSON_LAX_TO_FLOAT64_ARRAY}},
            zetasql::FunctionOptions().set_alias_name(
                options.language_options.product_mode() == PRODUCT_INTERNAL
                    ? "lax_double_array"
                    : ""));
        InsertFunction(
            functions, options, "lax_int64_array", SCALAR,
            {{array_int64_type, {json_type}, FN_JSON_LAX_TO_INT64_ARRAY}});
        InsertFunction(
            functions, options, "lax_string_array", SCALAR,
            {{array_string_type, {json_type}, FN_JSON_LAX_TO_STRING_ARRAY}});
      }
    }

    if (options.language_options.LanguageFeatureEnabled(
            FEATURE_V_1_4_JSON_MORE_VALUE_EXTRACTION_FUNCTIONS)) {
      InsertFunction(functions, options, "int32", SCALAR,
                     {{int32_type, {json_type}, FN_JSON_TO_INT32}});
      InsertFunction(functions, options, "uint32", SCALAR,
                     {{uint32_type, {json_type}, FN_JSON_TO_UINT32}});
      InsertFunction(functions, options, "uint64", SCALAR,
                     {{uint64_type, {json_type}, FN_JSON_TO_UINT64}});

      if (!options.language_options.LanguageFeatureEnabled(
              FEATURE_V_1_4_DISABLE_FLOAT32)) {
        InsertFunction(
            functions, options, "float32", SCALAR,
            {{float_type,
              {json_type,
               {string_type,
                FunctionArgumentTypeOptions()
                    .set_cardinality(FunctionEnums::OPTIONAL)
                    .set_argument_name("wide_number_mode", kNamedOnly)
                    .set_default(Value::String("round"))}},
              FN_JSON_TO_FLOAT32}},
            zetasql::FunctionOptions().set_alias_name(
                options.language_options.product_mode() == PRODUCT_INTERNAL
                    ? "float"
                    : ""));
      }

      if (options.language_options.LanguageFeatureEnabled(
              FEATURE_JSON_LAX_VALUE_EXTRACTION_FUNCTIONS)) {
        InsertFunction(functions, options, "lax_int32", SCALAR,
                       {{int32_type, {json_type}, FN_JSON_LAX_TO_INT32}});
        InsertFunction(functions, options, "lax_uint32", SCALAR,
                       {{uint32_type, {json_type}, FN_JSON_LAX_TO_UINT32}});
        InsertFunction(functions, options, "lax_uint64", SCALAR,
                       {{uint64_type, {json_type}, FN_JSON_LAX_TO_UINT64}});

        if (!options.language_options.LanguageFeatureEnabled(
                FEATURE_V_1_4_DISABLE_FLOAT32)) {
          InsertFunction(
              functions, options, "lax_float32", SCALAR,
              {{float_type, {json_type}, FN_JSON_LAX_TO_FLOAT32}},
              zetasql::FunctionOptions().set_alias_name(
                  options.language_options.product_mode() == PRODUCT_INTERNAL
                      ? "lax_float"
                      : ""));
        }
      }

      if (options.language_options.LanguageFeatureEnabled(
              FEATURE_V_1_4_JSON_ARRAY_VALUE_EXTRACTION_FUNCTIONS)) {
        InsertFunction(
            functions, options, "int32_array", SCALAR,
            {{array_int32_type, {json_type}, FN_JSON_TO_INT32_ARRAY}});
        InsertFunction(
            functions, options, "uint32_array", SCALAR,
            {{array_uint32_type, {json_type}, FN_JSON_TO_UINT32_ARRAY}});
        InsertFunction(
            functions, options, "uint64_array", SCALAR,
            {{array_uint64_type, {json_type}, FN_JSON_TO_UINT64_ARRAY}});

        if (!options.language_options.LanguageFeatureEnabled(
                FEATURE_V_1_4_DISABLE_FLOAT32)) {
          InsertFunction(
              functions, options, "float32_array", SCALAR,
              {{array_float_type,
                {json_type,
                 {string_type,
                  FunctionArgumentTypeOptions()
                      .set_cardinality(FunctionEnums::OPTIONAL)
                      .set_argument_name("wide_number_mode", kNamedOnly)
                      .set_default(Value::String("round"))}},
                FN_JSON_TO_FLOAT32_ARRAY}},
              zetasql::FunctionOptions().set_alias_name(
                  options.language_options.product_mode() == PRODUCT_INTERNAL
                      ? "float_array"
                      : ""));
        }

        if (options.language_options.LanguageFeatureEnabled(
                FEATURE_JSON_LAX_VALUE_EXTRACTION_FUNCTIONS)) {
          InsertFunction(
              functions, options, "lax_int32_array", SCALAR,
              {{array_int32_type, {json_type}, FN_JSON_LAX_TO_INT32_ARRAY}});
          InsertFunction(
              functions, options, "lax_uint32_array", SCALAR,
              {{array_uint32_type, {json_type}, FN_JSON_LAX_TO_UINT32_ARRAY}});
          InsertFunction(
              functions, options, "lax_uint64_array", SCALAR,
              {{array_uint64_type, {json_type}, FN_JSON_LAX_TO_UINT64_ARRAY}});

          if (!options.language_options.LanguageFeatureEnabled(
                  FEATURE_V_1_4_DISABLE_FLOAT32)) {
            InsertFunction(
                functions, options, "lax_float32_array", SCALAR,
                {{array_float_type, {json_type}, FN_JSON_LAX_TO_FLOAT32_ARRAY}},
                zetasql::FunctionOptions().set_alias_name(
                    options.language_options.product_mode() == PRODUCT_INTERNAL
                        ? "lax_float_array"
                        : ""));
          }
        }
      }
    }
  }

  InsertFunction(
      functions, options, "json_array", SCALAR,
      {{json_type,
        {{ARG_TYPE_ARBITRARY,
          FunctionArgumentTypeOptions().set_cardinality(REPEATED)}},
        FN_JSON_ARRAY}},
      FunctionOptions()
          .add_required_language_feature(FEATURE_JSON_TYPE)
          .add_required_language_feature(FEATURE_JSON_CONSTRUCTOR_FUNCTIONS));

  InsertFunction(functions, options, "json_extract", SCALAR,
                 json_extract_signatures);
  InsertFunction(functions, options, "json_query", SCALAR,
                 json_query_signatures);
  InsertFunction(functions, options, "json_extract_scalar", SCALAR,
                 json_extract_scalar_signatures);
  InsertFunction(functions, options, "json_value", SCALAR,
                 json_value_signatures);

  InsertFunction(functions, options, "json_extract_array", SCALAR,
                 json_extract_array_signatures);

  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_JSON_ARRAY_FUNCTIONS)) {
    InsertFunction(functions, options, "json_extract_string_array", SCALAR,
                   json_extract_string_array_signatures);
    InsertFunction(functions, options, "json_query_array", SCALAR,
                   json_query_array_signatures);
    InsertFunction(functions, options, "json_value_array", SCALAR,
                   json_value_array_signatures);
  }

  InsertFunction(
      functions, options, "to_json_string", SCALAR,
      {{string_type,
        {ARG_TYPE_ANY_1, {bool_type, FunctionArgumentType::OPTIONAL}},
        FN_TO_JSON_STRING,
        FunctionSignatureOptions().set_propagates_collation(false)}});

  InsertFunction(
      functions, options, "json_object", SCALAR,
      {{json_type,
        {{string_type, FunctionArgumentTypeOptions().set_cardinality(REPEATED)},
         {ARG_TYPE_ARBITRARY,
          FunctionArgumentTypeOptions().set_cardinality(REPEATED)}},
        FN_JSON_OBJECT},
       {json_type,
        {{array_string_type}, {ARG_ARRAY_TYPE_ANY_1}},
        FN_JSON_OBJECT_ARRAYS}},
      FunctionOptions()
          .add_required_language_feature(FEATURE_JSON_TYPE)
          .add_required_language_feature(FEATURE_JSON_CONSTRUCTOR_FUNCTIONS));

  InsertFunction(
      functions, options, "json_remove", SCALAR,
      {{json_type,
        {json_type, json_path_argument, repeated_json_path_argument},
        FN_JSON_REMOVE}},
      FunctionOptions()
          .add_required_language_feature(FEATURE_JSON_TYPE)
          .add_required_language_feature(FEATURE_JSON_MUTATOR_FUNCTIONS));

  FunctionArgumentType first_json_path = json_path_argument;
  FunctionArgumentType first_set_value{ARG_TYPE_ARBITRARY};
  FunctionArgumentType remaining_json_paths = repeated_json_path_argument;
  FunctionArgumentType remaining_set_values{
      ARG_TYPE_ARBITRARY,
      FunctionArgumentTypeOptions().set_cardinality(REPEATED)};
  FunctionArgumentType create_if_missing{
      bool_type, FunctionArgumentTypeOptions()
                     .set_cardinality(FunctionEnums::OPTIONAL)
                     .set_must_be_constant()
                     .set_argument_name("create_if_missing", kNamedOnly)
                     .set_default(Value::Bool(true))};

  InsertFunction(
      functions, options, "json_set", SCALAR,
      {{json_type,
        {json_type, first_json_path, first_set_value, remaining_json_paths,
         remaining_set_values, create_if_missing},
        FN_JSON_SET}},
      FunctionOptions()
          .add_required_language_feature(FEATURE_JSON_TYPE)
          .add_required_language_feature(FEATURE_JSON_MUTATOR_FUNCTIONS));

  InsertFunction(
      functions, options, "json_strip_nulls", SCALAR,
      {{json_type,
        {json_type,
         optional_json_path_argument,
         {bool_type, FunctionArgumentTypeOptions()
                         .set_cardinality(FunctionEnums::OPTIONAL)
                         .set_must_be_constant()
                         .set_argument_name("include_arrays", kNamedOnly)
                         .set_default(Value::Bool(true))},
         {bool_type, FunctionArgumentTypeOptions()
                         .set_cardinality(FunctionEnums::OPTIONAL)
                         .set_must_be_constant()
                         .set_argument_name("remove_empty", kNamedOnly)
                         .set_default(Value::Bool(false))}},
        FN_JSON_STRIP_NULLS}},
      FunctionOptions()
          .add_required_language_feature(FEATURE_JSON_TYPE)
          .add_required_language_feature(FEATURE_JSON_MUTATOR_FUNCTIONS));

  InsertFunction(
      functions, options, "json_array_insert", SCALAR,
      {{json_type,
        {json_type,
         // Ensure at least one repetition ...
         json_path_argument,
         {ARG_TYPE_ARBITRARY, FunctionArgumentTypeOptions()},
         // ... Then any number of additional pairs of args.
         repeated_json_path_argument,
         {ARG_TYPE_ARBITRARY,
          FunctionArgumentTypeOptions().set_cardinality(REPEATED)},
         {bool_type, FunctionArgumentTypeOptions()
                         .set_cardinality(FunctionEnums::OPTIONAL)
                         .set_must_be_constant()
                         .set_argument_name("insert_each_element", kNamedOnly)
                         .set_default(Value::Bool(true))}},
        FN_JSON_ARRAY_INSERT}},
      FunctionOptions()
          .add_required_language_feature(FEATURE_JSON_TYPE)
          .add_required_language_feature(FEATURE_JSON_MUTATOR_FUNCTIONS));

  InsertFunction(
      functions, options, "json_array_append", SCALAR,
      {{json_type,
        {json_type,
         // Ensure at least one repetition ...
         json_path_argument,
         {ARG_TYPE_ARBITRARY, FunctionArgumentTypeOptions()},
         // ... Then any number of additional pairs of args.
         repeated_json_path_argument,
         {ARG_TYPE_ARBITRARY,
          FunctionArgumentTypeOptions().set_cardinality(REPEATED)},
         {bool_type, FunctionArgumentTypeOptions()
                         .set_cardinality(FunctionEnums::OPTIONAL)
                         .set_must_be_constant()
                         .set_argument_name("append_each_element", kNamedOnly)
                         .set_default(Value::Bool(true))}},
        FN_JSON_ARRAY_APPEND}},
      FunctionOptions()
          .add_required_language_feature(FEATURE_JSON_TYPE)
          .add_required_language_feature(FEATURE_JSON_MUTATOR_FUNCTIONS));
}

absl::Status GetNumericFunctions(TypeFactory* type_factory,
                                 const ZetaSQLBuiltinFunctionOptions& options,
                                 NameToFunctionMap* functions,
                                 NameToTypeMap* types) {
  const Type* bool_type = type_factory->get_bool();
  const Type* int32_type = type_factory->get_int32();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint32_type = type_factory->get_uint32();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* float_type = type_factory->get_float();
  const Type* double_type = type_factory->get_double();
  const Type* numeric_type = type_factory->get_numeric();
  const Type* bignumeric_type = type_factory->get_bignumeric();
  const Type* string_type = type_factory->get_string();
  const Type* rounding_mode_type = types::RoundingModeEnumType();

  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality REPEATED =
      FunctionArgumentType::REPEATED;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  FunctionSignatureOptions has_floating_point_argument;
  has_floating_point_argument.set_constraints(&CheckHasFloatingPointArgument);
  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&CheckHasNumericTypeArgument);
  FunctionSignatureOptions has_bignumeric_type_argument;
  has_bignumeric_type_argument.set_constraints(&CheckHasBigNumericTypeArgument);

  InsertSimpleFunction(
      functions, options, "abs", SCALAR,
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

  // Only add in the third argument ROUND functions if the feature is enabled.
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_ROUND_WITH_ROUNDING_MODE)) {
    ZETASQL_RETURN_IF_ERROR(InsertFunctionAndTypes(
        functions, types, options, "round", SCALAR,
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
          has_bignumeric_type_argument},
         {numeric_type,
          {numeric_type, int64_type, rounding_mode_type},
          FN_ROUND_WITH_ROUNDING_MODE_NUMERIC,
          has_numeric_type_argument},
         {bignumeric_type,
          {bignumeric_type, int64_type, rounding_mode_type},
          FN_ROUND_WITH_ROUNDING_MODE_BIGNUMERIC,
          has_bignumeric_type_argument}},
        /* function_options=*/{}, {rounding_mode_type}));
  } else {
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
  }
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

  InsertSimpleFunction(functions, options, "is_inf", SCALAR,
                       {{bool_type, {double_type}, FN_IS_INF}});
  InsertSimpleFunction(functions, options, "is_nan", SCALAR,
                       {{bool_type, {double_type}, FN_IS_NAN}});

  InsertSimpleFunction(
      functions, options, "ieee_divide", SCALAR,
      {{double_type, {double_type, double_type}, FN_IEEE_DIVIDE_DOUBLE},
       {float_type, {float_type, float_type}, FN_IEEE_DIVIDE_FLOAT}});

  InsertFunction(
      functions, options, "greatest", SCALAR,
      {{ARG_TYPE_ANY_1,
        {{ARG_TYPE_ANY_1, REPEATED}},
        FN_GREATEST,
        FunctionSignatureOptions().set_uses_operation_collation()}},
      FunctionOptions().set_pre_resolution_argument_constraint(
          absl::bind_front(&CheckGreatestLeastArguments, "GREATEST")));

  InsertFunction(functions, options, "least", SCALAR,
                 {{ARG_TYPE_ANY_1,
                   {{ARG_TYPE_ANY_1, REPEATED}},
                   FN_LEAST,
                   FunctionSignatureOptions().set_uses_operation_collation()}},
                 FunctionOptions().set_pre_resolution_argument_constraint(
                     absl::bind_front(&CheckGreatestLeastArguments, "LEAST")));

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

  InsertFunction(functions, options, "pow", SCALAR,
                 {{double_type, {double_type, double_type}, FN_POW_DOUBLE},
                  {numeric_type,
                   {numeric_type, numeric_type},
                   FN_POW_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type, bignumeric_type},
                   FN_POW_BIGNUMERIC,
                   has_bignumeric_type_argument}},
                 FunctionOptions().set_alias_name("power"));
  InsertFunction(functions, options, "sqrt", SCALAR,
                 {{double_type, {double_type}, FN_SQRT_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_SQRT_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type},
                   FN_SQRT_BIGNUMERIC,
                   has_bignumeric_type_argument}});
  if (options.language_options.LanguageFeatureEnabled(FEATURE_CBRT_FUNCTIONS)) {
    InsertFunction(functions, options, "cbrt", SCALAR,
                   {{double_type, {double_type}, FN_CBRT_DOUBLE},
                    {numeric_type,
                     {numeric_type},
                     FN_CBRT_NUMERIC,
                     has_numeric_type_argument},
                    {bignumeric_type,
                     {bignumeric_type},
                     FN_CBRT_BIGNUMERIC,
                     has_bignumeric_type_argument}});
  }
  InsertFunction(functions, options, "exp", SCALAR,
                 {{double_type, {double_type}, FN_EXP_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_EXP_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type},
                   FN_EXP_BIGNUMERIC,
                   has_bignumeric_type_argument}});
  InsertFunction(functions, options, "ln", SCALAR,
                 {{double_type, {double_type}, FN_NATURAL_LOGARITHM_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_NATURAL_LOGARITHM_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type},
                   FN_NATURAL_LOGARITHM_BIGNUMERIC,
                   has_bignumeric_type_argument}});
  InsertFunction(functions, options, "log", SCALAR,
                 {{double_type,
                   {double_type, {double_type, OPTIONAL}},
                   FN_LOGARITHM_DOUBLE},
                  {numeric_type,
                   {numeric_type, {numeric_type, OPTIONAL}},
                   FN_LOGARITHM_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type, {bignumeric_type, OPTIONAL}},
                   FN_LOGARITHM_BIGNUMERIC,
                   has_bignumeric_type_argument}});
  InsertFunction(functions, options, "log10", SCALAR,
                 {{double_type, {double_type}, FN_DECIMAL_LOGARITHM_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_DECIMAL_LOGARITHM_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type},
                   FN_DECIMAL_LOGARITHM_BIGNUMERIC,
                   has_bignumeric_type_argument}});

  InsertSimpleFunction(functions, options, "parse_numeric", SCALAR,
                       {{numeric_type, {string_type}, FN_PARSE_NUMERIC}});
  InsertSimpleFunction(functions, options, "parse_bignumeric", SCALAR,
                       {{bignumeric_type, {string_type}, FN_PARSE_BIGNUMERIC}});
  return absl::OkStatus();
}

void GetTrigonometricFunctions(TypeFactory* type_factory,
                               const ZetaSQLBuiltinFunctionOptions& options,
                               NameToFunctionMap* functions) {
  const Type* double_type = type_factory->get_double();

  const Function::Mode SCALAR = Function::SCALAR;

  InsertSimpleFunction(functions, options, "cos", SCALAR,
                       {{double_type, {double_type}, FN_COS_DOUBLE}});
  InsertSimpleFunction(functions, options, "cosh", SCALAR,
                       {{double_type, {double_type}, FN_COSH_DOUBLE}});
  InsertSimpleFunction(functions, options, "acos", SCALAR,
                       {{double_type, {double_type}, FN_ACOS_DOUBLE}});
  InsertSimpleFunction(functions, options, "acosh", SCALAR,
                       {{double_type, {double_type}, FN_ACOSH_DOUBLE}});
  InsertSimpleFunction(functions, options, "sin", SCALAR,
                       {{double_type, {double_type}, FN_SIN_DOUBLE}});
  InsertSimpleFunction(functions, options, "sinh", SCALAR,
                       {{double_type, {double_type}, FN_SINH_DOUBLE}});
  InsertSimpleFunction(functions, options, "asin", SCALAR,
                       {{double_type, {double_type}, FN_ASIN_DOUBLE}});
  InsertSimpleFunction(functions, options, "asinh", SCALAR,
                       {{double_type, {double_type}, FN_ASINH_DOUBLE}});
  InsertSimpleFunction(functions, options, "tan", SCALAR,
                       {{double_type, {double_type}, FN_TAN_DOUBLE}});
  InsertSimpleFunction(functions, options, "tanh", SCALAR,
                       {{double_type, {double_type}, FN_TANH_DOUBLE}});
  InsertSimpleFunction(functions, options, "atan", SCALAR,
                       {{double_type, {double_type}, FN_ATAN_DOUBLE}});
  InsertSimpleFunction(functions, options, "atanh", SCALAR,
                       {{double_type, {double_type}, FN_ATANH_DOUBLE}});
  InsertSimpleFunction(
      functions, options, "atan2", SCALAR,
      {{double_type, {double_type, double_type}, FN_ATAN2_DOUBLE}});

  InsertSimpleFunction(functions, options, "csc", SCALAR,
                       {{double_type, {double_type}, FN_CSC_DOUBLE}});
  InsertSimpleFunction(functions, options, "sec", SCALAR,
                       {{double_type, {double_type}, FN_SEC_DOUBLE}});
  InsertSimpleFunction(functions, options, "cot", SCALAR,
                       {{double_type, {double_type}, FN_COT_DOUBLE}});
  InsertSimpleFunction(functions, options, "csch", SCALAR,
                       {{double_type, {double_type}, FN_CSCH_DOUBLE}});
  InsertSimpleFunction(functions, options, "sech", SCALAR,
                       {{double_type, {double_type}, FN_SECH_DOUBLE}});
  InsertSimpleFunction(functions, options, "coth", SCALAR,
                       {{double_type, {double_type}, FN_COTH_DOUBLE}});
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_4_PI_FUNCTIONS)) {
    constexpr absl::string_view kPiDoubleTemplate = R"sql(
    3.1415926535897931
    )sql";
    constexpr absl::string_view kPiNumericTemplate = R"sql(
    NUMERIC '3.141592654'
    )sql";
    constexpr absl::string_view kPiBigNumericTemplate = R"sql(
    BIGNUMERIC '3.1415926535897932384626433832795028842'
    )sql";
    InsertFunction(functions, options, "pi", SCALAR,
                   {{double_type,
                     {},
                     FN_PI_DOUBLE,
                     SetDefinitionForInlining(kPiDoubleTemplate)}});
    InsertFunction(functions, options, "pi_numeric", SCALAR,
                   {{type_factory->get_numeric(),
                     {},
                     FN_PI_NUMERIC,
                     SetDefinitionForInlining(kPiNumericTemplate)}});
    InsertFunction(functions, options, "pi_bignumeric", SCALAR,
                   {{type_factory->get_bignumeric(),
                     {},
                     FN_PI_BIGNUMERIC,
                     SetDefinitionForInlining(kPiBigNumericTemplate)}});
  }
}

absl::Status GetMathFunctions(TypeFactory* type_factory,
                              const ZetaSQLBuiltinFunctionOptions& options,
                              NameToFunctionMap* functions,
                              NameToTypeMap* types) {
  ZETASQL_RETURN_IF_ERROR(GetNumericFunctions(type_factory, options, functions, types));
  GetTrigonometricFunctions(type_factory, options, functions);
  return absl::OkStatus();
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

  InsertSimpleNamespaceFunction(
      functions, options, "net", "format_ip", SCALAR,
      {{string_type, {int64_type}, FN_NET_FORMAT_IP}},
      FunctionOptions().set_allow_external_usage(false));
  InsertSimpleNamespaceFunction(
      functions, options, "net", "parse_ip", SCALAR,
      {{int64_type, {string_type}, FN_NET_PARSE_IP}},
      FunctionOptions().set_allow_external_usage(false));
  InsertSimpleNamespaceFunction(
      functions, options, "net", "format_packed_ip", SCALAR,
      {{string_type, {bytes_type}, FN_NET_FORMAT_PACKED_IP}},
      FunctionOptions().set_allow_external_usage(false));
  InsertSimpleNamespaceFunction(
      functions, options, "net", "parse_packed_ip", SCALAR,
      {{bytes_type, {string_type}, FN_NET_PARSE_PACKED_IP}},
      FunctionOptions().set_allow_external_usage(false));
  InsertSimpleNamespaceFunction(
      functions, options, "net", "ip_in_net", SCALAR,
      {{bool_type, {string_type, string_type}, FN_NET_IP_IN_NET}},
      FunctionOptions().set_allow_external_usage(false));
  InsertSimpleNamespaceFunction(
      functions, options, "net", "make_net", SCALAR,
      {{string_type, {string_type, int32_type}, FN_NET_MAKE_NET}},
      FunctionOptions().set_allow_external_usage(false));
  InsertSimpleNamespaceFunction(functions, options, "net", "host", SCALAR,
                                {{string_type, {string_type}, FN_NET_HOST}});
  InsertSimpleNamespaceFunction(
      functions, options, "net", "reg_domain", SCALAR,
      {{string_type, {string_type}, FN_NET_REG_DOMAIN}});
  InsertSimpleNamespaceFunction(
      functions, options, "net", "public_suffix", SCALAR,
      {{string_type, {string_type}, FN_NET_PUBLIC_SUFFIX}});
  InsertSimpleNamespaceFunction(
      functions, options, "net", "ip_from_string", SCALAR,
      {{bytes_type, {string_type}, FN_NET_IP_FROM_STRING}});
  InsertSimpleNamespaceFunction(
      functions, options, "net", "safe_ip_from_string", SCALAR,
      {{bytes_type, {string_type}, FN_NET_SAFE_IP_FROM_STRING}});
  InsertSimpleNamespaceFunction(
      functions, options, "net", "ip_to_string", SCALAR,
      {{string_type, {bytes_type}, FN_NET_IP_TO_STRING}});
  InsertSimpleNamespaceFunction(
      functions, options, "net", "ip_net_mask", SCALAR,
      {{bytes_type, {int64_type, int64_type}, FN_NET_IP_NET_MASK}});
  InsertSimpleNamespaceFunction(
      functions, options, "net", "ip_trunc", SCALAR,
      {{bytes_type, {bytes_type, int64_type}, FN_NET_IP_TRUNC}});
  InsertSimpleNamespaceFunction(
      functions, options, "net", "ipv4_from_int64", SCALAR,
      {{bytes_type, {int64_type}, FN_NET_IPV4_FROM_INT64}});
  InsertSimpleNamespaceFunction(
      functions, options, "net", "ipv4_to_int64", SCALAR,
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
  const Type* bignumeric_type = type_factory->get_bignumeric();

  const Function::Mode AGGREGATE = Function::AGGREGATE;
  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
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

  const Function::Mode AGGREGATE = Function::AGGREGATE;
  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
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

  const Function::Mode AGGREGATE = Function::AGGREGATE;
  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
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
        // TODO: Add support for interpolation option for all merge/
        // extract/merge_point/extract_point functions.
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

void GetHashingFunctions(TypeFactory* type_factory,
                         const ZetaSQLBuiltinFunctionOptions& options,
                         NameToFunctionMap* functions) {
  const Function::Mode SCALAR = Function::SCALAR;
  const Type* int64_type = types::Int64Type();
  const Type* string_type = types::StringType();
  const Type* bytes_type = types::BytesType();

  InsertSimpleFunction(functions, options, "md5", SCALAR,
                       {{bytes_type, {bytes_type}, FN_MD5_BYTES},
                        {bytes_type, {string_type}, FN_MD5_STRING}});
  InsertSimpleFunction(functions, options, "sha1", SCALAR,
                       {{bytes_type, {bytes_type}, FN_SHA1_BYTES},
                        {bytes_type, {string_type}, FN_SHA1_STRING}});
  InsertSimpleFunction(functions, options, "sha256", SCALAR,
                       {{bytes_type, {bytes_type}, FN_SHA256_BYTES},
                        {bytes_type, {string_type}, FN_SHA256_STRING}});
  InsertSimpleFunction(functions, options, "sha512", SCALAR,
                       {{bytes_type, {bytes_type}, FN_SHA512_BYTES},
                        {bytes_type, {string_type}, FN_SHA512_STRING}});

  InsertSimpleFunction(
      functions, options, "farm_fingerprint", SCALAR,
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

  std::vector<zetasql::StructType::StructField> output_struct_fields = {
      {"kms_resource_name", types::StringType()},
      {"first_level_keyset", types::BytesType()},
      {"second_level_keyset", types::BytesType()}};
  const zetasql::StructType* keyset_chain_struct_type = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeStructType({output_struct_fields},
                                        &keyset_chain_struct_type));

  const FunctionArgumentTypeOptions const_arg_options =
      FunctionArgumentTypeOptions()
          .set_must_be_constant()
          .set_must_be_non_null();

  InsertNamespaceFunction(
      functions, options, "keys", "keyset_chain", SCALAR,
      {{keyset_chain_struct_type,
        {/*kms_resource_name=*/{string_type, const_arg_options},
         /*first_level_keyset=*/{bytes_type, const_arg_options},
         /*second_level_keyset=*/bytes_type},
        FN_KEYS_KEYSET_CHAIN_STRING_BYTES_BYTES},
       {keyset_chain_struct_type,
        {/*kms_resource_name=*/{string_type, const_arg_options},
         /*first_level_keyset=*/{bytes_type, const_arg_options}},
        FN_KEYS_KEYSET_CHAIN_STRING_BYTES}},
      FunctionOptions(encryption_required));

  // KEYS.NEW_KEYSET is volatile since it generates a random key for each
  // invocation.
  InsertSimpleNamespaceFunction(
      functions, options, "keys", "new_keyset", SCALAR,
      {{bytes_type, {string_type}, FN_KEYS_NEW_KEYSET}},
      FunctionOptions(encryption_required)
          .set_volatility(FunctionEnums::VOLATILE)
          .set_pre_resolution_argument_constraint(absl::bind_front(
              &CheckIsSupportedKeyType, "KEYS.NEW_KEYSET",
              GetSupportedKeyTypes(), /*key_type_argument_index=*/0)));

  InsertSimpleNamespaceFunction(
      functions, options, "keys", "add_key_from_raw_bytes", SCALAR,
      {{bytes_type,
        {bytes_type, string_type, bytes_type},
        FN_KEYS_ADD_KEY_FROM_RAW_BYTES}},
      FunctionOptions(encryption_required)
          .set_pre_resolution_argument_constraint(absl::bind_front(
              &CheckIsSupportedKeyType, "KEYS.ADD_KEY_FROM_RAW_BYTES",
              GetSupportedRawKeyTypes(), /*key_type_argument_index=*/1)));

  // KEYS.ROTATE_KEYSET is volatile since it generates a random key for each
  // invocation.
  InsertSimpleNamespaceFunction(
      functions, options, "keys", "rotate_keyset", SCALAR,
      {{bytes_type, {bytes_type, string_type}, FN_KEYS_ROTATE_KEYSET}},
      FunctionOptions(encryption_required)
          .set_volatility(FunctionEnums::VOLATILE)
          .set_pre_resolution_argument_constraint(
              absl::bind_front(&CheckIsSupportedKeyType, "KEYS.ROTATE_KEYSET",
                               GetSupportedKeyTypes(),
                               /*key_type_argument_index=*/1)));

  InsertSimpleNamespaceFunction(
      functions, options, "keys", "keyset_length", SCALAR,
      {{int64_type, {bytes_type}, FN_KEYS_KEYSET_LENGTH}}, encryption_required);
  InsertSimpleNamespaceFunction(
      functions, options, "keys", "keyset_to_json", SCALAR,
      {{string_type, {bytes_type}, FN_KEYS_KEYSET_TO_JSON}},
      encryption_required);
  InsertSimpleNamespaceFunction(
      functions, options, "keys", "keyset_from_json", SCALAR,
      {{bytes_type, {string_type}, FN_KEYS_KEYSET_FROM_JSON}},
      encryption_required);

  InsertSimpleNamespaceFunction(
      functions, options, "keys", "new_wrapped_keyset", SCALAR,
      {{bytes_type, {string_type, string_type}, FN_KEYS_NEW_WRAPPED_KEYSET}},
      FunctionOptions(encryption_required)
          .set_volatility(FunctionEnums::VOLATILE)
          .set_pre_resolution_argument_constraint(absl::bind_front(
              &CheckIsSupportedKeyType, "KEYS.NEW_WRAPPED_KEYSET",
              GetSupportedKeyTypes(), /*key_type_argument_index=*/1)));

  InsertSimpleNamespaceFunction(
      functions, options, "keys", "rotate_wrapped_keyset", SCALAR,
      {{bytes_type,
        {string_type, bytes_type, string_type},
        FN_KEYS_ROTATE_WRAPPED_KEYSET}},
      FunctionOptions(encryption_required)
          .set_volatility(FunctionEnums::VOLATILE)
          .set_pre_resolution_argument_constraint(absl::bind_front(
              &CheckIsSupportedKeyType, "KEYS.ROTATE_WRAPPED_KEYSET",
              GetSupportedKeyTypes(), /*key_type_argument_index=*/2)));

  InsertSimpleNamespaceFunction(functions, options, "keys", "rewrap_keyset",
                                SCALAR,
                                {{bytes_type,
                                  {string_type, string_type, bytes_type},
                                  FN_KEYS_REWRAP_KEYSET}},
                                FunctionOptions(encryption_required)
                                    .set_volatility(FunctionEnums::VOLATILE));

  // AEAD.ENCRYPT is volatile since it generates a random IV (initialization
  // vector) for each invocation so that encrypting the same plaintext results
  // in different ciphertext.
  InsertSimpleNamespaceFunction(
      functions, options, "aead", "encrypt", SCALAR,
      {{bytes_type,
        {bytes_type, string_type, string_type},
        FN_AEAD_ENCRYPT_STRING},
       {bytes_type,
        {bytes_type, bytes_type, bytes_type},
        FN_AEAD_ENCRYPT_BYTES},
       {bytes_type,
        {keyset_chain_struct_type, string_type, string_type},
        FN_AEAD_ENCRYPT_STRUCT_STRING},
       {bytes_type,
        {keyset_chain_struct_type, bytes_type, bytes_type},
        FN_AEAD_ENCRYPT_STRUCT_BYTES}},
      FunctionOptions(encryption_required)
          .set_volatility(FunctionEnums::VOLATILE));

  InsertSimpleNamespaceFunction(
      functions, options, "aead", "decrypt_string", SCALAR,
      {{string_type,
        {bytes_type, bytes_type, string_type},
        FN_AEAD_DECRYPT_STRING},
       {string_type,
        {keyset_chain_struct_type, bytes_type, string_type},
        FN_AEAD_DECRYPT_STRUCT_STRING}},
      encryption_required);

  InsertSimpleNamespaceFunction(
      functions, options, "aead", "decrypt_bytes", SCALAR,
      {{bytes_type,
        {bytes_type, bytes_type, bytes_type},
        FN_AEAD_DECRYPT_BYTES},
       {bytes_type,
        {keyset_chain_struct_type, bytes_type, bytes_type},
        FN_AEAD_DECRYPT_STRUCT_BYTES}},
      encryption_required);

  // AEAD.ENVELOPE_ENCRYPT is volatile since it generates a random IV
  // (initialization vector) for each invocation so that encrypting the same
  // plaintext results in different ciphertext.
  InsertSimpleNamespaceFunction(
      functions, options, "aead", "envelope_encrypt", SCALAR,
      {{bytes_type,
        {string_type, bytes_type, string_type, string_type},
        FN_AEAD_ENVELOPE_ENCRYPT_STRING},
       {bytes_type,
        {string_type, bytes_type, bytes_type, bytes_type},
        FN_AEAD_ENVELOPE_ENCRYPT_BYTES}},
      FunctionOptions(encryption_required)
          .set_volatility(FunctionEnums::VOLATILE));

  InsertSimpleNamespaceFunction(
      functions, options, "aead", "envelope_decrypt_string", SCALAR,
      {{string_type,
        {string_type, bytes_type, bytes_type, string_type},
        FN_AEAD_ENVELOPE_DECRYPT_STRING}},
      encryption_required);

  InsertSimpleNamespaceFunction(
      functions, options, "aead", "envelope_decrypt_bytes", SCALAR,
      {{bytes_type,
        {string_type, bytes_type, bytes_type, bytes_type},
        FN_AEAD_ENVELOPE_DECRYPT_BYTES}},
      encryption_required);

  // DETERMINISTIC_ENCRYPT is IMMUTABLE as it generates the same
  // cipher text for same input
  InsertSimpleFunction(functions, options, "deterministic_encrypt", SCALAR,
                       {{bytes_type,
                         {bytes_type, string_type, string_type},
                         FN_DETERMINISTIC_ENCRYPT_STRING},
                        {bytes_type,
                         {bytes_type, bytes_type, bytes_type},
                         FN_DETERMINISTIC_ENCRYPT_BYTES},
                        {bytes_type,
                         {keyset_chain_struct_type, string_type, string_type},
                         FN_DETERMINISTIC_ENCRYPT_STRUCT_STRING},
                        {bytes_type,
                         {keyset_chain_struct_type, bytes_type, bytes_type},
                         FN_DETERMINISTIC_ENCRYPT_STRUCT_BYTES}},
                       encryption_required);

  InsertSimpleFunction(functions, options, "deterministic_decrypt_string",
                       SCALAR,
                       {{string_type,
                         {bytes_type, bytes_type, string_type},
                         FN_DETERMINISTIC_DECRYPT_STRING},
                        {string_type,
                         {keyset_chain_struct_type, bytes_type, string_type},
                         FN_DETERMINISTIC_DECRYPT_STRUCT_STRING}},
                       encryption_required);

  InsertSimpleFunction(functions, options, "deterministic_decrypt_bytes",
                       SCALAR,
                       {{bytes_type,
                         {bytes_type, bytes_type, bytes_type},
                         FN_DETERMINISTIC_DECRYPT_BYTES},
                        {bytes_type,
                         {keyset_chain_struct_type, bytes_type, bytes_type},
                         FN_DETERMINISTIC_DECRYPT_STRUCT_BYTES}},
                       encryption_required);
}

void GetGeographyFunctions(TypeFactory* type_factory,
                           const ZetaSQLBuiltinFunctionOptions& options,
                           NameToFunctionMap* functions) {
  const Function::Mode SCALAR = Function::SCALAR;
  const Function::Mode AGGREGATE = Function::AGGREGATE;
  const Function::Mode ANALYTIC = Function::ANALYTIC;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  const Type* bool_type = types::BoolType();
  const Type* int64_type = types::Int64Type();
  const Type* double_type = types::DoubleType();
  const Type* string_type = types::StringType();
  const Type* bytes_type = types::BytesType();
  const Type* geography_type = types::GeographyType();
  const ArrayType* geography_array_type = types::GeographyArrayType();
  const ArrayType* int64_array_type = types::Int64ArrayType();

  const FunctionOptions geography_required =
      FunctionOptions().add_required_language_feature(
          zetasql::FEATURE_GEOGRAPHY);

  const FunctionOptions geography_required_analytic =
      FunctionOptions(geography_required)
          .set_supports_over_clause(true)
          .set_window_ordering_support(FunctionOptions::ORDER_OPTIONAL);

  const FunctionOptions geography_and_named_arg_required =
      FunctionOptions()
          .add_required_language_feature(zetasql::FEATURE_GEOGRAPHY)
          .add_required_language_feature(zetasql::FEATURE_NAMED_ARGUMENTS);

  const FunctionArgumentTypeOptions optional_const_arg_options =
      FunctionArgumentTypeOptions().set_must_be_constant().set_cardinality(
          OPTIONAL);

  auto const_with_mandatory_name_and_default_value = [](absl::string_view name,
                                                        Value default_value) {
    return FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
        .set_must_be_constant()
        .set_argument_name(name, kNamedOnly)
        .set_default(default_value);
  };

  auto arg_with_mandatory_name_and_default_value = [](absl::string_view name,
                                                      Value default_value) {
    return FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
        .set_argument_name(name, kNamedOnly)
        .set_default(default_value);
  };

  auto arg_with_optional_name_and_default_value = [](absl::string_view name,
                                                     Value default_value) {
    return FunctionArgumentTypeOptions(FunctionArgumentType::OPTIONAL)
        .set_argument_name(name, kPositionalOrNamed)
        .set_default(default_value);
  };

  auto required_arg_with_optional_name = [](absl::string_view name) {
    return FunctionArgumentTypeOptions(FunctionArgumentType::REQUIRED)
        .set_argument_name(name, kPositionalOrNamed);
  };

  // Constructors
  InsertSimpleFunction(
      functions, options, "st_geogpoint", SCALAR,
      {{geography_type, {double_type, double_type}, FN_ST_GEOG_POINT}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_makeline", SCALAR,
      {{geography_type, {geography_type, geography_type}, FN_ST_MAKE_LINE},
       {geography_type, {geography_array_type}, FN_ST_MAKE_LINE_ARRAY}},
      geography_required);
  InsertSimpleFunction(functions, options, "st_makepolygon", SCALAR,
                       {{geography_type,
                         {geography_type, {geography_array_type, OPTIONAL}},
                         FN_ST_MAKE_POLYGON}},
                       geography_required);
  InsertSimpleFunction(
      functions, options, "st_makepolygonoriented", SCALAR,
      {{geography_type, {geography_array_type}, FN_ST_MAKE_POLYGON_ORIENTED}},
      geography_required);

  // Transformations
  InsertSimpleFunction(
      functions, options, "st_intersection", SCALAR,
      {{geography_type, {geography_type, geography_type}, FN_ST_INTERSECTION}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_union", SCALAR,
      {{geography_type, {geography_type, geography_type}, FN_ST_UNION},
       {geography_type, {geography_array_type}, FN_ST_UNION_ARRAY}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_difference", SCALAR,
      {{geography_type, {geography_type, geography_type}, FN_ST_DIFFERENCE}},
      geography_required);
  InsertSimpleFunction(functions, options, "st_unaryunion", SCALAR,
                       {{geography_type, {geography_type}, FN_ST_UNARY_UNION}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_centroid", SCALAR,
                       {{geography_type, {geography_type}, FN_ST_CENTROID}},
                       geography_required);
  InsertFunction(
      functions, options, "st_buffer", SCALAR,
      {{geography_type,
        {geography_type,
         double_type,
         {double_type, arg_with_optional_name_and_default_value(
                           "num_seg_quarter_circle", Value::Double(8.0))},
         {bool_type, arg_with_optional_name_and_default_value(
                         "use_spheroid", Value::Bool(false))},
         {string_type, arg_with_mandatory_name_and_default_value(
                           "endcap", Value::String("ROUND"))},
         {string_type, arg_with_mandatory_name_and_default_value(
                           "side", Value::String("BOTH"))}},
        FN_ST_BUFFER}},
      geography_and_named_arg_required);
  InsertFunction(
      functions, options, "st_bufferwithtolerance", SCALAR,
      {{geography_type,
        {geography_type,
         double_type,
         {double_type, required_arg_with_optional_name("tolerance_meters")},
         {bool_type, arg_with_optional_name_and_default_value(
                         "use_spheroid", Value::Bool(false))},
         {string_type, arg_with_mandatory_name_and_default_value(
                           "endcap", Value::String("ROUND"))},
         {string_type, arg_with_mandatory_name_and_default_value(
                           "side", Value::String("BOTH"))}},
        FN_ST_BUFFER_WITH_TOLERANCE}},
      geography_and_named_arg_required);
  InsertSimpleFunction(
      functions, options, "st_simplify", SCALAR,
      {{geography_type, {geography_type, double_type}, FN_ST_SIMPLIFY}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_snaptogrid", SCALAR,
      {{geography_type, {geography_type, double_type}, FN_ST_SNAP_TO_GRID}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_closestpoint", SCALAR,
      {{geography_type,
        {geography_type, geography_type, {bool_type, OPTIONAL}},
        FN_ST_CLOSEST_POINT}},
      geography_required);
  InsertSimpleFunction(functions, options, "st_boundary", SCALAR,
                       {{geography_type, {geography_type}, FN_ST_BOUNDARY}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_convexhull", SCALAR,
                       {{geography_type, {geography_type}, FN_ST_CONVEXHULL}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_exteriorring", SCALAR,
                       {{geography_type, {geography_type}, FN_ST_EXTERIORRING}},
                       geography_required);
  InsertSimpleFunction(
      functions, options, "st_interiorrings", SCALAR,
      {{geography_array_type, {geography_type}, FN_ST_INTERIORRINGS}},
      geography_required);
  InsertFunction(functions, options, "st_lineinterpolatepoint", SCALAR,
                 {{geography_type,
                   {geography_type, double_type},
                   FN_ST_LINE_INTERPOLATE_POINT}},
                 geography_required);
  InsertFunction(functions, options, "st_linesubstring", SCALAR,
                 {{geography_type,
                   {geography_type, double_type, double_type},
                   FN_ST_LINE_SUBSTRING}},
                 geography_required);

  // Predicates
  InsertSimpleFunction(
      functions, options, "st_equals", SCALAR,
      {{bool_type, {geography_type, geography_type}, FN_ST_EQUALS}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_intersects", SCALAR,
      {{bool_type, {geography_type, geography_type}, FN_ST_INTERSECTS}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_contains", SCALAR,
      {{bool_type, {geography_type, geography_type}, FN_ST_CONTAINS}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_within", SCALAR,
      {{bool_type, {geography_type, geography_type}, FN_ST_WITHIN}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_covers", SCALAR,
      {{bool_type, {geography_type, geography_type}, FN_ST_COVERS}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_coveredby", SCALAR,
      {{bool_type, {geography_type, geography_type}, FN_ST_COVEREDBY}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_disjoint", SCALAR,
      {{bool_type, {geography_type, geography_type}, FN_ST_DISJOINT}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_touches", SCALAR,
      {{bool_type, {geography_type, geography_type}, FN_ST_TOUCHES}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_intersectsbox", SCALAR,
      {{bool_type,
        {geography_type, double_type, double_type, double_type, double_type},
        FN_ST_INTERSECTS_BOX}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_dwithin", SCALAR,
      {{bool_type,
        {geography_type, geography_type, double_type, {bool_type, OPTIONAL}},
        FN_ST_DWITHIN}},
      geography_required);

  // Accessors
  InsertSimpleFunction(functions, options, "st_isempty", SCALAR,
                       {{bool_type, {geography_type}, FN_ST_IS_EMPTY}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_iscollection", SCALAR,
                       {{bool_type, {geography_type}, FN_ST_IS_COLLECTION}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_dimension", SCALAR,
                       {{int64_type, {geography_type}, FN_ST_DIMENSION}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_numpoints", SCALAR,
                       {{int64_type, {geography_type}, FN_ST_NUM_POINTS}},
                       geography_required.Copy().set_alias_name("st_npoints"));
  InsertSimpleFunction(functions, options, "st_numgeometries", SCALAR,
                       {{int64_type, {geography_type}, FN_ST_NUM_GEOMETRIES}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_dump", SCALAR,
                       {{geography_array_type,
                         {geography_type, {int64_type, OPTIONAL}},
                         FN_ST_DUMP}},
                       geography_required);
  InsertSimpleFunction(
      functions, options, "st_dumppoints", SCALAR,
      {{geography_array_type, {geography_type}, FN_ST_DUMP_POINTS}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_pointn", SCALAR,
      {{geography_type, {geography_type, int64_type}, FN_ST_POINT_N}},
      geography_required);
  InsertSimpleFunction(functions, options, "st_startpoint", SCALAR,
                       {{geography_type, {geography_type}, FN_ST_START_POINT}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_endpoint", SCALAR,
                       {{geography_type, {geography_type}, FN_ST_END_POINT}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_geometrytype", SCALAR,
                       {{string_type, {geography_type}, FN_ST_GEOMETRY_TYPE}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_isclosed", SCALAR,
                       {{bool_type, {geography_type}, FN_ST_IS_CLOSED}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_isring", SCALAR,
                       {{bool_type, {geography_type}, FN_ST_IS_RING}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_linelocatepoint", SCALAR,
                       {{double_type,
                         {geography_type, geography_type},
                         FN_ST_LINE_LOCATE_POINT}},
                       geography_required);

  // Measures
  InsertSimpleFunction(
      functions, options, "st_length", SCALAR,
      {{double_type, {geography_type, {bool_type, OPTIONAL}}, FN_ST_LENGTH}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_perimeter", SCALAR,
      {{double_type, {geography_type, {bool_type, OPTIONAL}}, FN_ST_PERIMETER}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_area", SCALAR,
      {{double_type, {geography_type, {bool_type, OPTIONAL}}, FN_ST_AREA}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_distance", SCALAR,
      {{double_type,
        {geography_type, geography_type, {bool_type, OPTIONAL}},
        FN_ST_DISTANCE}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_maxdistance", SCALAR,
      {{double_type,
        {geography_type, geography_type, {bool_type, OPTIONAL}},
        FN_ST_MAX_DISTANCE}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_azimuth", SCALAR,
      {{double_type, {geography_type, geography_type}, FN_ST_AZIMUTH}},
      geography_required);
  InsertSimpleFunction(functions, options, "st_angle", SCALAR,
                       {{double_type,
                         {geography_type, geography_type, geography_type},
                         FN_ST_ANGLE}},
                       geography_required);
  InsertFunction(functions, options, "st_hausdorffdistance", SCALAR,
                 {{double_type,
                   {geography_type,
                    geography_type,
                    {bool_type, arg_with_mandatory_name_and_default_value(
                                    "directed", Value::Bool(false))}},
                   FN_ST_HAUSDORFF_DISTANCE}},
                 geography_and_named_arg_required);

  // Parsers/Formatters
  InsertSimpleFunction(functions, options, "st_astext", SCALAR,
                       {{string_type, {geography_type}, FN_ST_AS_TEXT}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_askml", SCALAR,
                       {{string_type, {geography_type}, FN_ST_AS_KML}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_asgeojson", SCALAR,
                       {{string_type,
                         {geography_type, {int64_type, OPTIONAL}},
                         FN_ST_AS_GEO_JSON}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_asbinary", SCALAR,
                       {{bytes_type, {geography_type}, FN_ST_AS_BINARY}},
                       geography_required);
  InsertSimpleFunction(
      functions, options, "st_geohash", SCALAR,
      {{string_type, {geography_type, {int64_type, OPTIONAL}}, FN_ST_GEOHASH}},
      geography_required);
  InsertSimpleFunction(
      functions, options, "st_geogpointfromgeohash", SCALAR,
      {{geography_type, {string_type}, FN_ST_GEOG_POINT_FROM_GEOHASH}},
      geography_required);

  // Extended signatures for ST_GeogFromText/FromGeoJson/etc.
  FunctionSignatureOptions extended_parser_signatures =
      FunctionSignatureOptions().add_required_language_feature(
          FEATURE_V_1_3_EXTENDED_GEOGRAPHY_PARSERS);

  FunctionArgumentType oriented_argument_type{
      bool_type, const_with_mandatory_name_and_default_value(
                     "oriented", Value::Bool(false))};
  FunctionArgumentType planar_argument_type{
      bool_type, const_with_mandatory_name_and_default_value(
                     "planar", Value::Bool(false))};
  FunctionArgumentType make_valid_argument_type{
      bool_type, const_with_mandatory_name_and_default_value(
                     "make_valid", Value::Bool(false))};
  InsertFunction(functions, options, "st_geogfromtext", SCALAR,
                 {{geography_type,
                   {string_type, {bool_type, optional_const_arg_options}},
                   FN_ST_GEOG_FROM_TEXT},
                  {geography_type,
                   {string_type, oriented_argument_type, planar_argument_type,
                    make_valid_argument_type},
                   FN_ST_GEOG_FROM_TEXT_EXT,
                   extended_parser_signatures}},
                 geography_required);
  InsertSimpleFunction(functions, options, "st_geogfromkml", SCALAR,
                       {{geography_type, {string_type}, FN_ST_GEOG_FROM_KML}},
                       geography_required);
  InsertFunction(functions, options, "st_geogfromgeojson", SCALAR,
                 {{geography_type, {string_type}, FN_ST_GEOG_FROM_GEO_JSON},
                  {geography_type,
                   {string_type, make_valid_argument_type},
                   FN_ST_GEOG_FROM_GEO_JSON_EXT,
                   extended_parser_signatures}},
                 geography_required);
  InsertFunction(functions, options, "st_geogfromwkb", SCALAR,
                 {// st_geogfromwkb(bytes)
                  {geography_type, {bytes_type}, FN_ST_GEOG_FROM_WKB},
                  // st_geogfromwkb(string)
                  {geography_type,
                   {string_type},
                   FN_ST_GEOG_FROM_WKB_HEX,
                   extended_parser_signatures},
                  // st_geogfromwkb(bytes, oriented, planar, make_valid)
                  {geography_type,
                   {bytes_type, oriented_argument_type, planar_argument_type,
                    make_valid_argument_type},
                   FN_ST_GEOG_FROM_WKB_EXT,
                   extended_parser_signatures},
                  // st_geogfromwkb(string, oriented, planar, make_valid)
                  {geography_type,
                   {string_type, oriented_argument_type, planar_argument_type,
                    make_valid_argument_type},
                   FN_ST_GEOG_FROM_WKB_HEX_EXT,
                   extended_parser_signatures}},
                 geography_required);
  InsertSimpleFunction(
      functions, options, "st_geogfrom", SCALAR,
      {{geography_type, {bytes_type}, FN_ST_GEOG_FROM_BYTES},
       {geography_type, {string_type}, FN_ST_GEOG_FROM_STRING}},
      geography_required);

  // Aggregate
  // By default, all built-in aggregate functions can be used as analytic
  // functions.
  FunctionOptions aggregate_analytic_function_options_and_geography_required =
      DefaultAggregateFunctionOptions().add_required_language_feature(
          zetasql::FEATURE_GEOGRAPHY);

  InsertSimpleFunction(
      functions, options, "st_union_agg", AGGREGATE,
      {{geography_type, {geography_type}, FN_ST_UNION_AGG}},
      aggregate_analytic_function_options_and_geography_required);
  InsertSimpleFunction(
      functions, options, "st_centroid_agg", AGGREGATE,
      {{geography_type, {geography_type}, FN_ST_CENTROID_AGG}},
      aggregate_analytic_function_options_and_geography_required);
  InsertSimpleFunction(
      functions, options, "st_nearest_neighbors", AGGREGATE,
      {{ARG_TYPE_ANY_1,  //  Return type will be overridden.
        {ARG_TYPE_ANY_1, geography_type, geography_type, int64_type},
        FN_ST_NEAREST_NEIGHBORS}},
      DefaultAggregateFunctionOptions()
          .add_required_language_feature(zetasql::FEATURE_GEOGRAPHY)
          .set_compute_result_type_callback(
              &ComputeResultTypeForNearestNeighborsStruct));

  const zetasql::StructType* bbox_result_type = nullptr;
  ZETASQL_CHECK_OK(type_factory->MakeStructType({{"xmin", types::DoubleType()},
                                         {"ymin", types::DoubleType()},
                                         {"xmax", types::DoubleType()},
                                         {"ymax", types::DoubleType()}},
                                        &bbox_result_type));
  InsertSimpleFunction(
      functions, options, "st_extent", AGGREGATE,
      {{bbox_result_type, {geography_type}, FN_ST_EXTENT}},
      aggregate_analytic_function_options_and_geography_required);

  // Other
  InsertSimpleFunction(functions, options, "st_x", SCALAR,
                       {{double_type, {geography_type}, FN_ST_X}},
                       geography_required);
  InsertSimpleFunction(functions, options, "st_y", SCALAR,
                       {{double_type, {geography_type}, FN_ST_Y}},
                       geography_required);
  InsertSimpleFunction(
      functions, options, "st_boundingbox", SCALAR,
      {{bbox_result_type, {geography_type}, FN_ST_BOUNDING_BOX}},
      geography_required);

  InsertFunction(functions, options, "s2_coveringcellids", SCALAR,
                 {{int64_array_type,
                   {geography_type,
                    {int64_type, arg_with_mandatory_name_and_default_value(
                                     "min_level", Value::Int64(0))},
                    {int64_type, arg_with_mandatory_name_and_default_value(
                                     "max_level", Value::Int64(30))},
                    {int64_type, arg_with_mandatory_name_and_default_value(
                                     "max_cells", Value::Int64(8))},
                    {double_type, arg_with_mandatory_name_and_default_value(
                                      "buffer", Value::Double(0.0))}},
                   FN_S2_COVERINGCELLIDS}},
                 geography_and_named_arg_required);

  InsertFunction(functions, options, "s2_cellidfrompoint", SCALAR,
                 {{int64_type,
                   {geography_type,
                    {int64_type, arg_with_optional_name_and_default_value(
                                     "level", Value::Int64(30))}},
                   FN_S2_CELLIDFROMPOINT}},
                 geography_required);

  const FunctionArgumentTypeOptions dbscan_arg_options =
      FunctionArgumentTypeOptions()
          .set_must_be_constant()
          .set_must_be_non_null()
          .set_min_value(0);

  InsertFunction(functions, options, "st_clusterdbscan", ANALYTIC,
                 {{int64_type,
                   {geography_type,
                    {double_type, dbscan_arg_options},
                    {int64_type, dbscan_arg_options}},
                   FN_ST_CLUSTERDBSCAN}},
                 geography_required_analytic);
}

void GetTypeOfFunction(TypeFactory* type_factory,
                       const ZetaSQLBuiltinFunctionOptions& options,
                       NameToFunctionMap* functions) {
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_3_TYPEOF_FUNCTION)) {
    InsertFunction(functions, options, "typeof", Function::SCALAR,
                   {{type_factory->get_string(),
                     {ARG_TYPE_ARBITRARY},
                     FN_TYPEOF,
                     SetRewriter(REWRITE_TYPEOF_FUNCTION)
                         .set_propagates_collation(false)}});
  }
}

void GetFilterFieldsFunction(TypeFactory* type_factory,
                             const ZetaSQLBuiltinFunctionOptions& options,
                             NameToFunctionMap* functions) {
  // Create function with no signatures for filter fields function, which has
  // special handling in the analyzer.
  const std::vector<FunctionSignatureOnHeap> empty_signatures;
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_V_1_3_FILTER_FIELDS)) {
    const FunctionOptions fn_options;
    InsertFunction(functions, options, "filter_fields", Function::SCALAR,
                   empty_signatures, fn_options);
  }
}

void GetElementWiseAggregationFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions) {
  // The signatures here match the coercion rules for the SUM aggregate
  // function. In particular,
  // INT32 -> INT64, UINT32 -> UINT64, and FLOAT -> DOUBLE.
  std::vector<FunctionSignatureOnHeap> elementwise_sum_signatures = {
      {types::Int64ArrayType(),
       {types::Int32ArrayType()},
       FN_ELEMENTWISE_SUM_INT32},
      {types::Int64ArrayType(),
       {types::Int64ArrayType()},
       FN_ELEMENTWISE_SUM_INT64},
      {types::Uint64ArrayType(),
       {types::Uint32ArrayType()},
       FN_ELEMENTWISE_SUM_UINT32},
      {types::Uint64ArrayType(),
       {types::Uint64ArrayType()},
       FN_ELEMENTWISE_SUM_UINT64},
      {types::DoubleArrayType(),
       {types::FloatArrayType()},
       FN_ELEMENTWISE_SUM_FLOAT},
      {types::DoubleArrayType(),
       {types::DoubleArrayType()},
       FN_ELEMENTWISE_SUM_DOUBLE},
      {types::NumericArrayType(),
       {types::NumericArrayType()},
       FN_ELEMENTWISE_SUM_NUMERIC},
      {types::BigNumericArrayType(),
       {types::BigNumericArrayType()},
       FN_ELEMENTWISE_SUM_BIGNUMERIC},
      {types::IntervalArrayType(),
       {types::IntervalArrayType()},
       FN_ELEMENTWISE_SUM_INTERVAL},
  };
  InsertFunction(functions, options, "elementwise_sum", Function::AGGREGATE,
                 elementwise_sum_signatures, DefaultAggregateFunctionOptions());
  std::vector<FunctionSignatureOnHeap> elementwise_avg_signatures = {
      {types::DoubleArrayType(),
       {types::Int32ArrayType()},
       FN_ELEMENTWISE_AVG_INT32},
      {types::DoubleArrayType(),
       {types::Int64ArrayType()},
       FN_ELEMENTWISE_AVG_INT64},
      {types::DoubleArrayType(),
       {types::Uint32ArrayType()},
       FN_ELEMENTWISE_AVG_UINT32},
      {types::DoubleArrayType(),
       {types::Uint64ArrayType()},
       FN_ELEMENTWISE_AVG_UINT64},
      {types::DoubleArrayType(),
       {types::FloatArrayType()},
       FN_ELEMENTWISE_AVG_FLOAT},
      {types::DoubleArrayType(),
       {types::DoubleArrayType()},
       FN_ELEMENTWISE_AVG_DOUBLE},
      {types::NumericArrayType(),
       {types::NumericArrayType()},
       FN_ELEMENTWISE_AVG_NUMERIC},
      {types::BigNumericArrayType(),
       {types::BigNumericArrayType()},
       FN_ELEMENTWISE_AVG_BIGNUMERIC},
      {types::IntervalArrayType(),
       {types::IntervalArrayType()},
       FN_ELEMENTWISE_AVG_INTERVAL},
  };
  InsertFunction(functions, options, "elementwise_avg", Function::AGGREGATE,
                 elementwise_avg_signatures, DefaultAggregateFunctionOptions());
};
}  // namespace zetasql
