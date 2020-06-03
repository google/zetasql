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
#include "zetasql/common/builtin_function_internal.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/cycle_detector.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
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
using ::zetasql::functions::DateTimestampPartToSQL;

void GetDatetimeExtractFunctions(TypeFactory* type_factory,
                                 const ZetaSQLBuiltinFunctionOptions& options,
                                 NameToFunctionMap* functions) {
  const Type* date_type = type_factory->get_date();
  const Type* datetime_type = type_factory->get_datetime();
  const Type* time_type = type_factory->get_time();
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* int64_type = type_factory->get_int64();
  const Type* datepart_type = types::DatePartEnumType();
  const Type* string_type = type_factory->get_string();

  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  // EXTRACT functions.
  const Type* extract_type = int64_type;

  InsertFunction(
      functions, options, "$extract", SCALAR,
      {{extract_type, {date_type, datepart_type}, FN_EXTRACT_FROM_DATE},
       {extract_type,
        {timestamp_type, datepart_type, {string_type, OPTIONAL}},
        FN_EXTRACT_FROM_TIMESTAMP},
       {extract_type, {datetime_type, datepart_type}, FN_EXTRACT_FROM_DATETIME},
       {extract_type, {time_type, datepart_type}, FN_EXTRACT_FROM_TIME}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_pre_resolution_argument_constraint(
              &CheckExtractPreResolutionArguments)
          .set_post_resolution_argument_constraint(
              &CheckExtractPostResolutionArguments)
          .set_no_matching_signature_callback(
              bind_front(&NoMatchingSignatureForExtractFunction,
                         /*explicit_datepart_name=*/""))
          .set_supported_signatures_callback(
              bind_front(&ExtractSupportedSignatures,
                         /*explicit_datepart_name=*/""))
          .set_get_sql_callback(&ExtractFunctionSQL));

  InsertFunction(functions, options, "$extract_date", SCALAR,
                 {{date_type,
                   {timestamp_type, {string_type, OPTIONAL}},
                   FN_EXTRACT_DATE_FROM_TIMESTAMP},
                  {date_type, {datetime_type}, FN_EXTRACT_DATE_FROM_DATETIME}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_pre_resolution_argument_constraint(
                         &CheckExtractPreResolutionArguments)
                     .set_sql_name("extract")
                     .set_no_matching_signature_callback(bind_front(
                         &NoMatchingSignatureForExtractFunction, "DATE"))
                     .set_supported_signatures_callback(
                         bind_front(&ExtractSupportedSignatures, "DATE"))
                     .set_get_sql_callback(
                         bind_front(ExtractDateOrTimeFunctionSQL, "DATE")));

  InsertFunction(functions, options, "$extract_time", SCALAR,
                 {{time_type,
                   {timestamp_type, {string_type, OPTIONAL}},
                   FN_EXTRACT_TIME_FROM_TIMESTAMP},
                  {time_type, {datetime_type}, FN_EXTRACT_TIME_FROM_DATETIME}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_pre_resolution_argument_constraint(
                         &CheckExtractPreResolutionArguments)
                     .set_sql_name("extract")
                     .set_no_matching_signature_callback(bind_front(
                         &NoMatchingSignatureForExtractFunction, "TIME"))
                     .set_supported_signatures_callback(
                         bind_front(&ExtractSupportedSignatures, "TIME"))
                     .set_get_sql_callback(
                         bind_front(ExtractDateOrTimeFunctionSQL, "TIME")));

  InsertFunction(functions, options, "$extract_datetime", SCALAR,
                 {{datetime_type,
                   {timestamp_type, {string_type, OPTIONAL}},
                   FN_EXTRACT_DATETIME_FROM_TIMESTAMP}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_pre_resolution_argument_constraint(
                         &CheckExtractPreResolutionArguments)
                     .set_sql_name("extract")
                     .set_no_matching_signature_callback(bind_front(
                         &NoMatchingSignatureForExtractFunction, "DATETIME"))
                     .set_supported_signatures_callback(
                         bind_front(&ExtractSupportedSignatures, "DATETIME"))
                     .set_get_sql_callback(
                         bind_front(ExtractDateOrTimeFunctionSQL, "DATETIME")));
}

void GetDatetimeConversionFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions) {
  const Type* date_type = type_factory->get_date();
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* datetime_type = type_factory->get_datetime();
  const Type* int64_type = type_factory->get_int64();
  const Type* string_type = type_factory->get_string();

  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  // Conversion functions from integer/string/date/timestamp to
  // date/timestamp.
  InsertFunction(functions, options, "date_from_unix_date", SCALAR,
                 {{date_type, {int64_type}, FN_DATE_FROM_UNIX_DATE}});
  InsertFunction(functions, options, "date", SCALAR,
                 {{date_type,
                   {timestamp_type, {string_type, OPTIONAL}},
                   FN_DATE_FROM_TIMESTAMP},
                  {date_type, {datetime_type}, FN_DATE_FROM_DATETIME},
                  {date_type,
                   {int64_type, int64_type, int64_type},
                   FN_DATE_FROM_YEAR_MONTH_DAY}});
  InsertFunction(
      functions, options, "timestamp_from_unix_seconds", SCALAR,
      {{timestamp_type, {int64_type}, FN_TIMESTAMP_FROM_UNIX_SECONDS_INT64},
       {timestamp_type,
        {timestamp_type},
        FN_TIMESTAMP_FROM_UNIX_SECONDS_TIMESTAMP}});
  InsertFunction(
      functions, options, "timestamp_from_unix_millis", SCALAR,
      {{timestamp_type, {int64_type}, FN_TIMESTAMP_FROM_UNIX_MILLIS_INT64},
       {timestamp_type,
        {timestamp_type},
        FN_TIMESTAMP_FROM_UNIX_MILLIS_TIMESTAMP}});
  InsertFunction(
      functions, options, "timestamp_from_unix_micros", SCALAR,
      {{timestamp_type, {int64_type}, FN_TIMESTAMP_FROM_UNIX_MICROS_INT64},
       {timestamp_type,
        {timestamp_type},
        FN_TIMESTAMP_FROM_UNIX_MICROS_TIMESTAMP}});
  InsertFunction(functions, options, "timestamp", SCALAR,
                 {{timestamp_type,
                   {string_type, {string_type, OPTIONAL}},
                   FN_TIMESTAMP_FROM_STRING},
                  {timestamp_type,
                   {date_type, {string_type, OPTIONAL}},
                   FN_TIMESTAMP_FROM_DATE},
                  {timestamp_type,
                   {datetime_type, {string_type, OPTIONAL}},
                   FN_TIMESTAMP_FROM_DATETIME}});
  InsertFunction(
      functions, options, "timestamp_seconds", SCALAR,
      {{timestamp_type, {int64_type}, FN_TIMESTAMP_FROM_INT64_SECONDS}});
  InsertFunction(
      functions, options, "timestamp_millis", SCALAR,
      {{timestamp_type, {int64_type}, FN_TIMESTAMP_FROM_INT64_MILLIS}});
  InsertFunction(
      functions, options, "timestamp_micros", SCALAR,
      {{timestamp_type, {int64_type}, FN_TIMESTAMP_FROM_INT64_MICROS}});

  const Type* unix_date_type = int64_type;

  // Conversion functions from date/timestamp to integer and string.
  InsertFunction(functions, options, "unix_date", SCALAR,
                 {{unix_date_type, {date_type}, FN_UNIX_DATE}});

  InsertFunction(
      functions, options, "unix_seconds", SCALAR,
      {{int64_type, {timestamp_type}, FN_UNIX_SECONDS_FROM_TIMESTAMP}});
  InsertFunction(
      functions, options, "unix_millis", SCALAR,
      {{int64_type, {timestamp_type}, FN_UNIX_MILLIS_FROM_TIMESTAMP}});
  InsertFunction(
      functions, options, "unix_micros", SCALAR,
      {{int64_type, {timestamp_type}, FN_UNIX_MICROS_FROM_TIMESTAMP}});
}

void GetTimeAndDatetimeConstructionAndConversionFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions) {
  const Type* date_type = type_factory->get_date();
  const Type* datetime_type = type_factory->get_datetime();
  const Type* time_type = type_factory->get_time();
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* int64_type = type_factory->get_int64();
  const Type* string_type = type_factory->get_string();

  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  FunctionOptions time_and_datetime_function_options =
      FunctionOptions().add_required_language_feature(FEATURE_V_1_2_CIVIL_TIME);

  InsertFunction(functions, options, "time", SCALAR,
                 {
                     {time_type,
                      {
                          int64_type,  // hour
                          int64_type,  // minute
                          int64_type,  // second
                      },
                      FN_TIME_FROM_HOUR_MINUTE_SECOND},
                     {time_type,
                      {
                          timestamp_type,           // timestamp
                          {string_type, OPTIONAL},  // timezone
                      },
                      FN_TIME_FROM_TIMESTAMP},
                     {time_type,
                      {
                          datetime_type,  // datetime
                      },
                      FN_TIME_FROM_DATETIME},
                 },
                 time_and_datetime_function_options);
  InsertFunction(functions, options, "datetime", SCALAR,
                 {
                     {datetime_type,
                      {
                          int64_type,  // year
                          int64_type,  // month
                          int64_type,  // day
                          int64_type,  // hour
                          int64_type,  // minute
                          int64_type,  // second
                      },
                      FN_DATETIME_FROM_YEAR_MONTH_DAY_HOUR_MINUTE_SECOND},
                     {datetime_type,
                      {
                          date_type,  // date
                          time_type,  // time
                      },
                      FN_DATETIME_FROM_DATE_AND_TIME},
                     {datetime_type,
                      {
                          timestamp_type,           // timestamp
                          {string_type, OPTIONAL},  // timezone
                      },
                      FN_DATETIME_FROM_TIMESTAMP},
                     {datetime_type,
                      {
                          date_type,  // date
                      },
                      FN_DATETIME_FROM_DATE},
                 },
                 time_and_datetime_function_options);
}

void GetDatetimeCurrentFunctions(TypeFactory* type_factory,
                                 const ZetaSQLBuiltinFunctionOptions& options,
                                 NameToFunctionMap* functions) {
  const Type* date_type = type_factory->get_date();
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* string_type = type_factory->get_string();

  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  FunctionOptions function_is_stable;
  function_is_stable.set_volatility(FunctionEnums::STABLE);

  InsertFunction(functions, options, "current_date", SCALAR,
                 {{date_type, {{string_type, OPTIONAL}}, FN_CURRENT_DATE}},
                 function_is_stable);
  InsertFunction(functions, options, "current_timestamp", SCALAR,
                 {{timestamp_type, {}, FN_CURRENT_TIMESTAMP}},
                 function_is_stable);

  const Type* datetime_type = type_factory->get_datetime();
  const Type* time_type = type_factory->get_time();
  FunctionOptions require_civil_time_types(function_is_stable);
  require_civil_time_types.add_required_language_feature(
      FEATURE_V_1_2_CIVIL_TIME);
  InsertFunction(
      functions, options, "current_datetime", SCALAR,
      {{datetime_type, {{string_type, OPTIONAL}}, FN_CURRENT_DATETIME}},
      require_civil_time_types);
  InsertFunction(functions, options, "current_time", SCALAR,
                 {{time_type, {{string_type, OPTIONAL}}, FN_CURRENT_TIME}},
                 require_civil_time_types);
}

void GetDatetimeAddSubFunctions(TypeFactory* type_factory,
                                const ZetaSQLBuiltinFunctionOptions& options,
                                NameToFunctionMap* functions) {
  const Type* date_type = type_factory->get_date();
  const Type* datetime_type = type_factory->get_datetime();
  const Type* time_type = type_factory->get_time();
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* int64_type = type_factory->get_int64();
  const Type* datepart_type = types::DatePartEnumType();

  const Function::Mode SCALAR = Function::SCALAR;

  FunctionOptions require_civil_time_types;
  require_civil_time_types.add_required_language_feature(
      FEATURE_V_1_2_CIVIL_TIME);

  // Common signature for both DATE_ADD and DATE_SUB
#define DATE_ADDSUB_SIGNATURE(timestamp_type, function_id) \
  timestamp_type, \
  {timestamp_type, int64_type, datepart_type, {string_type, OPTIONAL}}, \
  function_id

  InsertFunction(
      functions, options, "date_add", SCALAR,
      {
          {date_type, {date_type, int64_type, datepart_type}, FN_DATE_ADD_DATE},
      },
      FunctionOptions()
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForDateOrTimeAddOrSubFunction)
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckDateAddDateSubArguments, "DATE_ADD"))
          .set_get_sql_callback(
              bind_front(&DateAddOrSubFunctionSQL, "DATE_ADD")));

  InsertFunction(
      functions, options, "datetime_add", SCALAR,
      {
          {datetime_type,
           {datetime_type, int64_type, datepart_type},
           FN_DATETIME_ADD},
      },
      require_civil_time_types.Copy()
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForDateOrTimeAddOrSubFunction)
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckDatetimeAddSubDiffArguments, "DATETIME_ADD"))
          .set_get_sql_callback(
              bind_front(&DateAddOrSubFunctionSQL, "DATETIME_ADD")));

  InsertFunction(
      functions, options, "time_add", SCALAR,
      {
          {time_type, {time_type, int64_type, datepart_type}, FN_TIME_ADD},
      },
      require_civil_time_types.Copy()
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForDateOrTimeAddOrSubFunction)
          // time_add is sharing the same argument constraint as timestamp_add
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckTimestampAddTimestampSubArguments, "TIME_ADD"))
          .set_get_sql_callback(
              bind_front(&DateAddOrSubFunctionSQL, "TIME_ADD")));

  InsertFunction(
      functions, options, "timestamp_add", SCALAR,
      {{timestamp_type,
        {timestamp_type, int64_type, datepart_type},
        FN_TIMESTAMP_ADD}},
      FunctionOptions()
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForDateOrTimeAddOrSubFunction)
          .set_pre_resolution_argument_constraint(bind_front(
              &CheckTimestampAddTimestampSubArguments, "TIMESTAMP_ADD"))
          .set_get_sql_callback(
              bind_front(&DateAddOrSubFunctionSQL, "TIMESTAMP_ADD")));

  InsertFunction(
      functions, options, "date_sub", SCALAR,
      {
          {date_type, {date_type, int64_type, datepart_type}, FN_DATE_SUB_DATE},
      },
      FunctionOptions()
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForDateOrTimeAddOrSubFunction)
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckDateAddDateSubArguments, "DATE_SUB"))
          .set_get_sql_callback(
              bind_front(&DateAddOrSubFunctionSQL, "DATE_SUB")));

  InsertFunction(
      functions, options, "datetime_sub", SCALAR,
      {
          {datetime_type,
           {datetime_type, int64_type, datepart_type},
           FN_DATETIME_SUB},
      },
      require_civil_time_types.Copy()
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForDateOrTimeAddOrSubFunction)
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckDatetimeAddSubDiffArguments, "DATETIME_SUB"))
          .set_get_sql_callback(
              bind_front(&DateAddOrSubFunctionSQL, "DATETIME_SUB")));

  InsertFunction(
      functions, options, "time_sub", SCALAR,
      {
          {time_type, {time_type, int64_type, datepart_type}, FN_TIME_SUB},
      },
      require_civil_time_types.Copy()
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForDateOrTimeAddOrSubFunction)
          // time_sub is sharing the same argument constraint as timestamp_sub
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckTimestampAddTimestampSubArguments, "TIME_SUB"))
          .set_get_sql_callback(
              bind_front(&DateAddOrSubFunctionSQL, "TIME_SUB")));

  InsertFunction(
      functions, options, "timestamp_sub", SCALAR,
      {
          {timestamp_type,
           {timestamp_type, int64_type, datepart_type},
           FN_TIMESTAMP_SUB},
      },
      FunctionOptions()
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForDateOrTimeAddOrSubFunction)
          .set_pre_resolution_argument_constraint(bind_front(
              &CheckTimestampAddTimestampSubArguments, "TIMESTAMP_SUB"))
          .set_get_sql_callback(
              bind_front(&DateAddOrSubFunctionSQL, "TIMESTAMP_SUB")));
}

void GetDatetimeDiffTruncFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions) {
  const Type* date_type = type_factory->get_date();
  const Type* datetime_type = type_factory->get_datetime();
  const Type* time_type = type_factory->get_time();
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* int64_type = type_factory->get_int64();
  const Type* string_type = type_factory->get_string();
  const Type* datepart_type = types::DatePartEnumType();

  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  FunctionOptions require_civil_time_types;
  require_civil_time_types.add_required_language_feature(
      FEATURE_V_1_2_CIVIL_TIME);

#define DATE_DIFF_SIGNATURE(timestamp_type, function_id) \
  int64_type, \
  {timestamp_type, timestamp_type, datepart_type, {string_type, OPTIONAL}}, \
  function_id

  const Type* diff_type = int64_type;

  InsertFunction(
      functions, options, "date_diff", SCALAR,
      {
          {diff_type, {date_type, date_type, datepart_type}, FN_DATE_DIFF_DATE},
      },
      FunctionOptions().set_pre_resolution_argument_constraint(
          bind_front(&CheckDateDiffArguments, "DATE_DIFF")));

  InsertFunction(
      functions, options, "datetime_diff", SCALAR,
      {
          {int64_type,
           {datetime_type, datetime_type, datepart_type},
           FN_DATETIME_DIFF},
      },
      require_civil_time_types.Copy().set_pre_resolution_argument_constraint(
          bind_front(&CheckDatetimeAddSubDiffArguments, "DATETIME_DIFF")));

  InsertFunction(
      functions, options, "time_diff", SCALAR,
      {
          {int64_type, {time_type, time_type, datepart_type}, FN_TIME_DIFF},
      },
      require_civil_time_types
          .Copy()
          // time_diff is sharing the same argument constraint as timestamp_diff
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckTimestampDiffArguments, "TIME_DIFF")));

  InsertFunction(
      functions, options, "timestamp_diff", SCALAR,
      {{int64_type,
        {timestamp_type, timestamp_type, datepart_type},
        FN_TIMESTAMP_DIFF}},
      FunctionOptions().set_pre_resolution_argument_constraint(
          bind_front(&CheckTimestampDiffArguments, "TIMESTAMP_DIFF")));

#define DATE_TRUNC_SIGNATURE(timestamp_type, function_id) \
  timestamp_type, \
  {timestamp_type, datepart_type, {string_type, OPTIONAL}}, \
  function_id

  InsertFunction(
      functions, options, "date_trunc", SCALAR,
      {
          {date_type, {date_type, datepart_type}, FN_DATE_TRUNC_DATE},
      },
      FunctionOptions().set_pre_resolution_argument_constraint(
          bind_front(&CheckDateTruncArguments, "DATE_TRUNC")));

  InsertFunction(
      functions, options, "datetime_trunc", SCALAR,
      {{datetime_type, {datetime_type, datepart_type}, FN_DATETIME_TRUNC}},
      require_civil_time_types
          .Copy()
          // datetime_trunc is sharing the same argument constraint as
          // timestamp_trunc
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckTimestampTruncArguments, "DATETIME_TRUNC")));

  InsertFunction(
      functions, options, "time_trunc", SCALAR,
      {{time_type, {time_type, datepart_type}, FN_TIME_TRUNC}},
      require_civil_time_types.Copy().set_pre_resolution_argument_constraint(
          bind_front(&CheckTimeTruncArguments, "TIME_TRUNC")));

  InsertFunction(
      functions, options, "timestamp_trunc", SCALAR,
      {{timestamp_type,
        {timestamp_type, datepart_type, {string_type, OPTIONAL}},
        FN_TIMESTAMP_TRUNC}},
      FunctionOptions().set_pre_resolution_argument_constraint(
          bind_front(&CheckTimestampTruncArguments, "TIMESTAMP_TRUNC")));
}

void GetDatetimeFormatFunctions(TypeFactory* type_factory,
                                const ZetaSQLBuiltinFunctionOptions& options,
                                NameToFunctionMap* functions) {
  const Type* date_type = type_factory->get_date();
  const Type* datetime_type = type_factory->get_datetime();
  const Type* time_type = type_factory->get_time();
  const Type* timestamp_type = type_factory->get_timestamp();
  const Type* string_type = type_factory->get_string();

  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  FunctionOptions require_civil_time_types;
  require_civil_time_types.add_required_language_feature(
      FEATURE_V_1_2_CIVIL_TIME);

  InsertFunction(functions, options, "format_date", SCALAR,
                 {{string_type, {string_type, date_type}, FN_FORMAT_DATE}});
  InsertFunction(
      functions, options, "format_datetime", SCALAR,
      {{string_type, {string_type, datetime_type}, FN_FORMAT_DATETIME}},
      require_civil_time_types.Copy());
  InsertFunction(functions, options, "format_time", SCALAR,
                 {{string_type, {string_type, time_type}, FN_FORMAT_TIME}},
                 require_civil_time_types.Copy());
  InsertFunction(functions, options, "format_timestamp", SCALAR,
                 {{string_type,
                   {string_type, timestamp_type, {string_type, OPTIONAL}},
                   FN_FORMAT_TIMESTAMP}});

  InsertFunction(functions, options, "parse_date", SCALAR,
                 {{date_type, {string_type, string_type},
                   FN_PARSE_DATE}});
  InsertFunction(functions, options, "parse_datetime", SCALAR,
                 {{datetime_type, {string_type, string_type},
                   FN_PARSE_DATETIME}},
                 require_civil_time_types.Copy());
  InsertFunction(functions, options, "parse_time", SCALAR,
                 {{time_type, {string_type, string_type},
                   FN_PARSE_TIME}},
                 require_civil_time_types.Copy());
  InsertFunction(functions, options, "parse_timestamp", SCALAR,
                 {{timestamp_type,
                   {string_type, string_type, {string_type, OPTIONAL}},
                   FN_PARSE_TIMESTAMP}});
}

void GetDatetimeFunctions(TypeFactory* type_factory,
                          const ZetaSQLBuiltinFunctionOptions& options,
                          NameToFunctionMap* functions) {
  GetDatetimeConversionFunctions(type_factory, options, functions);
  GetDatetimeCurrentFunctions(type_factory, options, functions);
  GetDatetimeExtractFunctions(type_factory, options, functions);
  GetDatetimeFormatFunctions(type_factory, options, functions);
  GetDatetimeAddSubFunctions(type_factory, options, functions);
  GetDatetimeDiffTruncFunctions(type_factory, options, functions);

  GetTimeAndDatetimeConstructionAndConversionFunctions(type_factory, options,
                                                       functions);
}

void GetArithmeticFunctions(TypeFactory* type_factory,
                            const ZetaSQLBuiltinFunctionOptions& options,
                            NameToFunctionMap* functions) {
  const Type* int32_type = type_factory->get_int32();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* float_type = type_factory->get_float();
  const Type* double_type = type_factory->get_double();
  const Type* numeric_type = type_factory->get_numeric();
  const Type* bignumeric_type = type_factory->get_bignumeric();

  const Function::Mode SCALAR = Function::SCALAR;

  FunctionSignatureOptions has_floating_point_argument;
  has_floating_point_argument.set_constraints(&HasFloatingPointArgument);
  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&HasNumericTypeArgument);
  FunctionSignatureOptions has_bignumeric_type_argument;
  has_bignumeric_type_argument.set_constraints(&HasBigNumericTypeArgument);

  // Note that the '$' prefix is used in function names for those that do not
  // support function call syntax.  Otherwise, syntax like ADD(<op1>, <op2>)
  // would be implicitly supported.

  // Note that these arithmetic operators (+, -, *, /, <unary minus>) have
  // related SAFE versions (SAFE_ADD, SAFE_SUBTRACT, etc.) that must have
  // the same signatures as these operators.
  InsertFunction(
      functions, options, "$add", SCALAR,
      {{int64_type, {int64_type, int64_type}, FN_ADD_INT64},
       {uint64_type, {uint64_type, uint64_type}, FN_ADD_UINT64},
       {double_type,
        {double_type, double_type},
        FN_ADD_DOUBLE,
        has_floating_point_argument},
       {numeric_type,
        {numeric_type, numeric_type},
        FN_ADD_NUMERIC,
        has_numeric_type_argument},
       {bignumeric_type,
        {bignumeric_type, bignumeric_type},
        FN_ADD_BIGNUMERIC,
        has_bignumeric_type_argument}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("+")
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "+")));

  InsertFunction(
      functions, options, "$subtract", SCALAR,
      {{int64_type, {int64_type, int64_type}, FN_SUBTRACT_INT64},
       {int64_type, {uint64_type, uint64_type}, FN_SUBTRACT_UINT64},
       {numeric_type,
        {numeric_type, numeric_type},
        FN_SUBTRACT_NUMERIC,
        has_numeric_type_argument},
       {bignumeric_type,
        {bignumeric_type, bignumeric_type},
        FN_SUBTRACT_BIGNUMERIC,
        has_bignumeric_type_argument},
       {double_type,
        {double_type, double_type},
        FN_SUBTRACT_DOUBLE,
        has_floating_point_argument}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("-")
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "-")));

  InsertFunction(
      functions, options, "$divide", SCALAR,
      {{double_type, {double_type, double_type}, FN_DIVIDE_DOUBLE},
       {numeric_type,
        {numeric_type, numeric_type},
        FN_DIVIDE_NUMERIC,
        has_numeric_type_argument},
       {bignumeric_type,
        {bignumeric_type, bignumeric_type},
        FN_DIVIDE_BIGNUMERIC,
        has_bignumeric_type_argument}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("/")
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "/")));

  InsertFunction(
      functions, options, "$multiply", SCALAR,
      {{int64_type, {int64_type, int64_type}, FN_MULTIPLY_INT64},
       {uint64_type, {uint64_type, uint64_type}, FN_MULTIPLY_UINT64},
       {double_type,
        {double_type, double_type},
        FN_MULTIPLY_DOUBLE,
        has_floating_point_argument},
       {numeric_type,
        {numeric_type, numeric_type},
        FN_MULTIPLY_NUMERIC,
        has_numeric_type_argument},
       {bignumeric_type,
        {bignumeric_type, bignumeric_type},
        FN_MULTIPLY_BIGNUMERIC,
        has_bignumeric_type_argument}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("*")
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "*")));

  // We do not allow arguments to be coerced because we don't want unary
  // minus to apply to UINT32/UINT64 by implicitly matching the INT64/DOUBLE
  // signatures.
  InsertFunction(functions, options, "$unary_minus", SCALAR,
                 {{int32_type, {int32_type}, FN_UNARY_MINUS_INT32},
                  {int64_type, {int64_type}, FN_UNARY_MINUS_INT64},
                  {float_type, {float_type}, FN_UNARY_MINUS_FLOAT},
                  {double_type, {double_type}, FN_UNARY_MINUS_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_UNARY_MINUS_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type},
                   FN_UNARY_MINUS_BIGNUMERIC,
                   has_bignumeric_type_argument}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_sql_name("-")
                     .set_arguments_are_coercible(false)
                     .set_get_sql_callback(
                         bind_front(&PreUnaryFunctionSQL, "-")));
}

void GetBitwiseFunctions(TypeFactory* type_factory,
                         const ZetaSQLBuiltinFunctionOptions& options,
                         NameToFunctionMap* functions) {
  const Type* int32_type = type_factory->get_int32();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint32_type = type_factory->get_uint32();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* bytes_type = type_factory->get_bytes();

  const Function::Mode SCALAR = Function::SCALAR;

  InsertFunction(functions, options, "$bitwise_not", SCALAR,
                 {{int32_type, {int32_type}, FN_BITWISE_NOT_INT32},
                  {int64_type, {int64_type}, FN_BITWISE_NOT_INT64},
                  {uint32_type, {uint32_type}, FN_BITWISE_NOT_UINT32},
                  {uint64_type, {uint64_type}, FN_BITWISE_NOT_UINT64},
                  {bytes_type, {bytes_type}, FN_BITWISE_NOT_BYTES}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_sql_name("~")
                     .set_get_sql_callback(
                         bind_front(&PreUnaryFunctionSQL, "~")));
  InsertFunction(
      functions, options, "$bitwise_or", SCALAR,
      {{int32_type, {int32_type, int32_type}, FN_BITWISE_OR_INT32},
       {int64_type, {int64_type, int64_type}, FN_BITWISE_OR_INT64},
       {uint32_type, {uint32_type, uint32_type}, FN_BITWISE_OR_UINT32},
       {uint64_type, {uint64_type, uint64_type}, FN_BITWISE_OR_UINT64},
       {bytes_type, {bytes_type, bytes_type}, FN_BITWISE_OR_BYTES}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("|")
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckBitwiseOperatorArgumentsHaveSameType, "|"))
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "|")));
  InsertFunction(
      functions, options, "$bitwise_xor", SCALAR,
      {{int32_type, {int32_type, int32_type}, FN_BITWISE_XOR_INT32},
       {int64_type, {int64_type, int64_type}, FN_BITWISE_XOR_INT64},
       {uint32_type, {uint32_type, uint32_type}, FN_BITWISE_XOR_UINT32},
       {uint64_type, {uint64_type, uint64_type}, FN_BITWISE_XOR_UINT64},
       {bytes_type, {bytes_type, bytes_type}, FN_BITWISE_XOR_BYTES}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("^")
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckBitwiseOperatorArgumentsHaveSameType, "^"))
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "^")));
  InsertFunction(
      functions, options, "$bitwise_and", SCALAR,
      {{int32_type, {int32_type, int32_type}, FN_BITWISE_AND_INT32},
       {int64_type, {int64_type, int64_type}, FN_BITWISE_AND_INT64},
       {uint32_type, {uint32_type, uint32_type}, FN_BITWISE_AND_UINT32},
       {uint64_type, {uint64_type, uint64_type}, FN_BITWISE_AND_UINT64},
       {bytes_type, {bytes_type, bytes_type}, FN_BITWISE_AND_BYTES}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("&")
          .set_pre_resolution_argument_constraint(
              bind_front(&CheckBitwiseOperatorArgumentsHaveSameType, "&"))
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "&")));
  InsertFunction(
      functions, options, "$bitwise_left_shift", SCALAR,
      {{int32_type, {int32_type, int64_type}, FN_BITWISE_LEFT_SHIFT_INT32},
       {int64_type, {int64_type, int64_type}, FN_BITWISE_LEFT_SHIFT_INT64},
       {uint32_type, {uint32_type, int64_type}, FN_BITWISE_LEFT_SHIFT_UINT32},
       {uint64_type, {uint64_type, int64_type}, FN_BITWISE_LEFT_SHIFT_UINT64},
       {bytes_type, {bytes_type, int64_type}, FN_BITWISE_LEFT_SHIFT_BYTES}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("<<")
          .set_pre_resolution_argument_constraint(bind_front(
              &CheckBitwiseOperatorFirstArgumentIsIntegerOrBytes, "<<"))
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "<<")));
  InsertFunction(
      functions, options, "$bitwise_right_shift", SCALAR,
      {{int32_type, {int32_type, int64_type}, FN_BITWISE_RIGHT_SHIFT_INT32},
       {int64_type, {int64_type, int64_type}, FN_BITWISE_RIGHT_SHIFT_INT64},
       {uint32_type, {uint32_type, int64_type}, FN_BITWISE_RIGHT_SHIFT_UINT32},
       {uint64_type, {uint64_type, int64_type}, FN_BITWISE_RIGHT_SHIFT_UINT64},
       {bytes_type, {bytes_type, int64_type}, FN_BITWISE_RIGHT_SHIFT_BYTES}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name(">>")
          .set_pre_resolution_argument_constraint(bind_front(
              &CheckBitwiseOperatorFirstArgumentIsIntegerOrBytes, ">>"))
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, ">>")));
  InsertFunction(functions, options, "bit_count", SCALAR,
                 {{int64_type, {int32_type}, FN_BIT_COUNT_INT32},
                  {int64_type, {int64_type}, FN_BIT_COUNT_INT64},
                  {int64_type, {uint64_type}, FN_BIT_COUNT_UINT64},
                  {int64_type, {bytes_type}, FN_BIT_COUNT_BYTES}});
}

void GetAggregateFunctions(TypeFactory* type_factory,
                           const ZetaSQLBuiltinFunctionOptions& options,
                           NameToFunctionMap* functions) {
  const Type* int32_type = type_factory->get_int32();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint32_type = type_factory->get_uint32();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* double_type = type_factory->get_double();
  const Type* string_type = type_factory->get_string();
  const Type* bytes_type = type_factory->get_bytes();
  const Type* bool_type = type_factory->get_bool();
  const Type* numeric_type = type_factory->get_numeric();
  const Type* bignumeric_type = type_factory->get_bignumeric();

  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&HasNumericTypeArgument);
  FunctionSignatureOptions has_bignumeric_type_argument;
  has_bignumeric_type_argument.set_constraints(&HasBigNumericTypeArgument);

  const Function::Mode AGGREGATE = Function::AGGREGATE;

  InsertFunction(functions, options, "any_value", AGGREGATE,
                 {{ARG_TYPE_ANY_1, {ARG_TYPE_ANY_1}, FN_ANY_VALUE}});

  InsertFunction(functions, options, "count", AGGREGATE,
                 {{int64_type, {ARG_TYPE_ANY_1}, FN_COUNT}});

  InsertFunction(functions, options, "$count_star", AGGREGATE,
                 {{int64_type, {}, FN_COUNT_STAR}},
                 FunctionOptions()
                     .set_sql_name("count(*)")
                     .set_get_sql_callback(&CountStarFunctionSQL));

  InsertFunction(functions, options, "countif", AGGREGATE,
                 {{int64_type, {bool_type}, FN_COUNTIF}});

  // Let INT32 -> INT64, UINT32 -> UINT64, and FLOAT -> DOUBLE.
  InsertFunction(functions, options, "sum", AGGREGATE,
                 {{int64_type, {int64_type}, FN_SUM_INT64},
                  {uint64_type, {uint64_type}, FN_SUM_UINT64},
                  {double_type, {double_type}, FN_SUM_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_SUM_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type},
                   FN_SUM_BIGNUMERIC,
                   has_bignumeric_type_argument}});

  InsertFunction(functions, options, "avg", AGGREGATE,
                 {{double_type, {int64_type}, FN_AVG_INT64},
                  {double_type, {uint64_type}, FN_AVG_UINT64},
                  {double_type, {double_type}, FN_AVG_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_AVG_NUMERIC,
                   has_numeric_type_argument},
                  {bignumeric_type,
                   {bignumeric_type},
                   FN_AVG_BIGNUMERIC,
                   has_bignumeric_type_argument}});

  InsertFunction(functions, options, "bit_and", AGGREGATE,
                 {{int32_type, {int32_type}, FN_BIT_AND_INT32},
                  {int64_type, {int64_type}, FN_BIT_AND_INT64},
                  {uint32_type, {uint32_type}, FN_BIT_AND_UINT32},
                  {uint64_type, {uint64_type}, FN_BIT_AND_UINT64}});

  InsertFunction(functions, options, "bit_or", AGGREGATE,
                 {{int32_type, {int32_type}, FN_BIT_OR_INT32},
                  {int64_type, {int64_type}, FN_BIT_OR_INT64},
                  {uint32_type, {uint32_type}, FN_BIT_OR_UINT32},
                  {uint64_type, {uint64_type}, FN_BIT_OR_UINT64}});

  InsertFunction(functions, options, "bit_xor", AGGREGATE,
                 {{int32_type, {int32_type}, FN_BIT_XOR_INT32},
                  {int64_type, {int64_type}, FN_BIT_XOR_INT64},
                  {uint32_type, {uint32_type}, FN_BIT_XOR_UINT32},
                  {uint64_type, {uint64_type}, FN_BIT_XOR_UINT64}});

  InsertFunction(functions, options, "logical_and", AGGREGATE,
                 {{bool_type, {bool_type}, FN_LOGICAL_AND}});

  InsertFunction(functions, options, "logical_or", AGGREGATE,
                 {{bool_type, {bool_type}, FN_LOGICAL_OR}});

  // Resolution will verify that argument types are valid (not proto, struct,
  // or array).
  InsertFunction(functions, options, "min", AGGREGATE,
                 {{ARG_TYPE_ANY_1, {ARG_TYPE_ANY_1}, FN_MIN}},
                 FunctionOptions().set_pre_resolution_argument_constraint(
                     bind_front(&CheckMinMaxGreatestLeastArguments, "MIN")));

  InsertFunction(functions, options, "max", AGGREGATE,
                 {{ARG_TYPE_ANY_1, {ARG_TYPE_ANY_1}, FN_MAX}},
                 FunctionOptions().set_pre_resolution_argument_constraint(
                     bind_front(&CheckMinMaxGreatestLeastArguments, "MAX")));

  FunctionArgumentTypeOptions non_null_non_agg;
  non_null_non_agg.set_is_not_aggregate();
  non_null_non_agg.set_must_be_non_null();

  InsertFunction(
      functions, options, "string_agg", AGGREGATE,
      {{string_type, {string_type}, FN_STRING_AGG_STRING},
       // Resolution will verify that the second argument must be a literal.
       {string_type,
        {string_type, {string_type, non_null_non_agg}},
        FN_STRING_AGG_DELIM_STRING},
       // Bytes inputs
       {bytes_type, {bytes_type}, FN_STRING_AGG_BYTES},
       // Resolution will verify that the second argument must be a literal.
       {bytes_type,
        {bytes_type, {bytes_type, non_null_non_agg}},
        FN_STRING_AGG_DELIM_BYTES}},
      FunctionOptions().set_supports_order_by(true).set_supports_limit(true));

  InsertFunction(
      functions, options, "array_agg", AGGREGATE,
      {{ARG_ARRAY_TYPE_ANY_1, {ARG_TYPE_ANY_1}, FN_ARRAY_AGG}},
      FunctionOptions()
          .set_pre_resolution_argument_constraint(&CheckArrayAggArguments)
          .set_supports_null_handling_modifier(true)
          .set_supports_order_by(true)
          .set_supports_limit(true));

  InsertFunction(
      functions, options, "array_concat_agg", AGGREGATE,
      {{ARG_ARRAY_TYPE_ANY_1, {ARG_ARRAY_TYPE_ANY_1}, FN_ARRAY_CONCAT_AGG}},
      FunctionOptions()
          .set_pre_resolution_argument_constraint(&CheckArrayConcatArguments)
          .set_supports_order_by(true)
          .set_supports_limit(true));
}

void GetApproxFunctions(TypeFactory* type_factory,
                        const ZetaSQLBuiltinFunctionOptions& options,
                        NameToFunctionMap* functions) {
  const Type* int64_type = type_factory->get_int64();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* double_type = type_factory->get_double();
  const Type* numeric_type = type_factory->get_numeric();
  const Type* bignumeric_type = type_factory->get_bignumeric();

  const Function::Mode AGGREGATE = Function::AGGREGATE;

  FunctionArgumentTypeOptions comparable;
  comparable.set_must_support_ordering();

  FunctionArgumentTypeOptions supports_grouping;
  supports_grouping.set_must_support_grouping();

  FunctionArgumentTypeOptions non_null_positive_non_agg;
  non_null_positive_non_agg.set_is_not_aggregate();
  non_null_positive_non_agg.set_must_be_non_null();
  non_null_positive_non_agg.set_min_value(1);

  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&HasNumericTypeArgument);
  FunctionSignatureOptions has_bignumeric_type_argument;
  has_bignumeric_type_argument.set_constraints(&HasBigNumericTypeArgument);

  InsertFunction(functions, options, "approx_count_distinct", AGGREGATE,
                 {{int64_type,
                   {{ARG_TYPE_ANY_1, supports_grouping}},
                   FN_APPROX_COUNT_DISTINCT}});

  InsertFunction(functions, options, "approx_quantiles", AGGREGATE,
                 {{ARG_ARRAY_TYPE_ANY_1,
                   {{ARG_TYPE_ANY_1, comparable},
                    {int64_type, non_null_positive_non_agg}},
                   FN_APPROX_QUANTILES}},
                 FunctionOptions().set_supports_null_handling_modifier(true));

  InsertFunction(functions, options, "approx_top_count", AGGREGATE,
                 {{ARG_TYPE_ANY_1,  // Return type will be overridden.
                   {{ARG_TYPE_ANY_1, supports_grouping},
                    {int64_type, non_null_positive_non_agg}},
                   FN_APPROX_TOP_COUNT}},
                 FunctionOptions().set_compute_result_type_callback(
                     bind_front(&ComputeResultTypeForTopStruct, "count")));

  InsertFunction(functions, options, "approx_top_sum", AGGREGATE,
                 {{ARG_TYPE_ANY_1,  // Return type will be overridden.
                   {{ARG_TYPE_ANY_1, supports_grouping},
                    int64_type,
                    {int64_type, non_null_positive_non_agg}},
                   FN_APPROX_TOP_SUM_INT64},
                  {ARG_TYPE_ANY_1,  // Return type will be overridden.
                   {{ARG_TYPE_ANY_1, supports_grouping},
                    uint64_type,
                    {int64_type, non_null_positive_non_agg}},
                   FN_APPROX_TOP_SUM_UINT64},
                  {ARG_TYPE_ANY_1,  // Return type will be overridden.
                   {{ARG_TYPE_ANY_1, supports_grouping},
                    double_type,
                    {int64_type, non_null_positive_non_agg}},
                   FN_APPROX_TOP_SUM_DOUBLE},
                  {ARG_TYPE_ANY_1,  // Return type will be overridden.
                   {{ARG_TYPE_ANY_1, supports_grouping},
                    numeric_type,
                    {int64_type, non_null_positive_non_agg}},
                   FN_APPROX_TOP_SUM_NUMERIC,
                   has_numeric_type_argument},
                  {ARG_TYPE_ANY_1,  // Return type will be overridden.
                   {{ARG_TYPE_ANY_1, supports_grouping},
                    bignumeric_type,
                    {int64_type, non_null_positive_non_agg}},
                   FN_APPROX_TOP_SUM_BIGNUMERIC,
                   has_bignumeric_type_argument}},
                 FunctionOptions().set_compute_result_type_callback(
                     bind_front(&ComputeResultTypeForTopStruct, "sum")));
}

void GetStatisticalFunctions(TypeFactory* type_factory,
                             const ZetaSQLBuiltinFunctionOptions& options,
                             NameToFunctionMap* functions) {
  const Type* double_type = type_factory->get_double();
  const Type* numeric_type = type_factory->get_numeric();
  const Function::Mode AGGREGATE = Function::AGGREGATE;

  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&HasNumericTypeArgument);

  // Support statistical functions:
  // CORR, COVAR_POP, COVAR_SAMP,
  // STDDEV_POP, STDDEV_SAMP, STDDEV (alias for STDDEV_SAMP),
  // VAR_POP, VAR_SAMP, VARIANCE (alias for VAR_SAMP)
  // in both modes: aggregate and analytic.
  // Enable the language feature to have signatures with numeric input.
  // Binary functions
  if (!options.language_options.LanguageFeatureEnabled(
          FEATURE_NUMERIC_COVAR_CORR_SIGNATURES)) {
    InsertFunction(functions, options, "corr", AGGREGATE,
                   {{double_type, {double_type, double_type}, FN_CORR}});
    InsertFunction(functions, options, "covar_pop", AGGREGATE,
                   {{double_type, {double_type, double_type}, FN_COVAR_POP}});
    InsertFunction(functions, options, "covar_samp", AGGREGATE,
                   {{double_type, {double_type, double_type}, FN_COVAR_SAMP}});
  } else {
    InsertFunction(functions, options, "corr", AGGREGATE,
                   {{double_type,
                     {numeric_type, numeric_type},
                     FN_CORR_NUMERIC,
                     has_numeric_type_argument},
                    {double_type, {double_type, double_type}, FN_CORR}});
    InsertFunction(functions, options, "covar_pop", AGGREGATE,
                   {{double_type,
                     {numeric_type, numeric_type},
                     FN_COVAR_POP_NUMERIC,
                     has_numeric_type_argument},
                    {double_type, {double_type, double_type}, FN_COVAR_POP}});
    InsertFunction(functions, options, "covar_samp", AGGREGATE,
                   {{double_type,
                     {numeric_type, numeric_type},
                     FN_COVAR_SAMP_NUMERIC,
                     has_numeric_type_argument},
                    {double_type, {double_type, double_type}, FN_COVAR_SAMP}});
  }
  // Unary functions
  if (!options.language_options.LanguageFeatureEnabled(
          FEATURE_NUMERIC_VARIANCE_STDDEV_SIGNATURES)) {
    InsertFunction(functions, options, "stddev_pop", AGGREGATE,
                   {{double_type, {double_type}, FN_STDDEV_POP}});
    InsertFunction(functions, options, "stddev_samp", AGGREGATE,
                   {{double_type, {double_type}, FN_STDDEV_SAMP}},
                   FunctionOptions().set_alias_name("stddev"));
    InsertFunction(functions, options, "var_pop", AGGREGATE,
                   {{double_type, {double_type}, FN_VAR_POP}});
    InsertFunction(functions, options, "var_samp", AGGREGATE,
                   {{double_type, {double_type}, FN_VAR_SAMP}},
                   FunctionOptions().set_alias_name("variance"));
  } else {
    InsertFunction(functions, options, "stddev_pop", AGGREGATE,
                   {{double_type,
                     {numeric_type},
                     FN_STDDEV_POP_NUMERIC,
                     has_numeric_type_argument},
                    {double_type, {double_type}, FN_STDDEV_POP}});
    InsertFunction(functions, options, "stddev_samp", AGGREGATE,
                   {{double_type,
                     {numeric_type},
                     FN_STDDEV_SAMP_NUMERIC,
                     has_numeric_type_argument},
                    {double_type, {double_type}, FN_STDDEV_SAMP}},
                   FunctionOptions().set_alias_name("stddev"));
    InsertFunction(functions, options, "var_pop", AGGREGATE,
                   {{double_type,
                     {numeric_type},
                     FN_VAR_POP_NUMERIC,
                     has_numeric_type_argument},
                    {double_type, {double_type}, FN_VAR_POP}});
    InsertFunction(functions, options, "var_samp", AGGREGATE,
                   {{double_type,
                     {numeric_type},
                     FN_VAR_SAMP_NUMERIC,
                     has_numeric_type_argument},
                    {double_type, {double_type}, FN_VAR_SAMP}},
                   FunctionOptions().set_alias_name("variance"));
  }
}

void GetAnalyticFunctions(TypeFactory* type_factory,
                          const ZetaSQLBuiltinFunctionOptions& options,
                          NameToFunctionMap* functions) {
  const Type* int64_type = type_factory->get_int64();
  const Type* double_type = type_factory->get_double();
  const Function::Mode ANALYTIC = Function::ANALYTIC;

  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  const FunctionOptions::WindowOrderSupport ORDER_UNSUPPORTED =
      FunctionOptions::ORDER_UNSUPPORTED;
  const FunctionOptions::WindowOrderSupport ORDER_OPTIONAL =
      FunctionOptions::ORDER_OPTIONAL;
  const FunctionOptions::WindowOrderSupport ORDER_REQUIRED =
      FunctionOptions::ORDER_REQUIRED;

  const FunctionOptions optional_order_disallowed_frame(
      ORDER_OPTIONAL, false /* window_framing_support */);
  const FunctionOptions required_order_disallowed_frame(
      ORDER_REQUIRED, false /* window_framing_support */);
  const FunctionOptions required_order_allowed_frame(
      ORDER_REQUIRED, true /* window_framing_support */);
  const FunctionOptions required_order_allowed_frame_and_null_handling =
      FunctionOptions(required_order_allowed_frame)
          .set_supports_null_handling_modifier(true);
  const FunctionOptions disallowed_order_and_frame_allowed_null_handling =
      FunctionOptions(ORDER_UNSUPPORTED, false /* window_framing_support */)
          .set_supports_null_handling_modifier(true);

  FunctionArgumentTypeOptions non_null;
  non_null.set_must_be_non_null();

  FunctionArgumentTypeOptions non_null_positive_non_agg;
  non_null_positive_non_agg.set_is_not_aggregate();
  non_null_positive_non_agg.set_must_be_non_null();
  non_null_positive_non_agg.set_min_value(1);

  FunctionArgumentTypeOptions non_null_non_agg_between_0_and_1;
  non_null_non_agg_between_0_and_1.set_is_not_aggregate();
  non_null_non_agg_between_0_and_1.set_must_be_non_null();
  non_null_non_agg_between_0_and_1.set_min_value(0);
  non_null_non_agg_between_0_and_1.set_max_value(1);

  FunctionArgumentTypeOptions optional_non_null_non_agg;
  optional_non_null_non_agg.set_cardinality(OPTIONAL);
  optional_non_null_non_agg.set_is_not_aggregate();
  optional_non_null_non_agg.set_must_be_non_null();

  FunctionArgumentTypeOptions comparable;
  comparable.set_must_support_ordering();

  InsertFunction(functions, options, "dense_rank", ANALYTIC,
                 {{int64_type, {}, FN_DENSE_RANK}},
                 required_order_disallowed_frame);
  InsertFunction(functions, options, "rank", ANALYTIC,
                 {{int64_type, {}, FN_RANK}}, required_order_disallowed_frame);
  InsertFunction(functions, options, "percent_rank", ANALYTIC,
                 {{double_type, {}, FN_PERCENT_RANK}},
                 required_order_disallowed_frame);
  InsertFunction(functions, options, "cume_dist", ANALYTIC,
                 {{double_type, {}, FN_CUME_DIST}},
                 required_order_disallowed_frame);
  InsertFunction(
      functions, options, "ntile", ANALYTIC,
      {{int64_type, {{int64_type, non_null_positive_non_agg}}, FN_NTILE}},
      required_order_disallowed_frame);

  InsertFunction(functions, options, "row_number", ANALYTIC,
                 {{int64_type, {}, FN_ROW_NUMBER}},
                 optional_order_disallowed_frame);

  // The optional arguments will be populated in the resolved tree.
  // The second offset argument defaults to 1, and the third default
  // argument defaults to NULL.
  InsertFunction(
      functions, options, "lead", ANALYTIC,
      {{ARG_TYPE_ANY_1,
        {ARG_TYPE_ANY_1,
         {int64_type, optional_non_null_non_agg},
         // If present, the third argument must be a constant or parameter.
         // TODO: b/18709755: Give an error if it isn't.
         {ARG_TYPE_ANY_1, OPTIONAL}},
        FN_LEAD}},
      required_order_disallowed_frame);
  InsertFunction(
      functions, options, "lag", ANALYTIC,
      {{ARG_TYPE_ANY_1,
        {ARG_TYPE_ANY_1,
         {int64_type, optional_non_null_non_agg},
         // If present, the third argument must be a constant or parameter.
         // TODO: b/18709755: Give an error if it isn't.
         {ARG_TYPE_ANY_1, OPTIONAL}},
        FN_LAG}},
      required_order_disallowed_frame);

  InsertFunction(
      functions, options, "first_value", ANALYTIC,
      {{ARG_TYPE_ANY_1, {{ARG_TYPE_ANY_1, non_null}}, FN_FIRST_VALUE}},
      required_order_allowed_frame_and_null_handling);
  InsertFunction(
      functions, options, "last_value", ANALYTIC,
      {{ARG_TYPE_ANY_1, {{ARG_TYPE_ANY_1, non_null}}, FN_LAST_VALUE}},
      required_order_allowed_frame_and_null_handling);
  InsertFunction(functions, options, "nth_value", ANALYTIC,
                 {{ARG_TYPE_ANY_1,
                   {ARG_TYPE_ANY_1, {int64_type, non_null_positive_non_agg}},
                   FN_NTH_VALUE}},
                 required_order_allowed_frame_and_null_handling);

  InsertFunction(
      functions, options, "percentile_cont", ANALYTIC,
      {{double_type,
        {double_type, {double_type, non_null_non_agg_between_0_and_1}},
        FN_PERCENTILE_CONT}},
      disallowed_order_and_frame_allowed_null_handling);
  InsertFunction(functions, options, "percentile_disc", ANALYTIC,
                 {{ARG_TYPE_ANY_1,
                   {{ARG_TYPE_ANY_1, comparable},
                    {double_type, non_null_non_agg_between_0_and_1}},
                   FN_PERCENTILE_DISC}},
                 disallowed_order_and_frame_allowed_null_handling);
}

void GetBooleanFunctions(TypeFactory* type_factory,
                         const ZetaSQLBuiltinFunctionOptions& options,
                         NameToFunctionMap* functions) {
  const Type* bool_type = type_factory->get_bool();
  const Type* byte_type = type_factory->get_bytes();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* string_type = type_factory->get_string();

  const Function::Mode SCALAR = Function::SCALAR;

  const FunctionArgumentType::ArgumentCardinality REPEATED =
      FunctionArgumentType::REPEATED;

  InsertFunction(
      functions, options, "$equal", SCALAR,
      {{bool_type, {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_EQUAL},
       {bool_type, {int64_type, uint64_type}, FN_EQUAL_INT64_UINT64},
       {bool_type, {uint64_type, int64_type}, FN_EQUAL_UINT64_INT64}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("=")
          .set_post_resolution_argument_constraint(
              bind_front(&CheckArgumentsSupportEquality, "Equality"))
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForComparisonOperator)
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "=")));

  InsertFunction(
      functions, options, "$not_equal", SCALAR,
      {{bool_type, {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_NOT_EQUAL},
       {bool_type, {int64_type, uint64_type}, FN_NOT_EQUAL_INT64_UINT64},
       {bool_type, {uint64_type, int64_type}, FN_NOT_EQUAL_UINT64_INT64}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_sql_name("!=")
          .set_post_resolution_argument_constraint(
              bind_front(&CheckArgumentsSupportEquality, "Inequality"))
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForComparisonOperator)
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "!=")));

  InsertFunction(
      functions, options, "$less", SCALAR,
      {{bool_type, {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_LESS},
       {bool_type, {int64_type, uint64_type}, FN_LESS_INT64_UINT64},
       {bool_type, {uint64_type, int64_type}, FN_LESS_UINT64_INT64}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_post_resolution_argument_constraint(bind_front(
              &CheckArgumentsSupportComparison, "Less than"))
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForComparisonOperator)
          .set_sql_name("<")
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "<")));

  InsertFunction(
      functions, options, "$less_or_equal", SCALAR,
      {{bool_type, {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_LESS_OR_EQUAL},
       {bool_type, {int64_type, uint64_type}, FN_LESS_OR_EQUAL_INT64_UINT64},
       {bool_type, {uint64_type, int64_type}, FN_LESS_OR_EQUAL_UINT64_INT64}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_post_resolution_argument_constraint(
              bind_front(&CheckArgumentsSupportComparison, "Less than"))
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForComparisonOperator)
          .set_sql_name("<=")
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "<=")));

  InsertFunction(
      functions, options, "$greater_or_equal", SCALAR,
      {{bool_type, {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_GREATER_OR_EQUAL},
       {bool_type, {int64_type, uint64_type}, FN_GREATER_OR_EQUAL_INT64_UINT64},
       {bool_type,
        {uint64_type, int64_type},
        FN_GREATER_OR_EQUAL_UINT64_INT64}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_post_resolution_argument_constraint(
              bind_front(&CheckArgumentsSupportComparison, "Greater than"))
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForComparisonOperator)
          .set_sql_name(">=")
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, ">=")));

  InsertFunction(
      functions, options, "$greater", SCALAR,
      {{bool_type, {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_GREATER},
       {bool_type, {int64_type, uint64_type}, FN_GREATER_INT64_UINT64},
       {bool_type, {uint64_type, int64_type}, FN_GREATER_UINT64_INT64}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_post_resolution_argument_constraint(
              bind_front(&CheckArgumentsSupportComparison, "Greater than"))
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForComparisonOperator)
          .set_sql_name(">")
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, ">")));

  // Historically, BETWEEN had only one function signature where all
  // arguments must be coercible to the same type.  The implication is that
  // if arguments could not be coerced to the same type then resolving BETWEEN
  // would fail, including cases for BETWEEN with INT64 and UINT64 arguments
  // which logically should be ok.  To support such cases, additional
  // signatures have been added (and those signatures are protected by a
  // LanguageFeature so that engines can opt-in to those signatures when
  // ready).
  //
  // Note that we cannot rewrite BETWEEN into '>=' and '<=' because BETWEEN
  // has run-once semantics for each of its input expressions.
  if (!options.language_options.LanguageFeatureEnabled(
          FEATURE_BETWEEN_UINT64_INT64)) {
    InsertFunction(functions, options, "$between", SCALAR,
                   {{bool_type,
                     {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1, ARG_TYPE_ANY_1},
                     FN_BETWEEN}},
                   FunctionOptions()
                       .set_supports_safe_error_mode(false)
                       .set_post_resolution_argument_constraint(bind_front(
                           &CheckArgumentsSupportComparison, "BETWEEN"))
                       .set_get_sql_callback(&BetweenFunctionSQL));
  } else {
    InsertFunction(functions, options, "$between", SCALAR,
                   {{bool_type,
                     {int64_type, uint64_type, uint64_type},
                     FN_BETWEEN_INT64_UINT64_UINT64},
                    {bool_type,
                     {uint64_type, int64_type, uint64_type},
                     FN_BETWEEN_UINT64_INT64_UINT64},
                    {bool_type,
                     {uint64_type, uint64_type, int64_type},
                     FN_BETWEEN_UINT64_UINT64_INT64},
                    {bool_type,
                     {uint64_type, int64_type, int64_type},
                     FN_BETWEEN_UINT64_INT64_INT64},
                    {bool_type,
                     {int64_type, uint64_type, int64_type},
                     FN_BETWEEN_INT64_UINT64_INT64},
                    {bool_type,
                     {int64_type, int64_type, uint64_type},
                     FN_BETWEEN_INT64_INT64_UINT64},
                    {bool_type,
                     {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1, ARG_TYPE_ANY_1},
                     FN_BETWEEN}},
                   FunctionOptions()
                       .set_supports_safe_error_mode(false)
                       .set_post_resolution_argument_constraint(bind_front(
                           &CheckArgumentsSupportComparison, "BETWEEN"))
                       .set_get_sql_callback(&BetweenFunctionSQL));
  }

  InsertFunction(functions, options, "$like", SCALAR,
                 {{bool_type, {string_type, string_type}, FN_STRING_LIKE},
                  {bool_type, {byte_type, byte_type}, FN_BYTE_LIKE}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_no_matching_signature_callback(
                         &NoMatchingSignatureForComparisonOperator)
                     .set_get_sql_callback(
                         bind_front(&InfixFunctionSQL, "LIKE")));

  // TODO: Do we want to support IN for non-compatible integers, i.e.,
  // '<uint64col> IN (<int32col>, <int64col>)'?
  InsertFunction(
      functions, options, "$in", SCALAR,
      {{bool_type, {ARG_TYPE_ANY_1, {ARG_TYPE_ANY_1, REPEATED}}, FN_IN}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_post_resolution_argument_constraint(
              bind_front(&CheckArgumentsSupportEquality, "IN"))
          .set_no_matching_signature_callback(&NoMatchingSignatureForInFunction)
          .set_supported_signatures_callback(&EmptySupportedSignatures)
          .set_get_sql_callback(&InListFunctionSQL));

  // TODO: Do we want to support:
  //   '<uint64col>' IN UNNEST(<int64_array>)'?
  InsertFunction(
      functions, options, "$in_array", SCALAR,
      {{bool_type, {ARG_TYPE_ANY_1, ARG_ARRAY_TYPE_ANY_1}, FN_IN_ARRAY}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_pre_resolution_argument_constraint(
              // Verifies for <expr> IN UNNEST(<array_expr>)
              // * Argument to UNNEST is an array.
              // * <expr> and elements of <array_expr> are comparable.
              &CheckInArrayArguments)
          .set_no_matching_signature_callback(
              &NoMatchingSignatureForInArrayFunction)
          .set_sql_name("in unnest")
          .set_supported_signatures_callback(&EmptySupportedSignatures)
          .set_get_sql_callback(&InArrayFunctionSQL));
}

void GetLogicFunctions(TypeFactory* type_factory,
                       const ZetaSQLBuiltinFunctionOptions& options,
                       NameToFunctionMap* functions) {
  const Type* bool_type = type_factory->get_bool();

  const Function::Mode SCALAR = Function::SCALAR;

  const FunctionArgumentType::ArgumentCardinality REPEATED =
      FunctionArgumentType::REPEATED;

  InsertFunction(functions, options, "$is_null", SCALAR,
                 {{bool_type, {ARG_TYPE_ANY_1}, FN_IS_NULL}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_get_sql_callback(bind_front(
                         &PostUnaryFunctionSQL, " IS NULL")));
  InsertFunction(functions, options, "$is_true", SCALAR,
                 {{bool_type, {bool_type}, FN_IS_TRUE}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_get_sql_callback(bind_front(
                         &PostUnaryFunctionSQL, " IS TRUE")));
  InsertFunction(functions, options, "$is_false", SCALAR,
                 {{bool_type, {bool_type}, FN_IS_FALSE}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_get_sql_callback(bind_front(
                         &PostUnaryFunctionSQL, " IS FALSE")));

  InsertFunction(functions, options, "$and", SCALAR,
                 {{bool_type, {bool_type, {bool_type, REPEATED}}, FN_AND}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_get_sql_callback(
                         bind_front(&InfixFunctionSQL, "AND")));
  InsertFunction(functions, options, "$not", SCALAR,
                 {{bool_type, {bool_type}, FN_NOT}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_get_sql_callback(
                         bind_front(&PreUnaryFunctionSQL, "NOT ")));
  InsertFunction(
      functions, options, "$or", SCALAR,
      {{bool_type, {bool_type, {bool_type, REPEATED}}, FN_OR}},
      FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_get_sql_callback(bind_front(&InfixFunctionSQL, "OR")));
}

}  // namespace zetasql
