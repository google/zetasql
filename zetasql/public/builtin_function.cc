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

#include "zetasql/public/builtin_function.h"

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
#include "google/type/latlng.pb.h"
#include "google/type/timeofday.pb.h"
#include "zetasql/common/errors.h"
#include "zetasql/proto/options.pb.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/cycle_detector.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "zetasql/base/bind_front.h"
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

ZetaSQLBuiltinFunctionOptions::ZetaSQLBuiltinFunctionOptions(
    const ZetaSQLBuiltinFunctionOptionsProto& proto)
    : language_options(proto.language_options()) {
  for (int i = 0; i < proto.include_function_ids_size(); ++i) {
    include_function_ids.insert(proto.include_function_ids(i));
  }
  for (int i = 0; i < proto.exclude_function_ids_size(); ++i) {
    exclude_function_ids.insert(proto.exclude_function_ids(i));
  }
}

using ::zetasql_base::bind_front;
using ::zetasql::functions::DateTimestampPartToSQL;

using FunctionIdToNameMap = absl::flat_hash_map<FunctionSignatureId, std::string>;
using NameToFunctionMap = std::map<std::string, std::unique_ptr<Function>>;

// If std::string literal is compared against bytes, then we want the error message
// to be more specific and helpful.
static constexpr absl::string_view kErrorMessageCompareStringLiteralToBytes =
    ". STRING and BYTES are different types that are not directly "
    "comparable. To write a BYTES literal, use a b-prefixed literal "
    "such as b'bytes value'";

static const FunctionIdToNameMap& GetFunctionIdToNameMap() {
  static FunctionIdToNameMap* id_map = [] () {
    // Initialize map from ZetaSQL function to function names.
    FunctionIdToNameMap* id_map = new FunctionIdToNameMap();
    TypeFactory type_factory;
    NameToFunctionMap functions;

    // Enable the maximum language features.  This enables retrieving a maximum
    // set of functions and signatures.
    //
    // TODO: Maybe it is better to add a new function
    // GetAllZetaSQLFunctionsAndSignatures() that does not take any options,
    // to ensure that we get them all.  This could be a ZetaSQL-internal
    // only function.
    LanguageOptions options;
    options.EnableMaximumLanguageFeaturesForDevelopment();
    options.set_product_mode(PRODUCT_INTERNAL);

    GetZetaSQLFunctions(&type_factory, options, &functions);

    for (const auto& function_entry : functions) {
      for (const FunctionSignature& signature :
               function_entry.second->signatures()) {
        zetasql_base::InsertOrDie(
            id_map,
            static_cast<FunctionSignatureId>(signature.context_id()),
            function_entry.first);
      }
    }
    return id_map;
  } ();
  return *id_map;
}

const std::string FunctionSignatureIdToName(FunctionSignatureId id) {
  const std::string* name = zetasql_base::FindOrNull(GetFunctionIdToNameMap(), id);
  if (name != nullptr) {
    return *name;
  }
  return absl::StrCat("<INVALID FUNCTION ID: ", id, ">");
}

static bool ArgumentsAreComparable(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options, int* bad_argument_idx) {
  *bad_argument_idx = -1;
  for (int idx = 0; idx < arguments.size(); ++idx) {
    if (!arguments[idx].type()->SupportsOrdering(
            language_options, /*type_description=*/nullptr)) {
      *bad_argument_idx = idx;
      return false;
    }
  }
  return true;
}

// Checks whether all arguments are/are not arrays depending on the value of the
// 'is_array' flag.
static bool ArgumentsArrayType(const std::vector<InputArgumentType>& arguments,
                               bool is_array, int* bad_argument_idx) {
  *bad_argument_idx = -1;
  for (int idx = 0; idx < arguments.size(); ++idx) {
    if (arguments[idx].type()->IsArray() != is_array) {
      *bad_argument_idx = idx;
      return false;
    }
  }
  return true;
}

// The argument <display_name> in the below ...FunctionSQL represents the
// operator and should include space if inputs and operator needs to be space
// separated for pretty printing.
// TODO: Consider removing this callback, since Function now knows
// whether it is operator, and has correct sql_name to print.
static std::string InfixFunctionSQL(const std::string& display_name,
                               const std::vector<std::string>& inputs) {
  std::string sql;
  for (const std::string& text : inputs) {
    if (!sql.empty()) {
      absl::StrAppend(&sql, " ", absl::AsciiStrToUpper(display_name), " ");
    }
    // Check if input contains only alphanumeric characters or '_', in which
    // case enclosing ( )'s are not needed.
    if (find_if(
          text.begin(), text.end(),
          [](char c) { return !(isalnum(c) || (c == '_')); }) == text.end()) {
      absl::StrAppend(&sql, text);
    } else {
      absl::StrAppend(&sql, "(", text, ")");
    }
  }
  return sql;
}
static std::string PreUnaryFunctionSQL(const std::string& display_name,
                                  const std::vector<std::string>& inputs) {
  DCHECK_EQ(inputs.size(), 1);
  return absl::StrCat(display_name, "(", inputs[0], ")");
}
static std::string PostUnaryFunctionSQL(const std::string& display_name,
                                   const std::vector<std::string>& inputs) {
  DCHECK_EQ(inputs.size(), 1);
  return absl::StrCat("(", inputs[0], ")", display_name);
}
static std::string DateAddOrSubFunctionSQL(const std::string& display_name,
                                      const std::vector<std::string>& inputs) {
  DCHECK_EQ(inputs.size(), 3);
  return absl::StrCat(display_name, "(", inputs[0], ", INTERVAL ", inputs[1],
                      " ", inputs[2], ")");
}

static std::string CountStarFunctionSQL(const std::vector<std::string>& inputs) {
  DCHECK_LE(inputs.size(), 1);

  if (inputs.size() == 1) {
    return absl::StrCat("COUNT(* ", inputs[0], ")");
  } else {
    return "COUNT(*)";
  }
}

static std::string BetweenFunctionSQL(const std::vector<std::string>& inputs) {
  DCHECK_EQ(inputs.size(), 3);
  return absl::StrCat("(", inputs[0], ") BETWEEN (", inputs[1], ") AND (",
                      inputs[2], ")");
}
static std::string InListFunctionSQL(const std::vector<std::string>& inputs) {
  DCHECK_GT(inputs.size(), 1);
  std::vector<std::string> in_list(inputs.begin() + 1, inputs.end());
  return absl::StrCat("(", inputs[0], ") IN (", absl::StrJoin(in_list, ", "),
                      ")");
}
static std::string CaseWithValueFunctionSQL(const std::vector<std::string>& inputs) {
  DCHECK_GE(inputs.size(), 2);
  DCHECK_EQ((inputs.size() - 2) % 2, 0);

  std::string case_with_value;
  absl::StrAppend(&case_with_value, "CASE (", inputs[0], ")");
  for (int i = 1; i < inputs.size() - 1; i += 2) {
    absl::StrAppend(&case_with_value, " WHEN (", inputs[i], ") THEN (",
                    inputs[i + 1], ")");
  }
  absl::StrAppend(&case_with_value, " ELSE (", inputs[inputs.size() - 1], ")");
  absl::StrAppend(&case_with_value, " END");

  return case_with_value;
}
static std::string CaseNoValueFunctionSQL(const std::vector<std::string>& inputs) {
  DCHECK_GE(inputs.size(), 1);
  DCHECK_EQ((inputs.size() - 1) % 2, 0);

  std::string case_no_value;
  absl::StrAppend(&case_no_value, "CASE");
  for (int i = 0; i < inputs.size() - 1; i += 2) {
    absl::StrAppend(&case_no_value, " WHEN (", inputs[i], ") THEN (",
                    inputs[i + 1], ")");
  }
  absl::StrAppend(&case_no_value, " ELSE (", inputs[inputs.size() - 1], ")");
  absl::StrAppend(&case_no_value, " END");

  return case_no_value;
}
static std::string InArrayFunctionSQL(const std::vector<std::string>& inputs) {
  DCHECK_EQ(inputs.size(), 2);
  return absl::StrCat("(", inputs[0], ") IN UNNEST(", inputs[1], ")");
}
static std::string ArrayAtOffsetFunctionSQL(const std::vector<std::string>& inputs) {
  DCHECK_EQ(inputs.size(), 2);
  return absl::StrCat(inputs[0], "[OFFSET(", inputs[1], ")]");
}
static std::string ArrayAtOrdinalFunctionSQL(const std::vector<std::string>& inputs) {
  DCHECK_EQ(inputs.size(), 2);
  return absl::StrCat(inputs[0], "[ORDINAL(", inputs[1], ")]");
}
static std::string SafeArrayAtOffsetFunctionSQL(const std::vector<std::string>& inputs) {
  DCHECK_EQ(inputs.size(), 2);
  return absl::StrCat(inputs[0], "[SAFE_OFFSET(", inputs[1], ")]");
}
static std::string SafeArrayAtOrdinalFunctionSQL(const std::vector<std::string>& inputs) {
  DCHECK_EQ(inputs.size(), 2);
  return absl::StrCat(inputs[0], "[SAFE_ORDINAL(", inputs[1], ")]");
}
static std::string GenerateDateTimestampArrayFunctionSQL(
    const std::string& function_name, const std::vector<std::string>& inputs) {
  DCHECK(inputs.size() == 2 || inputs.size() == 4);
  std::string sql = absl::StrCat(function_name, "(", inputs[0], ", ", inputs[1]);
  if (inputs.size() == 4) {
    if (inputs[2][0] == '[') {
      // Fix the function signature text:
      // INTERVAL [INT64] [DATE_TIME_PART] --> [INTERVAL INT64 DATE_TIME_PART]
      DCHECK_EQ(inputs[3][0], '[');
      absl::StrAppend(&sql, ", [INTERVAL ",
                      inputs[2].substr(1, inputs[2].size() - 2), " ",
                      inputs[3].substr(1, inputs[3].size() - 2), "]");
    } else {
      absl::StrAppend(&sql, ", INTERVAL ", inputs[2], " ", inputs[3]);
    }
  }
  absl::StrAppend(&sql, ")");
  return sql;
}

// For MakeArray we explicitly prepend the array type to the function sql, thus
// array-type-name is expected to be passed as the first element of inputs.
static std::string MakeArrayFunctionSQL(const std::vector<std::string>& inputs) {
  DCHECK_GT(inputs.size(), 0);
  const std::string& type_name = inputs[0];
  const std::vector<std::string> args(inputs.begin() + 1, inputs.end());
  return absl::StrCat(type_name, "[", absl::StrJoin(args, ", "), "]");
}

static std::string ExtractFunctionSQL(const std::vector<std::string>& inputs) {
  DCHECK_GT(inputs.size(), 1);
  std::string prefix = absl::StrCat("EXTRACT(", inputs[1], " FROM ", inputs[0]);
  if (inputs.size() > 2) {
    absl::StrAppend(&prefix, " AT TIME ZONE ", inputs[2]);
  }
  return absl::StrCat(prefix, ")");
}

static std::string ExtractDateOrTimeFunctionSQL(const std::string& date_part,
                                           const std::vector<std::string>& inputs) {
  DCHECK_GT(inputs.size(), 0);
  std::string prefix = absl::StrCat("EXTRACT(", date_part, " FROM ", inputs[0]);
  if (inputs.size() > 1) {
    absl::StrAppend(&prefix, " AT TIME ZONE ", inputs[1]);
  }
  return absl::StrCat(prefix, ")");
}

static bool ArgumentIsStringLiteral(const InputArgumentType& argument) {
  if (argument.type()->IsString() && argument.is_literal()) {
    return true;
  }
  return false;
}

static zetasql_base::Status CheckDateDiffArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() != 3) {
    // Let validation happen normally.  It will return an error later.
    return ::zetasql_base::OkStatus();
  }

  // DATE_DIFF for Date only supports date parts DAY, WEEK*, MONTH, QUARTER,
  // YEAR, ISOYEAR, ISOWEEK.  If the third argument is a literal then check the
  // date part.
  // TODO: Add a test where the date part is an enum via a normal
  // function call, but not the expected enum type.
  // The argument should be non-NULL because of
  // Resolver::MakeDatePartEnumResolvedLiteralFromName, but check again for
  // safety so we can't crash.
  if (arguments[2].type()->IsEnum() && arguments[2].is_literal() &&
      !arguments[2].is_literal_null()) {
    switch (arguments[2].literal_value()->enum_value()) {
      case functions::YEAR:
      case functions::ISOYEAR:
      case functions::QUARTER:
      case functions::MONTH:
      case functions::WEEK:
      case functions::WEEK_MONDAY:
      case functions::WEEK_TUESDAY:
      case functions::WEEK_WEDNESDAY:
      case functions::WEEK_THURSDAY:
      case functions::WEEK_FRIDAY:
      case functions::WEEK_SATURDAY:
      case functions::ISOWEEK:
      case functions::DAY:
        return ::zetasql_base::OkStatus();
      default:
        return MakeSqlError() << function_name << " does not support the "
                              << DateTimestampPartToSQL(
                                     arguments[2].literal_value()->enum_value())
                              << " date part";
    }
  }
  // Let validation of other arguments happen normally.
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckBitwiseOperatorArgumentsHaveSameType(
    const std::string& operator_string,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  // We do not want non-literal arguments to implicitly coerce.  We currently
  // have signatures for all the integer/BYTES types, so we only need to check
  // that the arguments are both BYTES, or they are both integers and either
  // they are both the same type or at least one is a literal.
  if (arguments.size() != 2) {
    // Let validation happen normally.  It will return an error later.
    return ::zetasql_base::OkStatus();
  }
  if (arguments[0].type()->IsBytes() && arguments[1].type()->IsBytes()) {
    return ::zetasql_base::OkStatus();
  }
  if (!arguments[0].type()->IsInteger() ||
      !arguments[1].type()->IsInteger() ||
      (!arguments[0].type()->Equals(arguments[1].type()) &&
       !arguments[0].is_literal() &&
       !arguments[1].is_literal())) {
    return MakeSqlError()
           << "Bitwise operator " << operator_string
           << " requires two integer/BYTES arguments of the same type, "
           << "but saw " << arguments[0].type()->DebugString() << " and "
           << arguments[1].type()->DebugString();
  }
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckBitwiseOperatorFirstArgumentIsIntegerOrBytes(
    const std::string& operator_string,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  // We do not want the first argument to implicitly coerce to integer.
  // We currently have signatures for all the integer types and BYTES for the
  // first argument, so we only need to check that it is an integer or BYTES
  // and it is guaranteed to find an exact matching type for the first argument.
  if (arguments.empty()) {
    // Let validation happen normally.  It will return an error later.
    return ::zetasql_base::OkStatus();
  }
  if (!arguments[0].type()->IsInteger() && !arguments[0].type()->IsBytes()) {
    return MakeSqlError() << "The first argument to bitwise operator "
                          << operator_string
                          << " must be an integer or BYTES but saw "
                          << arguments[0].type()->DebugString();
  }
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckDateTruncArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() < 2) {
    // Let validation happen normally.  It will return an error later.
    return ::zetasql_base::OkStatus();
  }

  // DATE_TRUNC for Date only supports date parts DAY, WEEK, MONTH, QUARTER,
  // YEAR.  If the second argument is a literal then check the date part.
  if (arguments[1].type()->IsEnum() && arguments[1].is_literal()) {
    switch (arguments[1].literal_value()->enum_value()) {
      case functions::YEAR:
      case functions::ISOYEAR:
      case functions::QUARTER:
      case functions::MONTH:
      case functions::WEEK:
      case functions::ISOWEEK:
      case functions::WEEK_MONDAY:
      case functions::WEEK_TUESDAY:
      case functions::WEEK_WEDNESDAY:
      case functions::WEEK_THURSDAY:
      case functions::WEEK_FRIDAY:
      case functions::WEEK_SATURDAY:
      case functions::DAY:
        return ::zetasql_base::OkStatus();
      default:
        return MakeSqlError() << function_name << " does not support the "
                              << DateTimestampPartToSQL(
                                     arguments[1].literal_value()->enum_value())
                              << " date part";
    }
  }
  // Let validation of other arguments happen normally.
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckTimeTruncArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() < 2) {
    // Let validation happen normally.  It will return an error later.
    return ::zetasql_base::OkStatus();
  }

  // TIME_TRUNC for Time only supports date parts HOUR, MINUTE, SECOND,
  // MILLISECOND, MICROSECOND and NANOSECOD.
  // If the second argument is a literal then check the date part.
  if (arguments[1].type()->IsEnum() && arguments[1].is_literal()) {
    switch (arguments[1].literal_value()->enum_value()) {
      case functions::HOUR:
      case functions::MINUTE:
      case functions::SECOND:
      case functions::MILLISECOND:
      case functions::MICROSECOND:
        return ::zetasql_base::OkStatus();
      case functions::NANOSECOND:
        if (language_options.LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
          return ::zetasql_base::OkStatus();
        }
        break;
      default:
        break;
    }
    return MakeSqlError() << function_name << " does not support the "
                          << DateTimestampPartToSQL(
                                 arguments[1].literal_value()->enum_value())
                          << " date part";
  }
  // Let validation of other arguments happen normally.
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckExtractPreResolutionArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  // The first argument to the EXTRACT function cannot be a std::string literal
  // since that causes overloading issues.  The argument can be either DATE,
  // TIMESTAMP, or TIMESTAMP_* and a std::string literal coerces to all.
  if (ArgumentIsStringLiteral(arguments[0])) {
    return MakeSqlError()
           << "EXTRACT does not support literal STRING arguments";
  }
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckExtractPostResolutionArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() > 1) {
    ZETASQL_RET_CHECK(functions::DateTimestampPart_IsValid(
        arguments[1].literal_value()->enum_value()));
    functions::DateTimestampPart date_part =
        static_cast<zetasql::functions::DateTimestampPart>(
            arguments[1].literal_value()->enum_value());

    if (arguments[0].type()->IsDate()) {
      static std::set<functions::DateTimestampPart> valid_parts = {
          functions::YEAR,          functions::ISOYEAR,
          functions::QUARTER,       functions::MONTH,
          functions::WEEK,          functions::WEEK_MONDAY,
          functions::WEEK_TUESDAY,  functions::WEEK_WEDNESDAY,
          functions::WEEK_THURSDAY, functions::WEEK_FRIDAY,
          functions::WEEK_SATURDAY, functions::ISOWEEK,
          functions::DAY,           functions::DAYOFWEEK,
          functions::DAYOFYEAR};
      if (!zetasql_base::ContainsKey(valid_parts, date_part)) {
        return MakeSqlError() << ExtractingNotSupportedDatePart(
                   "DATE", DateTimestampPartToSQL(
                               arguments[1].literal_value()->enum_value()));
      }
    }
    if (arguments[0].type()->IsTime()) {
      static std::set<functions::DateTimestampPart> valid_parts = {
          functions::NANOSECOND,
          functions::MICROSECOND,
          functions::MILLISECOND,
          functions::SECOND,
          functions::MINUTE,
          functions::HOUR};
      if (!zetasql_base::ContainsKey(valid_parts, date_part)) {
        return MakeSqlError() << ExtractingNotSupportedDatePart(
                   "TIME", DateTimestampPartToSQL(
                               arguments[1].literal_value()->enum_value()));
      }
    }
  }
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckDateAddDateSubArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() != 3) {
    // Let validation happen normally.  It will return an error later.
    return ::zetasql_base::OkStatus();
  }

  // For Date, the only supported date parts are DAY, WEEK, MONTH, QUARTER,
  // YEAR.  For Timestamp, additional parts are supported.  If the third
  // parameter is a literal std::string then check the date part.
  if (arguments[2].type()->IsEnum() && arguments[2].is_literal()) {
    switch (arguments[2].literal_value()->enum_value()) {
      case functions::YEAR:
      case functions::QUARTER:
      case functions::MONTH:
      case functions::WEEK:
      case functions::DAY:
        return ::zetasql_base::OkStatus();
      default:
        return MakeSqlError() << function_name << " does not support the "
                              << DateTimestampPartToSQL(
                                     arguments[2].literal_value()->enum_value())
                              << " date part";
    }
  }
  // Let validation of other arguments happen normally.
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckDatetimeAddSubDiffArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() != 3) {
    // Let validation happen normally.  It will return an error later.
    return ::zetasql_base::OkStatus();
  }

  if (arguments[2].type()->IsEnum() && arguments[2].is_literal()) {
    switch (arguments[2].literal_value()->enum_value()) {
      case functions::YEAR:
      case functions::QUARTER:
      case functions::MONTH:
      case functions::WEEK:
      case functions::DAY:
      case functions::HOUR:
      case functions::MINUTE:
      case functions::SECOND:
      case functions::MILLISECOND:
      case functions::MICROSECOND:
        return ::zetasql_base::OkStatus();
      case functions::ISOYEAR:
      case functions::ISOWEEK:
      case functions::WEEK_MONDAY:
      case functions::WEEK_TUESDAY:
      case functions::WEEK_WEDNESDAY:
      case functions::WEEK_THURSDAY:
      case functions::WEEK_FRIDAY:
      case functions::WEEK_SATURDAY:
        if (function_name == "DATETIME_DIFF") {
          // Only DATETIME_DIFF supports ISOYEAR, ISOWEEK, and WEEK_*, not
          // DATETIME_ADD or DATETIME_SUB.
          return ::zetasql_base::OkStatus();
        }
        break;
      case functions::NANOSECOND:
        if (language_options.LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
          return ::zetasql_base::OkStatus();
        }
        break;
      default:
        break;
    }
    return MakeSqlError() << function_name << " does not support the "
                          << DateTimestampPartToSQL(
                                 arguments[2].literal_value()->enum_value())
                          << " date part";
  }
  // Let validation of other arguments happen normally.
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckTimestampAddTimestampSubArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() != 3) {
    // Let validation happen normally.  It will return an error later.
    return ::zetasql_base::OkStatus();
  }

  if (arguments[2].type()->IsEnum() && arguments[2].is_literal()) {
    switch (arguments[2].literal_value()->enum_value()) {
      case functions::DAY:
        if (zetasql_base::StringCaseEqual(function_name, "timestamp_add") ||
            zetasql_base::StringCaseEqual(function_name, "timestamp_sub")) {
          return ::zetasql_base::OkStatus();
        }
        break;
      case functions::HOUR:
      case functions::MINUTE:
      case functions::SECOND:
      case functions::MILLISECOND:
      case functions::MICROSECOND:
        return ::zetasql_base::OkStatus();
      case functions::NANOSECOND: {
        if (language_options.LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
          return ::zetasql_base::OkStatus();
        }
        break;
      }
      default:
        break;
    }
    return MakeSqlError() << function_name << " does not support the "
                          << DateTimestampPartToSQL(
                                 arguments[2].literal_value()->enum_value())
                          << " date part";
  }
  // Let validation of other arguments happen normally.
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckTimestampDiffArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() != 3) {
    // Let validation happen normally.  It will return an error later.
    return ::zetasql_base::OkStatus();
  }

  // TIMESTAMP_DIFF for TIMESTAMP only supports date parts at DAY or
  // finer granularity.
  if (arguments[2].type()->IsEnum() && arguments[2].is_literal()) {
    switch (arguments[2].literal_value()->enum_value()) {
      case functions::DAY:
        if (zetasql_base::StringCaseEqual(function_name, "timestamp_diff")) {
          return ::zetasql_base::OkStatus();
        }
        break;
      case functions::HOUR:
      case functions::MINUTE:
      case functions::SECOND:
      case functions::MILLISECOND:
      case functions::MICROSECOND:
        return ::zetasql_base::OkStatus();
      case functions::NANOSECOND: {
        if (language_options.LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
          return ::zetasql_base::OkStatus();
        }
        break;
      }
      default:
        // Other parts are unsupported.
        break;
    }
    return MakeSqlError() << function_name << " does not support the "
                          << DateTimestampPartToSQL(
                                 arguments[2].literal_value()->enum_value())
                          << " date part";
  }
  // Let validation of other arguments happen normally.
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckTimestampTruncArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() != 2) {
    // Let validation happen normally.  It will return an error later.
    return ::zetasql_base::OkStatus();
  }
  if (arguments[1].type()->IsEnum() && arguments[1].is_literal()) {
    switch (arguments[1].literal_value()->enum_value()) {
      case functions::YEAR:
      case functions::ISOYEAR:
      case functions::QUARTER:
      case functions::MONTH:
      case functions::WEEK:
      case functions::ISOWEEK:
      case functions::WEEK_MONDAY:
      case functions::WEEK_TUESDAY:
      case functions::WEEK_WEDNESDAY:
      case functions::WEEK_THURSDAY:
      case functions::WEEK_FRIDAY:
      case functions::WEEK_SATURDAY:
      case functions::DAY:
      case functions::HOUR:
      case functions::MINUTE:
      case functions::SECOND:
      case functions::MILLISECOND:
      case functions::MICROSECOND:
        return ::zetasql_base::OkStatus();
      case functions::NANOSECOND: {
        if (language_options.LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
          return ::zetasql_base::OkStatus();
        }
        break;
      }
      default:
        break;
    }
    return MakeSqlError() << function_name << " does not support the "
                          << DateTimestampPartToSQL(
                                 arguments[1].literal_value()->enum_value())
                          << " date part";
  }
  // Let validation of other arguments happen normally.
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckGenerateDateArrayArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() != 4) {
    // GENERATE_DATE_ARRAY(start, end, INTERVAL int) is not valid; if the
    // interval is present, the date part must be as well.
    if (arguments.size() != 2) {
      return MakeSqlError()
             << "Expected either GENERATE_DATE_ARRAY(start, end) or "
                "GENERATE_DATE_ARRAY(start, end, INTERVAL int [date_part])";
    }
    return ::zetasql_base::OkStatus();
  }

  // For Date, the only supported date parts are DAY, WEEK, MONTH, QUARTER,
  // YEAR.
  if (arguments[3].is_literal()) {
    switch (arguments[3].literal_value()->enum_value()) {
      case functions::YEAR:
      case functions::QUARTER:
      case functions::MONTH:
      case functions::WEEK:
      case functions::DAY:
        return ::zetasql_base::OkStatus();
      default:
        break;
    }
    return MakeSqlError() << "GENERATE_DATE_ARRAY does not support the "
                          << DateTimestampPartToSQL(
                                 arguments[3].literal_value()->enum_value())
                          << " date part";
  }
  // Let validation of other arguments happen normally.
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckGenerateTimestampArrayArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() != 4) {
    // Wrong number of arguments; let validation continue normally.
    return ::zetasql_base::OkStatus();
  }

  if (arguments[3].is_literal()) {
    switch (arguments[3].literal_value()->enum_value()) {
      case functions::MICROSECOND:
      case functions::MILLISECOND:
      case functions::SECOND:
      case functions::MINUTE:
      case functions::HOUR:
      case functions::DAY:
        return ::zetasql_base::OkStatus();
      case functions::NANOSECOND:
        if (language_options.LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
          return ::zetasql_base::OkStatus();
        }
        break;
      default:
        break;
    }
    return MakeSqlError() << "GENERATE_TIMESTAMP_ARRAY does not support the "
                          << DateTimestampPartToSQL(
                                 arguments[3].literal_value()->enum_value())
                          << " date part";
  }
  // Let validation of other arguments happen normally.
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckJsonArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& options) {
  if (arguments.size() != 2) {
    // Let validation happen normally. The resolver will return an error
    // later.
    return ::zetasql_base::OkStatus();
  }
  if (!arguments[1].is_literal() && !arguments[1].is_untyped() &&
      !arguments[1].is_query_parameter()) {
    return MakeSqlError()
           << "JSONPath must be a std::string literal or query parameter";
  }

  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckIsSupportedKeyType(
    absl::string_view function_name,
    const std::set<std::string>& supported_key_types,
    int key_type_argument_index,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() <= key_type_argument_index) {
    // Wrong number of arguments; let validation happen normally.
    return ::zetasql_base::OkStatus();
  }

  ZETASQL_RET_CHECK_LE(key_type_argument_index, 1);
  const absl::string_view argument_index_name =
      key_type_argument_index == 0 ? "First" : "Second";

  const InputArgumentType& argument = arguments[key_type_argument_index];
  if (!argument.is_literal() && !argument.is_query_parameter()) {
    return MakeSqlError() << argument_index_name
                          << " argument (key type) in function "
                          << function_name
                          << " must be a std::string literal or query parameter";
  }

  // If the type is wrong (or no type is assigned), or the argument is a query
  // parameter, let validation continue.
  if (!argument.type()->IsString() || !argument.is_literal()) {
    return ::zetasql_base::OkStatus();
  }

  // Check the key type for std::string literals.
  const std::string& key_type = argument.literal_value()->string_value();
  if (!zetasql_base::ContainsKey(supported_key_types, key_type)) {
    const std::string key_type_list =
        supported_key_types.size() == 1
            ? absl::StrCat("'", *supported_key_types.begin(), "'")
            : absl::StrCat(
                  "one of ",
                  absl::StrJoin(
                      supported_key_types, ", ",
                      [](std::string* out, const absl::string_view key_type) {
                        absl::StrAppend(out, "'", key_type, "'");
                      }));
    return MakeSqlError() << argument_index_name
                          << " argument (key type) in function "
                          << function_name << " must be " << key_type_list
                          << ", but is '" << key_type << "'";
  }
  return ::zetasql_base::OkStatus();
}

static const std::set<std::string>& GetSupportedKeyTypes() {
  static const auto* kSupportedKeyTypes =
      new std::set<std::string>{"AEAD_AES_GCM_256"};
  return *kSupportedKeyTypes;
}

static const std::set<std::string>& GetSupportedRawKeyTypes() {
  static const auto* kSupportedKeyTypes =
      new std::set<std::string>{"AES_GCM", "AES_CBC_PKCS"};
  return *kSupportedKeyTypes;
}

static bool IsStringLiteralComparedToBytes(const InputArgumentType& lhs_arg,
                                           const InputArgumentType& rhs_arg) {
  return (lhs_arg.type()->IsString() && lhs_arg.is_literal() &&
          rhs_arg.type()->IsBytes()) ||
         (lhs_arg.type()->IsBytes() && rhs_arg.type()->IsString() &&
          rhs_arg.is_literal());
}

static std::string NoMatchingSignatureForCaseNoValueFunction(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode) {
  std::string error_message = absl::StrCat("No matching signature for ",
                                      qualified_function_name);
  // Even number arguments represent WHEN expresssions and must be BOOL, except
  // for the last argument (which is an ELSE argument).  Other arguments must
  // all be coercible to a common supertype.
  //
  // We use ordered sets so that the order of types in error messages is
  // deterministic.
  std::set<std::string, zetasql_base::StringCaseLess> non_bool_when_types;
  std::set<std::string, zetasql_base::StringCaseLess> then_else_types;
  for (int i = 0; i < arguments.size(); ++i) {
    if (i % 2 == 0 && i < arguments.size() - 1) {
      // This must be a BOOL expression.  If not, remember it.
      if (!arguments[i].type()->IsBool()) {
        zetasql_base::InsertIfNotPresent(
            &non_bool_when_types,
            arguments[i].type()->ShortTypeName(product_mode));
      }
      continue;
    }
    // Otherwise, all THEN/ELSE types must be commonly coercible.  Ignore
    // untyped arguments since they can (mostly) coerce to any type.
    if (!arguments[i].is_untyped()) {
      zetasql_base::InsertIfNotPresent(&then_else_types,
                              arguments[i].type()->ShortTypeName(product_mode));
    }
  }
  if (!non_bool_when_types.empty()) {
    absl::StrAppend(&error_message,
                    "; all WHEN argument types must be BOOL but found: ",
                    absl::StrJoin(non_bool_when_types, ", "));
  }
  // Note that we can get a false positive in cases where the argument types
  // are different but coercible (i.e., INT32 and INT64).  If this becomes
  // a confusing issue in practice then we can tweak this logic further.
  if (then_else_types.size() > 1) {
    absl::StrAppend(&error_message,
                    "; all THEN/ELSE arguments must be coercible to a common "
                    "type but found: ", absl::StrJoin(then_else_types, ", "));
  }
  std::string argument_types;
  for (int idx = 0; idx < arguments.size(); ++idx) {
    if (idx % 2 == 0) {
      if (idx < arguments.size() - 1) {
        // WHEN, so add leading '('
        absl::StrAppend(&argument_types, " (");
      } else {
        absl::StrAppend(&argument_types, " ");
      }
      absl::StrAppend(&argument_types,
                      arguments[idx].UserFacingName(product_mode));
    } else {
      // THEN, so add trailing ')'
      absl::StrAppend(&argument_types, " ",
                      arguments[idx].UserFacingName(product_mode), ")");
    }
  }
  absl::StrAppend(&error_message, "; actual argument types (WHEN THEN) ELSE:",
                  argument_types);
  return error_message;
}

static std::string NoMatchingSignatureForInFunction(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode) {
  bool is_string_literal_compared_to_bytes = false;
  const InputArgumentType lhs_argument = arguments[0];
  InputArgumentTypeSet rhs_argument_set;
  for (int idx = 1; idx < arguments.size(); ++idx) {
    rhs_argument_set.Insert(arguments[idx]);
    is_string_literal_compared_to_bytes |=
        IsStringLiteralComparedToBytes(lhs_argument, arguments[idx]);
  }
  std::string error_message = absl::StrCat(
      "No matching signature for ", qualified_function_name,
      " for argument types ", lhs_argument.DebugString(), " and ",
      rhs_argument_set.ToString());
  if (is_string_literal_compared_to_bytes) {
    absl::StrAppend(&error_message, kErrorMessageCompareStringLiteralToBytes);
  }
  return error_message;
}

static std::string NoMatchingSignatureForInArrayFunction(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode) {
  std::string error_message =
      Function::GetGenericNoMatchingFunctionSignatureErrorMessage(
          qualified_function_name, arguments, product_mode);
  const InputArgumentType lhs_arg = arguments[0];
  const InputArgumentType rhs_arg = arguments[1];
  bool is_string_literal_compared_to_bytes = false;
  // The rhs can be an untyped or an array type. This is enforced in
  // CheckInArrayArguments.
  if (rhs_arg.type()->IsArray()) {
    const Type* element_type = rhs_arg.type()->AsArray()->element_type();
    if ((lhs_arg.type()->IsString() && lhs_arg.is_literal() &&
         element_type->IsBytes()) ||
        (lhs_arg.type()->IsBytes() && rhs_arg.is_literal() &&
         element_type->IsString())) {
      is_string_literal_compared_to_bytes = true;
    }
  }
  if (is_string_literal_compared_to_bytes) {
    absl::StrAppend(&error_message, kErrorMessageCompareStringLiteralToBytes);
  }
  return error_message;
}

static std::string NoMatchingSignatureForComparisonOperator(
    const std::string& operator_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode) {
  std::string error_message =
      Function::GetGenericNoMatchingFunctionSignatureErrorMessage(
          operator_name, arguments, product_mode);
  if (IsStringLiteralComparedToBytes(arguments[0], arguments[1])) {
    absl::StrAppend(&error_message, kErrorMessageCompareStringLiteralToBytes);
  }
  return error_message;
}

static std::string NoMatchingSignatureForFunctionUsingInterval(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode,
    int index_of_interval_argument) {
  // index_of_interval_argument is the index of the INTERVAL expression in the
  // function's argument list. For example, it is 1 for DATE_SUB(DATE, INTERVAL
  // INT64 DATE_TIME_PART), and it is 2 for GENERATE_DATE_ARRAY(DATE, DATE,
  // INTERVAL INT64 DATE_TIME_PART).
  if (arguments.size() < index_of_interval_argument + 2) {
    // No INTERVAL expression was resolved. This can happen if the INTERVAL
    // expression is optional and unspecified in the function call.
    return Function::GetGenericNoMatchingFunctionSignatureErrorMessage(
        qualified_function_name, arguments, product_mode);
  }
  // The INTERVAL expression was resolved and flattened as 2 arguments in the
  // argument list.
  std::vector<std::string> argument_texts;
  argument_texts.reserve(arguments.size() - 1);
  for (int i = 0; i < index_of_interval_argument; ++i) {
    argument_texts.push_back(arguments[i].UserFacingName(product_mode));
  }
  argument_texts.push_back(absl::StrCat(
      "INTERVAL ",
      arguments[index_of_interval_argument].UserFacingName(product_mode), " ",
      arguments[index_of_interval_argument + 1].UserFacingName(product_mode)));
  for (int i = index_of_interval_argument + 2; i < arguments.size(); ++i) {
    argument_texts.push_back(arguments[i].UserFacingName(product_mode));
  }
  return absl::StrCat(
      "No matching signature for ", qualified_function_name,
      " for argument types: ", absl::StrJoin(argument_texts, ", "));
}

static std::string NoMatchingSignatureForDateOrTimeAddOrSubFunction(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode) {
  return NoMatchingSignatureForFunctionUsingInterval(
      qualified_function_name, arguments, product_mode,
      /*index_of_interval_argument=*/1);
}

static std::string NoMatchingSignatureForGenerateDateOrTimestampArrayFunction(
    const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode) {
  return NoMatchingSignatureForFunctionUsingInterval(
      qualified_function_name, arguments, product_mode,
      /*index_of_interval_argument=*/2);
}

// Supports 'ArgumentType' of either InputArgumentType or FunctionArgumentType.
//
// Example return values:
//   DATE_TIME_PART FROM TIMESTAMP
//   DATE FROM TIMESTAMP
//   TIME FROM TIMESTAMP
//   DATETIME FROM TIMESTAMP
//   DATE_TIME_PART FROM TIMESTAMP AT TIME ZONE STRING
//   DATETIME FROM TIMESTAMP [AT TIME ZONE STRING]
//
// 'include_bracket' indicates whether or not the 'AT TIME ZONE' argument
// is enclosed in brackets to indicate that the clause is optional.
// The input 'arguments' must be a valid signature for EXTRACT.
//
// If 'explicit_datepart_name' is non-empty, then the signature must not
// have a date part argument.  Otherwise, the signature must have a date
// part argument.
//
// For $extract, the date part argument is present in 'arguments', and
// 'explicit_datepart_name' is empty.
//
// For $extract_date, $extract_time, and $extract_datetime, the date part
// argument is *not* present in 'arguments', and 'explicit_datepart_name'
// is non-empty.
template<class ArgumentType>
static std::string GetExtractFunctionSignatureString(
    const std::string& explicit_datepart_name,
    const std::vector<ArgumentType>& arguments, ProductMode product_mode,
    bool include_bracket) {
  // The 0th argument is the one we are extracting the date part from.
  const std::string source_type_string(arguments[0].UserFacingName(product_mode));
  std::string datepart_string;
  std::string timezone_string;
  if (explicit_datepart_name.empty()) {
    // The date part argument is present in 'arguments', so arguments[1]
    // is the date part and arguments[2] (if present) is the time zone.
    //
    // DCHECK validated - given the non-standard function call syntax for
    // EXTRACT, the parser enforces 2 or 3 arguments in the language.
    DCHECK(arguments.size() == 2 || arguments.size() == 3);
    // Expected invariant - the 1th argument is the date part argument.
    DCHECK(arguments[1].type()->Equivalent(types::DatePartEnumType()));
    datepart_string = arguments[1].UserFacingName(product_mode);
    if (arguments.size() == 3) {
      timezone_string = arguments[2].UserFacingName(product_mode);
    }
  } else {
    // The date part is populated from 'explicit_datepart_name' and the
    // date part argument is not present in 'arguments', so arguments[1]
    // (if present) is the time zone.
    //
    // DCHECK validated - given the non-standard function call syntax for
    // EXTRACT, the parser enforces 2 or 3 arguments in the language and
    // the date part argument has been omitted from this signature (i.e.,
    // $extract_date, etc.).
    DCHECK(arguments.size() == 1 || arguments.size() == 2);
    datepart_string = explicit_datepart_name;
    // If present, the 1th argument is the optional timezone argument.
    if (arguments.size() == 2) {
      timezone_string = arguments[1].UserFacingName(product_mode);
    }
  }

  std::string out;
  absl::StrAppend(
      &out, datepart_string, " FROM ", source_type_string,
      (timezone_string.empty()
           ? ""
           : absl::StrCat(" ", (include_bracket ? "[" : ""), "AT TIME ZONE ",
                          timezone_string, (include_bracket ? "]" : ""))));
  return out;
}

static std::string NoMatchingSignatureForExtractFunction(
    const std::string& explicit_datepart_name, const std::string& qualified_function_name,
    const std::vector<InputArgumentType>& arguments, ProductMode product_mode) {
  std::string msg("No matching signature for function EXTRACT for argument types: ");
  absl::StrAppend(&msg, GetExtractFunctionSignatureString(
                            explicit_datepart_name, arguments, product_mode,
                            /*include_bracket=*/false));
  return msg;
}

static std::string ExtractSupportedSignatures(
    const std::string& explicit_datepart_name,
    const LanguageOptions& language_options, const Function& function) {
  std::string supported_signatures;
  for (const FunctionSignature& signature : function.signatures()) {
    // Ignore deprecated signatures, and signatures that include
    // unsupported data types.
    if (signature.HasUnsupportedType(language_options)) {
      // We must check for unsupported types since some engines do not
      // support the DATETIME/TIME types yet.
      continue;
    }
    if (!supported_signatures.empty()) {
      absl::StrAppend(&supported_signatures, "; ");
    }
    absl::StrAppend(
        &supported_signatures, "EXTRACT(",
        GetExtractFunctionSignatureString(
            explicit_datepart_name, signature.arguments(),
            language_options.product_mode(), true /* include_bracket */),
        ")");
  }
  return supported_signatures;
}

static std::string EmptySupportedSignatures(
    const LanguageOptions& language_options, const Function& function) {
  return std::string();
}

static zetasql_base::Status CheckArgumentsSupportEquality(
    const std::string& comparison_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  for (int idx = 0; idx < arguments.size(); ++idx) {
    if (!arguments[idx].type()->SupportsEquality(language_options)) {
      return MakeSqlError() << comparison_name
                            << " is not defined for arguments of type "
                            << arguments[idx].DebugString();
    }
  }
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckArgumentsSupportComparison(
    const std::string& comparison_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  int bad_argument_idx;
  if (ArgumentsAreComparable(arguments, language_options, &bad_argument_idx)) {
    return ::zetasql_base::OkStatus();
  }
  return MakeSqlError() << comparison_name
                        << " is not defined for arguments of type "
                        << arguments[bad_argument_idx].DebugString();
}

static zetasql_base::Status CheckMinMaxGreatestLeastArguments(
    const std::string& function_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  ZETASQL_RETURN_IF_ERROR(CheckArgumentsSupportComparison(
      function_name, arguments, language_options));
  // If we got here then the arguments are (pairwise) comparable.  But the
  // MIN/MAX/GREATEST/LEAST functions do not support ARRAYs because they have
  // weird behavior if the arrays contain elements that are NULLs or NaNs.
  // This is because these functions are defined in terms of >/< operators,
  // and >/< operators do not provide a total order when arrays contain
  // elements that are NULLs/NaNs.
  //
  // For example, the following pair of arrays are clearly not equal, but both
  // the > and < comparisons return NULL for this pair:
  //
  // [1, NULL, 2]
  // [1, NULL, 3]
  //
  // >/< comparisons also return NULL for:
  //
  // [1, NULL, 3]
  // [1, 2, 3]
  //
  // Similarly, both > and < comparisons return FALSE for this pair:
  //
  // [1, NaN, 2]
  // [1, NaN, 3]
  //
  // As well as for this pair:
  //
  // [1, NaN, 3]
  // [1, 2, 3]
  //
  for (int idx = 0; idx < arguments.size(); ++idx) {
    if (arguments[idx].type()->IsArray()) {
      return MakeSqlError() << function_name
                            << " is not defined for arguments of type "
                            << arguments[idx].DebugString();
    }
  }
  return zetasql_base::OkStatus();
}

static zetasql_base::Status CheckFirstArgumentSupportsEquality(
    const std::string& comparison_name,
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.empty() ||
      arguments[0].type()->SupportsEquality(language_options)) {
    return ::zetasql_base::OkStatus();
  }
  return MakeSqlError() << comparison_name
                        << " is not defined for arguments of type "
                        << arguments[0].DebugString();
}

static zetasql_base::Status CheckArrayAggArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  int bad_argument_idx;
  if (!ArgumentsArrayType(arguments, false /* is_array */, &bad_argument_idx)) {
    return MakeSqlError() << "The argument to ARRAY_AGG must not be an array "
                          << "type but was "
                          << arguments[bad_argument_idx].DebugString();
  }
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckArrayConcatArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  int bad_argument_idx;
  if (!ArgumentsArrayType(arguments, true /* is_array */, &bad_argument_idx)) {
    return MakeSqlError()
           << "The argument to ARRAY_CONCAT (or ARRAY_CONCAT_AGG) "
           << "must be an array type but was "
           << arguments[bad_argument_idx].DebugString();
  }
  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckInArrayArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  ZETASQL_RET_CHECK_EQ(arguments.size(), 2);

  // Untyped parameter can be coerced to any type.
  bool array_is_untyped_parameter = arguments[1].is_untyped_query_parameter();

  if (!array_is_untyped_parameter && !arguments[1].type()->IsArray()) {
    return MakeSqlError()
           << "Second argument of IN UNNEST must be an array but was "
           << arguments[1].DebugString();
  }

  if (!arguments[0].type()->SupportsEquality(language_options)) {
    return MakeSqlError() << "First argument to IN UNNEST of type "
                          << arguments[0].DebugString()
                          << " does not support equality comparison";
  }

  if (!array_is_untyped_parameter &&
      !arguments[1].type()->AsArray()->element_type()->SupportsEquality(
          language_options)) {
    return MakeSqlError()
           << "Second argument to IN UNNEST of type "
           << arguments[1].DebugString()
           << " is not supported because array element type is not "
              "equality comparable";
  }

  return ::zetasql_base::OkStatus();
}

static zetasql_base::Status CheckRangeBucketArguments(
    const std::vector<InputArgumentType>& arguments,
    const LanguageOptions& language_options) {
  if (arguments.size() != 2) {
    // Let validation happen normally. It will return an error later.
    return ::zetasql_base::OkStatus();
  }

  if (!arguments[0].type()->SupportsOrdering(language_options,
                                             /*type_description=*/nullptr)) {
    return MakeSqlError() << "First argument to RANGE_BUCKET of type "
                          << arguments[0].type()->ShortTypeName(
                                 language_options.product_mode())
                          << " does not support ordering";
  }

  if (arguments[1].type()->IsArray()) {
    if (!arguments[1].type()->AsArray()->element_type()->SupportsOrdering(
            language_options, /*type_description=*/nullptr)) {
      return MakeSqlError()
             << "Second argument to RANGE_BUCKET of type "
             << arguments[1].type()->ShortTypeName(
                    language_options.product_mode())
             << " is not supported because array element type does not support "
             << "ordering";
    }
  } else {
    // Untyped parameter can be coerced to any type.
    if (!arguments[1].is_untyped_query_parameter()) {
      return MakeSqlError()
             << "Second argument of RANGE_BUCKET must be an array but was "
             << arguments[1].type()->ShortTypeName(
                    language_options.product_mode());
    }
  }

  return zetasql_base::OkStatus();
}

// Returns true if an arithmetic operation has a floating point type as its
// input.
static bool HasFloatingPointArgument(
    const std::vector<InputArgumentType>& arguments) {
  for (const InputArgumentType& argument : arguments) {
    if (argument.type()->IsFloatingPoint()) {
      return true;
    }
  }
  return false;
}

// Returns true if an arithmetic operation has a numeric point type as its
// input.
static bool HasNumericTypeArgument(
    const std::vector<InputArgumentType>& arguments) {
  for (const InputArgumentType& argument : arguments) {
    if (argument.type()->kind() == TYPE_NUMERIC) {
      return true;
    }
  }
  return false;
}

// Compute the result type for TOP_COUNT and TOP_SUM.
// The output type is
//   ARRAY<
//     STRUCT<`value` <arguments[0].type()>,
//            `<field2_name>` <arguments[1].type()> > >
static zetasql_base::StatusOr<const Type*> ComputeResultTypeForTopStruct(
    const std::string& field2_name, Catalog* catalog, TypeFactory* type_factory,
    CycleDetector* cycle_detector,
    const std::vector<InputArgumentType>& arguments,
    const AnalyzerOptions& analyzer_options) {
  ZETASQL_RET_CHECK_GE(arguments.size(), 2);
  const Type* element_type;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeStructType(
      {{"value", arguments[0].type()}, {field2_name, arguments[1].type()}},
      &element_type));
  const Type* result_type = nullptr;
  ZETASQL_RETURN_IF_ERROR(type_factory->MakeArrayType(element_type, &result_type));
  return result_type;
}

static void InsertCreatedFunction(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options, Function* function_in) {
  std::unique_ptr<Function> function(function_in);
  const LanguageOptions& language_options = options.language_options;

  if (language_options.product_mode() == PRODUCT_EXTERNAL &&
      !function->function_options().allow_external_usage) {
    return;
  }

  if (!function->function_options().check_all_required_features_are_enabled(
          language_options.GetEnabledLanguageFeatures())) {
    return;
  }

  // Identify each signature that is unsupported via options checks.
  absl::flat_hash_set<int> signatures_to_remove;
  for (int idx = 0; idx < function->signatures().size(); ++idx) {
    const FunctionSignature& signature = function->signatures()[idx];
    if ((!options.include_function_ids.empty() &&
         !zetasql_base::ContainsKey(
             options.include_function_ids,
             static_cast<FunctionSignatureId>(signature.context_id()))) ||
        zetasql_base::ContainsKey(
            options.exclude_function_ids,
            static_cast<FunctionSignatureId>(signature.context_id())) ||
        signature.HasUnsupportedType(language_options)) {
      signatures_to_remove.insert(idx);
    }
  }

  // Remove unsupported signatures.
  if (!signatures_to_remove.empty()) {
    if (signatures_to_remove.size() == function->signatures().size()) {
      // We are removing all signatures, so we do not need to insert
      // this function.
      VLOG(4) << "Function excluded: " << function->Name();
      return;
    }

    std::vector<FunctionSignature> new_signatures;
    for (int idx = 0; idx < function->signatures().size(); ++idx) {
      const FunctionSignature& signature = function->signatures()[idx];
      if (!zetasql_base::ContainsKey(signatures_to_remove, idx)) {
        new_signatures.push_back(signature);
      } else {
        VLOG(4) << "FunctionSignature excluded: "
                << signature.DebugString(function->Name());
      }
    }
    function->ResetSignatures(new_signatures);
  }

  // Not using IdentifierPathToString to avoid escaping things like '$add' and
  // 'if'.
  const std::string& name = absl::StrJoin(function->FunctionNamePath(), ".");
  CHECK(functions->emplace(name, std::move(function)).second)
      << name << "already exists";
}

// Wrapper around FunctionSignature that allocates on the heap to reduce the
// impact on stack size while still using the same initializer list.
class FunctionSignatureOnHeap {
 public:
  FunctionSignatureOnHeap(const FunctionArgumentType& result_type,
                          const FunctionArgumentTypeList& arguments,
                          int64_t context_id)
      : signature_(new FunctionSignature(result_type, arguments, context_id)) {}

  FunctionSignatureOnHeap(const FunctionArgumentType& result_type,
                          const FunctionArgumentTypeList& arguments,
                          int64_t context_id,
                          const FunctionSignatureOptions& options)
      : signature_(new FunctionSignature(result_type, arguments, context_id,
                                         options)) {}

  const FunctionSignature& Get() const { return *signature_; }

 private:
  std::shared_ptr<FunctionSignature> signature_;
};

static void InsertFunctionImpl(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options,
    const std::vector<std::string>& name, Function::Mode mode,
    const std::vector<FunctionSignatureOnHeap>& signatures_on_heap,
    const FunctionOptions& function_options) {
  std::vector<FunctionSignature> signatures;
  signatures.reserve(signatures_on_heap.size());
  for (const FunctionSignatureOnHeap& signature_on_heap : signatures_on_heap) {
    signatures.emplace_back(signature_on_heap.Get());
  }

  FunctionOptions local_options = function_options;
  if (mode == Function::AGGREGATE) {
    // By default, all built-in aggregate functions can be used as analytic
    // functions.
    local_options.supports_over_clause = true;
    local_options.window_ordering_support = FunctionOptions::ORDER_OPTIONAL;
    local_options.supports_window_framing = true;
  }

  InsertCreatedFunction(
      functions, options,
      new Function(name, Function::kZetaSQLFunctionGroupName, mode,
                   signatures, local_options));
}

static void InsertFunction(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options, const std::string& name,
    Function::Mode mode, const std::vector<FunctionSignatureOnHeap>& signatures,
    const FunctionOptions& function_options) {
  InsertFunctionImpl(functions, options, {name}, mode, signatures,
                     function_options);
}

// Note: This function is intentionally overloaded to prevent a default
// FunctionOptions object to be allocated on the callers stack.
static void InsertFunction(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options, const std::string& name,
    Function::Mode mode,
    const std::vector<FunctionSignatureOnHeap>& signatures) {
  InsertFunctionImpl(functions, options, {name}, mode, signatures,
                     FunctionOptions());
}

static void InsertNamespaceFunction(
    NameToFunctionMap* functions,
    const ZetaSQLBuiltinFunctionOptions& options, const std::string& space,
    const std::string& name, Function::Mode mode,
    const std::vector<FunctionSignatureOnHeap>& signatures,
    const FunctionOptions& function_options = FunctionOptions()) {
  InsertFunctionImpl(functions, options, {space, name}, mode, signatures,
                     function_options);
}

static void GetDatetimeExtractFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
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

static void GetDatetimeConversionFunctions(
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

  // Conversion functions from integer/std::string/date/timestamp to
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

  // Conversion functions from date/timestamp to integer and std::string.
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

static void GetTimeAndDatetimeConstructionAndConversionFunctions(
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

static void GetDatetimeCurrentFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
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

static void GetDatetimeAddSubFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
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

static void GetDatetimeDiffTruncFunctions(
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

  InsertFunction(functions, options, "timestamp_diff", SCALAR,
                 {{int64_type,
                   {timestamp_type, timestamp_type, datepart_type},
                   FN_TIMESTAMP_DIFF}},
                 FunctionOptions().set_pre_resolution_argument_constraint(
                     bind_front(&CheckTimestampDiffArguments,
                               "TIMESTAMP_DIFF")));

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

static void GetDatetimeFormatFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
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

static void GetDatetimeFunctions(TypeFactory* type_factory,
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

static void GetArithmeticFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions) {
  const Type* int32_type = type_factory->get_int32();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* float_type = type_factory->get_float();
  const Type* double_type = type_factory->get_double();
  const Type* numeric_type = type_factory->get_numeric();

  const Function::Mode SCALAR = Function::SCALAR;

  FunctionSignatureOptions has_floating_point_argument;
  has_floating_point_argument.set_constraints(&HasFloatingPointArgument);
  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&HasNumericTypeArgument);

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
        has_numeric_type_argument}},
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
        has_numeric_type_argument}},
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
        has_numeric_type_argument}},
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
                   has_numeric_type_argument}},
                 FunctionOptions()
                     .set_supports_safe_error_mode(false)
                     .set_sql_name("-")
                     .set_arguments_are_coercible(false)
                     .set_get_sql_callback(
                         bind_front(&PreUnaryFunctionSQL, "-")));
}

static void GetBitwiseFunctions(TypeFactory* type_factory,
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

static void GetAggregateFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
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

  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&HasNumericTypeArgument);

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
                   has_numeric_type_argument}});

  InsertFunction(functions, options, "avg", AGGREGATE,
                 {{double_type, {int64_type}, FN_AVG_INT64},
                  {double_type, {uint64_type}, FN_AVG_UINT64},
                  {double_type, {double_type}, FN_AVG_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_AVG_NUMERIC,
                   has_numeric_type_argument}});

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
       {string_type, {string_type, {string_type, non_null_non_agg}},
         FN_STRING_AGG_DELIM_STRING},
       // Bytes inputs
       {bytes_type, {bytes_type}, FN_STRING_AGG_BYTES},
       // Resolution will verify that the second argument must be a literal.
       {bytes_type, {bytes_type, {bytes_type, non_null_non_agg}},
         FN_STRING_AGG_DELIM_BYTES}},
      FunctionOptions()
          .set_supports_order_by(true)
          .set_supports_limit(true));

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

static void GetApproxFunctions(TypeFactory* type_factory,
                               const ZetaSQLBuiltinFunctionOptions& options,
                               NameToFunctionMap* functions) {
  const Type* int64_type = type_factory->get_int64();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* double_type = type_factory->get_double();
  const Type* numeric_type = type_factory->get_numeric();

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

  InsertFunction(functions, options, "approx_count_distinct", AGGREGATE,
                 {{int64_type, {{ARG_TYPE_ANY_1, supports_grouping}},
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
                   has_numeric_type_argument}},
                 FunctionOptions().set_compute_result_type_callback(
                     bind_front(&ComputeResultTypeForTopStruct, "sum")));
}

static void GetStatisticalFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions) {
  const Type* double_type = type_factory->get_double();
  const Function::Mode AGGREGATE = Function::AGGREGATE;

  // Support statistical finctions:
  // CORR, COVAR_POP, COVAR_SAMP,
  // STDDEV_POP, STDDEV_SAMP, STDDEV (alias for STDDEV_SAMP),
  // VAR_POP, VAR_SAMP, VARIANCE (alias for VAR_SAMP)
  // in both modes: aggregate and analytic.
  InsertFunction(functions, options, "corr", AGGREGATE,
                 {{double_type, {double_type, double_type}, FN_CORR}});
  InsertFunction(functions, options, "covar_pop", AGGREGATE,
                 {{double_type, {double_type, double_type}, FN_COVAR_POP}});
  InsertFunction(functions, options, "covar_samp", AGGREGATE,
                 {{double_type, {double_type, double_type}, FN_COVAR_SAMP}});
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
}

static void GetAnalyticFunctions(TypeFactory* type_factory,
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
      {{int64_type, {}, FN_RANK}},
      required_order_disallowed_frame);
  InsertFunction(functions, options, "percent_rank", ANALYTIC,
      {{double_type, {}, FN_PERCENT_RANK}},
      required_order_disallowed_frame);
  InsertFunction(functions, options, "cume_dist", ANALYTIC,
      {{double_type, {}, FN_CUME_DIST}},
      required_order_disallowed_frame);
  InsertFunction(functions, options, "ntile", ANALYTIC,
      {{int64_type, {{int64_type, non_null_positive_non_agg}}, FN_NTILE}},
        required_order_disallowed_frame);

  InsertFunction(functions, options, "row_number", ANALYTIC,
      {{int64_type, {}, FN_ROW_NUMBER}},
      optional_order_disallowed_frame);

  // The optional arguments will be populated in the resolved tree.
  // The second offset argument defaults to 1, and the third default
  // argument defaults to NULL.
  InsertFunction(functions, options, "lead", ANALYTIC,
      {{ARG_TYPE_ANY_1,
       {ARG_TYPE_ANY_1,
        {int64_type, optional_non_null_non_agg},
        // If present, the third argument must be a constant or parameter.
        // TODO: b/18709755: Give an error if it isn't.
        {ARG_TYPE_ANY_1, OPTIONAL}},
       FN_LEAD}},
      required_order_disallowed_frame);
  InsertFunction(functions, options, "lag", ANALYTIC,
      {{ARG_TYPE_ANY_1,
       {ARG_TYPE_ANY_1,
        {int64_type, optional_non_null_non_agg},
        // If present, the third argument must be a constant or parameter.
        // TODO: b/18709755: Give an error if it isn't.
        {ARG_TYPE_ANY_1, OPTIONAL}},
       FN_LAG}},
      required_order_disallowed_frame);

  InsertFunction(functions, options, "first_value", ANALYTIC,
      {{ARG_TYPE_ANY_1, {{ARG_TYPE_ANY_1, non_null}}, FN_FIRST_VALUE}},
      required_order_allowed_frame_and_null_handling);
  InsertFunction(functions, options, "last_value", ANALYTIC,
      {{ARG_TYPE_ANY_1, {{ARG_TYPE_ANY_1, non_null}}, FN_LAST_VALUE}},
      required_order_allowed_frame_and_null_handling);
  InsertFunction(functions, options, "nth_value", ANALYTIC,
      {{ARG_TYPE_ANY_1,
       {ARG_TYPE_ANY_1, {int64_type, non_null_positive_non_agg}},
       FN_NTH_VALUE}},
      required_order_allowed_frame_and_null_handling);

  InsertFunction(functions, options, "percentile_cont", ANALYTIC,
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

static void GetBooleanFunctions(TypeFactory* type_factory,
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
    InsertFunction(
        functions, options, "$between", SCALAR,
        {{bool_type,
          {ARG_TYPE_ANY_1, ARG_TYPE_ANY_1, ARG_TYPE_ANY_1},
          FN_BETWEEN}},
        FunctionOptions()
          .set_supports_safe_error_mode(false)
          .set_post_resolution_argument_constraint(
              bind_front(&CheckArgumentsSupportComparison, "BETWEEN"))
          .set_get_sql_callback(&BetweenFunctionSQL));
  } else {
    InsertFunction(
        functions, options, "$between", SCALAR,
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
          .set_post_resolution_argument_constraint(
              bind_front(&CheckArgumentsSupportComparison, "BETWEEN"))
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

static void GetLogicFunctions(TypeFactory* type_factory,
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

static void GetStringFunctions(TypeFactory* type_factory,
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

  InsertFunction(functions, options, "concat", SCALAR,
      {{string_type, {string_type, {string_type, REPEATED}}, FN_CONCAT_STRING},
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

  InsertFunction(functions, options, "starts_with", SCALAR,
      {{bool_type, {string_type, string_type}, FN_STARTS_WITH_STRING},
       {bool_type, {bytes_type, bytes_type}, FN_STARTS_WITH_BYTES}});

  InsertFunction(functions, options, "ends_with", SCALAR,
      {{bool_type, {string_type, string_type}, FN_ENDS_WITH_STRING},
       {bool_type, {bytes_type, bytes_type}, FN_ENDS_WITH_BYTES}});

  InsertFunction(functions, options, "substr", SCALAR,
      {{string_type, {string_type, int64_type, {int64_type, OPTIONAL}},
          FN_SUBSTR_STRING},
       {bytes_type, {bytes_type, int64_type, {int64_type, OPTIONAL}},
          FN_SUBSTR_BYTES}});

  InsertFunction(functions, options, "trim", SCALAR,
      {{string_type, {string_type, {string_type, OPTIONAL}}, FN_TRIM_STRING},
       {bytes_type, {bytes_type, bytes_type}, FN_TRIM_BYTES}});

  InsertFunction(functions, options, "ltrim", SCALAR,
      {{string_type, {string_type, {string_type, OPTIONAL}}, FN_LTRIM_STRING},
       {bytes_type, {bytes_type, bytes_type}, FN_LTRIM_BYTES}});

  InsertFunction(functions, options, "rtrim", SCALAR,
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

  InsertFunction(functions, options, "repeat", SCALAR,
                 {{string_type, {string_type, int64_type}, FN_REPEAT_STRING},
                  {bytes_type, {bytes_type, int64_type}, FN_REPEAT_BYTES}});

  InsertFunction(
      functions, options, "reverse", SCALAR,
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

  InsertFunction(functions, options, "safe_convert_bytes_to_string", SCALAR,
      {{string_type, {bytes_type}, FN_SAFE_CONVERT_BYTES_TO_STRING}});

  InsertFunction(functions, options, "normalize", SCALAR,
      {{string_type, {string_type, {normalize_mode_type, OPTIONAL}},
          FN_NORMALIZE_STRING},
      });
  InsertFunction(functions, options, "normalize_and_casefold", SCALAR,
      {{string_type, {string_type, {normalize_mode_type, OPTIONAL}},
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

static void GetRegexFunctions(TypeFactory* type_factory,

                              const ZetaSQLBuiltinFunctionOptions& options,
                              NameToFunctionMap* functions) {
  const Type* string_type = type_factory->get_string();
  const Type* bytes_type = type_factory->get_bytes();
  const Type* bool_type = type_factory->get_bool();

  const Function::Mode SCALAR = Function::SCALAR;

  InsertFunction(functions, options, "regexp_match", SCALAR,
      {{bool_type, {string_type, string_type}, FN_REGEXP_MATCH_STRING},
       {bool_type, {bytes_type, bytes_type}, FN_REGEXP_MATCH_BYTES}},
      FunctionOptions().set_allow_external_usage(false));
  InsertFunction(functions, options, "regexp_contains", SCALAR,
      {{bool_type, {string_type, string_type}, FN_REGEXP_CONTAINS_STRING},
       {bool_type, {bytes_type, bytes_type}, FN_REGEXP_CONTAINS_BYTES}});

  InsertFunction(functions, options, "regexp_extract", SCALAR,
      {{string_type, {string_type, string_type}, FN_REGEXP_EXTRACT_STRING},
       {bytes_type, {bytes_type, bytes_type}, FN_REGEXP_EXTRACT_BYTES}});

  InsertFunction(functions, options, "regexp_replace", SCALAR,
      {{string_type, {string_type, string_type, string_type},
          FN_REGEXP_REPLACE_STRING},
       {bytes_type, {bytes_type, bytes_type, bytes_type},
          FN_REGEXP_REPLACE_BYTES}});
  const ArrayType* string_array_type = types::StringArrayType();
  const ArrayType* bytes_array_type = types::BytesArrayType();

  InsertFunction(functions, options, "regexp_extract_all", SCALAR,
      {{string_array_type, {string_type, string_type},
           FN_REGEXP_EXTRACT_ALL_STRING},
       {bytes_array_type, {bytes_type, bytes_type},
           FN_REGEXP_EXTRACT_ALL_BYTES}});
}

static void GetProto3ConversionFunctions(
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

static void GetMiscellaneousFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions) {
  const Type* bool_type = type_factory->get_bool();
  const Type* int32_type = type_factory->get_int32();
  const Type* int64_type = type_factory->get_int64();
  const Type* uint32_type = type_factory->get_uint32();
  const Type* uint64_type = type_factory->get_uint64();
  const Type* numeric_type = type_factory->get_numeric();
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
  const Type* double_array_type = types::DoubleArrayType();
  const Type* date_array_type = types::DateArrayType();
  const Type* timestamp_array_type = types::TimestampArrayType();

  const Function::Mode SCALAR = Function::SCALAR;

  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;
  const FunctionArgumentType::ArgumentCardinality REPEATED =
      FunctionArgumentType::REPEATED;

  InsertFunction(functions, options, "if", SCALAR,
      {{ARG_TYPE_ANY_1, {bool_type, ARG_TYPE_ANY_1, ARG_TYPE_ANY_1}, FN_IF}});

  // COALESCE(expr1, ..., exprN): returns the first non-null expression.
  // In particular, COALESCE is used to express the output of FULL JOIN.
  InsertFunction(functions, options, "coalesce", SCALAR,
      {{ARG_TYPE_ANY_1, {{ARG_TYPE_ANY_1, REPEATED}}, FN_COALESCE}});

  // IFNULL(expr1, expr2): if expr1 is not null, returns expr1, else expr2
  InsertFunction(functions, options, "ifnull", SCALAR,
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
  // ARRAY_TO_STRING: returns concatentation of elements of the input array.
  InsertFunction(functions, options, "array_to_string", SCALAR,
                 {{string_type, {array_string_type, string_type,
                                 {string_type, OPTIONAL}},
                   FN_ARRAY_TO_STRING},
                  {bytes_type, {array_bytes_type, bytes_type,
                                {bytes_type, OPTIONAL}},
                   FN_ARRAY_TO_BYTES}
                 });

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
  InsertFunction(functions, options, "generate_array", SCALAR,
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

  InsertFunction(functions, options, "to_json_string", SCALAR,
                 {{string_type,
                   {ARG_TYPE_ANY_1, {bool_type, OPTIONAL}},
                   FN_TO_JSON_STRING}});

  // The signature is declared as
  //   ERROR(std::string) -> int64
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
        FunctionOptions()
            .set_allow_external_usage(false));
  }
}

static void GetNumericFunctions(TypeFactory* type_factory,
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

  const Function::Mode SCALAR = Function::SCALAR;
  const FunctionArgumentType::ArgumentCardinality REPEATED =
      FunctionArgumentType::REPEATED;
  const FunctionArgumentType::ArgumentCardinality OPTIONAL =
      FunctionArgumentType::OPTIONAL;

  FunctionSignatureOptions has_floating_point_argument;
  has_floating_point_argument.set_constraints(&HasFloatingPointArgument);
  FunctionSignatureOptions has_numeric_type_argument;
  has_numeric_type_argument.set_constraints(&HasNumericTypeArgument);

  InsertFunction(functions, options, "abs", SCALAR, {
      {int32_type, {int32_type}, FN_ABS_INT32},
      {int64_type, {int64_type}, FN_ABS_INT64},
      {uint32_type, {uint32_type}, FN_ABS_UINT32},
      {uint64_type, {uint64_type}, FN_ABS_UINT64},
      {float_type, {float_type}, FN_ABS_FLOAT},
      {double_type, {double_type}, FN_ABS_DOUBLE},
      {numeric_type, {numeric_type}, FN_ABS_NUMERIC}});

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
                   has_numeric_type_argument}});

  InsertFunction(functions, options, "round", SCALAR, {
      {float_type, {float_type}, FN_ROUND_FLOAT},
      {double_type, {double_type}, FN_ROUND_DOUBLE},
      {numeric_type, {numeric_type}, FN_ROUND_NUMERIC,
        has_numeric_type_argument},
      {float_type, {float_type, int64_type}, FN_ROUND_WITH_DIGITS_FLOAT},
      {double_type, {double_type, int64_type}, FN_ROUND_WITH_DIGITS_DOUBLE},
      {numeric_type, {numeric_type, int64_type},
        FN_ROUND_WITH_DIGITS_NUMERIC, has_numeric_type_argument}});
  InsertFunction(functions, options, "trunc", SCALAR, {
      {float_type, {float_type}, FN_TRUNC_FLOAT},
      {double_type, {double_type}, FN_TRUNC_DOUBLE},
      {numeric_type, {numeric_type}, FN_TRUNC_NUMERIC,
        has_numeric_type_argument},
      {float_type, {float_type, int64_type}, FN_TRUNC_WITH_DIGITS_FLOAT},
      {double_type, {double_type, int64_type}, FN_TRUNC_WITH_DIGITS_DOUBLE},
      {numeric_type, {numeric_type, int64_type},
        FN_TRUNC_WITH_DIGITS_NUMERIC, has_numeric_type_argument}});
  InsertFunction(functions, options, "ceil", SCALAR, {
      {float_type, {float_type}, FN_CEIL_FLOAT},
      {double_type, {double_type}, FN_CEIL_DOUBLE},
      {numeric_type, {numeric_type}, FN_CEIL_NUMERIC,
        has_numeric_type_argument}},
      FunctionOptions().set_alias_name("ceiling"));
  InsertFunction(functions, options, "floor", SCALAR, {
      {float_type, {float_type}, FN_FLOOR_FLOAT},
      {double_type, {double_type}, FN_FLOOR_DOUBLE},
      {numeric_type, {numeric_type}, FN_FLOOR_NUMERIC,
        has_numeric_type_argument}});

  InsertFunction(functions, options, "is_inf", SCALAR,
                 {{bool_type, {double_type}, FN_IS_INF}});
  InsertFunction(functions, options, "is_nan", SCALAR,
                 {{bool_type, {double_type}, FN_IS_NAN}});

  InsertFunction(functions, options, "ieee_divide", SCALAR, {
      {double_type, {double_type, double_type}, FN_IEEE_DIVIDE_DOUBLE},
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
       {numeric_type, {numeric_type, numeric_type},
        FN_MOD_NUMERIC, has_numeric_type_argument}});

  InsertFunction(functions, options, "div", SCALAR,
                 {{int64_type, {int64_type, int64_type}, FN_DIV_INT64},
                  {uint64_type, {uint64_type, uint64_type}, FN_DIV_UINT64},
                  {numeric_type,
                   {numeric_type, numeric_type},
                   FN_DIV_NUMERIC,
                   has_numeric_type_argument}});

  // The SAFE versions of arithmetic operators (+, -, *, /, <unary minus>) have
  // the same signatures as the operators themselves.
  InsertFunction(
      functions, options, "safe_add", SCALAR,
      {{int64_type, {int64_type, int64_type}, FN_SAFE_ADD_INT64},
       {uint64_type, {uint64_type, uint64_type}, FN_SAFE_ADD_UINT64},
       {double_type,
        {double_type, double_type},
        FN_SAFE_ADD_DOUBLE,
        has_floating_point_argument},
       {numeric_type,
        {numeric_type, numeric_type},
        FN_SAFE_ADD_NUMERIC,
        has_numeric_type_argument}});

  InsertFunction(
      functions, options, "safe_subtract", SCALAR,
      {{int64_type, {int64_type, int64_type}, FN_SAFE_SUBTRACT_INT64},
       {int64_type, {uint64_type, uint64_type}, FN_SAFE_SUBTRACT_UINT64},
       {numeric_type,
        {numeric_type, numeric_type},
        FN_SAFE_SUBTRACT_NUMERIC,
        has_numeric_type_argument},
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
        has_numeric_type_argument}});

  InsertFunction(
      functions, options, "safe_divide", SCALAR,
      {{double_type, {double_type, double_type}, FN_SAFE_DIVIDE_DOUBLE},
       {numeric_type,
        {numeric_type, numeric_type},
        FN_SAFE_DIVIDE_NUMERIC,
        has_numeric_type_argument}});

  InsertFunction(
      functions, options, "safe_negate", SCALAR,
                 {{int32_type, {int32_type}, FN_SAFE_UNARY_MINUS_INT32},
                  {int64_type, {int64_type}, FN_SAFE_UNARY_MINUS_INT64},
                  {float_type, {float_type}, FN_SAFE_UNARY_MINUS_FLOAT},
                  {double_type, {double_type}, FN_SAFE_UNARY_MINUS_DOUBLE},
                  {numeric_type,
                   {numeric_type},
                   FN_SAFE_UNARY_MINUS_NUMERIC,
                   has_numeric_type_argument}},
                 FunctionOptions()
                   .set_arguments_are_coercible(false));

  InsertFunction(functions, options, "sqrt", SCALAR, {
      {double_type, {double_type}, FN_SQRT_DOUBLE}});
  InsertFunction(functions, options, "pow", SCALAR, {
      {double_type, {double_type, double_type}, FN_POW_DOUBLE},
      {numeric_type, {numeric_type, numeric_type}, FN_POW_NUMERIC,
        has_numeric_type_argument}},
      FunctionOptions().set_alias_name("power"));
  InsertFunction(functions, options, "exp", SCALAR, {
      {double_type, {double_type}, FN_EXP_DOUBLE}});
  InsertFunction(functions, options, "ln", SCALAR, {
      {double_type, {double_type}, FN_NATURAL_LOGARITHM_DOUBLE}});
  InsertFunction(functions, options, "log", SCALAR, {
      {double_type, {double_type, {double_type, OPTIONAL}},
        FN_LOGARITHM_DOUBLE}});
  InsertFunction(functions, options, "log10", SCALAR, {
      {double_type, {double_type}, FN_DECIMAL_LOGARITHM_DOUBLE}});
}

static void GetTrigonometricFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
    NameToFunctionMap* functions) {
  const Type* double_type = type_factory->get_double();

  const Function::Mode SCALAR = Function::SCALAR;

  InsertFunction(functions, options, "cos", SCALAR, {
      {double_type, {double_type}, FN_COS_DOUBLE}});
  InsertFunction(functions, options, "cosh", SCALAR, {
      {double_type, {double_type}, FN_COSH_DOUBLE}});
  InsertFunction(functions, options, "acos", SCALAR, {
      {double_type, {double_type}, FN_ACOS_DOUBLE}});
  InsertFunction(functions, options, "acosh", SCALAR, {
      {double_type, {double_type}, FN_ACOSH_DOUBLE}});
  InsertFunction(functions, options, "sin", SCALAR, {
      {double_type, {double_type}, FN_SIN_DOUBLE}});
  InsertFunction(functions, options, "sinh", SCALAR, {
      {double_type, {double_type}, FN_SINH_DOUBLE}});
  InsertFunction(functions, options, "asin", SCALAR, {
      {double_type, {double_type}, FN_ASIN_DOUBLE}});
  InsertFunction(functions, options, "asinh", SCALAR, {
      {double_type, {double_type}, FN_ASINH_DOUBLE}});
  InsertFunction(functions, options, "tan", SCALAR, {
      {double_type, {double_type}, FN_TAN_DOUBLE}});
  InsertFunction(functions, options, "tanh", SCALAR, {
      {double_type, {double_type}, FN_TANH_DOUBLE}});
  InsertFunction(functions, options, "atan", SCALAR, {
      {double_type, {double_type}, FN_ATAN_DOUBLE}});
  InsertFunction(functions, options, "atanh", SCALAR, {
      {double_type, {double_type}, FN_ATANH_DOUBLE}});
  InsertFunction(functions, options, "atan2", SCALAR, {
      {double_type, {double_type, double_type}, FN_ATAN2_DOUBLE}});
}

static void GetMathFunctions(TypeFactory* type_factory,

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
  InsertNamespaceFunction(
      functions, options, "net", "host", SCALAR,
      {{string_type, {string_type}, FN_NET_HOST}});
  InsertNamespaceFunction(
      functions, options, "net", "reg_domain", SCALAR,
      {{string_type, {string_type}, FN_NET_REG_DOMAIN}});
  InsertNamespaceFunction(
      functions, options, "net", "public_suffix", SCALAR,
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

  InsertNamespaceFunction(
      functions, options, "hll_count", "merge", AGGREGATE,
      {{int64_type, {bytes_type}, FN_HLL_COUNT_MERGE}});
  InsertNamespaceFunction(
      functions, options, "hll_count", "extract", SCALAR,
      {{int64_type, {bytes_type}, FN_HLL_COUNT_EXTRACT}});
  InsertNamespaceFunction(
      functions, options, "hll_count", "init", AGGREGATE,
      {{bytes_type, {int64_type, {int64_type, hll_init_arg}},
         FN_HLL_COUNT_INIT_INT64},
       {bytes_type, {uint64_type, {int64_type, hll_init_arg}},
         FN_HLL_COUNT_INIT_UINT64},
       {bytes_type, {numeric_type, {int64_type, hll_init_arg}},
         FN_HLL_COUNT_INIT_NUMERIC, has_numeric_type_argument},
       {bytes_type, {string_type, {int64_type, hll_init_arg}},
         FN_HLL_COUNT_INIT_STRING},
       {bytes_type, {bytes_type, {int64_type, hll_init_arg}},
         FN_HLL_COUNT_INIT_BYTES}});
  InsertNamespaceFunction(
      functions, options, "hll_count", "merge_partial", AGGREGATE,
      {{bytes_type, {bytes_type}, FN_HLL_COUNT_MERGE_PARTIAL}});
}

static void GetKllQuantilesFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
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
                          FunctionOptions().
                              set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles", "init_uint64",
                          AGGREGATE,
                          {{bytes_type,
                            {uint64_type, {int64_type, init_inv_eps_arg}},
                            FN_KLL_QUANTILES_INIT_UINT64}},
                          FunctionOptions().
                              set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles", "init_double",
                          AGGREGATE,
                          {{bytes_type,
                            {double_type, {int64_type, init_inv_eps_arg}},
                            FN_KLL_QUANTILES_INIT_DOUBLE}},
                          FunctionOptions().
                              set_allow_external_usage(false));

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
                          FunctionOptions().
                              set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles", "merge_double",
                          AGGREGATE,
                          {{double_array_type,
                            {bytes_type, {int64_type, num_quantiles_merge_arg}},
                            FN_KLL_QUANTILES_MERGE_DOUBLE}},
                          FunctionOptions().
                              set_allow_external_usage(false));

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
                          FunctionOptions().
                              set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles",
                          "merge_point_uint64", AGGREGATE,
                          {{uint64_type,
                            {bytes_type, {double_type, phi_merge_arg}},
                            FN_KLL_QUANTILES_MERGE_POINT_UINT64}},
                          FunctionOptions().
                              set_allow_external_usage(false));
  InsertNamespaceFunction(functions, options, "kll_quantiles",
                          "merge_point_double", AGGREGATE,
                          {{double_type,
                            {bytes_type, {double_type, phi_merge_arg}},
                            FN_KLL_QUANTILES_MERGE_POINT_DOUBLE}},
                          FunctionOptions().
                              set_allow_external_usage(false));

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

static void GetHashingFunctions(TypeFactory* type_factory,
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

static void GetEncryptionFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
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
}

static void GetGeographyFunctions(
    TypeFactory* type_factory, const ZetaSQLBuiltinFunctionOptions& options,
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
      FunctionOptions()
          .add_required_language_feature(zetasql::FEATURE_GEOGRAPHY);

  const FunctionArgumentTypeOptions optional_const_arg_options =
      FunctionArgumentTypeOptions()
          .set_must_be_constant()
          .set_cardinality(OPTIONAL);

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

void GetZetaSQLFunctions(TypeFactory* type_factory,
                           const ZetaSQLBuiltinFunctionOptions& options,
                           NameToFunctionMap* functions) {
  GetDatetimeFunctions(type_factory, options, functions);
  GetArithmeticFunctions(type_factory, options, functions);
  GetBitwiseFunctions(type_factory, options, functions);
  GetAggregateFunctions(type_factory, options, functions);
  GetApproxFunctions(type_factory, options, functions);
  GetStatisticalFunctions(type_factory, options, functions);
  GetBooleanFunctions(type_factory, options, functions);
  GetLogicFunctions(type_factory, options, functions);
  GetStringFunctions(type_factory, options, functions);
  GetRegexFunctions(type_factory, options, functions);
  GetMiscellaneousFunctions(type_factory, options, functions);
  GetMathFunctions(type_factory, options, functions);
  GetHllCountFunctions(type_factory, options, functions);
  GetKllQuantilesFunctions(type_factory, options, functions);
  GetProto3ConversionFunctions(type_factory, options, functions);
  if (options.language_options.LanguageFeatureEnabled(
          FEATURE_ANALYTIC_FUNCTIONS)) {
    GetAnalyticFunctions(type_factory, options, functions);
  }
  GetNetFunctions(type_factory, options, functions);
  GetHashingFunctions(type_factory, options, functions);
  if (options.language_options.LanguageFeatureEnabled(FEATURE_ENCRYPTION)) {
    GetEncryptionFunctions(type_factory, options, functions);
  }
  if (options.language_options.LanguageFeatureEnabled(FEATURE_GEOGRAPHY)) {
    GetGeographyFunctions(type_factory, options, functions);
  }
}

bool FunctionMayHaveUnintendedArgumentCoercion(const Function* function) {
  if (function->NumSignatures() == 0 ||
      !function->ArgumentsAreCoercible()) {
    return false;
  }
  // This only tests between signature arguments at the same argument
  // index.  It would not correctly analyze multiple signatures whose
  // corresponding arguments are not related to each other, but that
  // is not an issue at the time of the initial implementation.
  int max_num_arguments = 0;
  for (int signature_idx = 0; signature_idx < function->NumSignatures();
       ++signature_idx) {
    const FunctionSignature* signature = function->GetSignature(signature_idx);
    if (signature->arguments().size() > max_num_arguments) {
      max_num_arguments = signature->arguments().size();
    }
  }
  for (int argument_idx = 0; argument_idx < max_num_arguments; ++argument_idx) {
    bool has_signed_arguments = false;
    bool has_unsigned_arguments = false;
    bool has_floating_point_arguments = false;
    for (int signature_idx = 0; signature_idx < function->NumSignatures();
         ++signature_idx) {
      const FunctionSignature* signature =
          function->GetSignature(signature_idx);
      if (argument_idx < signature->arguments().size()) {
        const FunctionArgumentType& argument_type =
            signature->argument(argument_idx);
        if (argument_type.type() != nullptr) {
          if (argument_type.type()->IsSignedInteger()) {
            has_signed_arguments = true;
          } else if (argument_type.type()->IsUnsignedInteger()) {
            has_unsigned_arguments = true;
          } else if (argument_type.type()->IsFloatingPoint()) {
            has_floating_point_arguments = true;
          }
        }
      }
    }
    if (has_signed_arguments &&
        has_floating_point_arguments &&
        !has_unsigned_arguments) {
      return true;
    }
  }
  return false;
}

}  // namespace zetasql
