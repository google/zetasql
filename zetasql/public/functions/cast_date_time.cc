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

#include "zetasql/public/functions/cast_date_time.h"

#include <string.h>
#include <time.h>

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/utf_util.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/functions/parse_date_time_utils.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include <cstdint>
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "unicode/uchar.h"
#include "unicode/utf8.h"
#include "zetasql/base/general_trie.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/mathutil.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace functions {
namespace {

using cast_date_time_internal::FormatElement;
using cast_date_time_internal::FormatElementType;
using cast_date_time_internal::GetFormatElements;
using parse_date_time_utils::ConvertTimeToTimestamp;
using TypeToElementMap =
    absl::flat_hash_map<FormatElementType, std::vector<const FormatElement*>>;
using StringToElementsMap =
    absl::flat_hash_map<std::string, const FormatElement*>;

static const int64_t powers_of_ten[] = {
    1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};

constexpr int64_t kNaiveNumSecondsPerMinute = 60;
constexpr int64_t kNaiveNumSecondsPerHour = 60 * kNaiveNumSecondsPerMinute;
constexpr int64_t kNaiveNumSecondsPerDay = 24 * kNaiveNumSecondsPerHour;
constexpr int64_t kNaiveNumMicrosPerDay = kNaiveNumSecondsPerDay * 1000 * 1000;

// Matches <target_str> with string within [<dp>, <end_of_data>) in a
// char-by-char manner. Returns a pointer to the first unparsed character upon
// successfully parsing an integer, and returns nullptr otherwise.
const char* ParseStringByExactMatch(const char* dp, const char* end_of_data,
                                    absl::string_view target_str) {
  if (dp == nullptr || dp >= end_of_data) {
    return nullptr;
  }

  if (absl::StartsWith(absl::string_view(dp, end_of_data - dp), target_str)) {
    return dp + target_str.size();
  } else {
    return nullptr;
  }
}

// Consumes the leading Unicode whitespaces in the string [<dp>, <end_of_data>).
// Returns a pointer to the first unparsed character.
const char* TrimLeadingUnicodeWhiteSpaces(const char* dp,
                                          const char* end_of_data) {
  if (dp == nullptr || end_of_data == nullptr) {
    return nullptr;
  }

  UChar32 character;
  size_t data_length = end_of_data - dp;
  // Offset for consecutive Unicode whitespaces since <dp>.
  size_t all_uwhitespace_offset = 0;
  size_t offset = 0;
  while (offset < end_of_data - dp) {
    U8_NEXT(dp, offset, data_length, character);
    if (u_isUWhiteSpace(character)) {
      all_uwhitespace_offset = offset;
    } else {
      break;
    }
  }
  return dp + all_uwhitespace_offset;
}

std::string FormatElementTypeString(const FormatElementType& type) {
  switch (type) {
    case FormatElementType::kFormatElementTypeUnspecified:
      return "FORMAT_ELEMENT_TYPE_UNSPECIFIED";
    case FormatElementType::kLiteral:
      return "LITERAL";
    case FormatElementType::kDoubleQuotedLiteral:
      return "DOUBLE_QUOTED_LITERAL";
    case FormatElementType::kWhitespace:
      return "WHITESPACE";
    case FormatElementType::kYear:
      return "YEAR";
    case FormatElementType::kMonth:
      return "MONTH";
    case FormatElementType::kDay:
      return "DAY";
    case FormatElementType::kHour:
      return "HOUR";
    case FormatElementType::kMinute:
      return "MINUTE";
    case FormatElementType::kSecond:
      return "SECOND";
    case FormatElementType::kMeridianIndicator:
      return "MERIDIAN_INDICATOR";
    case FormatElementType::kTimeZone:
      return "TIME_ZONE";
    case FormatElementType::kCentury:
      return "CENTURY";
    case FormatElementType::kQuarter:
      return "QUARTER";
    case FormatElementType::kWeek:
      return "WEEK";
    case FormatElementType::kEraIndicator:
      return "ERA_INDICATOR";
    case FormatElementType::kMisc:
      return "MISC";
  }
}

// This struct is used to determine whether the first character matched by the
// format element can be a digit character ('0'~'9'), which is useful when
// deciding whether we do delimited matching or not.
enum FirstMatchedCharType {
  IsDigit,
  // Depends on the <original_str> of format element.
  MayBeDigit,
  IsNotDigit,
};

const absl::flat_hash_map<std::string, FirstMatchedCharType>*
InitializeFormatElementInfoMap() {
  // We do not include type info here to avoid creating a new FormatElement
  // with upper <original_str> as key to do lookup with the map.
  auto* map = new absl::flat_hash_map<std::string, FirstMatchedCharType>{
      /*Literals*/
      {"-", IsNotDigit},
      {".", IsNotDigit},
      {"/", IsNotDigit},
      {",", IsNotDigit},
      {"'", IsNotDigit},
      {";", IsNotDigit},
      {":", IsNotDigit},
      // The first character matched by format element of
      // "kDoubleQuotedLiteral" type actually depends on text in <original_str>
      // of the element.
      {"\"", MayBeDigit},
      {" ", IsNotDigit},

      /*Year*/
      {"YYYY", IsDigit},
      {"YYY", IsDigit},
      {"YY", IsDigit},
      {"Y", IsDigit},
      {"RRRR", IsDigit},
      {"RR", IsDigit},
      {"Y,YYY", IsDigit},

      /*Month*/
      {"MM", IsDigit},
      {"MON", IsNotDigit},
      {"MONTH", IsNotDigit},

      /*Day*/
      {"DD", IsDigit},
      {"DDD", IsDigit},

      /*Hour*/
      {"HH", IsDigit},
      {"HH12", IsDigit},
      {"HH24", IsDigit},

      /*Minute*/
      {"MI", IsDigit},

      /*Second*/
      {"SS", IsDigit},
      {"SSSSS", IsDigit},

      /*Meridian indicator*/
      {"A.M.", IsNotDigit},
      {"AM", IsNotDigit},
      {"P.M.", IsNotDigit},
      {"PM", IsNotDigit},

      /*Timezone*/
      {"TZH", IsNotDigit},
      {"TZM", IsDigit},
  };
  // Format elements "FF1"~"FF9".
  for (int i = 1; i < 10; ++i) {
    map->emplace(absl::StrCat("FF", i), IsDigit);
  }

  return map;
}

// This map contains information for format elements supported for parsing.
// Please check comments of InitializeFormatElementInfoMap function for what
// info this map contains.
const absl::flat_hash_map<std::string, FirstMatchedCharType>&
GetParsingFormatElementInfoMap() {
  static const absl::flat_hash_map<std::string, FirstMatchedCharType>*
      format_element_info_map = InitializeFormatElementInfoMap();
  return *format_element_info_map;
}

// Checks whether the <format_element> is supported for parsing.
bool IsSupportedForParsing(const FormatElement& format_element) {
  std::string str_to_check;
  if (format_element.type == FormatElementType::kDoubleQuotedLiteral) {
    str_to_check = "\"";
  } else if (format_element.type == FormatElementType::kWhitespace) {
    str_to_check = " ";
  } else {
    str_to_check = absl::AsciiStrToUpper(format_element.original_str);
  }
  return GetParsingFormatElementInfoMap().contains(str_to_check);
}

// This function conducts the parsing for <format_elements> with
// <timestamp_string>.
absl::Status ParseTimeWithFormatElements(
    const std::vector<FormatElement>& format_elements,
    absl::string_view timestamp_string, const absl::TimeZone default_timezone,
    const absl::Time current_timestamp, TimestampScale scale,
    absl::Time* timestamp) {
  const char* data = timestamp_string.data();
  const char* const end_of_data = data + timestamp_string.size();

  size_t format_element_processed_size = 0;

  size_t timestamp_str_processed_size = 0;

  absl::TimeZone timezone = default_timezone;
  absl::Duration subseconds = absl::ZeroDuration();

  struct tm tm_now = absl::ToTM(current_timestamp, timezone);
  // We do not initialize <tm_wday> and <tm_yday> here since these fields are
  // not used when converting <tm> to absl::Time.
  struct tm tm = {0};
  tm.tm_year = tm_now.tm_year;
  tm.tm_mon = tm_now.tm_mon;
  tm.tm_mday = 1;
  tm.tm_hour = tm.tm_min = tm.tm_sec = 0;

  // Skips leading whitespaces.
  data = TrimLeadingUnicodeWhiteSpaces(data, end_of_data);

  while (data != nullptr && data < end_of_data &&
         format_element_processed_size < format_elements.size()) {
    const FormatElement& format_element =
        format_elements[format_element_processed_size];
    // Skips empty "kDoubleQuotedLiteral" format element.
    if (format_element.type == FormatElementType::kDoubleQuotedLiteral &&
        format_element.original_str.empty()) {
      format_element_processed_size++;
      continue;
    }

    const char* prev_data = data;

    switch (format_element.type) {
      case FormatElementType::kLiteral:
      case FormatElementType::kDoubleQuotedLiteral:
        data = ParseStringByExactMatch(data, end_of_data,
                                       format_element.original_str);
        break;
      case FormatElementType::kWhitespace:
        // Format element of "kWhitespace" type matches 1 or more Unicode
        // whitespaces.
        data = TrimLeadingUnicodeWhiteSpaces(data, end_of_data);
        if (data == prev_data) {
          // Matches 0 Unicode whitespace, so we set <data> to nullptr to
          // indicate an error.
          data = nullptr;
        }
        break;
      // TODO: Support all the valid format elements for parsing.
      default:
        break;
    }

    if (data == prev_data) {
      // If <data> and <prev_data> are the same, then we found an invalid format
      // element. Set <data> to be nullptr to indicate the error.
      data = nullptr;
    }

    // We successfully processed a format element, so update the number of
    // elements and characters processed.
    if (data != nullptr) {
      format_element_processed_size++;
      timestamp_str_processed_size = data - timestamp_string.data();
    }
  }

  // Skips any remaining whitespace.
  if (data != nullptr) {
    data = TrimLeadingUnicodeWhiteSpaces(data, end_of_data);
  }
  // Skips trailing empty format elements {kDoubleQuotedLiteral, ""} which match
  // "" in input string.
  while (format_element_processed_size < format_elements.size() &&
         format_elements[format_element_processed_size].type ==
             FormatElementType::kDoubleQuotedLiteral &&
         format_elements[format_element_processed_size].original_str.empty()) {
    format_element_processed_size++;
  }

  if (data == nullptr) {
    return MakeEvalError()
           << "Failed to parse input timestamp string at "
           << timestamp_str_processed_size << " with format element "
           << format_elements[format_element_processed_size].DebugString();
  }

  if (data != end_of_data) {
    return MakeEvalError() << "Illegal non-space trailing data '" << *data
                           << " in timestamp string";
  }

  if (format_element_processed_size < format_elements.size()) {
    return MakeEvalError()
           << "Entire timestamp string has been used before dealing with"
           << " format element "
           << format_elements[format_element_processed_size].DebugString();
  }

  *timestamp = absl::FromTM(tm, timezone) + subseconds;
  return absl::OkStatus();
}

// Returns an error if more than one format element of the target type exist in
// the format string, i.e. the value of <type> in <type_to_elements_map>
// contains more than one item. For example, you cannot have {kYear, "YY"}
// and {kYear, "RRRR"} at the same time since they are both of "kYear" type.
absl::Status CheckForDuplicateElementsOfType(
    FormatElementType type, const TypeToElementMap& type_to_elements_map) {
  if (type_to_elements_map.contains(type) &&
      type_to_elements_map.at(type).size() > 1) {
    return MakeEvalError() << "More than one format element of type "
                           << FormatElementTypeString(type) << " exist: "
                           << type_to_elements_map.at(type)[0]->DebugString()
                           << " and "
                           << type_to_elements_map.at(type)[1]->DebugString();
  }
  return absl::OkStatus();
}

// Returns an error if the element of the target type exists in the format
// string, i.e. <type> exists in <type_to_elements_map> as a key. For example,
// you cannot have any format element of "kHour" type if the output type is
// DATE.
absl::Status CheckTypeNotExist(FormatElementType type,
                               const TypeToElementMap& type_to_elements_map,
                               absl::string_view output_type_name) {
  if (type_to_elements_map.contains(type)) {
    std::string error_reason = absl::Substitute(
        "Format element of type $0 ($1) is not allowed for output type $2",
        FormatElementTypeString(type),
        type_to_elements_map.at(type)[0]->DebugString(), output_type_name);
    return MakeEvalError() << error_reason;
  }
  return absl::OkStatus();
}

// Returns an error if <format_element_str> is present in
// <upper_case_to_elements_map> and <type> is present in <type_to_elements_map>.
// For example, if you have a format element in the form of "HH24", you cannot
// have any format element of "kMeridianIndicator" type.
absl::Status CheckForMutuallyExclusiveElements(
    absl::string_view format_element_str, FormatElementType type,
    const StringToElementsMap& upper_case_to_elements_map,
    const TypeToElementMap& type_to_elements_map) {
  if (upper_case_to_elements_map.contains(format_element_str) &&
      type_to_elements_map.contains(type)) {
    std::string error_reason = absl::Substitute(
        "Format element of type $0 ($1) and format element $2 cannot exist "
        "simultaneously",
        FormatElementTypeString(type),
        type_to_elements_map.at(type)[0]->DebugString(),
        upper_case_to_elements_map.at(format_element_str)->DebugString());
    return MakeEvalError() << error_reason;
  }
  return absl::OkStatus();
}

// Returns an error if both <format_element_str1> and <format_element_str2> are
// present in <upper_case_to_elements_map>. For example, if you have a format
// element of form "SSSSS" which indicates seconds in a day, then you cannot
// have another element of form "SS" to indicate seconds in an hour.
absl::Status CheckForMutuallyExclusiveElements(
    absl::string_view format_element_str1,
    absl::string_view format_element_str2,
    const StringToElementsMap& upper_case_to_elements_map) {
  if (upper_case_to_elements_map.contains(format_element_str1) &&
      upper_case_to_elements_map.contains(format_element_str2)) {
    return MakeEvalError()
           << "Format elements "
           << upper_case_to_elements_map.at(format_element_str1)->DebugString()
           << " and "
           << upper_case_to_elements_map.at(format_element_str2)->DebugString()
           << " cannot exist simultaneously";
  }
  return absl::OkStatus();
}

// Confirms that a format element matching <type> is present if a format element
// in the form of any of <format_element_strs> exists and vice versa. For
// example, you must have a format element of "kMeridianIndicator" type if a
// format element in the form of "HH" or "HH12" is used. Also, if you have a
// format element of "kMeridianIndicator" type, you must have a format element
// in the form of "HH" or "HH12".
absl::Status CheckForCoexistance(
    std::vector<absl::string_view> format_element_strs, FormatElementType type,
    const StringToElementsMap& upper_case_to_elements_map,
    const TypeToElementMap& type_to_elements_map) {
  absl::string_view present_format_element_str;
  bool format_element_str_exists = false;
  for (const absl::string_view& format_element_str : format_element_strs) {
    if (upper_case_to_elements_map.contains(format_element_str)) {
      format_element_str_exists = true;
      present_format_element_str = format_element_str;
      break;
    }
  }

  if (format_element_str_exists && !type_to_elements_map.contains(type)) {
    return MakeEvalError() << "Format element of type "
                           << FormatElementTypeString(type)
                           << " is required when format element "
                           << upper_case_to_elements_map
                                  .at(present_format_element_str)
                                  ->DebugString()
                           << " exists";
  } else if (type_to_elements_map.contains(type) &&
             !format_element_str_exists) {
    struct FormatElementStrFormatter {
      void operator()(std::string* out, absl::string_view format_element_str) {
        out->append(absl::StrCat("\'", format_element_str, "\'"));
      }
    };

    std::string joined_format_element_strs =
        absl::StrJoin(format_element_strs, "/", FormatElementStrFormatter());
    std::string error_reason = absl::Substitute(
        "Format element in the form of $0 is required when format element of "
        "type $1 ($2) exists",
        joined_format_element_strs, FormatElementTypeString(type),
        type_to_elements_map.at(type)[0]->DebugString());
    return MakeEvalError() << error_reason;
  }
  return absl::OkStatus();
}

// Validates the elements in <format_elements> with specific rules, and also
// makes sure they are not of any type in <invalid_types>.
absl::Status ValidateFormatElements(
    const std::vector<FormatElement>& format_elements,
    const std::vector<FormatElementType>& invalid_types,
    absl::string_view target_type_name) {
  TypeToElementMap type_to_elements_map;

  // The map from uppercase <original_str> to the real FormatElement object.
  StringToElementsMap upper_case_to_elements_map;

  for (const FormatElement& format_element : format_elements) {
    if (!IsSupportedForParsing(format_element)) {
      return MakeEvalError()
             << "Format Element " << format_element.DebugString()
             << " is not supported for parsing";
    }

    // We store at most 2 elements inside this map, since this is enough to
    // print in error message when duplicate checks fail for a type.
    if (type_to_elements_map[format_element.type].size() < 2) {
      type_to_elements_map[format_element.type].push_back(&format_element);
    }

    // We do not store information for elements of type "kDoubleQuotedLiteral",
    // "kLiteral" or "kWhitespace" in <upper_case_to_elements_map> assuming that
    // we do not have validations regarding these types.
    if (format_element.type != FormatElementType::kDoubleQuotedLiteral &&
        format_element.type != FormatElementType::kLiteral &&
        format_element.type != FormatElementType::kWhitespace) {
      std::string upper_original_str =
          absl::AsciiStrToUpper(format_element.original_str);
      // We do not allow that more than one non-literal format element in the
      // same uppercase form exist at the same time. For example, the format
      // string "MiYYmI" is invalid since it includes two non-literal format
      // elements {FormatElementType::kMinute, "Mi"} and
      // {FormatElementType::kMinute, "mI"} and the uppercase forms of "Mi" and
      // "mI" are the same.
      if (upper_case_to_elements_map.contains(upper_original_str)) {
        return MakeEvalError() << absl::Substitute(
                   "More than one format element in upper case form $0 exist: "
                   "$1, $2",
                   upper_original_str,
                   upper_case_to_elements_map[upper_original_str]
                       ->DebugString(),
                   format_element.DebugString());
      }
      upper_case_to_elements_map[upper_original_str] = &format_element;
    }
  }

  // Checks types which do not allow duplications.
  const std::vector<FormatElementType> types_to_check_duplicate = {
      FormatElementType::kMeridianIndicator, FormatElementType::kYear,
      FormatElementType::kMonth, FormatElementType::kDay,
      FormatElementType::kHour
      // We do not check "kMinute" type since it only has one possible uppercase
      // form "MI" and we check single occurrence of each distinct uppercase
      // form above.
  };

  for (FormatElementType type : types_to_check_duplicate) {
    ZETASQL_RETURN_IF_ERROR(
        CheckForDuplicateElementsOfType(type, type_to_elements_map));
  }

  // Checks mutually exclusive format elements/types.
  // Elements in the form of "DDD" contain both Day and Month info, therefore
  // format elements of "kMonth" type or in the form of "DD" are disallowed.
  // "DDD"/"DD" check is covered by duplicate check for "kDay" type.
  ZETASQL_RETURN_IF_ERROR(CheckForMutuallyExclusiveElements(
      "DDD", FormatElementType::kMonth, upper_case_to_elements_map,
      type_to_elements_map));

  // The Check between "HH24" and "HH"/"HH12" is included in duplicate check for
  // "kHour" type.
  ZETASQL_RETURN_IF_ERROR(CheckForMutuallyExclusiveElements(
      "HH24", FormatElementType::kMeridianIndicator, upper_case_to_elements_map,
      type_to_elements_map));
  // A Format element of "kMeridianIndicator" type must exist when a format
  // element in the form of "HH" or "HH12" is present. Also, if we have a
  // format element of "kMeridianIndicator" type, a format element in the form
  // of "HH" or "HH12" must exist.
  ZETASQL_RETURN_IF_ERROR(
      CheckForCoexistance({"HH", "HH12"}, FormatElementType::kMeridianIndicator,
                          upper_case_to_elements_map, type_to_elements_map));

  // Format elements in the form of "SSSSS" contain Hour, Minute and Second
  // info, therefore elements of "kHour" (along with "kMeridianIndicator")
  // and "kMinute" types and elements in the form of "SS" are disallowed.
  ZETASQL_RETURN_IF_ERROR(CheckForMutuallyExclusiveElements(
      "SSSSS", FormatElementType::kHour, upper_case_to_elements_map,
      type_to_elements_map));
  ZETASQL_RETURN_IF_ERROR(CheckForMutuallyExclusiveElements(
      "SSSSS", FormatElementType::kMinute, upper_case_to_elements_map,
      type_to_elements_map));
  ZETASQL_RETURN_IF_ERROR(CheckForMutuallyExclusiveElements(
      "SSSSS", "SS", upper_case_to_elements_map));

  // Checks invalid format element types for the output type.
  for (auto& invalid_type : invalid_types) {
    ZETASQL_RETURN_IF_ERROR(CheckTypeNotExist(invalid_type, type_to_elements_map,
                                      target_type_name));
  }
  return absl::OkStatus();
}

// The result <timestamp> is always at microseconds precision.
absl::Status ParseTimeWithFormatElements(
    const std::vector<FormatElement>& format_elements,
    absl::string_view timestamp_string, const absl::TimeZone default_timezone,
    const absl::Time current_timestamp, TimestampScale scale,
    int64_t* timestamp_micros) {
  absl::Time base_time;
  ZETASQL_RETURN_IF_ERROR(ParseTimeWithFormatElements(
      format_elements, timestamp_string, default_timezone, current_timestamp,
      scale, &base_time));

  if (!ConvertTimeToTimestamp(base_time, timestamp_micros)) {
    return MakeEvalError() << "Invalid result from parsing function";
  }
  return absl::OkStatus();
}

bool CheckSupportedFormatYearElement(absl::string_view upper_format_string) {
  if (upper_format_string.empty() || upper_format_string.size() > 4) {
    return false;
  }
  // Currently the only supported format year strings are Y, YY, YYY, YYYY,
  // RR or RRRR.
  const char first_char = upper_format_string[0];
  if (first_char != 'Y' && first_char != 'R') {
    return false;
  }
  for (const char& c : upper_format_string) {
    if (c != first_char) {
      return false;
    }
  }
  return true;
}

// Checks to see if the format elements are valid for the date or time type.
absl::Status ValidateDateFormatElementsForFormatting(
    absl::Span<const FormatElement> format_elements) {
  for (const FormatElement& element : format_elements) {
    switch (element.type) {
      case FormatElementType::kLiteral:
      case FormatElementType::kDoubleQuotedLiteral:
      case FormatElementType::kWhitespace:
      case FormatElementType::kYear:
      case FormatElementType::kMonth:
      case FormatElementType::kDay:
        continue;
      default:
        return MakeEvalError()
               << "DATE does not support " << element.original_str;
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateTimeFormatElementsForFormatting(
    absl::Span<const FormatElement> format_elements) {
  for (const FormatElement& element : format_elements) {
    switch (element.type) {
      case FormatElementType::kLiteral:
      case FormatElementType::kDoubleQuotedLiteral:
      case FormatElementType::kWhitespace:
      case FormatElementType::kHour:
      case FormatElementType::kMinute:
      case FormatElementType::kSecond:
      case FormatElementType::kMeridianIndicator:
        continue;
      default:
        return MakeEvalError()
               << "TIME does not support " << element.original_str;
    }
  }
  return absl::OkStatus();
}

absl::Status ValidateDatetimeFormatElementsForFormatting(
    absl::Span<const FormatElement> format_elements) {
  for (const FormatElement& element : format_elements) {
    switch (element.type) {
      case FormatElementType::kLiteral:
      case FormatElementType::kDoubleQuotedLiteral:
      case FormatElementType::kWhitespace:
      case FormatElementType::kYear:
      case FormatElementType::kMonth:
      case FormatElementType::kDay:
      case FormatElementType::kHour:
      case FormatElementType::kMinute:
      case FormatElementType::kSecond:
      case FormatElementType::kMeridianIndicator:
        continue;
      default:
        return MakeEvalError()
               << "DATETIME does not support " << element.original_str;
    }
  }
  return absl::OkStatus();
}

}  // namespace

namespace cast_date_time_internal {

std::string FormatElement::DebugString() const {
  return absl::StrCat(
      "\'",
      type == FormatElementType::kDoubleQuotedLiteral
          ? absl::Substitute("\"$0\"", absl::CEscape(original_str))
          : original_str,
      "\'");
}

static const FormatElementType kFormatElementTypeNullValue =
    FormatElementType::kFormatElementTypeUnspecified;
using FormatElementTypeTrie =
    zetasql_base::GeneralTrie<FormatElementType, kFormatElementTypeNullValue>;

const FormatElementTypeTrie* InitializeFormatElementTrie() {
  FormatElementTypeTrie* trie = new FormatElementTypeTrie();
  /*Literals*/
  trie->Insert("-", FormatElementType::kLiteral);
  trie->Insert(".", FormatElementType::kLiteral);
  trie->Insert("/", FormatElementType::kLiteral);
  trie->Insert(",", FormatElementType::kLiteral);
  trie->Insert("'", FormatElementType::kLiteral);
  trie->Insert(";", FormatElementType::kLiteral);
  trie->Insert(":", FormatElementType::kLiteral);

  /*Double Quoted Literal*/
  // For the format element '\"xxxxx\"' (arbitrary text enclosed by ""), we
  // would match '\"' in the trie and then manually search the end of the
  // format element.
  trie->Insert("\"", FormatElementType::kDoubleQuotedLiteral);

  /*Whitespace*/
  // For the format element consisting of a sequence of consecutive ASCII space
  // characters (' '), we would match ' ' in the trie and then manually search
  // the end of the sequence.
  trie->Insert(" ", FormatElementType::kWhitespace);

  /*Year*/
  trie->Insert("YYYY", FormatElementType::kYear);
  trie->Insert("YYY", FormatElementType::kYear);
  trie->Insert("YY", FormatElementType::kYear);
  trie->Insert("Y", FormatElementType::kYear);
  trie->Insert("RRRR", FormatElementType::kYear);
  trie->Insert("RR", FormatElementType::kYear);
  trie->Insert("Y,YYY", FormatElementType::kYear);
  trie->Insert("IYYY", FormatElementType::kYear);
  trie->Insert("IYY", FormatElementType::kYear);
  trie->Insert("IY", FormatElementType::kYear);
  trie->Insert("I", FormatElementType::kYear);
  trie->Insert("SYYYY", FormatElementType::kYear);
  trie->Insert("YEAR", FormatElementType::kYear);
  trie->Insert("SYEAR", FormatElementType::kYear);

  /*Month*/
  trie->Insert("MM", FormatElementType::kMonth);
  trie->Insert("MON", FormatElementType::kMonth);
  trie->Insert("MONTH", FormatElementType::kMonth);
  trie->Insert("RM", FormatElementType::kMonth);

  /*Day*/
  trie->Insert("DDD", FormatElementType::kDay);
  trie->Insert("DD", FormatElementType::kDay);
  trie->Insert("D", FormatElementType::kDay);
  trie->Insert("DAY", FormatElementType::kDay);
  trie->Insert("DY", FormatElementType::kDay);
  trie->Insert("J", FormatElementType::kDay);

  /*Hour*/
  trie->Insert("HH", FormatElementType::kHour);
  trie->Insert("HH12", FormatElementType::kHour);
  trie->Insert("HH24", FormatElementType::kHour);

  /*Minute*/
  trie->Insert("MI", FormatElementType::kMinute);

  /*Second*/
  trie->Insert("SS", FormatElementType::kSecond);
  trie->Insert("SSSSS", FormatElementType::kSecond);
  // FF1~FF9
  for (int i = 1; i < 10; ++i) {
    trie->Insert(absl::StrCat("FF", i), FormatElementType::kSecond);
  }

  /*Meridian indicator*/
  trie->Insert("A.M.", FormatElementType::kMeridianIndicator);
  trie->Insert("AM", FormatElementType::kMeridianIndicator);
  trie->Insert("P.M.", FormatElementType::kMeridianIndicator);
  trie->Insert("PM", FormatElementType::kMeridianIndicator);

  /*Time zone*/
  trie->Insert("TZH", FormatElementType::kTimeZone);
  trie->Insert("TZM", FormatElementType::kTimeZone);

  /*Century*/
  trie->Insert("CC", FormatElementType::kCentury);
  trie->Insert("SCC", FormatElementType::kCentury);

  /*Quarter*/
  trie->Insert("Q", FormatElementType::kQuarter);

  /*Week*/
  trie->Insert("IW", FormatElementType::kWeek);
  trie->Insert("WW", FormatElementType::kWeek);
  trie->Insert("W", FormatElementType::kWeek);

  /*Era Indicator*/
  trie->Insert("AD", FormatElementType::kEraIndicator);
  trie->Insert("BC", FormatElementType::kEraIndicator);
  trie->Insert("A.D.", FormatElementType::kEraIndicator);
  trie->Insert("B.C.", FormatElementType::kEraIndicator);

  /*Misc*/
  trie->Insert("SP", FormatElementType::kMisc);
  trie->Insert("TH", FormatElementType::kMisc);
  trie->Insert("SPTH", FormatElementType::kMisc);
  trie->Insert("THSP", FormatElementType::kMisc);
  trie->Insert("FM", FormatElementType::kMisc);

  return trie;
}

const FormatElementTypeTrie& GetFormatElementTrie() {
  static const FormatElementTypeTrie* format_element_trie =
      InitializeFormatElementTrie();
  return *format_element_trie;
}

// We need the upper <format_str> to do the search in prefix tree since matching
// are case-sensitive and we need the original <format_str> to extract the
// original_str for the format element object.
zetasql_base::StatusOr<FormatElement> GetNextFormatElement(
    absl::string_view format_str, absl::string_view upper_format_str,
    size_t* matched_len) {
  FormatElement format_element;
  int matched_len_int;
  const FormatElementTypeTrie& format_element_trie = GetFormatElementTrie();
  const FormatElementType& type = format_element_trie.GetDataForMaximalPrefix(
      upper_format_str, &matched_len_int, /*is_terminator = */ nullptr);
  if (type == kFormatElementTypeNullValue) {
    return MakeEvalError() << "Cannot find matched format element";
  }
  *matched_len = static_cast<size_t>(matched_len_int);
  format_element.type = type;
  if (type != FormatElementType::kDoubleQuotedLiteral) {
    if (type == FormatElementType::kWhitespace) {
      // If the matched type is "kWhitespace", we search for the end of sequence
      // of consecutive ' ' (ASCII 32) characters.
      while (*matched_len < format_str.length() &&
             format_str[*matched_len] == ' ') {
        (*matched_len)++;
      }
    }
    format_element.original_str =
        std::string(format_str.substr(0, (*matched_len)));
    return format_element;
  }

  // if the matched type is kDoubleQuotedLiteral, we search for the end
  // manually and do the unescaping in this process.
  format_element.original_str = "";
  size_t ind_to_check = 1;
  bool is_escaped = false;
  bool stop_search = false;

  while (ind_to_check < format_str.length() && !stop_search) {
    // include the char at position ind_to_check
    (*matched_len)++;
    char char_to_check = format_str[ind_to_check];
    ind_to_check++;
    if (is_escaped) {
      if (char_to_check == '\\' || char_to_check == '\"') {
        is_escaped = false;
      } else {
        return MakeEvalError() << "Unsupported escape sequence \\"
                               << char_to_check << " in text";
      }
    } else if (char_to_check == '\\') {
      is_escaped = true;
      continue;
    } else if (char_to_check == '\"') {
      stop_search = true;
      break;
    }
    format_element.original_str.push_back(char_to_check);
  }
  if (!stop_search) {
    return MakeEvalError() << "Cannot find matching \" for quoted literal";
  }
  return format_element;
}

// We need the upper format_str to do the search in prefix tree since matching
// are case-sensitive and we need the original format_str to extract the
// original_str for the format element object.
zetasql_base::StatusOr<std::vector<FormatElement>> GetFormatElements(
    absl::string_view format_str) {
  std::vector<FormatElement> format_elements;
  size_t processed_len = 0;
  std::string upper_format_str_temp = absl::AsciiStrToUpper(format_str);
  absl::string_view upper_format_str = upper_format_str_temp;
  while (processed_len < format_str.size()) {
    size_t matched_len;
    auto res = GetNextFormatElement(format_str.substr(processed_len),
                                    upper_format_str.substr(processed_len),
                                    &matched_len);
    if (res.ok()) {
      FormatElement& format_element = res.value();
      format_elements.push_back(format_element);
      processed_len += matched_len;
    } else {
      return MakeEvalError()
             << res.status().message() << " at " << processed_len;
    }
  }

  return format_elements;
}

// Takes a format model vector and rewrites it to be a format element string
// that can be correctly formatted by FormatTime. Any elements that are not
// supported by FormatTime will be formatted manually in this function. Any
// elements that output strings will be outputted with the first letter
// capitalized and all subsequent letters will be lowercase.
zetasql_base::StatusOr<std::string> FromFormatElementToFormatString(
    const FormatElement& format_element, const absl::TimeZone::CivilInfo info) {
  const std::string upper_format =
      absl::AsciiStrToUpper(format_element.original_str);
  switch (format_element.type) {
    case FormatElementType::kLiteral:
    case FormatElementType::kDoubleQuotedLiteral:
    case FormatElementType::kWhitespace:
      return format_element.original_str;
    case FormatElementType::kYear: {
      if (CheckSupportedFormatYearElement(upper_format)) {
        // YYYY will output the whole year regardless of how many digits are in
        // the year.
        // FormatTime does not support the year with the last 3 digits.
        int trunc_year = static_cast<int>(info.cs.year()) %
                         powers_of_ten[format_element.original_str.size()];
        return absl::StrFormat(
            "%0*d", format_element.original_str.size(),
            (upper_format.size() == 4 ? info.cs.year() : trunc_year));
      }
      break;
    }
    case FormatElementType::kMonth: {
      if (upper_format == "MM")
        return "%m";
      else if (upper_format == "MON")
        return "%b";
      else if (upper_format == "MONTH")
        return "%B";
      break;
    }
    case FormatElementType::kDay: {
      if (upper_format == "D")
        //  FormatTime returns 0 as Sunday.
        return std::to_string(internal_functions::DayOfWeekIntegerSunToSat1To7(
            absl::GetWeekday(info.cs)));
      else if (upper_format == "DD")
        return "%d";
      else if (upper_format == "DDD")
        return "%j";
      else if (upper_format == "DAY")
        return "%A";
      else if (upper_format == "DY")
        return "%a";
      break;
    }
    case FormatElementType::kHour: {
      if (upper_format == "HH" || upper_format == "HH12")
        return "%I";
      else if (upper_format == "HH24")
        return "%H";
      break;
    }
    case FormatElementType::kMinute:
      return "%M";
    case FormatElementType::kSecond: {
      if (upper_format == "SS") {
        return "%S";
      } else if (upper_format == "SSSSS") {
        // FormatTime does not support having 5 digit second of the day.
        int second_of_day = info.cs.hour() * kNaiveNumSecondsPerHour +
                            info.cs.minute() * kNaiveNumSecondsPerMinute +
                            info.cs.second();
        return absl::StrFormat("%05d", second_of_day);
      } else if (absl::StartsWith(upper_format, "FF")) {
        // TODO : FormatTime does not round fractional seconds.
        return absl::StrCat("%E", format_element.original_str.substr(2), "f");
      }
      break;
    }
    case FormatElementType::kMeridianIndicator: {
      if (absl::StrContains(format_element.original_str, ".")) {
        if (info.cs.hour() > 12) {
          return "P.M.";
        } else {
          return "A.M.";
        }
      } else {
        if (info.cs.hour() > 12) {
          return "PM";
        } else {
          return "AM";
        }
      }
      break;
    }
    case FormatElementType::kTimeZone:
      bool positive_offset;
      int32_t hour_offset;
      int32_t minute_offset;
      internal_functions::GetSignHourAndMinuteTimeZoneOffset(
          info, &positive_offset, &hour_offset, &minute_offset);
      if (upper_format == "TZH") {
        return absl::StrFormat("%c%02d", positive_offset ? '+' : '-',
                               hour_offset);
      } else {
        return absl::StrFormat("%02d", minute_offset);
      }
      break;
    default:
      return MakeEvalError() << "Unsupported format_element type.";
  }
  return MakeEvalError() << "Unsupported format: "
                         << format_element.original_str;
}

zetasql_base::StatusOr<std::string> ResolveFormatString(
    const FormatElement& format_element, absl::Time base_time,
    absl::TimeZone timezone) {
  const absl::TimeZone::CivilInfo info = timezone.At(base_time);
  ZETASQL_ASSIGN_OR_RETURN(const std::string format_string,
                   FromFormatElementToFormatString(format_element, info));
  if (format_element.type == FormatElementType::kLiteral) {
    return format_string;
  }

  // The following resolves casing for format elements.
  std::string resolved_string =
      absl::FormatTime(format_string, base_time, timezone);
  // All of the format elements that are only one character long are numbers and
  // so do not need it's casing changed.
  if (format_element.original_str.size() < 2) {
    return resolved_string;
  }
  // If the first letter of the element is lowercase, then all the letters in
  // the output are lowercase.
  if (format_element.original_str[0] ==
      absl::ascii_tolower(format_element.original_str[0])) {
    return absl::AsciiStrToLower(resolved_string);
  }

  // For AM/PM only the first letter indicates the overall casing.
  if (format_element.type == FormatElementType::kMeridianIndicator) {
    return absl::AsciiStrToUpper(resolved_string);
  }
  // If the first letter is upper case and the second letter is lowercase, then
  // the first letter of each word in the output is capitalized and the other
  // letters are lowercase.
  if (format_element.original_str[0] ==
          absl::ascii_toupper(format_element.original_str[0]) &&
      format_element.original_str[1] ==
          absl::ascii_tolower(format_element.original_str[1])) {
    return resolved_string;
  }
  // If the first two letters of the element are both upper case, the output is
  // capitalized.
  return absl::AsciiStrToUpper(resolved_string);
}

zetasql_base::StatusOr<std::string> FromCastFormatTimestampToStringInternal(
    absl::Span<const FormatElement> format_elements, absl::Time base_time,
    absl::TimeZone timezone) {
  if (!IsValidTime(base_time)) {
    return MakeEvalError() << "Invalid timestamp value: "
                           << absl::ToUnixMicros(base_time);
  }
  absl::TimeZone normalized_timezone =
      internal_functions::GetNormalizedTimeZone(base_time, timezone);
  std::string updated_format_string;
  for (const FormatElement& format_element : format_elements) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::string str_format,
        ResolveFormatString(format_element, base_time, normalized_timezone));
    absl::StrAppend(&updated_format_string, str_format);
  }
  return updated_format_string;
}

}  // namespace cast_date_time_internal
absl::Status CastStringToTimestamp(absl::string_view format_string,
                                   absl::string_view timestamp_string,
                                   const absl::TimeZone default_timezone,
                                   const absl::Time current_timestamp,
                                   int64_t* timestamp_micros) {
  if (!IsWellFormedUTF8(timestamp_string) || !IsWellFormedUTF8(format_string)) {
    return MakeEvalError() << "Input string is not valid UTF-8";
  }
  ZETASQL_ASSIGN_OR_RETURN(const std::vector<FormatElement>& format_elements,
                   GetFormatElements(format_string));
  ZETASQL_RETURN_IF_ERROR(ValidateFormatElements(format_elements, {}, "TIMESTAMP"));

  return ParseTimeWithFormatElements(format_elements, timestamp_string,
                                     default_timezone, current_timestamp,
                                     kMicroseconds, timestamp_micros);
}

absl::Status CastStringToTimestamp(absl::string_view format_string,
                                   absl::string_view timestamp_string,
                                   absl::string_view default_timezone_string,
                                   const absl::Time current_timestamp,
                                   int64_t* timestamp) {
  // Other two input string arguments (<format_string> and <timestamp_string>)
  // are checked in the overload call to CastStringToTimestamp.
  if (!IsWellFormedUTF8(default_timezone_string)) {
    return MakeEvalError() << "Input string is not valid UTF-8";
  }
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(default_timezone_string, &timezone));

  return CastStringToTimestamp(format_string, timestamp_string, timezone,
                               current_timestamp, timestamp);
}

absl::Status CastStringToTimestamp(absl::string_view format_string,
                                   absl::string_view timestamp_string,
                                   const absl::TimeZone default_timezone,
                                   const absl::Time current_timestamp,
                                   absl::Time* timestamp) {
  if (!IsWellFormedUTF8(format_string) || !IsWellFormedUTF8(format_string)) {
    return MakeEvalError() << "Input string is not valid UTF-8";
  }
  ZETASQL_ASSIGN_OR_RETURN(const std::vector<FormatElement>& format_elements,
                   GetFormatElements(format_string));
  ZETASQL_RETURN_IF_ERROR(ValidateFormatElements(format_elements, {}, "TIMESTAMP"));

  return ParseTimeWithFormatElements(format_elements, timestamp_string,
                                     default_timezone, current_timestamp,
                                     kNanoseconds, timestamp);
}

absl::Status CastStringToTimestamp(absl::string_view format_string,
                                   absl::string_view timestamp_string,
                                   absl::string_view default_timezone_string,
                                   const absl::Time current_timestamp,
                                   absl::Time* timestamp) {
  // Other two input string arguments (<format_string> and <timestamp_string>)
  // are checked in the overload call to CastStringToTimestamp.
  if (!IsWellFormedUTF8(default_timezone_string)) {
    return MakeEvalError() << "Input string is not valid UTF-8";
  }
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(default_timezone_string, &timezone));

  return CastStringToTimestamp(format_string, timestamp_string, timezone,
                               current_timestamp, timestamp);
}

absl::Status ValidateFormatStringForParsing(absl::string_view format_string,
                                            zetasql::TypeKind out_type) {
  if (!IsWellFormedUTF8(format_string)) {
    return MakeEvalError() << "Input string is not valid UTF-8";
  }
  ZETASQL_ASSIGN_OR_RETURN(const std::vector<FormatElement>& format_elements,
                   GetFormatElements(format_string));
  // TODO: Add support for other output types.
  if (out_type == TYPE_TIMESTAMP) {
    return ValidateFormatElements(format_elements, {}, "TIMESTAMP");
  } else {
    return MakeSqlError() << "Unsupported output type for validation";
  }
}

absl::Status ValidateFormatStringForFormatting(absl::string_view format_string,
                                               zetasql::TypeKind out_type) {
  if (!IsWellFormedUTF8(format_string)) {
    return MakeEvalError() << "Format string is not a valid UTF-8 string.";
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<cast_date_time_internal::FormatElement> format_elements,
      cast_date_time_internal::GetFormatElements(format_string));
  switch (out_type) {
    case TYPE_DATE:
      return ValidateDateFormatElementsForFormatting(format_elements);
    case TYPE_DATETIME:
      return ValidateDatetimeFormatElementsForFormatting(format_elements);
    case TYPE_TIME:
      return ValidateTimeFormatElementsForFormatting(format_elements);
    case TYPE_TIMESTAMP:
      return absl::OkStatus();
    default:
      return MakeSqlError() << "Unsupported output type for validation";
  }
}

absl::Status CastFormatDateToString(absl::string_view format_string,
                                    int32_t date, std::string* out) {
  if (!IsWellFormedUTF8(format_string)) {
    return MakeEvalError() << "Format string is not a valid UTF-8 string.";
  }
  if (!IsValidDate(date)) {
    return MakeEvalError() << "Invalid date value: " << date;
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<cast_date_time_internal::FormatElement> format_elements,
      cast_date_time_internal::GetFormatElements(format_string));
  ZETASQL_RETURN_IF_ERROR(ValidateDateFormatElementsForFormatting(format_elements));
  // Treats it as a timestamp at midnight on that date and invokes the
  // format_timestamp function.
  int64_t date_timestamp = static_cast<int64_t>(date) * kNaiveNumMicrosPerDay;
  ZETASQL_ASSIGN_OR_RETURN(
      *out, cast_date_time_internal::FromCastFormatTimestampToStringInternal(
                format_elements, MakeTime(date_timestamp, kMicroseconds),
                absl::UTCTimeZone()));
  return absl::OkStatus();
}

absl::Status CastFormatDatetimeToString(absl::string_view format_string,
                                        const DatetimeValue& datetime,
                                        std::string* out) {
  if (!IsWellFormedUTF8(format_string)) {
    return MakeEvalError() << "Format string is not a valid UTF-8 string.";
  }
  if (!datetime.IsValid()) {
    return MakeEvalError() << "Invalid datetime value: "
                           << datetime.DebugString();
  }
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<cast_date_time_internal::FormatElement> format_elements,
      cast_date_time_internal::GetFormatElements(format_string));
  ZETASQL_RETURN_IF_ERROR(ValidateDatetimeFormatElementsForFormatting(format_elements));
  absl::Time datetime_in_utc =
      absl::UTCTimeZone().At(datetime.ConvertToCivilSecond()).pre;
  datetime_in_utc += absl::Nanoseconds(datetime.Nanoseconds());

  ZETASQL_ASSIGN_OR_RETURN(
      *out, cast_date_time_internal::FromCastFormatTimestampToStringInternal(
                format_elements, datetime_in_utc, absl::UTCTimeZone()));
  return absl::OkStatus();
}

absl::Status CastFormatTimeToString(absl::string_view format_string,
                                    const TimeValue& time, std::string* out) {
  if (!IsWellFormedUTF8(format_string)) {
    return MakeEvalError() << "Format string is not a valid UTF-8 string.";
  }
  if (!time.IsValid()) {
    return MakeEvalError() << "Invalid time value: " << time.DebugString();
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<cast_date_time_internal::FormatElement> format_elements,
      cast_date_time_internal::GetFormatElements(format_string));
  ZETASQL_RETURN_IF_ERROR(ValidateTimeFormatElementsForFormatting(format_elements));

  absl::Time time_in_epoch_day =
      absl::UTCTimeZone()
          .At(absl::CivilSecond(1970, 1, 1, time.Hour(), time.Minute(),
                                time.Second()))
          .pre;
  time_in_epoch_day += absl::Nanoseconds(time.Nanoseconds());

  ZETASQL_ASSIGN_OR_RETURN(
      *out, cast_date_time_internal::FromCastFormatTimestampToStringInternal(
                format_elements, time_in_epoch_day, absl::UTCTimeZone()));
  return absl::OkStatus();
}

absl::Status CastFormatTimestampToString(absl::string_view format_string,
                                         int64_t timestamp_micros,
                                         absl::TimeZone timezone,
                                         std::string* out) {
  if (!IsWellFormedUTF8(format_string)) {
    return MakeEvalError() << "Format string is not a valid UTF-8 string.";
  }
  return CastFormatTimestampToString(
      format_string, MakeTime(timestamp_micros, kMicroseconds), timezone, out);
}

absl::Status CastFormatTimestampToString(absl::string_view format_string,
                                         int64_t timestamp_micros,
                                         absl::string_view timezone_string,
                                         std::string* out) {
  if (!IsWellFormedUTF8(format_string)) {
    return MakeEvalError() << "Format string is not a valid UTF-8 string.";
  }
  if (!IsWellFormedUTF8(timezone_string)) {
    return MakeEvalError() << "Timezone string is not a valid UTF-8 string.";
  }
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return CastFormatTimestampToString(format_string, timestamp_micros, timezone,
                                     out);
}

absl::Status CastFormatTimestampToString(absl::string_view format_string,
                                         absl::Time timestamp,
                                         absl::string_view timezone_string,
                                         std::string* out) {
  if (!IsWellFormedUTF8(format_string)) {
    return MakeEvalError() << "Format string is not a valid UTF-8 string.";
  }
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));

  return CastFormatTimestampToString(format_string, timestamp, timezone, out);
}

absl::Status CastFormatTimestampToString(absl::string_view format_string,
                                         absl::Time timestamp,
                                         absl::TimeZone timezone,
                                         std::string* out) {
  if (!IsWellFormedUTF8(format_string)) {
    return MakeEvalError() << "Format string is not a valid UTF-8 string.";
  }
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<cast_date_time_internal::FormatElement> format_elements,
      cast_date_time_internal::GetFormatElements(format_string));
  ZETASQL_ASSIGN_OR_RETURN(
      *out, cast_date_time_internal::FromCastFormatTimestampToStringInternal(
                format_elements, timestamp, timezone));
  return absl::OkStatus();
}

}  // namespace functions
}  // namespace zetasql
