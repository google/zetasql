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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_CAST_DATE_TIME_H_
#define ZETASQL_PUBLIC_FUNCTIONS_CAST_DATE_TIME_H_

#include <cstdint>
#include <string>

#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/type.pb.h"
#include <cstdint>
#include "absl/time/time.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

// CastStringToTimestamp functions are used for CAST(input AS
// Timestamp FORMAT '...') syntax.
//
// Parses an input <timestamp_string> with the given input <format_string>,
// and produces the appropriate timestamp as output. Timestamp parts that are
// unspecified in the format are derived from 'current_year-current_month-01
// 00:00:00.000000' at the <default_timezone>. Produces <timestamp> at
// microseconds precision, and returns an error if the resulting timestamp is
// not in the ZetaSQL valid range.
//
// Requires that the string_view arguments are UTF8.
//
// WARNING: These functions are still under development and do not yet support
// all the valid format elements.
//
// The supported format elements and their semantics are different from those
// for ParseStringToTimestamp functions and they are defined in:
// (broken link).
absl::Status CastStringToTimestamp(absl::string_view format_string,
                                   absl::string_view timestamp_string,
                                   const absl::TimeZone default_timezone,
                                   const absl::Time current_timestamp,
                                   int64_t* timestamp_micros);

// Invokes MakeTimeZone() on <default_timezone_string> and invokes the prior
// function. Returns error status if <default_timezone_string> is invalid
// or conversion fails.
absl::Status CastStringToTimestamp(absl::string_view format_string,
                                   absl::string_view timestamp_string,
                                   absl::string_view default_timezone_string,
                                   const absl::Time current_timestamp,
                                   int64_t* timestamp_micros);

// The 2 functions below are similar to the above functions but support
// nanoseconds precision.
absl::Status CastStringToTimestamp(absl::string_view format_string,
                                   absl::string_view timestamp_string,
                                   const absl::TimeZone default_timezone,
                                   const absl::Time current_timestamp,
                                   absl::Time* timestamp);

absl::Status CastStringToTimestamp(absl::string_view format_string,
                                   absl::string_view timestamp_string,
                                   absl::string_view default_timezone_string,
                                   const absl::Time current_timestamp,
                                   absl::Time* timestamp);

// Perform validations on the <format_string> that is used for parse the given
// <out_type> according to specifications at (broken link).
absl::Status ValidateFormatStringForParsing(absl::string_view format_string,
                                            zetasql::TypeKind out_type);

// Perform validations on the <format_string> that is used to format the given
// <out_type>, according to specifications at (broken link).
absl::Status ValidateFormatStringForFormatting(absl::string_view format_string,
                                               zetasql::TypeKind out_type);

// Populates <out> using the <format_string> following the formatting rules from
// (broken link).
//
// Assumes <timestamp> is the number of microseconds from
// 1970-01-01 UTC.
// Returns error status if conversion fails.
//
// <format_string> must be a valid utf-8 string, else this and all the other
// CastFormat functions will fail.
//
// Note, this method is not locale aware and generally formats in an en-US
// style. For example, months and days of week will use the en-US names.
absl::Status CastFormatTimestampToString(absl::string_view format_string,
                                         int64_t timestamp_micros,
                                         absl::TimeZone timezone,
                                         std::string* out);

// Invokes MakeTimeZone() on <timezone_string> and invokes the prior function.
// Returns error status if <timezone_string> is invalid or conversion fails.
absl::Status CastFormatTimestampToString(absl::string_view format_string,
                                         int64_t timestamp_micros,
                                         absl::string_view timezone_string,
                                         std::string* out);

absl::Status CastFormatTimestampToString(absl::string_view format_string,
                                         absl::Time timestamp,
                                         absl::TimeZone timezone,
                                         std::string* out);

absl::Status CastFormatTimestampToString(absl::string_view format_string,
                                         absl::Time timestamp,
                                         absl::string_view timezone_string,
                                         std::string* out);

// Populates <out> using the <format_str> following the formatting rules from
// (broken link).
// Assumes <date>: number of days since the epoch (1970-01-01)
//
// Does not allow timezone or time format elements.
// Returns error status if the conversion fails.
absl::Status CastFormatDateToString(absl::string_view format_string,
                                    int32_t date, std::string* out);

// Populates <out> using the <format_str> following the formatting rules from
// (broken link).
//
// Does not allow timezone related format elements.
// Returns error status if the conversion fails.
absl::Status CastFormatDatetimeToString(absl::string_view format_string,
                                        const DatetimeValue& datetime,
                                        std::string* out);

// Populates <out> using the <format_string> as defined by absl::FormatTime() in
// base/time.h. Returns error status if conversion fails.
//
// Does not allow timezone or date format elements.
absl::Status CastFormatTimeToString(absl::string_view format_string,
                                    const TimeValue& time, std::string* out);

namespace cast_date_time_internal {
enum class FormatElementType {
  kFormatElementTypeUnspecified = 0,
  kLiteral,
  kDoubleQuotedLiteral,
  kWhitespace,
  kYear,
  kMonth,
  kDay,
  kHour,
  kMinute,
  kSecond,
  kMeridianIndicator,
  kTimeZone,
  kCentury,
  kQuarter,
  kWeek,
  kEraIndicator,
  kMisc,
};

struct FormatElement {
  FormatElementType type;
  // In case the format element is double quoted literal, the original string
  // will be unquoted and unescaped, e.g. if the user input format string is
  // "\"abc\\\\\"", the original_str for this format element would be "abc\\"
  std::string original_str;
  FormatElement() = default;
  FormatElement(FormatElementType type_in, absl::string_view original_str_in)
      : type(type_in), original_str(original_str_in) {}
  std::string DebugString() const;
};

zetasql_base::StatusOr<std::vector<FormatElement>> GetFormatElements(
    absl::string_view format_str);

}  // namespace cast_date_time_internal

}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_CAST_DATE_TIME_H_
