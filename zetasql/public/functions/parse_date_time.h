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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_PARSE_DATE_TIME_H_
#define ZETASQL_PUBLIC_FUNCTIONS_PARSE_DATE_TIME_H_

#include <cstdint>
#include <string>

#include "zetasql/public/civil_time.h"
#include "zetasql/public/functions/date_time_util.h"
#include <cstdint>
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace functions {

// Parses an input <timestamp_string> with the given input <format_string>,
// and produces the appropriate timestamp as output.  Timestamp parts that are
// unspecified in the format are derived from '1970-01-01 00:00:00.000000'
// at the <default_timezone>.  Produces <timestamp> at microseconds precision,
// and returns an error if the resulting timestamp is not in the
// ZetaSQL valid range.
//
// Requires that the string_view arguments are UTF8.  The last byte of the
// string_view can be (but is not required to be) a null-byte.
//
// The supported format elements and their semantics are defined in:
//   (broken link)
//
// <parse_version_2> indicates whether or not new format elements are
// supported:
//    %G - ISO year
//    %g - ISO year (00-99)
//    %V - ISO week number 01-53
//    %J - ISO day of year (001-364 or 001-371)
//    %U - week number (00-53, Sunday is the first day of the week)
//    %W - week number (00-53, Monday is the first day of the week)
//    %j - day of year (001-365 or 001-366)
//    %u - weekday 1-7 (Monday is the first day of the week)
//    %w - weekday 0-6 (Sunday is hte first day of the week)
//    %A - full weekday name
//    %a - abbreviated weekday name
//
// Note: The methods in cast_date_time.h, such as CastStringToTimestamp use a
// different format style.
absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    const absl::TimeZone default_timezone,
                                    bool parse_version2, int64_t* timestamp);
ABSL_DEPRECATED("Inline me!")
absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    const absl::TimeZone default_timezone,
                                    int64_t* timestamp,
                                    bool parse_version2 = false);

// Invokes MakeTimeZone() on <default_timezone_string> and invokes the prior
// function.  Returns error status if <default_timezone_string> is invalid
// or conversion fails.
absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    absl::string_view default_timezone_string,
                                    bool parse_version2, int64_t* timestamp);
ABSL_DEPRECATED("Inline me!")
absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    absl::string_view default_timezone_string,
                                    int64_t* timestamp,
                                    bool parse_version2 = false);

// The 2 functions below support nanoseconds precision.
absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    absl::string_view default_timezone_string,
                                    bool parse_version2, absl::Time* timestamp);
ABSL_DEPRECATED("Inline me!")
absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    absl::string_view default_timezone_string,
                                    absl::Time* timestamp,
                                    bool parse_version2 = false);

absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    const absl::TimeZone default_timezone,
                                    bool parse_version2, absl::Time* timestamp);
ABSL_DEPRECATED("Inline me!")
absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    const absl::TimeZone default_timezone,
                                    absl::Time* timestamp,
                                    bool parse_version2 = false);

// Parses an input <date_string> with the given input <format_string>,
// and produces the appropriate date as output. Date parts that are
// unspecified in the format are derived from '1970-01-01'.
// Returns an error if the <format_string> contains a format char
// unsupported for DATE or the resulting date is not in the ZetaSQL
// valid range.
//
// Requires that the string_view arguments are UTF8.  The last byte of the
// string_view can be (but is not required to be) a null-byte.
absl::Status ParseStringToDate(absl::string_view format_string,
                               absl::string_view date_string,
                               bool parse_version2, int32_t* date);
ABSL_DEPRECATED("Inline me!")
absl::Status ParseStringToDate(absl::string_view format_string,
                               absl::string_view date_string, int32_t* date,
                               bool parse_version2 = false);

// Parses an input <time_string> with the given input <format_string>, and
// produces the appropriate TIME as output. Time parts that are unspecified in
// the format are derived from '00:00:00.000000'.
// Returns an error if the <format_string> contains a format element unsupported
// for TIME or the resulting TIME is not in the range of [00:00:00, 24:00:00).
//
// <scale> is used to specify the maximum precision supported for the format
// element "%E#S", which will be 6 and 9 for micros and nanos, respectively. The
// parsed result value also respects this specified <scale>, which means that
// for the format element "%E*S", while parsing will consume as many numeric
// digits as present, the parsed result will truncate any digits beyond 6 or 9
// for micros and nanos, respectively. The same behavior also applys to the
// <scale> argument for ParseStringToDatetime() below.
//
// Requires that the string_view arguments are UTF8.  The last byte of the
// string_view can be (but is not required to be) a null-byte.
absl::Status ParseStringToTime(absl::string_view format_string,
                               absl::string_view time_string,
                               TimestampScale scale,
                               TimeValue* time);

// Parses an input <datetime_string> with the given input <format_string>, and
// produces the appropriate DATETIME as output. Date and time parts that are
// unspecified in the format are derived from '1970-01-01 00:00:00.000000'.
// Returns an error if the <format_string> contains a format element unsupported
// for DATETIME or the resulting DATETIME is not in the ZetaSQL valid range.
//
// Requires that the string_view arguments are UTF8.  The last byte of the
// string_view can be (but is not required to be) a null-byte.
absl::Status ParseStringToDatetime(absl::string_view format_string,
                                   absl::string_view datetime_string,
                                   TimestampScale scale, bool parse_version2,
                                   DatetimeValue* datetime);
ABSL_DEPRECATED("Inline me!")
absl::Status ParseStringToDatetime(absl::string_view format_string,
                                   absl::string_view datetime_string,
                                   TimestampScale scale,
                                   DatetimeValue* datetime,
                                   bool parse_version2 = false);

}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_PARSE_DATE_TIME_H_
