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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_PARSE_DATE_TIME_H_
#define ZETASQL_PUBLIC_FUNCTIONS_PARSE_DATE_TIME_H_

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
zetasql_base::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    const absl::TimeZone default_timezone,
                                    int64_t* timestamp);

// Invokes MakeTimeZone() on <default_timezone_string> and invokes the prior
// function.  Returns error status if <default_timezone_string> is invalid
// or conversion fails.
zetasql_base::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    absl::string_view default_timezone_string,
                                    int64_t* timestamp);

// The 2 functions below support nanoseconds precision.
zetasql_base::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    absl::string_view default_timezone_string,
                                    absl::Time* timestamp);

zetasql_base::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    const absl::TimeZone default_timezone,
                                    absl::Time* timestamp);

// Parses an input <date_string> with the given input <format_string>,
// and produces the appropriate date as output. Date parts that are
// unspecified in the format are derived from '1970-01-01'.
// Returns an error if the <format_string> contains a format char
// unsupported for DATE or the resulting date is not in the ZetaSQL
// valid range.
//
// Requires that the string_view arguments are UTF8.  The last byte of the
// string_view can be (but is not required to be) a null-byte.
zetasql_base::Status ParseStringToDate(absl::string_view format_string,
                               absl::string_view date_string,
                               int32_t* date);

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
zetasql_base::Status ParseStringToTime(absl::string_view format_string,
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
zetasql_base::Status ParseStringToDatetime(absl::string_view format_string,
                                   absl::string_view datetime_string,
                                   TimestampScale scale,
                                   DatetimeValue* datetime);

}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_PARSE_DATE_TIME_H_
