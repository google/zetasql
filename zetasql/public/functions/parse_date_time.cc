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

#include "zetasql/public/functions/parse_date_time.h"

#include <string.h>
#include <time.h>

#include <cctype>
#include <cstdint>
#include <limits>
#include <string>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/functions/parse_date_time_utils.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.h"
#include <cstdint>
#include "absl/base/optimization.h"
#include "absl/strings/str_format.h"
#include "absl/time/time.h"
#include "zetasql/base/mathutil.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

// This ParseTime() code was initially copied from base/time/format.cc.
// It has been modified to provide ZetaSQL defined behavior for
// the SQL PARSE_TIMESTAMP() function.

namespace zetasql {
namespace functions {

namespace {

using parse_date_time_utils::ConvertTimeToTimestamp;
using parse_date_time_utils::ParseInt;
using parse_date_time_utils::ParseSubSeconds;

constexpr int64_t kNumMillisPerSecond = 1000;

std::string TimeZoneOffsetToString(int minutes_offset) {
  const int timezone_hour = zetasql_base::MathUtil::Abs(minutes_offset) / 60;
  const int timezone_minute = zetasql_base::MathUtil::Abs(minutes_offset) % 60;
  std::string offset_string;
  absl::StrAppendFormat(&offset_string, "%c%02d:%02d",
                        (minutes_offset < 0 ? '-' : '+'), timezone_hour,
                        timezone_minute);
  return offset_string;
}

// Verify that the <data> is exactly equal to <chr> and increment past it;
// returns null otherwise.
static const char* ExpectChar(const char* data, const char* end_of_data,
                              char chr) {
  if (data != nullptr && data != end_of_data && *data == chr) {
    return data + 1;
  } else {
    return nullptr;
  }
}

// Returns an offset in minutes.
static const char* ParseOffset(const char* dp, const char* end_of_data,
                               char sep, int* offset) {
  if (dp != nullptr) {
    const char sign = *dp++;
    if (sign == '+' || sign == '-') {
      int hours = 0;
      const char* ap = ParseInt(dp, end_of_data, 2, 0, 23, &hours);
      if (ap != nullptr && ap - dp == 2) {
        dp = ap;
        if (sep != '\0' && ap < end_of_data && *ap == sep) ++ap;
        int minutes = 0;
        const char* bp = ParseInt(ap, end_of_data, 2, 0, 59, &minutes);
        if (bp != nullptr && bp - ap == 2) dp = bp;
        *offset = hours * 60 + minutes;
        if (sign == '-') *offset = -*offset;
      } else {
        dp = nullptr;
      }
    } else {
      dp = nullptr;
    }
  }
  return dp;
}

static const char* ParseZone(const char* dp, std::string* zone,
                             const char* end) {
  zone->clear();
  if (dp != nullptr) {
    while (dp < end && !absl::ascii_isspace(*dp)) zone->push_back(*dp++);
    if (zone->empty()) dp = nullptr;
  }
  return dp;
}

// Only parses up to <max_digits>, and ignores digits beyond <scale>.  Stops
// parsing if a non-digit character is encountered.
static const char* ParseSubSecondsIfStartingWithPoint(
    const char* dp, const char* end_of_data, int max_digits,
    TimestampScale scale, absl::Duration* subseconds) {
  if (dp == nullptr) {
    return nullptr;
  } else if (end_of_data > dp && *dp == '.') {
    // Start to parse the integer part from dp + 1
    return ParseSubSeconds(dp + 1, end_of_data, max_digits, scale, subseconds);
  }

  return dp;
}

// Parses a string into a struct tm using strptime(3).
static const char* ParseTM(const char* dp, const char* fmt, struct tm* tm) {
  if (dp != nullptr) {
    dp = strptime(dp, fmt, tm);
  }
  return dp;
}

// Consume any amount of whitespace (including none).
static const char* ConsumeWhitespace(const char* data,
                                     const char* end_of_data) {
  if (data == nullptr) {
    return nullptr;
  }
  // TODO: Handle UTF and unicode white space characters as well.
  while (data != end_of_data && absl::ascii_isspace(*data)) ++data;
  return data;
}

static const char* HandleTwelveHourFormatters(const char* data,
                                              const char* end_of_data,
                                              struct tm& tm,
                                              bool& twelve_hour) {
  // In format, these differ:
  // I: The hour (12-hour clock) as a decimal number (01-12).
  // l: The hour (12-hour clock) as a decimal number (1-12);
  //    single digits are preceded by a single space.
  // O: (as I)
  // But on parse, we treat them the same, additionally, we consume any
  // amount of whitespace prior to digits.
  int hour_number;
  data = ConsumeWhitespace(data, end_of_data);
  data = ParseInt(data, end_of_data, 2, 1, 12, &hour_number);
  if (data != nullptr) {
    tm.tm_hour = hour_number % 12;  // '12' becomes zero
    twelve_hour = true;
  }
  return data;
}

static const char* HandleMeridianFormatters(const char* data,
                                            const char* end_of_data,
                                            bool& afternoon) {
  if (data == nullptr || end_of_data - data < 2) {
    return nullptr;
  }
  if ((data[0] == 'P' || data[0] == 'p') &&
      (data[1] == 'M' || data[1] == 'm')) {
    afternoon = true;
  } else if ((data[0] == 'A' || data[0] == 'a') &&
             (data[1] == 'M' || data[1] == 'm')) {
    afternoon = false;

  } else {
    return nullptr;
  }
  return data + 2;
}

struct ParseElementInfo {
  std::string DebugString() const {
    std::string out;
    absl::StrAppend(&out, "{format: ", std::string(&fmt, &fmt + 1));
    absl::StrAppend(&out, ", data string: '", std::string(data, end_of_data),
                    "'");
    absl::StrAppend(&out, ", position: ", position, "}");
    return out;
  }

  char fmt;
  const char* data;
  const char* end_of_data;
  int position;
};

struct DateParseContext {
  std::string DebugString() const {
    std::string out;
    absl::StrAppend(&out, "\n{");
    absl::StrAppend(
        &out, "\n  last_year_element_position: ", last_year_element_position);
    absl::StrAppend(
        &out, "\n  last_month_element_position: ", last_month_element_position);
    absl::StrAppend(
        &out, "\n  last_mday_element_position: ", last_mday_element_position);
    absl::StrAppend(
        &out, "\n  non_iso_date_part_present: ", non_iso_date_part_present);
    absl::StrAppend(&out, "\n  iso_year_present: ", iso_year_present);
    absl::StrAppend(&out, "\n  iso_week_present: ", iso_week_present);
    absl::StrAppend(&out, "\n  iso_dayofyear_present: ", iso_dayofyear_present);
    absl::StrAppend(&out, "\n  non_iso_week_present: ", non_iso_week_present);
    for (int idx = 0; idx < elements.size(); ++idx) {
      absl::StrAppend(&out, "\n  elements[", idx,
                      "]: ", elements[idx].DebugString());
    }
    absl::StrAppend(&out, "\n}\n");
    return out;
  }

  // Indicates the position of the last format element that impacts the
  // specified part.  *Not* an index into <elements>.
  int last_year_element_position = -1;
  int last_month_element_position = -1;
  int last_mday_element_position = -1;

  bool non_iso_date_part_present = false;
  bool iso_year_present = false;
  bool iso_week_present = false;
  bool iso_dayofyear_present = false;
  bool non_iso_week_present = false;

  // Only includes new format elements enabled via the 'parse_version2'
  // flag.
  std::vector<ParseElementInfo> elements;
};

// Takes a list of ISO format elements and canonicalizes it.  Must be called
// with a valid ISO year.  Canonicalizes the output <date_parse_context>
// by using the 'rightmost' of the day-of-year and week parts, and unsetting
// weekday if week is not used.
//
// Note that redundant elements have already been removed from the
// DateParseContext list before invoking this method.
//
absl::Status CanonicalizeISODateParseContext(
    int64_t iso_year_idx, int64_t iso_week_idx, int64_t iso_dayofyear_idx,
    int64_t weekday_idx, DateParseContext* date_parse_context) {
  ZETASQL_RET_CHECK(!date_parse_context->non_iso_date_part_present);
  ZETASQL_RET_CHECK_GE(iso_year_idx, 0);

  // We might have an overlap between day of year (%J) and week (%V)
  // and/or dayofweek (%A, %a, %u, %w).
  //
  // The highest idx between day of year and week 'wins'.  For example:
  //
  // %J%V%u -> %V%u
  // %J%V   -> %V
  // %u%J%V -> %u%V
  // %V%J%u -> %J
  // %V%J   -> %J
  // %V%u%J -> %J
  //
  if (iso_dayofyear_idx >= 0) {
    // If day-of-year is after week, or weekday is unspecified, then
    // day-of-year wins.
    if (iso_dayofyear_idx > iso_week_idx || weekday_idx == -1) {
      // Day of year wins.
      weekday_idx = -1;
      iso_week_idx = -1;
    } else {
      // Day of year loses.
      iso_dayofyear_idx = -1;
    }
  } else if (iso_week_idx == -1) {
    // Week is unspecified so weekday is ignored.
    weekday_idx = -1;
  }

  // Create the canonicalized DateParseContext.
  DateParseContext canonicalized_date_parse_context;

  canonicalized_date_parse_context.elements.push_back(
      date_parse_context->elements[iso_year_idx]);
  canonicalized_date_parse_context.iso_year_present = true;

  // Set WEEK and DAYOFWEEK if needed.
  if (iso_week_idx >= 0) {
    // ISO week is mutually exclusive with day of year.
    ZETASQL_RET_CHECK_LT(iso_dayofyear_idx, 0);
    canonicalized_date_parse_context.elements.push_back(
        date_parse_context->elements[iso_week_idx]);
    canonicalized_date_parse_context.iso_week_present = true;
    if (weekday_idx >= 0) {
      canonicalized_date_parse_context.elements.push_back(
          date_parse_context->elements[weekday_idx]);
    }
  }

  // Set DAYOFYEAR if needed.
  if (iso_dayofyear_idx >= 0) {
    // ISO week is mutually exclusive with day of year.
    ZETASQL_RET_CHECK_LT(iso_week_idx, 0);
    canonicalized_date_parse_context.elements.push_back(
        date_parse_context->elements[iso_dayofyear_idx]);
    canonicalized_date_parse_context.iso_dayofyear_present = true;
  }
  *date_parse_context = canonicalized_date_parse_context;

  return absl::OkStatus();
}

absl::Status CanonicalizeNonISODateParseContext(
    int64_t week_idx, int64_t dayofyear_idx, int64_t weekday_idx,
    DateParseContext* date_parse_context) {
  // We might still have an overlap between day of year (%j) and
  // week (%U, %W) and/or weekday (%A, %a, %u, %w).
  //
  // The highest idx between day of year (%j) and week (%U or %W) 'wins'.
  // The location of weekday is irrelevant to determining which wins.  If
  // %j wins then weekday is ignored.
  //
  // For example:
  //
  // %j%U%u -> %U%u
  // %j%U   -> %U
  // %U%j%u -> %j
  // %u%j%U -> %u%U
  // %U%j   -> %j
  // %U%u%j -> %j
  //
  if (dayofyear_idx >= 0) {
    // If day-of-year is after week, or weekday is unspecified, then
    // day-of-year wins.
    if (dayofyear_idx > week_idx || weekday_idx == -1) {
      // day-of-year wins.
      weekday_idx = -1;
      week_idx = -1;
    } else {
      // day-of-year loses.
      dayofyear_idx = -1;
    }
  } else if (week_idx == -1) {
    // Week is unspecified so weekday is ignored.
    weekday_idx = -1;
  }

  // Create the canonicalized DateParseContext.
  DateParseContext canonicalized_date_parse_context;
  canonicalized_date_parse_context.last_year_element_position =
      date_parse_context->last_year_element_position;
  canonicalized_date_parse_context.last_month_element_position =
      date_parse_context->last_month_element_position;
  canonicalized_date_parse_context.last_mday_element_position =
      date_parse_context->last_mday_element_position;

  // Set WEEK and optionally DAYOFWEEK.
  if (week_idx >= 0) {
    // Week is mutually exclusive with day of year.
    ZETASQL_RET_CHECK_LT(dayofyear_idx, 0);
    canonicalized_date_parse_context.elements.push_back(
        date_parse_context->elements[week_idx]);
    canonicalized_date_parse_context.non_iso_week_present = true;
    canonicalized_date_parse_context.non_iso_date_part_present = true;
    if (weekday_idx >= 0) {
      canonicalized_date_parse_context.elements.push_back(
          date_parse_context->elements[weekday_idx]);
    }
  }

  // Set DAYOFYEAR.
  if (dayofyear_idx >= 0) {
    // Day of year is mutually exclusive with week/weekday.
    ZETASQL_RET_CHECK_LT(week_idx, 0);
    canonicalized_date_parse_context.elements.push_back(
        date_parse_context->elements[dayofyear_idx]);
    canonicalized_date_parse_context.non_iso_date_part_present = true;
  }
  *date_parse_context = canonicalized_date_parse_context;

  return absl::OkStatus();
}

// Eliminates redundant and non-contributing parse elements from the
// <date_parse_context>, reflecting the following rules:
//   1) ignore ISO parts if non-ISO parts are present
//   2) ignore day-of-week parts if a week part is not present
//   3) ignore ISO WEEK if ISO YEAR is not present
//   4) ignore the element if its position is not the rightmost of all
//      related year/month/day element positions.
//
// This method canonicalizes the DateParseContext by eliminating redundant
// and non-contributing parse elements by imposing the previous rules.
absl::Status CanonicalizeDateParseContext(
    DateParseContext* date_parse_context) {
  if (date_parse_context->elements.empty()) {
    // If there are no entries then there is nothing to canonicalize.
    // Note that if there is 1 entry we still might need to canonicalize it,
    // for example in the case where the only element present is day of week,
    // which should be removed/ignored.
    return absl::OkStatus();
  }
  int64_t weekday_idx = -1;
  int64_t iso_year_idx = -1;
  int64_t non_iso_dayofyear_idx = -1;
  int64_t non_iso_week_idx = -1;
  int64_t iso_week_idx = -1;
  int64_t iso_dayofyear_idx = -1;

  // Loop through the elements and record the last position of each of
  // the elements.
  for (int64_t idx = 0; idx < date_parse_context->elements.size(); ++idx) {
    switch (date_parse_context->elements[idx].fmt) {
      case 'A':  // Full weekday name
      case 'a':  // Abbreviated weekday name
      case 'u':  // weekday number 1-7, starting Monday
      case 'w':  // weekday number 0-6, starting Sunday
        weekday_idx = idx;
        break;
      case 'G':  // ISO 8601 year with century, e.g., 2019
      case 'g':  // ISO 8601 year without century, e.g., 19
        iso_year_idx = idx;
        break;
      case 'J':  // ISO day of year
        // We ignore ISO elements if non-ISO elements are present.
        iso_dayofyear_idx = idx;
        break;
      case 'j':  // Non-ISO day of year
        non_iso_dayofyear_idx = idx;
        break;
      case 'U':  // Non-ISO week number of the year (starting Sunday) 00-53
      case 'W':  // Non-ISO week number of the year (starting Monday) 00-53
        non_iso_week_idx = idx;
        break;
      case 'V':  // ISO 8601 week number of the ISO YEAR
        // We ignore ISO elements if non-ISO elements are present.
        iso_week_idx = idx;
        break;
      default:
        ZETASQL_RET_CHECK_FAIL() << "Unexpected format element: '"
                         << date_parse_context->elements[idx].fmt << "'";
    }
  }

  // Ignore ISO parts if non-ISO parts are present.
  if (date_parse_context->non_iso_date_part_present) {
    iso_year_idx = -1;
    iso_week_idx = -1;
    iso_dayofyear_idx = -1;
  }

  if (iso_year_idx >= 0) {
    // We have an ISO date.
    return CanonicalizeISODateParseContext(iso_year_idx, iso_week_idx,
                                           iso_dayofyear_idx, weekday_idx,
                                           date_parse_context);
  }

  // Otherise we have a non-ISO date.
  return CanonicalizeNonISODateParseContext(
      non_iso_week_idx, non_iso_dayofyear_idx, weekday_idx, date_parse_context);
}

// Returns <weekday>, which is 1-based day of week starting Monday.
absl::Status ParseWeekdayFromElement(const ParseElementInfo& weekday_element,
                                     int* weekday) {
  ZETASQL_RET_CHECK(weekday_element.fmt == 'A' || weekday_element.fmt == 'a' ||
            weekday_element.fmt == 'u' || weekday_element.fmt == 'w')
      << "format_element: " << weekday_element.fmt;

  // Use strptime to figure out the day of week.  Strings must be null
  // terminated, so we construct such strings here.
  // TODO: Replace call to strptime with something else, either
  // custom or using some other equivalent library.
  const std::string data_copy_str(
      weekday_element.data, weekday_element.end_of_data - weekday_element.data);
  const std::string fmt_copy_str =
      absl::StrCat("%", std::string(&weekday_element.fmt, 1));

  // Use ParseTM (strptime) to parse the day of the week.
  struct tm parsed_tm;
  const char* dp =
      ParseTM(data_copy_str.c_str(), fmt_copy_str.c_str(), &parsed_tm);
  // If ParseTM returns nullptr, that indicates an error.
  ZETASQL_RET_CHECK_NE(dp, nullptr)
      << "\nfmt: " << weekday_element.fmt << "\ndata: " << weekday_element.data
      << "\nend_of_data: " << weekday_element.end_of_data
      << "\ndata_copy_str: '" << data_copy_str << "'";

  // parsed_tm.tm_wday is a 1-based day of week, starting Monday.
  *weekday = parsed_tm.tm_wday;

  return absl::OkStatus();
}

// This helper currently assumes that the week value will parse correctly.
// This is currently enforced in the main loop, which already parses the week
// number between 0 and 53.  TODO: Memoize the week value during
// the parsing in the main loop, and remove the need to reparse and remove
// this method.
static absl::Status ParseWeek(const ParseElementInfo& week_element, int* week,
                              absl::Weekday* week_start_day) {
  const char* data = nullptr;
  // Week number of the year (0-53)
  data = ParseInt(week_element.data, week_element.end_of_data, 2, 0, 53, week);
  ZETASQL_RET_CHECK_NE(data, nullptr);

  if (week_element.fmt == 'U') {
    *week_start_day = absl::Weekday::sunday;
  } else if (week_element.fmt == 'W') {
    *week_start_day = absl::Weekday::monday;
  } else {
    ZETASQL_RET_CHECK_FAIL() << "Unexpected week parse element %" << week_element.fmt;
  }
  return absl::OkStatus();
}

// This helper currently assumes that the dayofyear value will parse correctly.
// This is currently enforced in the main loop, which already parses the
// dayofyear.  TODO: Memoize the dayofyear value during parsing in
// the main loop, and remove the need to reparse and remove this method.
static absl::Status ParseDayOfYear(const ParseElementInfo& dayofyear_element,
                                   int max_days_in_year, int* dayofyear) {
  const char* data =
      ParseInt(dayofyear_element.data, dayofyear_element.end_of_data, 3, 1,
               max_days_in_year, dayofyear);
  ZETASQL_RET_CHECK_NE(data, nullptr);
  return absl::OkStatus();
}

// This helper currently assumes that the ISO year value will parse correctly.
// This is currently enforced in the main loop, which already parses the
// ISO year.  TODO: Memoize the ISO year value during parsing in
// the main loop, and remove the need to reparse and remove this method.
static absl::Status ParseISOYear(const ParseElementInfo& year_element,
                                 int* iso_year) {
  if (year_element.fmt == 'G') {
    // For the call into ParseInt, we are passing '20' to indicate the number
    // of digits to parse, which is more than will fit into an int64_t. Note
    // that we're actually limiting the range of valid values from [0-99999]
    // anyway though, so the ability to parse a large number of digits is not
    // really needed.
    const char* data = ParseInt(year_element.data, year_element.end_of_data, 20,
                                0, 99999, iso_year);
    ZETASQL_RET_CHECK_NE(data, nullptr);
    return absl::OkStatus();
  }

  if (year_element.fmt == 'g') {
    const char* data = ParseInt(year_element.data, year_element.end_of_data, 2,
                                0, 99, iso_year);
    ZETASQL_RET_CHECK_NE(data, nullptr);

    // We only have the last two digits of the year, so we must determine
    // what the first two (millenia/century) digits are.  We mirror the
    // behavior of the two-digit year %y element - years 00-68 are 2000s,
    // years 69-99 are 1900s.  Note that '%g' is *NOT* sensitive to
    // century (%C) because century is a non-ISO part and if %C is present
    // then %g would be ignored.
    if (*iso_year <= 68) {
      *iso_year += 2000;
    } else {
      *iso_year += 1900;
    }
  } else {
    ZETASQL_RET_CHECK_FAIL() << "unexpected format_element: " << year_element.fmt;
  }
  return absl::OkStatus();
}

// Returns the week number of January 1st of the year.  This week number
// depends on which format element is considered (%U or %W).  Usually
// the first week number is 0, but it is 1 when the first day of the year
// is also the first day of the week (which depends on the format element).
static absl::StatusOr<int64_t> FirstWeekNumberOfYear(int64_t year,
                                                   const char element) {
  const absl::CivilDay january_first(year, 1, 1);
  if (element == 'U') {
    // Sunday is the first day of the week
    if (absl::GetWeekday(january_first) == absl::Weekday::sunday) {
      return 1;
    }
    return 0;
  } else if (element == 'W') {
    // Monday is the first day of the week
    if (absl::GetWeekday(january_first) == absl::Weekday::monday) {
      return 1;
    }
    return 0;
  }
  // We expect to return above.
  ZETASQL_RET_CHECK_FAIL() << "Unexpected format element: " << element;
}

static bool IsLeapYear(int64_t year) {
  if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
    return true;
  }
  return false;
}

// Returns the week number of December 31st of the year.  This week number
// depends on which format element is considered (%U or %W).
static absl::StatusOr<int64_t> LastWeekNumberOfYear(int64_t year,
                                                  const char element) {
  const absl::CivilDay january_first(year, 1, 1);
  const absl::Weekday january_first_weekday = absl::GetWeekday(january_first);
  const int32_t days_in_year = (IsLeapYear(year) ? 366 : 365);
  int32_t number_of_week_zero_days;

  // Compute the number of days in the year while excluding days in week 0.
  // If this number is greater than 52*7=364 then the last week number is 53,
  // otherwise the last week number is 52.
  int first_day_of_week_integer;
  int january_first_integer =
      internal_functions::DayOfWeekIntegerSunToSat1To7(january_first_weekday);
  if (element == 'U') {
    // Set Sunday as value 8, and normalize the January 1st value to [2...8].
    first_day_of_week_integer = 8;
    if (january_first_integer == 1) {
      january_first_integer = 8;
    }
  } else if (element == 'W') {
    // Set Monday as value 9, and normalize the January 1st value to [3...9].
    first_day_of_week_integer = 9;
    if (january_first_integer <= 2) {
      january_first_integer += 7;
    }
  } else {
    ZETASQL_RET_CHECK_FAIL() << "Unexpected format element: " << element;
  }
  // Compute the difference in days between the first day of the week and the
  // January 1st day of week.
  number_of_week_zero_days = first_day_of_week_integer - january_first_integer;
  if (days_in_year - number_of_week_zero_days > 364) {
    return 53;  // There are 53 weeks in the year
  }
  return 52;  // There are 52 weeks in the year
}

// Checks whether or not the specified week number is valid for the given
// year.  Note that validity depends on the format element (%U vs. %W).
static absl::Status CheckWeekNumberValidityForYear(int64_t year, int week_number,
                                                   const char element) {
  ZETASQL_ASSIGN_OR_RETURN(int64_t first_week_number,
                   FirstWeekNumberOfYear(year, element));
  ZETASQL_ASSIGN_OR_RETURN(int64_t last_week_number, LastWeekNumberOfYear(year, element));
  if (week_number >= first_week_number && week_number <= last_week_number) {
    return absl::OkStatus();
  }
  return MakeEvalError() << "Week number " << week_number
                         << " is invalid for year " << year;
}

// Returns the number of days in the given ISO year.
static int64_t NumberOfDaysInISOYear(int64_t iso_year) {
  // Formula as per wikipedia:
  const int64_t p =
      (iso_year + (iso_year / 4) - (iso_year / 100) + (iso_year / 400)) % 7;
  const int64_t y1 = iso_year - 1;
  const int64_t p1 = (y1 + (y1 / 4) - (y1 / 100) + (y1 / 400)) % 7;
  const int64_t num_weeks = 52 + (p == 4 || p1 == 3 ? 1 : 0);
  return num_weeks * 7;
}

// Handles ISO year (+ optional day of year).
static absl::Status ComputeDateFromISOYearAndDayOfYear(
    const ParseElementInfo& iso_year_element,
    const absl::optional<ParseElementInfo>& dayofyear_element,
    absl::CivilDay* civil_day) {
  int iso_year = -1;
  ZETASQL_RETURN_IF_ERROR(ParseISOYear(iso_year_element, &iso_year));

  // Get the first civil day of the ISO year.
  *civil_day =
      absl::PrevWeekday(absl::CivilDay(iso_year, 1, 5), absl::Weekday::monday);

  if (!dayofyear_element.has_value()) {
    return absl::OkStatus();
  }

  int dayofyear;
  ZETASQL_RETURN_IF_ERROR(ParseDayOfYear(dayofyear_element.value(),
                                 /*max_days_in_year=*/371, &dayofyear));
  // ISO years have either 364 or 371 days, so we need to validate that the
  // ISO day of year is valid for this particular ISO Year.
  if (dayofyear > 364) {
    if (dayofyear > NumberOfDaysInISOYear(iso_year)) {
      return MakeEvalError()
             << "ISO Year " << iso_year << " has "
             << NumberOfDaysInISOYear(iso_year)
             << " days, but the specified day of year was " << dayofyear;
    }
  }

  *civil_day += dayofyear - 1;
  return absl::OkStatus();
}

// Handles ISO year + ISO week (+ optional weekday);
// TODO: Memoize the ISO week value during parsing in the main loop,
// and remove the need to reparse it in this method.
static absl::Status ComputeDateFromISOYearWeekAndWeekday(
    const ParseElementInfo& iso_year_element,
    const ParseElementInfo& week_element,
    const absl::optional<ParseElementInfo>& weekday_element,
    absl::CivilDay* civil_day) {
  int iso_year = -1;
  ZETASQL_RETURN_IF_ERROR(ParseISOYear(iso_year_element, &iso_year));

  int iso_week;
  ZETASQL_RET_CHECK_EQ(week_element.fmt, 'V');
  const char* data = ParseInt(week_element.data, week_element.end_of_data, 2, 1,
                              53, &iso_week);
  ZETASQL_RET_CHECK_NE(data, nullptr);

  // Ensure that the specified ISO week is valid for the given ISO year.
  // Some ISO years have 52 weeks, and some ISO years have 53 weeks.
  if (iso_week == 53 && NumberOfDaysInISOYear(iso_year) < 371) {
    return MakeEvalError() << "Invalid ISO week " << iso_week
                           << " specified for ISO year " << iso_year;
  }

  int weekday = 1;  // 1-based week day number starting Monday
  if (weekday_element.has_value()) {
    ZETASQL_RETURN_IF_ERROR(ParseWeekdayFromElement(weekday_element.value(), &weekday));
  }

  return MakeEvalError() << "ISO parse elements are not supported yet";
  return absl::OkStatus();
}

// Computes a date given a year (and optional day of year). If day of year is
// unspecified then returns the first day of the year. Returns an error if the
// day number is not valid for the given year.
static absl::Status ComputeDateFromYearAndDayOfYear(
    int64_t year, const absl::optional<ParseElementInfo>& dayofyear_element,
    absl::CivilDay* civil_day) {
  *civil_day = absl::CivilDay(year, 1, 1);

  if (!dayofyear_element.has_value()) {
    return absl::OkStatus();
  }

  int dayofyear;
  ZETASQL_RETURN_IF_ERROR(ParseDayOfYear(dayofyear_element.value(),
                                 /*max_days_in_year=*/366, &dayofyear));
  if (dayofyear == 366) {
    if (!IsLeapYear(year)) {
      return MakeEvalError()
             << "Year " << year
             << " has 365 days, but the specified day of year was "
             << dayofyear;
    }
  }
  *civil_day += dayofyear - 1;
  return absl::OkStatus();
}

// Computes a date given a year and week number, and an optional day of the
// week.
static absl::Status ComputeDateFromYearWeekAndWeekday(
    int64_t year, const ParseElementInfo& week_element,
    const absl::optional<ParseElementInfo> weekday_element,
    absl::CivilDay* civil_day) {
  int week = -1;
  absl::Weekday week_start_day = absl::Weekday::sunday;
  ZETASQL_RETURN_IF_ERROR(ParseWeek(week_element, &week, &week_start_day));
  ZETASQL_RET_CHECK_GE(week, 0);
  ZETASQL_RET_CHECK_LE(week, 53);

  ZETASQL_RETURN_IF_ERROR(CheckWeekNumberValidityForYear(year, week, week_element.fmt));

  *civil_day = absl::CivilDay(year, 1, 1);
  // Compute starting date for the first day of the first week of the year.
  // NextWeekday() returns the day that follows the current day, not including
  // the current day, so we find the next <week_start_day> starting from the
  // day before the first day of the year.
  *civil_day = absl::NextWeekday(*civil_day - 1, week_start_day);

  // Compute the number of days offset from the first day of the first week
  // of the year.
  *civil_day += 7 * (week - 1);

  if (weekday_element.has_value()) {
    // The caller should verify that we only pass in weekday if week is present.
    int parsed_weekday;
    ZETASQL_RETURN_IF_ERROR(
        ParseWeekdayFromElement(weekday_element.value(), &parsed_weekday));

    return MakeEvalError() << "Weekday parse elements are not supported yet";
  }

  return absl::OkStatus();
}

static absl::Status ComputeYearMonthDayFromISOParts(
    int64_t* year, int* month, int* mday, DateParseContext* date_parse_context) {
  absl::optional<ParseElementInfo> iso_year_info;
  absl::optional<ParseElementInfo> iso_week_info;
  absl::optional<ParseElementInfo> weekday_info;
  absl::optional<ParseElementInfo> iso_dayofyear_info;

  for (const auto& element : date_parse_context->elements) {
    switch (element.fmt) {
      case 'A':  // Full weekday name
      case 'a':  // Abbreviated weekday name
      case 'u':  // weekday number 1-7, starting Monday
      case 'w':  // weekday number 0-6, starting Sunday
        weekday_info = element;
        break;
      case 'G':  // ISO 8601 year with century, e.g., 2019
      case 'g':  // ISO 8601 year without century, e.g., 19
        iso_year_info = element;
        break;
      case 'J':  // ISO day of year
        iso_dayofyear_info = element;
        break;
      case 'V':  // ISO 8601 week number of the ISO YEAR
        iso_week_info = element;
        break;
      default:
        ZETASQL_RET_CHECK_FAIL() << "Unexpected format element: '" << element.fmt
                         << "'";
    }
  }
  // The canonicalized date parse context should ensure that only one of
  // week or day of year is set, so we validate that here.
  ZETASQL_RET_CHECK(!iso_week_info.has_value() || !iso_dayofyear_info.has_value());

  absl::CivilDay civil_day;
  ZETASQL_RET_CHECK(iso_year_info.has_value());

  // Valid combinations are:
  // 1) ISO week - interpreted as the first day of this ISO week
  // 2) ISO week and day of week
  // 4) ISO day of year
  // 3) ISO year only - interpreted as the first day of the ISO year
  if (iso_week_info.has_value()) {
    ZETASQL_RET_CHECK(!iso_dayofyear_info.has_value());
    // Covers year/week, and year/week/weekday.
    ZETASQL_RETURN_IF_ERROR(ComputeDateFromISOYearWeekAndWeekday(
        iso_year_info.value(), iso_week_info.value(), weekday_info,
        &civil_day));
  } else {
    // Covers year, and year/dayofyear.
    ZETASQL_RET_CHECK(!iso_week_info.has_value());
    ZETASQL_RET_CHECK(!weekday_info.has_value());
    // Compute date from ISO year and day of year (optional).
    ZETASQL_RETURN_IF_ERROR(ComputeDateFromISOYearAndDayOfYear(
        iso_year_info.value(), iso_dayofyear_info, &civil_day));
  }
  *year = civil_day.year();
  *month = civil_day.month();
  *mday = civil_day.day();
  // Verify that the result date is valid.
  if (!IsValidDay(*year, *month, *mday)) {
    return MakeEvalError()
           << "Out-of-range datetime field in parsing function; year: " << *year
           << ", month: " << *month << ", day: " << *mday;
  }
  return absl::OkStatus();
}

static absl::Status ComputeYearMonthDayFromNonISOParts(
    int64_t* year, int* month, int* mday, DateParseContext* date_parse_context) {
  // Compute the non-ISO year/month/day from the canonicalized DateParseContext.
  absl::optional<ParseElementInfo> week_info;
  absl::optional<ParseElementInfo> weekday_info;
  absl::optional<ParseElementInfo> dayofyear_info;

  for (const auto& element : date_parse_context->elements) {
    switch (element.fmt) {
      case 'A':  // Full weekday name
      case 'a':  // Abbreviated weekday name
      case 'u':  // weekday number 1-7, starting Monday
      case 'w':  // weekday number 0-6, starting Sunday
        weekday_info = element;
        break;
      case 'j':  // Non-ISO day of year
        dayofyear_info = element;
        break;
      case 'U':  // Non-ISO week number of the year (starting Sunday) 00-53
      case 'W':  // Non-ISO week number of the year (starting Monday) 00-53
        week_info = element;
        break;
      default:
        ZETASQL_RET_CHECK_FAIL() << "Unexpected format element: '" << element.fmt
                         << "'";
    }
  }

  absl::CivilDay civil_day(*year, *month, *mday);

  // Non-ISO case.  The <year> is used as input to compute the final date.
  //
  // Valid combinations are:
  // 1) week - interpreted as the first day of the week.
  // 2) week and day of week
  // 3) <nothing additional> - interpreted as the first day of the year
  // 4) day of year
  int new_element_position = -1;
  if (week_info.has_value()) {
    ZETASQL_RET_CHECK(!dayofyear_info.has_value());
    ZETASQL_RETURN_IF_ERROR(ComputeDateFromYearWeekAndWeekday(
        *year, week_info.value(), weekday_info, &civil_day));
    new_element_position = week_info.value().position;
  } else {
    ZETASQL_RET_CHECK(!weekday_info.has_value());
    // Compute date from year and day of year (optional).
    ZETASQL_RETURN_IF_ERROR(
        ComputeDateFromYearAndDayOfYear(*year, dayofyear_info, &civil_day));
    new_element_position = dayofyear_info.value().position;
  }

  // Only update the month or day if the new part is after all related
  // original parts.
  if (date_parse_context->last_year_element_position < new_element_position) {
    *year = civil_day.year();
  }
  if (date_parse_context->last_month_element_position < new_element_position) {
    *month = civil_day.month();
  }
  if (date_parse_context->last_mday_element_position < new_element_position) {
    *mday = civil_day.day();
  }
  // Verify that the result date is valid.  This is needed for a case like
  // PARSE_DATE("%Y %W %d", "1999-09-29"), where we have updated the month
  // from the week date part element (in this case February), but the
  // originally specified day (whose element %d appears after %W) is out of
  // range for that updated month.
  if (!IsValidDay(*year, *month, *mday)) {
    return MakeEvalError()
           << "Out-of-range datetime field in parsing function; year: " << *year
           << ", month: " << *month << ", day: " << *mday;
  }
  return absl::OkStatus();
}

// Invoked if ParseTime was called with 'version 2' semantics, which respects
// format elements that were previously ignored (ISO parts, dayofyear, week,
// and day of week).  Updates the year, month, and/or day if these newly
// supported elements are present and relevant.
static absl::Status UpdateYearMonthDayIfNeeded(
    int64_t* year, int* month, int* mday, DateParseContext* date_parse_context) {
  // Canonicalize the DateParseContext, eliminating redundancy.  Note that
  // the returned DateParseContext will be validated via RET_CHECKs below
  // during processing.
  ZETASQL_RETURN_IF_ERROR(CanonicalizeDateParseContext(date_parse_context));

  if (date_parse_context->elements.empty()) {
    // All of the new elements were ignored, so return early.
    return absl::OkStatus();
  }

  if (date_parse_context->iso_year_present) {
    return ComputeYearMonthDayFromISOParts(year, month, mday,
                                           date_parse_context);
  }

  return ComputeYearMonthDayFromNonISOParts(year, month, mday,
                                            date_parse_context);
}

// This function generally uses strptime() to handle each format element,
// but supports additional format element extensions and a few behavior
// deviations for ZetaSQL semantics.
// 'format' and 'timestamp_string' do not need to be null-terminated.
static absl::Status ParseTime(absl::string_view format,
                              absl::string_view timestamp_string,
                              const absl::TimeZone default_timezone,
                              TimestampScale scale, bool parse_version2,
                              absl::Time* timestamp) {
  // The unparsed input.  Note that data and end_of_data can be nullptr
  // for an empty string_view.

  const char* data = timestamp_string.data();
  const char* end_of_data = data + timestamp_string.length();
  bool read_copy = false;
  const char* original_data_copy_position;
  std::string data_copy_str;

  // If the last byte of the 'timestamp_string' is a nul-byte then we ignore it.
  if (data != end_of_data) {
    const char* last_char = end_of_data - 1;
    if (*last_char == '\0') {
      end_of_data = last_char;
    }
  }

  // Skips leading whitespace.
  data = ConsumeWhitespace(data, end_of_data);

  // Sets default values for unspecified fields.
  struct tm tm = { 0 };
  tm.tm_year = 1970 - 1900;  // tm_year is an offset from 1900
  tm.tm_mon = 1 - 1;         // tm_mon is 0-based, so this is January
  tm.tm_mday = 1;
  tm.tm_hour = 0;
  tm.tm_min = 0;
  tm.tm_sec = 0;
  tm.tm_wday = 4;            // Thursday
  tm.tm_yday = 0;
  tm.tm_isdst = 0;

  DateParseContext date_parse_context;

  absl::Duration subseconds;

  int timezone_offset_minutes = 0;
  bool saw_timezone_offset = false;

  absl::TimeZone timezone = default_timezone;

  const char* fmt = format.data();
  const char* end_of_fmt = fmt + format.length();
  // If the last byte of the 'format' string is a nul-byte then we ignore it.
  if (fmt != end_of_fmt) {
    const char* last_char = end_of_fmt - 1;
    if (*last_char == '\0') {
      end_of_fmt = last_char;
    }
  }

  bool twelve_hour = false;
  bool afternoon = false;

  bool saw_percent_s = false;
  int64_t percent_s_time = 0;

  int century = 0;
  // Should the value in <century> be applied to <tm.tm_year>.
  bool use_century = false;
  // Has <century> been set by an explicit '%C'. <century> can be set by '%y'
  // but such an implicit value should be overwritten by a subsequent '%y'.
  bool explicit_century = false;

  // Steps through the format string one format element at a time.  Generally
  // uses strptime() to process the format elements, but has native
  // handling for timezones, subseconds, and many others.
  int current_element_position = 0;
  while (data != nullptr && data < end_of_data && fmt < end_of_fmt) {
    // If the next format character is a space, skip over all the next spaces
    // in both the format and the input timestamp string.
    // TODO: Handle UTF and unicode white space characters as well.
    if (absl::ascii_isspace(*fmt)) {
      data = ConsumeWhitespace(data, end_of_data);
      while (++fmt < end_of_fmt && absl::ascii_isspace(*fmt)) continue;
      continue;
    }

    // If the next character in the format string is not a format element,
    // then that character must match exactly with the input data or an
    // error is returned.

    if (fmt != nullptr && fmt < end_of_fmt && *fmt != '%') {
      if (data != nullptr && data < end_of_data && *data == *fmt) {
        ++data;
        ++fmt;
      } else {
        return MakeEvalError() << "Mismatch between format character '" << *fmt
                               << "' and string character '" << *data << "'";
      }
      continue;
    }

    const char* percent = fmt;
    if (++fmt == end_of_fmt) {
      // The format string cannot end with a single '%'.
      return MakeEvalError() << "Format string cannot end with a single '%'";
    }

    current_element_position++;

    switch (*fmt++) {
      case 'Y':
        // For ZetaSQL we accept years 0-10000 because after offsetting
        // the result timestamp with a time zone it may fall within the valid
        // range.  The actual result timestamp value will be range-checked
        // later.
        // Note that the year value is offset in the tm by 1900.
        // If the next element in the format is another formatting escape, don't
        // allow 'ParseInt' to consume a fifth digit.
        if (fmt < end_of_fmt && *fmt == '%') {
          data = ParseInt(data, end_of_data, 4, 0, 9999, &tm.tm_year);
        } else {
          data = ParseInt(data, end_of_data, 5, 0, 10000, &tm.tm_year);
        }
        if (data != nullptr) tm.tm_year -= 1900;
        // Full year form should overwrite century.
        use_century = false;
        explicit_century = false;
        date_parse_context.last_year_element_position =
            current_element_position;
        date_parse_context.non_iso_date_part_present = true;
        continue;
      case 'C': {
        // If the next element in the format is another formatting escape, don't
        // allow 'ParseInt' to consume a third digit.
        if (fmt < end_of_fmt && *fmt == '%') {
          data = ParseInt(data, end_of_data, 2, 0, 99, &century);
        } else {
          data = ParseInt(data, end_of_data, 3, 0, 100, &century);
        }
        // Note that the year value is offset in the tm by 1900.
        if (data != nullptr && !use_century) tm.tm_year = 0;
        use_century = true;
        explicit_century = true;
        date_parse_context.last_year_element_position =
            current_element_position;
        date_parse_context.non_iso_date_part_present = true;
        continue;
      }
      case 'm': {
        data = ParseInt(data, end_of_data, 2, 1, 12, &tm.tm_mon);
        tm.tm_mon -= 1;
        date_parse_context.last_month_element_position =
            current_element_position;
        date_parse_context.non_iso_date_part_present = true;
        continue;
      }
      case 'd': {
        data = ParseInt(data, end_of_data, 2, 1, 31, &tm.tm_mday);
        date_parse_context.last_mday_element_position =
            current_element_position;
        date_parse_context.non_iso_date_part_present = true;
        continue;
      }
      case 'H':
        data = ParseInt(data, end_of_data, 2, 0, 23, &tm.tm_hour);
        twelve_hour = false;
        continue;
      case 'M':
        data = ParseInt(data, end_of_data, 2, 0, 59, &tm.tm_min);
        continue;
      case 'S':
        data = ParseInt(data, end_of_data, 2, 0, 60, &tm.tm_sec);
        continue;
      case 'Q': {
        int quarter_number;
        data = ParseInt(data, end_of_data, 1, 1, 4, &quarter_number);
        if (data != nullptr) {
          tm.tm_mon = (quarter_number - 1) * 3;
          tm.tm_mday = 1;
        }
        date_parse_context.non_iso_date_part_present = true;
        date_parse_context.last_month_element_position =
            current_element_position;
        date_parse_context.last_mday_element_position =
            current_element_position;
        continue;
      }
      case 'p': {
        data = HandleMeridianFormatters(data, end_of_data, afternoon);
        continue;
      }
      case 'r':  // equivalent to %I:%M:%S %p
        data = HandleTwelveHourFormatters(data, end_of_data, tm, twelve_hour);
        data = ExpectChar(data, end_of_data, ':');
        data = ParseInt(data, end_of_data, 2, 0, 59, &tm.tm_min);
        data = ExpectChar(data, end_of_data, ':');
        data = ParseInt(data, end_of_data, 2, 0, 60, &tm.tm_sec);
        data = ConsumeWhitespace(data, end_of_data);
        data = HandleMeridianFormatters(data, end_of_data, afternoon);
        continue;
      case 'c':  // equivalent to '%a %b %e %T %Y'
                 // example: 'Tue Jul 20 12:34:56 2021'
        date_parse_context.non_iso_date_part_present = true;
        date_parse_context.last_year_element_position =
            current_element_position;
        date_parse_context.last_month_element_position =
            current_element_position;
        date_parse_context.last_mday_element_position =
            current_element_position;
        twelve_hour = false;  // probably uses %H
        break;
      case 'R':  // uses %H
      case 'T':  // uses %H
      case 'X':  // probably uses %H
        twelve_hour = false;
        break;
      case 'y':
        data = ParseInt(data, end_of_data, 2, 0, 99, &tm.tm_year);
        // Use century to keep track of combinations of %y and %C.
        if (data != nullptr && !explicit_century) {
          century = tm.tm_year < 69 ? 20 : 19;
        }
        use_century = true;
        date_parse_context.non_iso_date_part_present = true;
        date_parse_context.last_year_element_position =
            current_element_position;
        continue;
      case 'z':
        data = ParseOffset(data, end_of_data, '\0', &timezone_offset_minutes);
        if (!IsValidTimeZone(timezone_offset_minutes)) {
          return MakeEvalError()
                 << "Timezone offset out of valid range -14:00 to +14:00: "
                 << TimeZoneOffsetToString(timezone_offset_minutes);
        }
        saw_timezone_offset = true;
        continue;
      case 'Z': {
        std::string timezone_string;
        data = ParseZone(data, &timezone_string, end_of_data);
        // The input time zone string overrides the default time zone.
        ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));

        // Unset the timezone offset settings, we will use an offset derived
        // from the specified time zone name instead.
        timezone_offset_minutes = 0;
        saw_timezone_offset = false;
        continue;
      }
      case 's': {
        const int64_t seconds_min = types::kTimestampMin / kNumMillisPerSecond;
        const int64_t seconds_max = types::kTimestampMax / kNumMillisPerSecond;
        const int max_seconds_digits = 12;
        data = ParseInt(data, end_of_data, max_seconds_digits, seconds_min,
                        seconds_max, &percent_s_time);
        if (data != nullptr) saw_percent_s = true;
        // We don't really need to track element positions for year/month/day
        // since %s overrides everything else, but we do it for consistency
        // since it does impact the year/month/day parts.
        date_parse_context.non_iso_date_part_present = true;
        date_parse_context.last_year_element_position =
            current_element_position;
        date_parse_context.last_month_element_position =
            current_element_position;
        date_parse_context.last_mday_element_position =
            current_element_position;
        continue;
      }
      case 'E': {
        if (fmt < end_of_fmt && *fmt == 'z') {
          if (data != nullptr && *data == 'Z') {
            timezone_offset_minutes = 0;
            saw_timezone_offset = true;
            data += 1;
            fmt += 1;
            continue;
          }
          data = ParseOffset(data, end_of_data, ':', &timezone_offset_minutes);
          if (!IsValidTimeZone(timezone_offset_minutes)) {
            return MakeEvalError()
                   << "Timezone offset out of valid range -14:00 to +14:00: "
                   << TimeZoneOffsetToString(timezone_offset_minutes);
          }
          saw_timezone_offset = true;
          fmt += 1;
          continue;
        }
        if (fmt < end_of_fmt && *fmt == 'Y') {
          // If the next element in the format is another formatting escape,
          // don't allow 'ParseInt' to consume a fifth digit.
          if (fmt + 1 < end_of_fmt && fmt[1] == '%') {
            data = ParseInt(data, end_of_data, 4, 0, 9999, &tm.tm_year);
          } else {
            data = ParseInt(data, end_of_data, 5, 0, 10000, &tm.tm_year);
          }
          // Year with century.  '%EY' is treated like '%Y' in en_US locale.
          if (data != nullptr) tm.tm_year -= 1900;
          fmt += 1;
          // Full year form should overwrite century.
          use_century = false;
          explicit_century = false;
          date_parse_context.non_iso_date_part_present = true;
          date_parse_context.last_year_element_position =
              current_element_position;
          continue;
        }
        if (fmt < end_of_fmt && *fmt == 'y') {
          // Two digit year.  '%Ey' is treated like '%y' in en_US locale.
          data = ParseInt(data, end_of_data, 2, 0, 99, &tm.tm_year);
          // Use century to keep track of combinations of %y and %C.
          if (data != nullptr && !explicit_century) {
            century = tm.tm_year < 69 ? 20 : 19;
          }
          fmt += 1;
          use_century = true;
          date_parse_context.non_iso_date_part_present = true;
          date_parse_context.last_year_element_position =
              current_element_position;
          continue;
        }
        if (fmt < end_of_fmt && *fmt == 'C') {
          // '%EC' treated like '%C'.
          // If the next element in the format is another formatting escape,
          // don't allow 'ParseInt' to consume a third digit.
          if (fmt + 1 < end_of_fmt && fmt[1] == '%') {
            data = ParseInt(data, end_of_data, 2, 0, 99, &century);
          } else {
            data = ParseInt(data, end_of_data, 3, 0, 100, &century);
          }
          // Note that the year value is offset in the tm by 1900.
          if (data != nullptr && !use_century) tm.tm_year = 0;
          fmt += 1;
          use_century = true;
          explicit_century = true;
          date_parse_context.non_iso_date_part_present = true;
          date_parse_context.last_year_element_position =
              current_element_position;
          continue;
        }
        if (fmt + 1 < end_of_fmt && *fmt == '*' && *(fmt + 1) == 'S') {
          data = ParseInt(data, end_of_data, 2, 0, 60, &tm.tm_sec);
          data = ParseSubSecondsIfStartingWithPoint(
              data, end_of_data, 0 /* max_digits */, scale, &subseconds);
          fmt += 2;
          continue;
        }
        if (fmt + 1 < end_of_fmt && *fmt == '4' && *(fmt + 1) == 'Y') {
          const char* bp = data;
          // Valid year range is 0 - 9999.
          data = ParseInt(data, end_of_data, 4, 0, 9999, &tm.tm_year);
          if (data != nullptr) {
            if (data - bp == 4) {
              tm.tm_year -= 1900;
            } else {
              data = nullptr;  // Less than four digits, return an error.
            }
          }
          fmt += 2;
          // Full year form should overwrite century.
          use_century = false;
          date_parse_context.non_iso_date_part_present = true;
          date_parse_context.last_year_element_position =
              current_element_position;
          continue;
        }
        if (fmt < end_of_fmt && std::isdigit(*fmt)) {
          int n = 0;
          // Only %E0S to %E9S is supported (0-9 subseconds digits).
          if (const char* np = ParseInt(fmt, end_of_fmt, 1, 0,
                                        static_cast<int32_t>(scale), &n)) {
            if (*np++ == 'S') {
              data = ParseInt(data, end_of_data, 2, 0, 60, &tm.tm_sec);
              if (n > 0) {
                data = ParseSubSecondsIfStartingWithPoint(data, end_of_data, n,
                                                          scale, &subseconds);
              }
              fmt = np;
              continue;
            }
          }
        }
        // Uses %H in en_US locale.
        if (fmt < end_of_fmt && *fmt == 'c') twelve_hour = false;
        // Uses %H in en_US locale.
        if (fmt < end_of_fmt && *fmt == 'X') twelve_hour = false;
        if (fmt < end_of_fmt) {
          fmt += 1;
        }
        break;
      }
      case 'I':
      case 'l': {
        data = HandleTwelveHourFormatters(data, end_of_data, tm, twelve_hour);
        continue;
      }

      case 'O':
        if (fmt < end_of_fmt && *fmt == 'H') twelve_hour = false;
        if (fmt < end_of_fmt && *fmt == 'I') {
          data = HandleTwelveHourFormatters(data, end_of_data, tm, twelve_hour);
          fmt++;
          continue;
        }
        if (fmt < end_of_fmt && *fmt == 'u') {
          // Day of week 1-7.  '%Ou' is treated like '%u' in en_US locale.
          // '%u' is defined as weekday number 1-7, starting Monday
          date_parse_context.elements.push_back(
              {'u', data, end_of_data, current_element_position});
          data = ParseInt(data, end_of_data, 1, 1, 7, &tm.tm_wday);
          fmt += 1;
          continue;
        }
        if (fmt < end_of_fmt && *fmt == 'w') {
          // Day of week 0-6.  '%Ow' is treated like '%w' in en_US locale.
          // '%w' is defined as weekday number 0-6, starting Sunday
          date_parse_context.elements.push_back(
              {'w', data, end_of_data, current_element_position});
          data = ParseInt(data, end_of_data, 1, 0, 6, &tm.tm_wday);
          fmt += 1;
          continue;
        }
        if (fmt < end_of_fmt && *fmt == 'U') {
          // TODO: We should memoize the week number here and below
          // in the <date_parse_context> so that we do not have to re-parse it
          // later.  We could probably do the same for a lot of the new
          // 'version2' elements for week/weekday/dayofyear/etc.
          int week_number;
          // Week number 00-53.  '%OU' is treated like '%U' in en_US locale.
          date_parse_context.non_iso_week_present = true;
          date_parse_context.non_iso_date_part_present = true;
          date_parse_context.elements.push_back(
              {'U', data, end_of_data, current_element_position});
          data = ParseInt(data, end_of_data, 2, 0, 53, &week_number);
          fmt += 1;
          continue;
        }
        if (fmt < end_of_fmt && *fmt == 'V') {
          int week_number;
          // Week number 1-53.  '%OV' is treated like '%V' in en_US locale.
          date_parse_context.iso_week_present = true;
          date_parse_context.elements.push_back(
              {'V', data, end_of_data, current_element_position});
          data = ParseInt(data, end_of_data, 2, 1, 53, &week_number);
          fmt += 1;
          continue;
        }
        if (fmt < end_of_fmt && *fmt == 'W') {
          int week_number;
          // Week number 0-53.  '%OW' is treated like '%W' in en_US locale.
          date_parse_context.iso_week_present = true;
          date_parse_context.elements.push_back(
              {'W', data, end_of_data, current_element_position});
          data = ParseInt(data, end_of_data, 2, 0, 53, &week_number);
          fmt += 1;
          continue;
        }
        if (fmt < end_of_fmt) ++fmt;
        break;
      case 'D':  // %m/%d/%y
      case 'F':  // %Y-%m-%d
      case 'x':  // locale-specific YMD format, %m/%d/%y in en_US locale
        date_parse_context.non_iso_date_part_present = true;
        date_parse_context.last_year_element_position =
            current_element_position;
        date_parse_context.last_month_element_position =
            current_element_position;
        date_parse_context.last_mday_element_position =
            current_element_position;
        break;
      case 'B':  // Full month name
      case 'b':  // Abbreviated month name
      case 'h':  // Abbreviated month name
        date_parse_context.non_iso_date_part_present = true;
        date_parse_context.last_month_element_position =
            current_element_position;
        break;
      case 'e':  // day of month (single digits preceded by a space)
        date_parse_context.non_iso_date_part_present = true;
        date_parse_context.last_mday_element_position =
            current_element_position;
        break;
      case 'U':  // week number of the year (starting Sunday) 00-53
      case 'W':  // week number of the year (starting Monday) 00-53
        date_parse_context.non_iso_week_present = true;
        date_parse_context.non_iso_date_part_present = true;
        date_parse_context.elements.push_back(
            {*(fmt - 1), data, end_of_data, current_element_position});
        break;
      case 'V':  // ISO 8601 week number 01-53
        date_parse_context.iso_week_present = true;
        date_parse_context.elements.push_back(
            {'V', data, end_of_data, current_element_position});
        // ParseTM doesn't support this part, so parse the ISO week value
        // to advance 'data' and continue.
        int week_number;
        data = ParseInt(data, end_of_data, 2, 1, 53, &week_number);
        continue;
      case 'A':  // Full weekday name
      case 'a':  // Abbreviated weekday name
      case 'u':  // weekday number 1-7, starting Monday
      case 'w':  // weekday number 0-6, starting Sunday
        date_parse_context.elements.push_back(
            {*(fmt - 1), data, end_of_data, current_element_position});
        break;
      case 'J':  // ISO day of year
        date_parse_context.iso_dayofyear_present = true;
        date_parse_context.elements.push_back(
            {*(fmt - 1), data, end_of_data, current_element_position});
        // ParseTM doesn't support this part, so parse the ISO day value
        // to advance 'data' and continue.
        int iso_dayofyear;
        data = ParseInt(data, end_of_data, 3, 1, 371, &iso_dayofyear);
        continue;
      case 'j':  // Day of year (non-ISO)
        date_parse_context.non_iso_date_part_present = true;
        date_parse_context.elements.push_back(
            {*(fmt - 1), data, end_of_data, current_element_position});
        break;
      case 't':
      case 'n': {
        data = ConsumeWhitespace(data, end_of_data);
        continue;
      }
      case 'g': {  // ISO 8601 year without century, e.g., 19
        date_parse_context.iso_year_present = true;
        date_parse_context.elements.push_back(
            {*(fmt - 1), data, end_of_data, current_element_position});
        // Move 'data' past this element's data, but don't update the output.
        int ignored;
        data = ParseInt(data, end_of_data, 2, 0, 99, &ignored);
        continue;
      }
      case 'G': {  // ISO 8601 year with century, e.g., 2019
        // To be (mostly) backwards compatible with the previous strptime
        // implementation, we consume and ignore a large number of digits
        // here.  Technically, strptime will consume an arbitrarily large
        // number of digits, but we will only consume enough to more than
        // cover an int64_t (even though we only support a range of 10k years.
        date_parse_context.iso_year_present = true;
        date_parse_context.elements.push_back(
            {*(fmt - 1), data, end_of_data, current_element_position});
        // Move 'data' past this element's data, but don't update the output.
        int ignored;
        data = ParseInt(data, end_of_data, 20, 0, 99999, &ignored);
        continue;
      }
      default:
        // No special handling for this format element, let ParseTM/strptime()
        // do it.
        break;
    }

    std::string format_element(percent, fmt - percent);

    // When no special handling for this format element in the switch statement
    // above, call ParseTM() that invokes strptime() to parse the current
    // format element and updates tm.
    //
    // strptime() requires that the input strings are null terminated. Thus, we
    // make a string copy of the 'timestamp_string' from the position that we
    // cannot handle in the switch statement above to the end of
    // 'timestamp_string', because 'timestamp_string' is a string_view and may
    // not be null-terminated. We only make the copy once and 'read_copy'is
    // changed to true if the copy is made. If another format element is without
    // special handling in the switch statement above, we won't make a copy
    // again. The copy we made for the previous no-special-handling format
    // element will be used. We just recompute the offset of the string copy and
    // pass it to strptime().
    if (!read_copy) {
      read_copy = true;
      data_copy_str = std::string(data, end_of_data - data);
      original_data_copy_position = data;
    }

    const char* data_copy_pointer =
        data_copy_str.c_str() + (data - original_data_copy_position);
    const char* next_position =
        ParseTM(data_copy_pointer, format_element.c_str(), &tm);
    if (next_position != nullptr) {
      data += next_position - data_copy_pointer;
    } else {
      data = nullptr;
    }
  }

  // Adjust a 12-hour tm_hour value if it should be in the afternoon.
  if (twelve_hour && afternoon) {
    tm.tm_hour += 12;
  }

  // Skip any remaining whitespace.
  // TODO: Handle UTF and unicode white space characters as well.
  if (data != nullptr) {
    while (data < end_of_data && absl::ascii_isspace(*data)) ++data;
  }
  if (fmt != nullptr) {
    // Note that in addition to skipping trailing whitespace in the format
    // string, we must also handle a corner case where we have consumed the
    // entire input data string, but the format string still contains %n or %t
    // format elements (which consume 0 or more whitespaces).  So we must
    // also ignore any remaining %n or %t format elements.
    while (fmt < end_of_fmt && (absl::ascii_isspace(*fmt) || *fmt == '%')) {
      // TODO: Handle UTF and unicode white space characters as well.
      if (absl::ascii_isspace(*fmt)) {
        ++fmt;
        continue;
      }
      if (++fmt == end_of_fmt) {
        // The format string cannot end with a single '%'.
        return MakeEvalError() << "Format string cannot end with a single '%'";
      }
      if (*fmt == 'n' || *fmt == 't') {
        // We got '%n' or '%t', so increment and continue.
        ++fmt;
        continue;
      } else {
        // We got a different format element, so stop skipping white space.
        // This will cause us to return the 'Failed to parse input string'
        // error below.
        break;
      }
    }
  }

  if (data != end_of_data || fmt != end_of_fmt) {
    return MakeEvalError() << "Failed to parse input string "
                           << ToStringLiteral(timestamp_string);
  }

  // We must consume the entire input string and there must not be trailing
  // garbage or it is an error.
  if (data != end_of_data) {
    return MakeEvalError() << "Illegal non-space trailing data '" << *data
                           << "' in string "
                           << ToStringLiteral(timestamp_string);
  }

  // If we saw %s then we ignore everything else and return the
  // corresponding timestamp.
  if (saw_percent_s) {
    *timestamp = absl::FromUnixSeconds(percent_s_time);
    if (!IsValidTime(*timestamp)) {
      return MakeEvalError() << "Invalid result from parsing function";
    }

    return absl::OkStatus();
  }

  // If we saw %z or %Ez then we want to interpret the parsed fields in
  // UTC and then shift by that offset.  Otherwise we want to interpret
  // the fields using the default or specified time zone name.
  if (saw_timezone_offset) {
    // We will apply the timezone_offset from UTC.
    timezone = absl::UTCTimeZone();
  } else {
    ZETASQL_RET_CHECK_EQ(0, timezone_offset_minutes);
  }

  // Normalizes a leap second of 60 to the following ":00.000000".
  if (tm.tm_sec == 60) {
    tm.tm_sec -= 1;
    subseconds = absl::Seconds(1);
  }

  // Overflow cannot occur since the only valid range is years 0-10000.
  int64_t year = tm.tm_year + 1900;
  if (use_century) {
    year += century * 100 - 1900;
  }

  int month = tm.tm_mon + 1;
  int mday = tm.tm_mday;

  if (parse_version2) {
    ZETASQL_RETURN_IF_ERROR(
        UpdateYearMonthDayIfNeeded(&year, &month, &mday, &date_parse_context));
  }

  const absl::TimeConversion tc = absl::ConvertDateTime(
      year, month, mday, tm.tm_hour, tm.tm_min, tm.tm_sec, timezone);

  // ParseTime() fails if any normalization was done.  That is,
  // parsing "Sep 31" will not produce the equivalent of "Oct 1".
  if (tc.normalized) {
    return MakeEvalError() << "Out-of-range datetime field in parsing function";
  }

  *timestamp = tc.pre - absl::Minutes(timezone_offset_minutes) + subseconds;
  if (!IsValidTime(*timestamp)) {
    return MakeEvalError() << "Invalid result from parsing function";
  }

  return absl::OkStatus();
}  // NOLINT(readability/fn_size)

// Validates that <format_string> does not have any <invalid_elements>.
static absl::Status ValidateParseFormat(absl::string_view format_string,
                                        absl::string_view target_type_name,
                                        const char* invalid_elements) {
  const char* cur = format_string.data();
  const char* end = cur + format_string.size();

  while (cur != end) {
    while (cur != end && *cur != '%') ++cur;

    // Span the sequential percent signs.
    const char* percent = cur;
    while (cur != end && *cur == '%') ++cur;

    // Loop unless we have an unescaped percent.
    if (cur == end || (cur - percent) % 2 == 0) {
      continue;
    }

    // Returns error if the format is any of the <invalid_elements>
    if (strchr(invalid_elements, *cur)) {
      return MakeEvalError() << "Invalid format: %" << *cur
                             << " is not allowed for the " << target_type_name
                             << " type.";
    }

    const char* prev = cur;
    if ((*cur != 'E' && *cur != 'O') || ++cur == end) {
      continue;
    }

    if (*prev == 'E') {
      // Check %E extensions.
      if (strchr(invalid_elements, *cur) ||
          // If %S (second) is invalid, then %E#S and %E*S should also be
          // invalid.
          (strchr(invalid_elements, 'S') &&
           ((*cur == '*' || std::isdigit(*cur)) && ++cur != end &&
            *cur == 'S')) ||
          // If %Y (year) is invalid, then %E4Y should also be invalid.
          (strchr(invalid_elements, 'Y') && *cur == '4' && ++cur != end &&
           *cur == 'Y')) {
        std::string element;
        while (prev != cur) {
          element.push_back(*prev);
          ++prev;
        }
        element.push_back(*cur);
        return MakeEvalError() << "Invalid format: %" << element
                               << " is not allowed for the " << target_type_name
                               << " type.";
      }
    } else if (*prev == 'O') {
      // Check %O extensions.
      if (strchr(invalid_elements, *cur)) {
        return MakeEvalError() << "Invalid format: %O" << *cur
                               << " is not allowed for the " << target_type_name
                               << " type.";
      }
    }
  }
  return absl::OkStatus();
}

// Validates the <format_string> to only allow format elements applicable to the
// DATE type.  Returns error for non-DATE related formats such as
// Hour/Minute/Second/Timezone etc.
static absl::Status ValidateDateFormat(absl::string_view format_string) {
  return ValidateParseFormat(format_string, "DATE", "cHIklMPpRrSsTXZz");
}

// Similar to ValidateDateFormat, but return error for non-TIME related formats
// such as Year/Month/Week/Day/Timezone etc..
static absl::Status ValidateTimeFormat(absl::string_view format_string) {
  return ValidateParseFormat(format_string, "TIME",
                             "AaBbhCcDdeFGgjmsUuVWwxYyZz");
}

// Similar to ValidateDateFormat, but return error for format elements for
// timezones.
static absl::Status ValidateDatetimeFormat(absl::string_view format_string) {
  return ValidateParseFormat(format_string, "DATETIME", "Zz");
}

// The result timestamp is always at microseconds precision.
static absl::Status ParseTime(absl::string_view format,
                              absl::string_view timestamp_string,
                              const absl::TimeZone default_timezone,
                              bool parse_version2, int64_t* timestamp) {
  absl::Time base_time;
  ZETASQL_RETURN_IF_ERROR(ParseTime(format, timestamp_string, default_timezone,
                            kMicroseconds, parse_version2, &base_time));
  if (!ConvertTimeToTimestamp(base_time, timestamp)) {
    return MakeEvalError() << "Invalid result from parsing function";
  }
  return absl::OkStatus();
}

// Parses the given <date_string> with respect to <format> and stores the
// result in date.
// First validates the <format> to disallow any unsupported DATE formats,
// then invoke the ParseStringToTimestamp() to parse the <date_string> to
// a timestamp then extracts the date part.
static absl::Status ParseDate(absl::string_view format,
                              absl::string_view date_string,
                              bool parse_version2, int32_t* date) {
  // Validates if the <format> has any unsupported DATE formats.
  ZETASQL_RETURN_IF_ERROR(ValidateDateFormat(format));

  // Invoke the ParseStringToTimestamp() to parse the <date_string> to a
  // timestamp then extracts the date part.
  int64_t timestamp;
  ZETASQL_RETURN_IF_ERROR(ParseStringToTimestamp(
      format, date_string, absl::UTCTimeZone(), &timestamp, parse_version2));
  ZETASQL_RETURN_IF_ERROR(ExtractFromTimestamp(DATE, timestamp, kMicroseconds,
                                       absl::UTCTimeZone(), date));
  return absl::OkStatus();
}

}  // namespace

absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    const absl::TimeZone default_timezone,
                                    bool parse_version2, int64_t* timestamp) {
  return ParseTime(format_string, timestamp_string, default_timezone,
                   parse_version2, timestamp);
}

// deprecated
absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    const absl::TimeZone default_timezone,
                                    int64_t* timestamp, bool parse_version2) {
  return ParseStringToTimestamp(format_string, timestamp_string,
                                default_timezone, parse_version2, timestamp);
}

absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    absl::string_view default_timezone_string,
                                    bool parse_version2, int64_t* timestamp) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(default_timezone_string, &timezone));
  return ParseStringToTimestamp(format_string, timestamp_string, timezone,
                                parse_version2, timestamp);
}

// deprecated
absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    absl::string_view default_timezone_string,
                                    int64_t* timestamp, bool parse_version2) {
  return ParseStringToTimestamp(format_string, timestamp_string,
                                default_timezone_string, parse_version2,
                                timestamp);
}

absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    const absl::TimeZone default_timezone,
                                    bool parse_version2,
                                    absl::Time* timestamp) {
  ZETASQL_RETURN_IF_ERROR(ParseTime(format_string, timestamp_string, default_timezone,
                            kNanoseconds, parse_version2, timestamp));
  return absl::OkStatus();
}

// deprecated
absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    const absl::TimeZone default_timezone,
                                    absl::Time* timestamp,
                                    bool parse_version2) {
  return ParseStringToTimestamp(format_string, timestamp_string,
                                default_timezone, parse_version2, timestamp);
}

absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    absl::string_view default_timezone_string,
                                    bool parse_version2,
                                    absl::Time* timestamp) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(default_timezone_string, &timezone));
  return ParseStringToTimestamp(format_string, timestamp_string, timezone,
                                parse_version2, timestamp);
}

// deprecated
absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    absl::string_view default_timezone_string,
                                    absl::Time* timestamp,
                                    bool parse_version2) {
  return ParseStringToTimestamp(format_string, timestamp_string,
                                default_timezone_string, parse_version2,
                                timestamp);
}

absl::Status ParseStringToDate(absl::string_view format_string,
                               absl::string_view date_string,
                               bool parse_version2, int32_t* date) {
  return ParseDate(format_string, date_string, parse_version2, date);
}

// deprecated
absl::Status ParseStringToDate(absl::string_view format_string,
                               absl::string_view date_string, int32_t* date,
                               bool parse_version2) {
  return ParseStringToDate(format_string, date_string, parse_version2, date);
}

absl::Status ParseStringToTime(absl::string_view format_string,
                               absl::string_view time_string,
                               TimestampScale scale,
                               TimeValue* time) {
  ZETASQL_CHECK(scale == kNanoseconds || scale == kMicroseconds);
  ZETASQL_RETURN_IF_ERROR(ValidateTimeFormat(format_string));

  absl::Time base_time;
  ZETASQL_RETURN_IF_ERROR(ParseTime(format_string, time_string, absl::UTCTimeZone(),
                            scale, /*parse_version2=*/false, &base_time));
  return ConvertTimestampToTime(base_time, absl::UTCTimeZone(), scale, time);
}

absl::Status ParseStringToDatetime(absl::string_view format_string,
                                   absl::string_view datetime_string,
                                   TimestampScale scale, bool parse_version2,
                                   DatetimeValue* datetime) {
  ZETASQL_CHECK(scale == kNanoseconds || scale == kMicroseconds);
  ZETASQL_RETURN_IF_ERROR(ValidateDatetimeFormat(format_string));

  absl::Time base_time;
  ZETASQL_RETURN_IF_ERROR(ParseTime(format_string, datetime_string, absl::UTCTimeZone(),
                            scale, parse_version2, &base_time));
  return ConvertTimestampToDatetime(base_time, absl::UTCTimeZone(), datetime);
}

// deprecated
absl::Status ParseStringToDatetime(absl::string_view format_string,
                                   absl::string_view datetime_string,
                                   TimestampScale scale,
                                   DatetimeValue* datetime,
                                   bool parse_version2) {
  return ParseStringToDatetime(format_string, datetime_string, scale,
                               parse_version2, datetime);
}

}  // namespace functions
}  // namespace zetasql
