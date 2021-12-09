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

#include "zetasql/public/functions/date_time_util.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <string>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/utf_util.h"
#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/date_time_util_internal.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/types/timestamp_util.h"
#include "zetasql/base/case.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/strings/substitute.h"
#include "absl/time/civil_time.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "zetasql/base/mathutil.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/time_proto_util.h"

namespace zetasql {
namespace functions {
namespace {

using date_time_util_internal::GetIsoWeek;
using date_time_util_internal::GetIsoYear;
using date_time_util_internal::NextWeekdayOrToday;
using date_time_util_internal::PrevWeekdayOrToday;

constexpr int64_t kNaiveNumSecondsPerMinute = 60;
constexpr int64_t kNaiveNumSecondsPerHour = 60 * kNaiveNumSecondsPerMinute;
constexpr int64_t kNaiveNumSecondsPerDay = 24 * kNaiveNumSecondsPerHour;
constexpr int64_t kNaiveNumMicrosPerDay = kNaiveNumSecondsPerDay * 1000 * 1000;

enum NewOrLegacyTimestampType {
  NEW_TIMESTAMP_TYPE,
  // TODO: strip legacy timestamp type
  LEGACY_TIMESTAMP_TYPE,
};

const absl::CivilDay kEpochDay = absl::CivilDay(1970, 1, 1);

absl::CivilDay EpochDaysToCivilDay(int32_t days_since_epoch) {
  return kEpochDay + days_since_epoch;
}

}  // namespace

bool IsValidDate(int32_t date) {
  return date >= zetasql::types::kDateMin &&
         date <= zetasql::types::kDateMax;
}

static int32_t CivilDayToEpochDays(absl::CivilDay day) {
  return static_cast<int32_t>(day - kEpochDay);
}

static bool IsValidCivilDay(absl::CivilDay day) {
  return IsValidDate(CivilDayToEpochDays(day));
}

bool IsValidDay(absl::civil_year_t year, int month, int day) {
  // absl::CivilDay will 'normalize' its arguments, so simply compare
  // the input against the result to check whether a YMD is valid.
  absl::CivilDay civil_day(year, month, day);
  return civil_day.year() == year && civil_day.month() == month &&
         civil_day.day() == day;
}

absl::string_view DateTimestampPartToSQL(DateTimestampPart date_part) {
  switch (date_part) {
    case functions::WEEK_MONDAY:
      return "WEEK(MONDAY)";
    case functions::WEEK_TUESDAY:
      return "WEEK(TUESDAY)";
    case functions::WEEK_WEDNESDAY:
      return "WEEK(WEDNESDAY)";
    case functions::WEEK_THURSDAY:
      return "WEEK(THURSDAY)";
    case functions::WEEK_FRIDAY:
      return "WEEK(FRIDAY)";
    case functions::WEEK_SATURDAY:
      return "WEEK(SATURDAY)";
    default:
      return DateTimestampPart_Name(date_part);
  }
}

static absl::Status* const kNoError = nullptr;

// Returns whether or not there are at least <remaining_length> characters left
// beyond <current_idx> of <str>.
static bool CheckRemainingLength(absl::string_view str, int current_idx,
                                 int remaining_length) {
  if (current_idx + remaining_length > static_cast<int64_t>(str.length())) {
    return false;
  }
  return true;
}

// Parse between <min_digits> and <max_digits> from <str> starting at
// offset <idx>, incrementing <idx> for parsed digits and returning the
// result in <part_value>.  Returns true if successful, false if not.
static bool ParseDigits(absl::string_view str, int min_digits, int max_digits,
                        int* idx, int* part_value) {
  int num_digits = 0;
  *part_value = 0;
  while (num_digits < max_digits && *idx < static_cast<int64_t>(str.length()) &&
         absl::ascii_isdigit(str[*idx])) {
    *part_value *= 10;
    *part_value += str[*idx] - '0';
    ++*idx;
    ++num_digits;
  }
  if (num_digits < min_digits) {
    return false;
  }
  return true;
}

// Parse a single expected character from <str> at offset <idx>, incrementing
// <idx> if successful and return success or failure.
static bool ParseCharacter(absl::string_view str, const char character,
                           int* idx) {
  if (str[*idx] != character) {
    return false;
  }
  ++(*idx);
  return true;
}

// Parse <str> starting at offset <idx> into <year>, <month>, and <day> parts,
// advancing <idx> to point to the next unparsed digit. The valid format is
// YYYY-[M]M-[D]D.
// Returns success or failure.
static bool ParsePrefixToDateParts(absl::string_view str, int* idx, int* year,
                                   int* month, int* day) {
  // Minimum required length of a valid date is 8.
  if (!CheckRemainingLength(str, *idx, 8 /* remaining_length */) ||
      !ParseDigits(str, 4, 4, idx, year) || !ParseCharacter(str, '-', idx) ||
      !ParseDigits(str, 1, 2, idx, month) || !ParseCharacter(str, '-', idx) ||
      !ParseDigits(str, 1, 2, idx, day)) {
    return false;
  }
  return true;
}
// Same as ParsePrefixToDateParts, but requires the entire string to be
// consumed.
static bool ParseStringToDateParts(absl::string_view str, int* idx, int* year,
                                   int* month, int* day) {
  return ParsePrefixToDateParts(str, idx, year, month, day) &&
         *idx >= static_cast<int64_t>(str.length());
}

static const int64_t powers_of_ten[] = {
    1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};
// Parse <str> starting at offset <idx> into <hour>, <minute>, <second> and
// <subsecond> parts, incrementing <idx> for parsed digits. <scale> indicates
// the number of subsecond digits requested. The subsecond part is normalized to
// the requested scale.
// The valid format is [H]H:[M]M:[S]S[.DDDDDDDDD].
// Returns success or failure.
static bool ParsePrefixToTimeParts(absl::string_view str, TimestampScale scale,
                                   int* idx, int* hour, int* minute,
                                   int* second, int* subsecond) {
  if (!CheckRemainingLength(str, *idx, 5 /* remaining_length */) ||
      !ParseDigits(str, 1, 2, idx, hour) ||
      !CheckRemainingLength(str, *idx, 4 /* remaining_length */) ||
      !ParseCharacter(str, ':', idx) || !ParseDigits(str, 1, 2, idx, minute) ||
      !CheckRemainingLength(str, *idx, 2 /* remaining_length */) ||
      !ParseCharacter(str, ':', idx) || !ParseDigits(str, 1, 2, idx, second)) {
    return false;
  }
  if (*idx >= static_cast<int64_t>(str.length()))
    return true;  // Done consuming <str>.

  // Parse the subseconds, if any.
  if (str[*idx] == '.') {
    ++(*idx);
    const int start_subsecond_idx = *idx;
    if (!ParseDigits(str, 1, 9, idx, subsecond)) {
      return false;
    }

    // Normalize the subseconds to the specified scale.
    const int num_parsed_subsecond_digits = *idx - start_subsecond_idx;
    if (scale - num_parsed_subsecond_digits < 0) {
      return false;
    }
    ZETASQL_CHECK_LE(num_parsed_subsecond_digits, 9);

    // <scale> is at most 9, and <num_parsed_subsecond_digits> is at least
    // 1, so the difference is no larger than 8 so indexing into
    // powers_of_ten[] cannot read off the end of the array.
    *subsecond *= powers_of_ten[scale - num_parsed_subsecond_digits];
  }
  return true;
}

// Same as ParsePrefixToTimeParts, but requires the entire string to be
// consumed.
static bool ParseStringToTimeParts(absl::string_view str, TimestampScale scale,
                                   int* idx, int* hour, int* minute,
                                   int* second, int* subsecond) {
  return ParsePrefixToTimeParts(str, scale, idx, hour, minute, second,
                                subsecond) &&
         *idx >= static_cast<int64_t>(str.length());
}

// Parses the string into the relevant timestamp parts.  <scale> indicates
// the number of subsecond digits requested.  Parts not present in the string
// get initialized to 0 as appropriate.  The subsecond part is
// normalized to the requested scale, and if there are extra digits then
// an error is produced.
// The valid format is YYYY-[M]M-[D]D( |T)[[H]H:[M]M:[S]S[.DDDDDDDDD]].
// Returns success or failure.
static bool ParseStringToDatetimeParts(absl::string_view str,
                                       TimestampScale scale, int* year,
                                       int* month, int* day, int* hour,
                                       int* minute, int* second,
                                       int* subsecond) {
  int idx = 0;
  if (!ParsePrefixToDateParts(str, &idx, year, month, day)) {
    return false;
  }
  if (idx >= static_cast<int64_t>(str.length()))
    return true;  // Done consuming <str>.
  // The rest are all optional: time, subseconds, time zone

  // Initial space, 'T', or 't' to kick off the rest.
  if ((!ParseCharacter(str, ' ', &idx) && !ParseCharacter(str, 'T', &idx) &&
       !ParseCharacter(str, 't', &idx)) ||
      !CheckRemainingLength(str, idx, 2 /* remaining_length */)) {
    return false;
  }

  if (!ParseStringToTimeParts(str, scale, &idx, hour, minute, second,
                              subsecond)) {
    return false;
  }
  return true;
}

// This function expects the timezone hour and minute to be positive values,
// with the offset direction determined by <timezone_sign>.
static bool IsValidTimeZoneParts(const char timezone_sign, int timezone_hour,
                                 int timezone_minute) {
  if (timezone_sign != '+' && timezone_sign != '-') {
    return false;
  }
  if (timezone_hour > 14 || timezone_hour < 0 || timezone_minute > 59 ||
      timezone_minute < 0) {
    return false;
  }
  // Since the negative and positive ranges are the same, we don't need
  // to worry about the sign.  This check ensures that we don't allow a
  // timezone like +14:59 that would pass the previous check.
  return IsValidTimeZone(timezone_hour * 60 + timezone_minute);
}

static bool IsValidTimeOfDay(int hour, int minute, int second) {
  // Note that Google time scales do not include leap seconds, so <second>
  // ranges only from 0-59.  However, to support potential callers that
  // expect leap second support, we allow second == 60.
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 ||
      second > 60) {
    return false;
  }
  return true;
}

static std::string DateErrorString(int32_t date) {
  std::string out;
  if (!ConvertDateToString(date, &out).ok()) {
    out = absl::StrCat("DATE(", date, ")");
  }
  return out;
}

static std::string TimestampErrorString(int64_t timestamp, TimestampScale scale,
                                        absl::TimeZone timezone) {
  std::string out;
  if (!ConvertTimestampToStringWithoutTruncation(timestamp, scale, timezone,
                                                 &out)
           .ok()) {
    out = absl::StrCat("timestamp(", timestamp, ")");
  }
  return out;
}

static std::string DefaultTimestampFormatStr(TimestampScale scale) {
  switch (scale) {
    case kSeconds:
      return "%E4Y-%m-%d %H:%M:%S%Ez";
    case kMilliseconds:
      return "%E4Y-%m-%d %H:%M:%E3S%Ez";
    case kMicroseconds:
      return "%E4Y-%m-%d %H:%M:%E6S%Ez";
    case kNanoseconds:
      return "%E4Y-%m-%d %H:%M:%E9S%Ez";
  }
}

// The format string returned is used by absl::StrFormat().
static std::string DefaultTimeFormatStr(TimestampScale scale) {
  switch (scale) {
    case kSeconds:
      return "%02d:%02d:%02d";
    case kMilliseconds:
      return "%02d:%02d:%02d.%03d";
    case kMicroseconds:
      return "%02d:%02d:%02d.%06d";
    case kNanoseconds:
      return "%02d:%02d:%02d.%09d";
  }
}

// The format string returned is used by absl::StrFormat().
static std::string DefaultDatetimeFormatStr(TimestampScale scale) {
  switch (scale) {
    case kSeconds:
      return "%04d-%02d-%02d %02d:%02d:%02d";
    case kMilliseconds:
      return "%04d-%02d-%02d %02d:%02d:%02d.%03d";
    case kMicroseconds:
      return "%04d-%02d-%02d %02d:%02d:%02d.%06d";
    case kNanoseconds:
      return "%04d-%02d-%02d %02d:%02d:%02d.%09d";
  }
}

static std::string TimestampErrorString(absl::Time time,
                                        absl::TimeZone timezone) {
  std::string out;
  if (!ConvertTimestampToString(time, kMicroseconds, timezone, &out).ok()) {
    out =
        absl::StrCat("timestamp(",
                     absl::FormatTime(DefaultTimestampFormatStr(kMicroseconds),
                                      time, timezone),
                     ")");
  }
  return out;
}

static bool TimeFromParts(absl::civil_year_t year, int month, int day, int hour,
                          int minute, int second, absl::TimeZone timezone,
                          absl::Time* time) {
  if (!IsValidDay(year, month, day)) {
    return false;
  }
  if (!IsValidTimeOfDay(hour, minute, second)) {
    return false;
  }
  *time = timezone.At(absl::CivilSecond(year, month, day, hour, minute, second))
              .pre;
  return true;
}

static absl::Duration MakeDuration(int subsecond, TimestampScale scale) {
  switch (scale) {
    case kSeconds:
      return absl::Seconds(subsecond);
    case kMilliseconds:
      return absl::Milliseconds(subsecond);
    case kMicroseconds:
      return absl::Microseconds(subsecond);
    case kNanoseconds:
      return absl::Nanoseconds(subsecond);
  }
}

// It requires the timestamp parts to be within valid bounds (i.e.,
// hour <= 23, minutes <= 59, etc.). This function does not validate the
// range of the result. The caller is responsible for this check if necessary.
static bool TimestampFromParts(absl::civil_year_t year, int month, int day,
                               int hour, int minute, int second, int subsecond,
                               TimestampScale scale, absl::TimeZone timezone,
                               absl::Time* timestamp) {
  if (!TimeFromParts(year, month, day, hour, minute, second, timezone,
                     timestamp)) {
    return false;
  }
  *timestamp += MakeDuration(subsecond, scale);
  return true;
}

// Parse the offset-based time zone format with optional leading 'UTC':
//   [UTC]+/-HH[[:]MM].
// and returns time zone offset parts: sign, hour and minute.
static bool ParseTimeZoneOffsetFormat(absl::string_view timezone_string,
                                      char* timezone_sign, int* timezone_hour,
                                      int* timezone_minute) {
  // Optional "UTC" prefix before offset
  absl::ConsumePrefix(&timezone_string, "UTC");
  if (timezone_string.empty()) {
    return false;
  }

  // Check time zone sign.
  if (timezone_string[0] != '+' && timezone_string[0] != '-') {
    return false;
  }
  *timezone_sign = timezone_string[0];
  *timezone_hour = 0;
  *timezone_minute = 0;

  int idx = 1;
  if (!CheckRemainingLength(timezone_string, idx, 1 /* remaining_length */) ||
      !ParseDigits(timezone_string, 1, 2, &idx, timezone_hour)) {
    return false;
  }
  if (idx >= static_cast<int64_t>(timezone_string.size())) return true;

  // Skip optional ':' minutes delimiter
  ParseCharacter(timezone_string, ':', &idx);
  if (!CheckRemainingLength(timezone_string, idx, 1 /* remaining_length */) ||
      !ParseDigits(timezone_string, 1, 2, &idx, timezone_minute)) {
    return false;
  }
  if (idx >= static_cast<int64_t>(timezone_string.size())) return true;
  return false;
}

// Maps time zone sign, hour, and minute to a total offset in <scale> units.
// Returns success or failure.
static bool TimeZonePartsToOffset(const char timezone_sign,
                                  int64_t timezone_hour,
                                  int64_t timezone_minute, TimestampScale scale,
                                  int64_t* timezone_offset) {
  if (!IsValidTimeZoneParts(timezone_sign, timezone_hour, timezone_minute)) {
    return false;
  }

  *timezone_offset =
      (timezone_hour * 60 + timezone_minute) * 60 * powers_of_ten[scale];
  if (timezone_sign == '-') {
    *timezone_offset = -*timezone_offset;
  }
  return true;
}

// Parses the string into the relevant timestamp parts.  <scale> indicates
// the number of subsecond digits requested.  Parts not present in the string
// get initialized to 0 or "" as appropriate.  The subsecond part is
// normalized to the requested scale, and if there are extra digits then
// an error is produced.
static absl::Status ParseStringToTimestampParts(
    absl::string_view str, TimestampScale scale, int* year, int* month,
    int* day, int* hour, int* minute, int* second, int* subsecond,
    absl::TimeZone* timezone, bool* string_includes_timezone) {
  int idx = 0;

  // Minimum required length is 8 for a valid timestamp.
  if (!CheckRemainingLength(str, idx, 8 /* remaining_length */) ||
      !ParseDigits(str, 4, 5, &idx, year) || !ParseCharacter(str, '-', &idx) ||
      !ParseDigits(str, 1, 2, &idx, month) || !ParseCharacter(str, '-', &idx) ||
      !ParseDigits(str, 1, 2, &idx, day)) {
    return MakeEvalError() << "Invalid timestamp: '" << str << "'";
  }
  if (idx >= static_cast<int64_t>(str.length()))
    return absl::OkStatus();  // Done consuming <str>.

  // The rest are all optional: time, subseconds, time zone

  // Initial space, 'T', or 't' to kick off the rest.
  if ((!ParseCharacter(str, ' ', &idx) && !ParseCharacter(str, 'T', &idx) &&
       !ParseCharacter(str, 't', &idx)) ||
      !CheckRemainingLength(str, idx, 2 /* remaining_length */)) {
    return MakeEvalError() << "Invalid timestamp: '" << str << "'";
  }

  // Time is '[H]H:[M]M:[S]S'.
  if (absl::ascii_isdigit(str[idx])) {
    if (!ParsePrefixToTimeParts(str, scale, &idx, hour, minute, second,
                                subsecond)) {
      return MakeEvalError() << "Invalid timestamp: '" << str << "'";
    }
    if (idx >= static_cast<int64_t>(str.length()))
      return absl::OkStatus();  // Done consuming <str>.
  } else if (str[idx] != '+' && str[idx] != '-') {
    return MakeEvalError() << "Invalid timestamp: '" << str << "'";
  }

  if (absl::ClippedSubstr(str, idx).empty()) {
    *string_includes_timezone = false;
    return absl::OkStatus();
  }

  *string_includes_timezone = true;
  // Optional UTC prefix for timezone (preceded by exactly one space)
  if (absl::StartsWith(str.substr(idx), " UTC")) {
    idx += 4;
    // If there is no offset after "UTC", it was simply UTC timezone.
    if (absl::ClippedSubstr(str, idx).empty()) {
      *timezone = absl::UTCTimeZone();
      return absl::OkStatus();
    }
  }
  switch (str[idx]) {
    case '+':
    case '-':
      // Canonical time zone form.
      return MakeTimeZone(absl::ClippedSubstr(str, idx), timezone);
    case 'Z':
    case 'z':
      if (idx + 1 != str.size()) {
        // Trailing content after 'Z'.
        return MakeEvalError() << "Invalid timestamp: '" << str << "'";
      }
      // UTC.
      *timezone = absl::UTCTimeZone();
      return absl::OkStatus();
    default:
      break;
  }
  // For a time zone name, it must be a space followed by the name.   Do not
  // allow a space followed by the canonical form (as indicated by a leading
  // '+/-').
  if (str[idx] != ' ' || static_cast<int64_t>(str.size()) < idx + 2 ||
      str[idx + 1] == '+' || str[idx + 1] == '-') {
    return MakeEvalError() << "Invalid timestamp: '" << str << "'";
  }
  ++idx;
  return MakeTimeZone(absl::ClippedSubstr(str, idx), timezone);
}

// Converts timestamps between scales.
static absl::Status ConvertBetweenTimestampsInternal(
    int64_t input, TimestampScale input_scale, TimestampScale output_scale,
    int64_t* output) {
  absl::Status error;
  if (input_scale == output_scale) {
    // No-op case.
    *output = input;
  } else if (output_scale > input_scale) {
    // Widening case.
    int64_t multipler = powers_of_ten[output_scale - input_scale];
    Multiply<int64_t>(input, multipler, output, &error);
  } else {
    // Narrowing case.
    //
    // Converting to a narrowed subsecond type before epoch (1970-01-01
    // 00:00:00) needs some special treatment. This is because the subsecond is
    // represented as 10's complement (negative) for timestamp before epoch.
    //
    // From example:
    //   1969-12-31 23:59:59.123456  -->
    //        -876544 (TIMESTAMP_MICRO)
    //   1969-12-31 23:59:59.123  -->
    //        -877 (TIMESTAMP_MILLIS)
    //  The conversion needs to compensate -1 after the division.
    int64_t dividend = powers_of_ten[input_scale - output_scale];
    if (Divide<int64_t>(input, dividend, output, &error) && input < 0) {
      int64_t r = 0;
      if (Modulo<int64_t>(input, dividend, &r, &error) && r != 0) {
        Subtract<int64_t>(*output, 1, output, &error);
      }
    }
  }
  return error;
}

// Converts a timestamp interval between different scales.
static absl::Status ConvertTimestampInterval(int64_t interval,
                                             TimestampScale interval_scale,
                                             TimestampScale output_scale,
                                             int64_t* output) {
  if (interval_scale == output_scale) {
    *output = interval;
    return absl::OkStatus();
  }

#define FCT(scale1, scale2) (scale1 * 10 + scale2)

  switch (FCT(interval_scale, output_scale)) {
    case FCT(kSeconds, kMilliseconds):
    case FCT(kSeconds, kMicroseconds):
    case FCT(kSeconds, kNanoseconds):
    case FCT(kMilliseconds, kMicroseconds):
    case FCT(kMilliseconds, kNanoseconds):
    case FCT(kMicroseconds, kNanoseconds):
      if (Multiply<int64_t>(interval,
                            powers_of_ten[output_scale - interval_scale],
                            output, kNoError)) {
        return absl::OkStatus();
      }
      break;
    case FCT(kNanoseconds, kMicroseconds):
    case FCT(kNanoseconds, kMilliseconds):
    case FCT(kNanoseconds, kSeconds):
    case FCT(kMicroseconds, kMilliseconds):
    case FCT(kMicroseconds, kSeconds):
    case FCT(kMilliseconds, kSeconds):
      *output = interval / powers_of_ten[interval_scale - output_scale];
      return absl::OkStatus();
    default:
      break;
  }
  return MakeEvalError() << "Converting timestamp interval " << interval
                         << " at " << TimestampScale_Name(interval_scale)
                         << " scale to " << TimestampScale_Name(output_scale)
                         << " scale causes overflow";
}

// Adjust Year/Month/Day for overflow/underflow value.
// Roll year value forward if month is overflow, or backward if month is
// underflow, until month value falls between [1, 12].
// If day value is exceeding the maximum days of the month, day value will be
// truncated to the maximum day of that month, no month roll over.
static void AdjustYearMonthDay(int* year, int* month, int* day) {
  int m = *month % 12;
  *year += *month / 12;
  if (m <= 0) {
    ZETASQL_DCHECK_GT(m, -12);
    m += 12;
    (*year)--;
  }
  *month = m;
  if (IsValidDay(*year, *month, *day)) {
    // This is a valid day, return it.
    return;
  } else {
    // Get the day before the first day of the next month. Note, default
    // CivilDay construction will handle wrap-around (e.g. month = 12).
    absl::CivilDay last_day_of_month = absl::CivilDay(*year, *month + 1, 1) - 1;
    *day = last_day_of_month.day();
  }
}

static absl::Status CheckValidAddTimestampPart(DateTimestampPart part,
                                               bool is_legacy) {
  switch (part) {
    case YEAR:
    case QUARTER:
    case MONTH:
    case WEEK:
      if (!is_legacy) {
        return MakeEvalError()
               << "Unsupported DateTimestampPart "
               << DateTimestampPart_Name(part) << " for TIMESTAMP_ADD";
      }
      ABSL_FALLTHROUGH_INTENDED;
    case DAY:
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
    case NANOSECOND:
      return absl::OkStatus();
    case DATE:
    case DAYOFWEEK:
    case DAYOFYEAR:
    case ISOYEAR:
    case ISOWEEK:
      return MakeEvalError()
             << "Unsupported DateTimestampPart " << DateTimestampPart_Name(part)
             << " for TIMESTAMP_ADD";
    default:
      break;
  }
  return MakeEvalError() << "Unexpected DateTimestampPart "
                         << DateTimestampPart_Name(part)
                         << " for TIMESTAMP_ADD";
}

ABSL_MUST_USE_RESULT static bool MakeDate(int year, int month, int day,
                                          absl::CivilDay* civil_day);

// Returns false if date is out of bounds.
static bool MakeDate(int year, int month, int day, absl::CivilDay* civil_day) {
  if (year < 1 || year > 9999 || !IsValidDay(year, month, day)) {
    return false;
  }
  *civil_day = absl::CivilDay(year, month, day);
  return true;
}

static absl::Status MakeAddTimestampOverflowError(int64_t timestamp,
                                                  DateTimestampPart part,
                                                  int64_t interval,
                                                  TimestampScale scale,
                                                  absl::TimeZone timezone) {
  return MakeEvalError() << "Adding " << interval << " "
                         << DateTimestampPart_Name(part) << " to timestamp "
                         << TimestampErrorString(timestamp, scale, timezone)
                         << " causes overflow";
}

static absl::Status MakeAddTimestampOverflowError(absl::Time timestamp,
                                                  DateTimestampPart part,
                                                  int64_t interval,
                                                  absl::TimeZone timezone) {
  return MakeEvalError() << "Adding " << interval << " "
                         << DateTimestampPart_Name(part) << " to timestamp "
                         << TimestampErrorString(timestamp, timezone)
                         << " causes overflow";
}

static absl::Status MakeSubTimestampOverflowError(int64_t timestamp,
                                                  DateTimestampPart part,
                                                  int64_t interval,
                                                  TimestampScale scale,
                                                  absl::TimeZone timezone) {
  return MakeEvalError() << "Subtracting " << interval << " "
                         << DateTimestampPart_Name(part) << " from timestamp "
                         << TimestampErrorString(timestamp, scale, timezone)
                         << " causes overflow";
}

static absl::Status MakeSubTimestampOverflowError(absl::Time timestamp,
                                                  DateTimestampPart part,
                                                  int64_t interval,
                                                  absl::TimeZone timezone) {
  return MakeEvalError() << "Subtracting " << interval << " "
                         << DateTimestampPart_Name(part) << " from timestamp "
                         << TimestampErrorString(timestamp, timezone)
                         << " causes overflow";
}

// Performs no bounds checking on cs - assumed to be (conceptually) checked
// with IsValidTime.
static bool AddAtLeastDaysToCivilTime(DateTimestampPart part, int32_t interval,
                                      absl::CivilSecond cs, int subsecond,
                                      absl::TimeZone timezone,
                                      TimestampScale scale,
                                      absl::Time* result_timestamp) {
  switch (part) {
    case YEAR: {
      int year;
      // cast is safe, given method contract.
      if (!Add<int32_t>(static_cast<int32_t>(cs.year()), interval, &year,
                        kNoError)) {
        return false;
      }
      int month = cs.month();
      int day = cs.day();
      AdjustYearMonthDay(&year, &month, &day);
      if (!TimestampFromParts(year, month, day, cs.hour(), cs.minute(),
                              cs.second(), subsecond, scale, timezone,
                              result_timestamp)) {
        return false;
      }
      break;
    }
    case QUARTER:
      if (!Multiply<int32_t>(interval, 3, &interval, kNoError)) {
        return false;
      }
      ABSL_FALLTHROUGH_INTENDED;
    case MONTH: {
      int32_t month;
      if (!Add<int32_t>(cs.month(), interval, &month, kNoError)) {
        return false;
      }
      int day = cs.day();
      // cast is safe, given method contract.
      int year = static_cast<int32_t>(cs.year());
      AdjustYearMonthDay(&year, &month, &day);
      if (!TimestampFromParts(year, month, day, cs.hour(), cs.minute(),
                              cs.second(), subsecond, scale, timezone,
                              result_timestamp)) {
        return false;
      }
      break;
    }
    case WEEK:
      if (!Multiply<int32_t>(interval, 7, &interval, kNoError)) {
        return false;
      }
      ABSL_FALLTHROUGH_INTENDED;
    case DAY: {
      absl::CivilDay date;
      // cast is safe, given method contract.
      if (!MakeDate(static_cast<int32_t>(cs.year()), cs.month(), cs.day(),
                    &date)) {
        // This is unreachable since the input timestamp is valid and
        // therefore cannot be outside the Date range bounds.
        return false;
      }
      // This could probably be simplified to avoid bouncing back and forth
      // between civil day, but its not clear how important it is to preserve
      // exact overflow semantics, or how that would interact with
      // absl::CivilDay (which encodes days simply as 'int').
      int days = CivilDayToEpochDays(date);
      if (!Add<int32_t>(days, interval, &days, kNoError)) {
        return false;
      }
      date = EpochDaysToCivilDay(days);
      if (!TimestampFromParts(date.year(), date.month(), date.day(), cs.hour(),
                              cs.minute(), cs.second(), subsecond, scale,
                              timezone, result_timestamp)) {
        return false;
      }
      break;
    }
    default:
      ZETASQL_DCHECK(false) << "Should not reach here";
      return false;
  }
  return true;
}

// TODO:  This implementation requires a datetime to
// be converted to a absl::Time and then to a CivilInfo, in order to
// share the logic for adding DAY and larger intervals with the legacy
// timestamp implementation.  We should consider a more native
// implementation to avoid two conversions, if possible.
static bool AddAtLeastDaysToDatetime(const DatetimeValue& datetime,
                                     DateTimestampPart part,
                                     int64_t interval_in,
                                     DatetimeValue* output) {
  int32_t interval = interval_in;
  if (interval != interval_in) {
    // The interval overflowed an int32_t, for DAY or greater granularity the
    // datetime result would overflow as well.
    return false;
  }
  const absl::TimeZone utc = absl::UTCTimeZone();
  absl::Time timestamp = utc.At(datetime.ConvertToCivilSecond()).pre;
  timestamp += absl::Nanoseconds(datetime.Nanoseconds());

  const absl::TimeZone::CivilInfo info = utc.At(timestamp);

  // cast is safe, guaranteed to be less than 1 billion.
  int subsecond = static_cast<int>(absl::ToInt64Nanoseconds(info.subsecond));

  absl::Time result_timestamp;
  if (!AddAtLeastDaysToCivilTime(part, interval, info.cs, subsecond, utc,
                                 kNanoseconds, &result_timestamp)) {
    return false;
  }

  // Convert the result_timestamp back to datetime for output.
  if (!ConvertTimestampToDatetime(result_timestamp, utc, output).ok()) {
    return false;
  }
  return true;
}

static absl::Status AddDuration(absl::Time timestamp, int64_t interval,
                                DateTimestampPart part, absl::TimeZone timezone,
                                absl::Time* output, bool* had_overflow) {
  switch (part) {
    case HOUR:
      *output = timestamp + absl::Hours(interval);
      break;
    case MINUTE:
      *output = timestamp + absl::Minutes(interval);
      break;
    case SECOND:
      *output = timestamp + absl::Seconds(interval);
      break;
    case MILLISECOND:
      *output = timestamp + absl::Milliseconds(interval);
      break;
    case MICROSECOND:
      *output = timestamp + absl::Microseconds(interval);
      break;
    case NANOSECOND:
      *output = timestamp + absl::Nanoseconds(interval);
      break;
    default:
      break;
  }
  if (!IsValidTime(*output)) {
    *had_overflow = true;
    return MakeAddTimestampOverflowError(timestamp, part, interval, timezone);
  }
  return absl::OkStatus();
}

// The differences between this function and AddTimestampInternal of int64_t
// timestamp are the following:
// Adding an interval of granularity smaller than a day will not cause
// arithmetic overflow. But it returns error status if the <output>
// absl::Time is out of range.
static absl::Status AddTimestampInternal(absl::Time timestamp,
                                         absl::TimeZone timezone,
                                         DateTimestampPart part,
                                         int64_t interval, absl::Time* output,
                                         bool* had_overflow) {
  ZETASQL_RETURN_IF_ERROR(CheckValidAddTimestampPart(part, false /* is_legacy */));
  if (part == DAY) {
    // For TIMESTAMP_ADD(), the DAY interval is equivalent to 24 HOURs.
    part = HOUR;
    int64_t new_interval;
    if (!Multiply<int64_t>(interval, 24, &new_interval, kNoError)) {
      *had_overflow = true;
      return MakeEvalError()
             << "TIMESTAMP_ADD interval value  " << interval << " at "
             << DateTimestampPart_Name(part) << " precision causes overflow";
    }
    interval = new_interval;
  }
  return AddDuration(timestamp, interval, part, timezone, output, had_overflow);
}

static absl::Status AddTimestampNanos(int64_t nanos, absl::TimeZone timezone,
                                      DateTimestampPart part, int64_t interval,
                                      int64_t* output);

// Add interval to Timestamp second, millisecond, microsecond, or nanosecond.
// The caller must verify that the input timestamp is valid.
// Returns error status if the output timestamp is out of range or overflow
// occurs during computation.
static absl::Status AddTimestampInternal(int64_t timestamp,
                                         TimestampScale scale,
                                         absl::TimeZone timezone,
                                         DateTimestampPart part,
                                         int64_t interval, int64_t* output) {
  // Expected invariant.
  ZETASQL_DCHECK(IsValidTimestamp(timestamp, scale));

  ZETASQL_RETURN_IF_ERROR(CheckValidAddTimestampPart(part, false /* is_legacy */));

  // For scale == kNanoseconds, call AddTimestampNanos().
  if (scale == kNanoseconds) {
    return AddTimestampNanos(timestamp, timezone, part, interval, output);
  }

  if (part == DAY) {
    // For TIMESTAMP_ADD(), the DAY interval is equivalent to 24 HOURs.
    part = HOUR;
    int64_t new_interval;
    if (!Multiply<int64_t>(interval, 24, &new_interval, kNoError)) {
      return MakeEvalError()
             << "TIMESTAMP_ADD interval value  " << interval << " at "
             << DateTimestampPart_Name(part) << " precision causes overflow";
    }
    interval = new_interval;
  }

  ZETASQL_RET_CHECK(part == HOUR || part == MINUTE || part == SECOND ||
            part == MILLISECOND || part == MICROSECOND || part == NANOSECOND);

  // If the unit of interval (DateTimestampPart) is HOUR .. NANOSECOND, we can
  // always convert the interval to the input timestamp scale.
  int64_t new_interval = 0;
  switch (part) {
    case HOUR:
    case MINUTE:
    case SECOND: {
      if (part == HOUR) {
        if (!Multiply<int64_t>(interval, kNaiveNumSecondsPerHour, &new_interval,
                               kNoError)) {
          return MakeEvalError() << "TIMESTAMP_ADD interval value  " << interval
                                 << " at " << DateTimestampPart_Name(part)
                                 << " precision causes overflow";
        }
      }
      if (part == MINUTE) {
        if (!Multiply<int64_t>(interval, kNaiveNumSecondsPerMinute,
                               &new_interval, kNoError)) {
          return MakeEvalError() << "TIMESTAMP_ADD interval value  " << interval
                                 << " at " << DateTimestampPart_Name(part)
                                 << " precision causes overflow";
        }
      }
      if (part == SECOND) {
        new_interval = interval;
      }
      // convert interval from second scale to the input timestamp scale.
      ZETASQL_RETURN_IF_ERROR(ConvertTimestampInterval(new_interval, kSeconds, scale,
                                               &new_interval));
      break;
    }
    case MILLISECOND:
      // convert interval from millisecond scale to the input timestamp scale.
      ZETASQL_RETURN_IF_ERROR(ConvertTimestampInterval(interval, kMilliseconds, scale,
                                               &new_interval));
      break;
    case MICROSECOND:
      // convert interval from microsecond scale to the input timestamp scale.
      ZETASQL_RETURN_IF_ERROR(ConvertTimestampInterval(interval, kMicroseconds, scale,
                                               &new_interval));
      break;
    case NANOSECOND:
      // convert interval from nanosecond scale to the input timestamp scale.
      ZETASQL_RETURN_IF_ERROR(ConvertTimestampInterval(interval, kNanoseconds, scale,
                                               &new_interval));
      break;
    default:
      break;
  }
  if (!Add<int64_t>(timestamp, new_interval, output, kNoError) ||
      !IsValidTimestamp(*output, scale)) {
    return MakeAddTimestampOverflowError(timestamp, part, interval, scale,
                                         timezone);
  }
  return absl::OkStatus();
}

// Adds an interval to a nanosecond timestamp value.  AddTimestampInternal
// interprets the interval value at the target precision, and adds it to
// the input timestamp.  Unfortunately for nanoseconds timestamps, interpreting
// the interval value at nanoseconds precision can overflow an int64_t so
// we handle this case separately.  So we perform the addition at microsecond
// precision first, then add the nanosecond parts back in.
static absl::Status AddTimestampNanos(int64_t nanos, absl::TimeZone timezone,
                                      DateTimestampPart part, int64_t interval,
                                      int64_t* output) {
  if (part == NANOSECOND) {
    if (!Add<int64_t>(nanos, interval, output, kNoError)) {
      return MakeEvalError()
             << "Adding " << interval << " NANOs to TIMESTAMP_NANOS value "
             << nanos << " causes overflow";
    }
  } else {
    int64_t micros = nanos / powers_of_ten[3];
    int32_t nano_remains = nanos % powers_of_ten[3];
    int64_t micros_out;
    ZETASQL_RETURN_IF_ERROR(AddTimestampInternal(micros, kMicroseconds, timezone, part,
                                         interval, &micros_out));
    *output = micros_out * 1000l + nano_remains;
    // Given that the micros value is valid, the resulting nanoseconds
    // value must also be valid.  Check this invariant.
    ZETASQL_DCHECK(IsValidTimestamp(*output, kNanoseconds));
  }
  return absl::OkStatus();
}

static void NarrowTimestampIfPossible(int64_t* timestamp,
                                      TimestampScale* scale) {
  while (*timestamp % 1000 == 0) {
    switch (*scale) {
      case kSeconds:
        // Seconds do not narrow.
        return;
      case kMilliseconds:
        *scale = kSeconds;
        break;
      case kMicroseconds:
        *scale = kMilliseconds;
        break;
      case kNanoseconds:
        *scale = kMicroseconds;
        break;
    }
    *timestamp /= 1000;
  }
}

static absl::Status FormatTimestampToStringInternal(
    absl::string_view format_string, absl::Time base_time,
    absl::TimeZone timezone,
    const internal_functions::ExpansionOptions expansion_options,
    std::string* output) {
  if (!IsValidTime(base_time)) {
    return MakeEvalError() << "Invalid timestamp value: "
                           << absl::ToUnixMicros(base_time);
  }
  output->clear();
  absl::TimeZone normalized_timezone =
      internal_functions::GetNormalizedTimeZone(base_time, timezone);
  std::string updated_format_string;
  // We handle %Z, %Q, and %J here instead of passing them through to
  // FormatTime() because ZetaSQL behavior is different than FormatTime()
  // behavior.
  ZETASQL_RETURN_IF_ERROR(internal_functions::ExpandPercentZQJ(
      format_string, base_time, normalized_timezone, expansion_options,
      &updated_format_string));
  *output =
      absl::FormatTime(updated_format_string, base_time, normalized_timezone);
  if (expansion_options.truncate_tz) {
    // If ":00" appears at the end, remove it.  This is consistent with
    // Postgres.
    if (absl::EndsWith(*output, ":00")) {
      output->erase(output->size() - 3);
    }
  }
  return absl::OkStatus();
}

static absl::Status ConvertTimestampToStringInternal(
    int64_t timestamp, TimestampScale scale, absl::TimeZone timezone,
    bool truncate_trailing_zeros, std::string* out) {
  // When converting timestamp to string the result has 0, 3, or 6 digits
  // with trailing sets of three zeros truncated.
  if (truncate_trailing_zeros) {
    NarrowTimestampIfPossible(&timestamp, &scale);
  }
  const absl::Time base_time = MakeTime(timestamp, scale);
  // Note that the DefaultTimestampFormatStr does not use %Q or %J, so the
  // 'expand_quarter' and 'expand_iso_dayofyear' settings in ExpansionOptions
  // are not relevant.
  return FormatTimestampToStringInternal(
      DefaultTimestampFormatStr(scale), base_time, timezone,
      /*expansion_options=*/{.truncate_tz = true}, out);
}

// Returns the absl::Weekday corresponding to 'part', which must be one of the
// WEEK values.
static absl::StatusOr<absl::Weekday> GetFirstWeekDayOfWeek(
    DateTimestampPart part) {
  switch (part) {
    case WEEK:
      return absl::Weekday::sunday;
    case ISOWEEK:
    case WEEK_MONDAY:
      return absl::Weekday::monday;
    case WEEK_TUESDAY:
      return absl::Weekday::tuesday;
    case WEEK_WEDNESDAY:
      return absl::Weekday::wednesday;
    case WEEK_THURSDAY:
      return absl::Weekday::thursday;
    case WEEK_FRIDAY:
      return absl::Weekday::friday;
    case WEEK_SATURDAY:
      return absl::Weekday::saturday;
    default:
      return MakeEvalError()
             << "Unexpected date part " << DateTimestampPart_Name(part);
  }
}

// Does not do bounds checking. base_time (in the given timezone)
// must be guaranteed valid.
static absl::Status ExtractFromTimestampInternal(DateTimestampPart part,
                                                 absl::Time base_time,
                                                 absl::TimeZone timezone,
                                                 int32_t* output) {
  const absl::TimeZone::CivilInfo info = timezone.At(base_time);

  switch (part) {
    case YEAR:
      // Given contract of this method, year must be 'small'.
      *output = static_cast<int32_t>(info.cs.year());
      break;
    case MONTH:
      *output = info.cs.month();
      break;
    case DAY:
      *output = info.cs.day();
      break;
    case DAYOFWEEK:
      *output = internal_functions::DayOfWeekIntegerSunToSat1To7(
          absl::GetWeekday(info.cs));
      break;
    case DAYOFYEAR:
      *output = absl::GetYearDay(absl::CivilDay(info.cs));
      break;
    case QUARTER:
      *output = (info.cs.month() - 1) / 3 + 1;
      break;
    case DATE: {
      const int32_t date = CivilDayToEpochDays(absl::CivilDay(info.cs));
      if (!IsValidDate(date)) {
        // Error handling.
        std::string time_str;
        if (ConvertTimestampToString(base_time, kNanoseconds, timezone,
                                     &time_str)
                .ok()) {
          return MakeEvalError()
                 << "Invalid date extracted from timestamp " << time_str;
        }
        // Most likely should never happen.
        return MakeEvalError() << "Invalid date extracted from timestamp "
                               << absl::FormatTime(base_time, timezone);
      }
      *output = date;
      break;
    }
    case WEEK:  // _SUNDAY
    case WEEK_MONDAY:
    case WEEK_TUESDAY:
    case WEEK_WEDNESDAY:
    case WEEK_THURSDAY:
    case WEEK_FRIDAY:
    case WEEK_SATURDAY: {
      const absl::CivilDay first_calendar_day_of_year(info.cs.year(), 1, 1);

      ZETASQL_ASSIGN_OR_RETURN(const absl::Weekday weekday,
                       GetFirstWeekDayOfWeek(part));
      const absl::CivilDay effective_first_day_of_year =
          NextWeekdayOrToday(first_calendar_day_of_year, weekday);

      const absl::CivilDay day(info.cs);
      if (day < effective_first_day_of_year) {
        *output = 0;
      } else {
        // cast is safe, guaranteed to be less than 52.
        *output =
            static_cast<int32_t>(((day - effective_first_day_of_year) / 7) + 1);
      }
      break;
    }
    case ISOYEAR:
      // cast is safe, year "guaranteed" safe by method contract.
      *output = static_cast<int32_t>(GetIsoYear(absl::CivilDay(info.cs)));
      break;
    case ISOWEEK: {
      *output = GetIsoWeek(absl::CivilDay(info.cs));
      break;
    }
    case HOUR:
      *output = info.cs.hour();
      break;
    case MINUTE:
      *output = info.cs.minute();
      break;
    case SECOND:
      *output = info.cs.second();
      break;
    case MILLISECOND:
      // cast is safe, guaranteed to be less than 1 thousand.
      *output = static_cast<int32_t>(absl::ToInt64Milliseconds(info.subsecond));
      break;
    case MICROSECOND:
      // cast is safe, guaranteed to be less than 1 million.
      *output = static_cast<int32_t>(absl::ToInt64Microseconds(info.subsecond));
      break;
    case NANOSECOND:
      // cast is safe, guaranteed to be less than 1 billion.
      *output = static_cast<int32_t>(absl::ToInt64Nanoseconds(info.subsecond));
      break;
    default:
      return MakeEvalError()
             << "Unexpected DateTimestampPart " << DateTimestampPart_Name(part);
  }
  return absl::OkStatus();
}

static absl::Status MakeAddDateOverflowError(int32_t date,
                                             DateTimestampPart part,
                                             int64_t interval) {
  return MakeEvalError() << "Adding " << interval << " "
                         << DateTimestampPart_Name(part) << " to date "
                         << DateErrorString(date) << " causes overflow";
}

static absl::Status MakeSubDateOverflowError(int32_t date,
                                             DateTimestampPart part,
                                             int64_t interval) {
  return MakeEvalError() << "Subtracting " << interval << " "
                         << DateTimestampPart_Name(part) << " from date "
                         << DateErrorString(date) << " causes overflow";
}

static absl::Status MakeAddDatetimeOverflowError(const DatetimeValue& datetime,
                                                 DateTimestampPart part,
                                                 int64_t interval) {
  return MakeEvalError() << "Adding " << interval << " "
                         << DateTimestampPart_Name(part) << " to datetime "
                         << datetime.DebugString() << " causes overflow";
}

static absl::Status MakeSubDatetimeOverflowError(const DatetimeValue& datetime,
                                                 DateTimestampPart part,
                                                 int64_t interval) {
  return MakeEvalError() << "Subtracting " << interval << " "
                         << DateTimestampPart_Name(part) << " from datetime "
                         << datetime.DebugString() << " causes overflow";
}

static std::string MakeInvalidTypedStrErrorMsg(absl::string_view type_name,
                                               absl::string_view str,
                                               TimestampScale scale) {
  return absl::StrCat(
      "Invalid ", type_name, " string \"", str, "\"",
      (scale != kMicroseconds
           ? absl::StrCat(" (scale ", TimestampScale_Name(scale), ")")
           : ""));
}

static std::string MakeInvalidTimestampStrErrorMsg(
    absl::string_view timestamp_str, TimestampScale scale) {
  return MakeInvalidTypedStrErrorMsg("timestamp", timestamp_str, scale);
}

static absl::Status TruncateDateImpl(int32_t date, DateTimestampPart part,
                                     bool enforce_range, int32_t* output) {
  if (!IsValidDate(date)) {
    return MakeEvalError() << "Invalid date value: " << date;
  }
  absl::CivilDay civil_day = EpochDaysToCivilDay(date);
  switch (part) {
    case YEAR:
      *output = CivilDayToEpochDays(absl::CivilDay(civil_day.year(), 1, 1));
      break;
    case ISOYEAR: {
      *output = CivilDayToEpochDays(
          date_time_util_internal::GetFirstDayOfIsoYear(civil_day));
      break;
    }
    case MONTH: {
      *output = CivilDayToEpochDays(
          absl::CivilDay(civil_day.year(), civil_day.month(), 1));
      break;
    }
    case QUARTER: {
      int m = civil_day.month();
      m = (m - 1) / 3 * 3 + 1;
      *output = CivilDayToEpochDays(absl::CivilDay(civil_day.year(), m, 1));
      break;
    }
    case WEEK:
    case ISOWEEK:
    case WEEK_MONDAY:
    case WEEK_TUESDAY:
    case WEEK_WEDNESDAY:
    case WEEK_THURSDAY:
    case WEEK_FRIDAY:
    case WEEK_SATURDAY: {
      ZETASQL_ASSIGN_OR_RETURN(const absl::Weekday first_day_of_week,
                       GetFirstWeekDayOfWeek(part));
      *output =
          CivilDayToEpochDays(PrevWeekdayOrToday(civil_day, first_day_of_week));
      break;
    }
    case DAY:
      *output = date;  // nothing to truncate.
      break;
    default:
      return MakeEvalError() << "Unsupported DateTimestampPart "
                             << DateTimestampPart_Name(part);
  }
  // Truncating to WEEK and WEEK(<WEEKDAY>) can result in a date that is out
  // of bounds (i.e., before 0001-01-01), so we check the truncated date
  // result here. The other date parts do not have the potential to underflow,
  // but we validate the result anyway as a sanity check.
  if (enforce_range && !IsValidDate(*output)) {
    return MakeEvalError() << "Truncating date " << DateErrorString(date)
                           << " to " << DateTimestampPartToSQL(part)
                           << " resulted in an out of range date value: "
                           << *output;
  }
  return absl::OkStatus();
}

absl::Status LastDayOfDate(int32_t date, DateTimestampPart part,
                           int32_t* output) {
  if (!IsValidDate(date)) {
    return MakeEvalError() << "Invalid date value: " << date;
  }
  absl::CivilDay civil_day = EpochDaysToCivilDay(date);
  // For YEAR, MONTH, QUARTER, last_day is implemented by first calling AddDate
  // to add 1 to the part of the date, then truncate the date in the part,
  // finally subtract 1 day
  // However, AddDate may overflow for some input dates that can return a valid
  // last_day result, so those cases are handled first.
  if (civil_day.year() == 9999) {
    if (part == YEAR || (part == MONTH && civil_day.month() == 12) ||
        (part == QUARTER && civil_day.month() >= 10 &&
         civil_day.month() <= 12)) {
      *output = CivilDayToEpochDays(absl::CivilDay(9999, 12, 31));
      return absl::OkStatus();
    }
  }
  switch (part) {
    case YEAR:
    case MONTH:
    case QUARTER: {
      ZETASQL_RETURN_IF_ERROR(AddDate(date, part, 1, &date));
      ZETASQL_RETURN_IF_ERROR(
          TruncateDateImpl(date, part, /*enforce_range=*/false, &date));
      ZETASQL_RETURN_IF_ERROR(SubDate(date, DAY, 1, output));
      break;
    }
    case ISOYEAR: {
      // last day of ISOYEAR could out of range if the input is 9999,
      // the last day of ISOYEAR 9999 is 10000-01-02
      *output = CivilDayToEpochDays(
          date_time_util_internal::GetLastDayOfIsoYear(civil_day));
      break;
    }
    case WEEK:
    case ISOWEEK:
    case WEEK_MONDAY:
    case WEEK_TUESDAY:
    case WEEK_WEDNESDAY:
    case WEEK_THURSDAY:
    case WEEK_FRIDAY:
    case WEEK_SATURDAY: {
      ZETASQL_ASSIGN_OR_RETURN(const absl::Weekday first_day_of_week,
                       GetFirstWeekDayOfWeek(part));
      *output = CivilDayToEpochDays(
                    PrevWeekdayOrToday(civil_day, first_day_of_week)) +
                6;
      break;
    }
    default:
      return MakeEvalError() << "Unsupported DateTimestampPart "
                             << DateTimestampPart_Name(part);
  }
  // ISO_YEAR, WEEK and WEEK(<WEEKDAY>) as part can result in a date that is out
  // of bounds (i.e., after 9999-12-31), so we check the last day
  // result here. The other date parts do not have the potential to underflow,
  // but we validate the result anyway as a sanity check.
  if (!IsValidDate(*output)) {
    return MakeEvalError() << "Last day of date " << DateErrorString(date)
                           << " to " << DateTimestampPartToSQL(part)
                           << " resulted in an out of range date value: "
                           << *output;
  }
  return absl::OkStatus();
}

absl::Status LastDayOfDatetime(const DatetimeValue& datetime,
                               DateTimestampPart part, int32_t* output) {
  int32_t date;
  ZETASQL_RETURN_IF_ERROR(ExtractFromDatetime(DATE, datetime, &date));
  ZETASQL_RETURN_IF_ERROR(LastDayOfDate(date, part, output));
  return absl::OkStatus();
}

static absl::Status TimestampTruncAtLeastMinute(absl::Time timestamp,
                                                TimestampScale scale,
                                                absl::TimeZone timezone,
                                                DateTimestampPart part,
                                                absl::Time* output) {
  const absl::TimeZone::CivilInfo info = timezone.At(timestamp);

  // Given a valid input timestamp, truncation should never result in failure
  // when reconstructing the timestamp from its parts, so ZETASQL_RET_CHECK the results.
  switch (part) {
    case YEAR:
      ZETASQL_RET_CHECK(TimestampFromParts(info.cs.year(), 1 /* month */, 1 /* mday */,
                                   0 /* hour */, 0 /* minute */, 0 /* second */,
                                   0 /* subsecond */, scale, timezone, output));
      return absl::OkStatus();
    case ISOYEAR: {
      absl::CivilDay day = absl::CivilDay(info.cs);
      if (!IsValidCivilDay(day)) {
        return MakeEvalError()
               << "Invalid date value: " << CivilDayToEpochDays(day);
      }
      absl::CivilDay iso_civil_day =
          date_time_util_internal::GetFirstDayOfIsoYear(day);

      ZETASQL_RET_CHECK(TimestampFromParts(iso_civil_day.year(), iso_civil_day.month(),
                                   iso_civil_day.day(), 0 /* hour */,
                                   0 /* minute */, 0 /* second */,
                                   0 /* subsecond */, scale, timezone, output));
      return absl::OkStatus();
    }
    case QUARTER:
      ZETASQL_RET_CHECK(TimestampFromParts(
          info.cs.year(), (info.cs.month() - 1) / 3 * 3 + 1, 1 /* mday */,
          0 /* hour */, 0 /* minute */, 0 /* second */, 0 /* subsecond */,
          scale, timezone, output));
      return absl::OkStatus();
    case MONTH:
      ZETASQL_RET_CHECK(TimestampFromParts(info.cs.year(), info.cs.month(),
                                   1 /* mday */, 0 /* hour */, 0 /* minute */,
                                   0 /* second */, 0 /* subsecond */, scale,
                                   timezone, output));
      return absl::OkStatus();
    case WEEK:
    case ISOWEEK:
    case WEEK_MONDAY:
    case WEEK_TUESDAY:
    case WEEK_WEDNESDAY:
    case WEEK_THURSDAY:
    case WEEK_FRIDAY:
    case WEEK_SATURDAY: {
      // Convert to a CivilDay...
      ZETASQL_ASSIGN_OR_RETURN(absl::Weekday weekday, GetFirstWeekDayOfWeek(part));
      absl::CivilDay week_truncated_day =
          PrevWeekdayOrToday(absl::CivilDay(info.cs), weekday);
      if (week_truncated_day.year() < 1) {
        return MakeEvalError()
               << "Truncating " << TimestampErrorString(timestamp, timezone)
               << " to the nearest week causes overflow";
      }
      ZETASQL_RET_CHECK(TimestampFromParts(
          week_truncated_day.year(), week_truncated_day.month(),
          week_truncated_day.day(), 0 /* hour */, 0 /* minute */,
          0 /* second */, 0 /* subsecond */, scale, timezone, output));
      return absl::OkStatus();
    }
    case DAY:
      ZETASQL_RET_CHECK(TimestampFromParts(info.cs.year(), info.cs.month(),
                                   info.cs.day(), 0 /* hour */, 0 /* minute */,
                                   0 /* second */, 0 /* subsecond */, scale,
                                   timezone, output));
      return absl::OkStatus();
    case HOUR:
    case MINUTE: {
      // For HOUR or MINUTE truncation of a timestamp, we identify the
      // minute/second/subsecond parts as viewed via the specified time zone,
      // and subtract that interval from the timestamp.
      //
      // This operation is performed by adding the timezone seconds offset from
      // UTC (given the input timestamp) to 'align' the minute/seconds parts
      // for truncation.  The minute/seconds parts can then be effectively
      // truncated from the value.  We then re-adjust the result timestamp by
      // the seconds offset value.
      //
      // Note that with these semantics, when you truncate a timestamp based
      // on a specified time zone and then convert the resulting truncated
      // timestamp to a civil time in that same time zone, you are not
      // guaranteed to get a civil time that is at the hour or minute boundary
      // in that time zone.  For instance, this will happen when the truncation
      // crosses a DST change and the DST change is not a (multiple of an)
      // hour.  See the unit tests for specific examples.

      // Note that as documented, usage of CivilInfo.offset is discouraged.
      const int64_t seconds_offset_east_of_UTC = info.offset;

      int64_t timestamp_seconds;
      // Note: This conversion truncates the subseconds part
      ZETASQL_RET_CHECK(FromTime(timestamp, kSeconds, &timestamp_seconds))
          << "timestamp: " << timestamp
          << ", scale: seconds, timestamp_seconds: " << timestamp_seconds;

      // Adjust the timestamp by the time zone offset.
      timestamp_seconds += seconds_offset_east_of_UTC;

      // Truncate seconds from the timestamp to the hour/minute boundary
      const int64_t truncate_granularity = (part == HOUR ? 3600 : 60);
      int64_t num_seconds_to_truncate =
          timestamp_seconds % truncate_granularity;
      if (num_seconds_to_truncate < 0) {
        num_seconds_to_truncate += truncate_granularity;
      }
      timestamp_seconds -= num_seconds_to_truncate;

      // Re-adjust the timestamp by the time zone offset.
      timestamp_seconds -= seconds_offset_east_of_UTC;
      *output = MakeTime(timestamp_seconds, kSeconds);
      return absl::OkStatus();
    }
    case DATE:
    case DAYOFWEEK:
    case DAYOFYEAR:
      return MakeEvalError() << "Unsupported DateTimestampPart "
                             << DateTimestampPart_Name(part);
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
    case NANOSECOND:
      ZETASQL_RET_CHECK_FAIL() << "Should not reach here for part="
                       << DateTimestampPart_Name(part);
    default:
      return MakeEvalError()
             << "Unexpected DateTimestampPart " << DateTimestampPart_Name(part);
  }
}

static absl::Status TimestampTruncImpl(int64_t timestamp, TimestampScale scale,
                                       NewOrLegacyTimestampType timestamp_type,
                                       absl::TimeZone timezone,
                                       DateTimestampPart part,
                                       int64_t* output) {
  if (!IsValidTimestamp(timestamp, scale)) {
    return MakeEvalError() << "Invalid timestamp value: " << timestamp;
  }
  switch (scale) {
    case kSeconds:
      ZETASQL_RET_CHECK_EQ(LEGACY_TIMESTAMP_TYPE, timestamp_type);
      if (part == SECOND) {
        *output = timestamp;  // nothing to truncate;
        return absl::OkStatus();
      }
      if (part == MILLISECOND || part == MICROSECOND || part == NANOSECOND) {
        return MakeEvalError()
               << "Cannot truncate a TIMESTAMP_SECONDS value to "
               << DateTimestampPart_Name(part);
      }
      break;
    case kMilliseconds:
      ZETASQL_RET_CHECK_EQ(LEGACY_TIMESTAMP_TYPE, timestamp_type);
      if (part == SECOND) {
        // Truncating subsecond of a timestamp before epoch
        // (1970-01-01 00:00:00) to another subsecond needs special
        // treatment.  This is because the subsecond is represented as 10's
        // complement (negative) for timestamp before epoch.
        // From example:
        //   1969-12-31 23:59:59.123456  -->
        //        -876544 (TIMESTAMP_MICRO)
        //   1969-12-31 23:59:59.123  -->
        //        -877 (TIMESTAMP_MILLIS)
        // The truncation needs to compensate by -1 after the division if the
        // truncated part is not zero.
        *output = timestamp / powers_of_ten[3];
        if (timestamp < 0 && timestamp % powers_of_ten[3] != 0) {
          *output -= 1;
        }
        *output *= powers_of_ten[3];
        return absl::OkStatus();
      }
      if (part == MILLISECOND) {
        *output = timestamp;  // nothing to truncate;
        return absl::OkStatus();
      }
      if (part == MICROSECOND || part == NANOSECOND) {
        return MakeEvalError() << "Cannot truncate a TIMESTAMP_MILLIS value to "
                               << DateTimestampPart_Name(part);
      }
      break;
    case kMicroseconds:
      // This could be either the new TIMESTAMP type or the legacy
      // TIMESTAMP_MICROS type.
      if (part == SECOND) {
        *output = timestamp / powers_of_ten[6];
        if (timestamp < 0 && timestamp % powers_of_ten[6] != 0) {
          *output -= 1;
        }
        *output *= powers_of_ten[6];
        return absl::OkStatus();
      }
      if (part == MILLISECOND) {
        *output = timestamp / powers_of_ten[3];
        if (timestamp < 0 && timestamp % powers_of_ten[3] != 0) {
          *output -= 1;
        }
        *output *= powers_of_ten[3];
        return absl::OkStatus();
      }
      if (part == MICROSECOND) {
        *output = timestamp;  // nothing to truncate;
        return absl::OkStatus();
      }
      if (part == NANOSECOND) {
        return MakeEvalError()
               << "Cannot truncate a "
               << (timestamp_type == LEGACY_TIMESTAMP_TYPE ? "TIMESTAMP_MICROS"
                                                           : "TIMESTAMP")
               << " value to " << DateTimestampPart_Name(part);
      }
      break;
    case kNanoseconds:
      ZETASQL_RET_CHECK_EQ(LEGACY_TIMESTAMP_TYPE, timestamp_type);
      if (part == SECOND) {
        *output = timestamp / powers_of_ten[9];
        if (timestamp < 0 && timestamp % powers_of_ten[9] != 0) {
          *output -= 1;
        }
        *output *= powers_of_ten[9];
        return absl::OkStatus();
      }
      if (part == MILLISECOND) {
        *output = timestamp / powers_of_ten[6];
        if (timestamp < 0 && timestamp % powers_of_ten[6] != 0) {
          *output -= 1;
        }
        *output *= powers_of_ten[6];
        return absl::OkStatus();
      }
      if (part == MICROSECOND) {
        *output = timestamp / powers_of_ten[3];
        if (timestamp < 0 && timestamp % powers_of_ten[3] != 0) {
          *output -= 1;
        }
        *output *= powers_of_ten[3];
        return absl::OkStatus();
      }
      if (part == NANOSECOND) {
        *output = timestamp;  // nothing to truncate;
        return absl::OkStatus();
      }
      break;
  }
  const absl::Time base_time = MakeTime(timestamp, scale);
  absl::Time output_base_time;
  ZETASQL_RETURN_IF_ERROR(TimestampTruncAtLeastMinute(base_time, scale, timezone, part,
                                              &output_base_time));
  // In this case we know we have a valid timestamp input to the function, so
  // the truncated timestamp must be valid as well.
  ZETASQL_RET_CHECK(FromTime(output_base_time, scale, output))
      << "base_time: " << base_time
      << "\noutput_base_time: " << output_base_time << ", scale: " << scale
      << ", output: " << *output;
  return absl::OkStatus();
}

bool IsValidTimestamp(int64_t timestamp, TimestampScale scale) {
  switch (scale) {
    case kSeconds:
      return timestamp >= zetasql::types::kTimestampMin / 1000000 &&
             timestamp <= zetasql::types::kTimestampMax / 1000000;
    case kMilliseconds:
      return timestamp >= zetasql::types::kTimestampMin / 1000 &&
             timestamp <= zetasql::types::kTimestampMax / 1000;
    case kMicroseconds:
      return timestamp >= zetasql::types::kTimestampMin &&
             timestamp <= zetasql::types::kTimestampMax;
    case kNanoseconds:
      // There is no invalid range for int64_t timestamps with nanoseconds scale
      return true;
  }
}

bool IsValidTime(absl::Time time) {
  static const absl::Time kTimeMin =
      absl::FromUnixMicros(zetasql::types::kTimestampMin);
  static const absl::Time kTimeMax =
      absl::FromUnixMicros(zetasql::types::kTimestampMax + 1);
  return time >= kTimeMin && time < kTimeMax;
}

bool IsValidTimeZone(int timezone_minutes_offset) {
  const int64_t kTimezoneOffsetMin = -60 * 14;
  const int64_t kTimezoneOffsetMax = 60 * 14;
  return timezone_minutes_offset >= kTimezoneOffsetMin &&
         timezone_minutes_offset <= kTimezoneOffsetMax;
}

absl::Time MakeTime(int64_t timestamp, TimestampScale scale) {
  switch (scale) {
    case kSeconds:
      return absl::FromUnixSeconds(timestamp);
    case kMilliseconds:
      return absl::FromUnixMillis(timestamp);
    case kMicroseconds:
      return absl::FromUnixMicros(timestamp);
    case kNanoseconds:
      return absl::FromUnixNanos(timestamp);
  }
}

bool FromTime(absl::Time base_time, TimestampScale scale, int64_t* output) {
  switch (scale) {
    case kSeconds:
      *output = absl::ToUnixSeconds(base_time);
      break;
    case kMilliseconds:
      *output = absl::ToUnixMillis(base_time);
      break;
    case kMicroseconds:
      *output = absl::ToUnixMicros(base_time);
      break;
    case kNanoseconds: {
      if (base_time <
              absl::FromUnixNanos(std::numeric_limits<int64_t>::lowest()) ||
          base_time >
              absl::FromUnixNanos(std::numeric_limits<int64_t>::max())) {
        return false;
      }
      *output = absl::ToUnixNanos(base_time);
      break;
    }
  }

  return functions::IsValidTimestamp(*output, scale);
}

absl::Status ConvertDateToString(int32_t date, std::string* out) {
  if (!IsValidDate(date)) {
    return MakeEvalError() << "Invalid date value: " << date;
  }
  absl::CivilDay day = EpochDaysToCivilDay(date);
  *out = absl::StrFormat("%04d-%02d-%02d", day.year(), day.month(), day.day());
  return absl::OkStatus();
}

absl::StatusOr<absl::CivilDay> ConvertDateToCivilDay(int32_t date) {
  if (!IsValidDate(date)) {
    return MakeEvalError() << "Invalid date value: " << date;
  }
  return EpochDaysToCivilDay(date);
}

absl::Status ConvertTimestampToStringWithoutTruncation(int64_t timestamp,
                                                       TimestampScale scale,
                                                       absl::TimeZone timezone,
                                                       std::string* out) {
  return ConvertTimestampToStringInternal(
      timestamp, scale, timezone, false /* truncate_trailing_zeros */, out);
}

absl::Status ConvertTimestampToStringWithoutTruncation(
    int64_t timestamp, TimestampScale scale, absl::string_view timezone_string,
    std::string* out) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return ConvertTimestampToStringWithoutTruncation(timestamp, scale, timezone,
                                                   out);
}

// ConvertTimestampToStringWithTruncation is a popular function, because it is
// used for CASTs from TIMESTAMP to STRING, and in STRING conversion. This
// function is optimized for this conversion by hardcoding the rules of applying
// "%E4Y-%m-%d %H:%M:%E(0|3|6)S%Ez" formatting.
absl::Status ConvertTimestampMicrosToStringWithTruncation(
    int64_t timestamp, absl::TimeZone timezone, std::string* out) {
  absl::Time time = absl::FromUnixMicros(timestamp);
  if (ABSL_PREDICT_FALSE(!IsValidTime(time))) {
    return MakeEvalError() << "Invalid timestamp value: " << timestamp;
  }
  absl::TimeZone normalized_timezone =
      internal_functions::GetNormalizedTimeZone(time, timezone);

  absl::TimeZone::CivilInfo info = normalized_timezone.At(time);

  // YYYY-mm-dd HH:MM:SS.ssssss+oo:oo
  // 01234567890123456789012345678901
  // 0         1         2         3
  // Maximum possible string size for the result.
  static constexpr size_t kFormatYMDHMSSize = 32;
  out->resize(kFormatYMDHMSSize);
  char* data = out->data();

  // YYYY-mm-dd HH:MM:SS.ssssss+oooo
  // 0123456789012345678901234567890
  // 0         1         2
  // Carefully put characters corresponding to different parts of formatted
  // string to the right positions.
  data[0] = info.cs.year() / 1000 + '0';
  data[1] = (info.cs.year() % 1000) / 100 + '0';
  data[2] = (info.cs.year() % 100) / 10 + '0';
  data[3] = info.cs.year() % 10 + '0';
  data[4] = '-';
  data[5] = info.cs.month() / 10 + '0';
  data[6] = info.cs.month() % 10 + '0';
  data[7] = '-';
  data[8] = info.cs.day() / 10 + '0';
  data[9] = info.cs.day() % 10 + '0';
  data[10] = ' ';
  data[11] = info.cs.hour() / 10 + '0';
  data[12] = info.cs.hour() % 10 + '0';
  data[13] = ':';
  data[14] = info.cs.minute() / 10 + '0';
  data[15] = info.cs.minute() % 10 + '0';
  data[16] = ':';
  data[17] = info.cs.second() / 10 + '0';
  data[18] = info.cs.second() % 10 + '0';

  // Deal with subsecond truncation to 0, 3 or 6 digits.
  size_t pos = 19;
  int64_t sub_seconds = timestamp % 1000000;
  if (sub_seconds < 0) {
    sub_seconds += 1000000;
  }
  if (sub_seconds > 0) {
    data[19] = '.';
    if (sub_seconds % 1000 > 0) {
      for (int i = 5; i >= 0; i--) {
        data[20 + i] = (sub_seconds % 10) + '0';
        sub_seconds /= 10;
      }
      pos += 7;
    } else {
      sub_seconds /= 1000;
      for (int i = 2; i >= 0; i--) {
        data[20 + i] = (sub_seconds % 10) + '0';
        sub_seconds /= 10;
      }
      pos += 4;
    }
  }

  bool positive_offset;
  int32_t hour_offset;
  int32_t minute_offset;
  internal_functions::GetSignHourAndMinuteTimeZoneOffset(
      info, &positive_offset, &hour_offset, &minute_offset);
  if (positive_offset) {
    data[pos++] = '+';
  } else {
    data[pos++] = '-';
  }
  // Write timezone offset using HH:mm format
  data[pos++] = hour_offset / 10 + '0';
  data[pos++] = hour_offset % 10 + '0';
  if (minute_offset > 0) {
    data[pos++] = ':';
    data[pos++] = minute_offset / 10 + '0';
    data[pos++] = minute_offset % 10 + '0';
  }
  // Initially we resized result for maximum possible size, here we set actual
  // size.
  out->resize(pos);
  return absl::OkStatus();
}

absl::Status ConvertTimestampToStringWithTruncation(int64_t timestamp,
                                                    TimestampScale scale,
                                                    absl::TimeZone timezone,
                                                    std::string* out) {
  return ConvertTimestampToStringInternal(
      timestamp, scale, timezone, true /* truncate_trailing_zeros */, out);
}

absl::Status ConvertTimestampToStringWithTruncation(
    int64_t timestamp, TimestampScale scale, absl::string_view timezone_string,
    std::string* out) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return ConvertTimestampToStringWithTruncation(timestamp, scale, timezone,
                                                out);
}

absl::Status ConvertTimeToString(TimeValue time, TimestampScale scale,
                                 std::string* out) {
  ZETASQL_RET_CHECK(scale == kMicroseconds || scale == kNanoseconds)
      << "Only kMicroseconds and kNanoseconds are acceptable values for scale";
  if (!time.IsValid()) {
    return MakeEvalError() << "Invalid time value: " << time.DebugString();
  }
  int64_t fraction_second =
      (scale == kMicroseconds ? time.Microseconds() : time.Nanoseconds());
  NarrowTimestampIfPossible(&fraction_second, &scale);
  std::unique_ptr<absl::ParsedFormat<'d', 'd', 'd', 'd'>> format =
      absl::ParsedFormat<'d', 'd', 'd', 'd'>::NewAllowIgnored(
          DefaultTimeFormatStr(scale));
  ZETASQL_RET_CHECK(format != nullptr);
  *out = absl::StrFormat(*format, time.Hour(), time.Minute(), time.Second(),
                         fraction_second);
  return absl::OkStatus();
}

absl::Status ConvertDatetimeToString(DatetimeValue datetime,
                                     TimestampScale scale, std::string* out) {
  ZETASQL_RET_CHECK(scale == kMicroseconds || scale == kNanoseconds)
      << "Only kMicroseconds and kNanoseconds are acceptable values for scale";
  if (!datetime.IsValid()) {
    return MakeEvalError() << "Invalid datetime value: "
                           << datetime.DebugString();
  }
  int64_t fraction_second = (scale == kMicroseconds ? datetime.Microseconds()
                                                    : datetime.Nanoseconds());
  NarrowTimestampIfPossible(&fraction_second, &scale);
  auto format =
      absl::ParsedFormat<'d', 'd', 'd', 'd', 'd', 'd', 'd'>::NewAllowIgnored(
          DefaultDatetimeFormatStr(scale));
  ZETASQL_RET_CHECK(format != nullptr);
  *out = absl::StrFormat(*format, datetime.Year(), datetime.Month(),
                         datetime.Day(), datetime.Hour(), datetime.Minute(),
                         datetime.Second(), fraction_second);
  return absl::OkStatus();
}

// Sanitizes the <format_string> so that any character in the
// <elements_to_escape> will be escaped and pass through as is. If an element
// should be escaped, then any extension applied on it with %E or %O will also
// be escaped.
static void SanitizeFormat(absl::string_view format_string,
                           const char* elements_to_escape, std::string* out) {
  const char* cur = format_string.data();
  const char* pending = cur;
  const char* end = cur + format_string.size();

  while (cur != end) {
    // Moves cur to the next percent sign.
    while (cur != end && *cur != '%') ++cur;

    // Span the sequential percent signs.
    const char* percent = cur;
    while (cur != end && *cur == '%') ++cur;

    if (cur != pending) {
      out->append(pending, cur - pending);
      pending = cur;
    }

    // Loop unless we have an unescaped percent.
    if (cur == end || (cur - percent) % 2 == 0) {
      continue;
    }

    // Escape width modifier.
    while (cur != end && absl::ascii_isdigit(*cur)) ++cur;

    // Checks if the format should be escaped
    if (cur != end && strchr(elements_to_escape, *cur)) {
      out->push_back('%');  // Escape
      out->append(pending, ++cur - pending);
      pending = cur;
      continue;
    }

    if (cur != end) {
      if ((*cur != 'E' && *cur != 'O') || ++cur == end) {
        out->push_back(*pending++);
        continue;
      }
      if (*pending == 'E') {
        // Check %E extensions.
        if (strchr(elements_to_escape, *cur) ||
            // If %S (second) should be escaped, then %E#S and %E*S should also
            // be escaped.
            (strchr(elements_to_escape, 'S') &&
             ((*cur == '*' || absl::ascii_isdigit(*cur)) && ++cur != end &&
              *cur == 'S')) ||
            // If %Y (year) should be escaped, then %E4Y should also be escaped.
            (strchr(elements_to_escape, 'Y') && *cur == '4' && ++cur != end &&
             *cur == 'Y')) {
          cur++;
          out->push_back('%');  // Escape
        }
      } else if (*pending == 'O') {
        // Check %O extensions.
        if (strchr(elements_to_escape, *cur)) {
          cur++;
          out->push_back('%');  // Escape
        }
      }
    }

    if (cur != pending) {
      out->append(pending, cur - pending);
      pending = cur;
    }
  }
}

// Sanitizes the <format_string> to only allow ones applicable to the date type
// and produces a valid format string for date in <out>.
// For non-date related formats such as Hour/Minute/Second/Timezone etc.,
// escapes them to pass through as is.
static void SanitizeDateFormat(absl::string_view format_string,
                               std::string* out) {
  return SanitizeFormat(format_string, "cHIklMPpRrSsTXZz", out);
}

// Similar to SanitizeDateFormat, but escape those format elements for
// Year/Month/Week/Day/Timezone etc..
static void SanitizeTimeFormat(absl::string_view format_string,
                               std::string* out) {
  return SanitizeFormat(format_string, "AaBbhCcDdeFGgjmQsUuVWwxYyZz", out);
}

// Similar to SanitizeDateFormat, but escape the format elements for Timezone.
static void SanitizeDatetimeFormat(absl::string_view format_string,
                                   std::string* out) {
  return SanitizeFormat(format_string, "Zz", out);
}

absl::Status FormatDateToString(
    absl::string_view format_string, int64_t date,
    const FormatDateTimestampOptions& format_options, std::string* out) {
  if (!IsValidDate(date)) {
    return MakeEvalError() << "Invalid date value: " << date;
  }
  std::string date_format_string;
  SanitizeDateFormat(format_string, &date_format_string);
  // Treats it as a timestamp at midnight on that date and invokes the
  // format_timestamp function.
  int64_t date_timestamp = static_cast<int64_t>(date) * kNaiveNumMicrosPerDay;
  ZETASQL_RETURN_IF_ERROR(FormatTimestampToString(date_format_string, date_timestamp,
                                          absl::UTCTimeZone(), format_options,
                                          out));
  return absl::OkStatus();
}

absl::Status FormatDateToString(absl::string_view format_string, int32_t date,
                                std::string* out) {
  return FormatDateToString(format_string, date,
                            {.expand_Q = true, .expand_J = false}, out);
}

absl::Status FormatDatetimeToStringWithOptions(
    absl::string_view format_string, const DatetimeValue& datetime,
    const FormatDateTimestampOptions& format_options, std::string* out) {
  if (!datetime.IsValid()) {
    return MakeEvalError() << "Invalid datetime value: "
                           << datetime.DebugString();
  }
  std::string datetime_format_string;
  SanitizeDatetimeFormat(format_string, &datetime_format_string);
  absl::Time datetime_in_utc =
      absl::UTCTimeZone().At(datetime.ConvertToCivilSecond()).pre;
  datetime_in_utc += absl::Nanoseconds(datetime.Nanoseconds());

  ZETASQL_RETURN_IF_ERROR(FormatTimestampToString(datetime_format_string,
                                          datetime_in_utc, absl::UTCTimeZone(),
                                          format_options, out));
  return absl::OkStatus();
}

absl::Status FormatDatetimeToString(absl::string_view format_string,
                                    const DatetimeValue& datetime,
                                    std::string* out) {
  // By default, expand %Q but not %J.
  return FormatDatetimeToStringWithOptions(
      format_string, datetime, {.expand_Q = true, .expand_J = false}, out);
}

absl::Status FormatTimeToString(absl::string_view format_string,
                                const TimeValue& time, std::string* out) {
  if (!time.IsValid()) {
    return MakeEvalError() << "Invalid time value: " << time.DebugString();
  }
  std::string time_format_string;
  SanitizeTimeFormat(format_string, &time_format_string);
  absl::Time time_in_epoch_day =
      absl::UTCTimeZone()
          .At(absl::CivilSecond(1970, 1, 1, time.Hour(), time.Minute(),
                                time.Second()))
          .pre;
  time_in_epoch_day += absl::Nanoseconds(time.Nanoseconds());

  // TIME does not support %Q or %J, so 'expand_Q' and 'expand_J' are not
  // relevant.
  ZETASQL_RETURN_IF_ERROR(FormatTimestampToString(
      time_format_string, time_in_epoch_day, absl::UTCTimeZone(),
      {.expand_Q = false, .expand_J = false}, out));
  return absl::OkStatus();
}

absl::Status FormatTimestampToString(
    absl::string_view format_str, absl::Time timestamp, absl::TimeZone timezone,
    const FormatDateTimestampOptions& format_options, std::string* out) {
  return FormatTimestampToStringInternal(
      format_str, timestamp, timezone,
      {.truncate_tz = false,
       .expand_quarter = format_options.expand_Q,
       .expand_iso_dayofyear = format_options.expand_J},
      out);
}

absl::Status FormatTimestampToString(
    absl::string_view format_str, absl::Time timestamp,
    absl::string_view timezone_string,
    const FormatDateTimestampOptions& format_options, std::string* out) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return FormatTimestampToString(format_str, timestamp, timezone,
                                 format_options, out);
}

absl::Status FormatTimestampToString(
    absl::string_view format_str, int64_t timestamp,
    absl::string_view timezone_string,
    const FormatDateTimestampOptions& format_options, std::string* out) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return FormatTimestampToString(format_str, MakeTime(timestamp, kMicroseconds),
                                 timezone, format_options, out);
}

absl::Status FormatTimestampToString(
    absl::string_view format_str, int64_t timestamp, absl::TimeZone timezone,
    const FormatDateTimestampOptions& format_options, std::string* out) {
  return FormatTimestampToString(format_str, MakeTime(timestamp, kMicroseconds),
                                 timezone, format_options, out);
}

absl::Status FormatTimestampToString(absl::string_view format_str,
                                     int64_t timestamp, absl::TimeZone timezone,
                                     std::string* out) {
  return FormatTimestampToStringInternal(
      format_str, MakeTime(timestamp, kMicroseconds), timezone,
      {.truncate_tz = false,
       .expand_quarter = true,
       .expand_iso_dayofyear = false},
      out);
}

absl::Status FormatTimestampToString(absl::string_view format_str,
                                     int64_t timestamp,
                                     absl::string_view timezone_string,
                                     std::string* out) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return FormatTimestampToString(format_str, timestamp, timezone, out);
}

absl::Status FormatTimestampToString(absl::string_view format_str,
                                     absl::Time timestamp,
                                     absl::TimeZone timezone,
                                     std::string* out) {
  return FormatTimestampToStringInternal(format_str, timestamp, timezone,
                                         {.truncate_tz = false,
                                          .expand_quarter = true,
                                          .expand_iso_dayofyear = false},
                                         out);
}

absl::Status FormatTimestampToString(absl::string_view format_string,
                                     absl::Time timestamp,
                                     absl::string_view timezone_string,
                                     std::string* out) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return FormatTimestampToStringInternal(format_string, timestamp, timezone,
                                         {.truncate_tz = false,
                                          .expand_quarter = true,
                                          .expand_iso_dayofyear = false},
                                         out);
}

absl::Status ConvertTimestampToString(absl::Time input, TimestampScale scale,
                                      absl::TimeZone timezone,
                                      std::string* output) {
  NarrowTimestampScaleIfPossible(input, &scale);
  return FormatTimestampToStringInternal(DefaultTimestampFormatStr(scale),
                                         input, timezone,
                                         {.truncate_tz = true,
                                          .expand_quarter = true,
                                          .expand_iso_dayofyear = false},
                                         output);
}

absl::Status ConvertTimestampToString(absl::Time input, TimestampScale scale,
                                      absl::string_view timezone_string,
                                      std::string* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return ConvertTimestampToString(input, scale, timezone, output);
}

absl::Status MakeTimeZone(absl::string_view timezone_string,
                          absl::TimeZone* timezone) {
  // An empty time zone is an error.  There is no inherent default.
  if (timezone_string.empty()) {
    return MakeEvalError() << "Invalid empty time zone";
  }

  // First try to parse the time zone as of the canonical form (+HH:MM) since
  // that is not supported by the time library.
  char timezone_sign;
  int timezone_hour;
  int timezone_minute;
  if (ParseTimeZoneOffsetFormat(timezone_string, &timezone_sign, &timezone_hour,
                                &timezone_minute)) {
    int64_t seconds_offset;
    if (!TimeZonePartsToOffset(timezone_sign, timezone_hour, timezone_minute,
                               kSeconds, &seconds_offset)) {
      return MakeEvalError() << "Invalid time zone: " << timezone_string;
    }
    *timezone = absl::FixedTimeZone(seconds_offset);
    return absl::OkStatus();
  }

  // Otherwise, try to look the time zone up from the Abseil time library.
  // This ultimately looks into the zoneinfo directory (typically
  // /usr/share/zoneinfo, /usr/share/lib/zoneinfo, etc.).
  if (!absl::LoadTimeZone(timezone_string, timezone)) {
    return MakeEvalError() << "Invalid time zone: " << timezone_string;
  }
  return absl::OkStatus();
}

absl::Status ConvertStringToDate(absl::string_view str, int32_t* date) {
  int year = 0, month = 0, day = 0, idx = 0;
  if (!ParseStringToDateParts(str, &idx, &year, &month, &day) ||
      !IsValidDay(year, month, day)) {
    return MakeEvalError() << "Invalid date: '" << str << "'";
  }
  absl::CivilDay civil_day;
  if (!MakeDate(year, month, day, &civil_day)) {
    return MakeEvalError() << "Date value out of range: '" << str << "'";
  }
  *date = CivilDayToEpochDays(civil_day);
  ZETASQL_DCHECK(IsValidDate(*date));  // Invariant if MakeDate() succeeds.
  return absl::OkStatus();
}

absl::StatusOr<int32_t> ConvertCivilDayToDate(absl::CivilDay civil_day) {
  const int32_t date = CivilDayToEpochDays(civil_day);
  if (!IsValidDate(date)) {
    return MakeEvalError() << "Date value out of range: '" << civil_day << "'";
  }
  return date;
}

absl::Status ConvertStringToTimestamp(absl::string_view str,
                                      absl::TimeZone default_timezone,
                                      TimestampScale scale,
                                      int64_t* timestamp) {
  return ConvertStringToTimestamp(str, default_timezone, scale, true,
                                  timestamp);
}

absl::Status ConvertStringToTimestamp(absl::string_view str,
                                      absl::TimeZone default_timezone,
                                      TimestampScale scale,
                                      bool allow_tz_in_str,
                                      int64_t* timestamp) {
  absl::Time base_time;
  ZETASQL_RETURN_IF_ERROR(ConvertStringToTimestamp(str, default_timezone, scale,
                                           allow_tz_in_str, &base_time));
  if (!FromTime(base_time, scale, timestamp)) {
    return MakeEvalError() << MakeInvalidTimestampStrErrorMsg(str, scale);
  }
  if (IsValidTimestamp(*timestamp, scale)) {
    return absl::OkStatus();
  }
  return MakeEvalError() << MakeInvalidTimestampStrErrorMsg(str, scale);
}

absl::Status ConvertStringToTimestamp(absl::string_view str,
                                      absl::string_view default_timezone_string,
                                      TimestampScale scale,
                                      bool allow_tz_in_str,
                                      int64_t* timestamp) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(default_timezone_string, &timezone));
  return ConvertStringToTimestamp(str, timezone, scale, allow_tz_in_str,
                                  timestamp);
}

absl::Status ConvertStringToTimestamp(absl::string_view str,
                                      absl::string_view default_timezone_string,
                                      TimestampScale scale,
                                      int64_t* timestamp) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(default_timezone_string, &timezone));
  return ConvertStringToTimestamp(str, timezone, scale, timestamp);
}

absl::Status ConvertStringToTimestamp(absl::string_view str,
                                      absl::TimeZone default_timezone,
                                      TimestampScale scale,
                                      bool allow_tz_in_str,
                                      absl::Time* output) {
  int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
  int subsecond = 0;
  bool string_includes_timezone = false;
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(ParseStringToTimestampParts(
      str, scale, &year, &month, &day, &hour, &minute, &second, &subsecond,
      &timezone, &string_includes_timezone));
  if (!IsValidDay(year, month, day) ||
      !IsValidTimeOfDay(hour, minute, second)) {
    return MakeEvalError() << MakeInvalidTimestampStrErrorMsg(str, scale);
  }
  if (string_includes_timezone && !allow_tz_in_str) {
    return MakeEvalError() << "Timezone is not allowed in \"" << str << "\"";
  }
  if (!string_includes_timezone) {
    timezone = default_timezone;
  }
  const absl::CivilSecond cs(year, month, day, hour, minute, second);
  *output = timezone.At(cs).pre + MakeDuration(subsecond, scale);
  if (!IsValidTime(*output)) {
    return MakeEvalError() << MakeInvalidTimestampStrErrorMsg(str, scale);
  }
  return absl::OkStatus();
}

absl::Status ConvertStringToTime(absl::string_view str, TimestampScale scale,
                                 TimeValue* output) {
  ZETASQL_RET_CHECK(scale == kMicroseconds || scale == kNanoseconds)
      << "Only kMicroseconds and kNanoseconds are acceptable values for scale";
  int hour = 0, minute = 0, second = 0, subsecond = 0;
  int idx = 0;
  if (!ParseStringToTimeParts(str, scale, &idx, &hour, &minute, &second,
                              &subsecond) ||
      !IsValidTimeOfDay(hour, minute, second)) {
    return MakeEvalError() << MakeInvalidTypedStrErrorMsg("time", str, scale);
  }

  // Because we have checked for validity of the fields already, the only
  // situation where the input will be still be invalid here is due to the leap
  // second. In that case, we clear its sub-second part. See the comments in
  // header for explanation.
  if (second == 60) {
    subsecond = 0;
  }

  if (scale == kMicroseconds) {
    *output =
        TimeValue::FromHMSAndMicrosNormalized(hour, minute, second, subsecond);
  } else {
    *output =
        TimeValue::FromHMSAndNanosNormalized(hour, minute, second, subsecond);
  }
  return absl::OkStatus();
}

absl::Status ConvertStringToDatetime(absl::string_view str,
                                     TimestampScale scale,
                                     DatetimeValue* output) {
  ZETASQL_RET_CHECK(scale == kMicroseconds || scale == kNanoseconds)
      << "Only kMicroseconds and kNanoseconds are acceptable values for scale";
  int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
  int subsecond = 0;
  if (!ParseStringToDatetimeParts(str, scale, &year, &month, &day, &hour,
                                  &minute, &second, &subsecond) ||
      !IsValidDay(year, month, day) ||
      !IsValidTimeOfDay(hour, minute, second)) {
    return MakeEvalError() << MakeInvalidTypedStrErrorMsg("datetime", str,
                                                          scale);
  }

  // Because we have checked for validity of the fields already, the only
  // situation where the input will be still be invalid here is due to the leap
  // second. In that case, we clear its sub-second part. See the comments in
  // header for explanation.
  if (second == 60) {
    subsecond = 0;
  }

  if (scale == kMicroseconds) {
    *output = DatetimeValue::FromYMDHMSAndMicrosNormalized(
        year, month, day, hour, minute, second, subsecond);
  } else {
    *output = DatetimeValue::FromYMDHMSAndNanosNormalized(
        year, month, day, hour, minute, second, subsecond);
  }
  // Leap second is acceptable as input, but invalid for TimeValue,
  // normalize it to the first second of the next minute.
  // The only fail case in this context is "9999-12-31 23:59:60".
  if (!output->IsValid()) {
    return MakeEvalError() << MakeInvalidTypedStrErrorMsg("datetime", str,
                                                          scale);
  }

  return absl::OkStatus();
}

absl::Status ConstructDate(int year, int month, int day, int32_t* output) {
  absl::CivilDay civil_day;
  if (MakeDate(year, month, day, &civil_day)) {
    *output = CivilDayToEpochDays(civil_day);
    return absl::OkStatus();
  }
  return MakeEvalError() << "Input calculates to invalid date: "
                         << absl::StrFormat("%04d-%02d-%02d", year, month, day);
}

absl::Status ConstructTime(int hour, int minute, int second,
                           TimeValue* output) {
  if (IsValidTimeOfDay(hour, minute, second)) {
    *output = TimeValue::FromHMSAndMicrosNormalized(hour, minute, second,
                                                    /*microsecond=*/0);
    return absl::OkStatus();
  }
  return MakeEvalError() << "Input calculates to invalid time: "
                         << absl::StrFormat("%02d:%02d:%02d", hour, minute,
                                            second);
}

absl::Status ConstructDatetime(int year, int month, int day, int hour,
                               int minute, int second, DatetimeValue* output) {
  if (IsValidDay(year, month, day) && IsValidTimeOfDay(hour, minute, second)) {
    *output = DatetimeValue::FromYMDHMSAndMicrosNormalized(
        year, month, day, hour, minute, second, /*microsecond=*/0);
    if (output->IsValid()) {
      return absl::OkStatus();
    }
  }
  return MakeEvalError() << "Input calculates to invalid datetime: "
                         << absl::StrFormat("%04d-%02d-%02d %04d:%02d:%02d",
                                            year, month, day, hour, minute,
                                            second);
}

absl::Status ConstructDatetime(int32_t date, const TimeValue& time,
                               DatetimeValue* output) {
  if (IsValidDate(date) && time.IsValid()) {
    absl::CivilDay day = EpochDaysToCivilDay(date);
    *output = DatetimeValue::FromYMDHMSAndNanosNormalized(
        // We check "IsValidDate", guaranteed safe.
        // cast is safe, checked with IsValidDate.
        static_cast<int32_t>(day.year()), day.month(), day.day(), time.Hour(),
        time.Minute(), time.Second(), time.Nanoseconds());
    if (output->IsValid()) {
      return absl::OkStatus();
    }
  }
  return MakeEvalError() << "Input calculates to invalid datetime: "
                         << DateErrorString(date) << " " << time.DebugString();
}

absl::Status ExtractFromTimestamp(DateTimestampPart part, int64_t timestamp,
                                  TimestampScale scale, absl::TimeZone timezone,
                                  int32_t* output) {
  if (!IsValidTimestamp(timestamp, scale)) {
    return MakeEvalError() << "Invalid timestamp value: " << timestamp;
  }
  return ExtractFromTimestampInternal(part, MakeTime(timestamp, scale),
                                      timezone, output);
}

absl::Status ExtractFromTimestamp(DateTimestampPart part, int64_t timestamp,
                                  TimestampScale scale,
                                  absl::string_view timezone_string,
                                  int32_t* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return ExtractFromTimestamp(part, timestamp, scale, timezone, output);
}

absl::Status ExtractFromTimestamp(DateTimestampPart part, absl::Time base_time,
                                  absl::TimeZone timezone, int32_t* output) {
  if (IsValidTime(base_time)) {
    return ExtractFromTimestampInternal(part, base_time, timezone, output);
  }
  // Error handling.
  std::string time_str;
  if (ConvertTimestampToString(base_time, kNanoseconds, timezone, &time_str)
          .ok()) {
    return MakeEvalError() << "Invalid timestamp: " << time_str;
  }
  // Most likely should never happen.
  return MakeEvalError() << "Invalid timestamp: "
                         << absl::FormatTime(base_time, timezone);
}

absl::Status ExtractFromTimestamp(DateTimestampPart part, absl::Time base_time,
                                  absl::string_view timezone_string,
                                  int32_t* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return ExtractFromTimestamp(part, base_time, timezone, output);
}

absl::Status ExtractFromDate(DateTimestampPart part, int32_t date,
                             int32_t* output) {
  if (!IsValidDate(date)) {
    return MakeEvalError() << "Invalid date value: " << date;
  }
  absl::CivilDay day = EpochDaysToCivilDay(date);
  switch (part) {
    case YEAR:
      // Year for valid dates fits into int32_t
      *output = static_cast<int32_t>(day.year());
      break;
    case QUARTER:
      *output = (day.month() - 1) / 3 + 1;
      break;
    case MONTH:
      *output = day.month();
      break;
    case DAY:
      *output = day.day();
      break;
    case ISOYEAR:
      // Year for valid dates fits into int32_t
      *output = static_cast<int32_t>(GetIsoYear(day));
      break;
    case WEEK:
    case WEEK_MONDAY:
    case WEEK_TUESDAY:
    case WEEK_WEDNESDAY:
    case WEEK_THURSDAY:
    case WEEK_FRIDAY:
    case WEEK_SATURDAY: {
      const absl::CivilDay first_calendar_day_of_year(day.year(), 1, 1);
      ZETASQL_ASSIGN_OR_RETURN(const absl::Weekday weekday,
                       GetFirstWeekDayOfWeek(part));
      const absl::CivilDay effective_first_day_of_year =
          NextWeekdayOrToday(first_calendar_day_of_year, weekday);
      if (day < effective_first_day_of_year) {
        *output = 0;
      } else {
        // cast is safe, guaranteed to be less than 52.
        *output =
            static_cast<int32_t>(((day - effective_first_day_of_year) / 7) + 1);
      }
      break;
    }
    case ISOWEEK:
      *output = GetIsoWeek(day);
      break;
    case DAYOFWEEK:
      *output = internal_functions::DayOfWeekIntegerSunToSat1To7(
          absl::GetWeekday(day));
      break;
    case DAYOFYEAR:
      *output = absl::GetYearDay(day);
      break;
    case DATE:
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
    case NANOSECOND:
      return MakeEvalError()
             << "Unsupported DateTimestampPart " << DateTimestampPart_Name(part)
             << " to extract from date";
    default:
      return MakeEvalError()
             << "Unexpected DateTimestampPart " << DateTimestampPart_Name(part);
  }
  return absl::OkStatus();
}

absl::Status ExtractFromTime(DateTimestampPart part, const TimeValue& time,
                             int32_t* output) {
  if (!time.IsValid()) {
    return MakeEvalError() << "Invalid time value: " << time.DebugString();
  }
  switch (part) {
    case YEAR:
    case MONTH:
    case DAY:
    case DAYOFWEEK:
    case DAYOFYEAR:
    case QUARTER:
    case DATE:
    case WEEK:
    case WEEK_MONDAY:
    case WEEK_TUESDAY:
    case WEEK_WEDNESDAY:
    case WEEK_THURSDAY:
    case WEEK_FRIDAY:
    case WEEK_SATURDAY:
    case DATETIME:
    case TIME:
      return MakeEvalError()
             << "Unsupported DateTimestampPart " << DateTimestampPart_Name(part)
             << " to extract from time";
    case HOUR:
      *output = time.Hour();
      break;
    case MINUTE:
      *output = time.Minute();
      break;
    case SECOND:
      *output = time.Second();
      break;
    case MILLISECOND:
      *output = time.Microseconds() / 1000;
      break;
    case MICROSECOND:
      *output = time.Microseconds();
      break;
    case NANOSECOND:
      *output = time.Nanoseconds();
      break;
    default:
      return MakeEvalError()
             << "Unexpected DateTimestampPart " << DateTimestampPart_Name(part);
  }
  return absl::OkStatus();
}

absl::Status ExtractFromDatetime(DateTimestampPart part,
                                 const DatetimeValue& datetime,
                                 int32_t* output) {
  ZETASQL_RET_CHECK(part != TIME)
      << "Use ExtractTimeFromDatetime() for extracting time from datetime";
  if (!datetime.IsValid()) {
    return MakeEvalError() << "Invalid datetime value: "
                           << datetime.DebugString();
  }
  switch (part) {
    case YEAR:
      *output = datetime.Year();
      break;
    case QUARTER:
      *output = (datetime.Month() - 1) / 3 + 1;
      break;
    case MONTH:
      *output = datetime.Month();
      break;
    case DAY:
      *output = datetime.Day();
      break;
    case HOUR:
      *output = datetime.Hour();
      break;
    case MINUTE:
      *output = datetime.Minute();
      break;
    case SECOND:
      *output = datetime.Second();
      break;
    case MILLISECOND:
      *output = datetime.Microseconds() / 1000;
      break;
    case MICROSECOND:
      *output = datetime.Microseconds();
      break;
    case NANOSECOND:
      *output = datetime.Nanoseconds();
      break;
    case DATE:
      ZETASQL_RETURN_IF_ERROR(ConstructDate(datetime.Year(), datetime.Month(),
                                    datetime.Day(), output));
      break;
    case DAYOFWEEK:
      *output = internal_functions::DayOfWeekIntegerSunToSat1To7(
          absl::GetWeekday(absl::CivilDay(datetime.Year(), datetime.Month(),
                                          datetime.Day())));
      break;
    case DAYOFYEAR:
      *output = absl::GetYearDay(
          absl::CivilDay(datetime.Year(), datetime.Month(), datetime.Day()));
      break;
    case WEEK:
    case WEEK_MONDAY:
    case WEEK_TUESDAY:
    case WEEK_WEDNESDAY:
    case WEEK_THURSDAY:
    case WEEK_FRIDAY:
    case WEEK_SATURDAY: {
      const absl::CivilDay first_calendar_day_of_year(datetime.Year(), 1, 1);

      ZETASQL_ASSIGN_OR_RETURN(const absl::Weekday weekday,
                       GetFirstWeekDayOfWeek(part));
      const absl::CivilDay effective_first_day_of_year =
          NextWeekdayOrToday(first_calendar_day_of_year, weekday);

      const absl::CivilDay day(datetime.Year(), datetime.Month(),
                               datetime.Day());
      if (day < effective_first_day_of_year) {
        *output = 0;
      } else {
        // cast is safe, guaranteed to be less than 52.
        *output =
            static_cast<int32_t>(((day - effective_first_day_of_year) / 7) + 1);
      }
      break;
    }
    case ISOYEAR:
      // cast is safe, year "guaranteed" safe by method contract.
      *output = static_cast<int32_t>(GetIsoYear(
          absl::CivilDay(datetime.Year(), datetime.Month(), datetime.Day())));
      break;
    case ISOWEEK:
      *output = GetIsoWeek(
          absl::CivilDay(datetime.Year(), datetime.Month(), datetime.Day()));
      break;
    case DATETIME:
      return MakeEvalError()
             << "Unsupported DateTimestampPart " << DateTimestampPart_Name(part)
             << " to extract from datetime";
    case TIME:
      // Should never reach this.
      ZETASQL_RET_CHECK_FAIL()
          << "Use ExtractTimeFromDatetime() for extracting time from datetime";
      break;
    default:
      return MakeEvalError()
             << "Unexpected DateTimestampPart " << DateTimestampPart_Name(part);
  }
  return absl::OkStatus();
}

absl::Status ExtractTimeFromDatetime(const DatetimeValue& datetime,
                                     TimeValue* time) {
  if (!datetime.IsValid()) {
    return MakeEvalError() << "Invalid datetime value: "
                           << datetime.DebugString();
  }
  *time = TimeValue::FromHMSAndNanos(datetime.Hour(), datetime.Minute(),
                                     datetime.Second(), datetime.Nanoseconds());
  // Check should never fail because the fields taken from a valid datetime
  // should always be valid for a time value.
  ZETASQL_RET_CHECK(time->IsValid());
  return absl::OkStatus();
}

absl::Status ConvertDatetimeToTimestamp(const DatetimeValue& datetime,
                                        absl::TimeZone timezone,
                                        absl::Time* output) {
  if (datetime.IsValid()) {
    if (TimestampFromParts(datetime.Year(), datetime.Month(), datetime.Day(),
                           datetime.Hour(), datetime.Minute(),
                           datetime.Second(), datetime.Nanoseconds(),
                           kNanoseconds, timezone, output) &&
        IsValidTime(*output)) {
      return absl::OkStatus();
    }
    return MakeEvalError() << "Cannot convert Datetime "
                           << datetime.DebugString() << " at timezone "
                           << timezone.name() << " to a Timestamp";
  }
  return MakeEvalError() << "Invalid datetime: " << datetime.DebugString();
}

absl::Status ConvertDatetimeToTimestamp(const DatetimeValue& datetime,
                                        absl::string_view timezone_string,
                                        absl::Time* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return ConvertDatetimeToTimestamp(datetime, timezone, output);
}

absl::Status ConvertTimestampToDatetime(absl::Time base_time,
                                        absl::TimeZone timezone,
                                        DatetimeValue* output) {
  if (IsValidTime(base_time)) {
    const absl::TimeZone::CivilInfo info = timezone.At(base_time);
    *output = DatetimeValue::FromYMDHMSAndNanos(
        // cast is safe, since we check 'IsValidTime.
        static_cast<int32_t>(info.cs.year()), info.cs.month(), info.cs.day(),
        info.cs.hour(), info.cs.minute(), info.cs.second(),
        // cast is safe, guaranteed to be less than 1 billion.
        static_cast<int32_t>(info.subsecond / absl::Nanoseconds(1)));
    if (output->IsValid()) {
      return absl::OkStatus();
    }
    return MakeEvalError() << "Invalid Datetime " << output->DebugString()
                           << "extracted from timestamp "
                           << TimestampErrorString(base_time, timezone);
  }
  // Error handling.
  return MakeEvalError() << "Invalid timestamp: "
                         << TimestampErrorString(base_time, timezone);
}

absl::Status ConvertTimestampToDatetime(absl::Time base_time,
                                        absl::string_view timezone_string,
                                        DatetimeValue* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return ConvertTimestampToDatetime(base_time, timezone, output);
}

absl::Status ConvertTimestampToTime(absl::Time base_time,
                                    absl::TimeZone timezone,
                                    TimestampScale scale, TimeValue* output) {
  ZETASQL_RET_CHECK(scale == kNanoseconds || scale == kMicroseconds);
  if (IsValidTime(base_time)) {
    const absl::TimeZone::CivilInfo info = timezone.At(base_time);
    if (scale == kNanoseconds) {
      *output = TimeValue::FromHMSAndNanos(
          info.cs.hour(), info.cs.minute(), info.cs.second(),
          // cast is safe, guaranteed less than 1 billion
          static_cast<int32_t>(absl::ToInt64Nanoseconds(info.subsecond)));
    } else {
      *output = TimeValue::FromHMSAndMicros(
          info.cs.hour(), info.cs.minute(), info.cs.second(),
          // cast is safe, guaranteed less than 1 million
          static_cast<int32_t>(absl::ToInt64Microseconds(info.subsecond)));
    }
    if (output->IsValid()) {
      return absl::OkStatus();
    }
    return MakeEvalError() << "Invalid Time " << output->DebugString()
                           << "extracted from timestamp "
                           << TimestampErrorString(base_time, timezone);
  }
  // Error handling.
  return MakeEvalError() << "Invalid timestamp: "
                         << TimestampErrorString(base_time, timezone);
}

absl::Status ConvertTimestampToTime(absl::Time base_time,
                                    absl::string_view timezone_string,
                                    TimestampScale scale, TimeValue* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return ConvertTimestampToTime(base_time, timezone, scale, output);
}

absl::Status ConvertTimestampToTime(absl::Time base_time,
                                    absl::TimeZone timezone,
                                    TimeValue* output) {
  return ConvertTimestampToTime(base_time, timezone, kNanoseconds, output);
}

absl::Status ConvertTimestampToTime(absl::Time base_time,
                                    absl::string_view timezone_string,
                                    TimeValue* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return ConvertTimestampToTime(base_time, timezone, kNanoseconds, output);
}

absl::Status ConvertDateToTimestamp(int32_t date, absl::TimeZone timezone,
                                    absl::Time* output) {
  if (!IsValidDate(date)) {
    return MakeEvalError() << "Invalid date value: " << date;
  }
  *output = timezone.At(absl::CivilSecond(EpochDaysToCivilDay(date))).pre;
  return absl::OkStatus();
}

absl::Status ConvertDateToTimestamp(int32_t date,
                                    absl::string_view timezone_string,
                                    absl::Time* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return ConvertDateToTimestamp(date, timezone, output);
}

absl::Status ConvertDateToTimestamp(int32_t date, TimestampScale scale,
                                    absl::TimeZone timezone, int64_t* output) {
  absl::Time base_time;
  ZETASQL_RETURN_IF_ERROR(ConvertDateToTimestamp(date, timezone, &base_time));
  if (!FromTime(base_time, scale, output)) {
    return MakeEvalError() << "Cannot convert date " << DateErrorString(date)
                           << " to timestamp";
  }
  return absl::OkStatus();
}

absl::Status ConvertDateToTimestamp(int32_t date, TimestampScale scale,
                                    absl::string_view timezone_string,
                                    int64_t* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return ConvertDateToTimestamp(date, scale, timezone, output);
}

absl::Status ConvertProto3TimestampToTimestamp(
    const google::protobuf::Timestamp& input_timestamp,
    TimestampScale output_scale, int64_t* output) {
  absl::Time time;
  ZETASQL_RETURN_IF_ERROR(ConvertProto3TimestampToTimestamp(input_timestamp, &time));
  if (!FromTime(time, output_scale, output)) {
    return MakeEvalError() << "Invalid Proto3 Timestamp input: "
                           << input_timestamp.DebugString();
  }
  return absl::OkStatus();
}

absl::Status ConvertProto3TimestampToTimestamp(
    const google::protobuf::Timestamp& input_timestamp, absl::Time* output) {
  auto result_or = zetasql_base::DecodeGoogleApiProto(input_timestamp);
  if (!result_or.ok()) {
    return MakeEvalError() << "Invalid Proto3 Timestamp input: "
                        << input_timestamp.DebugString();
  }
  *output = result_or.value();
  // DecodeGoogleApiProto enforces the same valid timestamp range as ZetaSQL.
  // This check is meant to give us protection in case the contract of
  // DecodeGoogleApiProto changes in the future.
  ZETASQL_DCHECK(IsValidTime(*output));
  return absl::OkStatus();
}

absl::Status ConvertTimestampToProto3Timestamp(
    int64_t input_timestamp, TimestampScale scale,
    google::protobuf::Timestamp* output) {
  return ConvertTimestampToProto3Timestamp(MakeTime(input_timestamp, scale),
                                           output);
}

absl::Status ConvertTimestampToProto3Timestamp(
    absl::Time input_timestamp, google::protobuf::Timestamp* output) {
  // We don't need to explicity check the validity of <input_timestamp> as
  // EncodeGoogleApiProto enforces the same valid timestamp range as ZetaSQL.
  return zetasql_base::EncodeGoogleApiProto(input_timestamp, output);
}

absl::Status ConvertProto3DateToDate(const google::type::Date& input,
                                     int32_t* output) {
  return ConstructDate(input.year(), input.month(), input.day(), output);
}

absl::Status ConvertDateToProto3Date(int32_t input,
                                     google::type::Date* output) {
  if (!IsValidDate(input)) {
    return MakeEvalError() << "Input is outside of Proto3 Date range: "
                           << input;
  }
  absl::CivilDay converted_date = EpochDaysToCivilDay(input);
  // cast is guaranteed safe, since we check with IsValidDate.
  output->set_year(static_cast<int32_t>(converted_date.year()));
  output->set_month(converted_date.month());
  output->set_day(converted_date.day());
  return absl::OkStatus();
}

absl::Status ConvertBetweenTimestamps(int64_t input_timestamp,
                                      TimestampScale input_scale,
                                      TimestampScale output_scale,
                                      int64_t* output) {
  if (!IsValidTimestamp(input_timestamp, input_scale)) {
    return MakeEvalError() << "Invalid timestamp value: " << input_timestamp;
  }
  return ConvertBetweenTimestampsInternal(input_timestamp, input_scale,
                                          output_scale, output);
}

absl::Status AddDateOverflow(int32_t date, DateTimestampPart part,
                             int32_t interval, int32_t* output,
                             bool* had_overflow) {
  *had_overflow = false;
  if (!IsValidDate(date)) {
    return MakeEvalError() << "Invalid date value: " << date;
  }

  // Special cases to avoid computing converted_date.
  if (part == DAY) {
    if (!Add<int32_t>(date, interval, output, kNoError)) {
      *had_overflow = true;
      return absl::OkStatus();
    }
  } else if (part == WEEK) {
    if (!Multiply<int32_t>(7, interval, &interval, kNoError) ||
        !Add<int32_t>(date, interval, output, kNoError)) {
      *had_overflow = true;
      return absl::OkStatus();
    }
  } else if (part == YEAR || part == QUARTER || part == MONTH) {
    absl::CivilDay civil_day = EpochDaysToCivilDay(date);
    // cast is safe, checked with IsValidDate.
    int y = static_cast<int32_t>(civil_day.year());
    int month = civil_day.month();
    int day = civil_day.day();
    switch (part) {
      case YEAR: {
        if (!Add<int32_t>(y, interval, &y, kNoError)) {
          *had_overflow = true;
          return absl::OkStatus();
        }
        AdjustYearMonthDay(&y, &month, &day);
        absl::CivilDay date_value;
        if (!MakeDate(y, month, day, &date_value)) {
          *had_overflow = true;
          return absl::OkStatus();
        }
        *output = CivilDayToEpochDays(date_value);
        break;
      }
      case QUARTER:
        if (!Multiply<int32_t>(3, interval, &interval, kNoError)) {
          *had_overflow = true;
          return absl::OkStatus();
        }
        ABSL_FALLTHROUGH_INTENDED;
      case MONTH: {
        int32_t m;
        if (!Add<int32_t>(month, interval, &m, kNoError)) {
          *had_overflow = true;
          return absl::OkStatus();
        }

        AdjustYearMonthDay(&y, &m, &day);
        absl::CivilDay date_value;
        if (!MakeDate(y, m, day, &date_value)) {
          *had_overflow = true;
          return absl::OkStatus();
        }
        *output = CivilDayToEpochDays(date_value);
        break;
      }
      default:
        // Do nothing.
        break;
    }
  } else {  // Other DateTimestampPart are not supported.
    return MakeEvalError() << "Unsupported DateTimestampPart "
                           << DateTimestampPart_Name(part);
  }
  if (!IsValidDate(*output)) {
    *had_overflow = true;
    return absl::OkStatus();
  }
  return absl::OkStatus();
}

absl::Status AddDate(int32_t date, DateTimestampPart part, int64_t interval,
                     int32_t* output) {
  // The interval is an int64_t to match the ZetaSQL function signature.
  // Below we will do safe casting with it, so it must be in
  // the domain of int32_t numbers.
  if (interval > std::numeric_limits<int32_t>::max() ||
      interval < std::numeric_limits<int32_t>::lowest()) {
    return MakeAddDateOverflowError(date, part, interval);
  }

  bool had_overflow = false;
  ZETASQL_RETURN_IF_ERROR(AddDateOverflow(date, part, static_cast<int32_t>(interval),
                                  output, &had_overflow));
  if (had_overflow) {
    return MakeAddDateOverflowError(date, part, interval);
  }
  return absl::OkStatus();
}

absl::Status AddDate(int32_t date, zetasql::IntervalValue interval,
                     DatetimeValue* output) {
  DatetimeValue datetime;
  ZETASQL_RETURN_IF_ERROR(ConstructDatetime(date, TimeValue(), &datetime));
  ZETASQL_RETURN_IF_ERROR(AddDatetime(datetime, interval, output));
  return absl::OkStatus();
}

absl::Status SubDate(int32_t date, DateTimestampPart part, int64_t interval,
                     int32_t* output) {
  // The negation of std::numeric_limits<int64_t>::lowest() is undefined, so
  // protect against it explicitly.
  if (interval == std::numeric_limits<int64_t>::lowest()) {
    return MakeSubDateOverflowError(date, part, interval);
  }
  return AddDate(date, part, -interval, output);
}

absl::Status DiffDates(int32_t date1, int32_t date2, DateTimestampPart part,
                       int32_t* output) {
  if (!IsValidDate(date1)) {
    return MakeEvalError() << "Invalid date value: " << date1;
  }
  if (!IsValidDate(date2)) {
    return MakeEvalError() << "Invalid date value: " << date2;
  }

  switch (part) {
    case DAY:
      *output = date1 - date2;
      break;
    case WEEK:
    case WEEK_MONDAY:
    case WEEK_TUESDAY:
    case WEEK_WEDNESDAY:
    case WEEK_THURSDAY:
    case WEEK_FRIDAY:
    case WEEK_SATURDAY:
    case ISOWEEK:
      // TODO: Refactor the WEEK implementation to not rely on
      // a hacky version of TruncateDateImpl that can return a Date that
      // is out of the valid range (and remove the <enforce_range> argument).
      ZETASQL_RETURN_IF_ERROR(
          TruncateDateImpl(date1, part, /*enforce_range=*/false, &date1));
      ZETASQL_RETURN_IF_ERROR(
          TruncateDateImpl(date2, part, /*enforce_range=*/false, &date2));
      *output = (date1 - date2) / 7;
      break;
    case YEAR:
    case ISOYEAR:
    case QUARTER:
    case MONTH: {
      absl::CivilDay civil_day1 = EpochDaysToCivilDay(date1);
      absl::civil_year_t y1 = civil_day1.year();
      int32_t m1 = civil_day1.month();
      absl::CivilDay civil_day2 = EpochDaysToCivilDay(date2);
      absl::civil_year_t y2 = civil_day2.year();
      int32_t m2 = civil_day2.month();
      switch (part) {
        case YEAR:
          *output = y1 - y2;
          break;
        case ISOYEAR:
          // cast is safe because dates are severely restricted by IsValidDate.
          *output = static_cast<int32_t>(GetIsoYear(civil_day1) -
                                         GetIsoYear(civil_day2));
          break;
        case MONTH:
          *output = (y1 - y2) * 12 + (m1 - m2);
          break;
        case QUARTER:
          *output = (y1 * 12 + m1 - 1) / 3 - (y2 * 12 + m2 - 1) / 3;
          break;
        default:
          break;
      }
      break;
    }
    default:
      return MakeEvalError() << "Unsupported DateTimestampPart "
                             << DateTimestampPart_Name(part);
  }
  return absl::OkStatus();
}

// INTERVAL difference between DATEs is DAY granularity.
// Months and nanos are always zero.
absl::StatusOr<IntervalValue> IntervalDiffDates(int32_t date1, int32_t date2) {
  return IntervalValue::FromDays(date1 - date2);
}

// Valid <part> for this function are only HOUR, MINUTE, SECOND, MILLISECOND,
// MICROSECOND and NANOSECOND.
// For all valid input, overflow could happen only when <part> is NANOSECOND.
// When overflow happens, use the supplied <nano_overflow_error_maker> to make
// an appropriate error message to pass out.
static absl::Status DiffWithPartsSmallerThanDay(
    const absl::CivilSecond civil_second_1, int64_t nanosecond_1,
    const absl::CivilSecond civil_second_2, int64_t nanosecond_2,
    DateTimestampPart part,
    const std::function<absl::Status()>& nano_overflow_error_maker,
    int64_t* output) {
  if (part == HOUR) {
    *output = absl::CivilHour(civil_second_1) - absl::CivilHour(civil_second_2);
  } else if (part == MINUTE) {
    *output =
        absl::CivilMinute(civil_second_1) - absl::CivilMinute(civil_second_2);
  } else {
    int64_t diff_in_seconds = civil_second_1 - civil_second_2;
    switch (part) {
      case NANOSECOND:
        if (std::numeric_limits<int64_t>::lowest() / powers_of_ten[9] <=
                diff_in_seconds &&
            std::numeric_limits<int64_t>::max() / powers_of_ten[9] >=
                diff_in_seconds) {
          // std::numeric_limits<int64_t>::lowest() < nano_diff_1 <
          // std::numeric_limits<int64_t>::max() should always hold.
          int64_t nano_diff_1 = diff_in_seconds * powers_of_ten[9];
          int64_t nano_diff_2 = nanosecond_1 - nanosecond_2;
          // The output is valid only when
          // std::numeric_limits<int64_t>::lowest() <= nano_diff_1 + nano_diff_2
          // <= std::numeric_limits<int64_t>::max()
          if ((nano_diff_2 >= 0 &&
               std::numeric_limits<int64_t>::max() - nano_diff_2 >=
                   nano_diff_1) ||
              (nano_diff_2 < 0 &&
               std::numeric_limits<int64_t>::lowest() - nano_diff_2 <=
                   nano_diff_1)) {
            *output = nano_diff_1 + nano_diff_2;
            return absl::OkStatus();
          }
        }
        return nano_overflow_error_maker();
      case MICROSECOND:
        *output =
            diff_in_seconds * powers_of_ten[6] +
            (nanosecond_1 / powers_of_ten[3] - nanosecond_2 / powers_of_ten[3]);
        break;
      case MILLISECOND:
        *output =
            diff_in_seconds * powers_of_ten[3] +
            (nanosecond_1 / powers_of_ten[6] - nanosecond_2 / powers_of_ten[6]);
        break;
      case SECOND:
        *output = diff_in_seconds;
        break;
      default:
        ZETASQL_RET_CHECK_FAIL() << "Unexpected DateTimestampPart "
                         << DateTimestampPart_Name(part);
    }
  }
  return absl::OkStatus();
}

absl::Status DiffDatetimes(const DatetimeValue& datetime1,
                           const DatetimeValue& datetime2,
                           DateTimestampPart part, int64_t* output) {
  if (!datetime1.IsValid()) {
    return MakeEvalError() << "Invalid datetime value: "
                           << datetime1.DebugString();
  }
  if (!datetime2.IsValid()) {
    return MakeEvalError() << "Invalid datetime value: "
                           << datetime2.DebugString();
  }

  switch (part) {
    case YEAR:
      *output = absl::CivilYear(datetime1.ConvertToCivilSecond()) -
                absl::CivilYear(datetime2.ConvertToCivilSecond());
      break;
    case QUARTER: {
      // This logic is the same as the one in DiffDates()
      auto civil_time_1 = datetime1.ConvertToCivilSecond();
      auto civil_time_2 = datetime2.ConvertToCivilSecond();
      *output = (civil_time_1.year() * 12 + civil_time_1.month() - 1) / 3 -
                (civil_time_2.year() * 12 + civil_time_2.month() - 1) / 3;
      break;
    }
    case MONTH:
      *output = absl::CivilMonth(datetime1.ConvertToCivilSecond()) -
                absl::CivilMonth(datetime2.ConvertToCivilSecond());
      break;
    case WEEK:
    case WEEK_MONDAY:
    case WEEK_TUESDAY:
    case WEEK_WEDNESDAY:
    case WEEK_THURSDAY:
    case WEEK_FRIDAY:
    case WEEK_SATURDAY:
    case ISOYEAR:
    case ISOWEEK: {
      int32_t date1;
      int32_t date2;
      int32_t int32_diff;
      ZETASQL_RETURN_IF_ERROR(ExtractFromDatetime(DATE, datetime1, &date1));
      ZETASQL_RETURN_IF_ERROR(ExtractFromDatetime(DATE, datetime2, &date2));
      ZETASQL_RETURN_IF_ERROR(DiffDates(date1, date2, part, &int32_diff));
      *output = int32_diff;
      break;
    }
    case DAY:
      *output = absl::CivilDay(datetime1.ConvertToCivilSecond()) -
                absl::CivilDay(datetime2.ConvertToCivilSecond());
      break;
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
    case NANOSECOND: {
      auto DatetimeDiffOverflowErrorMaker = [&datetime1, &datetime2,
                                             part]() -> absl::Status {
        const std::string error_shared_prefix = absl::StrCat(
            "DATETIME_DIFF at ", DateTimestampPart_Name(part),
            " precision between datetime ", datetime1.DebugString(), " and ",
            datetime2.DebugString());
        if (part == NANOSECOND) {
          return MakeEvalError() << error_shared_prefix << " causes overflow";
        }
        ZETASQL_RET_CHECK_FAIL() << error_shared_prefix
                         << " should never have overflow error";
      };
      return DiffWithPartsSmallerThanDay(
          datetime1.ConvertToCivilSecond(), datetime1.Nanoseconds(),
          datetime2.ConvertToCivilSecond(), datetime2.Nanoseconds(), part,
          DatetimeDiffOverflowErrorMaker, output);
    }
    case DAYOFWEEK:
    case DAYOFYEAR:
    case DATE:
    case DATETIME:
    case TIME:
      return MakeEvalError()
             << "Unsupported DateTimestampPart " << DateTimestampPart_Name(part)
             << " for DATETIME_DIFF";
    default:
      return MakeEvalError()
             << "Unexpected DateTimestampPart " << DateTimestampPart_Name(part)
             << " for DATETIME_DIFF";
  }
  return absl::OkStatus();
}

// INTERVAL difference between DATETIMEs is DAY TO SECOND granularity.
// Months is always zero, days is the full number of days difference between two
// datetimes (based on 24 hour days), and any DATETIME remainder is encoded as
// nanos.
absl::StatusOr<IntervalValue> IntervalDiffDatetimes(
    const DatetimeValue& datetime1, const DatetimeValue& datetime2) {
  int64_t seconds;
  ZETASQL_RETURN_IF_ERROR(DiffDatetimes(datetime1, datetime2, SECOND, &seconds));
  __int128 nanos = IntervalValue::kNanosInSecond * seconds;
  nanos += (datetime1.Nanoseconds() - datetime2.Nanoseconds());
  int64_t days = nanos / IntervalValue::kNanosInDay;
  nanos = nanos % IntervalValue::kNanosInDay;
  return IntervalValue::FromMonthsDaysNanos(0, days, nanos);
}

// This internal wrapper is shared by both datetime_add and datetime_sub. The
// extra function is used to generate appropriate error messages when
// out-of-range error happens.
static absl::Status AddDatetimeInternal(
    const DatetimeValue& datetime, DateTimestampPart part, int64_t interval,
    DatetimeValue* output,
    const std::function<absl::Status()>& overflow_error_maker) {
  if (!datetime.IsValid()) {
    return MakeEvalError() << "Invalid datetime value: "
                           << datetime.DebugString();
  }
  DatetimeValue result;
  if (CheckValidAddTimestampPart(part, false /* is_legacy */).ok()) {
    // We can re-use the AddTimestampInternal implementation by converting
    // the datetime to a absl::Time at UTC, and then converting back to
    // datetime.
    absl::Time datetime_in_utc =
        absl::UTCTimeZone().At(datetime.ConvertToCivilSecond()).pre;
    datetime_in_utc += absl::Nanoseconds(datetime.Nanoseconds());

    absl::Time result_timestamp;
    bool had_overflow_unused;
    if (!AddTimestampInternal(datetime_in_utc, absl::UTCTimeZone(), part,
                              interval, &result_timestamp, &had_overflow_unused)
             .ok()) {
      return overflow_error_maker();
    }
    ZETASQL_RETURN_IF_ERROR(ConvertTimestampToDatetime(result_timestamp,
                                               absl::UTCTimeZone(), &result));
  } else {
    // We are adding WEEK or larger granularity.  Adding WEEK or larger is
    // not supported by TIMESTAMP_ADD/AddTimestamp(), so we have a separate
    // function for adding larger date parts to civil datetimes.
    if (!AddAtLeastDaysToDatetime(datetime, part, interval, &result)) {
      return overflow_error_maker();
    }
  }

  if (!result.IsValid()) {
    return overflow_error_maker();
  }

  *output = result;
  return absl::OkStatus();
}

absl::Status AddDatetime(const DatetimeValue& datetime, DateTimestampPart part,
                         int64_t interval, DatetimeValue* output) {
  return AddDatetimeInternal(
      datetime, part, interval, output, [datetime, part, interval] {
        return MakeAddDatetimeOverflowError(datetime, part, interval);
      });
}

absl::Status SubDatetime(const DatetimeValue& datetime, DateTimestampPart part,
                         int64_t interval, DatetimeValue* output) {
  auto error_maker = [datetime, part, interval]() {
    return MakeSubDatetimeOverflowError(datetime, part, interval);
  };
  // The negation of std::numeric_limits<int64_t>::lowest() is undefined so we
  // handle it specially here. Subtracting std::numeric_limits<int64_t>::lowest()
  // is equivalent to adding std::numeric_limits<int64_t>::max() (which is
  // -1*(std::numeric_limits<int64_t>::lowest()+1)) intervals and 1 extra one.
  if (interval == std::numeric_limits<int64_t>::lowest()) {
    ZETASQL_RETURN_IF_ERROR(AddDatetimeInternal(datetime, part,
                                        std::numeric_limits<int64_t>::max(),
                                        output, error_maker));
    return AddDatetimeInternal(*output, part, 1, output, error_maker);
  }

  return AddDatetimeInternal(datetime, part, -interval, output, error_maker);
}

absl::Status AddDatetime(DatetimeValue datetime,
                         zetasql::IntervalValue interval,
                         DatetimeValue* output) {
  if (interval.get_months() != 0) {
    ZETASQL_RETURN_IF_ERROR(
        AddDatetime(datetime, MONTH, interval.get_months(), &datetime));
  }
  if (interval.get_days() != 0) {
    ZETASQL_RETURN_IF_ERROR(AddDatetime(datetime, DAY, interval.get_days(), &datetime));
  }
  bool had_overflow = false;
  if (interval.get_micros() != 0) {
    // AddDatetimeInternal allows to pass custom lambda function which is called
    // in case of overflow. But the 'datetime' is not updated when overflow
    // happens. This can happen in valid cases, because IntervalValue is
    // normalized to always have positive nano_fractions. So -1 nanosecond is
    // encoded as -1 microsecond and +999 nanoseconds. The -1 microsecond may
    // cause overflow but it will be later compensated by +999 nanoseconds. So
    // we opportunistically add 1 extra microsecond in the hope that overflow
    // won't happen with it, and later remember to subtract it from final
    // result.
    ZETASQL_RETURN_IF_ERROR(AddDatetimeInternal(datetime, MICROSECOND,
                                        interval.get_micros(), &datetime,
                                        [&had_overflow] {
                                          had_overflow = true;
                                          return absl::OkStatus();
                                        }));
    if (had_overflow) {
      ZETASQL_RETURN_IF_ERROR(AddDatetime(datetime, MICROSECOND,
                                  interval.get_micros() + 1, &datetime));
    }
  }
  if (interval.get_nano_fractions() != 0) {
    ZETASQL_RETURN_IF_ERROR(AddDatetime(datetime, NANOSECOND,
                                interval.get_nano_fractions(), &datetime));
  }
  if (had_overflow) {
    ZETASQL_RETURN_IF_ERROR(AddDatetime(datetime, MICROSECOND, -1, &datetime));
  }

  *output = datetime;
  return absl::OkStatus();
}

absl::Status AddTimestamp(int64_t timestamp, TimestampScale scale,
                          absl::TimeZone timezone, DateTimestampPart part,
                          int64_t interval, int64_t* output) {
  if (!IsValidTimestamp(timestamp, scale)) {
    return MakeEvalError() << "Invalid timestamp: " << timestamp;
  }
  ZETASQL_RETURN_IF_ERROR(
      AddTimestampInternal(timestamp, scale, timezone, part, interval, output));
  if (!IsValidTimestamp(*output, scale)) {
    return MakeAddTimestampOverflowError(timestamp, part, interval, scale,
                                         timezone);
  }
  return absl::OkStatus();
}

absl::Status AddTimestamp(int64_t timestamp, TimestampScale scale,
                          absl::string_view timezone_string,
                          DateTimestampPart part, int64_t interval,
                          int64_t* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return AddTimestamp(timestamp, scale, timezone, part, interval, output);
}

absl::Status AddTimestamp(absl::Time timestamp, absl::TimeZone timezone,
                          DateTimestampPart part, int64_t interval,
                          absl::Time* output) {
  bool had_overflow_unused;
  ZETASQL_RETURN_IF_ERROR(AddTimestampInternal(timestamp, timezone, part, interval,
                                       output, &had_overflow_unused));
  if (!IsValidTime(*output)) {
    return MakeAddTimestampOverflowError(timestamp, part, interval, timezone);
  }
  return absl::OkStatus();
}

absl::Status AddTimestamp(absl::Time timestamp,
                          absl::string_view timezone_string,
                          DateTimestampPart part, int64_t interval,
                          absl::Time* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return AddTimestamp(timestamp, timezone, part, interval, output);
}

absl::Status AddTimestamp(absl::Time timestamp, absl::TimeZone timezone,
                          zetasql::IntervalValue interval,
                          absl::Time* output) {
  if (interval.get_months() != 0) {
    return MakeEvalError() << "TIMESTAMP +/- INTERVAL is not supported for "
                              "intervals with non-zero MONTH or YEAR part.";
  }
  if (interval.get_days() != 0) {
    ZETASQL_RETURN_IF_ERROR(AddTimestamp(timestamp, timezone, DAY, interval.get_days(),
                                 &timestamp));
  }
  bool had_overflow = false;
  if (interval.get_micros() != 0) {
    // AddTimestampInternal returns OUT_OF_RANGE only when there is overflow in
    // the operation. The 'timestamp' is still updated, but outside of allowed
    // range of values, and 'had_overflow' is set to true.
    // This can happen in valid cases, because IntervalValue is normalized to
    // always have positive nano_fractions. So -1 nanosecond is encoded as
    // -1 microsecond and +999 nanoseconds. The -1 microsecond may cause
    // overflow but it will be later compensated by +999 nanoseconds, so we
    // ignore error here and check for valid result at the end.
    AddTimestampInternal(timestamp, timezone, MICROSECOND,
                         interval.get_micros(), &timestamp, &had_overflow)
        .IgnoreError();
  }
  if (interval.get_nano_fractions() != 0) {
    ZETASQL_RETURN_IF_ERROR(AddTimestamp(timestamp, timezone, NANOSECOND,
                                 interval.get_nano_fractions(), &timestamp));
  }
  if (ABSL_PREDICT_FALSE(had_overflow)) {
    if (!IsValidTime(timestamp)) {
      return MakeAddTimestampOverflowError(timestamp, MICROSECOND,
                                           interval.get_micros(), timezone);
    }
  }
  *output = timestamp;
  return absl::OkStatus();
}

absl::Status AddTimestampOverflow(absl::Time timestamp, absl::TimeZone timezone,
                                  DateTimestampPart part, int64_t interval,
                                  absl::Time* output, bool* had_overflow) {
  *had_overflow = false;
  absl::Status status = AddTimestampInternal(timestamp, timezone, part,
                                             interval, output, had_overflow);
  return *had_overflow ? absl::OkStatus() : status;
}

absl::Status SubTimestamp(int64_t timestamp, TimestampScale scale,
                          absl::TimeZone timezone, DateTimestampPart part,
                          int64_t interval, int64_t* output) {
  if (!IsValidTimestamp(timestamp, scale)) {
    return MakeEvalError() << "Invalid timestamp: " << timestamp;
  }
  if (interval == std::numeric_limits<int64_t>::lowest()) {
    return MakeSubTimestampOverflowError(timestamp, part, interval, scale,
                                         timezone);
  }

  ZETASQL_RETURN_IF_ERROR(AddTimestampInternal(timestamp, scale, timezone, part,
                                       -interval, output));
  if (!IsValidTimestamp(*output, scale)) {
    return MakeSubTimestampOverflowError(timestamp, part, interval, scale,
                                         timezone);
  }
  return absl::OkStatus();
}

absl::Status SubTimestamp(int64_t timestamp, TimestampScale scale,
                          absl::string_view timezone_string,
                          DateTimestampPart part, int64_t interval,
                          int64_t* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return SubTimestamp(timestamp, scale, timezone, part, interval, output);
}

absl::Status SubTimestamp(absl::Time timestamp, absl::TimeZone timezone,
                          DateTimestampPart part, int64_t interval,
                          absl::Time* output) {
  if (!IsValidTime(timestamp)) {
    return MakeEvalError() << "Invalid timestamp: " << timestamp;
  }
  bool had_overflow_unused;
  if (interval == std::numeric_limits<int64_t>::lowest() ||
      !AddTimestampInternal(timestamp, timezone, part, -interval, output,
                            &had_overflow_unused)
           .ok() ||
      !IsValidTime(*output)) {
    return MakeSubTimestampOverflowError(timestamp, part, interval, timezone);
  }
  return absl::OkStatus();
}

absl::Status SubTimestamp(absl::Time timestamp,
                          absl::string_view timezone_string,
                          DateTimestampPart part, int64_t interval,
                          absl::Time* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return SubTimestamp(timestamp, timezone, part, interval, output);
}

// Helper function that adds <interval> to <field>, where <field> is in the
// range [0, <radix>), and addition that overflows wraps around.  The number
// of times wrapped around is returned in <*carry>.
//
// In our use case, the maximum <radix> is 1000000000 for nanoseconds, and the
// minimum <radix> is 24 for hours. For any field from a valid TimeValue where
// the input <field> is smaller than <radix>, there is no risk of overflow.
//
// Requires that <radix> is positive, and <*field> is in range [0, <radix>).
static void AddOnField(int64_t interval, int64_t radix, int* field,
                       int64_t* carry) {
  // Expected invariants.
  ZETASQL_DCHECK_LE(0, *field);
  ZETASQL_DCHECK_LT(*field, radix);
  ZETASQL_DCHECK_LE(radix, 1000000000);  // number of nanos in a second.

  // <modulus> is the number of parts that we want to add to <field> for
  // the given <interval>.  For instance, if we want to add 30 hours
  // to an existing Time of 3:45, then:
  //     <interval> = 30
  //     <radix> = 24
  //     <*field> = 3
  // We compute the number of hours to add to <field> to produce the resulting
  // hours part of the clock time by computing MOD(30, 24) = 6.
  const int64_t modulus = zetasql_base::MathUtil::NonnegativeMod(interval, radix);

  // Adding <modulus> to <*field> cannot overflow because <*field> and
  // <modulus> are both less than <radix>, and <radix> is not greater than
  // 1000000000.
  *field += modulus;
  *carry = zetasql_base::MathUtil::FloorOfRatio(interval, radix);

  // Because both the original <*field> and the <modulus> should be in
  // [0, <radix>), then the updated <*field> should always be in
  // [0, <radix> * 2).
  ZETASQL_DCHECK(*field >= 0 && *field < radix * 2)
      << "AddOnField() produced an unexpected result " << *field
      << " by adding " << interval << " on a field of radix " << radix;

  if (*field >= radix) {
    // Adjust <*field> and <*carry> if adding <modulus> causes us to wrap
    // around once more.  For instance, if:
    //     <interval> = 30
    //     <radix> = 24
    //     <*field> = 22
    // Then when we add the <modulus> 6 to <*field> we get 28, which wraps
    // around to 4 and adds 1 to <*carry>.
    *field -= radix;
    *carry += 1;
  }
}

// Note that this function will return an error only when the input <time> is
// invalid or the input <part> is not valid for TIME, and never produce an
// overflow because of the addition.
// Note that if addition of the interval results in a TimeValue outside of the
// [00:00:00, 24:00:00) range, the result will be wrapped around to stay within
// 24 hours.
static absl::Status AddTimeInternal(const TimeValue& time,
                                    DateTimestampPart part, int64_t interval,
                                    TimeValue* output) {
  if (!time.IsValid()) {
    return MakeEvalError() << "Invalid time value: " << time.DebugString();
  }
  if (!(part == HOUR || part == MINUTE || part == SECOND ||
        part == MILLISECOND || part == MICROSECOND || part == NANOSECOND)) {
    return MakeEvalError() << "Unsupported DateTimestampPart "
                           << DateTimestampPart_Name(part);
  }

  int hour = time.Hour();
  int minute = time.Minute();
  int second = time.Second();
  int nanoseconds = time.Nanoseconds();
  while (interval != 0 && part != DAY) {
    switch (part) {
      case NANOSECOND:
        AddOnField(interval, 1000000000, &nanoseconds, &interval);
        part = SECOND;
        break;
      case MICROSECOND: {
        int microseconds = 0;
        int64_t carry_1;
        AddOnField(interval, 1000000, &microseconds, &carry_1);
        int64_t carry_2;
        AddOnField(microseconds * 1000, 1000000000, &nanoseconds, &carry_2);
        interval = carry_1 + carry_2;
        part = SECOND;
        break;
      }
      case MILLISECOND: {
        int millisecond = 0;
        int64_t carry_1;
        AddOnField(interval, 1000, &millisecond, &carry_1);
        int64_t carry_2;
        AddOnField(millisecond * 1000000, 1000000000, &nanoseconds, &carry_2);
        interval = carry_1 + carry_2;
        part = SECOND;
        break;
      }
      case SECOND: {
        AddOnField(interval, 60, &second, &interval);
        part = MINUTE;
        break;
      }
      case MINUTE: {
        AddOnField(interval, 60, &minute, &interval);
        part = HOUR;
        break;
      }
      case HOUR: {
        AddOnField(interval, 24, &hour, &interval);
        part = DAY;
        break;
      }
      default:
        break;
    }
  }
  *output = TimeValue::FromHMSAndNanos(hour, minute, second, nanoseconds);
  // This check should never fail as all fields should be in range.
  ZETASQL_DCHECK(output->IsValid()) << output->DebugString();
  return absl::OkStatus();
}

absl::Status AddTime(const TimeValue& time, DateTimestampPart part,
                     int64_t interval, TimeValue* output) {
  return AddTimeInternal(time, part, interval, output);
}

absl::Status SubTime(const TimeValue& time, DateTimestampPart part,
                     int64_t interval, TimeValue* output) {
  if (interval == std::numeric_limits<int64_t>::lowest()) {
    ZETASQL_RETURN_IF_ERROR(AddTimeInternal(
        time, part, std::numeric_limits<int64_t>::max(), output));
    return AddTimeInternal(*output, part, 1, output);
  }
  return AddTimeInternal(time, part, -interval, output);
}

absl::Status DiffTimes(const TimeValue& time1, const TimeValue& time2,
                       DateTimestampPart part, int64_t* output) {
  if (!time1.IsValid()) {
    return MakeEvalError() << "Invalid time value: " << time1.DebugString();
  }
  if (!time2.IsValid()) {
    return MakeEvalError() << "Invalid time value: " << time2.DebugString();
  }

  const absl::CivilSecond civil_time_1(1970, 1, 1, time1.Hour(), time1.Minute(),
                                       time1.Second());
  const absl::CivilSecond civil_time_2(1970, 1, 1, time2.Hour(), time2.Minute(),
                                       time2.Second());
  switch (part) {
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
    case NANOSECOND:
      return DiffWithPartsSmallerThanDay(
          civil_time_1, time1.Nanoseconds(), civil_time_2, time2.Nanoseconds(),
          part,
          []() -> absl::Status {
            ZETASQL_RET_CHECK_FAIL() << "TIME_DIFF should never have overflow error";
          } /* nano_overflow_error_maker */,
          output);
    case YEAR:
    case MONTH:
    case DAY:
    case DAYOFWEEK:
    case DAYOFYEAR:
    case QUARTER:
    case DATE:
    case WEEK:
    case DATETIME:
    case TIME:
      return MakeEvalError()
             << "Unsupported DateTimestampPart " << DateTimestampPart_Name(part)
             << " for TIME_DIFF";
    default:
      return MakeEvalError()
             << "Unexpected DateTimestampPart " << DateTimestampPart_Name(part)
             << " for TIME_DIFF";
  }
}

// INTERVAL difference between TIMEs is HOUR TO SECOND granularity.
// Months and days are always zero.
absl::StatusOr<IntervalValue> IntervalDiffTimes(const TimeValue& time1,
                                                const TimeValue& time2) {
  int64_t nanos;
  ZETASQL_RETURN_IF_ERROR(DiffTimes(time1, time2, NANOSECOND, &nanos));
  return IntervalValue::FromNanos(nanos);
}

absl::Status TruncateTime(const TimeValue& time, DateTimestampPart part,
                          TimeValue* output) {
  if (!time.IsValid()) {
    return MakeEvalError() << "Invalid time value: " << time.DebugString();
  }
  switch (part) {
    case HOUR:
      *output = TimeValue::FromHMSAndNanos(time.Hour(), 0, 0, 0);
      break;
    case MINUTE:
      *output = TimeValue::FromHMSAndNanos(time.Hour(), time.Minute(), 0, 0);
      break;
    case SECOND:
      *output = TimeValue::FromHMSAndNanos(time.Hour(), time.Minute(),
                                           time.Second(), 0);
      break;
    case MILLISECOND:
      *output = TimeValue::FromHMSAndNanos(
          time.Hour(), time.Minute(), time.Second(),
          time.Nanoseconds() / powers_of_ten[6] * powers_of_ten[6]);
      break;
    case MICROSECOND:
      *output = TimeValue::FromHMSAndNanos(
          time.Hour(), time.Minute(), time.Second(),
          time.Nanoseconds() / powers_of_ten[3] * powers_of_ten[3]);
      break;
    case NANOSECOND:
      *output = time;
      break;
    case YEAR:
    case MONTH:
    case DAY:
    case DAYOFWEEK:
    case DAYOFYEAR:
    case QUARTER:
    case DATE:
    case WEEK:
    case DATETIME:
    case TIME:
      return MakeEvalError()
             << "Unsupported DateTimestampPart " << DateTimestampPart_Name(part)
             << " for TIME_TRUNC";
    default:
      return MakeEvalError()
             << "Unexpected DateTimestampPart " << DateTimestampPart_Name(part)
             << " for TIME_TRUNC";
  }
  return absl::OkStatus();
}

absl::Status TruncateDate(int32_t date, DateTimestampPart part,
                          int32_t* output) {
  return TruncateDateImpl(date, part, /*enforce_range=*/true, output);
}

absl::Status TimestampTrunc(int64_t timestamp, absl::TimeZone timezone,
                            DateTimestampPart part, int64_t* output) {
  return TimestampTruncImpl(timestamp, kMicroseconds, NEW_TIMESTAMP_TYPE,
                            timezone, part, output);
}

absl::Status TimestampTrunc(int64_t timestamp,
                            absl::string_view timezone_string,
                            DateTimestampPart part, int64_t* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return TimestampTrunc(timestamp, timezone, part, output);
}

absl::Status TimestampTrunc(absl::Time timestamp, absl::TimeZone timezone,
                            DateTimestampPart part, absl::Time* output) {
  if (!IsValidTime(timestamp)) {
    return MakeEvalError() << "Invalid timestamp value: "
                           << TimestampErrorString(timestamp, timezone);
  }
  if (part != SECOND && part != MILLISECOND && part != MICROSECOND &&
      part != NANOSECOND) {
    return TimestampTruncAtLeastMinute(timestamp, kNanoseconds, timezone, part,
                                       output);
  }
  switch (part) {
    case SECOND: {
      *output = absl::FromUnixSeconds(absl::ToUnixSeconds(timestamp));
      break;
    }
    case MILLISECOND: {
      *output = absl::FromUnixMillis(absl::ToUnixMillis(timestamp));
      break;
    }
    case MICROSECOND: {
      *output = absl::FromUnixMicros(absl::ToUnixMicros(timestamp));
      break;
    }
    case NANOSECOND: {
      *output = absl::UnixEpoch() + absl::Floor(timestamp - absl::UnixEpoch(),
                                                absl::Nanoseconds(1));
      break;
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Should not reach here for part="
                       << DateTimestampPart_Name(part);
  }
  return absl::OkStatus();
}

absl::Status TimestampTrunc(absl::Time timestamp,
                            absl::string_view timezone_string,
                            DateTimestampPart part, absl::Time* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return TimestampTrunc(timestamp, timezone, part, output);
}

absl::Status TruncateTimestamp(int64_t timestamp, TimestampScale scale,
                               absl::TimeZone timezone, DateTimestampPart part,
                               int64_t* output) {
  return TimestampTruncImpl(timestamp, scale, LEGACY_TIMESTAMP_TYPE, timezone,
                            part, output);
}

absl::Status TruncateTimestamp(int64_t timestamp, TimestampScale scale,
                               absl::string_view timezone_string,
                               DateTimestampPart part, int64_t* output) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  return TruncateTimestamp(timestamp, scale, timezone, part, output);
}

absl::Status TruncateDatetime(const DatetimeValue& datetime,
                              DateTimestampPart part, DatetimeValue* output) {
  if (!datetime.IsValid()) {
    return MakeEvalError() << "Invalid datetime value: "
                           << datetime.DebugString();
  }
  switch (part) {
    case YEAR:
    case ISOYEAR:
    case QUARTER:
    case MONTH:
    case WEEK:
    case ISOWEEK:
    case WEEK_MONDAY:
    case WEEK_TUESDAY:
    case WEEK_WEDNESDAY:
    case WEEK_THURSDAY:
    case WEEK_FRIDAY:
    case WEEK_SATURDAY:
    case DAY: {
      int32_t date;
      ZETASQL_RETURN_IF_ERROR(ExtractFromDatetime(DATE, datetime, &date));
      ZETASQL_RETURN_IF_ERROR(TruncateDate(date, part, &date));
      if (!IsValidDate(date)) {
        return MakeEvalError() << "Truncating " << datetime.DebugString()
                               << " to " << DateTimestampPart_Name(part)
                               << " produces an invalid Datetime value";
      }
      return ConstructDatetime(date, TimeValue() /* 00:00:00 */, output);
    }
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
    case NANOSECOND: {
      int32_t date;
      ZETASQL_RETURN_IF_ERROR(ExtractFromDatetime(DATE, datetime, &date));

      TimeValue time;
      ZETASQL_RETURN_IF_ERROR(ExtractTimeFromDatetime(datetime, &time));
      ZETASQL_RETURN_IF_ERROR(TruncateTime(time, part, &time));

      return ConstructDatetime(date, time, output);
    }
    case DAYOFWEEK:
    case DAYOFYEAR:
    case DATE:
    case DATETIME:
    case TIME:
      return MakeEvalError()
             << "Unsupported DateTimestampPart " << DateTimestampPart_Name(part)
             << " for TIME_TRUNC";
    default:
      return MakeEvalError()
             << "Unexpected DateTimestampPart " << DateTimestampPart_Name(part)
             << " for TIME_TRUNC";
  }
}

absl::Status TimestampDiff(int64_t timestamp1, int64_t timestamp2,
                           TimestampScale scale, DateTimestampPart part,
                           int64_t* output) {
  absl::Time base_time1 = MakeTime(timestamp1, scale);
  absl::Time base_time2 = MakeTime(timestamp2, scale);
  return TimestampDiff(base_time1, base_time2, part, output);
}

absl::Status TimestampDiff(absl::Time timestamp1, absl::Time timestamp2,
                           DateTimestampPart part, int64_t* output) {
  absl::Duration duration = timestamp1 - timestamp2;
  absl::Duration rem;
  absl::Duration divide;
  switch (part) {
    case DAY:
      divide = absl::Hours(24);
      break;
    case HOUR:
      divide = absl::Hours(1);
      break;
    case MINUTE:
      divide = absl::Minutes(1);
      break;
    case SECOND:
      divide = absl::Seconds(1);
      break;
    case MILLISECOND:
      divide = absl::Milliseconds(1);
      break;
    case MICROSECOND:
      divide = absl::Microseconds(1);
      break;
    case NANOSECOND:
      divide = absl::Nanoseconds(1);
      break;
    case YEAR:
    case QUARTER:
    case MONTH:
    case DATE:
    case DAYOFWEEK:
    case DAYOFYEAR:
    case WEEK:
      return MakeEvalError() << "Unsupported DateTimestampPart "
                             << DateTimestampPart_Name(part);
    default:
      return MakeEvalError()
             << "Unexpected DateTimestampPart " << DateTimestampPart_Name(part);
  }
  *output = absl::IDivDuration(duration, divide, &rem);
  // If we make sure the input timestamps are always valid, we can only do this
  // check for nano.
  if ((*output == std::numeric_limits<int64_t>::max() ||
       *output == std::numeric_limits<int64_t>::lowest()) &&
      rem != absl::ZeroDuration()) {
    return MakeEvalError() << "TIMESTAMP_DIFF at "
                           << DateTimestampPart_Name(part)
                           << " precision between values of " << timestamp1
                           << " and " << timestamp2 << " causes overflow";
  }
  return absl::OkStatus();
}

// INTERVAL difference between TIMESTAMPs is HOUR TO SECOND granularity.
// Months and days are always zero.
absl::StatusOr<IntervalValue> IntervalDiffTimestamps(absl::Time timestamp1,
                                                     absl::Time timestamp2) {
  absl::Duration duration = timestamp1 - timestamp2;
  absl::Duration nanos_rem;
  int64_t micros =
      absl::IDivDuration(duration, absl::Microseconds(1), &nanos_rem);
  absl::Duration rem;
  int64_t nano_fractions =
      absl::IDivDuration(nanos_rem, absl::Nanoseconds(1), &rem);
  ZETASQL_RET_CHECK(rem == absl::ZeroDuration());
  __int128 nanos = IntervalValue::kNanosInMicro128 * micros + nano_fractions;
  return IntervalValue::FromNanos(nanos);
}

std::string TimestampScale_Name(TimestampScale scale) {
  switch (scale) {
    case kSeconds:
      return "TIMESTAMP_SECOND";
    case kMilliseconds:
      return "TIMESTAMP_MILLISECOND";
    case kMicroseconds:
      return "TIMESTAMP_MICROSECOND";
    case kNanoseconds:
      return "TIMESTAMP_NANOSECOND";
  }
}

int DateTimestampPart_FromName(absl::string_view name) {
  const std::string upper_name = absl::AsciiStrToUpper(name);
  DateTimestampPart part;
  bool is_valid = DateTimestampPart_Parse(upper_name, &part);
  return is_valid ? part : -1;
}

// TODO These encode/decode interfaces will probably get generalized
// some to support more field types and more encodings, particularly once
// we start supporting NWP wrappers.
absl::Status DecodeFormattedDate(int64_t input_formatted_date,
                                 FieldFormat::Format format,
                                 int32_t* output_date, bool* output_is_null) {
  // Check for int64_t values that don't fit in int32_t.
  if (static_cast<int32_t>(input_formatted_date) != input_formatted_date) {
    return MakeEvalError() << "Invalid non-int32_t date: "
                           << input_formatted_date;
  }

  *output_is_null = false;
  switch (format) {
    case FieldFormat::DATE:
      *output_date = input_formatted_date;
      break;

    case FieldFormat::DATE_DECIMAL: {
      if (input_formatted_date == 0) {
        // For DATE_DECIMAL, the integer 0 decodes to NULL.
        *output_date = 0;
        *output_is_null = true;
      } else {
        const int day = input_formatted_date % 100;
        const int month = (input_formatted_date / 100) % 100;
        const int year = input_formatted_date / (100 * 100);
        absl::CivilDay date;
        if (!MakeDate(year, month, day, &date)) {
          return MakeEvalError()
                 << "Invalid DATE_DECIMAL: " << input_formatted_date;
        }
        *output_date = CivilDayToEpochDays(date);
      }
      break;
    }

    case FieldFormat::DEFAULT_FORMAT:
    default:
      return MakeEvalError() << "Invalid date decode format: " << format;
  }

  return absl::OkStatus();
}

absl::Status EncodeFormattedDate(int32_t input_date, FieldFormat::Format format,
                                 int32_t* output_formatted_date) {
  switch (format) {
    case FieldFormat::DATE:
      *output_formatted_date = input_date;
      return absl::OkStatus();

    case FieldFormat::DATE_DECIMAL: {
      if (!IsValidDate(input_date)) {
        return MakeEvalError()
               << "Invalid input date for encoding: " << input_date;
      }
      const absl::CivilDay date = EpochDaysToCivilDay(input_date);
      *output_formatted_date =
          // Cast to int32_t is safe; IsValidDate guarantees small numbers.
          static_cast<int32_t>(date.year()) * 100 * 100 + date.month() * 100 +
          date.day();
      return absl::OkStatus();
    }

    case FieldFormat::DEFAULT_FORMAT:
    default:
      return MakeEvalError() << "Invalid date decode format: " << format;
  }
}

int32_t CurrentDate(absl::TimeZone timezone) {
  return CivilDayToEpochDays(absl::ToCivilDay(absl::Now(), timezone));
}

absl::Status CurrentDate(absl::string_view timezone_string, int32_t* date) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(timezone_string, &timezone));
  *date = CurrentDate(timezone);
  return absl::OkStatus();
}

int64_t CurrentTimestamp() { return absl::ToUnixMicros(absl::Now()); }

// TODO: Consider replace NarrowTimestampIfPossible with
// NarrowTimestampScaleIfPossible. Need to run some perf/benchmark first.
void NarrowTimestampScaleIfPossible(absl::Time time, TimestampScale* scale) {
  // We only care about the subseconds of the absl::Time, so timezone doesn't
  // matter (assuming all UTC offsets in all timezones are whole seconds).
  int64_t subsecond = absl::ToInt64Nanoseconds(
      time - absl::FromUnixSeconds(absl::ToUnixSeconds(time)));
  const TimestampScale narrowed_scale =
      (subsecond == 0)  // (subsecond % 1000000000 == 0)
          ? kSeconds
          : (subsecond % 1000000 == 0) ? kMilliseconds
            : (subsecond % 1000 == 0)  ? kMicroseconds
                                       : kNanoseconds;
  if (narrowed_scale < *scale) {
    // return narrowed scale;
    *scale = narrowed_scale;
  }
}

namespace internal_functions {

// Expand "%Z" in <format_string> to the ZetaSQL-defined format:
//   'UTC[+/-HHMM]'
// The format produced by absl::FormatTime() is different from the ZetaSQL
// format and has not been stable over time, so we handle %Z here rather than
// pass it through to FormatTime().
//
// Expand "%Q" in <format_string> into 1 based quarter number.  This is
// ZetaSQL's extension to strftime format strings (see b/26564776).  We have
// to do it here because absl::FormatTime does not support %Q.
//
// Expand "%J" in <format_string> into an ISO day of year number.  This is a
// ZetaSQL extension to strftime, and we have to do it here because
// absl::FormatTime does not support %J.
//
// Requires that the <expanded_format_string> is empty, and if %Z is present
// then the <timezone> is normalized to an hours/minutes offset
// (i.e., <timezone> does not include any 'seconds' offset).
absl::Status ExpandPercentZQJ(absl::string_view format_string,
                              absl::Time base_time, absl::TimeZone timezone,
                              const ExpansionOptions& expansion_options,
                              std::string* expanded_format_string) {
  ZETASQL_RET_CHECK(expanded_format_string->empty());
  if (format_string.empty()) {
    return absl::OkStatus();
  }
  expanded_format_string->reserve(format_string.size());

  for (size_t index = 0;; index += 2) {
    const size_t pct = format_string.find('%', index);
    if (pct == format_string.size() - 1 ||
        pct == std::string::npos) {  // no "%?"
      absl::StrAppend(
          expanded_format_string,
          format_string.substr(index, format_string.size() - index));
      break;
    }
    if (pct != index) {
      absl::StrAppend(expanded_format_string,
                      format_string.substr(index, pct - index));
      index = pct;
    }
    if (expansion_options.expand_quarter && format_string[pct + 1] == 'Q') {
      // Handle %Q, computing quarter from month.
      absl::StrAppend(
          expanded_format_string,
          absl::StrFormat(
              "%d",
              (absl::ToCivilMonth(base_time, timezone).month() - 1) / 3 + 1));
    } else if (format_string[pct + 1] == 'Z') {
      // Handle %Z, computing the ZetaSQL defined timezone format.
      absl::StrAppend(expanded_format_string, "UTC");
      if (int seconds = timezone.At(base_time).offset) {
        const char sign = (seconds < 0 ? '-' : '+');
        int minutes = seconds / 60;
        seconds %= 60;
        if (sign == '-') {
          if (seconds > 0) {
            seconds -= 60;
            minutes += 1;
          }
          seconds = -seconds;
          minutes = -minutes;
        }
        int hours = minutes / 60;
        minutes %= 60;
        expanded_format_string->push_back(sign);
        ZETASQL_RET_CHECK_EQ(seconds, 0);
        if (minutes != 0) {
          absl::StrAppend(expanded_format_string,
                          absl::StrFormat("%02d%02d", hours, minutes));
        } else {
          absl::StrAppend(expanded_format_string, absl::StrFormat("%d", hours));
        }
      }
    } else if (expansion_options.expand_iso_dayofyear &&
               format_string[pct + 1] == 'J') {
      // Handle %J, computing ISO day of year.
      const absl::TimeZone::CivilInfo info = timezone.At(base_time);
      const absl::CivilDay civil_day = absl::CivilDay(info.cs);
      return MakeEvalError() << "Format element %J not supported yet";
    } else {
      // None of %J, %Q, %Z. Copy as is.
      absl::StrAppend(expanded_format_string, format_string.substr(index, 2));
    }
  }
  return absl::OkStatus();
}

int DayOfWeekIntegerSunToSat1To7(absl::Weekday weekday) {
  switch (weekday) {
    case absl::Weekday::sunday:
      return 1;
    case absl::Weekday::monday:
      return 2;
    case absl::Weekday::tuesday:
      return 3;
    case absl::Weekday::wednesday:
      return 4;
    case absl::Weekday::thursday:
      return 5;
    case absl::Weekday::friday:
      return 6;
    case absl::Weekday::saturday:
      return 7;
  }
}

// Returns the timezone sign, hour and minute.
void GetSignHourAndMinuteTimeZoneOffset(const absl::TimeZone::CivilInfo& info,
                                        bool* positive_offset,
                                        int32_t* hour_offset,
                                        int32_t* minute_offset) {
  int32_t second_offset = info.offset;
  if (info.offset >= 0) {
    *positive_offset = true;
  } else {
    *positive_offset = false;
    second_offset = -second_offset;
  }

  *hour_offset = second_offset / 60 / 60;
  *minute_offset = (second_offset / 60) % 60;
}

// Returns <timezone> if the timezone offset for (<base_time>, <timezone>)
// is minute aligned.  Otherwise returns a fixed-offset timezone using that
// offset truncated to a minute boundary (i.e., eliminating the non-zero
// seconds part of the offset).
//
// ZetaSQL does not support rendering numeric timezones with sub-minute
// offsets (neither do ISO6801 or RFC3339).  If the timezone has a sub-minute
// offset like -07:52:58 (which happens for the America/Los_Angeles time zone
// in years before 1884), we instead treat it (and display it) as -07:52.
absl::TimeZone GetNormalizedTimeZone(absl::Time base_time,
                                     absl::TimeZone timezone) {
  const int timezone_offset = timezone.At(base_time).offset;
  if (const int seconds_offset = timezone_offset % 60)
    return absl::FixedTimeZone(timezone_offset - seconds_offset);
  return timezone;
}
}  // namespace internal_functions
}  // namespace functions
}  // namespace zetasql
