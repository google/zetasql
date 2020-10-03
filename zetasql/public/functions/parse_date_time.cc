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
#include <limits>
#include <string>

#include "zetasql/base/logging.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/datetime.pb.h"
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

constexpr int64_t kNumMillisPerSecond = 1000;

static std::string TimeZoneOffsetToString(int minutes_offset) {
  const int timezone_hour = zetasql_base::MathUtil::Abs(minutes_offset) / 60;
  const int timezone_minute = zetasql_base::MathUtil::Abs(minutes_offset) % 60;
  std::string offset_string;
  absl::StrAppendFormat(&offset_string, "%c%02d:%02d",
                        (minutes_offset < 0 ? '-' : '+'), timezone_hour,
                        timezone_minute);
  return offset_string;
}

// Converts Time to int64_t microseconds and validates the value is within
// the supported ZetaSQL range.
static bool ConvertTimeToTimestamp(absl::Time time, int64_t* timestamp) {
  *timestamp = absl::ToUnixMicros(time);
  return IsValidTimestamp(*timestamp, kMicroseconds);
}

static const char kDigits[] = "0123456789";

// The input const char* 'dp' must be not nullptr and it must be smaller than
// 'end_of_data'. Otherwise, nullptr will be returned.
// The returned const char* can be nullptr or an address that in
// ['dp', 'end_of_data']. When it is in ('dp', 'end_of_data'], which means the
// integer is parsed successfully. Especially, when the returned const char* is
// 'end_of_data', it means that all the 'timestamp_string' is parsed.
template <typename T>
static const char* ParseInt(const char* dp, const char* end_of_data,
                            int max_width, T min, T max, T* vp) {
  if (dp == nullptr || dp >= end_of_data) {
    return nullptr;
  }

  const T kmin = std::numeric_limits<T>::min();
  bool neg = false;
  T value = 0;
  if (*dp == '-') {
    neg = true;
    if (max_width <= 0 || --max_width != 0) {
      ++dp;
    } else {
      return nullptr;  // max_width was 1
    }
  }
  if (const char* const bp = dp) {
    const char* cp;
    while (dp < end_of_data && (cp = strchr(kDigits, *dp))) {
      int d = static_cast<int>(cp - kDigits);
      if (d >= 10) break;
      if (ABSL_PREDICT_FALSE(value < kmin / 10)) {
        return nullptr;
      }
      value *= 10;
      if (ABSL_PREDICT_FALSE(value < kmin + d)) {
        return nullptr;
      }
      value -= d;
      dp += 1;
      if (max_width > 0 && --max_width == 0) break;
    }
    if (dp != bp && (neg || value != kmin)) {
      if (!neg || value != 0) {
        if (!neg) value = -value;  // make positive
        if (min <= value && value <= max) {
          *vp = value;
        } else {
          return nullptr;
        }
      } else {
        return nullptr;
      }
    } else {
      return nullptr;
    }
  }
  return dp;
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
    while (dp < end && !std::isspace(*dp))
      zone->push_back(*dp++);
    if (zone->empty()) dp = nullptr;
  }
  return dp;
}

static const int64_t powers_of_ten[] = {1, 10, 100, 1000, 10000, 100000, 1000000,
                                      10000000, 100000000, 1000000000};

// Parses up to <max_digits> (0 means unbounded), and returns a Duration
// (digits beyond the given precision are truncated).
static const char* ParseSubSeconds(const char* dp, const char* end_of_data,
                                   int max_digits, TimestampScale scale,
                                   absl::Duration* subseconds) {
  if (dp != nullptr) {
    if (dp < end_of_data && *dp == '.') {
      int64_t parsed_value = 0;
      int64_t num_digits = 0;
      const char* const bp = ++dp;
      const char* cp;
      while (dp < end_of_data &&
             (cp = strchr(kDigits, *dp)) &&
             (max_digits == 0 || num_digits < max_digits)) {
        int d = static_cast<int>(cp - kDigits);
        if (d < 0 || d >= 10) break;  // Not a digit.
        ++dp;
        if (num_digits >= scale) {
          // Consume but ignore digits beyond the given precision.
          continue;
        }
        parsed_value *= 10;
        parsed_value += d;
        num_digits++;
      }
      if (dp != bp) {
        if (num_digits < scale) {
          // We consumed less than precision digits, so widen parsed_value to
          // given precision.
          parsed_value *= powers_of_ten[scale - num_digits];
        }
        if (scale == kMicroseconds) {
          *subseconds = absl::Microseconds(parsed_value);
        } else {
          // NANO precision.
          *subseconds = absl::Nanoseconds(parsed_value);
        }
      } else {
        dp = nullptr;
      }
    }
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

// This function generally uses strptime() to handle each format element,
// but supports additional format element extensions and a few behavior
// deviations for ZetaSQL semantics.
// 'format' and 'timestamp_string' do not need to be null-terminated.
static absl::Status ParseTime(absl::string_view format,
                              absl::string_view timestamp_string,
                              const absl::TimeZone default_timezone,
                              TimestampScale scale, absl::Time* timestamp) {
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
  // TODO: Handle UTF and unicode white space characters as well.
  while (data != end_of_data && std::isspace(*data)) ++data;

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
  // handling for timezones, subseconds, and a few others.
  while (data != nullptr && data < end_of_data && fmt < end_of_fmt) {
    // If the next format character is a space, skip over all the next spaces
    // in both the format and the input timestamp string.
    // TODO: Handle UTF and unicode white space characters as well.
    if (std::isspace(*fmt)) {
      while (data < end_of_data && std::isspace(*data)) ++data;
      while (++fmt < end_of_fmt && std::isspace(*fmt)) continue;
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
        continue;
      }
      case 'm': {
        data = ParseInt(data, end_of_data, 2, 1, 12, &tm.tm_mon);
        tm.tm_mon -= 1;
        continue;
      }
      case 'd': {
        data = ParseInt(data, end_of_data, 2, 1, 31, &tm.tm_mday);
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
      case 'V': {
        int week_number;
        // It is validated for range, but not used in the resulting timestamp.
        data = ParseInt(data, end_of_data, 2, 1, 53, &week_number);
        continue;
      }
      case 'Q': {
        int quarter_number;
        data = ParseInt(data, end_of_data, 1, 1, 4, &quarter_number);
        if (data != nullptr) {
          tm.tm_mon = (quarter_number - 1) * 3;
          tm.tm_mday = 1;
        }
        continue;
      }
      case 'I':
      case 'r':  // probably uses %I
      case 'l':
        twelve_hour = true;
        break;
      case 'R':  // uses %H
      case 'T':  // uses %H
      case 'c':  // probably uses %H
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
        continue;
      }
      case 'E':
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
          continue;
        }
        if (fmt + 1 < end_of_fmt && *fmt == '*' && *(fmt + 1) == 'S') {
          data = ParseInt(data, end_of_data, 2, 0, 60, &tm.tm_sec);
          data = ParseSubSeconds(data, end_of_data, 0 /* max_digits */, scale,
                                 &subseconds);
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
                data = ParseSubSeconds(data, end_of_data, n, scale,
                                       &subseconds);
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
      case 'O':
        if (fmt < end_of_fmt && *fmt == 'H') twelve_hour = false;
        if (fmt < end_of_fmt && *fmt == 'I') twelve_hour = true;
        if (fmt < end_of_fmt && *fmt == 'u') {
          // Day of week 1-7.  '%Ou' is treated like '%u' in en_US locale.
          data = ParseInt(data, end_of_data, 1, 1, 7, &tm.tm_wday);
          fmt += 1;
          continue;
        }
        if (fmt < end_of_fmt && *fmt == 'V') {
          int week_number;
          // Week number 1-53.  '%OV' is treated like '%V' in en_US locale.
          data = ParseInt(data, end_of_data, 2, 1, 53, &week_number);
          fmt += 1;
          continue;
        }
        if (fmt < end_of_fmt) ++fmt;
        break;
      default:
        // No special handling for this format element, let strptime() do it.
        break;
    }

    const char* orig_data = data;
    std::string format_element(percent, fmt - percent);

    // When no special handling for this format element in the switch statement
    // above, call ParseTM() invokes strptime() to parse the current format
    // element and updates tm.
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

    // If we successfully parsed %p we need to remember whether the result
    // was AM or PM so that we can adjust tm_hour before calling
    // ConvertDateTime().  So reparse the input with a known AM hour, and
    // check if it is shifted to a PM hour.
    if (format_element == "%p" && data != nullptr) {
      std::string test_input = "1" + std::string(orig_data, data - orig_data);
      const char* test_data = test_input.c_str();
      struct tm tmp = { 0 };
      ParseTM(test_data, "%I%p", &tmp);
      afternoon = (tmp.tm_hour == 13);
    }
  }

  // Adjust a 12-hour tm_hour value if it should be in the afternoon.
  if (twelve_hour && afternoon) {
    tm.tm_hour += 12;
  }

  // Skip any remaining whitespace.
  // TODO: Handle UTF and unicode white space characters as well.
  if (data != nullptr) {
    while (data < end_of_data && std::isspace(*data)) ++data;
  }
  if (fmt != nullptr) {
    // Note that in addition to skipping trailing whitespace in the format
    // string, we must also handle a corner case where we have consumed the
    // entire input data string, but the format string still contains %n or %t
    // format elements (which consume 0 or more whitespaces).  So we must
    // also ignore any remaining %n or %t format elements.
    while (fmt < end_of_fmt && (std::isspace(*fmt) || *fmt == '%')) {
      // TODO: Handle UTF and unicode white space characters as well.
      if (std::isspace(*fmt)) {
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

  const absl::TimeConversion tc =
      ConvertDateTime(year, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                      tm.tm_sec, timezone);

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
}

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
                              int64_t* timestamp) {
  absl::Time base_time;
  ZETASQL_RETURN_IF_ERROR(ParseTime(format, timestamp_string, default_timezone,
                            kMicroseconds, &base_time));
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
                              absl::string_view date_string, int32_t* date) {
  // Validates if the <format> has any unsupported DATE formats.
  ZETASQL_RETURN_IF_ERROR(ValidateDateFormat(format));

  // Invoke the ParseStringToTimestamp() to parse the <date_string> to a
  // timestamp then extracts the date part.
  int64_t timestamp;
  ZETASQL_RETURN_IF_ERROR(ParseStringToTimestamp(format, date_string,
                                         absl::UTCTimeZone(), &timestamp));
  ZETASQL_RETURN_IF_ERROR(ExtractFromTimestamp(DATE, timestamp, kMicroseconds,
                                       absl::UTCTimeZone(), date));
  return absl::OkStatus();
}

}  // namespace

absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    const absl::TimeZone default_timezone,
                                    int64_t* timestamp) {
  return ParseTime(format_string, timestamp_string, default_timezone,
                   timestamp);
}

absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    absl::string_view default_timezone_string,
                                    int64_t* timestamp) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(default_timezone_string, &timezone));
  return ParseStringToTimestamp(format_string, timestamp_string, timezone,
                                timestamp);
}

absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    const absl::TimeZone default_timezone,
                                    absl::Time* timestamp) {
  ZETASQL_RETURN_IF_ERROR(ParseTime(format_string, timestamp_string, default_timezone,
                            kNanoseconds, timestamp));
  return absl::OkStatus();
}

absl::Status ParseStringToTimestamp(absl::string_view format_string,
                                    absl::string_view timestamp_string,
                                    absl::string_view default_timezone_string,
                                    absl::Time* timestamp) {
  absl::TimeZone timezone;
  ZETASQL_RETURN_IF_ERROR(MakeTimeZone(default_timezone_string, &timezone));
  return ParseStringToTimestamp(format_string, timestamp_string, timezone,
                                timestamp);
}

absl::Status ParseStringToDate(absl::string_view format_string,
                               absl::string_view date_string, int32_t* date) {
  return ParseDate(format_string, date_string, date);
}

absl::Status ParseStringToTime(absl::string_view format_string,
                               absl::string_view time_string,
                               TimestampScale scale,
                               TimeValue* time) {
  CHECK(scale == kNanoseconds || scale == kMicroseconds);
  ZETASQL_RETURN_IF_ERROR(ValidateTimeFormat(format_string));

  absl::Time base_time;
  ZETASQL_RETURN_IF_ERROR(ParseTime(format_string, time_string, absl::UTCTimeZone(),
                            scale, &base_time));
  return ConvertTimestampToTime(base_time, absl::UTCTimeZone(), scale, time);
}

absl::Status ParseStringToDatetime(absl::string_view format_string,
                                   absl::string_view datetime_string,
                                   TimestampScale scale,
                                   DatetimeValue* datetime) {
  CHECK(scale == kNanoseconds || scale == kMicroseconds);
  ZETASQL_RETURN_IF_ERROR(ValidateDatetimeFormat(format_string));

  absl::Time base_time;
  ZETASQL_RETURN_IF_ERROR(ParseTime(format_string, datetime_string, absl::UTCTimeZone(),
                            scale, &base_time));
  return ConvertTimestampToDatetime(base_time, absl::UTCTimeZone(), datetime);
}

}  // namespace functions
}  // namespace zetasql
