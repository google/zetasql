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

#include "zetasql/public/interval_value.h"

#include <cmath>

#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "absl/base/casts.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "zetasql/base/endian.h"
#include "re2/re2.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

zetasql_base::StatusOr<IntervalValue> IntervalValue::FromYMDHMS(int64_t years,
                                                        int64_t months,
                                                        int64_t days, int64_t hours,
                                                        int64_t minutes,
                                                        int64_t seconds) {
  absl::Status status;
  int64_t year_months;
  if (!functions::Multiply(IntervalValue::kMonthsInYear, years, &year_months,
                           &status)) {
    return status;
  }
  if (!functions::Add(months, year_months, &months, &status)) {
    return status;
  }

  // Int128 math cannot overflow
  __int128 nanos = kNanosInHour * hours + kNanosInMinute * minutes +
                   kNanosInSecond * seconds;
  return FromMonthsDaysNanos(months, days, nanos);
}

size_t IntervalValue::HashCode() const {
  return absl::Hash<IntervalValue>()(*this);
}

void IntervalValue::SerializeAndAppendToBytes(std::string* bytes) const {
  int64_t micros = zetasql_base::LittleEndian::FromHost64(micros_);
  bytes->append(reinterpret_cast<const char*>(&micros), sizeof(micros));
  int32_t days = zetasql_base::LittleEndian::FromHost32(days_);
  bytes->append(reinterpret_cast<const char*>(&days), sizeof(days));
  uint32_t months_nanos = zetasql_base::LittleEndian::FromHost32(months_nanos_);
  bytes->append(reinterpret_cast<const char*>(&months_nanos),
                sizeof(months_nanos));
}

zetasql_base::StatusOr<IntervalValue> IntervalValue::DeserializeFromBytes(
    absl::string_view bytes) {
  // Empty translates to interval value 0
  if (bytes.empty()) {
    return IntervalValue();
  }
  if (bytes.size() < sizeof(IntervalValue)) {
    return absl::OutOfRangeError(absl::StrCat("Size : ", bytes.size()));
  }
  const char* ptr = reinterpret_cast<const char*>(bytes.data());
  int64_t micros = zetasql_base::LittleEndian::ToHost64(*absl::bit_cast<int64_t*>(ptr));
  ptr += sizeof(micros);
  int32_t days = zetasql_base::LittleEndian::ToHost32(*absl::bit_cast<int32_t*>(ptr));
  ptr += sizeof(days);
  uint32_t months_nanos = zetasql_base::LittleEndian::ToHost32(*absl::bit_cast<uint32_t*>(ptr));
  IntervalValue interval;
  interval.micros_ = micros;
  interval.days_ = days;
  interval.months_nanos_ = months_nanos;
  ZETASQL_RETURN_IF_ERROR(ValidateMonths(interval.get_months()));
  ZETASQL_RETURN_IF_ERROR(ValidateDays(interval.get_days()));
  ZETASQL_RETURN_IF_ERROR(ValidateNanos(interval.get_nanos()));
  return interval;
}

std::string IntervalValue::ToString() const {
  // Interval conversion to string always uses fully expanded form:
  // [<sign>]x-x [<sign>]x [<sign>]x:x:x[.ddd[ddd[ddd]]]

  // Year-Month part
  int64_t total_months = std::abs(get_months());
  int64_t years = total_months / 12;
  int64_t months = total_months % 12;

  // Hour:Minute:Second and optional second fractions part.
  __int128 total_nanos = get_nanos();
  bool negative_nanos = false;
  if (total_nanos < 0) {
    // Cannot overflow because valid range of nanos is smaller than most
    // negative value.
    total_nanos = -total_nanos;
    negative_nanos = true;
  }
  int64_t hours = total_nanos / kNanosInHour;
  total_nanos -= hours * kNanosInHour;
  int64_t minutes = total_nanos / kNanosInMinute;
  total_nanos -= minutes * kNanosInMinute;
  int64_t seconds = total_nanos / kNanosInSecond;
  total_nanos -= seconds * kNanosInSecond;
  bool has_millis = total_nanos != 0;
  int64_t millis = total_nanos / kNanosInMilli;
  total_nanos -= millis * kNanosInMilli;
  bool has_micros = total_nanos != 0;
  int64_t micros = total_nanos / kNanosInMicro;
  int64_t nanos = total_nanos % kNanosInMicro;

  std::string result = absl::StrFormat(
      "%s%d-%d %d %s%d:%d:%d", get_months() < 0 ? "-" : "", years, months,
      get_days(), negative_nanos ? "-" : "", hours, minutes, seconds);
  // Fractions of second always come in group of 3
  if (has_millis) {
    absl::StrAppendFormat(&result, ".%03d", millis);
    if (has_micros) {
      absl::StrAppendFormat(&result, "%03d", micros);
      if (nanos != 0) {
        absl::StrAppendFormat(&result, "%03d", nanos);
      }
    }
  }
  return result;
}

zetasql_base::StatusOr<int64_t> NanosFromFractionDigits(absl::string_view input,
                                              absl::string_view digits) {
  int64_t nano_fractions;
  if (!absl::SimpleAtoi(digits, &nano_fractions)) {
    return MakeEvalError() << "Invalid interval literal '" << input << "'";
  }
  if (digits.size() > 9) {
    return MakeEvalError() << "Invalid interval literal '" << input << "'";
  }

  // Add enough zeros at the end to get nanoseconds. The maximum value is
  // limited by 10^10, hence cannot overflow
  for (int i = 0; i < 9 - digits.size(); i++) {
    nano_fractions *= 10;
  }

  return nano_fractions;
}

// Pattern for interval seconds: [+|-][s][.ddddddddd]. We only use it when
// there is a decimal dot in the input, therefore fractions are not optional.
const LazyRE2 kRESecond = {R"(([-+])?(\d*)\.(\d+))"};

zetasql_base::StatusOr<IntervalValue> IntervalValue::ParseFromString(
    absl::string_view input, functions::DateTimestampPart part) {
  absl::Status status;
  // SimpleAtoi ignores leading and trailing spaces, but we reject them.
  if (input.empty() || std::isspace(input.front()) ||
      std::isspace(input.back())) {
    return MakeEvalError() << "Invalid interval literal '" << input << "'";
  }

  // Seconds are special, because they allow fractions
  if (part == functions::SECOND && input.find('.') != input.npos) {
    absl::string_view sign;
    absl::string_view seconds_text;
    absl::string_view digits;
    // [+|-][s][.ddddddddd] - capture sign, seconds and digits of fractions.
    if (!RE2::FullMatch(input, *kRESecond, &sign, &seconds_text, &digits)) {
      return MakeEvalError() << "Invalid interval literal '" << input << "'";
    }
    int64_t seconds = 0;
    if (!seconds_text.empty()) {
      // This SimpleAtoi can fail if there were too many digits for seconds.
      if (!absl::SimpleAtoi(seconds_text, &seconds)) {
        return MakeEvalError() << "Invalid interval literal '" << input << "'";
      }
    }
    ZETASQL_RET_CHECK(!digits.empty());
    ZETASQL_ASSIGN_OR_RETURN(__int128 nano_fractions,
                     NanosFromFractionDigits(input, digits));
    // Result always fits into int128
    __int128 nanos = IntervalValue::kNanosInSecond * seconds + nano_fractions;
    bool negative = !sign.empty() && sign[0] == '-';
    if (negative) {
      nanos = -nanos;
    }
    return IntervalValue::FromNanos(nanos);
  }

  int64_t value;
  if (!absl::SimpleAtoi(input, &value)) {
    return MakeEvalError() << "Invalid interval literal '" << input << "'";
  }

  switch (part) {
    case functions::YEAR:
      if (!functions::Multiply(IntervalValue::kMonthsInYear, value, &value,
                               &status)) {
        return status;
      }
      return IntervalValue::FromMonths(value);
    case functions::QUARTER:
      if (!functions::Multiply(IntervalValue::kMonthsInQuarter, value, &value,
                               &status)) {
        return status;
      }
      return IntervalValue::FromMonths(value);
    case functions::MONTH:
      return IntervalValue::FromMonths(value);
    case functions::WEEK:
      if (!functions::Multiply(IntervalValue::kDaysInWeek, value, &value,
                               &status)) {
        return status;
      }
      return IntervalValue::FromDays(value);
    case functions::DAY:
      return IntervalValue::FromDays(value);
    case functions::HOUR:
      if (!functions::Multiply(IntervalValue::kMicrosInHour, value, &value,
                               &status)) {
        return status;
      }
      return IntervalValue::FromMicros(value);
    case functions::MINUTE:
      if (!functions::Multiply(IntervalValue::kMicrosInMinute, value, &value,
                               &status)) {
        return status;
      }
      return IntervalValue::FromMicros(value);
    case functions::SECOND:
      if (!functions::Multiply(IntervalValue::kMicrosInSecond, value, &value,
                               &status)) {
        return status;
      }
      return IntervalValue::FromMicros(value);
    default:
      return MakeEvalError() << "Unsupported interval datetime field "
                             << functions::DateTimestampPart_Name(part);
  }
}

// Regular expressions for parsing two datetime part intervals.

// YEAR TO MONTH '[+|-]x-x
const LazyRE2 kREYearToMonth = {R"(([-+])?(\d+)-(\d+))"};
// YEAR TO DAY '[+|-]x-x [+|-]x'
const LazyRE2 kREYearToDay = {R"(([-+])?(\d+)-(\d+) ([-+]?\d+))"};
// YEAR TO HOUR '[+|-]x-x [+|-]x [+|-]x'
const LazyRE2 kREYearToHour = {R"(([-+])?(\d+)-(\d+) ([-+]?\d+) ([-+])?(\d+))"};
// YEAR TO MINUTE '[+|-]x-x [+|-]x [+|-]x:x'
const LazyRE2 kREYearToMinute = {
    R"(([-+])?(\d+)-(\d+) ([-+]?\d+) ([-+])?(\d+):(\d+))"};
// YEAR TO SECOND '[+|-]x-x [+|-]x [+|-]x:x:x[.ddddddddd]'
const LazyRE2 kREYearToSecond = {
    R"(([-+])?(\d+)-(\d+) ([-+]?\d+) ([-+])?(\d+):(\d+):(\d+))"};
const LazyRE2 kREYearToSecondFractions = {
    R"(([-+])?(\d+)-(\d+) ([-+]?\d+) ([-+])?(\d+):(\d+):(\d+)\.(\d+))"};

// MONTH TO DAY '[+|-]x [+|-]x'
const LazyRE2 kREMonthToDay = {R"(([-+])?(\d+) ([-+]?\d+))"};
// MONTH TO HOUR '[+|-]x [+|-]x [+|-]x'
const LazyRE2 kREMonthToHour = {R"(([-+])?(\d+) ([-+]?\d+) ([-+])?(\d+))"};
// MONTH TO MINUTE '[+|-]x [+|-]x [+|-]x:x'
const LazyRE2 kREMonthToMinute = {
    R"(([-+])?(\d+) ([-+]?\d+) ([-+])?(\d+):(\d+))"};
// MONTH TO SECOND '[+|-]x [+|-]]x [+|-]x:x:x[.ddddddddd]'
const LazyRE2 kREMonthToSecond = {
    R"(([-+])?(\d+) ([-+]?\d+) ([-+])?(\d+):(\d+):(\d+))"};
const LazyRE2 kREMonthToSecondFractions = {
    R"(([-+])?(\d+) ([-+]?\d+) ([-+])?(\d+):(\d+):(\d+)\.(\d+))"};

// DAY TO HOUR '[+|-]x [+|-]x'
const LazyRE2 kREDayToHour = {R"(([-+]?\d+) ([-+])?(\d+))"};
// DAY TO MINUTE '[+|-]x [+|-]x:x'
const LazyRE2 kREDayToMinute = {R"(([-+]?\d+) ([-+])?(\d+):(\d+))"};
// DAY TO SECOND '[+|-]x [+|-]x:x:x[.ddddddddd]'
const LazyRE2 kREDayToSecond = {R"(([-+]?\d+) ([-+])?(\d+):(\d+):(\d+))"};
const LazyRE2 kREDayToSecondFractions = {
    R"(([-+]?\d+) ([-+])?(\d+):(\d+):(\d+)\.(\d+))"};

// HOUR TO MINUTE '[+|-]x:x'
const LazyRE2 kREHourToMinute = {R"(([-+])?(\d+):(\d+))"};
// HOUR TO SECOND '[+|-]x:x:x[.ddddddddd]'
const LazyRE2 kREHourToSecond = {R"(([-+])?(\d+):(\d+):(\d+))"};
const LazyRE2 kREHourToSecondFractions = {R"(([-+])?(\d+):(\d+):(\d+)\.(\d+))"};

// MINUTE TO SECOND '[+|-]x:x[.ddddddddd]'
const LazyRE2 kREMinuteToSecond = {R"(([-+])?(\d+):(\d+))"};
const LazyRE2 kREMinuteToSecondFractions = {R"(([-+])?(\d+):(\d+)\.(\d+))"};

zetasql_base::StatusOr<IntervalValue> IntervalValue::ParseFromString(
    absl::string_view input, functions::DateTimestampPart from,
    functions::DateTimestampPart to) {
  // Sign (empty, '-' or '+') for months and nano fields. There is no special
  // treatment for sign of days, because days are standalone number and are
  // matched and parsed by RE2 as part of ([-+]?\d+) group.
  std::string sign_months;
  std::string sign_nanos;
  // All the datetime fields
  int64_t years = 0;
  int64_t months = 0;
  int64_t days = 0;
  int64_t hours = 0;
  int64_t minutes = 0;
  int64_t seconds = 0;
  // Fractions of seconds
  absl::string_view fraction_digits;
  // Indication whether parsing succeeded.
  bool parsed = false;

  // Seconds are special, because they can have optional fractions
  if (to == functions::SECOND && input.find('.') != input.npos) {
    switch (from) {
      case functions::YEAR:
        parsed = RE2::FullMatch(input, *kREYearToSecondFractions, &sign_months,
                                &years, &months, &days, &sign_nanos, &hours,
                                &minutes, &seconds, &fraction_digits);
        break;
      case functions::MONTH:
        parsed = RE2::FullMatch(input, *kREMonthToSecondFractions, &sign_months,
                                &months, &days, &sign_nanos, &hours, &minutes,
                                &seconds, &fraction_digits);
        break;
      case functions::DAY:
        parsed =
            RE2::FullMatch(input, *kREDayToSecondFractions, &days, &sign_nanos,
                           &hours, &minutes, &seconds, &fraction_digits);
        break;
      case functions::HOUR:
        parsed = RE2::FullMatch(input, *kREHourToSecondFractions, &sign_nanos,
                                &hours, &minutes, &seconds, &fraction_digits);
        break;
      case functions::MINUTE:
        parsed = RE2::FullMatch(input, *kREMinuteToSecondFractions, &sign_nanos,
                                &minutes, &seconds, &fraction_digits);
        break;

      default:
        return MakeEvalError()
               << "Invalid interval datetime fields: "
               << functions::DateTimestampPart_Name(from) << " TO "
               << functions::DateTimestampPart_Name(to);
    }
  } else {
#define DATETIME_PARTS(from, to) (from << 16 | to)
    switch (DATETIME_PARTS(from, to)) {
      case DATETIME_PARTS(functions::YEAR, functions::MONTH):
        parsed = RE2::FullMatch(input, *kREYearToMonth, &sign_months, &years,
                                &months);
        break;
      case DATETIME_PARTS(functions::YEAR, functions::DAY):
        parsed = RE2::FullMatch(input, *kREYearToDay, &sign_months, &years,
                                &months, &days);
        break;
      case DATETIME_PARTS(functions::YEAR, functions::HOUR):
        parsed = RE2::FullMatch(input, *kREYearToHour, &sign_months, &years,
                                &months, &days, &sign_nanos, &hours);
        break;
      case DATETIME_PARTS(functions::YEAR, functions::MINUTE):
        parsed = RE2::FullMatch(input, *kREYearToMinute, &sign_months, &years,
                                &months, &days, &sign_nanos, &hours, &minutes);
        break;
      case DATETIME_PARTS(functions::YEAR, functions::SECOND):
        parsed = RE2::FullMatch(input, *kREYearToSecond, &sign_months, &years,
                                &months, &days, &sign_nanos, &hours, &minutes,
                                &seconds);
        break;
      case DATETIME_PARTS(functions::MONTH, functions::DAY):
        parsed =
            RE2::FullMatch(input, *kREMonthToDay, &sign_months, &months, &days);
        break;
      case DATETIME_PARTS(functions::MONTH, functions::HOUR):
        parsed = RE2::FullMatch(input, *kREMonthToHour, &sign_months, &months,
                                &days, &sign_nanos, &hours);
        break;
      case DATETIME_PARTS(functions::MONTH, functions::MINUTE):
        parsed = RE2::FullMatch(input, *kREMonthToMinute, &sign_months, &months,
                                &days, &sign_nanos, &hours, &minutes);
        break;
      case DATETIME_PARTS(functions::MONTH, functions::SECOND):
        parsed = RE2::FullMatch(input, *kREMonthToSecond, &sign_months, &months,
                                &days, &sign_nanos, &hours, &minutes, &seconds);
        break;
      case DATETIME_PARTS(functions::DAY, functions::HOUR):
        parsed =
            RE2::FullMatch(input, *kREDayToHour, &days, &sign_nanos, &hours);
        break;
      case DATETIME_PARTS(functions::DAY, functions::MINUTE):
        parsed = RE2::FullMatch(input, *kREDayToMinute, &days, &sign_nanos,
                                &hours, &minutes);
        break;
      case DATETIME_PARTS(functions::DAY, functions::SECOND):
        parsed = RE2::FullMatch(input, *kREDayToSecond, &days, &sign_nanos,
                                &hours, &minutes, &seconds);
        break;
      case DATETIME_PARTS(functions::HOUR, functions::MINUTE):
        parsed = RE2::FullMatch(input, *kREHourToMinute, &sign_nanos, &hours,
                                &minutes);
        break;
      case DATETIME_PARTS(functions::HOUR, functions::SECOND):
        parsed = RE2::FullMatch(input, *kREHourToSecond, &sign_nanos, &hours,
                                &minutes, &seconds);
        break;
      case DATETIME_PARTS(functions::MINUTE, functions::SECOND):
        parsed = RE2::FullMatch(input, *kREMinuteToSecond, &sign_nanos,
                                &minutes, &seconds);
        break;

      default:
        return MakeEvalError()
               << "Invalid interval datetime fields: "
               << functions::DateTimestampPart_Name(from) << " TO "
               << functions::DateTimestampPart_Name(to);
    }
#undef DATETIME_PARTS
  }

  if (!parsed) {
    return MakeEvalError() << "Invalid interval literal: '" << input << "'";
  }

  absl::Status status;
  int64_t years_as_months;
  if (!functions::Multiply(IntervalValue::kMonthsInYear, years,
                           &years_as_months, &status)) {
    return status;
  }
  if (!functions::Add(years_as_months, months, &months, &status)) {
    return status;
  }
  bool negative_months = !sign_months.empty() && sign_months[0] == '-';
  if (negative_months) {
    months = -months;
  }

  // Result always fits into int128.
  __int128 nanos = IntervalValue::kNanosInHour * hours +
                   IntervalValue::kNanosInMinute * minutes +
                   IntervalValue::kNanosInSecond * seconds;
  if (!fraction_digits.empty()) {
    ZETASQL_ASSIGN_OR_RETURN(int64_t nano_fractions,
                     NanosFromFractionDigits(input, fraction_digits));
    nanos += nano_fractions;
  }
  bool negative_nanos = !sign_nanos.empty() && sign_nanos[0] == '-';
  if (negative_nanos) {
    nanos = -nanos;
  }
  return IntervalValue::FromMonthsDaysNanos(months, days, nanos);
}

zetasql_base::StatusOr<IntervalValue> IntervalValue::ParseFromString(
    absl::string_view input) {
  // We can unambiguously determine possible datetime fields by counting number
  // of spaces, colons and dashes after digit in the input
  // (dash before digit could be a minus sign)
  //
  // ------------------+-------------+--------+--------+--------------------+
  // Datetime fields   | Format      | Spaces | Colons | Dashes after digit |
  // ------------------+-------------+--------+--------+--------------------+
  // YEAR TO SECOND    | Y-M D H:M:S |    2   |   2    |   1                |
  // YEAR TO MINUTE    | Y-M D H:M   |    2   |   1    |   1                |
  // YEAR TO HOUR      | Y-M D H     |    2   |   0    |   1                |
  // YEAR TO DAY       | Y-M D       |    1   |   0    |   1                |
  // YEAR TO MONTH     | Y-M         |    0   |   0    |   1                |
  // MONTH TO HOUR     | M D H       |    2   |   0    |   0                |
  // MONTH TO MINUTE   | M D H:M     |    2   |   1    |   0                |
  // MONTH TO SECOND   | M D H:M:S   |    2   |   2    |   0                |
  // DAY TO MINUTE     | D H:M       |    1   |   1    |   0                |
  // DAY TO SECOND     | D H:M:S     |    1   |   2    |   0                |
  // HOUR TO SECOND    | H:M:S       |    0   |   2    |   0                |
  // ------------------+-------------+--------+--------+--------------------+
  char p = '\0';
  int spaces = 0;
  int colons = 0;
  int dashes = 0;
  for (char c : input) {
    if (c == ' ') {
      spaces++;
    } else if (c == ':') {
      colons++;
    } else if (c == '-' && std::isdigit(p)) {
      dashes++;
    }
    p = c;
  }
#define SCD(s, c, d) ((s)*100 + (c)*10 + d)
  using functions::DAY;
  using functions::HOUR;
  using functions::MINUTE;
  using functions::MONTH;
  using functions::SECOND;
  using functions::YEAR;
  switch (SCD(spaces, colons, dashes)) {
    case SCD(2, 2, 1):
      return IntervalValue::ParseFromString(input, YEAR, SECOND);
    case SCD(2, 1, 1):
      return IntervalValue::ParseFromString(input, YEAR, MINUTE);
    case SCD(2, 0, 1):
      return IntervalValue::ParseFromString(input, YEAR, HOUR);
    case SCD(1, 0, 1):
      return IntervalValue::ParseFromString(input, YEAR, DAY);
    case SCD(0, 0, 1):
      return IntervalValue::ParseFromString(input, YEAR, MONTH);
    case SCD(2, 0, 0):
      return IntervalValue::ParseFromString(input, MONTH, HOUR);
    case SCD(2, 1, 0):
      return IntervalValue::ParseFromString(input, MONTH, MINUTE);
    case SCD(2, 2, 0):
      return IntervalValue::ParseFromString(input, MONTH, SECOND);
    case SCD(1, 1, 0):
      return IntervalValue::ParseFromString(input, DAY, MINUTE);
    case SCD(1, 2, 0):
      return IntervalValue::ParseFromString(input, DAY, SECOND);
    case SCD(0, 2, 0):
      return IntervalValue::ParseFromString(input, HOUR, SECOND);
  }
#undef SCD
  return MakeEvalError() << "Invalid interval literal: '" << input << "'";
}

zetasql_base::StatusOr<IntervalValue> IntervalValue::FromInteger(
    int64_t value, functions::DateTimestampPart part) {
  switch (part) {
    case functions::YEAR:
      return IntervalValue::FromYMDHMS(value, 0, 0, 0, 0, 0);
    case functions::MONTH:
      return IntervalValue::FromYMDHMS(0, value, 0, 0, 0, 0);
    case functions::DAY:
      return IntervalValue::FromYMDHMS(0, 0, value, 0, 0, 0);
    case functions::HOUR:
      return IntervalValue::FromYMDHMS(0, 0, 0, value, 0, 0);
    case functions::MINUTE:
      return IntervalValue::FromYMDHMS(0, 0, 0, 0, value, 0);
    case functions::SECOND:
      return IntervalValue::FromYMDHMS(0, 0, 0, 0, 0, value);
    case functions::QUARTER: {
      absl::Status status;
      if (!functions::Multiply(IntervalValue::kMonthsInQuarter, value, &value,
                               &status)) {
        return status;
      }
      return IntervalValue::FromYMDHMS(0, value, 0, 0, 0, 0);
    }
    case functions::WEEK: {
      absl::Status status;
      if (!functions::Multiply(IntervalValue::kDaysInWeek, value, &value,
                               &status)) {
        return status;
      }
      return IntervalValue::FromYMDHMS(0, 0, value, 0, 0, 0);
    }
    default:
      return MakeEvalError() << "Invalid interval datetime field "
                             << functions::DateTimestampPart_Name(part);
  }
}

std::ostream& operator<<(std::ostream& out, IntervalValue value) {
  return out << value.ToString();
}

}  // namespace zetasql
