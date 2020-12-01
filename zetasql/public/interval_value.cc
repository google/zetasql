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
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "zetasql/base/endian.h"
#include "re2/re2.h"

namespace zetasql {

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
    ZETASQL_RET_CHECK(!digits.empty());
    bool negative = !sign.empty() && sign[0] == '-';
    int64_t seconds = 0;
    if (!seconds_text.empty()) {
      // This SimpleAtoi can fail if there were too many digits for seconds.
      if (!absl::SimpleAtoi(seconds_text, &seconds)) {
        return MakeEvalError() << "Invalid interval literal '" << input << "'";
      }
    }
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

    // Result always fits into int128
    __int128 nanos = IntervalValue::kNanosInSecond * seconds + nano_fractions;
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

}  // namespace zetasql
