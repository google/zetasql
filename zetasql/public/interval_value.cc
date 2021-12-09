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

#include <cctype>
#include <cmath>

#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "absl/base/casts.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "zetasql/base/endian.h"
#include "re2/re2.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

std::string ToString(int64_t value) {
  std::string s;
  uint64_t v = value;
  if (value < 0) {
    v = 0 - v;  // MSVC 2013 errors on unary negation of unsigned.
    s += "-";
  }
  if (v < int64_t{1000}) {
    absl::StrAppendFormat(&s, "%d", v);
  } else if (v >= int64_t{1000000000000000}) {
    // Number bigger than 1E15; use that notation.
    absl::StrAppendFormat(&s, "%0.3G", static_cast<double>(v));
  } else {
    static const char units[] = "kMBT";
    const char* unit = units;
    while (v >= int64_t{1000000}) {
      v /= int64_t{1000};
      ++unit;
      ZETASQL_CHECK(unit < units + ABSL_ARRAYSIZE(units));
    }
    absl::StrAppendFormat(&s, "%.2f%c", v / 1000.0, *unit);
  }
  return s;
}

std::string Int128ToString(absl::int128 value) {
  if (value >= int64_t{1000000000000000} ||
      value <= int64_t{-1000000000000000}) {
    // Number bigger than 1E15; use that notation.
    return absl::StrFormat("%0.3G", static_cast<double>(value));
  }
  return ToString(static_cast<int64_t>(value));
}
}  // namespace

absl::StatusOr<IntervalValue> IntervalValue::FromYMDHMS(
    int64_t years, int64_t months, int64_t days, int64_t hours, int64_t minutes,
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

absl::StatusOr<IntervalValue> IntervalValue::operator*(int64_t value) const {
  absl::Status status;
  int64_t months;
  if (!zetasql::functions::Multiply(get_months(), value, &months, &status)) {
    return absl::OutOfRangeError("Interval overflow during multiplication");
  }

  int64_t days;
  if (!zetasql::functions::Multiply(get_days(), value, &days, &status)) {
    return absl::OutOfRangeError("Interval overflow during multiplication");
  }

  FixedInt<64, 3> nanos = FixedInt<64, 3>(get_nanos());
  nanos *= value;
  if (nanos > FixedInt<64, 3>(FixedInt<64, 2>::max()) ||
      nanos < FixedInt<64, 3>(FixedInt<64, 2>::min())) {
    return absl::OutOfRangeError("Interval overflow during multiplication");
  }
  return IntervalValue::FromMonthsDaysNanos(months, days,
                                            static_cast<__int128>(nanos));
}

absl::StatusOr<IntervalValue> IntervalValue::operator/(int64_t value) const {
  if (value == 0) {
    return absl::OutOfRangeError("Interval division by zero");
  }

  int64_t months = get_months() / value;
  int64_t months_remainder = get_months() % value;
  int64_t adjusted_days =
      get_days() + (months_remainder * IntervalValue::kDaysInMonth);
  int64_t days = adjusted_days / value;
  __int128 days_reminder = adjusted_days % value;
  FixedInt<64, 3> adjusted_nanos = FixedInt<64, 3>(get_nanos());
  adjusted_nanos += FixedInt<64, 3>(days_reminder * IntervalValue::kNanosInDay);
  FixedInt<64, 3> nanos = adjusted_nanos;
  nanos /= FixedInt<64, 3>(value);

  if (nanos > FixedInt<64, 3>(FixedInt<64, 2>::max()) ||
      nanos < FixedInt<64, 3>(FixedInt<64, 2>::min())) {
    return absl::OutOfRangeError("Interval overflow during division");
  }

  return IntervalValue::FromMonthsDaysNanos(months, days,
                                            static_cast<__int128>(nanos));
}

void IntervalValue::SumAggregator::Add(IntervalValue value) {
  months_ += value.get_months();
  days_ += value.get_days();
  nanos_ += FixedInt<64, 3>(value.get_nanos());
}

absl::StatusOr<IntervalValue> IntervalValue::SumAggregator::GetSum() const {
  // It is unlikely that months/days will overflow int64_t, and that nanos will
  // overflow int128 - but check it nevertheless.
  if (months_ > std::numeric_limits<int64_t>::max() ||
      months_ < std::numeric_limits<int64_t>::min() ||
      days_ > std::numeric_limits<int64_t>::max() ||
      days_ < std::numeric_limits<int64_t>::min() ||
      nanos_ > FixedInt<64, 3>(FixedInt<64, 2>::max()) ||
      nanos_ < FixedInt<64, 3>(FixedInt<64, 2>::min())) {
    return absl::OutOfRangeError("Interval overflow during Sum operation");
  }

  return IntervalValue::FromMonthsDaysNanos(static_cast<int64_t>(months_),
                                            static_cast<int64_t>(days_),
                                            static_cast<__int128>(nanos_));
}

absl::StatusOr<IntervalValue> IntervalValue::SumAggregator::GetAverage(
    int64_t count) const {
  ZETASQL_DCHECK_GT(count, 0);

  // AVG(interval) = SUM(interval) / count, but SUM(interval) may not be a
  // valid interval (because of overflow), so we do manual division of parts
  // instead of building interval object and using it's division operator.
  __int128 months = months_ / count;
  __int128 months_remainder = months_ % count;
  __int128 adjusted_days =
      days_ + (months_remainder * IntervalValue::kDaysInMonth);
  __int128 days = adjusted_days / count;
  __int128 days_reminder = adjusted_days % count;
  FixedInt<64, 3> adjusted_nanos = FixedInt<64, 3>(nanos_);
  adjusted_nanos += FixedInt<64, 3>(days_reminder * IntervalValue::kNanosInDay);
  FixedInt<64, 3> nanos = adjusted_nanos;
  nanos /= FixedInt<64, 3>(count);

  // It is unlikely that months/days will overflow int64_t, and that nanos will
  // overflow int128 - but check it nevertheless.
  if (months > std::numeric_limits<int64_t>::max() ||
      months < std::numeric_limits<int64_t>::min() ||
      days > std::numeric_limits<int64_t>::max() ||
      days < std::numeric_limits<int64_t>::min() ||
      nanos > FixedInt<64, 3>(FixedInt<64, 2>::max()) ||
      nanos < FixedInt<64, 3>(FixedInt<64, 2>::min())) {
    return absl::OutOfRangeError("Interval overflow during Avg operation");
  }

  return IntervalValue::FromMonthsDaysNanos(static_cast<int64_t>(months),
                                            static_cast<int64_t>(days),
                                            static_cast<__int128>(nanos));
}

std::string IntervalValue::SumAggregator::SerializeAsProtoBytes() const {
  std::string result;
  SerializeAndAppendToProtoBytes(&result);
  return result;
}

void IntervalValue::SumAggregator::SerializeAndAppendToProtoBytes(
    std::string* bytes) const {
  absl::uint128 months = zetasql_base::LittleEndian::FromHost128(months_);
  bytes->append(reinterpret_cast<const char*>(&months), sizeof(months));
  absl::uint128 days = zetasql_base::LittleEndian::FromHost128(days_);
  bytes->append(reinterpret_cast<const char*>(&days), sizeof(days));
  nanos_.SerializeToBytes(bytes);
}

absl::StatusOr<IntervalValue::SumAggregator>
IntervalValue::SumAggregator::DeserializeFromProtoBytes(
    absl::string_view bytes) {
  IntervalValue::SumAggregator aggregator;
  if (bytes.empty()) {
    return aggregator;
  }

  if (bytes.size() < sizeof(absl::uint128) * 2) {
    return absl::OutOfRangeError(
        "Invalid serialized INTERVAL::SumAggregator size too small");
  }

  const char* ptr = reinterpret_cast<const char*>(bytes.data());
  aggregator.months_ = static_cast<__int128>(
      zetasql_base::LittleEndian::ToHost128(*absl::bit_cast<absl::uint128*>(ptr)));
  ptr += sizeof(absl::uint128);

  aggregator.days_ = static_cast<__int128>(
      zetasql_base::LittleEndian::ToHost128(*absl::bit_cast<absl::uint128*>(ptr)));
  ptr += sizeof(absl::uint128);

  if (!aggregator.nanos_.DeserializeFromBytes(
          bytes.substr(sizeof(absl::uint128) * 2))) {
    return absl::OutOfRangeError(
        "Invalid serialized INTERVAL::SumAggregator failed to deserialize "
        "nanos");
  }

  return aggregator;
}

std::string IntervalValue::SumAggregator::DebugString() const {
  return absl::StrCat(
      "IntervalValue::SumAggregator (months=",
      Int128ToString(months_),
      ", days=", Int128ToString(days_),
      ", nanos=", nanos_.ToString(), ")");
}

void IntervalValue::SumAggregator::MergeWith(const SumAggregator& other) {
  months_ += other.months_;
  days_ += other.days_;
  nanos_ += other.nanos_;
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

absl::StatusOr<IntervalValue> IntervalValue::DeserializeFromBytes(
    absl::string_view bytes) {
  // Empty translates to interval value 0
  if (bytes.empty()) {
    return IntervalValue();
  }
  if (bytes.size() != sizeof(IntervalValue)) {
    return absl::OutOfRangeError(absl::StrCat(
        "Invalid serialized INTERVAL size, expected ", sizeof(IntervalValue),
        " bytes, but got ", bytes.size(), " bytes."));
  }
  const char* ptr = reinterpret_cast<const char*>(bytes.data());
  int64_t micros = zetasql_base::LittleEndian::ToHost64(*absl::bit_cast<int64_t*>(ptr));
  ptr += sizeof(micros);
  int32_t days = zetasql_base::LittleEndian::ToHost32(*absl::bit_cast<int32_t*>(ptr));
  ptr += sizeof(days);
  uint32_t months_nanos =
      zetasql_base::LittleEndian::ToHost32(*absl::bit_cast<uint32_t*>(ptr));
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

std::string IntervalValue::ToISO8601() const {
  int64_t years = get_months() / 12;
  int64_t months = get_months() % 12;
  int64_t days = get_days();
  __int128 total_nanos = get_nanos();
  int64_t hours = total_nanos / kNanosInHour;
  int64_t minutes = (total_nanos % kNanosInHour) / kNanosInMinute;
  int64_t seconds = (total_nanos % kNanosInMinute) / kNanosInSecond;
  int64_t subseconds = total_nanos % kNanosInSecond;

  std::string result("P");
  if (years != 0) absl::StrAppend(&result, years, "Y");
  if (months != 0) absl::StrAppend(&result, months, "M");
  if (days != 0) absl::StrAppend(&result, days, "D");
  if (total_nanos != 0) absl::StrAppend(&result, "T");
  if (hours != 0) absl::StrAppend(&result, hours, "H");
  if (minutes != 0) absl::StrAppend(&result, minutes, "M");
  if (seconds != 0 || subseconds != 0) {
    if (subseconds == 0) {
      absl::StrAppend(&result, seconds, "S");
    } else {
      if (seconds != 0) {
        absl::StrAppend(&result, seconds, ".");
      } else if (total_nanos < 0) {
        absl::StrAppend(&result, "-0.");
      } else {
        absl::StrAppend(&result, "0.");
      }
      // Print fractions of a second without trailing zeros
      if (subseconds < 0) subseconds = -subseconds;
      for (int64_t factor :
           {100000000, 10000000, 1000000, 100000, 10000, 1000, 100, 10, 1}) {
        int64_t digit = subseconds / factor;
        absl::StrAppend(&result, digit);
        subseconds %= factor;
        if (subseconds == 0) {
          break;
        }
      }
      absl::StrAppend(&result, "S");
    }
  }
  if (result.size() == 1) absl::StrAppend(&result, "0Y");
  return result;
}

absl::StatusOr<int64_t> NanosFromFractionDigits(absl::string_view input,
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

absl::StatusOr<IntervalValue> IntervalValue::ParseFromString(
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

absl::StatusOr<IntervalValue> IntervalValue::ParseFromString(
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

absl::StatusOr<IntervalValue> IntervalValue::ParseFromString(
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

namespace {

const LazyRE2 kRENumber = {R"((\d+)(\.|\,)?(\d+)?)"};

// Parser for ISO 8601 Duration format with following modifications:
// - negative datetime parts are allowed
// - multiple dateparts of same type are allowed
// - order of dateparts can be arbitrary
// - 'W' can be used for weeks in the date portion
// - Only seconds can have fractional numbers
class ISO8601Parser {
  const char kEof = '\0';

 public:
  absl::StatusOr<IntervalValue> Parse(absl::string_view input) {
    input_ = input;
    char c = GetChar();
    if (c != 'P') {
      return MakeEvalError() << "Interval must start with 'P'";
    }
    if (input_.empty()) {
      return MakeEvalError()
             << "At least one datetime part must be defined in the interval";
    }
    absl::Status status;

    // When true - parsing time part (after T), when false - parsing date part.
    bool in_time_part = false;
    int64_t years = 0;
    int64_t months = 0;
    int64_t weeks = 0;
    int64_t days = 0;
    int64_t hours = 0;
    int64_t minutes = 0;
    int64_t seconds = 0;
    int64_t nano_fractions = 0;
    for (;;) {
      int64_t sign = false;
      c = PeekChar();
      if (!std::isdigit(c)) {
        GetChar();
        if (c == '-') {
          // Proceed to parse the number and make it negative later
          sign = true;
        } else if (c == 'T') {
          // Switching from date to time part
          if (in_time_part) {
            return MakeEvalError() << "Unexpected duplicate time separator 'T'";
          }
          in_time_part = true;
          continue;
        } else if (c == kEof) {
          break;
        } else {
          return MakeEvalError() << "Unexpected " << PrintChar(c);
        }
      }
      // We now expect to see positive number (possibly with fractional digits)
      // followed by datetime part letter.
      ZETASQL_RETURN_IF_ERROR(ParseNumber());
      int64_t number;
      if (!absl::SimpleAtoi(digits_, &number)) {
        return MakeEvalError()
               << "Cannot convert '" << digits_ << "' to integer";
      }
      // number couldn't have been negative, so no worries about underflow
      // of int64_t::min
      if (sign) number = -number;
      c = GetChar();
      if (!in_time_part) {
        switch (c) {
          case 'Y':
            if (!functions::Add(years, number, &years, &status)) {
              return status;
            }
            break;
          case 'M':
            if (!functions::Add(months, number, &months, &status)) {
              return status;
            }
            break;
          case 'W':
            if (!functions::Add(weeks, number, &weeks, &status)) {
              return status;
            }
            break;
          case 'D':
            if (!functions::Add(days, number, &days, &status)) {
              return status;
            }
            break;
          default:
            return MakeEvalError() << "Unexpected " << PrintChar(c)
                                   << " in the date portion of interval";
        }
      } else {
        switch (c) {
          case 'H':
            if (!functions::Add(hours, number, &hours, &status)) {
              return status;
            }
            break;
          case 'M':
            if (!functions::Add(minutes, number, &minutes, &status)) {
              return status;
            }
            break;
          case 'S':
            if (!functions::Add(seconds, number, &seconds, &status)) {
              return status;
            }
            if (!decimal_point_.empty()) {
              ZETASQL_ASSIGN_OR_RETURN(
                  number, NanosFromFractionDigits(input_, decimal_digits_));
              if (sign) number = -number;
              nano_fractions += number;
            }
            break;
          default:
            return MakeEvalError() << "Unexpected " << PrintChar(c)
                                   << " in the time portion of interval";
        }
      }
      if (!decimal_point_.empty() && c != 'S') {
        return MakeEvalError() << "Fractional values are only allowed for "
                                  "seconds part 'S', but were used for "
                               << PrintChar(c);
      }
    }

    int64_t year_months;
    if (!functions::Multiply(IntervalValue::kMonthsInYear, years, &year_months,
                             &status)) {
      return status;
    }
    if (!functions::Add(months, year_months, &months, &status)) {
      return status;
    }

    int64_t week_days;
    if (!functions::Multiply(IntervalValue::kDaysInWeek, weeks, &week_days,
                             &status)) {
      return status;
    }
    if (!functions::Add(days, week_days, &days, &status)) {
      return status;
    }

    // Int128 math cannot overflow
    __int128 nanos = IntervalValue::kNanosInHour * hours +
                     IntervalValue::kNanosInMinute * minutes +
                     IntervalValue::kNanosInSecond * seconds + nano_fractions;
    return IntervalValue::FromMonthsDaysNanos(months, days, nanos);
  }

 private:
  absl::Status ParseNumber() {
    digits_ = {};
    decimal_point_ = {};
    decimal_digits_ = {};
    if (!RE2::Consume(&input_, *kRENumber, &digits_, &decimal_point_,
                      &decimal_digits_)) {
      return MakeEvalError() << "Expected number";
    }
    return absl::OkStatus();
  }

  char PeekChar() const {
    if (input_.empty()) {
      return kEof;
    }
    return input_[0];
  }

  char GetChar() {
    char c = PeekChar();
    input_.remove_prefix(1);
    return c;
  }

  std::string PrintChar(char c) {
    if (c == kEof) return "end of input";
    return absl::StrCat("'", std::string(1, c), "'");
  }

  // Points to the current position being parsed in input
  absl::string_view input_;

  // Parsed digits before decimal dot
  absl::string_view digits_;
  // Decimal dot itself (needed to detect trailing dot)
  absl::string_view decimal_point_;
  // Digits after the decimal dot
  absl::string_view decimal_digits_;
};

}  // namespace

absl::StatusOr<IntervalValue> IntervalValue::ParseFromISO8601(
    absl::string_view input) {
  ISO8601Parser parser;
  return parser.Parse(input);
}

absl::StatusOr<IntervalValue> IntervalValue::Parse(absl::string_view input) {
  if (absl::StartsWith(input, "P")) {
    return ParseFromISO8601(input);
  }
  return ParseFromString(input);
}

absl::StatusOr<IntervalValue> IntervalValue::FromInteger(
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

absl::StatusOr<int64_t> IntervalValue::Extract(
    functions::DateTimestampPart part) const {
  switch (part) {
    case functions::YEAR:
      return get_months() / kMonthsInYear;
    case functions::MONTH:
      return get_months() % kMonthsInYear;
    case functions::DAY:
      return get_days();
    case functions::HOUR:
      return get_nanos() / kNanosInHour;
    case functions::MINUTE:
      return (get_nanos() % kNanosInHour) / kNanosInMinute;
    case functions::SECOND:
      return (get_nanos() % kNanosInMinute) / kNanosInSecond;
    case functions::MILLISECOND:
      return (get_nanos() % kNanosInSecond) / kNanosInMilli;
    case functions::MICROSECOND:
      return (get_nanos() % kNanosInSecond) / kNanosInMicro;
    case functions::NANOSECOND:
      return (get_nanos() % kNanosInSecond);
    default:
      break;  // fall through
  }

  __int128 total_nanos = get_nanos();
  bool negative_nanos = false;
  if (total_nanos < 0) {
    // Cannot overflow because valid range of nanos is smaller than most
    // negative value.
    total_nanos = -total_nanos;
    negative_nanos = true;
  }
  int64_t value;
  switch (part) {
    case functions::HOUR:
      value = total_nanos / kNanosInHour;
      break;
    case functions::MINUTE:
      value = (total_nanos % kNanosInHour) / kNanosInMinute;
      break;
    case functions::SECOND:
      value = (total_nanos % kNanosInMinute) / kNanosInSecond;
      break;
    case functions::MILLISECOND:
      value = (total_nanos % kNanosInSecond) / kNanosInMilli;
      break;
    case functions::MICROSECOND:
      value = (total_nanos % kNanosInMilli) / kNanosInMicro;
      break;
    case functions::NANOSECOND:
      value = total_nanos % kNanosInMicro;
      break;
    default:
      return absl::OutOfRangeError(
          absl::StrFormat("Unsupported date part %s in EXTRACT FROM INTERVAL",
                          functions::DateTimestampPart_Name(part)));
  }
  if (negative_nanos) {
    value = -value;
  }
  return value;
}

std::ostream& operator<<(std::ostream& out, IntervalValue value) {
  return out << value.ToString();
}

absl::StatusOr<IntervalValue> JustifyHours(const IntervalValue& v) {
  __int128 nanos = v.get_nanos();
  int64_t days = v.get_days() + nanos / IntervalValue::kNanosInDay;
  nanos = nanos % IntervalValue::kNanosInDay;
  if (days > 0 && nanos < 0) {
    nanos += IntervalValue::kNanosInDay;
    days--;
  } else if (days < 0 && nanos > 0) {
    nanos -= IntervalValue::kNanosInDay;
    days++;
  }
  return IntervalValue::FromMonthsDaysNanos(v.get_months(), days, nanos);
}

absl::StatusOr<IntervalValue> JustifyDays(const IntervalValue& v) {
  int64_t months = v.get_months() + v.get_days() / IntervalValue::kDaysInMonth;
  int64_t days = v.get_days() % IntervalValue::kDaysInMonth;
  if (months > 0 && days < 0) {
    days += IntervalValue::kDaysInMonth;
    months--;
  } else if (months < 0 && days > 0) {
    days -= IntervalValue::kDaysInMonth;
    months++;
  }
  return IntervalValue::FromMonthsDaysNanos(months, days, v.get_nanos());
}

absl::StatusOr<IntervalValue> JustifyInterval(const IntervalValue& v) {
  __int128 nanos = v.get_nanos();
  int64_t days = v.get_days() + nanos / IntervalValue::kNanosInDay;
  nanos = nanos % IntervalValue::kNanosInDay;
  int64_t months = v.get_months() + days / IntervalValue::kDaysInMonth;
  days = days % IntervalValue::kDaysInMonth;
  // This logic might be non-intuitive, but it repeats the logic in Postgres
  // for making sure all datetime parts have same sign.
  if (months > 0 && (days < 0 || (days == 0 && nanos < 0))) {
    days += IntervalValue::kDaysInMonth;
    months--;
  } else if (months < 0 && (days > 0 || (days == 0 && nanos > 0))) {
    days -= IntervalValue::kDaysInMonth;
    months++;
  }

  if (days > 0 && nanos < 0) {
    nanos += IntervalValue::kNanosInDay;
    days--;
  } else if (days < 0 && nanos > 0) {
    nanos -= IntervalValue::kNanosInDay;
    days++;
  }
  return IntervalValue::FromMonthsDaysNanos(months, days, nanos);
}

}  // namespace zetasql
