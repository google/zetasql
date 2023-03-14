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

#include "zetasql/public/range_value.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "zetasql/common/errors.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/base/case.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "absl/time/time.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

static constexpr absl::string_view kStringFormat = "[%s, %s)";

namespace {

std::optional<absl::string_view> UnboundedOrValue(
    absl::string_view boundary_value) {
  if (zetasql_base::CaseEqual(boundary_value, "UNBOUNDED") ||
      zetasql_base::CaseEqual(boundary_value, "NULL")) {
    return std::nullopt;
  }
  return {boundary_value};
}

absl::Status UnsupportedElementKindError(const TypeKind& element_type_kind) {
  return MakeEvalError() << "Unsupported range element type kind "
                         << Type::TypeKindToString(element_type_kind,
                                                   PRODUCT_EXTERNAL);
}

}  // namespace

absl::StatusOr<std::pair<std::optional<absl::string_view>,
                         std::optional<absl::string_view>>>
GetRangeBoundaries(absl::string_view range_value) {
  if (range_value.empty() || range_value.front() != '[' ||
      range_value.back() != ')') {
    return absl::InvalidArgumentError(
        absl::StrFormat("Failed to parse range: ranges must formatted exactly "
                        "as [START, END) (found %s)",
                        range_value));
  }
  range_value.remove_prefix(1);
  range_value.remove_suffix(1);
  std::pair<absl::string_view, absl::string_view> range_parts =
      absl::StrSplit(range_value, absl::MaxSplits(',', 1));
  return std::make_pair(
      UnboundedOrValue(absl::StripAsciiWhitespace(range_parts.first)),
      UnboundedOrValue(absl::StripAsciiWhitespace(range_parts.second)));
}

absl::StatusOr<size_t> GetEncodedRangeSize(absl::string_view bytes,
                                           TypeKind element_type_kind) {
  if (bytes.size() < sizeof(uint8_t)) {
    return MakeEvalError() << absl::StrFormat(
               "Too few bytes to determine encoded RANGE size (needed %d; got "
               "%d)",
               sizeof(uint8_t), bytes.size());
  }
  switch (element_type_kind) {
    case TYPE_DATE:
      return GetEncodedRangeSize<int32_t>(bytes);
    case TYPE_DATETIME:
    case TYPE_TIMESTAMP:
      return GetEncodedRangeSize<int64_t>(bytes);
    default:
      return UnsupportedElementKindError(element_type_kind);
  }
}

absl::StatusOr<RangeValue<int32_t>> RangeValueFromDates(
    std::optional<int32_t> start_date, std::optional<int32_t> end_date) {
  if (start_date.has_value() && !functions::IsValidDate(start_date.value())) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid start date: " << start_date.value();
  }
  if (end_date.has_value() && !functions::IsValidDate(end_date.value())) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid end date: " << end_date.value();
  }
  if (start_date.has_value() && end_date.has_value() &&
      start_date.value() >= end_date.value()) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Range start element must be smaller than range end element: ["
           << start_date.value() << ", " << end_date.value() << ")";
  }
  return RangeValue<int32_t>(start_date, end_date);
}

absl::StatusOr<RangeValue<int32_t>> ParseRangeValueFromDateStrings(
    std::optional<absl::string_view> start_date_str,
    std::optional<absl::string_view> end_date_str) {
  std::optional<int32_t> start, end;
  if (start_date_str.has_value()) {
    start = std::optional<int32_t>(0);
    ZETASQL_RETURN_IF_ERROR(
        functions::ConvertStringToDate(start_date_str.value(), &start.value()));
  }
  if (end_date_str.has_value()) {
    end = std::optional<int32_t>(0);
    ZETASQL_RETURN_IF_ERROR(
        functions::ConvertStringToDate(end_date_str.value(), &end.value()));
  }
  return RangeValueFromDates(start, end);
}

absl::StatusOr<std::string> RangeValueDatesToString(
    const RangeValue<int32_t>& range) {
  std::string start = std::string(internal::kUnbounded);
  std::string end = std::string(internal::kUnbounded);
  if (!range.has_unbounded_start()) {
    ZETASQL_RETURN_IF_ERROR(
        functions::ConvertDateToString(range.start().value(), &start));
  }
  if (!range.has_unbounded_end()) {
    ZETASQL_RETURN_IF_ERROR(functions::ConvertDateToString(range.end().value(), &end));
  }
  return absl::StrFormat(kStringFormat, start, end);
}

absl::StatusOr<RangeValue<int64_t>> RangeValueFromDatetimeMicros(
    std::optional<DatetimeValue> start_datetime,
    std::optional<DatetimeValue> end_datetime) {
  if (start_datetime.has_value() && !start_datetime->IsValid()) {
    return zetasql_base::InvalidArgumentErrorBuilder() << "Invalid start datetime";
  }
  if (end_datetime.has_value() && !end_datetime->IsValid()) {
    return zetasql_base::InvalidArgumentErrorBuilder() << "Invalid end datetime";
  }
  if (start_datetime.has_value() && end_datetime.has_value() &&
      start_datetime->Packed64DatetimeMicros() >=
          end_datetime->Packed64DatetimeMicros()) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Range start element must be smaller than range end element: ["
           << start_datetime->DebugString() << ", "
           << end_datetime->DebugString() << ")";
  }
  return RangeValue<int64_t>(
      start_datetime.has_value()
          ? std::optional<int64_t>{start_datetime->Packed64DatetimeMicros()}
          : std::nullopt,
      end_datetime.has_value()
          ? std::optional<int64_t>{end_datetime->Packed64DatetimeMicros()}
          : std::nullopt);
}

absl::StatusOr<RangeValue<int64_t>> ParseRangeValueDatetimeMicrosFromStrings(
    std::optional<absl::string_view> start_datetime_str,
    std::optional<absl::string_view> end_datetime_str) {
  std::optional<DatetimeValue> start, end;
  if (start_datetime_str.has_value()) {
    DatetimeValue start_value;
    ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToDatetime(
        start_datetime_str.value(), functions::kMicroseconds, &start_value));
    start = std::optional<DatetimeValue>(start_value);
  }
  if (end_datetime_str.has_value()) {
    DatetimeValue end_value;
    ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToDatetime(
        end_datetime_str.value(), functions::kMicroseconds, &end_value));
    end = std::optional<DatetimeValue>(end_value);
  }
  return RangeValueFromDatetimeMicros(start, end);
}

absl::StatusOr<std::string> RangeValueDatetimeMicrosToString(
    const RangeValue<int64_t>& range) {
  std::string start = std::string(internal::kUnbounded);
  std::string end = std::string(internal::kUnbounded);
  if (!range.has_unbounded_start()) {
    ZETASQL_RETURN_IF_ERROR(functions::ConvertDatetimeToString(
        DatetimeValue::FromPacked64Micros(range.start().value()),
        functions::kMicroseconds, &start));
  }
  if (!range.has_unbounded_end()) {
    ZETASQL_RETURN_IF_ERROR(functions::ConvertDatetimeToString(
        DatetimeValue::FromPacked64Micros(range.end().value()),
        functions::kMicroseconds, &end));
  }
  return absl::StrFormat(kStringFormat, start, end);
}

absl::StatusOr<RangeValue<int64_t>> RangeValueFromTimestampMicros(
    std::optional<int64_t> start_timestamp,
    std::optional<int64_t> end_timestamp) {
  if (start_timestamp.has_value() &&
      !functions::IsValidTimestamp(start_timestamp.value(),
                                   functions::kMicroseconds)) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid start timestamp: " << start_timestamp.value();
  }
  if (end_timestamp.has_value() &&
      !functions::IsValidTimestamp(end_timestamp.value(),
                                   functions::kMicroseconds)) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid end timestamp: " << end_timestamp.value();
  }
  if (start_timestamp.has_value() && end_timestamp.has_value() &&
      start_timestamp.value() >= end_timestamp.value()) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Range start element must be smaller than range end element: ["
           << start_timestamp.value() << ", " << end_timestamp.value() << ")";
  }
  return RangeValue<int64_t>(start_timestamp, end_timestamp);
}

absl::StatusOr<RangeValue<int64_t>>
ParseRangeValueTimestampMicrosFromTimestampStrings(
    std::optional<absl::string_view> start_timestamp_str,
    std::optional<absl::string_view> end_timestamp_str,
    absl::TimeZone default_timezone) {
  std::optional<int64_t> start, end;
  if (start_timestamp_str.has_value()) {
    start = std::optional<int64_t>(0);
    ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTimestamp(
        start_timestamp_str.value(), default_timezone, functions::kMicroseconds,
        /*allow_tz_in_str=*/true, &start.value()));
  }
  if (end_timestamp_str.has_value()) {
    end = std::optional<int64_t>(0);
    ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTimestamp(
        end_timestamp_str.value(), default_timezone, functions::kMicroseconds,
        /*allow_tz_in_str=*/true, &end.value()));
  }
  return RangeValueFromTimestampMicros(start, end);
}

absl::StatusOr<std::string> RangeValueTimestampMicrosToString(
    const RangeValue<int64_t>& range) {
  std::string start = std::string(internal::kUnbounded);
  std::string end = std::string(internal::kUnbounded);
  if (!range.has_unbounded_start()) {
    ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToString(
        absl::FromUnixMicros(range.start().value()), functions::kMicroseconds,
        /*timezone_string=*/"+0", &start));
  }
  if (!range.has_unbounded_end()) {
    ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToString(
        absl::FromUnixMicros(range.end().value()), functions::kMicroseconds,
        /*timezone_string=*/"+0", &end));
  }
  return absl::StrFormat(kStringFormat, start, end);
}

}  // namespace zetasql
