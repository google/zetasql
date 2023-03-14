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

#ifndef ZETASQL_PUBLIC_RANGE_VALUE_H_
#define ZETASQL_PUBLIC_RANGE_VALUE_H_

#include <bitset>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "zetasql/common/errors.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/endian.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// The RANGE value is lightweight equivalent of ValueProto.Range
// The value contains two fields:
// 1. Start
// 2. End
// Both might be an "UNBOUNDED" boundary which is represented by an empty
// optional.
// If start and end are not "UNBOUNDED", then "end" must be greater than "end"
template <typename T>
class RangeValue {
  static_assert(std::is_same<T, int32_t>::value ||
                    std::is_same<T, int64_t>::value,
                "T must be int32_t or int64");

 public:
  constexpr RangeValue()
      : start_value_{}, end_value_{}, has_start_(false), has_end_(false) {}

  // RangeValue constructor.
  // Doesn't check if provided values of start and end are valid,
  // please use "From<Type>" methods instead to create valid RangeValue
  constexpr RangeValue(std::optional<T> start, std::optional<T> end)
      : start_value_(start.value_or(T{})),
        end_value_(end.value_or(T{})),
        has_start_(start.has_value()),
        has_end_(end.has_value()) {}

  std::optional<T> start() const {
    if (!has_start_) {
      return std::nullopt;
    }
    return std::optional<T>(start_value_);
  }

  std::optional<T> end() const {
    if (!has_end_) {
      return std::nullopt;
    }
    return std::optional<T>(end_value_);
  }

  bool has_unbounded_start() const { return !has_start_; }
  bool has_unbounded_end() const { return !has_end_; }

  // Serialization and deserialization methods for range values.
  absl::Status SerializeAndAppendToBytes(std::string* bytes) const;
  absl::StatusOr<std::string> SerializeAsBytes() const {
    std::string bytes;
    ZETASQL_RETURN_IF_ERROR(SerializeAndAppendToBytes(&bytes));
    return bytes;
  }

  static absl::StatusOr<RangeValue<T>> DeserializeFromBytes(
      absl::string_view bytes);

  std::string DebugString() const;

 private:
  static absl::Status VerifyEncodedRangeSize(absl::string_view bytes);

  // T x_value and has_x combination is used instead of
  // std::optional to reduce sizeof(RangeValue)
  // T is expected to be a primitive type such as int32_t, int64_t, or absl::Time,
  // so RangeValue is a very lightweight container that has only data.
  T start_value_;
  T end_value_;
  bool has_start_;
  bool has_end_;
};

absl::StatusOr<std::pair<std::optional<absl::string_view>,
                         std::optional<absl::string_view>>>
GetRangeBoundaries(absl::string_view range_value);

// Given a string_view that starts with encoded range, returns how many bytes
// the encoded range occupies. It's ok for provided string_view to be longer
// than encoded range
absl::StatusOr<size_t> GetEncodedRangeSize(absl::string_view bytes,
                                           TypeKind element_type_kind);
template <typename T>
absl::StatusOr<size_t> GetEncodedRangeSize(absl::string_view bytes);

// // Create Range from date values, where *_date represents number of dates
// // since unix epoch 1970-1-1, if not empty.
absl::StatusOr<RangeValue<int32_t>> RangeValueFromDates(
    std::optional<int32_t> start_date, std::optional<int32_t> end_date);

// // Create Range from date strings with functions::ConvertStringToDate
// // empty std::optional represents an "UNBOUNDED" boundary
absl::StatusOr<RangeValue<int32_t>> ParseRangeValueFromDateStrings(
    std::optional<absl::string_view> start_date_str,
    std::optional<absl::string_view> end_date_str);

absl::StatusOr<std::string> RangeValueDatesToString(
    const RangeValue<int32_t>& range);

absl::StatusOr<RangeValue<int64_t>> RangeValueFromDatetimeMicros(
    std::optional<DatetimeValue> start_datetime,
    std::optional<DatetimeValue> end_datetime);

// Create Range from datetime strings with functions::ConvertStringToDatetime
// empty std::optional represents an "UNBOUNDED" boundary
absl::StatusOr<RangeValue<int64_t>> ParseRangeValueDatetimeMicrosFromStrings(
    std::optional<absl::string_view> start_datetime_str,
    std::optional<absl::string_view> end_datetime_str);

absl::StatusOr<std::string> RangeValueDatetimeMicrosToString(
    const RangeValue<int64_t>& range);

absl::StatusOr<RangeValue<int64_t>> RangeValueFromTimestampMicros(
    std::optional<int64_t> start_timestamp,
    std::optional<int64_t> end_timestamp);
// Create Range from timestamp strings with
// functions::ConvertStringToTimestamp
// empty std::optional represents an "UNBOUNDED" boundary
absl::StatusOr<RangeValue<int64_t>>
ParseRangeValueTimestampMicrosFromTimestampStrings(
    std::optional<absl::string_view> start_timestamp_str,
    std::optional<absl::string_view> end_timestamp_str,
    absl::TimeZone default_timezone);

absl::StatusOr<std::string> RangeValueTimestampMicrosToString(
    const RangeValue<int64_t>& range);

// ------------------- Implementation

namespace internal {

constexpr uint8_t kHasBoundedStartMask = 0x01;
constexpr uint8_t kHasBoundedEndMask = 0x02;
static constexpr absl::string_view kUnbounded = "UNBOUNDED";

}  // namespace internal

/* static */
template <typename T>
absl::Status RangeValue<T>::VerifyEncodedRangeSize(absl::string_view bytes) {
  ZETASQL_ASSIGN_OR_RETURN(const size_t bytes_to_read, GetEncodedRangeSize<T>(bytes));
  if (ABSL_PREDICT_FALSE(bytes.size() < bytes_to_read)) {
    return MakeEvalError() << absl::StrFormat(
               "Too few bytes to read RANGE content (needed %d; got %d)",
               bytes_to_read, bytes.size());
  }
  return absl::OkStatus();
}

/* static */
template <typename T>
absl::Status RangeValue<T>::SerializeAndAppendToBytes(
    std::string* bytes) const {
  uint8_t header = 0x00;
  if (has_start_) {
    header |= internal::kHasBoundedStartMask;
  }
  if (has_end_) {
    header |= internal::kHasBoundedEndMask;
  }
  bytes->push_back(header);
  if (has_start_) {
    char start[sizeof(T)];
    zetasql_base::LittleEndian::Store<T>(start_value_, start);
    bytes->append(start, sizeof(start));
  }
  if (has_end_) {
    char end[sizeof(T)];
    zetasql_base::LittleEndian::Store<T>(end_value_, end);
    bytes->append(end, sizeof(end));
  }
  return absl::OkStatus();
}

template <typename T>
absl::StatusOr<size_t> GetEncodedRangeSize(absl::string_view bytes) {
  if (bytes.size() < sizeof(uint8_t)) {
    return MakeEvalError() << absl::StrFormat(
               "Too few bytes to determine encoded RANGE size (needed %d; got "
               "%d)",
               sizeof(uint8_t), bytes.size());
  }
  uint8_t header = zetasql_base::LittleEndian::Load<uint8_t>(bytes.data());
  size_t num_bounds = (header & internal::kHasBoundedStartMask) +
                      ((header & internal::kHasBoundedEndMask) >> 1);
  return sizeof(header) + num_bounds * sizeof(T);
}

/* static */
template <typename T>
absl::StatusOr<RangeValue<T>> RangeValue<T>::DeserializeFromBytes(
    absl::string_view bytes) {
  const char* data = reinterpret_cast<const char*>(bytes.data());
  // Read the header.
  uint8_t header = zetasql_base::LittleEndian::Load<uint8_t>(bytes.data());
  data += sizeof(header);
  ZETASQL_RETURN_IF_ERROR(VerifyEncodedRangeSize(bytes));

  std::optional<T> start, end;
  if (header & internal::kHasBoundedStartMask) {
    start = zetasql_base::LittleEndian::Load<T>(data);
    data += sizeof(T);
  }
  if (header & internal::kHasBoundedEndMask) {
    end = zetasql_base::LittleEndian::Load<T>(data);
    data += sizeof(T);
  }
  return RangeValue(start, end);
}

template <typename T>
std::string RangeValue<T>::DebugString() const {
  return absl::StrFormat(
      "[%s, %s)",
      has_start_ ? absl::StrCat(start_value_) : internal::kUnbounded,
      has_end_ ? absl::StrCat(end_value_) : internal::kUnbounded);
}

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_RANGE_VALUE_H_
