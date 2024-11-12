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

#ifndef ZETASQL_PUBLIC_TIMESTAMP_PICO_VALUE_H_
#define ZETASQL_PUBLIC_TIMESTAMP_PICO_VALUE_H_

#include <compare>
#include <cstddef>
#include <cstdint>
#include <string>

#include "absl/numeric/int128.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"

namespace zetasql {

// TODO: Code for this class is currently still under
// development; do NOT take a dependency on it.
//
// This class represents values of the ZetaSQL Timestamp_pico type.
//
// Internally the values are stored using two 64-bit integers.
class TimestampPicoValue final {
 public:
  // Representing "1970-01-01 00:00:00.000000000000 UTC".
  TimestampPicoValue() : seconds_(0), picos_(0) {}
  TimestampPicoValue(const TimestampPicoValue&) = default;
  TimestampPicoValue(TimestampPicoValue&&) = default;
  TimestampPicoValue& operator=(const TimestampPicoValue&) = default;
  TimestampPicoValue& operator=(TimestampPicoValue&&) = default;

  // TODO: introduce PicoTime to replace absl::Time.
  explicit TimestampPicoValue(absl::Time time);

  // TODO: introduce PicoTime to replace absl::Time.
  // absl::Time keeps the precision of nanoseconds.
  absl::Time ToTime() const;

  // Returns the maximum and minimum UUID values.
  // Minimum value is 0001-01-01 00:00:00.000000000000 UTC
  // Maximum value is 9999-12-31 23:59:59.999999999999 UTC
  static TimestampPicoValue MinValue() {
    return TimestampPicoValue(-62135596800, 0);
  };
  static TimestampPicoValue MaxValue() {
    return TimestampPicoValue(253402300799, 999999999999);
  }

  // Serializes into a fixed-size 16-byte representation and appends
  // it directly to the provided 'bytes' string.
  void SerializeAndAppendToProtoBytes(std::string* bytes) const;

  // Serializes into a fixed-size 16-byte representation and returns
  // it as a string.
  std::string SerializeAsProtoBytes() const {
    std::string result;
    SerializeAndAppendToProtoBytes(&result);
    return result;
  }

  // Deserializes from fixed-size 16-byte representation. Returns an
  // error if the given input doesn't contains exactly 16 bytes of data.
  static absl::StatusOr<TimestampPicoValue> DeserializeFromProtoBytes(
      absl::string_view bytes);

  size_t HashCode() const;

  std::string DebugString() const { return ToString(); }

  std::string ToString(absl::TimeZone timezone = absl::UTCTimeZone()) const;

  // If the time zone is not included in the string, then <default_timezone> is
  // applied for the conversion.  Uses the canonical timestamp string format.
  // If the time zone is included in the string and allow_tz_in_str is set to
  // false, an error will be returned.
  static absl::StatusOr<TimestampPicoValue> FromString(
      absl::string_view str, absl::TimeZone default_timezone,
      bool allow_tz_in_str);

  // Comparison operators.
  bool operator==(const TimestampPicoValue& rh) const;
  bool operator!=(const TimestampPicoValue& rh) const;
  bool operator<(const TimestampPicoValue& rh) const;
  bool operator>(const TimestampPicoValue& rh) const;
  bool operator>=(const TimestampPicoValue& rh) const;
  bool operator<=(const TimestampPicoValue& rh) const;
#ifdef __cpp_impl_three_way_comparison
  std::strong_ordering operator<=>(const TimestampPicoValue& rh) const;
#endif

 private:
  TimestampPicoValue(int64_t seconds, uint64_t picos)
      : seconds_(seconds), picos_(picos) {}

  // Seconds since epoch (1970-01-01 00:00:00.000000000000 UTC).
  int64_t seconds_ = 0;
  uint64_t picos_ = 0;
};

inline bool TimestampPicoValue::operator==(const TimestampPicoValue& rh) const {
  return seconds_ == rh.seconds_ && picos_ == rh.picos_;
}

inline bool TimestampPicoValue::operator!=(const TimestampPicoValue& rh) const {
  return !(*this == rh);
}

inline bool TimestampPicoValue::operator<(const TimestampPicoValue& rh) const {
  return seconds_ < rh.seconds_ ||
         (seconds_ == rh.seconds_ && picos_ < rh.picos_);
}

inline bool TimestampPicoValue::operator>(const TimestampPicoValue& rh) const {
  return seconds_ > rh.seconds_ ||
         (seconds_ == rh.seconds_ && picos_ > rh.picos_);
}

inline bool TimestampPicoValue::operator>=(const TimestampPicoValue& rh) const {
  return !(*this < rh);
}

inline bool TimestampPicoValue::operator<=(const TimestampPicoValue& rh) const {
  return !(*this > rh);
}

}  // namespace zetasql
#endif  // ZETASQL_PUBLIC_TIMESTAMP_PICO_VALUE_H_
