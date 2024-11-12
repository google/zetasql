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

#include "zetasql/public/timestamp_pico_value.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include "zetasql/public/functions/date_time_util.h"
#include "absl/hash/hash.h"
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "zetasql/base/endian.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

TimestampPicoValue::TimestampPicoValue(absl::Time time) {
  seconds_ = absl::ToUnixSeconds(time);
  int64_t nanos =
      (time - absl::FromUnixSeconds(seconds_)) / absl::Nanoseconds(1);
  picos_ = nanos * 1000;
}

absl::Time TimestampPicoValue::ToTime() const {
  return absl::FromUnixSeconds(seconds_) + absl::Nanoseconds(picos_ / 1000);
}

void TimestampPicoValue::SerializeAndAppendToProtoBytes(
    std::string* bytes) const {
  uint64_t seconds = zetasql_base::LittleEndian::FromHost64(seconds_);
  bytes->append(reinterpret_cast<const char*>(&seconds), sizeof(seconds));
  uint64_t picos = zetasql_base::LittleEndian::FromHost64(picos_);
  bytes->append(reinterpret_cast<const char*>(&picos), sizeof(picos));
}

absl::StatusOr<TimestampPicoValue>
TimestampPicoValue::DeserializeFromProtoBytes(absl::string_view bytes) {
  if (bytes.empty() || bytes.size() != sizeof(TimestampPicoValue)) {
    return absl::OutOfRangeError(
        absl::StrCat("Invalid serialized TIMESTAMP_PICO size, expected ",
                     sizeof(TimestampPicoValue), " bytes, but got ",
                     bytes.size(), " bytes."));
  }
  const char* data = bytes.data();
  TimestampPicoValue value;
  value.seconds_ = zetasql_base::LittleEndian::Load<uint64_t>(data);
  value.picos_ = zetasql_base::LittleEndian::Load<uint64_t>(data + sizeof(value.seconds_));
  return value;
}

std::string TimestampPicoValue::ToString(absl::TimeZone timezone) const {
  std::string s;
  // Failure cannot actually happen in this context since the value
  // is guaranteed to be valid.
  // TODO: output picosecond precision after introducing PicoTime
  // to replace absl::Time.
  auto status = functions::ConvertTimestampToString(
      ToTime(), functions::kNanoseconds, timezone, &s);
  ZETASQL_DCHECK_OK(status);
  return s;
}

size_t TimestampPicoValue::HashCode() const {
  return absl::Hash<std::pair<int64_t, uint64_t>>()(
      std::make_pair(this->seconds_, this->picos_));
}

absl::StatusOr<TimestampPicoValue> TimestampPicoValue::FromString(
    absl::string_view str, absl::TimeZone default_timezone,
    bool allow_tz_in_str) {
  // TODO: Support pico second precision.
  absl::Time timestamp;
  ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTimestamp(
      str, default_timezone, zetasql::functions::kNanoseconds,
      /*allow_tz_in_str=*/allow_tz_in_str, &timestamp));
  return TimestampPicoValue(timestamp);
}

}  // namespace zetasql
