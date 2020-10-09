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

#include "absl/base/casts.h"
#include "absl/status/status.h"
#include "zetasql/base/endian.h"

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

}  // namespace zetasql
