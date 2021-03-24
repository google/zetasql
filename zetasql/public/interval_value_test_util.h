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

#ifndef ZETASQL_PUBLIC_INTERVAL_VALUE_TEST_UTIL_H_
#define ZETASQL_PUBLIC_INTERVAL_VALUE_TEST_UTIL_H_

#include <cstdint>

#include "zetasql/public/interval_value.h"

namespace zetasql {

namespace interval_testing {

inline IntervalValue Months(int64_t months) {
  return IntervalValue::FromMonths(months).ValueOrDie();
}

inline IntervalValue Days(int64_t days) {
  return IntervalValue::FromDays(days).ValueOrDie();
}

inline IntervalValue Micros(int64_t micros) {
  return IntervalValue::FromMicros(micros).ValueOrDie();
}

inline IntervalValue Nanos(__int128 nanos) {
  return IntervalValue::FromNanos(nanos).ValueOrDie();
}

inline IntervalValue MonthsDaysMicros(int64_t months, int64_t days,
                                      int64_t micros) {
  return IntervalValue::FromMonthsDaysMicros(months, days, micros).ValueOrDie();
}

inline IntervalValue MonthsDaysNanos(int64_t months, int64_t days,
                                     __int128 nanos) {
  return IntervalValue::FromMonthsDaysNanos(months, days, nanos).ValueOrDie();
}

inline IntervalValue YMDHMS(int64_t years, int64_t months, int64_t days,
                            int64_t hours, int64_t minutes, int64_t seconds) {
  return IntervalValue::FromYMDHMS(years, months, days, hours, minutes, seconds)
      .ValueOrDie();
}

inline IntervalValue Years(int64_t years) {
  return YMDHMS(years, 0, 0, 0, 0, 0);
}

inline IntervalValue Hours(int64_t hours) {
  return YMDHMS(0, 0, 0, hours, 0, 0);
}

inline IntervalValue Minutes(int64_t minutes) {
  return YMDHMS(0, 0, 0, 0, minutes, 0);
}

inline IntervalValue Seconds(int64_t seconds) {
  return YMDHMS(0, 0, 0, 0, 0, seconds);
}

}  // namespace interval_testing

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_INTERVAL_VALUE_TEST_UTIL_H_
