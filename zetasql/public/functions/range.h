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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_RANGE_H_
#define ZETASQL_PUBLIC_FUNCTIONS_RANGE_H_

#include <optional>
#include <type_traits>

#include "zetasql/common/errors.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/interval_value.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

// Helper functions used for parsing range values from strings.
namespace zetasql {

struct StringRangeBoundaries {
  std::optional<absl::string_view> start;
  std::optional<absl::string_view> end;
};

// Extracts the "start" and "end" parts from a given range literal string
// with format "[<start>, <end>]", and returns it as a std::pair of optional
// "start" and "end" parts respectively. If the "start" and "end" parts are
// unbounded, then std::nullopt is returned in the corresponding part of the
// pair.
// If "strict_formatting" is set to true, then method will require
// <start> and <end> to be delimited by ", " (comma then space) with no extra
// trailing spaces, and will return error if it's not the case.
absl::StatusOr<StringRangeBoundaries> ParseRangeBoundaries(
    absl::string_view range_value, bool strict_formatting = true);

namespace functions {

// Utility class for GENERATE_RANGE_ARRAY function implementations
// that covers RANGE<TIMESTAMP>
//
// This class is structured to allow for more efficient computation when all,
// but first (input RANGE) function arguments are constant.
class TimestampRangeArrayGenerator {
 public:
  static absl::StatusOr<TimestampRangeArrayGenerator> Create(
      IntervalValue step, bool last_partial_range, TimestampScale scale);

  // Generates [start, end) subranges of the specified range (passed as
  // range_start and range_end arguments) and invokes the provided emitter Fn
  // for every subrange.
  //
  // Fn can be any invocable with the following signature:
  //   absl::Status(absl::Time, absl::Time)
  // Fn can terminate the process by returning a non-OK absl::Status.
  //
  // An error is returned if the input range has UNBOUNDED endpoints.
  template <typename Fn>
  absl::Status Generate(std::optional<absl::Time> range_start,
                        std::optional<absl::Time> range_end,
                        const Fn& emitter) const;

 private:
  static absl::Status ValidateStep(const IntervalValue& step,
                                   TimestampScale scale);
  static absl::Duration IntervalToDuration(const IntervalValue& step);

  TimestampRangeArrayGenerator(absl::Duration step, bool last_partial_range)
      : step_(step), last_partial_range_(last_partial_range) {}

  absl::Duration step_;
  bool last_partial_range_;
};

template <typename Fn>
absl::Status TimestampRangeArrayGenerator::Generate(
    std::optional<absl::Time> range_start, std::optional<absl::Time> range_end,
    const Fn& emitter) const {
  static_assert(
      std::is_invocable_r_v<absl::Status, Fn, absl::Time, absl::Time>,
      "emitter should be invocable as absl::Status(absl::Time, absl::Time)");
  if (!range_start || !range_end) {
    return MakeEvalError() << "input RANGE cannot have UNBOUNDED endpoints";
  }
  absl::Time start = range_start.value();
  absl::Time end = range_end.value();
  ZETASQL_RET_CHECK(IsValidTime(start));
  ZETASQL_RET_CHECK(IsValidTime(end));
  ZETASQL_RET_CHECK_LT(start, end) << "invalid input RANGE value";

  // Note that in the following code we don't need to check for overflows when
  // adding step_ to current_start, since the range of representable dates in
  // absl::Time (+/-100 billion years) is much wider than the range of allowed
  // dates in TIMESTAMP (1-10,000 years).

  absl::Time current_start;
  absl::Time current_end = start;
  for (;;) {
    current_start = current_end;
    current_end = current_start + step_;
    if (ABSL_PREDICT_FALSE(current_end >= end)) {
      if (current_end == end || last_partial_range_) {
        ZETASQL_RETURN_IF_ERROR(emitter(current_start, end));
      }
      break;
    }
    ZETASQL_RETURN_IF_ERROR(emitter(current_start, current_end));
  }

  return absl::OkStatus();
}

// Utility class for GENERATE_RANGE_ARRAY function implementations
// that covers RANGE<DATE>
//
// This class is structured to allow for more efficient computation when all,
// but first (input RANGE) function arguments are constant.
class DateRangeArrayGenerator {
 public:
  static absl::StatusOr<DateRangeArrayGenerator> Create(
      IntervalValue step, bool last_partial_range);

  // Generates [start, end) subranges of the specified range (passed as
  // range_start and range_end arguments) and invokes the provided emitter Fn
  // for every subrange.
  //
  // Fn can be any invocable with the following signature:
  //   absl::Status(int32_t, int32_t)
  // Fn can terminate the process by returning a non-OK absl::Status.
  //
  // An error is returned if the input range has UNBOUNDED endpoints.
  template <typename Fn>
  absl::Status Generate(std::optional<int32_t> range_start,
                        std::optional<int32_t> range_end,
                        const Fn& emitter) const;

 private:
  static absl::Status ValidateStep(const IntervalValue& step);

  DateRangeArrayGenerator(IntervalValue step, bool last_partial_range)
      : step_(step), last_partial_range_(last_partial_range) {}

  IntervalValue step_;
  bool last_partial_range_;
};

template <typename Fn>
absl::Status DateRangeArrayGenerator::Generate(
    std::optional<int32_t> range_start, std::optional<int32_t> range_end,
    const Fn& emitter) const {
  static_assert(
      std::is_invocable_r_v<absl::Status, Fn, int32_t, int32_t>,
      "emitter should be invocable as absl::Status(int32_t, int32_t)");
  if (!range_start || !range_end) {
    return MakeEvalError() << "input RANGE cannot have UNBOUNDED endpoints";
  }
  int32_t start = range_start.value();
  int32_t end = range_end.value();
  ZETASQL_RET_CHECK(IsValidDate(start));
  ZETASQL_RET_CHECK(IsValidDate(end));
  ZETASQL_RET_CHECK_LT(start, end) << "invalid input RANGE value";

  int32_t current_start;
  int32_t current_end = start;
  absl::Status status;
  // Note: current_step would never overflow because MAX_MONTHS and MAX_DAYS
  // do not exceed the boundary of int64_t
  // Every iteration, we use the incremented step value to handle cases where
  // dates are near the end of the month consistent with the existing method
  int64_t current_step = 0;
  for (;;) {
    current_start = current_end;
    if (step_.get_months() > 0) {
      // Case 1. step is composed of Y-M
      current_step += step_.get_months();
      status = AddDate(start, MONTH, current_step, &current_end);
    } else {
      // Case 2. step is composed of D
      current_step += step_.get_days();
      status = AddDate(start, DAY, current_step, &current_end);
    }

    // We expect status to be not OK when current_end overflows date bound
    if (ABSL_PREDICT_FALSE(!status.ok() || current_end >= end)) {
      if (current_end == end || last_partial_range_) {
        ZETASQL_RETURN_IF_ERROR(emitter(current_start, end));
      }
      break;
    }
    ZETASQL_RETURN_IF_ERROR(emitter(current_start, current_end));
  }

  return absl::OkStatus();
}

// Utility class for GENERATE_RANGE_ARRAY function implementations
// that covers RANGE<DATETIME>
//
// This class is structured to allow for more efficient computation when all,
// but first (input RANGE) function arguments are constant.
class DatetimeRangeArrayGenerator {
 public:
  static absl::StatusOr<DatetimeRangeArrayGenerator> Create(
      IntervalValue step, bool last_partial_range, TimestampScale scale);

  // Generates [start, end) subranges of the specified range (passed as
  // range_start and range_end arguments) and invokes the provided emitter Fn
  // for every subrange.
  //
  // Fn can be any invocable with the following signature:
  //   absl::Status(DatetimeValue, DatetimeValue)
  // Fn can terminate the process by returning a non-OK absl::Status.
  //
  // An error is returned if the input range has UNBOUNDED endpoints.
  template <typename Fn>
  absl::Status Generate(std::optional<DatetimeValue> range_start,
                        std::optional<DatetimeValue> range_end,
                        const Fn& emitter) const;

 private:
  static absl::Status ValidateStep(const IntervalValue& step,
                                   TimestampScale scale);

  DatetimeRangeArrayGenerator(IntervalValue step, bool last_partial_range)
      : step_(step), last_partial_range_(last_partial_range) {}

  IntervalValue step_;
  bool last_partial_range_;
};

template <typename Fn>
absl::Status DatetimeRangeArrayGenerator::Generate(
    std::optional<DatetimeValue> range_start,
    std::optional<DatetimeValue> range_end, const Fn& emitter) const {
  static_assert(
      std::is_invocable_r_v<absl::Status, Fn, DatetimeValue, DatetimeValue>,
      "emitter should be invocable as absl::Status(DatetimeValue, "
      "DatetimeValue)");
  if (!range_start || !range_end) {
    return MakeEvalError() << "input RANGE cannot have UNBOUNDED endpoints";
  }
  DatetimeValue start = range_start.value();
  DatetimeValue end = range_end.value();
  ZETASQL_RET_CHECK(start.IsValid());
  ZETASQL_RET_CHECK(end.IsValid());
  ZETASQL_RET_CHECK(IntervalDiffDatetimes(start, end)->GetAsNanos() <= 0)
      << "invalid input RANGE value";

  DatetimeValue current_start;
  DatetimeValue current_end = start;
  absl::Status status;
  // Every iteration, we use the incremented step value to handle cases where
  // dates are near the end of the month consistent with the existing method
  IntervalValue current_step;
  for (;;) {
    current_start = current_end;
    absl::StatusOr<IntervalValue> next_step = current_step + step_;
    // We expect status to be not OK when next_step overflowed IntervalValue
    if (ABSL_PREDICT_FALSE(!next_step.ok())) {
      // If the next_step has overflown, then it means the next_step is over
      // 10000 years (which exceeds the bound of Datetime).
      // Therefore, it is safe to exit the loop once the IntervalValue overflows
      if (last_partial_range_) {
        ZETASQL_RETURN_IF_ERROR(emitter(current_start, end));
      }
      break;
    }

    current_step = next_step.value();
    status = AddDatetime(start, current_step, &current_end);
    ZETASQL_ASSIGN_OR_RETURN(IntervalValue diff,
                     IntervalDiffDatetimes(current_end, end));

    // We expect status to be not OK when current_end overflows datetime bound
    if (ABSL_PREDICT_FALSE(!status.ok() || diff.GetAsNanos() >= 0)) {
      if (diff.GetAsNanos() == 0 || last_partial_range_) {
        ZETASQL_RETURN_IF_ERROR(emitter(current_start, end));
      }
      break;
    }

    ZETASQL_RETURN_IF_ERROR(emitter(current_start, current_end));
  }

  return absl::OkStatus();
}

}  // namespace functions

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_RANGE_H_
