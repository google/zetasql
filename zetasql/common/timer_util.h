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

#ifndef ZETASQL_COMMON_TIMER_UTIL_H_
#define ZETASQL_COMMON_TIMER_UTIL_H_

#include <algorithm>
#include <cstdint>
#include <optional>

#include "google/protobuf/duration.pb.h"
#include "zetasql/common/thread_stack.h"
#include "zetasql/public/proto/logging.pb.h"
#include "absl/base/attributes.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace zetasql::internal {

// Instantaneous snapshot of current resource usage (time, etc).
class ResourceMeasurement {
 private:
  static int64_t ThreadCPUNanos() {
    timespec ts;
#ifdef CLOCK_THREAD_CPUTIME_ID
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
#else
#warn Thread CPU time measurement not supported
    ts = {};
#endif
    return ts.tv_sec * 1000000000 + ts.tv_nsec;
  }
  int64_t cpu_nanos_ = ThreadCPUNanos();
  absl::Time time_ = absl::Now();
  ThreadStackStats stack_stats_ = GetCurrentThreadStackStats();
  friend class TimedValue;
};

// A timer for recording usage of CPU, stack, etc. inside ZetaSQL.
class ElapsedTimer {
 public:
  const ResourceMeasurement& GetStart() const { return timer_start_; }

 private:
  ResourceMeasurement timer_start_ = ResourceMeasurement();
};

// A helper class represent an accumulated total of resources used (like CPU,
// wall time and stack). It provides a few
// properties:
//  - Coupled with the type returned to MakeTimerStarted, this allows
//    complete abstraction of the actual type of the timer. This helpful
//    to allow different types internally and in ZetaSQL.
//  - Helpful Accumulate methods.
class TimedValue {
 public:
  TimedValue() = default;
  TimedValue(const ResourceMeasurement& start, const ResourceMeasurement& end) {
    wall_time_ = end.time_ - start.time_;
    cpu_time_ = absl::Nanoseconds(end.cpu_nanos_ - start.cpu_nanos_);
    stack_available_bytes_ = start.stack_stats_.ThreadStackAvailableBytes();
    stack_peak_used_bytes_ = end.stack_stats_.ThreadStackPeakUsedBytes();
  }

  ExecutionStats ToExecutionStatsProto() const {
    ExecutionStats ret;
    DurationToProto(cpu_time_, ret.mutable_cpu_time());
    DurationToProto(wall_time_, ret.mutable_wall_time());
    ret.set_stack_available_bytes(stack_available_bytes_);
    ret.set_stack_peak_used_bytes(stack_peak_used_bytes_);
    return ret;
  }

  ABSL_DEPRECATED(
      "Consider ToExecutionStatsProto for more than wall time usage")
  absl::Duration elapsed_duration() const { return wall_time_; }

  void Accumulate(absl::Duration duration) { wall_time_ += duration; }
  void Accumulate(ElapsedTimer timer) {
    Accumulate(TimedValue(timer.GetStart(), ResourceMeasurement()));
  }
  void Accumulate(const TimedValue& timer) {
    wall_time_ += timer.wall_time_;
    cpu_time_ += timer.cpu_time_;
    stack_available_bytes_ =
        std::max(stack_available_bytes_, timer.stack_available_bytes_);
    stack_peak_used_bytes_ =
        std::max(stack_peak_used_bytes_, timer.stack_peak_used_bytes_);
  }

 private:
  static void DurationToProto(absl::Duration d,
                              google::protobuf::Duration* out) {
    out->set_seconds(absl::IDivDuration(d, absl::Seconds(1), &d));
    out->set_nanos(absl::IDivDuration(d, absl::Nanoseconds(1), &d));
  }
  absl::Duration wall_time_;
  absl::Duration cpu_time_;
  size_t stack_available_bytes_ = 0;
  size_t stack_peak_used_bytes_ = 0;
};

inline ElapsedTimer MakeTimerStarted() {
  ElapsedTimer timer;
  return timer;
}

class ScopedTimer {
 public:
  explicit ScopedTimer(TimedValue* timed_value)
      : timed_value_(timed_value), timer_(MakeTimerStarted()) {
    ABSL_DCHECK(timed_value_ != nullptr);
    original_stack_usage_ =
        GetCurrentThreadStackStats().ResetPeakStackUsedBytes();
  }
  ScopedTimer() = delete;
  ScopedTimer(const ScopedTimer&) = delete;

  ~ScopedTimer() { EndTiming(); }

  void EndTiming() {
    if (timed_value_ != nullptr) {
      timed_value_->Accumulate(timer_);
      GetCurrentThreadStackStats().MergeStackEstimatedUsage(
          original_stack_usage_);
    }
    timed_value_ = nullptr;
  }

 private:
  TimedValue* timed_value_;
  ElapsedTimer timer_;
  ThreadStackEstimatedUsage original_stack_usage_;
};

// Create a scoped timer which updates TimedValue when it goes out of scope.
// Example:
//  TimedValue& my_timed_value = ...;
//  {
//    ScopedTimer scoped_timer = MakeScopedTimer(&my_timed_value);
//    // computation
//    if (...) return;
//    ...
//  }
//
inline ScopedTimer MakeScopedTimerStarted(TimedValue* timed_value) {
  return ScopedTimer(timed_value);
}

}  // namespace zetasql::internal

#endif  // ZETASQL_COMMON_TIMER_UTIL_H_
