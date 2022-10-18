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

#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace zetasql::internal {

// A simple timer for recording basic runtime statistics inside zetasql.
// Meant to stay compatible-ish with /util/timer.h // coypbara:strip(docs)
class ElapsedWallTimer {
 public:
  void Start() { start_time_ = absl::Now(); }
  // Get the wall-clock duration since the last call to 'Start'.
  absl::Duration GetDuration() const { return absl::Now() - start_time_; }

 private:
  absl::Time start_time_;
};

// A helper class represent an accumulated elapsed timer. It provides a few
// properties:
//  - Coupled with the type returned to MakeTimerStarted, this allows
//    complete abstraction of the actual type of the timer. This helpful
//    to allow different types internally and in ZetaSQL.
//  - Helpful Accumulate methods.
class TimedValue {
 public:
  absl::Duration elapsed_duration() const { return elapsed_; }

  void Accumulate(absl::Duration duration) { elapsed_ += duration; }

  void Accumulate(ElapsedWallTimer timer) { elapsed_ += timer.GetDuration(); }
  void Accumulate(const TimedValue& timer) { elapsed_ += timer.elapsed_; }

 private:
  absl::Duration elapsed_;
};

// We want to (slightly) obfuscate the underlying type we use in case we want
// to switch it out in the future.
using ElapsedTimer = ElapsedWallTimer;

inline ElapsedTimer MakeTimerStarted() {
  ElapsedTimer timer;
  timer.Start();
  return timer;
}

class ScopedWallTimer {
 public:
  explicit ScopedWallTimer(TimedValue* timed_value)
      : timed_value_(timed_value), timer_(MakeTimerStarted()) {}
  ScopedWallTimer() = delete;
  ScopedWallTimer(const ScopedWallTimer&) = delete;

  ~ScopedWallTimer() { timed_value_->Accumulate(timer_); }

 private:
  TimedValue* timed_value_;
  ElapsedTimer timer_;
};

// We want to (slightly) obfuscate the underlying type we use in case we want
// to switch it out in the future.
using ScopedTimer = ScopedWallTimer;

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
