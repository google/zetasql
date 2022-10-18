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

#include "zetasql/common/timer_util.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"

namespace zetasql::internal {

// We don't really want to test their absolute correctness, we assume the
// the timers are generally correct, we are mostly ensuring they aren't
// wildly wrong, which indicates a problem with our interaction in some way.
TEST(Timer, TimerApproxCorrect) {
  for (int i = 0; i < 10; ++i) {
    constexpr absl::Duration sleep_duration = absl::Milliseconds(100);
    auto timer = MakeTimerStarted();
    absl::SleepFor(sleep_duration);

    const absl::Duration measured = timer.GetDuration();
    const absl::Duration expected = sleep_duration;
    const absl::Duration tolerance = absl::Milliseconds(30);

    if (absl::AbsDuration(expected - measured) < tolerance) {
      return;
    }
  }
  // The tolerance is already very very high, probably worth investigating for
  // a real bug before increasing the tolerance more.
  FAIL() << "After 10 tries, timer still not returning times within tolerance";
}

TEST(TimedValue, InitializesToZero) {
  TimedValue v1;
  EXPECT_EQ(v1.elapsed_duration(), absl::Nanoseconds(0));
}

TEST(TimedValue, AccumulateDuration) {
  TimedValue v1;
  v1.Accumulate(absl::Nanoseconds(15));
  EXPECT_EQ(v1.elapsed_duration(), absl::Nanoseconds(15));

  v1.Accumulate(absl::Nanoseconds(15));
  EXPECT_EQ(v1.elapsed_duration(), absl::Nanoseconds(30));
}

TEST(TimedValue, AccumulateTimedValue) {
  TimedValue v1;
  v1.Accumulate(absl::Nanoseconds(15));

  TimedValue v2;
  v2.Accumulate(v1);
  EXPECT_EQ(v2.elapsed_duration(), absl::Nanoseconds(15));
  v2.Accumulate(v1);
  EXPECT_EQ(v2.elapsed_duration(), absl::Nanoseconds(30));
}

// We just want to make sure it's somewhere in the ballpark.
TEST(TimedValue, AccumulateTimerApproxCorrect) {
  for (int i = 0; i < 10; ++i) {
    TimedValue v1;
    constexpr absl::Duration sleep_duration = absl::Milliseconds(100);
    auto timer = MakeTimerStarted();
    absl::SleepFor(sleep_duration);
    v1.Accumulate(timer);
    const absl::Duration measured = v1.elapsed_duration();
    const absl::Duration expected = sleep_duration;
    const absl::Duration tolerance = absl::Milliseconds(30);

    if (absl::AbsDuration(expected - measured) < tolerance) {
      return;
    }
  }
  // The tolerance is already very very high, probably worth investigating for
  // a real bug before increasing the tolerance more.
  FAIL() << "After 10 tries, timer still not returning times within tolerance";
}

// We just want to make sure it's somewhere in the ballpark.
TEST(TimedValue, MakeScopedTimerStarted) {
  TimedValue v1;
  {
    auto scoped_timer = MakeScopedTimerStarted(&v1);
    absl::SleepFor(absl::Milliseconds(10));
  }

  // We just want to ensure the timer triggers, since we test the validity
  // of the timers in other tests.
  EXPECT_GT(v1.elapsed_duration(), absl::Milliseconds(1));
}

}  // namespace zetasql::internal
