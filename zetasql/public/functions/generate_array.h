//
// Copyright 2019 ZetaSQL Authors
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

#ifndef ZETASQL_PUBLIC_FUNCTIONS_GENERATE_ARRAY_H_
#define ZETASQL_PUBLIC_FUNCTIONS_GENERATE_ARRAY_H_

#include <stddef.h>

#include <cmath>
#include <vector>

#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/numeric_value.h"
#include <cstdint>
#include "absl/time/time.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

namespace zetasql {
namespace functions {

template <typename T, typename TStep>
struct ArrayGenTrait {
  using elem_t = T;
  using step_t = TStep;
  static T ExtractStep(TStep in) { return in; }
  static zetasql_base::Status GenerateNextValue(T start, T cur, TStep step,
                                        size_t num_elements, T* out) {
    // In the case of doubles we don't want to use the previous element,
    // as it might accumulate more error.
    // It will use: start + (num_elements*step)
    zetasql_base::Status status;
    zetasql::functions::Multiply<T>(num_elements, step, out, &status);
    if (!status.ok()) {
      return status;
    }
    zetasql::functions::Add(start, *out, out, &status);
    return status;
  }
};

// static
template <>
inline zetasql_base::Status
ArrayGenTrait<NumericValue, NumericValue>::GenerateNextValue(
    NumericValue start, NumericValue cur, NumericValue step,
    size_t num_elements, NumericValue* out) {
  ZETASQL_ASSIGN_OR_RETURN(*out, cur.Add(step));
  return ::zetasql_base::OkStatus();
}

struct DateIncrement {
  functions::DateTimestampPart unit;
  int64_t value;
};

template <>
struct ArrayGenTrait<int64_t, DateIncrement> {
  using elem_t = int64_t;
  using step_t = DateIncrement;
  static int64_t ExtractStep(DateIncrement in) { return in.value; }
  static zetasql_base::Status GenerateNextValue(int64_t start, int64_t cur,
                                        DateIncrement step, size_t num_elements,
                                        int64_t* out) {
    int32_t temp;
    ZETASQL_RETURN_IF_ERROR(AddDate(cur, step.unit, step.value, &temp));
    *out = temp;
    return zetasql_base::OkStatus();
  }
};

struct TimestampIncrement {
  functions::DateTimestampPart unit;
  int64_t value;
};

template <>
struct ArrayGenTrait<absl::Time, TimestampIncrement> {
  using elem_t = absl::Time;
  using step_t = TimestampIncrement;
  // For timestamps, the result of ExtractStep is meaningless. Its only use is
  // in checking whether the step is zero (see CheckStartEndStep), so the return
  // value is the Unix epoch for a step of zero, and an arbitrary, meaningless
  // absl::Time for other values.
  static absl::Time ExtractStep(TimestampIncrement in) {
    return absl::FromUnixNanos(in.value);
  }
  static zetasql_base::Status GenerateNextValue(absl::Time start, absl::Time cur,
                                        TimestampIncrement step,
                                        size_t num_elements, absl::Time* out) {
    // The time zone is used only for error messages; passing UTC has no effect
    // on the absl::Time that comes out.
    return AddTimestamp(cur, absl::UTCTimeZone(), step.unit, step.value, out);
  }
};

template <typename ElementType>
zetasql_base::Status CheckStartEndStep(ElementType start, ElementType end,
                               ElementType step_value) {
  if (step_value == 0) {
    return ::zetasql_base::OutOfRangeErrorBuilder() << "Sequence step cannot be 0.";
  }

  // NaN case.
  if (std::isnan(start) || std::isnan(end) || std::isnan(step_value)) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "Sequence start, end, and step cannot be NaN.";
  }

  // +/-inf step case.
  if (std::isinf(step_value)) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "Sequence step cannot be +/-inf.";
  }
  return ::zetasql_base::OkStatus();
}

template <>
zetasql_base::Status inline CheckStartEndStep(NumericValue start, NumericValue end,
                                      NumericValue step_value) {
  if (step_value == NumericValue()) {
    return ::zetasql_base::OutOfRangeErrorBuilder() << "Sequence step cannot be 0.";
  }
  return ::zetasql_base::OkStatus();
}

template <>
zetasql_base::Status inline CheckStartEndStep(absl::Time start, absl::Time end,
                                      absl::Time step_value) {
  if (step_value == absl::UnixEpoch()) {
    return ::zetasql_base::OutOfRangeErrorBuilder() << "Sequence step cannot be 0.";
  }
  return ::zetasql_base::OkStatus();
}

template <typename T>
zetasql_base::Status GenerateArrayHelper(typename T::elem_t start,
                                 typename T::elem_t end,
                                 typename T::step_t step,
                                 std::vector<typename T::elem_t>* values) {
  // To avoid memory exhaustion, we place a hard (arbitrary) limit on the size
  // of generated arrays.
  static constexpr int kMaxGeneratedArraySize = 16000;

  const typename T::elem_t step_value = T::ExtractStep(step);
  const typename T::elem_t zero_value = typename T::elem_t();

  ZETASQL_RETURN_IF_ERROR(CheckStartEndStep(start, end, step_value));

  // Empty range cases.
  if ((start < end && step_value < zero_value) ||
      (start > end && step_value > zero_value)) {
    return ::zetasql_base::OkStatus();
  }

  // Single element case. Handles start == end == +/-inf.
  if (start == end) {
    values->emplace_back(start);
    return ::zetasql_base::OkStatus();
  }

  // When start <= end, generate the range [start, end].
  // When start > end, generate the range [end, start].
  for (typename T::elem_t val = start;
       start <= end ? (val <= end) : (val >= end);) {
    if (values->size() >= kMaxGeneratedArraySize) {
      return ::zetasql_base::OutOfRangeErrorBuilder()
             << "Cannot generate arrays with more than "
             << kMaxGeneratedArraySize << " elements.";
    }
    values->emplace_back(val);
    zetasql_base::Status status =
        T::GenerateNextValue(start, val, step, values->size(), &val);
    if (!status.ok()) {
      // An overflow can only happen here if the generated element value would
      // have been outside the start end range anyway.
      break;
    }
  }
  return ::zetasql_base::OkStatus();
}

template <typename T, typename TStep>
zetasql_base::Status GenerateArray(T start, T end, TStep step, std::vector<T>* values) {
  return GenerateArrayHelper<ArrayGenTrait<T, TStep>>(start, end, step, values);
}

}  // namespace functions
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_FUNCTIONS_GENERATE_ARRAY_H_
