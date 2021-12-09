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

#ifndef ZETASQL_PUBLIC_TYPES_VALUE_EQUALITY_CHECK_OPTIONS_H_
#define ZETASQL_PUBLIC_TYPES_VALUE_EQUALITY_CHECK_OPTIONS_H_

#include <string>

#include "zetasql/common/float_margin.h"

namespace zetasql {

// Options to control how two INTERVAL values should be compared for equality.
enum class IntervalCompareMode {
  // Considers two INTERVAL values to be equal if they represent the same total
  // amount of time, where 1 MONTH = 30 DAYS and 1 DAY = 24 HOURS.
  //
  // This follows the semantics used by the "=" operator, GROUP BY, DISTINCT,
  // etc.
  //
  // INTERVAL values that are "equal" in this mode may still produce different
  // resuts in situations such as casting to STRING and adding to a DATE.
  //
  // See (broken link) for details.
  kSqlEquals,

  // Considers two INTERVAL values to be equal only if all component parts (
  // months, days, and nanoseconds) are all equal individually.
  //
  // This is used by compliance tests to compare actual vs. expected output on
  // queries that produce results of INTERVAL type.
  kAllPartsEqual,
};

// Contains value equality check options that can be provided to
// Type::ValueContentEquals function.
struct ValueEqualityCheckOptions {
  IntervalCompareMode interval_compare_mode = IntervalCompareMode::kSqlEquals;

  // Defines the maximum allowed absolute error when comparing floating point
  // numbers (float and double).
  FloatMargin float_margin = kExactFloatMargin;

  // If 'reason' is not null, upon inequality it may be set to human-readable
  // explanation of what parts of values differ.
  std::string* reason = nullptr;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_VALUE_EQUALITY_CHECK_OPTIONS_H_
