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

#ifndef ZETASQL_PUBLIC_ANONYMIZATION_UTILS_H_
#define ZETASQL_PUBLIC_ANONYMIZATION_UTILS_H_

#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace anonymization {

// We are rewriting ANON_VAR_POP, ANON_STDDEV_POP, ANON_PERCENTILE_CONT, and
// ANON_QUANTILES, to per-user aggregation
// ARRAY_AGG(expr IGNORE NULLS ORDER BY rand() LIMIT 5).
// The limit of 5 is proposed to be consistent with the current
// implementation, and at some point we may want to make this configurable.
// For more information, see (broken link).
static constexpr int kPerUserArrayAggLimit = 5;

// Computes the threshold for Laplace partition selection from anonymization
// option delta.
//
// epsilon_value and delta_value must be a valid
// Value, while max_groups_contributed_value is optional (as indicated by an
// invalid Value).
//
// A valid max_groups_contributed_value implies contributions are being bounded
// across GROUP BY groups, while an invalid max_groups_contributed_value implies
// the opposite.
//
// Returns an error if the inputs are not logically valid.
absl::StatusOr<Value> ComputeLaplaceThresholdFromDelta(
    Value epsilon_value, Value delta_value, Value max_groups_contributed_value);

// Computes anonymization option delta from the threshold for Laplace
// partition selection.
//
// epsilon_value and laplace_threshold_value must be valid Values, while
// max_groups_contributed_value is optional (as indicated by an invalid Value).
//
// A valid max_groups_contributed_value implies contributions are being bounded
// across GROUP BY groups, while an invalid max_groups_contributed_value implies
// the opposite.
//
// Returns an error if the inputs are not logically valid, or if the computed
// delta is outside the valid range [0.0, 1.0).
absl::StatusOr<Value> ComputeDeltaFromLaplaceThreshold(
    Value epsilon_value, Value laplace_threshold_value,
    Value max_groups_contributed_value);

}  // namespace anonymization
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_ANONYMIZATION_UTILS_H_
