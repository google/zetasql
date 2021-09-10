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
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace anonymization {

// Computes anonymization option k-threshold from epsilon, delta, and kappa.
// Epsilon and delta must be a valid Value, while kappa is optional
// (as indicated by an invalid Value).
//
// A valid kappa implies implies contributions are being bounded across
// GROUP BY groups, while an invalid kappa implies the opposite.
//
// Returns an error if the inputs are not logically valid.
absl::StatusOr<Value> ComputeKThresholdFromEpsilonDeltaKappa(
    Value epsilon_value, Value delta_value, Value kappa_value);

// Computes anonymization option delta from epsilon, k-threshold, and kappa.
// Epsilon and k-threshold must be valid Values, while kappa is
// optional (as indicated by an invalid Value).
//
// A valid kappa implies implies contributions are being bounded across
// GROUP BY groups, while an invalid kappa implies the opposite.
//
// Returns an error if the inputs are not logically valid, or if the computed
// delta is outside the valid range [0.0, 1.0).
absl::StatusOr<Value> ComputeDeltaFromEpsilonKThresholdKappa(
    Value epsilon_value, Value k_threshold_value, Value kappa_value);

}  // namespace anonymization
}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_ANONYMIZATION_UTILS_H_
