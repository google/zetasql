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

#include "zetasql/public/anonymization_utils.h"

#include <cstdint>

#include "zetasql/public/type.h"
#include "zetasql/base/case.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "algorithms/partition-selection.h"

namespace zetasql {
namespace anonymization {

absl::StatusOr<Value> ComputeKThresholdFromEpsilonDeltaKappa(
    Value epsilon_value, Value delta_value,
    Value kappa_value) {
  if (!kappa_value.is_valid()) {
    // Invalid kappa indicates unset.  Unset kappa implies contributions are not
    // being bounded across GROUP BY groups.  Setting kappa to 1 is equivalent
    // to removing it from the threshold calculation.
    kappa_value = Value::Int64(1);
  }
  ZETASQL_RET_CHECK_EQ(epsilon_value.type_kind(), TYPE_DOUBLE);
  ZETASQL_RET_CHECK_EQ(delta_value.type_kind(), TYPE_DOUBLE);
  ZETASQL_RET_CHECK_EQ(kappa_value.type_kind(), TYPE_INT64);

  const double epsilon = epsilon_value.double_value();
  const double delta = delta_value.double_value();
  const int64_t kappa = kappa_value.int64_value();

  // The caller should have verified that epsilon and delta are not infinity
  // or nan.  The caller should also have verified that epsilon > 0, and that
  // delta is in the range [0, 1]. Kappa must also be > 0.
  ZETASQL_RET_CHECK(!std::isnan(epsilon));
  ZETASQL_RET_CHECK(!std::isnan(delta));
  ZETASQL_RET_CHECK(!std::isinf(epsilon));
  ZETASQL_RET_CHECK(!std::isinf(delta));
  ZETASQL_RET_CHECK_GT(epsilon, 0);
  ZETASQL_RET_CHECK_GE(delta, 0);
  ZETASQL_RET_CHECK_LE(delta, 1);
  ZETASQL_RET_CHECK_GT(kappa, 0);

  ZETASQL_ASSIGN_OR_RETURN(
      double double_k_threshold,
      differential_privacy::LaplacePartitionSelection::CalculateThreshold(
          epsilon, delta, kappa));

  double_k_threshold = ceil(double_k_threshold);
  int64_t k_threshold;
  if (double_k_threshold >= std::numeric_limits<int64_t>::max()) {
    k_threshold = std::numeric_limits<int64_t>::max();
  } else if (double_k_threshold <= std::numeric_limits<int64_t>::lowest()) {
    k_threshold = std::numeric_limits<int64_t>::lowest();
  } else {
    k_threshold = static_cast<int64_t>(double_k_threshold);
  }

  return Value::Int64(k_threshold);
}

absl::StatusOr<Value> ComputeDeltaFromEpsilonKThresholdKappa(
    Value epsilon_value, Value k_threshold_value,
    Value kappa_value) {
  if (!kappa_value.is_valid()) {
    // Invalid kappa indicates unset.  Unset kappa implies contributions are not
    // being bounded across GROUP BY groups.  Setting kappa to 1 is equivalent
    // to removing it from the delta calculation.
    kappa_value = Value::Int64(1);
  }
  ZETASQL_RET_CHECK_EQ(epsilon_value.type_kind(), TYPE_DOUBLE);
  ZETASQL_RET_CHECK_EQ(k_threshold_value.type_kind(), TYPE_INT64);
  ZETASQL_RET_CHECK_EQ(kappa_value.type_kind(), TYPE_INT64);

  const double epsilon = epsilon_value.double_value();
  const double k_threshold = k_threshold_value.int64_value();
  const int64_t kappa = kappa_value.int64_value();

  // The caller should have verified that epsilon is not infinity or nan.
  // The caller should have also verified that epsilon is greater than 0.
  // Note that k_threshold is allowed to be negative.  Kappa must also be
  // greater than 0.
  ZETASQL_RET_CHECK(!std::isnan(epsilon));
  ZETASQL_RET_CHECK(!std::isinf(epsilon));
  ZETASQL_RET_CHECK_GT(epsilon, 0);
  ZETASQL_RET_CHECK_GT(kappa, 0);

  ZETASQL_ASSIGN_OR_RETURN(
      double delta,
      differential_privacy::LaplacePartitionSelection::CalculateDelta(
          epsilon, k_threshold, kappa));

  if (std::isnan(delta) || delta < 0.0 || delta > 1.0) {
    ZETASQL_RET_CHECK_FAIL() << "Invalid computed delta; delta: " << delta
      << ", epsilon: " << epsilon << ", k_threshold: " << k_threshold
      << ", kappa: " << kappa;
  }

  return Value::Double(delta);
}

}  // namespace anonymization
}  // namespace zetasql
