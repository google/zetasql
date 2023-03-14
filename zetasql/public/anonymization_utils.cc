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

#include <cmath>
#include <cstdint>
#include <limits>

#include "zetasql/public/type.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "algorithms/partition-selection.h"

namespace zetasql {
namespace anonymization {

absl::StatusOr<Value> ComputeLaplaceThresholdFromDelta(
    Value epsilon_value, Value delta_value,
    Value max_groups_contributed_value) {
  if (!max_groups_contributed_value.is_valid()) {
    // Invalid max_groups_contributed_value indicates unset.  Unset
    // max_groups_contributed_value implies contributions are not being bounded
    // across GROUP BY groups.  Setting max_groups_contributed_value to 1 is
    // equivalent to removing it from the threshold calculation.
    max_groups_contributed_value = Value::Int64(1);
  }
  ZETASQL_RET_CHECK_EQ(epsilon_value.type_kind(), TYPE_DOUBLE);
  ZETASQL_RET_CHECK_EQ(delta_value.type_kind(), TYPE_DOUBLE);
  ZETASQL_RET_CHECK_EQ(max_groups_contributed_value.type_kind(), TYPE_INT64);

  const double epsilon = epsilon_value.double_value();
  const double delta = delta_value.double_value();
  const int64_t max_groups_contributed =
      max_groups_contributed_value.int64_value();

  // The caller should have verified that epsilon and delta are not infinity
  // or nan.  The caller should also have verified that epsilon > 0, and that
  // delta is in the range [0, 1]. max_groups_contributed must also be > 0.
  ZETASQL_RET_CHECK(!std::isnan(epsilon));
  ZETASQL_RET_CHECK(!std::isnan(delta));
  ZETASQL_RET_CHECK(!std::isinf(epsilon));
  ZETASQL_RET_CHECK(!std::isinf(delta));
  ZETASQL_RET_CHECK_GT(epsilon, 0);
  ZETASQL_RET_CHECK_GE(delta, 0);
  ZETASQL_RET_CHECK_LE(delta, 1);
  ZETASQL_RET_CHECK_GT(max_groups_contributed, 0);

  ZETASQL_ASSIGN_OR_RETURN(
      double double_laplace_threshold,
      differential_privacy::LaplacePartitionSelection::CalculateThreshold(
          epsilon, delta, max_groups_contributed));

  double_laplace_threshold = ceil(double_laplace_threshold);
  int64_t laplace_threshold;
  if (double_laplace_threshold >= std::numeric_limits<int64_t>::max()) {
    laplace_threshold = std::numeric_limits<int64_t>::max();
  } else if (double_laplace_threshold <=
             std::numeric_limits<int64_t>::lowest()) {
    laplace_threshold = std::numeric_limits<int64_t>::lowest();
  } else {
    laplace_threshold = static_cast<int64_t>(double_laplace_threshold);
  }

  return Value::Int64(laplace_threshold);
}

absl::StatusOr<Value> ComputeDeltaFromLaplaceThreshold(
    Value epsilon_value, Value laplace_threshold_value,
    Value max_groups_contributed_value) {
  if (!max_groups_contributed_value.is_valid()) {
    // Invalid max_groups_contributed_value indicates unset.  Unset
    // max_groups_contributed_value implies contributions are not being bounded
    // across GROUP BY groups.  Setting max_groups_contributed_value to 1 is
    // equivalent to removing it from the delta calculation.
    max_groups_contributed_value = Value::Int64(1);
  }
  ZETASQL_RET_CHECK_EQ(epsilon_value.type_kind(), TYPE_DOUBLE);
  ZETASQL_RET_CHECK_EQ(laplace_threshold_value.type_kind(), TYPE_INT64);
  ZETASQL_RET_CHECK_EQ(max_groups_contributed_value.type_kind(), TYPE_INT64);

  const double epsilon = epsilon_value.double_value();
  const double laplace_threshold = laplace_threshold_value.int64_value();
  const int64_t max_groups_contributed =
      max_groups_contributed_value.int64_value();

  // The caller should have verified that epsilon is not infinity or nan.
  // The caller should have also verified that epsilon is greater than 0.
  // Note that laplace_threshold is allowed to be negative.
  // max_groups_contributed must also be greater than 0.
  ZETASQL_RET_CHECK(!std::isnan(epsilon));
  ZETASQL_RET_CHECK(!std::isinf(epsilon));
  ZETASQL_RET_CHECK_GT(epsilon, 0);
  ZETASQL_RET_CHECK_GT(max_groups_contributed, 0);

  ZETASQL_ASSIGN_OR_RETURN(
      double delta,
      differential_privacy::LaplacePartitionSelection::CalculateDelta(
          epsilon, laplace_threshold, max_groups_contributed));

  if (std::isnan(delta) || delta < 0.0 || delta > 1.0) {
    ZETASQL_RET_CHECK_FAIL() << "Invalid computed delta; delta: " << delta
                     << ", epsilon: " << epsilon
                     << ", laplace_threshold: " << laplace_threshold
                     << ", max_groups_contributed: " << max_groups_contributed;
  }

  return Value::Double(delta);
}

}  // namespace anonymization
}  // namespace zetasql
