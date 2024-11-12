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
#include <memory>
#include <optional>

#include "zetasql/public/function_signature.h"
#include "zetasql/public/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "algorithms/partition-selection.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace anonymization {
namespace {
constexpr absl::string_view kArgumentNameEpsilon = "epsilon";
}  // namespace

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

namespace {

std::optional<int> GetEpsilonArgumentIndex(const FunctionSignature& signature) {
  int index = 0;
  for (const FunctionArgumentType& arg : signature.arguments()) {
    if (arg.has_argument_name() &&
        zetasql_base::CaseEqual(arg.argument_name(), kArgumentNameEpsilon)) {
      return index;
    }
    ++index;
  }
  return std::nullopt;
}

const ResolvedExpr* GetEpsilonArgumentExpr(
    const ResolvedFunctionCallBase* call) {
  std::optional epsilon_index = GetEpsilonArgumentIndex(call->signature());
  if (!epsilon_index.has_value()) {
    return nullptr;
  }
  return call->argument_list(epsilon_index.value());
}

class FunctionEpsilonAssignerImpl : public FunctionEpsilonAssigner {
 public:
  explicit FunctionEpsilonAssignerImpl(double split_remainder_epsilon)
      : split_remainder_epsilon_(split_remainder_epsilon) {}

  absl::StatusOr<double> GetEpsilonForFunction(
      const ResolvedFunctionCallBase* function_call) override {
    const ResolvedExpr* epsilon_expr = GetEpsilonArgumentExpr(function_call);
    if (epsilon_expr == nullptr) {
      return split_remainder_epsilon_;
    }
    ZETASQL_RET_CHECK_EQ(epsilon_expr->node_kind(), RESOLVED_LITERAL);
    const ResolvedLiteral* epsilon_literal =
        epsilon_expr->GetAs<ResolvedLiteral>();
    if (epsilon_literal->value().is_null()) {
      return split_remainder_epsilon_;
    }
    return epsilon_literal->value().double_value();
  }

 private:
  // The equally split epsilon value from the scan options, divided by the
  // number of aggregate functions having a null `epsilon` named argument or do
  // not support such a named argument.
  const double split_remainder_epsilon_;
};

const ResolvedExpr* GetValueOfOptionOrNull(
    absl::Span<const std::unique_ptr<const ResolvedOption>> option_list,
    absl::string_view option_name) {
  for (const std::unique_ptr<const ResolvedOption>& option : option_list) {
    if (zetasql_base::CaseEqual(option->name(), option_name)) {
      return option->value();
    }
  }
  return nullptr;
}

absl::StatusOr<bool> HasNonNullMinPrivacyUnitsPerGroupOption(
    const ResolvedDifferentialPrivacyAggregateScan* scan) {
  const ResolvedExpr* min_privacy_units_per_group = GetValueOfOptionOrNull(
      scan->option_list(), "min_privacy_units_per_group");
  if (min_privacy_units_per_group == nullptr) {
    return false;
  }
  ZETASQL_RET_CHECK_EQ(min_privacy_units_per_group->node_kind(), RESOLVED_LITERAL);
  return !min_privacy_units_per_group->GetAs<ResolvedLiteral>()
              ->value()
              .is_null();
}

absl::StatusOr<int> GetNumFunctionsForEvenSplitting(
    const ResolvedDifferentialPrivacyAggregateScan* scan) {
  int num_functions_even_splitting = 0;
  for (const std::unique_ptr<const ResolvedComputedColumnBase>& aggregate :
       scan->aggregate_list()) {
    const ResolvedFunctionCallBase* call =
        aggregate->expr()->GetAs<ResolvedFunctionCallBase>();
    const ResolvedExpr* epsilon_expr = GetEpsilonArgumentExpr(call);
    if (epsilon_expr == nullptr ||
        epsilon_expr->GetAs<ResolvedLiteral>()->value().is_null()) {
      ++num_functions_even_splitting;
    }
  }

  // If the `min_privacy_units_per_group` option is set, then the rewriter added
  // a column to the aggregate list counting the exact number of distinct users
  // per group/partition. Thus, in that case, the `aggregate_list` contains
  // an additional item without a parameter named `epsilon`.
  ZETASQL_ASSIGN_OR_RETURN(bool has_non_null_min_privacy_units_per_group_option,
                   HasNonNullMinPrivacyUnitsPerGroupOption(scan));
  if (has_non_null_min_privacy_units_per_group_option) {
    return num_functions_even_splitting - 1;
  }

  return num_functions_even_splitting;
}

absl::StatusOr<double> GetRemainingEpsilonForEvenSplitting(
    const ResolvedDifferentialPrivacyAggregateScan* scan) {
  const ResolvedExpr* epsilon_expr =
      GetValueOfOptionOrNull(scan->option_list(), kArgumentNameEpsilon);
  if (epsilon_expr == nullptr) {
    return 0;
  }

  ZETASQL_RET_CHECK_EQ(epsilon_expr->node_kind(), RESOLVED_LITERAL);
  double remaining_epsilon =
      epsilon_expr->GetAs<ResolvedLiteral>()->value().double_value();

  for (const std::unique_ptr<const ResolvedComputedColumnBase>& aggregate :
       scan->aggregate_list()) {
    const ResolvedExpr* epsilon_expr = GetEpsilonArgumentExpr(
        aggregate->expr()->GetAs<ResolvedFunctionCallBase>());
    if (epsilon_expr == nullptr) {
      continue;
    }
    ZETASQL_RET_CHECK_EQ(epsilon_expr->node_kind(), RESOLVED_LITERAL);
    const Value& epsilon_value =
        epsilon_expr->GetAs<ResolvedLiteral>()->value();
    if (!epsilon_value.is_null()) {
      remaining_epsilon -= epsilon_value.double_value();
    }
  }
  return remaining_epsilon;
}

}  // namespace

absl::StatusOr<std::unique_ptr<FunctionEpsilonAssigner>>
FunctionEpsilonAssigner::CreateFromScan(
    const ResolvedDifferentialPrivacyAggregateScan* scan) {
  ZETASQL_ASSIGN_OR_RETURN(const int num_functions_even_splitting,
                   GetNumFunctionsForEvenSplitting(scan));
  if (num_functions_even_splitting == 0) {
    return std::make_unique<FunctionEpsilonAssignerImpl>(0);
  }
  ZETASQL_ASSIGN_OR_RETURN(const double remaining_epsilon,
                   GetRemainingEpsilonForEvenSplitting(scan));
  if (remaining_epsilon + std::abs(remaining_epsilon) * 1e-8 < 0) {
    return absl::InvalidArgumentError(
        "Epsilon overconsumption: the sum of all `epsilon` arguments cannot "
        "exceed the `epsilon` option in SELECT WITH DIFFERENTIAL_PRIVACY "
        "scans");
  }
  return std::make_unique<FunctionEpsilonAssignerImpl>(
      remaining_epsilon / num_functions_even_splitting);
}

}  // namespace anonymization
}  // namespace zetasql
