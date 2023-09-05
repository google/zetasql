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

#include "zetasql/public/functions/distance.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/math.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/value.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "zetasql/base/edit_distance.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {
namespace functions {
namespace {

using ::zetasql::functions::Add;
using ::zetasql::functions::Divide;
using ::zetasql::functions::Multiply;
using ::zetasql::functions::Sqrt;
using ::zetasql::functions::Subtract;

absl::Status Apply(absl::FunctionRef<bool(double in1, double in2, double* out,
                                          absl::Status* error)>
                       f,
                   double in1, double in2, double* out) {
  absl::Status status;
  if (!f(in1, in2, out, &status)) {
    return status;
  }
  return absl::OkStatus();
}

absl::Status Apply(
    absl::FunctionRef<bool(double in, double* out, absl::Status* error)> f,
    double in, double* out) {
  absl::Status status;
  if (!f(in, out, &status)) {
    return status;
  }
  return absl::OkStatus();
}

// Populates a sparse vector content into its representation as a map and a list
// of all non-zero indices.
template <typename IdxType>
absl::Status PopulateSparseInput(const std::vector<Value>& input_array,
                                 absl::flat_hash_map<IdxType, double>& map,
                                 absl::btree_set<IdxType>& all_indices) {
  map.reserve(input_array.size());
  absl::Status status;
  for (const Value& element : input_array) {
    if (element.is_null()) {
      return absl::InvalidArgumentError("NULL array element");
    }
    if (element.field(0).is_null() || element.field(1).is_null()) {
      return absl::InvalidArgumentError("NULL struct field");
    }
    IdxType index;
    if constexpr (std::is_same_v<IdxType, std::string>) {
      index = element.field(0).string_value();
    } else {
      index = element.field(0).Get<IdxType>();
    }
    double value = element.field(1).Get<double>();
    if (!map.emplace(index, value).second) {
      return absl::InvalidArgumentError(absl::Substitute(
          "Duplicate index $0 found in the input array.", index));
    }
    all_indices.emplace(index);
  }
  return absl::OkStatus();
}

// Gets the value of a dimension of a sparse vector, or zero if the dimension is
// not populated.
template <typename IdxType>
double GetValueOrZero(const absl::flat_hash_map<IdxType, double>& map,
                      IdxType index) {
  auto iter = map.find(index);
  if (iter != map.end()) {
    return iter->second;
  }
  return 0.0;
}

absl::StatusOr<Value> ComputeCosineDistance(
    absl::FunctionRef<
        absl::StatusOr<std::optional<std::pair<double, double>>>()>
        array_elements_supplier) {
  double numerator = 0;
  double len_a = 0;
  double len_b = 0;
  while (true) {
    std::optional<std::pair<double, double>> paired_elements;
    ZETASQL_ASSIGN_OR_RETURN(paired_elements, array_elements_supplier());
    if (!paired_elements.has_value()) {
      break;
    }
    double a = paired_elements->first;
    double b = paired_elements->second;

    double mult = 0;
    ZETASQL_RETURN_IF_ERROR(Apply(Multiply<double>, a, b, &mult));
    ZETASQL_RETURN_IF_ERROR(Apply(Add<double>, numerator, mult, &numerator));

    double a_square;
    ZETASQL_RETURN_IF_ERROR(Apply(Multiply<double>, a, a, &a_square));
    ZETASQL_RETURN_IF_ERROR(Apply(Add<double>, len_a, a_square, &len_a));
    double b_square;
    ZETASQL_RETURN_IF_ERROR(Apply(Multiply<double>, b, b, &b_square));
    ZETASQL_RETURN_IF_ERROR(Apply(Add<double>, len_b, b_square, &len_b));
  }
  if (len_a == 0 || len_b == 0) {
    return absl::InvalidArgumentError(
        "Cannot compute cosine distance against zero vector.");
  }

  double sqrt_len_a;
  ZETASQL_RETURN_IF_ERROR(Apply(Sqrt<double>, len_a, &sqrt_len_a));
  double sqrt_len_b;
  ZETASQL_RETURN_IF_ERROR(Apply(Sqrt<double>, len_b, &sqrt_len_b));

  double denominator;
  ZETASQL_RETURN_IF_ERROR(
      Apply(Multiply<double>, sqrt_len_a, sqrt_len_b, &denominator));
  double result;
  ZETASQL_RETURN_IF_ERROR(Apply(Divide<double>, numerator, denominator, &result));
  ZETASQL_RETURN_IF_ERROR(Apply(Subtract<double>, 1.0, result, &result));

  return Value::Double(result);
}

template <typename IdxType>
absl::StatusOr<Value> ComputeCosineDistanceFunctionSparse(const Value vector1,
                                                          const Value vector2) {
  absl::flat_hash_map<IdxType, double> input_map_a;
  absl::flat_hash_map<IdxType, double> input_map_b;
  absl::btree_set<IdxType> all_indices;
  ZETASQL_RETURN_IF_ERROR(
      PopulateSparseInput(vector1.elements(), input_map_a, all_indices));
  ZETASQL_RETURN_IF_ERROR(
      PopulateSparseInput(vector2.elements(), input_map_b, all_indices));

  auto iter = all_indices.begin();
  auto array_elements_supplier =
      [&input_map_a, &input_map_b, &all_indices,
       &iter]() -> absl::StatusOr<std::optional<std::pair<double, double>>> {
    if (iter != all_indices.end()) {
      auto pair = std::make_pair(GetValueOrZero(input_map_a, *iter),
                                 GetValueOrZero(input_map_b, *iter));
      ++iter;
      return pair;
    } else {
      return std::nullopt;
    }
  };
  return ComputeCosineDistance(array_elements_supplier);
}

absl::StatusOr<Value> ComputeEuclideanDistance(
    absl::FunctionRef<
        absl::StatusOr<std::optional<std::pair<double, double>>>()>
        array_elements_supplier) {
  double result = 0;
  while (true) {
    std::optional<std::pair<double, double>> paired_elements;
    ZETASQL_ASSIGN_OR_RETURN(paired_elements, array_elements_supplier());
    if (!paired_elements.has_value()) {
      break;
    }
    double a = paired_elements.value().first;
    double b = paired_elements.value().second;

    double c;
    ZETASQL_RETURN_IF_ERROR(Apply(Subtract<double>, a, b, &c));
    ZETASQL_RETURN_IF_ERROR(Apply(Multiply<double>, c, c, &c));
    ZETASQL_RETURN_IF_ERROR(Apply(Add<double>, result, c, &result));
  }
  ZETASQL_RETURN_IF_ERROR(Apply(Sqrt<double>, result, &result));

  return Value::Double(result);
}

template <typename IdxType>
absl::StatusOr<Value> ComputeEuclideanDistanceFunctionSparse(
    const Value vector1, const Value vector2) {
  absl::flat_hash_map<IdxType, double> input_map_a;
  absl::flat_hash_map<IdxType, double> input_map_b;
  absl::btree_set<IdxType> all_indices;
  ZETASQL_RETURN_IF_ERROR(
      PopulateSparseInput(vector1.elements(), input_map_a, all_indices));
  ZETASQL_RETURN_IF_ERROR(
      PopulateSparseInput(vector2.elements(), input_map_b, all_indices));
  auto iter = all_indices.begin();
  auto array_elements_supplier =
      [&input_map_a, &input_map_b, &all_indices,
       &iter]() -> std::optional<std::pair<double, double>> {
    if (iter != all_indices.end()) {
      auto pair = std::make_pair(GetValueOrZero(input_map_a, *iter),
                                 GetValueOrZero(input_map_b, *iter));
      ++iter;
      return pair;
    } else {
      return std::nullopt;
    }
  };

  return ComputeEuclideanDistance(array_elements_supplier);
}

}  // namespace

absl::StatusOr<Value> CosineDistanceDense(Value vector1, Value vector2) {
  if (vector1.num_elements() != vector2.num_elements()) {
    return absl::InvalidArgumentError(
        absl::Substitute("Array length mismatch: $0 and $1.",
                         vector1.num_elements(), vector2.num_elements()));
  }

  int i = 0;
  auto array_elements_supplier =
      [&vector1, &vector2,
       &i]() -> absl::StatusOr<std::optional<std::pair<double, double>>> {
    if (i < vector1.num_elements()) {
      if (vector1.element(i).is_null() || vector2.element(i).is_null()) {
        return absl::InvalidArgumentError("NULL array element");
      }
      auto pair = std::make_pair(vector1.element(i).double_value(),
                                 vector2.element(i).double_value());
      ++i;
      return pair;
    } else {
      return std::nullopt;
    }
  };
  return ComputeCosineDistance(array_elements_supplier);
}

absl::StatusOr<Value> CosineDistanceSparseInt64Key(Value vector1,
                                                   Value vector2) {
  return ComputeCosineDistanceFunctionSparse<int64_t>(vector1, vector2);
}

absl::StatusOr<Value> CosineDistanceSparseStringKey(Value vector1,
                                                    Value vector2) {
  return ComputeCosineDistanceFunctionSparse<std::string>(vector1, vector2);
}

absl::StatusOr<Value> EuclideanDistanceDense(Value vector1, Value vector2) {
  if (vector1.num_elements() != vector2.num_elements()) {
    return absl::InvalidArgumentError(
        absl::Substitute("Array length mismatch: $0 and $1.",
                         vector1.num_elements(), vector2.num_elements()));
  }
  int i = 0;
  auto array_elements_supplier =
      [&vector1, &vector2,
       &i]() -> absl::StatusOr<std::optional<std::pair<double, double>>> {
    if (i < vector1.num_elements()) {
      if (vector1.element(i).is_null() || vector2.element(i).is_null()) {
        return absl::InvalidArgumentError("NULL array element");
      }
      auto pair = std::make_pair(vector1.element(i).double_value(),
                                 vector2.element(i).double_value());
      ++i;
      return pair;
    } else {
      return std::nullopt;
    }
  };

  return ComputeEuclideanDistance(array_elements_supplier);
}

absl::StatusOr<Value> EuclideanDistanceSparseInt64Key(Value vector1,
                                                      Value vector2) {
  return ComputeEuclideanDistanceFunctionSparse<int64_t>(vector1, vector2);
}

absl::StatusOr<Value> EuclideanDistanceSparseStringKey(Value vector1,
                                                       Value vector2) {
  return ComputeEuclideanDistanceFunctionSparse<std::string>(vector1, vector2);
}

absl::StatusOr<Value> EditDistance(Value v1, Value v2,
                                   std::optional<Value> max_distance_value) {
  absl::string_view s0 =
      v1.type()->IsBytes() ? v1.bytes_value() : v1.string_value();
  absl::string_view s1 =
      v2.type()->IsBytes() ? v2.bytes_value() : v2.string_value();

  int64_t max_distance = max_distance_value.has_value()
                             ? max_distance_value.value().int64_value()
                             : std::max(s0.size(), s1.size());
  if (max_distance < 0) {
    return absl::InvalidArgumentError("max_distance must be non-negative");
  }
  const int64_t max_possible_distance = std::max(s0.size(), s1.size());
  if (max_distance > max_possible_distance) {
    max_distance = max_possible_distance;
  }

  int64_t result = zetasql_base::CappedLevenshteinDistance(
      s0.begin(), s0.end(), s1.begin(), s1.end(), std::equal_to<char>(),
      static_cast<int>(max_distance));

  return Value::Int64(result);
}

}  // namespace functions
}  // namespace zetasql
