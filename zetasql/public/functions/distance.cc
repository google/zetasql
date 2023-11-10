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
#include <type_traits>
#include <utility>
#include <vector>

#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/math.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "unicode/umachine.h"
#include "unicode/utf8.h"
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

template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
absl::StatusOr<Value> ComputeCosineDistance(
    absl::FunctionRef<absl::StatusOr<std::optional<std::pair<T, T>>>()>
        array_elements_supplier) {
  double numerator = 0;
  double len_a = 0;
  double len_b = 0;
  while (true) {
    std::optional<std::pair<T, T>> paired_elements;
    ZETASQL_ASSIGN_OR_RETURN(paired_elements, array_elements_supplier());
    if (!paired_elements.has_value()) {
      break;
    }
    double a = (double)paired_elements->first;
    double b = (double)paired_elements->second;

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
  return ComputeCosineDistance<double>(array_elements_supplier);
}

template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
absl::StatusOr<Value> ComputeEuclideanDistance(
    absl::FunctionRef<absl::StatusOr<std::optional<std::pair<T, T>>>()>
        array_elements_supplier) {
  double result = 0;
  while (true) {
    std::optional<std::pair<T, T>> paired_elements;
    ZETASQL_ASSIGN_OR_RETURN(paired_elements, array_elements_supplier());
    if (!paired_elements.has_value()) {
      break;
    }
    double a = (double)paired_elements.value().first;
    double b = (double)paired_elements.value().second;

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

  return ComputeEuclideanDistance<double>(array_elements_supplier);
}

}  // namespace

template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
std::function<absl::StatusOr<std::optional<std::pair<T, T>>>()>
MakeZippedArrayElementsSupplier(const std::vector<Value>& vector1,
                                const std::vector<Value>& vector2) {
  return [v1_it = vector1.begin(), v1_end = vector1.end(),
          v2_it = vector2.begin(), v2_end = vector2.end()]() mutable
         -> absl::StatusOr<std::optional<std::pair<T, T>>> {
    if (v1_it == v1_end || v2_it == v2_end) {
      return std::nullopt;
    }

    if (v1_it->is_null() || v2_it->is_null()) {
      return absl::InvalidArgumentError("NULL array element");
    }

    auto pair = std::make_pair(v1_it->Get<T>(), v2_it->Get<T>());
    v1_it++;
    v2_it++;
    return pair;
  };
}

absl::StatusOr<Value> CosineDistanceDense(Value vector1, Value vector2) {
  if (vector1.num_elements() != vector2.num_elements()) {
    return absl::InvalidArgumentError(
        absl::Substitute("Array length mismatch: $0 and $1.",
                         vector1.num_elements(), vector2.num_elements()));
  }
  if (vector1.type()->AsArray()->element_type() == types::DoubleType()) {
    return ComputeCosineDistance<double>(
        MakeZippedArrayElementsSupplier<double>(vector1.elements(),
                                                vector2.elements()));
  } else {
    return ComputeCosineDistance<float>(MakeZippedArrayElementsSupplier<float>(
        vector1.elements(), vector2.elements()));
  }
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

  if (vector1.type()->AsArray()->element_type() == types::DoubleType()) {
    return ComputeEuclideanDistance<double>(
        MakeZippedArrayElementsSupplier<double>(vector1.elements(),
                                                vector2.elements()));
  } else {
    return ComputeEuclideanDistance<float>(
        MakeZippedArrayElementsSupplier<float>(vector1.elements(),
                                               vector2.elements()));
  }
}

absl::StatusOr<Value> EuclideanDistanceSparseInt64Key(Value vector1,
                                                      Value vector2) {
  return ComputeEuclideanDistanceFunctionSparse<int64_t>(vector1, vector2);
}

absl::StatusOr<Value> EuclideanDistanceSparseStringKey(Value vector1,
                                                       Value vector2) {
  return ComputeEuclideanDistanceFunctionSparse<std::string>(vector1, vector2);
}

absl::StatusOr<std::vector<char32_t>> GetUtf8CodePoints(absl::string_view s) {
  std::vector<char32_t> result;
  int32_t offset = 0;
  while (offset < s.size()) {
    UChar32 character;
    U8_NEXT(s, offset, s.size(), character);
    if (character < 0) {
      return absl::InvalidArgumentError("invalid UTF8 string");
    }
    result.push_back(character);
  }

  return result;
}

absl::StatusOr<int64_t> EditDistance(
    absl::string_view s0, absl::string_view s1,
    std::optional<int64_t> max_distance_value) {
  int64_t max_distance = max_distance_value.has_value()
                             ? max_distance_value.value()
                             : std::max(s0.size(), s1.size());
  if (max_distance < 0) {
    return absl::InvalidArgumentError("max_distance must be non-negative");
  }
  const int64_t max_possible_distance = std::max(s0.size(), s1.size());
  if (max_distance > max_possible_distance) {
    max_distance = max_possible_distance;
  }

  ZETASQL_ASSIGN_OR_RETURN(std::vector<char32_t> code_points0, GetUtf8CodePoints(s0));
  ZETASQL_ASSIGN_OR_RETURN(std::vector<char32_t> code_points1, GetUtf8CodePoints(s1));

  int64_t result = zetasql_base::CappedLevenshteinDistance(
      code_points0.begin(), code_points0.end(), code_points1.begin(),
      code_points1.end(), std::equal_to<char32_t>(),
      static_cast<int>(max_distance));

  return result;
}

absl::StatusOr<int64_t> EditDistanceBytes(
    absl::string_view s0, absl::string_view s1,
    std::optional<int64_t> max_distance_value) {
  int64_t max_distance = max_distance_value.has_value()
                             ? max_distance_value.value()
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

  return result;
}

}  // namespace functions
}  // namespace zetasql
