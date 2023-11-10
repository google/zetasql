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

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "zetasql/common/float_margin.h"
#include "zetasql/compliance/functions_testlib.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/testing/test_function.h"
#include "zetasql/testing/using_test_value.cc"  // NOLINT
#include "zetasql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace zetasql {
namespace {

struct Int64DoubleKeyValuePair {
  std::optional<int64_t> key = 0;
  std::optional<double> value = 0.0;
};

struct StringDoubleKeyValuePair {
  std::optional<std::string> key;
  std::optional<double> value = 0.0;
};

Value ValueOrDie(absl::StatusOr<Value>& s) {
  ZETASQL_CHECK_OK(s.status());
  return s.value();
}

template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
Value MakeArray(std::vector<T> arr, bool is_null = false,
                bool ends_with_null = false) {
  if (is_null) {
    if constexpr (std::is_same_v<T, float>) {
      return Value::Null(types::FloatArrayType());
    }
    return Value::Null(types::DoubleArrayType());
  }
  std::vector<Value> values;
  values.reserve(arr.size());
  for (const auto& v : arr) {
    values.push_back(Value::Make<T>(v));
  }
  if (ends_with_null) {
    values.push_back(Value::MakeNull<T>());
  }
  absl::StatusOr<Value> array;
  if constexpr (std::is_same_v<T, float>) {
    array = Value::MakeArray(types::FloatArrayType(), values);
  } else {
    array = Value::MakeArray(types::DoubleArrayType(), values);
  }
  return ValueOrDie(array);
}

template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
Value MakeRepeatedArray(T value, int64_t repeated_count) {
  std::vector<T> values;
  values.reserve(repeated_count);
  for (int64_t i = 0; i < repeated_count; ++i) {
    values.push_back(value);
  }
  return MakeArray<T>(values);
}

template <typename T, typename = std::enable_if_t<std::is_floating_point_v<T>>>
Value MakeArrayEndingWithNull(std::vector<T> arr) {
  return MakeArray<T>(arr, /*is_null=*/false, /*ends_with_null=*/true);
}

Value MakeNullDoubleArray() { return MakeArray<double>({}, /*is_null=*/true); }
Value MakeNullFloatArray() { return MakeArray<float>({}, /*is_null=*/true); }

Value MakeArray(std::vector<Int64DoubleKeyValuePair> arr, bool is_null = false,
                bool ends_with_null = false,
                std::string struct_field_key_name = "key",
                std::string struct_field_value_name = "value") {
  TypeFactory type_factory;
  std::vector<StructType::StructField> struct_fields;
  struct_fields.push_back(
      StructType::StructField(struct_field_key_name, type_factory.get_int64()));
  struct_fields.push_back(StructType::StructField(struct_field_value_name,
                                                  type_factory.get_double()));
  const StructType* struct_type = nullptr;

  auto struct_type_status =
      type_factory.MakeStructType(struct_fields, &struct_type);
  ZETASQL_DCHECK_OK(struct_type_status);

  std::vector<Value> values;
  values.reserve(arr.size());
  for (const auto& v : arr) {
    auto struct_status = Value::MakeStruct(
        struct_type, {v.key.has_value() ? Value::Int64(v.key.value())
                                        : Value::Null(Int64Type()),
                      v.value.has_value() ? Value::Double(v.value.value())
                                          : Value::Null(types::DoubleType())});
    values.push_back(ValueOrDie(struct_status));
  }
  if (ends_with_null) {
    values.push_back(Value::Null(struct_type));
  }
  const ArrayType* array_type;
  auto array_type_status = type_factory.MakeArrayType(struct_type, &array_type);
  ZETASQL_DCHECK_OK(array_type_status);
  if (is_null) {
    return Value::Null(array_type);
  }
  auto status = Value::MakeArray(array_type, values);
  return ValueOrDie(status);
}

Value MakeArrayEndingWithNull(std::vector<Int64DoubleKeyValuePair> arr) {
  return MakeArray(arr, /*is_null=*/false, /*ends_with_null=*/true);
}

Value MakeArrayCustomStructFieldNames(std::vector<Int64DoubleKeyValuePair> arr,
                                      std::string key_field_name,
                                      std::string value_field_name) {
  return MakeArray(arr, /*is_null=*/false, /*ends_with_null=*/false,
                   key_field_name, value_field_name);
}

Value MakeNullInt64KeyArray() {
  return MakeArray(std::vector<Int64DoubleKeyValuePair>{}, /*is_null=*/true);
}

Value MakeArray(std::vector<StringDoubleKeyValuePair> arr, bool is_null = false,
                bool ends_with_null = false,
                std::string struct_field_key_name = "key",
                std::string struct_field_value_name = "value") {
  TypeFactory type_factory;
  std::vector<StructType::StructField> struct_fields;
  struct_fields.push_back(StructType::StructField(struct_field_key_name,
                                                  type_factory.get_string()));
  struct_fields.push_back(StructType::StructField(struct_field_value_name,
                                                  type_factory.get_double()));
  const StructType* struct_type = nullptr;

  auto struct_type_status =
      type_factory.MakeStructType(struct_fields, &struct_type);
  ZETASQL_DCHECK_OK(struct_type_status);

  std::vector<Value> values;
  values.reserve(arr.size());
  for (const auto& v : arr) {
    auto status = Value::MakeStruct(
        struct_type, {v.key.has_value() ? Value::StringValue(v.key.value())
                                        : Value::Null(StringType()),
                      v.value.has_value() ? Value::Double(v.value.value())
                                          : Value::Null(types::DoubleType())});
    values.push_back(ValueOrDie(status));
  }
  if (ends_with_null) {
    values.push_back(Value::Null(struct_type));
  }
  const ArrayType* array_type;
  auto array_type_status = type_factory.MakeArrayType(struct_type, &array_type);
  ZETASQL_DCHECK_OK(array_type_status);
  if (is_null) {
    return Value::Null(array_type);
  }
  auto status = Value::MakeArray(array_type, values);
  return ValueOrDie(status);
}

Value MakeArrayEndingWithNull(std::vector<StringDoubleKeyValuePair> arr) {
  return MakeArray(arr, /*is_null=*/false, /*ends_with_null=*/true);
}

Value MakeArrayCustomStructFieldNames(std::vector<StringDoubleKeyValuePair> arr,
                                      std::string key_field_name,
                                      std::string value_field_name) {
  return MakeArray(arr, /*is_null=*/false, /*ends_with_null=*/false,
                   key_field_name, value_field_name);
}

Value MakeNullStringKeyArray() {
  return MakeArray(std::vector<StringDoubleKeyValuePair>{}, /*is_null=*/true);
}

static constexpr FloatMargin kDistanceFloatMargin = FloatMargin::UlpMargin(12);

}  // namespace

std::vector<FunctionTestCall> GetFunctionTestsCosineDistance() {
  std::vector<FunctionTestCall> tests = {
      // NULL inputs
      {"cosine_distance",
       {MakeNullDoubleArray(), MakeArray<double>({3.0, 4.0})},
       values::NullDouble()},
      {"cosine_distance",
       {MakeArray<double>({5.0, 6.0}), MakeNullDoubleArray()},
       values::NullDouble()},
      {"cosine_distance",
       {MakeNullDoubleArray(), MakeNullDoubleArray()},
       values::NullDouble()},
      {"cosine_distance",
       {MakeArray<double>({5.0, 6.0}),
        MakeArrayEndingWithNull(std::vector<double>{3.0})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL array element.")},

      {"cosine_distance",
       {MakeArray({{1, 1.0}, {2, 2.0}}), MakeNullInt64KeyArray()},
       values::NullDouble()},
      {"cosine_distance",
       {MakeNullInt64KeyArray(), MakeArray({{1, 5.0}, {2, 6.0}})},
       values::NullDouble()},
      {"cosine_distance",
       {MakeNullInt64KeyArray(), MakeNullInt64KeyArray()},
       values::NullDouble()},
      {"cosine_distance",
       {MakeArray({{1, 5.0}, {2, 6.0}}),
        MakeArrayEndingWithNull(
            std::vector<Int64DoubleKeyValuePair>{{3, 5.0}})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL array element.")},
      {"cosine_distance",
       {MakeArray({{1, 5.0}, {2, 6.0}}),
        MakeArray({{1, 1.0}, {std::nullopt, 6.0}})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL struct field.")},
      {"cosine_distance",
       {MakeArray({{1, 5.0}, {2, 6.0}}),
        MakeArray({{1, 1.0}, {2, std::nullopt}})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL struct field.")},

      {"cosine_distance",
       {MakeNullStringKeyArray(), MakeArray({{"a", 5.0}, {"b", 6.0}})},
       values::NullDouble()},
      {"cosine_distance",
       {MakeArray({{"a", 5.0}, {"b", 6.0}}), MakeNullStringKeyArray()},
       values::NullDouble()},
      {"cosine_distance",
       {MakeNullStringKeyArray(), MakeNullStringKeyArray()},
       values::NullDouble()},
      {"cosine_distance",
       {MakeArray({{"a", 5.0}, {"b", 6.0}}),
        MakeArrayEndingWithNull({{"a", 5.0}})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL array element.")},
      {"cosine_distance",
       {MakeArray({{"a", 5.0}, {"b", 6.0}}),
        MakeArray({{"b", 1.0}, {std::nullopt, 6.0}})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL struct field.")},
      {"cosine_distance",
       {MakeArray({{"a", 5.0}, {"b", 6.0}}),
        MakeArray({{"b", 1.0}, {"a", std::nullopt}})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL struct field.")},

      // Zero length array
      {"cosine_distance",
       {MakeArray(std::vector<double>{}), MakeArray(std::vector<double>{})},
       values::NullDouble(),
       absl::OutOfRangeError(
           "Cannot compute cosine distance against zero vector.")},

      // Zero length vector
      {"cosine_distance",
       {MakeArray<double>({0.0, 0.0}), MakeArray<double>({1.0, 2.0})},
       values::NullDouble(),
       absl::OutOfRangeError(
           "Cannot compute cosine distance against zero vector.")},

      // Mismatch length vector
      {"cosine_distance",
       {MakeArray<double>({1.0, 2.0}), MakeArray<double>({1.0, 2.0, 3.0})},
       values::NullDouble(),
       absl::OutOfRangeError("Array length mismatch 2 and 3.")},

      // Long vector
      // When 2 vectors are parallel, the angle is 0, so cosine distance is
      // 1 - cos(0) = 0.
      {"cosine_distance",
       {MakeRepeatedArray(1.0, 128), MakeRepeatedArray(2.0, 128)},
       0.0,
       kDistanceFloatMargin},

      // NaN
      {"cosine_distance",
       {MakeArray<double>({1.0, std::numeric_limits<double>::quiet_NaN()}),
        MakeArray<double>({3.0, 4.0})},
       std::numeric_limits<double>::quiet_NaN()},

      // Inf
      {"cosine_distance",
       {MakeArray<double>({1.0, std::numeric_limits<double>::infinity()}),
        MakeArray<double>({3.0, 4.0})},
       std::numeric_limits<double>::quiet_NaN()},

      // Overflow
      {"cosine_distance",
       {MakeArray<double>({std::numeric_limits<double>::max(), 0.0}),
        MakeArray<double>({0.0, std::numeric_limits<double>::max()})},
       values::NullDouble(),
       absl::OutOfRangeError("double overflow: 1.79769e+308 * 1.79769e+308")},

      // Dense array
      {"cosine_distance",
       {MakeArray<double>({1.0, 2.0}), MakeArray<double>({3.0, 4.0})},
       0.01613008990009257,
       kDistanceFloatMargin},
      {"cosine_distance",
       {MakeArray<double>({5.0, 6.0}), MakeArray<double>({7.0, 8.0})},
       0.0002901915325176363,
       kDistanceFloatMargin},
      {"cosine_distance",
       {MakeArray<double>({1.0, 2.0}), MakeArray<double>({-3.0, -4.0})},
       1.9838699100999073,
       kDistanceFloatMargin},

      // Dense array with significant floating point values difference.
      // The 2 vectors are parallel so cosine distance = 0.
      {"cosine_distance",
       {MakeArray<double>({1e140, 2.0e140}),
        MakeArray<double>({1.0e-140, 2.0e-140})},
       0.0,
       kDistanceFloatMargin},

      // Dense array with significant floating point values difference.
      // The 2 vectors are antiparallel so cosine distance = 2.
      {"cosine_distance",
       {MakeArray<double>({1e140, 2.0e140}),
        MakeArray<double>({-1.0e-140, -2.0e-140})},
       2.0,
       kDistanceFloatMargin},

      // Dense array with significant floating point values difference.
      // The 2 vectors are perpendicular so cosine distance = 1.
      {"cosine_distance",
       {MakeArray<double>({1e140, 2.0e140}),
        MakeArray<double>({-2.0e-140, 1.0e-140})},
       1.0,
       kDistanceFloatMargin},

      // Sparse int64_t key array
      {"cosine_distance",
       {MakeArray({{1, 1.0}, {2, 2.0}}), MakeArray({{1, 3.0}, {2, 4.0}})},
       0.01613008990009257,
       kDistanceFloatMargin},
      {"cosine_distance",
       {MakeArray({{1, 5.0}, {2, 6.0}}), MakeArray({{1, 7.0}, {2, 8.0}})},
       0.0002901915325176363,
       kDistanceFloatMargin},
      // Sparse array with significant floating point values difference.
      // The 2 vectors are parallel so cosine distance = 0.
      {"cosine_distance",
       {MakeArray({{1, 5.0e140}, {2, 6.0e140}}),
        MakeArray({{1, 5.0e-140}, {2, 6.0e-140}})},
       0.0,
       kDistanceFloatMargin},
      // Sparse array with significant floating point values difference.
      // The inner product is 25 + 36 = 61. The vector length is in the order
      // of 10^20, so the cosine similarity is 0, meaning cosine distance is ~1.
      {"cosine_distance",
       {MakeArray({{1, 5.0e20}, {2, 6.0e-20}}),
        MakeArray({{1, 5.0e-20}, {2, 6.0e20}})},
       1.0,
       kDistanceFloatMargin},
      // Sparse array with significant floating point values difference.
      // The 2 vectors are perpendicular so cosine distance = 1.
      {"cosine_distance",
       {MakeArray({{1, 5.0e140}, {2, 6.0e140}}),
        MakeArray({{1, -6.0e-140}, {2, 5.0e-140}})},
       1.0,
       kDistanceFloatMargin},
      // Sparse int64_t key array custom struct field names
      {"cosine_distance",
       {MakeArrayCustomStructFieldNames({{1, 1.0}, {2, 2.0}}, "key", ""),
        MakeArrayCustomStructFieldNames({{1, 3.0}, {2, 4.0}}, "", "value")},
       0.01613008990009257,
       kDistanceFloatMargin},
      {"cosine_distance",
       {MakeArrayCustomStructFieldNames({{1, 1.0}, {2, 2.0}}, "", ""),
        MakeArrayCustomStructFieldNames({{1, 3.0}, {2, 4.0}}, "value1",
                                        "value2")},
       0.01613008990009257,
       kDistanceFloatMargin},

      // Sparse string key array
      {"cosine_distance",
       {MakeArray({{"a", 1.0}, {"b", 2.0}}),
        MakeArray({{"a", 3.0}, {"b", 4.0}})},
       0.01613008990009257,
       kDistanceFloatMargin},
      {"cosine_distance",
       {MakeArray({{"a", 5.0}, {"b", 6.0}}),
        MakeArray({{"a", 7.0}, {"b", 8.0}})},
       0.0002901915325176363,
       kDistanceFloatMargin},
      // Sparse string key array with significant floating point values
      // difference.
      // The 2 vectors are parallel so cosine distance = 0.
      {"cosine_distance",
       {MakeArray({{"a", 5.0e140}, {"b", 6.0e140}}),
        MakeArray({{"a", 5.0e-140}, {"b", 6.0e-140}})},
       0.0,
       kDistanceFloatMargin},
      // Sparse string key array with significant floating point values
      // difference.
      // The 2 vectors are perpendicular so cosine distance = 1.
      {"cosine_distance",
       {MakeArray({{"a", 5.0e140}, {"b", 6.0e140}}),
        MakeArray({{"a", -6.0e-140}, {"b", 5.0e-140}})},
       1.0,
       kDistanceFloatMargin},
      // Sparse string key array with significant floating point values
      // difference.
      // The inner product is 25 + 36 = 61. The vector length is in the order
      // of 10^20, so the cosine similarity is 0, meaning cosine distance is ~1.
      {"cosine_distance",
       {MakeArray({{"a", 5.0e20}, {"b", 6.0e-20}}),
        MakeArray({{"a", 5.0e-20}, {"b", 6.0e20}})},
       1.0,
       kDistanceFloatMargin},
      // Sparse string key array custom struct field names
      {"cosine_distance",
       {MakeArrayCustomStructFieldNames({{"a", 1.0}, {"b", 2.0}}, "key", ""),
        MakeArrayCustomStructFieldNames({{"a", 3.0}, {"b", 4.0}}, "", "value")},
       0.01613008990009257,
       kDistanceFloatMargin},
      {"cosine_distance",
       {MakeArrayCustomStructFieldNames({{"a", 1.0}, {"b", 2.0}}, "", ""),
        MakeArrayCustomStructFieldNames({{"a", 3.0}, {"b", 4.0}}, "value1",
                                        "value2")},
       0.01613008990009257,
       kDistanceFloatMargin}};

  std::vector<FunctionTestCall> float_array_tests = {
      // NULL inputs.
      {"cosine_distance",
       {MakeNullFloatArray(), MakeArray<float>({3.0, 4.0})},
       values::NullDouble()},
      {"cosine_distance",
       {MakeArray<float>({5.0, 6.0}), MakeNullFloatArray()},
       values::NullDouble()},
      {"cosine_distance",
       {MakeNullFloatArray(), MakeNullFloatArray()},
       values::NullDouble()},
      {"cosine_distance",
       {MakeArray<float>({5.0, 6.0}),
        MakeArrayEndingWithNull(std::vector<float>{3.0})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL array element.")},

      // Long vector
      // When 2 vectors are parallel, the angle is 0, so cosine distance is
      // 1 - cos(0) = 0.
      {"cosine_distance",
       {MakeRepeatedArray(1.0f, 128), MakeRepeatedArray(2.0f, 128)},
       0.0,
       kDistanceFloatMargin},

      // Zero length vector.
      {"cosine_distance",
       {MakeArray<float>({0.0, 0.0}), MakeArray<float>({1.0, 2.0})},
       values::NullDouble(),
       absl::OutOfRangeError(
           "Cannot compute cosine distance against zero vector.")},

      // Mismatched vector length.
      {"cosine_distance",
       {MakeArray<float>({1.0, 2.0}), MakeArray<float>({1.0, 2.0, 3.0})},
       values::NullDouble(),
       absl::OutOfRangeError("Array length mismatch 2 and 3.")},

      // NaN
      {"cosine_distance",
       {MakeArray<float>({1.0, std::numeric_limits<float>::quiet_NaN()}),
        MakeArray<float>({3.0, 4.0})},
       std::numeric_limits<double>::quiet_NaN()},

      // Inf
      {"cosine_distance",
       {MakeArray<float>({1.0, std::numeric_limits<float>::infinity()}),
        MakeArray<float>({3.0, 4.0})},
       std::numeric_limits<double>::quiet_NaN()},
      {"cosine_distance",
       {MakeArray<float>({1.0, -std::numeric_limits<float>::infinity()}),
        MakeArray<float>({3.0, 4.0})},
       std::numeric_limits<double>::quiet_NaN()},

      // Dense array with float values as input.
      {"cosine_distance",
       {MakeArray<float>({1.0, 2.0}), MakeArray<float>({3.0, 4.0})},
       0.01613008990009257,
       kDistanceFloatMargin},
      {"cosine_distance",
       {MakeArray<float>({5.0, 6.0}), MakeArray<float>({7.0, 8.0})},
       0.0002901915325176363,
       kDistanceFloatMargin},
      {"cosine_distance",
       {MakeArray<float>({1.0, 2.0}), MakeArray<float>({-3.0, -4.0})},
       1.9838699100999073,
       kDistanceFloatMargin},

      // Dense array with significant floating point values difference.
      // The 2 vectors are parallel so cosine distance = 0.
      {"cosine_distance",
       {MakeArray<float>({1e18, 2.0e18}), MakeArray<float>({1.0e-18, 2.0e-18})},
       0.0,
       kDistanceFloatMargin},

      // Dense array with significant floating point values difference.
      // The 2 vectors are antiparallel so cosine distance = 2.
      {"cosine_distance",
       {MakeArray<float>({1e18, 2.0e18}),
        MakeArray<float>({-1.0e-18, -2.0e-18})},
       2.0,
       kDistanceFloatMargin},

      // Dense array with significant floating point values difference.
      // The 2 vectors are perpendicular so cosine distance = 1.
      {"cosine_distance",
       {MakeArray<float>({1e18, 2.0e18}),
        MakeArray<float>({-2.0e-18, 1.0e-18})},
       1.0,
       kDistanceFloatMargin}};

  for (auto& test_case : float_array_tests) {
    test_case.params.AddRequiredFeature(
        FEATURE_V_1_4_ENABLE_FLOAT_DISTANCE_FUNCTIONS);
    tests.emplace_back(test_case);
  }

  return tests;
}

std::vector<FunctionTestCall> GetFunctionTestsEuclideanDistance() {
  std::vector<FunctionTestCall> tests = {
      // NULL inputs
      {"euclidean_distance",
       {MakeNullDoubleArray(), MakeArray<double>({3.0, 4.0})},
       values::NullDouble()},
      {"euclidean_distance",
       {MakeArray<double>({5.0, 6.0}), MakeNullDoubleArray()},
       values::NullDouble()},
      {"euclidean_distance",
       {MakeNullDoubleArray(), MakeNullDoubleArray()},
       values::NullDouble()},
      {"euclidean_distance",
       {MakeArray<double>({5.0, 6.0}),
        MakeArrayEndingWithNull(std::vector<double>{3.0})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL array element.")},

      {"euclidean_distance",
       {MakeArray({{1, 1.0}, {2, 2.0}}), MakeNullInt64KeyArray()},
       values::NullDouble()},
      {"euclidean_distance",
       {MakeNullInt64KeyArray(), MakeArray({{1, 5.0}, {2, 6.0}})},
       values::NullDouble()},
      {"euclidean_distance",
       {MakeNullInt64KeyArray(), MakeNullInt64KeyArray()},
       values::NullDouble()},
      {"euclidean_distance",
       {MakeArray({{1, 5.0}, {2, 6.0}}),
        MakeArrayEndingWithNull(
            std::vector<Int64DoubleKeyValuePair>{{3, 5.0}})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL array element.")},
      {"euclidean_distance",
       {MakeArray({{1, 5.0}, {2, 6.0}}),
        MakeArray({{1, 1.0}, {std::nullopt, 6.0}})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL struct field.")},
      {"euclidean_distance",
       {MakeArray({{1, 5.0}, {2, 6.0}}),
        MakeArray({{1, 1.0}, {2, std::nullopt}})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL struct field.")},

      {"euclidean_distance",
       {MakeNullStringKeyArray(), MakeArray({{"a", 5.0}, {"b", 6.0}})},
       values::NullDouble()},
      {"euclidean_distance",
       {MakeArray({{"a", 5.0}, {"b", 6.0}}), MakeNullStringKeyArray()},
       values::NullDouble()},
      {"euclidean_distance",
       {MakeNullStringKeyArray(), MakeNullStringKeyArray()},
       values::NullDouble()},
      {"euclidean_distance",
       {MakeArray({{"a", 5.0}, {"b", 6.0}}),
        MakeArrayEndingWithNull({{"a", 5.0}})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL array element.")},
      {"euclidean_distance",
       {MakeArray({{"a", 5.0}, {"b", 6.0}}),
        MakeArray({{"b", 1.0}, {std::nullopt, 6.0}})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL struct field.")},
      {"euclidean_distance",
       {MakeArray({{"a", 5.0}, {"b", 6.0}}),
        MakeArray({{"b", 1.0}, {"a", std::nullopt}})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL struct field.")},

      // Zero length array.
      {"euclidean_distance",
       {MakeArray(std::vector<double>{}), MakeArray(std::vector<double>{})},
       0.0,
       kDistanceFloatMargin},

      // Zero length vector
      {"euclidean_distance",
       {MakeArray<double>({0.0, 0.0}), MakeArray<double>({3.0, 4.0})},
       5.0,
       kDistanceFloatMargin},

      // Mismatch length vector.
      {"euclidean_distance",
       {MakeArray<double>({0.0, 0.1}), MakeArray<double>({1.0, 2.0, 3.0})},
       values::NullDouble(),
       absl::OutOfRangeError("Array length mismatch 2 and 3.")},

      // Inf
      {"euclidean_distance",
       {MakeArray<double>({1.0, std::numeric_limits<double>::infinity()}),
        MakeArray<double>({3.0, 4.0})},
       std::numeric_limits<double>::infinity()},

      // NaN
      {"euclidean_distance",
       {MakeArray<double>({1.0, std::numeric_limits<double>::quiet_NaN()}),
        MakeArray<double>({3.0, 4.0})},
       std::numeric_limits<double>::quiet_NaN()},

      // Long vector
      // Distance = sqrt(64 * 64 * (2 - 1)^2) = 64
      {"euclidean_distance",
       {MakeRepeatedArray(1.0, 64 * 64), MakeRepeatedArray(2.0, 64 * 64)},
       64.0,
       kDistanceFloatMargin},

      // Overflow
      {"euclidean_distance",
       {MakeArray<double>({std::numeric_limits<double>::max(), 2.0}),
        MakeArray<double>({3.0, 4.0})},
       values::NullDouble(),
       absl::OutOfRangeError("double overflow: 1.79769e+308 * 1.79769e+308")},

      // Dense array
      {"euclidean_distance",
       {MakeArray<double>({1.0, 2.0}), MakeArray<double>({3.0, 4.0})},
       2.8284271247461903,
       kDistanceFloatMargin},
      {"euclidean_distance",
       {MakeArray<double>({5.0, 6.0}), MakeArray<double>({7.0, 9.0})},
       3.6055512754639891,
       kDistanceFloatMargin},

      // Dense array with significantly different floating point values.
      {"euclidean_distance",
       {MakeArray<double>({3.0e140, 4.0e140}),
        MakeArray<double>({7.0e-140, 9.0e-140})},
       5e140,
       kDistanceFloatMargin},

      // Sparse int64_t key array
      {"euclidean_distance",
       {MakeArray({{1, 1.0}, {2, 2.0}}), MakeArray({{1, 3.0}, {2, 4.0}})},
       2.8284271247461903,
       kDistanceFloatMargin},
      {"euclidean_distance",
       {MakeArray({{1, 5.0}, {2, 6.0}}), MakeArray({{1, 7.0}, {2, 9.0}})},
       3.6055512754639891,
       kDistanceFloatMargin},
      // Sparse int64_t key array with significantly different floating point
      // values.
      {"euclidean_distance",
       {MakeArray({{1, 7.0e-140}, {2, 4.0e140}}),
        MakeArray({{1, 3.0e140}, {2, 9.0e-140}})},
       5e140,
       kDistanceFloatMargin},
      // Sparse int64_t key array custom struct field names.
      {"euclidean_distance",
       {MakeArrayCustomStructFieldNames({{1, 1.0}, {2, 2.0}}, "key", ""),
        MakeArrayCustomStructFieldNames({{1, 3.0}, {2, 4.0}}, "", "value")},
       2.8284271247461903,
       kDistanceFloatMargin},
      {"euclidean_distance",
       {MakeArrayCustomStructFieldNames({{1, 1.0}, {2, 2.0}}, "", ""),
        MakeArrayCustomStructFieldNames({{1, 3.0}, {2, 4.0}}, "value1",
                                        "value2")},
       2.8284271247461903,
       kDistanceFloatMargin},

      // Sparse string key array.
      {"euclidean_distance",
       {MakeArray({{"a", 1.0}, {"b", 2.0}}),
        MakeArray({{"a", 3.0}, {"b", 4.0}})},
       2.8284271247461903,
       kDistanceFloatMargin},
      {"euclidean_distance",
       {MakeArray({{"a", 5.0}, {"b", 6.0}}),
        MakeArray({{"a", 7.0}, {"b", 9.0}})},
       3.6055512754639891,
       kDistanceFloatMargin},
      // Sparse string key array with significantly different floating point
      // values.
      {"euclidean_distance",
       {MakeArray({{"a", 7.0e-140}, {"b", 4.0e140}}),
        MakeArray({{"a", 3.0e140}, {"b", 9.0e-140}})},
       5e140,
       kDistanceFloatMargin},
      // Sparse string key array custom struct field names.
      {"euclidean_distance",
       {MakeArrayCustomStructFieldNames({{"a", 1.0}, {"b", 2.0}}, "key", ""),
        MakeArrayCustomStructFieldNames({{"a", 3.0}, {"b", 4.0}}, "", "value")},
       2.8284271247461903,
       kDistanceFloatMargin},
      {"euclidean_distance",
       {MakeArrayCustomStructFieldNames({{"a", 1.0}, {"b", 2.0}}, "", ""),
        MakeArrayCustomStructFieldNames({{"a", 3.0}, {"b", 4.0}}, "value1",
                                        "value2")},
       2.8284271247461903,
       kDistanceFloatMargin}};

  std::vector<FunctionTestCall> float_array_tests = {
      // NULL inputs.
      {"euclidean_distance",
       {MakeNullFloatArray(), MakeArray<float>({3.0, 4.0})},
       values::NullDouble()},
      {"euclidean_distance",
       {MakeArray<float>({5.0, 6.0}), MakeNullFloatArray()},
       values::NullDouble()},
      {"euclidean_distance",
       {MakeNullFloatArray(), MakeNullFloatArray()},
       values::NullDouble()},
      {"euclidean_distance",
       {MakeArray<float>({5.0, 6.0}),
        MakeArrayEndingWithNull(std::vector<float>{3.0})},
       values::NullDouble(),
       absl::OutOfRangeError("NULL array element.")},

      // Long vector.
      // Distance = sqrt(64 * 64 * (2 - 1)^2) = 64
      {"euclidean_distance",
       {MakeRepeatedArray(1.0f, 64 * 64), MakeRepeatedArray(2.0f, 64 * 64)},
       64.0,
       kDistanceFloatMargin},

      // Zero length vector.
      {"euclidean_distance",
       {MakeArray<float>({0.0, 0.0}), MakeArray<float>({3.0, 4.0})},
       5.0,
       kDistanceFloatMargin},

      // Mismatched vector length.
      {"euclidean_distance",
       {MakeArray<float>({0.0, 0.1}), MakeArray<float>({1.0, 2.0, 3.0})},
       values::NullDouble(),
       absl::OutOfRangeError("Array length mismatch 2 and 3.")},

      // Inf
      {"euclidean_distance",
       {MakeArray<float>({1.0, std::numeric_limits<float>::infinity()}),
        MakeArray<float>({3.0, 4.0})},
       std::numeric_limits<double>::infinity()},
      {"euclidean_distance",
       {MakeArray<float>({1.0, -std::numeric_limits<float>::infinity()}),
        MakeArray<float>({3.0, 4.0})},
       std::numeric_limits<double>::infinity()},

      // NaN
      {"euclidean_distance",
       {MakeArray<float>({1.0, std::numeric_limits<float>::quiet_NaN()}),
        MakeArray<float>({3.0, 4.0})},
       std::numeric_limits<double>::quiet_NaN()},

      // Dense array with float values as input.
      {"euclidean_distance",
       {MakeArray<float>({1.0, 2.0}), MakeArray<float>({3.0, 4.0})},
       2.8284271247461903,
       kDistanceFloatMargin},
      {"euclidean_distance",
       {MakeArray<float>({5.0, 6.0}), MakeArray<float>({7.0, 9.0})},
       3.6055512754639891,
       kDistanceFloatMargin},

      // Dense array with significantly different floating point values.
      {"euclidean_distance",
       {MakeArray<float>({3.0e18, 4.0e18}),
        MakeArray<float>({7.0e-19, 9.0e-19})},
       5e18,
       FloatMargin::UlpMargin(30)}};

  for (auto& test_case : float_array_tests) {
    test_case.params.AddRequiredFeature(
        FEATURE_V_1_4_ENABLE_FLOAT_DISTANCE_FUNCTIONS);
    tests.emplace_back(test_case);
  }

  return tests;
}

std::vector<FunctionTestCall> GetFunctionTestsEditDistance() {
  std::vector<FunctionTestCall> tests = {
      // NULL values
      {"edit_distance",
       {values::NullString(), values::NullString()},
       values::NullInt64()},
      {"edit_distance", {values::NullString(), "abc"}, values::NullInt64()},
      {"edit_distance", {"abc", values::NullString()}, values::NullInt64()},
      {"edit_distance", {"abc", "a", values::NullInt64()}, values::NullInt64()},

      // Invalid max_distance.
      {"edit_distance",
       {"abc", "", -3},
       3ll,
       absl::OutOfRangeError("max_distance must be non-negative")},

      // Unicode strings
      {"edit_distance", {"𨉟€", "€€"}, 1ll},
      {"edit_distance", {"a𨉟b", "a€b"}, 1ll},
      // àx is \u00e0 \u0078
      // a̋x is \u0061 \u030b \u0078
      {"edit_distance", {"àx", "a̋x"}, 2ll},
      // Note that the following use different code points than the above.
      // àx is \u0061 \u0300 \u0078
      // áx is \u0061 \u0301 \u0078
      {"edit_distance", {"àx", "áx"}, 1ll},
      // zàx is \u007a \u00e0 \u0078
      // za̋x is \u007a \u0061 \u030b \u0078
      {"edit_distance", {"zàx", "za̋x"}, 2ll},

      // No max distance
      {"edit_distance", {"", "abc"}, 3ll},
      {"edit_distance", {"abc", "ade"}, 2ll},
      {"edit_distance", {"abc", "abc"}, 0ll},

      // Unicode with max distance.
      {"edit_distance", {"𨉟€", "€€", 3}, 1ll},
      {"edit_distance", {"a𨉟b", "a€b", 2}, 1ll},
      // àx is \u00e0 \u0078
      // a̋x is \u0061 \u030b \u0078
      {"edit_distance", {"àx", "a̋x", 1}, 1ll},
      // Note that the following use different code points than the above.
      // àx is \u0061 \u0300 \u0078
      // áx is \u0061 \u0301 \u0078
      {"edit_distance", {"àx", "áx", 4}, 1ll},
      // zàx is \u007a \u00e0 \u0078
      // za̋x is \u007a \u0061 \u030b \u0078
      {"edit_distance", {"zàx", "za̋x", 2}, 2ll},

      // With max distance
      {"edit_distance", {"abc", "", 4}, 3ll},
      {"edit_distance", {"abc", "", 3}, 3ll},
      {"edit_distance", {"abc", "", 2}, 2ll},
      {"edit_distance", {"abc", "", 1}, 1ll},
      {"edit_distance", {"abc", "", 0}, 0ll}};
  return tests;
}

std::vector<FunctionTestCall> GetFunctionTestsEditDistanceBytes() {
  std::vector<FunctionTestCall> tests = {
      // BYTES input with NULL arguments.
      {"edit_distance",
       {values::NullBytes(), values::NullBytes()},
       values::NullInt64()},
      {"edit_distance",
       {values::NullBytes(), values::Bytes("abc")},
       values::NullInt64()},
      {"edit_distance",
       {values::Bytes("abc"), values::NullBytes()},
       values::NullInt64()},
      {"edit_distance",
       {values::Bytes("abc"), values::Bytes("a"), values::NullInt64()},
       values::NullInt64()},

      // Invalid max_distance.
      {"edit_distance",
       {values::Bytes("abc"), values::Bytes(""), -3},
       3ll,
       absl::OutOfRangeError("max_distance must be non-negative")},

      // Unicode strings
      {"edit_distance", {values::Bytes("𨉟€"), values::Bytes("€€")}, 4ll},
      {"edit_distance", {values::Bytes("a𨉟b"), values::Bytes("a€b")}, 4ll},
      // àx is \u00e0 \u0078
      // a̋x is \u0061 \u030b \u0078
      {"edit_distance", {values::Bytes("àx"), values::Bytes("a̋x")}, 3ll},
      // Note that the following use different code points than the above.
      // àx is \u0061 \u0300 \u0078
      // áx is \u0061 \u0301 \u0078
      {"edit_distance", {values::Bytes("àx"), values::Bytes("áx")}, 1ll},
      // zàx is \u007a \u00e0 \u0078
      // za̋x is \u007a \u0061 \u030b \u0078
      {"edit_distance", {values::Bytes("zàx"), values::Bytes("za̋x")}, 3ll},

      // No max distance
      {"edit_distance", {values::Bytes(""), values::Bytes("abc")}, 3ll},
      {"edit_distance", {values::Bytes("abc"), values::Bytes("ade")}, 2ll},
      {"edit_distance", {values::Bytes("abc"), values::Bytes("abc")}, 0ll},

      // Unicode with max distance.
      {"edit_distance", {values::Bytes("𨉟€"), values::Bytes("€€"), 3}, 3ll},
      {"edit_distance", {values::Bytes("a𨉟b"), values::Bytes("a€b"), 2}, 2ll},
      // àx is \u00e0 \u0078
      // a̋x is \u0061 \u030b \u0078
      {"edit_distance", {values::Bytes("àx"), values::Bytes("a̋x"), 2}, 2ll},
      // Note that the following use different code points than the above.
      // àx is \u0061 \u0300 \u0078
      // áx is \u0061 \u0301 \u0078
      {"edit_distance", {values::Bytes("àx"), values::Bytes("áx"), 4}, 1ll},
      // zàx is \u007a \u00e0 \u0078
      // za̋x is \u007a \u0061 \u030b \u0078
      {"edit_distance", {values::Bytes("zàx"), values::Bytes("za̋x"), 2}, 2ll},

      // With max distance
      {"edit_distance", {values::Bytes("abc"), values::Bytes(""), 4}, 3ll},
      {"edit_distance", {values::Bytes("abc"), values::Bytes(""), 3}, 3ll},
      {"edit_distance", {values::Bytes("abc"), values::Bytes(""), 2}, 2ll},
      {"edit_distance", {values::Bytes("abc"), values::Bytes(""), 1}, 1ll},
      {"edit_distance", {values::Bytes("abc"), values::Bytes(""), 0}, 0ll}};
  return tests;
}

}  // namespace zetasql
