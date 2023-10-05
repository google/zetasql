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

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace functions {
namespace {

using ::zetasql_base::testing::StatusIs;

struct Int64Value {
  int64_t key = 0;
  double value = 0.0;
};

struct StringValue {
  std::string key;
  double value = 0.0;
};

template <typename T>
T ValueOrDie(absl::StatusOr<T>& s) {
  ZETASQL_CHECK_OK(s.status());
  return s.value();
}

Value MakeArray(std::vector<double> arr) {
  std::vector<Value> values;
  values.reserve(arr.size());
  for (const auto& v : arr) {
    values.push_back(Value::Double(v));
  }
  auto status = Value::MakeArray(zetasql::types::DoubleArrayType(), values);
  return ValueOrDie(status);
}

Value MakeArray(std::vector<Int64Value> arr) {
  zetasql::TypeFactory type_factory;
  std::vector<zetasql::StructType::StructField> struct_fields;
  struct_fields.push_back(
      zetasql::StructType::StructField("k", type_factory.get_int64()));
  struct_fields.push_back(
      zetasql::StructType::StructField("v", type_factory.get_double()));
  const zetasql::StructType* struct_type = nullptr;

  ZETASQL_CHECK_OK(type_factory.MakeStructType(struct_fields, &struct_type));

  std::vector<Value> values;
  values.reserve(arr.size());
  for (const auto& v : arr) {
    auto status = Value::MakeStruct(
        struct_type, {Value::Int64(v.key), Value::Double(v.value)});
    values.push_back(ValueOrDie(status));
  }
  const ArrayType* array_type;
  ZETASQL_CHECK_OK(type_factory.MakeArrayType(struct_type, &array_type));
  auto status = Value::MakeArray(array_type, values);
  return ValueOrDie(status);
}

Value MakeArray(std::vector<StringValue> arr) {
  zetasql::TypeFactory type_factory;
  std::vector<zetasql::StructType::StructField> struct_fields;
  struct_fields.push_back(
      zetasql::StructType::StructField("k", type_factory.get_string()));
  struct_fields.push_back(
      zetasql::StructType::StructField("v", type_factory.get_double()));
  const zetasql::StructType* struct_type = nullptr;

  ZETASQL_CHECK_OK(type_factory.MakeStructType(struct_fields, &struct_type));

  std::vector<Value> values;
  values.reserve(arr.size());
  for (const auto& v : arr) {
    auto status = Value::MakeStruct(
        struct_type, {Value::StringValue(v.key), Value::Double(v.value)});
    values.push_back(ValueOrDie(status));
  }
  const ArrayType* array_type;
  ZETASQL_CHECK_OK(type_factory.MakeArrayType(struct_type, &array_type));
  auto status = Value::MakeArray(array_type, values);
  return ValueOrDie(status);
}

template <typename IdxType>
std::vector<Value> CreateArrayPair(std::vector<IdxType> arr1,
                                   std::vector<IdxType> arr2) {
  std::vector<Value> result;
  result.reserve(2);
  result.emplace_back(MakeArray(arr1));
  result.emplace_back(MakeArray(arr2));
  return result;
}

TEST(CosineDistanceTest, DenseArrayLengthMismatch) {
  std::vector<Value> args = CreateArrayPair<double>({1.0, 2.0}, {3.0});
  EXPECT_THAT(CosineDistanceDense(args[0], args[1]),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Array length mismatch: 2 and 1."));
}

TEST(CosineDistanceTest, DenseZeroArray) {
  std::vector<Value> args = CreateArrayPair<double>({1.0, 2.0}, {0.0, 0.0});
  EXPECT_THAT(CosineDistanceDense(args[0], args[1]),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Cannot compute cosine distance against zero vector."));
}

TEST(CosineDistanceTest, SparseArrayDuplicateInt64Key) {
  std::vector<Value> args =
      CreateArrayPair<Int64Value>({{1, 1.0}, {1, 2.0}}, {{2, 3.0}, {3, 4.0}});
  EXPECT_THAT(CosineDistanceSparseInt64Key(args[0], args[1]),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Duplicate index 1 found in the input array."));
}

TEST(CosineDistanceTest, SparseArrayDuplicateStringKey) {
  std::vector<Value> args = CreateArrayPair<StringValue>(
      {{"a", 1.0}, {"a", 2.0}}, {{"a", 3.0}, {"b", 4.0}});
  EXPECT_THAT(CosineDistanceSparseStringKey(args[0], args[1]),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Duplicate index a found in the input array."));
}

TEST(EuclideanDistanceTest, DenseArrayLengthMismatch) {
  std::vector<Value> args = CreateArrayPair<double>({1.0, 2.0}, {0.0});
  EXPECT_THAT(EuclideanDistanceDense(args[0], args[1]),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Array length mismatch: 2 and 1."));
}

TEST(EuclideanDistanceTest, SparseArrayDuplicateInt64Key) {
  std::vector<Value> args =
      CreateArrayPair<Int64Value>({{1, 1.0}, {1, 2.0}}, {{2, 3.0}, {3, 4.0}});
  EXPECT_THAT(EuclideanDistanceSparseInt64Key(args[0], args[1]),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Duplicate index 1 found in the input array."));
}

TEST(EuclideanDistanceTest, SparseArrayDuplicateStringKey) {
  std::vector<Value> args = CreateArrayPair<StringValue>(
      {{"a", 1.0}, {"a", 2.0}}, {{"a", 3.0}, {"b", 4.0}});
  EXPECT_THAT(EuclideanDistanceSparseStringKey(args[0], args[1]),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       "Duplicate index a found in the input array."));
}

enum class DistanceFunctionVectorTypeEnum {
  kDense,
  kSparseInt64Key,
  kSparseStringKey,
};

struct VectorDistanceParam {
  // Using std::function because value-parameterized tests require values to be
  // copyable.
  std::function<absl::StatusOr<Value>(Value vector1, Value vector2)> function;
  DistanceFunctionVectorTypeEnum type;
  std::vector<Value> args;
  double expected_output = 0.0;
};

VectorDistanceParam CosineTest(DistanceFunctionVectorTypeEnum type,
                               std::vector<Value> args, double output) {
  switch (type) {
    case DistanceFunctionVectorTypeEnum::kDense:
      return {CosineDistanceDense, type, args, output};
    case DistanceFunctionVectorTypeEnum::kSparseInt64Key:
      return {CosineDistanceSparseInt64Key, type, args, output};
    case DistanceFunctionVectorTypeEnum::kSparseStringKey:
      return {CosineDistanceSparseStringKey, type, args, output};
  }
}

VectorDistanceParam EuclideanTest(DistanceFunctionVectorTypeEnum type,
                                  std::vector<Value> args, double output) {
  switch (type) {
    case DistanceFunctionVectorTypeEnum::kDense:
      return {EuclideanDistanceDense, type, args, output};
    case DistanceFunctionVectorTypeEnum::kSparseInt64Key:
      return {EuclideanDistanceSparseInt64Key, type, args, output};
    case DistanceFunctionVectorTypeEnum::kSparseStringKey:
      return {EuclideanDistanceSparseStringKey, type, args, output};
  }
}

class VectorDistanceParamTest
    : public ::testing::TestWithParam<VectorDistanceParam> {};

TEST_P(VectorDistanceParamTest, VectorDistance) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(Value result, GetParam().function(GetParam().args[0],
                                                         GetParam().args[1]));
  EXPECT_NEAR(result.double_value(), GetParam().expected_output, 1e-6)
      << " actual " << result.double_value() << " vs expected "
      << GetParam().expected_output;
}

INSTANTIATE_TEST_SUITE_P(
    CosineDistance, VectorDistanceParamTest,
    testing::Values(

        // Cosine distance.
        // Dense.
        CosineTest(DistanceFunctionVectorTypeEnum::kDense,
                   CreateArrayPair<double>({1.0, 2.0}, {3.0, 4.0}),
                   0.01613008990009257),
        CosineTest(DistanceFunctionVectorTypeEnum::kDense,
                   CreateArrayPair<double>({5.0, 6.0}, {7.0, 8.0}),
                   0.0002901915325176363),

        // Sparse int.
        CosineTest(DistanceFunctionVectorTypeEnum::kSparseInt64Key,
                   CreateArrayPair<Int64Value>({{1, 1.0}, {2, 2.0}},
                                               {{1, 3.0}, {2, 4.0}}),
                   0.01613008990009257),
        CosineTest(DistanceFunctionVectorTypeEnum::kSparseInt64Key,
                   CreateArrayPair<Int64Value>({{1, 5.0}, {2, 6.0}},
                                               {{1, 7.0}, {2, 8.0}}),
                   0.0002901915325176363),
        CosineTest(DistanceFunctionVectorTypeEnum::kSparseInt64Key,
                   CreateArrayPair<Int64Value>({{1, 5.0}, {2, 6.0}},
                                               {{2, 7.0}, {3, 8.0}}),
                   0.49412274752247876),

        // Sparse string.
        CosineTest(DistanceFunctionVectorTypeEnum::kSparseStringKey,
                   CreateArrayPair<StringValue>({{"a", 1.0}, {"b", 2.0}},
                                                {{"a", 3.0}, {"b", 4.0}}),
                   0.01613008990009257),
        CosineTest(DistanceFunctionVectorTypeEnum::kSparseStringKey,
                   CreateArrayPair<StringValue>({{"a", 5.0}, {"b", 6.0}},
                                                {{"a", 7.0}, {"b", 8.0}}),
                   0.0002901915325176363),
        CosineTest(DistanceFunctionVectorTypeEnum::kSparseStringKey,
                   CreateArrayPair<StringValue>({{"a", 5.0}, {"b", 6.0}},
                                                {{"b", 7.0}, {"c", 8.0}}),
                   0.49412274752247876),

        // Euclidean distance.
        // Dense.
        EuclideanTest(DistanceFunctionVectorTypeEnum::kDense,
                      CreateArrayPair<double>({1.0, 2.0}, {3.0, 4.0}),
                      2.828427),
        EuclideanTest(DistanceFunctionVectorTypeEnum::kDense,
                      CreateArrayPair<double>({5.0, 6.0}, {7.0, 9.0}),
                      3.6055512754639891),

        // Sparse int.
        EuclideanTest(DistanceFunctionVectorTypeEnum::kSparseInt64Key,
                      CreateArrayPair<Int64Value>({{1, 1.0}, {2, 2.0}},
                                                  {{1, 3.0}, {2, 4.0}}),
                      2.828427),
        EuclideanTest(DistanceFunctionVectorTypeEnum::kSparseInt64Key,
                      CreateArrayPair<Int64Value>({{1, 5.0}, {2, 6.0}},
                                                  {{1, 7.0}, {2, 9.0}}),
                      3.6055512754639891),
        EuclideanTest(DistanceFunctionVectorTypeEnum::kSparseInt64Key,
                      CreateArrayPair<Int64Value>({{1, 5.0}, {2, 6.0}},
                                                  {{2, 7.0}, {3, 9.0}}),
                      10.34408),

        // Sparse string.
        EuclideanTest(DistanceFunctionVectorTypeEnum::kSparseStringKey,
                      CreateArrayPair<StringValue>({{"a", 1.0}, {"b", 2.0}},
                                                   {{"a", 3.0}, {"b", 4.0}}),
                      2.828427),
        EuclideanTest(DistanceFunctionVectorTypeEnum::kSparseStringKey,
                      CreateArrayPair<StringValue>({{"a", 5.0}, {"b", 6.0}},
                                                   {{"a", 7.0}, {"b", 9.0}}),
                      3.6055512754639891),
        EuclideanTest(DistanceFunctionVectorTypeEnum::kSparseStringKey,
                      CreateArrayPair<StringValue>({{"a", 5.0}, {"b", 6.0}},
                                                   {{"b", 7.0}, {"c", 9.0}}),
                      10.34408)

            ));

struct EditDistanceParam {
  std::string text1;
  std::string text2;

  std::optional<int64_t> max_distance;

  int64_t expected_output = 0;
  int64_t expected_bytes_output = 0;

  EditDistanceParam(absl::string_view text1, absl::string_view text2,
                    int64_t expected_output)
      : text1(text1),
        text2(text2),
        expected_output(expected_output),
        expected_bytes_output(expected_output) {}
  EditDistanceParam(absl::string_view text1, absl::string_view text2,
                    int64_t max_distance, int64_t expected_output)
      : text1(text1),
        text2(text2),
        max_distance(max_distance),
        expected_output(expected_output),
        expected_bytes_output(expected_output) {}
};

EditDistanceParam EditDistanceParamWithExpectedBytesOutput(
    absl::string_view text1, absl::string_view text2, int64_t expected_output,
    int64_t expected_bytes_output) {
  EditDistanceParam param(text1, text2, expected_output);
  param.expected_bytes_output = expected_bytes_output;
  return param;
}

class EditDistanceParamTest
    : public ::testing::TestWithParam<EditDistanceParam> {};

TEST_P(EditDistanceParamTest, EditDistance) {
  std::vector<Value> params;
  std::optional<int64_t> max_distance =
      GetParam().max_distance.has_value()
          ? std::make_optional(GetParam().max_distance.value())
          : std::nullopt;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      int64_t result,
      EditDistance(GetParam().text1, GetParam().text2, max_distance));
  EXPECT_EQ(result, GetParam().expected_output);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      int64_t result_bytes,
      EditDistanceBytes(GetParam().text1, GetParam().text2, max_distance));
  EXPECT_EQ(result_bytes, GetParam().expected_bytes_output);
}

INSTANTIATE_TEST_SUITE_P(
    EditDistance, EditDistanceParamTest,
    testing::Values(

        // No max distance.
        EditDistanceParam("a", "b", 1), EditDistanceParam("a", "a", 0),
        EditDistanceParam("aa", "a", 1), EditDistanceParam("abc", "abd", 1),
        EditDistanceParam("abc", "abd", 1), EditDistanceParam("aac", "abd", 2),

        // Unicode
        EditDistanceParamWithExpectedBytesOutput("𨉟€", "€€", 1, 4),
        EditDistanceParamWithExpectedBytesOutput("a𨉟b", "a€b", 1, 4),
        // àx is \u00e0 \u0078
        // a̋x is \u0061 \u030b \u0078
        EditDistanceParamWithExpectedBytesOutput("àx", "a̋x", 2, 3),
        EditDistanceParamWithExpectedBytesOutput("zàx", "za̋x", 2, 3),
        // Note that the following use different code points than the above.
        // àx is \u0061 \u0300 \u0078
        // áx is \u0061 \u0301 \u0078
        EditDistanceParam("àx", "áx", 1),

        // With max distance.
        EditDistanceParam("123", "1234567", 5, 4),
        EditDistanceParam("1234", "1234567", 5, 3),
        EditDistanceParam("12345", "1234567", 5, 2),
        EditDistanceParam("abc", "", 3, 3), EditDistanceParam("abc", "", 2, 2),
        EditDistanceParam("abc", "", 1, 1), EditDistanceParam("abc", "", 0, 0),
        EditDistanceParam("a", "b", 4, 1),
        EditDistanceParam("aac", "bbc", 2, 2),
        EditDistanceParam("aac", "bbc", 1, 1),
        EditDistanceParam("abcdef", "a12345", 3, 3)

            ));

TEST(EditDistance, TwoLongStrings) {
  std::vector<Value> params;
  std::string very_long_string1 = std::string(1 << 12, '1');
  std::string very_long_string2 = std::string(1 << 12, '2');

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      int64_t result,
      EditDistance(very_long_string1, very_long_string2, std::nullopt));
  EXPECT_EQ(result, 1 << 12);
}

TEST(EditDistance, VeryLongStringAndShortString) {
  std::string very_long_string = std::string(1 << 20, ' ');
  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t result,
                       EditDistance(very_long_string, "   ", std::nullopt));
  EXPECT_EQ(result, (1 << 20) - 3);
}

}  // namespace
}  // namespace functions
}  // namespace zetasql
