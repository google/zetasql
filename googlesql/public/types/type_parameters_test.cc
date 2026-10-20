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

#include "googlesql/public/types/type_parameters.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/functions/rounding_mode.pb.h"
#include "googlesql/public/proto/vector_encoding_id.pb.h"
#include "googlesql/public/type.pb.h"
#include "googlesql/public/type_parameters.pb.h"
#include "googlesql/public/types/annotation.h"
#include "googlesql/public/types/collation.h"
#include "googlesql/public/types/declarative_type.h"
#include "googlesql/public/types/simple_value.h"
#include "googlesql/public/types/struct_type.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/types/type_modifiers.h"
#include "googlesql/public/types/vector_type_util.h"
#include "googlesql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/text_format.h"

using ::testing::Property;
using ::absl_testing::IsOkAndHolds;

namespace googlesql {
namespace {

TEST(TypeParameters, CreateStringTypeParametersWithMaxLiteral) {
  StringTypeParametersProto proto;
  proto.set_is_max_length(true);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_param,
                       TypeParameters::MakeStringTypeParameters(proto));
  EXPECT_TRUE(type_param.IsStringTypeParameters());
  EXPECT_EQ(type_param.string_type_parameters().is_max_length(), true);
  EXPECT_EQ(type_param.DebugString(), "(max_length=MAX)");
}
TEST(TypeParameters, CreateStringTypeParametersWithMaxLength) {
  StringTypeParametersProto proto;
  proto.set_max_length(1000);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_param,
                       TypeParameters::MakeStringTypeParameters(proto));
  EXPECT_TRUE(type_param.IsStringTypeParameters());
  EXPECT_EQ(type_param.string_type_parameters().max_length(), 1000);
  EXPECT_EQ(type_param.DebugString(), "(max_length=1000)");
}
TEST(TypeParameters, CreateNumericTypeParametersWithMaxLiteral) {
  NumericTypeParametersProto proto;
  proto.set_is_max_precision(true);
  proto.set_scale(20);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_param,
                       TypeParameters::MakeNumericTypeParameters(proto));
  EXPECT_TRUE(type_param.IsNumericTypeParameters());
  EXPECT_EQ(type_param.DebugString(), "(precision=MAX,scale=20)");
}
TEST(TypeParameters, CreateNumericTypeParametersWithPrecisonAndScale) {
  NumericTypeParametersProto proto;
  proto.set_precision(20);
  proto.set_scale(7);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_param,
                       TypeParameters::MakeNumericTypeParameters(proto));
  EXPECT_TRUE(type_param.IsNumericTypeParameters());
  EXPECT_EQ(type_param.DebugString(), "(precision=20,scale=7)");
}
TEST(TypeParameters, CreateNumericTypeParametersWithPrecisionOnly) {
  NumericTypeParametersProto proto;
  proto.set_precision(30);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_param,
                       TypeParameters::MakeNumericTypeParameters(proto));
  EXPECT_TRUE(type_param.IsNumericTypeParameters());
  EXPECT_EQ(type_param.DebugString(), "(precision=30,scale=0)");
}

TEST(TypeParameters, CreateExtendedTypeParameters) {
  std::vector<SimpleValue> parameters;
  parameters.push_back(SimpleValue::Int64(100));
  parameters.push_back(SimpleValue::String("random"));
  TypeParameters type_param = TypeParameters::MakeExtendedTypeParameters(
      ExtendedTypeParameters(parameters));
  ExtendedTypeParameters extended_type_parameters =
      type_param.extended_type_parameters();

  EXPECT_EQ(extended_type_parameters.num_parameters(), 2);
  EXPECT_EQ(extended_type_parameters.parameter(0), SimpleValue::Int64(100));
  EXPECT_EQ(extended_type_parameters.parameter(1),
            SimpleValue::String("random"));
  EXPECT_EQ(extended_type_parameters.DebugString(), "(100,\"random\")");
}

TEST(TypeParameters, TypeParametersWithChildList) {
  std::vector<TypeParameters> child_list;
  StringTypeParametersProto string_param;
  string_param.set_max_length(10);
  NumericTypeParametersProto numeric_param;
  numeric_param.set_precision(10);
  numeric_param.set_scale(5);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_child,
                       TypeParameters::MakeStringTypeParameters(string_param));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters numeric_child,
      TypeParameters::MakeNumericTypeParameters(numeric_param));
  // sub-field without type parameters.
  child_list.push_back(TypeParameters());
  child_list.push_back(string_child);
  child_list.push_back(numeric_child);
  TypeParameters struct_type_param =
      TypeParameters::MakeTypeParametersWithChildList(child_list);
  EXPECT_EQ(struct_type_param.num_children(), 3);
  EXPECT_FALSE(struct_type_param.child_list().empty());
  EXPECT_FALSE(struct_type_param.IsEmpty());
  EXPECT_TRUE(struct_type_param.child(0).IsEmpty());
  EXPECT_EQ(struct_type_param.child(1).string_type_parameters().max_length(),
            10);
  EXPECT_EQ(struct_type_param.child(2).numeric_type_parameters().precision(),
            10);
  EXPECT_EQ(struct_type_param.child(2).numeric_type_parameters().scale(), 5);
  EXPECT_EQ(struct_type_param.DebugString(),
            "[null,(max_length=10),(precision=10,scale=5)]");
}

TEST(TypeParameters, TypeParametersWithSetChildList) {
  TypeParameters params = TypeParameters();

  // Create child_list.
  std::vector<TypeParameters> child_list;
  StringTypeParametersProto string_param;
  string_param.set_max_length(10);
  NumericTypeParametersProto numeric_param;
  numeric_param.set_precision(10);
  numeric_param.set_scale(5);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_child,
                       TypeParameters::MakeStringTypeParameters(string_param));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters numeric_child,
      TypeParameters::MakeNumericTypeParameters(numeric_param));
  child_list.push_back(TypeParameters());
  child_list.push_back(string_child);
  child_list.push_back(numeric_child);

  EXPECT_TRUE(params.IsEmpty());
  EXPECT_TRUE(params.IsTopLevelEmpty());

  params.set_child_list(child_list);
  EXPECT_FALSE(params.IsEmpty());
  EXPECT_FALSE(params.child_list().empty());
  // However, the top-level itself is still empty
  EXPECT_TRUE(params.IsTopLevelEmpty());

  EXPECT_EQ(params.num_children(), 3);
  EXPECT_EQ(params.child(1).string_type_parameters().max_length(), 10);
  EXPECT_EQ(params.child(2).numeric_type_parameters().precision(), 10);
  EXPECT_EQ(params.child(2).numeric_type_parameters().scale(), 5);
  EXPECT_EQ(params.DebugString(),
            "[null,(max_length=10),(precision=10,scale=5)]");
}

TEST(TypeParameters, ExtendedTypeParametersWithChildList) {
  std::vector<TypeParameters> child_list;
  StringTypeParametersProto string_param;
  string_param.set_max_length(10);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_child,
                       TypeParameters::MakeStringTypeParameters(string_param));
  child_list.push_back(string_child);
  std::vector<SimpleValue> parameters;
  parameters.push_back(SimpleValue::Int64(999));
  parameters.push_back(SimpleValue::String("abc"));
  TypeParameters extended_type_with_children =
      TypeParameters::MakeExtendedTypeParameters(
          ExtendedTypeParameters(parameters), child_list);
  EXPECT_FALSE(extended_type_with_children.child_list().empty());
  EXPECT_EQ(extended_type_with_children.num_children(), 1);
  EXPECT_FALSE(extended_type_with_children.IsEmpty());
  EXPECT_EQ(extended_type_with_children.child(0)
                .string_type_parameters()
                .max_length(),
            10);
  EXPECT_EQ(extended_type_with_children.DebugString(),
            "(999,\"abc\")[(max_length=10)]");
}

// Roundtrips TypeParameters through TypeParametersProto and back.
static void SerializeDeserialize(const TypeParameters& type_parameters) {
  TypeParametersProto type_parameters_proto;
  GOOGLESQL_ASSERT_OK(type_parameters.Serialize(&type_parameters_proto))
      << type_parameters.DebugString();
  auto status_or_type_parameters =
      TypeParameters::Deserialize(type_parameters_proto);
  GOOGLESQL_ASSERT_OK(status_or_type_parameters.status());
  std::string output;
  google::protobuf::TextFormat::PrintToString(type_parameters_proto, &output);
  EXPECT_TRUE(type_parameters.Equals(status_or_type_parameters.value()))
      << "\nSerialized type_parameters:\n"
      << absl::StrCat(type_parameters_proto);
}

// Roundtrips TypeParametersProto through TypeParameters and back.
static void DeserializeSerialize(const std::string& type_parameters_proto_str) {
  TypeParametersProto type_parameters_proto;
  ABSL_QCHECK(google::protobuf::TextFormat::ParseFromString(type_parameters_proto_str,
                                             &type_parameters_proto))
      << type_parameters_proto_str;
  auto status_or_value = TypeParameters::Deserialize(type_parameters_proto);
  GOOGLESQL_ASSERT_OK(status_or_value.status()) << absl::StrCat(type_parameters_proto);
  TypeParametersProto roundtrip_type_parameters_proto;
  GOOGLESQL_ASSERT_OK(status_or_value.value().Serialize(&roundtrip_type_parameters_proto))
      << absl::StrCat(roundtrip_type_parameters_proto);
  EXPECT_THAT(type_parameters_proto,
              testing::EqualsProto(roundtrip_type_parameters_proto));
}

static void DeserializeWithExpectedError(
    const std::string& type_parameters_proto_str,
    absl::string_view expected_error_message) {
  TypeParametersProto type_parameters_proto;
  ABSL_QCHECK(google::protobuf::TextFormat::ParseFromString(type_parameters_proto_str,
                                             &type_parameters_proto))
      << type_parameters_proto_str;
  auto status_or_value = TypeParameters::Deserialize(type_parameters_proto);
  EXPECT_EQ(status_or_value.status().code(), absl::StatusCode::kInternal);
  EXPECT_THAT(status_or_value.status().message(),
              ::testing::HasSubstr(expected_error_message));
}

TEST(TypeParameters, SerializeStringTypeParameters) {
  // STRING(L)/STRING(MAX) or BYTES(L)/BYTES(MAX)
  StringTypeParametersProto string_param;
  string_param.set_max_length(10);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_type_param,
                       TypeParameters::MakeStringTypeParameters(string_param));
  SerializeDeserialize(string_type_param);
  string_param.set_is_max_length(true);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(string_type_param,
                       TypeParameters::MakeStringTypeParameters(string_param));
  SerializeDeserialize(string_type_param);
}

TEST(TypeParameters, SerializeNumericTypeParameters) {
  // NUMERIC/BIGNUMERIC(P,S) or NUMERIC/BIGNUMERIC(P) or BIGNUMERIC(MAX) or
  // BIGNUMERIC(MAX,S)
  NumericTypeParametersProto numeric_param;
  numeric_param.set_precision(10);
  numeric_param.set_scale(5);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters numeric_type_param,
      TypeParameters::MakeNumericTypeParameters(numeric_param));
  SerializeDeserialize(numeric_type_param);
  numeric_param.set_is_max_precision(true);
  numeric_param.set_scale(20);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      numeric_type_param,
      TypeParameters::MakeNumericTypeParameters(numeric_param));
  SerializeDeserialize(numeric_type_param);

  // Extended type parameters with EnumValue parameters.
  std::vector<SimpleValue> parameters;
  parameters.push_back(SimpleValue::Int64(100));
  parameters.push_back(SimpleValue::String("random"));
  ExtendedTypeParameters extended_type_param =
      ExtendedTypeParameters(parameters);
  SerializeDeserialize(
      TypeParameters::MakeExtendedTypeParameters(extended_type_param));
}

TEST(TypeParameters, SerializeTypeParametersWithChildList) {
  // Type parameters with child_list.
  std::vector<TypeParameters> child_list;
  StringTypeParametersProto bytes_param;
  NumericTypeParametersProto numeric_param;
  numeric_param.set_precision(22);
  numeric_param.set_scale(7);
  bytes_param.set_max_length(100);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters bytes_type_param,
                       TypeParameters::MakeStringTypeParameters(bytes_param));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters numeric_type_param,
      TypeParameters::MakeNumericTypeParameters(numeric_param));
  // sub-field without type parameters.
  child_list.push_back(TypeParameters());
  child_list.push_back(bytes_type_param);
  child_list.push_back(numeric_type_param);
  TypeParameters struct_type_param =
      TypeParameters::MakeTypeParametersWithChildList(child_list);
  SerializeDeserialize(struct_type_param);
}

TEST(TypeParameters, SerializeExtendedTypeParametersWithChildList) {
  // Type parameters with child_list.
  std::vector<TypeParameters> child_list;
  NumericTypeParametersProto numeric_param;
  numeric_param.set_precision(22);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters numeric_type_param,
      TypeParameters::MakeNumericTypeParameters(numeric_param));
  // sub-field without type parameters.
  child_list.push_back(TypeParameters());
  child_list.push_back(numeric_type_param);
  std::vector<SimpleValue> parameters;
  parameters.push_back(SimpleValue::Int64(333));
  parameters.push_back(SimpleValue::Int64(666));
  TypeParameters extended_type_with_children =
      TypeParameters::MakeExtendedTypeParameters(
          ExtendedTypeParameters(parameters), child_list);
  SerializeDeserialize(extended_type_with_children);
}

TEST(TypeParameters, DeserializeStringTypeParametersSuccess) {
  // STRING(L)/STRING(MAX) or BYTES(L)/BYTES(MAX)
  DeserializeSerialize(R"proto(
    string_type_parameters { max_length: 100 })proto");
  DeserializeSerialize(R"proto(
    string_type_parameters { is_max_length: true })proto");
}

TEST(TypeParameters, DeserializeNumericTypeParametersSuccess) {
  // NUMERIC/BIGNUMERIC(P,S) or NUMERIC/BIGNUMERIC(P) or BIGNUMERIC(MAX) or
  // BIGNUMERIC(MAX,S)
  DeserializeSerialize(
      R"proto(
        numeric_type_parameters { precision: 20 scale: 5 })proto");
  DeserializeSerialize(
      R"proto(
        numeric_type_parameters { precision: 60 scale: 30 })proto");
  DeserializeSerialize(
      R"proto(
        numeric_type_parameters { is_max_precision: true scale: 0 })proto");
  DeserializeSerialize(
      R"proto(
        numeric_type_parameters { is_max_precision: true scale: 20 })proto");
}

TEST(TypeParameters, DeserializeExtendedTypeParametersSuccess) {
  // Extended type parameters.
  DeserializeSerialize(
      R"proto(
        extended_type_parameters {
          parameters { int64_value: 100 }
          parameters { string_value: "random" }
        })proto");
}

TEST(TypeParameters, DeserializeTypeParameterWithChildList) {
  // Type parameters with child_list.
  DeserializeSerialize(
      R"proto(
        child_list { string_type_parameters { is_max_length: true } }
        child_list { string_type_parameters { max_length: 300 } }
        child_list { numeric_type_parameters { precision: 20 scale: 5 } })proto");
}

TEST(TypeParameters, DeserializeExtendedTypeParametersWithChildList) {
  // Extended type parameters.
  DeserializeSerialize(
      R"proto(
        extended_type_parameters {
          parameters { int64_value: 100 }
          parameters { string_value: "random" }
        }
        child_list { string_type_parameters { is_max_length: true } }
        child_list { string_type_parameters { max_length: 300 } }
      )proto");
}

TEST(TypeParameters, DeserializeStringTypeParametersFailed) {
  DeserializeWithExpectedError(
      R"proto(
        string_type_parameters { max_length: -100 })proto",
      "max_length must be larger than 0, actual max_length: -100");

  DeserializeWithExpectedError(
      R"proto(
        string_type_parameters { is_max_length: false })proto",
      "is_max_length should either be unset or true");
}

TEST(TypeParameters, DeserializeNumericTypeParametersFailed) {
  DeserializeWithExpectedError(
      R"proto(
        numeric_type_parameters { is_max_precision: false scale: 0 })proto",
      "is_max_precision should either be unset or true");

  DeserializeWithExpectedError(
      R"proto(
        numeric_type_parameters { precision: 100 scale: 30 })proto",
      "precision must be within range [1, 76] or MAX, actual precision: 100");

  DeserializeWithExpectedError(
      R"proto(
        numeric_type_parameters { precision: 50 scale: 40 })proto",
      "scale must be within range [0, 38], actual scale: 40");

  DeserializeWithExpectedError(
      R"proto(
        numeric_type_parameters { precision: 30 scale: 35 })proto",
      "precision must be equal or larger than scale, actual precision: 30, "
      "scale: 35");
}

TEST(TypeParameters, DeserializeVectorTypeParametersFailed) {
  DeserializeWithExpectedError(
      R"pb(
        vector_type_parameters { dims: -10 })pb",
      "vector_type_parameters.dims() > 0");

  DeserializeWithExpectedError(
      R"pb(
        vector_type_parameters { dims: 10 encoding: UNKNOWN_VECTOR_ENCODING })pb",
      "Unrecognized VECTOR encoding: \"UNKNOWN_VECTOR_ENCODING\"");
}

// StringTypeParameters matches STRING and BYTES.
TEST(TypeParameters, MatchStringOrBytesType) {
  StringTypeParametersProto string_param;
  string_param.set_max_length(10);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_type_param,
                       TypeParameters::MakeStringTypeParameters(string_param));
  TypeFactory type_factory;
  EXPECT_TRUE(string_type_param.MatchType(type_factory.get_string()));
  EXPECT_TRUE(string_type_param.MatchType(type_factory.get_bytes()));
  EXPECT_FALSE(string_type_param.MatchType(type_factory.get_bool()));
  EXPECT_FALSE(string_type_param.MatchType(type_factory.get_bignumeric()));
}

// NumericTypeParameters matches NUMERIC and BIGNUMERIC.
TEST(TypeParameters, MatchNumericOrBigNumericType) {
  TypeFactory type_factory;
  NumericTypeParametersProto numeric_param;
  numeric_param.set_precision(10);
  numeric_param.set_scale(5);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters numeric_type_param,
      TypeParameters::MakeNumericTypeParameters(numeric_param));
  EXPECT_TRUE(numeric_type_param.MatchType(type_factory.get_numeric()));
  EXPECT_TRUE(numeric_type_param.MatchType(type_factory.get_bignumeric()));
  EXPECT_FALSE(numeric_type_param.MatchType(type_factory.get_int64()));
}

// Empty TypeParameters matches all types.
TEST(TypeParameters, MatchNonParameterizedType) {
  TypeFactory type_factory;
  const Type* struct_type;
  const Type* array_type;
  const Type* range_type;
  GOOGLESQL_ASSERT_OK(type_factory.MakeStructType({{"int64", type_factory.get_int64()}},
                                        &struct_type));
  GOOGLESQL_ASSERT_OK(
      type_factory.MakeArrayType(type_factory.get_string(), &array_type));
  GOOGLESQL_ASSERT_OK(type_factory.MakeRangeType(type_factory.get_date(), &range_type));
  TypeParameters empty_type_param = TypeParameters();
  EXPECT_TRUE(empty_type_param.MatchType(type_factory.get_bool()));
  EXPECT_TRUE(empty_type_param.MatchType(type_factory.get_bignumeric()));
  EXPECT_TRUE(empty_type_param.MatchType(struct_type));
  EXPECT_TRUE(empty_type_param.MatchType(array_type));
  EXPECT_TRUE(empty_type_param.MatchType(range_type));
}

// TypeParameters with child_list matches ARRAY type.
TEST(TypeParameters, MatchTypeParametersWithArrayType) {
  TypeFactory type_factory;

  // Create TypeParameters with child_list with one child.
  std::vector<TypeParameters> child_list;
  StringTypeParametersProto string_param;
  string_param.set_max_length(10);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_child,
                       TypeParameters::MakeStringTypeParameters(string_param));
  child_list.push_back(string_child);
  TypeParameters array_type_param =
      TypeParameters::MakeTypeParametersWithChildList(child_list);

  // Create array types.
  const Type* string_array = nullptr;
  const Type* numeric_array = nullptr;
  GOOGLESQL_ASSERT_OK(
      type_factory.MakeArrayType(type_factory.get_string(), &string_array));
  GOOGLESQL_ASSERT_OK(
      type_factory.MakeArrayType(type_factory.get_numeric(), &numeric_array));

  EXPECT_TRUE(array_type_param.MatchType(string_array));
  EXPECT_FALSE(array_type_param.MatchType(numeric_array));
}

// TypeParameters with a one-child child_list matches STRUCT type.
TEST(TypeParameters, MatchTypeParametersWithOneFieldStructType) {
  TypeFactory type_factory;

  // Create TypeParameters with child_list with one child.
  std::vector<TypeParameters> child_list;
  StringTypeParametersProto string_param;
  string_param.set_max_length(10);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_child,
                       TypeParameters::MakeStringTypeParameters(string_param));
  child_list.push_back(string_child);
  TypeParameters one_child_type_param =
      TypeParameters::MakeTypeParametersWithChildList(child_list);

  // Create struct/array types.
  const Type* invalid_one_field_struct = nullptr;
  const Type* one_field_struct = nullptr;
  const Type* two_field_struct = nullptr;
  const Type* string_array = nullptr;
  GOOGLESQL_ASSERT_OK(type_factory.MakeStructType({{"int64", type_factory.get_int64()}},
                                        &invalid_one_field_struct));
  GOOGLESQL_ASSERT_OK(type_factory.MakeStructType({{"string", type_factory.get_string()}},
                                        &one_field_struct));
  GOOGLESQL_ASSERT_OK(
      type_factory.MakeStructType({{"string", type_factory.get_string()},
                                   {"numeric", type_factory.get_numeric()}},
                                  &two_field_struct));
  GOOGLESQL_ASSERT_OK(
      type_factory.MakeArrayType(type_factory.get_string(), &string_array));

  EXPECT_TRUE(one_child_type_param.MatchType(one_field_struct));
  EXPECT_FALSE(one_child_type_param.MatchType(invalid_one_field_struct));
  EXPECT_FALSE(one_child_type_param.MatchType(two_field_struct));
  // A TypeParameters object with one child can be applicable to both an ARRAY
  // and STRUCT type if the STRUCT has one field and that field is the same type
  // as that of the ARRAY elements.
  EXPECT_TRUE(one_child_type_param.MatchType(string_array));
}

// TypeParameters with a two-child child_list matches STRUCT type.
TEST(TypeParameters, MatchTypeParametersWithTwoFieldStructType) {
  TypeFactory type_factory;

  // Create TypeParameters with child_list with two children.
  StringTypeParametersProto string_param;
  string_param.set_max_length(10);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_child,
                       TypeParameters::MakeStringTypeParameters(string_param));
  NumericTypeParametersProto numeric_param;
  numeric_param.set_precision(10);
  numeric_param.set_scale(5);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters numeric_child,
      TypeParameters::MakeNumericTypeParameters(numeric_param));
  std::vector<TypeParameters> child_list = {string_child, numeric_child};
  TypeParameters two_children_type_param =
      TypeParameters::MakeTypeParametersWithChildList(child_list);

  // Create struct types.
  const Type* one_field_struct = nullptr;
  const Type* invalid_two_field_struct = nullptr;
  const Type* two_field_struct = nullptr;
  GOOGLESQL_ASSERT_OK(type_factory.MakeStructType({{"string", type_factory.get_string()}},
                                        &one_field_struct));
  GOOGLESQL_ASSERT_OK(
      type_factory.MakeStructType({{"numeric", type_factory.get_numeric()},
                                   {"string", type_factory.get_string()}},
                                  &invalid_two_field_struct));
  GOOGLESQL_ASSERT_OK(
      type_factory.MakeStructType({{"string", type_factory.get_string()},
                                   {"numeric", type_factory.get_numeric()}},
                                  &two_field_struct));

  EXPECT_FALSE(two_children_type_param.MatchType(one_field_struct));
  EXPECT_FALSE(two_children_type_param.MatchType(invalid_two_field_struct));
  EXPECT_TRUE(two_children_type_param.MatchType(two_field_struct));
}

// TypeParameters with a nested child_list matches
// STRUCT<ARRAY<STRUCT<STRING, NUMERIC>>, STRING> type.
TEST(TypeParameters, MatchTypeParametersNestedStructType) {
  TypeFactory type_factory;

  // Create nested TypeParameters.
  StringTypeParametersProto string_param;
  string_param.set_max_length(10);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_child,
                       TypeParameters::MakeStringTypeParameters(string_param));
  NumericTypeParametersProto numeric_param;
  numeric_param.set_precision(10);
  numeric_param.set_scale(5);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters numeric_child,
      TypeParameters::MakeNumericTypeParameters(numeric_param));
  std::vector<TypeParameters> child_list = {string_child, numeric_child};
  TypeParameters struct_params =
      TypeParameters::MakeTypeParametersWithChildList(child_list);
  child_list = {struct_params};
  TypeParameters array_params =
      TypeParameters::MakeTypeParametersWithChildList(child_list);
  child_list = {array_params, string_child};
  TypeParameters nested_struct_params =
      TypeParameters::MakeTypeParametersWithChildList(child_list);

  // Create nested struct type.
  const Type* child_struct_type = nullptr;
  const Type* child_array_type = nullptr;
  const Type* nested_struct_type = nullptr;
  GOOGLESQL_ASSERT_OK(
      type_factory.MakeStructType({{"string", type_factory.get_string()},
                                   {"numeric", type_factory.get_numeric()}},
                                  &child_struct_type));
  GOOGLESQL_ASSERT_OK(
      type_factory.MakeArrayType(child_struct_type, &child_array_type));
  GOOGLESQL_ASSERT_OK(
      type_factory.MakeStructType({{"array", child_array_type},
                                   {"string", type_factory.get_string()}},
                                  &nested_struct_type));

  EXPECT_TRUE(nested_struct_params.MatchType(nested_struct_type));
  EXPECT_FALSE(nested_struct_params.MatchType(child_struct_type));
}

static absl::StatusOr<TypeParameters>
CreateTypeParametersForStructWithTimestampPrecision(int64_t precision) {
  TimestampTypeParametersProto timestamp_type_parameters_proto;
  timestamp_type_parameters_proto.set_precision(precision);
  GOOGLESQL_ASSIGN_OR_RETURN(TypeParameters ts_type_params_precision,
                   TypeParameters::MakeTimestampTypeParameters(
                       timestamp_type_parameters_proto));
  return TypeParameters::MakeTypeParametersWithChildList(
      {TypeParameters(), std::move(ts_type_params_precision)});
}

TEST(TypeParametersTest, EqualsWithDefaultTimestampPrecision) {
  TypeParameters type_parameters1;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_parameters2,
                       CreateTypeParametersForStructWithTimestampPrecision(6));

  EXPECT_FALSE(type_parameters1.Equals(type_parameters2));
  EXPECT_FALSE(type_parameters1.Equals(
      type_parameters2, /*default_timestamp_precision=*/std::nullopt));

  EXPECT_TRUE(type_parameters1.Equals(type_parameters2,
                                      /*default_timestamp_precision=*/6));
  EXPECT_FALSE(type_parameters1.Equals(type_parameters2,
                                       /*default_timestamp_precision=*/3));
}

TEST(TypeParametersTest, VectorTypeParametersResolution) {
  VectorTypeParametersProto proto;
  proto.set_dims(128);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_params,
                       TypeParameters::MakeVectorTypeParameters(proto));
  EXPECT_TRUE(type_params.IsVectorTypeParameters());
  const VectorTypeParametersProto* vector_params =
      type_params.vector_type_parameters();
  ASSERT_NE(vector_params, nullptr);
  EXPECT_EQ(vector_params->dims(), 128);
  EXPECT_EQ(type_params.DebugString(), "(dims=128)");
  EXPECT_EQ(type_params.ToParenthesizedString(), "(128)");
}

TEST(TypeParametersTest, VectorTypeParametersWithEncodingResolution) {
  VectorTypeParametersProto proto;
  proto.set_dims(128);
  proto.set_encoding(googlesql::VectorEncodingId::FLOAT32);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_params,
                       TypeParameters::MakeVectorTypeParameters(proto));
  EXPECT_TRUE(type_params.IsVectorTypeParameters());
  const VectorTypeParametersProto* vector_params =
      type_params.vector_type_parameters();
  ASSERT_NE(vector_params, nullptr);
  EXPECT_EQ(vector_params->dims(), 128);
  EXPECT_EQ(vector_params->encoding(), googlesql::VectorEncodingId::FLOAT32);
  EXPECT_EQ(type_params.DebugString(), "(dims=128, encoding=FLOAT32)");
  EXPECT_EQ(type_params.ToParenthesizedString(), "(128, 'FLOAT32')");
}

TEST(TypeParametersTest, CompositeTypeParametersResolution) {
  TypeFactory type_factory;

  // 1. Construct the declarative type for VECTOR.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(const Type* vector_type, MakeVectorType(&type_factory));

  // 2. Construct the array type for ARRAY<VECTOR>.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(const Type* array_vector_type,
                       type_factory.MakeArrayType(vector_type));

  // 3. Construct the struct type for STRUCT<va ARRAY<VECTOR>, s STRING>.
  const Type* struct_type = nullptr;
  GOOGLESQL_ASSERT_OK(type_factory.MakeStructType(
      {{"va", array_vector_type}, {"s", type_factory.get_string()}},
      &struct_type));

  // 4. Construct the type parameters.
  // VECTOR(128)
  VectorTypeParametersProto vector_proto;
  vector_proto.set_dims(128);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters vector_params,
                       TypeParameters::MakeVectorTypeParameters(vector_proto));

  // ARRAY<VECTOR(128)>
  TypeParameters array_params =
      TypeParameters::MakeTypeParametersWithChildList({vector_params});

  // STRING(MAX)
  StringTypeParametersProto string_proto;
  string_proto.set_is_max_length(true);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_params,
                       TypeParameters::MakeStringTypeParameters(string_proto));

  // STRUCT<va ARRAY<VECTOR(128)>, s STRING(MAX)>
  TypeParameters struct_params =
      TypeParameters::MakeTypeParametersWithChildList(
          {array_params, string_params});

  // Validate the type parameters.
  TypeParametersProto proto;
  GOOGLESQL_ASSERT_OK(struct_params.Serialize(&proto));
  ASSERT_THAT(
      proto, testing::EqualsProto(R"pb(
        child_list { child_list { vector_type_parameters { dims: 128 } } }
        child_list { string_type_parameters { is_max_length: true } }
      )pb"));

  // MatchType validates type match.
  ASSERT_TRUE(struct_params.MatchType(struct_type));

  // Verify mismatch logic works.
  const Type* mismatch_struct_type = nullptr;
  GOOGLESQL_ASSERT_OK(type_factory.MakeStructType(
      {{"va", array_vector_type}, {"s", type_factory.get_int64()}},
      &mismatch_struct_type));
  ASSERT_FALSE(struct_params.MatchType(mismatch_struct_type));

  // DebugString formatting verification.
  ASSERT_EQ(struct_params.DebugString(), "[[(dims=128)],(max_length=MAX)]");
  ASSERT_EQ(struct_params.ToParenthesizedString(), "");

  // Serialization / Deserialization.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(TypeParameters deserialized,
                       TypeParameters::Deserialize(proto));
  ASSERT_TRUE(struct_params.Equals(deserialized));

  // TypeModifiers Verification.
  TypeModifiers struct_modifiers =
      TypeModifiers::MakeTypeModifiers(struct_params, Collation());
  TypeModifiersProto modifiers_proto;
  GOOGLESQL_ASSERT_OK(struct_modifiers.Serialize(&modifiers_proto));
  ASSERT_THAT(
      modifiers_proto, testing::EqualsProto(R"pb(
        type_parameters {
          child_list { child_list { vector_type_parameters { dims: 128 } } }
          child_list { string_type_parameters { is_max_length: true } }
        }
        collation {}
      )pb"));
}

TEST(TypeParametersTest, ToParenthesizedStringAllVariants) {
  // String
  StringTypeParametersProto string_proto1;
  string_proto1.set_max_length(10);
  ASSERT_THAT(
      TypeParameters::MakeStringTypeParameters(string_proto1),
      IsOkAndHolds(Property(&TypeParameters::ToParenthesizedString, "(10)")));

  StringTypeParametersProto string_proto2;
  string_proto2.set_is_max_length(true);
  ASSERT_THAT(
      TypeParameters::MakeStringTypeParameters(string_proto2),
      IsOkAndHolds(Property(&TypeParameters::ToParenthesizedString, "(MAX)")));

  // Numeric
  NumericTypeParametersProto numeric_proto1;
  numeric_proto1.set_precision(10);
  numeric_proto1.set_scale(5);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters p1,
      TypeParameters::MakeNumericTypeParameters(numeric_proto1));
  ASSERT_EQ(p1.ToParenthesizedString(), "(10, 5)");

  NumericTypeParametersProto numeric_proto2;
  numeric_proto2.set_precision(38);
  numeric_proto2.set_scale(9);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters p2,
      TypeParameters::MakeNumericTypeParameters(numeric_proto2));
  ASSERT_EQ(p2.ToParenthesizedString(), "(38, 9)");

  NumericTypeParametersProto numeric_proto3;
  numeric_proto3.set_is_max_precision(true);
  numeric_proto3.set_scale(9);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters p3,
      TypeParameters::MakeNumericTypeParameters(numeric_proto3));
  ASSERT_EQ(p3.ToParenthesizedString(), "(MAX, 9)");

  // Timestamp
  TimestampTypeParametersProto timestamp_proto;
  timestamp_proto.set_precision(6);
  ASSERT_THAT(
      TypeParameters::MakeTimestampTypeParameters(timestamp_proto),
      IsOkAndHolds(Property(&TypeParameters::ToParenthesizedString, "(6)")));
}

}  // namespace
}  // namespace googlesql
