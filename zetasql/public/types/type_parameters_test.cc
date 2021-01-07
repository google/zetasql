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

#include "zetasql/public/types/type_parameters.h"

#include "google/protobuf/descriptor.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/type_parameters.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/strings/string_view.h"

namespace zetasql {
namespace {

TEST(TypeParameters, CreateStringTypeParametersWithMaxLiteral) {
  StringTypeParametersProto proto;
  proto.set_is_max_length(true);
  ZETASQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_param,
                       TypeParameters::MakeStringTypeParameters(proto));
  EXPECT_TRUE(type_param.IsStringTypeParameters());
  EXPECT_EQ(type_param.string_type_parameters().is_max_length(), true);
  EXPECT_EQ(type_param.DebugString(), "(max_length=MAX)");
}
TEST(TypeParameters, CreateStringTypeParametersWithMaxLength) {
  StringTypeParametersProto proto;
  proto.set_max_length(1000);
  ZETASQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_param,
                       TypeParameters::MakeStringTypeParameters(proto));
  EXPECT_TRUE(type_param.IsStringTypeParameters());
  EXPECT_EQ(type_param.string_type_parameters().max_length(), 1000);
  EXPECT_EQ(type_param.DebugString(), "(max_length=1000)");
}
TEST(TypeParameters, CreateNumericTypeParametersWithMaxLiteral) {
  NumericTypeParametersProto proto;
  proto.set_is_max_precision(true);
  proto.set_scale(20);
  ZETASQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_param,
                       TypeParameters::MakeNumericTypeParameters(proto));
  EXPECT_TRUE(type_param.IsNumericTypeParameters());
  EXPECT_EQ(type_param.DebugString(), "(precision=MAX,scale=20)");
}
TEST(TypeParameters, CreateNumericTypeParametersWithPrecisonAndScale) {
  NumericTypeParametersProto proto;
  proto.set_precision(20);
  proto.set_scale(7);
  ZETASQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_param,
                       TypeParameters::MakeNumericTypeParameters(proto));
  EXPECT_TRUE(type_param.IsNumericTypeParameters());
  EXPECT_EQ(type_param.DebugString(), "(precision=20,scale=7)");
}
TEST(TypeParameters, CreateNumericTypeParametersWithPrecisionOnly) {
  NumericTypeParametersProto proto;
  proto.set_precision(30);
  ZETASQL_ASSERT_OK_AND_ASSIGN(TypeParameters type_param,
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

  ZETASQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_child,
                       TypeParameters::MakeStringTypeParameters(string_param));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters numeric_child,
      TypeParameters::MakeNumericTypeParameters(numeric_param));
  // sub-field without type parameters.
  child_list.push_back(TypeParameters());
  child_list.push_back(string_child);
  child_list.push_back(numeric_child);
  TypeParameters struct_type_param =
      TypeParameters::MakeTypeParametersWithChildList(child_list);
  EXPECT_EQ(struct_type_param.num_children(), 3);
  EXPECT_TRUE(struct_type_param.child(0).IsEmptyTypeParameters());
  EXPECT_EQ(struct_type_param.child(1).string_type_parameters().max_length(),
            10);
  EXPECT_EQ(struct_type_param.child(2).numeric_type_parameters().precision(),
            10);
  EXPECT_EQ(struct_type_param.child(2).numeric_type_parameters().scale(), 5);
  EXPECT_EQ(struct_type_param.DebugString(),
            "[null,(max_length=10),(precision=10,scale=5)]");
}

TEST(TypeParameters, ExtendedTypeParametersWithChildList) {
  std::vector<TypeParameters> child_list;
  StringTypeParametersProto string_param;
  string_param.set_max_length(10);
  ZETASQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_child,
                       TypeParameters::MakeStringTypeParameters(string_param));
  child_list.push_back(string_child);
  std::vector<SimpleValue> parameters;
  parameters.push_back(SimpleValue::Int64(999));
  parameters.push_back(SimpleValue::String("abc"));
  TypeParameters extended_type_with_children =
      TypeParameters::MakeExtendedTypeParameters(
          ExtendedTypeParameters(parameters), child_list);
  EXPECT_EQ(extended_type_with_children.num_children(), 1);
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
  ZETASQL_ASSERT_OK(type_parameters.Serialize(&type_parameters_proto))
      << type_parameters.DebugString();
  auto status_or_type_parameters =
      TypeParameters::Deserialize(type_parameters_proto);
  ZETASQL_ASSERT_OK(status_or_type_parameters.status());
  std::string output;
  google::protobuf::TextFormat::PrintToString(type_parameters_proto, &output);
  EXPECT_TRUE(type_parameters.Equals(status_or_type_parameters.value()))
      << "\nSerialized type_parameters:\n"
      << type_parameters_proto.DebugString();
}

// Roundtrips TypeParametersProto through TypeParameters and back.
static void DeserializeSerialize(const std::string& type_parameters_proto_str) {
  TypeParametersProto type_parameters_proto;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(type_parameters_proto_str,
                                             &type_parameters_proto))
      << type_parameters_proto_str;
  auto status_or_value = TypeParameters::Deserialize(type_parameters_proto);
  ZETASQL_ASSERT_OK(status_or_value.status()) << type_parameters_proto.DebugString();
  TypeParametersProto roundtrip_type_parameters_proto;
  ZETASQL_ASSERT_OK(status_or_value.value().Serialize(&roundtrip_type_parameters_proto))
      << roundtrip_type_parameters_proto.DebugString();
  EXPECT_THAT(type_parameters_proto,
              testing::EqualsProto(roundtrip_type_parameters_proto));
}

static void DeserializeWithExpectedError(
    const std::string& type_parameters_proto_str,
    absl::string_view expected_error_message) {
  TypeParametersProto type_parameters_proto;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(type_parameters_proto_str,
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_type_param,
                       TypeParameters::MakeStringTypeParameters(string_param));
  SerializeDeserialize(string_type_param);
  string_param.set_is_max_length(true);
  ZETASQL_ASSERT_OK_AND_ASSIGN(string_type_param,
                       TypeParameters::MakeStringTypeParameters(string_param));
  SerializeDeserialize(string_type_param);
}

TEST(TypeParameters, SerializeNumericTypeParameters) {
  // NUMERIC/BIGNUMERIC(P,S) or NUMERIC/BIGNUMERIC(P) or BIGNUMERIC(MAX) or
  // BIGNUMERIC(MAX,S)
  NumericTypeParametersProto numeric_param;
  numeric_param.set_precision(10);
  numeric_param.set_scale(5);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters numeric_type_param,
      TypeParameters::MakeNumericTypeParameters(numeric_param));
  SerializeDeserialize(numeric_type_param);
  numeric_param.set_is_max_precision(true);
  numeric_param.set_scale(20);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(TypeParameters bytes_type_param,
                       TypeParameters::MakeStringTypeParameters(bytes_param));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(
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

// StringTypeParameters matches STRING and BYTES.
TEST(TypeParameters, MatchStringOrBytesType) {
  StringTypeParametersProto string_param;
  string_param.set_max_length(10);
  ZETASQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_type_param,
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
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters numeric_type_param,
      TypeParameters::MakeNumericTypeParameters(numeric_param));
  EXPECT_TRUE(numeric_type_param.MatchType(type_factory.get_numeric()));
  EXPECT_TRUE(numeric_type_param.MatchType(type_factory.get_bignumeric()));
}

// EmptyTypeParameters matches all types.
TEST(TypeParameters, MatchNonParameterizedType) {
  TypeFactory type_factory;
  TypeParameters empty_type_param = TypeParameters();
  EXPECT_TRUE(empty_type_param.MatchType(type_factory.get_bool()));
  EXPECT_TRUE(empty_type_param.MatchType(type_factory.get_bignumeric()));
}

// TypeParameters with child_list matches ARRAY and STRUCT type.
TEST(TypeParameters, MatchTypeParametersWithChildList) {
  TypeFactory type_factory;
  std::vector<TypeParameters> child_list;
  StringTypeParametersProto string_param;
  string_param.set_max_length(10);
  TypeParameters empty_type_param = TypeParameters();
  ZETASQL_ASSERT_OK_AND_ASSIGN(TypeParameters string_child,
                       TypeParameters::MakeStringTypeParameters(string_param));
  child_list.push_back(string_child);
  TypeParameters struct_type_param =
      TypeParameters::MakeTypeParametersWithChildList(child_list);
  const Type* array_type = nullptr;
  ZETASQL_ASSERT_OK(type_factory.MakeArrayType(type_factory.get_string(), &array_type));
  EXPECT_TRUE(empty_type_param.MatchType(array_type));
}

}  // namespace
}  // namespace zetasql
