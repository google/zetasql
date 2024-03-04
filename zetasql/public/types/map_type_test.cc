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

#include "zetasql/public/types/map_type.h"

#include <initializer_list>
#include <string>
#include <tuple>
#include <utility>

#include "zetasql/base/testing/status_matchers.h"  
#include "zetasql/public/language_options.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/enum_type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/range_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "zetasql/testing/test_value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "google/protobuf/descriptor.h"

namespace zetasql {
namespace {

using google::protobuf::EnumDescriptor;
using testing::HasSubstr;
using testing::NotNull;
using MapTestAllSimpleTypes = testing::TestWithParam<TypeKind>;

using MapTestFormatValueContentDebugMode =
    testing::TestWithParam<std::tuple<Value, std::string>>;

}  // namespace

// All types which can be constructed via TypeFactory::TypeFromSimpleTypeKind.
const auto kSimpleTypes = {
    TYPE_INT32, TYPE_INT64, TYPE_UINT32, TYPE_UINT64, TYPE_BOOL, TYPE_FLOAT,
    TYPE_DOUBLE, TYPE_STRING, TYPE_BYTES, TYPE_TIMESTAMP,
    TYPE_DATE, TYPE_TIME, TYPE_DATETIME, TYPE_INTERVAL, TYPE_GEOGRAPHY,
    TYPE_NUMERIC, TYPE_BIGNUMERIC, TYPE_JSON, TYPE_TOKENLIST};

INSTANTIATE_TEST_SUITE_P(
    TypeTest, MapTestAllSimpleTypes, testing::ValuesIn(kSimpleTypes),
    [](const testing::TestParamInfo<MapTestAllSimpleTypes::ParamType>& info) {
      return TypeKind_Name(info.param);
    });

// Asserts map type conformance to Type's Is...() and As...() methods, and
// asserts that equality and partitioning are disabled.
void BasicMapAsserts(const Type* map_type) {
  EXPECT_FALSE(map_type->IsSimpleType());
  EXPECT_FALSE(map_type->IsEnum());
  EXPECT_FALSE(map_type->IsArray());
  EXPECT_FALSE(map_type->IsStruct());
  EXPECT_FALSE(map_type->IsProto());
  EXPECT_FALSE(map_type->IsStructOrProto());
  EXPECT_FALSE(map_type->IsRangeType());
  EXPECT_TRUE(map_type->IsMapType());
  EXPECT_EQ(map_type->AsStruct(), nullptr);
  EXPECT_EQ(map_type->AsArray(), nullptr);
  EXPECT_EQ(map_type->AsProto(), nullptr);
  EXPECT_EQ(map_type->AsEnum(), nullptr);
  EXPECT_EQ(map_type->AsRange(), nullptr);

  EXPECT_FALSE(map_type->SupportsEquality());

  LanguageOptions language_options;
  EXPECT_FALSE(map_type->IsSupportedType(language_options));

  language_options.EnableLanguageFeature(FEATURE_V_1_4_MAP_TYPE);
  std::string no_partitioning_type;
  EXPECT_FALSE(
      map_type->SupportsPartitioning(language_options, &no_partitioning_type));
  EXPECT_EQ(no_partitioning_type, "MAP");
}

TEST_P(MapTestAllSimpleTypes, MapCanBeConstructedWithSimpleType) {
  TypeFactory factory;
  TypeKind type_kind = GetParam();

  const Type* key_type = types::TypeFromSimpleTypeKind(type_kind);
  const Type* value_type = types::TypeFromSimpleTypeKind(type_kind);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* map_type,
                       factory.MakeMapType(key_type, value_type));

  EXPECT_TRUE(key_type == GetMapKeyType(map_type));
  EXPECT_TRUE(value_type == GetMapValueType(map_type));

  BasicMapAsserts(map_type);

  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* map_with_map_key_type,
                       factory.MakeMapType(map_type, value_type));
  BasicMapAsserts(map_with_map_key_type);
  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* map_with_map_value_type,
                       factory.MakeMapType(key_type, map_type));
  BasicMapAsserts(map_with_map_value_type);
}

TEST(TypeTest, MapTypeRequiresKeyTypeToBeGroupable) {
  TypeFactory factory;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Type* map_type_with_groupable_simple_key,
      factory.MakeMapType(factory.get_string(), factory.get_string()));

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_MAP_TYPE);
  EXPECT_TRUE(
      map_type_with_groupable_simple_key->IsSupportedType(language_options));
}

TEST(TypeTest, MapTypeRequiresKeyTypeToBeGroupableConditionallyGroupableKey) {
  TypeFactory factory;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Type* map_type_with_array_key,
      factory.MakeMapType(types::Int32ArrayType(), factory.get_string()));

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_MAP_TYPE);
  EXPECT_FALSE(map_type_with_array_key->IsSupportedType(language_options));
  language_options.EnableLanguageFeature(FEATURE_V_1_2_GROUP_BY_ARRAY);
  EXPECT_TRUE(map_type_with_array_key->IsSupportedType(language_options));
}

TEST(TypeTest, MapTypeRequiresKeyAndValueTypesToBeSupported) {
  TypeFactory factory;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Type* map_type,
      factory.MakeMapType(types::DateRangeType(), factory.get_time()));
  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_MAP_TYPE);
  EXPECT_FALSE(map_type->IsSupportedType(language_options));

  language_options.EnableLanguageFeature(FEATURE_V_1_2_CIVIL_TIME);
  EXPECT_FALSE(map_type->IsSupportedType(language_options));

  language_options.EnableLanguageFeature(FEATURE_RANGE_TYPE);
  EXPECT_TRUE(map_type->IsSupportedType(language_options));
}

TEST(TypeTest, TestNamesValid) {
  TypeFactory factory;
  zetasql_test__::KitchenSinkPB kitchen_sink;
  const ProtoType* proto_type;
  ZETASQL_EXPECT_OK(factory.MakeProtoType(kitchen_sink.GetDescriptor(), &proto_type));
  EXPECT_THAT(proto_type, NotNull());

  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* map_type,
                       factory.MakeMapType(factory.get_string(), proto_type));

  EXPECT_EQ(map_type->DebugString(),
            "MAP<STRING, "
            "PROTO<zetasql_test__.KitchenSinkPB>>");
  EXPECT_EQ(map_type->ShortTypeName(PRODUCT_INTERNAL),
            "MAP<STRING, "
            "zetasql_test__.KitchenSinkPB>");
  EXPECT_EQ(map_type->TypeName(PRODUCT_INTERNAL),
            "MAP<STRING, "
            "`zetasql_test__.KitchenSinkPB`>");
}

TEST(TypeTest, TestNamesValidWithNesting) {
  TypeFactory factory;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Type* map_type,
      factory.MakeMapType(factory.get_string(), factory.get_string()));
  const StructType* struct_type;
  ZETASQL_ASSERT_OK(factory.MakeStructType({{"a", map_type}}, &struct_type));
  const ArrayType* array_type;
  ZETASQL_ASSERT_OK(factory.MakeArrayType(struct_type, &array_type));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* outer_map_type,
                       factory.MakeMapType(array_type, array_type));

  EXPECT_EQ(outer_map_type->DebugString(),
            "MAP<ARRAY<STRUCT<a MAP<STRING, STRING>>>, ARRAY<STRUCT<a "
            "MAP<STRING, STRING>>>>");

  EXPECT_EQ(outer_map_type->ShortTypeName(PRODUCT_INTERNAL),
            "MAP<ARRAY<STRUCT<a MAP<STRING, STRING>>>, ARRAY<STRUCT<a "
            "MAP<STRING, STRING>>>>");
  EXPECT_EQ(outer_map_type->ShortTypeName(PRODUCT_EXTERNAL),
            "MAP<ARRAY<STRUCT<a MAP<STRING, STRING>>>, ARRAY<STRUCT<a "
            "MAP<STRING, STRING>>>>");

  EXPECT_EQ(outer_map_type->TypeName(PRODUCT_INTERNAL),
            "MAP<ARRAY<STRUCT<a MAP<STRING, STRING>>>, ARRAY<STRUCT<a "
            "MAP<STRING, STRING>>>>");
  EXPECT_EQ(outer_map_type->TypeName(PRODUCT_EXTERNAL),
            "MAP<ARRAY<STRUCT<a MAP<STRING, STRING>>>, ARRAY<STRUCT<a "
            "MAP<STRING, STRING>>>>");
}
TEST(TypeTest, MapTypeWithStructValid) {
  TypeFactory factory;
  const StructType* struct_type;
  ZETASQL_ASSERT_OK(
      factory.MakeStructType({{"a", factory.get_string()}}, &struct_type));

  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* map_type,
                       factory.MakeMapType(struct_type, struct_type));
  BasicMapAsserts(map_type);

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_MAP_TYPE);
  EXPECT_FALSE(map_type->IsSupportedType(language_options));
  language_options.EnableLanguageFeature(FEATURE_V_1_2_GROUP_BY_STRUCT);
  EXPECT_TRUE(map_type->IsSupportedType(language_options));
}

TEST(MapTest, MapTypeWithArrayValid) {
  TypeFactory factory;
  const ArrayType* array_type;
  ZETASQL_ASSERT_OK(factory.MakeArrayType(factory.get_string(), &array_type));

  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* map_type,
                       factory.MakeMapType(array_type, array_type));
  BasicMapAsserts(map_type);

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_MAP_TYPE);
  EXPECT_FALSE(map_type->IsSupportedType(language_options));
  language_options.EnableLanguageFeature(FEATURE_V_1_2_GROUP_BY_ARRAY);
  EXPECT_TRUE(map_type->IsSupportedType(language_options));
}

TEST(MapTest, MapTypeWithRangeValid) {
  TypeFactory factory;
  const RangeType* range_type;
  ZETASQL_ASSERT_OK(factory.MakeRangeType(factory.get_timestamp(), &range_type));

  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* map_type,
                       factory.MakeMapType(range_type, range_type));
  BasicMapAsserts(map_type);

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_MAP_TYPE);
  EXPECT_FALSE(map_type->IsSupportedType(language_options));
  language_options.EnableLanguageFeature(FEATURE_RANGE_TYPE);
  EXPECT_TRUE(map_type->IsSupportedType(language_options));
}

TEST(MapTest, MapTypeWithProtoValid) {
  TypeFactory factory;

  zetasql_test__::KitchenSinkPB kitchen_sink;
  const ProtoType* proto_type;

  ZETASQL_EXPECT_OK(factory.MakeProtoType(kitchen_sink.GetDescriptor(), &proto_type));
  EXPECT_THAT(proto_type, NotNull());

  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* map_type,
                       factory.MakeMapType(factory.get_string(), proto_type));
  BasicMapAsserts(map_type);

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_MAP_TYPE);
  EXPECT_TRUE(map_type->IsSupportedType(language_options));
}
TEST(MapTest, MapTypeWithEnumValid) {
  TypeFactory factory;

  const EnumType* enum_type;
  const EnumDescriptor* enum_descriptor = zetasql_test__::TestEnum_descriptor();
  ZETASQL_EXPECT_OK(factory.MakeEnumType(enum_descriptor, &enum_type));
  EXPECT_THAT(enum_type, NotNull());

  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* map_type,
                       factory.MakeMapType(enum_type, enum_type));
  BasicMapAsserts(map_type);

  LanguageOptions language_options;
  language_options.EnableLanguageFeature(FEATURE_V_1_4_MAP_TYPE);
  EXPECT_TRUE(map_type->IsSupportedType(language_options));
}

INSTANTIATE_TEST_SUITE_P(
    MapTest, MapTestFormatValueContentDebugMode,
    testing::ValuesIn(std::initializer_list<std::tuple<Value, std::string>>{
        {
            test_values::Map({{"a", true}}),
            R"({"a": true})",
        },
        {
            test_values::Map(
                {{"a", true}, {"b", false}, {"c", Value::NullBool()}}),
            R"({"a": true, "b": false, "c": NULL})",
        },
        {
            test_values::Map({{"foobar", Value::Int32(1)},
                              {"zoobar", Value::Int32(2)}}),
            R"({"foobar": 1, "zoobar": 2})",
        },
        {
            test_values::Map({{"a", test_values::Array({Value::Int32(1),
                                                        Value::Int32(2)})}}),
            R"({"a": Array[Int32(1), Int32(2)]})",
        },
        {
            test_values::Map(
                {{"nested",
                  test_values::Map(
                      {{"a", test_values::Map(
                                 {{"b", test_values::Map(
                                            {{"c", Value::Int32(1)}})}})}})}}),
            R"({"nested": {"a": {"b": {"c": 1}}}})",
        },
        {
            test_values::Map(
                {{"nested",
                  test_values::Struct(
                      {{"field",
                        test_values::Map(
                            {{"a", test_values::Map(
                                       {{"b", Value::Int32(1)}})}})}})}}),
            R"({"nested": Struct{field:Map<String, Map<String, Int32>>({"a": {"b": 1}})}})",
        },
    }));

TEST_P(MapTestFormatValueContentDebugMode, FormatValueContentDebugMode) {
  auto& [map_value, expected_format_str] = GetParam();

  Type::FormatValueContentOptions options;
  options.mode = Type::FormatValueContentOptions::Mode::kDebug;
  options.verbose = true;

  EXPECT_EQ(
      map_value.type()->FormatValueContent(map_value.GetContent(), options),
      expected_format_str);
}

TEST(MapTest, FormatValueContentDebugModeEmptyMap) {
  TypeFactory factory;

  Type::FormatValueContentOptions options;
  options.mode = Type::FormatValueContentOptions::Mode::kDebug;
  options.verbose = true;

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Type* map_type,
      factory.MakeMapType(factory.get_string(), factory.get_int64()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(Value map_value, Value::MakeMap(map_type, {}));
  EXPECT_EQ(map_type->FormatValueContent(map_value.GetContent(), options),
            "{}");
}

TEST(TypeFactoryTest, MapTypesAreCached) {
  TypeFactory factory;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Type* type1,
      factory.MakeMapType(factory.get_int64(), factory.get_double()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const Type* type2,
      factory.MakeMapType(factory.get_int64(), factory.get_double()));
  EXPECT_TRUE(type1 == type2)
      << "Expected the two type pointers to be identical";
}

TEST_P(MapTestAllSimpleTypes, MapWithSimpleTypesUsesStaticFactory) {
  TypeFactory factory;
  TypeKind type_kind = GetParam();
  const Type* simple_type = types::TypeFromSimpleTypeKind(type_kind);

  const auto initial_factory_size = factory.GetEstimatedOwnedMemoryBytesSize();
  ZETASQL_ASSERT_OK(factory.MakeMapType(simple_type, simple_type));

  // Our factory should not change size, because the static factory was used.
  ASSERT_EQ(initial_factory_size, factory.GetEstimatedOwnedMemoryBytesSize());
}

TEST(TypeFactoryTest, MapWithComplexTypesUsesInstanceFactory) {
  TypeFactory factory;
  const Type* struct_type;
  ZETASQL_ASSERT_OK(factory.MakeStructType({{"a", types::Int32Type()}}, &struct_type));

  const auto initial_factory_size = factory.GetEstimatedOwnedMemoryBytesSize();
  ZETASQL_ASSERT_OK(factory.MakeMapType(struct_type, types::Int32Type()));

  ASSERT_NE(initial_factory_size, factory.GetEstimatedOwnedMemoryBytesSize());
}

}  // namespace zetasql
