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

#include "zetasql/public/types/collation.h"

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {

namespace {

const ArrayType* MakeArrayType(const Type* element_type,
                               TypeFactory* type_factory) {
  const ArrayType* array_type;
  ZETASQL_CHECK_OK(type_factory->MakeArrayType(element_type, &array_type));
  return array_type;
}

const StructType* MakeStructType(
    absl::Span<const StructType::StructField> fields,
    TypeFactory* type_factory) {
  const StructType* struct_type;
  ZETASQL_CHECK_OK(type_factory->MakeStructType(
      std::vector<StructType::StructField>(fields.begin(), fields.end()),
      &struct_type));
  return struct_type;
}

// Make type
//   STRUCT< a STRING, b ARRAY < STRUCT < a STRING, b INT64 > > >
const Type* MakeNestedStructType(TypeFactory* type_factory) {
  return MakeStructType(
      {{"a", types::StringType()},
       {"b", MakeArrayType(MakeStructType({{"a", types::StringType()},
                                           {"b", types::Int64Type()}},
                                          type_factory),
                           type_factory)}},
      type_factory);
}

}  // namespace

TEST(CollationTest, Creation) {
  {
    // Test empty collation name. An empty Collation should be created.
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(types::StringType());
    annotation_map->SetAnnotation(static_cast<int>(AnnotationKind::kCollation),
                                  SimpleValue::String(""));
    ZETASQL_ASSERT_OK_AND_ASSIGN(Collation collation,
                         Collation::MakeCollation(*annotation_map));

    // Test serialization / deserialization.
    CollationProto proto;
    ZETASQL_ASSERT_OK(collation.Serialize(&proto));
    ZETASQL_ASSERT_OK_AND_ASSIGN(Collation deserialized_collation,
                         Collation::Deserialize(proto));
    ASSERT_TRUE(collation.Equals(deserialized_collation));

    EXPECT_EQ(collation.CollationName(), "");
    ASSERT_TRUE(collation.Empty());
    EXPECT_EQ(collation.DebugString(), "_");
  }
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(types::StringType());
    annotation_map->SetAnnotation(static_cast<int>(AnnotationKind::kCollation),
                                  SimpleValue::String("unicode:ci"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(Collation collation,
                         Collation::MakeCollation(*annotation_map));

    // Test serialization / deserialization.
    CollationProto proto;
    ZETASQL_ASSERT_OK(collation.Serialize(&proto));
    ZETASQL_ASSERT_OK_AND_ASSIGN(Collation deserialized_collation,
                         Collation::Deserialize(proto));
    ASSERT_TRUE(collation.Equals(deserialized_collation));

    EXPECT_EQ(collation.CollationName(), "unicode:ci");
    EXPECT_EQ(collation.num_children(), 0);
    EXPECT_EQ(collation.DebugString(), "unicode:ci");
  }
  {
    // Test empty nested annotation map.
    TypeFactory type_factory;
    // STRUCT< a STRING, b ARRAY < STRUCT < a STRING, b INT64 > > >
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(MakeNestedStructType(&type_factory));
    ZETASQL_ASSERT_OK_AND_ASSIGN(Collation collation,
                         Collation::MakeCollation(*annotation_map));
    EXPECT_TRUE(collation.Empty());

    // Test serialization / deserialization.
    CollationProto proto;
    ZETASQL_ASSERT_OK(collation.Serialize(&proto));
    ZETASQL_ASSERT_OK_AND_ASSIGN(Collation deserialized_collation,
                         Collation::Deserialize(proto));
    ASSERT_TRUE(collation.Equals(deserialized_collation));
    EXPECT_EQ(collation.DebugString(), "_");
  }
  {
    // Test struct with the first field having collation.
    TypeFactory type_factory;
    // STRUCT< a STRING, b ARRAY < STRUCT < a STRING, b INT64 > > >
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(MakeNestedStructType(&type_factory));
    // Set collation on a.
    annotation_map->AsStructMap()->mutable_field(0)->SetAnnotation(
        static_cast<int>(AnnotationKind::kCollation),
        SimpleValue::String("unicode:ci"));

    ZETASQL_ASSERT_OK_AND_ASSIGN(Collation collation,
                         Collation::MakeCollation(*annotation_map));

    EXPECT_FALSE(collation.HasCollation());
    EXPECT_EQ(collation.num_children(), 2);
    EXPECT_TRUE(collation.child(0).HasCollation());
    EXPECT_EQ(collation.child(0).CollationName(), "unicode:ci");
    EXPECT_EQ(collation.child(0).num_children(), 0);
    EXPECT_TRUE(collation.child(1).Empty());

    // Test serialization / deserialization.
    CollationProto proto;
    ZETASQL_ASSERT_OK(collation.Serialize(&proto));
    ZETASQL_ASSERT_OK_AND_ASSIGN(Collation deserialized_collation,
                         Collation::Deserialize(proto));
    ASSERT_TRUE(collation.Equals(deserialized_collation));
    EXPECT_EQ(collation.DebugString(), "[unicode:ci,_]");
  }

  {
    // Test struct with nested array child having collation.
    TypeFactory type_factory;
    // STRUCT< a STRING, b ARRAY < STRUCT < a STRING, b INT64 > > >
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(MakeNestedStructType(&type_factory));
    // Set collation on b.[].a
    annotation_map->AsStructMap()
        ->mutable_field(1)
        ->AsArrayMap()
        ->mutable_element()
        ->AsStructMap()
        ->mutable_field(0)
        ->SetAnnotation(static_cast<int>(AnnotationKind::kCollation),
                        SimpleValue::String("unicode:ci"));
    annotation_map->Normalize();

    ZETASQL_ASSERT_OK_AND_ASSIGN(Collation collation,
                         Collation::MakeCollation(*annotation_map));

    EXPECT_FALSE(collation.HasCollation());
    EXPECT_EQ(collation.num_children(), 2);
    EXPECT_TRUE(collation.child(0).Empty());
    EXPECT_EQ(collation.child(1).num_children(), 1);
    EXPECT_EQ(collation.child(1).child(0).num_children(), 2);
    EXPECT_EQ(collation.child(1).child(0).child(0).CollationName(),
              "unicode:ci");
    EXPECT_TRUE(collation.child(1).child(0).child(1).Empty());
    EXPECT_EQ(collation.DebugString(), "[_,[[unicode:ci,_]]]");

    // Test serialization / deserialization.
    CollationProto proto;
    ZETASQL_ASSERT_OK(collation.Serialize(&proto));
    ZETASQL_ASSERT_OK_AND_ASSIGN(Collation deserialized_collation,
                         Collation::Deserialize(proto));
    ASSERT_TRUE(collation.Equals(deserialized_collation));
  }
}

TEST(CollationTest, EqualAndCompatibilityTest) {
  std::unique_ptr<AnnotationMap> single_string_annotation_map =
      AnnotationMap::Create(types::StringType());
  ZETASQL_ASSERT_OK_AND_ASSIGN(Collation empty_single_string,
                       Collation::MakeCollation(*single_string_annotation_map));
  Collation non_empty_single_string = Collation::MakeScalar("unicode:ci");

  EXPECT_FALSE(empty_single_string.Equals(non_empty_single_string));
  EXPECT_FALSE(non_empty_single_string.Equals(empty_single_string));
  EXPECT_TRUE(empty_single_string.HasCompatibleStructure(types::StringType()));
  EXPECT_TRUE(
      non_empty_single_string.HasCompatibleStructure(types::StringType()));
  EXPECT_FALSE(
      non_empty_single_string.HasCompatibleStructure(types::Int64Type()));

  // Test struct with the first field having collation.
  TypeFactory type_factory;
  // STRUCT< a STRING, b ARRAY < STRUCT < a STRING, b INT64 > > >
  const StructType* struct_type =
      MakeNestedStructType(&type_factory)->AsStruct();
  const ArrayType* array_type = struct_type->field(1).type->AsArray();
  std::unique_ptr<AnnotationMap> struct_annotation_map =
      AnnotationMap::Create(struct_type);
  ZETASQL_ASSERT_OK_AND_ASSIGN(Collation empty_struct,
                       Collation::MakeCollation(*struct_annotation_map));

  // Set collation on a and b.a
  struct_annotation_map->AsStructMap()->mutable_field(0)->SetAnnotation(
      static_cast<int>(AnnotationKind::kCollation),
      SimpleValue::String("unicode:ci"));
  struct_annotation_map->AsStructMap()
      ->mutable_field(1)
      ->AsArrayMap()
      ->mutable_element()
      ->AsStructMap()
      ->mutable_field(0)
      ->SetAnnotation(static_cast<int>(AnnotationKind::kCollation),
                      SimpleValue::String("unicode:ci"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(Collation non_empty_struct,
                       Collation::MakeCollation(*struct_annotation_map));

  Collation non_empty_array = non_empty_struct.child(1);

  EXPECT_FALSE(empty_struct.Equals(non_empty_struct));
  EXPECT_TRUE(empty_struct.HasCompatibleStructure(struct_type));
  EXPECT_TRUE(empty_struct.HasCompatibleStructure(array_type));

  EXPECT_FALSE(non_empty_struct.Equals(empty_struct));
  EXPECT_TRUE(non_empty_struct.HasCompatibleStructure(struct_type));
  EXPECT_FALSE(non_empty_struct.HasCompatibleStructure(array_type));
  EXPECT_FALSE(non_empty_struct.HasCompatibleStructure(types::StringType()));

  EXPECT_FALSE(non_empty_array.Equals(non_empty_struct));
  EXPECT_TRUE(non_empty_array.HasCompatibleStructure(array_type));
  EXPECT_FALSE(non_empty_array.HasCompatibleStructure(struct_type));
  EXPECT_FALSE(non_empty_array.HasCompatibleStructure(types::StringType()));

  EXPECT_FALSE(non_empty_single_string.HasCompatibleStructure(struct_type));
  EXPECT_FALSE(non_empty_single_string.HasCompatibleStructure(array_type));

  // Cross comparison between single_string and struct.
  EXPECT_TRUE(empty_single_string.Equals(empty_struct));
  EXPECT_TRUE(empty_struct.Equals(empty_single_string));
  EXPECT_FALSE(non_empty_single_string.Equals(empty_struct));
  EXPECT_FALSE(empty_struct.Equals(non_empty_single_string));
  EXPECT_FALSE(empty_single_string.Equals(non_empty_struct));
  EXPECT_FALSE(non_empty_struct.Equals(empty_single_string));
  EXPECT_FALSE(non_empty_single_string.Equals(non_empty_struct));
  EXPECT_FALSE(non_empty_struct.Equals(non_empty_single_string));
}

}  // namespace zetasql
