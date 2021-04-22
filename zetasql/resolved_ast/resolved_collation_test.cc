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

#include "zetasql/resolved_ast/resolved_collation.h"

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

TEST(ResolvedCollationTest, Creation) {
  {
    // Test empty collation name. An empty ResolvedCollation should be created.
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(types::StringType());
    annotation_map->SetAnnotation(static_cast<int>(AnnotationKind::COLLATION),
                                  SimpleValue::String(""));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        ResolvedCollation resolved_collation,
        ResolvedCollation::MakeResolvedCollation(*annotation_map));

    // Test serialization / deserialization.
    ResolvedCollationProto proto;
    ZETASQL_ASSERT_OK(resolved_collation.Serialize(&proto));
    ZETASQL_ASSERT_OK_AND_ASSIGN(ResolvedCollation deserialized_collation,
                         ResolvedCollation::Deserialize(proto));
    ASSERT_TRUE(resolved_collation.Equals(deserialized_collation));

    EXPECT_EQ(resolved_collation.CollationName(), "");
    ASSERT_TRUE(resolved_collation.Empty());
  }
  {
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(types::StringType());
    annotation_map->SetAnnotation(static_cast<int>(AnnotationKind::COLLATION),
                                  SimpleValue::String("unicode:ci"));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        ResolvedCollation resolved_collation,
        ResolvedCollation::MakeResolvedCollation(*annotation_map));

    // Test serialization / deserialization.
    ResolvedCollationProto proto;
    ZETASQL_ASSERT_OK(resolved_collation.Serialize(&proto));
    ZETASQL_ASSERT_OK_AND_ASSIGN(ResolvedCollation deserialized_collation,
                         ResolvedCollation::Deserialize(proto));
    ASSERT_TRUE(resolved_collation.Equals(deserialized_collation));

    EXPECT_EQ(resolved_collation.CollationName(), "unicode:ci");
    EXPECT_EQ(resolved_collation.num_children(), 0);
  }
  {
    // Test empty nested annotation map.
    TypeFactory type_factory;
    // STRUCT< a STRING, b ARRAY < STRUCT < a STRING, b INT64 > > >
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(MakeNestedStructType(&type_factory));
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        ResolvedCollation resolved_collation,
        ResolvedCollation::MakeResolvedCollation(*annotation_map));
    EXPECT_TRUE(resolved_collation.Empty());

    // Test serialization / deserialization.
    ResolvedCollationProto proto;
    ZETASQL_ASSERT_OK(resolved_collation.Serialize(&proto));
    ZETASQL_ASSERT_OK_AND_ASSIGN(ResolvedCollation deserialized_collation,
                         ResolvedCollation::Deserialize(proto));
    ASSERT_TRUE(resolved_collation.Equals(deserialized_collation));
  }
  {
    // Test struct with the first field having collation.
    TypeFactory type_factory;
    // STRUCT< a STRING, b ARRAY < STRUCT < a STRING, b INT64 > > >
    std::unique_ptr<AnnotationMap> annotation_map =
        AnnotationMap::Create(MakeNestedStructType(&type_factory));
    // Set collation on a.
    annotation_map->AsStructMap()->mutable_field(0)->SetAnnotation(
        static_cast<int>(AnnotationKind::COLLATION),
        SimpleValue::String("unicode:ci"));

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        ResolvedCollation resolved_collation,
        ResolvedCollation::MakeResolvedCollation(*annotation_map));

    EXPECT_FALSE(resolved_collation.HasCollation());
    EXPECT_EQ(resolved_collation.num_children(), 2);
    EXPECT_TRUE(resolved_collation.child(0).HasCollation());
    EXPECT_EQ(resolved_collation.child(0).CollationName(), "unicode:ci");
    EXPECT_EQ(resolved_collation.child(0).num_children(), 0);
    EXPECT_TRUE(resolved_collation.child(1).Empty());

    // Test serialization / deserialization.
    ResolvedCollationProto proto;
    ZETASQL_ASSERT_OK(resolved_collation.Serialize(&proto));
    ZETASQL_ASSERT_OK_AND_ASSIGN(ResolvedCollation deserialized_collation,
                         ResolvedCollation::Deserialize(proto));
    ASSERT_TRUE(resolved_collation.Equals(deserialized_collation));
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
        ->SetAnnotation(static_cast<int>(AnnotationKind::COLLATION),
                        SimpleValue::String("unicode:ci"));
    annotation_map->Normalize();

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        ResolvedCollation resolved_collation,
        ResolvedCollation::MakeResolvedCollation(*annotation_map));

    EXPECT_FALSE(resolved_collation.HasCollation());
    EXPECT_EQ(resolved_collation.num_children(), 2);
    EXPECT_TRUE(resolved_collation.child(0).Empty());
    EXPECT_EQ(resolved_collation.child(1).num_children(), 1);
    EXPECT_EQ(resolved_collation.child(1).child(0).num_children(), 2);
    EXPECT_EQ(resolved_collation.child(1).child(0).child(0).CollationName(),
              "unicode:ci");
    EXPECT_TRUE(resolved_collation.child(1).child(0).child(1).Empty());

    // Test serialization / deserialization.
    ResolvedCollationProto proto;
    ZETASQL_ASSERT_OK(resolved_collation.Serialize(&proto));
    ZETASQL_ASSERT_OK_AND_ASSIGN(ResolvedCollation deserialized_collation,
                         ResolvedCollation::Deserialize(proto));
    ASSERT_TRUE(resolved_collation.Equals(deserialized_collation));
  }
}

TEST(ResolvedCollationTest, EqualTest) {
  std::unique_ptr<AnnotationMap> single_string_annotation_map =
      AnnotationMap::Create(types::StringType());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ResolvedCollation empty_single_string,
      ResolvedCollation::MakeResolvedCollation(*single_string_annotation_map));
  single_string_annotation_map->SetAnnotation(
      static_cast<int>(AnnotationKind::COLLATION),
      SimpleValue::String("unicode:ci"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ResolvedCollation non_empty_single_string,
      ResolvedCollation::MakeResolvedCollation(*single_string_annotation_map));

  EXPECT_FALSE(empty_single_string.Equals(non_empty_single_string));
  EXPECT_FALSE(non_empty_single_string.Equals(empty_single_string));

  // Test struct with the first field having collation.
  TypeFactory type_factory;
  // STRUCT< a STRING, b ARRAY < STRUCT < a STRING, b INT64 > > >
  std::unique_ptr<AnnotationMap> struct_annotation_map =
      AnnotationMap::Create(MakeNestedStructType(&type_factory));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ResolvedCollation empty_struct,
      ResolvedCollation::MakeResolvedCollation(*struct_annotation_map));

  // Set collation on a.
  struct_annotation_map->AsStructMap()->mutable_field(0)->SetAnnotation(
      static_cast<int>(AnnotationKind::COLLATION),
      SimpleValue::String("unicode:ci"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      ResolvedCollation non_empty_struct,
      ResolvedCollation::MakeResolvedCollation(*struct_annotation_map));

  EXPECT_FALSE(empty_struct.Equals(non_empty_struct));
  EXPECT_FALSE(non_empty_struct.Equals(empty_struct));

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
