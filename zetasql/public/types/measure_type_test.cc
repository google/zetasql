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

#include <memory>

#include "zetasql/base/testing/status_matchers.h"  
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/collation.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"

namespace zetasql {

namespace {

using testing::HasSubstr;
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

TEST(MeasureTypeTest, TestNames) {
  TypeFactory factory;
  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* measure_type_with_int64,
                       factory.MakeMeasureType(types::Int64Type()));
  EXPECT_EQ(measure_type_with_int64->DebugString(), "MEASURE<INT64>");
  EXPECT_EQ(measure_type_with_int64->ShortTypeName(PRODUCT_INTERNAL),
            "MEASURE<INT64>");
  EXPECT_EQ(measure_type_with_int64->TypeName(PRODUCT_INTERNAL),
            "MEASURE<INT64>");

  zetasql_test__::KitchenSinkPB kitchen_sink;
  const ProtoType* proto_type = nullptr;
  ZETASQL_EXPECT_OK(factory.MakeProtoType(kitchen_sink.GetDescriptor(), &proto_type));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* measure_type_with_proto,
                       factory.MakeMeasureType(proto_type));
  EXPECT_EQ(measure_type_with_proto->DebugString(),
            "MEASURE<PROTO<zetasql_test__.KitchenSinkPB>>");
  EXPECT_EQ(measure_type_with_proto->ShortTypeName(PRODUCT_INTERNAL),
            "MEASURE<zetasql_test__.KitchenSinkPB>");
  EXPECT_EQ(measure_type_with_proto->TypeName(PRODUCT_INTERNAL),
            "MEASURE<`zetasql_test__.KitchenSinkPB`>");

  const StructType* struct_type;
  ZETASQL_ASSERT_OK(factory.MakeStructType({{"a", types::StringType()}}, &struct_type));
  const ArrayType* array_type;
  ZETASQL_ASSERT_OK(factory.MakeArrayType(struct_type, &array_type));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* measure_type_with_nested,
                       factory.MakeMeasureType(array_type));
  EXPECT_EQ(measure_type_with_nested->DebugString(),
            "MEASURE<ARRAY<STRUCT<a STRING>>>");
  EXPECT_EQ(measure_type_with_nested->ShortTypeName(PRODUCT_INTERNAL),
            "MEASURE<ARRAY<STRUCT<a STRING>>>");
  EXPECT_EQ(measure_type_with_nested->TypeName(PRODUCT_INTERNAL),
            "MEASURE<ARRAY<STRUCT<a STRING>>>");
}

TEST(MeasureTypeTest, TestTypeNameWithModifiers) {
  TypeFactory factory;
  ZETASQL_ASSERT_OK_AND_ASSIGN(const Type* measure_type_with_string,
                       factory.MakeMeasureType(types::StringType()));

  // No modifiers prints the same as TypeName() and other functions.
  TypeModifiers modifiers_empty =
      TypeModifiers::MakeTypeModifiers(TypeParameters(), Collation());
  EXPECT_THAT(measure_type_with_string->TypeNameWithModifiers(modifiers_empty,
                                                              PRODUCT_INTERNAL),
              IsOkAndHolds("MEASURE<STRING>"));

  // String length parameter on the result type.
  StringTypeParametersProto string_parameters_proto;
  string_parameters_proto.set_max_length(10);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      TypeParameters string_parameters,
      TypeParameters::MakeStringTypeParameters(string_parameters_proto));
  TypeParameters parameters_string_max_length =
      TypeParameters::MakeTypeParametersWithChildList({string_parameters});
  TypeModifiers modifiers_string_max_length = TypeModifiers::MakeTypeModifiers(
      parameters_string_max_length, Collation());
  EXPECT_THAT(measure_type_with_string->TypeNameWithModifiers(
                  modifiers_string_max_length, PRODUCT_INTERNAL),
              IsOkAndHolds("MEASURE<STRING(10)>"));

  // Invalid type parameter on the MEASURE itself instead of the result type.
  TypeModifiers modifiers_bad_type_param =
      TypeModifiers::MakeTypeModifiers(string_parameters, Collation());
  EXPECT_THAT(
      measure_type_with_string->TypeNameWithModifiers(modifiers_bad_type_param,
                                                      PRODUCT_INTERNAL),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr(
                   "Input type parameter does not correspond to MeasureType")));

  // Collation annotation on the result type.
  std::unique_ptr<AnnotationMap> annotation_map =
      AnnotationMap::Create(types::StringType());
  annotation_map->SetAnnotation(static_cast<int>(AnnotationKind::kCollation),
                                SimpleValue::String("und:ci"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Collation string_collation,
                       Collation::MakeCollation(*annotation_map));
  Collation measure_collation =
      Collation::MakeCollationWithChildList({string_collation});
  TypeModifiers modifiers_collation =
      TypeModifiers::MakeTypeModifiers(TypeParameters(), measure_collation);
  EXPECT_THAT(measure_type_with_string->TypeNameWithModifiers(
                  modifiers_collation, PRODUCT_INTERNAL),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("MeasureType does not support collation")));
}

// TODO: b/350555383 - Add tests for all class methods.

}  // namespace

}  // namespace zetasql
