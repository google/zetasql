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

#include "zetasql/resolved_ast/resolved_node.h"

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/types/type_parameters.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {

static TypeParameters MakeStringTypeParameters(int max_length) {
  StringTypeParametersProto proto;
  proto.set_max_length(max_length);
  absl::StatusOr<TypeParameters> string_type_parameters_or_error =
      TypeParameters::MakeStringTypeParameters(proto);
  ZETASQL_EXPECT_OK(string_type_parameters_or_error.status());
  return *string_type_parameters_or_error;
}

static TypeParameters MakeNumericTypeParameters(int precision, int scale) {
  NumericTypeParametersProto proto;
  proto.set_precision(precision);
  proto.set_scale(scale);
  absl::StatusOr<TypeParameters> numeric_type_parameters_or_error =
      TypeParameters::MakeNumericTypeParameters(proto);
  ZETASQL_EXPECT_OK(numeric_type_parameters_or_error.status());
  return *numeric_type_parameters_or_error;
}

TEST(ResolvedColumnDefinitionTest, TestGetFullTypeParameters) {
  // Column type is STRUCT<INT64, STRING(10), ARRAY<NUMERIC(10,5)>, DATE>
  TypeFactory type_factory;
  const Type *numeric_array = nullptr;
  ZETASQL_EXPECT_OK(
      type_factory.MakeArrayType(type_factory.get_numeric(), &numeric_array));
  std::vector<StructType::StructField> struct_fields = {
      {"f1", type_factory.get_int64()},
      {"f2", type_factory.get_string()},
      {"f3", numeric_array},
      {"f4", type_factory.get_date()}};
  const Type *struct_type = nullptr;
  ZETASQL_EXPECT_OK(type_factory.MakeStructType(struct_fields, &struct_type));

  // Constructs annotation for
  // STRUCT<INT64, STRING(10), ARRAY<NUMERIC(10,5)>, DATE> type.
  // Only INT64 and STRING(10) and ARRAY<NUMERIC(10,5)> have annotations. DATE
  // has no annotation since it doesn't have type parameters and is the end of
  // the list.
  std::vector<std::unique_ptr<const ResolvedColumnAnnotations>>
      child_annotations;
  child_annotations.push_back(MakeResolvedColumnAnnotations(
      /*collation_name=*/nullptr, /*not_null=*/false, /*option_list=*/{},
      /*child_list=*/{}, TypeParameters()));
  child_annotations.push_back(MakeResolvedColumnAnnotations(
      /*collation_name=*/nullptr, /*not_null=*/false, /*option_list=*/{},
      /*child_list=*/{}, MakeStringTypeParameters(10)));
  std::vector<std::unique_ptr<const ResolvedColumnAnnotations>>
      grand_child_annotations;
  grand_child_annotations.push_back(MakeResolvedColumnAnnotations(
      /*collation_name=*/nullptr, /*not_null=*/false, /*option_list=*/{},
      /*child_list=*/{}, MakeNumericTypeParameters(10, 5)));
  child_annotations.push_back(MakeResolvedColumnAnnotations(
      /*collation_name=*/nullptr, /*not_null=*/false, /*option_list=*/{},
      std::move(grand_child_annotations), TypeParameters()));
  std::unique_ptr<const ResolvedColumnAnnotations> annotations =
      MakeResolvedColumnAnnotations(
          /*collation_name=*/nullptr, /*not_null=*/false, /*option_list=*/{},
          std::move(child_annotations), TypeParameters());

  // Constructs ResolvedColumnDefinition.
  zetasql::ResolvedColumn resolved_column1;
  std::unique_ptr<const ResolvedColumnDefinition> resolved_column_definition =
      MakeResolvedColumnDefinition("test_column", struct_type,
                                   std::move(annotations),
                                   /*is_hidden=*/false, resolved_column1,
                                   /*generated_column_info=*/{},
                                   /*default_value=*/{});

  absl::StatusOr<TypeParameters> type_parameters_or_error =
      resolved_column_definition->GetFullTypeParameters();
  ZETASQL_EXPECT_OK(type_parameters_or_error.status());
  EXPECT_EQ(type_parameters_or_error->DebugString(),
            "[null,(max_length=10),[(precision=10,scale=5)],null]");
}

}  // namespace zetasql
