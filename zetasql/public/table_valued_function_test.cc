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

#include "zetasql/public/table_valued_function.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/proto/function.pb.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/simple_value.h"
#include "zetasql/public/types/type_deserializer.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/status/status.h"

namespace zetasql {

using ::testing::HasSubstr;
using ::testing::IsNull;
using ::testing::NotNull;
using ::zetasql_base::testing::StatusIs;

void ExpectEqualTVFSchemaColumn(const TVFSchemaColumn& column1,
                                const TVFSchemaColumn& column2) {
  EXPECT_EQ(column1.name, column2.name);
  EXPECT_EQ(column1.is_pseudo_column, column2.is_pseudo_column);
  EXPECT_TRUE(column1.type->Equals(column2.type));
  AnnotationMap::Equals(column1.annotation_map, column2.annotation_map);
  EXPECT_EQ(column1.name_parse_location_range,
            column2.name_parse_location_range);
  EXPECT_EQ(column1.type_parse_location_range,
            column2.type_parse_location_range);
}

void ExpectEqualTVFRelations(const TVFRelation& relation1,
                             const TVFRelation& relation2) {
  EXPECT_EQ(relation1.is_value_table(), relation2.is_value_table());
  ASSERT_EQ(relation1.num_columns(), relation2.num_columns());
  for (int i = 0; i < relation1.num_columns(); ++i) {
    ExpectEqualTVFSchemaColumn(relation1.column(i), relation2.column(i));
  }
}

// Serializes given TVFRelation first. Then deserializes and returns the
// deserialized TVFRelation.
void SerializeDeserializeAndCompare(const TVFRelation& relation) {
  FileDescriptorSetMap file_descriptor_set_map;
  TVFRelationProto tvf_relation_proto;
  ZETASQL_ASSERT_OK(relation.Serialize(&file_descriptor_set_map, &tvf_relation_proto));

  std::vector<const google::protobuf::DescriptorPool*> pools(
      file_descriptor_set_map.size());
  for (const auto& pair : file_descriptor_set_map) {
    pools[pair.second->descriptor_set_index] = pair.first;
  }

  TypeFactory type_factory;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      TVFRelation result,
      TVFRelation::Deserialize(tvf_relation_proto,
                               TypeDeserializer(&type_factory, pools)));
  ExpectEqualTVFRelations(relation, result);
}

// Serializes given TVFSchemaColumn first. Then deserializes and returns the
// deserialized TVFSchemaColumn.
void SerializeDeserializeAndCompare(const TVFSchemaColumn& column) {
  FileDescriptorSetMap file_descriptor_set_map;
  ZETASQL_ASSERT_OK_AND_ASSIGN(TVFRelationColumnProto tvf_schema_column,
                       column.ToProto(&file_descriptor_set_map));

  std::vector<const google::protobuf::DescriptorPool*> pools(
      file_descriptor_set_map.size());
  for (const auto& pair : file_descriptor_set_map) {
    pools[pair.second->descriptor_set_index] = pair.first;
  }

  TypeFactory type_factory;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      TVFSchemaColumn result,
      TVFSchemaColumn::FromProto(tvf_schema_column,
                                 TypeDeserializer(&type_factory, pools)));

  ExpectEqualTVFSchemaColumn(column, result);
}

// Check serialization and deserialization of TVFRelation.
TEST(TVFTest, TVFRelationSerializationAndDeserialization) {
  TVFRelation::Column column("Col1", zetasql::types::DoubleType());
  TVFRelation relation({column});

  SerializeDeserializeAndCompare(relation);
}

TEST(TVFTest, TVFRelationSerializationAndDeserializationWithColumnLocations) {
  ParseLocationRange location_range1, location_range2;
  location_range1.set_start(ParseLocationPoint::FromByteOffset("file1", 17));
  location_range1.set_end(ParseLocationPoint::FromByteOffset("file1", 25));

  location_range2.set_start(ParseLocationPoint::FromByteOffset("file1", 29));
  location_range2.set_end(ParseLocationPoint::FromByteOffset("file1", 32));

  TVFRelation::Column column("Col1", zetasql::types::DoubleType());
  // Set parse locations for TVFSchema Column.
  column.name_parse_location_range = location_range1;
  column.type_parse_location_range = location_range2;
  TVFRelation relation({column});

  SerializeDeserializeAndCompare(relation);
  // Test for relation with collated column in value table.
  TypeFactory type_factory;
  std::unique_ptr<AnnotationMap> annotation =
      AnnotationMap::Create(type_factory.get_string());
  annotation->SetAnnotation(static_cast<int>(AnnotationKind::kSampleAnnotation),
                            SimpleValue::String("abc"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const AnnotationMap* annotation_map,
                       type_factory.TakeOwnership(std::move(annotation)));
  TVFSchemaColumn pseudo_column =
      TVFSchemaColumn("pseudo_column", zetasql::types::Int64Type(), true);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      TVFRelation value_table_relation,
      TVFRelation::ValueTable({zetasql::types::DoubleType(), annotation_map},
                              {pseudo_column}));
  SerializeDeserializeAndCompare(relation);
}

// Check serialization and deserialization of TVFSchemaColumn
TEST(TVFTest,
     TVFSchemaColumnSerializationAndDeserializationWithColumnLocations) {
  ParseLocationRange location_range1, location_range2;
  location_range1.set_start(ParseLocationPoint::FromByteOffset("file1", 17));
  location_range1.set_end(ParseLocationPoint::FromByteOffset("file1", 25));

  location_range2.set_start(ParseLocationPoint::FromByteOffset("file1", 29));
  location_range2.set_end(ParseLocationPoint::FromByteOffset("file1", 32));

  TVFRelation::Column column("Col1", zetasql::types::DoubleType());
  // Set parse locations for TVFSchema Column.
  column.name_parse_location_range = location_range1;
  column.type_parse_location_range = location_range2;

  SerializeDeserializeAndCompare(column);

  // Test for column with non-empty <annotation_map>.
  TypeFactory type_factory;
  std::unique_ptr<AnnotationMap> annotation =
      AnnotationMap::Create(type_factory.get_string());
  annotation->SetAnnotation(static_cast<int>(AnnotationKind::kSampleAnnotation),
                            SimpleValue::String("abc"));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const AnnotationMap* annotation_map,
                       type_factory.TakeOwnership(std::move(annotation)));
  TVFRelation::Column column_with_annotations(
      "Col2", {zetasql::types::DoubleType(), annotation_map});
  column_with_annotations.name_parse_location_range = location_range1;
  column_with_annotations.type_parse_location_range = location_range2;

  SerializeDeserializeAndCompare(column_with_annotations);
}

TEST(TVFTest, TestInvalidColumnNameForTVFWithExtraColumns) {
  TypeFactory factory;
  std::unique_ptr<TableValuedFunction> tvf;

  EXPECT_DEATH(
      tvf.reset(new ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
          {"tvf_append_column_empty_name"},
          FunctionSignature(ARG_TYPE_RELATION, {ARG_TYPE_RELATION}, -1),
          {TVFSchemaColumn("", zetasql::types::Int64Type())})),
      "invalid empty column name in extra columns");
}

TEST(TVFTest, TestDuplicateColumnNameForTVFWithExtraColumns) {
  TypeFactory factory;
  std::unique_ptr<TableValuedFunction> tvf;
  TVFSchemaColumn int64_col =
      TVFSchemaColumn("int64_col", zetasql::types::Int64Type());

  EXPECT_DEATH(
      tvf.reset(new ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
          {"tvf_append_column_with_duplicated_names"},
          FunctionSignature(ARG_TYPE_RELATION, {ARG_TYPE_RELATION}, -1),
          {int64_col, int64_col})),
      "extra columns have duplicated column names: int64_col");
}

TEST(TVFTest, TestInvalidNonTemplatedArgumentForTVFWithExtraColumns) {
  TypeFactory factory;
  std::unique_ptr<TableValuedFunction> tvf;
  // Generate an output schema that returns an int64 value table.
  TVFRelation output_schema_int64_value_table =
      TVFRelation::ValueTable(zetasql::types::Int64Type());
  TVFSchemaColumn int64_col =
      TVFSchemaColumn("int64_col", zetasql::types::Int64Type());
  TVFRelation::ColumnList columns = {int64_col};
  TVFRelation tvf_relation(columns);

  EXPECT_DEATH(
      tvf.reset(new ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
          {"tvf_append_column_with_value_table"},
          FunctionSignature(
              FunctionArgumentType::RelationWithSchema(
                  tvf_relation,
                  /*extra_relation_input_columns_allowed=*/false),
              {FunctionArgumentType::RelationWithSchema(
                   output_schema_int64_value_table,
                   /*extra_relation_input_columns_allowed=*/false),
               FunctionArgumentType(ARG_TYPE_ARBITRARY,
                                    FunctionArgumentType::REQUIRED)},
              -1),
          {int64_col})),
      HasSubstr("ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF must "
                "have a templated relation as the first argument"));
}

TEST(TVFTest, FullNameShouldNotPrependEmptyGroupName) {
  TypeFactory factory;
  std::vector<std::string> name_path = {"tvf"};
  auto tvf = std::make_unique<TableValuedFunction>(
      name_path, "",
      std::vector<FunctionSignature>{
          FunctionSignature(ARG_TYPE_RELATION, {}, -1)},
      TableValuedFunctionOptions().set_uses_upper_case_sql_name(false));
  EXPECT_EQ(tvf->FullName(), "tvf");
  EXPECT_FALSE(tvf->IsZetaSQLBuiltin());
  EXPECT_EQ(tvf->SQLName(), "tvf");
}

TEST(TVFTest, FullNameShouldPrependGroupName) {
  TypeFactory factory;
  std::vector<std::string> name_path = {"tvf"};
  auto tvf = std::make_unique<TableValuedFunction>(
      name_path, "group",
      std::vector<FunctionSignature>{
          FunctionSignature(ARG_TYPE_RELATION, {}, -1)},
      TableValuedFunctionOptions().set_uses_upper_case_sql_name(false));
  EXPECT_EQ(tvf->FullName(), "group:tvf");
  EXPECT_FALSE(tvf->IsZetaSQLBuiltin());
  EXPECT_EQ(tvf->SQLName(), "group:tvf");
}

TEST(TVFTest, ZetaSQLFunctionGroupNameIsZetaSQLBuiltin) {
  TypeFactory factory;
  std::vector<std::string> name_path = {"builtin_tvf"};
  auto tvf = std::make_unique<TableValuedFunction>(
      name_path, Function::kZetaSQLFunctionGroupName,
      std::vector<FunctionSignature>{
          FunctionSignature(ARG_TYPE_RELATION, {}, -1)},
      TableValuedFunctionOptions().set_uses_upper_case_sql_name(false));
  EXPECT_TRUE(tvf->IsZetaSQLBuiltin());
  EXPECT_EQ(tvf->SQLName(), "builtin_tvf");
}

TEST(TVFTest, TVFResolveCallsComputeResultCallBack) {
  TypeFactory factory;
  TVFSchemaColumn int64_col =
      TVFSchemaColumn("int64_col", zetasql::types::Int64Type());
  TVFRelation::ColumnList columns = {int64_col};
  TVFRelation tvf_relation(columns);

  TVFComputeResultTypeCallback result_type_callback =
      [&tvf_relation](Catalog* catalog, TypeFactory* type_factory,
                      const FunctionSignature& signature,
                      const std::vector<TVFInputArgumentType>& arguments,
                      const AnalyzerOptions& analyzer_options) {
        TVFSignatureOptions tvf_signature_options;
        tvf_signature_options.additional_deprecation_warnings =
            signature.AdditionalDeprecationWarnings();
        return new TVFSignature(arguments, tvf_relation, tvf_signature_options);
      };

  FunctionSignature signature(ARG_TYPE_RELATION, {}, -1);
  std::vector<std::string> name_path = {"tvf"};
  auto tvf = std::make_unique<TableValuedFunction>(
      name_path, signature,
      TableValuedFunctionOptions().set_compute_result_type_callback(
          result_type_callback));

  AnalyzerOptions options;
  std::vector<TVFInputArgumentType> arguments;
  std::shared_ptr<TVFSignature> tvf_signature;
  ZETASQL_ASSERT_OK(tvf->Resolve(&options, arguments, signature, nullptr, &factory,
                         &tvf_signature));
  ASSERT_THAT(tvf_signature, NotNull());
  ASSERT_EQ(tvf_signature->result_schema(), tvf_relation);
}

TEST(TVFTest, TVFResolveComputeResultCallBackNullPtr) {
  TypeFactory factory;
  std::vector<std::string> name_path = {"tvf"};
  auto tvf = std::make_unique<TableValuedFunction>(
      name_path, FunctionSignature(ARG_TYPE_RELATION, {}, -1),
      TableValuedFunctionOptions());

  AnalyzerOptions options;
  std::vector<TVFInputArgumentType> arguments;
  FunctionSignature concrete_signature(ARG_TYPE_RELATION, {}, -1);
  std::shared_ptr<TVFSignature> tvf_signature;

  ASSERT_THAT(tvf->Resolve(&options, arguments, concrete_signature, nullptr,
                           &factory, &tvf_signature),
              StatusIs(absl::StatusCode::kInternal));
  ASSERT_THAT(tvf_signature, IsNull());
}

TEST(TVFTest, TestInvalidConcreteSignatureTVFWithExtraColumns) {
  TypeFactory factory;
  std::unique_ptr<TableValuedFunction> tvf;
  TVFRelation::Column int64_col =
      TVFSchemaColumn("int64_col", zetasql::types::Int64Type());
  TVFRelation::ColumnList columns = {int64_col};
  TVFRelation tvf_relation(columns);
  FunctionArgumentType arg_type = FunctionArgumentType::RelationWithSchema(
      tvf_relation, /*extra_relation_input_columns_allowed=*/false);

  EXPECT_DEATH(
      tvf.reset(new ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
          {"tvf_append_column_input_table_has_concrete_signature"},
          FunctionSignature(arg_type, {arg_type}, -1), {int64_col})),
      HasSubstr("ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF must "
                "have a templated relation as the first argument"));
}

TEST(TVFTest, TestPseudoColumnForTVFWithExtraColumns) {
  TypeFactory factory;
  std::unique_ptr<TableValuedFunction> tvf;
  TVFSchemaColumn pseudo_column =
      TVFSchemaColumn("pseudo_column", zetasql::types::Int64Type(), true);

  EXPECT_DEATH(
      tvf.reset(new ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
          {"tvf_append_pseudo_column"},
          FunctionSignature(ARG_TYPE_RELATION, {ARG_TYPE_RELATION}, -1),
          {pseudo_column})),
      "extra columns cannot be pseudo column");
}

TEST(TVFTest, TestInputTableWithPseudoColumnForTVFWithExtraColumns) {
  TypeFactory factory;
  std::unique_ptr<TableValuedFunction> tvf;
  TVFRelation::Column int64_col =
      TVFSchemaColumn("int64_col", zetasql::types::Int64Type());
  TVFRelation::Column double_col =
      TVFSchemaColumn("double_col", zetasql::types::DoubleType());
  TVFRelation::Column pseudo_column =
      TVFSchemaColumn("pseudo_column", zetasql::types::Int64Type(), true);
  TVFRelation::ColumnList columns = {int64_col, pseudo_column};
  TVFRelation tvf_relation(columns);
  FunctionArgumentType arg_type = FunctionArgumentType::RelationWithSchema(
      tvf_relation, /*extra_relation_input_columns_allowed=*/false);

  EXPECT_DEATH(
      tvf.reset(new ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF(
          {"tvf_append_pseudo_column"},
          FunctionSignature(ARG_TYPE_RELATION, {arg_type}, -1), {double_col})),
      HasSubstr("ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF must "
                "have a templated relation as the first argument"));
}

TEST(TVFTest, TestSignatureTextUppercasesNameByDefault) {
  TypeFactory factory;

  const std::vector<std::string> function_path = {"test_tvf_name"};

  ::zetasql::TVFRelation::ColumnList tvf_schema_columns;
  tvf_schema_columns.emplace_back("value", factory.MakeSimpleType(TYPE_INT64));
  auto tvf_schema =
      std::make_unique<::zetasql::TVFRelation>(tvf_schema_columns);

  std::unique_ptr<TableValuedFunction> deserialized_tvf =
      std::make_unique<FixedOutputSchemaTVF>(
          function_path,
          ::zetasql::FunctionSignature(
              ::zetasql::ARG_TYPE_RELATION,
              {::zetasql::FunctionArgumentType(
                  ::zetasql::ARG_TYPE_ARBITRARY,
                  ::zetasql::FunctionArgumentType::REPEATED)},
              /*context_id=*/static_cast<int64_t>(0)),
          *tvf_schema);

  EXPECT_EQ(deserialized_tvf->GetSupportedSignaturesUserFacingText(
                LanguageOptions(), /*print_template_and_name_details=*/false),
            "TEST_TVF_NAME([ANY, ...])");
}

TEST(TVFTest, TestSignatureTextLowercasesNameWhenSpecified) {
  TypeFactory factory;

  const std::vector<std::string> function_path = {"test_tvf_name"};

  ::zetasql::TVFRelation::ColumnList tvf_schema_columns;
  tvf_schema_columns.emplace_back("value", factory.MakeSimpleType(TYPE_INT64));
  auto tvf_schema =
      std::make_unique<::zetasql::TVFRelation>(tvf_schema_columns);

  TableValuedFunctionOptions tvf_options;
  tvf_options.uses_upper_case_sql_name = false;

  std::unique_ptr<TableValuedFunction> deserialized_tvf =
      std::make_unique<FixedOutputSchemaTVF>(
          function_path,
          ::zetasql::FunctionSignature(
              ::zetasql::ARG_TYPE_RELATION,
              {::zetasql::FunctionArgumentType(
                  ::zetasql::ARG_TYPE_ARBITRARY,
                  ::zetasql::FunctionArgumentType::REPEATED)},
              /*context_id=*/static_cast<int64_t>(0)),
          *tvf_schema, tvf_options);

  EXPECT_EQ(deserialized_tvf->GetSupportedSignaturesUserFacingText(
                LanguageOptions(), /*print_template_and_name_details=*/false),
            "test_tvf_name([ANY, ...])");
}

TEST(TVFTest, TestFixedOutputSchemaTVFSerializeAndDeserialize) {
  TypeFactory factory;

  const std::vector<std::string> function_path = {"test_tvf_name"};

  ::zetasql::TVFRelation::ColumnList tvf_schema_columns;
  tvf_schema_columns.emplace_back("value", factory.MakeSimpleType(TYPE_INT64));
  auto tvf_schema =
      std::make_unique<::zetasql::TVFRelation>(tvf_schema_columns);

  TableValuedFunctionOptions tvf_options;
  tvf_options.uses_upper_case_sql_name = false;

  std::unique_ptr<TableValuedFunction> tvf =
      std::make_unique<FixedOutputSchemaTVF>(
          function_path,
          ::zetasql::FunctionSignature(
              FunctionArgumentType::RelationWithSchema(
                  *tvf_schema,
                  /*extra_relation_input_columns_allowed=*/false),
              {::zetasql::FunctionArgumentType(
                  ::zetasql::ARG_TYPE_ARBITRARY,
                  ::zetasql::FunctionArgumentType::REPEATED)},
              /*context_id=*/static_cast<int64_t>(0)),
          *tvf_schema, tvf_options);

  FileDescriptorSetMap file_descriptor_set_map;
  TableValuedFunctionProto tvf_proto;
  ZETASQL_ASSERT_OK(tvf->Serialize(&file_descriptor_set_map, &tvf_proto));

  std::vector<const google::protobuf::DescriptorPool*> pools(
      file_descriptor_set_map.size());
  for (const auto& pair : file_descriptor_set_map) {
    pools[pair.second->descriptor_set_index] = pair.first;
  }

  TypeFactory type_factory;
  std::unique_ptr<TableValuedFunction> deserialized_tvf;
  ZETASQL_ASSERT_OK(TableValuedFunction::Deserialize(
      tvf_proto, TypeDeserializer(&type_factory, pools), &deserialized_tvf));
  EXPECT_TRUE(deserialized_tvf->Is<FixedOutputSchemaTVF>());
  ExpectEqualTVFRelations(
      *tvf_schema,
      deserialized_tvf->GetAs<FixedOutputSchemaTVF>()->result_schema());
}

TEST(TVFTest, TvfSerializationAndDeserializationWithMultipleSignatures) {
  TypeFactory type_factory;
  FileDescriptorSetMap file_descriptor_set_map;
  std::vector<const google::protobuf::DescriptorPool*> pools(
      file_descriptor_set_map.size());
  for (const auto& pair : file_descriptor_set_map) {
    pools[pair.second->descriptor_set_index] = pair.first;
  }
  const std::vector<std::string> function_path = {"test_tvf_name"};
  TVFRelation::ColumnList tvf_schema_columns;
  tvf_schema_columns.emplace_back("column1",
                                  type_factory.MakeSimpleType(TYPE_INT64));
  auto tvf_schema = std::make_unique<TVFRelation>(tvf_schema_columns);
  TableValuedFunctionOptions tvf_options;
  tvf_options.uses_upper_case_sql_name = false;
  FunctionArgumentType relation_with_schema_arg =
      FunctionArgumentType::RelationWithSchema(
          *tvf_schema,
          /*extra_relation_input_columns_allowed=*/false);
  FunctionArgumentType any_relation_arg = FunctionArgumentType::AnyRelation();
  FunctionArgumentType any_arg =
      FunctionArgumentType(ARG_TYPE_ARBITRARY, FunctionArgumentType::REQUIRED);

  int64_t context_id = 0;
  FunctionSignature signature_relation_arg(
      relation_with_schema_arg, {relation_with_schema_arg}, ++context_id);
  FunctionSignature signature_relation_and_any_arg(
      relation_with_schema_arg, {relation_with_schema_arg, any_arg},
      ++context_id);
  FunctionSignature signature_any_relation_arg(
      any_relation_arg, {any_relation_arg}, ++context_id);
  FunctionSignature signature_any_relation_and_any_arg(
      any_relation_arg, {any_relation_arg, any_arg}, ++context_id);

  // Test FixedOutputSchemaTVF
  {
    std::vector<FunctionSignature> signatures{signature_relation_arg,
                                              signature_relation_and_any_arg};
    std::unique_ptr<TableValuedFunction> tvf =
        std::make_unique<FixedOutputSchemaTVF>(function_path, signatures,
                                               *tvf_schema, tvf_options);

    TableValuedFunctionProto tvf_proto;
    ZETASQL_ASSERT_OK(tvf->Serialize(&file_descriptor_set_map, &tvf_proto));

    std::unique_ptr<TableValuedFunction> deserialized_tvf;
    ZETASQL_ASSERT_OK(TableValuedFunction::Deserialize(
        tvf_proto, TypeDeserializer(&type_factory, pools), &deserialized_tvf));

    ASSERT_TRUE(deserialized_tvf->Is<FixedOutputSchemaTVF>());
    ExpectEqualTVFRelations(
        *tvf_schema,
        deserialized_tvf->GetAs<FixedOutputSchemaTVF>()->result_schema());
    EXPECT_EQ(deserialized_tvf->NumSignatures(), 2);
    EXPECT_EQ(deserialized_tvf->DebugString(), tvf->DebugString());
  }

  // Test ForwardInputSchemaToOutputSchemaTVF
  {
    std::vector<FunctionSignature> signatures{signature_relation_arg,
                                              signature_relation_and_any_arg,
                                              signature_any_relation_arg};
    std::unique_ptr<TableValuedFunction> tvf =
        std::make_unique<ForwardInputSchemaToOutputSchemaTVF>(
            function_path, signatures, tvf_options);

    TableValuedFunctionProto tvf_proto;
    ZETASQL_ASSERT_OK(tvf->Serialize(&file_descriptor_set_map, &tvf_proto));

    std::unique_ptr<TableValuedFunction> deserialized_tvf;
    ZETASQL_ASSERT_OK(TableValuedFunction::Deserialize(
        tvf_proto, TypeDeserializer(&type_factory, pools), &deserialized_tvf));
    ASSERT_TRUE(deserialized_tvf->Is<ForwardInputSchemaToOutputSchemaTVF>());
    EXPECT_EQ(deserialized_tvf->NumSignatures(), 3);
    EXPECT_EQ(deserialized_tvf->DebugString(), tvf->DebugString());
  }

  // Test ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF
  {
    std::vector<FunctionSignature> signatures{
        signature_any_relation_arg, signature_any_relation_and_any_arg};
    TVFSchemaColumn extra_column("extra_col",
                                 type_factory.MakeSimpleType(TYPE_STRING));
    std::unique_ptr<TableValuedFunction> tvf =
        std::make_unique<ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF>(
            function_path, signatures,
            std::vector<TVFSchemaColumn>{extra_column}, tvf_options);

    TableValuedFunctionProto tvf_proto;
    ZETASQL_ASSERT_OK(tvf->Serialize(&file_descriptor_set_map, &tvf_proto));

    std::unique_ptr<TableValuedFunction> deserialized_tvf;
    ZETASQL_ASSERT_OK(TableValuedFunction::Deserialize(
        tvf_proto, TypeDeserializer(&type_factory, pools), &deserialized_tvf));

    ASSERT_TRUE(
        deserialized_tvf
            ->Is<ForwardInputSchemaToOutputSchemaWithAppendedColumnTVF>());
    EXPECT_EQ(deserialized_tvf->NumSignatures(), 2);
    EXPECT_EQ(deserialized_tvf->DebugString(), tvf->DebugString());
  }
}

TEST(TVFTest, TestAnonymizationInfo) {
  TypeFactory factory;

  const std::vector<std::string> function_path = {"test_tvf_name"};

  ::zetasql::TVFRelation::ColumnList tvf_schema_columns;
  tvf_schema_columns.emplace_back("value", factory.MakeSimpleType(TYPE_INT64));
  auto tvf_schema =
      std::make_unique<::zetasql::TVFRelation>(tvf_schema_columns);

  TableValuedFunctionOptions tvf_options;
  tvf_options.uses_upper_case_sql_name = false;

  std::unique_ptr<TableValuedFunction> tvf_with_userid =
      std::make_unique<FixedOutputSchemaTVF>(
          function_path,
          ::zetasql::FunctionSignature(
              FunctionArgumentType::RelationWithSchema(
                  *tvf_schema,
                  /*extra_relation_input_columns_allowed=*/false),
              {::zetasql::FunctionArgumentType(
                  ::zetasql::ARG_TYPE_ARBITRARY,
                  ::zetasql::FunctionArgumentType::REPEATED)},
              /*context_id=*/static_cast<int64_t>(0)),
          *tvf_schema, tvf_options);

  ZETASQL_ASSERT_OK(tvf_with_userid->SetUserIdColumnNamePath({"value"}));

  std::optional<const AnonymizationInfo> anonymization_info =
      tvf_with_userid->anonymization_info();

  EXPECT_TRUE(anonymization_info.has_value());
  EXPECT_EQ(anonymization_info->UserIdColumnNamePath().size(), 1);
  EXPECT_EQ(anonymization_info->UserIdColumnNamePath().at(0), "value");

  FileDescriptorSetMap file_descriptor_set_map;
  TableValuedFunctionProto tvf_proto;
  ZETASQL_ASSERT_OK(tvf_with_userid->Serialize(&file_descriptor_set_map, &tvf_proto));

  std::vector<const google::protobuf::DescriptorPool*> pools(
      file_descriptor_set_map.size());
  for (const auto& pair : file_descriptor_set_map) {
    pools[pair.second->descriptor_set_index] = pair.first;
  }

  TypeFactory type_factory;
  std::unique_ptr<TableValuedFunction> deserialized_tvf_with_userid;
  ZETASQL_ASSERT_OK(TableValuedFunction::Deserialize(
      tvf_proto, TypeDeserializer(&type_factory, pools),
      &deserialized_tvf_with_userid));
  EXPECT_TRUE(deserialized_tvf_with_userid->Is<TableValuedFunction>());
  std::optional<const AnonymizationInfo> deserialized_anonymization_info =
      deserialized_tvf_with_userid->GetAs<TableValuedFunction>()
          ->anonymization_info();

  EXPECT_TRUE(deserialized_anonymization_info.has_value());
  EXPECT_EQ(deserialized_anonymization_info->UserIdColumnNamePath().size(), 1);
  EXPECT_EQ(deserialized_anonymization_info->UserIdColumnNamePath().at(0),
            "value");
}

TEST(TVFTest, TestTableValueFunctionConstructorWithAnonymizationInfo) {
  TypeFactory factory;

  const std::vector<std::string> function_path = {"test_tvf_name"};

  ::zetasql::TVFRelation::ColumnList tvf_schema_columns;
  tvf_schema_columns.emplace_back("value", factory.MakeSimpleType(TYPE_INT64));
  auto tvf_schema =
      std::make_unique<::zetasql::TVFRelation>(tvf_schema_columns);

  TableValuedFunctionOptions tvf_options;
  tvf_options.uses_upper_case_sql_name = false;

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<AnonymizationInfo> anonymization_info,
                       AnonymizationInfo::Create({"value"}));

  std::unique_ptr<TableValuedFunction> tvf_with_userid =
      std::make_unique<FixedOutputSchemaTVF>(
          function_path,
          std::vector<FunctionSignature>{FunctionSignature(
              FunctionArgumentType::RelationWithSchema(
                  *tvf_schema,
                  /*extra_relation_input_columns_allowed=*/false),
              {::zetasql::FunctionArgumentType(
                  ::zetasql::ARG_TYPE_ARBITRARY,
                  ::zetasql::FunctionArgumentType::REPEATED)},
              /*context_id=*/static_cast<int64_t>(0))},
          std::move(anonymization_info), *tvf_schema, tvf_options);

  std::optional<const AnonymizationInfo> tvf_anonymization_info =
      tvf_with_userid->anonymization_info();

  EXPECT_TRUE(tvf_anonymization_info.has_value());
  EXPECT_EQ(tvf_anonymization_info->UserIdColumnNamePath().size(), 1);
  EXPECT_EQ(tvf_anonymization_info->UserIdColumnNamePath().at(0), "value");
}

TEST(TVFTest, TestGetSQLDeclarationForValueTable) {
  TVFRelation tvf_relation = TVFRelation::ValueTable(types::Int64Type());

  std::string sql_declaration =
      tvf_relation.GetSQLDeclaration(ProductMode::PRODUCT_EXTERNAL);
  EXPECT_EQ(sql_declaration, "TABLE<INT64>");
}

TEST(TVFTest, TestGetSQLDeclarationForTableWithRegularColumnNames) {
  TVFRelation::Column int64_col =
      TVFSchemaColumn("int64_col", types::Int64Type());
  TVFRelation::Column double_col =
      TVFSchemaColumn("double_col", types::DoubleType());
  TVFRelation::ColumnList columns = {int64_col, double_col};
  TVFRelation tvf_relation(columns);

  std::string sql_declaration =
      tvf_relation.GetSQLDeclaration(ProductMode::PRODUCT_EXTERNAL);
  EXPECT_EQ(sql_declaration, "TABLE<int64_col INT64, double_col FLOAT64>");
}

TEST(TVFTest, TestGetSQLDeclarationForTableWithReservedColumnNames) {
  TVFRelation::Column int64_col = TVFSchemaColumn("window", types::Int64Type());
  TVFRelation::Column double_col = TVFSchemaColumn("from", types::DoubleType());
  TVFRelation::ColumnList columns = {int64_col, double_col};
  TVFRelation tvf_relation(columns);

  std::string sql_declaration =
      tvf_relation.GetSQLDeclaration(ProductMode::PRODUCT_EXTERNAL);
  EXPECT_EQ(sql_declaration, "TABLE<`window` INT64, `from` FLOAT64>");
}

TEST(TVFTest, TestGetSQLDeclarationForTableWithEmptyColumnName) {
  TVFRelation::Column int64_col = TVFSchemaColumn("", types::Int64Type());
  TVFRelation::Column double_col = TVFSchemaColumn("col2", types::DoubleType());
  TVFRelation::ColumnList columns = {int64_col, double_col};
  TVFRelation tvf_relation(columns);

  std::string sql_declaration =
      tvf_relation.GetSQLDeclaration(ProductMode::PRODUCT_EXTERNAL);
  EXPECT_EQ(sql_declaration, "TABLE<INT64, col2 FLOAT64>");
}

TEST(TVFTest, TVFInputArgumentType_Copyable) {
  TVFRelation::Column int64_col = TVFSchemaColumn("window", types::Int64Type());
  TVFInputArgumentType arg = TVFInputArgumentType(TVFRelation({int64_col}));
  TVFInputArgumentType copy = arg;
  EXPECT_EQ(copy.DebugString(), arg.DebugString());
  TVFInputArgumentType moved = std::move(arg);
  EXPECT_EQ(moved.DebugString(), copy.DebugString());
}

}  // namespace zetasql
