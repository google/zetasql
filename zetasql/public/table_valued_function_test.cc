//
// Copyright 2019 ZetaSQL Authors
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

#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/proto/function.pb.h"
#include "zetasql/public/type.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/statusor.h"

namespace zetasql {

void ExpectEqualTVFSchemaColumn(const TVFSchemaColumn& column1,
                                const TVFSchemaColumn& column2) {
  EXPECT_EQ(column1.name, column2.name);
  EXPECT_EQ(column1.is_pseudo_column, column2.is_pseudo_column);
  EXPECT_TRUE(column1.type->Equals(column2.type));
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
      TVFRelation::Deserialize(tvf_relation_proto, pools, &type_factory));
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
      TVFSchemaColumn::FromProto(tvf_schema_column, pools, &type_factory));

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
  // Generate an output schema that returns an int64_t value table.
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
                  /*extra_relation_input_columns_allowed=*/false)},
              -1),
          {int64_col})),
      "Does not support non-templated argument type");
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
      "Does not support non-templated argument type");
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
      "Does not support non-templated argument type");
}
}  // namespace zetasql
