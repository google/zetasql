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

}  // namespace zetasql
