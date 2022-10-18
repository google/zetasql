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

#include "zetasql/compliance/test_driver.h"

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace zetasql {

using ::zetasql_test__::KitchenSinkPB;

TEST(TestDriverTest, ClassAndProtoSize) {
  // We're forced to replicate the exact structure for this test because the
  // presence of bools makes computing an 'expected'
  // (sizeof(X) == sizeof(X.field1) + sizeof(X.field2) +...) weird due
  // to alignment (bool will take 64 or 32 bytes in this case).
  struct MockTestDatabase {
    std::set<std::string> proto_files;
    bool runs_as_test;
    std::set<std::string> proto_names;
    std::set<std::string> enum_names;
    std::map<std::string, TestTable> tables;
  };
  struct MockTestTestTableOptions {
    int expected_table_size_min;
    int expected_table_size_max;
    bool is_value_table;
    double nullable_probability;
    std::set<LanguageFeature> required_features;
    std::string userid_column;
  };
  struct MockTestTable {
    Value table_as_value;
    MockTestTestTableOptions options;
  };
  static_assert(sizeof(TestDatabase) == sizeof(MockTestDatabase),
                "Please change SerializeTestDatabase (test_driver.cc) and "
                "TestDatabaseProto (test_driver.proto) tests if TestDatabase "
                "is modified.");
  EXPECT_EQ(5, TestDatabaseProto::descriptor()->field_count());
  EXPECT_EQ(7, TestTableOptionsProto::descriptor()->field_count());
  EXPECT_EQ(3, TestTableProto::descriptor()->field_count());
}

Value KitchenSinkPBValue(const ProtoType* proto_type,
                         const KitchenSinkPB proto) {
  return Value::Proto(proto_type, SerializeToCord(proto));
}

TEST(TestDriverTest, SerializeDeserializeTestDbWithProtos) {
  TypeFactory type_factory;

  const ProtoType* proto_type;
  ZETASQL_ASSERT_OK(
      type_factory.MakeProtoType(KitchenSinkPB::descriptor(), &proto_type));

  const StructType* struct_proto_type;
  ZETASQL_ASSERT_OK(
      type_factory.MakeStructType({{"a", proto_type}}, &struct_proto_type));

  const ArrayType* array_struct_proto_type;
  ZETASQL_ASSERT_OK(
      type_factory.MakeArrayType(struct_proto_type, &array_struct_proto_type));

  KitchenSinkPB pb;
  pb.set_int64_key_1(3);
  pb.set_int64_key_2(4);
  pb.set_string_val("abcde");
  ZETASQL_ASSERT_OK_AND_ASSIGN(Value row1,
                       Value::MakeStruct(struct_proto_type,
                                         {KitchenSinkPBValue(proto_type, pb)}));
  pb.set_string_val("defgh");
  ZETASQL_ASSERT_OK_AND_ASSIGN(Value row2,
                       Value::MakeStruct(struct_proto_type,
                                         {KitchenSinkPBValue(proto_type, pb)}));
  TestTable t;
  t.table_as_value = Value::Array(array_struct_proto_type, {row1, row2});
  TestDatabase test_db;
  test_db.tables["t"] = t;

  TestDatabaseProto test_db_proto;
  ZETASQL_ASSERT_OK(SerializeTestDatabase(test_db, &test_db_proto));

  google::protobuf::DescriptorPool test_pool;
  std::vector<google::protobuf::DescriptorPool*> descriptor_pools;
  descriptor_pools.push_back(&test_pool);
  std::vector<std::unique_ptr<const AnnotationMap>> annotation_maps;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      TestDatabase deserialized_db,
      DeserializeTestDatabase(test_db_proto, &type_factory, descriptor_pools,
                              annotation_maps));
  ASSERT_EQ(deserialized_db.tables["t"].table_as_value.DebugString(),
            test_db.tables["t"].table_as_value.DebugString())
      << "Table differs after round-trip serialization/deserialization";
}

TEST(TestDriverTest, SerializeDeserializeTableOptions) {
  TestDatabase test_db;
  TestTable& t = test_db.tables["t"];
  t.options.set_expected_table_size_range(3, 8);
  t.options.set_is_value_table(true);
  t.options.mutable_required_features()->insert(
      FEATURE_ALTER_TABLE_RENAME_COLUMN);
  t.options.set_userid_column("abc");
  t.options.set_nullable_probability(0.12345);

  TypeFactory type_factory;
  std::unique_ptr<AnnotationMap> annotation_map1 =
      AnnotationMap::Create(type_factory.get_int64());
  annotation_map1->SetAnnotation(1, SimpleValue::Int64(1234));
  annotation_map1->SetAnnotation(2, SimpleValue::Int64(5678));
  t.options.set_column_annotations({annotation_map1.get()});

  const StructType* row_type;
  ZETASQL_ASSERT_OK(type_factory.MakeStructType(
      {{"a", type_factory.get_int64()}, {"b", type_factory.get_string()}},
      &row_type));
  const ArrayType* table_type;
  ZETASQL_ASSERT_OK(type_factory.MakeArrayType(row_type, &table_type));
  t.table_as_value = Value::Array(table_type, {});

  TestDatabaseProto test_db_proto;
  ZETASQL_ASSERT_OK(SerializeTestDatabase(test_db, &test_db_proto));

  google::protobuf::DescriptorPool test_pool;
  std::vector<google::protobuf::DescriptorPool*> descriptor_pools;
  descriptor_pools.push_back(&test_pool);
  std::vector<std::unique_ptr<const AnnotationMap>> annotation_maps;
  SCOPED_TRACE(test_db_proto.DebugString());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      TestDatabase deserialized_db,
      DeserializeTestDatabase(test_db_proto, &type_factory, descriptor_pools,
                              annotation_maps));
  const TestTable& deserialized_t = deserialized_db.tables["t"];
  ASSERT_EQ(deserialized_t.options.expected_table_size_min(), 3);
  ASSERT_EQ(deserialized_t.options.expected_table_size_max(), 8);
  ASSERT_EQ(deserialized_t.options.is_value_table(), true);
  ASSERT_EQ(deserialized_t.options.required_features().size(), 1);
  ASSERT_TRUE(deserialized_t.options.required_features().find(
                  FEATURE_ALTER_TABLE_RENAME_COLUMN) !=
              deserialized_t.options.required_features().end());
  ASSERT_EQ(deserialized_t.options.userid_column(), "abc");
  ASSERT_EQ(deserialized_t.options.nullable_probability(), 0.12345);
  ASSERT_EQ(deserialized_t.options.column_annotations().size(), 1);
  ASSERT_TRUE(deserialized_t.options.column_annotations().at(0) != nullptr);
  ASSERT_TRUE(deserialized_t.options.column_annotations().at(0)->GetAnnotation(
                  1) != nullptr);
  ASSERT_TRUE(deserialized_t.options.column_annotations().at(0)->GetAnnotation(
                  2) != nullptr);
  ASSERT_EQ(deserialized_t.options.column_annotations()
                .at(0)
                ->GetAnnotation(1)
                ->DebugString(),
            SimpleValue::Int64(1234).DebugString());
  ASSERT_EQ(deserialized_t.options.column_annotations()
                .at(0)
                ->GetAnnotation(2)
                ->DebugString(),
            SimpleValue::Int64(5678).DebugString());
}

}  // namespace zetasql
