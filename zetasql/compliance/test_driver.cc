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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/annotation.pb.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/status/status.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

constexpr int TestTableOptions::kDefaultExpectedTableSize;
constexpr double TestTableOptions::kDefaultNullableProbability;

absl::Status SerializeTestDatabase(const TestDatabase& database,
                                   TestDatabaseProto* proto) {
  proto->Clear();
  for (const std::string& proto_file : database.proto_files) {
    proto->add_proto_files(proto_file);
  }
  proto->set_runs_as_test(database.runs_as_test);
  for (const std::string& proto_name : database.proto_names) {
    proto->add_proto_names(proto_name);
  }
  for (const std::string& enum_name : database.enum_names) {
    proto->add_enum_names(enum_name);
  }
  for (const auto& entry : database.tables) {
    TestTableProto* t = proto->add_test_tables();
    t->set_name(entry.first);
    const TestTable& table = entry.second;
    ZETASQL_RETURN_IF_ERROR(table.table_as_value.type()->SerializeToSelfContainedProto(
        t->mutable_contents()->mutable_type()));
    ZETASQL_RETURN_IF_ERROR(
        table.table_as_value.Serialize(t->mutable_contents()->mutable_value()));
    const TestTableOptions& options = table.options;

    TestTableOptionsProto* options_proto = t->mutable_options();
    options_proto->set_expected_table_size_min(
        options.expected_table_size_min());
    options_proto->set_expected_table_size_max(
        options.expected_table_size_max());
    options_proto->set_is_value_table(options.is_value_table());
    options_proto->set_nullable_probability(options.nullable_probability());
    for (LanguageFeature feature : options.required_features()) {
      options_proto->add_required_features(feature);
    }
    if (!options.userid_column().empty()) {
      options_proto->set_userid_column(options.userid_column());
    }
    if (!options.column_annotations().empty()) {
      std::unique_ptr<AnnotationMap> empty_annotation =
          AnnotationMap::Create(types::Int64Type());
      for (const AnnotationMap* annotation : options.column_annotations()) {
        AnnotationMapProto* proto = options_proto->add_column_annotations();
        if (annotation != nullptr) {
          ZETASQL_RETURN_IF_ERROR(annotation->Serialize(proto));
        } else {
          ZETASQL_RETURN_IF_ERROR(empty_annotation->Serialize(proto));
        }
      }
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<TestDatabase> DeserializeTestDatabase(
    const TestDatabaseProto& proto, TypeFactory* type_factory,
    const std::vector<google::protobuf::DescriptorPool*>& descriptor_pools,
    std::vector<std::unique_ptr<const AnnotationMap>>& annotation_maps) {
  TestDatabase db;
  for (const std::string& proto_file : proto.proto_files()) {
    if (!db.proto_files.insert(proto_file).second) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Duplicate proto file: " << proto_file;
    }
  }
  db.runs_as_test = proto.runs_as_test();
  for (const std::string& proto_name : proto.proto_names()) {
    if (!db.proto_names.insert(proto_name).second) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Duplicate proto name: " << proto_name;
    }
  }
  for (const std::string& enum_name : proto.enum_names()) {
    if (!db.enum_names.insert(enum_name).second) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Duplicate enum name: " << enum_name;
    }
  }
  for (const TestTableProto& table_proto : proto.test_tables()) {
    auto [it, inserted] = db.tables.try_emplace(table_proto.name());
    if (!inserted) {
      return zetasql_base::InvalidArgumentErrorBuilder()
             << "Duplicate table name: " << table_proto.name();
    }
    TestTable& table = it->second;
    const Type* contents_type;
    ZETASQL_RETURN_IF_ERROR(
        type_factory->DeserializeFromSelfContainedProtoWithDistinctFiles(
            table_proto.contents().type(), descriptor_pools, &contents_type));
    ZETASQL_ASSIGN_OR_RETURN(
        table.table_as_value,
        Value::Deserialize(table_proto.contents().value(), contents_type));

    table.options.set_expected_table_size_range(
        static_cast<int>(table_proto.options().expected_table_size_min()),
        static_cast<int>(table_proto.options().expected_table_size_max()));
    table.options.set_is_value_table(table_proto.options().is_value_table());
    table.options.set_nullable_probability(
        table_proto.options().nullable_probability());

    for (int feature_int : table_proto.options().required_features()) {
      LanguageFeature feature = static_cast<LanguageFeature>(feature_int);
      if (!table.options.mutable_required_features()->insert(feature).second) {
        return zetasql_base::InvalidArgumentErrorBuilder()
               << "Duplicate required language feature: "
               << LanguageFeature_Name(feature);
      }
    }
    table.options.set_userid_column(table_proto.options().userid_column());
    std::vector<const AnnotationMap*> column_annotations;
    for (const AnnotationMapProto& annotation :
         table_proto.options().column_annotations()) {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<const AnnotationMap> annotation_map,
                       AnnotationMap::Deserialize(annotation));
      column_annotations.push_back(annotation_map.get());
      annotation_maps.push_back(std::move(annotation_map));
    }
    table.options.set_column_annotations(std::move(column_annotations));
  }
  return db;
}

}  // namespace zetasql
