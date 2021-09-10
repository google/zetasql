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
#include "zetasql/public/annotation.pb.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/type_factory.h"

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

}  // namespace zetasql
