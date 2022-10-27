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

#include "zetasql/compliance/test_database_catalog.h"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/compliance/test_util.h"
#include "absl/status/status.h"
#include "absl/strings/str_join.h"

namespace zetasql {

TestDatabaseCatalog::BuiltinFunctionCache::~BuiltinFunctionCache() {
  DumpStats();
}
absl::Status TestDatabaseCatalog::BuiltinFunctionCache::SetLanguageOptions(
    const LanguageOptions& options, SimpleCatalog* catalog) {
  ++total_calls_;
  const CacheEntry* cache_entry = nullptr;
  if (auto it = builtins_cache_.find(options); it != builtins_cache_.end()) {
    cache_hit_++;
    cache_entry = &it->second;
  } else {
    CacheEntry entry;
    // We have to call type_factory() while not holding mutex_.
    TypeFactory* type_factory = catalog->type_factory();
    ZETASQL_RETURN_IF_ERROR(GetZetaSQLFunctionsAndTypes(
        type_factory, options, &entry.functions, &entry.types));
    cache_entry =
        &(builtins_cache_.emplace(options, std::move(entry)).first->second);
  }

  auto builtin_function_predicate = [](const Function* fn) {
    return fn->IsZetaSQLBuiltin();
  };

  // We need to remove all the types that are added along with builtin
  // functions, which, at the moment is limited to opaque enum types.
  auto builtin_type_predicate = [](const Type* type) {
    return type->IsEnum() && type->AsEnum()->IsOpaque();
  };

  catalog->RemoveFunctions(builtin_function_predicate);
  catalog->RemoveTypes(builtin_type_predicate);

  std::vector<const Function*> functions;
  functions.reserve(cache_entry->functions.size());
  for (const auto& [_, function] : cache_entry->functions) {
    ZETASQL_RET_CHECK(builtin_function_predicate(function.get()));
    functions.push_back(function.get());
  }
  catalog->AddZetaSQLFunctions(functions);

  for (const auto& [name, type] : cache_entry->types) {
    // Make sure we are consistent with types we add and remove.
    ZETASQL_RET_CHECK(builtin_type_predicate(type));
    // Note, we currently don't support builtin types with catalog paths.
    catalog->AddType(name, type);
  }
  return absl::OkStatus();
}
void TestDatabaseCatalog::BuiltinFunctionCache::DumpStats() {
  ZETASQL_LOG(INFO) << "BuiltinFunctionCache: hit: " << cache_hit_ << " / "
            << total_calls_ << "(" << (cache_hit_ * 100. / total_calls_) << "%)"
            << " size: " << builtins_cache_.size();
}

TestDatabaseCatalog::TestDatabaseCatalog(TypeFactory* type_factory)
    : function_cache_(std::make_unique<BuiltinFunctionCache>()),
      type_factory_(type_factory) {}

absl::Status TestDatabaseCatalog::LoadProtoEnumTypes(
    const std::set<std::string>& filenames,
    const std::set<std::string>& proto_names,
    const std::set<std::string>& enum_names) {
  errors_.clear();
  for (const std::string& filename : filenames) {
    importer_->Import(filename);
  }
  if (!errors_.empty()) {
    return ::zetasql_base::InternalErrorBuilder() << absl::StrJoin(errors_, "\n");
  }

  std::set<std::string> proto_closure;
  std::set<std::string> enum_closure;
  ZETASQL_RETURN_IF_ERROR(ComputeTransitiveClosure(importer_->pool(), proto_names,
                                           enum_names, &proto_closure,
                                           &enum_closure));

  for (const std::string& proto : proto_closure) {
    const google::protobuf::Descriptor* descriptor =
        importer_->pool()->FindMessageTypeByName(proto);
    if (!descriptor) {
      return ::zetasql_base::NotFoundErrorBuilder() << "Proto Message Type: " << proto;
    }
    const ProtoType* proto_type;
    ZETASQL_RETURN_IF_ERROR(
        catalog_->type_factory()->MakeProtoType(descriptor, &proto_type));
    catalog_->AddType(descriptor->full_name(), proto_type);
  }
  for (const std::string& enum_name : enum_closure) {
    const google::protobuf::EnumDescriptor* enum_descriptor =
        importer_->pool()->FindEnumTypeByName(enum_name);
    if (!enum_descriptor) {
      return ::zetasql_base::NotFoundErrorBuilder() << "Enum Type: " << enum_name;
    }
    const EnumType* enum_type;
    ZETASQL_RETURN_IF_ERROR(
        catalog_->type_factory()->MakeEnumType(enum_descriptor, &enum_type));
    catalog_->AddType(enum_descriptor->full_name(), enum_type);
  }
  return absl::OkStatus();
}

void TestDatabaseCatalog::AddTable(const std::string& table_name,
                                   const TestTable& table) {
  const Value& array_value = table.table_as_value;
  ZETASQL_CHECK(array_value.type()->IsArray())
      << table_name << " " << array_value.DebugString(true);
  auto element_type = array_value.type()->AsArray()->element_type();
  SimpleTable* simple_table = nullptr;

  if (!table.options.is_value_table()) {
    // Non-value tables are represented as arrays of structs.
    const StructType* row_type = element_type->AsStruct();
    std::vector<SimpleTable::NameAndAnnotatedType> columns;
    const std::vector<const AnnotationMap*>& column_annotations =
        table.options.column_annotations();
    ZETASQL_CHECK(column_annotations.empty() ||
          column_annotations.size() == row_type->num_fields());
    columns.reserve(row_type->num_fields());
    for (int i = 0; i < row_type->num_fields(); i++) {
      columns.push_back(
          {row_type->field(i).name,
           {row_type->field(i).type,
            column_annotations.empty() ? nullptr : column_annotations[i]}});
    }
    simple_table = new SimpleTable(table_name, columns);
  } else {
    // We got a value table. Create a table with a single column named "value".
    simple_table = new SimpleTable(table_name, {{"value", element_type}});
    simple_table->set_is_value_table(true);
  }
  if (!table.options.userid_column().empty()) {
    ZETASQL_CHECK_OK(simple_table->SetAnonymizationInfo(table.options.userid_column()));
  }
  catalog_->AddOwnedTable(simple_table);
}

absl::Status TestDatabaseCatalog::SetTestDatabase(const TestDatabase& test_db) {
  catalog_ = std::make_unique<SimpleCatalog>("root_catalog", type_factory_);
  // Prepare proto importer.
  if (test_db.runs_as_test) {
    proto_source_tree_ = CreateProtoSourceTree();
  } else {
    proto_source_tree_ = std::make_unique<TestDriver::ProtoSourceTree>("");
  }
  proto_error_collector_ =
      std::make_unique<TestDriver::ProtoErrorCollector>(&errors_);
  importer_ = std::make_unique<google::protobuf::compiler::Importer>(
      proto_source_tree_.get(), proto_error_collector_.get());
  // Load protos and enums.
  ZETASQL_RETURN_IF_ERROR(LoadProtoEnumTypes(test_db.proto_files, test_db.proto_names,
                                     test_db.enum_names));
  // Add tables to the catalog.
  for (const auto& t : test_db.tables) {
    const std::string& table_name = t.first;
    const TestTable& test_table = t.second;
    AddTable(table_name, test_table);
  }
  // Add functions to the catalog.
  return absl::OkStatus();
}

void TestDatabaseCatalog::SetLanguageOptions(
    const LanguageOptions& language_options) {
  if (catalog_ != nullptr) {
    ZETASQL_CHECK_OK(
        function_cache_->SetLanguageOptions(language_options, catalog_.get()));
  }
}
}  // namespace zetasql
