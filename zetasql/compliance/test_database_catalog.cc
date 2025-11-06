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

#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/common/internal_value.h"
#include "zetasql/common/measure_analysis_utils.h"
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/compliance/test_util.h"
#include "zetasql/public/builtin_function.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/simple_catalog_util.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/struct_type.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "zetasql/base/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "google/protobuf/compiler/importer.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

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
    ZETASQL_RETURN_IF_ERROR(GetBuiltinFunctionsAndTypes(BuiltinFunctionOptions(options),
                                                *type_factory, entry.functions,
                                                entry.types));
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

absl::Status TestDatabaseCatalog::IsInitialized() const {
  if (!is_initialized_) {
    return absl::FailedPreconditionError(
        "TestDatabaseCatalog is not initialized. "
        "TestDatabaseCatalog::SetTestDatabase() must be called first, "
        "before calling any other methods.");
  }
  return absl::OkStatus();
}

void TestDatabaseCatalog::BuiltinFunctionCache::DumpStats() {
  ABSL_LOG(INFO) << "BuiltinFunctionCache: hit: " << cache_hit_ << " / "
            << total_calls_ << "("
            << (total_calls_ == 0 ? 0 : cache_hit_ * 100. / total_calls_)
            << "%) size: " << builtins_cache_.size();
}

static absl::Status LoadStaticEnum(
    const google::protobuf::EnumDescriptor* enum_descriptor, SimpleCatalog* catalog) {
  const Type* enum_type;
  ZETASQL_RETURN_IF_ERROR(
      catalog->type_factory()->MakeEnumType(enum_descriptor, &enum_type));
  catalog->AddTypeIfNotPresent(enum_descriptor->full_name(), enum_type);
  return absl::OkStatus();
}

static absl::Status LoadStaticProto(const google::protobuf::Descriptor* proto_descriptor,
                                    SimpleCatalog* catalog) {
  const Type* proto_type;
  ZETASQL_RETURN_IF_ERROR(
      catalog->type_factory()->MakeProtoType(proto_descriptor, &proto_type));
  catalog->AddTypeIfNotPresent(proto_descriptor->full_name(), proto_type);
  return absl::OkStatus();
}

absl::Status TestDatabaseCatalog::LoadProtoEnumTypes(
    const std::set<std::string>& filenames,
    const std::set<std::string>& proto_names,
    const std::set<std::string>& enum_names) {
  for (const std::string& filename : filenames) {
    ZETASQL_RETURN_IF_ERROR(importer_->Import(filename));
  }
  for (const auto& [name, enum_descriptor] : GetBuiltinEnumDescriptors()) {
    ZETASQL_RETURN_IF_ERROR(LoadStaticEnum(enum_descriptor, catalog_.get()));
  }
  for (const auto& [name, proto_descriptor] : GetBuiltinProtoDescriptors()) {
    ZETASQL_RETURN_IF_ERROR(LoadStaticProto(proto_descriptor, catalog_.get()));
  }

  std::set<std::string> proto_closure;
  std::set<std::string> enum_closure;
  ZETASQL_RETURN_IF_ERROR(ComputeTransitiveClosure(descriptor_pool(), proto_names,
                                           enum_names, &proto_closure,
                                           &enum_closure));

  const absl::flat_hash_map<absl::string_view, const google::protobuf::Descriptor*>&
      builtin_proto_descriptors = GetBuiltinProtoDescriptors();

  for (const std::string& proto : proto_closure) {
    const google::protobuf::Descriptor* descriptor =
        descriptor_pool()->FindMessageTypeByName(proto);
    auto it = builtin_proto_descriptors.find(proto);
    if (it != builtin_proto_descriptors.end()) {
      // Builtin protos should only use the global descriptor.
      ZETASQL_RETURN_IF_ERROR(LoadStaticProto(it->second, catalog_.get()));
    } else {
      if (!descriptor) {
        return ::zetasql_base::NotFoundErrorBuilder()
               << "Proto Message Type: " << proto;
      }
      const ProtoType* proto_type;
      ZETASQL_RETURN_IF_ERROR(
          catalog_->type_factory()->MakeProtoType(descriptor, &proto_type));
      catalog_->AddType(descriptor->full_name(), proto_type);
    }
  }

  const absl::flat_hash_map<absl::string_view, const google::protobuf::EnumDescriptor*>&
      builtin_enum_descriptors = GetBuiltinEnumDescriptors();

  for (const std::string& enum_name : enum_closure) {
    auto it = builtin_enum_descriptors.find(enum_name);
    if (it != builtin_enum_descriptors.end()) {
      // Builtin enums should only use the global descriptor.
      ZETASQL_RETURN_IF_ERROR(LoadStaticEnum(it->second, catalog_.get()));
    } else {
      const google::protobuf::EnumDescriptor* enum_descriptor =
          descriptor_pool()->FindEnumTypeByName(enum_name);
      if (!enum_descriptor) {
        return ::zetasql_base::NotFoundErrorBuilder() << "Enum Type: " << enum_name;
      }
      const EnumType* enum_type;
      ZETASQL_RETURN_IF_ERROR(
          catalog_->type_factory()->MakeEnumType(enum_descriptor, &enum_type));
      catalog_->AddType(enum_descriptor->full_name(), enum_type);
    }
  }
  return absl::OkStatus();
}

static std::unique_ptr<SimpleTable> MakeSimpleTable(
    const std::string& table_name, const TestTable& table) {
  const Value& array_value = table.table_as_value;
  ABSL_CHECK(array_value.type()->IsArray())
      << table_name << " " << array_value.DebugString(true);
  auto element_type = array_value.type()->AsArray()->element_type();
  std::unique_ptr<SimpleTable> simple_table;
  if (!table.options.is_value_table()) {
    // Non-value tables are represented as arrays of structs.
    const StructType* row_type = element_type->AsStruct();
    std::vector<SimpleTable::NameAndAnnotatedType> columns;
    const std::vector<const AnnotationMap*>& column_annotations =
        table.options.column_annotations();
    ABSL_CHECK(column_annotations.empty() ||
          column_annotations.size() == row_type->num_fields());
    columns.reserve(row_type->num_fields());
    for (int i = 0; i < row_type->num_fields(); i++) {
      columns.push_back(
          {row_type->field(i).name,
           {row_type->field(i).type,
            column_annotations.empty() ? nullptr : column_annotations[i]}});
    }
    simple_table = std::make_unique<SimpleTable>(table_name, columns);
  } else {
    // We got a value table. Create a table with a single column named "value".
    ABSL_CHECK(table.measure_column_defs.empty());
    std::vector<SimpleTable::NameAndAnnotatedType> columns;
    columns.push_back(
        std::make_pair("value", AnnotatedType(element_type, nullptr)));
    simple_table =
        std::make_unique<SimpleTable>(table_name, std::move(columns));
    simple_table->set_is_value_table(true);
  }
  if (!table.options.userid_column().empty()) {
    ZETASQL_CHECK_OK(simple_table->SetAnonymizationInfo(table.options.userid_column()));
  }
  return simple_table;
}

void TestDatabaseCatalog::AddTable(const std::string& table_name,
                                   const TestTable& table) {
  if (!table.measure_column_defs.empty()) {
    return;
  }
  std::unique_ptr<SimpleTable> simple_table =
      MakeSimpleTable(table_name, table);
  catalog_->AddOwnedTable(simple_table.release());
}

absl::Status TestDatabaseCatalog::AddTablesWithMeasures(
    const TestDatabase& test_db, const LanguageOptions& language_options) {
  ZETASQL_RETURN_IF_ERROR(IsInitialized());

  for (const auto& [table_name, table] : test_db.tables) {
    if (table.measure_column_defs.empty()) {
      continue;
    }
    ZETASQL_RET_CHECK(!table.row_identity_columns.empty());
    // TODO: b/350555383 - Value tables should be supported. Remove this.
    ZETASQL_RET_CHECK(!table.options.is_value_table());
    const Value& array_value = table.table_as_value;
    ZETASQL_RET_CHECK(array_value.type()->IsArray())
        << table_name << " " << array_value.DebugString(true);
    auto element_type = array_value.type()->AsArray()->element_type();
    std::unique_ptr<SimpleTable> simple_table =
        MakeSimpleTable(table_name, table);
    ZETASQL_RET_CHECK_OK(
        simple_table->SetRowIdentityColumns(table.row_identity_columns));
    AnalyzerOptions analyzer_options(language_options);
    // Make sure that the test table's required features are enabled.
    for (LanguageFeature feature : table.options.required_features()) {
      analyzer_options.mutable_language()->EnableLanguageFeature(feature);
    }
    ZETASQL_ASSIGN_OR_RETURN(
        auto measure_expr_analyzer_outputs,
        AddMeasureColumnsToTable(*simple_table, table.measure_column_defs,
                                 *type_factory_, *catalog_, analyzer_options));
    sql_object_artifacts_.insert(
        sql_object_artifacts_.end(),
        std::make_move_iterator(measure_expr_analyzer_outputs.begin()),
        std::make_move_iterator(measure_expr_analyzer_outputs.end()));
    ZETASQL_RET_CHECK(element_type->IsStruct());
    ZETASQL_ASSIGN_OR_RETURN(
        Value new_array_value,
        UpdateTableRowsWithMeasureValues(
            array_value, simple_table.get(), table.row_identity_columns,
            type_factory_.get(), language_options));
    table.table_as_value_with_measures = std::move(new_array_value);
    catalog_->AddOwnedTable(simple_table.release());
  }
  return absl::OkStatus();
}

absl::Status TestDatabaseCatalog::AddUdfsForMeasureDefinitions(
    const TestDatabase& test_db, const LanguageOptions& language_options) {
  ZETASQL_RETURN_IF_ERROR(IsInitialized());
  if (!test_db.measure_function_defs.empty()) {
    AnalyzerOptions analyzer_options(language_options);
    analyzer_options.mutable_language()->AddSupportedStatementKind(
        RESOLVED_CREATE_FUNCTION_STMT);
    auto func_opts = FunctionOptions()
                         // TODO: Relax this once IFERROR
                         //     non-determinism is mitigated.
                         .set_supports_safe_error_mode(false)
                         .set_supports_distinct_modifier(false)
                         .set_supports_having_modifier(false);
    for (const auto& stmt : test_db.measure_function_defs) {
      std::unique_ptr<const AnalyzerOutput> analyzer_output;
      ZETASQL_RETURN_IF_ERROR(
          AddFunctionFromCreateFunction(stmt, analyzer_options,
                                        /*allow_persistent_function=*/false,
                                        /*function_options=*/&func_opts,
                                        analyzer_output, *catalog_, *catalog_));
      sql_object_artifacts_.push_back(std::move(analyzer_output));
    }
  }
  return absl::OkStatus();
}

absl::Status TestDatabaseCatalog::FindTable(
    const absl::Span<const std::string> path, const Table** table,
    const Catalog::FindOptions& options) {
  ZETASQL_RETURN_IF_ERROR(IsInitialized());
  return catalog_->FindTable(path, table, options);
}

absl::Status TestDatabaseCatalog::GetTables(
    absl::flat_hash_set<const Table*>* output) const {
  ZETASQL_RETURN_IF_ERROR(IsInitialized());
  ZETASQL_RET_CHECK_NE(output, nullptr);
  ZETASQL_RET_CHECK(output->empty());
  return catalog_->GetTables(output);
}

absl::Status TestDatabaseCatalog::GetTypes(
    absl::flat_hash_set<const Type*>* output) const {
  ZETASQL_RETURN_IF_ERROR(IsInitialized());
  ZETASQL_RET_CHECK_NE(output, nullptr);
  ZETASQL_RET_CHECK(output->empty());
  return catalog_->GetTypes(output);
}

absl::Status TestDatabaseCatalog::SetTestDatabaseWithLeakyDescriptors(
    const TestDatabase& test_db) {
  importer_ = std::make_unique<ProtoImporter>(test_db.runs_as_test);
  catalog_ =
      std::make_unique<SimpleCatalog>("root_catalog", type_factory_.get());
  // Load protos and enums.
  ZETASQL_RETURN_IF_ERROR(LoadProtoEnumTypes(test_db.proto_files, test_db.proto_names,
                                     test_db.enum_names));

  // Add tables to the catalog.
  for (const auto& t : test_db.tables) {
    const std::string& table_name = t.first;
    const TestTable& test_table = t.second;
    AddTable(table_name, test_table);
  }
  is_initialized_ = true;
  return absl::OkStatus();
}

absl::Status TestDatabaseCatalog::SetTestDatabase(
    const TestDatabaseProto& test_db_proto) {
  importer_ = std::make_unique<ProtoImporter>(test_db_proto.runs_as_test());
  catalog_ =
      std::make_unique<SimpleCatalog>("root_catalog", type_factory_.get());

  std::set<std::string> proto_files(test_db_proto.proto_files().begin(),
                                    test_db_proto.proto_files().end());
  std::set<std::string> proto_names(test_db_proto.proto_names().begin(),
                                    test_db_proto.proto_names().end());
  std::set<std::string> enum_names(test_db_proto.enum_names().begin(),
                                   test_db_proto.enum_names().end());

  // Load protos and enums.
  ZETASQL_RETURN_IF_ERROR(LoadProtoEnumTypes(proto_files, proto_names, enum_names));

  // With a fresh TypeFactory and DescriptorPool, we can now deserialize the
  // test database.
  ZETASQL_ASSIGN_OR_RETURN(TestDatabase test_db,
                   DeserializeTestDatabase(test_db_proto, type_factory_.get(),
                                           descriptor_pool()));

  // Add tables to the catalog.
  for (const auto& t : test_db.tables) {
    const std::string& table_name = t.first;
    const TestTable& test_table = t.second;
    AddTable(table_name, test_table);
  }
  is_initialized_ = true;
  return absl::OkStatus();
}

absl::Status TestDatabaseCatalog::SetLanguageOptions(
    const LanguageOptions& language_options) {
  if (catalog_ == nullptr) {
    return absl::FailedPreconditionError(
        "Cannot set language options since underlying catalog is unexpectedly "
        "null. TestDatabaseCatalog::SetTestDatabase() must be called first, "
        "before calling TestDatabaseCatalog::SetLanguageOptions().");
  }
  return function_cache_->SetLanguageOptions(language_options, catalog_.get());
}
}  // namespace zetasql
