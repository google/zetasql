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

#ifndef ZETASQL_COMPLIANCE_TEST_DATABASE_CATALOG_H_
#define ZETASQL_COMPLIANCE_TEST_DATABASE_CATALOG_H_

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "zetasql/compliance/test_driver.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/types/span.h"
#include "google/protobuf/compiler/importer.h"

namespace zetasql {

// Class which manages a Catalog constructed from a TestDatabase and
// LanguageOptions.
class TestDatabaseCatalog {
 public:
  explicit TestDatabaseCatalog(TypeFactory* type_factory);

  SimpleCatalog* catalog() const { return catalog_.get(); }
  // Creates a catalog from `test_db` and preloads types and functions. This
  // method is to support analyzing randomly generating ZetaSQL measure
  // expressions. In general, users should refrain from using this method
  // and use `SetTestDatabase` instead.
  absl::Status CreateCatalogAndPreloadTypesAndFunctions(
      const TestDatabase& test_db);
  absl::Status SetTestDatabase(const TestDatabase& test_db);
  absl::Status SetLanguageOptions(const LanguageOptions& language_options);
  // Populate the `table_as_value_with_measures` field for all tables with
  // measure columns in `test_db`.
  absl::Status AddTablesWithMeasures(const TestDatabase& test_db,
                                     const LanguageOptions& language_options);

  absl::Status IsInitialized() const;

  void AddTable(const std::string& table_name, const TestTable& table);
  absl::Status LoadProtoEnumTypes(const std::set<std::string>& filenames,
                                  const std::set<std::string>& proto_names,
                                  const std::set<std::string>& enum_names);

  // Catalog- and EnumerableCatalog-analogous functions needed by callers.
  absl::Status FindTable(
      absl::Span<const std::string> path, const Table** table,
      const Catalog::FindOptions& options = Catalog::FindOptions());
  absl::Status GetTables(absl::flat_hash_set<const Table*>* output) const;
  absl::Status GetTypes(absl::flat_hash_set<const Type*>* output) const;

  google::protobuf::compiler::Importer* importer() const { return importer_.get(); }

 private:
  class BuiltinFunctionCache {
   public:
    ~BuiltinFunctionCache();
    absl::Status SetLanguageOptions(const LanguageOptions& options,
                                    SimpleCatalog* catalog);
    void DumpStats();

   private:
    using BuiltinFunctionMap =
        absl::flat_hash_map<std::string, std::unique_ptr<Function>>;
    using BuiltinTypeMap = absl::flat_hash_map<std::string, const Type*>;
    struct CacheEntry {
      BuiltinFunctionMap functions;
      BuiltinTypeMap types;
    };
    int total_calls_ = 0;
    int cache_hit_ = 0;
    absl::flat_hash_map<LanguageOptions, CacheEntry> builtins_cache_;
  };

  // Only true after `SetTestDatabase` is called.
  bool is_initialized_ = false;
  std::vector<std::string> errors_;
  std::unique_ptr<google::protobuf::compiler::SourceTree> proto_source_tree_;
  std::unique_ptr<google::protobuf::compiler::MultiFileErrorCollector>
      proto_error_collector_;
  std::unique_ptr<google::protobuf::compiler::Importer> importer_;
  std::unique_ptr<BuiltinFunctionCache> function_cache_;
  std::unique_ptr<SimpleCatalog> catalog_;
  TypeFactory* type_factory_;
  std::vector<std::unique_ptr<const AnalyzerOutput>> analyzed_measure_outputs_;
};

}  // namespace zetasql

#endif  // ZETASQL_COMPLIANCE_TEST_DATABASE_CATALOG_H_
