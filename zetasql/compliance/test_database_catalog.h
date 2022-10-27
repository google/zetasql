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

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "google/protobuf/compiler/importer.h"
#include "zetasql/compliance/test_driver.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type_factory.h"

namespace zetasql {

// Class which manages a Catalog constructed from a TestDatabase and
// LanguageOptions.
class TestDatabaseCatalog {
 public:
  explicit TestDatabaseCatalog(TypeFactory* type_factory);

  SimpleCatalog* catalog() const { return catalog_.get(); }
  absl::Status SetTestDatabase(const TestDatabase& test_db);
  void SetLanguageOptions(const LanguageOptions& language_options);

  void AddTable(const std::string& table_name, const TestTable& table);
  absl::Status LoadProtoEnumTypes(const std::set<std::string>& filenames,
                                  const std::set<std::string>& proto_names,
                                  const std::set<std::string>& enum_names);

 private:
  class BuiltinFunctionCache {
   public:
    ~BuiltinFunctionCache();
    absl::Status SetLanguageOptions(const LanguageOptions& options,
                                    SimpleCatalog* catalog);
    void DumpStats();

   private:
    using BuiltinFunctionMap = std::map<std::string, std::unique_ptr<Function>>;
    using BuiltinTypeMap = absl::flat_hash_map<std::string, const Type*>;
    struct CacheEntry {
      BuiltinFunctionMap functions;
      BuiltinTypeMap types;
    };
    int total_calls_ = 0;
    int cache_hit_ = 0;
    absl::flat_hash_map<LanguageOptions, CacheEntry> builtins_cache_;
  };

  std::vector<std::string> errors_;
  std::unique_ptr<google::protobuf::compiler::SourceTree> proto_source_tree_;
  std::unique_ptr<google::protobuf::compiler::MultiFileErrorCollector>
      proto_error_collector_;
  std::unique_ptr<google::protobuf::compiler::Importer> importer_;
  std::unique_ptr<BuiltinFunctionCache> function_cache_;
  std::unique_ptr<SimpleCatalog> catalog_;
  TypeFactory* type_factory_;
};

}  // namespace zetasql

#endif  // ZETASQL_COMPLIANCE_TEST_DATABASE_CATALOG_H_
