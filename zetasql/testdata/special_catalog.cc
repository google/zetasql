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

#include "zetasql/testdata/special_catalog.h"

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/type.h"
#include "zetasql/testdata/test_schema.pb.h"
#include <cstdint>
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/map_util.h"

namespace zetasql {

// Simplified version of SimpleColumn to allow empty column names.
class ColumnAllowEmptyName : public Column {
 public:
  ColumnAllowEmptyName(const std::string& table_name, const std::string& name,
                       const Type* type)
      : name_(name),
        full_name_(absl::StrCat(table_name, ".", name)),
        type_(type) {}
  ~ColumnAllowEmptyName() override {}

  std::string Name() const override { return name_; }
  std::string FullName() const override { return full_name_; }
  const Type* GetType() const override { return type_; }
  bool IsPseudoColumn() const override { return false; }

 private:
  const std::string name_;
  const std::string full_name_;
  const Type* type_;
};

// Simplified version of SimpleTable to support anonymous / duplicated column
// names.
class TableWithAnonymousAndDuplicatedColumnNames : public Table {
 public:
  // Make a table with columns with the given names and types.
  typedef std::pair<std::string, const Type*> NameAndType;
  TableWithAnonymousAndDuplicatedColumnNames(
      const std::string& name, const std::vector<NameAndType>& columns)
      : name_(name) {
    std::set<std::string> column_names;
    for (const NameAndType& name_and_type : columns) {
      const std::string& column_name = name_and_type.first;
      if (!zetasql_base::ContainsKey(column_names, column_name)) {
        zetasql_base::InsertOrDie(&column_names, column_name);
      } else {
        zetasql_base::InsertIfNotPresent(&duplicated_column_names_, column_name);
      }
    }
    for (const NameAndType& name_and_type : columns) {
      const std::string& column_name = name_and_type.first;
      const Type* column_type = name_and_type.second;
      columns_.emplace_back(new ColumnAllowEmptyName(
          name, column_name, column_type));
      if (!zetasql_base::ContainsKey(duplicated_column_names_, column_name)) {
        zetasql_base::InsertOrDie(&column_by_unique_name_, column_name,
                         columns_.back().get());
      }
    }
  }
  ~TableWithAnonymousAndDuplicatedColumnNames() override {}

  std::string Name() const override { return name_; }
  std::string FullName() const override { return name_; }

  int NumColumns() const override { return columns_.size(); }
  const Column* GetColumn(int i) const override { return columns_[i].get(); }

  // FindColumnByName only works for columns with a unique <name>. For columns
  // with duplicated name, it returns nullptr. Also, there is no name that can
  // be used to find anonymous columns.
  const Column* FindColumnByName(const std::string& name) const override {
    return zetasql_base::FindWithDefault(column_by_unique_name_, name, nullptr);
  }

  bool IsValueTable() const override { return false; }

  int64_t GetSerializationId() const override { return 0; }

 private:
  const std::string name_;
  std::vector<std::unique_ptr<Column>> columns_;
  std::map<std::string, Column*> column_by_unique_name_;
  std::set<std::string> duplicated_column_names_;
};

std::unique_ptr<SimpleCatalog> GetSpecialCatalog() {
  auto catalog = absl::make_unique<SimpleCatalog>("special_catalog",
                                                  nullptr /* type_factory */);

  TypeFactory* types = catalog->type_factory();
  catalog->AddOwnedTable(new TableWithAnonymousAndDuplicatedColumnNames(
        "TableWithAnonymousAndDuplicatedColumnNames",
        {{"key", types->get_int32()},
         {"" /* anonymous column name */, types->get_int32()},
         {"DuplicatedColumnName", types->get_int32()},
         {"DuplicatedColumnName", types->get_int32()}}));

  catalog->AddOwnedTable(new TableWithAnonymousAndDuplicatedColumnNames(
        "TableWithTwoAnonymousColumns",
        {{"key", types->get_int32()},
         {"" /* anonymous column name */, types->get_int32()},
         {"" /* anonymous column name */, types->get_int32()}}));

  // Add types with custom catalog path to the catalog.
  const EnumType* enum_EnumAnnotations = nullptr;
  ZETASQL_CHECK_OK(catalog->type_factory()->MakeEnumType(
      zetasql_test__::EnumAnnotations_NestedEnum_descriptor(),
      &enum_EnumAnnotations, {"custom", "catalog"}));
  catalog->AddType("zetasql_test__.EnumAnnotations.NestedEnum",
                    enum_EnumAnnotations);

  const ProtoType* proto_FakeStringInt32EntryType = nullptr;
  ZETASQL_CHECK_OK(catalog->type_factory()->MakeProtoType(
      zetasql_test__::FakeStringInt32EntryType::descriptor(),
      &proto_FakeStringInt32EntryType, {"custom", "catalog"}));
  catalog->AddType("zetasql_test__.FakeStringInt32EntryType",
                    proto_FakeStringInt32EntryType);

  // Add the sub-catalogs to make the re-analysis happy.
  auto sub_catalog = absl::make_unique<SimpleCatalog>("custom",
                                                      /*type_factory=*/nullptr);
  auto sub_catalog2 =
      absl::make_unique<SimpleCatalog>("catalog",
                                       /*type_factory=*/nullptr);
  sub_catalog2->AddType("zetasql_test__.EnumAnnotations.NestedEnum",
                        enum_EnumAnnotations);
  sub_catalog2->AddType("zetasql_test__.FakeStringInt32EntryType",
                        proto_FakeStringInt32EntryType);
  sub_catalog->AddOwnedCatalog(std::move(sub_catalog2));
  catalog->AddOwnedCatalog(std::move(sub_catalog));

  return catalog;
}

}  // namespace zetasql
