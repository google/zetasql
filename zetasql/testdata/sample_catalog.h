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

#ifndef ZETASQL_TESTDATA_SAMPLE_CATALOG_H_
#define ZETASQL_TESTDATA_SAMPLE_CATALOG_H_

#include <memory>

#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor_database.h"
#include "zetasql/public/constant.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/type.h"
#include "absl/container/node_hash_map.h"

namespace zetasql {

// SampleCatalog provides a SimpleCatalog loaded with a shared sample schema,
// used by several tests.  Look at the .cc file to see what's in the Catalog.
// All proto types compiled into this binary will be available in the catalog.
class SampleCatalog {
 public:
  // Default constructor using default LanguageOptions and a locally owned
  // TypeFactory.
  SampleCatalog();
  // Constructor given 'language_options' and an optional 'type_factory'.
  // If 'type_factory' is specified then it must outlive this SampleCatalog
  // and this SampleCatalog does not take ownership of it.  If 'type_factory'
  // is not specified then a locally owned TypeFactory is created and
  // used instead.
  explicit SampleCatalog(const LanguageOptions& language_options,
                         TypeFactory* type_factory = nullptr);
  SampleCatalog(const SampleCatalog&) = delete;
  SampleCatalog& operator=(const SampleCatalog&) = delete;
  ~SampleCatalog();

  SimpleCatalog* catalog() { return catalog_.get(); }

  TypeFactory* type_factory() { return types_; }

  // Useful for configuring EvaluatorTableIterators for tables in the catalog.
  SimpleTable* GetTableOrDie(const std::string& name);
  zetasql_base::StatusOr<SimpleTable*> GetTable(const std::string& name);

  // Get the SimpleCatalog from a static singleton default SampleCatalog
  // (with default LanguageOptions()).  The caller must not modify this
  // default catalog since it can be reused.
  static const SimpleCatalog* const Get();

 private:
  std::unique_ptr<google::protobuf::DescriptorPoolDatabase> alt_descriptor_database_;
  std::unique_ptr<google::protobuf::DescriptorPool> alt_descriptor_pool_;
  std::unique_ptr<google::protobuf::DescriptorPool> ambiguous_has_descriptor_pool_;
  std::unique_ptr<TypeFactory> internal_type_factory_;
  std::unique_ptr<SimpleCatalog> catalog_;
  TypeFactory* types_;  // Not owned.

  void LoadCatalog(const LanguageOptions& language_options);
  void LoadCatalogBuiltins(const LanguageOptions& language_options);
  void LoadCatalogImpl(const LanguageOptions& language_options);
  void LoadTypes();
  void LoadTables();
  void LoadProtoTables();
  void LoadNestedCatalogs();
  void AddFunctionWithArgumentType(std::string type_name, const Type* arg_type);
  void LoadFunctions();
  void LoadTemplatedSQLUDFs();

  // Loads several table-valued functions into the sample catalog. For a full
  // list of the signatures added, please see the beginning of the method
  // definition. LoadTableValuedFunctions() has gotten so large that we have to
  // split it up in order to avoid lint warnings.
  void LoadTableValuedFunctions1();
  void LoadTableValuedFunctions2();
  void LoadTVFWithExtraColumns();
  void LoadConnectionTableValuedFunctions();
  void LoadDescriptorTableValuedFunctions();
  void LoadTableValuedFunctionsWithDeprecationWarnings();
  void LoadTemplatedSQLTableValuedFunctions();
  void AddProcedureWithArgumentType(std::string type_name,
                                    const Type* arg_type);
  void LoadProcedures();
  void LoadConstants();
  void LoadConnections();

  void AddOwnedTable(SimpleTable* table);

  const ProtoType* GetProtoType(const google::protobuf::Descriptor* descriptor);
  const EnumType* GetEnumType(const google::protobuf::EnumDescriptor* descriptor);

  const ArrayType* int32array_type_;
  const ArrayType* int64array_type_;
  const ArrayType* uint32array_type_;
  const ArrayType* uint64array_type_;
  const ArrayType* bytes_array_type_;
  const ArrayType* bool_array_type_;
  const ArrayType* float_array_type_;
  const ArrayType* double_array_type_;
  const ArrayType* date_array_type_;
  const ArrayType* string_array_type_;
  const ArrayType* timestamp_array_type_;
  const ArrayType* proto_array_type_;
  const ArrayType* struct_array_type_;

  const EnumType* enum_TestEnum_;
  const EnumType* enum_AnotherTestEnum_;
  const EnumType* enum_TestEnumWithAnnotations_;
  const ProtoType* proto_KitchenSinkPB_;
  const ProtoType* proto_MessageWithKitchenSinkPB_;
  const ProtoType* proto_CivilTimeTypesSinkPB_;
  const ProtoType* proto_TestExtraPB_;
  const ProtoType* proto_EmptyMessage_;
  const ProtoType* proto3_KitchenSinkPB_;
  const ProtoType* proto_ambiguous_has_;
  const ProtoType* proto_field_formats_proto_;

  const StructType* struct_type_;
  const StructType* nested_struct_type_;
  const StructType* doubly_nested_struct_type_;
  const StructType* struct_with_array_field_type_;
  const StructType* struct_with_one_field_type_;
  const StructType* struct_with_kitchen_sink_type_;

  const SimpleTable* key_value_table_;

  static SampleCatalog* instance_;

  // A constant to load. Owned by this catalog to get coverage for
  // SimpleCatalog::AddConstant().
  std::unique_ptr<SimpleConstant> owned_constant_;

  // Pointers are owned by 'catalog_'.
  absl::node_hash_map<std::string, SimpleTable*> tables_;

  // Connections owned by this catalog.
  std::unordered_map<std::string, std::unique_ptr<SimpleConnection>>
      owned_connections_;
};

}  // namespace zetasql

#endif  // ZETASQL_TESTDATA_SAMPLE_CATALOG_H_
