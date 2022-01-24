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

#ifndef ZETASQL_PUBLIC_SIMPLE_CATALOG_H_
#define ZETASQL_PUBLIC_SIMPLE_CATALOG_H_

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/simple_evaluator_table_iterator.h"
#include "zetasql/public/builtin_function.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/constant.h"
#include "zetasql/public/function.h"
#include "zetasql/public/procedure.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/type_deserializer.h"
#include "zetasql/public/value.h"
#include <cstdint>
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"

namespace zetasql {

class SimpleCatalogProto;
class SimpleColumn;
class SimpleColumnProto;
class SimpleConstantProto;
class SimpleTableProto;

// SimpleCatalog is a concrete implementation of the Catalog interface.
// It acts as a simple container for objects in the Catalog.
//
// This class is thread-safe.
class SimpleCatalog : public EnumerableCatalog {
 public:
  // Construct a Catalog with catalog name <name>.
  //
  // If <type_factory> is non-NULL, it will be stored (unowned) with this
  // SimpleCatalog and used to allocate any Types needed for the Catalog.
  // If <type_factory> is NULL, an owned TypeFactory will be constructed
  // internally when needed.
  explicit SimpleCatalog(const std::string& name,
                         TypeFactory* type_factory = nullptr);
  SimpleCatalog(const SimpleCatalog&) = delete;
  SimpleCatalog& operator=(const SimpleCatalog&) = delete;

  std::string FullName() const override { return name_; }

  absl::Status GetTable(const std::string& name, const Table** table,
                        const FindOptions& options = FindOptions()) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  absl::Status GetModel(const std::string& name, const Model** model,
                        const FindOptions& options = FindOptions()) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  absl::Status GetConnection(const std::string& name,
                             const Connection** connection,
                             const FindOptions& options) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  absl::Status GetFunction(const std::string& name, const Function** function,
                           const FindOptions& options = FindOptions()) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  absl::Status GetTableValuedFunction(
      const std::string& name, const TableValuedFunction** function,
      const FindOptions& options = FindOptions()) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  absl::Status GetProcedure(
      const std::string& name, const Procedure** procedure,
      const FindOptions& options = FindOptions()) override;

  absl::Status GetType(const std::string& name, const Type** type,
                       const FindOptions& options = FindOptions()) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  absl::Status GetCatalog(const std::string& name, Catalog** catalog,
                          const FindOptions& options = FindOptions()) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  absl::Status GetConstant(const std::string& name, const Constant** constant,
                           const FindOptions& options = FindOptions()) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  // For suggestions we look from the last level of <mistyped_path>:
  //  - Whether the object exists directly in sub-catalogs.
  //  - If not above, whether there is a single name that's misspelled in the
  //    current catalog.
  std::string SuggestTable(
      const absl::Span<const std::string>& mistyped_path) override;
  std::string SuggestFunction(
      const absl::Span<const std::string>& mistyped_path) override;
  std::string SuggestTableValuedFunction(
      const absl::Span<const std::string>& mistyped_path) override;
  std::string SuggestConstant(
      const absl::Span<const std::string>& mistyped_path) override;
  // TODO: Implement SuggestModel function.
  // TODO: Implement SuggestConnection function.

  // Add objects to the SimpleCatalog.
  //  - Names must be unique (case-insensitively) or the call will die.
  //  - For Add* methods, caller maintains ownership of all added objects, which
  // must outlive this catalog.
  //  - AddOwned* methods transfer ownership of the added object to the catalog.
  //  - Methods that do not take name of the object as a parameter use object's
  // own name.
  //  - *IfNotPresent methods attempt to add the object to the catalog, and
  // return false if the object is already present.
  //  - AddOwned*IfNotPresent methods will take ownership only if they
  // successfully added the object to the catalog.
  //  - AddOwned*IfNotPresent methods that take unique_ptr as a parameter
  //  will take ownership if they successfully added the object to the catalog,
  //  or deallocate the object otherwise.
  //
  // NOTE: Consider using unique_ptr version of AddOwned* methods where
  // available.

  // Tables
  void AddTable(absl::string_view name, const Table* table)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddTable(const Table* table) ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedTable(absl::string_view name, std::unique_ptr<const Table> table)
      ABSL_LOCKS_EXCLUDED(mutex_);
  bool AddOwnedTableIfNotPresent(absl::string_view name,
                                 std::unique_ptr<const Table> table)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedTable(std::unique_ptr<const Table> table)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedTable(absl::string_view name, const Table* table);
  void AddOwnedTable(const Table* table) ABSL_LOCKS_EXCLUDED(mutex_);

  // Models
  void AddModel(const std::string& name, const Model* model)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddModel(const Model* model) ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedModel(const std::string& name,
                     std::unique_ptr<const Model> model)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedModel(std::unique_ptr<const Model> model)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedModel(const std::string& name, const Model* model);
  void AddOwnedModel(const Model* model) ABSL_LOCKS_EXCLUDED(mutex_);

  // Connections
  void AddConnection(const std::string& name, const Connection* connection)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddConnection(const Connection* connection) ABSL_LOCKS_EXCLUDED(mutex_);

  // Types
  void AddType(const std::string& name, const Type* type)
      ABSL_LOCKS_EXCLUDED(mutex_);
  bool AddTypeIfNotPresent(const std::string& name, const Type* type)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Catalogs
  void AddCatalog(const std::string& name, Catalog* catalog)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddCatalog(Catalog* catalog) ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedCatalog(const std::string& name,
                       std::unique_ptr<Catalog> catalog);
  // TODO: Cleanup callers and delete
  void AddOwnedCatalog(std::unique_ptr<Catalog> catalog)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedCatalog(const std::string& name, Catalog* catalog);
  void AddOwnedCatalog(Catalog* catalog) ABSL_LOCKS_EXCLUDED(mutex_);
  bool AddOwnedCatalogIfNotPresent(const std::string& name,
                                   std::unique_ptr<Catalog> catalog)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Add a new (owned) SimpleCatalog named <name>, and return it.
  SimpleCatalog* MakeOwnedSimpleCatalog(const std::string& name)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Functions
  void AddFunction(const std::string& name, const Function* function)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddFunction(const Function* function) ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedFunction(const std::string& name,
                        std::unique_ptr<const Function> function);
  void AddOwnedFunction(std::unique_ptr<const Function> function)
      ABSL_LOCKS_EXCLUDED(mutex_);
  bool AddOwnedFunctionIfNotPresent(const std::string& name,
                                    std::unique_ptr<Function>* function);
  bool AddOwnedFunctionIfNotPresent(std::unique_ptr<Function>* function);
  void AddOwnedFunction(const std::string& name, const Function* function);
  void AddOwnedFunction(const Function* function) ABSL_LOCKS_EXCLUDED(mutex_);

  // Table Valued Functions
  void AddTableValuedFunction(const std::string& name,
                              const TableValuedFunction* function)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddTableValuedFunction(const TableValuedFunction* function)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedTableValuedFunction(
      const std::string& name,
      std::unique_ptr<const TableValuedFunction> function);
  void AddOwnedTableValuedFunction(
      std::unique_ptr<const TableValuedFunction> function);
  bool AddOwnedTableValuedFunctionIfNotPresent(
      const std::string& name,
      std::unique_ptr<TableValuedFunction>* table_function);
  bool AddOwnedTableValuedFunctionIfNotPresent(
      std::unique_ptr<TableValuedFunction>* table_function);
  void AddOwnedTableValuedFunction(const std::string& name,
                                   const TableValuedFunction* function);
  void AddOwnedTableValuedFunction(const TableValuedFunction* function)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Procedures
  void AddProcedure(const std::string& name, const Procedure* procedure);
  void AddProcedure(const Procedure* procedure) ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedProcedure(const std::string& name,
                         std::unique_ptr<const Procedure> procedure);
  void AddOwnedProcedure(std::unique_ptr<const Procedure> procedure)
      ABSL_LOCKS_EXCLUDED(mutex_);
  bool AddOwnedProcedureIfNotPresent(std::unique_ptr<Procedure> procedure)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedProcedure(const std::string& name, const Procedure* procedure);
  void AddOwnedProcedure(const Procedure* procedure)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Constants
  void AddConstant(const std::string& name, const Constant* constant);
  void AddConstant(const Constant* constant) ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedConstant(const std::string& name,
                        std::unique_ptr<const Constant> constant);
  void AddOwnedConstant(std::unique_ptr<const Constant> constant)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void AddOwnedConstant(const std::string& name, const Constant* constant);
  void AddOwnedConstant(const Constant* constant) ABSL_LOCKS_EXCLUDED(mutex_);
  bool AddOwnedConstantIfNotPresent(std::unique_ptr<const Constant> constant)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Add ZetaSQL built-in function definitions into this catalog.
  // <options> can be used to select which functions get loaded.
  // See builtin_function.h. Provided such functions are specified in <options>
  // this can add functions in both the global namespace and more specific
  // namespaces. If any of the selected functions are in namespaces,
  // sub-Catalogs will be created and the appropriate functions will be added in
  // those sub-Catalogs.
  // Also: Functions and Catalogs with the same names must not already exist.
  void AddZetaSQLFunctions(const ZetaSQLBuiltinFunctionOptions& options =
                                 ZetaSQLBuiltinFunctionOptions())
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Add ZetaSQL built-in function definitions into this catalog.
  // This can add functions in both the global namespace and more specific
  // namespaces. If any of the selected functions are in namespaces,
  // sub-Catalogs will be created and the appropriate functions will be added in
  // those sub-Catalogs.
  // Also: Functions and Catalogs with the same names must not already exist.
  // Functions are unowned, and must outlive this catalog.
  void AddZetaSQLFunctions(const std::vector<const Function*>& functions)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Set the google::protobuf::DescriptorPool to use when resolving Types.
  // All message and enum types declared in <pool> will be resolvable with
  // FindType or GetType, treating the full name as one identifier.
  // These type name lookups will be case sensitive.
  //
  // If overlapping names are registered using AddType, those names will
  // take precedence.  There is no check for ambiguous names, and currently
  // there is no mechanism to return an ambiguous type name error.
  //
  // The DescriptorPool can only be set once and cannot be changed.
  // Any types returned from <pool> must stay live for the lifetime of
  // this SimpleCatalog.
  //
  void SetDescriptorPool(const google::protobuf::DescriptorPool* pool)
      ABSL_LOCKS_EXCLUDED(mutex_);
  ABSL_DEPRECATED("Inline me!")
  void SetOwnedDescriptorPool(const google::protobuf::DescriptorPool* pool)
      ABSL_LOCKS_EXCLUDED(mutex_);
  void SetOwnedDescriptorPool(
      std::unique_ptr<const google::protobuf::DescriptorPool> pool)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Clear the set of functions stored in this Catalog and any subcatalogs
  // created for zetasql namespaces. Does not affect any other catalogs.
  // This can be called between calls to AddZetaSQLFunctions with different
  // options.
  void ClearFunctions() ABSL_LOCKS_EXCLUDED(mutex_);

  // Clear the set of table-valued functions stored in this Catalog and any
  // subcatalogs created for zetasql namespaces. Does not affect any other
  // catalogs.
  void ClearTableValuedFunctions() ABSL_LOCKS_EXCLUDED(mutex_);

  // Deserialize SimpleCatalog from proto. Types will be deserialized using
  // the TypeFactory owned by this catalog and given Descriptors from the
  // given DescriptorPools. The DescriptorPools should have been created by
  // type serialization, and all proto types in the catalog are treated as
  // references into these pools. The DescriptorPools must both outlive the
  // result SimpleCatalog.
  static absl::StatusOr<std::unique_ptr<SimpleCatalog>> Deserialize(
      const SimpleCatalogProto& proto,
      absl::Span<const google::protobuf::DescriptorPool* const> pools,
      const ExtendedTypeDeserializer* extended_type_deserializer = nullptr);
  ABSL_DEPRECATED("Inline me!")
  static absl::Status Deserialize(
      const SimpleCatalogProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      std::unique_ptr<SimpleCatalog>* result);

  // Serialize the SimpleCatalog to proto, optionally ignoring built-in
  // functions and recursive subcatalogs. file_descriptor_set_map is used
  // to store serialized FileDescriptorSets, which can be deserialized into
  // separate DescriptorPools in order to reconstruct the Type. The map may
  // be non-empty and may be used across calls to this method in order to
  // serialize multiple types. The map may NOT be null.
  // NOTE: recursion detection is done with seen catalogs pointers, which
  // may effectively detect multiple-step recursions, but also recognize
  // siblings pointing to the same catalog object as false positives.
  absl::Status Serialize(FileDescriptorSetMap* file_descriptor_set_map,
                         SimpleCatalogProto* proto, bool ignore_builtin = true,
                         bool ignore_recursive = true) const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Return a TypeFactory owned by this SimpleCatalog.
  TypeFactory* type_factory() ABSL_LOCKS_EXCLUDED(mutex_);

  absl::Status GetCatalogs(
      absl::flat_hash_set<const Catalog*>* output) const override;
  absl::Status GetTables(
      absl::flat_hash_set<const Table*>* output) const override;
  absl::Status GetTypes(
      absl::flat_hash_set<const Type*>* output) const override;
  absl::Status GetFunctions(
      absl::flat_hash_set<const Function*>* output) const override;

  // Accessors for reading a copy of the object lists in this SimpleCatalog.
  // This is intended primarily for tests.
  std::vector<const Table*> tables() const ABSL_LOCKS_EXCLUDED(mutex_);
  std::vector<const Model*> models() const ABSL_LOCKS_EXCLUDED(mutex_);
  std::vector<const Type*> types() const ABSL_LOCKS_EXCLUDED(mutex_);
  std::vector<const Function*> functions() const ABSL_LOCKS_EXCLUDED(mutex_);
  std::vector<const TableValuedFunction*> table_valued_functions() const
      ABSL_LOCKS_EXCLUDED(mutex_);
  std::vector<const Procedure*> procedures() const ABSL_LOCKS_EXCLUDED(mutex_);
  std::vector<Catalog*> catalogs() const ABSL_LOCKS_EXCLUDED(mutex_);
  std::vector<const Constant*> constants() const ABSL_LOCKS_EXCLUDED(mutex_);

  // Accessors for reading a copy of the key (object-name) lists in this
  // SimpleCatalog. Note that all keys are lower case.
  std::vector<std::string> table_names() const ABSL_LOCKS_EXCLUDED(mutex_);
  std::vector<std::string> model_names() const ABSL_LOCKS_EXCLUDED(mutex_);
  std::vector<std::string> function_names() const ABSL_LOCKS_EXCLUDED(mutex_);
  std::vector<std::string> table_valued_function_names() const
      ABSL_LOCKS_EXCLUDED(mutex_);
  std::vector<std::string> catalog_names() const ABSL_LOCKS_EXCLUDED(mutex_);
  std::vector<std::string> constant_names() const ABSL_LOCKS_EXCLUDED(mutex_);

 protected:
  absl::Status DeserializeImpl(const SimpleCatalogProto& proto,
                               const TypeDeserializer& type_deserializer);

 private:
  absl::Status SerializeImpl(absl::flat_hash_set<const Catalog*>* seen_catalogs,
                             FileDescriptorSetMap* file_descriptor_set_map,
                             SimpleCatalogProto* proto, bool ignore_builtin,
                             bool ignore_recursive) const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Helper methods for adding objects while holding <mutex_>.
  void AddCatalogLocked(const std::string& name, Catalog* catalog)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void AddOwnedCatalogLocked(const std::string& name,
                             std::unique_ptr<Catalog> catalog)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  // TODO: Refactor the Add*() methods for other object types
  // to use a common locked implementation, similar to these for Function.
  void AddFunctionLocked(const std::string& name, const Function* function)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void AddOwnedFunctionLocked(const std::string& name,
                              std::unique_ptr<const Function> function)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void AddTableValuedFunctionLocked(const std::string& name,
                                    const TableValuedFunction* table_function)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void AddOwnedTableValuedFunctionLocked(
      const std::string& name,
      std::unique_ptr<const TableValuedFunction> table_function)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void AddConstantLocked(const std::string& name, const Constant* constant)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Unified implementation of SuggestFunction and SuggestTableValuedFunction.
  std::string SuggestFunctionOrTableValuedFunction(
      bool is_table_valued_function,
      absl::Span<const std::string> mistyped_path);

  const std::string name_;

  mutable absl::Mutex mutex_;

  // The TypeFactory can be allocated lazily, so may be NULL.
  TypeFactory* type_factory_ ABSL_GUARDED_BY(mutex_);
  std::unique_ptr<TypeFactory> owned_type_factory_ ABSL_GUARDED_BY(mutex_);

  absl::flat_hash_map<std::string, const Table*> tables_
      ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string, const Connection*> connections_
      ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string, const Model*> models_
      ABSL_GUARDED_BY(mutex_);
  // Case-insensitive map of names to Types explicitly added to the Catalog via
  // AddType (including proto an enum types).
  absl::flat_hash_map<std::string, const Type*> types_ ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string, const Function*> functions_
      ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string, const TableValuedFunction*>
      table_valued_functions_ ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string, const Procedure*> procedures_
      ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string, Catalog*> catalogs_ ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string, const Constant*> constants_
      ABSL_GUARDED_BY(mutex_);

  std::vector<std::unique_ptr<const Table>> owned_tables_
      ABSL_GUARDED_BY(mutex_);
  std::vector<std::unique_ptr<const Model>> owned_models_
      ABSL_GUARDED_BY(mutex_);
  std::vector<std::unique_ptr<const Connection>> owned_connections_
      ABSL_GUARDED_BY(mutex_);
  std::vector<std::unique_ptr<const Function>> owned_functions_
      ABSL_GUARDED_BY(mutex_);
  std::vector<std::unique_ptr<const TableValuedFunction>>
      owned_table_valued_functions_ ABSL_GUARDED_BY(mutex_);
  std::vector<std::unique_ptr<const Procedure>> owned_procedures_
      ABSL_GUARDED_BY(mutex_);
  std::vector<std::unique_ptr<const Catalog>> owned_catalogs_
      ABSL_GUARDED_BY(mutex_);
  std::vector<std::unique_ptr<const Constant>> owned_constants_
      ABSL_GUARDED_BY(mutex_);

  // Subcatalogs added for zetasql function namespaces. Kept separate from
  // owned_catalogs_ to keep them as SimpleCatalog types.
  absl::flat_hash_map<std::string, std::unique_ptr<SimpleCatalog>>
      owned_zetasql_subcatalogs_ ABSL_GUARDED_BY(mutex_);

  const google::protobuf::DescriptorPool* descriptor_pool_ ABSL_GUARDED_BY(mutex_) =
      nullptr;
  std::unique_ptr<const google::protobuf::DescriptorPool> ABSL_GUARDED_BY(mutex_)
      owned_descriptor_pool_;
};

// SimpleTable is a concrete implementation of the Table interface.
class SimpleTable : public Table {
 public:
  // Make a table with columns with the given names and types.
  // Crashes if there are duplicate column names.
  //
  // If serialization is to be supported, 'serialization_id' will be returned
  // by calls to GetSerializationId(); it should be unique across all tables in
  // the same catalog.
  typedef std::pair<std::string, const Type*> NameAndType;
  typedef std::pair<std::string, AnnotatedType> NameAndAnnotatedType;
  SimpleTable(absl::string_view name, const std::vector<NameAndType>& columns,
              const int64_t serialization_id = 0);
  SimpleTable(absl::string_view name,
              const std::vector<NameAndAnnotatedType>& columns,
              const int64_t serialization_id = 0);

  // Make a table with the given Columns.
  // Crashes if there are duplicate column names.
  // Takes ownership of elements of <columns> if <take_ownership> is true.
  SimpleTable(absl::string_view name, const std::vector<const Column*>& columns,
              bool take_ownership = false, const int64_t serialization_id = 0);

  // Make a value table with row type <row_type>.
  // This constructor inserts a single column of type <row_type> into
  // <columns_>, with the default name "value".  This column name "value"
  // is not visible in SQL but will be appear in the resolved AST.
  //
  // One implication of this implementation is that FindColumnByName() will
  // only be able to find the column with name "value", not the top level table
  // columns of this SimpleTable (and the top level table columns of this
  // SimpleTable do not have an associated Column).
  SimpleTable(absl::string_view, const Type* row_type, const int64_t id = 0);

  // Make a table with no Columns.  (Other constructors are ambiguous for this.)
  explicit SimpleTable(absl::string_view, const int64_t id = 0);

  SimpleTable(const SimpleTable&) = delete;
  SimpleTable& operator=(const SimpleTable&) = delete;

  std::string Name() const override { return name_; }
  std::string FullName() const override {
    return full_name_.empty() ? name_ : full_name_;
  }

  int NumColumns() const override { return columns_.size(); }
  const Column* GetColumn(int i) const override {
    // Note: The column interface does not define what should happen
    // when `i` is out of range.
    if (i < 0 || i >= columns_.size()) {
      return nullptr;
    }
    return columns_[i];
  }
  const Column* FindColumnByName(const std::string& name) const override;
  std::optional<std::vector<int>> PrimaryKey() const override {
    return primary_key_;
  };

  bool IsValueTable() const override { return is_value_table_; }

  void set_is_value_table(bool value) { is_value_table_ = value; }

  bool AllowAnonymousColumnName() const { return allow_anonymous_column_name_; }

  // Setter for allow_anonymous_column_name_. If the existing condition
  // conflicts with the value to be set, the setting will fail.
  absl::Status set_allow_anonymous_column_name(bool value) {
    ZETASQL_RET_CHECK(value || !anonymous_column_seen_);
    allow_anonymous_column_name_ = value;
    return absl::OkStatus();
  }

  bool AllowDuplicateColumnNames() const {
    return allow_duplicate_column_names_;
  }

  // Setter for allow_duplicate_column_names_. If the existing condition
  // conflicts with the value to be set, the setting will fail.
  absl::Status set_allow_duplicate_column_names(bool value) {
    ZETASQL_RET_CHECK(value || duplicate_column_names_.empty());
    allow_duplicate_column_names_ = value;
    return absl::OkStatus();
  }

  // Add a column. Returns an error if constraints allow_anonymous_column_name_
  // or allow_duplicate_column_names_ are violated.
  // If is_owned is set to true but an error is returned, the column will be
  // deleted inside this function.
  absl::Status AddColumn(const Column* column, bool is_owned);

  // Set primary key with give column ordinal indexes.
  absl::Status SetPrimaryKey(std::vector<int> primary_key);

  // Set the full name. If empty, name will be used as the full name.
  absl::Status set_full_name(absl::string_view full_name) {
    if (full_name != name_) {
      full_name_ = full_name;
    } else {
      full_name_ = "";
    }
    return absl::OkStatus();
  }

  int64_t GetSerializationId() const override { return id_; }

  // Constructs an EvaluatorTableIterator from a list of column indexes.
  // Represents the signature of Table::CreateEvaluatorTableIterator().
  using EvaluatorTableIteratorFactory =
      std::function<absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>(
          absl::Span<const int>)>;

  // Sets a factory to be returned by CreateEvaluatorTableIterator().
  // CAVEAT: This is not preserved by serialization/deserialization. It is only
  // relevant to users of the evaluator API defined in public/evaluator.h.
  void SetEvaluatorTableIteratorFactory(
      const EvaluatorTableIteratorFactory& factory) {
    evaluator_table_iterator_factory_ =
        absl::make_unique<EvaluatorTableIteratorFactory>(factory);
  }

  // Convenience method that calls SetEvaluatorTableIteratorFactory to
  // correspond to a list of rows. More specifically, sets the table contents
  // to a copy of 'rows' and sets up a callback to return those values when
  // CreateEvaluatorTableIterator() is called.
  // CAVEAT: This is not preserved by serialization/deserialization.  It is only
  // relevant to users of the evaluator API defined in public/evaluator.h.
  void SetContents(const std::vector<std::vector<Value>>& rows);

  absl::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
  CreateEvaluatorTableIterator(
      absl::Span<const int> column_idxs) const override;

  // Sets the <anonymization_info_> with the specified <userid_column_name>
  // (overwriting any previous anonymization info).  An error is returned if
  // the named column is ambiguous or does not exist in this table.
  //
  // Setting the AnonymizationInfo defines this table as supporting
  // anonymization semantics and containing sensitive private data.
  absl::Status SetAnonymizationInfo(const std::string& userid_column_name);

  // Same as above, but with the specified <userid_column_name_path>.
  absl::Status SetAnonymizationInfo(
      absl::Span<const std::string> userid_column_name_path);

  // Resets <anonymization_info_>, implying that the table does not support
  // anonymization queries.
  void ResetAnonymizationInfo() {
    anonymization_info_.reset();
  }

  // Returns anonymization info for a table, including a column reference that
  // indicates the userid column for anonymization purposes.
  std::optional<const AnonymizationInfo> GetAnonymizationInfo() const override {
    if (anonymization_info_ != nullptr) {
      return *anonymization_info_;
    }
    return std::optional<const AnonymizationInfo>();
  }

  // Serialize this table into protobuf. The provided map is used to store
  // serialized FileDescriptorSets, which can be deserialized into separate
  // DescriptorPools in order to reconstruct the Type. The map may be
  // non-empty and may be used across calls to this method in order to
  // serialize multiple types. The map may NOT be null.
  absl::Status Serialize(
      FileDescriptorSetMap* file_descriptor_set_map,
      SimpleTableProto* proto) const;

  // Deserializes SimpleTable from proto using TypeDeserializer.
  static absl::StatusOr<std::unique_ptr<SimpleTable>> Deserialize(
      const SimpleTableProto& proto, const TypeDeserializer& deserializer);

  // Deserialize SimpleTable from proto. Types will be deserialized using
  // the given TypeFactory and Descriptors from the given DescriptorPools.
  // The DescriptorPools should have been created by type serialization for
  // columns, and all proto type are treated as references into these pools.
  // The TypeFactory and the DescriptorPools must both outlive the result
  // SimpleTable.
  ABSL_DEPRECATED("Inline me!")
  static absl::Status Deserialize(
      const SimpleTableProto& proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      TypeFactory* factory,
      std::unique_ptr<SimpleTable>* result);

 protected:
  // Returns the current contents (passed to the last call to SetContents()) in
  // column-major order.
  const std::vector<std::shared_ptr<const std::vector<Value>>>&
  column_major_contents() const {
    return column_major_contents_;
  }

  // Returns the number of rows set in the last call to SetContents().
  int64_t num_rows() const { return num_rows_; }

 private:
  // Insert a column to columns_map_. Return error when
  // allow_anonymous_column_name_ or allow_duplicate_column_names_ are violated.
  // Furthermore, if the column's name is duplicated, it's recorded in
  // duplicate_column_names_ and the original column is removed from
  // columns_map_.
  absl::Status InsertColumnToColumnMap(const Column* column);

  const std::string name_;
  std::string full_name_;
  bool is_value_table_ = false;
  std::vector<const Column*> columns_;
  std::optional<std::vector<int>> primary_key_;
  std::vector<std::unique_ptr<const Column>> owned_columns_;
  absl::flat_hash_map<std::string, const Column*> columns_map_;
  absl::flat_hash_set<std::string> duplicate_column_names_;
  int64_t id_ = 0;
  // Does not own the referenced Column* (if set).
  std::unique_ptr<AnonymizationInfo> anonymization_info_;
  bool allow_anonymous_column_name_ = false;
  bool anonymous_column_seen_ = false;
  bool allow_duplicate_column_names_ = false;

  // We use shared_ptrs to handle calls to SetContets() while there are
  // iterators outstanding.
  int64_t num_rows_ = 0;
  std::vector<std::shared_ptr<const std::vector<Value>>> column_major_contents_;
  std::unique_ptr<EvaluatorTableIteratorFactory>
      evaluator_table_iterator_factory_;

  static absl::Status ValidateNonEmptyColumnName(
      const std::string& column_name);
};

// SimpleModel is a concrete implementation of the Model interface.
class SimpleModel : public Model {
 public:
  // Make a model with input and output columns with the given names and types.
  // Crashes if there are duplicate column names.
  typedef std::pair<std::string, const Type*> NameAndType;
  SimpleModel(const std::string& name, const std::vector<NameAndType>& inputs,
              const std::vector<NameAndType>& outputs, const int64_t id = 0);

  // Make a model with the given inputs and outputs.
  // Crashes if there are duplicate column names.
  // Takes ownership of elements of <inputs> and <outputs> if <take_ownership>
  // is true.
  SimpleModel(const std::string& name, const std::vector<const Column*>& inputs,
              const std::vector<const Column*>& outputs,
              bool take_ownership = false, const int64_t id = 0);

  SimpleModel(const SimpleModel&) = delete;
  SimpleModel& operator=(const SimpleModel&) = delete;

  std::string Name() const override { return name_; }
  std::string FullName() const override { return name_; }

  uint64_t NumInputs() const override { return inputs_.size(); }
  // i must be less than NumInputs.
  const Column* GetInput(int i) const override { return inputs_[i]; }
  const Column* FindInputByName(const std::string& name) const override;

  uint64_t NumOutputs() const override { return outputs_.size(); }
  // i must be less than NumOutputs.
  const Column* GetOutput(int i) const override { return outputs_[i]; }
  const Column* FindOutputByName(const std::string& name) const override;

  // Add an input.
  // If is_owned is set to true but an error is returned, the column will be
  // deleted inside this function.
  absl::Status AddInput(const Column* column, bool is_owned);
  // Add an output.
  // If is_owned is set to true but an error is returned, the column will be
  // deleted inside this function.
  absl::Status AddOutput(const Column* column, bool is_owned);

  int64_t GetSerializationId() const override { return id_; }

  // TODO: Add serialize and deserialize functions.
 private:
  const std::string name_;
  // Columns added to the <inputs_>, <outputs_> and the corresponding maps may
  // be owned by this class or not. In case they are owned by this class, they
  // will be added to <owned_inputs_outputs_> and will be deleted at destruction
  // time.
  absl::flat_hash_map<std::string, const Column*> inputs_map_;
  std::vector<const Column*> inputs_;

  absl::flat_hash_map<std::string, const Column*> outputs_map_;
  std::vector<const Column*> outputs_;

  // All the input and output columns that this class owns.
  std::vector<std::unique_ptr<const Column>> owned_inputs_outputs_;
  int64_t id_ = 0;
};

class SimpleConnection : public Connection {
 public:
  explicit SimpleConnection(const std::string& name) : name_(name) {}
  SimpleConnection(const SimpleConnection&) = delete;
  SimpleConnection& operator=(const Connection&) = delete;

  std::string Name() const override { return name_; }
  std::string FullName() const override { return name_; }

  // TODO: Add serialize and deserialize functions.
 private:
  const std::string name_;
};

// SimpleColumn is a concrete implementation of the Column interface.
class SimpleColumn : public Column {
 public:
  // Constructor.
  SimpleColumn(const std::string& table_name, const std::string& name,
               const Type* type, bool is_pseudo_column = false,
               bool is_writable_column = true);
  SimpleColumn(const std::string& table_name, const std::string& name,
               AnnotatedType annotated_type, bool is_pseudo_column = false,
               bool is_writable_column = true);
  SimpleColumn(const SimpleColumn&) = delete;
  SimpleColumn& operator=(const SimpleColumn&) = delete;

  ~SimpleColumn() override;

  std::string Name() const override { return name_; }
  std::string FullName() const override { return full_name_; }
  const Type* GetType() const override { return annotated_type_.type; }
  const AnnotationMap* GetTypeAnnotationMap() const override {
    return annotated_type_.annotation_map;
  }
  AnnotatedType annotated_type() const { return annotated_type_; }

  bool IsPseudoColumn() const override { return is_pseudo_column_; }
  bool IsWritableColumn() const override { return is_writable_column_; }

  void set_is_pseudo_column(bool v) { is_pseudo_column_ = v; }

  // Serialize this column into protobuf, the provided map is used to store
  // serialized FileDescriptorSets, which can be deserialized into separate
  // DescriptorPools in order to reconstruct the Type. The map may be
  // non-empty and may be used across calls to this method in order to
  // serialize multiple types. The map may NOT be null.
  absl::Status Serialize(FileDescriptorSetMap* file_descriptor_set_map,
                         SimpleColumnProto* proto) const;

  // Deserializes SimpleColumn from proto using TypeDeserializer.
  static absl::StatusOr<std::unique_ptr<SimpleColumn>> Deserialize(
      const SimpleColumnProto& proto, const std::string& table_name,
      const TypeDeserializer& type_deserializer);

 private:
  const std::string name_;
  const std::string full_name_;
  bool is_pseudo_column_ = false;
  bool is_writable_column_ = true;
  AnnotatedType annotated_type_;
};

// A named constant with a concrete value in the catalog.
class SimpleConstant : public Constant {
 public:
  // Creates and returns a SimpleConstant, returning an error if <value> is
  // an invalid Value or the <name_path> is empty.
  static absl::Status Create(const std::vector<std::string>& name_path,
                             const Value& value,
                             std::unique_ptr<SimpleConstant>* simple_constant);

  ~SimpleConstant() override {}

  // This class is neither copyable nor assignable.
  SimpleConstant(const SimpleConstant& other_simple_constant) = delete;
  SimpleConstant& operator=(const SimpleConstant& other_simple_constant) =
      delete;

  // Serializes this SimpleConstant to proto.
  //
  // See SimpleCatalog::Serialize() for details about <file_descriptor_set_map>.
  absl::Status Serialize(FileDescriptorSetMap* file_descriptor_set_map,
                         SimpleConstantProto* simple_constant_proto) const;

  // Deserializes this SimpleConstant from proto.
  //
  // See SimpleCatalog::Deserialize() for details about <descriptor_pools>.
  static absl::StatusOr<std::unique_ptr<SimpleConstant>> Deserialize(
      const SimpleConstantProto& simple_constant_proto,
      const TypeDeserializer& type_deserializer);

  const Type* type() const override { return value_.type(); }

  const Value& value() const { return value_; }

  // Returns a string describing this Constant for debugging purposes.
  std::string DebugString() const override;
  // Same as the previous, but includes the Type debug string.
  std::string VerboseDebugString() const;

 private:
  SimpleConstant(std::vector<std::string> name_path, Value value)
      : Constant(std::move(name_path)), value_(std::move(value)) {}

  // The value of this Constant. This is the RHS in a CREATE CONSTANT statement.
  Value value_;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_SIMPLE_CATALOG_H_
