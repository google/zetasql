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

#ifndef ZETASQL_PUBLIC_CATALOG_H_
#define ZETASQL_PUBLIC_CATALOG_H_

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/type.h"
#include <cstdint>
#include "absl/types/span.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_builder.h"
#include "zetasql/base/statusor.h"

// TODO: Over time the number of files related to catalog objects
// has grown.  Consider creating a new public subdirectory for Catalog
// objects.

namespace zetasql {

class Column;
class Constant;
class CycleDetector;
class Function;
class Model;
class Procedure;
class Table;
class TableValuedFunction;

// ZetaSQL uses the Catalog interface to look up names that are visible
// at global scope inside a query.  Names include
//   - tables (which could also be views or other table-like objects)
//   - types (e.g. the named types defined in some database's schema)
//   - functions (e.g. UDFs defined in a database's schema)
//   - nested catalogs (which allow names like catalog1.catalog2.Table)
//   - table-valued functions (TVFs)
//   - procedures (can be invoked with CALL statement)
//   - named constants
//
// A Catalog includes a separate namespace for each of these object types.
// Objects of different types with the same name are allowed.
//
// Name resolution interfaces specify which object type they are looking for.
// When looking up a path like A.B.C, the first N-1 names on the path are
// normally looked up as Catalogs, and the final name is looked up as the
// requested object type.
//
// Name lookups are usually case insensitive.  However, Catalog
// implementations can use case sensitive lookups when appropriate.
// For example, a Catalog representing an external database with case
// sensitive table names can do case sensitive lookups in FindTable.
// Type name lookups for proto messages and proto enums are case sensitive.
//
// A ZetaSQL caller provides a Catalog implementation that can resolve all
// names that should be visible at global scope in the query.  The resolved
// query returned by ZetaSQL can include Table, etc. objects that were
// returned in Catalog lookups.
//
// Catalog has no method to list all visible top-level names.  This allows
// using namespaces that require expensive lookups to retrieve table
// definitions (e.g. datascape).  Catalog pointers are always non-const to allow
// lazy materialization of resolved objects.
//
// TODO: A batch lookup interface is possible if needed for latency.
// TODO: We could allow best-effort methods like ListTables so commands
// like show tables can be implemented inside ZetaSQL, if necessary.
//
// ZetaSQL will not cache resolved names across queries.  If caching is
// necessary, it should be done inside the Catalog implementation.
//
// All objects returned from Catalog lookups must stay valid for the lifetime
// of the Catalog.
//
// For more control of lifetime or consistency across lookups, implementors can
// create per-query proxy objects that implement the Catalog interface.
// For example:
//   MyActualCatalog catalog;
//   void HandleQuery(...) {
//     std::unique_ptr<Catalog> catalog_snapshot = catalog.GetSnapshot(...);
//     zetasql::ResolvedQueryStmt query;
//     zetasql::AnalyzeStatement(..., catalog_snapshot.get(), &query);
//     // ... Process query, which now contains pointers into Catalog objects.
//   }
//
// Catalog objects and all objects returned from lookups should be thread-safe.
class Catalog {
 public:
  virtual ~Catalog() {}

  // Get a fully-qualified description of this Catalog.
  // Suitable for log messages, but not necessarily a valid SQL path expression.
  virtual std::string FullName() const = 0;

  // Options for a LookupName call.
  class FindOptions {
   public:
    FindOptions() {}
    explicit FindOptions(CycleDetector* cycle_detector) :
        cycle_detector_(cycle_detector) {}

    void set_cycle_detector(CycleDetector* cycle_detector) {
      cycle_detector_ = cycle_detector;
    }

    CycleDetector* cycle_detector() const {
      return cycle_detector_;
    }

   private:
    // Possibly deadlines, Tasks for cancellation, etc.

    // For internal use only.  Used for detecting cycles during recursive
    // Find*() calls in a ModuleCatalog/LazyResolutionCatalog.  Mutable,
    // since Find*() calls may update the CycleDetector.
    // Not owned.
    CycleDetector* cycle_detector_ = nullptr;
  };

  // The FindX methods look up an object of type X from this Catalog on <path>.
  //
  // If a Catalog implementation supports looking up an object by path, it
  // should implement the FindX method (except that there is no FindCatalog).
  // Alternatively, a Catalog can also contain nested catalogs, and implement
  // GetX method on the inner-most Catalog.
  //
  // The default FindX implementation traverses nested Catalogs until it reaches
  // a Catalog that overrides FindX, or until it gets to the last level of the
  // path and then calls GetX.
  //
  // Pseudo-code of default FindX implementation:
  //
  // if path.size > 1
  //   Catalog* catalog = GetCatalog(path[0])
  //   catalog->FindX(path[1...n-1])
  // else
  //   GetX()
  //
  // NOTE: The FindX methods take precedence over GetX methods and will always
  // be called first. So GetX method does not need to be implemented if FindX
  // method is implemented. If both GetX and FindX are implemented(though not
  // recommended), it is the implementation's responsibility to keep them
  // consistent.
  //
  // Returns zetasql_base::NOT_FOUND if the name wasn't found.
  // Other errors indicate failures in the lookup mechanism and should
  // make the user's request fail.
  // TODO: Pass <path> by value, like for FindConstant(). Same below.
  virtual zetasql_base::Status FindTable(const absl::Span<const std::string>& path,
                                 const Table** table,
                                 const FindOptions& options = FindOptions());

  virtual zetasql_base::Status FindModel(const absl::Span<const std::string>& path,
                                 const Model** model,
                                 const FindOptions& options = FindOptions());

  virtual zetasql_base::Status FindFunction(const absl::Span<const std::string>& path,
                                    const Function** function,
                                    const FindOptions& options = FindOptions());

  virtual zetasql_base::Status FindTableValuedFunction(
      const absl::Span<const std::string>& path,
      const TableValuedFunction** function,
      const FindOptions& options = FindOptions());

  virtual zetasql_base::Status FindProcedure(
      const absl::Span<const std::string>& path, const Procedure** procedure,
      const FindOptions& options = FindOptions());

  // FindType has some additional conventions for protocol buffer type names:
  // We allow protocol buffer type names to be written either as a single
  // identifier, like `package.ProtoName`, or as an unquoted path, like
  // package.ProtoName.
  //
  // When FindType sees a path A.B.C, it should first check if A is a nested
  // Catalog, and if so, recurse into A looking for B.C.  Otherwise, it should
  // look for a type named `A.B.C` in the current Catalog.
  //
  // When a proto package name overlaps with a nested Catalog name, the
  // nested Catalog will take precedence.  The protocol buffer type name is
  // still reachable by writing its qualified name as one quoted identifier.
  //
  // Protocol buffer type names should be written either as single quoted
  // identifiers or as paths with no quoting.  They should not be written
  // mixed.  A.B.`C.D` should find proto `C.D` in Catalog A.B, but should not
  // find proto `B.C.D` in Catalog A.  This is achieved by not doing the
  // lookup with joined paths if any identifier has dots is not a valid
  // proto identifier (including if it contains dots).
  // See ConvertPathToProtoName below.
  virtual zetasql_base::Status FindType(const absl::Span<const std::string>& path,
                                const Type** type,
                                const FindOptions& options = FindOptions());

  // Unlike the other FindX methods, FindConstant is not virtual. Subclasses may
  // override FindConstantWithPathPrefix instead. It resolves a constant even
  // if <path> contains a suffix of field extractions from the constant.
  // FindConstant delegates to FindConstantWithPathPrefix and checks that the
  // path suffix is empty in case of a successful resolution.
  zetasql_base::Status FindConstant(
      const absl::Span<const std::string> path, const Constant** constant,
      const FindOptions& options = FindOptions());

  // Variant of FindConstant() that allows for trailing field references in
  // <path>.
  //
  // Finds the longest prefix of <path> that references a Constant in this
  // Catalog (or a nested Catalog, if <path> has more than one name in it).
  // Returns the result in the output parameters <constant> and
  // <num_names_consumed>:
  // - If a prefix of <path> references a Constant, binds <constant> to that
  //   constant and returns the length of the path prefix in
  //   <num_names_consumed>.
  // - If no such path prefix exists, sets <constant> to null and
  //   <num_names_consumed> to 0, and returns zetasql_base::NOT_FOUND.
  //
  // Called by FindConstant. Subclasses can override this method to change the
  // lookup behavior.
  virtual zetasql_base::Status FindConstantWithPathPrefix(
      const absl::Span<const std::string> path, int* num_names_consumed,
      const Constant** constant, const FindOptions& options = FindOptions());

  // Overloaded helper functions that forward the call to the appropriate
  // Find*() function based on the <object> argument type.
  zetasql_base::Status FindObject(absl::Span<const std::string> path,
                          const Function** object,
                          const FindOptions& options);
  zetasql_base::Status FindObject(absl::Span<const std::string> path,
                          const TableValuedFunction** object,
                          const FindOptions& options);
  zetasql_base::Status FindObject(absl::Span<const std::string> path,
                          const Table** object, const FindOptions& options);
  zetasql_base::Status FindObject(const absl::Span<const std::string> path,
                          const Model** object, const FindOptions& options);
  zetasql_base::Status FindObject(absl::Span<const std::string> path,
                          const Procedure** object, const FindOptions& options);
  zetasql_base::Status FindObject(absl::Span<const std::string> path,
                          const Type** object,
                          const FindOptions& options);
  zetasql_base::Status FindObject(absl::Span<const std::string> path,
                          const Constant** object,
                          const FindOptions& options);

  // Given an identifier path, return the type name that results when combining
  // that path into a single protocol buffer type name, if applicable.
  // Returns empty std::string if <path> cannot form a valid proto-style type name.
  //
  // Examples:
  //   ["A","B","C"] -> "A.B.C"
  //   ["A"] -> "A"
  //   ["A.B"] -> "A.B"
  //   ["A","B.C"] -> ""
  //   ["A","B C"] -> ""
  //   [] -> ""
  static std::string ConvertPathToProtoName(absl::Span<const std::string> path);

  // The SuggestX methods are used to return a suggested alternate name. This is
  // used to give suggestions in error messages when the user-provided name is
  // not found.
  // Return an empty std::string when there is nothing to suggest.
  //
  // As an example implementation, refer to SimpleCatalog::SuggestX(...)
  virtual std::string SuggestTable(const absl::Span<const std::string>& mistyped_path);
  virtual std::string SuggestModel(const absl::Span<const std::string>& mistyped_path);
  virtual std::string SuggestFunction(const absl::Span<const std::string>& mistyped_path);
  virtual std::string SuggestTableValuedFunction(
      const absl::Span<const std::string>& mistyped_path);
  virtual std::string SuggestConstant(const absl::Span<const std::string>& mistyped_path);

  // Returns whether or not this Catalog is a specific catalog interface or
  // implementation.
  template <class CatalogSubclass>
  bool Is() const {
    return dynamic_cast<const CatalogSubclass*>(this) != nullptr;
  }

  // Returns this Catalog as CatalogSubclass*. Must only be used when it is
  // known that the object *is* this subclass, which can be checked using Is()
  // before calling GetAs().
  template <class CatalogSubclass>
  const CatalogSubclass* GetAs() const {
    return static_cast<const CatalogSubclass*>(this);
  }

 protected:
  // The GetX methods get an object of type X from this Catalog, without
  // looking at any nested Catalogs.
  // NOTE: If FindX is implemented, there is no need to implement GetX,
  // as FindX method takes precedence over GetX and is always called first.
  //
  // A NULL pointer should be returned if the object doesn't exist.
  //
  // Errors indicate failures in the lookup mechanism, and should make the
  // user's request fail.
  //
  // These are normally overridden in subclasses.  The default implementations
  // always return not found, for Catalogs with no objects of that type.
  virtual zetasql_base::Status GetTable(
      const std::string& name,
      const Table** table,
      const FindOptions& options = FindOptions());

  virtual zetasql_base::Status GetModel(const std::string& name, const Model** model,
                                const FindOptions& options = FindOptions());

  virtual zetasql_base::Status GetFunction(
      const std::string& name,
      const Function** function,
      const FindOptions& options = FindOptions());

  virtual zetasql_base::Status GetTableValuedFunction(
      const std::string& full_name,
      const TableValuedFunction** function,
      const FindOptions& options = FindOptions());

  virtual zetasql_base::Status GetProcedure(
      const std::string& full_name,
      const Procedure** procedure,
      const FindOptions& options = FindOptions());

  virtual zetasql_base::Status GetType(
      const std::string& name,
      const Type** type,
      const FindOptions& options = FindOptions());

  virtual zetasql_base::Status GetCatalog(
      const std::string& name,
      Catalog** catalog,
      const FindOptions& options = FindOptions());

  virtual zetasql_base::Status GetConstant(
      const std::string& name,
      const Constant** constant,
      const FindOptions& options = FindOptions());

  // Helper functions for getting canonical versions of NOT_FOUND error
  // messages.
  zetasql_base::Status GenericNotFoundError(
      const std::string& object_type, absl::Span<const std::string> path) const;
  // TODO: Remove these object-type specific functions, and have the
  // calling locations invoke the templatized version below instead.
  zetasql_base::Status TableNotFoundError(absl::Span<const std::string> path) const;
  zetasql_base::Status ModelNotFoundError(absl::Span<const std::string> path) const;
  zetasql_base::Status FunctionNotFoundError(
      absl::Span<const std::string> path) const;
  zetasql_base::Status TableValuedFunctionNotFoundError(
      absl::Span<const std::string> path) const;
  zetasql_base::Status ProcedureNotFoundError(
      absl::Span<const std::string> path) const;
  zetasql_base::Status TypeNotFoundError(
      absl::Span<const std::string> path) const;
  zetasql_base::Status ConstantNotFoundError(
      absl::Span<const std::string> path) const;

  // Templatized version of the previous functions.
  template<class ObjectType>
  zetasql_base::Status ObjectNotFoundError(absl::Span<const std::string> path) const {
    static_assert(
        std::is_same<ObjectType, Function>::value ||
            std::is_same<ObjectType, TableValuedFunction>::value ||
            std::is_same<ObjectType, Table>::value ||
            std::is_same<ObjectType, Model>::value ||
            std::is_same<ObjectType, Type>::value ||
            std::is_same<ObjectType, Procedure>::value ||
            std::is_same<ObjectType, Constant>::value,
        "ObjectNotFoundError only supports Function, "
        "TableValuedFunction, Table, Model, Type, Procedure, and Constant");
    if (std::is_same<ObjectType, Function>::value) {
      return FunctionNotFoundError(path);
    } else if (std::is_same<ObjectType, TableValuedFunction>::value) {
      return TableValuedFunctionNotFoundError(path);
    } else if (std::is_same<ObjectType, Table>::value) {
      return TableNotFoundError(path);
    } else if (std::is_same<ObjectType, Model>::value) {
      return ModelNotFoundError(path);
    } else if (std::is_same<ObjectType, Type>::value) {
      return TypeNotFoundError(path);
    } else if (std::is_same<ObjectType, Procedure>::value) {
      return ProcedureNotFoundError(path);
    } else if (std::is_same<ObjectType, Constant>::value) {
      return ConstantNotFoundError(path);
    }
  }

  zetasql_base::Status EmptyNamePathInternalError(const std::string& object_type)
      const;

  // Templatized version of the previous function.  The std::string argument
  // passed to EmptyNamePathInternalError matches those in catalog.cc.
  // TODO: Have catalog.cc call these templated methods and
  // take the EmptyNamePathInternalError(<std::string) version private.
  template <class ObjectType>
  zetasql_base::Status EmptyNamePathInternalError() const {
    static_assert(
        std::is_same<ObjectType, Constant>::value ||
            std::is_same<ObjectType, Function>::value ||
            std::is_same<ObjectType, TableValuedFunction>::value ||
            std::is_same<ObjectType, Table>::value ||
            std::is_same<ObjectType, Model>::value ||
            std::is_same<ObjectType, Type>::value ||
            std::is_same<ObjectType, Procedure>::value,
        "EmptyNamePathInternalError only supports Constant, Function, "
        "TableValuedFunction, Table, Type, and Procedure");
    if (std::is_same<ObjectType, Constant>::value) {
      return EmptyNamePathInternalError("Constant");
    } else if (std::is_same<ObjectType, Function>::value) {
      return EmptyNamePathInternalError("Function");
    } else if (std::is_same<ObjectType, TableValuedFunction>::value) {
      return EmptyNamePathInternalError("TableValuedFunction");
    } else if (std::is_same<ObjectType, Table>::value) {
      return EmptyNamePathInternalError("Table");
    } else if (std::is_same<ObjectType, Model>::value) {
      return EmptyNamePathInternalError("Model");
    } else if (std::is_same<ObjectType, Type>::value) {
      return EmptyNamePathInternalError("Type");
    } else if (std::is_same<ObjectType, Procedure>::value) {
      return EmptyNamePathInternalError("Procedure");
    }
  }

  // Recursive implementation of FindConstantWithPathPrefix().
  //
  // <num_names_consumed> is an input/output parameter that indicates the length
  // of the path prefix processed so far. It must be 0 in the outermost
  // invocation. It will get set to 0 if resolution fails.
  zetasql_base::Status FindConstantWithPathPrefixImpl(
      const absl::Span<const std::string> path, int* num_names_consumed,
      const Constant** constant, const FindOptions& options);
};

// A table or table-like object visible in a ZetaSQL query.
class Table {
 public:
  virtual ~Table() {}

  // Get the table name.
  virtual std::string Name() const = 0;

  // Get a fully-qualified description of this Table.
  // Suitable for log messages, but not necessarily a valid SQL path expression.
  virtual std::string FullName() const = 0;

  virtual int NumColumns() const = 0;
  virtual const Column* GetColumn(int i) const = 0;

  // This function returns nullptr for anonymous or duplicate column names.
  // TODO: The Table interface allows anonymous and duplicate columns,
  //                but the only way to access them is through GetColumn().
  //                Add helper methods as needed, for instance to check if a
  //                name is duplicate, or to fetch all columns associated with
  //                a name, or fetch all anonymous columns, etc.
  virtual const Column* FindColumnByName(const std::string& name) const = 0;

  // If true, this table is a value table, and should act like each row is a
  // single unnamed value with some type rather than acting like each row is a
  // vector of named columns.
  //
  // The table must have at least one column, and the first column (column 0)
  // is treated as the value of the row.  Additional columns may be present
  // but must be pseudo-columns.
  //
  // For more information on value tables, refer to the value tables spec:
  // (broken link)
  virtual bool IsValueTable() const { return false; }

  // Return an ID that can be used to represent this table in a serialized
  // resolved AST. Callers using serialized resolved ASTs should ensure that
  // all tables in their Catalog have unique IDs.
  virtual int64_t GetSerializationId() const { return 0; }

  // Returns an iterator over this table.
  //
  // Not used for zetasql analysis.
  // Used only for evaluating queries on this table with the reference
  // implementation, using the interfaces in evaluator.h.
  virtual zetasql_base::StatusOr<std::unique_ptr<EvaluatorTableIterator>>
  CreateEvaluatorTableIterator(absl::Span<const int> column_idxs) const {
    return zetasql_base::UnimplementedErrorBuilder(ZETASQL_LOC)
           << "Table " << FullName()
           << " does not support the API in evaluator.h";
  }

  // Returns whether or not this Table is a specific table interface or
  // implementation.
  template <class TableSubclass>
  bool Is() const {
    return dynamic_cast<const TableSubclass*>(this) != nullptr;
  }

  // Returns this Table as TableSubclass*. Must only be used when it is known
  // that the object *is* this subclass, which can be checked using Is() before
  // calling GetAs().
  template <class TableSubclass>
  const TableSubclass* GetAs() const {
    return static_cast<const TableSubclass*>(this);
  }
};

// A Model object visible in a ZetaSQL query.
//
// Each model is defined by a set of inputs and outputs. Inputs and outputs
// must all have unique names.
class Model {
 public:
  virtual ~Model() {}

  // Get the model name.
  virtual std::string Name() const = 0;

  // Get a fully-qualified description of this Model.
  // Suitable for log messages, but not necessarily a valid SQL path expression.
  virtual std::string FullName() const = 0;

  virtual uint64_t NumInputs() const = 0;
  virtual const Column* GetInput(int i) const = 0;

  virtual uint64_t NumOutputs() const = 0;
  virtual const Column* GetOutput(int i) const = 0;

  // This function returns nullptr for anonymous columns.
  virtual const Column* FindInputByName(const std::string& name) const = 0;
  virtual const Column* FindOutputByName(const std::string& name) const = 0;

  // Return an ID that can be used to represent this model in a serialized
  // resolved AST. Callers using serialized resolved ASTs should ensure that
  // all models in their Catalog have unique IDs.
  virtual int64_t GetSerializationId() const { return 0; }

  // Returns whether or not this Model is a specific model interface or
  // implementation.
  template <class ModelSubclass>
  bool Is() const {
    return dynamic_cast<const ModelSubclass*>(this) != nullptr;
  }

  // Returns this Model as ModelSubclass*. Must only be used when it is known
  // that the object *is* this subclass, which can be checked using Is() before
  // calling GetAs().
  template <class ModelSubclass>
  const ModelSubclass* GetAs() const {
    return static_cast<const ModelSubclass*>(this);
  }
};

class Column {
 public:
  virtual ~Column() {}

  // The column name.  Empty name means anonymous column.
  virtual std::string Name() const = 0;

  // The fully-qualified name, including the table name.
  virtual std::string FullName() const = 0;

  virtual const Type* GetType() const = 0;

  // Pseudo-columns can be selected explicitly but do not show up in SELECT *.
  // This can be used for any hidden or virtual column or lazily computed value
  // in a table.
  //
  // Pseudo-columns can be used on value tables to provide additional named
  // values outside the content of the row value.
  //
  // Pseudo-columns are normally not writable in INSERTs or UPDATEs, but this
  // is up to the engine and not checked by ZetaSQL.
  //
  // Pseudo-columns are specified in more detail in the value tables spec:
  // (broken link)
  virtual bool IsPseudoColumn() const { return false; }

  // Returns true if the column is writable. Non-writable columns cannot have
  // their value specified in either INSERT or UPDATE dml statements.
  virtual bool IsWritableColumn() const { return true; }

  // Returns whether or not this Column is a specific column interface or
  // implementation.
  template <class ColumnSubclass>
  bool Is() const {
    return dynamic_cast<const ColumnSubclass*>(this) != nullptr;
  }

  // Returns this Column as ColumnSubclass*. Must only be used when it is known
  // that the object *is* this subclass, which can be checked using Is() before
  // calling GetAs().
  template <class ColumnSubclass>
  const ColumnSubclass* GetAs() const {
    return static_cast<const ColumnSubclass*>(this);
  }
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_CATALOG_H_
