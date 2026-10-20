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

#ifndef GOOGLESQL_PUBLIC_TYPES_ROW_TYPE_H_
#define GOOGLESQL_PUBLIC_TYPES_ROW_TYPE_H_

#include <any>
#include <cstdint>
#include <string>
#include <vector>

#include "googlesql/public/options.pb.h"
#include "googlesql/public/type.pb.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/value_equality_check_options.h"
#include "absl/hash/hash.h"
#include "googlesql/base/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace googlesql {

// These have to be forward declared to avoid circular dependencies.
class Column;
class Table;

// Base class for RowType and TableRefType.
// Represents types that are related to table rows.
// See (broken link).
class RowOrTableType : public Type {
 public:
  // The table this ROW or TABLE<ROW> points at.
  // Values of this type are like references to rows of that table.
  const Table* table() const { return table_; }
  const std::string& table_name() const { return table_name_; }

  const RowOrTableType* AsRowOrTable() const override { return this; }

  bool EqualsForSameKind(const Type* that, bool equivalent) const override {
    // A RowOrTableType is only equal to itself; two RowTypes for the same Table
    // are not considered equal or equivalent.
    return this == that;
  }

  bool HasAnyFields() const override;

  bool SupportsEquality() const override { return false; }

  void DebugStringImpl(bool details, TypeOrStringVector* stack,
                       std::string* debug_string) const override;

  absl::StatusOr<std::string> TypeNameWithModifiers(
      const TypeModifiers& type_modifiers, ProductMode mode) const override;

  absl::HashState HashTypeParameter(absl::HashState state) const override;

  bool ValueContentEquals(
      const ValueContent& x, const ValueContent& y,
      const ValueEqualityCheckOptions& options) const override {
    // ROW/TABLE types do not support equality.
    return false;
  }

  bool ValueContentLess(const ValueContent& x, const ValueContent& y,
                        const Type* other_type) const override {
    // ROW/TABLE types do not support ordering.
    return false;
  }

  absl::HashState HashValueContent(const ValueContent& value,
                                   absl::HashState state) const override {
    // ROW/TABLE types do not support hashing.
    return state;
  }

  std::string FormatValueContent(
      const ValueContent& value,
      const FormatValueContentOptions& options) const override;

  absl::Status SerializeValueContent(const ValueContent& value,
                                     ValueProto* value_proto) const override;

  absl::Status DeserializeValueContent(const ValueProto& value_proto,
                                       ValueContent* value) const override;

  // Get Table::LazyColumnsTableScanContext for this type.
  // ROW types hold their own context object.
  // For TABLE types, this fetches the context from the ROW type it points at.
  absl::StatusOr<std::any*> GetTableScanContext() const;

 protected:
  RowOrTableType(const TypeFactoryBase& type_factory, const Table* table,
                 const std::string& table_name);

  int64_t GetEstimatedOwnedMemoryBytesSize() const override {
    return sizeof(*this);
  }

  absl::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      const BuildFileDescriptorMapOptions& options, TypeProto* type_proto,
      FileDescriptorSetMap* file_descriptor_set_map) const override;

  bool SupportsGroupingImpl(const LanguageOptions& language_options,
                            const Type** no_grouping_type) const override {
    if (no_grouping_type != nullptr) {
      *no_grouping_type = this;
    }
    return false;
  }

  bool SupportsPartitioningImpl(
      const LanguageOptions& language_options,
      const Type** no_partitioning_type) const override {
    if (no_partitioning_type != nullptr) {
      *no_partitioning_type = this;
    }
    return false;
  }

  bool SupportsReturningImpl(const LanguageOptions& language_options,
                             const Type** no_returning_type) const override {
    if (no_returning_type != nullptr) {
      *no_returning_type = this;
    }
    return false;
  }

  HasFieldResult HasFieldImpl(absl::string_view name, int* field_id,
                              bool include_pseudo_fields) const override;

  absl::StatusOr<FindFieldResult> FindFieldImpl(
      absl::string_view name, bool include_pseudo_fields) const override;

 private:
  // The table this ROW or TABLE<ROW> points at.
  const Table* table_;

  // The table name from `table_->FullName()`, bound in at construction time.
  const std::string table_name_;

  friend class TypeFactory;
};

// RowType represents a single row of a table, typically from a TableScan.
// This is a ROW<TableName> type.
class RowType : public RowOrTableType {
 public:
  bool IsRow() const override { return true; }

  const RowType* AsRowType() const override { return this; }

  bool IsSupportedType(const LanguageOptions& language_options) const override;

  bool SupportsOrdering(const LanguageOptions& language_options,
                        std::string* type_description) const override;
  std::string TypeName(ProductMode mode) const override;

  // This is a Table::LazyColumnsTableScanContext owned by this RowType.
  std::any* GetTableScanContext() const { return &table_scan_context_; }

 private:
  RowType(const TypeFactoryBase& type_factory, const Table* table,
          const std::string& table_name);
  ~RowType() override;

  // A Table::LazyColumnsTableScanContext owned by this RowType.
  //
  // This is used for Table-defined state shared across all LazyColumn lookups
  // associated with the same ResolvedTableScan. This uses an assumption that
  // the resolver makes a separate RowType for each distinct table scan with
  // `read_as_row_type`.
  //
  // TODO: This object isn't thread-safe and isn't safe to use for
  // types created in the Catalog and shared across queries. It works for
  // RowTypes dynamically created by the analyzer for read_as_row_type scans and
  // used in the scope of a single query. This needs to be improved before the
  // context feature goes beyond experimental.
  mutable std::any table_scan_context_;

  friend class TypeFactory;
};

// TableRefType is a `TABLE [UNIQUE]<ROW<TableName>>` type, describing
// a virtual table (containing rows from `TableName`) that could be returned
// from a join column.
// The UNIQUE property indicates the virtual table has at most one row.
//
// Note: This is called TableRefType because googlesql::TableType was taken.
class TableRefType : public RowOrTableType {
 public:
  bool IsTable() const override { return true; }
  const TableRefType* AsTableRefType() const override { return this; }

  // True for TABLE types with UNIQUE (single-row tables).
  bool IsSingleRowTable() const override { return !multi_row_; }
  // True for TABLE types without UNIQUE (multi-row tables).
  bool IsMultiRowTable() const override { return multi_row_; }

  bool IsSupportedType(const LanguageOptions& language_options) const override;

  bool SupportsOrdering(const LanguageOptions& language_options,
                        std::string* type_description) const override;
  std::string TypeName(ProductMode mode) const override;

  // The bound columns describe how to scan this virtual table, producing
  // rows by doing a join.
  // The `bound_source_columns` on `bound_source_table` join to
  // `bound_columns` on `table`.
  // TABLE values hold values for the `bound_source_columns`.
  // `bound_columns` and `bound_source_columns` may be empty if the engine
  // handles expanding the TABLE type without using GoogleSQL's RowTypeRewriter.
  const std::vector<const Column*>& bound_columns() const {
    return bound_columns_;
  }
  const Table* bound_source_table() const { return bound_source_table_; }
  const std::vector<const Column*>& bound_source_columns() const {
    return bound_source_columns_;
  }

  // The element type when scanning the TableRefType like an array.
  // This is currently always a RowType.
  const RowType* element_type() const { return element_type_->AsRowType(); }

  TypeListView ComponentTypes() const override {
    return absl::MakeConstSpan(&element_type_, /*size=*/1);
  }

 private:
  TableRefType(const TypeFactoryBase& type_factory, const Table* table,
               const std::string& table_name, bool multi_row,
               std::vector<const Column*> bound_columns,
               const Table* bound_source_table,
               std::vector<const Column*> bound_source_columns,
               const RowType* element_type);
  ~TableRefType() override;

  const std::vector<const Column*> bound_columns_;
  const Table* bound_source_table_ = nullptr;
  const std::vector<const Column*> bound_source_columns_;

  // True if this TABLE can produce multiple rows.
  // False if this TABLE has UNIQUE. (It's an N:1 join.)
  bool multi_row_;

  const Type* element_type_;

  friend class TypeFactory;
};

}  // namespace googlesql

#endif  // GOOGLESQL_PUBLIC_TYPES_ROW_TYPE_H_
