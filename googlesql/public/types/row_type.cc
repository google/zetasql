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

#include "googlesql/public/types/row_type.h"

#include <any>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/language_options.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_modifiers.h"
#include "absl/hash/hash.h"
#include "googlesql/base/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "googlesql/base/status_macros.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "googlesql/base/ret_check.h"

namespace googlesql {

RowOrTableType::RowOrTableType(const TypeFactoryBase& type_factory,
                               const Table* table,
                               const std::string& table_name)
    : Type(type_factory, TYPE_ROW), table_(table), table_name_(table_name) {}

void RowOrTableType::DebugStringImpl(bool details, TypeOrStringVector* stack,
                                     std::string* debug_string) const {
  absl::StrAppend(debug_string, TypeName(PRODUCT_INTERNAL));
}

absl::HashState RowOrTableType::HashTypeParameter(absl::HashState state) const {
  // Hash based on the table name
  return absl::HashState::combine(std::move(state), table_name_);
}

absl::StatusOr<std::string> RowOrTableType::TypeNameWithModifiers(
    const TypeModifiers& type_modifiers, ProductMode mode) const {
  GOOGLESQL_RET_CHECK(type_modifiers.IsEmpty());
  return TypeName(mode);
}

std::string RowOrTableType::FormatValueContent(
    const ValueContent& value, const FormatValueContentOptions& options) const {
  return absl::StrCat("UNIMPLEMENTED ROW/TABLE VALUE: ", DebugString());
}

absl::Status RowOrTableType::SerializeValueContent(
    const ValueContent& value, ValueProto* value_proto) const {
  return absl::UnimplementedError(
      "SerializeValueContent not implemented for RowOrTableType");
}

absl::Status RowOrTableType::DeserializeValueContent(
    const ValueProto& value_proto, ValueContent* value) const {
  return absl::UnimplementedError(
      "DeserializeValueContent not implemented for RowOrTableType");
}

absl::Status RowOrTableType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    const BuildFileDescriptorMapOptions& options, TypeProto* type_proto,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(TYPE_ROW);
  // TODO: Add RowTypeProto and serialize table reference.
  return absl::UnimplementedError(
      "Serialize not implemented for RowOrTableType");
}

bool RowOrTableType::HasAnyFields() const {
  // Assume we always have fields under a ROW.
  // This would be false if we have ROW for a value table with scalar type,
  // but we don't allow value tables in ROW yet.
  return true;
}

absl::StatusOr<std::any*> RowOrTableType::GetTableScanContext() const {
  if (IsRow()) {
    return AsRowType()->GetTableScanContext();
  } else {
    GOOGLESQL_RET_CHECK(IsTable());
    const Type* element_type = AsTableRefType()->element_type();
    GOOGLESQL_RET_CHECK(element_type->IsRow()) << element_type->DebugString();
    return element_type->AsRowType()->GetTableScanContext();
  }
}

// This stores a callback that is used to implement lookups for FindFieldImpl
// using Table::FindColumn methods.
using HasColumnCallbackType =
    std::function<absl::StatusOr<Type::HasFieldResult>(
        const Table&, absl::string_view, std::any*)>;
static HasColumnCallbackType* row_type_has_column_callback = nullptr;
// This setter is called from `:type_with_catalog_impl` (if it's linked in)
// to install the callback.
void SetRowTypeHasColumnColumnCallback(HasColumnCallbackType callback) {
  ABSL_DCHECK(row_type_has_column_callback == nullptr);
  row_type_has_column_callback = new HasColumnCallbackType(callback);
}

// Type::HasField was replaced with Type::FindField in order to return a Status,
// but there are many callers outside of the GoogleSQL codebase that call
// HasField for other types. An unconditional ABSL_LOG(ERROR) helps catch callers
// that need to switch to FindField, without crashing if the function is ever
// reached in a non-debug build.
Type::HasFieldResult RowOrTableType::HasFieldImpl(
    absl::string_view name, int* field_id, bool include_pseudo_fields) const {
  ABSL_LOG(ERROR) << "ROW type field lookup may produce an error, callers must "
                 "call FindField() instead of HasField()";
  absl::StatusOr<Type::FindFieldResult> find_result =
      FindFieldImpl(name, include_pseudo_fields);
  if (!find_result.ok()) {
    // Error status, but we can't propagate it, so just return a placeholder of
    // HAS_NO_FIELD. Callers must migrate to call FindField() instead.
    if (field_id != nullptr) {
      *field_id = -1;
    }
    return Type::HAS_NO_FIELD;
  }
  if (field_id != nullptr) {
    *field_id = (*find_result).field_id;
  }
  return (*find_result).has_field;
}

absl::StatusOr<Type::FindFieldResult> RowOrTableType::FindFieldImpl(
    absl::string_view name, bool include_pseudo_fields) const {
  // TODO: b/452955184 - Current API design doesn't allow HasField and FindField
  // to handle pseudo-columns and ambiguous names. `include_pseudo_fields` is
  // ignored, and ambiguous columns will result in a `HAS_NO_FIELD` result. Fix
  // to handle these cases, or document behavior in the API.
  GOOGLESQL_RET_CHECK(row_type_has_column_callback != nullptr)
      << "Called FindFieldImpl without linking in :type_with_catalog_impl";
  GOOGLESQL_RET_CHECK(table_ != nullptr);
  GOOGLESQL_ASSIGN_OR_RETURN(std::any * table_scan_context, GetTableScanContext());
  GOOGLESQL_ASSIGN_OR_RETURN(
      Type::HasFieldResult lookup_result,
      (*row_type_has_column_callback)(*table_, name, table_scan_context));
  return Type::FindFieldResult{.has_field = lookup_result, .field_id = -1};
}

RowType::RowType(const TypeFactoryBase& type_factory, const Table* table,
                 const std::string& table_name)
    : RowOrTableType(type_factory, table, table_name) {}

RowType::~RowType() = default;

bool RowType::IsSupportedType(const LanguageOptions& language_options) const {
  return language_options.LanguageFeatureEnabled(FEATURE_ROW_TYPE);
}

bool RowType::SupportsOrdering(const LanguageOptions& language_options,
                               std::string* type_description) const {
  if (type_description) {
    *type_description = "ROW";
  }
  return false;
}

std::string RowType::TypeName(ProductMode mode) const {
  return absl::StrCat("ROW<", table_name(), ">");
}

TableRefType::TableRefType(const TypeFactoryBase& type_factory,
                           const Table* table, const std::string& table_name,
                           bool multi_row,
                           std::vector<const Column*> bound_columns,
                           const Table* bound_source_table,
                           std::vector<const Column*> bound_source_columns,
                           const RowType* element_type)
    : RowOrTableType(type_factory, table, table_name),
      bound_columns_(std::move(bound_columns)),
      bound_source_table_(bound_source_table),
      bound_source_columns_(std::move(bound_source_columns)),
      multi_row_(multi_row),
      element_type_(element_type) {
  ABSL_DCHECK(bound_source_table_ != nullptr);
  ABSL_DCHECK_EQ(bound_columns_.size(), bound_source_columns_.size());
  // The element type is the corresponding ROW<T> type.
  // It's created in TypeFactory::MakeRowType rather than here because the
  // :types library doesn't depend on :type_factory.
  ABSL_DCHECK(element_type_ != nullptr);
  ABSL_DCHECK(!element_type_->IsTable());
}

TableRefType::~TableRefType() = default;

bool TableRefType::IsSupportedType(
    const LanguageOptions& language_options) const {
  return language_options.LanguageFeatureEnabled(FEATURE_TABLE_TYPE);
}

bool TableRefType::SupportsOrdering(const LanguageOptions& language_options,
                                    std::string* type_description) const {
  if (type_description) {
    *type_description = "TABLE";
  }
  return false;
}

std::string TableRefType::TypeName(ProductMode mode) const {
  return absl::StrCat("TABLE", IsSingleRowTable() ? " UNIQUE" : "", "<ROW<",
                      table_name(), ">>");
}

}  // namespace googlesql
