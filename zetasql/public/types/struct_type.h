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

#ifndef ZETASQL_PUBLIC_TYPES_STRUCT_TYPE_H_
#define ZETASQL_PUBLIC_TYPES_STRUCT_TYPE_H_

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/base/case.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"

namespace zetasql {

// Field contained in a struct, representing a name and type.
// The SWIG compiler does not understand nested classes, so this cannot be
// defined inside the scope of StructType.
class LanguageOptions;
class TypeFactory;
class TypeParameterValue;
class TypeParameters;
class ValueContent;
class ValueProto;

struct StructField {
  StructField(std::string name_in, const Type* type_in)
      : name(std::move(name_in)), type(type_in) {}

  std::string name;  // Empty string means this is an unnamed field.
  const Type* type;
};

// A struct type.
// Structs are allowed to have zero fields, but this is not normal usage.
// Field names do not have to be unique.
// Empty field names are used to indicate anonymous fields - such fields are
// unnamed and cannot be looked up by name.
class StructType : public Type {
 public:
#ifndef SWIG
  StructType(const StructType&) = delete;
  StructType& operator=(const StructType&) = delete;

  // StructField is declared here for compatibility with existing code.
  using StructField = ::zetasql::StructField;
#endif  // SWIG

  int num_fields() const { return fields_.size(); }
  const StructField& field(int i) const { return fields_[i]; }
  const std::vector<StructField>& fields() const { return fields_; }

  const StructType* AsStruct() const override { return this; }

  // Look up a field by name.
  // Returns NULL if <name> is not found (uniquely).
  // Returns in <*is_ambiguous> whether this lookup was ambiguous.
  // If found_idx is non-NULL, returns position of found field in <*found_idx>.
  const StructField* FindField(absl::string_view name, bool* is_ambiguous,
                               int* found_idx = nullptr) const;

  // Check if structure has some fields.
  bool HasAnyFields() const override;

  // Helper functions for determining Equals() or Equivalent() for struct
  // types. For structs, Equals() means that the fields have the same name
  // and Equals() types.  Struct Equivalent() means that the fields have
  // Equivalent() types (but not necessarily the same names).
  static bool EqualsImpl(const StructType* type1, const StructType* type2,
                         bool equivalent);
  static bool FieldEqualsImpl(const StructField& field1,
                              const StructField& field2, bool equivalent);

  bool SupportsOrdering(const LanguageOptions& language_options,
                        std::string* type_description) const override;

  // Struct types support equality iff all of the field types support equality.
  bool SupportsEquality() const override;

  bool UsingFeatureV12CivilTimeType() const override;

  std::string ShortTypeName(ProductMode mode) const override;
  std::string TypeName(ProductMode mode) const override;

  // Same as above, but if <type_params> is not empty, any nested SimpleTypes
  // include their type parameters within parenthesis appended to their SQL
  // name.
  absl::StatusOr<std::string> TypeNameWithParameters(
      const TypeParameters& type_params, ProductMode mode) const override;

  // Check if the names in <fields> are valid.
  static absl::Status FieldNamesAreValid(
      const absl::Span<const StructField>& fields);

  int nesting_depth() const override { return nesting_depth_; }

  bool IsSupportedType(const LanguageOptions& language_options) const override;

  // Validate and resolve type parameters for struct type, currently always
  // return error since struct type itself doesn't support type parameters.
  absl::StatusOr<TypeParameters> ValidateAndResolveTypeParameters(
      const std::vector<TypeParameterValue>& type_parameter_values,
      ProductMode mode) const override;

  // Validates resolved type parameters for struct subfields recursively.
  absl::Status ValidateResolvedTypeParameters(
      const TypeParameters& type_parameters, ProductMode mode) const override;

 protected:
  // Return estimated size of memory owned by this type. Owned memory includes
  // field names, but not the memory associated with field types (which are
  // owned by some TypeFactory).
  int64_t GetEstimatedOwnedMemoryBytesSize() const override
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  // Caller must enforce that <nesting_depth> is accurate. No verification is
  // done.
  StructType(const TypeFactory* factory, std::vector<StructField> fields,
             int nesting_depth);
  ~StructType() override;

  bool SupportsGroupingImpl(const LanguageOptions& language_options,
                            const Type** no_grouping_type) const override;

  bool SupportsPartitioningImpl(
      const LanguageOptions& language_options,
      const Type** no_partitioning_type) const override;

  absl::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      const BuildFileDescriptorMapOptions& options, TypeProto* type_proto,
      FileDescriptorSetMap* file_descriptor_set_map) const override;

  absl::StatusOr<std::string> TypeNameImpl(
      int field_limit,
      const std::function<absl::StatusOr<std::string>(
          const zetasql::Type*, int field_index)>& field_debug_fn) const;

  bool EqualsForSameKind(const Type* that, bool equivalent) const override;

  void DebugStringImpl(bool details, TypeOrStringVector* stack,
                       std::string* debug_string) const override;

  HasFieldResult HasFieldImpl(const std::string& name, int* field_id,
                              bool include_pseudo_fields) const override;

  void CopyValueContent(const ValueContent& from,
                        ValueContent* to) const override;
  void ClearValueContent(const ValueContent& value) const override;
  absl::HashState HashTypeParameter(absl::HashState state) const override;
  absl::HashState HashValueContent(const ValueContent& value,
                                   absl::HashState state) const override;
  bool ValueContentEquals(
      const ValueContent& x, const ValueContent& y,
      const ValueEqualityCheckOptions& options) const override;
  bool ValueContentLess(const ValueContent& x, const ValueContent& y,
                        const Type* other_type) const override;
  std::string FormatValueContent(
      const ValueContent& value,
      const FormatValueContentOptions& options) const override;
  absl::Status SerializeValueContent(const ValueContent& value,
                                     ValueProto* value_proto) const override;
  absl::Status DeserializeValueContent(const ValueProto& value_proto,
                                       ValueContent* value) const override;

  const std::vector<StructField> fields_;

  // The deepest nesting depth in the type tree rooted at this StructType, i.e.,
  // the maximum nesting_depth of the field types, plus 1 for the StructType
  // itself. If all fields are simple types, then this is 1.
  // This field is not serialized. It is recalculated during deserialization.
  const int nesting_depth_;

  // Lazily built map from name to struct field index. Ambiguous lookups are
  // designated with an index of -1. This is only built if FindField is called.
  mutable absl::Mutex mutex_;
  mutable absl::flat_hash_map<absl::string_view, int, zetasql_base::StringViewCaseHash,
                              zetasql_base::StringViewCaseEqual>
      field_name_to_index_map_ ABSL_GUARDED_BY(mutex_);

  friend class TypeFactory;
};

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_STRUCT_TYPE_H_
