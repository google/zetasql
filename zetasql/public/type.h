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

#ifndef ZETASQL_PUBLIC_TYPE_H_
#define ZETASQL_PUBLIC_TYPE_H_

// TODO Maybe store and re-use the same type object for all identical
//   struct/array/proto/enum types?

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/errors.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/proto/type_annotation.pb.h"
#include "zetasql/public/type.pb.h"
#include "absl/base/attributes.h"
#include <cstdint>
#include "absl/base/macros.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/declare.h"
#include "zetasql/base/case.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

ABSL_DECLARE_FLAG(int32_t, zetasql_type_factory_nesting_depth_limit);

namespace zetasql {

class ArrayType;
class EnumType;
class LanguageOptions;
class ProtoType;
class StructType;
class Type;
class TypeFactory;

typedef std::vector<Type*> TypeList;

// In-memory representation of a ZetaSQL type.
// See (broken link) for more information on the type system.
//
// Types are always const pointers, and always owned by a TypeFactory.
// Types can only be constructed by a TypeFactory, and their lifetime extends
// until that TypeFactory is destroyed.
class Type {
 public:
#ifndef SWIG
  Type(const Type&) = delete;
  Type& operator=(const Type&) = delete;
#endif  // SWIG

  TypeKind kind() const { return kind_; }

  bool IsInt32() const { return kind_ == TYPE_INT32; }
  bool IsInt64() const { return kind_ == TYPE_INT64; }
  bool IsUint32() const { return kind_ == TYPE_UINT32; }
  bool IsUint64() const { return kind_ == TYPE_UINT64; }
  bool IsBool() const { return kind_ == TYPE_BOOL; }
  bool IsFloat() const { return kind_ == TYPE_FLOAT; }
  bool IsDouble() const { return kind_ == TYPE_DOUBLE; }
  bool IsString() const { return kind_ == TYPE_STRING; }
  bool IsBytes() const { return kind_ == TYPE_BYTES; }
  bool IsDate() const { return kind_ == TYPE_DATE; }
  bool IsTimestamp() const { return kind_ == TYPE_TIMESTAMP; }

  bool IsTime() const { return kind_ == TYPE_TIME; }
  bool IsDatetime() const { return kind_ == TYPE_DATETIME; }
  bool IsNumericType() const { return kind_ == TYPE_NUMERIC; }

  // DEPRECATED, use UsingFeatureV12CivilTimeType() instead.
  //
  // Civil time types are TIME and DATETIME, which are controlled by the
  // language option FEATURE_V_1_2_CIVIL_TIME.
  // Technically, DATE is also a "civil time" type, but it's always available
  // and not controlled by FEATURE_V_1_2_CIVIL_TIME.
  bool IsFeatureV12CivilTimeType() const {
    return kind_ == TYPE_TIME || kind_ == TYPE_DATETIME;
  }

  // ArrayType and StructType will override this function to reflect that
  // TIME or DATETIME appears in the array or the struct.
  //
  // EnumType will return false directly.
  //
  // ProtoType will always return false. The proto itself is always a valid
  // type to pass around as a value, even if it contains civil time fields.
  // Extracting those fields may give an error if FEATURE_V_1_2_CIVIL_TIME is
  // not enabled.
  virtual bool UsingFeatureV12CivilTimeType() const {
    return kind_ == TYPE_TIME || kind_ == TYPE_DATETIME;
  }

  // Return true if the type is DATE, TIME or DATETIME.
  bool IsCivilDateOrTimeType() const {
    return kind_ == TYPE_DATE || kind_ == TYPE_TIME || kind_ == TYPE_DATETIME;
  }

  bool IsGeography() const { return kind_ == TYPE_GEOGRAPHY; }
  bool IsEnum() const { return kind_ == TYPE_ENUM; }
  bool IsArray() const { return kind_ == TYPE_ARRAY; }
  bool IsStruct() const { return kind_ == TYPE_STRUCT; }
  bool IsProto() const { return kind_ == TYPE_PROTO; }
  bool IsStructOrProto() const { return IsStruct() || IsProto(); }

  bool IsFloatingPoint() const { return IsFloat() || IsDouble(); }
  bool IsNumerical() const { return IsInt32() || IsInt64() || IsUint32() ||
                                    IsUint64() || IsFloat() || IsDouble() ||
                                    IsNumericType(); }
  bool IsInteger() const { return IsInt32() || IsInt64() || IsUint32() ||
                                   IsUint64(); }
  bool IsSignedInteger() const { return IsInt32() || IsInt64(); }
  bool IsUnsignedInteger() const { return IsUint32() || IsUint64(); }

  // Simple types are those that can be represented with just a TypeKind,
  // with no parameters.
  // This exists instead of IsScalarType because enums act more like scalars,
  // but require parameters.
  bool IsSimpleType() const { return IsSimpleType(kind_); }
  // Return this Type cast to the given subclass, or nullptr if this type
  // is not of the requested type.
  virtual const ArrayType* AsArray() const { return nullptr; }
  virtual const StructType* AsStruct() const { return nullptr; }
  virtual const ProtoType* AsProto() const { return nullptr; }
  virtual const EnumType* AsEnum() const { return nullptr; }

  // Returns true if the type supports grouping with respect to the
  // 'language_options'. E.g. struct type supports grouping if the
  // FEATURE_V_1_2_GROUP_BY_STRUCT option is enabled.
  // When this returns false and 'type_description' is not null, also returns in
  // 'type_description' a description of the type that does not support
  // grouping. e.g. "DOUBLE", "STRUCT containing DOUBLE", etc.
  // TODO: Make <type_description> required, and require that it
  // is not nullptr (update the contract so that it crashes if it is
  // nullptr).  Also do this for SupportsPartitioning and SupportsOrdering().
  bool SupportsGrouping(const LanguageOptions& language_options,
                        std::string* type_description = nullptr) const;

  // Returns true of type supports partitioning with respect to the
  // 'language_options'. E.g. struct type supports partitioning if the
  // FEATURE_V_1_2_GROUP_BY_STRUCT option is enabled.
  // When this returns false and 'type_description' is not null, also returns in
  // 'type_description' a description of the type that does not support
  // partitioning. e.g. "DOUBLE", "STRUCT containing DOUBLE", etc.
  // TODO: Make <type_description> required, and require that it
  // is not nullptr (update the contract so that it crashes if it is
  // nullptr).  Also do this for SupportsGrouping and SupportsOrdering().
  bool SupportsPartitioning(const LanguageOptions& language_options,
                            std::string* type_description = nullptr) const;

  // Returns true if the type supports ordering with respect to the
  // 'language_options'.  Determines whether the type is supported for
  // an expression in the ORDER BY clause (of the query, in aggregate function
  // arguments, and in analytic function arguments).  Also determines whether
  // the type supports comparison for non-equality operators like '<'.
  // Is also a prerequisite for supporting the type as an argument for functions
  // MIN/MAX and GREATEST/LEAST.
  //
  // When this returns false and 'type_description' is not null, also returns
  // in 'type_description' a description of the type that does not support
  // ordering. e.g. "ARRAY containing STRUCT", etc.
  //
  // Note - the 'language_options' are currently unused.
  //
  // TODO: Require that <type_description> is not nullptr (update the
  // contract so that it crashes if it is nullptr).  Also do this for
  // SupportsGrouping and SupportsPartitioning().
  virtual bool SupportsOrdering(const LanguageOptions& language_options,
                                std::string* type_description) const;
  // Deprecated signature - use SupportsOrdering defined above that takes
  // LanguageOptions.  This version hardcodes LanguageOptions to be NO options,
  // which is almost certainly not what you want.
  ABSL_DEPRECATED(
      "use SupportsOrdering(language_options, type_description) instead")
  bool SupportsOrdering() const;

  // Whether the type is supported in equality operators, i.e. '=', '!=', IN
  // NOT IN, USING and CASE.
  // Note that this means the ZetaSQL type supports equality, but there are
  // LanguageOptions that may restrict this for particular engines.
  virtual bool SupportsEquality() const { return !IsGeography(); }

  // Returns true if type supports equality with respect to the
  // 'language_options'. E.g. array type supports equality if the
  // FEATURE_V_1_1_ARRAY_EQUALITY option is enabled.
  virtual bool SupportsEquality(const LanguageOptions& language_options) const;

  // Compare types for equality.  Equal types can be used interchangeably
  // without any casting.
  //
  // This compares structurally inside structs and arrays.
  // For protos and enums, this does proto descriptor pointer comparison only.
  // Two versions of identical descriptors (from different DescriptorPools)
  // will not be considered equal.
  bool Equals(const Type* other_type) const {
    return EqualsImpl(other_type, false /* equivalent */);
  }

  // Compare types for equivalence.  Equivalent types can be used
  // interchangeably in a query, but casts will always be added to convert
  // from one to the other.
  //
  // This differs from Equals in that it treats Enums and Protos as equivalent
  // if their full_name() is equal.  Different versions of the same proto
  // or enum are equivalent in a query, but CASTs will be added to do the
  // conversion.
  //
  // Structs with different field names are considered Equivalent if they
  // have the same number of fields and the corresponding fields have
  // Equivalent types.
  bool Equivalent(const Type* other_type) const {
    return EqualsImpl(other_type, true /* equivalent */);
  }

  // Serialize the Type to a fully self-contained protocol buffer into
  // <type_proto>.  Note that the related FileDescriptorSet is serialized into
  // <type_proto>.  Supports Types depending on descriptors from different
  // DescriptorPools by serializing into multiple FileDescriptorSets within
  // <type_proto>.
  zetasql_base::Status SerializeToSelfContainedProto(
      TypeProto* type_proto) const;

  // Serialize the Type to protocol buffer form into <type_proto>.
  // The <type_proto> will *not* contain the related FileDescriptorSet.
  // Either <file_descriptors> or <file_descriptor_set> can be NULL.
  // For each FileDescriptor referenced by the Type, the following will
  // occur:
  //
  // 1) If both <file_descriptors> and <file_descriptor_set> are NULL,
  //    then the FileDescriptor is ignored.
  // 2) If only <file_descriptors> is NULL, then the FileDescriptor
  //    is serialized and stored into <file_descriptor_set>
  // 3) If only <file_descriptor_set> is NULL, then the FileDescriptor*
  //    is added to <file_descriptors> if it is not already there.
  // 4) If neither are NULL, then if the FileDescriptor* is not already in
  //    <file_descriptors> then it is added to <file_descriptors> and it
  //    is serialized and stored into <file_descriptor_set> as well.
  //
  // Returns an error if this Type contains proto or enum types that originate
  // from different DescriptorPools. If contained types may span multiple
  // DescriptorPools, then SerializeToProtoAndDistinctFileDescriptors must be
  // used instead.
  // TODO: Migrate callers to use
  // SerializeToProtoAndDistinctFileDescriptors instead, then delete this
  // method.
  zetasql_base::Status SerializeToProtoAndFileDescriptors(
      TypeProto* type_proto,
      google::protobuf::FileDescriptorSet* file_descriptor_set = nullptr,
      std::set<const google::protobuf::FileDescriptor*>* file_descriptors =
          nullptr) const;

  // Stores state associated with type serialization.
  struct FileDescriptorEntry {
    // The unique 0-based index of a particular DescriptorPool within the
    // descriptor map. Indices will be in the range [0, map_size - 1].
    int descriptor_set_index = 0;
    // The file descriptors associated with types from a particular
    // DescriptorPool.
    google::protobuf::FileDescriptorSet file_descriptor_set;
    std::set<const google::protobuf::FileDescriptor*> file_descriptors;
  };

  typedef std::map<const google::protobuf::DescriptorPool*,
          std::unique_ptr<FileDescriptorEntry> > FileDescriptorSetMap;

  // Similar to SerializeToProtoAndFileDescriptors, but supports types from
  // distinct DescriptorPools. The provided map is used to store serialized
  // FileDescriptorSets, which can later be deserialized into separate
  // DescriptorPools in order to reconstruct the Type using
  // DeserializeFromProtoUsingExistingPools. The map may be non-empty and may
  // be used across calls to this method in order to serialize multiple types.
  // The map may not be null.
  //
  // Usage:
  // const Type* type1 = ...;
  // const Type* type2 = ...;
  // TypeProto type_proto1;
  // TypeProto type_proto2;
  // // After serialization, for each proto or enum type that was serialized,
  // // all of its associated FileDescriptors will be stored in the map under
  // // an entry for its originating DescriptorPool.
  // FileDescriptorSetMap file_descriptor_set_map;
  // ZETASQL_RETURN_IF_ERROR(
  //     type1->SerializeToProtoAndDistinctFileDescriptors(
  //         &type_proto1, &file_descriptor_set_map));
  // ZETASQL_RETURN_IF_ERROR(
  //     type2->SerializeToProtoAndDistinctFileDescriptors(
  //         &type_proto2, &file_descriptor_set_map));
  // ...
  // vector<const proto::DescriptorPool*> pools;
  // // Deserialize the FileDescriptorSets within the file_descriptor_set_map
  // // into separate pools, ordered according to descriptor_set_index.
  // ...
  // const Type* deserialized_type1 = nullptr;
  // const Type* deserialized_type2 = nullptr;
  // ZETASQL_RETURN_IF_ERROR(
  //     factory.DeserializeFromProtoUsingExistingPools(
  //         type_proto1, pools, &deserialized_type1));
  // ZETASQL_RETURN_IF_ERROR(
  //     factory.DeserializeFromProtoUsingExistingPools(
  //         type_proto2, pools, &deserialized_type2));
  zetasql_base::Status SerializeToProtoAndDistinctFileDescriptors(
      TypeProto* type_proto,
      FileDescriptorSetMap* file_descriptor_set_map) const;

  // Same as the previous, but also optionally takes a limit on total
  // FileDescriptorSet size in <file_descriptor_sets_max_size_bytes>.
  // Returns an error and aborts if this size limit is exceeded by
  // the FileDescriptorSets in the <file_descriptor_set_map>.
  zetasql_base::Status SerializeToProtoAndDistinctFileDescriptors(
      TypeProto* type_proto,
      absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
      FileDescriptorSetMap* file_descriptor_set_map) const;

  // Returns the SQL name for this type, which in general is not reparseable as
  // part of a query. This is intended for user-facing informational or error
  // messages; for a parseable type name, use TypeName, and for logging, use
  // DebugString. For proto-based types, this just returns the type name, which
  // does not easily distinguish PROTOs from ENUMs.
  virtual std::string ShortTypeName(ProductMode mode) const;

  // Same as above, but returns a SQL name that is reparseable as part of a
  // query. This is not intended for user-facing informational or error
  // messages.
  // TODO: Rename this to FullTypeName() or SQLTypeName() instead.
  virtual std::string TypeName(ProductMode mode) const = 0;

  // Returns the full description of the type without truncation. This should
  // only be used for logging or tests and not for any user-facing messages. For
  // proto-based types, this will return PROTO<name> or ENUM<name>, which are
  // not valid to parse as SQL.
  // If <details> is true, then the description includes full proto descriptors.
  virtual std::string DebugString(bool details = false) const = 0;

  // Check if this type contains a field with the given name.
  enum HasFieldResult {
    HAS_NO_FIELD,        // No field with that name.
    HAS_FIELD,           // Exactly one field with that name.
    HAS_PSEUDO_FIELD,    // Exactly one virtual field, including
                         // * has_X for field X
    HAS_AMBIGUOUS_FIELD  // Multiple fields with that name.
  };
  // If this method returns HAS_FIELD or HAS_PSEUDO_FIELD and <field_id> is
  // non-NULL, then <field_id> is set to the field index for STRUCTs and the
  // field tag number for PROTOs.
  // <include_pseudo_fields> specifies whether virtual fields should be
  // returned or used for ambiguity check.
  HasFieldResult HasField(const std::string& name, int* field_id = nullptr,
                          bool include_pseudo_fields = true) const;

  // Return true if this type has any fields.
  // Will return false for structs or protos with zero fields.
  // Any pseudo-fields are not counted. Always returns false for arrays.
  bool HasAnyFields() const;

  // Returns true if this type is enabled given 'language_options'.
  // Checks for ProductMode, TimestampMode, and supported LanguageFeatures.
  bool IsSupportedType(const LanguageOptions& language_options) const;

  // Returns true if this type is enabled given 'language_options'.
  // Checks for ProductMode, TimestampMode, and supported LanguageFeatures.
  // Only works with simple types, because complex types need full Type object,
  // just TypeKind is usually not enough.
  static bool IsSupportedSimpleTypeKind(
      TypeKind kind, const LanguageOptions& language_options);

  static bool IsSimpleType(TypeKind kind);

  static std::string TypeKindToString(TypeKind kind, ProductMode mode);
  static std::string TypeKindListToString(const std::vector<TypeKind>& kinds,
                                     ProductMode mode);

  // Returns whether <type_name> identifies a simple Type.
  static bool IsSimpleTypeName(const std::string& type_name, ProductMode mode);

  // Returns the matching TypeKind associated with <type_name>, or crashes
  // if the name does not identify a simple Type.
  static TypeKind SimpleTypeNameToTypeKindOrDie(const std::string& type_name,
                                                ProductMode mode);

  // Functions below this line are for internal use only.

  // Returns an integer identifying relative specificity, where lower values
  // mean more specific.  Specificity details are defined in:
  //   (broken link)
  // TODO: Update document location to reflect the final
  // cast/coercion/supertype document when it is available.
  static int KindSpecificity(TypeKind kind);

  // Compares whether one TypeKind specificity is less than another.
  static bool KindSpecificityLess(TypeKind kind1, TypeKind kind2);

  // Returns an integer identifying the relative cost of coercing from one type
  // to another.  Always returns a non-negative result.  When considering
  // coercion from type1 to either type2 or type3, we prefer coercing to the
  // type with the lower cost.
  static int GetTypeCoercionCost(TypeKind kind1, TypeKind kind2);

  // The nesting depth of the tree of types (via StructType and ArrayType) below
  // this type. For simple types this is 0.
  virtual int nesting_depth() const { return 0; }

 protected:
  // Types can only be created and destroyed by TypeFactory.
  Type(const TypeFactory* factory, TypeKind kind);
  virtual ~Type();

  bool EqualsImpl(const Type* other_type, bool equivalent) const {
    if (this == other_type) {
      return true;
    }
    if (kind() != other_type->kind()) {
      return false;
    }
    if (IsSimpleType()) {
      return true;
    }
    return EqualsNonSimpleTypes(other_type, equivalent);
  }
  bool EqualsNonSimpleTypes(const Type* that, bool equivalent) const;

  // Internal implementation for Serialize methods.  This will append
  // Type information to <type_proto>, so the caller should make sure
  // that <type_proto> has been initialized properly before invoking.
  virtual zetasql_base::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      TypeProto* type_proto,
      absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
      FileDescriptorSetMap* file_descriptor_set_map) const = 0;

  const TypeFactory* type_factory_;  // Used for lifetime checking only.
  const TypeKind kind_;

 private:
  // Recursive implementation of SupportsGrouping, which returns in
  // "no_grouping_type" the contained type that made grouping unsupported.
  virtual bool SupportsGroupingImpl(const LanguageOptions& language_options,
                                    const Type** no_grouping_type) const;

  // Recursive implementatiion of SupportsPartitioning, which returns in
  // "no_partitioning_type" the contained type that made partitioning
  // unsupported.
  virtual bool SupportsPartitioningImpl(
      const LanguageOptions& language_options,
      const Type** no_partitioning_type) const;

  // Helper function invoked by IsSupportedType(), specifically for
  // simple types.  Returns true if 'type' is enabled given
  // 'language_options'.  Checks for ProductMode, TimestampMode, and
  // supported LanguageFeatures.  Crashes if 'type' is not a simple type.
  bool IsSupportedSimpleType(const LanguageOptions& language_options) const;

  friend class TypeFactory;
  friend class ArrayType;
  friend class StructType;
};

typedef Type::FileDescriptorSetMap FileDescriptorSetMap;

// SimpleType includes all the non-parameterized types (all scalar types
// except enum).
class SimpleType : public Type {
 public:
#ifndef SWIG
  SimpleType(const SimpleType&) = delete;
  SimpleType& operator=(const SimpleType&) = delete;
#endif  // SWIG

  std::string TypeName(ProductMode mode) const override;
  std::string DebugString(bool details = false) const override;

 protected:
  SimpleType(const TypeFactory* factory, TypeKind kind);
  ~SimpleType() override;

 private:
  zetasql_base::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      TypeProto* type_proto,
      absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
      FileDescriptorSetMap* file_descriptor_set_map) const override;

  friend class TypeFactory;
};

// An array type.
// Arrays of arrays are not supported.
class ArrayType : public Type {
 public:
#ifndef SWIG
  ArrayType(const ArrayType&) = delete;
  ArrayType& operator=(const ArrayType&) = delete;
#endif  // SWIG

  const Type* element_type() const { return element_type_; }

  const ArrayType* AsArray() const override { return this; }

  // Helper function to determine deep equality or equivalence for array types.
  static bool EqualsImpl(const ArrayType* type1, const ArrayType* type2,
                         bool equivalent);

  // Arrays support ordering if FEATURE_V_1_3_ARRAY_ORDERING is enabled
  // and the array's element Type supports ordering.
  bool SupportsOrdering(const LanguageOptions& language_options,
                        std::string* type_description) const override;
  bool SupportsEquality() const override;

  std::string ShortTypeName(ProductMode mode) const override;
  std::string TypeName(ProductMode mode) const override;
  std::string DebugString(bool details = false) const override;

  bool UsingFeatureV12CivilTimeType() const override {
    return element_type_->UsingFeatureV12CivilTimeType();
  }

  int nesting_depth() const override {
    return element_type_->nesting_depth() + 1;
  }

 private:
  ArrayType(const TypeFactory* factory, const Type* element_type);
  ~ArrayType() override;

  bool SupportsGroupingImpl(const LanguageOptions& language_options,
                            const Type** no_grouping_type) const override;

  bool SupportsPartitioningImpl(
      const LanguageOptions& language_options,
      const Type** no_partitioning_type) const override;

  zetasql_base::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      TypeProto* type_proto,
      absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
      FileDescriptorSetMap* file_descriptor_set_map) const override;

  const Type* const element_type_;

  friend class TypeFactory;
};

// Field contained in a struct, representing a name and type.
// The SWIG compiler does not understand nested classes, so this cannot be
// defined inside the scope of StructType.
struct StructField {
  StructField(std::string name_in, const Type* type_in)
      : name(std::move(name_in)), type(type_in) {}

  std::string name;  // Empty std::string means this is an unnamed field.
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
  std::string DebugString(bool details = false) const override;

  // Check if the names in <fields> are valid.
  static zetasql_base::Status FieldNamesAreValid(
      const absl::Span<const StructField>& fields);

  int nesting_depth() const override { return nesting_depth_; }

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

  zetasql_base::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      TypeProto* type_proto,
      absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
      FileDescriptorSetMap* file_descriptor_set_map) const override;

  std::string DebugStringImpl(
      int field_limit,
      const std::function<std::string(const zetasql::Type*)>&
          field_debug_fn) const;

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
      field_name_to_index_map_ GUARDED_BY(mutex_);

  friend class TypeFactory;
};

// A proto type.
class ProtoType : public Type {
 public:
#ifndef SWIG
  ProtoType(const ProtoType&) = delete;
  ProtoType& operator=(const ProtoType&) = delete;
#endif  // SWIG

  const ProtoType* AsProto() const override { return this; }

  const google::protobuf::Descriptor* descriptor() const;

  // Helper function to determine equality or equivalence for proto types.
  static bool EqualsImpl(const ProtoType* type1, const ProtoType* type2,
                         bool equivalent);

  bool SupportsOrdering(const LanguageOptions& language_options,
                        std::string* type_description) const override;
  bool SupportsEquality() const override { return false; }

  bool UsingFeatureV12CivilTimeType() const override { return false; }

  // TODO: The current implementation of TypeName/ShortTypeName
  // should be re-examined for Proto and Enum Types.  Currently, the
  // TypeName is the back-ticked descriptor full_name, while the ShortTypeName
  // is just the descriptor full_name (without back-ticks).  The back-ticks
  // are not necessary for TypeName() to be reparseable, so should be removed.
  std::string TypeName(ProductMode mode_unused) const override;
  std::string ShortTypeName(
      ProductMode mode_unused = ProductMode::PRODUCT_INTERNAL) const override;
  std::string TypeName() const;  // Proto-specific version does not need mode.
  std::string DebugString(bool details = false) const override;

  // Get the ZetaSQL Type of the requested field of the proto, identified by
  // either tag number or name.  A new Type may be created so a type factory
  // is required.  If the field name or number is not found, then
  // zetasql_base::INVALID_ARGUMENT is returned.  The last argument can be used
  // to output the corresponding name/number as appropriate.
  zetasql_base::Status GetFieldTypeByTagNumber(int number, TypeFactory* factory,
                                       const Type** type,
                                       std::string* name = nullptr) const {
    return GetFieldTypeByTagNumber(
        number, factory, /*use_obsolete_timestamp=*/false, type, name);
  }
  zetasql_base::Status GetFieldTypeByName(const std::string& name, TypeFactory* factory,
                                  const Type** type,
                                  int* number = nullptr) const {
    return GetFieldTypeByName(name, factory,
                              /*use_obsolete_timestamp=*/false, type, number);
  }

  // DEPRECATED: Callers should remove their dependencies on obsolete types and
  // move to the methods above.
  zetasql_base::Status GetFieldTypeByTagNumber(int number, TypeFactory* factory,
                                       bool use_obsolete_timestamp,
                                       const Type** type,
                                       std::string* name = nullptr) const;
  zetasql_base::Status GetFieldTypeByName(const std::string& name, TypeFactory* factory,
                                  bool use_obsolete_timestamp,
                                  const Type** type,
                                  int* number = nullptr) const;

  // Get the ZetaSQL TypeKind of the requested proto field. If
  // <ignore_annotations> is false, then format annotations on the field are
  // respected when determining the TypeKind. If <ignore_annotations> is true,
  // then format annotations are ignored and the default TypeKind for the proto
  // field type is returned.
  //
  // This always ignores (does not unwrap) is_struct and is_wrapper annotations.
  static zetasql_base::Status FieldDescriptorToTypeKind(
      bool ignore_annotations, const google::protobuf::FieldDescriptor* field,
      TypeKind* kind);

  // Get the ZetaSQL TypeKind of the requested proto field.
  // This is the same as the above signature with ignore_annotations = false.
  // This is the TypeKind for the field type visible in ZetaSQL, matching
  // the Type returned by GetProtoFieldType (except for array types).
  static zetasql_base::Status FieldDescriptorToTypeKind(
      const google::protobuf::FieldDescriptor* field, TypeKind* kind) {
    return FieldDescriptorToTypeKind(
        /*ignore_annotations=*/false, field, kind);
  }
  // DEPRECATED: Callers should remove their dependencies on obsolete types and
  // move to the method above.
  static zetasql_base::Status FieldDescriptorToTypeKind(
      const google::protobuf::FieldDescriptor* field, bool use_obsolete_timestamp,
      TypeKind* kind);

  // This is the same as FieldDescriptorToTypeKind except it ignores
  // repeatedness of the proto field and never returns TYPE_ARRAY.
  static zetasql_base::Status FieldDescriptorToTypeKindBase(
      bool ignore_annotations, const google::protobuf::FieldDescriptor* field,
      TypeKind* kind) {
    return GetTypeKindFromFieldDescriptor(field, ignore_annotations,
                                          /*use_obsolete_timestamp=*/false,
                                          kind);
  }
  // This is the same as the above signature with ignore_annotations = false.
  static zetasql_base::Status FieldDescriptorToTypeKindBase(
      const google::protobuf::FieldDescriptor* field, TypeKind* kind) {
    return FieldDescriptorToTypeKindBase(/*ignore_annotations=*/false, field,
                                         kind);
  }
  // DEPRECATED: Callers should remove their dependencies on obsolete types and
  // move to the method above.
  static zetasql_base::Status FieldDescriptorToTypeKindBase(
      const google::protobuf::FieldDescriptor* field, bool use_obsolete_timestamp,
      TypeKind* kind) {
    return GetTypeKindFromFieldDescriptor(field,
                                          /*ignore_format_annotations=*/false,
                                          use_obsolete_timestamp, kind);
  }

  // Case insensitive version of google::protobuf::Descriptor::FindFieldByName.
  // Returns NULL if the name is not found.
  static const google::protobuf::FieldDescriptor* FindFieldByNameIgnoreCase(
      const google::protobuf::Descriptor* descriptor, const std::string& name);

  // Get the zetasql Format from a FieldDescriptor.
  // Note that if a deprecated Encoding annotation exists and is valid,
  // this merges it over top of the Format annotation and acts as if the
  // Format was written in the current non-deprecated syntax.
  static bool HasFormatAnnotation(const google::protobuf::FieldDescriptor* field);
  static FieldFormat::Format GetFormatAnnotation(
      const google::protobuf::FieldDescriptor* field);

  // Returns true if default value for <field> should be used.
  // Returns false if SQL NULL should be used instead.
  // This is based on the zetasql.use_defaults annotation on the field and
  // the zetasql.use_field_defaults annotation on the containing message.
  static bool GetUseDefaultsExtension(const google::protobuf::FieldDescriptor* field);

  // Returns true if <message> is annotated with zetasql.is_wrapper=true.
  static bool GetIsWrapperAnnotation(const google::protobuf::Descriptor* message);

  // Returns true if <message> is annotated with zetasql.is_struct=true.
  static bool GetIsStructAnnotation(const google::protobuf::Descriptor* message);

  // Get the struct field name from a FieldDescriptor.
  static bool HasStructFieldName(const google::protobuf::FieldDescriptor* field);
  static const std::string& GetStructFieldName(const google::protobuf::FieldDescriptor* field);

  // Validate TypeAnnotations for a file, proto, or field.  Protos not
  // in <validated_descriptor_set> are added to the set and validated.
  // <validated_descriptor_set> may be a pointer to any set type that contains
  // 'const google::protobuf::Descriptor*' (absl::flat_hash_set<const google::protobuf::Descriptor*>
  // is recommended), or it may simply be nullptr.  Proto validation includes
  // recursively validating proto types of fields if <validated_descriptor_set>
  // is not NULL.
  template <typename SetPtrType>
  static zetasql_base::Status ValidateTypeAnnotations(
      const google::protobuf::FileDescriptor* file_descriptor,
      SetPtrType validated_descriptor_set);

  template <typename SetPtrType>
  static zetasql_base::Status ValidateTypeAnnotations(
      const google::protobuf::Descriptor* descriptor,
      SetPtrType validated_descriptor_set);
  static zetasql_base::Status ValidateTypeAnnotations(
      const google::protobuf::Descriptor* descriptor) {
    return ValidateTypeAnnotations(descriptor,
                                   /*validated_descriptor_set=*/nullptr);
  }

  template <typename SetPtrType>
  static zetasql_base::Status ValidateTypeAnnotations(
      const google::protobuf::FieldDescriptor* field,
      SetPtrType validated_descriptor_set);
  static zetasql_base::Status ValidateTypeAnnotations(
      const google::protobuf::FieldDescriptor* field) {
    return ValidateTypeAnnotations(field, /*validated_descriptor_set=*/nullptr);
  }

 private:
  // Returns true iff <validated_descriptor_set> is not null and already
  // contains <descriptor>.  Otherwise returns false and, if
  // <validated_descriptor_set> is non-null, inserts <descriptor> into it.
  template <typename SetPtrType>
  static bool IsAlreadyValidated(SetPtrType validated_descriptor_set,
                                 const google::protobuf::Descriptor* descriptor) {
    return validated_descriptor_set != nullptr &&
           !validated_descriptor_set->insert(descriptor).second;
  }

  // Does not take ownership of <factory> or <descriptor>.  The <descriptor>
  // must outlive the type.
  ProtoType(const TypeFactory* factory,
            const google::protobuf::Descriptor* descriptor);
  ~ProtoType() override;

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

  // Internal version of GetFormatAnnotation that just merges <type> and
  // <format>, without merging <encoding>.
  static FieldFormat::Format GetFormatAnnotationImpl(
      const google::protobuf::FieldDescriptor* field);

  // Get the ZetaSQL TypeKind of the requested proto field. If
  // <ignore_format_annotations> is true, then format annotations are ignored
  // and the default TypeKind for the proto field type is returned.
  static zetasql_base::Status GetTypeKindFromFieldDescriptor(
      const google::protobuf::FieldDescriptor* field, bool ignore_format_annotations,
      bool use_obsolete_timestamp, TypeKind* kind);

  zetasql_base::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      TypeProto* type_proto,
      absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
      FileDescriptorSetMap* file_descriptor_set_map) const override;

  const google::protobuf::Descriptor* descriptor_;  // Not owned.

  friend class TypeFactory;
};

// An enum type.
// Each EnumType object defines a set of legal numeric (int32_t) values.  Each
// value has one (or more) std::string names.  The first name is treated as the
// canonical name for that numeric value, and additional names are treated
// as aliases.
//
// We currently support creating a ZetaSQL EnumType from a protocol
// buffer EnumDescriptor.  This is likely to be extended to support EnumTypes
// without an EnumDescriptor by explicitly providing a list of enum values
// instead.
class EnumType : public Type {
 public:
#ifndef SWIG
  EnumType(const EnumType&) = delete;
  EnumType& operator=(const EnumType&) = delete;
#endif  // SWIG

  const EnumType* AsEnum() const override { return this; }

  const google::protobuf::EnumDescriptor* enum_descriptor() const;

  // Helper function to determine equality or equivalence for enum types.
  static bool EqualsImpl(const EnumType* type1, const EnumType* type2,
                         bool equivalent);

  // TODO: The current implementation of TypeName/ShortTypeName
  // should be re-examined for Proto and Enum Types.  Currently, the
  // TypeName is the back-ticked descriptor full_name, while the ShortTypeName
  // is just the descriptor full_name (without back-ticks).  The back-ticks
  // are not necessary for TypeName() to be reparseable, so should be removed.
  std::string TypeName(ProductMode mode_unused) const override;
  std::string ShortTypeName(
      ProductMode mode_unused = ProductMode::PRODUCT_INTERNAL) const override;
  std::string TypeName() const;  // Enum-specific version does not need mode.
  std::string DebugString(bool details = false) const override;

  bool UsingFeatureV12CivilTimeType() const override { return false; }

  // Finds the enum name given a corresponding enum number.  Returns true
  // upon success, and false if the number is not found.  For enum numbers
  // that are not unique, this function will return the canonical name
  // for that number.
  ABSL_MUST_USE_RESULT bool FindName(int number, const std::string** name) const;

  // Find the enum number given a corresponding name.  Returns true
  // upon success, and false if the name is not found.
  ABSL_MUST_USE_RESULT bool FindNumber(const std::string& name, int* number) const;

 private:
  // Does not take ownership of <factory> or <enum_descr>.  The
  // <enum_descriptor> must outlive the type.  The <enum_descr> must not be
  // NULL.
  EnumType(const TypeFactory* factory,
           const google::protobuf::EnumDescriptor* enum_descr);
  ~EnumType() override;

  bool SupportsGroupingImpl(const LanguageOptions& language_options,
                            const Type** no_grouping_type) const override {
    if (no_grouping_type != nullptr) {
      *no_grouping_type = nullptr;
    }
    return true;
  }

  bool SupportsPartitioningImpl(
      const LanguageOptions& language_options,
      const Type** no_partitioning_type) const override {
    if (no_partitioning_type != nullptr) {
      *no_partitioning_type = nullptr;
    }
    return true;
  }

  zetasql_base::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      TypeProto* type_proto,
      absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
      FileDescriptorSetMap* file_descriptor_set_map) const override;

  const google::protobuf::EnumDescriptor* enum_descriptor_;  // Not owned.

  friend class TypeFactory;
};

#ifndef SWIG
// Provides equality comparison operator for Types.  This primarily invokes
// Type::Equals().
struct TypeEquals {
 public:
  bool operator()(const Type* const type1,
                  const Type* const type2) const;
};
#endif  // SWIG

// A TypeFactory creates and owns Type objects.
// Created Type objects live until the TypeFactory is destroyed.
// The TypeFactory may return the same Type object from multiple calls that
// request equivalent types.
//
// When a compound Type (array or struct) is constructed referring to a Type
// from a separate TypeFactory, the constructed type may refer to the Type from
// the separate TypeFactory, so that TypeFactory must outlive this one.
//
// This class is thread-safe.
class TypeFactory {
 public:
  TypeFactory();
#ifndef SWIG
  TypeFactory(const TypeFactory&) = delete;
  TypeFactory& operator=(const TypeFactory&) = delete;
#endif  // SWIG
  ~TypeFactory();

  // Helpers to get simple scalar types directly.
  const Type* get_int32();
  const Type* get_int64();
  const Type* get_uint32();
  const Type* get_uint64();
  const Type* get_string();
  const Type* get_bytes();
  const Type* get_bool();
  const Type* get_float();
  const Type* get_double();
  const Type* get_date();
  const Type* get_timestamp();
  const Type* get_time();
  const Type* get_datetime();
  const Type* get_geography();
  const Type* get_numeric();

  // Return a Type object for a simple type.  This works for all
  // non-parameterized scalar types.  Enums, arrays, structs and protos must
  // use the parameterized constructors.
  const Type* MakeSimpleType(TypeKind kind);

  // Make an array type.
  // Arrays of arrays are not supported and will fail with an error.
  zetasql_base::Status MakeArrayType(const Type* element_type,
                             const ArrayType** result);
  zetasql_base::Status MakeArrayType(const Type* element_type,
                             const Type** result);

  // Make a struct type.
  // The field names must be valid.
  zetasql_base::Status MakeStructType(absl::Span<const StructType::StructField> fields,
                              const StructType** result);
  zetasql_base::Status MakeStructType(absl::Span<const StructType::StructField> fields,
                              const Type** result);
  zetasql_base::Status MakeStructTypeFromVector(
      std::vector<StructType::StructField> fields, const StructType** result);
  zetasql_base::Status MakeStructTypeFromVector(
      std::vector<StructType::StructField> fields, const Type** result);

  // Make a proto type.
  // The <descriptor> must outlive this TypeFactory.
  //
  // This always constructs a ProtoType, even for protos that are
  // annotated with zetasql.is_struct or zetasql.is_wrapper,
  // which normally indicate the proto should be interpreted as
  // a different type.  Use MakeUnwrappedTypeFromProto instead
  // to get the unwrapped type.
  zetasql_base::Status MakeProtoType(const google::protobuf::Descriptor* descriptor,
                             const ProtoType** result);
  zetasql_base::Status MakeProtoType(const google::protobuf::Descriptor* descriptor,
                             const Type** result);

  // Make a zetasql type from a proto, honoring zetasql.is_struct and
  // zetasql.is_wrapper annotations.
  // These annotations allow creating a proto representation of any zetasql
  // type, including structs and arrays, with nullability.
  // Such protos can be created with methods in convert_type_to_proto.h.
  // This method converts protos back to the represented zetasql type.
  zetasql_base::Status MakeUnwrappedTypeFromProto(const google::protobuf::Descriptor* message,
                                          const Type** result_type) {
    return MakeUnwrappedTypeFromProto(message, /*use_obsolete_timestamp=*/false,
                                      result_type);
  }
  // DEPRECATED: Callers should remove their dependencies on obsolete types and
  // move to the method above.
  zetasql_base::Status MakeUnwrappedTypeFromProto(const google::protobuf::Descriptor* message,
                                          bool use_obsolete_timestamp,
                                          const Type** result_type);

  // Like the method above, but starting from a zetasql::Type.
  // If the Type is not a proto, it will be returned unchanged.
  zetasql_base::Status UnwrapTypeIfAnnotatedProto(const Type* input_type,
                                          const Type** result_type) {
    return UnwrapTypeIfAnnotatedProto(
        input_type, /*use_obsolete_timestamp=*/false, result_type);
  }
  // DEPRECATED: Callers should remove their dependencies on obsolete types and
  // move to the method above.
  zetasql_base::Status UnwrapTypeIfAnnotatedProto(const Type* input_type,
                                          bool use_obsolete_timestamp,
                                          const Type** result_type);

  // Make an enum type from a protocol buffer EnumDescriptor.
  // The <enum_descriptor> must outlive this TypeFactory.
  zetasql_base::Status MakeEnumType(const google::protobuf::EnumDescriptor* enum_descriptor,
                            const EnumType** result);
  zetasql_base::Status MakeEnumType(const google::protobuf::EnumDescriptor* enum_descriptor,
                            const Type** result);

  // Get the Type for a proto field.
  // If <ignore_annotations> is false, this looks at format annotations on the
  // field and possibly its parent message to help select the Type. If
  // <ignore_annotations> is true, annotations on the field are not considered
  // and the returned type is that of which ZetaSQL sees before applying any
  // annotations or automatic conversions. This function always ignores (does
  // not unwrap) is_struct and is_wrapper annotations.
  zetasql_base::Status GetProtoFieldType(bool ignore_annotations,
                                 const google::protobuf::FieldDescriptor* field_descr,
                                 const Type** type);

  // Get the Type for a proto field.
  // This is the same as the above signature with ignore_annotations = false.
  //
  // NOTE: There is a similar method GetProtoFieldTypeAndDefault in proto_util.h
  // that also extracts the default value.
  zetasql_base::Status GetProtoFieldType(const google::protobuf::FieldDescriptor* field_descr,
                                 const Type** type) {
    return GetProtoFieldType(/*ignore_annotations=*/false, field_descr, type);
  }
  // DEPRECATED: Callers should remove their dependencies on obsolete types and
  // move to the method above.
  zetasql_base::Status GetProtoFieldType(const google::protobuf::FieldDescriptor* field_descr,
                                 bool use_obsolete_timestamp,
                                 const Type** type);

  // Makes a ZetaSQL Type from a self-contained ZetaSQL TypeProto.  The
  // <type_proto> FileDescriptorSets are loaded into the pool.  The <pool>
  // must outlive the TypeFactory.  Will return an error if the
  // FileDescriptorSets cannot be deserialized into a single DescriptorPool,
  // i.e. if type_proto.file_descriptor_set_size() > 1.  For serialized types
  // spanning multiple pools, see
  // DeserializeFromSelfContainedProtoWithDistinctFiles below.
  zetasql_base::Status DeserializeFromSelfContainedProto(
      const TypeProto& type_proto,
      google::protobuf::DescriptorPool* pool,
      const Type** type);

  // Similar to the above, but supports types referencing multiple
  // DescriptorPools.  The provided pools must match the number of
  // FileDescriptorSets stored in <type_proto>.  Each FileDescriptorSet from
  // <type_proto> is loaded into the DescriptorPool corresponding to its index.
  zetasql_base::Status DeserializeFromSelfContainedProtoWithDistinctFiles(
      const TypeProto& type_proto,
      const std::vector<google::protobuf::DescriptorPool*>& pools,
      const Type** type);

  // Make a ZetaSQL Type from a ZetaSQL TypeProto.  All protos referenced
  // by <type_proto> must already have related descriptors in the <pool>.
  // The <pool> must outlive the TypeFactory.  May only be used with a
  // <type_proto> serialized via Type::SerializeToProtoAndFileDescriptors.
  zetasql_base::Status DeserializeFromProtoUsingExistingPool(
      const TypeProto& type_proto,
      const google::protobuf::DescriptorPool* pool,
      const Type** type);

  // Similar to the above, but expects that all protos and enums referenced by
  // <type_proto> must have related descriptors in the pool corresponding to
  // the ProtoTypeProto or EnumTypeProto's file_descriptor_set_index. May be
  // used with a <type_proto> serialized via
  // Type::SerializeToProtoAndFileDescriptors or
  // Type::SerializeToProtoAndDistinctFileDescriptors.
  zetasql_base::Status DeserializeFromProtoUsingExistingPools(
      const TypeProto& type_proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      const Type** type);

  // Maximum nesting depth for types supported by this TypeFactory. Any attempt
  // to create a type with a nesting_depth() greater than this will return an
  // error. If a limit is not set, the ZetaSQL analyzer may create types that
  // it cannot destruct. Use kint32max for no limit (the default).
  // The limit value must be >= 0. The default value of this field can be
  // overidden with FLAGS_zetasql_type_factory_nesting_depth_limit.
  int nesting_depth_limit() const LOCKS_EXCLUDED(mutex_);
  void set_nesting_depth_limit(int value) LOCKS_EXCLUDED(mutex_);

 private:
  // Store links to and from TypeFactories that this TypeFactory depends on.
  // This is used as a sanity check to catch incorrect destruction order.
  mutable absl::flat_hash_set<const TypeFactory*> depends_on_factories_
      GUARDED_BY(mutex_);
  mutable absl::flat_hash_set<const TypeFactory*> factories_depending_on_this_
      GUARDED_BY(mutex_);

  // Add <type> into <owned_types_>.  Templated so it can return the
  // specific subclass of Type.
  template <class TYPE>
  const TYPE* TakeOwnership(const TYPE* type) LOCKS_EXCLUDED(mutex_);
  template <class TYPE>
  const TYPE* TakeOwnershipLocked(const TYPE* type)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Mark that <other_type>'s factory must outlive <this>.
  void AddDependency(const Type* other_type) LOCKS_EXCLUDED(mutex_);

  // Get the Type for a proto field from its corresponding TypeKind. For
  // repeated fields, <kind> must be the base TypeKind for the field (i.e., the
  // TypeKind of the field, ignoring repeatedness), which can be obtained by
  // FieldDescriptorToTypeKindBase().
  zetasql_base::Status GetProtoFieldTypeWithKind(
      const google::protobuf::FieldDescriptor* field_descr, TypeKind kind,
      const Type** type);

  // Implementation of MakeUnwrappedTypeFromProto above that detects invalid use
  // of type annotations with recursive protos by storing all visited message
  // types in 'ancestor_messages'.
  zetasql_base::Status MakeUnwrappedTypeFromProtoImpl(
      const google::protobuf::Descriptor* message, const Type* existing_message_type,
      bool use_obsolete_timestamp, const Type** result_type,
      std::set<const google::protobuf::Descriptor*>* ancestor_messages);

  // Implementation of UnwrapTypeIfAnnotatedProto above that detects invalid use
  // of type annotations with recursive protos by storing all visited message
  // types in 'ancestor_messages'.
  zetasql_base::Status UnwrapTypeIfAnnotatedProtoImpl(
      const Type* input_type, bool use_obsolete_timestamp,
      const Type** result_type,
      std::set<const google::protobuf::Descriptor*>* ancestor_messages);

  // TODO: Should TypeFactory have a DescriptorPool?
  mutable absl::Mutex mutex_;
  const Type* cached_simple_types_[TypeKind_ARRAYSIZE] GUARDED_BY(mutex_);
  std::vector<const Type*> owned_types_ GUARDED_BY(mutex_);

  int nesting_depth_limit_ GUARDED_BY(mutex_);
};

namespace types {
// The following functions do *not* create any new types using the static
// factory.
const Type* Int32Type();
const Type* Int64Type();
const Type* Uint32Type();
const Type* Uint64Type();
const Type* BoolType();
const Type* FloatType();
const Type* DoubleType();
const Type* StringType();
const Type* BytesType();
const Type* DateType();
const Type* TimestampType();
const Type* TimeType();
const Type* DatetimeType();
const Type* GeographyType();
const Type* NumericType();
const StructType* EmptyStructType();

// ArrayTypes
const ArrayType* Int32ArrayType();
const ArrayType* Int64ArrayType();
const ArrayType* Uint32ArrayType();
const ArrayType* Uint64ArrayType();
const ArrayType* BoolArrayType();
const ArrayType* FloatArrayType();
const ArrayType* DoubleArrayType();
const ArrayType* StringArrayType();
const ArrayType* BytesArrayType();
const ArrayType* TimestampArrayType();
const ArrayType* DateArrayType();
const ArrayType* DatetimeArrayType();
const ArrayType* TimeArrayType();
const ArrayType* GeographyArrayType();
const ArrayType* NumericArrayType();

// Accessor for the ZetaSQL enum Type (functions::DateTimestampPart)
// that represents date parts in function signatures.  Intended
// to be used primarily within the ZetaSQL library, rather than as a
// part of the public ZetaSQL api.
const EnumType* DatePartEnumType();

// Accessor for the ZetaSQL enum Type (functions::NormalizeMode)
// that represents the normalization mode in NORMALIZE and
// NORMALIZE_AND_CASEFOLD.  Intended to be used primarily within the ZetaSQL
// library, rather than as a part of the public ZetaSQL API.
const EnumType* NormalizeModeEnumType();

// Return a type of 'type_kind' if 'type_kind' is a simple type, otherwise
// returns nullptr. This is similar to TypeFactory::MakeSimpleType, but doesn't
// require TypeFactory.
const Type* TypeFromSimpleTypeKind(TypeKind type_kind);

// Returns an array type with element type of 'type_kind' if 'type_kind' is a
// simple type, otherwise returns nullptr.
const ArrayType* ArrayTypeFromSimpleTypeKind(TypeKind type_kind);

// The valid date range is [ 0001-01-01, 9999-12-31 ].
static const int64_t kDateMin = -719162;
static const int64_t kDateMax = 2932896;

// The valid timestamp range for timestamps is:
//   [ 0001-01-01 00:00:00 UTC, 9999-12-31 23:59:59.999999 UTC ]
static const int64_t kTimestampMin = -62135596800LL * 1000000;
static const int64_t kTimestampMax = 253402300800LL * 1000000 - 1;

// The valid timestamp range for absl::Time is:
// [ 0001-01-01 00:00:00 UTC, 9999-12-31 23:59:59.999999999 UTC ]
absl::Time TimestampMinBaseTime();
absl::Time TimestampMaxBaseTime();

// The valid legacy timestamp range is from the beginning of 1678-01-01 to the
// end of 2261-12-31 UTC.  These are the years fully representable in nanos in
// an int64_t.  The bounds were computed with an online date converter, and
// verified with C library date formatting with TZ=UTC in unittest.
// TODO: Deprecated TIMESTAMP_XXX types, to be removed.
static const int64_t kTimestampNanosMin = -9214560000LL * 1000000000;
static const int64_t kTimestampNanosMax = 9214646400LL * 1000000000 - 1;
static const int64_t kTimestampMicrosMin = kTimestampNanosMin / 1000;
static const int64_t kTimestampMillisMin = kTimestampNanosMin / 1000000;
static const int64_t kTimestampSecondsMin = kTimestampNanosMin / 1000000000;
static const int64_t kTimestampMicrosMax = kTimestampNanosMax / 1000;
static const int64_t kTimestampMillisMax = kTimestampNanosMax / 1000000;
static const int64_t kTimestampSecondsMax = kTimestampNanosMax / 1000000000;

}  // namespace types

typedef std::pair<TypeKind, TypeKind> TypeKindPair;
struct TypeKindPairHasher {
  size_t operator()(const TypeKindPair& kinds) const {
    return (kinds.first * (TypeKind_MAX + 1)) + kinds.second;
  }
};

struct TypeKindHasher {
  size_t operator()(TypeKind kind) const {
    return kind;
  }
};

// Returns true if this is a valid TypeKind. This is stronger than the proto
// provided TypeKind_IsValid(), as it also returns false for the dummy
// __TypeKind__switch_must_have_a_default__ value.
bool IsValidTypeKind(int kind);

// Implementation of templated methods of ProtoType.
template <typename SetPtrType>
zetasql_base::Status ProtoType::ValidateTypeAnnotations(
    const google::protobuf::FileDescriptor* file_descriptor,
    SetPtrType validated_descriptor_set) {
  // Check all messages.
  for (int idx = 0; idx < file_descriptor->message_type_count(); ++idx) {
    const google::protobuf::Descriptor* message_type =
        file_descriptor->message_type(idx);
    ZETASQL_RETURN_IF_ERROR(
        ValidateTypeAnnotations(message_type, validated_descriptor_set));
  }

  // Check all extensions.
  for (int idx = 0; idx < file_descriptor->extension_count(); ++idx) {
    const google::protobuf::FieldDescriptor* extension =
        file_descriptor->extension(idx);
    ZETASQL_RETURN_IF_ERROR(
        ValidateTypeAnnotations(extension, validated_descriptor_set));
  }

  return ::zetasql_base::OkStatus();
}

template <typename SetPtrType>
zetasql_base::Status ProtoType::ValidateTypeAnnotations(
    const google::protobuf::Descriptor* descriptor,
    SetPtrType validated_descriptor_set) {
  if (IsAlreadyValidated(validated_descriptor_set, descriptor)) {
    // Already validated this proto, return OK.
    return ::zetasql_base::OkStatus();
  }

  // Check zetasql.is_wrapper.
  if (GetIsWrapperAnnotation(descriptor)) {
    if (descriptor->field_count() != 1) {
      return MakeSqlError()
             << "Proto " << descriptor->full_name()
             << " has zetasql.is_wrapper = true but does not have exactly"
             << " one field:\n"
             << descriptor->DebugString();
    }
    // We cannot have both is_wrapper and is_struct.  Beyond that, there is
    // no checking to do for is_struct.
    if (GetIsStructAnnotation(descriptor)) {
      return MakeSqlError()
             << "Proto " << descriptor->full_name()
             << " has both zetasql.is_wrapper = true and"
                " zetasql.is_struct = true:\n"
             << descriptor->DebugString();
    }
  }

  // Check all fields.
  for (int idx = 0; idx < descriptor->field_count(); ++idx) {
    const google::protobuf::FieldDescriptor* field = descriptor->field(idx);
    ZETASQL_RETURN_IF_ERROR(ValidateTypeAnnotations(field, validated_descriptor_set));
  }

  return ::zetasql_base::OkStatus();
}

template <typename SetPtrType>
zetasql_base::Status ProtoType::ValidateTypeAnnotations(
    const google::protobuf::FieldDescriptor* field,
    SetPtrType validated_descriptor_set) {
  const google::protobuf::FieldDescriptor::Type field_type = field->type();

  // Check zetasql.format and the deprecated zetasql.type version.
  // While validating, we check HasExtension explicitly because we want to
  // make sure the extension is not explicitly written as DEFAULT_FORMAT.
  if (field->options().HasExtension(zetasql::format) ||
      field->options().HasExtension(zetasql::type)) {
    const FieldFormat::Format field_format = GetFormatAnnotationImpl(field);
    // NOTE: This should match ProtoUtil::CheckIsSupportedFieldFormat in
    // reference_impl/proto_util.cc.
    switch (field_type) {
      case google::protobuf::FieldDescriptor::TYPE_INT32:
      case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
      case google::protobuf::FieldDescriptor::TYPE_SINT32:
        // DATE and DATE_DECIMAL are valid for int32_t.
        if (field_format != FieldFormat::DATE &&
            field_format != FieldFormat::DATE_DECIMAL) {
          return MakeSqlError()
                 << "Proto " << field->containing_type()->full_name()
                 << " has invalid zetasql.format for INT32 field: "
                 << field->DebugString();
        }
        break;
      case google::protobuf::FieldDescriptor::TYPE_INT64:
      case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
      case google::protobuf::FieldDescriptor::TYPE_SINT64:
        {
          // DATE, DATE_DECIMAL, DATETIME, TIME and TIMESTAMP_* are valid for
          // int64_t.
          if (field_format != FieldFormat::DATE &&
              field_format != FieldFormat::DATE_DECIMAL &&
              field_format != FieldFormat::TIMESTAMP_SECONDS &&
              field_format != FieldFormat::TIMESTAMP_MILLIS &&
              field_format != FieldFormat::TIMESTAMP_MICROS &&
              field_format != FieldFormat::TIMESTAMP_NANOS &&
              field_format != FieldFormat::TIME_MICROS &&
              field_format != FieldFormat::DATETIME_MICROS) {
            return MakeSqlError()
                   << "Proto " << field->containing_type()->full_name()
                   << " has invalid zetasql.format for INT64 field: "
                   << field->DebugString();
          }
        }
        break;
      case google::protobuf::FieldDescriptor::TYPE_UINT64:
        {
          if (field_format != FieldFormat::TIMESTAMP_MICROS) {
            return MakeSqlError()
                   << "Proto " << field->containing_type()->full_name()
                   << " has invalid zetasql.format for UINT64 field: "
                   << field->DebugString();
          }
        }
        break;
      case google::protobuf::FieldDescriptor::TYPE_BYTES:
        {
          if (field_format != FieldFormat::ST_GEOGRAPHY_ENCODED &&
              field_format != FieldFormat::NUMERIC) {
            return MakeSqlError()
                   << "Proto " << field->containing_type()->full_name()
                   << " has invalid zetasql.format for BYTES field: "
                   << field->DebugString();
          }
        }
        break;
      default:
        return MakeSqlError()
               << "Proto " << field->containing_type()->full_name()
               << " has invalid zetasql.format for field: "
               << field->DebugString();
        break;
    }
  }

  // Check zetasql.encoding (which is deprecated).
  if (field->options().HasExtension(zetasql::encoding)) {
    const DeprecatedEncoding::Encoding encoding_annotation =
        field->options().GetExtension(zetasql::encoding);
    const FieldFormat::Format format_annotation =
        GetFormatAnnotationImpl(field);
    switch (encoding_annotation) {
      case DeprecatedEncoding::DATE_DECIMAL:
        // This is allowed only on DATE fields.
        if (format_annotation != FieldFormat::DATE) {
          return MakeSqlError()
                 << "Proto " << field->containing_type()->full_name()
                 << " has zetasql.encoding that can only be applied"
                    " along with zetasql.format=DATE for field: "
                 << field->DebugString();
        }
        break;

      default:
        return MakeSqlError()
               << "Proto " << field->containing_type()->full_name()
               << " has invalid zetasql.encoding for field: "
               << field->DebugString();
    }
  }

  // Check zetasql.use_defaults.  Explicitly setting use_defaults to
  // true for repeated fields is an error.
  if (field->options().HasExtension(zetasql::use_defaults) &&
      field->options().GetExtension(zetasql::use_defaults) &&
      field->is_repeated()) {
    return MakeSqlError()
           << "Proto " << field->containing_type()->full_name()
           << " has invalid zetasql.use_defaults for repeated field: "
           << field->DebugString();
  }

  // Recurse if relevant.
  if (validated_descriptor_set != nullptr &&
      (field_type == google::protobuf::FieldDescriptor::TYPE_GROUP ||
       field_type == google::protobuf::FieldDescriptor::TYPE_MESSAGE)) {
    return ValidateTypeAnnotations(field->message_type(),
                                   validated_descriptor_set);
  }

  return ::zetasql_base::OkStatus();
}

// Template override for std::nullptr_t. This is needed when the caller simply
// passes in 'nullptr' for 'validated_descriptor_set', which should be supported
// even though it is not a set type that can work with the generic version.
template <>
inline bool ProtoType::IsAlreadyValidated(
    std::nullptr_t validated_descriptor_set,
    const google::protobuf::Descriptor* descriptor) {
  return false;
}

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPE_H_
