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

#ifndef ZETASQL_PUBLIC_TYPES_TYPE_H_
#define ZETASQL_PUBLIC_TYPES_TYPE_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/float_margin.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/proto/type_annotation.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/timestamp_util.h"
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
#include "absl/types/span.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

class ArrayType;
class EnumType;
class ExtendedType;
class LanguageOptions;
class ProtoType;
class StructType;
class Type;
class TypeFactory;
class Value;
class ValueContent;

namespace internal {
class TypeStore;
}  // namespace internal

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
  bool IsBigNumericType() const { return kind_ == TYPE_BIGNUMERIC; }
  bool IsJsonType() const { return kind_ == TYPE_JSON; }

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
  bool IsNumerical() const {
    switch (kind_) {
      case TYPE_INT32:
      case TYPE_INT64:
      case TYPE_UINT32:
      case TYPE_UINT64:
      case TYPE_FLOAT:
      case TYPE_DOUBLE:
      case TYPE_NUMERIC:
      case TYPE_BIGNUMERIC:
        return true;
      default:
        return false;
    }
  }
  bool IsInteger() const {
    switch (kind_) {
      case TYPE_INT32:
      case TYPE_INT64:
      case TYPE_UINT32:
      case TYPE_UINT64:
        return true;
      default:
        return false;
    }
  }
  bool IsSignedInteger() const { return IsInt32() || IsInt64(); }
  bool IsUnsignedInteger() const { return IsUint32() || IsUint64(); }

  // Simple types are those builtin types that can be represented with just a
  // TypeKind, with no parameters. This exists instead of IsScalarType because
  // enums act more like scalars, but require parameters.
  bool IsSimpleType() const { return IsSimpleType(kind_); }

  // Extended types are defined outside of ZetaSQL codebase. They have
  // TYPE_EXTENDED type kind and their classes inherit zetasql::ExtendedType
  // class.
  bool IsExtendedType() const { return kind_ == TYPE_EXTENDED; }

  // Return this Type cast to the given subclass, or nullptr if this type
  // is not of the requested type.
  virtual const ArrayType* AsArray() const { return nullptr; }
  virtual const StructType* AsStruct() const { return nullptr; }
  virtual const ProtoType* AsProto() const { return nullptr; }
  virtual const EnumType* AsEnum() const { return nullptr; }
  virtual const ExtendedType* AsExtendedType() const { return nullptr; }

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
  absl::Status SerializeToSelfContainedProto(
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
  absl::Status SerializeToProtoAndFileDescriptors(
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
  absl::Status SerializeToProtoAndDistinctFileDescriptors(
      TypeProto* type_proto,
      FileDescriptorSetMap* file_descriptor_set_map) const;

  // Same as the previous, but also optionally takes a limit on total
  // FileDescriptorSet size in <file_descriptor_sets_max_size_bytes>.
  // Returns an error and aborts if this size limit is exceeded by
  // the FileDescriptorSets in the <file_descriptor_set_map>.
  absl::Status SerializeToProtoAndDistinctFileDescriptors(
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
  virtual std::string TypeName(ProductMode mode) const = 0;

  // Returns the full description of the type without truncation. This should
  // only be used for logging or tests and not for any user-facing messages. For
  // proto-based types, this will return PROTO<name> or ENUM<name>, which are
  // not valid to parse as SQL.
  // If <details> is true, then the description includes full proto descriptors.
  std::string DebugString(bool details = false) const;

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
                          bool include_pseudo_fields = true) const {
    return HasFieldImpl(name, field_id, include_pseudo_fields);
  }

  // Return true if this type has any fields.
  // Will return false for structs or protos with zero fields.
  // Any pseudo-fields are not counted. Always returns false for arrays.
  virtual bool HasAnyFields() const { return false; }

  // Returns true if this type is enabled given 'language_options'.
  // Checks for ProductMode, TimestampMode, and supported LanguageFeatures.
  virtual bool IsSupportedType(
      const LanguageOptions& language_options) const = 0;

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
  // Assumes all language features are enabled.
  ABSL_DEPRECATED("Use GetTypeKindIfSimple instead")
  static bool IsSimpleTypeName(const std::string& type_name, ProductMode mode) {
    return GetTypeKindIfSimple(type_name, mode) != TYPE_UNKNOWN;
  }

  // Returns the matching TypeKind associated with <type_name>, or crashes
  // if the name does not identify a simple Type.
  ABSL_DEPRECATED("Use GetTypeKindIfSimple instead")
  static TypeKind SimpleTypeNameToTypeKindOrDie(const std::string& type_name,
                                                ProductMode mode) {
    TypeKind type_kind = GetTypeKindIfSimple(type_name, mode);
    CHECK_NE(type_kind, TYPE_UNKNOWN);
    return type_kind;
  }

  // Returns the type kind if 'type_name' is a simple type in 'mode', assuming
  // all language features are enabled. Returns TYPE_UNKNOWN otherwise.
  static TypeKind GetTypeKindIfSimple(absl::string_view type_name,
                                      ProductMode mode);
  // Returns the type kind if 'type_name' is a simple type given
  // 'language_options', or TYPE_UNKNOWN otherwise.
  static TypeKind GetTypeKindIfSimple(absl::string_view type_name,
                                      const LanguageOptions& language_options);

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

  // Controls for the building of the FileDescriptorSetMap.
  struct BuildFileDescriptorSetMapOptions {
    // If true, FileDescriptorSetMap is populated with the transitive dependency
    // set of the required types and the FileDescriptorSets are built.
    // Otherwise, FileDescriptorSetMap is populated only with the directly
    // required FileDescriptors and the FileDescriptorSets are not initialized.
    bool build_file_descriptor_sets = true;

    // A limit for the size of the FileDescriptorSets. If the sum of the sizes
    // passes this limit, the next attempt to add an element will cause an
    // error.
    absl::optional<int64_t> file_descriptor_sets_max_size_bytes = absl::nullopt;
  };

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
    return EqualsForSameKind(other_type, equivalent);
  }

  // Hashes the type. Hash is generated based on a type's kind and (if type is
  // not built-in simple type) on a type's parameter.
  absl::HashState Hash(absl::HashState state) const;

  // Hashes the type's parameter of non-simple (parameterized) types. Simple
  // built-in types should not update the hash state.
  virtual absl::HashState HashTypeParameter(absl::HashState state) const = 0;

  // Internal implementation for Serialize methods.  This will append
  // Type information to <type_proto>, so the caller should make sure
  // that <type_proto> has been initialized properly before invoking.
  // <options> controls whether FileDescriptors are deep-scanned and
  // FileDescriptorSets are generated, and also whether there are any limits
  // placed on the size of the FileDescriptorSets.
  virtual absl::Status SerializeToProtoAndDistinctFileDescriptorsImpl(
      const BuildFileDescriptorSetMapOptions& options, TypeProto* type_proto,
      FileDescriptorSetMap* file_descriptor_set_map) const = 0;

  // Returns estimated size of memory owned by this type. Note: type can never
  // own another type, only reference (all types are owned by TypeFactory). So,
  // this function never calls GetEstimatedOwnedMemoryBytesSize for other types
  // (such as element types of arrays or field types of structs).
  virtual int64_t GetEstimatedOwnedMemoryBytesSize() const = 0;

  // Functions below are used as an interface between zetasql::Value and Type
  // to manage Values content life-cycle. Make Value a friend, so it can access
  // them.
  friend class Value;

  // Initializes a value's content before it is used. This function is called
  // from "Value::Value(const Type*)". It's expected that ValueContent is
  // empty (all bytes are zeros) before this call.
  // TODO: We need to consider removing "Value(const Type*)" together
  // with this function: today that Value's constructor is used for creation of
  // null, struct and array values. For null values we can just allocate nothing
  // (also can save a space this way). For arrays and struct we can have a
  // special factory function within corresponding Type subclasses.
  virtual void InitializeValueContent(ValueContent* value) const {}

  // Copies value's content to another value. Is called when one value is
  // assigned to another. It's expected that content of destination is empty
  // (doesn't contain any valid content that needs to be destructed): if
  // operation is a replacement, it should have been cleaned with
  // ClearValueContent before this call. Default implementation just copy
  // ValueContent's memory.
  virtual void CopyValueContent(const ValueContent& from,
                                ValueContent* to) const;

  // Releases value's content if it owns some memory allocation. This function
  // is called from Value::Clear when value is replaced with another or freed.
  // The value's content lifetime is managed by Value class, which also ensures
  // that ClearValueContent is called only once for each constructed
  // content. Note: this function must not be used outside of the Value
  // destruction context.
  virtual void ClearValueContent(const ValueContent& value) const {}

  // Contains value equality check options that can be provided to
  // ValueContentEquals function.
  struct ValueEqualityCheckOptions {
    ValueEqualityCheckOptions(const Type* other_value_type,
                              FloatMargin float_margin, std::string* reason)
        : other_value_type(other_value_type),
          float_margin(float_margin),
          reason(reason) {}

    // The type of compared value. This type should always be equivalent to the
    // type for which ValueContentEquals is called:
    // this->Equivalent(options.other_value_type) == true.
    const Type* other_value_type;

    // Defines the maximum allowed absolute error when comparing floating point
    // numbers (float and double).
    FloatMargin float_margin = kExactFloatMargin;

    // If 'reason' is not null, upon inequality it may be set to human-readable
    // explanation of what parts of values differ.
    std::string* reason = nullptr;
  };

  // Checks two values for equality. The first value belongs to the current
  // type, while the second one belongs to an equivalent type
  // (this->Equivalent(other_value_type) == true). The pointer to the value of
  // the second type can be found in 'options'. If both values are nulls, they
  // are considered equal.
  // Note: this function doesn't perform SQL equality.
  // This function should only be used from zetasql::Value class.
  bool ValueContentEquals(const ValueContent& x, const ValueContent& y,
                          const ValueEqualityCheckOptions& options) const;

  // Checks for equality a content of two values. It should be expected that
  // both values that are passed to this function are not-null, the first value
  // belongs to the current type and the second to the type that is equivalent
  // to the current type (pointer to the actual type can be found in
  // ValueEqualityCheckOptions::other_value_type). This function should only be
  // called from ValueContentEquals that enforces these assumptions.
  virtual bool ValueContentEqualsImpl(
      const ValueContent& x, const ValueContent& y,
      const ValueEqualityCheckOptions& options) const = 0;

  // Returns memory size allocated by value's content (outside of Value class
  // memory itself).
  virtual uint64_t GetValueContentExternallyAllocatedByteSize(
      const ValueContent& value) const {
    return 0;
  }

  // Hashes the content of the value. This function is called from
  // Value::HashValueInternal. It's not required that generated hash depends on
  // type's information (like, a kind or a parameter), and from performance
  // considerations it should only be based on a value's content (if dependence
  // on a type is important for a caller, caller can use Type's Hash function
  // after HashValueContent).
  virtual absl::HashState HashValueContent(const ValueContent& value,
                                           absl::HashState state) const = 0;

  // Formatting options that can be provided to FormatValueContent.
  struct FormatValueContentOptions {
    enum class Mode {
      // Should generate a string value to use for debugging purposes.
      // This mode is used by Value::DebugString: please check the comments to
      // this function for more details.
      kDebug = 0,

      // Should generate a SQL literal that can be used as a literal to produce
      // a value with the content compatible to the given ValueContent object.
      // This mode is used by Value::GetSQLLiteral: please check the comments to
      // this function for more details.
      kSQLLiteral,

      // Should generate a SQL expression that produces a value with the given
      // content and belonging to the current type.
      // This mode is used by Value::GetSQL: please check the comments to this
      // function for more details.
      kSQLExpression,
    };

    // The getters below are here mostly for historical reasons: originally
    // internal zetasql::Value formatting functions were using these two flags
    // to figure out which formatting mode is requested. New types should not
    // use them, but should rely on the "mode" field directly.
    bool as_literal() const { return mode != Mode::kSQLExpression; }
    bool add_simple_type_prefix() const { return mode != Mode::kDebug; }

    ProductMode product_mode = ProductMode::PRODUCT_EXTERNAL;
    Mode mode = Mode::kDebug;
    bool verbose = false;  // Used with debug mode only.
  };

  // Returns a string representation of the value content based on the given
  // formatting options. Value content is required to be non-null. This function
  // is called from Value::GetSQLInternal, which is used by Value::GetSQL,
  // Value::GetSQLLiteral and Value::DebugString functions. Please check
  // comments to these functions for the details of expected result
  // representation.
  virtual std::string FormatValueContent(
      const ValueContent& value,
      const FormatValueContentOptions& options) const = 0;

  // List of DebugStringImpl outputs. Used to serve as a stack in
  // DebugStringImpl to protect from stack overflows.
  // Note: SWIG will fail to process this file if we remove a white space
  // between '>' at the TypeOrStringVector definition or use "using" instead of
  // "typedef".
  typedef std::vector<absl::variant<const Type*, std::string> >
      TypeOrStringVector;

 private:
  // Recursive implementation of SupportsGrouping, which returns in
  // "no_grouping_type" the contained type that made grouping unsupported.
  virtual bool SupportsGroupingImpl(const LanguageOptions& language_options,
                                    const Type** no_grouping_type) const;

  // Recursive implementation of SupportsPartitioning, which returns in
  // "no_partitioning_type" the contained type that made partitioning
  // unsupported.
  virtual bool SupportsPartitioningImpl(
      const LanguageOptions& language_options,
      const Type** no_partitioning_type) const;

  // Compares type instances belonging to the same type kind.
  virtual bool EqualsForSameKind(const Type* that, bool equivalent) const = 0;

  // Outputs elements describing type representation for debugging purposes.
  // Type should append these textual elements to debug_string. However,
  // if type references other types, to prevent possibility of stack overflow it
  // needs to push them into the "stack" instead of calling DebugStringImpl
  // directly. If some other textual information (e.g. brackets) need to be
  // added to the output after the type that is pushed into the stack gets
  // processed, this information can be pushed into the stack before this type.
  // Stack elements will eventually be appended to debug_string.
  virtual void DebugStringImpl(bool details, TypeOrStringVector* stack,
                               std::string* debug_string) const = 0;

  // Checks whether type has field of given name. Is called from HasField.
  virtual HasFieldResult HasFieldImpl(const std::string& name, int* field_id,
                                      bool include_pseudo_fields) const {
    return HAS_NO_FIELD;
  }

  // Make TypeFactory and TypeStore friend classes to provide an access to
  // type_store_ field.
  friend class TypeFactory;
  friend class internal::TypeStore;

  friend class ArrayType;
  friend class StructType;

  const internal::TypeStore* type_store_;  // Used for lifetime checking only.
  const TypeKind kind_;
};

typedef Type::FileDescriptorSetMap FileDescriptorSetMap;
typedef Type::BuildFileDescriptorSetMapOptions BuildFileDescriptorMapOptions;

#ifndef SWIG
// Provides equality comparison operator for Types.  This primarily invokes
// Type::Equals().
struct TypeEquals {
 public:
  bool operator()(const Type* const type1,
                  const Type* const type2) const;
};
#endif  // SWIG

typedef std::pair<TypeKind, TypeKind> TypeKindPair;

// Returns true if this is a valid TypeKind. This is stronger than the proto
// provided TypeKind_IsValid(), as it also returns false for the dummy
// __TypeKind__switch_must_have_a_default__ value.
bool IsValidTypeKind(int kind);

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_TYPE_H_
