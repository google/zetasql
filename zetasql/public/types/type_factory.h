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

#ifndef ZETASQL_PUBLIC_TYPES_TYPE_FACTORY_H_
#define ZETASQL_PUBLIC_TYPES_TYPE_FACTORY_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "zetasql/public/annotation.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/annotation.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/enum_type.h"
#include "zetasql/public/types/extended_type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/simple_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "absl/base/attributes.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/container/node_hash_map.h"
#include "absl/flags/declare.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"

namespace zetasql {
class TypeFactory;
}  // namespace zetasql

ABSL_DECLARE_FLAG(int32_t, zetasql_type_factory_nesting_depth_limit);

namespace zetasql {

class ValueTest;

struct TypeFactoryOptions {
  // If this option is enabled, types allocated by a TypeFactory will not be
  // deleted until all Value instances that belong to types created by this
  // TypeFactory get released. Reference counting affects performance, since
  // each Value's set or clear operation comes with atomic
  // increment/decrement operation which could be expensive.
  // If performance is an issue, this option can be disabled: however, in this
  // case, the user needs to ensure that TypeFactory is released only after all
  // Value instances that reference its types are released too. This condition
  // is enforced in debug mode.
  // Caveat: under both cases user is responsible for making sure that the
  // DescriptorPool used to provide proto descriptors for proto and enum types
  // stays alive during the whole lifetime of Value objects belonging to these
  // types.
  bool keep_alive_while_referenced_from_value = true;

  // Disables keep_alive_while_referenced_from_value.
  TypeFactoryOptions& IgnoreValueLifeCycle() {
    keep_alive_while_referenced_from_value = false;
    return *this;
  }
};

namespace internal {  // For internal use only

// Class is used by TypeFactory to store created types.
// TODO: should we consider doing refcounting for Type obects
// instead of refcounting for TypeStores? This requires to add a counter and a
// flag per each non-simple type, but on the other hand, will allow us to have
// more granular memory management and avoid checks for cycles between
// TypeStores.
// TODO: Should TypeStore have a DescriptorPool?
class TypeStore {
 public:
#ifndef SWIG
  TypeStore(const TypeStore&) = delete;
  TypeStore& operator=(const TypeStore&) = delete;
#endif

 private:
  friend class zetasql::TypeFactory;
  friend class TypeStoreHelper;

  explicit TypeStore(bool keep_alive_while_referenced_from_value);
  ~TypeStore();

  void Ref() const;
  void Unref() const;

  // Use our own ref counter because zetasql_base::SimpleReferenceCounted uses int32_t for its
  // counter, which could be not enough to count references from all values.
  // Ref count is 1 for type factory that creates it.
  mutable std::atomic<int64_t> ref_count_{1};

  const bool keep_alive_while_referenced_from_value_;

  mutable absl::Mutex mutex_;

  std::vector<const Type*> owned_types_ ABSL_GUARDED_BY(mutex_);

  std::vector<const AnnotationMap*> owned_annotation_maps_
      ABSL_GUARDED_BY(mutex_);

  // Store links to and from TypeStores that this TypeStores depends on.
  // This is used as a sanity check to catch incorrect destruction order.
  mutable absl::flat_hash_set<const TypeStore*> depends_on_factories_
      ABSL_GUARDED_BY(mutex_);
  mutable absl::flat_hash_set<const TypeStore*> factories_depending_on_this_
      ABSL_GUARDED_BY(mutex_);
};

// Helper class to work with TypeStore. These internal helpers are usable only
// in the friend classes.
class TypeStoreHelper {
 private:
  friend class zetasql::Value;
  friend class zetasql::ValueTest;
  friend class zetasql::Type;

  static void RefFromValue(const TypeStore* store);
  static void UnrefFromValue(const TypeStore* store);
  static const TypeStore* GetTypeStore(const TypeFactory* factory);
  static int64_t Test_GetRefCount(const TypeStore* store);
};

// Chain of the catalog names that reference TypeProto or TypeEnum. Prepended to
// the type name.
struct CatalogName {
  absl::InlinedVector<std::string, 1> path;
  // Backticked path components.
  const std::string* path_string = nullptr;
};

}  // namespace internal

// A TypeFactory creates and owns Type objects.
//
// Created Type objects live until the TypeFactory is destroyed, with these
// exceptions:
//  * If keep_alive_while_referenced_from_value is true (the default), then
//  created Types will live longer than the TypeFactory as long as they are
//  referenced by Value objects.
//
// The TypeFactory may return the same Type object from multiple calls that
// request equivalent types.
//
// When a compound Type (array or struct) or an AnnotationMap is constructed
// referring to a Type from a separate TypeFactory, the constructed type may
// refer to the Type from the separate TypeFactory, so that TypeFactory must
// outlive this one.
//
// This class is thread-safe.
class TypeFactory {
 public:
  explicit TypeFactory(const TypeFactoryOptions& options);
  TypeFactory() : TypeFactory(TypeFactoryOptions{}) {}
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
  const Type* get_interval();
  const Type* get_geography();
  const Type* get_numeric();
  const Type* get_bignumeric();
  const Type* get_json();

  // Return a Type object for a simple type.  This works for all
  // non-parameterized scalar types.  Enums, arrays, structs and protos must
  // use the parameterized constructors.
  const Type* MakeSimpleType(TypeKind kind);

  // Make an array type.
  // Arrays of arrays are not supported and will fail with an error.
  // If <element_type> is not created by this TypeFactory, the TypeFactory that
  // created the <type> must outlive this TypeFactory.
  absl::Status MakeArrayType(const Type* element_type,
                             const ArrayType** result);
  absl::Status MakeArrayType(const Type* element_type,
                             const Type** result);

  // Make a struct type.
  // The field names must be valid.
  // If StructField.type is not created by this TypeFactory, the TypeFactory
  // that created the type must outlive this TypeFactory.
  absl::Status MakeStructType(absl::Span<const StructType::StructField> fields,
                              const StructType** result);
  absl::Status MakeStructType(absl::Span<const StructType::StructField> fields,
                              const Type** result);
  absl::Status MakeStructTypeFromVector(
      std::vector<StructType::StructField> fields, const StructType** result);
  absl::Status MakeStructTypeFromVector(
      std::vector<StructType::StructField> fields, const Type** result);

  // Make a proto type.
  // The <descriptor> must outlive this TypeFactory.
  // The <catalog_name> if provided is prepended to type's FullName.
  //
  // This always constructs a ProtoType, even for protos that are
  // annotated with zetasql.is_struct or zetasql.is_wrapper,
  // which normally indicate the proto should be interpreted as
  // a different type.  Use MakeUnwrappedTypeFromProto instead
  // to get the unwrapped type.
  absl::Status MakeProtoType(
      const google::protobuf::Descriptor* descriptor, const ProtoType** result,
      absl::Span<const std::string> catalog_name_path = {});
  absl::Status MakeProtoType(
      const google::protobuf::Descriptor* descriptor, const Type** result,
      absl::Span<const std::string> catalog_name_path = {});

  // Stores the unique copy of an ExtendedType in the TypeFactory. If such
  // extended type already exists in the cache, frees `extended_type` and
  // returns a pointer to existing type. Otherwise, returns a pointer to added
  // type. Type equality is checked with TypeEquals and TypeHash functions.
  // These rely on correct implementations of Type::HashTypeParameter and
  // Type::EqualsForSameKind on the ExtendedType.
  absl::StatusOr<const ExtendedType*> InternalizeExtendedType(
      std::unique_ptr<const ExtendedType> extended_type);

  // Make a zetasql type from a proto, honoring zetasql.is_struct and
  // zetasql.is_wrapper annotations.
  // These annotations allow creating a proto representation of any zetasql
  // type, including structs and arrays, with nullability.
  // Such protos can be created with methods in convert_type_to_proto.h.
  // This method converts protos back to the represented zetasql type.
  absl::Status MakeUnwrappedTypeFromProto(const google::protobuf::Descriptor* message,
                                          const Type** result_type) {
    return MakeUnwrappedTypeFromProto(message, /*use_obsolete_timestamp=*/false,
                                      result_type);
  }
  // DEPRECATED: Callers should remove their dependencies on obsolete types and
  // move to the method above.
  ABSL_DEPRECATED("Obsolete timestamp types are deprecated")
  absl::Status MakeUnwrappedTypeFromProto(const google::protobuf::Descriptor* message,
                                          bool use_obsolete_timestamp,
                                          const Type** result_type);

  // Like the method above, but starting from a zetasql::Type.
  // If the Type is not a proto, it will be returned unchanged.
  absl::Status UnwrapTypeIfAnnotatedProto(const Type* input_type,
                                          const Type** result_type) {
    return UnwrapTypeIfAnnotatedProto(
        input_type, /*use_obsolete_timestamp=*/false, result_type);
  }
  // DEPRECATED: Callers should remove their dependencies on obsolete types and
  // move to the method above.
  ABSL_DEPRECATED("Obsolete timestamp types are deprecated")
  absl::Status UnwrapTypeIfAnnotatedProto(const Type* input_type,
                                          bool use_obsolete_timestamp,
                                          const Type** result_type);

  // Make an enum type from a protocol buffer EnumDescriptor.
  // The <enum_descriptor> must outlive this TypeFactory.
  // The <catalog_name> if provided is prepended to type's FullName.
  absl::Status MakeEnumType(
      const google::protobuf::EnumDescriptor* enum_descriptor, const EnumType** result,
      absl::Span<const std::string> catalog_name_path = {});
  absl::Status MakeEnumType(
      const google::protobuf::EnumDescriptor* enum_descriptor, const Type** result,
      absl::Span<const std::string> catalog_name_path = {});

  // Get the Type for a proto field.
  // If <ignore_annotations> is false, this looks at format annotations on the
  // field and possibly its parent message to help select the Type. If
  // <ignore_annotations> is true, annotations on the field are not considered
  // and the returned type is that of which ZetaSQL sees before applying any
  // annotations or automatic conversions. This function always ignores (does
  // not unwrap) is_struct and is_wrapper annotations.
  absl::Status GetProtoFieldType(bool ignore_annotations,
                                 const google::protobuf::FieldDescriptor* field_descr,
                                 const Type** type);

  // Get the Type for a proto field.
  // This is the same as the above signature with ignore_annotations = false.
  //
  // NOTE: There is a similar method GetProtoFieldTypeAndDefault in proto_util.h
  // that also extracts the default value.
  absl::Status GetProtoFieldType(const google::protobuf::FieldDescriptor* field_descr,
                                 const Type** type) {
    return GetProtoFieldType(/*ignore_annotations=*/false, field_descr, type);
  }
  // DEPRECATED: Callers should remove their dependencies on obsolete types and
  // move to the method above.
  ABSL_DEPRECATED("Obsolete timestamp types are deprecated")
  absl::Status GetProtoFieldType(const google::protobuf::FieldDescriptor* field_descr,
                                 bool use_obsolete_timestamp,
                                 const Type** type);

  // Deserializes and creates an instance of AnnotationMap from <proto>.
  absl::Status DeserializeAnnotationMap(const AnnotationMapProto& proto,
                                        const AnnotationMap** annotation_map);

  // Takes ownership of <annotation_map> and returns a raw pointer owned by this
  // TypeFactory. The output pointer may be different from the input.
  absl::StatusOr<const AnnotationMap*> TakeOwnership(
      std::unique_ptr<AnnotationMap> annotation_map);

  // Makes a ZetaSQL Type from a self-contained ZetaSQL TypeProto.  The
  // <type_proto> FileDescriptorSets are loaded into the pool.  The <pool>
  // must outlive the TypeFactory.  Will return an error if the
  // FileDescriptorSets cannot be deserialized into a single DescriptorPool,
  // i.e. if type_proto.file_descriptor_set_size() > 1.  For serialized types
  // spanning multiple pools, see
  // DeserializeFromSelfContainedProtoWithDistinctFiles below.
  ABSL_DEPRECATED(
      "Use TypeDeserializer calling "
      "DeserializeFromSelfContainedProtoWithDistinctFiles to populate "
      "DesciptorPools and Deserialize for type deserialization")
  absl::Status DeserializeFromSelfContainedProto(
      const TypeProto& type_proto,
      google::protobuf::DescriptorPool* pool,
      const Type** type);

  // Similar to the above, but supports types referencing multiple
  // DescriptorPools.  The provided pools must match the number of
  // FileDescriptorSets stored in <type_proto>.  Each FileDescriptorSet from
  // <type_proto> is loaded into the DescriptorPool corresponding to its index.
  ABSL_DEPRECATED(
      "Use TypeDeserializer calling "
      "DeserializeFromSelfContainedProtoWithDistinctFiles to populate "
      "DesciptorPools and Deserialize for type deserialization")
  absl::Status DeserializeFromSelfContainedProtoWithDistinctFiles(
      const TypeProto& type_proto,
      const std::vector<google::protobuf::DescriptorPool*>& pools,
      const Type** type);

  // Make a ZetaSQL Type from a ZetaSQL TypeProto.  All protos referenced
  // by <type_proto> must already have related descriptors in the <pool>.
  // The <pool> must outlive the TypeFactory.  May only be used with a
  // <type_proto> serialized via Type::SerializeToProtoAndFileDescriptors.
  ABSL_DEPRECATED("Use TypeDeserializer instead")
  absl::Status DeserializeFromProtoUsingExistingPool(
      const TypeProto& type_proto,
      const google::protobuf::DescriptorPool* pool,
      const Type** type);

  // Similar to the above, but expects that all protos and enums referenced by
  // <type_proto> must have related descriptors in the pool corresponding to
  // the ProtoTypeProto or EnumTypeProto's file_descriptor_set_index. May be
  // used with a <type_proto> serialized via
  // Type::SerializeToProtoAndFileDescriptors or
  // Type::SerializeToProtoAndDistinctFileDescriptors.
  ABSL_DEPRECATED("Use TypeDeserializer instead")
  absl::Status DeserializeFromProtoUsingExistingPools(
      const TypeProto& type_proto,
      const std::vector<const google::protobuf::DescriptorPool*>& pools,
      const Type** type);

  // Maximum nesting depth for types supported by this TypeFactory. Any attempt
  // to create a type with a nesting_depth() greater than this will return an
  // error. If a limit is not set, the ZetaSQL analyzer may create types that
  // it cannot destruct. Use kint32max for no limit (the default).
  // The limit value must be >= 0. The default value of this field can be
  // overidden with FLAGS_zetasql_type_factory_nesting_depth_limit.
  int nesting_depth_limit() const ABSL_LOCKS_EXCLUDED(store_->mutex_);
  void set_nesting_depth_limit(int value) ABSL_LOCKS_EXCLUDED(store_->mutex_);

  // Estimate memory size allocated to store TypeFactory's data in bytes
  int64_t GetEstimatedOwnedMemoryBytesSize() const;

 private:
  // Add <type> into <owned_types_>.  Templated so it can return the
  // specific subclass of Type.
  template <class TYPE>
  const TYPE* TakeOwnership(const TYPE* type)
      ABSL_LOCKS_EXCLUDED(store_->mutex_);
  template <class TYPE>
  const TYPE* TakeOwnershipLocked(const TYPE* type)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(store_->mutex_);
  template <class TYPE>
  const TYPE* TakeOwnershipLocked(const TYPE* type,
                                  int64_t type_owned_bytes_size)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(store_->mutex_);

  // Takes ownership of <annotation_map> and updates estimated used memory.
  const AnnotationMap* TakeOwnershipInternal(
      const AnnotationMap* annotation_map);

  // Mark that <other_type>'s factory must outlive <this>.
  void AddDependency(const Type* other_type)
      ABSL_LOCKS_EXCLUDED(store_->mutex_);

  // Returns TypeProto or TypeEnum.
  template <typename Descriptor>
  const auto* MakeDescribedType(const Descriptor* descriptor,
                                absl::Span<const std::string> catalog_name_path)
      ABSL_LOCKS_EXCLUDED(store_->mutex_);

  template <typename Descriptor>
  const auto*& FindOrCreateCachedType(const Descriptor* descriptor,
                                      const internal::CatalogName* catalog)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(store_->mutex_);

  // Find or create cached catalog name.
  const internal::CatalogName* FindOrCreateCatalogName(
      absl::Span<const std::string> catalog_name_path)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(store_->mutex_);

  // Get the Type for a proto field from its corresponding TypeKind. For
  // repeated fields, <kind> must be the base TypeKind for the field (i.e., the
  // TypeKind of the field, ignoring repeatedness), which can be obtained by
  // FieldDescriptorToTypeKindBase().
  absl::Status GetProtoFieldTypeWithKind(
      const google::protobuf::FieldDescriptor* field_descr, TypeKind kind,
      const Type** type);

  // Implementation of MakeUnwrappedTypeFromProto above that detects invalid use
  // of type annotations with recursive protos by storing all visited message
  // types in 'ancestor_messages'.
  ABSL_DEPRECATED("Obsolete timestamp types are deprecated")
  absl::Status MakeUnwrappedTypeFromProtoImpl(
      const google::protobuf::Descriptor* message, const Type* existing_message_type,
      bool use_obsolete_timestamp, const Type** result_type,
      std::set<const google::protobuf::Descriptor*>* ancestor_messages);

  // Implementation of UnwrapTypeIfAnnotatedProto above that detects invalid use
  // of type annotations with recursive protos by storing all visited message
  // types in 'ancestor_messages'.
  ABSL_DEPRECATED("Obsolete timestamp types are deprecated")
  absl::Status UnwrapTypeIfAnnotatedProtoImpl(
      const Type* input_type, bool use_obsolete_timestamp,
      const Type** result_type,
      std::set<const google::protobuf::Descriptor*>* ancestor_messages);

  friend class internal::TypeStoreHelper;

  absl::flat_hash_map<const Type*, const ArrayType*> cached_array_types_
      ABSL_GUARDED_BY(store_->mutex_);
  absl::flat_hash_map<const google::protobuf::Descriptor*, const ProtoType*>
      cached_proto_types_ ABSL_GUARDED_BY(store_->mutex_);
  absl::flat_hash_map<const google::protobuf::EnumDescriptor*, const EnumType*>
      cached_enum_types_ ABSL_GUARDED_BY(store_->mutex_);

  // The key is a descriptor and a catalog name path.
  absl::flat_hash_map<
      std::pair<const google::protobuf::Descriptor*, const internal::CatalogName*>,
      const ProtoType*>
      cached_proto_types_with_catalog_name_ ABSL_GUARDED_BY(store_->mutex_);
  absl::flat_hash_map<
      std::pair<const google::protobuf::EnumDescriptor*, const internal::CatalogName*>,
      const EnumType*>
      cached_enum_types_with_catalog_name_ ABSL_GUARDED_BY(store_->mutex_);

  // The key is a catalog name path.
  absl::node_hash_map<std::string, internal::CatalogName> cached_catalog_names_
      ABSL_GUARDED_BY(store_->mutex_);

  // Cached extended types.
  TypeFlatHashSet<> cached_extended_types_ ABSL_GUARDED_BY(store_->mutex_);

  internal::TypeStore* store_;  // Stores created types.

  int nesting_depth_limit_ ABSL_GUARDED_BY(store_->mutex_);

  // Stores estimation of how much memory was allocated by instances
  // of types owned by this TypeFactory (in bytes)
  int64_t estimated_memory_used_by_types_;
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
const Type* IntervalType();
const Type* GeographyType();
const Type* NumericType();
const Type* BigNumericType();
const Type* JsonType();
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
const ArrayType* IntervalArrayType();
const ArrayType* GeographyArrayType();
const ArrayType* NumericArrayType();
const ArrayType* BigNumericArrayType();
const ArrayType* JsonArrayType();

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
}  // namespace types

}  // namespace zetasql

#endif  // ZETASQL_PUBLIC_TYPES_TYPE_FACTORY_H_
