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

#include "zetasql/public/types/type_factory.h"

#include "zetasql/common/proto_helper.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/functions/normalize_mode.pb.h"
#include "zetasql/public/proto/type_annotation.pb.h"
#include "zetasql/public/proto/wire_format_annotation.pb.h"
#include "zetasql/public/types/internal_utils.h"
#include "zetasql/public/types/type.h"
#include "zetasql/base/cleanup.h"
#include "absl/flags/flag.h"
#include "zetasql/base/map_util.h"

ABSL_FLAG(int32_t, zetasql_type_factory_nesting_depth_limit,
          std::numeric_limits<int32_t>::max(),
          "The maximum nesting depth for types that zetasql::TypeFactory "
          "will allow to be created. Set this to a bounded value to avoid "
          "stack overflows.");

namespace zetasql {

namespace internal {

TypeStore::TypeStore(bool keep_alive_while_referenced_from_value)
    : keep_alive_while_referenced_from_value_(
          keep_alive_while_referenced_from_value) {}

TypeStore::~TypeStore() {
  // Need to delete these in a loop because the destructor is only visible
  // via friend declaration on Type.
  for (const Type* type : owned_types_) {
    delete type;
  }

  if (!factories_depending_on_this_.empty()) {
    LOG(DFATAL) << "Destructing TypeFactory " << this
                << " is unsafe because TypeFactory "
                << *factories_depending_on_this_.begin()
                << " depends on it staying alive.\n"
                << "Using --vmodule=type=2 may aid debugging.\n"
                ;
    // Avoid crashing on the TypeFactory dependency reference itself.
    for (const TypeStore* other : factories_depending_on_this_) {
      absl::MutexLock l(&other->mutex_);
      other->depends_on_factories_.erase(this);
    }
  }

  for (const TypeStore* other : depends_on_factories_) {
    bool need_to_unref = false;
    {
      absl::MutexLock l(&other->mutex_);
      if (other->factories_depending_on_this_.erase(this) != 0) {
        need_to_unref = other->keep_alive_while_referenced_from_value_;
      }
    }
    if (need_to_unref) {
      other->Unref();
    }
  }
}

void TypeStore::Ref() const {
  ref_count_.fetch_add(1, std::memory_order_relaxed);
}

void TypeStore::Unref() const {
  if (ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    delete this;
  }
}

void TypeStoreHelper::RefFromValue(const TypeStore* store) {
  DCHECK(store);

  // We still do TypeStore reference counting in debug mode regardless of
  // whether keep_alive_while_referenced_from_value_ is true or not: this is
  // done to check that no values that reference types from this TypeStore are
  // alive when TypeFactore gets released. In release mode (NDEBUG), we do
  // refcounting only if keep_alive_while_referenced_from_value_ is true.
#ifdef NDEBUG
  if (!store->keep_alive_while_referenced_from_value_) return;
#endif

  store->Ref();
}

void TypeStoreHelper::UnrefFromValue(const TypeStore* store) {
  DCHECK(store);

#ifdef NDEBUG
  if (!store->keep_alive_while_referenced_from_value_) return;
#endif

  store->Unref();
}

const TypeStore* TypeStoreHelper::GetTypeStore(const TypeFactory* factory) {
  DCHECK(factory);
  return factory->store_;
}

int64_t TypeStoreHelper::Test_GetRefCount(const TypeStore* store) {
  DCHECK(store);
  return store->ref_count_.load(std::memory_order_seq_cst);
}

}  // namespace internal

TypeFactory::TypeFactory(const TypeFactoryOptions& options)
    : store_(new internal::TypeStore(
          options.keep_alive_while_referenced_from_value)),
      nesting_depth_limit_(
          absl::GetFlag(FLAGS_zetasql_type_factory_nesting_depth_limit)),
      estimated_memory_used_by_types_(0) {
  VLOG(2) << "Created TypeFactory " << store_ << ":\n"
          ;
}

TypeFactory::~TypeFactory() {
#ifndef NDEBUG
  // In debug mode, we check that there shouldn't be any values that reference
  // types from this TypeFactory.
  if (!store_->keep_alive_while_referenced_from_value_ &&
      store_->ref_count_.load(std::memory_order_seq_cst) != 1) {
    LOG(DFATAL)
        << "Type factory is released while there are still some objects "
           "that reference it";
  }
#endif

  store_->Unref();
}

int TypeFactory::nesting_depth_limit() const {
  absl::MutexLock l(&store_->mutex_);
  return nesting_depth_limit_;
}

void TypeFactory::set_nesting_depth_limit(int value) {
  // We don't want to have to check the depth for simple types, so a depth of
  // 0 must be allowed.
  DCHECK_GE(value, 0);
  absl::MutexLock l(&store_->mutex_);
  nesting_depth_limit_ = value;
}

int64_t TypeFactory::GetEstimatedOwnedMemoryBytesSize() const {
  // While we don't promise exact size (only estimation), we still lock a
  // mutex here in case we may need protection from side effects of multi
  // threaded accesses during concurrent unit tests. Also, function
  // GetExternallyAllocatedMemoryEstimate doesn't declare thread safety (even
  // though current implementation is safe).
  absl::MutexLock l(&store_->mutex_);
  return sizeof(*this) + sizeof(internal::TypeStore) +
         estimated_memory_used_by_types_ +
         internal::GetExternallyAllocatedMemoryEstimate(store_->owned_types_) +
         internal::GetExternallyAllocatedMemoryEstimate(
             store_->depends_on_factories_) +
         internal::GetExternallyAllocatedMemoryEstimate(
             store_->factories_depending_on_this_) +
         internal::GetExternallyAllocatedMemoryEstimate(cached_array_types_) +
         internal::GetExternallyAllocatedMemoryEstimate(cached_proto_types_) +
         internal::GetExternallyAllocatedMemoryEstimate(cached_enum_types_);
}

template <class TYPE>
const TYPE* TypeFactory::TakeOwnership(const TYPE* type) {
  const int64_t type_owned_bytes_size = type->GetEstimatedOwnedMemoryBytesSize();
  absl::MutexLock l(&store_->mutex_);
  return TakeOwnershipLocked(type, type_owned_bytes_size);
}

template <class TYPE>
const TYPE* TypeFactory::TakeOwnershipLocked(const TYPE* type) {
  return TakeOwnershipLocked(type, type->GetEstimatedOwnedMemoryBytesSize());
}

template <class TYPE>
const TYPE* TypeFactory::TakeOwnershipLocked(const TYPE* type,
                                             int64_t type_owned_bytes_size) {
  DCHECK_EQ(type->type_store_, store_);
  DCHECK_GT(type_owned_bytes_size, 0);
  store_->owned_types_.push_back(type);
  estimated_memory_used_by_types_ += type_owned_bytes_size;
  return type;
}

const Type* TypeFactory::get_int32() { return types::Int32Type(); }
const Type* TypeFactory::get_int64() { return types::Int64Type(); }
const Type* TypeFactory::get_uint32() { return types::Uint32Type(); }
const Type* TypeFactory::get_uint64() { return types::Uint64Type(); }
const Type* TypeFactory::get_string() { return types::StringType(); }
const Type* TypeFactory::get_bytes() { return types::BytesType(); }
const Type* TypeFactory::get_bool() { return types::BoolType(); }
const Type* TypeFactory::get_float() { return types::FloatType(); }
const Type* TypeFactory::get_double() { return types::DoubleType(); }
const Type* TypeFactory::get_date() { return types::DateType(); }
const Type* TypeFactory::get_timestamp() { return types::TimestampType(); }
const Type* TypeFactory::get_time() { return types::TimeType(); }
const Type* TypeFactory::get_datetime() { return types::DatetimeType(); }
const Type* TypeFactory::get_geography() { return types::GeographyType(); }
const Type* TypeFactory::get_numeric() { return types::NumericType(); }
const Type* TypeFactory::get_bignumeric() { return types::BigNumericType(); }
const Type* TypeFactory::get_json() { return types::JsonType(); }

const Type* TypeFactory::MakeSimpleType(TypeKind kind) {
  CHECK(Type::IsSimpleType(kind)) << kind;
  const Type* type = types::TypeFromSimpleTypeKind(kind);
  CHECK(type != nullptr);
  return type;
}

absl::Status TypeFactory::MakeArrayType(
    const Type* element_type, const ArrayType** result) {
  *result = nullptr;
  AddDependency(element_type);
  if (element_type->IsArray()) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Array of array types are not supported";
  } else {
    const int depth_limit = nesting_depth_limit();
    if (element_type->nesting_depth() + 1 > depth_limit) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Array type would exceed nesting depth limit of "
             << depth_limit;
    }
    absl::MutexLock lock(&store_->mutex_);
    auto& cached_result = cached_array_types_[element_type];
    if (cached_result == nullptr) {
      cached_result = TakeOwnershipLocked(new ArrayType(this, element_type));
    }
    *result = cached_result;
    return absl::OkStatus();
  }
}

absl::Status TypeFactory::MakeArrayType(
    const Type* element_type, const Type** result) {
  return MakeArrayType(element_type,
                       reinterpret_cast<const ArrayType**>(result));
}

absl::Status TypeFactory::MakeStructType(
    absl::Span<const StructType::StructField> fields,
    const StructType** result) {
  std::vector<StructType::StructField> new_fields(fields.begin(), fields.end());
  return MakeStructTypeFromVector(std::move(new_fields), result);
}

absl::Status TypeFactory::MakeStructType(
    absl::Span<const StructType::StructField> fields, const Type** result) {
  return MakeStructType(fields, reinterpret_cast<const StructType**>(result));
}

absl::Status TypeFactory::MakeStructTypeFromVector(
    std::vector<StructType::StructField> fields, const StructType** result) {
  *result = nullptr;
  const int depth_limit = nesting_depth_limit();
  int max_nesting_depth = 0;
  for (const StructType::StructField& field : fields) {
    const int nesting_depth = field.type->nesting_depth();
    max_nesting_depth = std::max(max_nesting_depth, nesting_depth);
    if (ABSL_PREDICT_FALSE(nesting_depth + 1 > depth_limit)) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Struct type would exceed nesting depth limit of "
             << depth_limit;
    }
    AddDependency(field.type);
  }
  // We calculate <max_nesting_depth> in the previous loop. We also need to
  // increment it to take into account the struct itself.
  *result = TakeOwnership(
      new StructType(this, std::move(fields), max_nesting_depth + 1));
  return absl::OkStatus();
}

absl::Status TypeFactory::MakeStructTypeFromVector(
    std::vector<StructType::StructField> fields, const Type** result) {
  return MakeStructTypeFromVector(std::move(fields),
                                  reinterpret_cast<const StructType**>(result));
}

absl::Status TypeFactory::MakeProtoType(
    const google::protobuf::Descriptor* descriptor, const ProtoType** result) {
  absl::MutexLock lock(&store_->mutex_);
  auto& cached_result = cached_proto_types_[descriptor];
  if (cached_result == nullptr) {
    cached_result = TakeOwnershipLocked(new ProtoType(this, descriptor));
  }
  *result = cached_result;
  return absl::OkStatus();
}

absl::Status TypeFactory::MakeProtoType(
    const google::protobuf::Descriptor* descriptor, const Type** result) {
  return MakeProtoType(descriptor, reinterpret_cast<const ProtoType**>(result));
}

absl::Status TypeFactory::MakeEnumType(
    const google::protobuf::EnumDescriptor* enum_descriptor, const EnumType** result) {
  absl::MutexLock lock(&store_->mutex_);
  auto& cached_result = cached_enum_types_[enum_descriptor];
  if (cached_result == nullptr) {
    cached_result = TakeOwnershipLocked(new EnumType(this, enum_descriptor));
  }
  *result = cached_result;
  return absl::OkStatus();
}

absl::Status TypeFactory::MakeEnumType(
    const google::protobuf::EnumDescriptor* enum_descriptor, const Type** result) {
  return MakeEnumType(enum_descriptor,
                      reinterpret_cast<const EnumType**>(result));
}

absl::Status TypeFactory::MakeUnwrappedTypeFromProto(
    const google::protobuf::Descriptor* message, bool use_obsolete_timestamp,
    const Type** result_type) {
  std::set<const google::protobuf::Descriptor*> ancestor_messages;
  return MakeUnwrappedTypeFromProtoImpl(
      message, nullptr /* existing_message_type */, use_obsolete_timestamp,
      result_type, &ancestor_messages);
}

absl::Status TypeFactory::UnwrapTypeIfAnnotatedProto(
    const Type* input_type, bool use_obsolete_timestamp,
    const Type** result_type) {
  std::set<const google::protobuf::Descriptor*> ancestor_messages;
  return UnwrapTypeIfAnnotatedProtoImpl(input_type, use_obsolete_timestamp,
                                        result_type, &ancestor_messages);
}

absl::Status TypeFactory::UnwrapTypeIfAnnotatedProtoImpl(
    const Type* input_type, bool use_obsolete_timestamp,
    const Type** result_type,
    std::set<const google::protobuf::Descriptor*>* ancestor_messages) {
  if (input_type->IsArray()) {
    // For Arrays, unwrap the element type inside the array.
    const ArrayType* array_type = input_type->AsArray();
    const Type* element_type = array_type->element_type();
    const Type* unwrapped_element_type;
    // If this is an array<proto>, unwrap the proto element if necessary.
    if (element_type->IsProto()) {
      ZETASQL_RETURN_IF_ERROR(MakeUnwrappedTypeFromProtoImpl(
          element_type->AsProto()->descriptor(), element_type,
          use_obsolete_timestamp, &unwrapped_element_type, ancestor_messages));
      ZETASQL_RETURN_IF_ERROR(MakeArrayType(unwrapped_element_type, &array_type));
    }
    *result_type = array_type;
    return absl::OkStatus();
  } else if (input_type->IsProto()) {
    return MakeUnwrappedTypeFromProtoImpl(input_type->AsProto()->descriptor(),
                                          input_type, use_obsolete_timestamp,
                                          result_type, ancestor_messages);
  } else {
    *result_type = input_type;
    return absl::OkStatus();
  }
}

absl::Status TypeFactory::MakeUnwrappedTypeFromProtoImpl(
    const google::protobuf::Descriptor* message, const Type* existing_message_type,
    bool use_obsolete_timestamp, const Type** result_type,
    std::set<const google::protobuf::Descriptor*>* ancestor_messages) {
  if (!ancestor_messages->insert(message).second) {
    return ::zetasql_base::InvalidArgumentErrorBuilder()
           << "Invalid proto " << message->full_name()
           << " has type annotations but is recursive";
  }
  // Always erase 'message' before returning so 'ancestor_messages' contains
  // only ancestors of the current message being unwrapped.
  auto cleanup = ::zetasql_base::MakeCleanup(
      [message, ancestor_messages] { ancestor_messages->erase(message); });
  absl::Status return_status;
  if (ProtoType::GetIsWrapperAnnotation(message)) {
    // If we have zetasql.is_wrapper, unwrap the proto and return the type
    // of the contained field.
    if (message->field_count() != 1) {
      return ::zetasql_base::InvalidArgumentErrorBuilder()
             << "Proto " << message->full_name()
             << " is invalid because it has zetasql.is_wrapper annotation"
                " but does not have exactly one field";
    }
    const google::protobuf::FieldDescriptor* proto_field = message->field(0);
    const Type* field_type;
    ZETASQL_RETURN_IF_ERROR(
        GetProtoFieldType(proto_field, use_obsolete_timestamp, &field_type));
    ZETASQL_RET_CHECK_EQ(field_type->IsArray(), proto_field->is_repeated());
    if (!proto_field->options().GetExtension(zetasql::is_raw_proto)) {
      return_status = UnwrapTypeIfAnnotatedProtoImpl(
          field_type, use_obsolete_timestamp, result_type, ancestor_messages);
    } else {
      *result_type = field_type;
    }
  } else if (ProtoType::GetIsStructAnnotation(message)) {
    // If we have zetasql.is_struct, convert this proto to a struct type.
    std::vector<StructType::StructField> struct_fields;
    for (int i = 0; i < message->field_count(); ++i) {
      const google::protobuf::FieldDescriptor* proto_field = message->field(i);
      const Type* field_type;
      ZETASQL_RETURN_IF_ERROR(
          GetProtoFieldType(proto_field, use_obsolete_timestamp, &field_type));
      if (!proto_field->options().GetExtension(zetasql::is_raw_proto)) {
        const Type* unwrapped_field_type;
        ZETASQL_RETURN_IF_ERROR(UnwrapTypeIfAnnotatedProtoImpl(
            field_type, use_obsolete_timestamp, &unwrapped_field_type,
            ancestor_messages));
        field_type = unwrapped_field_type;
      }

      std::string name = proto_field->name();
      if (ProtoType::HasStructFieldName(proto_field)) {
        name = ProtoType::GetStructFieldName(proto_field);
      }

      struct_fields.emplace_back(name, field_type);
    }
    return_status = MakeStructType(struct_fields, result_type);
  } else if (existing_message_type != nullptr) {
    // Use the message_type we already have allocated.
    DCHECK(existing_message_type->IsProto());
    DCHECK_EQ(message->full_name(),
              existing_message_type->AsProto()->descriptor()->full_name());
    *result_type = existing_message_type;
    return_status = absl::OkStatus();
  } else {
    return_status = MakeProtoType(message, result_type);
  }
  return return_status;
}

absl::Status TypeFactory::GetProtoFieldTypeWithKind(
    const google::protobuf::FieldDescriptor* field_descr, TypeKind kind,
    const Type** type) {
  if (Type::IsSimpleType(kind)) {
    *type = MakeSimpleType(kind);
  } else if (kind == TYPE_ENUM) {
    const EnumType* enum_type;
    ZETASQL_RETURN_IF_ERROR(MakeEnumType(field_descr->enum_type(), &enum_type));
    *type = enum_type;
  } else if (kind == TYPE_PROTO) {
    ZETASQL_RETURN_IF_ERROR(MakeProtoType(field_descr->message_type(), type));
  } else {
    return ::zetasql_base::UnimplementedErrorBuilder()
           << "Unsupported type found: "
           << Type::TypeKindToString(kind, PRODUCT_INTERNAL);
  }
  if (field_descr->is_repeated()) {
    const ArrayType* array_type;
    ZETASQL_RETURN_IF_ERROR(MakeArrayType(*type, &array_type));
    *type = array_type;
  }

  return absl::OkStatus();
}

absl::Status TypeFactory::GetProtoFieldType(
    bool ignore_annotations, const google::protobuf::FieldDescriptor* field_descr,
    const Type** type) {
  TypeKind kind;
  ZETASQL_RETURN_IF_ERROR(ProtoType::FieldDescriptorToTypeKindBase(ignore_annotations,
                                                           field_descr, &kind));
  ZETASQL_RETURN_IF_ERROR(GetProtoFieldTypeWithKind(field_descr, kind, type));
  if (ZETASQL_DEBUG_MODE) {
    // For testing, make sure the TypeKinds we get from
    // FieldDescriptorToTypeKind match the Types returned by this method.
    TypeKind computed_type_kind;
    ZETASQL_RETURN_IF_ERROR(ProtoType::FieldDescriptorToTypeKind(
        ignore_annotations, field_descr, &computed_type_kind));
    ZETASQL_RET_CHECK_EQ((*type)->kind(), computed_type_kind)
        << (*type)->DebugString() << "\n"
        << field_descr->DebugString();
  }
  return absl::OkStatus();
}

absl::Status TypeFactory::GetProtoFieldType(
    const google::protobuf::FieldDescriptor* field_descr, bool use_obsolete_timestamp,
    const Type** type) {
  TypeKind kind;
  ZETASQL_RETURN_IF_ERROR(ProtoType::FieldDescriptorToTypeKindBase(
      field_descr, use_obsolete_timestamp, &kind));

  ZETASQL_RETURN_IF_ERROR(GetProtoFieldTypeWithKind(field_descr, kind, type));
  if (ZETASQL_DEBUG_MODE) {
    // For testing, make sure the TypeKinds we get from
    // FieldDescriptorToTypeKind match the Types returned by this method.
    TypeKind computed_type_kind;
    ZETASQL_RETURN_IF_ERROR(ProtoType::FieldDescriptorToTypeKind(
        field_descr, use_obsolete_timestamp, &computed_type_kind));
    ZETASQL_RET_CHECK_EQ((*type)->kind(), computed_type_kind)
        << (*type)->DebugString() << "\n" << field_descr->DebugString();
  }

  return absl::OkStatus();
}

absl::Status TypeFactory::DeserializeFromProtoUsingExistingPool(
    const TypeProto& type_proto,
    const google::protobuf::DescriptorPool* pool,
    const Type** type) {
  return DeserializeFromProtoUsingExistingPools(type_proto, {pool}, type);
}

absl::Status TypeFactory::DeserializeFromProtoUsingExistingPools(
    const TypeProto& type_proto,
    const std::vector<const google::protobuf::DescriptorPool*>& pools,
    const Type** type) {
  *type = nullptr;
  if (!type_proto.has_type_kind() ||
      (type_proto.type_kind() == TYPE_ARRAY) != type_proto.has_array_type() ||
      (type_proto.type_kind() == TYPE_ENUM) != type_proto.has_enum_type() ||
      (type_proto.type_kind() == TYPE_PROTO) != type_proto.has_proto_type() ||
      (type_proto.type_kind() == TYPE_STRUCT) != type_proto.has_struct_type() ||
      type_proto.type_kind() == __TypeKind__switch_must_have_a_default__) {
    // TODO: Temporary hack to allow deserialization of existing
    // zetasql::TypeProto with TYPE_GEOGRAPHY which have proto_type for
    // stlib.proto.Geography in them.
    if (type_proto.type_kind() != TYPE_GEOGRAPHY) {
      return MakeSqlError()
             << "Invalid TypeProto provided for deserialization: "
             << type_proto.DebugString();
    }
  }
  if (Type::IsSimpleType(type_proto.type_kind())) {
    *type = MakeSimpleType(type_proto.type_kind());
    return absl::OkStatus();
  }
  switch (type_proto.type_kind()) {
    case TYPE_ARRAY: {
      const Type* element_type;
      const ArrayType* array_type;
      ZETASQL_RETURN_IF_ERROR(DeserializeFromProtoUsingExistingPools(
          type_proto.array_type().element_type(), pools, &element_type));
      ZETASQL_RETURN_IF_ERROR(MakeArrayType(element_type, &array_type));
      *type = array_type;
    } break;
    case TYPE_STRUCT: {
      std::vector<StructType::StructField> fields;
      const StructType* struct_type;
      for (int idx = 0; idx < type_proto.struct_type().field_size(); ++idx) {
        const StructFieldProto& field_proto =
            type_proto.struct_type().field(idx);
        const Type* field_type;
        ZETASQL_RETURN_IF_ERROR(DeserializeFromProtoUsingExistingPools(
            field_proto.field_type(), pools, &field_type));
        StructType::StructField struct_field(field_proto.field_name(),
                                             field_type);
        fields.push_back(struct_field);
      }
      ZETASQL_RETURN_IF_ERROR(MakeStructType(fields, &struct_type));
      *type = struct_type;
    } break;
    case TYPE_ENUM: {
      const EnumType* enum_type;
      const int set_index = type_proto.enum_type().file_descriptor_set_index();
      if (set_index < 0 || set_index >= pools.size()) {
        return MakeSqlError()
               << "Descriptor pool index " << set_index
               << " is out of range for the provided pools of size "
               << pools.size();
      }
      const google::protobuf::DescriptorPool* pool = pools[set_index];
      const google::protobuf::EnumDescriptor* enum_descr =
          pool->FindEnumTypeByName(type_proto.enum_type().enum_name());
      if (enum_descr == nullptr) {
        return MakeSqlError()
               << "Enum type name not found in the specified DescriptorPool: "
               << type_proto.enum_type().enum_name();
      }
      if (enum_descr->file()->name() !=
          type_proto.enum_type().enum_file_name()) {
        return MakeSqlError()
               << "Enum " << type_proto.enum_type().enum_name() << " found in "
               << enum_descr->file()->name() << ", not "
               << type_proto.enum_type().enum_file_name() << " as specified.";
      }
      ZETASQL_RETURN_IF_ERROR(MakeEnumType(enum_descr, &enum_type));
      *type = enum_type;
    } break;
    case TYPE_PROTO: {
      const ProtoType* proto_type;
      const int set_index = type_proto.proto_type().file_descriptor_set_index();
      if (set_index < 0 || set_index >= pools.size()) {
        return MakeSqlError()
               << "Descriptor pool index " << set_index
               << " is out of range for the provided pools of size "
               << pools.size();
      }
      const google::protobuf::DescriptorPool* pool = pools[set_index];
      const google::protobuf::Descriptor* proto_descr =
          pool->FindMessageTypeByName(type_proto.proto_type().proto_name());
      if (proto_descr == nullptr) {
        return MakeSqlError()
               << "Proto type name not found in the specified DescriptorPool: "
               << type_proto.proto_type().proto_name();
      }
      if (proto_descr->file()->name() !=
          type_proto.proto_type().proto_file_name()) {
        return MakeSqlError()
               << "Proto " << type_proto.proto_type().proto_name()
               << " found in " << proto_descr->file()->name() << ", not "
               << type_proto.proto_type().proto_file_name() << " as specified.";
      }
      ZETASQL_RETURN_IF_ERROR(MakeProtoType(proto_descr, &proto_type));
      *type = proto_type;
    } break;
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Making Type of kind "
             << Type::TypeKindToString(type_proto.type_kind(), PRODUCT_INTERNAL)
             << " from TypeProto is not implemented.";
  }
  return absl::OkStatus();
}

absl::Status TypeFactory::DeserializeFromSelfContainedProto(
      const TypeProto& type_proto,
      google::protobuf::DescriptorPool* pool,
      const Type** type) {
  if (type_proto.file_descriptor_set_size() > 1) {
    return MakeSqlError()
           << "DeserializeFromSelfContainedProto cannot be used to deserialize "
              "types that rely on multiple FileDescriptorSets. Use "
              "DeserializeFromSelfContainedProtoWithDistinctFiles instead";
  }
  return DeserializeFromSelfContainedProtoWithDistinctFiles(type_proto, {pool},
                                                            type);
}

absl::Status TypeFactory::DeserializeFromSelfContainedProtoWithDistinctFiles(
      const TypeProto& type_proto,
      const std::vector<google::protobuf::DescriptorPool*>& pools,
      const Type** type) {
  if (!type_proto.file_descriptor_set().empty() &&
      type_proto.file_descriptor_set_size() != pools.size()) {
    return MakeSqlError()
           << "Expected the number of provided FileDescriptorSets "
              "and DescriptorPools to match. Found "
           << type_proto.file_descriptor_set_size()
           << " FileDescriptorSets and " << pools.size() << " DescriptorPools";
  }
  for (int i = 0; i < type_proto.file_descriptor_set_size(); ++i) {
    ZETASQL_RETURN_IF_ERROR(AddFileDescriptorSetToPool(
        &type_proto.file_descriptor_set(i), pools[i]));
  }
  const std::vector<const google::protobuf::DescriptorPool*> const_pools(pools.begin(),
                                                          pools.end());
  return DeserializeFromProtoUsingExistingPools(type_proto, const_pools, type);
}

bool IsValidTypeKind(int kind) {
  return TypeKind_IsValid(kind) &&
      kind != __TypeKind__switch_must_have_a_default__;
}

namespace {

// Staticly initialize a few commonly used types.
static TypeFactory* s_type_factory() {
  static TypeFactory* s_type_factory = new TypeFactory();
  return s_type_factory;
}

static const Type* s_int32_type() {
  static const Type* s_int32_type =
      new SimpleType(s_type_factory(), TYPE_INT32);
  return s_int32_type;
}

static const Type* s_int64_type() {
  static const Type* s_int64_type =
      new SimpleType(s_type_factory(), TYPE_INT64);
  return s_int64_type;
}

static const Type* s_uint32_type() {
  static const Type* s_uint32_type =
      new SimpleType(s_type_factory(), TYPE_UINT32);
  return s_uint32_type;
}

static const Type* s_uint64_type() {
  static const Type* s_uint64_type =
      new SimpleType(s_type_factory(), TYPE_UINT64);
  return s_uint64_type;
}

static const Type* s_bool_type() {
  static const Type* s_bool_type = new SimpleType(s_type_factory(), TYPE_BOOL);
  return s_bool_type;
}

static const Type* s_float_type() {
  static const Type* s_float_type =
      new SimpleType(s_type_factory(), TYPE_FLOAT);
  return s_float_type;
}

static const Type* s_double_type() {
  static const Type* s_double_type =
      new SimpleType(s_type_factory(), TYPE_DOUBLE);
  return s_double_type;
}

static const Type* s_string_type() {
  static const Type* s_string_type =
      new SimpleType(s_type_factory(), TYPE_STRING);
  return s_string_type;
}

static const Type* s_bytes_type() {
  static const Type* s_bytes_type =
      new SimpleType(s_type_factory(), TYPE_BYTES);
  return s_bytes_type;
}

static const Type* s_timestamp_type() {
  static const Type* s_timestamp_type =
      new SimpleType(s_type_factory(), TYPE_TIMESTAMP);
  return s_timestamp_type;
}

static const Type* s_date_type() {
  static const Type* s_date_type = new SimpleType(s_type_factory(), TYPE_DATE);
  return s_date_type;
}

static const Type* s_time_type() {
  static const Type* s_time_type = new SimpleType(s_type_factory(), TYPE_TIME);
  return s_time_type;
}

static const Type* s_datetime_type() {
  static const Type* s_datetime_type =
      new SimpleType(s_type_factory(), TYPE_DATETIME);
  return s_datetime_type;
}

static const Type* s_geography_type() {
  static const Type* s_geography_type =
      new SimpleType(s_type_factory(), TYPE_GEOGRAPHY);
  return s_geography_type;
}

static const Type* s_numeric_type() {
  static const Type* s_numeric_type =
      new SimpleType(s_type_factory(), TYPE_NUMERIC);
  return s_numeric_type;
}

static const Type* s_bignumeric_type() {
  static const Type* s_bignumeric_type =
      new SimpleType(s_type_factory(), TYPE_BIGNUMERIC);
  return s_bignumeric_type;
}

static const Type* s_json_type() {
  static const Type* s_json_type =
      new SimpleType(s_type_factory(), TYPE_JSON);
  return s_json_type;
}

static const EnumType* s_date_part_enum_type() {
  static const EnumType* s_date_part_enum_type = [] {
    const EnumType* enum_type;
    ZETASQL_CHECK_OK(s_type_factory()->MakeEnumType(
        functions::DateTimestampPart_descriptor(), &enum_type));
    return enum_type;
  }();
  return s_date_part_enum_type;
}

static const EnumType* s_normalize_mode_enum_type() {
  static const EnumType* s_normalize_mode_enum_type = [] {
    const EnumType* enum_type;
    ZETASQL_CHECK_OK(s_type_factory()->MakeEnumType(
        functions::NormalizeMode_descriptor(), &enum_type));
    return enum_type;
  }();
  return s_normalize_mode_enum_type;
}

static const StructType* s_empty_struct_type() {
  static const StructType* s_empty_struct_type = [] {
    const StructType* type;
    ZETASQL_CHECK_OK(s_type_factory()->MakeStructType({}, &type));
    return type;
  }();
  return s_empty_struct_type;
}

static const ArrayType* MakeArrayType(const Type* element_type) {
  const ArrayType* array_type;
  ZETASQL_CHECK_OK(s_type_factory()->MakeArrayType(element_type, &array_type));
  return array_type;
}

static const ArrayType* s_int32_array_type() {
  static const ArrayType* s_int32_array_type =
      MakeArrayType(s_type_factory()->get_int32());
  return s_int32_array_type;
}

static const ArrayType* s_int64_array_type() {
  static const ArrayType* s_int64_array_type =
      MakeArrayType(s_type_factory()->get_int64());
  return s_int64_array_type;
}

static const ArrayType* s_uint32_array_type() {
  static const ArrayType* s_uint32_array_type =
      MakeArrayType(s_type_factory()->get_uint32());
  return s_uint32_array_type;
}

static const ArrayType* s_uint64_array_type() {
  static const ArrayType* s_uint64_array_type =
      MakeArrayType(s_type_factory()->get_uint64());
  return s_uint64_array_type;
}

static const ArrayType* s_bool_array_type() {
  static const ArrayType* s_bool_array_type =
      MakeArrayType(s_type_factory()->get_bool());
  return s_bool_array_type;
}

static const ArrayType* s_float_array_type() {
  static const ArrayType* s_float_array_type =
      MakeArrayType(s_type_factory()->get_float());
  return s_float_array_type;
}

static const ArrayType* s_double_array_type() {
  static const ArrayType* s_double_array_type =
      MakeArrayType(s_type_factory()->get_double());
  return s_double_array_type;
}

static const ArrayType* s_string_array_type() {
  static const ArrayType* s_string_array_type =
      MakeArrayType(s_type_factory()->get_string());
  return s_string_array_type;
}

static const ArrayType* s_bytes_array_type() {
  static const ArrayType* s_bytes_array_type =
      MakeArrayType(s_type_factory()->get_bytes());
  return s_bytes_array_type;
}

static const ArrayType* s_timestamp_array_type() {
  static const ArrayType* s_timestamp_array_type =
      MakeArrayType(s_type_factory()->get_timestamp());
  return s_timestamp_array_type;
}

static const ArrayType* s_date_array_type() {
  static const ArrayType* s_date_array_type =
      MakeArrayType(s_type_factory()->get_date());
  return s_date_array_type;
}

static const ArrayType* s_datetime_array_type() {
  static const ArrayType* s_datetime_array_type =
      MakeArrayType(s_type_factory()->get_datetime());
  return s_datetime_array_type;
}

static const ArrayType* s_time_array_type() {
  static const ArrayType* s_time_array_type =
      MakeArrayType(s_type_factory()->get_time());
  return s_time_array_type;
}

static const ArrayType* s_geography_array_type() {
  static const ArrayType* s_geography_array_type =
      MakeArrayType(s_type_factory()->get_geography());
  return s_geography_array_type;
}

static const ArrayType* s_numeric_array_type() {
  static const ArrayType* s_numeric_array_type =
      MakeArrayType(s_type_factory()->get_numeric());
  return s_numeric_array_type;
}

static const ArrayType* s_bignumeric_array_type() {
  static const ArrayType* s_bignumeric_array_type =
      MakeArrayType(s_type_factory()->get_bignumeric());
  return s_bignumeric_array_type;
}

static const ArrayType* s_json_array_type() {
  static const ArrayType* s_json_array_type =
      MakeArrayType(s_type_factory()->get_json());
  return s_json_array_type;
}

}  // namespace

namespace types {

const Type* Int32Type() { return s_int32_type(); }
const Type* Int64Type() { return s_int64_type(); }
const Type* Uint32Type() { return s_uint32_type(); }
const Type* Uint64Type() { return s_uint64_type(); }
const Type* BoolType() { return s_bool_type(); }
const Type* FloatType() { return s_float_type(); }
const Type* DoubleType() { return s_double_type(); }
const Type* StringType() { return s_string_type(); }
const Type* BytesType() { return s_bytes_type(); }
const Type* DateType() { return s_date_type(); }
const Type* TimestampType() { return s_timestamp_type(); }
const Type* TimeType() { return s_time_type(); }
const Type* DatetimeType() { return s_datetime_type(); }
const Type* GeographyType() { return s_geography_type(); }
const Type* NumericType() { return s_numeric_type(); }
const Type* BigNumericType() { return s_bignumeric_type(); }
const Type* JsonType() { return s_json_type(); }
const StructType* EmptyStructType() { return s_empty_struct_type(); }
const EnumType* DatePartEnumType() { return s_date_part_enum_type(); }
const EnumType* NormalizeModeEnumType() { return s_normalize_mode_enum_type(); }

const ArrayType* Int32ArrayType() { return s_int32_array_type(); }
const ArrayType* Int64ArrayType() { return s_int64_array_type(); }
const ArrayType* Uint32ArrayType() { return s_uint32_array_type(); }
const ArrayType* Uint64ArrayType() { return s_uint64_array_type(); }
const ArrayType* BoolArrayType() { return s_bool_array_type(); }
const ArrayType* FloatArrayType() { return s_float_array_type(); }
const ArrayType* DoubleArrayType() { return s_double_array_type(); }
const ArrayType* StringArrayType() { return s_string_array_type(); }
const ArrayType* BytesArrayType() { return s_bytes_array_type(); }

const ArrayType* TimestampArrayType() { return s_timestamp_array_type(); }
const ArrayType* DateArrayType() { return s_date_array_type(); }

const ArrayType* DatetimeArrayType() { return s_datetime_array_type(); }

const ArrayType* TimeArrayType() { return s_time_array_type(); }

const ArrayType* GeographyArrayType() { return s_geography_array_type(); }

const ArrayType* NumericArrayType() { return s_numeric_array_type(); }

const ArrayType* BigNumericArrayType() { return s_bignumeric_array_type(); }

const ArrayType* JsonArrayType() { return s_json_array_type(); }

const Type* TypeFromSimpleTypeKind(TypeKind type_kind) {
  switch (type_kind) {
    case TYPE_INT32:
      return Int32Type();
    case TYPE_INT64:
      return Int64Type();
    case TYPE_UINT32:
      return Uint32Type();
    case TYPE_UINT64:
      return Uint64Type();
    case TYPE_BOOL:
      return BoolType();
    case TYPE_FLOAT:
      return FloatType();
    case TYPE_DOUBLE:
      return DoubleType();
    case TYPE_STRING:
      return StringType();
    case TYPE_BYTES:
      return BytesType();
    case TYPE_TIMESTAMP:
      return TimestampType();
    case TYPE_DATE:
      return DateType();
    case TYPE_TIME:
      return TimeType();
    case TYPE_DATETIME:
      return DatetimeType();
    case TYPE_GEOGRAPHY:
      return GeographyType();
    case TYPE_NUMERIC:
      return NumericType();
    case TYPE_BIGNUMERIC:
      return BigNumericType();
    case TYPE_JSON:
      return JsonType();
    default:
      VLOG(1) << "Could not build static Type from type: "
              << Type::TypeKindToString(type_kind, PRODUCT_INTERNAL);
      return nullptr;
  }
}

const ArrayType* ArrayTypeFromSimpleTypeKind(TypeKind type_kind) {
  switch (type_kind) {
    case TYPE_INT32:
      return Int32ArrayType();
    case TYPE_INT64:
      return Int64ArrayType();
    case TYPE_UINT32:
      return Uint32ArrayType();
    case TYPE_UINT64:
      return Uint64ArrayType();
    case TYPE_BOOL:
      return BoolArrayType();
    case TYPE_FLOAT:
      return FloatArrayType();
    case TYPE_DOUBLE:
      return DoubleArrayType();
    case TYPE_STRING:
      return StringArrayType();
    case TYPE_BYTES:
      return BytesArrayType();
    case TYPE_TIMESTAMP:
      return TimestampArrayType();
    case TYPE_DATE:
      return DateArrayType();
    case TYPE_TIME:
      return TimeArrayType();
    case TYPE_DATETIME:
      return DatetimeArrayType();
    case TYPE_GEOGRAPHY:
      return GeographyArrayType();
    case TYPE_NUMERIC:
      return NumericArrayType();
    case TYPE_BIGNUMERIC:
      return BigNumericArrayType();
    case TYPE_JSON:
      return JsonArrayType();
    default:
      VLOG(1) << "Could not build static ArrayType from type: "
              << Type::TypeKindToString(type_kind, PRODUCT_INTERNAL);
      return nullptr;
  }
}

}  // namespace types

void TypeFactory::AddDependency(const Type* other_type) {
  const internal::TypeStore* other_store = other_type->type_store_;

  // Do not add a dependency if the other factory is the same as this factory or
  // is the static factory (since the static factory is never destroyed).
  if (other_store == store_ || other_store == s_type_factory()->store_) return;

  {
    absl::MutexLock l(&store_->mutex_);
    if (!zetasql_base::InsertIfNotPresent(&store_->depends_on_factories_, other_store)) {
      return;  // Already had it.
    }
    VLOG(2) << "Added dependency from TypeFactory " << this << " to "
            << other_store << " which owns the type "
            << other_type->DebugString() << ":\n"
            ;

    // This detects trivial cycles between two TypeFactories.  It won't detect
    // longer cycles, so those won't give this error message, but the
    // destructor error will still fire because no destruction order is safe.
    if (zetasql_base::ContainsKey(store_->factories_depending_on_this_, other_store)) {
      LOG(DFATAL) << "Created cyclical dependency between TypeFactories, "
                     "which is not legal because there can be no safe "
                     "destruction order";
    }
  }
  {
    absl::MutexLock l(&other_store->mutex_);
    if (zetasql_base::InsertIfNotPresent(&other_store->factories_depending_on_this_,
                                store_)) {
      if (other_store->keep_alive_while_referenced_from_value_) {
        other_store->Ref();
      }
    }
  }
}

}  // namespace zetasql
