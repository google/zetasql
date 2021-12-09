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

#include "zetasql/public/types/type_deserializer.h"

#include <string>
#include <type_traits>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/proto_helper.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/array_type.h"
#include "zetasql/public/types/enum_type.h"
#include "zetasql/public/types/extended_type.h"
#include "zetasql/public/types/proto_type.h"
#include "zetasql/public/types/struct_type.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

namespace {

absl::Status ValidateTypeProto(const TypeProto& type_proto) {
  if (!type_proto.has_type_kind() ||
      (type_proto.type_kind() == TYPE_ARRAY) != type_proto.has_array_type() ||
      (type_proto.type_kind() == TYPE_ENUM) != type_proto.has_enum_type() ||
      (type_proto.type_kind() == TYPE_PROTO) != type_proto.has_proto_type() ||
      (type_proto.type_kind() == TYPE_STRUCT) != type_proto.has_struct_type() ||
      type_proto.type_kind() == __TypeKind__switch_must_have_a_default__) {
    if (type_proto.type_kind() != TYPE_GEOGRAPHY) {
      return MakeSqlError()
             << "Invalid TypeProto provided for deserialization: "
             << type_proto.DebugString();
    }
  }

  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<const Type*> TypeDeserializer::Deserialize(
    const TypeProto& type_proto) const {
  ZETASQL_RET_CHECK_NE(type_factory_, nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateTypeProto(type_proto));

  if (Type::IsSimpleType(type_proto.type_kind())) {
    return type_factory_->MakeSimpleType(type_proto.type_kind());
  }

  switch (type_proto.type_kind()) {
    case TYPE_ARRAY: {
      ZETASQL_ASSIGN_OR_RETURN(const Type* element_type,
                       Deserialize(type_proto.array_type().element_type()));
      const ArrayType* array_type;
      ZETASQL_RETURN_IF_ERROR(type_factory_->MakeArrayType(element_type, &array_type));
      return array_type;
    }

    case TYPE_STRUCT: {
      std::vector<StructType::StructField> fields;
      const StructType* struct_type;
      for (int idx = 0; idx < type_proto.struct_type().field_size(); ++idx) {
        const StructFieldProto& field_proto =
            type_proto.struct_type().field(idx);
        ZETASQL_ASSIGN_OR_RETURN(const Type* field_type,
                         Deserialize(field_proto.field_type()));
        StructType::StructField struct_field(field_proto.field_name(),
                                             field_type);
        fields.push_back(struct_field);
      }
      ZETASQL_RETURN_IF_ERROR(type_factory_->MakeStructType(fields, &struct_type));
      return struct_type;
    }

    case TYPE_ENUM: {
      const EnumType* enum_type;
      const int set_index = type_proto.enum_type().file_descriptor_set_index();
      if (set_index < 0 || set_index >= descriptor_pools().size()) {
        return MakeSqlError()
               << "Descriptor pool index " << set_index
               << " is out of range for the provided pools of size "
               << descriptor_pools().size();
      }
      const google::protobuf::DescriptorPool* pool = descriptor_pools()[set_index];
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
      const google::protobuf::RepeatedPtrField<std::string>& catalog_name_path =
          type_proto.enum_type().catalog_name_path();
      ZETASQL_RETURN_IF_ERROR(type_factory_->MakeEnumType(
          enum_descr, &enum_type,
          std::vector<std::string>{catalog_name_path.begin(),
                                   catalog_name_path.end()}));
      return enum_type;
    }

    case TYPE_PROTO: {
      const ProtoType* proto_type;
      const int set_index = type_proto.proto_type().file_descriptor_set_index();
      if (set_index < 0 || set_index >= descriptor_pools().size()) {
        return MakeSqlError()
               << "Descriptor pool index " << set_index
               << " is out of range for the provided pools of size "
               << descriptor_pools().size();
      }
      const google::protobuf::DescriptorPool* pool = descriptor_pools()[set_index];
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
      const google::protobuf::RepeatedPtrField<std::string>& catalog_name_path =
          type_proto.proto_type().catalog_name_path();
      ZETASQL_RETURN_IF_ERROR(type_factory_->MakeProtoType(
          proto_descr, &proto_type,
          std::vector<std::string>{catalog_name_path.begin(),
                                   catalog_name_path.end()}));
      return proto_type;
    }

    case TYPE_EXTENDED: {
      if (!extended_type_deserializer_) {
        return MakeSqlError()
               << "Extended type found in TypeProto, but extended type "
                  "deserializer is not set of TypeDeserializer";
      }

      return extended_type_deserializer_->Deserialize(type_proto, *this);
    }

    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Making Type of kind "
             << Type::TypeKindToString(type_proto.type_kind(), PRODUCT_INTERNAL)
             << " from TypeProto is not implemented.";
  }
}

absl::Status TypeDeserializer::DeserializeDescriptorPoolsFromSelfContainedProto(
    const TypeProto& type_proto,
    absl::Span<google::protobuf::DescriptorPool* const> pools) {
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
  return absl::OkStatus();
}

}  // namespace zetasql
