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

#include "zetasql/public/types/proto_type.h"

#include "zetasql/public/language_options.h"
#include "zetasql/public/proto/wire_format_annotation.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/types/internal_utils.h"
#include "zetasql/public/types/type_factory.h"

namespace zetasql {

ProtoType::ProtoType(const TypeFactory* factory,
                     const google::protobuf::Descriptor* descriptor)
    : Type(factory, TYPE_PROTO), descriptor_(descriptor) {
  CHECK(descriptor_ != nullptr);
}

ProtoType::~ProtoType() {
}

bool ProtoType::SupportsOrdering(const LanguageOptions& language_options,
                                 std::string* type_description) const {
  if (type_description != nullptr) {
    *type_description = TypeKindToString(this->kind(),
                                         language_options.product_mode());
  }
  return false;
}

const google::protobuf::Descriptor* ProtoType::descriptor() const {
  return descriptor_;
}

absl::Status ProtoType::SerializeToProtoAndDistinctFileDescriptorsImpl(
    TypeProto* type_proto,
    absl::optional<int64_t> file_descriptor_sets_max_size_bytes,
    FileDescriptorSetMap* file_descriptor_set_map) const {
  type_proto->set_type_kind(kind_);
  ProtoTypeProto* proto_type_proto = type_proto->mutable_proto_type();
  proto_type_proto->set_proto_name(descriptor_->full_name());
  proto_type_proto->set_proto_file_name(descriptor_->file()->name());
  // Note that right now we are not supporting extensions to TypeProto, so
  // we do not need to look for all extensions of this proto.  Therefore the
  // FileDescriptorSet can be derived from the descriptor's FileDescriptor
  // dependencies.
  int set_index;
  ZETASQL_RETURN_IF_ERROR(internal::PopulateDistinctFileDescriptorSets(
      descriptor_->file(), file_descriptor_sets_max_size_bytes,
      file_descriptor_set_map, &set_index));
  if (set_index != 0) {
    proto_type_proto->set_file_descriptor_set_index(set_index);
  }
  return absl::OkStatus();
}

absl::Status ProtoType::GetFieldTypeByTagNumber(int number,
                                                TypeFactory* factory,
                                                bool use_obsolete_timestamp,
                                                const Type** type,
                                                std::string* name) const {
  const google::protobuf::FieldDescriptor* field_descr =
      descriptor_->FindFieldByNumber(number);
  if (field_descr == nullptr) {
    return MakeSqlError()
           << "Field number " << number << " not found in descriptor "
           << descriptor_->full_name();
  }
  if (name != nullptr) {
    *name = field_descr->name();
  }
  return factory->GetProtoFieldType(field_descr, use_obsolete_timestamp, type);
}

absl::Status ProtoType::GetFieldTypeByName(const std::string& name,
                                           TypeFactory* factory,
                                           bool use_obsolete_timestamp,
                                           const Type** type,
                                           int* number) const {
  const google::protobuf::FieldDescriptor* field_descr =
      descriptor_->FindFieldByName(name);
  if (field_descr == nullptr) {
    return MakeSqlError()
           << "Field name " << name << " not found in descriptor "
           << descriptor_->full_name();
  }
  if (number != nullptr) {
    *number = field_descr->number();
  }
  return factory->GetProtoFieldType(field_descr, use_obsolete_timestamp, type);
}

std::string ProtoType::TypeName() const {
  return ToIdentifierLiteral(descriptor_->full_name());
}

std::string ProtoType::ShortTypeName(ProductMode mode_unused) const {
  return descriptor_->full_name();
}

std::string ProtoType::TypeName(ProductMode mode_unused) const {
  return TypeName();
}

// static
absl::Status ProtoType::GetTypeKindFromFieldDescriptor(
    const google::protobuf::FieldDescriptor* field, bool ignore_format_annotations,
    bool use_obsolete_timestamp, TypeKind* kind) {
  const google::protobuf::FieldDescriptor::Type field_type = field->type();
  if (!ignore_format_annotations) {
    ZETASQL_RETURN_IF_ERROR(ProtoType::ValidateTypeAnnotations(field));
  }
  const FieldFormat::Format format =
      ignore_format_annotations ? FieldFormat::DEFAULT_FORMAT
                                : ProtoType::GetFormatAnnotation(field);
  switch (field_type) {
    case google::protobuf::FieldDescriptor::TYPE_INT32:
    case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
    case google::protobuf::FieldDescriptor::TYPE_SINT32: {
      switch (format) {
        case FieldFormat::DEFAULT_FORMAT:
          *kind = TYPE_INT32;
          break;
        case FieldFormat::DATE:
        case FieldFormat::DATE_DECIMAL:
          *kind = TYPE_DATE;
          break;
        default:
          // Should not reach this if ValidateTypeAnnotations() is working
          // properly.
          return MakeSqlError()
                 << "Proto " << field->containing_type()->full_name()
                 << " has invalid zetasql.format for INT32 field: "
                 << field->DebugString();
      }
      break;
    }
    case google::protobuf::FieldDescriptor::TYPE_INT64:
    case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
    case google::protobuf::FieldDescriptor::TYPE_SINT64: {
      switch (format) {
        case FieldFormat::DEFAULT_FORMAT:
          *kind = TYPE_INT64;
          break;
        case FieldFormat::DATE:
        case FieldFormat::DATE_DECIMAL:
          *kind = TYPE_DATE;
          break;
        case FieldFormat::TIMESTAMP_SECONDS:
          if (use_obsolete_timestamp) {
          } else {
            *kind = TYPE_TIMESTAMP;
          }
          break;
        case FieldFormat::TIMESTAMP_MILLIS:
          if (use_obsolete_timestamp) {
          } else {
            *kind = TYPE_TIMESTAMP;
          }
          break;
        case FieldFormat::TIMESTAMP_MICROS:
          if (use_obsolete_timestamp) {
          } else {
            *kind = TYPE_TIMESTAMP;
          }
          break;
        case FieldFormat::TIMESTAMP_NANOS:
          *kind = TYPE_TIMESTAMP;
          break;
        case FieldFormat::TIME_MICROS:
          *kind = TYPE_TIME;
          break;
        case FieldFormat::DATETIME_MICROS:
          *kind = TYPE_DATETIME;
          break;
        default:
          // Should not reach this if ValidateTypeAnnotations() is working
          // properly.
          return MakeSqlError()
                 << "Proto " << field->containing_type()->full_name()
                 << " has invalid zetasql.format for INT64 field: "
                 << field->DebugString();
      }
      break;
    }
    case google::protobuf::FieldDescriptor::TYPE_FIXED32:
    case google::protobuf::FieldDescriptor::TYPE_UINT32:
      *kind = TYPE_UINT32;
      break;
    case google::protobuf::FieldDescriptor::TYPE_FIXED64:
      *kind = TYPE_UINT64;
      break;
    case google::protobuf::FieldDescriptor::TYPE_UINT64: {
      switch (format) {
        case FieldFormat::DEFAULT_FORMAT:
          *kind = TYPE_UINT64;
          break;
        case FieldFormat::TIMESTAMP_MICROS:
          if (use_obsolete_timestamp) {
          } else {
            *kind = TYPE_TIMESTAMP;
          }
          break;
        default:
          // Should not reach this if ValidateTypeAnnotations() is working
          // properly.
          return MakeSqlError()
                 << "Proto " << field->containing_type()->full_name()
                 << " has invalid zetasql.format for UINT64 field: "
                 << field->DebugString();
      }
      break;
    }
    case google::protobuf::FieldDescriptor::TYPE_BYTES: {
      switch (format) {
        case FieldFormat::DEFAULT_FORMAT:
          *kind = TYPE_BYTES;
          break;
        case FieldFormat::ST_GEOGRAPHY_ENCODED:
          *kind = TYPE_GEOGRAPHY;
          break;
        case FieldFormat::NUMERIC:
          *kind = TYPE_NUMERIC;
          break;
        case FieldFormat::BIGNUMERIC:
          *kind = TYPE_BIGNUMERIC;
          break;
        default:
          // Should not reach this if ValidateTypeAnnotations() is working
          // properly.
          return MakeSqlError()
                 << "Proto " << field->containing_type()->full_name()
                 << " has invalid zetasql.format for BYTES field: "
                 << field->DebugString();
      }
      break;
    }
    case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
      *kind = TYPE_DOUBLE;
      break;
    case google::protobuf::FieldDescriptor::TYPE_FLOAT:
      *kind = TYPE_FLOAT;
      break;
    case google::protobuf::FieldDescriptor::TYPE_BOOL:
      *kind = TYPE_BOOL;
      break;
    case google::protobuf::FieldDescriptor::TYPE_ENUM:
      *kind = TYPE_ENUM;
      break;
    case google::protobuf::FieldDescriptor::TYPE_STRING:
      *kind = TYPE_STRING;
      break;
    case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
      if (ignore_format_annotations) {
        *kind = TYPE_PROTO;
        break;
      }
      ABSL_FALLTHROUGH_INTENDED;
    case google::protobuf::FieldDescriptor::TYPE_GROUP:
      // NOTE: We are not unwrapping is_wrapper or is_struct here.
      // Use MakeUnwrappedTypeFromProto to get an unwrapped type.
      *kind = TYPE_PROTO;
      break;
    default:
      return MakeSqlError() << "Invalid protocol buffer Type: " << field_type;
  }
  return absl::OkStatus();
}

absl::Status ProtoType::FieldDescriptorToTypeKind(
    bool ignore_annotations, const google::protobuf::FieldDescriptor* field,
    TypeKind* kind) {
  if (field->label() == google::protobuf::FieldDescriptor::LABEL_REPEATED) {
    *kind = TYPE_ARRAY;
  } else {
    ZETASQL_RETURN_IF_ERROR(
        FieldDescriptorToTypeKindBase(ignore_annotations, field, kind));
  }
  return absl::OkStatus();
}

absl::Status ProtoType::FieldDescriptorToTypeKind(
    const google::protobuf::FieldDescriptor* field, bool use_obsolete_timestamp,
    TypeKind* kind) {
  if (field->label() == google::protobuf::FieldDescriptor::LABEL_REPEATED) {
    *kind = TYPE_ARRAY;
  } else {
    ZETASQL_RETURN_IF_ERROR(
        FieldDescriptorToTypeKindBase(field, use_obsolete_timestamp, kind));
  }
  return absl::OkStatus();
}

const google::protobuf::FieldDescriptor* ProtoType::FindFieldByNameIgnoreCase(
    const google::protobuf::Descriptor* descriptor, const std::string& name) {
  // Try the fast way first.
  const google::protobuf::FieldDescriptor* found = descriptor->FindFieldByName(name);
  if (found != nullptr) return found;

  // We don't bother looking for multiple names that match.
  // Protos with duplicate names (case insensitively) do not compile in
  // c++ or java, so we don't worry about them.
  for (int i = 0; i < descriptor->field_count(); ++i) {
    const google::protobuf::FieldDescriptor* field = descriptor->field(i);
    if (zetasql_base::StringCaseEqual(field->name(), name)) {
      return field;
    }
  }
  return nullptr;
}

bool ProtoType::HasFormatAnnotation(
    const google::protobuf::FieldDescriptor* field) {
  return GetFormatAnnotationImpl(field) != FieldFormat::DEFAULT_FORMAT;
}

FieldFormat::Format ProtoType::GetFormatAnnotationImpl(
    const google::protobuf::FieldDescriptor* field) {
  // Read the format encoding, or if it doesn't exist, the type encoding.
  if (field->options().HasExtension(zetasql::format)) {
    return field->options().GetExtension(zetasql::format);
  } else if (field->options().HasExtension(zetasql::type)) {
    return field->options().GetExtension(zetasql::type);
  } else {
    return FieldFormat::DEFAULT_FORMAT;
  }
}

FieldFormat::Format ProtoType::GetFormatAnnotation(
    const google::protobuf::FieldDescriptor* field) {
  // Read the format (or deprecated type) encoding.
  const FieldFormat::Format format = GetFormatAnnotationImpl(field);

  const DeprecatedEncoding::Encoding encoding =
      field->options().GetExtension(zetasql::encoding);
  // If we also have a (valid) deprecated encoding annotation, merge that over
  // top of the type encoding.  Ignore any invalid encoding annotation.
  // This exists for backward compatibility with existing .proto files only.
  if (encoding == DeprecatedEncoding::DATE_DECIMAL &&
      format == FieldFormat::DATE) {
    return FieldFormat::DATE_DECIMAL;
  }
  return format;
}

bool ProtoType::GetUseDefaultsExtension(
    const google::protobuf::FieldDescriptor* field) {
  if (field->options().HasExtension(zetasql::use_defaults)) {
    // If the field has a use_defaults extension, use that.
    return field->options().GetExtension(zetasql::use_defaults);
  } else {
    // Otherwise, use its message's use_field_defaults extension
    // (which defaults to true if not explicitly set).
    const google::protobuf::Descriptor* parent = field->containing_type();
    return parent->options().GetExtension(zetasql::use_field_defaults);
  }
}

bool ProtoType::GetIsWrapperAnnotation(const google::protobuf::Descriptor* message) {
  return message->options().GetExtension(zetasql::is_wrapper);
}

bool ProtoType::GetIsStructAnnotation(const google::protobuf::Descriptor* message) {
  return message->options().GetExtension(zetasql::is_struct);
}

bool ProtoType::HasStructFieldName(const google::protobuf::FieldDescriptor* field) {
  return field->options().HasExtension(zetasql::struct_field_name);
}

const std::string& ProtoType::GetStructFieldName(
    const google::protobuf::FieldDescriptor* field) {
  return field->options().GetExtension(zetasql::struct_field_name);
}

bool ProtoType::EqualsImpl(const ProtoType* const type1,
                           const ProtoType* const type2, bool equivalent) {
  if (type1->descriptor() == type2->descriptor()) {
    return true;
  }
  if (equivalent &&
      type1->descriptor()->full_name() == type2->descriptor()->full_name()) {
    return true;
  }
  return false;
}

}  // namespace zetasql
