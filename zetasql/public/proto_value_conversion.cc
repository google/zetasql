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

#include "zetasql/public/proto_value_conversion.h"

#include <cstdint>
#include <string>
#include <vector>

#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/json_value.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/proto/type_annotation.pb.h"
#include "zetasql/public/proto/wire_format_annotation.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "zetasql/public/value.pb.h"
#include <cstdint>
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

// Returns true in 'is_wrapper_out' if 'field' should be treated as a wrapper
// for the given 'type'.
//
// Note that this function does not actually check that the type of the
// wrapped field is compatible with 'type'.  It primarily serves to check
// that the descriptor declares itself with an is_wrapper annotation, and uses
// 'type' only to gracefully handle a few tricky edge cases:
//   1) ZetaSQL users are allowed to use the is_wrapper annotation on their
//      own protos.  If a user puts such a proto into a STRUCT, we don't want to
//      mistakenly treat it as its own wrapper.  Therefore, we return error
//      if 'field' and 'type' have the same proto message name and that proto
//      declares itself to be a wrapper.
//   2) When type->IsArray(), we should only return true if 'field' has a
//      descriptor that is a wrapper for the array.  We will return false if
//      'field' is a repeated field of element wrappers.
static absl::Status ShouldTreatAsWrapperForType(
    const google::protobuf::FieldDescriptor* field,
    const Type* type,
    bool* is_wrapper_out) {
  if (field->type() != google::protobuf::FieldDescriptor::TYPE_MESSAGE ||
      field->options().GetExtension(zetasql::is_raw_proto)) {
    *is_wrapper_out = false;
    return absl::OkStatus();
  }

  if (type->IsProto()) {
    const ProtoType* proto_type = type->AsProto();

    ZETASQL_RET_CHECK(field->message_type() != nullptr) << field->DebugString();
    *is_wrapper_out = ProtoType::GetIsWrapperAnnotation(field->message_type());
    if (*is_wrapper_out) {
      // As mentioned in point 1 in the function comment, we do not allow
      // serialization of values containing proto types that declare themselves
      // to be wrappers.  So if this is a wrapper type, make sure that
      // the proto names are different.
      ZETASQL_RET_CHECK_NE(field->message_type()->full_name(),
                   proto_type->descriptor()->full_name());
    }
    return absl::OkStatus();
  }

  if (type->IsArray()) {
    // For ARRAYs, we determine whether this is a wrapper based on whether
    // or not the field is repeated.  Wrapped arrays show up as optional fields,
    // and unwrapped arrays show up as repeated fields.
    if (field->is_repeated()) {
      // We cannot look at the is_wrapper annotation because we would be
      // looking at whether the array element is a wrapper.
      *is_wrapper_out = false;
      return absl::OkStatus();
    }
    // When an ARRAY is represented as a non-repeated field, it must be
    // a wrapper.
    ZETASQL_RET_CHECK(ProtoType::GetIsWrapperAnnotation(field->message_type()))
        << field->DebugString();
    *is_wrapper_out = true;
    return absl::OkStatus();
  }

  *is_wrapper_out = ProtoType::GetIsWrapperAnnotation(field->message_type());
  return absl::OkStatus();
}

// Fills 'proto_out' with the data in 'value'.  'value' must be non-NULL
// and must have a STRUCT type.  'proto_out' must have a descriptor
// corresponding to that STRUCT type.
//
// This function is mutually recursive with MergeValueToProtoField.
static absl::Status StructValueToProto(const Value& value,
                                       bool use_wire_format_annotations,
                                       google::protobuf::MessageFactory* message_factory,
                                       google::protobuf::Message* proto_out) {
  ZETASQL_RET_CHECK(value.is_valid());
  ZETASQL_RET_CHECK(!value.is_null());
  const StructType* struct_type = value.type()->AsStruct();
  ZETASQL_RET_CHECK(struct_type != nullptr) << value.DebugString();
  ZETASQL_RET_CHECK_EQ(struct_type->num_fields(), value.num_fields());
  for (int i = 0; i < struct_type->num_fields(); ++i) {
    const Value& field_value = value.field(i);
    const google::protobuf::FieldDescriptor* field =
        proto_out->GetDescriptor()->FindFieldByNumber(i + 1);
    ZETASQL_RET_CHECK(field != nullptr) << i;
    ZETASQL_RETURN_IF_ERROR(MergeValueToProtoField(field_value, field,
                                           use_wire_format_annotations,
                                           message_factory, proto_out));
  }

  return absl::OkStatus();
}

// Checks that the 'field_format' is appropriate for the type of the 'value'.
// RET_CHECKs if the format is invalid.
static absl::Status CheckFieldFormat(const Value& value,
                                     FieldFormat::Format field_format) {
  // First get the leaf type (unwrap the arrays).
  const Type* leaf_type = value.type();
  while (leaf_type->IsArray()) {
    leaf_type = leaf_type->AsArray()->element_type();
  }

  switch (leaf_type->kind()) {
    // TODO: Check non-default formats as well.
    case TYPE_DATE:
    case TYPE_TIMESTAMP:
    case TYPE_TIME:
    case TYPE_DATETIME:
    case TYPE_INTERVAL:
    case TYPE_GEOGRAPHY:
    case TYPE_NUMERIC:
    case TYPE_BIGNUMERIC:
    case TYPE_JSON:
      break;

    default:
      ZETASQL_RET_CHECK_EQ(FieldFormat::DEFAULT_FORMAT, field_format)
          << "Format " << field_format << " not supported for zetasql type "
          << leaf_type->DebugString();
  }

  return absl::OkStatus();
}

// Helper function that converts the timestamp field format annotation to the
// timestamp scale enum. RET_CHECKs in case of unexpected input.
static absl::StatusOr<functions::TimestampScale> FormatToScale(
    FieldFormat::Format field_format) {
  switch (field_format) {
    case FieldFormat::TIMESTAMP_MICROS:
      return functions::kMicroseconds;
    case FieldFormat::TIMESTAMP_MILLIS:
      return functions::kMilliseconds;
    case FieldFormat::TIMESTAMP_SECONDS:
      return functions::kSeconds;
    case FieldFormat::TIMESTAMP_NANOS:
      return functions::kNanoseconds;
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unexpected FieldFormat "
                       << FieldFormat::Format_Name(field_format)
                       << " for TIMESTAMP";
  }
}

absl::Status MergeValueToProtoField(const Value& value,
                                    const google::protobuf::FieldDescriptor* field,
                                    bool use_wire_format_annotations,
                                    google::protobuf::MessageFactory* message_factory,
                                    google::protobuf::Message* proto_out) {
  ZETASQL_RET_CHECK(value.is_valid());
  ZETASQL_RET_CHECK(field != nullptr);
  ZETASQL_RET_CHECK(message_factory != nullptr);
  ZETASQL_RET_CHECK(proto_out != nullptr);
  ZETASQL_RET_CHECK_EQ(field->containing_type(), proto_out->GetDescriptor())
      << "Field and output proto descriptors do not match";
  const google::protobuf::Reflection* reflection = proto_out->GetReflection();

  const FieldFormat::Format field_format =
      ProtoType::GetFormatAnnotation(field);
  ZETASQL_RETURN_IF_ERROR(CheckFieldFormat(value, field_format));

  bool is_wrapper = use_wire_format_annotations;
  if (use_wire_format_annotations) {
    ZETASQL_RETURN_IF_ERROR(
        ShouldTreatAsWrapperForType(field, value.type(), &is_wrapper));
  }
  if (is_wrapper) {
    // Special case for handling NULL arrays.  NULL arrays are indicated
    // by the absence of a wrapper.  NULL non-arrays are indicated by the
    // absence of the field within the wrapper.
    if (value.type()->IsArray() && value.is_null()) {
      return absl::OkStatus();
    }
    google::protobuf::Message* wrapper = field->is_repeated() ?
        reflection->AddMessage(proto_out, field, message_factory) :
        reflection->MutableMessage(proto_out, field, message_factory);
    const google::protobuf::Descriptor* wrapper_descriptor = wrapper->GetDescriptor();
    ZETASQL_RET_CHECK_EQ(1, wrapper_descriptor->field_count());
    const google::protobuf::FieldDescriptor* unwrapped_field =
        wrapper_descriptor->field(0);
    return MergeValueToProtoField(value, unwrapped_field,
                                  use_wire_format_annotations, message_factory,
                                  wrapper);
  }

  if (value.is_null()) {
    // Since ZetaSQL does not support arrays of arrays, if we encounter a
    // NULL non-array value for a repeated field, we know we are attempting to
    // append a NULL element, which is not allowed.
    if (value.type_kind() != TYPE_ARRAY) {
      ZETASQL_RET_CHECK(!field->is_repeated())
          << "Cannot serialize a NULL array element into a proto that doesn't "
          << "have an array element wrapper";
    }
    return absl::OkStatus();
  }

  switch (value.type_kind()) {
    case TYPE_INT32:
      ZETASQL_RET_CHECK_EQ(field->cpp_type(), google::protobuf::FieldDescriptor::CPPTYPE_INT32);
      if (field->is_repeated()) {
        reflection->AddInt32(proto_out, field, value.int32_value());
      } else {
        reflection->SetInt32(proto_out, field, value.int32_value());
      }
      return absl::OkStatus();
    case TYPE_INT64:
      ZETASQL_RET_CHECK_EQ(field->cpp_type(), google::protobuf::FieldDescriptor::CPPTYPE_INT64);
      if (field->is_repeated()) {
        reflection->AddInt64(proto_out, field, value.int64_value());
      } else {
        reflection->SetInt64(proto_out, field, value.int64_value());
      }
      return absl::OkStatus();
    case TYPE_UINT32:
      ZETASQL_RET_CHECK_EQ(field->cpp_type(), google::protobuf::FieldDescriptor::CPPTYPE_UINT32);
      if (field->is_repeated()) {
        reflection->AddUInt32(proto_out, field, value.uint32_value());
      } else {
        reflection->SetUInt32(proto_out, field, value.uint32_value());
      }
      return absl::OkStatus();
    case TYPE_UINT64:
      ZETASQL_RET_CHECK_EQ(field->cpp_type(), google::protobuf::FieldDescriptor::CPPTYPE_UINT64);
      if (field->is_repeated()) {
        reflection->AddUInt64(proto_out, field, value.uint64_value());
      } else {
        reflection->SetUInt64(proto_out, field, value.uint64_value());
      }
      return absl::OkStatus();
    case TYPE_BOOL:
      ZETASQL_RET_CHECK_EQ(field->type(), google::protobuf::FieldDescriptor::TYPE_BOOL);
      if (field->is_repeated()) {
        reflection->AddBool(proto_out, field, value.bool_value());
      } else {
        reflection->SetBool(proto_out, field, value.bool_value());
      }
      return absl::OkStatus();
    case TYPE_FLOAT:
      ZETASQL_RET_CHECK_EQ(field->type(), google::protobuf::FieldDescriptor::TYPE_FLOAT);
      if (field->is_repeated()) {
        reflection->AddFloat(proto_out, field, value.float_value());
      } else {
        reflection->SetFloat(proto_out, field, value.float_value());
      }
      return absl::OkStatus();
    case TYPE_DOUBLE:
      ZETASQL_RET_CHECK_EQ(field->type(), google::protobuf::FieldDescriptor::TYPE_DOUBLE);
      if (field->is_repeated()) {
        reflection->AddDouble(proto_out, field, value.double_value());
      } else {
        reflection->SetDouble(proto_out, field, value.double_value());
      }
      return absl::OkStatus();
    case TYPE_STRING:
      ZETASQL_RET_CHECK_EQ(field->type(), google::protobuf::FieldDescriptor::TYPE_STRING);
      if (field->is_repeated()) {
        reflection->AddString(proto_out, field, value.string_value());
      } else {
        reflection->SetString(proto_out, field, value.string_value());
      }
      return absl::OkStatus();
    case TYPE_BYTES:
      ZETASQL_RET_CHECK_EQ(field->type(), google::protobuf::FieldDescriptor::TYPE_BYTES);
      if (field->is_repeated()) {
        reflection->AddString(proto_out, field, value.bytes_value());
      } else {
        reflection->SetString(proto_out, field, value.bytes_value());
      }
      return absl::OkStatus();
    case TYPE_DATE: {
      ZETASQL_RET_CHECK(field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT32 ||
                field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT64)
          << field->type_name();
      int32_t encoded_date_value = -1;
      ZETASQL_RETURN_IF_ERROR(functions::EncodeFormattedDate(
          value.date_value(), field_format, &encoded_date_value));
      if (field->type() == google::protobuf::FieldDescriptor::TYPE_INT64) {
        if (field->is_repeated()) {
          reflection->AddInt64(proto_out, field, encoded_date_value);
        } else {
          reflection->SetInt64(proto_out, field, encoded_date_value);
        }
      } else {
        if (field->is_repeated()) {
          reflection->AddInt32(proto_out, field, encoded_date_value);
        } else {
          reflection->SetInt32(proto_out, field, encoded_date_value);
        }
      }
      return absl::OkStatus();
    }
    case TYPE_TIMESTAMP: {
      ZETASQL_RET_CHECK(field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT64 ||
                (field_format == FieldFormat::TIMESTAMP_MICROS &&
                 field->type() == google::protobuf::FieldDescriptor::TYPE_UINT64))
          << field->type_name();
      ZETASQL_ASSIGN_OR_RETURN(functions::TimestampScale scale,
                       FormatToScale(field_format));
      int64_t converted_timestamp;
      if (!functions::ConvertBetweenTimestamps(value.ToUnixMicros(),
                                               functions::kMicroseconds, scale,
                                               &converted_timestamp)
               .ok()) {
        return ::zetasql_base::OutOfRangeErrorBuilder().LogError()
               << "Cannot encode timestamp: " << value.ToUnixMicros()
               << " with format: " << FieldFormat::Format_Name(field_format);
      }

      if (field->type() == google::protobuf::FieldDescriptor::TYPE_UINT64) {
        if (field->is_repeated()) {
          reflection->AddUInt64(proto_out, field, converted_timestamp);
        } else {
          reflection->SetUInt64(proto_out, field, converted_timestamp);
        }
      } else {
        if (field->is_repeated()) {
          reflection->AddInt64(proto_out, field, converted_timestamp);
        } else {
          reflection->SetInt64(proto_out, field, converted_timestamp);
        }
      }
      return absl::OkStatus();
    }
    case TYPE_DATETIME:
      ZETASQL_RET_CHECK_EQ(field->cpp_type(), google::protobuf::FieldDescriptor::CPPTYPE_INT64);
      if (field->is_repeated()) {
        reflection->AddInt64(proto_out, field,
                             value.ToPacked64DatetimeMicros());
      } else {
        reflection->SetInt64(proto_out, field,
                             value.ToPacked64DatetimeMicros());
      }
      return absl::OkStatus();
    case TYPE_TIME:
      ZETASQL_RET_CHECK_EQ(field->cpp_type(), google::protobuf::FieldDescriptor::CPPTYPE_INT64);
      if (field->is_repeated()) {
        reflection->AddInt64(proto_out, field, value.ToPacked64TimeMicros());
      } else {
        reflection->SetInt64(proto_out, field, value.ToPacked64TimeMicros());
      }
      return absl::OkStatus();
    case TYPE_INTERVAL: {
      ZETASQL_RET_CHECK_EQ(field->type(), google::protobuf::FieldDescriptor::TYPE_BYTES);
      const std::string serialized_value =
          value.interval_value().SerializeAsBytes();
      if (field->is_repeated()) {
        reflection->AddString(proto_out, field, serialized_value);
      } else {
        reflection->SetString(proto_out, field, serialized_value);
      }
      return absl::OkStatus();
    }
    case TYPE_ENUM: {
      ZETASQL_RET_CHECK_EQ(field->type(), google::protobuf::FieldDescriptor::TYPE_ENUM);
      const google::protobuf::EnumDescriptor* enum_descriptor = field->enum_type();
      const google::protobuf::EnumValueDescriptor* enum_value =
          enum_descriptor->FindValueByNumber(value.enum_value());
      ZETASQL_RET_CHECK(enum_value != nullptr) << value.enum_value();
      if (field->is_repeated()) {
        reflection->AddEnum(proto_out, field, enum_value);
      } else {
        reflection->SetEnum(proto_out, field, enum_value);
      }
      return absl::OkStatus();
    }
    case TYPE_ARRAY: {
      // We can require that field is repeated because ARRAY wrappers result
      // in a recursive call using the wrapper's internal repeated field.
      ZETASQL_RET_CHECK(field->is_repeated());

      // ARRAYs of ARRAYs are not allowed in ZetaSQL.
      const ArrayType* array_type = value.type()->AsArray();
      ZETASQL_RET_CHECK(array_type != nullptr) << value.FullDebugString();
      ZETASQL_RET_CHECK(!array_type->element_type()->IsArray())
          << value.FullDebugString();

      for (const Value& element : value.elements()) {
        if (!use_wire_format_annotations && element.is_null()) {
          return ::zetasql_base::OutOfRangeErrorBuilder()
             << "Cannot encode a null value " << element.FullDebugString()
             << " in repeated protocol message field " << field->full_name();
        }
        ZETASQL_RETURN_IF_ERROR(MergeValueToProtoField(element, field,
                                               use_wire_format_annotations,
                                               message_factory, proto_out));
      }
      return absl::OkStatus();
    }
    case TYPE_STRUCT: {
      ZETASQL_RET_CHECK_EQ(field->type(), google::protobuf::FieldDescriptor::TYPE_MESSAGE);
      google::protobuf::Message* submessage = field->is_repeated() ?
         reflection->AddMessage(proto_out, field, message_factory) :
         reflection->MutableMessage(proto_out, field, message_factory);
      ZETASQL_RETURN_IF_ERROR(StructValueToProto(value, use_wire_format_annotations,
                                         message_factory, submessage));
      return absl::OkStatus();
    }
    case TYPE_PROTO: {
      ZETASQL_RET_CHECK_EQ(field->type(), google::protobuf::FieldDescriptor::TYPE_MESSAGE);
      google::protobuf::Message* submessage = field->is_repeated() ?
         reflection->AddMessage(proto_out, field, message_factory) :
         reflection->MutableMessage(proto_out, field, message_factory);
      ValueProto value_proto;
      ZETASQL_RETURN_IF_ERROR(value.Serialize(&value_proto));
      ZETASQL_RET_CHECK(submessage->ParseFromString(
          std::string(value_proto.proto_value())));

      return absl::OkStatus();
    }
    case TYPE_NUMERIC: {
      ZETASQL_RET_CHECK_EQ(field->type(), google::protobuf::FieldDescriptor::TYPE_BYTES);
      const std::string serialized_value =
          value.numeric_value().SerializeAsProtoBytes();
      if (field->is_repeated()) {
        reflection->AddString(proto_out, field, serialized_value);
      } else {
        reflection->SetString(proto_out, field, serialized_value);
      }
      return absl::OkStatus();
    }
    case TYPE_BIGNUMERIC: {
      ZETASQL_RET_CHECK_EQ(field->type(), google::protobuf::FieldDescriptor::TYPE_BYTES);
      const std::string serialized_value =
          value.bignumeric_value().SerializeAsProtoBytes();
      if (field->is_repeated()) {
        reflection->AddString(proto_out, field, serialized_value);
      } else {
        reflection->SetString(proto_out, field, serialized_value);
      }
      return absl::OkStatus();
    }
    case TYPE_JSON: {
      ZETASQL_RET_CHECK_EQ(field->type(), google::protobuf::FieldDescriptor::TYPE_STRING);
      std::string json_string = value.json_string();
      if (field->is_repeated()) {
        reflection->AddString(proto_out, field, std::move(json_string));
      } else {
        reflection->SetString(proto_out, field, std::move(json_string));
      }
      return absl::OkStatus();
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Found Value with unsupported type: "
                       << value.FullDebugString();
  }
}

absl::Status ConvertStructOrArrayValueToProtoMessage(
    const Value& value, google::protobuf::MessageFactory* message_factory,
    google::protobuf::Message* proto_out) {
  ZETASQL_RET_CHECK(value.is_valid());
  ZETASQL_RET_CHECK(!value.is_null()) << "Cannot convert NULL Values to proto";
  ZETASQL_RET_CHECK(proto_out != nullptr);
  proto_out->Clear();

  const bool use_wire_format_annotations = true;
  if (value.type()->IsStruct()) {
    ZETASQL_RETURN_IF_ERROR(StructValueToProto(value, use_wire_format_annotations,
                                       message_factory, proto_out));
    return absl::OkStatus();
  } else if (value.type()->IsArray()) {
    // At the top level, an ARRAY is always a wrapper with a single repeated
    // field.  Pull out the underlying field and fill it.
    const google::protobuf::Descriptor* descriptor = proto_out->GetDescriptor();
    ZETASQL_RET_CHECK_EQ(1, descriptor->field_count());
    ZETASQL_RET_CHECK(ProtoType::GetIsWrapperAnnotation(descriptor));

    const google::protobuf::FieldDescriptor* field = descriptor->field(0);
    ZETASQL_RETURN_IF_ERROR(MergeValueToProtoField(
        value, field, use_wire_format_annotations, message_factory, proto_out));
    return absl::OkStatus();
  }
  ZETASQL_RET_CHECK_FAIL() << "ConvertStructOrArrayValueToProtoMessage() called on "
                      "value that is neither a struct nor an array: "
                   << value.DebugString();
}

static absl::Status ProtoToStructValue(const google::protobuf::Message& proto,
                                       const Type* type,
                                       bool use_wire_format_annotations,
                                       Value* value_out);

// Mutually recursive with ProtoToStructValue.
absl::Status ProtoFieldToValue(const google::protobuf::Message& proto,
                               const google::protobuf::FieldDescriptor* field, int index,
                               const Type* type,
                               bool use_wire_format_annotations,
                               Value* value_out) {
  ZETASQL_RET_CHECK_NE(nullptr, value_out);
  const google::protobuf::Reflection* reflection = proto.GetReflection();

  const FieldFormat::Format field_format =
      ProtoType::GetFormatAnnotation(field);
  if (!type->IsDate() && !type->IsTimestamp() && !type->IsArray() &&
      !type->IsTime() && !type->IsDatetime() && !type->IsGeography() &&
      !type->IsNumericType() && !type->IsBigNumericType() &&
      !type->IsInterval() && !type->IsJsonType()) {
    ZETASQL_RET_CHECK_EQ(FieldFormat::DEFAULT_FORMAT, field_format)
        << "Format " << FieldFormat::Format_Name(field_format)
        << " not supported for zetasql type " << type->DebugString();
  }

  bool is_wrapper = use_wire_format_annotations;
  if (use_wire_format_annotations) {
    ZETASQL_RETURN_IF_ERROR(ShouldTreatAsWrapperForType(field, type, &is_wrapper));
  }
  if (is_wrapper) {
    // Special case for handling NULL arrays.  NULL arrays are indicated
    // by the absence of a wrapper.  NULL non-arrays are indicated by the
    // absence of the field within the wrapper.
    if (type->IsArray()) {
      ZETASQL_RET_CHECK(!field->is_repeated()) << field->DebugString();
      if (!reflection->HasField(proto, field)) {
        *value_out = Value::Null(type);
        return absl::OkStatus();
      }
    }

    ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::TYPE_MESSAGE, field->type())
        << field->DebugString();
    const google::protobuf::Message& wrapper = field->is_repeated() ?
        reflection->GetRepeatedMessage(proto, field, index) :
        reflection->GetMessage(proto, field);
    const google::protobuf::Descriptor* wrapper_descriptor =
        wrapper.GetDescriptor();
    ZETASQL_RET_CHECK_EQ(1, wrapper_descriptor->field_count());
    const google::protobuf::FieldDescriptor* unwrapped_field =
        wrapper_descriptor->field(0);
    return ProtoFieldToValue(wrapper, unwrapped_field, -1 /* index */, type,
                             use_wire_format_annotations, value_out);
  }

  // If a non-repeated field is missing, the value is NULL.
  if (!field->is_repeated()) {
    ZETASQL_RET_CHECK_EQ(-1, index) << field->DebugString();
    if (!reflection->HasField(proto, field)) {
      *value_out = Value::Null(type);
      return absl::OkStatus();
    }
  } else if (!type->IsArray()) {
    // If the field is repeated, then we'll need to know the index we're
    // looking at.  Except when we are expecting an array, in which case
    // we will look at all members of the repeated field.
    ZETASQL_RET_CHECK_GE(index, 0) << field->DebugString();
  }

  switch (type->kind()) {
    case TypeKind::TYPE_ENUM: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_ENUM, field->cpp_type())
          << field->DebugString();
      const EnumType* enum_type = type->AsEnum();
      ZETASQL_RET_CHECK_NE(nullptr, enum_type);
      *value_out = Value::Enum(
          enum_type,
          field->is_repeated() ?
          reflection->GetRepeatedEnumValue(proto, field, index) :
          reflection->GetEnumValue(proto, field));
      return absl::OkStatus();
    }
    case TypeKind::TYPE_PROTO: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE, field->cpp_type())
          << field->DebugString();
      const ProtoType* proto_type = type->AsProto();
      ZETASQL_RET_CHECK_NE(nullptr, proto_type);
      const google::protobuf::Message& submessage =
          field->is_repeated() ?
          reflection->GetRepeatedMessage(proto, field, index) :
          reflection->GetMessage(proto, field);
      absl::Cord serialized;
      std::string serialized_str;
      submessage.SerializeToString(&serialized_str);
      serialized = absl::Cord(serialized_str);
      *value_out = Value::Proto(proto_type, serialized);
      return absl::OkStatus();
    }
    case TypeKind::TYPE_BOOL: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_BOOL, field->cpp_type())
          << field->DebugString();
      *value_out = Value::Bool(
          field->is_repeated() ?
          reflection->GetRepeatedBool(proto, field, index) :
          reflection->GetBool(proto, field));
      return absl::OkStatus();
    }
    case TypeKind::TYPE_DATE: {
      ZETASQL_RET_CHECK(field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT32 ||
                field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT64)
          << field->DebugString();
      int64_t encoded_date_value;
      if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT32) {
        encoded_date_value =
            field->is_repeated()
                ? reflection->GetRepeatedInt32(proto, field, index)
                : reflection->GetInt32(proto, field);
      } else {
        encoded_date_value =
            field->is_repeated()
                ? reflection->GetRepeatedInt64(proto, field, index)
                : reflection->GetInt64(proto, field);
      }
      int32_t decoded_date_value = -1;
      bool output_is_null = false;
      ZETASQL_RETURN_IF_ERROR(
          functions::DecodeFormattedDate(
              encoded_date_value, field_format, &decoded_date_value,
              &output_is_null));
      if (output_is_null) {
        *value_out = Value::NullDate();
        return absl::OkStatus();
      }
      ZETASQL_RET_CHECK(functions::IsValidDate(decoded_date_value))
          << "Invalid date " << decoded_date_value << " converted from "
          << encoded_date_value << " with format "
          << FieldFormat::Format_Name(field_format);

      *value_out =
          output_is_null ? Value::NullDate() : Value::Date(decoded_date_value);
      return absl::OkStatus();
    }
    case TypeKind::TYPE_BYTES: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_STRING, field->cpp_type())
          << field->DebugString();
      *value_out = Value::Bytes(
          field->is_repeated() ?
          reflection->GetRepeatedString(proto, field, index) :
          reflection->GetString(proto, field));
      return absl::OkStatus();
    }
    case TypeKind::TYPE_FLOAT: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_FLOAT, field->cpp_type())
          << field->DebugString();
      *value_out = Value::Float(
          field->is_repeated() ?
          reflection->GetRepeatedFloat(proto, field, index) :
          reflection->GetFloat(proto, field));
      return absl::OkStatus();
    }
    case TypeKind::TYPE_DOUBLE: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE, field->cpp_type())
          << field->DebugString();
      *value_out = Value::Double(
          field->is_repeated() ?
          reflection->GetRepeatedDouble(proto, field, index) :
          reflection->GetDouble(proto, field));
      return absl::OkStatus();
    }
    case TypeKind::TYPE_STRING: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_STRING, field->cpp_type())
          << field->DebugString();
      *value_out = Value::String(
          field->is_repeated() ?
          reflection->GetRepeatedString(proto, field, index) :
          reflection->GetString(proto, field));
      return absl::OkStatus();
    }
    case TypeKind::TYPE_ARRAY: {
      // Array wrappers should have been handled above, so we can assert
      // that we have a repeated field.
      ZETASQL_RET_CHECK(field->is_repeated());
      ZETASQL_RET_CHECK_EQ(-1, index);
      const ArrayType* array_type = type->AsArray();
      const int num_elements = reflection->FieldSize(proto, field);
      std::vector<Value> values(num_elements);
      for (int i = 0; i < num_elements; ++i) {
        ZETASQL_RETURN_IF_ERROR(
            ProtoFieldToValue(proto, field, i, array_type->element_type(),
                              use_wire_format_annotations, &values[i]));
      }
      *value_out = Value::Array(array_type, values);
      return absl::OkStatus();
    }
    case TypeKind::TYPE_STRUCT: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE, field->cpp_type())
          << field->DebugString();
      const google::protobuf::Message& submessage =
          field->is_repeated() ?
          reflection->GetRepeatedMessage(proto, field, index) :
          reflection->GetMessage(proto, field);
      ZETASQL_RETURN_IF_ERROR(ProtoToStructValue(
          submessage, type, use_wire_format_annotations, value_out));
      return absl::OkStatus();
    }
    case TypeKind::TYPE_INT32: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_INT32, field->cpp_type())
          << field->DebugString();
      *value_out = Value::Int32(
          field->is_repeated() ?
          reflection->GetRepeatedInt32(proto, field, index) :
          reflection->GetInt32(proto, field));
      return absl::OkStatus();
    }
    case TypeKind::TYPE_INT64: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_INT64, field->cpp_type())
          << field->DebugString();
      *value_out = Value::Int64(
          field->is_repeated() ?
          reflection->GetRepeatedInt64(proto, field, index) :
          reflection->GetInt64(proto, field));
      return absl::OkStatus();
    }
    case TypeKind::TYPE_UINT32: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_UINT32, field->cpp_type())
          << field->DebugString();
      *value_out = Value::Uint32(
          field->is_repeated() ?
          reflection->GetRepeatedUInt32(proto, field, index) :
          reflection->GetUInt32(proto, field));
      return absl::OkStatus();
    }
    case TypeKind::TYPE_UINT64: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_UINT64, field->cpp_type())
          << field->DebugString();
      *value_out = Value::Uint64(
          field->is_repeated() ?
          reflection->GetRepeatedUInt64(proto, field, index) :
          reflection->GetUInt64(proto, field));
      return absl::OkStatus();
    }
    case TypeKind::TYPE_TIMESTAMP: {
      ZETASQL_RET_CHECK(field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT64 ||
                (field_format == FieldFormat::TIMESTAMP_MICROS &&
                 field->type() == google::protobuf::FieldDescriptor::TYPE_UINT64))
          << field->DebugString();
      ZETASQL_ASSIGN_OR_RETURN(functions::TimestampScale scale,
                       FormatToScale(field_format));
      int64_t encoded_timestamp;
      if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_UINT64) {
        encoded_timestamp =
            field->is_repeated()
                ? reflection->GetRepeatedUInt64(proto, field, index)
                : reflection->GetUInt64(proto, field);
      } else {
        encoded_timestamp =
            field->is_repeated()
                ? reflection->GetRepeatedInt64(proto, field, index)
                : reflection->GetInt64(proto, field);
      }
      int64_t micros;
      if (!functions::ConvertBetweenTimestamps(
               encoded_timestamp, scale, functions::kMicroseconds, &micros)
               .ok()) {
        return ::zetasql_base::OutOfRangeErrorBuilder().LogError()
               << "Invalid encoded timestamp: " << encoded_timestamp
               << " with format: " << FieldFormat::Format_Name(field_format);
      }
      *value_out = Value::TimestampFromUnixMicros(micros);
      return absl::OkStatus();
    }
    case TypeKind::TYPE_DATETIME: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_INT64, field->cpp_type())
          << field->DebugString();
      ZETASQL_RET_CHECK(field_format == FieldFormat::DEFAULT_FORMAT ||
                field_format == FieldFormat::DATETIME_MICROS)
          << FieldFormat::Format_Name(field_format);
      const int64_t encoded_datetime =
          field->is_repeated()
              ? reflection->GetRepeatedInt64(proto, field, index)
              : reflection->GetInt64(proto, field);
      DatetimeValue datetime =
          DatetimeValue::FromPacked64Micros(encoded_datetime);
      if (!datetime.IsValid()) {
        return ::zetasql_base::OutOfRangeErrorBuilder().LogError()
               << "Invalid encoded datetime: " << encoded_datetime
               << " with format: " << FieldFormat::Format_Name(field_format);
      }
      *value_out = Value::Datetime(datetime);
      return absl::OkStatus();
    }
    case TypeKind::TYPE_TIME: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_INT64, field->cpp_type())
          << field->DebugString();
      ZETASQL_RET_CHECK(field_format == FieldFormat::DEFAULT_FORMAT ||
                field_format == FieldFormat::TIME_MICROS)
          << FieldFormat::Format_Name(field_format);
      const int64_t encoded_time =
          field->is_repeated()
              ? reflection->GetRepeatedInt64(proto, field, index)
              : reflection->GetInt64(proto, field);
      TimeValue time = TimeValue::FromPacked64Micros(encoded_time);
      if (!time.IsValid()) {
        return ::zetasql_base::OutOfRangeErrorBuilder().LogError()
               << "Invalid encoded time: " << encoded_time
               << " with format: " << FieldFormat::Format_Name(field_format);
      }
      *value_out = Value::Time(time);
      return absl::OkStatus();
    }
    case TypeKind::TYPE_INTERVAL: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_STRING, field->cpp_type())
          << field->DebugString();
      std::string value =
          field->is_repeated() ?
          reflection->GetRepeatedString(proto, field, index) :
          reflection->GetString(proto, field);
      ZETASQL_ASSIGN_OR_RETURN(IntervalValue interval_value,
                       IntervalValue::DeserializeFromBytes(value));
      *value_out = Value::Interval(interval_value);
      return absl::OkStatus();
    }
    case TypeKind::TYPE_NUMERIC: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_STRING, field->cpp_type())
          << field->DebugString();
      std::string value =
          field->is_repeated() ?
          reflection->GetRepeatedString(proto, field, index) :
          reflection->GetString(proto, field);
      ZETASQL_ASSIGN_OR_RETURN(
          NumericValue numeric_value,
          NumericValue::DeserializeFromProtoBytes(value));
      *value_out = Value::Numeric(numeric_value);
      return absl::OkStatus();
    }
    case TypeKind::TYPE_BIGNUMERIC: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_STRING, field->cpp_type())
          << field->DebugString();
      ZETASQL_RET_CHECK_EQ(FieldFormat::BIGNUMERIC, field_format)
          << FieldFormat::Format_Name(field_format);
      std::string value =
          field->is_repeated() ?
          reflection->GetRepeatedString(proto, field, index) :
          reflection->GetString(proto, field);
      ZETASQL_ASSIGN_OR_RETURN(
          BigNumericValue bignumeric_value,
          BigNumericValue::DeserializeFromProtoBytes(value));
      *value_out = Value::BigNumeric(bignumeric_value);
      return absl::OkStatus();
    }
    case TypeKind::TYPE_JSON: {
      ZETASQL_RET_CHECK_EQ(google::protobuf::FieldDescriptor::CPPTYPE_STRING, field->cpp_type())
          << field->DebugString();
      ZETASQL_RET_CHECK_EQ(FieldFormat::JSON, field_format)
          << FieldFormat::Format_Name(field_format);
      std::string value =
          field->is_repeated() ?
          reflection->GetRepeatedString(proto, field, index) :
          reflection->GetString(proto, field);
      ZETASQL_ASSIGN_OR_RETURN(
          JSONValue json_value,
          JSONValue::ParseJSONString(value));
      *value_out = Value::Json(std::move(json_value));
      return absl::OkStatus();
    }
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unsupported TypeKind: " << type->DebugString();
  }
}

// Converts 'proto' to a Value of type 'type' and returns it in
// 'value_out'.  'type' must be a STRUCT type.
//
// Mutually recursive with ProtoFieldToValue.
static absl::Status ProtoToStructValue(const google::protobuf::Message& proto,
                                       const Type* type,
                                       bool use_wire_format_annotations,
                                       Value* value_out) {
  const StructType* struct_type = type->AsStruct();
  ZETASQL_RET_CHECK(struct_type != nullptr) << type->DebugString();

  const google::protobuf::Descriptor* descriptor = proto.GetDescriptor();
  std::vector<Value> values(struct_type->num_fields());
  ZETASQL_RET_CHECK_EQ(struct_type->num_fields(), descriptor->field_count());
  for (int i = 0; i < descriptor->field_count(); ++i) {
    const google::protobuf::FieldDescriptor* field_descriptor = descriptor->field(i);
    const StructType::StructField& struct_field = struct_type->field(i);
    ZETASQL_RETURN_IF_ERROR(ProtoFieldToValue(proto, field_descriptor, -1 /* index */,
                                      struct_field.type,
                                      use_wire_format_annotations, &values[i]));
  }

  *value_out = Value::Struct(struct_type, values);
  return absl::OkStatus();
}

absl::Status ConvertProtoMessageToStructOrArrayValue(
    const google::protobuf::Message& proto, const Type* type, Value* value_out) {
  const bool use_wire_format_annotations = true;
  if (type->IsStruct()) {
    return ProtoToStructValue(proto, type, use_wire_format_annotations,
                              value_out);
  } else if (type->IsArray()) {
    // At the top level, an ARRAY is always a wrapper.  Pull out the underlying
    // field and read it into value_out.
    const google::protobuf::Descriptor* descriptor = proto.GetDescriptor();
    ZETASQL_RET_CHECK(ProtoType::GetIsWrapperAnnotation(descriptor));
    ZETASQL_RET_CHECK_EQ(1, descriptor->field_count());
    const google::protobuf::FieldDescriptor* field = descriptor->field(0);

    return ProtoFieldToValue(proto, field, -1 /* index */, type,
                             use_wire_format_annotations, value_out);
  }
  ZETASQL_RET_CHECK_FAIL() << type->DebugString();
}

}  // namespace zetasql
