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

#include "zetasql/reference_impl/proto_util.h"

#include <cstdint>
#include <string>

#include "zetasql/base/logging.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/wire_format_lite.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/public/functions/arithmetics.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/proto/type_annotation.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/value.h"
#include "absl/base/casts.h"
#include <cstdint>
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

using google::protobuf::FieldDescriptor;
using google::protobuf::io::CodedOutputStream;
using google::protobuf::internal::WireFormatLite;

namespace zetasql {

static bool IsInt32FieldType(FieldDescriptor::Type type) {
  return type == FieldDescriptor::TYPE_INT32 ||
         type == FieldDescriptor::TYPE_SINT32 ||
         type == FieldDescriptor::TYPE_SFIXED32;
}

static bool IsInt64FieldType(FieldDescriptor::Type type) {
  return type == FieldDescriptor::TYPE_INT64 ||
         type == FieldDescriptor::TYPE_SINT64 ||
         type == FieldDescriptor::TYPE_SFIXED64;
}

absl::Status ProtoUtil::CheckIsSupportedFieldFormat(
    FieldFormat::Format format,
    const google::protobuf::FieldDescriptor* field) {
  // NOTE: This should match ProtoType::ValidateTypeAnnotations.
  switch (format) {
    case FieldFormat::DEFAULT_FORMAT:
      // We allow reading anything with DEFAULT_FORMAT so we can use it to
      // read the raw proto values.
      return absl::OkStatus();

    case FieldFormat::DATE:
    case FieldFormat::DATE_DECIMAL:
      if (IsInt32FieldType(field->type()) ||
          IsInt64FieldType(field->type())) {
        return absl::OkStatus();
      }
      break;

    case FieldFormat::DATETIME_MICROS:
    case FieldFormat::TIME_MICROS:
    case FieldFormat::TIMESTAMP_SECONDS:
    case FieldFormat::TIMESTAMP_MILLIS:
    case FieldFormat::TIMESTAMP_NANOS:
      if (IsInt64FieldType(field->type())) {
        return absl::OkStatus();
      }
      break;

    case FieldFormat::TIMESTAMP_MICROS:
      if (IsInt64FieldType(field->type()) ||
          field->type() == google::protobuf::FieldDescriptor::TYPE_UINT64) {
        return absl::OkStatus();
      }
      break;

      // NOTE: If more formats get added here as supported, make sure all
      // other functions in this file that use Format args can handle the
      // new formats.  Add tests in ComplianceCodebasedTests::TestProtoFields
      // for any new supported formats.
    default:
      break;
  }

  return ::zetasql_base::UnimplementedErrorBuilder()
         << "Field has an unsupported zetasql.type annotation: "
         << (field == nullptr ? absl::StrCat(format) : field->DebugString());
}

// Takes a Value of TYPE_TIMESTAMP and produces an int64_t result adjusted
// appropriately given the <format> (seconds, millis, micros, nanos).
static absl::Status TimestampValueToAdjustedInt64(FieldFormat::Format format,
                                                  const Value& timestamp,
                                                  int64_t* adjusted_int64) {
  ZETASQL_RET_CHECK(timestamp.type()->IsTimestamp());
  absl::Status status;
  const int64_t micros = timestamp.ToUnixMicros();
  switch (format) {
    case FieldFormat::TIMESTAMP_SECONDS:
      if (!functions::Divide<int64_t>(micros, int64_t{1000000}, adjusted_int64,
                                      &status)) {
        return status;
      }
      if (micros < 0 && micros % 1000000LL != 0) {
        (*adjusted_int64)--;  // Always round down, not toward zero.
      }
      break;
    case FieldFormat::TIMESTAMP_MILLIS:
      if (!functions::Divide<int64_t>(micros, int64_t{1000}, adjusted_int64,
                                      &status)) {
        return status;
      }
      if (micros < 0 && micros % 1000LL != 0) {
        (*adjusted_int64)--;  // Always round down, not toward zero.
      }
      break;
    case FieldFormat::TIMESTAMP_MICROS:
      *adjusted_int64 = micros;
      break;
    case FieldFormat::TIMESTAMP_NANOS:
      return timestamp.ToUnixNanos(adjusted_int64);
    default:
      return ::zetasql_base::OutOfRangeErrorBuilder()
             << "Invalid timestamp field format: " << format;
  }
  return absl::OkStatus();
}

// Macro used to switch on a (proto type, ZetaSQL TypeKind) pair.
#define PAIR(proto_type, zetasql_type) \
  ((static_cast<uint64_t>(proto_type) << 32) + zetasql_type)

static absl::Status WriteScalarValue(
    const google::protobuf::FieldDescriptor* field_descr, const Value& v,
    CodedOutputStream* dst) {
  // Compatibility of date/timestamp field types is managed by the analyzer,
  // driven by ZetaSQL annotations.
  switch (PAIR(field_descr->type(), v.type_kind())) {
    // Floating point types.
    case PAIR(FieldDescriptor::TYPE_DOUBLE, TYPE_DOUBLE):
      dst->WriteLittleEndian64(WireFormatLite::EncodeDouble(v.double_value()));
      break;
    case PAIR(FieldDescriptor::TYPE_FLOAT, TYPE_FLOAT):
      dst->WriteLittleEndian32(WireFormatLite::EncodeFloat(v.float_value()));
      break;

    // Unsigned types.
    case PAIR(FieldDescriptor::TYPE_FIXED64, TYPE_UINT64):
      dst->WriteLittleEndian64(v.uint64_value());
      break;
    case PAIR(FieldDescriptor::TYPE_FIXED32, TYPE_UINT32):
      dst->WriteLittleEndian32(v.uint32_value());
      break;
    case PAIR(FieldDescriptor::TYPE_UINT64, TYPE_UINT64):
      dst->WriteVarint64(v.uint64_value());
      break;
    case PAIR(FieldDescriptor::TYPE_UINT64, TYPE_TIMESTAMP):
      dst->WriteVarint64(absl::bit_cast<uint64_t>(v.ToInt64()));
      break;
    case PAIR(FieldDescriptor::TYPE_UINT32, TYPE_UINT32):
      dst->WriteVarint32(v.uint32_value());
      break;

    case PAIR(FieldDescriptor::TYPE_BOOL, TYPE_BOOL):
      dst->WriteVarint32(v.bool_value() ? 1 : 0);
      break;

    // Signed types.
    case PAIR(FieldDescriptor::TYPE_SFIXED64, TYPE_INT64):
      dst->WriteLittleEndian64(static_cast<uint64_t>(v.int64_value()));
      break;
    case PAIR(FieldDescriptor::TYPE_SFIXED64, TYPE_DATE):
    case PAIR(FieldDescriptor::TYPE_SFIXED64, TYPE_TIMESTAMP):
      dst->WriteLittleEndian64(static_cast<uint64_t>(v.ToInt64()));
      break;
    case PAIR(FieldDescriptor::TYPE_SFIXED64, TYPE_TIME):
      dst->WriteLittleEndian64(
          absl::bit_cast<uint64_t>(v.ToPacked64TimeMicros()));
      break;
    case PAIR(FieldDescriptor::TYPE_SFIXED64, TYPE_DATETIME):
      dst->WriteLittleEndian64(
          absl::bit_cast<uint64_t>(v.ToPacked64DatetimeMicros()));
      break;
    case PAIR(FieldDescriptor::TYPE_SFIXED32, TYPE_INT32):
      dst->WriteLittleEndian32(static_cast<uint32_t>(v.int32_value()));
      break;
    case PAIR(FieldDescriptor::TYPE_SFIXED32, TYPE_DATE):
      dst->WriteLittleEndian32(static_cast<uint32_t>(v.date_value()));
      break;
    case PAIR(FieldDescriptor::TYPE_INT64, TYPE_INT64):
      dst->WriteVarint64(v.int64_value());
      break;
    case PAIR(FieldDescriptor::TYPE_INT64, TYPE_DATE):
    case PAIR(FieldDescriptor::TYPE_INT64, TYPE_TIMESTAMP):
      dst->WriteVarint64(v.ToInt64());
      break;
    case PAIR(FieldDescriptor::TYPE_INT64, TYPE_TIME):
      dst->WriteVarint64(v.ToPacked64TimeMicros());
      break;
    case PAIR(FieldDescriptor::TYPE_INT64, TYPE_DATETIME):
      dst->WriteVarint64(v.ToPacked64DatetimeMicros());
      break;
    case PAIR(FieldDescriptor::TYPE_INT32, TYPE_INT32):
      dst->WriteVarint32SignExtended(v.int32_value());
      break;
    case PAIR(FieldDescriptor::TYPE_INT32, TYPE_DATE):
      dst->WriteVarint32SignExtended(v.date_value());
      break;
    case PAIR(FieldDescriptor::TYPE_ENUM, TYPE_ENUM):
      dst->WriteVarint32SignExtended(v.enum_value());
      break;
    case PAIR(FieldDescriptor::TYPE_SINT32, TYPE_INT32):
      dst->WriteVarint32(
          WireFormatLite::ZigZagEncode32(v.int32_value()));
      break;
    case PAIR(FieldDescriptor::TYPE_SINT32, TYPE_DATE):
      dst->WriteVarint32(
          WireFormatLite::ZigZagEncode32(v.date_value()));
      break;
    case PAIR(FieldDescriptor::TYPE_SINT64, TYPE_INT64):
      dst->WriteVarint64(
          WireFormatLite::ZigZagEncode64(v.int64_value()));
      break;
    case PAIR(FieldDescriptor::TYPE_SINT64, TYPE_DATE):
    case PAIR(FieldDescriptor::TYPE_SINT64, TYPE_TIMESTAMP):
      dst->WriteVarint64(
          WireFormatLite::ZigZagEncode64(v.ToInt64()));
      break;
    case PAIR(FieldDescriptor::TYPE_SINT64, TYPE_TIME):
      dst->WriteVarint64(
          WireFormatLite::ZigZagEncode64(v.ToPacked64TimeMicros()));
      break;
    case PAIR(FieldDescriptor::TYPE_SINT64, TYPE_DATETIME):
      dst->WriteVarint64(
          WireFormatLite::ZigZagEncode64(v.ToPacked64DatetimeMicros()));
      break;

    // String types.
    case PAIR(FieldDescriptor::TYPE_STRING, TYPE_STRING):
      dst->WriteString(v.string_value());
      break;
    case PAIR(FieldDescriptor::TYPE_BYTES, TYPE_BYTES):
      dst->WriteString(v.bytes_value());
      break;
    case PAIR(FieldDescriptor::TYPE_MESSAGE, TYPE_PROTO):
    case PAIR(FieldDescriptor::TYPE_GROUP, TYPE_PROTO):
      dst->WriteString(std::string(v.ToCord()));
      break;

    default:
      return ::zetasql_base::OutOfRangeErrorBuilder()
             << "Cannot encode value " << v.DebugString()
             << " in protocol buffer field " << field_descr->DebugString();
  }
  return absl::OkStatus();
}

#undef PAIR

static absl::Status WriteValue(const google::protobuf::FieldDescriptor* field_descr,
                               FieldFormat::Format format, const Value& value,
                               CodedOutputStream* dst) {
  if (value.is_null() &&
      (field_descr->is_required() || field_descr->is_repeated())) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "Cannot encode a null value " << value.FullDebugString() << " in "
           << (field_descr->is_required() ? "required" : "repeated")
           << " protocol message field " << field_descr->full_name();
  }

  WireFormatLite::WireType wire_type = WireFormatLite::WireTypeForFieldType(
      static_cast<WireFormatLite::FieldType>(field_descr->type()));
  if (wire_type == WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
    switch (value.type_kind()) {
      case TYPE_STRING: dst->WriteVarint32(value.string_value().size()); break;
      case TYPE_BYTES: dst->WriteVarint32(value.bytes_value().size()); break;
      case TYPE_PROTO: dst->WriteVarint32(value.ToCord().size()); break;
      default:
        // This can happen for some FieldFormat annotations. Ultimately the
        // value will be written with a different wire type.
        break;
    }
  }
  // The formats allowed by CheckIsSupportedFieldFormat only apply to scalars,
  // and this is the only one that requires translation currently.
  if (format == FieldFormat::DATE_DECIMAL) {
    // TODO:  Maybe this should not be a ZETASQL_RET_CHECK?  If we tried to
    // write an incompatible Value into this field then it would pop.  There
    // is a similar unit test case for writing a string Value into a
    // TIMESTAMP field, so we avoid a ZETASQL_RET_CHECK in that case to avoid
    // crashing the test.
    ZETASQL_RET_CHECK_EQ(value.type_kind(), TypeKind::TYPE_DATE);
    int32_t encoded_date;
    const absl::Status status = functions::EncodeFormattedDate(
        value.date_value(), format, &encoded_date);
    if (!status.ok()) {
      return ::zetasql_base::StatusBuilder(status.code())
             << "Cannot encode date " << value.FullDebugString()
             << " for field " << field_descr->full_name() << ": "
             << status.message();
    }
    // We are writing an integer value, not a zetasql DATE.  We have to
    // generate a date with the right width.
    Value encoded_value;
    if (IsInt32FieldType(field_descr->type())) {
      encoded_value = Value::Int32(encoded_date);
    } else {
      // This should never fail because CheckIsSupportedFieldFormat would
      // have rejected any other types.
      ZETASQL_DCHECK(IsInt64FieldType(field_descr->type()))
          << field_descr->DebugString();
      encoded_value = Value::Int64(encoded_date);
    }
    return WriteScalarValue(field_descr, encoded_value, dst);
  }
  if ((format == FieldFormat::TIMESTAMP_SECONDS ||
       format == FieldFormat::TIMESTAMP_MILLIS ||
       format == FieldFormat::TIMESTAMP_MICROS) &&
      value.type_kind() == TypeKind::TYPE_TIMESTAMP) {
    // If the format is seconds/millis/micros and this is a new timestamp
    // value, then the value may need to be adjusted since the timestamp
    // is at micros precision and the field may not be.  If this is not
    // true then we just want to go ahead and call WriteScalarValue
    // below, which will detect any type incompatibility and provide an
    // appropriate error if so.
    int64_t adjusted_int64;
    ZETASQL_RETURN_IF_ERROR(TimestampValueToAdjustedInt64(format, value,
                                                  &adjusted_int64));
    Value adjusted_value;
    if (field_descr->type() == google::protobuf::FieldDescriptor::TYPE_UINT64) {
      adjusted_value = Value::Uint64(absl::bit_cast<uint64_t>(adjusted_int64));
    } else {
      adjusted_value = Value::Int64(adjusted_int64);
    }
    return WriteScalarValue(field_descr, adjusted_value, dst);
  }
  return WriteScalarValue(field_descr, value, dst);
}

static absl::Status WriteTagAndValue(const google::protobuf::FieldDescriptor* field_descr,
                                     FieldFormat::Format format,
                                     const Value& value,
                                     CodedOutputStream* dst) {
  // Errors for NULL values are handled by WriteValue().
  if (value.is_null() && field_descr->is_optional()) {
    return absl::OkStatus();
  }
  const int32_t proto_tag = field_descr->number();
  const FieldDescriptor::Type proto_type = field_descr->type();
  if (proto_type == FieldDescriptor::TYPE_GROUP) {
    dst->WriteVarint32((proto_tag << 3) | 3 /* STARTGROUP */);
    ZETASQL_RETURN_IF_ERROR(
        WriteValue(field_descr, FieldFormat::DEFAULT_FORMAT, value, dst));
    dst->WriteVarint32((proto_tag << 3) | 4 /* ENDGROUP */);
  } else {
    // Emit native tag and value.
    WireFormatLite::WireType wire_type = WireFormatLite::WireTypeForFieldType(
        static_cast<WireFormatLite::FieldType>(proto_type));
    dst->WriteVarint32(WireFormatLite::MakeTag(proto_tag, wire_type));
    ZETASQL_RETURN_IF_ERROR(WriteValue(field_descr, format, value, dst));
  }
  return absl::OkStatus();
}

absl::Status ProtoUtil::WriteField(const WriteFieldOptions& options,
                                   const FieldDescriptor* field_descr,
                                   const FieldFormat::Format format,
                                   const Value& value, bool* nondeterministic,
                                   CodedOutputStream* dst) {
  ZETASQL_RETURN_IF_ERROR(CheckIsSupportedFieldFormat(format, field_descr));

  if (!options.allow_null_map_keys && value.is_null() &&
      field_descr->containing_type()->options().map_entry()) {
    return MakeEvalError()
           << "Cannot write NULL to key or value of map field in "
           << field_descr->containing_type()->name();
  }

  if (field_descr->is_repeated() != value.type()->IsArray()) {
    return ::zetasql_base::OutOfRangeErrorBuilder()
           << "Cannot encode value " << value.DebugString()
           << " in protocol message field " << field_descr->full_name()
           << " of type " << FieldDescriptor::TypeName(field_descr->type());
  }
  if (field_descr->is_repeated()) {
    if (!value.is_null()) {
      // The proto field is assumed ordered. If the array is not, set the
      // non-determinism bit.
      if (InternalValue::GetOrderKind(value) !=
          InternalValue::kPreservesOrder) {
        *nondeterministic = true;
      }

      if (field_descr->is_packed()) {
        // Serialize all the values into a Cord, then write the Cord using the
        // LENGTH_DELIMITED wire type.
        std::string packed_contents;
        {
          // The cord cannot be read directly until the streams are destroyed.
          google::protobuf::io::StringOutputStream cord_stream(&packed_contents);
          CodedOutputStream coded_cord_stream(&cord_stream);
          for (const Value& v : value.elements()) {
            ZETASQL_RETURN_IF_ERROR(
                WriteValue(field_descr, format, v, &coded_cord_stream));
          }
        }

        dst->WriteVarint32(WireFormatLite::MakeTag(
            field_descr->number(), WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        dst->WriteVarint32(packed_contents.size());
        dst->WriteString(packed_contents);
      } else {
        for (const Value& v : value.elements()) {
          ZETASQL_RETURN_IF_ERROR(WriteTagAndValue(field_descr, format, v, dst));
        }
      }
    }
    return absl::OkStatus();
  }
  return WriteTagAndValue(field_descr, format, value, dst);
}

}  // namespace zetasql
