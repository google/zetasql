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

#include "zetasql/public/proto_util.h"

#include <cstdint>
#include <string>
#include <utility>

#include "zetasql/base/logging.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/common/testing/proto_matchers.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/proto/type_annotation.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/flags/flag.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

using ::testing::HasSubstr;

using zetasql_test__::KitchenSinkPB;
using zetasql_test__::ProtoWithIntervalField;

using zetasql::testing::EqualsProto;

using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

namespace {

// Test fixture for ReadProtoFields(). Note that we also get some additional
// test coverage from the proto_*.test compliance tests and
// reference_impl/evaluation_test.
class ReadProtoFieldsTest : public ::testing::TestWithParam<bool> {
 protected:
  ReadProtoFieldsTest() {
    absl::SetFlag(&FLAGS_zetasql_read_proto_field_optimized_path, GetParam());
  }

  absl::StatusOr<Value> ReadField(const std::string& field_name,
                                  FieldFormat::Format format, const Type* type,
                                  const Value& default_value,
                                  const absl::Cord& bytes,
                                  bool get_has_bit = false) {
    const google::protobuf::FieldDescriptor* field_descriptor =
        kitchen_sink_.GetDescriptor()->FindFieldByName(field_name);
    ZETASQL_RET_CHECK(field_descriptor != nullptr) << field_name;

    Value value;
    ZETASQL_RETURN_IF_ERROR(ReadProtoField(field_descriptor, format, type,
                                   default_value, get_has_bit, bytes, &value));
    return value;
  }

  absl::StatusOr<Value> ReadField(const std::string& field_name,
                                  FieldFormat::Format format, const Type* type,
                                  const Value& default_value,
                                  bool get_has_bit = false) {
    absl::Cord bytes = SerializePartialToCord(kitchen_sink_);
    return ReadField(field_name, format, type, default_value, bytes,
                     get_has_bit);
  }

  absl::StatusOr<Value> ReadHasBit(const std::string& name,
                                   FieldFormat::Format format,
                                   const Type* type) {
    Value default_value;
    return ReadField(name, format, type, default_value, /*get_has_bit=*/true);
  }

  KitchenSinkPB kitchen_sink_;

  TypeFactory type_factory_;
};

TEST_P(ReadProtoFieldsTest, Int32) {
  kitchen_sink_.set_int32_val(10);
  EXPECT_THAT(ReadField("int32_val", FieldFormat::DEFAULT_FORMAT,
                        types::Int32Type(), values::Int32(0)),
              IsOkAndHolds(values::Int32(10)));
}

TEST_P(ReadProtoFieldsTest, Int64) {
  kitchen_sink_.set_int64_val(10);
  EXPECT_THAT(ReadField("int64_val", FieldFormat::DEFAULT_FORMAT,
                        types::Int64Type(), values::Int64(0)),
              IsOkAndHolds(values::Int64(10)));
}

TEST_P(ReadProtoFieldsTest, Uint32) {
  kitchen_sink_.set_uint32_val(10);
  EXPECT_THAT(ReadField("uint32_val", FieldFormat::DEFAULT_FORMAT,
                        types::Uint32Type(), values::Uint32(0)),
              IsOkAndHolds(values::Uint32(10)));
}

TEST_P(ReadProtoFieldsTest, Uint64) {
  kitchen_sink_.set_uint64_val(10);
  EXPECT_THAT(ReadField("uint64_val", FieldFormat::DEFAULT_FORMAT,
                        types::Uint64Type(), values::Uint64(0)),
              IsOkAndHolds(values::Uint64(10)));
}

TEST_P(ReadProtoFieldsTest, Sint32) {
  kitchen_sink_.set_sint32_val(10);
  EXPECT_THAT(ReadField("sint32_val", FieldFormat::DEFAULT_FORMAT,
                        types::Int32Type(), values::Int32(0)),
              IsOkAndHolds(values::Int32(10)));
}

TEST_P(ReadProtoFieldsTest, Sint64) {
  kitchen_sink_.set_sint64_val(10);
  EXPECT_THAT(ReadField("sint64_val", FieldFormat::DEFAULT_FORMAT,
                        types::Int64Type(), values::Int64(0)),
              IsOkAndHolds(values::Int64(10)));
}

TEST_P(ReadProtoFieldsTest, Fixed32) {
  kitchen_sink_.set_fixed32_val(10);
  EXPECT_THAT(ReadField("fixed32_val", FieldFormat::DEFAULT_FORMAT,
                        types::Uint32Type(), values::Uint32(0)),
              IsOkAndHolds(values::Uint32(10)));
}

TEST_P(ReadProtoFieldsTest, Fixed64) {
  kitchen_sink_.set_fixed64_val(10);
  EXPECT_THAT(ReadField("fixed64_val", FieldFormat::DEFAULT_FORMAT,
                        types::Uint64Type(), values::Uint64(0)),
              IsOkAndHolds(values::Uint64(10)));
}

TEST_P(ReadProtoFieldsTest, SFixed32) {
  kitchen_sink_.set_sfixed32_val(10);
  EXPECT_THAT(ReadField("sfixed32_val", FieldFormat::DEFAULT_FORMAT,
                        types::Int32Type(), values::Int32(0)),
              IsOkAndHolds(values::Int32(10)));
}

TEST_P(ReadProtoFieldsTest, SFixed64) {
  kitchen_sink_.set_sfixed64_val(10);
  EXPECT_THAT(ReadField("sfixed64_val", FieldFormat::DEFAULT_FORMAT,
                        types::Int64Type(), values::Int64(0)),
              IsOkAndHolds(values::Int64(10)));
}

TEST_P(ReadProtoFieldsTest, Bool) {
  kitchen_sink_.set_bool_val(true);
  EXPECT_THAT(ReadField("bool_val", FieldFormat::DEFAULT_FORMAT,
                        types::BoolType(), values::Bool(false)),
              IsOkAndHolds(values::Bool(true)));
}

TEST_P(ReadProtoFieldsTest, Float) {
  kitchen_sink_.set_float_val(1.5);
  EXPECT_THAT(ReadField("float_val", FieldFormat::DEFAULT_FORMAT,
                        types::FloatType(), values::Float(0)),
              IsOkAndHolds(values::Float(1.5)));
}

TEST_P(ReadProtoFieldsTest, Double) {
  kitchen_sink_.set_double_val(1.5);
  EXPECT_THAT(ReadField("double_val", FieldFormat::DEFAULT_FORMAT,
                        types::DoubleType(), values::Double(0)),
              IsOkAndHolds(values::Double(1.5)));
}

TEST_P(ReadProtoFieldsTest, String) {
  kitchen_sink_.set_string_val("foo");
  EXPECT_THAT(ReadField("string_val", FieldFormat::DEFAULT_FORMAT,
                        types::StringType(), values::String("")),
              IsOkAndHolds(values::String("foo")));
}

TEST_P(ReadProtoFieldsTest, Bytes) {
  kitchen_sink_.set_bytes_val("bar");
  EXPECT_THAT(ReadField("bytes_val", FieldFormat::DEFAULT_FORMAT,
                        types::BytesType(), values::Bytes("")),
              IsOkAndHolds(values::Bytes("bar")));
}

TEST_P(ReadProtoFieldsTest, Enum) {
  kitchen_sink_.set_test_enum(zetasql_test__::TESTENUM2);

  const google::protobuf::FieldDescriptor* field_descriptor =
      kitchen_sink_.GetDescriptor()->FindFieldByName("test_enum");
  ASSERT_TRUE(field_descriptor != nullptr);

  const google::protobuf::EnumDescriptor* enum_descriptor = field_descriptor->enum_type();
  ASSERT_TRUE(enum_descriptor != nullptr);

  const EnumType* enum_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeEnumType(enum_descriptor, &enum_type));

  EXPECT_THAT(ReadField("test_enum", FieldFormat::DEFAULT_FORMAT, enum_type,
                        Value::Enum(enum_type, 0)),
              IsOkAndHolds(Value::Enum(enum_type, 2)));
}

TEST_P(ReadProtoFieldsTest, EnumOutOfRange) {
  absl::Cord bytes;

  // Append test_enum with value 1000. The streams are scoped to be closed
  // correctly.
  {
    using google::protobuf::internal::WireFormatLite;
    std::string bytes_str;
    google::protobuf::io::StringOutputStream cord_stream(&bytes_str);

    google::protobuf::io::CodedOutputStream out(&cord_stream);
    out.WriteVarint32(WireFormatLite::MakeTag(36 /* tag for test_enum */,
                                              WireFormatLite::WIRETYPE_VARINT));
    out.WriteVarint32(1000);
    bytes = absl::Cord(bytes_str);
  }

  const google::protobuf::FieldDescriptor* field_descriptor =
      kitchen_sink_.GetDescriptor()->FindFieldByName("test_enum");
  ASSERT_TRUE(field_descriptor != nullptr);

  const google::protobuf::EnumDescriptor* enum_descriptor = field_descriptor->enum_type();
  ASSERT_TRUE(enum_descriptor != nullptr);

  const EnumType* enum_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeEnumType(enum_descriptor, &enum_type));

  EXPECT_THAT(
      ReadField("test_enum", FieldFormat::DEFAULT_FORMAT, enum_type,
                Value::Enum(enum_type, 0), bytes),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Failed to interpret value for field "
                         "zetasql_test__.KitchenSinkPB.test_enum: 1000")));

  EXPECT_THAT(ReadField("test_enum", FieldFormat::DEFAULT_FORMAT, enum_type,
                        Value::Enum(enum_type, 0), bytes, /*get_has_bit=*/true),
              IsOkAndHolds(values::Bool(true)));
}

TEST_P(ReadProtoFieldsTest, Message) {
  KitchenSinkPB::Nested* nested = kitchen_sink_.mutable_nested_value();
  nested->set_nested_int64(10);
  nested->add_nested_repeated_int64(100);
  nested->add_nested_repeated_int64(200);

  const google::protobuf::Descriptor* nested_descriptor = nested->GetDescriptor();
  const ProtoType* proto_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(nested_descriptor, &proto_type));

  auto result = ReadField("nested_value", FieldFormat::DEFAULT_FORMAT,
                          proto_type, Value::Null(proto_type));

  ZETASQL_ASSERT_OK(result.status());
  const Value& output_value = result.value();
  ASSERT_EQ(output_value.type_kind(), TYPE_PROTO);

  KitchenSinkPB::Nested output_nested;
  ASSERT_TRUE(ParseFromCord(output_value.ToCord(), &output_nested));
  EXPECT_THAT(output_nested, EqualsProto(*nested));
}

TEST_P(ReadProtoFieldsTest, MultiOccurrencesOfSingularMessage) {
  KitchenSinkPB part_1, part_2;
  KitchenSinkPB::Nested* part_1_nested = part_1.mutable_nested_value();
  part_1_nested->set_nested_int64(1);
  part_1_nested->add_nested_repeated_int64(100);
  part_1_nested->add_nested_repeated_int64(200);

  KitchenSinkPB::Nested* part_2_nested = part_2.mutable_nested_value();
  part_2_nested->set_nested_int64(2);
  part_2_nested->add_nested_repeated_int64(300);
  part_2_nested->add_nested_repeated_int64(400);

  absl::Cord merged_bytes;
  merged_bytes.Append(SerializePartialToCord(part_1));
  merged_bytes.Append(SerializePartialToCord(part_2));

  KitchenSinkPB merged_message;
  ASSERT_TRUE(ParsePartialFromCord(merged_bytes, &merged_message));

  const google::protobuf::Descriptor* nested_descriptor = part_1_nested->GetDescriptor();
  const ProtoType* proto_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(nested_descriptor, &proto_type));

  auto result = ReadField("nested_value", FieldFormat::DEFAULT_FORMAT,
                          proto_type, Value::Null(proto_type), merged_bytes);

  ZETASQL_ASSERT_OK(result.status());
  const Value& output_value = result.value();
  ASSERT_EQ(output_value.type_kind(), TYPE_PROTO);

  KitchenSinkPB::Nested output_nested;
  ASSERT_TRUE(ParsePartialFromCord(output_value.ToCord(), &output_nested));
  EXPECT_EQ(output_nested.nested_repeated_int64_size(), 4);
  EXPECT_THAT(output_nested, EqualsProto(merged_message.nested_value()));
}

TEST_P(ReadProtoFieldsTest, MultiOccurrencesOfSingularGroup) {
  KitchenSinkPB part_1, part_2;
  KitchenSinkPB::OptionalGroup* part_1_group = part_1.mutable_optional_group();
  part_1_group->set_int64_val(1);
  part_1_group->add_optionalgroupnested()->set_int64_val(100);

  KitchenSinkPB::OptionalGroup* part_2_group = part_2.mutable_optional_group();
  part_2_group->set_int64_val(2);
  part_2_group->add_optionalgroupnested()->set_int64_val(200);

  absl::Cord merged_bytes;
  merged_bytes.Append(SerializePartialToCord(part_1));
  merged_bytes.Append(SerializePartialToCord(part_2));

  KitchenSinkPB merged_message;
  ASSERT_TRUE(ParsePartialFromCord(merged_bytes, &merged_message));

  const google::protobuf::Descriptor* nested_descriptor = part_1_group->GetDescriptor();
  const ProtoType* proto_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(nested_descriptor, &proto_type));

  auto result = ReadField("optional_group", FieldFormat::DEFAULT_FORMAT,
                          proto_type, Value::Null(proto_type), merged_bytes);

  ZETASQL_ASSERT_OK(result.status());
  const Value& output_value = result.value();
  ASSERT_EQ(output_value.type_kind(), TYPE_PROTO);

  KitchenSinkPB::OptionalGroup output_group;
  ASSERT_TRUE(ParsePartialFromCord(output_value.ToCord(), &output_group));
  EXPECT_EQ(output_group.optionalgroupnested_size(), 2);
  EXPECT_THAT(output_group, EqualsProto(merged_message.optional_group()));
}

TEST_P(ReadProtoFieldsTest, Group) {
  KitchenSinkPB::OptionalGroup* group = kitchen_sink_.mutable_optionalgroup();
  group->set_int64_val(10);
  group->set_string_val("foo");

  const google::protobuf::Descriptor* group_descriptor = group->GetDescriptor();
  const ProtoType* proto_type;
  ZETASQL_ASSERT_OK(type_factory_.MakeProtoType(group_descriptor, &proto_type));

  auto result = ReadField("optionalgroup", FieldFormat::DEFAULT_FORMAT,
                          proto_type, Value::Null(proto_type));

  ZETASQL_ASSERT_OK(result.status());
  const Value& output_value = result.value();
  ASSERT_EQ(output_value.type_kind(), TYPE_PROTO);

  KitchenSinkPB::OptionalGroup output_group;
  ASSERT_TRUE(ParseFromCord(output_value.ToCord(), &output_group));
  EXPECT_THAT(output_group, EqualsProto(*group));
}

TEST_P(ReadProtoFieldsTest, Date) {
  kitchen_sink_.set_date(10);
  EXPECT_THAT(
      ReadField("date", FieldFormat::DATE, types::DateType(), values::Date(0)),
      IsOkAndHolds(values::Date(10)));
}

TEST_P(ReadProtoFieldsTest, DateOutOfRange) {
  kitchen_sink_.set_date(-100 * 1000 * 1000);
  EXPECT_THAT(
      ReadField("date", FieldFormat::DATE, types::DateType(), values::Date(0)),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Failed to interpret value for field "
                         "zetasql_test__.KitchenSinkPB.date with field "
                         "format DATE: -100000000")));

  kitchen_sink_.set_date(100 * 1000 * 1000);
  EXPECT_THAT(
      ReadField("date", FieldFormat::DATE, types::DateType(), values::Date(0)),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Failed to interpret value for field "
                         "zetasql_test__.KitchenSinkPB.date with field "
                         "format DATE: 100000000")));
}

TEST_P(ReadProtoFieldsTest, NullDateDecimal) {
  // For DATE_DECIMAL, 0 represents NULL.
  kitchen_sink_.set_date_decimal(0);
  EXPECT_THAT(ReadField("date_decimal", FieldFormat::DATE_DECIMAL,
                        types::DateType(), values::Date(10)),
              IsOkAndHolds(values::NullDate()));
}

TEST_P(ReadProtoFieldsTest, DecodeDateDecimalFails) {
  kitchen_sink_.set_date_decimal(-100 * 1000 * 1000);
  EXPECT_THAT(
      ReadField("date_decimal", FieldFormat::DATE_DECIMAL, types::DateType(),
                values::Date(10)),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Failed to interpret value for field "
                         "zetasql_test__.KitchenSinkPB.date_decimal with "
                         "field format DATE_DECIMAL: -100000000")));
}

TEST_P(ReadProtoFieldsTest, DateDecimal) {
  kitchen_sink_.set_date_decimal(19700111);
  EXPECT_THAT(ReadField("date_decimal", FieldFormat::DATE_DECIMAL,
                        types::DateType(), values::Date(0)),
              IsOkAndHolds(values::Date(10)));
}

TEST_P(ReadProtoFieldsTest, TimestampSeconds) {
  kitchen_sink_.set_timestamp_seconds(10);
  EXPECT_THAT(
      ReadField("timestamp_seconds", FieldFormat::TIMESTAMP_SECONDS,
                types::TimestampType(), values::TimestampFromUnixMicros(0)),
      IsOkAndHolds(values::TimestampFromUnixMicros(10 * 1000 * 1000)));
}

TEST_P(ReadProtoFieldsTest, TimestampSecondsOutOfRange) {
  kitchen_sink_.set_timestamp_seconds(std::numeric_limits<int64_t>::lowest());
  EXPECT_THAT(
      ReadField("timestamp_seconds", FieldFormat::TIMESTAMP_SECONDS,
                types::TimestampType(), values::TimestampFromUnixMicros(0)),
      StatusIs(
          absl::StatusCode::kOutOfRange,
          HasSubstr("Failed to interpret value for field "
                    "zetasql_test__.KitchenSinkPB.timestamp_seconds with "
                    "field format TIMESTAMP_SECONDS: -9223372036854775808")));

  kitchen_sink_.set_timestamp_seconds(std::numeric_limits<int64_t>::max());
  EXPECT_THAT(
      ReadField("timestamp_seconds", FieldFormat::TIMESTAMP_SECONDS,
                types::TimestampType(), values::TimestampFromUnixMicros(0)),
      StatusIs(
          absl::StatusCode::kOutOfRange,
          HasSubstr("Failed to interpret value for field "
                    "zetasql_test__.KitchenSinkPB.timestamp_seconds with "
                    "field format TIMESTAMP_SECONDS: 9223372036854775807")));
}

TEST_P(ReadProtoFieldsTest, TimestampMillis) {
  kitchen_sink_.set_timestamp_millis(10);
  EXPECT_THAT(
      ReadField("timestamp_millis", FieldFormat::TIMESTAMP_MILLIS,
                types::TimestampType(), values::TimestampFromUnixMicros(0)),
      IsOkAndHolds(values::TimestampFromUnixMicros(10 * 1000)));
}

TEST_P(ReadProtoFieldsTest, TimestampMillisOutOfRange) {
  kitchen_sink_.set_timestamp_millis(std::numeric_limits<int64_t>::lowest());
  EXPECT_THAT(
      ReadField("timestamp_millis", FieldFormat::TIMESTAMP_MILLIS,
                types::TimestampType(), values::TimestampFromUnixMicros(0)),
      StatusIs(
          absl::StatusCode::kOutOfRange,
          HasSubstr("Failed to interpret value for field "
                    "zetasql_test__.KitchenSinkPB.timestamp_millis with "
                    "field format TIMESTAMP_MILLIS: -9223372036854775808")));

  kitchen_sink_.set_timestamp_millis(std::numeric_limits<int64_t>::max());
  EXPECT_THAT(
      ReadField("timestamp_millis", FieldFormat::TIMESTAMP_MILLIS,
                types::TimestampType(), values::TimestampFromUnixMicros(0)),
      StatusIs(
          absl::StatusCode::kOutOfRange,
          HasSubstr("Failed to interpret value for field "
                    "zetasql_test__.KitchenSinkPB.timestamp_millis with "
                    "field format TIMESTAMP_MILLIS: 9223372036854775807")));
}

TEST_P(ReadProtoFieldsTest, TimestampMicros) {
  kitchen_sink_.set_timestamp_micros(10);
  EXPECT_THAT(
      ReadField("timestamp_micros", FieldFormat::TIMESTAMP_MICROS,
                types::TimestampType(), values::TimestampFromUnixMicros(0)),
      IsOkAndHolds(values::TimestampFromUnixMicros(10)));
}

TEST_P(ReadProtoFieldsTest, TimestampMicrosOutOfRange) {
  kitchen_sink_.set_timestamp_micros(std::numeric_limits<int64_t>::lowest());
  EXPECT_THAT(
      ReadField("timestamp_micros", FieldFormat::TIMESTAMP_MICROS,
                types::TimestampType(), values::TimestampFromUnixMicros(0)),
      StatusIs(
          absl::StatusCode::kOutOfRange,
          HasSubstr("Failed to interpret value for field "
                    "zetasql_test__.KitchenSinkPB.timestamp_micros with "
                    "field format TIMESTAMP_MICROS: -9223372036854775808")));

  kitchen_sink_.set_timestamp_micros(std::numeric_limits<int64_t>::max());
  EXPECT_THAT(
      ReadField("timestamp_micros", FieldFormat::TIMESTAMP_MICROS,
                types::TimestampType(), values::TimestampFromUnixMicros(0)),
      StatusIs(
          absl::StatusCode::kOutOfRange,
          HasSubstr("Failed to interpret value for field "
                    "zetasql_test__.KitchenSinkPB.timestamp_micros with "
                    "field format TIMESTAMP_MICROS: 9223372036854775807")));
}

TEST_P(ReadProtoFieldsTest, TimestampMicrosFromUint64) {
  kitchen_sink_.set_timestamp_uint64(10);
  EXPECT_THAT(
      ReadField("timestamp_uint64", FieldFormat::TIMESTAMP_MICROS,
                types::TimestampType(), values::TimestampFromUnixMicros(0)),
      IsOkAndHolds(values::TimestampFromUnixMicros(10)));
}

TEST_P(ReadProtoFieldsTest, TimestampMicrosFromUint64BitCast) {
  kitchen_sink_.set_timestamp_uint64(std::numeric_limits<uint64_t>::max());
  EXPECT_THAT(
      ReadField("timestamp_uint64", FieldFormat::TIMESTAMP_MICROS,
                types::TimestampType(), values::TimestampFromUnixMicros(0)),
      IsOkAndHolds(values::TimestampFromUnixMicros(-1)));
}

TEST_P(ReadProtoFieldsTest, Time) {
  kitchen_sink_.set_int64_val(10);
  EXPECT_THAT(ReadField("int64_val", FieldFormat::TIME_MICROS,
                        types::TimeType(), values::TimeFromPacked64Micros(0)),
              IsOkAndHolds(values::TimeFromPacked64Micros(10)));
}

TEST_P(ReadProtoFieldsTest, TimeOutOfRange) {
  kitchen_sink_.set_int64_val(std::numeric_limits<int64_t>::max());
  EXPECT_THAT(
      ReadField("int64_val", FieldFormat::TIME_MICROS, types::TimeType(),
                values::TimeFromPacked64Micros(0)),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Failed to interpret value for field "
                         "zetasql_test__.KitchenSinkPB.int64_val with field "
                         "format TIME_MICROS: 9223372036854775807")));
}

TEST_P(ReadProtoFieldsTest, DateTime) {
  kitchen_sink_.set_int64_val(138630961515462656);
  EXPECT_THAT(ReadField("int64_val", FieldFormat::DATETIME_MICROS,
                        types::DatetimeType(), values::NullDatetime()),
              IsOkAndHolds(values::Datetime(DatetimeValue())));
}

TEST_P(ReadProtoFieldsTest, InvalidDateTime) {
  kitchen_sink_.set_int64_val(-1000);
  EXPECT_THAT(ReadField("int64_val", FieldFormat::DATETIME_MICROS,
                        types::DatetimeType(), values::NullDatetime()),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Failed to interpret value for field "
                                 "zetasql_test__.KitchenSinkPB.int64_val with "
                                 "field format DATETIME_MICROS: -1000")));
}

TEST_P(ReadProtoFieldsTest, Uint64Datetime) {
  kitchen_sink_.set_uint64_val(138630961515462656);
  EXPECT_THAT(ReadField("uint64_val", FieldFormat::DATETIME_MICROS,
                        types::DatetimeType(), values::NullDatetime()),
              IsOkAndHolds(values::Datetime(DatetimeValue())));
}

TEST_P(ReadProtoFieldsTest, LargeUint64Datetime) {
  kitchen_sink_.set_uint64_val(uint64_t{1} << 60);
  EXPECT_THAT(
      ReadField("uint64_val", FieldFormat::DATETIME_MICROS,
                types::DatetimeType(), values::NullDatetime()),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Failed to interpret value for field "
                         "zetasql_test__.KitchenSinkPB.uint64_val with "
                         "field format DATETIME_MICROS: 1152921504606846976")));
}

TEST_P(ReadProtoFieldsTest, GetHasBitForPresentOptionalField) {
  kitchen_sink_.set_int32_val(10);

  EXPECT_THAT(
      ReadHasBit("int32_val", FieldFormat::DEFAULT_FORMAT, types::Int32Type()),
      IsOkAndHolds(values::Bool(true)));
}

TEST_P(ReadProtoFieldsTest, MissingOptionalField) {
  EXPECT_THAT(ReadField("int32_val", FieldFormat::DEFAULT_FORMAT,
                        types::Int32Type(), values::Int32(999)),
              IsOkAndHolds(values::Int32(999)));

  EXPECT_THAT(
      ReadHasBit("int32_val", FieldFormat::DEFAULT_FORMAT, types::Int32Type()),
      IsOkAndHolds(values::Bool(false)));
}

TEST_P(ReadProtoFieldsTest, OptionalFieldLastValueTakesPrecedence) {
  kitchen_sink_.set_int32_val(1);
  absl::Cord bytes1 = SerializePartialToCord(kitchen_sink_);

  kitchen_sink_.set_int32_val(2);
  absl::Cord bytes2 = SerializePartialToCord(kitchen_sink_);

  absl::Cord bytes = bytes1;
  bytes.Append(bytes2);

  EXPECT_THAT(ReadField("int32_val", FieldFormat::DEFAULT_FORMAT,
                        types::Int32Type(), values::Int32(999), bytes),
              IsOkAndHolds(values::Int32(2)));
}

TEST_P(ReadProtoFieldsTest, MissingRequiredField) {
  EXPECT_THAT(ReadField("int64_key_1", FieldFormat::DEFAULT_FORMAT,
                        types::Int64Type(), values::Int64(999)),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Protocol buffer missing required field "
                                 "zetasql_test__.KitchenSinkPB.int64_key_1")));

  EXPECT_THAT(ReadHasBit("int64_key_1", FieldFormat::DEFAULT_FORMAT,
                         types::Int64Type()),
              IsOkAndHolds(values::Bool(false)));
}

TEST_P(ReadProtoFieldsTest, PresentRequiredField) {
  kitchen_sink_.set_int64_key_1(1);
  EXPECT_THAT(ReadField("int64_key_1", FieldFormat::DEFAULT_FORMAT,
                        types::Int64Type(), values::Int64(0)),
              IsOkAndHolds(values::Int64(1)));

  EXPECT_THAT(ReadHasBit("int64_key_1", FieldFormat::DEFAULT_FORMAT,
                         types::Int64Type()),
              IsOkAndHolds(values::Bool(true)));
}

TEST_P(ReadProtoFieldsTest, Repeated) {
  const Value ignored_default_value =
      Value::Array(types::Int32ArrayType(), {values::Int32(999)});

  EXPECT_THAT(ReadField("repeated_int32_val", FieldFormat::DEFAULT_FORMAT,
                        types::Int32ArrayType(), ignored_default_value),
              IsOkAndHolds(Value::Array(types::Int32ArrayType(), {})));

  EXPECT_THAT(ReadHasBit("repeated_int32_val", FieldFormat::DEFAULT_FORMAT,
                         types::Int32ArrayType()),
              IsOkAndHolds(values::Bool(false)));

  kitchen_sink_.add_repeated_int32_val(10);
  EXPECT_THAT(
      ReadField("repeated_int32_val", FieldFormat::DEFAULT_FORMAT,
                types::Int32ArrayType(), ignored_default_value),
      IsOkAndHolds(Value::Array(types::Int32ArrayType(), {values::Int32(10)})));

  EXPECT_THAT(ReadHasBit("repeated_int32_val", FieldFormat::DEFAULT_FORMAT,
                         types::Int32ArrayType()),
              IsOkAndHolds(values::Bool(true)));

  kitchen_sink_.add_repeated_int32_val(20);
  EXPECT_THAT(
      ReadField("repeated_int32_val", FieldFormat::DEFAULT_FORMAT,
                types::Int32ArrayType(), ignored_default_value),
      IsOkAndHolds(Value::Array(types::Int32ArrayType(),
                                {values::Int32(10), values::Int32(20)})));

  EXPECT_THAT(ReadHasBit("repeated_int32_val", FieldFormat::DEFAULT_FORMAT,
                         types::Int32ArrayType()),
              IsOkAndHolds(values::Bool(true)));
}

TEST_P(ReadProtoFieldsTest, PackedRepeated) {
  const Value ignored_default_value =
      Value::Array(types::Int32ArrayType(), {values::Int32(999)});

  EXPECT_THAT(ReadField("repeated_int32_packed", FieldFormat::DEFAULT_FORMAT,
                        types::Int32ArrayType(), ignored_default_value),
              IsOkAndHolds(Value::Array(types::Int32ArrayType(), {})));

  EXPECT_THAT(ReadHasBit("repeated_int32_packed", FieldFormat::DEFAULT_FORMAT,
                         types::Int32ArrayType()),
              IsOkAndHolds(values::Bool(false)));

  kitchen_sink_.add_repeated_int32_packed(10);
  EXPECT_THAT(
      ReadField("repeated_int32_packed", FieldFormat::DEFAULT_FORMAT,
                types::Int32ArrayType(), ignored_default_value),
      IsOkAndHolds(Value::Array(types::Int32ArrayType(), {values::Int32(10)})));

  EXPECT_THAT(ReadHasBit("repeated_int32_packed", FieldFormat::DEFAULT_FORMAT,
                         types::Int32ArrayType()),
              IsOkAndHolds(values::Bool(true)));

  kitchen_sink_.add_repeated_int32_packed(20);
  EXPECT_THAT(
      ReadField("repeated_int32_packed", FieldFormat::DEFAULT_FORMAT,
                types::Int32ArrayType(), ignored_default_value),
      IsOkAndHolds(Value::Array(types::Int32ArrayType(),
                                {values::Int32(10), values::Int32(20)})));

  EXPECT_THAT(ReadHasBit("repeated_int32_packed", FieldFormat::DEFAULT_FORMAT,
                         types::Int32ArrayType()),
              IsOkAndHolds(values::Bool(true)));
}

TEST_P(ReadProtoFieldsTest, IgnoreAField) {
  // Set two fields but only read one. We should ignore the other one.
  kitchen_sink_.set_int64_key_1(1);
  kitchen_sink_.set_int64_key_2(2);
  EXPECT_THAT(ReadField("int64_key_1", FieldFormat::DEFAULT_FORMAT,
                        types::Int64Type(), values::Int64(0)),
              IsOkAndHolds(values::Int64(1)));
}

TEST_P(ReadProtoFieldsTest, ReadTwoFieldsAndOneMissingRequiredFieldTwice) {
  kitchen_sink_.set_int64_key_1(1);
  // Don't set int64_key_2, even though it is required.
  kitchen_sink_.set_int64_val(20);

  absl::Cord bytes = SerializePartialToCord(kitchen_sink_);

  const google::protobuf::FieldDescriptor* field_descriptor1 =
      kitchen_sink_.GetDescriptor()->FindFieldByName("int64_key_1");
  ASSERT_TRUE(field_descriptor1 != nullptr);

  const google::protobuf::FieldDescriptor* field_descriptor2 =
      kitchen_sink_.GetDescriptor()->FindFieldByName("int64_key_2");
  ASSERT_TRUE(field_descriptor1 != nullptr);

  const google::protobuf::FieldDescriptor* field_descriptor3 =
      kitchen_sink_.GetDescriptor()->FindFieldByName("int64_val");
  ASSERT_TRUE(field_descriptor1 != nullptr);

  std::vector<ProtoFieldInfo> infos;

  // infos[0] accesses int64_key_1, a present required field.
  ProtoFieldInfo info;
  info.descriptor = field_descriptor1;
  info.format = FieldFormat::DEFAULT_FORMAT;
  info.type = types::Int64Type();
  info.default_value = Value::Int64(0);
  infos.push_back(info);

  // infos[1] accesses int64_key_2, a missing required field.
  info.descriptor = field_descriptor2;
  infos.push_back(info);

  // infos[2] accesses int64_val, a present optional field,
  info = infos[0];
  info.descriptor = field_descriptor3;
  infos.push_back(info);

  // infos[3,4,5] access the has bits for the above fields.
  ASSERT_EQ(infos.size(), 3);
  for (int i = 0; i < 3; ++i) {
    info = infos[i];
    info.get_has_bit = true;
    infos.push_back(info);
  }
  ASSERT_EQ(infos.size(), 6);

  // infos[6..11] repeat all the above accesses again.
  const int num_infos = infos.size();
  for (int i = 0; i < num_infos; ++i) {
    infos.push_back(infos[i]);
  }

  std::vector<const ProtoFieldInfo*> info_ptrs;
  for (const ProtoFieldInfo& info : infos) {
    info_ptrs.push_back(&info);
  }
  ASSERT_EQ(info_ptrs.size(), 12);

  ProtoFieldValueList value_list;
  ZETASQL_ASSERT_OK(ReadProtoFields(info_ptrs, bytes, &value_list));
  EXPECT_EQ(value_list.size(), 12);

  EXPECT_THAT(value_list[0], IsOkAndHolds(Value::Int64(1)));
  EXPECT_THAT(value_list[1],
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Protocol buffer missing required field "
                                 "zetasql_test__.KitchenSinkPB.int64_key_2")));
  EXPECT_THAT(value_list[2], IsOkAndHolds(Value::Int64(20)));
  EXPECT_THAT(value_list[3], IsOkAndHolds(Value::Bool(true)));
  EXPECT_THAT(value_list[4], IsOkAndHolds(Value::Bool(false)));
  EXPECT_THAT(value_list[5], IsOkAndHolds(Value::Bool(true)));

  EXPECT_THAT(value_list[6], IsOkAndHolds(Value::Int64(1)));
  EXPECT_THAT(value_list[7],
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Protocol buffer missing required field "
                                 "zetasql_test__.KitchenSinkPB.int64_key_2")));
  EXPECT_THAT(value_list[8], IsOkAndHolds(Value::Int64(20)));
  EXPECT_THAT(value_list[9], IsOkAndHolds(Value::Bool(true)));
  EXPECT_THAT(value_list[10], IsOkAndHolds(Value::Bool(false)));
  EXPECT_THAT(value_list[11], IsOkAndHolds(Value::Bool(true)));
}

TEST_P(ReadProtoFieldsTest, SameFieldWithTwoFormats) {
  kitchen_sink_.set_date(10);

  absl::Cord bytes = SerializePartialToCord(kitchen_sink_);

  const google::protobuf::FieldDescriptor* field_descriptor =
      kitchen_sink_.GetDescriptor()->FindFieldByName("date");
  ASSERT_TRUE(field_descriptor != nullptr);

  ProtoFieldInfo info1;
  info1.descriptor = field_descriptor;
  info1.format = FieldFormat::DEFAULT_FORMAT;
  info1.type = types::Int32Type();
  info1.default_value = values::Int32(0);

  ProtoFieldInfo info2;
  info2.descriptor = field_descriptor;
  info2.format = FieldFormat::DATE;
  info2.type = types::DateType();
  info2.default_value = values::NullDate();

  ProtoFieldValueList value_list;
  ZETASQL_ASSERT_OK(ReadProtoFields({&info1, &info2}, bytes, &value_list));
  EXPECT_EQ(value_list.size(), 2);

  EXPECT_THAT(value_list[0], IsOkAndHolds(values::Int32(10)));
  EXPECT_THAT(value_list[1], IsOkAndHolds(values::Date(10)));
}

TEST(GetProtoFieldDefault, Interval) {
  ProtoWithIntervalField proto;
  ProtoFieldDefaultOptions options;
  const google::protobuf::FieldDescriptor* interval_field =
      proto.GetDescriptor()->FindFieldByName("interval_value");
  Value default_value;
  ZETASQL_ASSERT_OK(GetProtoFieldDefault(options, interval_field, types::IntervalType(),
                                 &default_value));
  ASSERT_EQ(default_value.interval_value().ToString(), "0-0 0 0:0:0");
}

INSTANTIATE_TEST_SUITE_P(ReadProtoFieldsTestInstantiation, ReadProtoFieldsTest,
                         ::testing::Values(false, true));

}  // namespace
}  // namespace zetasql
