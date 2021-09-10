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

#include "zetasql/testing/sql_types_test.h"

#include "zetasql/base/logging.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "absl/memory/memory.h"
#include "absl/strings/cord.h"
#include "absl/time/time.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/status.h"

namespace zetasql {

void ZetaSQLTypesTest::SetUp() {
  language_options_.EnableLanguageFeature(
      FEATURE_V_1_1_CAST_DIFFERENT_ARRAY_TYPES);
  coercer_ = absl::make_unique<Coercer>(&type_factory_, absl::UTCTimeZone(),
                                        &language_options_);

  enum_value = Value::Enum(GetTestEnumType(), 1);
  enum_null = Value::Null(GetTestEnumType());
  proto_value = Value::Proto(GetKitchenSinkNestedProtoType(), absl::Cord("1"));
  proto_null = Value::Null(GetKitchenSinkNestedProtoType());
  array_int32_value = Value::EmptyArray(GetInt32ArrayType());
  array_int64_value = Value::EmptyArray(GetInt64ArrayType());
  array_struct_value = Value::EmptyArray(GetStructArrayType());
  array_int32_null = Value::Null(GetInt32ArrayType());
  array_int64_null = Value::Null(GetInt64ArrayType());
  array_struct_null = Value::Null(GetStructArrayType());
  struct_value = Value::Struct(
      GetSimpleStructType(), {Value::String("aaa"), Value::Bytes("bbb")});
  struct_null = Value::Null(GetSimpleStructType());

#define RESET_ARGS(ltype, utype)                                           \
  ltype##_arg.reset(new InputArgumentType(utype##Type(), false));          \
  ltype##_literal_arg.reset(new InputArgumentType(ltype##_value));         \
  ltype##_parameter_arg.reset(new InputArgumentType(utype##Type(), true)); \
  ltype##_null_arg.reset(new InputArgumentType(ltype##_null));

  // Instantiates non-literal, literal, and null arguments of specified
  // type.
  RESET_ARGS(bool, Bool);
  RESET_ARGS(int32, Int32);
  RESET_ARGS(int64, Int64);
  RESET_ARGS(uint32, Uint32);
  RESET_ARGS(uint64, Uint64);
  RESET_ARGS(float, Float);
  RESET_ARGS(double, Double);
  RESET_ARGS(numeric, Numeric);
  RESET_ARGS(bignumeric, BigNumeric);
  RESET_ARGS(string, String);
  RESET_ARGS(bytes, Bytes);
  RESET_ARGS(date, Date);
  RESET_ARGS(time, Time);
  RESET_ARGS(datetime, Datetime);
  RESET_ARGS(timestamp, Timestamp);
  RESET_ARGS(geography, types::Geography);

  enum_arg = absl::make_unique<InputArgumentType>(GetTestEnumType(),
                                                  false /* is_parameter */);
  enum_parameter_arg = absl::make_unique<InputArgumentType>(
      GetTestEnumType(), true /* is_parameter */);
  enum_literal_arg = absl::make_unique<InputArgumentType>(enum_value);
  enum_null_arg = absl::make_unique<InputArgumentType>(enum_null);

  proto_arg = absl::make_unique<InputArgumentType>(
      GetKitchenSinkNestedProtoType(), false /* is_parameter */);
  proto_parameter_arg = absl::make_unique<InputArgumentType>(
      GetKitchenSinkNestedProtoType(), true /* is_parameter */);
  proto_literal_arg = absl::make_unique<InputArgumentType>(proto_value);
  proto_null_arg = absl::make_unique<InputArgumentType>(proto_null);

  array_int32_arg = absl::make_unique<InputArgumentType>(
      GetInt32ArrayType(), false /* is_parameter */);
  array_int32_parameter_arg = absl::make_unique<InputArgumentType>(
      GetInt32ArrayType(), true /* is_parameter */);
  array_int32_literal_arg =
      absl::make_unique<InputArgumentType>(array_int32_value);
  array_int32_null_arg = absl::make_unique<InputArgumentType>(array_int32_null);

  array_int64_arg =
      absl::make_unique<InputArgumentType>(GetInt64ArrayType(),
                                           /*is_parameter=*/false);
  array_int64_parameter_arg =
      absl::make_unique<InputArgumentType>(GetInt64ArrayType(),
                                           /*is_parameter=*/true);
  array_int64_literal_arg =
      absl::make_unique<InputArgumentType>(array_int64_value);
  array_int64_null_arg = absl::make_unique<InputArgumentType>(array_int64_null);

  array_struct_arg =
      absl::make_unique<InputArgumentType>(GetStructArrayType(),
                                           /*is_parameter=*/false);
  array_struct_parameter_arg =
      absl::make_unique<InputArgumentType>(GetStructArrayType(),
                                           /*is_parameter=*/true);
  array_struct_literal_arg =
      absl::make_unique<InputArgumentType>(array_struct_value);
  array_struct_null_arg =
      absl::make_unique<InputArgumentType>(array_struct_null);

  std::vector<InputArgumentType> field_types;
  const StructType* struct_type = GetSimpleStructType();
  for (int i = 0; i < struct_type->num_fields(); ++i) {
    field_types.push_back(InputArgumentType(struct_type->field(i).type));
  }
  struct_arg = absl::make_unique<InputArgumentType>(struct_type, field_types);
  struct_parameter_arg = absl::make_unique<InputArgumentType>(
      struct_type, true /* is_parameter */);

  struct_literal_arg = absl::make_unique<InputArgumentType>(struct_value);
  struct_null_arg = absl::make_unique<InputArgumentType>(struct_null);

  untyped_null_arg_ = absl::make_unique<InputArgumentType>();

  zetasql_base::InsertOrDie(&null_of_type_, BOOL.type(), &BOOL_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, INT32.type(), &INT32_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, INT64.type(), &INT64_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, UINT32.type(), &UINT32_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, UINT64.type(), &UINT64_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, FLOAT.type(), &FLOAT_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, DOUBLE.type(), &DOUBLE_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, NUMERIC.type(), &NUMERIC_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, BIGNUMERIC.type(), &BIGNUMERIC_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, STRING.type(), &STRING_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, BYTES.type(), &BYTES_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, DATE.type(), &DATE_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, TIME.type(), &TIME_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, DATETIME.type(), &DATETIME_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, TIMESTAMP.type(), &TIMESTAMP_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, GEOGRAPHY.type(), &GEOGRAPHY_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, ENUM.type(), &ENUM_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, PROTO.type(), &PROTO_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, ARRAY_INT32.type(), &ARRAY_INT32_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, ARRAY_INT64.type(), &ARRAY_INT64_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, ARRAY_STRUCT.type(), &ARRAY_STRUCT_NULL);
  zetasql_base::InsertOrDie(&null_of_type_, STRUCT.type(), &STRUCT_NULL);

  zetasql_base::InsertOrDie(&literal_of_type_, BOOL.type(), &BOOL_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, INT32.type(), &INT32_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, INT64.type(), &INT64_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, UINT32.type(), &UINT32_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, UINT64.type(), &UINT64_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, FLOAT.type(), &FLOAT_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, DOUBLE.type(), &DOUBLE_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, NUMERIC.type(), &NUMERIC_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, BIGNUMERIC.type(), &BIGNUMERIC_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, STRING.type(), &STRING_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, BYTES.type(), &BYTES_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, DATE.type(), &DATE_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, TIME.type(), &TIME_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, DATETIME.type(), &DATETIME_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, TIMESTAMP.type(), &TIMESTAMP_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, ENUM.type(), &ENUM_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, PROTO.type(), &PROTO_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, ARRAY_INT32.type(), &ARRAY_INT32_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, ARRAY_INT64.type(), &ARRAY_INT64_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, ARRAY_STRUCT.type(),
                   &ARRAY_STRUCT_LITERAL);
  zetasql_base::InsertOrDie(&literal_of_type_, STRUCT.type(), &STRUCT_LITERAL);

  zetasql_base::InsertOrDie(&parameter_of_type_, BOOL.type(), &BOOL_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, INT32.type(), &INT32_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, INT64.type(), &INT64_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, UINT32.type(), &UINT32_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, UINT64.type(), &UINT64_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, FLOAT.type(), &FLOAT_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, DOUBLE.type(), &DOUBLE_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, NUMERIC.type(), &NUMERIC_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, BIGNUMERIC.type(),
                   &BIGNUMERIC_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, STRING.type(), &STRING_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, BYTES.type(), &BYTES_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, DATE.type(), &DATE_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, TIME.type(), &TIME_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, DATETIME.type(), &DATETIME_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, TIMESTAMP.type(), &TIMESTAMP_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, ENUM.type(), &ENUM_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, PROTO.type(), &PROTO_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, ARRAY_INT32.type(),
                   &ARRAY_INT32_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, ARRAY_INT64.type(),
                   &ARRAY_INT64_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, ARRAY_STRUCT.type(),
                   &ARRAY_STRUCT_PARAMETER);
  zetasql_base::InsertOrDie(&parameter_of_type_, STRUCT.type(), &STRUCT_PARAMETER);

  all_non_literal_args_ = {
      &BOOL,        &INT32,        &INT64,      &UINT32, &UINT64, &FLOAT,
      &DOUBLE,      &NUMERIC,      &BIGNUMERIC, &STRING, &BYTES,  &DATE,
      &TIME,        &DATETIME,     &TIMESTAMP,  &ENUM,   &PROTO,  &ARRAY_INT32,
      &ARRAY_INT64, &ARRAY_STRUCT, &STRUCT};

  all_literal_args_ = {
      &BOOL_LITERAL,        &INT32_LITERAL,        &INT64_LITERAL,
      &UINT32_LITERAL,      &UINT64_LITERAL,       &FLOAT_LITERAL,
      &DOUBLE_LITERAL,      &NUMERIC_LITERAL,      &BIGNUMERIC_LITERAL,
      &STRING_LITERAL,      &BYTES_LITERAL,        &DATE_LITERAL,
      &TIME_LITERAL,        &DATETIME_LITERAL,     &TIMESTAMP_LITERAL,
      &ENUM_LITERAL,        &PROTO_LITERAL,        &ARRAY_INT32_LITERAL,
      &ARRAY_INT64_LITERAL, &ARRAY_STRUCT_LITERAL, &STRUCT_LITERAL};

  all_parameter_args_ = {
      &BOOL_PARAMETER,        &INT32_PARAMETER,        &INT64_PARAMETER,
      &UINT32_PARAMETER,      &UINT64_PARAMETER,       &FLOAT_PARAMETER,
      &DOUBLE_PARAMETER,      &NUMERIC_PARAMETER,      &BIGNUMERIC_PARAMETER,
      &STRING_PARAMETER,      &BYTES_PARAMETER,        &DATE_PARAMETER,
      &TIME_PARAMETER,        &DATETIME_PARAMETER,     &TIMESTAMP_PARAMETER,
      &ENUM_PARAMETER,        &PROTO_PARAMETER,        &ARRAY_INT32_PARAMETER,
      &ARRAY_INT64_PARAMETER, &ARRAY_STRUCT_PARAMETER, &STRUCT_PARAMETER};

  all_null_args_ = {
      &BOOL_NULL,         &INT32_NULL,    &INT64_NULL,       &UINT32_NULL,
      &UINT64_NULL,       &FLOAT_NULL,    &DOUBLE_NULL,      &NUMERIC_NULL,
      &BIGNUMERIC_NULL,   &STRING_NULL,   &BYTES_NULL,       &DATE_NULL,
      &TIME_NULL,         &DATETIME_NULL, &TIMESTAMP_NULL,   &GEOGRAPHY_NULL,
      &ENUM_NULL,         &PROTO_NULL,    &ARRAY_INT32_NULL, &ARRAY_INT64_NULL,
      &ARRAY_STRUCT_NULL, &STRUCT_NULL,   &UNTYPED_NULL};

  all_literal_and_null_args_ = all_literal_args_;
  all_literal_and_null_args_.insert(all_literal_and_null_args_.end(),
                                    all_null_args_.begin(),
                                    all_null_args_.end());

  all_types_ = {BOOL.type(),
                INT32.type(),
                INT64.type(),
                UINT32.type(),
                UINT64.type(),
                FLOAT.type(),
                DOUBLE.type(),
                NUMERIC.type(),
                BIGNUMERIC.type(),
                STRING.type(),
                BYTES.type(),
                DATE.type(),
                TIME.type(),
                DATETIME.type(),
                TIMESTAMP.type(),
                GEOGRAPHY.type(),
                ARRAY_INT32.type(),
                ARRAY_INT64.type(),
                ARRAY_STRUCT.type(),
                GetTestEnumType(),
                GetAnotherTestEnumType(),
                GetKitchenSinkNestedProtoType(),
                GetKitchenSinkNestedDatesProtoType(),
                GetSimpleStructType()};
}

const ArrayType* ZetaSQLTypesTest::GetInt32ArrayType() {
  if (int32_array_type_ == nullptr) {
    ZETASQL_CHECK_OK(type_factory_.MakeArrayType(type_factory_.get_int32(),
                                         &int32_array_type_));
  }
  return int32_array_type_;
}

const ArrayType* ZetaSQLTypesTest::GetInt64ArrayType() {
  if (int64_array_type_ == nullptr) {
    ZETASQL_CHECK_OK(type_factory_.MakeArrayType(type_factory_.get_int64(),
                                         &int64_array_type_));
  }
  return int64_array_type_;
}

const ArrayType* ZetaSQLTypesTest::GetStructArrayType() {
  if (struct_array_type_ == nullptr) {
    ZETASQL_CHECK_OK(type_factory_.MakeArrayType(GetSimpleStructType(),
                                         &struct_array_type_));
  }
  return struct_array_type_;
}

const EnumType* ZetaSQLTypesTest::GetTestEnumType() {
  if (enum_type_ == nullptr) {
    const google::protobuf::EnumDescriptor* enum_descriptor =
        zetasql_test__::TestEnum_descriptor();
    ZETASQL_CHECK_OK(type_factory_.MakeEnumType(enum_descriptor, &enum_type_));
  }
  return enum_type_;
}

const EnumType* ZetaSQLTypesTest::GetAnotherTestEnumType() {
  if (another_enum_type_ == nullptr) {
    const google::protobuf::EnumDescriptor* enum_descriptor =
        zetasql_test__::AnotherTestEnum_descriptor();
    ZETASQL_CHECK_OK(type_factory_.MakeEnumType(enum_descriptor, &another_enum_type_));
  }
  return another_enum_type_;
}

const ProtoType* ZetaSQLTypesTest::GetKitchenSinkNestedProtoType() {
  if (kitchen_sink_nested_proto_type_ == nullptr) {
    ZETASQL_CHECK_OK(type_factory_.MakeProtoType(
        zetasql_test__::KitchenSinkPB::Nested::descriptor(),
        &kitchen_sink_nested_proto_type_));
  }
  return kitchen_sink_nested_proto_type_;
}

const ProtoType* ZetaSQLTypesTest::GetKitchenSinkNestedDatesProtoType() {
  if (kitchen_sink_nested_dates_proto_type_ == nullptr) {
    ZETASQL_CHECK_OK(type_factory_.MakeProtoType(
        zetasql_test__::KitchenSinkPB::NestedDates::descriptor(),
        &kitchen_sink_nested_dates_proto_type_));
  }
  return kitchen_sink_nested_dates_proto_type_;
}

const StructType* ZetaSQLTypesTest::GetSimpleStructType() {
  if (simple_struct_type_ == nullptr) {
    const Type* string_type = type_factory_.get_string();
    const Type* bytes_type = type_factory_.get_bytes();
    ZETASQL_CHECK_OK(type_factory_.MakeStructType(
        {{"a", string_type}, {"b", bytes_type}}, &simple_struct_type_));
  }
  return simple_struct_type_;
}

void ZetaSQLTypesTest::GetSampleTestTypes(
    std::vector<const Type*>* sample_types) {
  GetSimpleTypes(sample_types);

  sample_types->push_back(GetInt32ArrayType());
  sample_types->push_back(GetStructArrayType());
  sample_types->push_back(GetTestEnumType());
  sample_types->push_back(GetAnotherTestEnumType());
  sample_types->push_back(GetKitchenSinkNestedProtoType());
  sample_types->push_back(GetKitchenSinkNestedDatesProtoType());
  sample_types->push_back(GetSimpleStructType());
}

std::vector<const Type*> ZetaSQLTypesTest::GetSampleTestTypes() {
  std::vector<const Type*> result;
  GetSampleTestTypes(&result);
  return result;
}

void ZetaSQLTypesTest::GetSimpleTypes(
    std::vector<const Type*>* simple_types) {
  for (int i = TypeKind::TYPE_UNKNOWN; i < TypeKind_ARRAYSIZE; i++) {
    TypeKind kind = static_cast<TypeKind>(i);
    const Type* type = types::TypeFromSimpleTypeKind(kind);
    if (type == nullptr) {
      continue;
    }

    simple_types->push_back(type);
  }
}

std::vector<const Type*> ZetaSQLTypesTest::GetSimpleTypes() {
  std::vector<const Type*> result;
  GetSimpleTypes(&result);
  return result;
}

}  // namespace zetasql
