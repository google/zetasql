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

#include "zetasql/testing/type_util.h"

#include "zetasql/base/logging.h"
#include "google/protobuf/timestamp.pb.h"
#include "google/protobuf/wrappers.pb.h"
#include "google/type/date.pb.h"
#include "google/type/latlng.pb.h"
#include "google/type/timeofday.pb.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/public/type.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/testdata/test_proto3.pb.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "zetasql/base/status.h"

namespace zetasql {
namespace testing {

bool HasFloatingPointNumber(const zetasql::Type* type) {
  if (type->IsArray()) {
    return HasFloatingPointNumber(type->AsArray()->element_type());
  } else if (type->IsStruct()) {
    for (int i = 0; i < type->AsStruct()->num_fields(); i++) {
      if (HasFloatingPointNumber(type->AsStruct()->field(i).type)) {
        return true;
      }
    }
    return false;
  } else if (type->kind() == zetasql::TYPE_DOUBLE ||
             type->kind() == zetasql::TYPE_FLOAT) {
    return true;
  } else {
    return false;
  }
}

std::vector<const Type*> ZetaSqlComplexTestTypes(
    zetasql::TypeFactory* type_factory) {
  std::vector<const Type*> output;
  std::vector<const google::protobuf::Descriptor*> proto_descriptors = {
      zetasql_test__::KitchenSinkPB::descriptor(),
      google::protobuf::Timestamp::descriptor(),
      google::type::Date::descriptor(),
      google::type::TimeOfDay::descriptor(),
      google::type::LatLng::descriptor(),
      google::protobuf::DoubleValue::descriptor(),
      google::protobuf::FloatValue::descriptor(),
      google::protobuf::Int64Value::descriptor(),
      google::protobuf::UInt64Value::descriptor(),
      google::protobuf::Int32Value::descriptor(),
      google::protobuf::UInt32Value::descriptor(),
      google::protobuf::BoolValue::descriptor(),
      google::protobuf::StringValue::descriptor(),
      google::protobuf::BytesValue::descriptor()};
  for (const auto& descriptor : proto_descriptors) {
    const Type* proto_type;
    ZETASQL_CHECK_OK(type_factory->MakeProtoType(descriptor, &proto_type));
    output.push_back(proto_type);
  }

  std::vector<const google::protobuf::EnumDescriptor*> enum_descriptors = {
      zetasql_test__::TestEnum_descriptor()};
  for (const auto& descriptor : enum_descriptors) {
    const Type* enum_type;
    ZETASQL_CHECK_OK(type_factory->MakeEnumType(descriptor, &enum_type));
    output.push_back(enum_type);
  }

  const Type* struct_int64_type;
  ZETASQL_CHECK_OK(type_factory->MakeStructType(
      {{"int64_val", zetasql::types::Int64Type()}}, &struct_int64_type));
  output.push_back(struct_int64_type);

  return output;
}

std::vector<std::string> ZetaSqlTestProtoFilepaths() {
  return {"zetasql/testdata/test_schema.proto",
          "zetasql/testdata/test_proto3.proto",
          "google/protobuf/timestamp.proto",
          "google/protobuf/wrappers.proto",
          "google/type/latlng.proto",
          "google/type/timeofday.proto",
          "google/type/date.proto"};
}

std::vector<std::string> ZetaSqlTestProtoNames() {
  return {"zetasql_test__.KitchenSinkPB",
          "zetasql_test__.MessageWithMapField",
          "zetasql_test__.MessageWithMapField.StringInt32MapEntry",
          "zetasql_test__.CivilTimeTypesSinkPB",
          "zetasql_test__.RecursiveMessage",
          "zetasql_test__.Proto3KitchenSink",
          "zetasql_test__.Proto3KitchenSink.Nested",
          "zetasql_test__.Proto3MessageWithNulls",
          "zetasql_test__.EmptyMessage",
          "zetasql_test__.Proto3TestExtraPB",
          "google.protobuf.Timestamp",
          "google.type.Date",
          "google.type.TimeOfDay",
          "google.type.LatLng",
          "google.protobuf.DoubleValue",
          "google.protobuf.FloatValue",
          "google.protobuf.Int64Value",
          "google.protobuf.UInt64Value",
          "google.protobuf.Int32Value",
          "google.protobuf.UInt32Value",
          "google.protobuf.BoolValue",
          "google.protobuf.StringValue",
          "google.protobuf.BytesValue"};
}

std::vector<std::string> ZetaSqlRandomTestProtoNames() {
  return {"zetasql_test__.KitchenSinkPB",
          "google.protobuf.Timestamp",
          "google.type.Date",
          "google.type.TimeOfDay",
          "google.type.LatLng",
          "google.protobuf.DoubleValue",
          "google.protobuf.FloatValue",
          "google.protobuf.Int64Value",
          "google.protobuf.UInt64Value",
          "google.protobuf.Int32Value",
          "google.protobuf.UInt32Value",
          "google.protobuf.BoolValue",
          "google.protobuf.StringValue",
          "google.protobuf.BytesValue"};
}

std::vector<std::string> ZetaSqlTestEnumNames() {
  return {"zetasql_test__.TestEnum", "zetasql_test__.AnotherTestEnum"};
}

}  // namespace testing
}  // namespace zetasql
