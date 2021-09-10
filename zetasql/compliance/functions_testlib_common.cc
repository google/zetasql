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

#include "zetasql/compliance/functions_testlib_common.h"

#include <cstdint>
#include <string>

#include "google/protobuf/timestamp.pb.h"
#include "google/type/date.pb.h"
#include "google/type/latlng.pb.h"
#include "google/type/timeofday.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/text_format.h"
#include "zetasql/common/status_payload_utils.h"
#include "zetasql/common/testing/testing_proto_util.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/testdata/test_schema.pb.h"
#include "zetasql/testing/using_test_value.cc"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"

namespace zetasql {
namespace {

Value ProtoToValue(const ProtoType* type, const google::protobuf::Message& msg) {
  return Value::Proto(type, SerializeToCord(msg));
}

}  // namespace

static TypeFactory* s_type_factory_ = nullptr;

TypeFactory* type_factory() {
  if (!s_type_factory_) {
    s_type_factory_ = new TypeFactory();
  }
  return s_type_factory_;
}

const EnumType* TestEnumType() {
  const EnumType* enum_type;
  const google::protobuf::EnumDescriptor* enum_descriptor =
      zetasql_test__::TestEnum_descriptor();
  ZETASQL_CHECK_OK(type_factory()->MakeEnumType(enum_descriptor, &enum_type));
  return enum_type;
}

const ProtoType* KitchenSinkProtoType() {
  const ProtoType* kitchen_sink_proto_type;
  ZETASQL_CHECK_OK(type_factory()->MakeProtoType(
      zetasql_test__::KitchenSinkPB::descriptor(), &kitchen_sink_proto_type));
  return kitchen_sink_proto_type;
}

const ProtoType* Proto3TimestampType() {
  const ProtoType* proto3_timestamp_type;
  ZETASQL_CHECK_OK(type_factory()->MakeProtoType(
      google::protobuf::Timestamp::descriptor(), &proto3_timestamp_type));
  return proto3_timestamp_type;
}

const ProtoType* Proto3DateType() {
  const ProtoType* proto3_date_type;
  ZETASQL_CHECK_OK(type_factory()->MakeProtoType(
      google::type::Date::descriptor(), &proto3_date_type));
  return proto3_date_type;
}

const ProtoType* Proto3LatLngType() {
  const ProtoType* proto3_latlng_type;
  ZETASQL_CHECK_OK(type_factory()->MakeProtoType(
      google::type::LatLng::descriptor(), &proto3_latlng_type));
  return proto3_latlng_type;
}

const ProtoType* Proto3TimeOfDayType() {
  const ProtoType* proto3_time_of_day_type;
  ZETASQL_CHECK_OK(type_factory()->MakeProtoType(
      google::type::TimeOfDay::descriptor(), &proto3_time_of_day_type));
  return proto3_time_of_day_type;
}

const ProtoType* CivilTimeTypesSinkProtoType() {
  const ProtoType* civil_time_types_sink_proto_type;
  ZETASQL_CHECK_OK(type_factory()->MakeProtoType(
      zetasql_test__::CivilTimeTypesSinkPB::descriptor(),
      &civil_time_types_sink_proto_type));
  return civil_time_types_sink_proto_type;
}

const ProtoType* NullableIntProtoType() {
  const ProtoType* nullable_int_proto_type;
  ZETASQL_CHECK_OK(type_factory()->MakeProtoType(
      zetasql_test__::NullableInt::descriptor(), &nullable_int_proto_type));
  return nullable_int_proto_type;
}

const StructType* SimpleStructType() {
  const StructType* struct_type;
  ZETASQL_CHECK_OK(type_factory()->MakeStructType(
      {{"a", StringType()}, {"b", Int32Type()}}, &struct_type));
  return struct_type;
}

const StructType* AnotherStructType() {
  const StructType* struct_type;
  ZETASQL_CHECK_OK(type_factory()->MakeStructType(
      {{"a", StringType()}}, &struct_type));
  return struct_type;
}

Value DateFromStr(absl::string_view str) {
  int32_t date;
  ZETASQL_CHECK_OK(functions::ConvertStringToDate(str, &date));
  return Value::Date(date);
}
Value TimestampFromStr(absl::string_view str, functions::TimestampScale scale) {
  absl::Time timestamp;
  // Test cases use consistently same timezone.
  ZETASQL_CHECK_OK(functions::ConvertStringToTimestamp(
      str, absl::UTCTimeZone(), scale, true /* allow_tz_in_str */, &timestamp));
  return Value::Timestamp(timestamp);
}
Value DatetimeFromStr(absl::string_view str, functions::TimestampScale scale) {
  DatetimeValue datetime;
  ZETASQL_CHECK_OK(functions::ConvertStringToDatetime(str, scale, &datetime));
  return Value::Datetime(datetime);
}
Value TimeFromStr(absl::string_view str, functions::TimestampScale scale) {
  TimeValue time;
  ZETASQL_CHECK_OK(functions::ConvertStringToTime(str, scale, &time));
  return Value::Time(time);
}

Value Timestamp(int64_t v) { return Value::TimestampFromUnixMicros(v); }
Value Timestamp(absl::Time v) { return Value::Timestamp(v); }
Value TimeMicros(int hour, int minute, int second, int microseconds) {
  return Value::Time(TimeValue::FromHMSAndMicros(
      hour, minute, second, microseconds));
}
Value TimeNanos(int hour, int minute, int second, int nanoseconds) {
  return Value::Time(TimeValue::FromHMSAndNanos(
      hour, minute, second, nanoseconds));
}
Value DatetimeMicros(int year, int month, int day, int hour, int minute,
                     int second, int microseconds) {
  return Value::Datetime(DatetimeValue::FromYMDHMSAndMicros(
      year, month, day, hour, minute, second, microseconds));
}
Value DatetimeNanos(int year, int month, int day, int hour, int minute,
                     int second, int nanoseconds) {
  return Value::Datetime(DatetimeValue::FromYMDHMSAndNanos(
      year, month, day, hour, minute, second, nanoseconds));
}
Value KitchenSink(const std::string& proto_str) {
  zetasql_test__::KitchenSinkPB kitchen_sink_message;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(proto_str,
                                            &kitchen_sink_message));
  return ProtoToValue(KitchenSinkProtoType(), kitchen_sink_message);
}

Value Proto3Timestamp(int64_t seconds, int32_t nanos) {
  google::protobuf::Timestamp proto3_timestamp;
  proto3_timestamp.set_seconds(seconds);
  proto3_timestamp.set_nanos(nanos);

  return ProtoToValue(Proto3TimestampType(), proto3_timestamp);
}

Value Proto3Date(int32_t year, int32_t month, int32_t day) {
  google::type::Date proto3_date;
  proto3_date.set_year(year);
  proto3_date.set_month(month);
  proto3_date.set_day(day);
  return ProtoToValue(Proto3DateType(), proto3_date);
}

Value Proto3LatLng(double latitude, double longitude) {
  google::type::LatLng lat_lng;
  lat_lng.set_latitude(latitude);
  lat_lng.set_longitude(longitude);

  return ProtoToValue(Proto3LatLngType(), lat_lng);
}

Value Proto3TimeOfDay(int32_t hour, int32_t minute, int32_t seconds,
                      int32_t nanos) {
  google::type::TimeOfDay proto3_time;
  proto3_time.set_hours(hour);
  proto3_time.set_minutes(minute);
  proto3_time.set_seconds(seconds);
  proto3_time.set_nanos(nanos);

  return ProtoToValue(Proto3TimeOfDayType(), proto3_time);
}

Value CivilTimeTypesSink(const std::string& proto_str) {
  zetasql_test__::CivilTimeTypesSinkPB civil_time_types_sink_message;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(proto_str,
                                            &civil_time_types_sink_message));
  return ProtoToValue(CivilTimeTypesSinkProtoType(),
                      civil_time_types_sink_message);
}

Value NullableInt(const std::string& proto_str) {
  zetasql_test__::NullableInt nullable_int_message;
  ZETASQL_CHECK(google::protobuf::TextFormat::ParseFromString(proto_str,
                                            &nullable_int_message));
  return ProtoToValue(NullableIntProtoType(), nullable_int_message);
}

// Each returned row (vector) contains distinct values of a certain type.
std::vector<std::vector<Value>> GetRowsOfValues() {
  const StructType* struct_type = SimpleStructType();  // a: string, b: int32_t
  const Value struct0 = Value::Struct(struct_type, {String("foo"), Int32(0)});
  const Value struct1 = Value::Struct(struct_type, {String("bar"), Int32(1)});
  const Value array0 = Value::Array(Int64ArrayType(), {Int64(0), Int64(1)});
  const Value array1 = Value::Array(Int64ArrayType(), {Int64(10), Int64(11)});
  const Value enum0 = Value::Enum(TestEnumType(), 1);
  const Value enum1 = Value::Enum(TestEnumType(), 2);

  std::vector<std::vector<Value>> v = {
      {Int32(1), Int32(2)},
      {Int64(1), Int64(2)},
      {Uint32(1), Uint32(2)},
      {Uint64(1), Uint64(2)},
      {True(), False()},
      {Float(1.3), Float(1.5)},
      {Double(1.3), Double(1.5)},
      {String("yay"), String("nay")},
      {Bytes("yay"), Bytes("nay")},
      {Timestamp(0), Timestamp(1)},
      {Date(0), Date(1)},
      {struct0, struct1},
      {array0, array1},
      {enum0, enum1},
      {KitchenSink("int64_key_1: 1 int64_key_2: 2"),
       KitchenSink("int64_key_1: 10 int64_key_2: 20")},
      {NumericFromDouble(3.14), Numeric(-100)},
      {BigNumericFromDouble(3.14), BigNumeric(-100)}};
  return v;
}

CivilTimeTestCase::CivilTimeTestCase(
    const std::vector<ValueConstructor>& input,
    const absl::StatusOr<Value>& micros_output,
    const absl::StatusOr<Value>& nanos_output, const Type* output_type,
    const std::set<LanguageFeature>& required_features)
    : input(input),
      micros_output(micros_output),
      nanos_output(nanos_output),
      required_features(required_features) {
  this->output_type = nullptr;
  if (micros_output.status().ok()) {
    this->output_type = micros_output.value().type();
  }
  if (nanos_output.status().ok()) {
    if (this->output_type != nullptr) {
      ZETASQL_CHECK(this->output_type->Equals(nanos_output.value().type()));
    } else {
      this->output_type = nanos_output.value().type();
    }
  }
  if (output_type != nullptr) {
    if (this->output_type != nullptr) {
      ZETASQL_CHECK(this->output_type->Equals(output_type));
    } else {
      this->output_type = output_type;
    }
  } else {
    std::string test_info("\ninput: ");
    bool add_comma = false;
    for (const ValueConstructor& input_argument : input) {
      absl::StrAppend(&test_info, (add_comma ? "," : ""),
                      input_argument.get().DebugString());
      add_comma = true;
    }
    absl::StrAppend(
        &test_info, "\nmicros_output: ",
        (micros_output.status().ok()
             ? micros_output.value().DebugString()
             : zetasql::internal::StatusToString(micros_output.status())));
    absl::StrAppend(
        &test_info, "\nnanos_output: ",
        (nanos_output.status().ok()
             ? nanos_output.value().DebugString()
             : zetasql::internal::StatusToString(nanos_output.status())));
    absl::StrAppend(&test_info, "\noutput_type: ",
                    (output_type == nullptr ? "nullptr" :
                     output_type->DebugString()));
    // The constructor must set a non-null output_type for this test case.
    ZETASQL_CHECK(this->output_type != nullptr)
        << "The test 'output_type' cannot be nullptr.  This can potentially "
        << "happen if both the micros and nanos results are errors, in which "
        << "case the output type must be explicitly passed into the "
        << "constructor as per the constructor contract.\nThis test info:\n"
        << test_info;
  }
}

QueryParamsWithResult WrapResultForCivilTimeAndNanos(
    const CivilTimeTestCase& test_case) {
  using Result = QueryParamsWithResult::Result;
  const Result micros_result = test_case.micros_output.status().ok()
                                   ? Result(test_case.micros_output.value())
                                   : Result(Value::Null(test_case.output_type),
                                            test_case.micros_output.status());
  const Result nanos_result = test_case.nanos_output.status().ok()
                                  ? Result(test_case.nanos_output.value())
                                  : Result(Value::Null(test_case.output_type),
                                           test_case.nanos_output.status());

  using FeatureSet = QueryParamsWithResult::FeatureSet;
  FeatureSet micros_features = test_case.required_features;
  micros_features.insert(FEATURE_V_1_2_CIVIL_TIME);

  FeatureSet nanos_features = micros_features;
  nanos_features.insert(FEATURE_TIMESTAMP_NANOS);

  return QueryParamsWithResult(
      test_case.input,
      {{micros_features, micros_result}, {nanos_features, nanos_result}});
}

void AddTestCaseWithWrappedResultForCivilTimeAndNanos(
    const CivilTimeTestCase& test_case,
    std::vector<QueryParamsWithResult>* result) {
  result->push_back(WrapResultForCivilTimeAndNanos(test_case));
}

QueryParamsWithResult WrapResultForNumeric(
    const std::vector<ValueConstructor>& params,
    const QueryParamsWithResult::Result& result) {
  QueryParamsWithResult::FeatureSet numeric_feature_set;
  numeric_feature_set.insert(FEATURE_NUMERIC_TYPE);
  QueryParamsWithResult::ResultMap result_map;
  result_map.emplace(numeric_feature_set, QueryParamsWithResult::Result(
                                              result.result, result.status));
  return QueryParamsWithResult(params, result_map);
}

QueryParamsWithResult WrapResultForBigNumeric(
    const std::vector<ValueConstructor>& params,
    const QueryParamsWithResult::Result& result) {
  QueryParamsWithResult::FeatureSet bignumeric_feature_set;
  bignumeric_feature_set.insert(FEATURE_BIGNUMERIC_TYPE);
  QueryParamsWithResult::ResultMap result_map;
  result_map.emplace(bignumeric_feature_set, QueryParamsWithResult::Result(
                                                 result.result, result.status));
  return QueryParamsWithResult(params, result_map);
}

QueryParamsWithResult WrapResultForInterval(
    const std::vector<ValueConstructor>& params,
    const QueryParamsWithResult::Result& result) {
  QueryParamsWithResult::FeatureSet interval_feature_set;
  interval_feature_set.insert(FEATURE_INTERVAL_TYPE);
  QueryParamsWithResult::ResultMap result_map;
  result_map.emplace(interval_feature_set, QueryParamsWithResult::Result(
                                               result.result, result.status));
  return QueryParamsWithResult(params, result_map);
}

const std::string EscapeKey(bool sql_standard_mode, const std::string& key) {
  if (sql_standard_mode) {
    return absl::StrCat(".\"", key, "\"");
  } else {
    return absl::StrCat("['", key, "']");
  }
}

Value StringToBytes(const Value& value) {
  ZETASQL_CHECK_EQ(value.type_kind(), TYPE_STRING);
  if (value.is_null()) {
    return Value::NullBytes();
  } else {
    return Value::Bytes(value.string_value());
  }
}

}  // namespace zetasql
