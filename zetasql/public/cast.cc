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

#include "zetasql/public/cast.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/arena.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/common/utf_util.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/functions/cast_date_time.h"
#include "zetasql/public/functions/convert.h"
#include "zetasql/public/functions/convert_proto.h"
#include "zetasql/public/functions/convert_string.h"
#include "zetasql/public/functions/convert_string_with_format.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/functions/string.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/proto_value_conversion.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.pb.h"
#include <cstdint>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_split.h"
#include "absl/time/time.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

#define MAX_LITERAL_DISPLAY_LENGTH 60

namespace {

functions::TimestampScale GetTimestampScale(
    const LanguageOptions& language_options) {
  if (language_options.LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
    return functions::kNanoseconds;
  } else {
    return functions::kMicroseconds;
  }
}

void AddToCastMap(TypeKind from, TypeKind to, CastFunctionType type,
                  CastHashMap* map) {
  zetasql_base::InsertIfNotPresent(map, {from, to},
                          {type, Type::GetTypeCoercionCost(to, from)});
}

const CastHashMap* InitializeZetaSQLCasts() {
  CastHashMap* map = new CastHashMap();

  const CastFunctionType IMPLICIT = CastFunctionType::IMPLICIT;
  const CastFunctionType EXPLICIT = CastFunctionType::EXPLICIT;
  const CastFunctionType EXPLICIT_OR_LITERAL =
      CastFunctionType::EXPLICIT_OR_LITERAL;
  const CastFunctionType EXPLICIT_OR_LITERAL_OR_PARAMETER =
      CastFunctionType::EXPLICIT_OR_LITERAL_OR_PARAMETER;

#define ADD_TO_MAP(from_type, to_type, cast_type) \
  AddToCastMap(TYPE_##from_type, TYPE_##to_type, cast_type, map);

  // Note that by convention, all type kinds are currently castable to
  // themselves as IMPLICIT.

  ADD_TO_MAP(BOOL,       BOOL,       IMPLICIT);
  ADD_TO_MAP(BOOL,       INT32,      EXPLICIT);
  ADD_TO_MAP(BOOL,       INT64,      EXPLICIT);
  ADD_TO_MAP(BOOL,       UINT32,     EXPLICIT);
  ADD_TO_MAP(BOOL,       UINT64,     EXPLICIT);
  ADD_TO_MAP(BOOL,       STRING,     EXPLICIT);

  ADD_TO_MAP(INT32,      BOOL,       EXPLICIT);
  ADD_TO_MAP(INT32,      INT32,      IMPLICIT);
  ADD_TO_MAP(INT32,      INT64,      IMPLICIT);
  ADD_TO_MAP(INT32,      UINT32,     EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(INT32,      UINT64,     EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(INT32,      FLOAT,      EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(INT32,      DOUBLE,     IMPLICIT);
  ADD_TO_MAP(INT32,      STRING,     EXPLICIT);
  ADD_TO_MAP(INT32,      ENUM,       EXPLICIT_OR_LITERAL_OR_PARAMETER);
  ADD_TO_MAP(INT32,      NUMERIC,    IMPLICIT);
  ADD_TO_MAP(INT32,      BIGNUMERIC, IMPLICIT);

  ADD_TO_MAP(INT64,      BOOL,       EXPLICIT);
  ADD_TO_MAP(INT64,      INT32,      EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(INT64,      INT64,      IMPLICIT);
  ADD_TO_MAP(INT64,      UINT32,     EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(INT64,      UINT64,     EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(INT64,      FLOAT,      EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(INT64,      DOUBLE,     IMPLICIT);
  ADD_TO_MAP(INT64,      STRING,     EXPLICIT);
  ADD_TO_MAP(INT64,      ENUM,       EXPLICIT_OR_LITERAL_OR_PARAMETER);
  ADD_TO_MAP(INT64,      NUMERIC,    IMPLICIT);
  ADD_TO_MAP(INT64,      BIGNUMERIC, IMPLICIT);

  ADD_TO_MAP(UINT32,     BOOL,       EXPLICIT);
  ADD_TO_MAP(UINT32,     INT32,      EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(UINT32,     INT64,      IMPLICIT);
  ADD_TO_MAP(UINT32,     UINT32,     IMPLICIT);
  ADD_TO_MAP(UINT32,     UINT64,     IMPLICIT);
  ADD_TO_MAP(UINT32,     FLOAT,      EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(UINT32,     DOUBLE,     IMPLICIT);
  ADD_TO_MAP(UINT32,     STRING,     EXPLICIT);
  ADD_TO_MAP(UINT32,     ENUM,       EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(UINT32,     NUMERIC,    IMPLICIT);
  ADD_TO_MAP(UINT32,     BIGNUMERIC, IMPLICIT);

  ADD_TO_MAP(UINT64,     BOOL,       EXPLICIT);
  ADD_TO_MAP(UINT64,     INT32,      EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(UINT64,     INT64,      EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(UINT64,     UINT32,     EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(UINT64,     UINT64,     IMPLICIT);
  ADD_TO_MAP(UINT64,     FLOAT,      EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(UINT64,     DOUBLE,     IMPLICIT);
  ADD_TO_MAP(UINT64,     STRING,     EXPLICIT);
  ADD_TO_MAP(UINT64,     ENUM,       EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(UINT64,     NUMERIC,    IMPLICIT);
  ADD_TO_MAP(UINT64,     BIGNUMERIC, IMPLICIT);

  ADD_TO_MAP(NUMERIC,    INT32,      EXPLICIT);
  ADD_TO_MAP(NUMERIC,    INT64,      EXPLICIT);
  ADD_TO_MAP(NUMERIC,    UINT32,     EXPLICIT);
  ADD_TO_MAP(NUMERIC,    UINT64,     EXPLICIT);
  ADD_TO_MAP(NUMERIC,    FLOAT,      EXPLICIT);
  ADD_TO_MAP(NUMERIC,    DOUBLE,     IMPLICIT);
  ADD_TO_MAP(NUMERIC,    STRING,     EXPLICIT);
  ADD_TO_MAP(NUMERIC,    NUMERIC,    IMPLICIT);
  ADD_TO_MAP(NUMERIC,    BIGNUMERIC, IMPLICIT);

  ADD_TO_MAP(BIGNUMERIC, INT32,      EXPLICIT);
  ADD_TO_MAP(BIGNUMERIC, INT64,      EXPLICIT);
  ADD_TO_MAP(BIGNUMERIC, UINT32,     EXPLICIT);
  ADD_TO_MAP(BIGNUMERIC, UINT64,     EXPLICIT);
  ADD_TO_MAP(BIGNUMERIC, FLOAT,      EXPLICIT);
  ADD_TO_MAP(BIGNUMERIC, DOUBLE,     IMPLICIT);
  ADD_TO_MAP(BIGNUMERIC, STRING,     EXPLICIT);
  ADD_TO_MAP(BIGNUMERIC, NUMERIC,    EXPLICIT);
  ADD_TO_MAP(BIGNUMERIC, BIGNUMERIC, IMPLICIT);

  ADD_TO_MAP(FLOAT,      INT32,      EXPLICIT);
  ADD_TO_MAP(FLOAT,      INT64,      EXPLICIT);
  ADD_TO_MAP(FLOAT,      UINT32,     EXPLICIT);
  ADD_TO_MAP(FLOAT,      UINT64,     EXPLICIT);
  ADD_TO_MAP(FLOAT,      FLOAT,      IMPLICIT);
  ADD_TO_MAP(FLOAT,      DOUBLE,     IMPLICIT);
  ADD_TO_MAP(FLOAT,      STRING,     EXPLICIT);
  ADD_TO_MAP(FLOAT,      NUMERIC,    EXPLICIT);
  ADD_TO_MAP(FLOAT,      BIGNUMERIC, EXPLICIT);

  ADD_TO_MAP(DOUBLE,     INT32,      EXPLICIT);
  ADD_TO_MAP(DOUBLE,     INT64,      EXPLICIT);
  ADD_TO_MAP(DOUBLE,     UINT32,     EXPLICIT);
  ADD_TO_MAP(DOUBLE,     UINT64,     EXPLICIT);
  ADD_TO_MAP(DOUBLE,     FLOAT,      EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(DOUBLE,     DOUBLE,     IMPLICIT);
  ADD_TO_MAP(DOUBLE,     STRING,     EXPLICIT);
  ADD_TO_MAP(DOUBLE,     NUMERIC,    EXPLICIT_OR_LITERAL);
  ADD_TO_MAP(DOUBLE,     BIGNUMERIC, EXPLICIT_OR_LITERAL);

  ADD_TO_MAP(STRING,     INT32,      EXPLICIT);
  ADD_TO_MAP(STRING,     INT64,      EXPLICIT);
  ADD_TO_MAP(STRING,     UINT32,     EXPLICIT);
  ADD_TO_MAP(STRING,     UINT64,     EXPLICIT);
  ADD_TO_MAP(STRING,     FLOAT,      EXPLICIT);
  ADD_TO_MAP(STRING,     DOUBLE,     EXPLICIT);
  ADD_TO_MAP(STRING,     STRING,     IMPLICIT);
  ADD_TO_MAP(STRING,     BYTES,      EXPLICIT);
  ADD_TO_MAP(STRING,     DATE,       EXPLICIT_OR_LITERAL_OR_PARAMETER);
  ADD_TO_MAP(STRING,     TIMESTAMP,  EXPLICIT_OR_LITERAL_OR_PARAMETER);
  ADD_TO_MAP(STRING,     TIME,       EXPLICIT_OR_LITERAL_OR_PARAMETER);
  ADD_TO_MAP(STRING,     DATETIME,   EXPLICIT_OR_LITERAL_OR_PARAMETER);
  ADD_TO_MAP(STRING,     INTERVAL,   EXPLICIT);
  ADD_TO_MAP(STRING,     ENUM,       EXPLICIT_OR_LITERAL_OR_PARAMETER);
  ADD_TO_MAP(STRING,     PROTO,      EXPLICIT_OR_LITERAL_OR_PARAMETER);
  ADD_TO_MAP(STRING,     BOOL,       EXPLICIT);
  ADD_TO_MAP(STRING,     NUMERIC,    EXPLICIT);
  ADD_TO_MAP(STRING,     BIGNUMERIC, EXPLICIT);

  ADD_TO_MAP(BYTES,      BYTES,      IMPLICIT);
  ADD_TO_MAP(BYTES,      STRING,     EXPLICIT);
  ADD_TO_MAP(BYTES,      PROTO,      EXPLICIT_OR_LITERAL_OR_PARAMETER);

  ADD_TO_MAP(DATE,       DATE,       IMPLICIT);
  ADD_TO_MAP(DATE,       DATETIME,   IMPLICIT);
  ADD_TO_MAP(DATE,       TIMESTAMP,  EXPLICIT);
  ADD_TO_MAP(DATE,       STRING,     EXPLICIT);

  ADD_TO_MAP(TIMESTAMP,  DATE,       EXPLICIT);
  ADD_TO_MAP(TIMESTAMP,  DATETIME,   EXPLICIT);
  ADD_TO_MAP(TIMESTAMP,  TIME,       EXPLICIT);
  ADD_TO_MAP(TIMESTAMP,  TIMESTAMP,  IMPLICIT);
  ADD_TO_MAP(TIMESTAMP,  STRING,     EXPLICIT);

  // TODO: Add relevant tests for TIME and DATETIME.

  ADD_TO_MAP(TIME,       TIME,       IMPLICIT);
  ADD_TO_MAP(TIME,       STRING,     EXPLICIT);

  ADD_TO_MAP(DATETIME,   DATE,       EXPLICIT);
  ADD_TO_MAP(DATETIME,   DATETIME,   IMPLICIT);
  ADD_TO_MAP(DATETIME,   STRING,     EXPLICIT);
  ADD_TO_MAP(DATETIME,   TIME,       EXPLICIT);
  ADD_TO_MAP(DATETIME,   TIMESTAMP,  EXPLICIT);

  ADD_TO_MAP(INTERVAL,   INTERVAL,   IMPLICIT);
  ADD_TO_MAP(INTERVAL,   STRING,     EXPLICIT);

  ADD_TO_MAP(GEOGRAPHY,  GEOGRAPHY,  IMPLICIT);

  ADD_TO_MAP(JSON,       JSON,       IMPLICIT);

  ADD_TO_MAP(ENUM,       STRING,     EXPLICIT);

  ADD_TO_MAP(ENUM,       INT32,      EXPLICIT);
  ADD_TO_MAP(ENUM,       INT64,      EXPLICIT);
  ADD_TO_MAP(ENUM,       UINT32,     EXPLICIT);
  ADD_TO_MAP(ENUM,       UINT64,     EXPLICIT);

  ADD_TO_MAP(PROTO,      STRING,     EXPLICIT);
  ADD_TO_MAP(PROTO,      BYTES,      EXPLICIT);

  // The non-simple types show up in this table as IMPLICIT, but coercions of
  // any kind should only be allowed if the types are Equivalent.
  // This must be checked by the caller, like in TypeCoercesTo.
  ADD_TO_MAP(ENUM,       ENUM,       IMPLICIT);
  ADD_TO_MAP(PROTO,      PROTO,      IMPLICIT);
  ADD_TO_MAP(ARRAY,      ARRAY,      IMPLICIT);
  ADD_TO_MAP(STRUCT,     STRUCT,     IMPLICIT);

  return map;
}

// Returns a single uint64_t that represents an (input kind, output kind) pair.
// Useful for switching on a combination of two kinds.
constexpr uint64_t FCT(TypeKind input_kind, TypeKind output_kind) {
  return ((static_cast<uint64_t>(input_kind) << 32) + output_kind);
}

template <typename FromType, typename ToType>
absl::StatusOr<Value> NumericCast(const Value& value) {
  absl::Status status;
  FromType in = value.Get<FromType>();
  ToType out;
  functions::Convert<FromType, ToType>(in, &out, &status);
  if (status.ok()) {
    return Value::Make<ToType>(out);
  } else {
    return status;
  }
}

template <typename FromType, typename ToType>
absl::StatusOr<Value> NumericValueCast(const FromType& in) {
  absl::Status status;
  ToType out;
  functions::Convert<FromType, ToType>(in, &out, &status);
  if (status.ok()) {
    return Value::Make<ToType>(out);
  } else {
    return status;
  }
}

absl::Status CheckLegacyRanges(int64_t timestamp,
                               functions::TimestampScale precision,
                               const std::string& from_type_name,
                               const std::string& from_type_value) {
  int64_t min, max;
  switch (precision) {
    case functions::kNanoseconds:
      min = types::kTimestampNanosMin;
      max = types::kTimestampNanosMax;
      break;
    case functions::kMicroseconds:
      min = types::kTimestampMicrosMin;
      max = types::kTimestampMicrosMax;
      break;
    case functions::kMilliseconds:
      min = types::kTimestampMillisMin;
      max = types::kTimestampMillisMax;
      break;
    case functions::kSeconds:
      min = types::kTimestampSecondsMin;
      max = types::kTimestampSecondsMax;
      break;
  }
  if (timestamp < min || timestamp > max) {
    return MakeEvalError() << "Cast from " << from_type_name << " "
                           << from_type_value << " to "
                           << TimestampScale_Name(precision)
                           << " out of bounds";
  }
  return absl::OkStatus();
}

// Conversion function from a numeric Value to a string Value that
// handles NULL Values but otherwise just wraps the ZetaSQL function
// library function (which does not handle NULL values).
// The function is invoked like:
//   status = NumericToString<int32_t>(value)
//
// Crashes if the Value type does not correspond with <T>.
template <typename T>
absl::StatusOr<Value> NumericToString(const Value& v) {
  if (v.is_null()) return Value::NullString();
  T value = v.Get<T>();
  std::string str;
  absl::Status error;
  if (zetasql::functions::NumericToString<T>(value, &str, &error)) {
    return Value::String(str);
  } else {
    return error;
  }
}

// Conversion function from a numeric Value to a string Value that
// handles NULL Values but otherwise just wraps the ZetaSQL function
// library function (which does not handle NULL values).
// The function is invoked like:
//   status = StringToNumeric<int32_t>(value)
//
// Crashes if the Value <v> is not a string.
template <typename T>
absl::StatusOr<Value> StringToNumeric(const Value& v) {
  if (v.is_null()) return Value::MakeNull<T>();
  const std::string& value = v.string_value();
  T out;
  absl::Status error;
  if (zetasql::functions::StringToNumeric<T>(value, &out, &error)) {
    return Value::Make<T>(out);
  } else {
    return error;
  }
}

// Returns whether this cast is a map-entry cast (see below).
bool IsMapEntryCast(const Type* from, const Type* to) {
  return from->IsStruct() && from->AsStruct()->fields().size() == 2 &&
         to->IsProto() && to->AsProto()->descriptor()->options().map_entry();
}

// Tries to perform a STRUCT->PROTO cast if to_type is a map_entry. See
// (broken link). <from_value> is a two-field struct where
// the fields represent the key and value of the requested map_entry proto
// in <to_type>.
absl::StatusOr<Value> DoMapEntryCast(const Value& from_value,
                                     absl::TimeZone default_timezone,
                                     const LanguageOptions& language_options,
                                     const Type* to_type) {
  ZETASQL_RET_CHECK(IsMapEntryCast(from_value.type(), to_type));

  const ProtoType* to_proto_type = to_type->AsProto();
  TypeFactory type_factory;
  const Type* key_type;
  const Type* value_type;

  ZETASQL_RETURN_IF_ERROR(to_proto_type->GetFieldTypeByTagNumber(
      to_proto_type->map_key()->number(), &type_factory, &key_type));
  ZETASQL_RETURN_IF_ERROR(to_proto_type->GetFieldTypeByTagNumber(
      to_proto_type->map_value()->number(), &type_factory, &value_type));

  ZETASQL_ASSIGN_OR_RETURN(Value key, CastValue(from_value.field(0), default_timezone,
                                        language_options, key_type));
  ZETASQL_ASSIGN_OR_RETURN(Value value, CastValue(from_value.field(1), default_timezone,
                                          language_options, value_type));

  google::protobuf::Arena arena;
  google::protobuf::DynamicMessageFactory factory;
  google::protobuf::Message* message =
      factory.GetPrototype(to_proto_type->descriptor())->New(&arena);

  bool use_wire_format_annotations = true;
  ZETASQL_RETURN_IF_ERROR(MergeValueToProtoField(key, to_proto_type->map_key(),
                                         use_wire_format_annotations, &factory,
                                         message));
  ZETASQL_RETURN_IF_ERROR(MergeValueToProtoField(value, to_proto_type->map_value(),
                                         use_wire_format_annotations, &factory,
                                         message));

  absl::Cord bytes;
  std::string bytes_str;
  ZETASQL_CHECK(message->SerializeToString(&bytes_str));
  bytes = absl::Cord(bytes_str);
  return Value::Proto(to_proto_type, bytes);
}

}  // namespace

bool SupportsImplicitCoercion(CastFunctionType type) {
  return type == CastFunctionType::IMPLICIT;
}

bool SupportsLiteralCoercion(CastFunctionType type) {
  return type == CastFunctionType::IMPLICIT ||
         type == CastFunctionType::EXPLICIT_OR_LITERAL ||
         type == CastFunctionType::EXPLICIT_OR_LITERAL_OR_PARAMETER;
}

bool SupportsParameterCoercion(CastFunctionType type) {
  return type == CastFunctionType::IMPLICIT ||
         type == CastFunctionType::EXPLICIT_OR_LITERAL_OR_PARAMETER;
}

bool SupportsExplicitCast(CastFunctionType type) {
  return type == CastFunctionType::IMPLICIT ||
         type == CastFunctionType::EXPLICIT ||
         type == CastFunctionType::EXPLICIT_OR_LITERAL ||
         type == CastFunctionType::EXPLICIT_OR_LITERAL_OR_PARAMETER;
}

namespace {

// CastContext is an abstract class containing basic set of properties and
// methods needed to execute a cast. Serves as a base class for classes
// responsible for execution of validated (CastValue) and plain
// (CastValueWithoutTypeValidation) casts.
class CastContext {
 public:
  // Deprecated. Use CastContext(default_timezone, current_timestamp,
  // language_options) instead.
  // Otherwise, if the cast is a cast from STRING to DATE, DATETIME or TIMESTAMP
  // with a format string, the cast will fail with error that current timestamp
  // is not set.
  ABSL_DEPRECATED(
      "Use CastContext(default_timezone, current_timestamp, language_options) "
      "instead")
  CastContext(absl::TimeZone default_timezone,
              const LanguageOptions& language_options)
      : default_timezone_(default_timezone),
        language_options_(language_options),
        current_timestamp_(absl::nullopt) {}

  CastContext(absl::TimeZone default_timezone,
              absl::optional<absl::Time> current_timestamp,
              const LanguageOptions& language_options)
      : default_timezone_(default_timezone),
        language_options_(language_options),
        current_timestamp_(current_timestamp) {
    if (current_timestamp_.has_value()) {
      // Extracting the DATE from the current timestamp should never fail since
      // it will be in the supported range 0001-01-01 to 9999-12-31.
      int32_t current_date;
      ZETASQL_CHECK_OK(functions::ExtractFromTimestamp(
          functions::DATE, current_timestamp_.value(),
          default_timezone_, &current_date));
      current_date_ = current_date;
    }
  }

  virtual ~CastContext() {}

  CastContext(const CastContext&) = delete;
  CastContext& operator=(const CastContext&) = delete;

  absl::StatusOr<Value> CastValue(
      const Value& from_value,
      const Type* to_type,
      const absl::optional<std::string>& format = absl::nullopt)
      const;

 protected:
  const absl::TimeZone& default_timezone() const { return default_timezone_; }
  const LanguageOptions& language_options() const { return language_options_; }
  const absl::optional<absl::Time> current_timestamp() const {
    return current_timestamp_;
  }
  const absl::optional<int32_t> current_date() const { return current_date_; }

 private:
  // Executes a cast which involves extended types: source and/or destination
  // type is extended.
  virtual absl::StatusOr<Value> CastWithExtendedType(
      const Value& from_value, const Type* to_type) const = 0;

  // Checks that coercion is valid using Coercer.
  virtual absl::Status ValidateCoercion(const Value& from_value,
                                        const Type* to_type) const = 0;

  const absl::TimeZone default_timezone_;
  const LanguageOptions& language_options_;
  const absl::optional<absl::Time> current_timestamp_;
  absl::optional<int32_t> current_date_;
};

static absl::Status ValidateFormatStringToDate(absl::string_view format) {
  return functions::ValidateFormatStringForParsing(
      format, zetasql::TypeKind::TYPE_DATE);
}

static absl::Status ValidateFormatStringToDatetime(absl::string_view format) {
  return functions::ValidateFormatStringForParsing(
      format, zetasql::TypeKind::TYPE_DATETIME);
}

static absl::Status ValidateFormatStringToTime(absl::string_view format) {
  return functions::ValidateFormatStringForParsing(
      format, zetasql::TypeKind::TYPE_TIME);
}

static absl::Status ValidateFormatStringToTimestamp(absl::string_view format) {
  return functions::ValidateFormatStringForParsing(
      format, zetasql::TypeKind::TYPE_TIMESTAMP);
}

static absl::Status ValidateFormatStringFromDate(absl::string_view format) {
  return functions::ValidateFormatStringForFormatting(format, TYPE_DATE);
}

static absl::Status ValidateFormatStringFromTime(absl::string_view format) {
  return functions::ValidateFormatStringForFormatting(format, TYPE_TIME);
}

static absl::Status ValidateFormatStringFromDateTime(absl::string_view format) {
  return functions::ValidateFormatStringForFormatting(format, TYPE_DATETIME);
}

static absl::Status ValidateFormatStringFromTimestamp(
    absl::string_view format) {
  return functions::ValidateFormatStringForFormatting(format, TYPE_TIMESTAMP);
}

absl::StatusOr<Value> NumericToStringWithFormat(const Value& v,
                                                absl::string_view format,
                                                ProductMode product_mode) {
  if (v.is_null()) {
    return Value::NullString();
  }

  ZETASQL_ASSIGN_OR_RETURN(const std::string str,
                   zetasql::functions::NumericalToStringWithFormat(
                       v, format, product_mode));
  return Value::String(str);
}

absl::StatusOr<Value> CastContext::CastValue(
    const Value& from_value,
    const Type* to_type,
    const absl::optional<std::string>& format) const {
  ZETASQL_RET_CHECK(from_value.is_valid());
  // Use a shorter name inside the body of this method.
  const Value& v = from_value;

  if (v.type()->Equals(to_type)) {
    // Coercion from a value to the exact same type always works.
    return v;
  }

  if (from_value.type()->IsExtendedType() || to_type->IsExtendedType()) {
    return CastWithExtendedType(from_value, to_type);
  }

  // Special case: STRUCT are not generally castable to PROTO, but there is an
  // exception for two-field structs whose fields are castable to the fields
  // of a map_entry protocol buffer (see (broken link)).
  if (language_options().LanguageFeatureEnabled(
          LanguageFeature::FEATURE_V_1_3_PROTO_MAPS) &&
      IsMapEntryCast(from_value.type(), to_type)) {
    return DoMapEntryCast(from_value, default_timezone(), language_options(),
                          to_type);
  }

  // Check to see if the type kinds are castable.
  if (!internal::GetZetaSQLCasts().contains(
          TypeKindPair(v.type_kind(), to_type->kind()))) {
    return MakeSqlError() << "Unsupported cast from " << v.type()->DebugString()
                          << " to " << to_type->DebugString();
  }

  //  NULL handling for Values occurs here.
  if (v.is_null()) {
    if (!v.type()->IsSimpleType() && v.type_kind() == to_type->kind()) {
      // This is a cast of a complex type to a complex type with the same
      // kind.  Type kind checks are not enough to verify that the cast
      // between types is valid (i.e., array to array or struct to struct),
      // so perform a literal coercion check to see if the complex types
      // are compatible and therefore a NULL value can cast from one to
      // the other.
      ZETASQL_RETURN_IF_ERROR(ValidateCoercion(v, to_type));
    }
    // We have already validated that this is a valid cast for NULL values,
    // so just return a NULL value of <to_type>.
    return Value::Null(to_type);
  }

  // TODO: Consider breaking this up, as the switch is extremely
  // large.
  switch (FCT(v.type()->kind(), to_type->kind())) {
    // Numeric casts. Identity casts are handled above.
    case FCT(TYPE_INT32, TYPE_INT64):
      return NumericCast<int32_t, int64_t>(v);
    case FCT(TYPE_INT32, TYPE_UINT32):
      return NumericCast<int32_t, uint32_t>(v);
    case FCT(TYPE_INT32, TYPE_UINT64):
      return NumericCast<int32_t, uint64_t>(v);
    case FCT(TYPE_INT32, TYPE_BOOL):
      return NumericCast<int32_t, bool>(v);
    case FCT(TYPE_INT32, TYPE_FLOAT):
      return NumericCast<int32_t, float>(v);
    case FCT(TYPE_INT32, TYPE_DOUBLE):
      return NumericCast<int32_t, double>(v);
    case FCT(TYPE_INT32, TYPE_STRING):
      if (format.has_value()) {
        return NumericToStringWithFormat(v, format.value(),
                                         language_options().product_mode());
      } else {
        return NumericToString<int32_t>(v);
      }
    case FCT(TYPE_INT32, TYPE_NUMERIC):
      return NumericCast<int32_t, NumericValue>(v);
    case FCT(TYPE_INT32, TYPE_BIGNUMERIC):
      return NumericCast<int32_t, BigNumericValue>(v);

    case FCT(TYPE_UINT32, TYPE_INT32):
      return NumericCast<uint32_t, int32_t>(v);
    case FCT(TYPE_UINT32, TYPE_INT64):
      return NumericCast<uint32_t, int64_t>(v);
    case FCT(TYPE_UINT32, TYPE_UINT64):
      return NumericCast<uint32_t, uint64_t>(v);
    case FCT(TYPE_UINT32, TYPE_BOOL):
      return NumericCast<uint32_t, bool>(v);
    case FCT(TYPE_UINT32, TYPE_FLOAT):
      return NumericCast<uint32_t, float>(v);
    case FCT(TYPE_UINT32, TYPE_DOUBLE):
      return NumericCast<uint32_t, double>(v);
    case FCT(TYPE_UINT32, TYPE_STRING):
      if (format.has_value()) {
        return NumericToStringWithFormat(v, format.value(),
                                         language_options().product_mode());
      } else {
        return NumericToString<uint32_t>(v);
      }
    case FCT(TYPE_UINT32, TYPE_NUMERIC):
      return NumericCast<uint32_t, NumericValue>(v);
    case FCT(TYPE_UINT32, TYPE_BIGNUMERIC):
      return NumericCast<uint32_t, BigNumericValue>(v);

    case FCT(TYPE_INT64, TYPE_INT32):
      return NumericCast<int64_t, int32_t>(v);
    case FCT(TYPE_INT64, TYPE_UINT32):
      return NumericCast<int64_t, uint32_t>(v);
    case FCT(TYPE_INT64, TYPE_UINT64):
      return NumericCast<int64_t, uint64_t>(v);
    case FCT(TYPE_INT64, TYPE_BOOL):
      return NumericCast<int64_t, bool>(v);
    case FCT(TYPE_INT64, TYPE_FLOAT):
      return NumericCast<int64_t, float>(v);
    case FCT(TYPE_INT64, TYPE_DOUBLE):
      return NumericCast<int64_t, double>(v);
    case FCT(TYPE_INT64, TYPE_STRING):
      if (format.has_value()) {
        return NumericToStringWithFormat(v, format.value(),
                                         language_options().product_mode());
      } else {
        return NumericToString<int64_t>(v);
      }
    case FCT(TYPE_INT64, TYPE_NUMERIC):
      return NumericCast<int64_t, NumericValue>(v);
    case FCT(TYPE_INT64, TYPE_BIGNUMERIC):
      return NumericCast<int64_t, BigNumericValue>(v);

    case FCT(TYPE_UINT64, TYPE_INT32):
      return NumericCast<uint64_t, int32_t>(v);
    case FCT(TYPE_UINT64, TYPE_INT64):
      return NumericCast<uint64_t, int64_t>(v);
    case FCT(TYPE_UINT64, TYPE_UINT32):
      return NumericCast<uint64_t, uint32_t>(v);
    case FCT(TYPE_UINT64, TYPE_BOOL):
      return NumericCast<uint64_t, bool>(v);
    case FCT(TYPE_UINT64, TYPE_FLOAT):
      return NumericCast<uint64_t, float>(v);
    case FCT(TYPE_UINT64, TYPE_DOUBLE):
      return NumericCast<uint64_t, double>(v);
    case FCT(TYPE_UINT64, TYPE_STRING):
      if (format.has_value()) {
        return NumericToStringWithFormat(v, format.value(),
                                         language_options().product_mode());
      } else {
        return NumericToString<uint64_t>(v);
      }
    case FCT(TYPE_UINT64, TYPE_NUMERIC):
      return NumericCast<uint64_t, NumericValue>(v);
    case FCT(TYPE_UINT64, TYPE_BIGNUMERIC):
      return NumericCast<uint64_t, BigNumericValue>(v);

    case FCT(TYPE_BOOL, TYPE_INT32):
      return NumericCast<bool, int32_t>(v);
    case FCT(TYPE_BOOL, TYPE_INT64):
      return NumericCast<bool, int64_t>(v);
    case FCT(TYPE_BOOL, TYPE_UINT32):
      return NumericCast<bool, uint32_t>(v);
    case FCT(TYPE_BOOL, TYPE_UINT64):
      return NumericCast<bool, uint64_t>(v);
    case FCT(TYPE_BOOL, TYPE_STRING): return NumericToString<bool>(v);

    case FCT(TYPE_FLOAT, TYPE_INT32):
      return NumericCast<float, int32_t>(v);
    case FCT(TYPE_FLOAT, TYPE_INT64):
      return NumericCast<float, int64_t>(v);
    case FCT(TYPE_FLOAT, TYPE_UINT32):
      return NumericCast<float, uint32_t>(v);
    case FCT(TYPE_FLOAT, TYPE_UINT64):
      return NumericCast<float, uint64_t>(v);
    case FCT(TYPE_FLOAT, TYPE_DOUBLE): return NumericCast<float, double>(v);
    case FCT(TYPE_FLOAT, TYPE_STRING):
      if (format.has_value()) {
        return NumericToStringWithFormat(v, format.value(),
                                         language_options().product_mode());
      } else {
        return NumericToString<float>(v);
      }
    case FCT(TYPE_FLOAT, TYPE_NUMERIC):
      return NumericCast<float, NumericValue>(v);
    case FCT(TYPE_FLOAT, TYPE_BIGNUMERIC):
      return NumericCast<float, BigNumericValue>(v);

    case FCT(TYPE_DOUBLE, TYPE_INT32):
      return NumericCast<double, int32_t>(v);
    case FCT(TYPE_DOUBLE, TYPE_INT64):
      return NumericCast<double, int64_t>(v);
    case FCT(TYPE_DOUBLE, TYPE_UINT32):
      return NumericCast<double, uint32_t>(v);
    case FCT(TYPE_DOUBLE, TYPE_UINT64):
      return NumericCast<double, uint64_t>(v);
    case FCT(TYPE_DOUBLE, TYPE_FLOAT): return NumericCast<double, float>(v);
    case FCT(TYPE_DOUBLE, TYPE_STRING):
      if (format.has_value()) {
        return NumericToStringWithFormat(v, format.value(),
                                         language_options().product_mode());
      } else {
        return NumericToString<double>(v);
      }
    case FCT(TYPE_DOUBLE, TYPE_NUMERIC):
      return NumericCast<double, NumericValue>(v);
    case FCT(TYPE_DOUBLE, TYPE_BIGNUMERIC):
      return NumericCast<double, BigNumericValue>(v);

    case FCT(TYPE_INT32, TYPE_ENUM):
    case FCT(TYPE_INT64, TYPE_ENUM):
    case FCT(TYPE_UINT32, TYPE_ENUM): {
      const Value to_value = Value::Enum(to_type->AsEnum(), v.ToInt64());
      if (!to_value.is_valid()) {
        return MakeEvalError() << "Out of range cast of integer " << v.ToInt64()
                               << " to enum type " << to_type->DebugString();
      }
      return to_value;
    }
    case FCT(TYPE_UINT64, TYPE_ENUM): {
      // Static cast may turn out-of-bound uint64_t's to negative int64_t's which
      // will yield invalid enums.
      const Value to_value = Value::Enum(
          to_type->AsEnum(), static_cast<int64_t>(v.uint64_value()));
      if (!to_value.is_valid()) {
        return MakeEvalError() << "Out of range cast of integer "
                               << v.uint64_value() << " to enum type "
                               << to_type->DebugString();
      }
      return to_value;
    }

    case FCT(TYPE_STRING, TYPE_BOOL): return StringToNumeric<bool>(v);
    case FCT(TYPE_STRING, TYPE_INT32):
      return StringToNumeric<int32_t>(v);
    case FCT(TYPE_STRING, TYPE_INT64):
      return StringToNumeric<int64_t>(v);
    case FCT(TYPE_STRING, TYPE_UINT32):
      return StringToNumeric<uint32_t>(v);
    case FCT(TYPE_STRING, TYPE_UINT64):
      return StringToNumeric<uint64_t>(v);
    case FCT(TYPE_STRING, TYPE_FLOAT): return StringToNumeric<float>(v);
    case FCT(TYPE_STRING, TYPE_DOUBLE): return StringToNumeric<double>(v);
    case FCT(TYPE_STRING, TYPE_NUMERIC):
      return StringToNumeric<NumericValue>(v);
    case FCT(TYPE_STRING, TYPE_BIGNUMERIC):
      return StringToNumeric<BigNumericValue>(v);

    case FCT(TYPE_STRING, TYPE_ENUM): {
      const Value to_value = Value::Enum(to_type->AsEnum(), v.string_value());
      if (!to_value.is_valid()) {
        return MakeEvalError() << "Out of range cast of string '"
                               << v.string_value() << "' to enum type "
                               << to_type->DebugString();
      }
      return to_value;
    }

    case FCT(TYPE_STRING, TYPE_DATE): {
      int32_t date;
      if (format.has_value()) {
        if (!current_date().has_value()) {
          return MakeEvalError() << "current timestamp is not set";
        }

        ZETASQL_RETURN_IF_ERROR(functions::CastStringToDate(
            format.value(), v.string_value(), current_date().value(), &date));
      } else {
        ZETASQL_RETURN_IF_ERROR(
            functions::ConvertStringToDate(v.string_value(), &date));
      }
      return Value::Date(date);
    }
    case FCT(TYPE_STRING, TYPE_TIMESTAMP): {
      // TODO: These should be using the non-deprecated signature
      // that includes an argument to indicate if a timezone is allowed in
      // the string or not.  If not allowed and there is a timezone then
      // an error should be provided.
      if (language_options().LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
        absl::Time timestamp;

        if (format.has_value()) {
          if (!current_timestamp().has_value()) {
            return MakeEvalError() << "current timestamp is not set";
          }

          ZETASQL_RETURN_IF_ERROR(functions::CastStringToTimestamp(
              format.value(),
              v.string_value(), default_timezone(),
              current_timestamp().value(),
              &timestamp));
        } else {
          ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTimestamp(
              v.string_value(), default_timezone(), functions::kNanoseconds,
              /*allow_tz_in_str=*/true, &timestamp));
        }
        return Value::Timestamp(timestamp);
      } else {
        int64_t timestamp;
        if (format.has_value()) {
          if (!current_timestamp().has_value()) {
            return MakeEvalError() << "current timestamp is not set";
          }

          ZETASQL_RETURN_IF_ERROR(functions::CastStringToTimestamp(
              format.value(),
              v.string_value(), default_timezone(),
              current_timestamp().value(),
              &timestamp));
        } else {
          ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTimestamp(
              v.string_value(), default_timezone(), functions::kMicroseconds,
              &timestamp));
        }
        return Value::TimestampFromUnixMicros(timestamp);
      }
    }
    case FCT(TYPE_TIMESTAMP, TYPE_STRING): {
      std::string timestamp;
      if (format.has_value()) {
        ZETASQL_RETURN_IF_ERROR(functions::CastFormatTimestampToString(
            format.value(), v.ToTime(), default_timezone(), &timestamp));
      } else {
        if (language_options().LanguageFeatureEnabled(
                FEATURE_TIMESTAMP_NANOS)) {
          ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToString(
              v.ToTime(), functions::kNanoseconds, default_timezone(),
              &timestamp));
        } else {
          ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToStringWithTruncation(
              v.ToUnixMicros(), functions::kMicroseconds, default_timezone(),
              &timestamp));
        }
      }
      return Value::String(timestamp);
    }
    case FCT(TYPE_DATE, TYPE_TIMESTAMP): {
      int64_t timestamp;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertDateToTimestamp(
          v.date_value(), functions::kMicroseconds, default_timezone(),
          &timestamp));
      return Value::TimestampFromUnixMicros(timestamp);
    }
    case FCT(TYPE_TIMESTAMP, TYPE_DATE): {
      int32_t date;
      ZETASQL_RETURN_IF_ERROR(ExtractFromTimestamp(
          functions::DateTimestampPart::DATE, v.ToUnixMicros(),
          functions::kMicroseconds, default_timezone(), &date));
      return Value::Date(date);
    }
    case FCT(TYPE_STRING, TYPE_BYTES):
      if (format.has_value()) {
        std::string output;
        ZETASQL_RETURN_IF_ERROR(functions::StringToBytes(v.string_value(),
                                                 format.value(), &output));
        return Value::Bytes(output);
      }

      return Value::Bytes(v.string_value());

    case FCT(TYPE_STRING, TYPE_PROTO): {
      if (to_type->AsProto()->descriptor() == nullptr) {
        // TODO: Cannot currently get here, since a ProtoType
        // requires a non-nullptr descriptor.  This may change when we
        // implement  opaque protos.  Additionally, opaque protos may affect
        // the ability to successfully parse or serialize the proto (note
        // also that a fully-defined proto might have a descendant field
        // that is an opaque proto).
        return MakeEvalError()
               << "Invalid cast from string to opaque proto type "
               << to_type->DebugString();
      }
      google::protobuf::DynamicMessageFactory msg_factory;
      std::unique_ptr<google::protobuf::Message> message(
          msg_factory.GetPrototype(to_type->AsProto()->descriptor())->New());
      absl::Status error;
      functions::StringToProto(v.string_value(), message.get(), &error);
      ZETASQL_RETURN_IF_ERROR(error);
      // TODO: SerializeToCord returns false if not all required
      // fields are present.  If we want to allow missing required fields
      // We could use SerializePartialToCord().
      absl::Cord cord_value;
      std::string string_value;
      bool is_valid = message->SerializeToString(&string_value);
      cord_value = absl::Cord(string_value);
      if (!is_valid) {
        // TODO: This does not seem reachable given that we just
        // successfully parsed the string to a valid message.
        std::string output_string(ToStringLiteral(v.string_value()));
        output_string =
            PrettyTruncateUTF8(output_string, MAX_LITERAL_DISPLAY_LENGTH);
        return MakeEvalError() << "Invalid cast to type "
                               << to_type->DebugString()
                               << " from string: " << output_string;
      }
      return Value::Proto(to_type->AsProto(), std::move(cord_value));
    }

    case FCT(TYPE_BYTES, TYPE_STRING): {
      if (format.has_value()) {
        std::string output;
        ZETASQL_RETURN_IF_ERROR(
            functions::BytesToString(v.bytes_value(), format.value(), &output));
        return Value::String(output);
      }

      const std::string& utf8 = v.bytes_value();
      // No escaping is needed since the bytes value is already unescaped.
      if (!IsWellFormedUTF8(utf8)) {
        return MakeEvalError() << "Invalid cast of bytes to UTF8 string";
      }
      return Value::String(utf8);
    }

    case FCT(TYPE_BYTES, TYPE_PROTO):
      // Opaque proto support does not affect this implementation, which does
      // no validation.
      return Value::Proto(to_type->AsProto(), absl::Cord(v.bytes_value()));
    case FCT(TYPE_DATE, TYPE_STRING): {
      std::string date;
      if (format.has_value()) {
        ZETASQL_RETURN_IF_ERROR(functions::CastFormatDateToString(
            format.value(), v.date_value(), &date));
      } else {
        ZETASQL_RETURN_IF_ERROR(functions::ConvertDateToString(v.date_value(), &date));
      }
      return Value::String(date);
    }

    case FCT(TYPE_ENUM, TYPE_STRING):
      return Value::String(v.enum_name());

    case FCT(TYPE_ENUM, TYPE_INT32):
      return Value::Int32(v.enum_value());
    case FCT(TYPE_ENUM, TYPE_INT64):
      return NumericValueCast<int32_t, int64_t>(v.enum_value());
    case FCT(TYPE_ENUM, TYPE_UINT32):
      return NumericValueCast<int32_t, uint32_t>(v.enum_value());
    case FCT(TYPE_ENUM, TYPE_UINT64):
      return NumericValueCast<int32_t, uint64_t>(v.enum_value());

    case FCT(TYPE_ENUM, TYPE_ENUM): {
      if (!v.type()->Equivalent(to_type)) {
        return MakeSqlError() << "Invalid enum cast from "
                              << v.type()->DebugString() << " to "
                              << to_type->DebugString();
      }
      const Value to_value = Value::Enum(to_type->AsEnum(), v.enum_value());
      if (!to_value.is_valid()) {
        return MakeEvalError() << "Out of range enum value " << v.ToInt64()
                               << " when converting enum type "
                               << to_type->DebugString()
                               << " to a different definition of the same enum";
      }
      return to_value;
    }

    case FCT(TYPE_STRING, TYPE_TIME): {
      TimeValue time;
      if (format.has_value()) {
        ZETASQL_RETURN_IF_ERROR(functions::CastStringToTime(
            format.value(), v.string_value(),
            GetTimestampScale(language_options()), &time));
      } else {
        ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTime(
            v.string_value(), GetTimestampScale(language_options()), &time));
      }
      return Value::Time(time);
    }
    case FCT(TYPE_TIME, TYPE_STRING): {
      std::string result;
      if (format.has_value()) {
        ZETASQL_RETURN_IF_ERROR(functions::CastFormatTimeToString(
            format.value(), v.time_value(), &result));
      } else {
        ZETASQL_RETURN_IF_ERROR(functions::ConvertTimeToString(
            v.time_value(), GetTimestampScale(language_options()), &result));
      }
      return Value::String(result);
    }
    case FCT(TYPE_TIMESTAMP, TYPE_TIME): {
      TimeValue time;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToTime(
          v.ToTime(), default_timezone(), &time));
      return Value::Time(time);
    }

    case FCT(TYPE_STRING, TYPE_DATETIME): {
      DatetimeValue datetime;
      if (format.has_value()) {
        if (!current_date().has_value()) {
          return MakeEvalError() << "current timestamp is not set";
        }

        ZETASQL_RETURN_IF_ERROR(functions::CastStringToDatetime(
            format.value(), v.string_value(),
            GetTimestampScale(language_options()), current_date().value(),
            &datetime));
      } else {
        ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToDatetime(
            v.string_value(), GetTimestampScale(language_options()),
            &datetime));
      }
      return Value::Datetime(datetime);
    }
    case FCT(TYPE_DATETIME, TYPE_STRING): {
      std::string result;
      if (format.has_value()) {
        ZETASQL_RETURN_IF_ERROR(functions::CastFormatDatetimeToString(
            format.value(), v.datetime_value(), &result));
      } else {
        ZETASQL_RETURN_IF_ERROR(functions::ConvertDatetimeToString(
            v.datetime_value(), GetTimestampScale(language_options()),
            &result));
      }
      return Value::String(result);
    }
    case FCT(TYPE_DATETIME, TYPE_TIMESTAMP): {
      absl::Time time;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertDatetimeToTimestamp(
          v.datetime_value(), default_timezone(), &time));
      return Value::Timestamp(time);
    }
    case FCT(TYPE_TIMESTAMP, TYPE_DATETIME): {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToDatetime(
          v.ToTime(), default_timezone(), &datetime));
      return Value::Datetime(datetime);
    }
    case FCT(TYPE_DATETIME, TYPE_DATE): {
      int32_t date;
      ZETASQL_RETURN_IF_ERROR(functions::ExtractFromDatetime(
          functions::DATE, v.datetime_value(), &date));
      return Value::Date(date);
    }
    case FCT(TYPE_DATE, TYPE_DATETIME): {
      DatetimeValue datetime;
      ZETASQL_CHECK_OK(
          functions::ConstructDatetime(v.date_value(), TimeValue(), &datetime));
      return Value::Datetime(datetime);
    }
    case FCT(TYPE_DATETIME, TYPE_TIME): {
      TimeValue time;
      ZETASQL_RETURN_IF_ERROR(
          functions::ExtractTimeFromDatetime(v.datetime_value(), &time));
      return Value::Time(time);
    }

    case FCT(TYPE_INTERVAL, TYPE_STRING): {
      return Value::String(v.interval_value().ToString());
    }
    case FCT(TYPE_STRING, TYPE_INTERVAL): {
      ZETASQL_ASSIGN_OR_RETURN(IntervalValue interval,
                       IntervalValue::Parse(v.string_value()));
      return Value::Interval(interval);
    }

    case FCT(TYPE_STRUCT, TYPE_STRUCT): {
      const StructType* v_type = v.type()->AsStruct();
      std::vector<Value> casted_field_values(v_type->num_fields());
      if (v_type->num_fields() != to_type->AsStruct()->num_fields()) {
        return MakeSqlError() << "Unsupported cast from "
                              << v.type()->DebugString() << " to "
                              << to_type->DebugString();
      }
      for (int i = 0; i < v_type->num_fields(); ++i) {
        ZETASQL_ASSIGN_OR_RETURN(
            casted_field_values[i],
            CastValue(v.field(i), to_type->AsStruct()->field(i).type));
      }

      return Value::Struct(to_type->AsStruct(), casted_field_values);
    }

    case FCT(TYPE_PROTO, TYPE_STRING): {
      if (v.type()->AsProto()->descriptor() == nullptr) {
        // TODO: Cannot currently get here.  The implementation of
        // opaque protos may affect this.
        return MakeEvalError() << "Invalid cast from opaque proto type "
                               << to_type->DebugString() << " to string";
      }
      google::protobuf::DynamicMessageFactory msg_factory;
      std::unique_ptr<google::protobuf::Message> message(
          msg_factory.GetPrototype(v.type()->AsProto()->descriptor())->New());
      bool is_valid = message->ParsePartialFromString(std::string(v.ToCord()));
      if (!is_valid) {
        std::string display_bytes =
            PrettyTruncateUTF8(ToBytesLiteral(std::string(v.ToCord())),
                               MAX_LITERAL_DISPLAY_LENGTH);
        return MakeEvalError() << "Invalid cast to string from type "
                               << v.type()->DebugString() << ": "
                               << display_bytes;
      }
      absl::Status error;
      absl::Cord printed_msg;
      functions::ProtoToString(message.get(), &printed_msg, &error);
      ZETASQL_RETURN_IF_ERROR(error);
      return Value::String(std::string(printed_msg));
    }

    case FCT(TYPE_PROTO, TYPE_BYTES):
      // Opaque proto support does not affect this implementation, which does
      // no validation.
      return Value::Bytes(v.ToCord());

    case FCT(TYPE_PROTO, TYPE_PROTO):
      if (!v.type()->Equivalent(to_type)) {
        return MakeSqlError() << "Invalid proto cast from "
                              << v.type()->DebugString() << " to "
                              << to_type->DebugString();
      }
      // We don't currently do any validity checking on the serialized bytes.
      return Value::Proto(to_type->AsProto(), v.ToCord());

    case FCT(TYPE_ARRAY, TYPE_ARRAY): {
      ZETASQL_RETURN_IF_ERROR(ValidateCoercion(v, to_type));

      const Type* to_element_type = to_type->AsArray()->element_type();
      std::vector<Value> casted_elements(v.num_elements());
      for (int i = 0; i < v.num_elements(); ++i) {
        if (v.element(i).is_null()) {
          casted_elements[i] = Value::Null(to_element_type);
        } else {
          ZETASQL_ASSIGN_OR_RETURN(casted_elements[i],
                           CastValue(v.element(i), to_element_type));
        }
      }
      return InternalValue::ArrayChecked(to_type->AsArray(),
                                         InternalValue::order_kind(v),
                                         std::move(casted_elements));
    }

    case FCT(TYPE_NUMERIC, TYPE_INT32):
      return NumericCast<NumericValue, int32_t>(v);
    case FCT(TYPE_NUMERIC, TYPE_INT64):
      return NumericCast<NumericValue, int64_t>(v);
    case FCT(TYPE_NUMERIC, TYPE_UINT32):
      return NumericCast<NumericValue, uint32_t>(v);
    case FCT(TYPE_NUMERIC, TYPE_UINT64):
      return NumericCast<NumericValue, uint64_t>(v);
    case FCT(TYPE_NUMERIC, TYPE_FLOAT):
      return NumericCast<NumericValue, float>(v);
    case FCT(TYPE_NUMERIC, TYPE_DOUBLE):
      return NumericCast<NumericValue, double>(v);
    case FCT(TYPE_NUMERIC, TYPE_BIGNUMERIC):
      return NumericCast<NumericValue, BigNumericValue>(v);
    case FCT(TYPE_NUMERIC, TYPE_STRING):
      if (format.has_value()) {
        return NumericToStringWithFormat(v, format.value(),
                                         language_options().product_mode());
      } else {
        return NumericToString<NumericValue>(v);
      }
    case FCT(TYPE_BIGNUMERIC, TYPE_INT32):
      return NumericCast<BigNumericValue, int32_t>(v);
    case FCT(TYPE_BIGNUMERIC, TYPE_INT64):
      return NumericCast<BigNumericValue, int64_t>(v);
    case FCT(TYPE_BIGNUMERIC, TYPE_UINT32):
      return NumericCast<BigNumericValue, uint32_t>(v);
    case FCT(TYPE_BIGNUMERIC, TYPE_UINT64):
      return NumericCast<BigNumericValue, uint64_t>(v);
    case FCT(TYPE_BIGNUMERIC, TYPE_FLOAT):
      return NumericCast<BigNumericValue, float>(v);
    case FCT(TYPE_BIGNUMERIC, TYPE_DOUBLE):
      return NumericCast<BigNumericValue, double>(v);
    case FCT(TYPE_BIGNUMERIC, TYPE_NUMERIC):
      return NumericCast<BigNumericValue, NumericValue>(v);
    case FCT(TYPE_BIGNUMERIC, TYPE_STRING):
      if (format.has_value()) {
        return NumericToStringWithFormat(v, format.value(),
                                         language_options().product_mode());
      } else {
        return NumericToString<BigNumericValue>(v);
      }

    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unimplemented cast from " << v.type()->DebugString() << " to "
             << to_type->DebugString();
  }
}

// CastContextWithValidation implements a validated cast. Used by CastValue.
class CastContextWithValidation : public CastContext {
 public:
  CastContextWithValidation(absl::TimeZone default_timezone,
                            absl::optional<absl::Time> current_timestamp,
                            const LanguageOptions& language_options,
                            Catalog* catalog)
      : CastContext(default_timezone, current_timestamp, language_options),
        catalog_(catalog) {}

 private:
  absl::StatusOr<Value> CastWithExtendedType(
      const Value& from_value, const Type* to_type) const override {
    if (catalog_ == nullptr) {
      return zetasql_base::FailedPreconditionErrorBuilder()
             << "Attempt to cast a Value of extened type without providing a "
                "Catalog";
    }

    Catalog::FindConversionOptions options(
        /*is_explicit=*/true, Catalog::ConversionSourceExpressionKind::kLiteral,
        language_options().product_mode());
    Conversion conversion = Conversion::Invalid();
    ZETASQL_RETURN_IF_ERROR(catalog_->FindConversion(from_value.type(), to_type,
                                             options, &conversion));
    return conversion.evaluator().Eval(from_value);
  }

  absl::Status ValidateCoercion(const Value& from_value,
                                const Type* to_type) const override {
    SignatureMatchResult result;
    TypeFactory type_factory;
    Coercer coercer(&type_factory, &language_options(), catalog_);
    if (!coercer.CoercesTo(InputArgumentType(from_value), to_type,
                           /*is_explicit=*/true, &result)) {
      return MakeSqlError()
             << "Unsupported cast from " << from_value.type()->DebugString()
             << " to " << to_type->DebugString();
    }

    return absl::OkStatus();
  }

  mutable Catalog* catalog_;
};

// CastContextWithoutValidation implements an unvalidated cast. Used by the
// CastValueWithoutTypeValidation.
class CastContextWithoutValidation : public CastContext {
 public:
  CastContextWithoutValidation(
      absl::TimeZone default_timezone,
      absl::optional<absl::Time> current_timestamp,
      const LanguageOptions& language_options,
      const ExtendedCompositeCastEvaluator* extended_cast_evaluator)
      : CastContext(default_timezone, current_timestamp, language_options),
        extended_cast_evaluator_(extended_cast_evaluator) {}

  absl::StatusOr<Value> CastWithExtendedType(
      const Value& from_value, const Type* to_type) const override {
    if (extended_cast_evaluator_ == nullptr) {
      return zetasql_base::FailedPreconditionErrorBuilder()
             << "Attempt to cast a Value of extened type without providing an "
                "extended conversion function";
    }

    return extended_cast_evaluator_->Eval(from_value, to_type);
  }

  absl::Status ValidateCoercion(const Value& from_value,
                                const Type* to_type) const override {
    return absl::OkStatus();
  }

 private:
  const ExtendedCompositeCastEvaluator* extended_cast_evaluator_;
};

}  // namespace

absl::StatusOr<Value> CastValue(const Value& from_value,
                                absl::TimeZone default_timezone,
                                const LanguageOptions& language_options,
                                const Type* to_type, Catalog* catalog) {
  return CastValue(from_value, default_timezone, language_options, to_type,
                   /*format=*/absl::nullopt, catalog);
}

absl::StatusOr<Value> CastValue(const Value& from_value,
                                absl::TimeZone default_timezone,
                                const LanguageOptions& language_options,
                                const Type* to_type,
                                const absl::optional<std::string>& format,
                                Catalog* catalog) {
  return CastContextWithValidation(default_timezone,
                                   /*current_timestamp=*/absl::nullopt,
                                   language_options, catalog)
      .CastValue(from_value, to_type, format);
}

namespace internal {

absl::StatusOr<Value> CastValueWithoutTypeValidation(
    const Value& from_value, absl::TimeZone default_timezone,
    absl::optional<absl::Time> current_timestamp,
    const LanguageOptions& language_options, const Type* to_type,
    const absl::optional<std::string>& format,
    const absl::optional<std::string>& explicit_time_zone,
    const ExtendedCompositeCastEvaluator* extended_cast_evaluator) {
  absl::TimeZone timezone = default_timezone;
  if (explicit_time_zone.has_value()) {
    ZETASQL_RETURN_IF_ERROR(
        functions::MakeTimeZone(explicit_time_zone.value(), &timezone));
  }
  return CastContextWithoutValidation(timezone,
                                      current_timestamp,
                                      language_options,
                                      extended_cast_evaluator)
      .CastValue(from_value, to_type, format);
}

const CastHashMap& GetZetaSQLCasts() {
  static const CastHashMap* cast_hash_map = InitializeZetaSQLCasts();
  return *cast_hash_map;
}

const CastFormatMap& GetCastFormatMap() {
  static const CastFormatMap* cast_format_map = nullptr;
  if (cast_format_map == nullptr) {
    CastFormatMap* map = new CastFormatMap();
    map->insert(
        {{TYPE_STRING, TYPE_BYTES}, functions::ValidateFormat});
    map->insert(
        {{TYPE_BYTES, TYPE_STRING}, functions::ValidateFormat});

    // String to Date/DateTime/Time/Timestamp
    map->insert(
        {{TYPE_STRING, TYPE_DATE}, ValidateFormatStringToDate});
    map->insert(
        {{TYPE_STRING, TYPE_DATETIME}, ValidateFormatStringToDatetime});
    map->insert(
        {{TYPE_STRING, TYPE_TIME}, ValidateFormatStringToTime});
    map->insert(
        {{TYPE_STRING, TYPE_TIMESTAMP}, ValidateFormatStringToTimestamp});

    // Date/DateTime/Time/Timestamp to String
    map->insert(
        {{TYPE_DATE, TYPE_STRING}, ValidateFormatStringFromDate});
    map->insert(
        {{TYPE_TIME, TYPE_STRING}, ValidateFormatStringFromTime});
    map->insert(
        {{TYPE_DATETIME, TYPE_STRING}, ValidateFormatStringFromDateTime});
    map->insert(
        {{TYPE_TIMESTAMP, TYPE_STRING}, ValidateFormatStringFromTimestamp});

    // Numerical types to String
    map->insert({{TYPE_INT32, TYPE_STRING},
                 zetasql::functions::ValidateNumericalToStringFormat});
    map->insert({{TYPE_UINT32, TYPE_STRING},
                 zetasql::functions::ValidateNumericalToStringFormat});
    map->insert({{TYPE_INT64, TYPE_STRING},
                 zetasql::functions::ValidateNumericalToStringFormat});
    map->insert({{TYPE_UINT64, TYPE_STRING},
                 zetasql::functions::ValidateNumericalToStringFormat});
    map->insert({{TYPE_FLOAT, TYPE_STRING},
                 zetasql::functions::ValidateNumericalToStringFormat});
    map->insert({{TYPE_DOUBLE, TYPE_STRING},
                 zetasql::functions::ValidateNumericalToStringFormat});
    map->insert({{TYPE_NUMERIC, TYPE_STRING},
                 zetasql::functions::ValidateNumericalToStringFormat});
    map->insert({{TYPE_BIGNUMERIC, TYPE_STRING},
                 zetasql::functions::ValidateNumericalToStringFormat});

    cast_format_map = map;
  }
  return *cast_format_map;
}

}  // namespace internal

absl::StatusOr<ConversionEvaluator> ConversionEvaluator::Create(
    const Type* from_type, const Type* to_type, const Function* function) {
  ZETASQL_RET_CHECK(from_type);
  ZETASQL_RET_CHECK(to_type);
  ZETASQL_RET_CHECK(function);
  ZETASQL_RET_CHECK(!from_type->Equals(to_type));

  return ConversionEvaluator(from_type, to_type, function);
}

FunctionSignature ConversionEvaluator::GetFunctionSignature(
    const Type* from_type, const Type* to_type) {
  return FunctionSignature({to_type, /*num_occurrences=*/1},
                           {{from_type, /*num_occurrences=*/1}},
                           /*context_ptr=*/nullptr);
}

absl::StatusOr<Value> ConversionEvaluator::Eval(const Value& from_value) const {
  if (!is_valid()) {
    return zetasql_base::FailedPreconditionErrorBuilder()
           << "Attempt to cast a value using invalid conversion";
  }

  if (!from_type_->Equals(from_value.type())) {
    return zetasql_base::InvalidArgumentErrorBuilder()
           << "Type of casted value doesn't match the source type of "
              "conversion";
  }

  ZETASQL_ASSIGN_OR_RETURN(auto evaluator, function_->GetFunctionEvaluatorFactory()(
                                       function_signature()));
  return evaluator({from_value});
}

absl::StatusOr<Conversion> Conversion::Create(
    const Type* from_type, const Type* to_type, const Function* function,
    const CastFunctionProperty& property) {
  ZETASQL_ASSIGN_OR_RETURN(ConversionEvaluator evaluator,
                   ConversionEvaluator::Create(from_type, to_type, function));
  return Create(evaluator, property);
}

absl::StatusOr<Conversion> Conversion::Create(
    const ConversionEvaluator& evaluator,
    const CastFunctionProperty& property) {
  ZETASQL_RET_CHECK(evaluator.is_valid());
  return Conversion(evaluator, property);
}

bool Conversion::IsMatch(const Catalog::FindConversionOptions& options) const {
  if (!is_valid()) {
    return false;
  }

  // Conversion can be: 1) explicit 2) implicit 3) implicit for literals and
  // explicit for other expressions 4) implicit for literals & parameters and
  // explicit for other expressions. If conversion is implicit, it also always
  // can be applied explicitly.

  if (options.is_explicit()) {
    return true;  // All types of conversions can be applied explicitly.
  }

  // We are looking for implicit conversion below and need to check whether it
  // can be applied to all kinds of expression (unconditional) or only to some.

  if (property().is_implicit()) {
    return true;  // Conversion is unconditionally implicit.
  }

  switch (options.source_kind()) {
    case Catalog::ConversionSourceExpressionKind::kLiteral:
      return property().type == CastFunctionType::EXPLICIT_OR_LITERAL ||
             property().type ==
                 CastFunctionType::EXPLICIT_OR_LITERAL_OR_PARAMETER;
    case Catalog::ConversionSourceExpressionKind::kParameter:
      return property().type ==
             CastFunctionType::EXPLICIT_OR_LITERAL_OR_PARAMETER;
    default:
      return false;
  }
}

absl::StatusOr<Value> ExtendedCompositeCastEvaluator::Eval(
    const Value& from_value, const Type* to_type) const {
  for (const ConversionEvaluator& evaluator : evaluators_) {
    if (evaluator.from_type()->Equals(from_value.type()) &&
        evaluator.to_type()->Equals(to_type)) {
      return evaluator.Eval(from_value);
    }
  }

  return zetasql_base::InvalidArgumentErrorBuilder()
         << "Conversion from type " << from_value.type()->DebugString()
         << " to type " << to_type->DebugString()
         << " is not found in ExtendedCompositeCastEvaluator";
}

}  // namespace zetasql
