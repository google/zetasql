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

#include "zetasql/public/cast.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "google/protobuf/dynamic_message.h"
#include "google/protobuf/message.h"
#include "zetasql/common/errors.h"
#include "zetasql/common/internal_value.h"
#include "zetasql/common/utf_util.h"
#include "zetasql/public/civil_time.h"
#include "zetasql/public/coercer.h"
#include "zetasql/public/functions/convert.h"
#include "zetasql/public/functions/convert_proto.h"
#include "zetasql/public/functions/convert_string.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/functions/datetime.pb.h"
#include "zetasql/public/input_argument_type.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/signature_match_result.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/type.pb.h"
#include <cstdint>
#include "absl/strings/cord.h"
#include "absl/strings/str_split.h"
#include "absl/time/time.h"
#include "zetasql/base/map_util.h"
#include "zetasql/base/source_location.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"

namespace zetasql {

#define MAX_LITERAL_DISPLAY_LENGTH 60


static functions::TimestampScale GetTimestampScale(
    const LanguageOptions& language_options) {
  if (language_options.LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
    return functions::kNanoseconds;
  } else {
    return functions::kMicroseconds;
  }
}

static void AddToCastMap(TypeKind from, TypeKind to, CastFunctionType type,
                         CastHashMap* map) {
  zetasql_base::InsertIfNotPresent(map, {from, to},
                          {type, Type::GetTypeCoercionCost(to, from)});
}

static const CastHashMap* InitializeZetaSQLCasts() {
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
  ADD_TO_MAP(STRING,     ENUM,       EXPLICIT_OR_LITERAL_OR_PARAMETER);
  ADD_TO_MAP(STRING,     PROTO,      EXPLICIT_OR_LITERAL_OR_PARAMETER);
  ADD_TO_MAP(STRING,     BOOL,       EXPLICIT);
  ADD_TO_MAP(STRING,     NUMERIC,    EXPLICIT);
  ADD_TO_MAP(STRING,     BIGNUMERIC, EXPLICIT);

  ADD_TO_MAP(BYTES,      BYTES,      IMPLICIT);
  ADD_TO_MAP(BYTES,      STRING,     EXPLICIT);
  ADD_TO_MAP(BYTES,      PROTO,      EXPLICIT_OR_LITERAL_OR_PARAMETER);

  ADD_TO_MAP(DATE,       DATE,       IMPLICIT);
  ADD_TO_MAP(DATE,       DATETIME,   EXPLICIT);
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

const CastHashMap& GetZetaSQLCasts() {
  static const CastHashMap* cast_hash_map = InitializeZetaSQLCasts();
  return *cast_hash_map;
}

// Returns a single uint64_t that represents an (input kind, output kind) pair.
// Useful for switching on a combination of two kinds.
static constexpr uint64_t FCT(TypeKind input_kind, TypeKind output_kind) {
  return ((static_cast<uint64_t>(input_kind) << 32) + output_kind);
}

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

template <typename FromType, typename ToType>
static zetasql_base::StatusOr<Value> NumericCast(const Value& value) {
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
static zetasql_base::StatusOr<Value> NumericValueCast(const FromType& in) {
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
static zetasql_base::StatusOr<Value> NumericToString(const Value& v) {
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
static zetasql_base::StatusOr<Value> StringToNumeric(const Value& v) {
  if (v.is_null()) return Value::MakeNull<T>();
  std::string value = v.string_value();
  T out;
  absl::Status error;
  if (zetasql::functions::StringToNumeric<T>(value, &out, &error)) {
    return Value::Make<T>(out);
  } else {
    return error;
  }
}

zetasql_base::StatusOr<Value> CastValue(const Value& from_value,
                                absl::TimeZone default_timezone,
                                const LanguageOptions& language_options,
                                const Type* to_type) {
  DCHECK(from_value.is_valid());
  // Use a shorter name inside the body of this method.
  const Value& v = from_value;

  if (v.type()->Equals(to_type)) {
    // Coercion from a value to the exact same type always works.
    return v;
  }
  // Check to see if the type kinds are castable.
  if (!zetasql_base::ContainsKey(GetZetaSQLCasts(),
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
      SignatureMatchResult result;
      TypeFactory type_factory;
      Coercer coercer(&type_factory, default_timezone, &language_options);
      if (!coercer.CoercesTo(InputArgumentType(v), to_type,
                             true /* is_explicit */, &result)) {
        return MakeSqlError() << "Unsupported cast from "
                              << v.type()->DebugString() << " to "
                              << to_type->DebugString();
      }
    }
    // We have already validated that this is a valid cast for NULL values,
    // so just return a NULL value of <to_type>.
    return Value::Null(to_type);
  }

  // TODO: Consider breaking this up, as the switch is extremely
  // large.
  switch (FCT(v.type()->kind(), to_type->kind())) {
    // Numeric casts. Identity casts are handled above.
    case FCT(TYPE_INT32, TYPE_INT64): return NumericCast<int32_t, int64_t>(v);
    case FCT(TYPE_INT32, TYPE_UINT32): return NumericCast<int32_t, uint32_t>(v);
    case FCT(TYPE_INT32, TYPE_UINT64): return NumericCast<int32_t, uint64_t>(v);
    case FCT(TYPE_INT32, TYPE_BOOL): return NumericCast<int32_t, bool>(v);
    case FCT(TYPE_INT32, TYPE_FLOAT): return NumericCast<int32_t, float>(v);
    case FCT(TYPE_INT32, TYPE_DOUBLE): return NumericCast<int32_t, double>(v);
    case FCT(TYPE_INT32, TYPE_STRING): return NumericToString<int32_t>(v);
    case FCT(TYPE_INT32, TYPE_NUMERIC):
      return NumericCast<int32_t, NumericValue>(v);
    case FCT(TYPE_INT32, TYPE_BIGNUMERIC):
      return NumericCast<int32_t, BigNumericValue>(v);

    case FCT(TYPE_UINT32, TYPE_INT32): return NumericCast<uint32_t, int32_t>(v);
    case FCT(TYPE_UINT32, TYPE_INT64): return NumericCast<uint32_t, int64_t>(v);
    case FCT(TYPE_UINT32, TYPE_UINT64): return NumericCast<uint32_t, uint64_t>(v);
    case FCT(TYPE_UINT32, TYPE_BOOL): return NumericCast<uint32_t, bool>(v);
    case FCT(TYPE_UINT32, TYPE_FLOAT): return NumericCast<uint32_t, float>(v);
    case FCT(TYPE_UINT32, TYPE_DOUBLE): return NumericCast<uint32_t, double>(v);
    case FCT(TYPE_UINT32, TYPE_STRING): return NumericToString<uint32_t>(v);
    case FCT(TYPE_UINT32, TYPE_NUMERIC):
      return NumericCast<uint32_t, NumericValue>(v);
    case FCT(TYPE_UINT32, TYPE_BIGNUMERIC):
      return NumericCast<uint32_t, BigNumericValue>(v);

    case FCT(TYPE_INT64, TYPE_INT32): return NumericCast<int64_t, int32_t>(v);
    case FCT(TYPE_INT64, TYPE_UINT32): return NumericCast<int64_t, uint32_t>(v);
    case FCT(TYPE_INT64, TYPE_UINT64): return NumericCast<int64_t, uint64_t>(v);
    case FCT(TYPE_INT64, TYPE_BOOL): return NumericCast<int64_t, bool>(v);
    case FCT(TYPE_INT64, TYPE_FLOAT): return NumericCast<int64_t, float>(v);
    case FCT(TYPE_INT64, TYPE_DOUBLE): return NumericCast<int64_t, double>(v);
    case FCT(TYPE_INT64, TYPE_STRING): return NumericToString<int64_t>(v);
    case FCT(TYPE_INT64, TYPE_NUMERIC):
      return NumericCast<int64_t, NumericValue>(v);
    case FCT(TYPE_INT64, TYPE_BIGNUMERIC):
      return NumericCast<int64_t, BigNumericValue>(v);

    case FCT(TYPE_UINT64, TYPE_INT32): return NumericCast<uint64_t, int32_t>(v);
    case FCT(TYPE_UINT64, TYPE_INT64): return NumericCast<uint64_t, int64_t>(v);
    case FCT(TYPE_UINT64, TYPE_UINT32): return NumericCast<uint64_t, uint32_t>(v);
    case FCT(TYPE_UINT64, TYPE_BOOL): return NumericCast<uint64_t, bool>(v);
    case FCT(TYPE_UINT64, TYPE_FLOAT): return NumericCast<uint64_t, float>(v);
    case FCT(TYPE_UINT64, TYPE_DOUBLE): return NumericCast<uint64_t, double>(v);
    case FCT(TYPE_UINT64, TYPE_STRING): return NumericToString<uint64_t>(v);
    case FCT(TYPE_UINT64, TYPE_NUMERIC):
      return NumericCast<uint64_t, NumericValue>(v);
    case FCT(TYPE_UINT64, TYPE_BIGNUMERIC):
      return NumericCast<uint64_t, BigNumericValue>(v);

    case FCT(TYPE_BOOL, TYPE_INT32): return NumericCast<bool, int32_t>(v);
    case FCT(TYPE_BOOL, TYPE_INT64): return NumericCast<bool, int64_t>(v);
    case FCT(TYPE_BOOL, TYPE_UINT32): return NumericCast<bool, uint32_t>(v);
    case FCT(TYPE_BOOL, TYPE_UINT64): return NumericCast<bool, uint64_t>(v);
    case FCT(TYPE_BOOL, TYPE_STRING): return NumericToString<bool>(v);

    case FCT(TYPE_FLOAT, TYPE_INT32): return NumericCast<float, int32_t>(v);
    case FCT(TYPE_FLOAT, TYPE_INT64): return NumericCast<float, int64_t>(v);
    case FCT(TYPE_FLOAT, TYPE_UINT32): return NumericCast<float, uint32_t>(v);
    case FCT(TYPE_FLOAT, TYPE_UINT64): return NumericCast<float, uint64_t>(v);
    case FCT(TYPE_FLOAT, TYPE_DOUBLE): return NumericCast<float, double>(v);
    case FCT(TYPE_FLOAT, TYPE_STRING): return NumericToString<float>(v);
    case FCT(TYPE_FLOAT, TYPE_NUMERIC):
      return NumericCast<float, NumericValue>(v);
    case FCT(TYPE_FLOAT, TYPE_BIGNUMERIC):
      return NumericCast<float, BigNumericValue>(v);

    case FCT(TYPE_DOUBLE, TYPE_INT32): return NumericCast<double, int32_t>(v);
    case FCT(TYPE_DOUBLE, TYPE_INT64): return NumericCast<double, int64_t>(v);
    case FCT(TYPE_DOUBLE, TYPE_UINT32): return NumericCast<double, uint32_t>(v);
    case FCT(TYPE_DOUBLE, TYPE_UINT64): return NumericCast<double, uint64_t>(v);
    case FCT(TYPE_DOUBLE, TYPE_FLOAT): return NumericCast<double, float>(v);
    case FCT(TYPE_DOUBLE, TYPE_STRING): return NumericToString<double>(v);
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
    case FCT(TYPE_STRING, TYPE_INT32): return StringToNumeric<int32_t>(v);
    case FCT(TYPE_STRING, TYPE_INT64): return StringToNumeric<int64_t>(v);
    case FCT(TYPE_STRING, TYPE_UINT32): return StringToNumeric<uint32_t>(v);
    case FCT(TYPE_STRING, TYPE_UINT64): return StringToNumeric<uint64_t>(v);
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
      ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToDate(v.string_value(), &date));
      return Value::Date(date);
    }
    case FCT(TYPE_STRING, TYPE_TIMESTAMP): {
      // TODO: These should be using the non-deprecated signature
      // that includes an argument to indicate if a timezone is allowed in
      // the string or not.  If not allowed and there is a timezone then
      // an error should be provided.
      if (language_options.LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
        absl::Time timestamp;
        ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTimestamp(
            v.string_value(), default_timezone, functions::kNanoseconds,
            true /* allow_tz_in_str */, &timestamp));
        return Value::Timestamp(timestamp);
      } else {
        int64_t timestamp;
        ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTimestamp(
            v.string_value(), default_timezone, functions::kMicroseconds,
            &timestamp));
        return Value::TimestampFromUnixMicros(timestamp);
      }
    }
    case FCT(TYPE_TIMESTAMP, TYPE_STRING): {
      std::string timestamp;
      if (language_options.LanguageFeatureEnabled(FEATURE_TIMESTAMP_NANOS)) {
        ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToString(
            v.ToTime(), functions::kNanoseconds, default_timezone, &timestamp));
      } else {
        ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToStringWithTruncation(
            v.ToUnixMicros(), functions::kMicroseconds, default_timezone,
            &timestamp));
      }
      return Value::String(timestamp);
    }
    case FCT(TYPE_DATE, TYPE_TIMESTAMP): {
      int64_t timestamp;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertDateToTimestamp(
          v.date_value(), functions::kMicroseconds, default_timezone,
          &timestamp));
      return Value::TimestampFromUnixMicros(timestamp);
    }
    case FCT(TYPE_TIMESTAMP, TYPE_DATE): {
      int32_t date;
      ZETASQL_RETURN_IF_ERROR(ExtractFromTimestamp(
          functions::DateTimestampPart::DATE, v.ToUnixMicros(),
          functions::kMicroseconds, default_timezone, &date));
      return Value::Date(date);
    }
    case FCT(TYPE_STRING, TYPE_BYTES):
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
      ZETASQL_RETURN_IF_ERROR(functions::ConvertDateToString(v.date_value(), &date));
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
      ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToTime(
          v.string_value(), GetTimestampScale(language_options), &time));
      return Value::Time(time);
    }
    case FCT(TYPE_TIME, TYPE_STRING): {
      std::string result;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertTimeToString(
          v.time_value(), GetTimestampScale(language_options), &result));
      return Value::String(result);
    }
    case FCT(TYPE_TIMESTAMP, TYPE_TIME): {
      TimeValue time;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToTime(
          v.ToTime(), default_timezone, &time));
      return Value::Time(time);
    }

    case FCT(TYPE_STRING, TYPE_DATETIME): {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertStringToDatetime(
          v.string_value(), GetTimestampScale(language_options), &datetime));
      return Value::Datetime(datetime);
    }
    case FCT(TYPE_DATETIME, TYPE_STRING): {
      std::string result;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertDatetimeToString(
          v.datetime_value(), GetTimestampScale(language_options), &result));
      return Value::String(result);
    }
    case FCT(TYPE_DATETIME, TYPE_TIMESTAMP): {
      absl::Time time;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertDatetimeToTimestamp(
          v.datetime_value(), default_timezone, &time));
      return Value::Timestamp(time);
    }
    case FCT(TYPE_TIMESTAMP, TYPE_DATETIME): {
      DatetimeValue datetime;
      ZETASQL_RETURN_IF_ERROR(functions::ConvertTimestampToDatetime(
          v.ToTime(), default_timezone, &datetime));
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
            CastValue(v.field(i), default_timezone, language_options,
                      to_type->AsStruct()->field(i).type));
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
      SignatureMatchResult result;
      TypeFactory type_factory;
      Coercer coercer(&type_factory, default_timezone, &language_options);
      if (!coercer.CoercesTo(InputArgumentType(v), to_type,
                             true /* is_explicit */, &result)) {
        return MakeSqlError() << "Unsupported cast from "
                              << v.type()->DebugString() << " to "
                              << to_type->DebugString();
      }

      const Type* to_element_type = to_type->AsArray()->element_type();
      std::vector<Value> casted_elements(v.num_elements());
      for (int i = 0; i < v.num_elements(); ++i) {
        if (v.element(i).is_null()) {
          casted_elements[i] = Value::Null(to_element_type);
        } else {
          ZETASQL_ASSIGN_OR_RETURN(casted_elements[i],
                           CastValue(v.element(i), default_timezone,
                                     language_options, to_element_type));
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
      return NumericToString<NumericValue>(v);

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
      return NumericToString<BigNumericValue>(v);

    // TODO: implement missing casts.
    default:
      return ::zetasql_base::UnimplementedErrorBuilder()
             << "Unimplemented cast from " << v.type()->DebugString() << " to "
             << to_type->DebugString();
  }
}

Conversion::Conversion(const Type* from_type, const Type* to_type,
                       const Function* function,
                       const CastFunctionProperty& property)
    : from_type_(from_type),
      to_type_(to_type),
      function_(function),
      cast_function_property_(property) {
  DCHECK(from_type);
  DCHECK(to_type);
  DCHECK(function);
  DCHECK(!from_type->Equals(to_type));
}

FunctionSignature Conversion::GetFunctionSignature(const Type* from_type,
                                                   const Type* to_type) {
  return FunctionSignature({to_type}, {{from_type}}, /*context_ptr=*/nullptr);
}

zetasql_base::StatusOr<Value> Conversion::CastValue(const Value& from_value) const {
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

  if (cast_function_property().is_implicit()) {
    return true;  // Conversion is unconditionally implicit.
  }

  switch (options.source_kind()) {
    case Catalog::ConversionSourceExpressionKind::kLiteral:
      return cast_function_property().type ==
                 CastFunctionType::EXPLICIT_OR_LITERAL ||
             cast_function_property().type ==
                 CastFunctionType::EXPLICIT_OR_LITERAL_OR_PARAMETER;
    case Catalog::ConversionSourceExpressionKind::kParameter:
      return cast_function_property().type ==
             CastFunctionType::EXPLICIT_OR_LITERAL_OR_PARAMETER;
    default:
      return false;
  }
}

}  // namespace zetasql
