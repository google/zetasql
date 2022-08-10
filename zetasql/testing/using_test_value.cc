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

// This file may be included at the top of other *_test.cc files to
// include the useful zetasql type/value tools in the tests namespace.
//
// Non test use is discouraged.
// Inclusion in a *.h file is forbidden by the styleguide.
//
// Use with caution.

#include "zetasql/testing/test_value.h"

// NOLINTBEGIN(google-global-names-in-headers)
using zetasql::test_values::Array;
using zetasql::test_values::kIgnoresOrder;
using zetasql::test_values::kPreservesOrder;
using zetasql::test_values::MakeArrayType;
using zetasql::test_values::MakeRangeType;
using zetasql::test_values::MakeStructType;
using zetasql::test_values::OrderPreservationKind;
using zetasql::test_values::Range;
using zetasql::test_values::Struct;
using zetasql::test_values::StructArray;

using zetasql::types::BigNumericType;
using zetasql::types::BoolType;
using zetasql::types::BytesType;
using zetasql::types::DatetimeType;
using zetasql::types::DateType;
using zetasql::types::DoubleType;
using zetasql::types::EmptyStructType;
using zetasql::types::FloatType;
using zetasql::types::GeographyType;
using zetasql::types::Int32Type;
using zetasql::types::Int64Type;
using zetasql::types::IntervalType;
using zetasql::types::JsonType;
using zetasql::types::NumericType;
using zetasql::types::StringType;
using zetasql::types::TimestampType;
using zetasql::types::TimeType;
using zetasql::types::Uint32Type;
using zetasql::types::Uint64Type;

using zetasql::types::BigNumericArrayType;
using zetasql::types::BoolArrayType;
using zetasql::types::BytesArrayType;
using zetasql::types::DateArrayType;
using zetasql::types::DatetimeArrayType;
using zetasql::types::DoubleArrayType;
using zetasql::types::FloatArrayType;
using zetasql::types::GeographyArrayType;
using zetasql::types::Int32ArrayType;
using zetasql::types::Int64ArrayType;
using zetasql::types::IntervalArrayType;
using zetasql::types::JsonArrayType;
using zetasql::types::NumericArrayType;
using zetasql::types::StringArrayType;
using zetasql::types::TimeArrayType;
using zetasql::types::TimestampArrayType;
using zetasql::types::Uint32ArrayType;
using zetasql::types::Uint64ArrayType;

using zetasql::values::Bool;
using zetasql::values::Bytes;
using zetasql::values::Date;
using zetasql::values::Datetime;
using zetasql::values::Double;
using zetasql::values::Enum;
using zetasql::values::Float;
using zetasql::values::Int32;
using zetasql::values::Int64;
using zetasql::values::Interval;
using zetasql::values::Json;
using zetasql::values::Numeric;
inline zetasql::Value NumericFromDouble(double v) {
  return Numeric(zetasql::NumericValue::FromDouble(v).value());
}
using zetasql::values::BigNumeric;
inline zetasql::Value BigNumericFromDouble(double v) {
  return BigNumeric(zetasql::BigNumericValue::FromDouble(v).value());
}
using zetasql::values::Json;
using zetasql::values::Proto;
using zetasql::values::String;
using zetasql::values::Time;
using zetasql::values::TimestampFromUnixMicros;
using zetasql::values::Uint32;
using zetasql::values::Uint64;

using zetasql::values::False;
using zetasql::values::True;

using zetasql::values::Null;
using zetasql::values::NullBigNumeric;
using zetasql::values::NullBool;
using zetasql::values::NullBytes;
using zetasql::values::NullDate;
using zetasql::values::NullDatetime;
using zetasql::values::NullDouble;
using zetasql::values::NullFloat;
using zetasql::values::NullGeography;
using zetasql::values::NullInt32;
using zetasql::values::NullInt64;
using zetasql::values::NullInterval;
using zetasql::values::NullJson;
using zetasql::values::NullNumeric;
using zetasql::values::NullString;
using zetasql::values::NullTime;
using zetasql::values::NullTimestamp;
using zetasql::values::NullUint32;
using zetasql::values::NullUint64;

using zetasql::values::BigNumericArray;
using zetasql::values::BoolArray;
using zetasql::values::BytesArray;
using zetasql::values::DoubleArray;
using zetasql::values::FloatArray;
using zetasql::values::Int32Array;
using zetasql::values::Int64Array;
using zetasql::values::JsonArray;
using zetasql::values::NumericArray;
using zetasql::values::StringArray;
using zetasql::values::Uint32Array;
using zetasql::values::Uint64Array;

using zetasql::values::EmptyArray;
// NOLINTEND(google-global-names-in-headers)
